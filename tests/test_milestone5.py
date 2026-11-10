"""MediSlot Milestone 5 - every offline test in ONE file.

Sections (search for the banner lines):
  * Shared test support          - fakes for RDS (SQLite), DynamoDB, EventBridge, SSM and the World builder
  * MANDATORY TEST 1             - concurrency: tests named test_mandatory1_*   (pytest -k mandatory1)
  * MANDATORY TEST 2             - idempotency: tests named test_mandatory2_*   (pytest -k mandatory2)
  * Refund tiers                 - every boundary to the microsecond
  * Booking API edge cases       - validation, patient limits, isolation, cancellation, doctor slot cancellation
  * Family members               - one account booking for several people
  * Dashboard pages              - Book / appointments / refunds / family / doctor "cancel slot"
  * Wiring                       - single schema.sql, API Gateway routes, IAM, SSM, CI packaging

The tests against the DEPLOYED API live in tests/integration/test_booking_live_api.py (they need the stack and
are run by the deploy workflow). Tests for Milestones 2-4 are unchanged in their own files.
"""
import hashlib
import json
import re
import sqlite3
import threading
import time
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from decimal import Decimal
from pathlib import Path
from zoneinfo import ZoneInfo

import pymysql
import pytest
import yaml
from botocore.exceptions import ClientError

from support import FakeMysql, api_event, load_lambda
from test_patient_dashboard import FakeResp, doc_profile, env, login_patient, slots_payload  # noqa: F401 (env is a fixture)


# ==============================================================================================================
# Shared test support: SQLite stand-in for RDS, DynamoDB/EventBridge/SSM fakes, World builder
# ==============================================================================================================
# Fakes used by every section below.
#
# * BookingDb: the shared in-memory SQLite stand-in (tests/support.py FakeMysql) plus the appointments and refund_ledger
#   tables. Each `with dbconn(...)` block is one serialised "connection" that rolls back anything it did not commit,
#   exactly like closing a PyMySQL connection. `FOR UPDATE` is accepted (the serialisation provides the locking).
#   The DynamoDB part is NOT serialised: parallel bookings really race on the slot item.
# * FakeDynamo: a thread-safe DynamoDB low-level client (get/put/update/delete item, transact_write_items) that
#   EVALUATES the condition and update expressions the Lambda sends, so a wrong condition fails a test.
# * FakeEvents / FakeSsm: EventBridge put_events and SSM get_parameters.

sqlite3.register_adapter(datetime, lambda d: d.isoformat(sep=" ", timespec="microseconds"))
sqlite3.register_adapter(Decimal, lambda d: str(d))

booking = load_lambda("booking", "booking_under_test")

BOOKING_SCHEMA = """
CREATE TABLE patient_dependents (dependent_id INTEGER PRIMARY KEY AUTOINCREMENT, patient_id INTEGER NOT NULL,
    first_name TEXT NOT NULL, last_name TEXT NOT NULL, relation TEXT NOT NULL, date_of_birth TEXT NOT NULL, gender TEXT,
    is_active INTEGER NOT NULL DEFAULT 1, created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CHECK (relation IN ('Mother','Father','Spouse','Child','Sibling','Grandparent','Grandchild','Other')));
CREATE TABLE appointments (appointment_id INTEGER PRIMARY KEY AUTOINCREMENT, patient_id INTEGER NOT NULL,
    doctor_id INTEGER NOT NULL, consultation_type_id INTEGER NOT NULL, slot_id TEXT NOT NULL, start_at TEXT NOT NULL,
    end_at TEXT NOT NULL, fee_snapshot NUMERIC NOT NULL, status TEXT NOT NULL DEFAULT 'CONFIRMED',
    reschedule_count INTEGER NOT NULL DEFAULT 0, dependent_id INTEGER REFERENCES patient_dependents(dependent_id),
    idempotency_key TEXT UNIQUE,
    active_patient_slot TEXT GENERATED ALWAYS AS
        (CASE WHEN status = 'CONFIRMED' THEN patient_id || '#' || COALESCE(dependent_id, 0) || '#' || slot_id END) STORED UNIQUE,
    cancelled_at TEXT, cancellation_note TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CHECK (end_at > start_at), CHECK (fee_snapshot >= 0),
    CHECK (status IN ('CONFIRMED','COMPLETED','CANCELLED_BY_PATIENT','CANCELLED_BY_DOCTOR','NO_SHOW')));
CREATE TABLE refund_ledger (refund_id INTEGER PRIMARY KEY AUTOINCREMENT, appointment_id INTEGER NOT NULL UNIQUE,
    fee_amount NUMERIC NOT NULL, refund_amount NUMERIC NOT NULL, deduction_amount NUMERIC NOT NULL,
    refund_percentage NUMERIC NOT NULL, refund_tier TEXT NOT NULL, cancellation_reason TEXT NOT NULL, refund_policy TEXT,
    calculated_at TEXT NOT NULL, created_at TEXT DEFAULT CURRENT_TIMESTAMP,
    CHECK (refund_amount >= 0 AND deduction_amount >= 0 AND round(refund_amount + deduction_amount, 2) = round(fee_amount, 2)),
    CHECK (refund_percentage BETWEEN 0 AND 100),
    CHECK (cancellation_reason IN ('PATIENT_CANCEL','DOCTOR_CANCEL','NO_SHOW')));
"""


class _Cursor:
    def __init__(self, db):
        self.db, self.rows, self.lastrowid, self.rowcount = db, [], None, 0

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, args=()):
        if self.db.fail_on and self.db.fail_on in sql:
            raise pymysql.err.OperationalError(2013, "Lost connection to MySQL server during query (injected)")
        sql = re.sub(r"\s+FOR UPDATE\b", "", sql).replace("%s", "?")
        sql = re.sub(r"LIKE \?", r"LIKE ? ESCAPE '\\'", sql)
        self.db.log.append((" ".join(sql.split()), tuple(args)))
        try:
            cur = self.db.db.execute(sql, tuple(args))
        except sqlite3.IntegrityError as exc:
            if "UNIQUE" in str(exc):
                raise pymysql.err.IntegrityError(1062, f"Duplicate entry for key '{exc}'")
            raise pymysql.err.IntegrityError(3819, f"Check constraint violated ({exc})")
        self.lastrowid, self.rowcount = cur.lastrowid, cur.rowcount
        self.rows = [dict(r) for r in cur.fetchall()] if cur.description else []
        return len(self.rows) if cur.description else cur.rowcount

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows


class _Conn:
    """One `with dbconn()` block: holds the database lock for its lifetime and rolls back on exit."""

    def __init__(self, db):
        self.db = db

    def __enter__(self):
        self.db.lock.acquire()
        self.db.connections += 1
        return self

    def __exit__(self, *exc):
        try:
            self.db.db.rollback()
        finally:
            self.db.lock.release()
        return False

    def cursor(self):
        return _Cursor(self.db)

    def commit(self):
        if self.db.fail_commit:
            raise pymysql.err.OperationalError(2013, "Lost connection during COMMIT (injected)")
        self.db.db.commit()

    def rollback(self):
        self.db.db.rollback()


class BookingDb(FakeMysql):
    def __init__(self):
        super().__init__()
        self.db.executescript(BOOKING_SCHEMA)
        self.db.commit()
        self.lock = threading.RLock()
        self.fail_on = None
        self.fail_commit = False
        self.connections = 0

    def connect(self, _cfg=None):
        return _Conn(self)

    def rows(self, sql, args=()):
        with self.lock:
            return [dict(r) for r in self.db.execute(sql, args).fetchall()]


# =========================================================================== DynamoDB expression evaluator
_TOKEN = re.compile(r"\s*(<>|<=|>=|=|<|>|\(|\)|,|\+|-|[#:]?[A-Za-z_][A-Za-z0-9_]*)")


def _tokens(text):
    out, pos = [], 0
    text = text.strip()
    while pos < len(text):
        m = _TOKEN.match(text, pos)
        if not m:
            raise ValueError(f"cannot parse expression near {text[pos:]!r}")
        out.append(m.group(1))
        pos = m.end()
    return out


class _Expr:
    def __init__(self, item, names, values):
        self.item, self.names, self.values = item, names or {}, values or {}

    def name(self, tok):
        return self.names[tok] if tok.startswith("#") else tok

    def operand(self, tok):
        if tok.startswith(":"):
            return self.values[tok]
        return self.item.get(self.name(tok), _MISSING)

    # condition grammar: or_expr := and_expr (OR and_expr)* ; and_expr := not_expr (AND not_expr)* ; ...
    def evaluate(self, text):
        self.toks, self.i = _tokens(text), 0
        result = self._or()
        assert self.i == len(self.toks), f"trailing tokens in {text!r}"
        return result

    def _peek(self):
        return self.toks[self.i] if self.i < len(self.toks) else None

    def _take(self):
        tok = self.toks[self.i]
        self.i += 1
        return tok

    def _or(self):
        value = self._and()
        while (self._peek() or "").upper() == "OR":
            self._take()
            right = self._and()
            value = value or right
        return value

    def _and(self):
        value = self._not()
        while (self._peek() or "").upper() == "AND":
            self._take()
            right = self._not()
            value = value and right
        return value

    def _not(self):
        if (self._peek() or "").upper() == "NOT":
            self._take()
            return not self._not()
        return self._atom()

    def _atom(self):
        tok = self._take()
        if tok == "(":
            value = self._or()
            assert self._take() == ")"
            return value
        if tok in ("attribute_not_exists", "attribute_exists"):
            assert self._take() == "("
            path = self.name(self._take())
            assert self._take() == ")"
            present = path in self.item
            return not present if tok == "attribute_not_exists" else present
        left = self.operand(tok)
        op = self._take()
        right = self.operand(self._take())
        if left is _MISSING or right is _MISSING:
            return False
        if op == "=":
            return left == right
        if op == "<>":
            return left != right
        if type(left) is not type(right) and not (isinstance(left, (int, Decimal)) and isinstance(right, (int, Decimal))):
            return False
        return {"<": left < right, ">": left > right, "<=": left <= right, ">=": left >= right}[op]


class _Missing:
    pass


_MISSING = _Missing()


def _apply_update(item, text, names, values):
    expr = _Expr(item, names, values)
    sections = re.split(r"\b(SET|REMOVE)\b", text)
    new = dict(item)
    mode = None
    for part in sections:
        if part in ("SET", "REMOVE"):
            mode = part
            continue
        if not part.strip():
            continue
        for clause in part.split(","):
            clause = clause.strip()
            if mode == "REMOVE":
                new.pop(expr.name(clause), None)
                continue
            target, rhs = (s.strip() for s in clause.split("=", 1))
            toks = _tokens(rhs)
            value = expr.operand(toks[0])
            if len(toks) == 3:
                other = expr.operand(toks[2])
                if value is _MISSING or other is _MISSING:
                    raise ClientError({"Error": {"Code": "ValidationException", "Message": "missing operand"}}, "UpdateItem")
                value = value + other if toks[1] == "+" else value - other
            new[expr.name(target)] = value
    return new


def to_py(av):
    (kind, v), = av.items()
    if kind == "N":
        d = Decimal(v)
        return int(d) if d == d.to_integral_value() else d
    if kind == "M":
        return {k: to_py(x) for k, x in v.items()}
    if kind == "L":
        return [to_py(x) for x in v]
    if kind == "NULL":
        return None
    return v


def to_av(v):
    if v is None:
        return {"NULL": True}
    if isinstance(v, bool):
        return {"BOOL": v}
    if isinstance(v, (int, Decimal, float)):
        return {"N": str(v)}
    if isinstance(v, dict):
        return {"M": {k: to_av(x) for k, x in v.items()}}
    if isinstance(v, list):
        return {"L": [to_av(x) for x in v]}
    return {"S": v}


class FakeDynamo:
    KEYS = {"Slots": "slot_id", "Idempotency": "idempotency_key"}

    def __init__(self):
        self.tables = {"Slots": {}, "Idempotency": {}}
        self.lock = threading.Lock()
        self.calls = []
        self.on_slot_read = None       # hook called after a Slots read, outside the lock (tests use a barrier)
        self.fail = {}                 # operation name -> exception to raise once

    def _t(self, name):
        for suffix in self.KEYS:
            if name.endswith(suffix):
                return suffix
        raise AssertionError(f"unexpected table {name}")

    def _key(self, table, key):
        return to_py(key[self.KEYS[table]])

    def _maybe_fail(self, op):
        exc = self.fail.pop(op, None)
        if exc:
            raise exc

    def _check(self, item, cond, names, values):
        if not cond:
            return True
        return _Expr(item or {}, names, {k: to_py(v) for k, v in (values or {}).items()}).evaluate(cond)

    @staticmethod
    def _ccf(op):
        return ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "The conditional request failed"}}, op)

    # ------------------------------------------------------------------ API
    def get_item(self, TableName, Key, ConsistentRead=False):
        self._maybe_fail("get_item")
        t = self._t(TableName)
        with self.lock:
            self.calls.append(("get_item", t, ConsistentRead))
            item = self.tables[t].get(self._key(t, Key))
            out = {"Item": {k: to_av(v) for k, v in item.items()}} if item else {}
        if t == "Slots" and self.on_slot_read:
            self.on_slot_read()
        return out

    def put_item(self, TableName, Item, ConditionExpression=None, ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        self._maybe_fail("put_item")
        t = self._t(TableName)
        item = {k: to_py(v) for k, v in Item.items()}
        k = item[self.KEYS[t]]
        with self.lock:
            self.calls.append(("put_item", t))
            if not self._check(self.tables[t].get(k), ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues):
                raise self._ccf("PutItem")
            self.tables[t][k] = item
        return {}

    def update_item(self, TableName, Key, UpdateExpression, ConditionExpression=None, ExpressionAttributeNames=None,
                    ExpressionAttributeValues=None, ReturnValues=None):
        self._maybe_fail("update_item")
        t = self._t(TableName)
        k = self._key(t, Key)
        values = {key: to_py(v) for key, v in (ExpressionAttributeValues or {}).items()}
        with self.lock:
            self.calls.append(("update_item", t, UpdateExpression))
            current = self.tables[t].get(k)
            if not self._check(current, ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues):
                raise self._ccf("UpdateItem")
            new = _apply_update(current or {self.KEYS[t]: k}, UpdateExpression, ExpressionAttributeNames, values)
            self.tables[t][k] = new
        return {"Attributes": {key: to_av(v) for key, v in new.items()}} if ReturnValues == "ALL_NEW" else {}

    def delete_item(self, TableName, Key, ConditionExpression=None, ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        self._maybe_fail("delete_item")
        t = self._t(TableName)
        k = self._key(t, Key)
        with self.lock:
            self.calls.append(("delete_item", t))
            if not self._check(self.tables[t].get(k), ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues):
                raise self._ccf("DeleteItem")
            self.tables[t].pop(k, None)
        return {}

    def transact_write_items(self, TransactItems):
        self._maybe_fail("transact_write_items")
        with self.lock:
            self.calls.append(("transact_write_items", len(TransactItems)))
            reasons, plans = [], []
            for entry in TransactItems:
                (op, spec), = entry.items()
                assert op == "Update", op
                t = self._t(spec["TableName"])
                k = self._key(t, spec["Key"])
                current = self.tables[t].get(k)
                ok = self._check(current, spec.get("ConditionExpression"), spec.get("ExpressionAttributeNames"),
                                 spec.get("ExpressionAttributeValues"))
                reasons.append({"Code": "None" if ok else "ConditionalCheckFailed"})
                plans.append((t, k, current, spec))
            if any(r["Code"] != "None" for r in reasons):
                raise ClientError({"Error": {"Code": "TransactionCanceledException", "Message": "Transaction cancelled"},
                                   "CancellationReasons": reasons}, "TransactWriteItems")
            for t, k, current, spec in plans:
                values = {key: to_py(v) for key, v in (spec.get("ExpressionAttributeValues") or {}).items()}
                self.tables[t][k] = _apply_update(current or {self.KEYS[t]: k}, spec["UpdateExpression"],
                                                  spec.get("ExpressionAttributeNames"), values)
        return {}

    # ------------------------------------------------------------------ helpers
    def slot(self, slot_id):
        with self.lock:
            return dict(self.tables["Slots"][slot_id])

    def idem(self):
        with self.lock:
            return {k: dict(v) for k, v in self.tables["Idempotency"].items()}


class FakeEvents:
    def __init__(self):
        self.entries, self.lock, self.fail_times = [], threading.Lock(), 0

    def put_events(self, Entries):
        with self.lock:
            if self.fail_times:
                self.fail_times -= 1
                raise ClientError({"Error": {"Code": "InternalException", "Message": "boom"}}, "PutEvents")
            self.entries.extend(Entries)
        return {"FailedEntryCount": 0, "Entries": [{"EventId": str(i)} for i, _ in enumerate(Entries)]}

    def types(self):
        return [e["DetailType"] for e in self.entries]


class FakeSsm:
    def __init__(self, values=None):
        self.values = dict(values or {})
        self.calls = 0

    def get_parameters(self, Names):
        self.calls += 1
        return {"Parameters": [{"Name": n, "Value": self.values[n]} for n in Names if n in self.values],
                "InvalidParameters": [n for n in Names if n not in self.values]}


# =========================================================================== world builder
def slot_id_for(doctor_id, type_id, start):
    return "SLOT#" + hashlib.sha256(f"{doctor_id}#{type_id}#{start.isoformat()}".encode()).hexdigest()[:40]


def iso_z(dt):
    return dt.astimezone(timezone.utc).isoformat(timespec="seconds").replace("+00:00", "Z")


class World:
    """Everything a booking test needs, wired into the Lambda module."""

    def __init__(self, monkeypatch, settings=None):
        self.db, self.dynamo, self.events = BookingDb(), FakeDynamo(), FakeEvents()
        self.metrics, self.metrics_lock = [], threading.Lock()
        self.settings = {"max_active": 3, "max_per_doctor_day": 1, "max_retries": 10,
                         "refund_policy_raw": '{"tiers":[{"name":"FULL","minHoursBefore":24,"refundPercent":100},'
                                              '{"name":"PARTIAL","minHoursBefore":2,"refundPercent":50},'
                                              '{"name":"NONE","minHoursBefore":0,"refundPercent":0}]}'}
        self.settings.update(settings or {})
        self.now = None
        m = booking
        monkeypatch.setattr(m, "db_config", lambda: {"env": "dev"})
        monkeypatch.setattr(m, "dbconn", self.db.connect)
        monkeypatch.setattr(m, "ddb", self.dynamo)
        monkeypatch.setattr(m, "events", self.events)
        monkeypatch.setattr(m, "load_settings", lambda cid=None: dict(self.settings))
        monkeypatch.setattr(m, "metric", self._metric)
        monkeypatch.setattr(m, "sleep", lambda s: None)
        monkeypatch.setattr(m, "utcnow", lambda: self.now or datetime.now(timezone.utc))
        self.m = m

    def _metric(self, name, value=1, unit="Count", **dims):
        with self.metrics_lock:
            self.metrics.append((name, value, dims))

    def metric_sum(self, name, **dims):
        return sum(v for n, v, d in self.metrics if n == name and all(d.get(k) == x for k, x in dims.items()))

    def doctor(self, first="Ravi", last="Kumar", types=((1, 300.0, 1),), active=True, tz="Asia/Kolkata"):
        return self.db.add_doctor(first, last, types=types, active=active, tz=tz)

    def patient(self, n):
        return self.db.add_patient(f"p{n}@example.test", f"9{n:09d}", first="Pat", last=f"Num{n}")

    def slot(self, doctor_id, type_id=1, start=None, minutes=15, capacity=1, status="OPEN", booked=0, held=0, version=1):
        start = start or (datetime.now(timezone.utc) + timedelta(days=3)).replace(minute=0, second=0, microsecond=0)
        sid = slot_id_for(doctor_id, type_id, start)
        self.dynamo.tables["Slots"][sid] = {
            "slot_id": sid, "doctor_id": doctor_id, "consultation_type_id": type_id,
            "doctor_date": f"DOCTOR#{doctor_id}#{start.date().isoformat()}", "start_at": iso_z(start),
            "end_at": iso_z(start + timedelta(minutes=minutes)), "timezone": "Asia/Kolkata", "capacity": capacity,
            "booked_count": booked, "held_count": held, "version": version, "status": status,
            "local_date": start.date().isoformat()}
        return sid

    def dependent(self, patient_id, first="Lakshmi", last="Rao", relation="Mother", dob="1965-04-02", active=True):
        with self.db.lock:
            cur = self.db.db.execute("""INSERT INTO patient_dependents (patient_id, first_name, last_name, relation, date_of_birth, is_active)
                                        VALUES (?,?,?,?,?,?)""", (patient_id, first, last, relation, dob, 1 if active else 0))
            self.db.db.commit()
            return cur.lastrowid

    def appointments(self, **where):
        sql = "SELECT * FROM appointments"
        if where:
            sql += " WHERE " + " AND ".join(f"{k} = ?" for k in where)
        return self.db.rows(sql + " ORDER BY appointment_id", tuple(where.values()))

    def ledger(self):
        return self.db.rows("SELECT * FROM refund_ledger ORDER BY refund_id")


# ==============================================================================================================
# MANDATORY TEST 1 - concurrency (20+ simultaneous bookings, capacity 3 -> exactly 3 succeed)
# ==============================================================================================================
# MANDATORY TEST 1 - Concurrency (offline, runs in CI on every push / PR).
#
# 20+ simultaneous bookings for a capacity-3 slot: exactly 3 succeed, every other request gets 409 SLOT_FULL,
# booked_count ends at exactly 3 and RDS holds exactly 3 CONFIRMED appointments.
#
# The requests run on real threads against the real Lambda handler. The DynamoDB fake evaluates the conditional
# writes atomically (like DynamoDB) and is NOT serialised; a barrier makes every request read the slot at the same
# version before anyone writes, so contention is guaranteed and the retry path is genuinely exercised (the
# BookingConflictRetries metric must be > 0). The live version of this test runs against the deployed API in
# tests/integration/test_booking_live_api.py.

def book_event(patient_id, slot_id, key=None):
    ev = api_event("POST", "/appointments", user_id=patient_id, role="PATIENT", body={"slotId": slot_id})
    ev["headers"]["Idempotency-Key"] = key or uuid.uuid4().hex
    return ev


def fire(world, patients, slot_id, barrier_parties=None):
    """Start every booking at the same instant; return [(status, body)]."""
    start = threading.Barrier(len(patients))
    if barrier_parties:
        read_barrier = threading.Barrier(barrier_parties, timeout=5)
        first_read = threading.local()

        def hold_first_read():
            if not getattr(first_read, "done", False):
                first_read.done = True
                try:
                    read_barrier.wait()
                except threading.BrokenBarrierError:
                    pass
        world.dynamo.on_slot_read = hold_first_read

    def one(pid):
        start.wait()
        res = booking.handler(book_event(pid, slot_id), None)
        return res["statusCode"], json.loads(res["body"])

    with ThreadPoolExecutor(max_workers=len(patients)) as pool:
        return list(pool.map(one, patients))


@pytest.mark.parametrize("contenders,capacity", [(24, 3), (20, 3), (30, 10), (12, 1)])
def test_mandatory1_exactly_capacity_bookings_succeed_under_contention(monkeypatch, contenders, capacity):
    w = World(monkeypatch, {"max_retries": 15})
    doctor = w.doctor(types=((4, 500.0, capacity),) if capacity > 1 else ((1, 300.0, 1),))
    slot = w.slot(doctor, type_id=4 if capacity > 1 else 1, minutes=60 if capacity > 1 else 15, capacity=capacity)
    patients = [w.patient(i) for i in range(1, contenders + 1)]

    results = fire(w, patients, slot, barrier_parties=contenders)

    ok = [b for s, b in results if s == 201]
    full = [b for s, b in results if s == 409]
    assert len(ok) == capacity, [(s, b.get("code")) for s, b in results]
    assert len(full) == contenders - capacity
    assert {b["code"] for b in full} == {"SLOT_FULL"}
    assert all(s in (201, 409) for s, _ in results)                       # no 5xx, no SLOT_BUSY

    item = w.dynamo.slot(slot)
    assert item["booked_count"] == capacity and item["version"] == 1 + capacity
    rows = w.appointments(slot_id=slot, status="CONFIRMED")
    assert len(rows) == capacity
    assert len({r["patient_id"] for r in rows}) == capacity                 # different winners
    assert sorted(b["appointment"]["appointmentId"] for b in ok) == [r["appointment_id"] for r in rows]
    # Contention was real and was handled by retrying, not avoided by serialisation.
    assert w.metric_sum("BookingConflictRetries") > 0
    assert w.events.types().count("AppointmentBooked") == capacity


def test_mandatory1_retries_are_bounded_and_answer_slot_busy_not_overbooking(monkeypatch):
    """With max_retries=0 every loser of the first race gives up at once: SLOT_BUSY (retryable), never overbooks."""
    w = World(monkeypatch, {"max_retries": 0})
    doctor = w.doctor(types=((4, 500.0, 5),))
    slot = w.slot(doctor, type_id=4, minutes=60, capacity=5)
    patients = [w.patient(i) for i in range(1, 9)]
    results = fire(w, patients, slot, barrier_parties=8)
    codes = sorted(b.get("code", "OK") for s, b in results)
    assert codes.count("OK") == 1 and codes.count("SLOT_BUSY") == 7
    assert w.dynamo.slot(slot)["booked_count"] == 1
    assert len(w.appointments(slot_id=slot)) == 1
    # A SLOT_BUSY answer is not final: its idempotency key is released so the client can retry with it.
    assert len(w.dynamo.idem()) == 1


def test_mandatory1_sequential_overflow_gets_409_with_capacity(monkeypatch):
    w = World(monkeypatch)
    doctor = w.doctor(types=((4, 500.0, 2),))
    slot = w.slot(doctor, type_id=4, minutes=60, capacity=2)
    codes = []
    for i in range(1, 5):
        res = booking.handler(book_event(w.patient(i), slot), None)
        codes.append((res["statusCode"], json.loads(res["body"]).get("code")))
    assert codes == [(201, None), (201, None), (409, "SLOT_FULL"), (409, "SLOT_FULL")]


def test_mandatory1_held_seats_count_against_capacity(monkeypatch):
    """Seats held for a waitlist offer (held_count) are not free."""
    w = World(monkeypatch)
    doctor = w.doctor(types=((4, 500.0, 3),))
    slot = w.slot(doctor, type_id=4, minutes=60, capacity=3, booked=1, held=1)
    first = booking.handler(book_event(w.patient(1), slot), None)
    second = booking.handler(book_event(w.patient(2), slot), None)
    assert first["statusCode"] == 201
    assert second["statusCode"] == 409 and json.loads(second["body"])["code"] == "SLOT_FULL"


# ==============================================================================================================
# MANDATORY TEST 2 - idempotency (same key replayed -> exactly one appointment)
# ==============================================================================================================
# MANDATORY TEST 2 - Idempotency (offline, runs in CI on every push / PR).
#
# The same Idempotency-Key replayed -> exactly one appointment, and every replay returns the ORIGINAL appointment.
# Also covers the hard cases: parallel duplicates, key reuse with a different body, keys scoped per patient,
# a crashed attempt that already claimed the seat, an expired record, and non-final answers freeing the key.

def post(patient_id, slot_id, key, body=None):
    ev = api_event("POST", "/appointments", user_id=patient_id, role="PATIENT", body=body if body is not None else {"slotId": slot_id})
    if key is not None:
        ev["headers"]["Idempotency-Key"] = key
    res = booking.handler(ev, None)
    return res["statusCode"], json.loads(res["body"]), res["headers"]


def day(n):
    return datetime.now(timezone.utc).replace(minute=0, second=0, microsecond=0) + timedelta(days=n)


@pytest.fixture
def w_idem(monkeypatch):
    world = World(monkeypatch)
    world.doc = world.doctor(types=((4, 450.0, 5),))
    world.slot_id = world.slot(world.doc, type_id=4, minutes=60, capacity=5)
    world.pat = world.patient(1)
    return world


def test_mandatory2_same_key_replayed_creates_exactly_one_appointment(w_idem):
    key = "key-" + uuid.uuid4().hex
    first = post(w_idem.pat, w_idem.slot_id, key)
    replays = [post(w_idem.pat, w_idem.slot_id, key) for _ in range(5)]
    assert first[0] == 201 and "Idempotent-Replayed" not in first[2]
    appointment_id = first[1]["appointment"]["appointmentId"]
    for status, data, headers in replays:
        assert status == 201 and headers.get("Idempotent-Replayed") == "true"
        assert data["appointment"]["appointmentId"] == appointment_id
        assert data["appointment"] == first[1]["appointment"]               # the original answer, byte for byte
    assert len(w_idem.appointments()) == 1
    assert w_idem.dynamo.slot(w_idem.slot_id)["booked_count"] == 1                    # no extra seat taken by replays
    assert w_idem.events.types().count("AppointmentBooked") == 1                 # no duplicate event either
    assert w_idem.metric_sum("IdempotentReplays") == 5


def test_mandatory2_parallel_duplicates_with_one_key_create_one_appointment(w_idem):
    key = "par-" + uuid.uuid4().hex
    gate = threading.Barrier(12)

    def one(_):
        gate.wait()
        return post(w_idem.pat, w_idem.slot_id, key)

    with ThreadPoolExecutor(12) as pool:
        results = list(pool.map(one, range(12)))
    created = [d for s, d, _ in results if s == 201]
    in_progress = [d for s, d, _ in results if s == 409]
    assert created and all(d["appointment"]["appointmentId"] == created[0]["appointment"]["appointmentId"] for d in created)
    assert all(d["code"] == "REQUEST_IN_PROGRESS" for d in in_progress)
    assert len(created) + len(in_progress) == 12
    assert len(w_idem.appointments()) == 1 and w_idem.dynamo.slot(w_idem.slot_id)["booked_count"] == 1
    # After the first one finished, a retry with that key is a clean replay.
    assert post(w_idem.pat, w_idem.slot_id, key)[0] == 201


def test_mandatory2_same_key_with_a_different_slot_is_rejected(w_idem):
    other = w_idem.slot(w_idem.doc, type_id=4, minutes=60, capacity=5, start=day(5))
    key = "mix-" + uuid.uuid4().hex
    assert post(w_idem.pat, w_idem.slot_id, key)[0] == 201
    status, data, _ = post(w_idem.pat, other, key)
    assert status == 422 and data["code"] == "IDEMPOTENCY_KEY_REUSED"
    assert len(w_idem.appointments()) == 1


def test_mandatory2_keys_are_scoped_to_the_patient(w_idem):
    """Another patient sending the same key gets their OWN booking, never the first patient's appointment."""
    key = "shared-" + uuid.uuid4().hex
    p2 = w_idem.patient(2)
    a = post(w_idem.pat, w_idem.slot_id, key)
    b = post(p2, w_idem.slot_id, key)
    assert a[0] == b[0] == 201
    assert a[1]["appointment"]["appointmentId"] != b[1]["appointment"]["appointmentId"]
    assert b[1]["appointment"]["patientId"] == p2 and "Idempotent-Replayed" not in b[2]


@pytest.mark.parametrize("key,code", [(None, "IDEMPOTENCY_KEY_REQUIRED"), ("", "IDEMPOTENCY_KEY_REQUIRED"),
                                      ("short", "IDEMPOTENCY_KEY_INVALID"), ("has spaces in it", "IDEMPOTENCY_KEY_INVALID"),
                                      ("x" * 129, "IDEMPOTENCY_KEY_INVALID")])
def test_mandatory2_key_is_required_and_validated(w_idem, key, code):
    status, data, _ = post(w_idem.pat, w_idem.slot_id, key)
    assert status == 400 and data["code"] == code
    assert w_idem.appointments() == [] and w_idem.dynamo.idem() == {}


def test_mandatory2_final_rejections_are_replayed_too(w_idem):
    """A 409 SLOT_FULL is the answer for that key: replaying it does not suddenly book a freed seat."""
    full = w_idem.slot(w_idem.doc, type_id=4, minutes=60, capacity=2, booked=2, start=day(6))
    key = "full-" + uuid.uuid4().hex
    assert post(w_idem.pat, full, key)[1]["code"] == "SLOT_FULL"
    w_idem.dynamo.tables["Slots"][full]["booked_count"] = 0
    status, data, headers = post(w_idem.pat, full, key)
    assert status == 409 and data["code"] == "SLOT_FULL" and headers.get("Idempotent-Replayed") == "true"
    assert post(w_idem.pat, full, "fresh-" + uuid.uuid4().hex)[0] == 201         # a new key is a new request


def test_mandatory2_non_final_failure_frees_the_key_for_a_retry(w_idem):
    """Database outage after the seat was claimed: seat given back, key released, the same key then succeeds."""
    key = "retry-" + uuid.uuid4().hex
    w_idem.db.fail_on = "INSERT INTO appointments"
    status, data, _ = post(w_idem.pat, w_idem.slot_id, key)
    assert status == 500
    assert w_idem.dynamo.slot(w_idem.slot_id)["booked_count"] == 0 and w_idem.dynamo.idem() == {}
    w_idem.db.fail_on = None
    status, data, _ = post(w_idem.pat, w_idem.slot_id, key)
    assert status == 201 and len(w_idem.appointments()) == 1 and w_idem.dynamo.slot(w_idem.slot_id)["booked_count"] == 1


def test_mandatory2_crashed_attempt_that_claimed_a_seat_is_resumed_not_double_claimed(w_idem):
    """Simulate a Lambda that died after the seat transaction but before the RDS insert: the record says
    seat_claimed=true with an expired lease. The retry takes over, reuses that seat and inserts once."""
    key = "crash-" + uuid.uuid4().hex
    scoped = booking.scoped_key(w_idem.pat, key)
    w_idem.dynamo.tables["Idempotency"][scoped] = {
        "idempotency_key": scoped, "status": "IN_PROGRESS", "request_hash": booking.request_hash(w_idem.slot_id),
        "patient_id": str(w_idem.pat), "slot_id": w_idem.slot_id, "attempt_id": "dead", "seat_claimed": True,
        "lease_expires_at": int(time.time()) - 5, "expires_at": int(time.time()) + 3600}
    w_idem.dynamo.tables["Slots"][w_idem.slot_id].update(booked_count=1, version=2)     # the seat the dead attempt took
    status, data, _ = post(w_idem.pat, w_idem.slot_id, key)
    assert status == 201
    assert w_idem.dynamo.slot(w_idem.slot_id)["booked_count"] == 1                        # not 2
    assert len(w_idem.appointments()) == 1
    assert w_idem.dynamo.idem()[scoped]["status"] == "COMPLETED"


def test_mandatory2_crash_after_commit_is_answered_from_the_database(w_idem):
    """The insert committed but the idempotency record was never completed: the retry finds the appointment
    by its idempotency_key in RDS and answers with it - no second seat, no second row."""
    key = "late-" + uuid.uuid4().hex
    assert post(w_idem.pat, w_idem.slot_id, key)[0] == 201
    scoped = booking.scoped_key(w_idem.pat, key)
    w_idem.dynamo.tables["Idempotency"][scoped].update(status="IN_PROGRESS", lease_expires_at=int(time.time()) - 1,
                                                  seat_claimed=True)
    status, data, _ = post(w_idem.pat, w_idem.slot_id, key)
    assert status == 201 and len(w_idem.appointments()) == 1 and w_idem.dynamo.slot(w_idem.slot_id)["booked_count"] == 1


def test_mandatory2_in_progress_with_a_live_lease_is_409(w_idem):
    key = "live-" + uuid.uuid4().hex
    scoped = booking.scoped_key(w_idem.pat, key)
    w_idem.dynamo.tables["Idempotency"][scoped] = {
        "idempotency_key": scoped, "status": "IN_PROGRESS", "request_hash": booking.request_hash(w_idem.slot_id),
        "attempt_id": "other", "seat_claimed": False, "lease_expires_at": int(time.time()) + 20,
        "expires_at": int(time.time()) + 3600}
    status, data, headers = post(w_idem.pat, w_idem.slot_id, key)
    assert status == 409 and data["code"] == "REQUEST_IN_PROGRESS" and headers["Retry-After"] == "2"
    assert w_idem.appointments() == []


def test_mandatory2_expired_record_is_treated_as_absent_without_waiting_for_ttl(w_idem):
    """DynamoDB TTL deletes lazily. A record past expires_at must not block (or replay) a new request."""
    key = "old-" + uuid.uuid4().hex
    scoped = booking.scoped_key(w_idem.pat, key)
    w_idem.dynamo.tables["Idempotency"][scoped] = {
        "idempotency_key": scoped, "status": "COMPLETED", "request_hash": "something-else", "attempt_id": "x",
        "response_status": 201, "response_body": "{}", "expires_at": int(time.time()) - 10}
    status, data, headers = post(w_idem.pat, w_idem.slot_id, key)
    assert status == 201 and "Idempotent-Replayed" not in headers and len(w_idem.appointments()) == 1


def test_mandatory2_database_backstop_blocks_a_duplicate_even_if_the_idempotency_store_forgot(w_idem):
    """If the DynamoDB record vanished (expired after 24 h) and the same key is replayed, the UNIQUE
    idempotency_key in RDS still prevents a second appointment, and the extra seat is given back."""
    key = "backstop-" + uuid.uuid4().hex
    first = post(w_idem.pat, w_idem.slot_id, key)
    w_idem.dynamo.tables["Idempotency"].clear()
    second = post(w_idem.pat, w_idem.slot_id, key)
    assert second[0] == 201 and second[1]["appointment"]["appointmentId"] == first[1]["appointment"]["appointmentId"]
    assert len(w_idem.appointments()) == 1 and w_idem.dynamo.slot(w_idem.slot_id)["booked_count"] == 1


# ==============================================================================================================
# Refund tiers - every boundary, including the exact cutoff instants
# ==============================================================================================================
# Refund tier boundaries - every tier edge, including the EXACT cutoff instants, to the microsecond.
#
# Default policy (SSM /medislot/<env>/booking/refund-policy):
#     notice >= 24 h  -> FULL     100 %
#     notice >=  2 h  -> PARTIAL   50 %
#     notice >   0    -> NONE       0 %
#     notice <=  0    -> refused (the appointment has started)
# Boundaries are inclusive on the longer-notice side: cancelling exactly 24 h before the start is still FULL.

US = timedelta(microseconds=1)
START = datetime(2026, 11, 20, 10, 30, tzinfo=timezone.utc)
TIERS = booking.parse_refund_policy(booking.DEFAULT_REFUND_POLICY)


def tier_at(notice):
    return booking.refund_tier(START, START - notice, TIERS)["name"]


# ----------------------------------------------------------------------------- pure boundary table
@pytest.mark.parametrize("notice,expected", [
    (timedelta(days=30), "FULL"),
    (timedelta(hours=24) + US, "FULL"),
    (timedelta(hours=24), "FULL"),                       # exact cutoff instant -> the better tier
    (timedelta(hours=24) - US, "PARTIAL"),               # one microsecond later -> next tier
    (timedelta(hours=12), "PARTIAL"),
    (timedelta(hours=2) + US, "PARTIAL"),
    (timedelta(hours=2), "PARTIAL"),                     # exact cutoff instant
    (timedelta(hours=2) - US, "NONE"),
    (timedelta(minutes=1), "NONE"),
    (US, "NONE"),                                        # the last microsecond before the start
])
def test_every_tier_boundary(notice, expected):
    assert tier_at(notice) == expected


@pytest.mark.parametrize("notice", [timedelta(0), -US, -timedelta(hours=1)])
def test_at_or_after_start_cannot_be_cancelled(notice):
    with pytest.raises(booking.TooLate):
        tier_at(notice)


@pytest.mark.parametrize("fee,percent,refund,deduction", [
    ("300.00", 100, "300.00", "0.00"),
    ("300.00", 50, "150.00", "150.00"),
    ("300.00", 0, "0.00", "300.00"),
    ("333.33", 50, "166.67", "166.66"),                  # half-up on the refund; parts still add up to the fee
    ("0.01", 50, "0.01", "0.00"),
    ("0.00", 50, "0.00", "0.00"),                        # free consultation
    ("99999999.99", 50, "50000000.00", "49999999.99"),   # DECIMAL(10,2) maximum
    ("200.00", Decimal("12.5"), "25.00", "175.00"),
])
def test_refund_money_is_exact_and_always_adds_up(fee, percent, refund, deduction):
    r, d = booking.refund_amounts(fee, percent)
    assert (str(r), str(d)) == (refund, deduction)
    assert r + d == Decimal(fee)


# ----------------------------------------------------------------------------- policy parsing (SSM content)
def test_custom_policy_with_fractional_hours_and_four_tiers():
    tiers = booking.parse_refund_policy(json.dumps({"tiers": [
        {"name": "partial", "minHoursBefore": "1.5", "refundPercent": 25},
        {"name": "FULL", "minHoursBefore": 48, "refundPercent": 100},
        {"name": "MOST", "minHoursBefore": 12, "refundPercent": 75.5},
        {"name": "NONE", "minHoursBefore": 0, "refundPercent": 0}]}))
    assert [t["name"] for t in tiers] == ["FULL", "MOST", "PARTIAL", "NONE"]
    s = START
    assert booking.refund_tier(s, s - timedelta(minutes=90), tiers)["name"] == "PARTIAL"
    assert booking.refund_tier(s, s - timedelta(minutes=90) + US, tiers)["name"] == "NONE"
    assert booking.refund_tier(s, s - timedelta(hours=12), tiers)["percent"] == Decimal("75.5")


@pytest.mark.parametrize("raw,why", [
    ("not json", "JSON"),
    ({"tiers": []}, "at least one"),
    ({"tiers": [{"name": "A", "minHoursBefore": 2, "refundPercent": 50}]}, "minHoursBefore 0"),
    ({"tiers": [{"name": "A", "minHoursBefore": 0, "refundPercent": 101}]}, "0..100"),
    ({"tiers": [{"name": "A", "minHoursBefore": 0, "refundPercent": -1}]}, "0..100"),
    ({"tiers": [{"name": "A", "minHoursBefore": 0, "refundPercent": 33.333}]}, "2 decimals"),
    ({"tiers": [{"name": "A", "minHoursBefore": -1, "refundPercent": 0}, {"name": "B", "minHoursBefore": 0, "refundPercent": 0}]}, "between"),
    ({"tiers": [{"name": "A", "minHoursBefore": 5, "refundPercent": 10}, {"name": "B", "minHoursBefore": 0, "refundPercent": 50}]}, "earlier"),
    ({"tiers": [{"name": "A", "minHoursBefore": 0, "refundPercent": 0}, {"name": "B", "minHoursBefore": 0, "refundPercent": 0}]}, "same"),
    ({"tiers": [{"name": "A", "minHoursBefore": 2, "refundPercent": 0}, {"name": "A", "minHoursBefore": 0, "refundPercent": 0}]}, "repeated"),
    ({"tiers": [{"name": "DOCTOR_CANCELLED", "minHoursBefore": 0, "refundPercent": 0}]}, "repeated"),
    ({"tiers": [{"name": "A", "minHoursBefore": "NaN", "refundPercent": 0}]}, "finite"),
    ({"tiers": [{"name": "A", "minHoursBefore": True, "refundPercent": 0}]}, "numeric"),
    ({"tiers": [{"name": "A", "refundPercent": 0}]}, "numeric"),
    ({"tiers": ["oops"]}, "object"),
])
def test_invalid_policies_are_rejected(raw, why):
    with pytest.raises(booking.PolicyError, match=why):
        booking.parse_refund_policy(raw)


# ----------------------------------------------------------------------------- end to end at the exact instants
@pytest.fixture
def w_refund(monkeypatch):
    world = World(monkeypatch, {"max_active": 10})
    world.doc = world.doctor(types=((2, 333.33, 1),))
    return world


def book_and_cancel_at(w_refund, n, notice, fee_slot_minutes=30):
    start = START + timedelta(days=n)                    # a different slot per case
    sid = w_refund.slot(w_refund.doc, type_id=2, start=start, minutes=fee_slot_minutes)
    pid = w_refund.patient(n)
    w_refund.now = start - timedelta(days=10)
    ev = api_event("POST", "/appointments", user_id=pid, role="PATIENT", body={"slotId": sid})
    ev["headers"]["Idempotency-Key"] = f"tier-case-{n:04d}"
    res = booking.handler(ev, None)
    assert res["statusCode"] == 201, res["body"]
    appt = json.loads(res["body"])["appointment"]["appointmentId"]
    w_refund.now = start - notice
    res = booking.handler(api_event("POST", f"/appointments/{appt}/cancel", user_id=pid, role="PATIENT"), None)
    return res["statusCode"], json.loads(res["body"]), appt


@pytest.mark.parametrize("n,notice,tier,refund,deduction", [
    (1, timedelta(hours=24), "FULL", 333.33, 0),
    (2, timedelta(hours=24) - US, "PARTIAL", 166.67, 166.66),
    (3, timedelta(hours=2), "PARTIAL", 166.67, 166.66),
    (4, timedelta(hours=2) - US, "NONE", 0, 333.33),
    (5, US, "NONE", 0, 333.33),
])
def test_cancel_endpoint_at_exact_cutoffs(w_refund, n, notice, tier, refund, deduction):
    status, data, appt = book_and_cancel_at(w_refund, n, notice)
    assert status == 200, data
    assert data["refund"]["tier"] == tier
    assert data["refund"]["refundAmount"] == refund and data["refund"]["deductionAmount"] == deduction
    row = w_refund.db.rows("SELECT * FROM refund_ledger WHERE appointment_id = ?", (appt,))[0]
    assert row["refund_tier"] == tier and row["cancellation_reason"] == "PATIENT_CANCEL"
    assert row["calculated_at"].startswith((START + timedelta(days=n) - notice).replace(tzinfo=None).isoformat(sep=" ")[:19])
    assert json.loads(row["refund_policy"])["tiers"][0]["name"] == "FULL"   # the policy used is kept for audit
    assert w_refund.metric_sum("CancellationsByTier", Tier=tier) == 1


def test_cancel_endpoint_at_the_start_instant_is_refused(w_refund):
    status, data, appt = book_and_cancel_at(w_refund, 6, timedelta(0))
    assert status == 409 and data["code"] == "ALREADY_STARTED"
    assert w_refund.appointments(appointment_id=appt)[0]["status"] == "CONFIRMED" and w_refund.ledger() == []


def test_policy_change_in_ssm_applies_to_the_next_cancellation(w_refund):
    w_refund.settings["refund_policy_raw"] = json.dumps({"tiers": [{"name": "FLAT", "minHoursBefore": 0, "refundPercent": 80}]})
    status, data, _ = book_and_cancel_at(w_refund, 7, timedelta(hours=30))
    assert data["refund"]["tier"] == "FLAT" and data["refund"]["refundAmount"] == 266.66


def test_broken_policy_fails_closed_without_touching_the_appointment(w_refund):
    w_refund.settings["refund_policy_raw"] = '{"tiers":[{"name":"X","minHoursBefore":5,"refundPercent":10}]}'
    status, data, appt = book_and_cancel_at(w_refund, 8, timedelta(hours=30))
    assert status == 503 and data["code"] == "REFUND_POLICY_UNAVAILABLE"
    assert w_refund.appointments(appointment_id=appt)[0]["status"] == "CONFIRMED" and w_refund.ledger() == []


def test_missing_policy_parameter_uses_the_documented_default(w_refund):
    w_refund.settings["refund_policy_raw"] = None
    status, data, _ = book_and_cancel_at(w_refund, 9, timedelta(hours=3))
    assert status == 200 and data["refund"]["tier"] == "PARTIAL"


def test_cancellation_preview_names_the_tier_and_when_it_ends(w_refund):
    start = START + timedelta(days=20)
    sid = w_refund.slot(w_refund.doc, type_id=2, start=start, minutes=30)
    pid = w_refund.patient(20)
    w_refund.now = start - timedelta(hours=30)
    ev = api_event("POST", "/appointments", user_id=pid, role="PATIENT", body={"slotId": sid})
    ev["headers"]["Idempotency-Key"] = "preview-case-1"
    appt = json.loads(booking.handler(ev, None)["body"])["appointment"]
    assert appt["cancellationPreview"] == {"tier": "FULL", "refundPercent": 100, "refundAmount": 333.33, "deductionAmount": 0,
                                           "validUntil": "2026-12-09T10:30:00Z"}
    w_refund.now = start - timedelta(hours=1)
    res = booking.handler(api_event("GET", f"/appointments/{appt['appointmentId']}", user_id=pid, role="PATIENT"), None)
    assert json.loads(res["body"])["appointment"]["cancellationPreview"]["tier"] == "NONE"


# ----------------------------------------------------------------------------- settings loader
def test_settings_loader_reads_ssm_with_defaults_and_caches(monkeypatch):
    real = booking.load_settings
    fake = FakeSsm({"/medislot/dev/booking/max-active-appointments": "5",
                    "/medislot/dev/booking/max-per-doctor-per-day": "abc",
                    "/medislot/dev/booking/refund-policy": '{"tiers":[{"name":"A","minHoursBefore":0,"refundPercent":0}]}'})
    monkeypatch.setattr(booking, "ssm", fake)
    booking._settings_cache.update(at=0.0, value=None)
    s = real()
    assert s["max_active"] == 5 and s["max_per_doctor_day"] == 1 and s["max_retries"] == 10
    assert s["refund_policy_raw"].startswith('{"tiers"')
    real()
    assert fake.calls == 1                                   # cached for 60 s
    booking._settings_cache.update(at=0.0, value=None)


# ==============================================================================================================
# Booking API edge cases - validation, limits, isolation, cancellation, doctor slot cancel
# ==============================================================================================================
# Booking engine edge cases: validation, patient limits, listing / detail isolation, cancellations, doctor slot
# cancellation, refund ledger endpoint, events and failure handling. Offline (SQLite + fake DynamoDB/EventBridge).

IST = ZoneInfo("Asia/Kolkata")


def at_api(days, hour=10, minute=0):
    base = datetime.now(timezone.utc).replace(hour=hour, minute=minute, second=0, microsecond=0)
    return base + timedelta(days=days)


def call_api(method, path, user, role="PATIENT", body=None, key="auto", query=None, path_params=None):
    ev = api_event(method, path, user_id=user, role=role, body=body, query=query)
    if method == "POST" and path == "/appointments" and key is not None:
        ev["headers"]["Idempotency-Key"] = uuid.uuid4().hex if key == "auto" else key
    if path_params:
        ev["pathParameters"] = path_params
    res = booking.handler(ev, None)
    return res["statusCode"], json.loads(res["body"])


def book_api(pid, slot, **kw):
    return call_api("POST", "/appointments", pid, body={"slotId": slot}, **kw)


@pytest.fixture
def w_api(monkeypatch):
    world = World(monkeypatch)
    world.doc = world.doctor("Ravi", "Kumar", types=((1, 300.0, 1), (4, 500.0, 3)))
    world.doc2 = world.doctor("Meena", "Rao", types=((2, 700.0, 1),))
    world.p1, world.p2 = world.patient(1), world.patient(2)
    return world


# ----------------------------------------------------------------------------- access and request validation
def test_access_rules(w_api):
    s = w_api.slot(w_api.doc)
    assert booking.handler(api_event("GET", "/appointments"), None)["statusCode"] == 403          # no authorizer context
    assert call_api("GET", "/appointments", 5, role="ADMIN")[0] == 403
    assert book_api(w_api.doc, s, role="DOCTOR")[0] == 403                                                # doctors cannot book
    assert call_api("GET", "/patients/me/refunds", w_api.doc, role="DOCTOR")[0] == 403
    assert call_api("POST", "/doctors/me/slots/" + s[5:] + "/cancel", w_api.p1, body={})[0] == 403
    assert call_api("DELETE", "/appointments", w_api.p1)[0] == 405
    assert call_api("GET", "/appointments/1/cancel", w_api.p1)[0] == 405
    assert call_api("GET", "/appointments/abc", w_api.p1)[1]["code"] == "APPOINTMENT_ID_INVALID"
    assert call_api("GET", "/appointments/0", w_api.p1)[0] == 422
    assert call_api("GET", "/appointments/1/other", w_api.p1)[0] == 404
    assert call_api("GET", "/dev/appointments", w_api.p1)[0] == 200                                       # stage prefix tolerated


@pytest.mark.parametrize("payload,status,code", [
    ({}, 422, "SLOT_ID_INVALID"),
    ({"slotId": "SLOT#xyz"}, 422, "SLOT_ID_INVALID"),
    ({"slotId": 12}, 422, "SLOT_ID_INVALID"),
    ({"slotId": "SLOT#" + "a" * 40, "patientId": 99}, 422, "UNKNOWN_FIELDS"),
    ({"slotId": "SLOT#" + "a" * 40, "fee": 1}, 422, "UNKNOWN_FIELDS"),
    ({"slotId": "SLOT#" + "a" * 40}, 404, "SLOT_NOT_FOUND"),
])
def test_booking_body_validation(w_api, payload, status, code):
    st, data = call_api("POST", "/appointments", w_api.p1, body=payload)
    assert (st, data["code"]) == (status, code)
    assert w_api.appointments() == []


def test_invalid_json_body(w_api):
    ev = api_event("POST", "/appointments", user_id=w_api.p1)
    ev["body"], ev["headers"]["Idempotency-Key"] = "{not json", "k" * 10
    assert booking.handler(ev, None)["statusCode"] == 400


def test_header_lookup_is_case_insensitive_and_supports_multi_value(w_api):
    s = w_api.slot(w_api.doc)
    ev = api_event("POST", "/appointments", user_id=w_api.p1, body={"slotId": s})
    ev["multiValueHeaders"] = {"idempotency-key": ["mv-key-123456"]}
    assert booking.handler(ev, None)["statusCode"] == 201


# ----------------------------------------------------------------------------- slot state
def test_slot_in_the_past_or_started(w_api):
    past = w_api.slot(w_api.doc, start=at_api(-1))
    started = w_api.slot(w_api.doc, start=datetime.now(timezone.utc) - timedelta(minutes=5), minutes=15)
    assert book_api(w_api.p1, past)[1]["code"] == "SLOT_IN_PAST"
    assert book_api(w_api.p1, started)[1]["code"] == "SLOT_IN_PAST"


@pytest.mark.parametrize("status", ["CLOSED", "CANCELLED"])
def test_closed_or_cancelled_slot(w_api, status):
    s = w_api.slot(w_api.doc, status=status)
    st, data = book_api(w_api.p1, s)
    assert st == 409 and data["code"] == "SLOT_CLOSED"
    assert w_api.dynamo.slot(s)["booked_count"] == 0


def test_type_withdrawn_or_doctor_deactivated(w_api):
    s = w_api.slot(w_api.doc)
    w_api.db.db.execute("UPDATE doctor_consultation_types SET is_active = 0 WHERE doctor_id = ? AND consultation_type_id = 1", (w_api.doc,))
    w_api.db.db.commit()
    assert book_api(w_api.p1, s)[1]["code"] == "SLOT_UNAVAILABLE"
    s2 = w_api.slot(w_api.doc2, type_id=2, minutes=30)
    w_api.db.db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (w_api.doc2,))
    w_api.db.db.commit()
    assert book_api(w_api.p1, s2)[1]["code"] == "SLOT_UNAVAILABLE"
    assert w_api.dynamo.slot(s)["booked_count"] == 0 and w_api.dynamo.slot(s2)["booked_count"] == 0


def test_closed_account_and_missing_profile(w_api):
    s = w_api.slot(w_api.doc)
    w_api.db.db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (w_api.p1,))
    w_api.db.db.execute("DELETE FROM patients WHERE patient_id = ?", (w_api.p2,))
    w_api.db.db.commit()
    assert book_api(w_api.p1, s)[0] == 403
    assert book_api(w_api.p2, s)[1]["code"] == "PROFILE_REQUIRED"
    assert w_api.dynamo.slot(s)["booked_count"] == 0 and w_api.dynamo.idem() == {}     # not final: keys released


# ----------------------------------------------------------------------------- patient limits
def test_same_slot_twice_is_a_duplicate(w_api):
    s = w_api.slot(w_api.doc, type_id=4, minutes=60, capacity=3)
    assert book_api(w_api.p1, s)[0] == 201
    st, data = book_api(w_api.p1, s)                          # new key, same slot
    assert st == 409 and data["code"] == "DUPLICATE_BOOKING"
    assert w_api.dynamo.slot(s)["booked_count"] == 1


def test_overlapping_appointments_with_different_doctors(w_api):
    a = w_api.slot(w_api.doc, start=at_api(3, 10, 0), minutes=15)
    b = w_api.slot(w_api.doc2, type_id=2, start=at_api(3, 10, 10), minutes=30)   # 10:10-10:40 overlaps 10:00-10:15
    c = w_api.slot(w_api.doc2, type_id=2, start=at_api(3, 10, 15), minutes=30)   # back-to-back is fine
    assert book_api(w_api.p1, a)[0] == 201
    assert book_api(w_api.p1, b)[1]["code"] == "OVERLAPPING_APPOINTMENT"
    assert book_api(w_api.p1, c)[0] == 201


def test_one_booking_per_doctor_per_local_day(w_api):
    # 23:30 and 00:30 IST are different local days even though both are on the same UTC day (18:00 / 19:00 UTC).
    d = (datetime.now(IST) + timedelta(days=4)).date()
    late = w_api.slot(w_api.doc, start=datetime(d.year, d.month, d.day, 23, 30, tzinfo=IST).astimezone(timezone.utc))
    early_next = w_api.slot(w_api.doc, start=datetime(d.year, d.month, d.day, 23, 30, tzinfo=IST).astimezone(timezone.utc) + timedelta(hours=1))
    same_day = w_api.slot(w_api.doc, start=datetime(d.year, d.month, d.day, 9, 0, tzinfo=IST).astimezone(timezone.utc))
    assert book_api(w_api.p1, late)[0] == 201
    assert book_api(w_api.p1, early_next)[0] == 201
    assert book_api(w_api.p1, same_day)[1]["code"] == "DOCTOR_DAILY_LIMIT_REACHED"


def test_max_active_upcoming_appointments(w_api):
    docs = [w_api.doctor(f"Doc{i}", "Limit", types=((1, 100.0, 1),)) for i in range(4)]
    slots = [w_api.slot(d, start=at_api(2 + i)) for i, d in enumerate(docs)]
    assert [book_api(w_api.p1, s)[0] for s in slots[:3]] == [201, 201, 201]
    st, data = book_api(w_api.p1, slots[3])
    assert st == 409 and data["code"] == "ACTIVE_LIMIT_REACHED" and "at most 3" in data["message"]
    # cancelling one frees a place
    first = w_api.appointments()[0]["appointment_id"]
    assert call_api("POST", f"/appointments/{first}/cancel", w_api.p1)[0] == 200
    assert book_api(w_api.p1, slots[3])[0] == 201


def test_limits_hold_when_the_same_patient_books_in_parallel(w_api):
    """Five parallel bookings by ONE patient on five doctors' slots with max_active=2: the per-patient row lock
    in RDS makes the re-check authoritative, so exactly 2 win and the other seats are given back."""
    w_api.settings["max_active"] = 2
    docs = [w_api.doctor(f"Par{i}", "Doc", types=((1, 100.0, 1),)) for i in range(5)]
    slots = [w_api.slot(d, start=at_api(2 + i)) for i, d in enumerate(docs)]
    gate = threading.Barrier(5)

    def one(s):
        gate.wait()
        return book_api(w_api.p1, s)

    with ThreadPoolExecutor(5) as pool:
        results = list(pool.map(one, slots))
    assert sorted(st for st, _ in results) == [201, 201, 409, 409, 409]
    assert {d["code"] for st, d in results if st == 409} == {"ACTIVE_LIMIT_REACHED"}
    assert sum(w_api.dynamo.slot(s)["booked_count"] for s in slots) == 2           # losers' seats released
    assert len(w_api.appointments(patient_id=w_api.p1, status="CONFIRMED")) == 2


def test_fee_is_snapshotted(w_api):
    s = w_api.slot(w_api.doc)
    appt = book_api(w_api.p1, s)[1]["appointment"]
    w_api.db.db.execute("UPDATE doctor_consultation_types SET fee = 999 WHERE doctor_id = ?", (w_api.doc,))
    w_api.db.db.commit()
    again = call_api("GET", f"/appointments/{appt['appointmentId']}", w_api.p1)[1]["appointment"]
    assert appt["fee"] == again["fee"] == 300


# ----------------------------------------------------------------------------- events and failure handling
def test_booked_event_has_ids_but_no_personal_data(w_api):
    s = w_api.slot(w_api.doc)
    appt = book_api(w_api.p1, s)[1]["appointment"]
    (entry,) = w_api.events.entries
    detail = json.loads(entry["Detail"])
    assert entry["Source"] == "medislot.booking" and entry["DetailType"] == "AppointmentBooked"
    assert entry["EventBusName"] == "medislot-dev-events"
    assert detail["appointmentId"] == appt["appointmentId"] and detail["slotId"] == s and detail["patientId"] == w_api.p1
    assert detail["eventId"] and detail["occurredAt"] and detail["correlationId"]
    text = json.dumps(detail)
    assert "Pat" not in text and "@" not in text


def test_event_publish_failure_never_undoes_a_booking(w_api):
    w_api.events.fail_times = 3
    s = w_api.slot(w_api.doc)
    assert book_api(w_api.p1, s)[0] == 201
    assert len(w_api.appointments()) == 1 and w_api.metric_sum("EventPublishFailures") == 1


def test_transient_event_failure_is_retried(w_api):
    w_api.events.fail_times = 1
    assert book_api(w_api.p1, w_api.slot(w_api.doc))[0] == 201
    assert w_api.events.types() == ["AppointmentBooked"] and w_api.metric_sum("EventPublishFailures") == 0


def test_ambiguous_commit_failure_keeps_the_seat(w_api):
    """If COMMIT itself fails we cannot know whether it committed: the seat is NOT released (a leaked seat is
    recoverable; releasing it could double-book)."""
    s = w_api.slot(w_api.doc)
    w_api.db.fail_commit = True
    assert book_api(w_api.p1, s)[0] == 500
    assert w_api.dynamo.slot(s)["booked_count"] == 1


def test_doctor_cancels_while_the_booking_is_in_flight(w_api):
    """Seat claimed, then the doctor cancels the slot (their sweep finds nothing yet), then our insert commits.
    The post-commit check sees CANCELLED and refunds this appointment in full."""
    s = w_api.slot(w_api.doc)
    real = w_api.dynamo.transact_write_items

    def claim_then_doctor_cancels(**kw):
        out = real(**kw)
        w_api.dynamo.tables["Slots"][s]["status"] = "CANCELLED"
        return out

    w_api.dynamo.transact_write_items = claim_then_doctor_cancels
    st, data = book_api(w_api.p1, s)
    assert st == 409 and data["code"] == "SLOT_CANCELLED"
    assert data["appointment"]["status"] == "CANCELLED_BY_DOCTOR" and data["appointment"]["refund"]["refundPercent"] == 100
    assert w_api.ledger()[0]["refund_tier"] == "DOCTOR_CANCELLED"
    assert "AppointmentCancelled" in w_api.events.types() and "AppointmentBooked" not in w_api.events.types()


# ----------------------------------------------------------------------------- listing and detail
def test_listing_is_scoped_and_filterable(w_api):
    s1, s2 = w_api.slot(w_api.doc, start=at_api(2)), w_api.slot(w_api.doc2, type_id=2, minutes=30, start=at_api(3))
    a1 = book_api(w_api.p1, s1)[1]["appointment"]["appointmentId"]
    book_api(w_api.p1, s2)
    book_api(w_api.p2, w_api.slot(w_api.doc, start=at_api(5)))
    call_api("POST", f"/appointments/{a1}/cancel", w_api.p1)

    st, data = call_api("GET", "/appointments", w_api.p1, query={"when": "upcoming"})
    assert st == 200 and data["total"] == 2 and all(i["patientId"] == w_api.p1 for i in data["items"])
    assert [i["startAt"] for i in data["items"]] == sorted(i["startAt"] for i in data["items"])
    assert call_api("GET", "/appointments", w_api.p1, query={"status": "CANCELLED"})[1]["total"] == 1
    assert call_api("GET", "/appointments", w_api.p1, query={"status": "confirmed"})[1]["total"] == 1
    assert call_api("GET", "/appointments", w_api.p1, query={"when": "past"})[1]["total"] == 0
    paged = call_api("GET", "/appointments", w_api.p1, query={"pageSize": "1", "page": "2"})[1]
    assert paged["totalPages"] == 2 and len(paged["items"]) == 1 and paged["hasPrevious"] and not paged["hasNext"]
    assert call_api("GET", "/appointments", w_api.p1, query={"page": "9"})[1]["items"] == []

    doc_view = call_api("GET", "/appointments", w_api.doc, role="DOCTOR")[1]
    assert doc_view["total"] == 2 and {i["patient"]["id"] for i in doc_view["items"]} == {w_api.p1, w_api.p2}
    assert all("cancellationPreview" not in i for i in doc_view["items"])


@pytest.mark.parametrize("query", [{"status": "LOST"}, {"when": "tomorrow"}, {"page": "0"}, {"pageSize": "51"}, {"page": "x"}])
def test_listing_rejects_bad_filters(w_api, query):
    assert call_api("GET", "/appointments", w_api.p1, query=query)[0] == 422


def test_detail_isolation(w_api):
    appt = book_api(w_api.p1, w_api.slot(w_api.doc))[1]["appointment"]["appointmentId"]
    assert call_api("GET", f"/appointments/{appt}", w_api.p1)[0] == 200
    assert call_api("GET", f"/appointments/{appt}", w_api.doc, role="DOCTOR")[1]["appointment"]["patient"]["name"] == "Pat Num1"
    other = call_api("GET", f"/appointments/{appt}", w_api.p2)                         # another patient
    missing = call_api("GET", "/appointments/99999", w_api.p2)
    assert other[0] == missing[0] == 404 and other[1]["message"] == missing[1]["message"]
    assert call_api("GET", f"/appointments/{appt}", w_api.doc2, role="DOCTOR")[0] == 404   # a different doctor
    assert call_api("POST", f"/appointments/{appt}/cancel", w_api.p2)[0] == 404             # cannot cancel someone else's
    assert w_api.appointments()[0]["status"] == "CONFIRMED"


# ----------------------------------------------------------------------------- patient cancellation
def test_cancel_releases_the_seat_and_publishes_seat_freed(w_api):
    s = w_api.slot(w_api.doc, type_id=4, minutes=60, capacity=3, start=at_api(5))
    a = book_api(w_api.p1, s)[1]["appointment"]["appointmentId"]
    book_api(w_api.p2, s)
    st, data = call_api("POST", f"/appointments/{a}/cancel", w_api.p1)
    assert st == 200 and data["appointment"]["status"] == "CANCELLED_BY_PATIENT"
    assert data["refund"]["tier"] == "FULL" and data["refund"]["refundAmount"] == 500
    assert w_api.dynamo.slot(s)["booked_count"] == 1
    freed = [json.loads(e["Detail"]) for e in w_api.events.entries if e["DetailType"] == "SlotSeatFreed"]
    assert freed and freed[0]["freeSeats"] == 2 and freed[0]["slotId"] == s and freed[0]["localDate"]
    assert w_api.events.types().count("AppointmentCancelled") == 1
    # the freed seat is bookable again - even by the same patient
    assert book_api(w_api.p1, s)[0] == 201


def test_cancel_is_idempotent_and_pays_once(w_api):
    a = book_api(w_api.p1, w_api.slot(w_api.doc, start=at_api(5)))[1]["appointment"]["appointmentId"]
    first = call_api("POST", f"/appointments/{a}/cancel", w_api.p1)
    second = call_api("POST", f"/appointments/{a}/cancel", w_api.p1)
    assert first[0] == second[0] == 200 and second[1]["alreadyCancelled"] is True
    assert second[1]["refund"] == first[1]["refund"]
    assert len(w_api.ledger()) == 1 and w_api.dynamo.slot(w_api.appointments()[0]["slot_id"])["booked_count"] == 0


def test_parallel_cancels_pay_once(w_api):
    s = w_api.slot(w_api.doc, start=at_api(5))
    a = book_api(w_api.p1, s)[1]["appointment"]["appointmentId"]
    gate = threading.Barrier(6)

    def one(_):
        gate.wait()
        return call_api("POST", f"/appointments/{a}/cancel", w_api.p1)

    with ThreadPoolExecutor(6) as pool:
        results = list(pool.map(one, range(6)))
    assert all(st == 200 for st, _ in results)
    assert sum(1 for _, d in results if not d.get("alreadyCancelled")) == 1
    assert len(w_api.ledger()) == 1 and w_api.dynamo.slot(s)["booked_count"] == 0       # seat released exactly once


def test_cancel_of_a_completed_appointment_is_refused(w_api):
    a = book_api(w_api.p1, w_api.slot(w_api.doc))[1]["appointment"]["appointmentId"]
    w_api.db.db.execute("UPDATE appointments SET status = 'COMPLETED' WHERE appointment_id = ?", (a,))
    w_api.db.db.commit()
    assert call_api("POST", f"/appointments/{a}/cancel", w_api.p1)[1]["code"] == "NOT_CANCELLABLE"


def test_seat_release_problem_is_reported_but_the_cancellation_stands(w_api):
    s = w_api.slot(w_api.doc, start=at_api(5))
    a = book_api(w_api.p1, s)[1]["appointment"]["appointmentId"]
    w_api.dynamo.tables["Slots"][s]["booked_count"] = 0          # counters drifted
    st, data = call_api("POST", f"/appointments/{a}/cancel", w_api.p1)
    assert st == 200 and w_api.metric_sum("SeatReleaseFailures") == 1
    assert w_api.dynamo.slot(s)["booked_count"] == 0             # never negative
    assert "SlotSeatFreed" not in w_api.events.types()


# ----------------------------------------------------------------------------- doctor slot cancellation
def doctor_cancel(w_api, slot, doctor=None, raw=None, body=None):
    raw = raw if raw is not None else slot.replace("#", "%23")
    return call_api("POST", f"/doctors/me/slots/{raw}/cancel", doctor or w_api.doc, role="DOCTOR", body=body or {})


def test_doctor_cancels_a_slot_and_every_patient_is_refunded_in_full(w_api):
    s = w_api.slot(w_api.doc, type_id=4, minutes=60, capacity=3, start=datetime.now(timezone.utc) + timedelta(hours=1))
    a1 = book_api(w_api.p1, s)[1]["appointment"]["appointmentId"]
    a2 = book_api(w_api.p2, s)[1]["appointment"]["appointmentId"]
    st, data = doctor_cancel(w_api, s, body={"reason": "  Emergency   surgery "})
    assert st == 200 and data["cancelledAppointments"] == 2 and data["totalRefunded"] == 1000
    assert {r["appointmentId"] for r in data["refunds"]} == {a1, a2}
    for row in w_api.appointments():
        assert row["status"] == "CANCELLED_BY_DOCTOR" and row["cancellation_note"] == "Emergency surgery"
    assert [(l["refund_percentage"], l["refund_tier"], l["cancellation_reason"]) for l in w_api.ledger()] == \
        [(100, "DOCTOR_CANCELLED", "DOCTOR_CANCEL")] * 2          # 100 % even inside the "no refund" window
    item = w_api.dynamo.slot(s)
    assert item["status"] == "CANCELLED" and item["closed_reason"] == "DOCTOR_CANCELLED"
    assert w_api.events.types().count("AppointmentCancelled") == 2 and "SlotCancelled" in w_api.events.types()
    assert "SlotSeatFreed" not in w_api.events.types()                    # the seats are not bookable
    assert w_api.metric_sum("CancellationsByTier", Tier="DOCTOR_CANCELLED") == 2
    # nobody can book it any more, and the patients see the refund
    assert book_api(w_api.patient(3), s)[1]["code"] == "SLOT_CLOSED"
    refunds = call_api("GET", "/patients/me/refunds", w_api.p1)[1]
    assert refunds["total"] == 1 and refunds["items"][0]["refundAmount"] == 500 and refunds["totals"]["refunded"] == 500
    assert call_api("GET", f"/appointments/{a1}", w_api.doc, role="DOCTOR")[1]["appointment"]["cancellationNote"] == "Emergency surgery"
    # repeating the call is safe: no second refund
    again = doctor_cancel(w_api, s)
    assert again[0] == 200 and again[1]["alreadyCancelled"] and again[1]["cancelledAppointments"] == 0
    assert len(w_api.ledger()) == 2


def test_doctor_cancel_accepts_every_slot_id_form(w_api):
    for raw in (lambda s: s[5:], lambda s: s.replace("#", "%23")):
        s = w_api.slot(w_api.doc, start=at_api(6 + len(w_api.dynamo.tables["Slots"])))
        assert doctor_cancel(w_api, s, raw=raw(s))[0] == 200
    s = w_api.slot(w_api.doc, start=at_api(20))
    assert call_api("POST", "/doctors/me/slots/x/cancel", w_api.doc, role="DOCTOR", body={}, path_params={"slotId": s})[0] == 200
    assert doctor_cancel(w_api, s, raw="nonsense")[1]["code"] == "SLOT_ID_INVALID"


def test_doctor_cannot_cancel_someone_elses_or_a_started_slot(w_api):
    mine_started = w_api.slot(w_api.doc, start=datetime.now(timezone.utc) - timedelta(minutes=5))
    theirs = w_api.slot(w_api.doc2, type_id=2, minutes=30)
    assert doctor_cancel(w_api, theirs)[1]["code"] == "SLOT_NOT_FOUND"
    assert w_api.dynamo.slot(theirs)["status"] == "OPEN"
    assert doctor_cancel(w_api, "SLOT#" + "b" * 40)[1]["code"] == "SLOT_NOT_FOUND"
    assert doctor_cancel(w_api, mine_started)[1]["code"] == "SLOT_STARTED"
    assert w_api.dynamo.slot(mine_started)["status"] == "OPEN"


def test_doctor_without_profile(w_api):
    s = w_api.slot(w_api.doc)
    w_api.db.db.execute("DELETE FROM doctor_consultation_types WHERE doctor_id = ?", (w_api.doc,))
    w_api.db.db.execute("DELETE FROM doctors WHERE doctor_id = ?", (w_api.doc,))
    w_api.db.db.commit()
    assert doctor_cancel(w_api, s)[1]["code"] == "DOCTOR_PROFILE_REQUIRED"


def test_patient_cancel_after_doctor_cancel_reports_the_doctor_refund(w_api):
    s = w_api.slot(w_api.doc, start=at_api(4))
    a = book_api(w_api.p1, s)[1]["appointment"]["appointmentId"]
    doctor_cancel(w_api, s)
    st, data = call_api("POST", f"/appointments/{a}/cancel", w_api.p1)
    assert st == 200 and data["alreadyCancelled"] and data["refund"]["tier"] == "DOCTOR_CANCELLED"
    assert len(w_api.ledger()) == 1


# ----------------------------------------------------------------------------- refund ledger endpoint
def test_refund_ledger_endpoint_is_private_and_paged(w_api):
    for i in range(3):
        a = book_api(w_api.p1, w_api.slot(w_api.doctor(f"R{i}", "Doc", types=((1, 200.0, 1),)), start=at_api(5 + i)))[1]["appointment"]["appointmentId"]
        call_api("POST", f"/appointments/{a}/cancel", w_api.p1)
    b = book_api(w_api.p2, w_api.slot(w_api.doc, start=at_api(9)))[1]["appointment"]["appointmentId"]
    call_api("POST", f"/appointments/{b}/cancel", w_api.p2)
    st, data = call_api("GET", "/patients/me/refunds", w_api.p1, query={"pageSize": "2"})
    assert st == 200 and data["total"] == 3 and len(data["items"]) == 2 and data["hasNext"]
    assert data["totals"] == {"refunded": 600, "deducted": 0}
    assert all(i["appointment"]["appointmentId"] != b for i in data["items"])
    assert call_api("GET", "/patients/me/refunds", w_api.p2)[1]["total"] == 1
    assert call_api("GET", "/patients/me/refunds", w_api.patient(7))[1] == {**call_api("GET", "/patients/me/refunds", w_api.patient(8))[1],
                                                                    "correlationId": "test-cid"}
    assert call_api("POST", "/patients/me/refunds", w_api.p1)[0] == 405


# ==============================================================================================================
# Family members - one account booking for several people
# ==============================================================================================================
# Family members: one patient account books for itself AND for family members ("dependents").
#
# The rules about one human being apply per person (overlap, same slot twice, per doctor per day), so a patient and
# their mother can both be booked 11:00-11:30 with two different doctors from the same account. The cap on upcoming
# appointments stays per account.

def at_family(days, hour=5, minute=30):           # 05:30 UTC = 11:00 IST
    base = datetime.now(timezone.utc).replace(hour=hour, minute=minute, second=0, microsecond=0)
    return base + timedelta(days=days)


def call_family(method, path, user, role="PATIENT", body=None, key="auto", query=None):
    ev = api_event(method, path, user_id=user, role=role, body=body, query=query)
    if method == "POST" and path == "/appointments" and key is not None:
        ev["headers"]["Idempotency-Key"] = uuid.uuid4().hex if key == "auto" else key
    res = booking.handler(ev, None)
    return res["statusCode"], json.loads(res["body"])


def book_family(pid, slot, dependent=None, **kw):
    payload = {"slotId": slot}
    if dependent is not None:
        payload["dependentId"] = dependent
    return call_family("POST", "/appointments", pid, body=payload, **kw)


@pytest.fixture
def w_family(monkeypatch):
    world = World(monkeypatch, {"max_active": 5})
    world.cardio = world.doctor("Ravi", "Kumar", types=((2, 500.0, 1), (4, 300.0, 3)))
    world.derma = world.doctor("Meena", "Rao", types=((2, 400.0, 1),))
    world.me = world.patient(1)
    world.mom = world.dependent(world.me, "Lakshmi", "Rao", "Mother")
    return world


# ----------------------------------------------------------------------------- the user's scenario
def test_me_and_my_mother_at_the_same_time_with_different_doctors(w_family):
    """11:00-11:30 IST: me with the cardiologist, Mom with the dermatologist - same account, both confirmed."""
    mine = w_family.slot(w_family.cardio, type_id=2, start=at_family(3), minutes=30)
    moms = w_family.slot(w_family.derma, type_id=2, start=at_family(3), minutes=30)
    a = book_family(w_family.me, mine)
    b = book_family(w_family.me, moms, dependent=w_family.mom)
    assert a[0] == 201 and b[0] == 201, (a, b)
    assert a[1]["appointment"]["bookedFor"] == {"type": "SELF"}
    assert b[1]["appointment"]["bookedFor"] == {"type": "DEPENDENT", "dependentId": w_family.mom, "relation": "Mother", "name": "Lakshmi Rao"}
    assert a[1]["appointment"]["localStart"] == b[1]["appointment"]["localStart"] == "11:00"
    rows = w_family.appointments(status="CONFIRMED")
    assert [(r["patient_id"], r["dependent_id"]) for r in rows] == [(w_family.me, None), (w_family.me, w_family.mom)]


def test_each_person_still_cannot_be_in_two_places_at_once(w_family):
    first = w_family.slot(w_family.cardio, type_id=2, start=at_family(3), minutes=30)
    second = w_family.slot(w_family.derma, type_id=2, start=at_family(3, minute=45), minutes=30)     # 11:15-11:45 overlaps
    assert book_family(w_family.me, first, dependent=w_family.mom)[0] == 201
    st, data = book_family(w_family.me, second, dependent=w_family.mom)
    assert st == 409 and data["code"] == "OVERLAPPING_APPOINTMENT" and data["message"].startswith("Lakshmi Rao already has")
    st, data = book_family(w_family.me, second)                                                # but I am free then
    assert st == 201
    third = w_family.slot(w_family.cardio, type_id=2, start=at_family(3, minute=50), minutes=30)
    st, data = book_family(w_family.me, third)
    assert data["code"] == "OVERLAPPING_APPOINTMENT" and data["message"] == "You already have another appointment at that time"


def test_two_family_members_in_the_same_health_camp_slot(w_family):
    camp = w_family.slot(w_family.cardio, type_id=4, start=at_family(4), minutes=60, capacity=3)
    dad = w_family.dependent(w_family.me, "Suresh", "Rao", "Father", "1960-01-01")
    assert [book_family(w_family.me, camp, dependent=d)[0] for d in (None, w_family.mom, dad)] == [201, 201, 201]
    assert w_family.dynamo.slot(camp)["booked_count"] == 3
    st, data = book_family(w_family.me, camp, dependent=w_family.mom)
    assert st == 409 and data["code"] in ("DUPLICATE_BOOKING", "SLOT_FULL")


def test_same_person_same_slot_twice_is_a_duplicate(w_family):
    camp = w_family.slot(w_family.cardio, type_id=4, start=at_family(4), minutes=60, capacity=3)
    assert book_family(w_family.me, camp, dependent=w_family.mom)[0] == 201
    st, data = book_family(w_family.me, camp, dependent=w_family.mom)
    assert st == 409 and data["code"] == "DUPLICATE_BOOKING" and "Lakshmi Rao is already booked" in data["message"]
    assert w_family.dynamo.slot(camp)["booked_count"] == 1


def test_per_doctor_per_day_is_per_person(w_family):
    morning = w_family.slot(w_family.cardio, type_id=2, start=at_family(5), minutes=30)
    noon = w_family.slot(w_family.cardio, type_id=2, start=at_family(5, hour=7), minutes=30)
    evening = w_family.slot(w_family.cardio, type_id=2, start=at_family(5, hour=9), minutes=30)
    assert book_family(w_family.me, morning)[0] == 201
    assert book_family(w_family.me, noon, dependent=w_family.mom)[0] == 201                           # Mom: her own daily allowance
    assert book_family(w_family.me, evening)[1]["code"] == "DOCTOR_DAILY_LIMIT_REACHED"
    assert book_family(w_family.me, evening, dependent=w_family.mom)[1]["code"] == "DOCTOR_DAILY_LIMIT_REACHED"


def test_upcoming_cap_counts_the_whole_account(w_family):
    w_family.settings["max_active"] = 2
    a, b, c = (w_family.slot(w_family.derma, type_id=2, start=at_family(d), minutes=30) for d in (2, 3, 4))
    assert book_family(w_family.me, a)[0] == 201 and book_family(w_family.me, b, dependent=w_family.mom)[0] == 201
    st, data = book_family(w_family.me, c, dependent=w_family.mom)
    assert data["code"] == "ACTIVE_LIMIT_REACHED" and "including family members" in data["message"]


def test_parallel_bookings_for_me_and_mom_at_the_same_time(w_family):
    """Both requests race through the per-account lock; both must succeed (they are different people)."""
    mine = w_family.slot(w_family.cardio, type_id=2, start=at_family(6), minutes=30)
    moms = w_family.slot(w_family.derma, type_id=2, start=at_family(6), minutes=30)
    gate = threading.Barrier(2)

    def go(args):
        gate.wait()
        return book_family(w_family.me, *args)

    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(go, [(mine, None), (moms, w_family.mom)]))
    assert [st for st, _ in results] == [201, 201]


# ----------------------------------------------------------------------------- who can be booked for
def test_cannot_book_for_someone_elses_or_a_removed_family_member(w_family):
    other = w_family.patient(2)
    strangers_mom = w_family.dependent(other, "Kamala", "Devi", "Mother")
    removed = w_family.dependent(w_family.me, "Old", "Entry", "Other", active=False)
    s = w_family.slot(w_family.derma, type_id=2, start=at_family(3), minutes=30)
    for dep in (strangers_mom, removed, 999999):
        st, data = book_family(w_family.me, s, dependent=dep)
        assert st == 404 and data["code"] == "DEPENDENT_NOT_FOUND"
    assert w_family.dynamo.slot(s)["booked_count"] == 0 and w_family.appointments() == []


@pytest.mark.parametrize("value", ["12", 0, -1, 1.5, True, [1]])
def test_dependent_id_must_be_a_positive_integer_or_null(w_family, value):
    st, data = book_family(w_family.me, w_family.slot(w_family.derma, type_id=2, start=at_family(3), minutes=30), dependent=value)
    assert st == 422 and data["code"] == "DEPENDENT_ID_INVALID"


def test_null_dependent_means_myself(w_family):
    s = w_family.slot(w_family.derma, type_id=2, start=at_family(3), minutes=30)
    st, data = call_family("POST", "/appointments", w_family.me, body={"slotId": s, "dependentId": None})
    assert st == 201 and data["appointment"]["bookedFor"] == {"type": "SELF"}


def test_idempotency_key_reused_for_a_different_person_is_rejected(w_family):
    s = w_family.slot(w_family.cardio, type_id=4, start=at_family(4), minutes=60, capacity=3)
    assert book_family(w_family.me, s, key="family-key-001")[0] == 201
    st, data = book_family(w_family.me, s, dependent=w_family.mom, key="family-key-001")
    assert st == 422 and data["code"] == "IDEMPOTENCY_KEY_REUSED"
    st, data = book_family(w_family.me, s, dependent=w_family.mom, key="family-key-002")
    assert st == 201
    replay = book_family(w_family.me, s, dependent=w_family.mom, key="family-key-002")
    assert replay[0] == 201 and replay[1]["appointment"]["appointmentId"] == data["appointment"]["appointmentId"]


def test_family_member_removed_while_the_booking_races_gives_the_seat_back(w_family):
    s = w_family.slot(w_family.derma, type_id=2, start=at_family(3), minutes=30)
    real = w_family.dynamo.transact_write_items

    def claim_then_remove(**kw):
        out = real(**kw)
        w_family.db.db.execute("UPDATE patient_dependents SET is_active = 0 WHERE dependent_id = ?", (w_family.mom,))
        w_family.db.db.commit()
        return out

    w_family.dynamo.transact_write_items = claim_then_remove
    st, data = book_family(w_family.me, s, dependent=w_family.mom)
    assert st == 404 and data["code"] == "DEPENDENT_NOT_FOUND"
    assert w_family.dynamo.slot(s)["booked_count"] == 0 and w_family.appointments() == []


# ----------------------------------------------------------------------------- what everyone sees
def test_doctor_sees_the_actual_patient_and_who_booked(w_family):
    s = w_family.slot(w_family.derma, type_id=2, start=at_family(3), minutes=30)
    a = book_family(w_family.me, s, dependent=w_family.mom)[1]["appointment"]["appointmentId"]
    st, data = call_family("GET", f"/appointments/{a}", w_family.derma, role="DOCTOR")
    assert data["appointment"]["patient"] == {"id": w_family.me, "name": "Lakshmi Rao", "bookedBy": "Pat Num1", "relation": "Mother"}


def test_listing_filter_by_person_and_events_carry_the_dependent(w_family):
    a = w_family.slot(w_family.cardio, type_id=2, start=at_family(3), minutes=30)
    b = w_family.slot(w_family.derma, type_id=2, start=at_family(3), minutes=30)
    book_family(w_family.me, a)
    book_family(w_family.me, b, dependent=w_family.mom)
    assert call_family("GET", "/appointments", w_family.me)[1]["total"] == 2
    assert [i["slotId"] for i in call_family("GET", "/appointments", w_family.me, query={"bookedFor": "self"})[1]["items"]] == [a]
    assert [i["slotId"] for i in call_family("GET", "/appointments", w_family.me, query={"bookedFor": str(w_family.mom)})[1]["items"]] == [b]
    assert call_family("GET", "/appointments", w_family.me, query={"bookedFor": "mom"})[0] == 422
    assert call_family("GET", "/appointments", w_family.cardio, role="DOCTOR", query={"bookedFor": "self"})[0] == 422
    details = [json.loads(e["Detail"]) for e in w_family.events.entries if e["DetailType"] == "AppointmentBooked"]
    assert [d["dependentId"] for d in details] == [None, w_family.mom]
    assert "Lakshmi" not in json.dumps(details)                                   # ids only, no names in events


def test_cancel_and_refund_work_for_a_family_booking(w_family):
    s = w_family.slot(w_family.derma, type_id=2, start=at_family(5), minutes=30)
    a = book_family(w_family.me, s, dependent=w_family.mom)[1]["appointment"]["appointmentId"]
    st, data = call_family("POST", f"/appointments/{a}/cancel", w_family.me)
    assert st == 200 and data["refund"]["tier"] == "FULL" and data["appointment"]["bookedFor"]["name"] == "Lakshmi Rao"
    refunds = call_family("GET", "/patients/me/refunds", w_family.me)[1]["items"]
    assert refunds[0]["appointment"]["bookedFor"]["dependentId"] == w_family.mom
    cancelled = [json.loads(e["Detail"]) for e in w_family.events.entries if e["DetailType"] == "AppointmentCancelled"]
    assert cancelled[0]["dependentId"] == w_family.mom


# ----------------------------------------------------------------------------- managing family members
def dep(method, path, user, body=None):
    return call_family(method, "/patients/me/dependents" + path, user, body=body)


def test_add_list_edit_remove(w_family):
    st, data = dep("POST", "", w_family.me, {"firstName": "  Anu ", "lastName": "Rao", "relation": "child", "dateOfBirth": "2015-06-01",
                                      "gender": "female"})
    assert st == 201 and data["dependent"]["firstName"] == "Anu" and data["dependent"]["relation"] == "Child"
    assert data["dependent"]["gender"] == "Female"
    anu = data["dependent"]["dependentId"]
    items = dep("GET", "", w_family.me)[1]["items"]
    assert [i["displayName"] for i in items] == ["Lakshmi Rao", "Anu Rao"]
    st, data = dep("PATCH", f"/{anu}", w_family.me, {"lastName": "Kumar", "gender": None})
    assert st == 200 and data["dependent"]["lastName"] == "Kumar" and data["dependent"]["gender"] is None
    assert dep("DELETE", f"/{anu}", w_family.me)[0] == 200
    assert [i["displayName"] for i in dep("GET", "", w_family.me)[1]["items"]] == ["Lakshmi Rao"]
    assert dep("DELETE", f"/{anu}", w_family.me)[0] == 404                                # already removed


@pytest.mark.parametrize("payload,field", [
    ({"lastName": "Rao", "relation": "Mother", "dateOfBirth": "1965-01-01"}, "firstName"),
    ({"firstName": "A1", "lastName": "Rao", "relation": "Mother", "dateOfBirth": "1965-01-01"}, "firstName"),
    ({"firstName": "A", "lastName": "Rao", "relation": "Friend", "dateOfBirth": "1965-01-01"}, "relation"),
    ({"firstName": "A", "lastName": "Rao", "relation": "Mother"}, "dateOfBirth"),
    ({"firstName": "A", "lastName": "Rao", "relation": "Mother", "dateOfBirth": "2999-01-01"}, "dateOfBirth"),
    ({"firstName": "A", "lastName": "Rao", "relation": "Mother", "dateOfBirth": "1965-02-30"}, "dateOfBirth"),
    ({"firstName": "A", "lastName": "Rao", "relation": "Mother", "dateOfBirth": "1965-01-01", "gender": "x"}, "gender"),
    ({"firstName": "A", "lastName": "Rao", "relation": "Mother", "dateOfBirth": "1965-01-01", "patientId": 9}, "patientId"),
])
def test_family_member_validation(w_family, payload, field):
    st, data = dep("POST", "", w_family.me, payload)
    assert st == 422 and field in data["errors"]


def test_duplicate_family_member_and_limit(w_family):
    st, data = dep("POST", "", w_family.me, {"firstName": "lakshmi", "lastName": "RAO", "relation": "Mother", "dateOfBirth": "1965-04-02"})
    assert st == 409 and data["code"] == "DEPENDENT_EXISTS"
    for i in range(booking.MAX_DEPENDENTS - 1):
        assert dep("POST", "", w_family.me, {"firstName": f"Kid{'abcdefghij'[i]}", "lastName": "Rao", "relation": "Child",
                                      "dateOfBirth": "2010-01-01"})[0] == 201
    st, data = dep("POST", "", w_family.me, {"firstName": "One", "lastName": "More", "relation": "Other", "dateOfBirth": "2000-01-01"})
    assert st == 409 and data["code"] == "DEPENDENT_LIMIT_REACHED"


def test_cannot_remove_a_family_member_with_upcoming_appointments(w_family):
    s = w_family.slot(w_family.derma, type_id=2, start=at_family(5), minutes=30)
    a = book_family(w_family.me, s, dependent=w_family.mom)[1]["appointment"]["appointmentId"]
    assert dep("GET", "", w_family.me)[1]["items"][0]["upcomingAppointments"] == 1
    st, data = dep("DELETE", f"/{w_family.mom}", w_family.me)
    assert st == 409 and data["code"] == "DEPENDENT_HAS_APPOINTMENTS"
    call_family("POST", f"/appointments/{a}/cancel", w_family.me)
    assert dep("DELETE", f"/{w_family.mom}", w_family.me)[0] == 200
    # history still shows who the cancelled appointment was for
    assert call_family("GET", f"/appointments/{a}", w_family.me)[1]["appointment"]["bookedFor"]["name"] == "Lakshmi Rao"


def test_family_members_are_private_to_the_account(w_family):
    other = w_family.patient(2)
    assert dep("GET", "", other)[1]["items"] == []
    assert dep("PATCH", f"/{w_family.mom}", other, {"lastName": "X"})[0] == 404
    assert dep("DELETE", f"/{w_family.mom}", other)[0] == 404
    assert call_family("GET", "/patients/me/dependents", w_family.cardio, role="DOCTOR")[0] == 403
    assert dep("GET", "/abc", w_family.me)[0] == 422
    assert dep("POST", f"/{w_family.mom}", w_family.me, {})[0] == 405
    assert dep("PATCH", f"/{w_family.mom}", w_family.me, {})[0] == 400


def test_profile_required_before_adding_family(w_family):
    p = w_family.patient(3)
    w_family.db.db.execute("DELETE FROM patients WHERE patient_id = ?", (p,))
    w_family.db.db.commit()
    st, data = dep("POST", "", p, {"firstName": "A", "lastName": "B", "relation": "Mother", "dateOfBirth": "1960-01-01"})
    assert st == 409 and data["code"] == "PROFILE_REQUIRED"


# ==============================================================================================================
# Dashboard pages - Book button, appointments, refunds, family, doctor slot cancel
# ==============================================================================================================
# Milestone 5 dashboard pages: Book button (with its own Idempotency-Key), My appointments, cancel with refund
# preview, Refunds, and the doctor's "Cancel this slot". Same harness as test_patient_dashboard.py (fake API).

SLOT_A = "SLOT#" + "a" * 40
SLOT_B = "SLOT#" + "b" * 40


def appt(i=11, status="CONFIRMED", preview=True, refund=None):
    a = {"appointmentId": i, "status": status, "patientId": 7, "doctorId": 1, "slotId": SLOT_A, "startAt": "2026-11-20T04:30:00Z",
         "endAt": "2026-11-20T04:45:00Z", "timezone": "Asia/Kolkata", "localDate": "2026-11-20", "localStart": "10:00", "localEnd": "10:15",
         "fee": 300, "consultationType": {"id": 1, "name": "Quick Consult", "durationMinutes": 15},
         "doctor": {"id": 1, "displayName": "Dr. Anil <Rao>", "hospital": "Apollo", "city": "Hyderabad"},
         "cancellable": status == "CONFIRMED", "refund": refund}
    if preview and status == "CONFIRMED":
        a["cancellationPreview"] = {"tier": "FULL", "refundPercent": 100, "refundAmount": 300, "deductionAmount": 0, "validUntil": "2026-11-19T04:30:00Z"}
    return a


def capture_headers(env):
    seen = []
    inner = env.dash.requests.request

    def wrapper(method, url, headers=None, timeout=None, **kw):
        seen.append((method, url.replace("https://api.test/dev", ""), dict(headers or {}), kw))
        return inner(method, url, headers=headers, timeout=timeout, **kw)
    env.dash.requests.request = wrapper
    return seen


def open_doctor_with_bookable_slots(env):
    login_patient(env)
    env.route("GET", "/doctors/1", FakeResp(200, doc_profile()))
    days = slots_payload()["days"]
    days[0]["slots"][0]["slotId"] = SLOT_A          # free, valid id      -> Book button
    days[0]["slots"][1]["slotId"] = "not-a-slot"    # free, invalid id    -> no button
    days[0]["slots"][2]["slotId"] = SLOT_B          # full                -> no button
    env.route("GET", "/doctors/1/slots", FakeResp(200, slots_payload(days)))


# ----------------------------------------------------------------------------- Book button
def test_only_free_slots_with_valid_ids_get_a_book_button_each_with_its_own_key(env):
    open_doctor_with_bookable_slots(env)
    page = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert page.count('action="/patient/book"') == 1
    assert f'value="{SLOT_A}"' in page and f'value="{SLOT_B}"' not in page
    keys = re.findall(r'name="key" value="([0-9a-f]{32})"', page)
    assert len(keys) == 1
    second = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert re.findall(r'name="key" value="([0-9a-f]{32})"', second) != keys           # a new page = a new key
    assert 'name="back" value="/patient/doctors/1"' in page


def test_booking_sends_the_form_key_as_idempotency_key_and_opens_the_appointment(env):
    login_patient(env)
    seen = capture_headers(env)
    env.route("POST", "/appointments", FakeResp(201, {"appointment": appt()}))
    r = env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "k" * 32, "back": "/patient/doctors/1"})
    assert r.headers["Location"].endswith("/patient/appointments/11")
    method, path, headers, kw = [c for c in seen if c[1] == "/appointments"][0]
    assert method == "POST" and headers["Idempotency-Key"] == "k" * 32 and kw["json"] == {"slotId": SLOT_A}
    env.route("GET", "/appointments/11", FakeResp(200, {"appointment": appt()}))
    page = env.client.get("/patient/appointments/11").get_data(as_text=True)
    assert "Booked: Quick Consult with Dr. Anil &lt;Rao&gt;" in page


def test_double_submit_reuses_the_key(env):
    login_patient(env)
    seen = capture_headers(env)
    env.route("POST", "/appointments", FakeResp(201, {"appointment": appt()}))
    for _ in range(2):
        env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "same-key-123", "back": "/patient/doctors/1"})
    keys = [h["Idempotency-Key"] for m, p, h, _ in seen if p == "/appointments"]
    assert keys == ["same-key-123", "same-key-123"]


def test_slot_full_message_goes_back_to_the_doctor_page(env):
    login_patient(env)
    env.route("POST", "/appointments", FakeResp(409, {"message": "Sorry, this slot is fully booked", "code": "SLOT_FULL"}))
    env.route("GET", "/doctors/1", FakeResp(200, doc_profile()))
    env.route("GET", "/doctors/1/slots", FakeResp(200, slots_payload()))
    r = env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "k" * 32, "back": "/patient/doctors/1?from=2026-11-04"})
    assert r.headers["Location"].endswith("/patient/doctors/1?from=2026-11-04")
    assert "fully booked" in env.client.get("/patient/doctors/1").get_data(as_text=True)


@pytest.mark.parametrize("form", [{"slotId": "SLOT#xyz", "key": "k" * 32}, {"slotId": SLOT_A, "key": "bad key!"}, {"slotId": SLOT_A}])
def test_tampered_booking_forms_never_reach_the_api(env, form):
    login_patient(env)
    env.client.post("/patient/book", data=form)
    assert not [c for c in env.calls if c[1] == "/appointments"]


@pytest.mark.parametrize("back", ["https://evil.example/x", "//evil.example", "/doctor-area", "/patient\\..\\x"])
def test_back_link_cannot_leave_the_patient_area(env, back):
    login_patient(env)
    env.route("POST", "/appointments", FakeResp(409, {"message": "full", "code": "SLOT_FULL"}))
    r = env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "k" * 32, "back": back})
    assert r.headers["Location"].endswith("/patient/doctors")


def test_server_error_on_booking_is_friendly(env):
    login_patient(env)
    env.route("POST", "/appointments", FakeResp(500, {"message": "Internal server error"}))
    env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "k" * 32, "back": "/patient/doctors/1"})
    env.route("GET", "/doctors/1", FakeResp(200, doc_profile()))
    env.route("GET", "/doctors/1/slots", FakeResp(200, slots_payload()))
    page = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert "Check My appointments before trying again" in page


def test_doctors_cannot_use_the_book_route(env):
    env.route("POST", "/auth/login", FakeResp(200, {"accessToken": "a", "refreshToken": "r", "role": "DOCTOR", "userId": 3}))
    env.client.post("/login", data={"email": "d@example.com", "password": "pw"})
    r = env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "k" * 32})
    assert r.status_code == 302 and not [c for c in env.calls if c[1] == "/appointments"]


# ----------------------------------------------------------------------------- My appointments / detail / cancel
def test_appointments_tabs_forward_the_right_filters(env):
    login_patient(env)
    params = []
    env.route("GET", "/appointments", lambda kw: params.append(kw.get("params")) or FakeResp(200, {
        "items": [appt()], "page": 1, "pageSize": 9, "total": 1, "totalPages": 1, "hasPrevious": False, "hasNext": False}))
    page = env.client.get("/patient/appointments").get_data(as_text=True)
    assert "Quick Consult" in page and "Dr. Anil &lt;Rao&gt;" in page and "Cancel now: ₹300 back" in page
    env.client.get("/patient/appointments?tab=past")
    env.client.get("/patient/appointments?tab=cancelled")
    env.client.get("/patient/appointments?tab=<script>")
    assert [p.get("when") for p in params] == ["upcoming", "past", None, "upcoming"]
    assert [p.get("status") for p in params] == ["CONFIRMED", None, "CANCELLED", "CONFIRMED"]


def test_appointments_outage_is_friendly(env):
    login_patient(env)
    env.route("GET", "/appointments", FakeResp(503, {"message": "db down"}))
    page = env.client.get("/patient/appointments").get_data(as_text=True)
    assert "could not be loaded" in page and "db down" not in page


def test_detail_shows_refund_preview_and_cancel_form(env):
    login_patient(env)
    env.route("GET", "/appointments/11", FakeResp(200, {"appointment": appt()}))
    page = env.client.get("/patient/appointments/11").get_data(as_text=True)
    assert "Cancel now and get <b>₹300</b> back (100%)" in page and "Thu 19 Nov, 10:00" in page
    assert 'action="/patient/appointments/11/cancel"' in page


def test_cancelled_appointment_shows_the_refund_and_no_cancel_form(env):
    login_patient(env)
    refund = {"refundAmount": 150, "deductionAmount": 150, "refundPercent": 50, "tier": "PARTIAL", "reason": "PATIENT_CANCEL"}
    env.route("GET", "/appointments/11", FakeResp(200, {"appointment": appt(status="CANCELLED_BY_PATIENT", refund=refund)}))
    page = env.client.get("/patient/appointments/11").get_data(as_text=True)
    assert "₹150" in page and "Partial" in page and "/cancel" not in page and "Cancelled by you" in page


def test_cancel_needs_confirmation_then_reports_the_refund(env):
    login_patient(env)
    env.client.post("/patient/appointments/11/cancel", data={})
    assert not [c for c in env.calls if c[1].endswith("/cancel")]
    env.route("POST", "/appointments/11/cancel", FakeResp(200, {"refund": {"refundAmount": 150, "refundPercent": 50}, "appointment": appt()}))
    env.route("GET", "/appointments/11", FakeResp(200, {"appointment": appt(status="CANCELLED_BY_PATIENT")}))
    r = env.client.post("/patient/appointments/11/cancel", data={"confirm": "yes"})
    assert r.headers["Location"].endswith("/patient/appointments/11")
    assert "Refund: ₹150 (50%)" in env.client.get("/patient/appointments/11").get_data(as_text=True)


def test_cancel_too_late_shows_the_api_message(env):
    login_patient(env)
    env.route("POST", "/appointments/11/cancel", FakeResp(409, {"message": "This appointment has already started", "code": "ALREADY_STARTED"}))
    env.route("GET", "/appointments/11", FakeResp(200, {"appointment": appt()}))
    env.client.post("/patient/appointments/11/cancel", data={"confirm": "yes"})
    assert "already started" in env.client.get("/patient/appointments/11").get_data(as_text=True)


def test_unknown_appointment_returns_to_the_list(env):
    login_patient(env)
    env.route("GET", "/appointments/99", FakeResp(404, {"message": "Appointment not found"}))
    assert env.client.get("/patient/appointments/99").headers["Location"].endswith("/patient/appointments")


# ----------------------------------------------------------------------------- refunds
def test_refunds_page_shows_totals_and_rows(env):
    login_patient(env)
    env.route("GET", "/patients/me/refunds", FakeResp(200, {
        "items": [{"refundId": 1, "appointmentId": 11, "feeAmount": 300, "refundAmount": 300, "deductionAmount": 0, "refundPercent": 100,
                   "tier": "DOCTOR_CANCELLED", "reason": "DOCTOR_CANCEL", "calculatedAt": "2026-11-10T06:00:00Z",
                   "appointment": appt(status="CANCELLED_BY_DOCTOR")}],
        "page": 1, "pageSize": 9, "total": 1, "totalPages": 1, "hasPrevious": False, "hasNext": False,
        "totals": {"refunded": 300, "deducted": 0}}))
    page = env.client.get("/patient/refunds").get_data(as_text=True)
    assert "Total refunded" in page and "₹300" in page and "Doctor cancelled" in page and "/patient/appointments/11" in page


def test_empty_refunds(env):
    login_patient(env)
    env.route("GET", "/patients/me/refunds", FakeResp(200, {"items": [], "total": 0, "totals": {"refunded": 0, "deducted": 0},
                                                             "hasPrevious": False, "hasNext": False}))
    assert "No refunds yet" in env.client.get("/patient/refunds").get_data(as_text=True)


# ----------------------------------------------------------------------------- doctor: cancel a slot
def login_doctor(env):
    env.route("POST", "/auth/login", FakeResp(200, {"accessToken": "a", "refreshToken": "r", "role": "DOCTOR", "userId": 3}))
    env.client.post("/login", data={"email": "d@example.com", "password": "pw"})


def iso(dt):
    return dt.strftime("%Y-%m-%dT%H:%M:%SZ")


def test_doctor_schedule_offers_cancel_only_for_future_open_slots(env):
    login_doctor(env)
    future, past = datetime.now(timezone.utc) + timedelta(days=2), datetime.now(timezone.utc) - timedelta(hours=2)
    slots = [
        {"slot_id": SLOT_A, "start_at": iso(future), "end_at": iso(future + timedelta(minutes=60)), "capacity": 3, "booked_count": 2, "status": "OPEN"},
        {"slot_id": SLOT_B, "start_at": iso(past), "end_at": iso(past + timedelta(minutes=15)), "capacity": 1, "booked_count": 0, "status": "OPEN"},
        {"slot_id": "SLOT#" + "c" * 40, "start_at": iso(future + timedelta(hours=2)), "end_at": iso(future + timedelta(hours=3)), "capacity": 3,
         "booked_count": 3, "status": "CANCELLED", "closed_reason": "DOCTOR_CANCELLED"}]
    env.route("GET", "/doctors/me/schedule", FakeResp(200, {"date": future.date().isoformat(), "timezone": "UTC", "slots": slots, "appointments": []}))
    page = env.client.get("/schedule?date=" + future.date().isoformat()).get_data(as_text=True)
    assert page.count('action="/schedule/slots/cancel"') == 1 and 'value="' + "a" * 40 + '"' in page
    assert "Cancel and refund 2 patients in full" in page and "Cancelled" in page


def test_doctor_cancel_slot_calls_the_api_and_reports(env):
    login_doctor(env)
    env.route("POST", "/doctors/me/slots/" + "a" * 40 + "/cancel", FakeResp(200, {"cancelledAppointments": 2}))
    r = env.client.post("/schedule/slots/cancel", data={"slot": "a" * 40, "date": "2026-11-20", "confirm": "yes", "reason": "  Surgery  "})
    assert r.headers["Location"].endswith("/schedule?date=2026-11-20")
    call = [c for c in env.calls if c[1].endswith("/cancel")][0]
    assert call[2]["json"] == {"reason": "Surgery"}


@pytest.mark.parametrize("form", [{"slot": "a" * 40, "date": "2026-11-20"}, {"slot": "../x", "confirm": "yes", "date": "2026-11-20"}])
def test_doctor_cancel_slot_needs_confirmation_and_a_valid_id(env, form):
    login_doctor(env)
    r = env.client.post("/schedule/slots/cancel", data=form)
    assert r.status_code == 302 and not [c for c in env.calls if c[1].endswith("/cancel")]


# ----------------------------------------------------------------------------- family members
FAMILY = {"items": [{"dependentId": 5, "firstName": "Lakshmi", "lastName": "O'Neil", "displayName": "Lakshmi O'Neil", "relation": "Mother",
                     "dateOfBirth": "1965-04-02", "gender": None, "upcomingAppointments": 0},
                    {"dependentId": 6, "firstName": "Anu", "lastName": "Rao", "displayName": "Anu Rao", "relation": "Child",
                     "dateOfBirth": "2015-06-01", "gender": "Female", "upcomingAppointments": 2}],
          "maxDependents": 10, "relations": []}


def test_book_form_offers_booking_for_family_members(env):
    open_doctor_with_bookable_slots(env)
    env.route("GET", "/patients/me/dependents", FakeResp(200, FAMILY))
    page = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert 'name="dependentId"' in page and '<option value="">Me</option>' in page
    assert '<option value="5">Lakshmi O&#39;Neil (Mother)</option>' in page and '<option value="6">Anu Rao (Child)</option>' in page


def test_no_family_means_no_selector(env):
    open_doctor_with_bookable_slots(env)
    env.route("GET", "/patients/me/dependents", FakeResp(200, {"items": []}))
    page = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert 'name="dependentId"' not in page and 'action="/patient/book"' in page and "Add them as a family member" in page


def test_booking_for_a_family_member_sends_dependent_id(env):
    login_patient(env)
    a = appt()
    a["bookedFor"] = {"type": "DEPENDENT", "dependentId": 5, "name": "Lakshmi Rao", "relation": "Mother"}
    env.route("POST", "/appointments", FakeResp(201, {"appointment": a}))
    env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "k" * 32, "dependentId": "5", "back": "/patient/doctors/1"})
    sent = [c for c in env.calls if c[1] == "/appointments"][0][2]["json"]
    assert sent == {"slotId": SLOT_A, "dependentId": 5}
    env.route("GET", "/appointments/11", FakeResp(200, {"appointment": a}))
    page = env.client.get("/patient/appointments/11").get_data(as_text=True)
    assert "for Lakshmi Rao (Mother)." in page and "<dt>Patient</dt><dd>Lakshmi Rao (Mother)</dd>" in page


def test_booking_for_myself_sends_no_dependent_id(env):
    login_patient(env)
    env.route("POST", "/appointments", FakeResp(201, {"appointment": appt()}))
    env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "k" * 32, "dependentId": "", "back": "/patient/doctors/1"})
    assert [c for c in env.calls if c[1] == "/appointments"][0][2]["json"] == {"slotId": SLOT_A}


@pytest.mark.parametrize("bad", ["abc", "0", "-3", "5; drop"])
def test_tampered_dependent_id_never_reaches_the_api(env, bad):
    login_patient(env)
    env.client.post("/patient/book", data={"slotId": SLOT_A, "key": "k" * 32, "dependentId": bad})
    assert not [c for c in env.calls if c[1] == "/appointments"]


def test_family_page_lists_members_and_guards_removal(env):
    login_patient(env)
    env.route("GET", "/patients/me/dependents", FakeResp(200, FAMILY))
    page = env.client.get("/patient/family").get_data(as_text=True)
    assert "Lakshmi O&#39;Neil" in page and "Mother" in page and "2 upcoming appointments" in page
    assert 'action="/patient/family/5/remove"' in page and 'action="/patient/family/6/remove"' not in page
    assert "confirm(&#34;Remove Lakshmi from your family members?&#34;)" in page or \
        'confirm("Remove Lakshmi from your family members?")' in page          # JSON-encoded, safe inside the script
    assert 'action="/patient/family"' in page


def test_family_add_and_remove_call_the_api(env):
    login_patient(env)
    env.route("POST", "/patients/me/dependents", FakeResp(201, {"dependent": FAMILY["items"][0]}))
    env.route("DELETE", "/patients/me/dependents/5", FakeResp(200, {"message": "Family member removed"}))
    env.route("GET", "/patients/me/dependents", FakeResp(200, FAMILY))
    r = env.client.post("/patient/family", data={"firstName": "Lakshmi", "lastName": "Rao", "relation": "Mother",
                                                 "dateOfBirth": "1965-04-02", "gender": ""})
    assert r.headers["Location"].endswith("/patient/family")
    sent = [c for c in env.calls if c[0] == "POST" and c[1] == "/patients/me/dependents"][0][2]["json"]
    assert sent == {"firstName": "Lakshmi", "lastName": "Rao", "relation": "Mother", "dateOfBirth": "1965-04-02"}
    assert "Lakshmi was added" in env.client.get("/patient/family").get_data(as_text=True)
    env.client.post("/patient/family/5/remove")
    assert "Family member removed" in env.client.get("/patient/family").get_data(as_text=True)


def test_family_add_needs_mandatory_fields_and_keeps_the_input(env):
    login_patient(env)
    env.route("GET", "/patients/me/dependents", FakeResp(200, {"items": []}))
    env.client.post("/patient/family", data={"firstName": "Lakshmi", "lastName": "", "relation": "Mother", "dateOfBirth": ""})
    assert not [c for c in env.calls if c[0] == "POST" and c[1] == "/patients/me/dependents"]
    page = env.client.get("/patient/family").get_data(as_text=True)
    assert "Please fill in: last name, date of birth." in page and 'value="Lakshmi"' in page


def test_family_remove_refused_shows_the_reason(env):
    login_patient(env)
    env.route("DELETE", "/patients/me/dependents/6", FakeResp(409, {"message": "Anu still has 2 upcoming appointments. Cancel them first."}))
    env.route("GET", "/patients/me/dependents", FakeResp(200, FAMILY))
    env.client.post("/patient/family/6/remove")
    assert "Cancel them first" in env.client.get("/patient/family").get_data(as_text=True)


def test_appointment_cards_say_who_they_are_for(env):
    login_patient(env)
    a = appt()
    a["bookedFor"] = {"type": "DEPENDENT", "dependentId": 5, "name": "Lakshmi Rao", "relation": "Mother"}
    env.route("GET", "/appointments", FakeResp(200, {"items": [appt(i=10), a], "page": 1, "total": 2, "totalPages": 1,
                                                     "hasPrevious": False, "hasNext": False}))
    page = env.client.get("/patient/appointments").get_data(as_text=True)
    assert "For <b>you</b>" in page and "For <b>Lakshmi Rao (Mother)</b>" in page


# ==============================================================================================================
# Wiring - single SQL file, API Gateway routes, IAM, SSM, CI packaging
# ==============================================================================================================
# Milestone 5 wiring: the three copies of the booking tables agree, every booking route exists in API Gateway and
# is in the deployment, the Lambda is packaged by CI and deploy, and the IAM role stays least-privilege.

ROOT = Path(__file__).resolve().parents[1]
M5_INDEXES = {
    "uq_appointments_idempotency_key": ("appointments", ("idempotency_key",)),
    "uq_appointments_active_patient_slot": ("appointments", ("active_patient_slot",)),
    "ix_appointments_patient_start": ("appointments", ("patient_id", "start_at")),
    "ix_appointments_doctor_start": ("appointments", ("doctor_id", "start_at")),
    "ix_appointments_slot_status": ("appointments", ("slot_id", "status")),
    "ix_appointments_patient_doctor_status_start": ("appointments", ("patient_id", "doctor_id", "status", "start_at")),
    "ix_appointments_doctor_type": ("appointments", ("doctor_id", "consultation_type_id")),
    "ix_appointments_dependent": ("appointments", ("dependent_id",)),
    "ix_dependents_patient_active": ("patient_dependents", ("patient_id", "is_active")),
    "uq_refund_ledger_appointment": ("refund_ledger", ("appointment_id",)),
}


def table_ddl(text, name):
    m = re.search(rf"CREATE TABLE IF NOT EXISTS {name} \((.*?)\n\s*\) ENGINE=InnoDB", text, re.S)
    assert m, f"{name} missing"
    return [" ".join(line.split()) for line in m.group(1).splitlines() if line.strip()]


def indexes(text):
    found, table = {}, None
    for line in text.splitlines():
        m = re.search(r"CREATE TABLE\s+(?:IF NOT EXISTS\s+)?(\w+)", line)
        if m:
            table = m.group(1)
        m = re.match(r"\s*(?:UNIQUE\s+)?KEY\s+(\w+)\s*\(([^)]*)\)", line)
        if m and table:
            found[m.group(1)] = (table, tuple(c.strip() for c in m.group(2).split(",")))
    return found


def test_single_sql_file_and_rds_check_define_identical_booking_tables():
    assert sorted(p.name for p in (ROOT / "scehma").glob("*.sql")) == ["schema.sql"]       # one SQL file only
    schema = (ROOT / "scehma" / "schema.sql").read_text()
    rds = (ROOT / "lambdas" / "rds_check" / "lambda_function.py").read_text()
    for table in ("patient_dependents", "appointments", "refund_ledger"):
        assert table_ddl(schema, table) == table_ddl(rds, table), table
    for name, expected in M5_INDEXES.items():
        assert indexes(schema).get(name) == expected, name
    # foreign-key order: parents first
    order = [schema.index(f"CREATE TABLE IF NOT EXISTS {t}") for t in ("patients", "patient_dependents", "appointments", "refund_ledger")]
    assert order == sorted(order)


def test_rds_check_requires_the_tables_and_reports_the_indexes():
    rds = (ROOT / "lambdas" / "rds_check" / "lambda_function.py").read_text()
    required = re.search(r"REQUIRED_TABLES = \{(.*?)\}", rds, re.S).group(1)
    assert '"appointments"' in required and '"refund_ledger"' in required and '"patient_dependents"' in required
    report = re.search(r"REQUIRED_INDEXES\s*=\s*\{(.*?)\n\}", rds, re.S).group(1)
    for name in M5_INDEXES:
        if name != "ix_appointments_doctor_type":
            assert f'"{name}"' in report, name


def test_appointments_never_cascade_delete():
    schema = (ROOT / "scehma" / "schema.sql").read_text()
    m5 = schema[schema.index("CREATE TABLE IF NOT EXISTS patient_dependents"):schema.index("-- Idempotent reference data")]
    assert "CASCADE" not in m5 and m5.count("ON DELETE RESTRICT") == 6


class _Loader(yaml.SafeLoader):
    pass


_Loader.add_multi_constructor("!", lambda loader, suffix, node: (
    loader.construct_scalar(node) if isinstance(node, yaml.ScalarNode)
    else loader.construct_sequence(node) if isinstance(node, yaml.SequenceNode) else loader.construct_mapping(node)))


def stack(name):
    return yaml.load((ROOT / "cloudformation" / name).read_text(), Loader=_Loader)["Resources"]


def full_path(res, rid):
    parts = []
    while rid in res:
        parts.append(res[rid]["Properties"]["PathPart"])
        rid = res[rid]["Properties"]["ParentId"]
    return "/" + "/".join(reversed(parts))


def test_every_booking_route_is_an_api_method_wired_to_the_booking_lambda_and_deployed():
    r = stack("auth-stack.yaml")
    resources = {k: v for k, v in r.items() if v["Type"] == "AWS::ApiGateway::Resource"}
    methods = {(full_path(resources, v["Properties"]["ResourceId"]), v["Properties"]["HttpMethod"]): k
               for k, v in r.items() if v["Type"] == "AWS::ApiGateway::Method"}
    wanted = [("/appointments", "POST"), ("/appointments", "GET"), ("/appointments/{appointmentId}", "GET"),
              ("/appointments/{appointmentId}/cancel", "POST"), ("/doctors/me/slots/{slotId}/cancel", "POST"),
              ("/patients/me/refunds", "GET"), ("/patients/me/dependents", "GET"), ("/patients/me/dependents", "POST"),
              ("/patients/me/dependents/{dependentId}", "PATCH"), ("/patients/me/dependents/{dependentId}", "DELETE")]
    deployments = [k for k, v in r.items() if v["Type"] == "AWS::ApiGateway::Deployment"]
    assert deployments == ["ApiDeploymentM5Family"]
    assert r["ApiStage"]["Properties"]["DeploymentId"] == "ApiDeploymentM5Family"
    for route in wanted:
        name = methods.get(route)
        assert name, route
        m = r[name]["Properties"]
        assert m["AuthorizationType"] == "CUSTOM" and "BookingFunction.Arn" in m["Integration"]["Uri"], route
        assert name in r["ApiDeploymentM5Family"]["DependsOn"], route
    # every method that existed before is still deployed
    assert all(k in r["ApiDeploymentM5Family"]["DependsOn"] for k, v in r.items() if v["Type"] == "AWS::ApiGateway::Method")


def test_booking_role_is_least_privilege():
    r = stack("auth-stack.yaml")
    statements = r["BookingRole"]["Properties"]["Policies"][0]["PolicyDocument"]["Statement"]
    by_resource = {}
    for s in statements:
        for res in (s["Resource"] if isinstance(s["Resource"], list) else [s["Resource"]]):
            by_resource.setdefault(str(res), set()).update(s["Action"])
    slots = next(v for k, v in by_resource.items() if k.endswith("-Slots"))
    assert slots == {"dynamodb:GetItem", "dynamodb:UpdateItem"}                    # never Put/Delete a slot
    assert not any("jwt-secret" in k for k in by_resource)                          # no signing key access
    assert any(a == "events:PutEvents" for v in by_resource.values() for a in v)


def test_booking_settings_exist_in_ssm_with_a_valid_default_policy():
    import json
    d = stack("data-stack.yaml")
    names = {v["Properties"]["Name"]: v["Properties"]["Value"] for v in d.values() if v["Type"] == "AWS::SSM::Parameter"}
    policy = names["/medislot/${Environment}/booking/refund-policy"]
    assert booking.parse_refund_policy(policy) == booking.parse_refund_policy(booking.DEFAULT_REFUND_POLICY)
    assert json.loads(policy)["tiers"][-1]["minHoursBefore"] == 0
    for key in ("max-active-appointments", "max-per-doctor-per-day", "max-conflict-retries"):
        assert int(names[f"/medislot/${{Environment}}/booking/{key}"]) >= 1


def test_booking_lambda_is_packaged_everywhere():
    deploy = (ROOT / ".github" / "workflows" / "deploy.yaml").read_text()
    build = (ROOT / "scripts" / "build-lambda.sh").read_text()
    auth = (ROOT / "cloudformation" / "auth-stack.yaml").read_text()
    assert re.search(r"for fn in [^;]*\bbooking\b", deploy) and re.search(r'FUNCTIONS="[^"]*\bbooking\b', build)
    assert "booking.zip" in auth and (ROOT / "lambdas" / "booking" / "requirements.txt").exists()
    ci = (ROOT / ".github" / "workflows" / "ci.yaml").read_text()
    for workflow in (deploy, ci):
        assert 'tests/test_milestone5.py -k "mandatory1 or mandatory2"' in workflow
    names = re.findall(r"^def (test_mandatory[12]_\w+)", Path(__file__).read_text(), re.M)
    assert sum(n.startswith("test_mandatory1_") for n in names) >= 4 and sum(n.startswith("test_mandatory2_") for n in names) >= 10
