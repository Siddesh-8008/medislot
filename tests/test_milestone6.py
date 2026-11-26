"""MediSlot Milestone 6 - every offline test in ONE file.

Sections (search for the banner lines):
  * Shared test support      - a generic DynamoDB fake (every table, GSIs, transactions, Query), a Step Functions
                               interpreter that runs the REAL state machine from cloudformation/auth-stack.yaml, and an
                               EventBridge pump that routes events with the REAL rule patterns from the same template
  * MANDATORY TEST 3         - reschedule saga: forced failure during release -> the patient holds exactly ONE
                               appointment (pytest -k mandatory3)
  * MANDATORY TEST 4         - waitlist exclusivity: one freed seat, three waitlisted patients -> only one live offer at
                               any moment (pytest -k mandatory4)
  * Reschedule edge cases, Waitlist + offers, Notifications (exactly once), Reminders + no-show sweeper, Wiring
"""
import io
import json
import re
import threading
import uuid
from concurrent.futures import ThreadPoolExecutor
from datetime import datetime, timedelta, timezone
from pathlib import Path
from zoneinfo import ZoneInfo

import pymysql
import pytest
import yaml
from botocore.exceptions import ClientError

from support import api_event, load_lambda
from common import slot_key
from test_milestone5 import World, _Expr, _Loader, _apply_update, booking, iso_z, slot_id_for, to_av, to_py

ROOT = Path(__file__).resolve().parents[1]
UTC = timezone.utc
IST = ZoneInfo("Asia/Kolkata")
waitlist = load_lambda("waitlist", "waitlist_under_test")
notifications = load_lambda("notifications", "notifications_under_test")
reminders = load_lambda("reminders", "reminders_under_test")
SM_ARN = "arn:aws:states:ap-south-1:111122223333:stateMachine:medislot-dev-reschedule-saga"


def stack(name):
    return yaml.load((ROOT / "cloudformation" / name).read_text(), Loader=_Loader)["Resources"]


AUTH = stack("auth-stack.yaml")
DEFINITION = AUTH["RescheduleStateMachine"]["Properties"]["Definition"]


# ==============================================================================================================
# Shared test support
# ==============================================================================================================
TABLES = {   # table suffix -> (partition key, sort key, {index: (partition key, sort key)})
    "Slots": ("doctor_id", "slot_key", {}),
    "Idempotency": ("idempotency_key", None, {}),
    "WaitlistEntries": ("waitlist_key", "entry_key", {"GSI_Patient": ("patient_id", "joined_at")}),
    "Offers": ("offer_id", None, {"GSI_Status": ("status", "expires_at")}),
    "NotificationLog": ("dispatch_key", None, {}),
}
_KC = re.compile(r"\s*(\S+)\s*=\s*(:\w+)\s*(?:AND\s+(\S+)\s+(?:BETWEEN\s+(:\w+)\s+AND\s+(:\w+)|(<=|<|>=|>|=)\s*(:\w+)))?\s*")


def ccf(op):
    return ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "The conditional request failed"}}, op)


class Dynamo:
    """Thread-safe DynamoDB low-level client covering what the Lambdas use. Conditions and update expressions are
    EVALUATED (a wrong condition fails a test), transactions are atomic, GSIs are sparse and eventually... no:
    they are read straight from the table (the code never relies on GSI freshness for correctness)."""

    def __init__(self):
        self.tables = {t: {} for t in TABLES}
        self.lock = threading.RLock()
        self.watchers = []           # called after every successful write, under the lock (invariant checks)
        self.fail = []               # [(predicate(op, table, spec) -> exception or None)]
        self.writes = 0

    def _t(self, name):
        for suffix in TABLES:
            if name.endswith("-" + suffix):
                return suffix
        raise AssertionError(f"unexpected table {name}")

    def _key(self, t, key):
        pk, sk, _ = TABLES[t]
        assert set(key) == ({pk, sk} if sk else {pk}), f"{t} key must be {pk}/{sk}, got {sorted(key)}"
        return (to_py(key[pk]), to_py(key[sk]) if sk else None)

    def _injected(self, op, t, spec):
        for rule in list(self.fail):
            exc = rule(op, t, spec)
            if exc:
                raise exc

    @staticmethod
    def _ok(item, cond, names, values):
        if not cond:
            return True
        return _Expr(item or {}, names, {k: to_py(v) for k, v in (values or {}).items()}).evaluate(cond)

    def _write_done(self):
        self.writes += 1
        for w in self.watchers:
            w(self)

    def _updated(self, t, key, current, spec):
        values = {k: to_py(v) for k, v in (spec.get("ExpressionAttributeValues") or {}).items()}
        pk, sk, _ = TABLES[t]
        base = current or {pk: key[0], **({sk: key[1]} if sk else {})}
        return _apply_update(base, spec["UpdateExpression"], spec.get("ExpressionAttributeNames"), values)

    # ---------------------------------------------------------------- API
    def get_item(self, TableName, Key, ConsistentRead=False):
        t = self._t(TableName)
        self._injected("get_item", t, {"Key": Key})
        with self.lock:
            item = self.tables[t].get(self._key(t, Key))
            return {"Item": {k: to_av(v) for k, v in item.items()}} if item else {}

    def put_item(self, TableName, Item, ConditionExpression=None, ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        t = self._t(TableName)
        self._injected("put_item", t, {"Item": Item})
        pk, sk, _ = TABLES[t]
        item = {k: to_py(v) for k, v in Item.items()}
        k = (item[pk], item[sk] if sk else None)
        with self.lock:
            if not self._ok(self.tables[t].get(k), ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues):
                raise ccf("PutItem")
            self.tables[t][k] = item
            self._write_done()
        return {}

    def update_item(self, TableName, Key, UpdateExpression, ConditionExpression=None, ExpressionAttributeNames=None,
                    ExpressionAttributeValues=None, ReturnValues=None):
        t = self._t(TableName)
        spec = {"Key": Key, "UpdateExpression": UpdateExpression, "ConditionExpression": ConditionExpression,
                "ExpressionAttributeNames": ExpressionAttributeNames, "ExpressionAttributeValues": ExpressionAttributeValues}
        self._injected("update_item", t, spec)
        k = self._key(t, Key)
        with self.lock:
            current = self.tables[t].get(k)
            if not self._ok(current, ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues):
                raise ccf("UpdateItem")
            new = self._updated(t, k, current, spec)
            self.tables[t][k] = new
            self._write_done()
        return {"Attributes": {kk: to_av(v) for kk, v in new.items()}} if ReturnValues == "ALL_NEW" else {}

    def delete_item(self, TableName, Key, ConditionExpression=None, ExpressionAttributeNames=None, ExpressionAttributeValues=None):
        t = self._t(TableName)
        self._injected("delete_item", t, {"Key": Key})
        k = self._key(t, Key)
        with self.lock:
            if not self._ok(self.tables[t].get(k), ConditionExpression, ExpressionAttributeNames, ExpressionAttributeValues):
                raise ccf("DeleteItem")
            self.tables[t].pop(k, None)
            self._write_done()
        return {}

    def transact_write_items(self, TransactItems):
        assert len(TransactItems) <= 100
        for entry in TransactItems:
            (op, spec), = entry.items()
            self._injected("transact", self._t(spec["TableName"]), spec)
        with self.lock:
            reasons, plans = [], []
            for entry in TransactItems:
                (op, spec), = entry.items()
                assert op in ("Update", "Put"), op
                t = self._t(spec["TableName"])
                if op == "Put":
                    pk, sk, _ = TABLES[t]
                    item = {k: to_py(v) for k, v in spec["Item"].items()}
                    k = (item[pk], item[sk] if sk else None)
                else:
                    k = self._key(t, spec["Key"])
                assert (t, k) not in [(p[0], p[1]) for p in plans], "one transaction cannot touch the same item twice"
                current = self.tables[t].get(k)
                ok = self._ok(current, spec.get("ConditionExpression"), spec.get("ExpressionAttributeNames"),
                              spec.get("ExpressionAttributeValues"))
                reasons.append({"Code": "None" if ok else "ConditionalCheckFailed"})
                plans.append((t, k, current, op, spec))
            if any(r["Code"] != "None" for r in reasons):
                raise ClientError({"Error": {"Code": "TransactionCanceledException", "Message": "Transaction cancelled"},
                                   "CancellationReasons": reasons}, "TransactWriteItems")
            for t, k, current, op, spec in plans:
                self.tables[t][k] = ({kk: to_py(v) for kk, v in spec["Item"].items()} if op == "Put"
                                     else self._updated(t, k, current, spec))
            self._write_done()
        return {}

    def query(self, TableName, KeyConditionExpression, ExpressionAttributeValues, ExpressionAttributeNames=None, IndexName=None,
              ConsistentRead=False, ScanIndexForward=True, Limit=None, ExclusiveStartKey=None, FilterExpression=None):
        t = self._t(TableName)
        assert not (IndexName and ConsistentRead), "a GSI cannot be read with ConsistentRead"
        pk, sk, gsis = TABLES[t]
        kpk, ksk = gsis[IndexName] if IndexName else (pk, sk)
        m = _KC.fullmatch(KeyConditionExpression)
        assert m, KeyConditionExpression
        names = ExpressionAttributeNames or {}
        name = lambda n: names.get(n, n)                                               # noqa: E731
        vals = {k: to_py(v) for k, v in ExpressionAttributeValues.items()}
        assert name(m.group(1)) == kpk, (m.group(1), kpk)
        with self.lock:
            hits = [dict(i) for i in self.tables[t].values() if kpk in i and (not ksk or ksk in i) and i[kpk] == vals[m.group(2)]]
        if m.group(3):
            assert name(m.group(3)) == ksk
            if m.group(4):
                hits = [i for i in hits if vals[m.group(4)] <= i[ksk] <= vals[m.group(5)]]
            else:
                op, v = m.group(6), vals[m.group(7)]
                hits = [i for i in hits if {"<=": i[ksk] <= v, "<": i[ksk] < v, ">=": i[ksk] >= v, ">": i[ksk] > v, "=": i[ksk] == v}[op]]
        hits.sort(key=lambda i: (i.get(ksk) if ksk else 0, str(i.get(sk)) if sk else ""), reverse=not ScanIndexForward)
        if FilterExpression:
            hits = [i for i in hits if _Expr(i, names, vals).evaluate(FilterExpression)]
        return {"Items": [{k: to_av(v) for k, v in i.items()} for i in hits]}

    # ---------------------------------------------------------------- helpers
    def slot(self, slot_id):
        with self.lock:
            return dict(next(i for i in self.tables["Slots"].values() if i["slot_id"] == slot_id))

    def offers(self, status=None, slot_id=None):
        with self.lock:
            return [dict(o) for o in self.tables["Offers"].values()
                    if (status is None or o["status"] == status) and (slot_id is None or o["slot_id"] == slot_id)]

    def entries(self, status=None):
        with self.lock:
            return [dict(e) for e in self.tables["WaitlistEntries"].values() if e["entry_key"] != "SUMMARY"
                    and (status is None or e.get("status") == status)]


# --------------------------------------------------------------------------- Step Functions interpreter
def jpath(data, path):
    if path == "$":
        return data
    assert path.startswith("$."), path
    cur = data
    for part in path[2:].split("."):
        if not isinstance(cur, dict) or part not in cur:
            raise KeyError(f"States.Runtime: {path} not found")
        cur = cur[part]
    return cur


def resolve(params, data):
    if isinstance(params, dict):
        return {(k[:-2] if k.endswith(".$") else k): (jpath(data, v) if k.endswith(".$") else resolve(v, data))
                for k, v in params.items()}
    return params


def put_path(data, path, value):
    if path == "$":
        return value
    out = dict(data)
    out[path[2:]] = value
    return out


def matches(errors, name):
    return "States.ALL" in errors or name in errors


class Sfn:
    """Runs the REAL Amazon States Language definition: Task (Parameters, ResultPath, Retry, Catch), Choice, Pass,
    Succeed, Fail. Tasks call booking.handler exactly as Step Functions would invoke the Lambda."""

    def __init__(self, definition, invoke):
        self.defn, self.invoke = definition, invoke
        self.executions, self.lock = {}, threading.Lock()
        self.manual = False            # True: start_execution only records; tests call run() themselves
        self.before = {}               # state name -> callback(state) run just before that state executes
        self.start_error = None

    def start_execution(self, stateMachineArn, name, input):
        assert stateMachineArn == SM_ARN and re.fullmatch(r"[A-Za-z0-9_-]{1,80}", name), name
        if self.start_error:
            raise self.start_error
        with self.lock:
            ex = self.executions.get(name)
            if ex:
                if ex["input"] == input and ex["status"] == "RUNNING":
                    return {"executionArn": SM_ARN.replace(":stateMachine:", ":execution:") + ":" + name}
                raise ClientError({"Error": {"Code": "ExecutionAlreadyExists", "Message": "exists"}}, "StartExecution")
            ex = self.executions[name] = {"input": input, "status": "RUNNING", "history": [], "retries": [], "error": None}
        if not self.manual:
            self.run(name)
        return {"executionArn": SM_ARN.replace(":stateMachine:", ":execution:") + ":" + name}

    def describe_execution(self, executionArn):
        return {"status": self.executions[executionArn.rsplit(":", 1)[1]]["status"]}

    def run(self, name):
        ex = self.executions[name]
        data, cur = json.loads(ex["input"]), self.defn["StartAt"]
        for _ in range(200):
            st = self.defn["States"][cur]
            ex["history"].append(cur)
            if cur in self.before:
                self.before[cur](data)
            kind = st["Type"]
            if kind == "Succeed":
                ex.update(status="SUCCEEDED", output=data)
                return ex
            if kind == "Fail":
                ex.update(status="FAILED", error=st["Error"], output=data)
                return ex
            if kind == "Pass":
                data = put_path(data, st.get("ResultPath", "$"), st.get("Result", data))
                cur = st["Next"]
                continue
            if kind == "Choice":
                nxt = st.get("Default")
                for c in st["Choices"]:
                    value = jpath(data, c["Variable"])
                    if ("BooleanEquals" in c and value is c["BooleanEquals"]) or ("StringEquals" in c and value == c["StringEquals"]):
                        nxt = c["Next"]
                        break
                assert nxt, f"no Choice matched in {cur}"
                cur = nxt
                continue
            assert kind == "Task", kind
            params, used, caught = resolve(st.get("Parameters", {}), data), {}, None
            while True:
                try:
                    result = json.loads(json.dumps(self.invoke(st["Resource"], params)))   # results must be JSON
                    break
                except Exception as exc:  # noqa: BLE001
                    err = type(exc).__name__
                    idx = next((i for i, r in enumerate(st.get("Retry", [])) if matches(r["ErrorEquals"], err)), None)
                    if idx is not None and used.get(idx, 0) < st["Retry"][idx].get("MaxAttempts", 3):
                        used[idx] = used.get(idx, 0) + 1
                        ex["retries"].append((cur, err))
                        continue
                    caught = next((c for c in st.get("Catch", []) if matches(c["ErrorEquals"], err)), None)
                    if not caught:
                        ex.update(status="FAILED", error=err)
                        return ex
                    data = put_path(data, caught.get("ResultPath", "$"), {"Error": err, "Cause": str(exc)})
                    break
            if caught:
                cur = caught["Next"]
                continue
            data = put_path(data, st.get("ResultPath", "$"), result)
            cur = st["Next"]
        raise AssertionError("state machine did not terminate")

    def only(self):
        assert len(self.executions) == 1, list(self.executions)
        return next(iter(self.executions.values()))


# --------------------------------------------------------------------------- EventBridge rules -> SQS -> consumers
def rule_matches(pattern, event):
    for key, want in pattern.items():
        have = event.get(key)
        if isinstance(want, dict):
            if not isinstance(have, dict) or not rule_matches(want, have):
                return False
            continue
        ok = False
        for w in want:
            if isinstance(w, dict) and "prefix" in w:
                ok = ok or (isinstance(have, str) and have.startswith(w["prefix"]))
            else:
                ok = ok or have == w
        if not ok:
            return False
    return True


RULES = [(v["Properties"]["EventPattern"], t["Arn"]) for v in AUTH.values() if v["Type"] == "AWS::Events::Rule"
         for t in v["Properties"]["Targets"] if "Queue" in str(t["Arn"])]


class FakeSns:
    def __init__(self):
        self.published, self.fail_times, self.lock = [], 0, threading.Lock()

    def publish(self, TopicArn, Subject, Message, MessageAttributes):
        with self.lock:
            if self.fail_times:
                self.fail_times -= 1
                raise ClientError({"Error": {"Code": "InternalError", "Message": "sns down"}}, "Publish")
            assert len(Subject) <= 100
            self.published.append({"subject": Subject, "message": Message, "attrs": MessageAttributes, "topic": TopicArn})
            return {"MessageId": uuid.uuid4().hex}

    def kinds(self):
        return [p["attrs"]["eventType"]["StringValue"] for p in self.published]


class World6(World):
    def __init__(self, monkeypatch):
        super().__init__(monkeypatch)
        self.settings.update(max_reschedules=2, reschedule_cutoff_hours=6, fault_injection="")
        self.wl_settings = {"hold_minutes": 15, "max_active": 5, "max_per_doctor_day": 1}
        self.dynamo, self.sns = Dynamo(), FakeSns()
        self.sfn = Sfn(DEFINITION, lambda resource, payload: booking.handler(payload, None))
        self.pumped = 0
        self.queues = {"notifications": [], "waitlist": []}
        clock = lambda: self.now or datetime.now(UTC)                                    # noqa: E731
        monkeypatch.setattr(booking, "ddb", self.dynamo)
        monkeypatch.setattr(booking, "sfn", self.sfn)
        monkeypatch.setenv("RESCHEDULE_STATE_MACHINE_ARN", SM_ARN)
        for name, value in (("ddb", self.dynamo), ("events", self.events), ("dbconn", self.db.connect),
                            ("db_config", lambda: {"env": "dev"}), ("metric", self._metric), ("sleep", lambda s: None),
                            ("utcnow", clock), ("load_settings", lambda cid=None: dict(self.wl_settings))):
            monkeypatch.setattr(waitlist, name, value)
        for name, value in (("ddb", self.dynamo), ("sns", self.sns), ("metric", self._metric)):
            monkeypatch.setattr(notifications, name, value)
        for name, value in (("events", self.events), ("dbconn", self.db.connect), ("db_config", lambda: {"env": "dev"}),
                            ("metric", self._metric), ("utcnow", clock)):
            monkeypatch.setattr(reminders, name, value)
        monkeypatch.setattr(reminders.time, "sleep", lambda s: None)
        monkeypatch.setenv("PATIENT_TOPIC_ARN", "arn:aws:sns:ap-south-1:111122223333:medislot-dev-patient-notifications")

    def slot(self, doctor_id, type_id=1, start=None, minutes=15, capacity=1, status="OPEN", booked=0, held=0, version=1):
        start = start or (datetime.now(UTC) + timedelta(days=3)).replace(hour=5, minute=30, second=0, microsecond=0)
        sid = slot_id_for(doctor_id, type_id, start)
        item = {"slot_id": sid, "doctor_id": doctor_id, "slot_key": slot_key(iso_z(start), type_id), "consultation_type_id": type_id,
                "start_at": iso_z(start), "end_at": iso_z(start + timedelta(minutes=minutes)), "timezone": "Asia/Kolkata",
                "capacity": capacity, "booked_count": booked, "held_count": held, "version": version, "status": status,
                "local_date": start.astimezone(IST).date().isoformat()}
        self.dynamo.tables["Slots"][(doctor_id, item["slot_key"])] = item
        return sid

    def pump(self, deliver=True, duplicate=1):
        """Route every new event through the template's rules into the two queues, then deliver the queues."""
        while self.pumped < len(self.events.entries):
            e = self.events.entries[self.pumped]
            self.pumped += 1
            event = {"id": uuid.uuid4().hex, "source": e["Source"], "detail-type": e["DetailType"], "detail": json.loads(e["Detail"])}
            for pattern, arn in RULES:
                if rule_matches(pattern, event):
                    q = "notifications" if "Notifications" in str(arn) else "waitlist"
                    self.queues[q] += [{"messageId": uuid.uuid4().hex, "body": json.dumps(event)}] * duplicate
        if deliver:
            self.deliver()

    def deliver(self):
        for _ in range(20):
            if not (self.queues["waitlist"] or self.queues["notifications"]):
                return
            for q, module in (("waitlist", waitlist), ("notifications", notifications)):
                batch, self.queues[q] = self.queues[q], []
                if batch:
                    out = module.handler({"Records": batch}, None)
                    failed = {f["itemIdentifier"] for f in out["batchItemFailures"]}
                    self.queues[q] += [r for r in batch if r["messageId"] in failed]
            self.pump(deliver=False)

    def kinds_published(self):
        return [e["DetailType"] for e in self.events.entries]


@pytest.fixture
def w(monkeypatch):
    world = World6(monkeypatch)
    world.doc = world.doctor("Ravi", "Kumar", types=((1, 300.0, 1), (4, 500.0, 3)))
    world.doc2 = world.doctor("Meena", "Rao", types=((1, 400.0, 1),))
    world.p1, world.p2, world.p3, world.p4 = (world.patient(i) for i in range(1, 5))
    return world


def api(module, method, path, user, role="PATIENT", body=None, headers=None, path_params=None, query=None):
    ev = api_event(method, path, user_id=user, role=role, body=body, query=query)
    ev["headers"].update(headers or {})
    if path_params:
        ev["pathParameters"] = path_params
    res = module.handler(ev, None)
    return res["statusCode"], json.loads(res["body"]), res.get("headers") or {}


def book(w, pid, slot, dep=None):
    body = {"slotId": slot, **({"dependentId": dep} if dep else {})}
    st, data, _ = api(booking, "POST", "/appointments", pid, body=body, headers={"Idempotency-Key": uuid.uuid4().hex})
    assert st == 201, data
    return data["appointment"]["appointmentId"]


def reschedule(w, pid, appointment_id, target, key=None, fault=None):
    headers = {"Idempotency-Key": key or uuid.uuid4().hex}
    if fault:
        headers["X-Fault-Injection"] = fault
    return api(booking, "POST", f"/appointments/{appointment_id}/reschedule", pid, body={"targetSlotId": target}, headers=headers)


def day_at(days, hour_utc=5, minute=30):
    base = datetime.now(UTC).replace(hour=hour_utc, minute=minute, second=0, microsecond=0)
    return base + timedelta(days=days)


def seats(w, slot):
    s = w.dynamo.slot(slot)
    return s["booked_count"], s["held_count"]


def appts_of(w, pid):
    return w.db.rows("SELECT * FROM appointments WHERE patient_id = ? ORDER BY appointment_id", (pid,))


# ==============================================================================================================
# MANDATORY TEST 3 - reschedule saga with a forced failure while releasing the original seat
# ==============================================================================================================
# The saga claims the target seat, moves the appointment, then fails to release the original seat (forced). The
# compensation branch must move the appointment back and give the target seat back: the patient ends with exactly ONE
# appointment, on the original slot, and every seat counter is exactly what it was before.

def _assert_untouched(w, appointment_id, original, target):
    rows = appts_of(w, w.p1)
    assert len(rows) == 1, rows                                                  # exactly one appointment ...
    assert rows[0]["appointment_id"] == appointment_id and rows[0]["status"] == "CONFIRMED"
    assert rows[0]["slot_id"] == original and rows[0]["reschedule_count"] == 0  # ... still on the original slot
    assert seats(w, original) == (1, 0) and seats(w, target) == (0, 0)           # both seat counters restored


def test_mandatory3_forced_failure_during_release_leaves_exactly_one_appointment(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    st, data, _ = reschedule(w, w.p1, a, target, fault="release-original")
    assert st == 409 and data["code"] == "RESCHEDULE_COMPENSATED" and data["failedStep"] == "RELEASE_ORIGINAL"
    assert data["appointment"]["slotId"] == original
    _assert_untouched(w, a, original, target)
    ex = w.sfn.only()
    assert ex["status"] == "FAILED" and ex["error"] == "RescheduleCompensated"
    hist = ex["history"]
    for state in ("ClaimTargetSeat", "MoveAppointment", "ReleaseOriginalSeat", "ReleaseFailed", "CompensateRevertMove",
                  "CompensateReleaseTargetSeat", "RecordCompensated", "Compensated"):
        assert state in hist, (state, hist)
    assert "Finalize" not in hist
    assert hist.index("MoveAppointment") < hist.index("ReleaseOriginalSeat") < hist.index("CompensateRevertMove")
    assert ex["retries"].count(("ReleaseOriginalSeat", "SeatReleaseError")) == 2           # retried before compensating
    assert w.metric_sum("ReschedulesCompensated") == 1 and w.metric_sum("ReschedulesSucceeded") == 0
    assert "AppointmentRescheduled" not in w.kinds_published()
    # the lock is gone: the patient can cancel straight away
    st, data, _ = api(booking, "POST", f"/appointments/{a}/cancel", w.p1, body={})
    assert st == 200 and seats(w, original) == (0, 0)


def test_mandatory3_fault_injected_through_ssm_parameter(w):
    w.settings["fault_injection"] = "release-original"
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    st, data, _ = reschedule(w, w.p1, a, target)
    assert st == 409 and data["code"] == "RESCHEDULE_COMPENSATED"
    _assert_untouched(w, a, original, target)


def test_mandatory3_real_dynamodb_failure_on_the_original_slot_is_compensated(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    orig_key = w.dynamo.slot(original)["slot_key"]

    def boom(op, table, spec):        # every write to the ORIGINAL slot during the saga's release fails
        if op == "transact" and table == "Slots" and to_py(spec["Key"]["slot_key"]) == orig_key and "booked_count - :one" in spec["UpdateExpression"]:
            return ClientError({"Error": {"Code": "InternalServerError", "Message": "injected"}}, "TransactWriteItems")
    w.dynamo.fail.append(boom)
    st, data, _ = reschedule(w, w.p1, a, target)
    assert st == 409 and data["code"] == "RESCHEDULE_COMPENSATED"
    _assert_untouched(w, a, original, target)
    assert w.metric_sum("ReschedulesCompensated") == 1


def test_mandatory3_failure_while_moving_is_compensated(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    st, data, _ = reschedule(w, w.p1, a, target, fault="move")
    assert st == 409 and data["code"] == "RESCHEDULE_COMPENSATED" and data["failedStep"] == "MOVE_APPOINTMENT"
    _assert_untouched(w, a, original, target)


def test_mandatory3_rds_outage_during_the_move_is_retried_then_compensated(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    w.db.fail_on = "UPDATE appointments SET slot_id"
    st, data, _ = reschedule(w, w.p1, a, target)
    w.db.fail_on = None
    assert st == 409 and data["code"] == "RESCHEDULE_COMPENSATED"
    assert w.sfn.only()["retries"].count(("MoveAppointment", "OperationalError")) == 3
    _assert_untouched(w, a, original, target)


def test_mandatory3_release_that_succeeded_before_a_crash_is_not_released_twice(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    real = booking.saga_release_original
    calls = []

    def crash_after_release(state, cid):
        out = real(state, cid)
        calls.append(out)
        if len(calls) == 1:
            raise TimeoutError("Lambda timed out after the transaction committed")
        return out
    booking.SAGA_STEPS["rescheduleReleaseOriginal"] = crash_after_release
    try:
        st, data, _ = reschedule(w, w.p1, a, target)
    finally:
        booking.SAGA_STEPS["rescheduleReleaseOriginal"] = real
    # The retry is caught by Catch (TimeoutError is not in the step's Retry list) -> RevertMove sees the original seat
    # already released and goes FORWARD to Finalize instead of moving the appointment back onto a released seat.
    assert st == 200 and data["appointment"]["slotId"] == target
    assert seats(w, original) == (0, 0) and seats(w, target) == (1, 0)
    rows = appts_of(w, w.p1)
    assert len(rows) == 1 and rows[0]["slot_id"] == target and rows[0]["reschedule_count"] == 1


def test_mandatory3_ambiguous_commit_during_the_move_is_safe(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    real_connect, state = w.db.connect, {"done": False}

    class Flaky:
        def __init__(self, conn):
            self.conn = conn

        def __enter__(self):
            self.conn.__enter__()
            return self

        def __exit__(self, *exc):
            return self.conn.__exit__(*exc)

        def cursor(self):
            return self.conn.cursor()

        def rollback(self):
            self.conn.rollback()

        def commit(self):
            self.conn.commit()
            if not state["done"] and any("UPDATE appointments SET slot_id" in q for q, _ in w.db.log[-3:]):
                state["done"] = True
                raise pymysql.err.OperationalError(2013, "Lost connection during COMMIT (it did commit)")
    booking.dbconn = lambda cfg=None: Flaky(real_connect())
    try:
        st, data, _ = reschedule(w, w.p1, a, target)
    finally:
        booking.dbconn = w.db.connect
    assert st == 200 and state["done"]                       # the retried move found its own committed row
    rows = appts_of(w, w.p1)
    assert len(rows) == 1 and rows[0]["slot_id"] == target and rows[0]["reschedule_count"] == 1
    assert seats(w, original) == (0, 0) and seats(w, target) == (1, 0)


def test_mandatory3_two_parallel_reschedules_of_one_appointment(w):
    original = w.slot(w.doc, start=day_at(3))
    t1, t2 = w.slot(w.doc, start=day_at(4)), w.slot(w.doc, start=day_at(5))
    a = book(w, w.p1, original)
    barrier = threading.Barrier(2)
    w.sfn.before["Validate"] = lambda data: barrier.wait(timeout=5)
    with ThreadPoolExecutor(2) as pool:
        results = list(pool.map(lambda t: reschedule(w, w.p1, a, t)[:2], (t1, t2)))
    codes = sorted(st for st, _ in results)
    assert codes[0] == 200 and codes[1] == 409, results
    rows = appts_of(w, w.p1)
    assert len(rows) == 1 and rows[0]["slot_id"] in (t1, t2)
    total = sum(seats(w, s)[0] for s in (original, t1, t2))
    assert total == 1 and seats(w, rows[0]["slot_id"]) == (1, 0)


# ==============================================================================================================
# MANDATORY TEST 4 - waitlist exclusivity: one freed seat, three waitlisted patients
# ==============================================================================================================
def join(w, pid, doctor, day, dep=None):
    body = {"doctorId": doctor, "date": day, **({"dependentId": dep} if dep else {})}
    return api(waitlist, "POST", "/waitlists", pid, body=body)


def live_offer_watcher(slot_id, holder):
    """Invariant checked after EVERY DynamoDB write: never more than one live offer for the one freed seat, and the
    slot's held_count always equals the number of LIVE offers (an ACCEPTING offer has already turned its hold into a
    booked seat, in the same transaction)."""
    def check(d):
        offers = [o for o in d.tables["Offers"].values() if o["slot_id"] == slot_id]
        live = [o for o in offers if o["status"] == "LIVE"]
        slot = next(i for i in d.tables["Slots"].values() if i["slot_id"] == slot_id)
        assert len(live) <= 1, live
        assert slot["held_count"] == len(live), (slot["held_count"], live)
        assert slot["booked_count"] + slot["held_count"] <= slot["capacity"]
        holder["max_live"] = max(holder.get("max_live", 0), len(live))
        holder["checks"] = holder.get("checks", 0) + 1
    return check


def test_mandatory4_one_freed_seat_three_waitlisted_only_one_live_offer(w):
    w.now = datetime.now(UTC).replace(microsecond=0)
    start = day_at(3)
    slot = w.slot(w.doc, start=start)
    day = start.astimezone(IST).date().isoformat()
    p0 = w.patient(9)
    a0 = book(w, p0, slot)
    for i, pid in enumerate((w.p1, w.p2, w.p3)):
        w.now += timedelta(seconds=1)                        # a clear order of arrival
        st, data, _ = join(w, pid, w.doc, day)
        assert st == 201 and data["waitlist"]["position"] == i + 1, data
    seen = {}
    w.dynamo.watchers.append(live_offer_watcher(slot, seen))

    assert api(booking, "POST", f"/appointments/{a0}/cancel", p0, body={})[0] == 200
    w.pump(deliver=False, duplicate=3)                      # the SAME SlotSeatFreed delivered three times ...
    batch = w.queues["waitlist"]
    w.queues["waitlist"] = []
    with ThreadPoolExecutor(3) as pool:                       # ... concurrently
        list(pool.map(lambda r: waitlist.handler({"Records": [r]}, None), batch))
    w.deliver()
    live = w.dynamo.offers("LIVE")
    assert len(live) == 1 and live[0]["patient_id"] == str(w.p1) and seats(w, slot) == (0, 1)
    # held = invisible: nobody else can book it
    st, data, _ = api(booking, "POST", "/appointments", w.p4, body={"slotId": slot}, headers={"Idempotency-Key": uuid.uuid4().hex})
    assert st == 409 and data["code"] == "SLOT_FULL"

    expected = [str(w.p2), str(w.p3)]
    for nxt in expected + [None]:                            # each hold expires; two sweepers race every time
        w.now += timedelta(minutes=16)
        with ThreadPoolExecutor(2) as pool:
            list(pool.map(lambda _: waitlist.handler({"action": "sweep"}, None), range(2)))
        w.pump()
        live = w.dynamo.offers("LIVE")
        if nxt:
            assert len(live) == 1 and live[0]["patient_id"] == nxt and seats(w, slot) == (0, 1)
        else:
            assert live == [] and seats(w, slot) == (0, 0)  # nobody left in line: the seat is released
    assert seen["max_live"] == 1 and seen["checks"] > 10
    assert w.metric_sum("WaitlistPromotions") == 3 and w.metric_sum("OffersExpired") == 3
    assert sorted(o["patient_id"] for o in w.dynamo.offers("EXPIRED")) == sorted([str(w.p1), str(w.p2), str(w.p3)])
    assert w.sns.kinds().count("HoldOfferIssued") == 3 and w.sns.kinds().count("HoldOfferExpired") == 3
    assert book(w, w.p4, slot)                               # and now it is bookable again


def test_mandatory4_accepting_turns_the_hold_into_one_confirmed_appointment(w):
    start = day_at(3)
    slot = w.slot(w.doc, start=start)
    day = start.astimezone(IST).date().isoformat()
    p0 = w.patient(9)
    a0 = book(w, p0, slot)
    join(w, w.p1, w.doc, day)
    join(w, w.p2, w.doc, day)
    seen = {}
    w.dynamo.watchers.append(live_offer_watcher(slot, seen))
    api(booking, "POST", f"/appointments/{a0}/cancel", p0, body={})
    w.pump()
    (offer,) = w.dynamo.offers("LIVE")
    with ThreadPoolExecutor(3) as pool:                       # a triple click on "Accept"
        results = list(pool.map(lambda _: api(booking, "POST", f"/offers/{offer['offer_id']}/accept", w.p1)[:2], range(3)))
    assert sorted(st for st, _ in results)[0] in (200, 201) and all(st in (200, 201, 409) for st, _ in results), results
    assert len([r for r in appts_of(w, w.p1) if r["status"] == "CONFIRMED"]) == 1
    assert seats(w, slot) == (1, 0) and w.dynamo.offers("LIVE") == [] and w.dynamo.offers("ACCEPTED")[0]["patient_id"] == str(w.p1)
    assert [e["status"] for e in w.dynamo.entries() if e["patient_id"] == str(w.p1)] == ["BOOKED"]
    assert w.metric_sum("OffersAccepted") == 1
    w.pump()
    assert "AppointmentBooked" in w.sns.kinds()
    st, data, _ = api(booking, "POST", f"/offers/{offer['offer_id']}/accept", w.p1)
    assert st == 200 and data["alreadyAccepted"]


def test_mandatory4_three_seats_freed_at_once_go_to_three_different_patients(w):
    start = day_at(3)
    slot = w.slot(w.doc, type_id=4, start=start, minutes=60, capacity=3)
    day = start.astimezone(IST).date().isoformat()
    holders = [w.patient(10 + i) for i in range(3)]
    booked = [book(w, p, slot) for p in holders]
    waiting = [w.p1, w.p2, w.p3, w.p4]
    for pid in waiting:
        assert join(w, pid, w.doc, day)[0] == 201
    with ThreadPoolExecutor(3) as pool:
        list(pool.map(lambda pa: api(booking, "POST", f"/appointments/{pa[1]}/cancel", pa[0], body={}), zip(holders, booked)))
    w.pump(deliver=False, duplicate=2)
    batch = w.queues["waitlist"]
    w.queues["waitlist"] = []
    with ThreadPoolExecutor(6) as pool:
        list(pool.map(lambda r: waitlist.handler({"Records": [r]}, None), batch))
    live = w.dynamo.offers("LIVE")
    assert len(live) == 3 and len({o["patient_id"] for o in live}) == 3
    assert {o["patient_id"] for o in live} == {str(p) for p in waiting[:3]}           # first come, first served
    assert seats(w, slot) == (0, 3)


# ==============================================================================================================
# Reschedule edge cases
# ==============================================================================================================
def test_reschedule_happy_path_moves_the_one_appointment_and_frees_the_old_seat(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    st, data, _ = reschedule(w, w.p1, a, target)
    assert st == 200 and data["appointment"]["slotId"] == target and data["appointment"]["rescheduleCount"] == 1
    assert data["previous"]["slotId"] == original and data["appointment"]["appointmentId"] == a
    rows = appts_of(w, w.p1)
    assert len(rows) == 1 and rows[0]["slot_id"] == target
    assert seats(w, original) == (0, 0) and seats(w, target) == (1, 0)
    assert w.sfn.only()["status"] == "SUCCEEDED"
    types = w.kinds_published()
    assert "AppointmentRescheduled" in types and "SlotSeatFreed" in types
    detail = next(json.loads(e["Detail"]) for e in w.events.entries if e["DetailType"] == "AppointmentRescheduled")
    assert detail["previousSlotId"] == original and detail["slotId"] == target and "Pat" not in json.dumps(detail)
    assert w.metric_sum("ReschedulesSucceeded") == 1
    assert booking.reschedule_lock_owner(a) is None


def test_reschedule_attempt_limit(w):
    slots = [w.slot(w.doc, start=day_at(3 + i)) for i in range(4)]
    a = book(w, w.p1, slots[0])
    assert reschedule(w, w.p1, a, slots[1])[0] == 200
    assert reschedule(w, w.p1, a, slots[2])[0] == 200
    st, data, _ = reschedule(w, w.p1, a, slots[3])
    assert st == 409 and data["code"] == "RESCHEDULE_LIMIT_REACHED"
    assert seats(w, slots[3]) == (0, 0) and appts_of(w, w.p1)[0]["reschedule_count"] == 2


@pytest.mark.parametrize("hours_before,ok", [(timedelta(hours=6), True), (timedelta(hours=6) - timedelta(microseconds=1), False),
                                             (timedelta(hours=1), False), (timedelta(days=2), True)])
def test_six_hour_cutoff(w, hours_before, ok):
    w.now = datetime.now(UTC).replace(microsecond=0)
    start = w.now + timedelta(days=1)
    original, target = w.slot(w.doc, start=start), w.slot(w.doc, start=start + timedelta(days=1))
    a = book(w, w.p1, original)
    w.now = start - hours_before
    st, data, _ = reschedule(w, w.p1, a, target)
    assert (st == 200) is ok, data
    if not ok:
        assert data["code"] == "RESCHEDULE_CUTOFF" and seats(w, target) == (0, 0)


def test_reschedule_rejections(w):
    original = w.slot(w.doc, start=day_at(3))
    a = book(w, w.p1, original)
    other_doc = w.slot(w.doc2, start=day_at(4))
    other_type = w.slot(w.doc, type_id=4, start=day_at(4, 7), minutes=60, capacity=3)
    full = w.slot(w.doc, start=day_at(5), booked=1)
    closed = w.slot(w.doc, start=day_at(6), status="CLOSED")
    past = w.slot(w.doc, start=datetime.now(UTC) - timedelta(hours=1))
    for target, status, code in ((other_doc, 422, "DIFFERENT_DOCTOR"), (other_type, 422, "DIFFERENT_TYPE"),
                                 (original, 422, "SAME_SLOT"), (full, 409, "SLOT_FULL"), (closed, 409, "SLOT_CLOSED"),
                                 (past, 422, "SLOT_IN_PAST"),
                                 (slot_id_for(w.doc, 1, day_at(9)), 404, "SLOT_NOT_FOUND")):
        st, data, _ = reschedule(w, w.p1, a, target)
        assert (st, data["code"]) == (status, code), (target, data)
    assert reschedule(w, w.p2, a, w.slot(w.doc, start=day_at(7)))[1]["code"] == "APPOINTMENT_NOT_FOUND"     # not yours
    assert all(seats(w, s)[0] == 0 for s in (other_doc, other_type, closed))
    assert appts_of(w, w.p1)[0]["slot_id"] == original and w.metric_sum("ReschedulesRejected") >= 7


def test_reschedule_request_validation(w):
    a = book(w, w.p1, w.slot(w.doc, start=day_at(3)))
    t = w.slot(w.doc, start=day_at(4))
    path = f"/appointments/{a}/reschedule"
    assert api(booking, "POST", path, w.p1, body={"targetSlotId": t})[1]["code"] == "IDEMPOTENCY_KEY_REQUIRED"
    assert api(booking, "POST", path, w.p1, body={"targetSlotId": "x"}, headers={"Idempotency-Key": "k" * 10})[1]["code"] == "SLOT_ID_INVALID"
    assert api(booking, "POST", path, w.p1, body={"targetSlotId": t, "fee": 0}, headers={"Idempotency-Key": "k" * 10})[1]["code"] == "UNKNOWN_FIELDS"
    assert api(booking, "POST", path, w.doc, role="DOCTOR", body={"targetSlotId": t}, headers={"Idempotency-Key": "k" * 10})[0] == 403
    assert api(booking, "GET", path, w.p1)[0] == 405
    assert w.sfn.executions == {}


def test_reschedule_is_idempotent_per_key(w):
    original, target, other = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4)), w.slot(w.doc, start=day_at(5))
    a = book(w, w.p1, original)
    first = reschedule(w, w.p1, a, target, key="resched-key-1")
    again = reschedule(w, w.p1, a, target, key="resched-key-1")
    assert first[0] == again[0] == 200 and again[2].get("Idempotent-Replayed") == "true"
    assert len(w.sfn.executions) == 1 and appts_of(w, w.p1)[0]["reschedule_count"] == 1
    st, data, _ = reschedule(w, w.p1, a, other, key="resched-key-1")
    assert st == 422 and data["code"] == "IDEMPOTENCY_KEY_REUSED"


def test_reschedule_answers_202_while_the_saga_is_still_running_then_the_result(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    w.sfn.manual = True
    booking.SAGA_WAIT_SECONDS, saved = 0, booking.SAGA_WAIT_SECONDS
    try:
        st, data, headers = reschedule(w, w.p1, a, target, key="slow-saga-key")
        assert st == 202 and data["status"] == "IN_PROGRESS" and headers.get("Retry-After")
        w.sfn.run(next(iter(w.sfn.executions)))
        st, data, _ = reschedule(w, w.p1, a, target, key="slow-saga-key")
    finally:
        booking.SAGA_WAIT_SECONDS = saved
    assert st == 200 and data["appointment"]["slotId"] == target and len(w.sfn.executions) == 1


def test_reschedule_start_failure_frees_the_key(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    w.sfn.start_error = ClientError({"Error": {"Code": "ServiceUnavailable", "Message": "down"}}, "StartExecution")
    assert reschedule(w, w.p1, a, target, key="flaky-start-key")[0] == 503
    w.sfn.start_error = None
    assert reschedule(w, w.p1, a, target, key="flaky-start-key")[0] == 200


def test_patient_cannot_cancel_while_a_reschedule_holds_the_lock(w):
    a = book(w, w.p1, w.slot(w.doc, start=day_at(3)))
    assert booking.acquire_reschedule_lock(a, "r-someone")
    st, data, headers = api(booking, "POST", f"/appointments/{a}/cancel", w.p1, body={})
    assert st == 409 and data["code"] == "RESCHEDULE_IN_PROGRESS" and headers.get("Retry-After")
    w.dynamo.tables["Idempotency"][(booking.lock_key(a), None)]["lease_expires_at"] = 0      # lease ran out
    assert api(booking, "POST", f"/appointments/{a}/cancel", w.p1, body={})[0] == 200


def test_doctor_cancels_the_appointment_before_the_move_compensates(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    w.sfn.before["MoveAppointment"] = lambda data: api(booking, "POST", f"/appointments/{a}/status", w.doc, role="DOCTOR",
                                                       body={"status": "CANCELLED_BY_DOCTOR"})
    st, data, _ = reschedule(w, w.p1, a, target)
    assert st == 409 and data["code"] == "RESCHEDULE_COMPENSATED"
    rows = appts_of(w, w.p1)
    assert len(rows) == 1 and rows[0]["status"] == "CANCELLED_BY_DOCTOR"
    assert seats(w, original) == (0, 0) and seats(w, target) == (0, 0)


def test_doctor_cancels_the_new_slot_during_the_saga_refunds_in_full(w):
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    a = book(w, w.p1, original)
    ref = target[5:]
    w.sfn.before["Finalize"] = lambda data: api(booking, "POST", f"/doctors/me/slots/{ref}/cancel", w.doc, role="DOCTOR", body={})
    st, data, _ = reschedule(w, w.p1, a, target)
    assert st == 409 and data["code"] == "APPOINTMENT_CANCELLED" and "refunded in full" in data["message"]
    assert data["appointment"]["refund"]["refundPercent"] == 100
    rows = appts_of(w, w.p1)
    assert len(rows) == 1 and rows[0]["status"] == "CANCELLED_BY_DOCTOR"
    assert w.db.rows("SELECT refund_percentage FROM refund_ledger")[0]["refund_percentage"] == 100
    assert seats(w, original) == (0, 0)


def test_doctor_cancels_the_moved_appointment_then_release_fails(w):
    """The nastiest ordering: moved -> the doctor cancels it (its NEW seat is released by that cancel) -> releasing
    the old seat fails. Compensation must NOT release the new seat a second time."""
    original, target = w.slot(w.doc, start=day_at(3)), w.slot(w.doc, start=day_at(4))
    other = w.patient(20)
    a = book(w, w.p1, original)
    w.sfn.before["ReleaseOriginalSeat"] = lambda data: (
        data.get("_once") or api(booking, "POST", f"/appointments/{a}/status", w.doc, role="DOCTOR", body={"status": "CANCELLED_BY_DOCTOR"}),
        data.__setitem__("_once", True))
    st, data, _ = reschedule(w, w.p1, a, target, fault="release-original")
    assert st == 409
    assert seats(w, target)[0] == 0
    book(w, other, target)                                 # the new slot really has its seat back, exactly once
    assert seats(w, target) == (1, 0)


def test_state_machine_definition_is_complete():
    states = DEFINITION["States"]
    for name, st in states.items():
        for nxt in [st.get("Next"), st.get("Default")] + [c["Next"] for c in st.get("Choices", []) + st.get("Catch", [])]:
            assert nxt is None or nxt in states, (name, nxt)
        if st["Type"] == "Task":
            assert st["Resource"] == "${BookingFunctionArn}" and st["Parameters"]["action"] in booking.SAGA_STEPS
            assert any("States.ALL" in c["ErrorEquals"] for c in st.get("Catch", [])), name
        assert st["Type"] in ("Succeed", "Fail") or "Next" in st or st["Type"] == "Choice", name
    assert AUTH["RescheduleStateMachine"]["Properties"]["StateMachineType"] == "STANDARD"


# ==============================================================================================================
# Waitlist and offers
# ==============================================================================================================
@pytest.fixture
def full_day(w):
    """One fully booked slot on a day three days ahead (11:00 IST) and its waitlist day."""
    start = day_at(3)
    w.full_slot = w.slot(w.doc, start=start)
    w.day = start.astimezone(IST).date().isoformat()
    w.holder = w.patient(9)
    w.holder_appt = book(w, w.holder, w.full_slot)
    return w


def free_the_seat(w):
    assert api(booking, "POST", f"/appointments/{w.holder_appt}/cancel", w.holder, body={})[0] == 200
    w.pump()


def test_join_rules(full_day):
    w = full_day
    st, data, _ = join(w, w.p1, w.doc, w.day)
    assert st == 201 and data["waitlist"]["waitlistId"] == f"{w.doc}_{w.day}_0" and data["waitlist"]["position"] == 1
    assert join(w, w.p1, w.doc, w.day)[1]["code"] == "ALREADY_ON_WAITLIST"
    free_day = (day_at(4)).astimezone(IST).date().isoformat()
    w.slot(w.doc, start=day_at(4))
    assert join(w, w.p2, w.doc, free_day)[1]["code"] == "SLOTS_AVAILABLE"
    assert join(w, w.p2, w.doc, day_at(5).astimezone(IST).date().isoformat())[1]["code"] == "NO_SLOTS_THAT_DAY"
    assert join(w, w.p2, w.doc, (datetime.now(IST) - timedelta(days=1)).date().isoformat())[1]["code"] == "DATE_IN_PAST"
    assert join(w, w.p2, w.doc, (datetime.now(IST) + timedelta(days=60)).date().isoformat())[1]["code"] == "DATE_TOO_FAR"
    assert join(w, w.holder, w.doc, w.day)[1]["code"] == "ALREADY_BOOKED_THAT_DAY"
    assert join(w, w.p2, 999, w.day)[0] == 404
    assert join(w, w.p2, w.doc, w.day, dep=12345)[1]["code"] == "DEPENDENT_NOT_FOUND"
    for bad in ({"doctorId": "1", "date": w.day}, {"doctorId": w.doc, "date": "2026-02-30"}, {"doctorId": w.doc, "date": w.day, "x": 1}):
        assert api(waitlist, "POST", "/waitlists", w.p2, body=bad)[0] == 422
    assert api(waitlist, "POST", "/waitlists", w.doc, role="DOCTOR", body={"doctorId": w.doc, "date": w.day})[0] == 403
    with w.db.lock:
        w.db.db.execute("INSERT INTO blackout_dates (doctor_id, blackout_date) VALUES (?, ?)", (w.doc, w.day))
        w.db.db.commit()
    assert join(w, w.p3, w.doc, w.day)[1]["code"] == "DOCTOR_UNAVAILABLE"


def test_a_family_member_waits_separately(full_day):
    w = full_day
    mom = w.dependent(w.p1)
    assert join(w, w.p1, w.doc, w.day)[0] == 201
    st, data, _ = join(w, w.p1, w.doc, w.day, dep=mom)
    assert st == 201 and data["waitlist"]["waitlistId"].endswith(f"_{mom}")


def test_at_most_five_waitlists_and_the_count_heals_itself(w):
    days = []
    for i in range(6):
        start = day_at(3 + i)
        w.slot(w.doc, start=start, booked=1)
        days.append(start.astimezone(IST).date().isoformat())
    for d in days[:5]:
        assert join(w, w.p1, w.doc, d)[0] == 201
    st, data, _ = join(w, w.p1, w.doc, days[5])
    assert st == 409 and data["code"] == "WAITLIST_LIMIT_REACHED" and data["maxActive"] == 5
    assert api(waitlist, "DELETE", f"/waitlists/{w.doc}_{days[0]}_0", w.p1)[0] == 200
    assert join(w, w.p1, w.doc, days[5])[0] == 201


def test_parallel_joins_never_exceed_the_cap(w):
    days = []
    for i in range(8):
        start = day_at(3 + i)
        w.slot(w.doc, start=start, booked=1)
        days.append(start.astimezone(IST).date().isoformat())
    with ThreadPoolExecutor(8) as pool:
        results = list(pool.map(lambda d: join(w, w.p1, w.doc, d)[0], days))
    assert results.count(201) == 5 and results.count(409) == 3
    assert len([e for e in w.dynamo.entries("ACTIVE") if e["patient_id"] == str(w.p1)]) == 5


def test_decline_and_leave_pass_the_seat_on(full_day):
    w = full_day
    for pid in (w.p1, w.p2, w.p3):
        join(w, pid, w.doc, w.day)
    free_the_seat(w)
    (o1,) = w.dynamo.offers("LIVE")
    st, data, _ = api(waitlist, "POST", f"/offers/{o1['offer_id']}/decline", w.p1)
    assert st == 200 and "next person" in data["message"]
    (o2,) = w.dynamo.offers("LIVE")
    assert o2["patient_id"] == str(w.p2) and seats(w, w.full_slot) == (0, 1)
    assert api(waitlist, "POST", f"/offers/{o1['offer_id']}/decline", w.p1)[1]["alreadyEnded"]
    assert api(waitlist, "POST", f"/offers/{o2['offer_id']}/decline", w.p1)[0] == 404        # not yours
    assert api(waitlist, "DELETE", f"/waitlists/{w.doc}_{w.day}_0", w.p2)[0] == 200           # leave while offered
    (o3,) = w.dynamo.offers("LIVE")
    assert o3["patient_id"] == str(w.p3)
    assert api(waitlist, "DELETE", f"/waitlists/{w.doc}_{w.day}_0", w.p4)[0] == 404
    assert w.metric_sum("OffersDeclined") == 2 and w.metric_sum("OffersExpired") == 0


def test_offer_expiry_releases_when_nobody_is_left(full_day):
    w = full_day
    w.now = datetime.now(UTC)
    join(w, w.p1, w.doc, w.day)
    free_the_seat(w)
    (o,) = w.dynamo.offers("LIVE")
    assert api(booking, "POST", f"/offers/{o['offer_id']}/accept", w.p2)[0] == 404             # someone else's
    w.now += timedelta(minutes=16)
    assert api(booking, "POST", f"/offers/{o['offer_id']}/accept", w.p1)[1]["code"] == "OFFER_EXPIRED"
    out = waitlist.handler({"action": "sweep"}, None)
    assert out["expired"] == 1 and out["released"] == 1 and seats(w, w.full_slot) == (0, 0)
    assert api(booking, "POST", f"/offers/{o['offer_id']}/accept", w.p1)[1]["code"] == "OFFER_EXPIRED"
    assert waitlist.handler({"action": "sweep"}, None)["expired"] == 0                            # nothing left to do


def test_accept_is_refused_by_the_booking_rules_and_the_seat_stays_held(full_day):
    w = full_day
    join(w, w.p1, w.doc, w.day)
    other = w.slot(w.doc, type_id=4, start=day_at(3, 3), minutes=60, capacity=3)          # same day, same doctor
    free_the_seat(w)
    book(w, w.p1, other)                                                                       # now at the daily limit
    (o,) = w.dynamo.offers("LIVE")
    st, data, _ = api(booking, "POST", f"/offers/{o['offer_id']}/accept", w.p1)
    assert st == 409 and data["code"] == "DOCTOR_DAILY_LIMIT_REACHED" and "held" in data["message"]
    assert seats(w, w.full_slot) == (0, 1) and w.dynamo.offers("LIVE")[0]["offer_id"] == o["offer_id"]


def test_accept_survives_an_rds_failure(full_day):
    w = full_day
    join(w, w.p1, w.doc, w.day)
    free_the_seat(w)
    (o,) = w.dynamo.offers("LIVE")
    w.db.fail_on = "INSERT INTO appointments"
    assert api(booking, "POST", f"/offers/{o['offer_id']}/accept", w.p1)[0] == 500
    w.db.fail_on = None
    assert seats(w, w.full_slot) == (0, 1) and w.dynamo.offers("LIVE")
    assert api(booking, "POST", f"/offers/{o['offer_id']}/accept", w.p1)[0] == 201
    assert seats(w, w.full_slot) == (1, 0)


@pytest.mark.parametrize("committed", [False, True])
def test_sweeper_resolves_an_offer_stuck_in_accepting(full_day, committed):
    w = full_day
    w.now = datetime.now(UTC)
    join(w, w.p1, w.doc, w.day)
    free_the_seat(w)
    (o,) = w.dynamo.offers("LIVE")
    real = booking.patient_ready

    def die(cur, uid):
        if committed:
            return real(cur, uid)
        raise MemoryError("Lambda killed mid-request")
    if not committed:
        booking.patient_ready = die
        booking.revert_accept, saved = (lambda *a, **k: None), booking.revert_accept     # the process is gone: no revert
        try:
            with pytest.raises(MemoryError):
                booking.accept_offer(o["offer_id"], w.p1, "cid")
        finally:
            booking.patient_ready, booking.revert_accept = real, saved
    else:
        w.dynamo.fail.append(lambda op, t, spec: ClientError({"Error": {"Code": "InternalServerError", "Message": "x"}}, "UpdateItem")
                             if op == "update_item" and t == "Offers" and "ACCEPTED" in json.dumps(spec.get("ExpressionAttributeValues")) else None)
        assert api(booking, "POST", f"/offers/{o['offer_id']}/accept", w.p1)[0] == 201
        w.dynamo.fail.clear()
    assert w.dynamo.offers("ACCEPTING") and seats(w, w.full_slot) == (1, 0)
    w.now += timedelta(minutes=2)                                                              # lease over
    assert waitlist.handler({"action": "sweep"}, None)["resolved"] == 1
    if committed:
        assert w.dynamo.offers("ACCEPTED") and seats(w, w.full_slot) == (1, 0)
    else:
        assert w.dynamo.offers("LIVE") and seats(w, w.full_slot) == (0, 1)


def test_doctor_cancels_the_slot_under_a_live_offer(full_day):
    w = full_day
    join(w, w.p1, w.doc, w.day)
    join(w, w.p2, w.doc, w.day)
    free_the_seat(w)
    (o,) = w.dynamo.offers("LIVE")
    api(booking, "POST", f"/doctors/me/slots/{w.full_slot[5:]}/cancel", w.doc, role="DOCTOR", body={})
    w.pump()
    assert w.dynamo.offers("LIVE") == [] and w.dynamo.offers("VOID")[0]["offer_id"] == o["offer_id"]
    entries = {e["patient_id"]: e for e in w.dynamo.entries()}
    assert entries[str(w.p1)]["status"] == "ACTIVE" and "offer_id" not in entries[str(w.p1)]           # back in line
    queue = waitlist.active_queue(w.doc, w.day)
    assert [e["patient_id"] for e in queue] == [str(w.p1), str(w.p2)]                                    # ... at the front
    assert seats(w, w.full_slot)[1] == 0
    assert api(booking, "POST", f"/offers/{o['offer_id']}/accept", w.p1)[0] == 410


def test_blackout_closes_the_whole_days_waitlist(full_day):
    w = full_day
    join(w, w.p1, w.doc, w.day)
    out = waitlist.on_slot_cancelled({"slotId": w.full_slot, "reason": "blackout"}, "cid")
    assert out["closed"] == 1 and w.dynamo.entries("CLOSED")


def test_no_offer_when_the_slot_starts_too_soon(w):
    w.now = datetime.now(UTC).replace(microsecond=0)
    start = w.now + timedelta(minutes=30)
    slot = w.slot(w.doc, start=start)
    day = start.astimezone(IST).date().isoformat()
    holder = w.patient(9)
    a = book(w, holder, slot)
    assert join(w, w.p1, w.doc, day)[0] == 201
    w.now = start - timedelta(minutes=6)
    api(booking, "POST", f"/appointments/{a}/cancel", holder, body={})
    w.pump()
    assert w.dynamo.offers() == [] and seats(w, slot) == (0, 0)


def test_listing_shows_my_place_and_my_offer(full_day):
    w = full_day
    join(w, w.p1, w.doc, w.day)
    join(w, w.p2, w.doc, w.day)
    st, data, _ = api(waitlist, "GET", "/waitlists", w.p2)
    assert st == 200 and data["items"][0]["position"] == 2 and data["maxActive"] == 5 and data["offers"] == []
    free_the_seat(w)
    data = api(waitlist, "GET", "/waitlists", w.p1)[1]
    (offer,) = data["offers"]
    assert offer["secondsLeft"] > 0 and offer["doctorName"] == "Dr. Ravi Kumar" and data["items"][0]["status"] == "OFFERED"
    assert api(waitlist, "GET", "/waitlists", w.p2)[1]["items"][0]["position"] == 1


def test_bad_offer_ids_and_methods(w):
    assert api(booking, "POST", "/offers/nope/accept", w.p1)[0] == 422
    assert api(waitlist, "POST", "/offers/nope/decline", w.p1)[0] == 422
    assert api(booking, "POST", "/offers/OFR-" + "0" * 32 + "/accept", w.p1)[0] == 404
    assert api(booking, "POST", "/offers/OFR-" + "0" * 32 + "/accept", w.doc, role="DOCTOR")[0] == 403
    assert api(waitlist, "PUT", "/waitlists", w.p1)[0] == 405
    assert api(waitlist, "DELETE", "/waitlists/bad", w.p1)[0] == 422


def test_poison_messages_are_retried_then_dead_lettered(w):
    out = waitlist.handler({"Records": [{"messageId": "m1", "body": "not json"}, {"messageId": "m2", "body": json.dumps(
        {"detail-type": "Other", "detail": {}})}]}, None)
    assert out["batchItemFailures"] == [{"itemIdentifier": "m1"}]


# ==============================================================================================================
# Notifications - EventBridge -> SQS -> Lambda -> SNS, exactly once
# ==============================================================================================================
def sqs(detail_type, detail, source="medislot.booking"):
    return {"messageId": uuid.uuid4().hex, "body": json.dumps({"id": uuid.uuid4().hex, "source": source,
                                                               "detail-type": detail_type, "detail": detail})}


NOTIFY = {"doctorName": "Dr. Ravi Kumar", "localDate": "2026-12-01", "localStart": "11:00", "timezone": "Asia/Kolkata",
          "consultationType": "Quick Consult", "holdUntilLocal": "10:15", "previousLocalDate": "2026-11-30", "previousLocalStart": "09:00"}
EIGHT = [("AppointmentBooked", {}, "Appointment confirmed"), ("AppointmentCancelled", {"refundAmount": 300, "refundPercent": 100}, "Appointment cancelled"),
         ("AppointmentRescheduled", {}, "Appointment rescheduled"), ("HoldOfferIssued", {}, "A seat is held for you"),
         ("HoldOfferExpired", {}, "Your held seat has expired"), ("AppointmentReminder24h", {}, "Reminder: appointment in 24 hours"),
         ("AppointmentReminder2h", {}, "Reminder: appointment in 2 hours"), ("AppointmentStatusChanged", {"status": "NO_SHOW"}, "Missed appointment")]


@pytest.mark.parametrize("kind,extra,subject", EIGHT)
def test_each_of_the_eight_events_is_delivered_once(w, kind, extra, subject):
    rec = sqs(kind, {"eventId": uuid.uuid4().hex, "patientId": 7, "appointmentId": 41, "notify": NOTIFY, **extra})
    for _ in range(3):                                        # SQS at-least-once: the same message three times
        assert notifications.handler({"Records": [rec]}, None) == {"batchItemFailures": []}
    assert len(w.sns.published) == 1 and w.sns.published[0]["subject"] == subject
    p = w.sns.published[0]
    assert p["attrs"]["patientId"]["StringValue"] == "7" and "Dr. Ravi Kumar" in p["message"]
    assert w.metric_sum("NotificationsDispatched") == 1 and w.metric_sum("NotificationsDuplicateSuppressed") == 2


def test_eight_rules_cover_exactly_the_eight_events():
    routed = set()
    for kind, extra, _ in EIGHT:
        event = {"source": "medislot.booking", "detail-type": kind, "detail": {"status": "NO_SHOW", **extra}}
        if any(rule_matches(p, event) for p, arn in RULES if "Notifications" in str(arn)):
            routed.add(kind)
    assert routed == {k for k, _, _ in EIGHT}
    completed = {"source": "medislot.booking", "detail-type": "AppointmentStatusChanged", "detail": {"status": "COMPLETED"}}
    assert not any(rule_matches(p, completed) for p, arn in RULES if "Notifications" in str(arn))
    assert notifications.notification_type("AppointmentStatusChanged", {"status": "COMPLETED"}) is None


def test_sns_failure_is_retried_and_still_sent_once(w):
    w.sns.fail_times = 1
    rec = sqs("AppointmentBooked", {"eventId": "e1", "patientId": 7, "notify": NOTIFY})
    assert notifications.handler({"Records": [rec]}, None)["batchItemFailures"] == [{"itemIdentifier": rec["messageId"]}]
    assert w.metric_sum("NotificationFailures") == 1
    assert notifications.handler({"Records": [rec]}, None)["batchItemFailures"] == []
    assert notifications.handler({"Records": [rec]}, None)["batchItemFailures"] == []
    assert len(w.sns.published) == 1


def test_a_live_lease_from_another_attempt_is_not_double_sent(w):
    rec = sqs("AppointmentBooked", {"eventId": "e2", "patientId": 7, "notify": NOTIFY})
    key = "AppointmentBooked#e2"
    w.dynamo.tables["NotificationLog"][(key, None)] = {"dispatch_key": key, "status": "PENDING", "lease_until": 2 ** 40}
    assert notifications.handler({"Records": [rec]}, None)["batchItemFailures"]
    assert w.sns.published == []


def test_bad_records_fail_only_themselves(w):
    good = sqs("AppointmentBooked", {"eventId": "e3", "patientId": 7, "notify": NOTIFY})
    no_patient = sqs("AppointmentBooked", {"eventId": "e4", "notify": NOTIFY})
    junk = {"messageId": "junk", "body": "{"}
    out = notifications.handler({"Records": [good, no_patient, junk]}, None)
    assert {f["itemIdentifier"] for f in out["batchItemFailures"]} == {no_patient["messageId"], "junk"}
    assert len(w.sns.published) == 1


def test_end_to_end_booking_produces_one_notification_without_patient_details(w):
    slot = w.slot(w.doc, start=day_at(3))
    book(w, w.p1, slot)
    w.pump(duplicate=2)
    assert w.sns.kinds() == ["AppointmentBooked"]
    assert "Pat" not in w.sns.published[0]["message"] and "@" not in w.sns.published[0]["message"]


# ==============================================================================================================
# Reminder dispatcher (24 h and 2 h) and no-show sweeper
# ==============================================================================================================
def test_reminders_at_24h_and_2h_and_running_twice_never_notifies_twice(w):
    w.now = datetime.now(UTC).replace(second=0, microsecond=0)
    a24 = book(w, w.p1, w.slot(w.doc, start=w.now + timedelta(hours=23, minutes=40)))
    a2 = book(w, w.p2, w.slot(w.doc2, start=w.now + timedelta(hours=1, minutes=50)))
    later = book(w, w.p3, w.slot(w.doc, start=w.now + timedelta(days=3)))
    cancelled = book(w, w.p4, w.slot(w.doc, start=w.now + timedelta(hours=23, minutes=30)))
    api(booking, "POST", f"/appointments/{cancelled}/cancel", w.p4, body={})
    w.pump()
    w.sns.published.clear()
    first = reminders.handler({"action": "reminders"}, None)
    second = reminders.handler({"action": "reminders"}, None)              # the mentor triggers it twice
    assert first["found"] == second["found"] == {"24H": 1, "2H": 1}
    w.pump(duplicate=2)
    sent = sorted((p["attrs"]["eventType"]["StringValue"], p["message"]) for p in w.sns.published)
    assert [k for k, _ in sent] == ["AppointmentReminder24h", "AppointmentReminder2h"]
    assert w.metric_sum("NotificationsDuplicateSuppressed") >= 6
    keys = {k[0] for k in w.dynamo.tables["NotificationLog"]}
    assert any(k.startswith(f"REMINDER_24H#{a24}#") for k in keys) and any(k.startswith(f"REMINDER_2H#{a2}#") for k in keys)
    assert not any(f"#{later}#" in k or f"#{cancelled}#" in k for k in keys)


def test_a_rescheduled_appointment_is_reminded_again_for_its_new_time(w):
    w.now = datetime.now(UTC).replace(second=0, microsecond=0)
    old = w.slot(w.doc, start=w.now + timedelta(hours=23, minutes=40))
    new = w.slot(w.doc, start=w.now + timedelta(days=2, hours=23, minutes=40))
    a = book(w, w.p1, old)
    reminders.handler({"action": "reminders"}, None)
    assert reschedule(w, w.p1, a, new)[0] == 200
    w.now += timedelta(days=2)
    reminders.handler({"action": "reminders"}, None)
    w.pump()
    assert w.sns.kinds().count("AppointmentReminder24h") == 2


def test_no_show_sweeper(w):
    w.now = datetime.now(UTC).replace(microsecond=0)
    stale = book(w, w.p1, w.slot(w.doc, start=w.now + timedelta(hours=1)))
    recent = book(w, w.p2, w.slot(w.doc2, start=w.now + timedelta(hours=1)))
    done = book(w, w.p3, w.slot(w.doc, start=w.now + timedelta(hours=3)))
    w.now += timedelta(hours=4)
    api(booking, "POST", f"/appointments/{done}/status", w.doc, role="DOCTOR", body={"status": "COMPLETED"})
    with w.db.lock:                                   # the second one ended only an hour ago
        w.db.db.execute("UPDATE appointments SET end_at = ? WHERE appointment_id = ?",
                        ((w.now - timedelta(hours=1)).replace(tzinfo=None), recent))
        w.db.db.commit()
    w.pump()
    w.now += timedelta(hours=10)
    out = reminders.sweep_no_shows("cid", grace=12)
    assert out["marked"] == 1
    status = {r["appointment_id"]: r["status"] for r in w.db.rows("SELECT appointment_id, status FROM appointments")}
    assert status == {stale: "NO_SHOW", recent: "CONFIRMED", done: "COMPLETED"}
    assert reminders.sweep_no_shows("cid", grace=12)["marked"] == 0
    w.pump()
    assert w.sns.kinds().count("AppointmentNoShow") == 1 and w.metric_sum("NoShowsMarked") == 1


# ==============================================================================================================
# Wiring
# ==============================================================================================================
def resources(kind):
    return {k: v for k, v in AUTH.items() if v["Type"] == kind}


def full_path(res, rid):
    parts = []
    while rid in res:
        parts.append(res[rid]["Properties"]["PathPart"])
        rid = res[rid]["Properties"]["ParentId"]
    return "/" + "/".join(reversed(parts))


def test_milestone6_routes_are_wired_and_deployed():
    res = resources("AWS::ApiGateway::Resource")
    methods = {(full_path(res, v["Properties"]["ResourceId"]), v["Properties"]["HttpMethod"]): (k, v["Properties"])
               for k, v in AUTH.items() if v["Type"] == "AWS::ApiGateway::Method"}
    wanted = {("/appointments/{appointmentId}/reschedule", "POST"): "BookingFunction", ("/offers/{offerId}/accept", "POST"): "BookingFunction",
              ("/offers/{offerId}/decline", "POST"): "WaitlistFunction", ("/waitlists", "GET"): "WaitlistFunction",
              ("/waitlists", "POST"): "WaitlistFunction", ("/waitlists/{waitlistId}", "DELETE"): "WaitlistFunction"}
    deployment = AUTH["ApiDeploymentM7"]["DependsOn"]
    for route, fn in wanted.items():
        name, props = methods[route]
        assert props["AuthorizationType"] == "CUSTOM" and f"{fn}.Arn" in props["Integration"]["Uri"], route
        assert name in deployment
    perms = " ".join(str(v["Properties"]["SourceArn"]) for v in resources("AWS::Lambda::Permission").values())
    assert "/*/*/waitlists*" in perms and "/*/POST/offers/*/decline" in perms and "/*/POST/offers/*/accept" in perms


def test_every_consumer_has_a_dlq_and_partial_batch_failures():
    queues = resources("AWS::SQS::Queue")
    for name in ("NotificationsQueue", "WaitlistEventsQueue"):
        rp = queues[name]["Properties"]["RedrivePolicy"]
        assert rp["maxReceiveCount"] == 5 and rp["deadLetterTargetArn"].endswith("DLQ.Arn")
        assert queues[name]["Properties"]["VisibilityTimeout"] >= 6 * 30
    for m in resources("AWS::Lambda::EventSourceMapping").values():
        assert m["Properties"]["FunctionResponseTypes"] == ["ReportBatchItemFailures"]
    assert {s["Properties"]["Target"]["Input"] for s in resources("AWS::Scheduler::Schedule").values() if "Input" in s["Properties"]["Target"]} == \
        {'{"action":"sweep"}', '{"action":"reminders"}', '{"action":"noShowSweep"}'}


def test_new_roles_are_least_privilege():
    for role in ("WaitlistRole", "NotificationsRole", "RemindersRole", "RescheduleStateMachineRole", "BookingRole"):
        for policy in AUTH[role]["Properties"].get("Policies", []):
            for s in policy["PolicyDocument"]["Statement"]:
                actions = s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
                assert not any("*" in a for a in actions), (role, actions)
                if s["Resource"] == "*":
                    assert all(a.startswith("xray:") for a in actions), (role, actions)
                if any(a.startswith("dynamodb:") for a in actions) and "Slots" in str(s["Resource"]):
                    assert not {"dynamodb:PutItem", "dynamodb:DeleteItem"} & set(actions), role
    notif = AUTH["NotificationsFunction"]["Properties"]
    assert "VpcConfig" not in notif                                       # SNS + DynamoDB only, no NAT needed


def test_new_lambdas_are_packaged_and_the_mandatory_tests_run_in_ci():
    deploy = (ROOT / ".github" / "workflows" / "deploy.yaml").read_text()
    build = (ROOT / "scripts" / "build-lambda.sh").read_text()
    ci = (ROOT / ".github" / "workflows" / "ci.yaml").read_text()
    for fn in ("waitlist", "notifications", "reminders"):
        assert re.search(rf"for fn in [^;]*\b{fn}\b", deploy) and re.search(rf'FUNCTIONS="[^"]*\b{fn}\b', build), fn
        assert f"{fn}.zip" in (ROOT / "cloudformation" / "auth-stack.yaml").read_text()
        assert (ROOT / "lambdas" / fn / "requirements.txt").exists()
    for workflow in (deploy, ci):
        assert 'tests/test_milestone6.py -k "mandatory3 or mandatory4"' in workflow
    names = re.findall(r"^def (test_mandatory[34]_\w+)", Path(__file__).read_text(), re.M)
    assert sum(n.startswith("test_mandatory3_") for n in names) >= 5 and sum(n.startswith("test_mandatory4_") for n in names) >= 3


def test_tables_and_settings_exist():
    data = stack("data-stack.yaml")
    tables = {v["Properties"]["TableName"]: v["Properties"] for v in data.values() if v["Type"] == "AWS::DynamoDB::Table"}
    offers = tables["MediSlot-${Environment}-Offers"]
    assert offers["TimeToLiveSpecification"]["AttributeName"] == "purge_at"        # never the hold's expires_at
    assert [g["IndexName"] for g in offers["GlobalSecondaryIndexes"]] == ["GSI_Status"]
    assert tables["MediSlot-${Environment}-WaitlistEntries"]["GlobalSecondaryIndexes"][0]["IndexName"] == "GSI_Patient"
    assert all(t["PointInTimeRecoverySpecification"]["PointInTimeRecoveryEnabled"] for t in tables.values())
    names = {v["Properties"]["Name"] for v in data.values() if v["Type"] == "AWS::SSM::Parameter"}
    for p in ("booking/max-reschedules", "booking/reschedule-cutoff-hours", "booking/no-show-grace-hours",
              "waitlist/hold-minutes", "waitlist/max-active"):
        assert f"/medislot/${{Environment}}/{p}" in names
