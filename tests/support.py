"""Test support (not a test module): run the Lambdas' REAL SQL against an in-memory SQLite database.

The Lambdas talk to MySQL through PyMySQL. For fast, offline tests this module stands in for the connection:
  * `%s` placeholders become `?`
  * `LIKE ?` gets `ESCAPE '\\'` because MySQL treats backslash as the LIKE escape character by default
  * UTC_TIMESTAMP(6) exists as a SQLite function
  * SQLite UNIQUE violations are re-raised as pymysql IntegrityError(1062, "Duplicate entry ...")
  * A missing table is re-raised as pymysql ProgrammingError(1146), as MySQL reports it
So the queries exercised here are the same strings that run in production, not hand-written fakes.
(The MySQL-specific parts - index usage, collation - are reviewed in MILESTONE4.md and on RDS.)
"""
import importlib.util
import json
import os
import re
import sqlite3
import sys
from datetime import datetime, timezone
from pathlib import Path

import pymysql

ROOT = Path(__file__).resolve().parents[1]
os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
os.environ.setdefault("AWS_REGION", "ap-south-1")
if str(ROOT / "lambdas") not in sys.path:
    sys.path.insert(0, str(ROOT / "lambdas"))


def load_lambda(folder, alias):
    """Import lambdas/<folder>/lambda_function.py under a unique module name (every Lambda uses the same file name)."""
    spec = importlib.util.spec_from_file_location(alias, ROOT / "lambdas" / folder / "lambda_function.py")
    module = importlib.util.module_from_spec(spec)
    spec.loader.exec_module(module)
    return module


SCHEMA = """
CREATE TABLE users (user_id INTEGER PRIMARY KEY AUTOINCREMENT, email TEXT NOT NULL UNIQUE, password_hash TEXT NOT NULL,
    role TEXT NOT NULL, phone TEXT UNIQUE, is_active INTEGER NOT NULL DEFAULT 1,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE patients (patient_id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
    date_of_birth TEXT, gender TEXT, address TEXT,
    created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE hospitals (hospital_id INTEGER PRIMARY KEY, hospital_name TEXT NOT NULL UNIQUE, address TEXT NOT NULL,
    city TEXT NOT NULL DEFAULT 'Hyderabad' COLLATE NOCASE, state TEXT NOT NULL DEFAULT 'Telangana', is_active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE specializations (specialization_id INTEGER PRIMARY KEY, specialization_name TEXT NOT NULL UNIQUE,
    is_active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE doctors (doctor_id INTEGER PRIMARY KEY, first_name TEXT NOT NULL, last_name TEXT NOT NULL,
    hospital_id INTEGER NOT NULL, specialization_id INTEGER NOT NULL, license_number TEXT NOT NULL UNIQUE,
    years_experience INTEGER, clinic_address TEXT, qualifications TEXT, languages_spoken TEXT,
    timezone TEXT NOT NULL DEFAULT 'Asia/Kolkata', photo_key TEXT);
CREATE TABLE consultation_types (consultation_type_id INTEGER PRIMARY KEY, name TEXT NOT NULL UNIQUE,
    duration_minutes INTEGER NOT NULL, min_capacity INTEGER NOT NULL DEFAULT 1, max_capacity INTEGER NOT NULL DEFAULT 1,
    is_active INTEGER NOT NULL DEFAULT 1);
CREATE TABLE doctor_consultation_types (doctor_consultation_type_id INTEGER PRIMARY KEY AUTOINCREMENT,
    doctor_id INTEGER NOT NULL, consultation_type_id INTEGER NOT NULL, fee REAL NOT NULL,
    capacity INTEGER NOT NULL DEFAULT 1, is_active INTEGER NOT NULL DEFAULT 1,
    UNIQUE (doctor_id, consultation_type_id));
"""

HOSPITALS = [
    (1, "Apollo Hospitals Jubilee Hills", "Road No. 72", "Hyderabad", "Telangana"),
    (2, "Yashoda Hospitals Somajiguda", "Raj Bhavan Road", "Hyderabad", "Telangana"),
    (3, "Fortis Hospital Bannerghatta", "Bannerghatta Road", "Bengaluru", "Karnataka"),
]
SPECIALIZATIONS = [(1, "Cardiology"), (2, "Dermatology"), (6, "General Medicine"), (18, "Pediatrics")]
TYPES = [(1, "Quick Consult", 15, 1, 1), (2, "Standard Consult", 30, 1, 1), (4, "Health Camp Session", 60, 2, 10)]


class _Cursor:
    def __init__(self, owner):
        self.owner = owner
        self.rows = []
        self.lastrowid = None

    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def execute(self, sql, args=()):
        sql = sql.replace("%s", "?")
        sql = re.sub(r"LIKE \?", r"LIKE ? ESCAPE '\\'", sql)
        self.owner.log.append((" ".join(sql.split()), tuple(args)))
        try:
            cur = self.owner.db.execute(sql, tuple(args))
        except sqlite3.IntegrityError as exc:
            raise pymysql.err.IntegrityError(1062, f"Duplicate entry for key ({exc})")
        except sqlite3.OperationalError as exc:
            if "no such table" in str(exc):          # MySQL: ER_NO_SUCH_TABLE
                raise pymysql.err.ProgrammingError(1146, str(exc))
            raise
        self.lastrowid = cur.lastrowid
        self.rows = [dict(r) for r in cur.fetchall()] if cur.description else []
        return len(self.rows)

    def fetchone(self):
        return self.rows.pop(0) if self.rows else None

    def fetchall(self):
        rows, self.rows = self.rows, []
        return rows


class FakeMysql:
    """Connection-like object returned by the monkeypatched `dbconn`. The same instance serves every request."""

    def __init__(self):
        self.db = sqlite3.connect(":memory:", check_same_thread=False)
        self.db.row_factory = sqlite3.Row
        self.db.create_function("UTC_TIMESTAMP", 1, lambda _p: datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S.%f"))
        self.db.executescript(SCHEMA)
        self.db.executemany("INSERT INTO hospitals (hospital_id, hospital_name, address, city, state) VALUES (?,?,?,?,?)", HOSPITALS)
        self.db.executemany("INSERT INTO specializations (specialization_id, specialization_name) VALUES (?,?)", SPECIALIZATIONS)
        self.db.executemany("INSERT INTO consultation_types VALUES (?,?,?,?,?,1)", TYPES)
        self.db.commit()
        self.log = []

    # connection protocol
    def __enter__(self):
        return self

    def __exit__(self, *exc):
        return False

    def cursor(self):
        return _Cursor(self)

    def commit(self):
        self.db.commit()

    def rollback(self):
        self.db.rollback()

    def close(self):
        pass

    # test conveniences
    def one(self, sql, args=()):
        row = self.db.execute(sql, args).fetchone()
        return dict(row) if row else None

    def add_patient(self, email, phone, first="Asha", last="Rao", dob="1990-05-17", gender="Female", address="12 MG Road",
                    password="Passw0rd!xyz1"):
        import bcrypt
        digest = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=4)).decode()
        cur = self.db.execute("INSERT INTO users (email, password_hash, role, phone) VALUES (?,?,?,?)", (email, digest, "PATIENT", phone))
        self.db.execute("INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, gender, address) VALUES (?,?,?,?,?,?)",
                        (cur.lastrowid, first, last, dob, gender, address))
        self.db.commit()
        return cur.lastrowid

    def add_doctor(self, first, last, hospital=1, spec=1, types=((1, 300.0, 1),), active=True, photo=None, tz="Asia/Kolkata",
                   languages=("English", "Telugu"), years=10):
        cur = self.db.execute("INSERT INTO users (email, password_hash, role, phone, is_active) VALUES (?,?,?,?,?)",
                              (f"{first}.{last}@example.test".lower(), "x", "DOCTOR", None, 1 if active else 0))
        did = cur.lastrowid
        self.db.execute(
            """INSERT INTO doctors (doctor_id, first_name, last_name, hospital_id, specialization_id, license_number,
               years_experience, clinic_address, qualifications, languages_spoken, timezone, photo_key)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?)""",
            (did, first, last, hospital, spec, f"TS-{did:04d}", years, "Clinic address", "MBBS, MD",
             json.dumps(list(languages)), tz, photo))
        for tid, fee, cap in types:
            self.db.execute("INSERT INTO doctor_consultation_types (doctor_id, consultation_type_id, fee, capacity) VALUES (?,?,?,?)",
                            (did, tid, fee, cap))
        self.db.commit()
        return did


def api_event(method, path, user_id=None, role="PATIENT", body=None, query=None, jti="jti-1"):
    """An API Gateway proxy event as the Lambdas receive it (authorizer context already filled in)."""
    ctx = {}
    if user_id is not None:
        ctx = {"userId": str(user_id), "role": role, "jti": jti}
    return {
        "httpMethod": method,
        "path": path,
        "queryStringParameters": query or None,
        "body": json.dumps(body) if body is not None else None,
        "headers": {"x-correlation-id": "test-cid"},
        "requestContext": {"requestId": "req-1", "authorizer": ctx},
    }


def call(module, event):
    res = module.handler(event, None)
    return res["statusCode"], json.loads(res["body"])


class FakeAuthTable:
    """Stands in for the AuthTokens DynamoDB table."""

    def __init__(self):
        self.items = {}

    def put_item(self, Item, **_):
        self.items[(Item["token_key"], Item["token_type"])] = dict(Item)

    def get_item(self, Key):
        item = self.items.get((Key["token_key"], Key["token_type"]))
        return {"Item": dict(item)} if item else {}

    def update_item(self, Key, ExpressionAttributeValues=None, **_):
        self.items[(Key["token_key"], Key["token_type"])]["status"] = ExpressionAttributeValues[":r"]


def _cond(cond, item):
    """Evaluate a boto3 Key()/Attr() condition (real boto3 or the offline stub) against one item."""
    expr = cond.get_expression()
    op, vals = expr["operator"], expr["values"]
    if op == "AND":
        return _cond(vals[0], item) and _cond(vals[1], item)
    name = vals[0].name
    if name not in item:
        return False
    if op == "=":
        return item[name] == vals[1]
    if op == "BETWEEN":
        return vals[1] <= item[name] <= vals[2]
    raise AssertionError(f"condition {op} not modelled")


def _key_attrs(cond):
    expr = cond.get_expression()
    if expr["operator"] == "AND":
        return _key_attrs(expr["values"][0]) | _key_attrs(expr["values"][1])
    return {expr["values"][0].name}


class FakeSlotsTable:
    """DynamoDB Slots table: partition key doctor_id, sort key slot_key, no secondary index.
    Query enforces the real rules - key condition only on the table keys, partition key equality required,
    results sorted by sort key, FilterExpression applied after the read - and returns pages of `page_size`
    items with LastEvaluatedKey so callers must paginate."""

    def __init__(self, items=(), fail=False, page_size=1000):
        self.items = list(items)
        self.fail = fail
        self.page_size = page_size
        self.queries = []            # one entry per Query call: (doctor_id, lo, hi)
        self.updates = []

    def add(self, item):
        item = dict(item)
        item.setdefault("slot_key", f"{item['start_at']}#{int(item['consultation_type_id'])}")
        self.items.append(item)
        return item

    def query(self, **kwargs):
        if self.fail:
            raise RuntimeError("dynamodb unavailable")
        assert "IndexName" not in kwargs, "the Slots table has no secondary index"
        key = kwargs["KeyConditionExpression"]
        assert _key_attrs(key) <= {"doctor_id", "slot_key"} and "doctor_id" in _key_attrs(key), _key_attrs(key)
        parts = key.get_expression()["values"]
        rng = parts[1].get_expression()["values"] if key.get_expression()["operator"] == "AND" else None
        self.queries.append((parts[0].get_expression()["values"][1] if rng else parts[1],
                             rng[1] if rng else None, rng[2] if rng else None))
        hits = sorted((i for i in self.items if _cond(key, i)), key=lambda i: i["slot_key"])
        start = kwargs.get("ExclusiveStartKey")
        if start:
            hits = [i for i in hits if i["slot_key"] > start["slot_key"]]
        page, rest = hits[:self.page_size], hits[self.page_size:]
        if "FilterExpression" in kwargs:
            page = [i for i in page if _cond(kwargs["FilterExpression"], i)]
        out = {"Items": [dict(i) for i in page]}
        if rest:
            last = hits[self.page_size - 1]
            out["LastEvaluatedKey"] = {"doctor_id": last["doctor_id"], "slot_key": last["slot_key"]}
        return out

    def put_item(self, Item, ConditionExpression=None, **_):
        exists = any(i["doctor_id"] == Item["doctor_id"] and i["slot_key"] == Item["slot_key"] for i in self.items)
        if exists and ConditionExpression == "attribute_not_exists(slot_key)":
            from botocore.exceptions import ClientError
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "exists"}}, "PutItem")
        self.items = [i for i in self.items if not (i["doctor_id"] == Item["doctor_id"] and i["slot_key"] == Item["slot_key"])]
        self.items.append(dict(Item))
        return {}

    def update_item(self, Key, **kwargs):
        assert set(Key) == {"doctor_id", "slot_key"}, Key
        self.updates.append((Key, kwargs))
        return {}


class FakeDdb:
    def __init__(self, slots):
        self.slots = slots

    def Table(self, name):
        assert name.endswith("-Slots"), name
        return self.slots
