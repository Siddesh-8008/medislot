"""Doctor consultation types: master table -> doctor's own choice, plus the CloudWatch event logging."""
import os, sys, json, logging, importlib.util
from pathlib import Path

os.environ.setdefault("AWS_DEFAULT_REGION", "ap-south-1")
os.environ.setdefault("AWS_REGION", "ap-south-1")
ROOT = Path(__file__).resolve().parents[1]
sys.path.insert(0, str(ROOT / "lambdas"))
spec = importlib.util.spec_from_file_location("doctor_domain_under_test", ROOT / "lambdas/doctor_domain/lambda_function.py")
doctor = importlib.util.module_from_spec(spec)
spec.loader.exec_module(doctor)
import common

MASTER = {
    1: dict(consultation_type_id=1, name="Quick Consult", duration_minutes=15, min_capacity=1, max_capacity=1),
    4: dict(consultation_type_id=4, name="Health Camp Session", duration_minutes=60, min_capacity=2, max_capacity=10),
}


class FakeDb:
    """Just enough SQL matching to drive the handler without a real MySQL server."""

    def __init__(self):
        self.mine = {}      # consultation_type_id -> row in doctor_consultation_types
        self.next_id = 100
        self.last = None
        self.lastrowid = None

    # connection / cursor protocol
    def __enter__(self): return self
    def __exit__(self, *a): return False
    def cursor(self): return self
    def commit(self): pass
    def close(self): pass

    def execute(self, sql, args=()):
        q = " ".join(sql.split())
        self.last = None
        if q.startswith("SELECT doctor_id FROM doctors"):
            self.last = [{"doctor_id": args[0]}]
        elif "FROM consultation_types WHERE is_active=1 ORDER BY" in q:
            self.last = list(MASTER.values())
        elif "FROM consultation_types WHERE consultation_type_id=%s AND is_active=1" in q:
            self.last = [MASTER[args[0]]] if args[0] in MASTER else []
        elif q.startswith("SELECT doctor_consultation_type_id,is_active FROM doctor_consultation_types"):
            row = self.mine.get(args[1])
            self.last = [dict(doctor_consultation_type_id=row["id"], is_active=row["is_active"])] if row else []
        elif q.startswith("INSERT INTO doctor_consultation_types"):
            _, tid, fee, cap = args
            self.mine[tid] = dict(id=self.next_id, fee=fee, capacity=cap, is_active=1)
            self.lastrowid = self.next_id; self.next_id += 1
        elif q.startswith("UPDATE doctor_consultation_types SET fee=%s,capacity=%s,is_active=1"):
            fee, cap, row_id = args
            for r in self.mine.values():
                if r["id"] == row_id: r.update(fee=fee, capacity=cap, is_active=1)
        elif "FROM doctor_consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1" in q:
            row = self.mine.get(args[0])
            self.last = [{"consultation_type_id": args[0]}] if row and row["is_active"] else []
        else:
            raise AssertionError("unexpected SQL: " + q)

    def fetchone(self): return self.last[0] if self.last else None
    def fetchall(self): return self.last or []


def call(db, monkeypatch, method, path, body=None):
    monkeypatch.setattr(doctor, "config", lambda: {"env": "dev"})
    monkeypatch.setattr(doctor, "dbconn", lambda c: db)
    event = {"httpMethod": method, "path": path, "body": json.dumps(body) if body is not None else None,
             "requestContext": {"requestId": "req-1", "authorizer": {"userId": "7", "role": "DOCTOR"}}}
    res = doctor.handler(event, None)
    return res["statusCode"], json.loads(res["body"])


def test_catalog_lists_standard_types(monkeypatch):
    status, data = call(FakeDb(), monkeypatch, "GET", "/doctors/catalog/consultation-types")
    assert status == 200
    assert [i["name"] for i in data["items"]] == ["Quick Consult", "Health Camp Session"]


def test_doctor_chooses_type_and_gets_name_and_duration_from_master(monkeypatch):
    db = FakeDb()
    status, data = call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 1, "fee": 300})
    assert status == 201
    assert data["name"] == "Quick Consult" and data["durationMinutes"] == 15 and data["capacity"] == 1
    assert db.mine[1]["fee"] == 300


def test_capacity_must_fit_the_master_range(monkeypatch):
    db = FakeDb()
    assert call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 1, "fee": 300, "capacity": 3})[0] == 422
    assert call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 4, "fee": 900, "capacity": 1})[0] == 422
    assert call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 4, "fee": 900, "capacity": 6})[0] == 201


def test_unknown_type_negative_fee_and_duplicates_are_rejected(monkeypatch):
    db = FakeDb()
    assert call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 99, "fee": 1})[0] == 422
    assert call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 1, "fee": -5})[0] == 422
    assert call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 1, "fee": 300})[0] == 201
    assert call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 1, "fee": 300})[0] == 409


def test_previously_removed_type_is_reactivated_not_duplicated(monkeypatch):
    db = FakeDb()
    call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 1, "fee": 300})
    db.mine[1]["is_active"] = 0
    status, _ = call(db, monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 1, "fee": 450})
    assert status == 201 and db.mine[1]["is_active"] == 1 and db.mine[1]["fee"] == 450 and len(db.mine) == 1


def test_pattern_requires_a_type_the_doctor_has_chosen(monkeypatch):
    db = FakeDb()
    body = {"consultationTypeId": 1, "dayOfWeek": 1, "startTime": "09:00", "endTime": "11:00"}
    status, data = call(db, monkeypatch, "POST", "/doctors/me/availability-patterns", body)
    assert status == 422 and "consultation type" in data["message"].lower()


def test_non_doctor_is_refused(monkeypatch):
    event = {"httpMethod": "GET", "path": "/doctors/me", "requestContext": {"authorizer": {"userId": "7", "role": "PATIENT"}}}
    assert doctor.handler(event, None)["statusCode"] == 403


def test_every_invocation_is_logged_without_secrets(monkeypatch, caplog):
    caplog.set_level(logging.INFO)
    call(FakeDb(), monkeypatch, "POST", "/doctors/me/consultation-types", {"consultationTypeId": 1, "fee": 300})
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "lambda_invoked" in text and "lambda_completed" in text and "doctor_consultation_type_added" in text
    assert '"statusCode": 201' in text


def test_describe_event_never_logs_tokens_or_bodies():
    auth = common.describe_event({"methodArn": "arn:x/dev/GET/doctors/me", "authorizationToken": "Bearer SECRET.JWT"})
    assert "SECRET" not in json.dumps(auth)
    http = common.describe_event({"httpMethod": "POST", "path": "/auth/login", "body": '{"password":"hunter2"}',
                                  "headers": {"Authorization": "Bearer SECRET"}, "requestContext": {}})
    assert "hunter2" not in json.dumps(http) and "SECRET" not in json.dumps(http) and http["hasBody"] is True
    direct = common.describe_event({"doctorId": 7, "password": "x", "nested": {"refreshToken": "abc"}})
    assert direct["event"]["password"] == "***" and direct["event"]["nested"]["refreshToken"] == "***"


def test_schema_defines_master_and_doctor_tables_and_seeds_four_types():
    sql = (ROOT / "scehma/schema.sql").read_text()
    for table in ("consultation_types", "doctor_consultation_types", "availability_patterns"):
        assert f"CREATE TABLE IF NOT EXISTS {table} (" in sql
    for name in ("Quick Consult", "Standard Consult", "Detailed Consult", "Health Camp Session"):
        assert f"'{name}'" in sql
    # the master table must no longer belong to a doctor
    master = sql.split("CREATE TABLE IF NOT EXISTS consultation_types (")[1].split(") ENGINE=InnoDB;")[0]
    assert "doctor_id" not in master and "fee" not in master
