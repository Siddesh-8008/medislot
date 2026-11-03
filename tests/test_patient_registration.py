"""Patient self-registration: mandatory fields, edge cases, and that closed accounts cannot refresh tokens."""
import json
import time

import pytest

from support import FakeAuthTable, FakeMysql, api_event, call, load_lambda

auth = load_lambda("authentication", "authentication_under_test")
GOOD = {"firstName": "Asha", "lastName": "Rao", "email": "Asha@Example.test", "phone": "9876543210",
        "dateOfBirth": "1990-05-17", "gender": "Female", "address": "12 MG Road, Hyderabad", "password": "StrongPass!234"}


@pytest.fixture
def env(monkeypatch):
    db = FakeMysql()
    table = FakeAuthTable()
    monkeypatch.setattr(auth, "config", lambda: {"env": "dev", "prefix": "/medislot/dev", "jwt": "unit-test-signing-secret-0123456789"})
    monkeypatch.setattr(auth, "dbconn", lambda c: db)
    monkeypatch.setattr(auth, "get_param", lambda name, *a, **k: "12" if "password-min-length" in name else "15")
    monkeypatch.setattr(auth, "auth_table", lambda: table)
    db.tokens = table
    return db


def register(payload, role="patient"):
    event = api_event("POST", f"/auth/register/{role}", body=payload)
    return call(auth, event)


def test_patient_registers_with_all_mandatory_fields(env):
    status, data = register(GOOD)
    assert status == 201 and data["role"] == "PATIENT"
    user = env.one("SELECT * FROM users WHERE user_id = ?", (int(data["userId"]),))
    patient = env.one("SELECT * FROM patients WHERE patient_id = ?", (int(data["userId"]),))
    assert user["email"] == "asha@example.test" and user["phone"] == "9876543210" and user["role"] == "PATIENT"
    assert user["password_hash"] != GOOD["password"]
    assert (patient["first_name"], patient["last_name"], patient["date_of_birth"], patient["gender"]) == ("Asha", "Rao", "1990-05-17", "Female")


def test_address_is_optional(env):
    payload = {k: v for k, v in GOOD.items() if k != "address"}
    status, data = register(payload)
    assert status == 201
    assert env.one("SELECT address FROM patients WHERE patient_id = ?", (int(data["userId"]),))["address"] is None


def test_input_is_normalised(env):
    status, data = register({**GOOD, "firstName": "  asha   devi ", "gender": "FEMALE", "email": "  MiXed@Example.TEST "})
    assert status == 201
    row = env.one("SELECT p.first_name, p.gender, u.email FROM patients p JOIN users u ON u.user_id = p.patient_id")
    assert row == {"first_name": "asha devi", "gender": "Female", "email": "mixed@example.test"}


@pytest.mark.parametrize("missing", ["firstName", "lastName", "email", "phone", "dateOfBirth", "gender", "password"])
def test_every_mandatory_field_is_required(env, missing):
    payload = {k: v for k, v in GOOD.items() if k != missing}
    status, data = register(payload)
    assert status == 400 and data["message"]
    assert env.one("SELECT COUNT(*) AS n FROM users")["n"] == 0


def test_all_form_problems_are_returned_at_once(env):
    status, data = register({"firstName": "", "lastName": "R4o", "email": "not-an-email", "phone": "123",
                             "dateOfBirth": "2999-01-01", "gender": "?", "password": GOOD["password"]})
    assert status == 400
    assert set(data["errors"]) == {"firstName", "lastName", "email", "phone", "dateOfBirth", "gender"}


@pytest.mark.parametrize("field,value", [
    ("phone", "98765"), ("phone", "98765432101"), ("phone", "abcdefghij"), ("phone", "٩٨٧٦٥٤٣٢١٠"), ("phone", 9876543210.0),
    ("dateOfBirth", "1990/05/17"), ("dateOfBirth", "1990-02-30"), ("dateOfBirth", "1700-01-01"), ("dateOfBirth", "2999-12-31"),
    ("dateOfBirth", 19900517), ("gender", "Banana"), ("gender", ""), ("gender", 1),
    ("firstName", "<b>x</b>"), ("firstName", "Asha1"), ("lastName", "x" * 76), ("address", "a" * 501),
    ("email", "a@b"), ("email", "x" * 250 + "@e.com"),
])
def test_invalid_values_are_rejected(env, field, value):
    status, _ = register({**GOOD, field: value})
    assert status == 400
    assert env.one("SELECT COUNT(*) AS n FROM users")["n"] == 0


def test_weak_password_is_rejected(env):
    for weak in ("short1!A", "alllowercase123!", "ALLUPPERCASE123!", "NoDigitsHere!!!!", "NoSymbol123456Aa", "x" * 80 + "A1!"):
        assert register({**GOOD, "password": weak})[0] == 400, weak


def test_duplicate_email_or_phone_gets_a_generic_conflict(env):
    assert register(GOOD)[0] == 201
    status, data = register({**GOOD, "phone": "9000000009"})          # same email
    assert status == 409 and data["message"] == "Unable to register with those details"
    status, data = register({**GOOD, "email": "other@example.test"})  # same phone
    assert status == 409 and data["message"] == "Unable to register with those details"
    assert env.one("SELECT COUNT(*) AS n FROM users")["n"] == 1


def test_invalid_json_and_wrong_shapes(env):
    event = api_event("POST", "/auth/register/patient")
    event["body"] = "{oops"
    assert call(auth, event)[0] == 400
    assert register([1, 2, 3])[0] == 400


def test_doctor_registration_is_unchanged(env):
    doctor = {"firstName": "Ravi", "lastName": "Kumar", "email": "dr.ravi@example.test", "phone": "9876543211",
              "password": "StrongPass!234", "hospitalId": 1, "specializationId": 1, "licenseNumber": "TS-1234"}
    status, data = register(doctor, role="doctor")
    assert status == 201 and data["role"] == "DOCTOR"
    assert register({**doctor, "email": "x@example.test", "phone": None, "licenseNumber": "TS-1235"}, role="doctor")[0] == 400
    assert register({**doctor, "email": "y@example.test", "phone": "9876543212", "licenseNumber": "bad"}, role="doctor")[0] == 400


# ------------------------------------------------------------------ closed accounts cannot refresh
def store_refresh(env, user_id, raw="refresh-token-1"):
    env.tokens.put_item({"token_key": "REFRESH#" + auth.token_hash(raw), "token_type": "REFRESH", "user_id": str(user_id),
                         "role": "PATIENT", "expires_at": int(time.time()) + 3600, "status": "ACTIVE"})
    return raw


def test_active_account_can_refresh(env):
    uid = env.add_patient("a@example.test", "9000000001")
    raw = store_refresh(env, uid)
    status, data = call(auth, api_event("POST", "/auth/refresh", body={"refreshToken": raw}))
    assert status == 200 and data["accessToken"]


def test_closed_account_cannot_refresh(env):
    uid = env.add_patient("a@example.test", "9000000001")
    raw = store_refresh(env, uid)
    env.db.execute("UPDATE users SET is_active = 0 WHERE user_id = ?", (uid,))
    env.db.commit()
    status, data = call(auth, api_event("POST", "/auth/refresh", body={"refreshToken": raw}))
    assert status == 401 and "accessToken" not in data


def test_closed_account_cannot_log_in(env):
    env.add_patient("a@example.test", "9000000001", password="StrongPass!234")
    assert call(auth, api_event("POST", "/auth/login", body={"email": "a@example.test", "password": "StrongPass!234"}))[0] == 200
    env.db.execute("UPDATE users SET is_active = 0")
    env.db.commit()
    assert call(auth, api_event("POST", "/auth/login", body={"email": "a@example.test", "password": "StrongPass!234"}))[0] == 401
