"""Patient profile API: CRUD, validation, data isolation and account closing."""
import logging

import pytest

from support import FakeAuthTable, FakeMysql, api_event, call, load_lambda

patient_api = load_lambda("patient_domain", "patient_domain_under_test")


@pytest.fixture
def env(monkeypatch):
    db = FakeMysql()
    auth = FakeAuthTable()
    monkeypatch.setattr(patient_api, "db_config", lambda: {"env": "dev"})
    monkeypatch.setattr(patient_api, "dbconn", lambda c: db)
    monkeypatch.setattr(patient_api, "auth_table", lambda: auth)
    db.asha = db.add_patient("asha@example.test", "9000000001", first="Asha", last="Rao", address="12 MG Road")
    db.ravi = db.add_patient("ravi@example.test", "9000000002", first="Ravi", last="Kumar", address="Secret Lane 7")
    return db, auth


def me(env, method="GET", body=None, user=None, role="PATIENT", path="/patients/me", jti="jti-asha"):
    db, _ = env
    return call(patient_api, api_event(method, path, user_id=user if user is not None else db.asha, role=role, body=body, jti=jti))


# ------------------------------------------------------------------ read
def test_patient_reads_own_profile(env):
    status, data = me(env)
    p = data["profile"]
    assert status == 200
    assert (p["firstName"], p["lastName"], p["email"], p["phone"], p["gender"]) == ("Asha", "Rao", "asha@example.test", "9000000001", "Female")
    assert p["dateOfBirth"] == "1990-05-17" and p["address"] == "12 MG Road" and p["patientId"] == env[0].asha
    assert "password" not in str(data).lower() and "hash" not in str(data).lower()


def test_unauthenticated_or_unknown_role_is_refused(env):
    assert call(patient_api, api_event("GET", "/patients/me"))[0] == 403
    assert me(env, role="DOCTOR")[0] == 403
    assert me(env, role=None)[0] == 403


# ------------------------------------------------------------------ isolation (the checkpoint)
def test_patient_cannot_read_another_patients_profile(env, caplog):
    db, _ = env
    caplog.set_level(logging.INFO)
    status, data = me(env, path=f"/patients/{db.ravi}")
    assert status == 403
    assert data["message"] == "You can only access your own profile"
    leaked = str(data)
    assert "ravi" not in leaked.lower() and "Secret Lane" not in leaked and "9000000002" not in leaked
    assert "patient_isolation_violation" in "\n".join(r.getMessage() for r in caplog.records)


def test_own_id_in_the_path_is_allowed(env):
    db, _ = env
    status, data = me(env, path=f"/patients/{db.asha}")
    assert status == 200 and data["profile"]["email"] == "asha@example.test"


@pytest.mark.parametrize("requested", ["999999", "0", "-1", "abc", "1e3", f"{10**30}", "%2e%2e", "me%20", "٣"])
def test_any_other_id_is_refused_the_same_way_whether_or_not_it_exists(env, requested):
    status, data = me(env, path=f"/patients/{requested}")
    assert status == 403 and data["message"] == "You can only access your own profile"


def test_the_patient_id_is_never_taken_from_the_request_body_or_query(env):
    db, _ = env
    # trying to smuggle another id into an update is rejected as an unsupported field...
    status, data = me(env, "PATCH", {"patientId": db.ravi, "firstName": "Hacked"})
    assert status == 422 and "patientId" in data["errors"]
    assert db.one("SELECT first_name FROM patients WHERE patient_id = ?", (db.ravi,))["first_name"] == "Ravi"
    assert db.one("SELECT first_name FROM patients WHERE patient_id = ?", (db.asha,))["first_name"] == "Asha"
    # ...and a query string id is ignored: /patients/me always means the caller
    event = api_event("GET", "/patients/me", user_id=db.asha, query={"patientId": str(db.ravi)})
    status, data = call(patient_api, event)
    assert status == 200 and data["profile"]["patientId"] == db.asha


def test_a_write_to_another_id_is_not_possible(env):
    db, _ = env
    for method in ("PUT", "PATCH", "DELETE", "POST"):
        status, _ = me(env, method, {"firstName": "X", "password": "x"}, path=f"/patients/{db.ravi}")
        assert status == 405
    assert db.one("SELECT first_name FROM patients WHERE patient_id = ?", (db.ravi,))["first_name"] == "Ravi"


# ------------------------------------------------------------------ update
def test_update_changes_only_the_fields_sent(env):
    db, _ = env
    status, data = me(env, "PATCH", {"firstName": "  Asha   Devi ", "gender": "other", "address": ""})
    p = data["profile"]
    assert status == 200 and p["firstName"] == "Asha Devi" and p["gender"] == "Other" and p["address"] is None
    assert p["lastName"] == "Rao" and p["dateOfBirth"] == "1990-05-17" and p["phone"] == "9000000001"
    assert me(env, "PUT", {"lastName": "Reddy"})[1]["profile"]["lastName"] == "Reddy"


def test_phone_is_updated_on_the_account(env):
    db, _ = env
    status, data = me(env, "PATCH", {"phone": "9111111111"})
    assert status == 200 and data["profile"]["phone"] == "9111111111"
    assert db.one("SELECT phone FROM users WHERE user_id = ?", (db.asha,))["phone"] == "9111111111"


def test_phone_already_used_by_someone_else_is_a_conflict(env):
    status, data = me(env, "PATCH", {"phone": "9000000002"})
    assert status == 409 and "already registered" in data["message"]
    assert me(env)[1]["profile"]["phone"] == "9000000001"   # unchanged after the rolled-back attempt


def test_resubmitting_my_own_phone_is_fine(env):
    assert me(env, "PATCH", {"phone": "9000000001"})[0] == 200


@pytest.mark.parametrize("payload,field", [
    ({"phone": "12345"}, "phone"),
    ({"phone": "98765 43210"}, "phone"),
    ({"phone": 9876543210}, "phone"),
    ({"dateOfBirth": "2999-01-01"}, "dateOfBirth"),
    ({"dateOfBirth": "1990-02-30"}, "dateOfBirth"),
    ({"dateOfBirth": "17/05/1990"}, "dateOfBirth"),
    ({"firstName": "R4vi"}, "firstName"),
    ({"firstName": "<script>alert(1)</script>"}, "firstName"),
    ({"lastName": ""}, "lastName"),
    ({"lastName": None}, "lastName"),
    ({"firstName": "x" * 76}, "firstName"),
    ({"gender": "robot"}, "gender"),
    ({"address": "a" * 501}, "address"),
    ({"address": ["list"]}, "address"),
])
def test_invalid_values_are_rejected_with_the_field_named(env, payload, field):
    status, data = me(env, "PATCH", payload)
    assert status == 422 and field in data["errors"] and data["message"]
    assert me(env)[1]["profile"]["firstName"] == "Asha"      # nothing was written


def test_all_problems_are_reported_together(env):
    status, data = me(env, "PATCH", {"firstName": "1", "phone": "1", "gender": "?"})
    assert status == 422 and set(data["errors"]) == {"firstName", "phone", "gender"}


@pytest.mark.parametrize("payload", ["email", "role", "password", "isActive", "userId", "createdAt"])
def test_account_fields_cannot_be_changed_here(env, payload):
    status, data = me(env, "PATCH", {payload: "x"})
    assert status == 422 and payload in data["errors"]


def test_empty_and_malformed_bodies(env):
    assert me(env, "PATCH", {})[0] == 400
    event = api_event("PATCH", "/patients/me", user_id=env[0].asha)
    event["body"] = "{not json"
    assert call(patient_api, event)[0] == 400
    event["body"] = "[1,2]"
    assert call(patient_api, event)[0] == 400


def test_sql_metacharacters_are_stored_literally_not_executed(env):
    db, _ = env
    status, _ = me(env, "PATCH", {"address": "x'); DROP TABLE patients;--"})
    assert status == 200
    assert db.one("SELECT address FROM patients WHERE patient_id = ?", (db.asha,))["address"] == "x'); DROP TABLE patients;--"
    assert db.one("SELECT COUNT(*) AS n FROM patients")["n"] == 2


# ------------------------------------------------------------------ create
def test_create_profile_for_an_account_that_has_none(env):
    db, _ = env
    db.db.execute("DELETE FROM patients WHERE patient_id = ?", (db.asha,))
    db.db.commit()
    assert me(env)[0] == 404                                   # nothing to read yet
    assert me(env, "PATCH", {"firstName": "X"})[0] == 404
    status, data = me(env, "POST", {"firstName": "Asha", "lastName": "Rao", "dateOfBirth": "1990-05-17", "gender": "Female"})
    assert status == 201 and data["profile"]["firstName"] == "Asha" and data["profile"]["email"] == "asha@example.test"


def test_create_requires_the_mandatory_fields_and_refuses_duplicates(env):
    db, _ = env
    full = {"firstName": "Asha", "lastName": "Rao", "dateOfBirth": "1990-05-17", "gender": "Female"}
    status, data = me(env, "POST", full)                      # a profile already exists for this account
    assert status == 409 and "already exists" in data["message"]
    db.db.execute("DELETE FROM patients WHERE patient_id = ?", (db.asha,))
    db.db.commit()
    status, data = me(env, "POST", {"firstName": "Asha"})
    assert status == 422 and set(data["errors"]) == {"lastName", "dateOfBirth", "gender"}
    assert me(env, "POST", {**full, "email": "other@example.test"})[0] == 422   # accounts fields are not accepted
    assert me(env, "POST", full)[0] == 201


# ------------------------------------------------------------------ delete / close account
def test_closing_needs_the_password(env):
    db, auth = env
    assert me(env, "DELETE", {})[0] == 422
    assert me(env, "DELETE", {"password": ""})[0] == 422
    status, data = me(env, "DELETE", {"password": "wrong-password"})
    assert status == 403 and "not correct" in data["message"]
    assert db.one("SELECT is_active FROM users WHERE user_id = ?", (db.asha,))["is_active"] == 1
    assert not auth.items


def test_closing_erases_personal_data_frees_the_email_and_revokes_tokens(env):
    db, auth = env
    auth.put_item({"token_key": "REFRESH#" + patient_api.token_hash("my-refresh"), "token_type": "REFRESH",
                   "user_id": str(db.asha), "status": "ACTIVE"})
    status, data = me(env, "DELETE", {"password": "Passw0rd!xyz1", "refreshToken": "my-refresh"})
    assert status == 200 and "closed" in data["message"]

    user = db.one("SELECT * FROM users WHERE user_id = ?", (db.asha,))
    patient = db.one("SELECT * FROM patients WHERE patient_id = ?", (db.asha,))
    assert user["is_active"] == 0 and user["phone"] is None and "asha" not in user["email"]
    assert patient["date_of_birth"] is None and patient["gender"] is None and patient["address"] is None
    assert patient["first_name"] != "Asha" and patient["last_name"] != "Rao"

    assert ("JTI#jti-asha", "DENYLIST") in auth.items                       # this access token is dead
    assert auth.items[("REFRESH#" + patient_api.token_hash("my-refresh"), "REFRESH")]["status"] == "REVOKED"

    # the same email and phone can register again, and the other patient is untouched
    db.add_patient("asha@example.test", "9000000001", first="Asha", last="New")
    assert db.one("SELECT first_name FROM patients WHERE patient_id = ?", (db.ravi,))["first_name"] == "Ravi"


def test_a_closed_account_is_refused_even_with_an_unexpired_token(env):
    me(env, "DELETE", {"password": "Passw0rd!xyz1"})
    for method, body in (("GET", None), ("PATCH", {"firstName": "Zed"}), ("DELETE", {"password": "Passw0rd!xyz1"}), ("POST", {})):
        status, data = me(env, method, body)
        assert status == 401 and "closed" in data["message"]


def test_someone_elses_refresh_token_is_not_revoked(env):
    db, auth = env
    auth.put_item({"token_key": "REFRESH#" + patient_api.token_hash("ravis-refresh"), "token_type": "REFRESH",
                   "user_id": str(db.ravi), "status": "ACTIVE"})
    me(env, "DELETE", {"password": "Passw0rd!xyz1", "refreshToken": "ravis-refresh"})
    assert auth.items[("REFRESH#" + patient_api.token_hash("ravis-refresh"), "REFRESH")]["status"] == "ACTIVE"


def test_a_failure_while_revoking_tokens_does_not_fail_the_request(env, monkeypatch):
    def boom():
        raise RuntimeError("dynamodb down")
    monkeypatch.setattr(patient_api, "auth_table", boom)
    assert me(env, "DELETE", {"password": "Passw0rd!xyz1"})[0] == 200


# ------------------------------------------------------------------ robustness & logging
def test_unknown_route_and_method(env):
    assert me(env, path="/patients/me/secrets/more")[0] == 404
    assert me(env, "OPTIONS")[0] == 405


def test_database_failure_is_a_clean_500_without_details(env, monkeypatch):
    def broken(_c):
        raise RuntimeError("password=hunter2 host=secret.internal")
    monkeypatch.setattr(patient_api, "dbconn", broken)
    status, data = me(env)
    assert status == 500 and "hunter2" not in str(data) and "secret.internal" not in str(data)


def test_events_are_logged_without_personal_data(env, caplog):
    caplog.set_level(logging.INFO)
    me(env, "PATCH", {"firstName": "Asha", "phone": "9333333333", "address": "Very Private Street 9"})
    me(env, "DELETE", {"password": "Passw0rd!xyz1"})
    text = "\n".join(r.getMessage() for r in caplog.records)
    for event in ("lambda_invoked", "lambda_completed", "patient_profile_updated", "patient_account_closed"):
        assert event in text
    for secret in ("Passw0rd", "9333333333", "Very Private", "asha@example.test"):
        assert secret not in text
