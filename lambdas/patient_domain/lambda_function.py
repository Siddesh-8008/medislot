"""MediSlot patient domain: the signed-in patient's own profile (create, read, update, delete).

Routes (all behind the JWT authorizer; the caller must have the PATIENT role):
    GET    /patients/me          read my profile
    POST   /patients/me          create my profile if it is missing (registration normally does this)
    PUT    /patients/me          update my profile (partial update, same as PATCH)
    PATCH  /patients/me          update my profile
    DELETE /patients/me          close my account (needs my password); personal details are erased
    GET    /patients/{id}        only ever succeeds when {id} is my own id - anything else is 403

Patient data isolation
    * The patient id is NEVER taken from the request. It comes from the authorizer context, which API
      Gateway fills in from the verified JWT. Every SQL statement in this file is bound to that id.
    * /patients/{id} exists so the rule is explicit and testable: a different id is refused with 403
      and a `patient_isolation_violation` warning in CloudWatch, whether or not that patient exists
      (so the response cannot be used to find out which ids are real).
    * The writable fields are a fixed allow-list; email, role and ids cannot be changed through here.
"""
import re
import secrets
import time
import traceback

import bcrypt
import pymysql

from common import (
    FieldErrors,
    PATIENT_FIELDS,
    PATIENT_MANDATORY,
    auth_table,
    body,
    clean_patient_fields,
    correlation,
    db_config,
    dbconn,
    emit,
    instrument,
    json_out,
    token_hash,
)

COLUMN_FOR = {
    "firstName": "first_name",
    "lastName": "last_name",
    "dateOfBirth": "date_of_birth",
    "gender": "gender",
    "address": "address",
}
DENYLIST_SECONDS = 3600  # longer than any access token lives; DynamoDB TTL removes the row afterwards


def caller(event):
    ctx = (event.get("requestContext") or {}).get("authorizer") or {}
    try:
        uid = int(ctx.get("userId") or 0)
    except (TypeError, ValueError):
        uid = 0
    return uid, ctx.get("role"), ctx.get("jti")


def route(event):
    """'/dev/patients/me' or '/patients/me' -> '/patients/me' (API Gateway may or may not include the stage)."""
    path = event.get("path") or event.get("rawPath") or ""
    m = re.search(r"(/patients(?:/[^/]*)?)/?$", path)
    return m.group(1) if m else path


def method_of(event):
    return (event.get("httpMethod") or (event.get("requestContext") or {}).get("http", {}).get("method") or "GET").upper()


def msg(cid, text, **extra):
    return {"message": text, "correlationId": cid, **extra}


def load(cur, uid):
    """The account + profile for the caller, or None. LEFT JOIN so 'no profile yet' is distinguishable."""
    cur.execute(
        """SELECT u.user_id, u.email, u.phone, u.is_active, p.patient_id, p.first_name, p.last_name,
                  p.date_of_birth, p.gender, p.address, p.created_at, p.updated_at
           FROM users u LEFT JOIN patients p ON p.patient_id = u.user_id
           WHERE u.user_id = %s AND u.role = 'PATIENT'""",
        (uid,),
    )
    return cur.fetchone()


def profile_json(r):
    return {
        "patientId": int(r["user_id"]),
        "firstName": r["first_name"],
        "lastName": r["last_name"],
        "email": r["email"],
        "phone": r["phone"],
        "dateOfBirth": r["date_of_birth"],
        "gender": r["gender"],
        "address": r["address"],
        "createdAt": r["created_at"],
        "updatedAt": r["updated_at"],
    }


def unusable_password_hash():
    return bcrypt.hashpw(secrets.token_urlsafe(32).encode(), bcrypt.gensalt(rounds=4)).decode()


def is_duplicate(exc):
    return bool(exc.args) and exc.args[0] == 1062


def revoke_tokens(uid, jti, refresh_token, cid):
    """Kill the access token that made this request and, if given, the refresh token. Best effort: the
    account is already inactive, which every protected API and the refresh endpoint also check."""
    now = int(time.time())
    try:
        table = auth_table()
        if jti:
            table.put_item(Item={
                "token_key": "JTI#" + str(jti), "token_type": "DENYLIST", "user_id": str(uid),
                "created_at": now, "expires_at": now + DENYLIST_SECONDS, "status": "REVOKED"})
        if isinstance(refresh_token, str) and refresh_token:
            key = {"token_key": "REFRESH#" + token_hash(refresh_token), "token_type": "REFRESH"}
            item = table.get_item(Key=key).get("Item")
            if item and item.get("user_id") == str(uid):
                table.update_item(Key=key, UpdateExpression="SET #s = :r",
                                  ExpressionAttributeNames={"#s": "status"},
                                  ExpressionAttributeValues={":r": "REVOKED"})
    except Exception as exc:  # noqa: BLE001 - never fail the request after the account is closed
        emit("warning", "token_revocation_failed", cid, userId=uid, error=type(exc).__name__)


@instrument("patient_domain")
def handler(event, context):
    cid = correlation(event)
    uid, role, jti = caller(event)
    if role != "PATIENT" or uid <= 0:
        emit("warning", "patient_access_denied", cid, role=role)
        return json_out(403, msg(cid, "Patient role required"))

    method = method_of(event)
    path = route(event)
    try:
        if path == "/patients/me":
            return handle_me(event, method, uid, jti, cid)
        m = re.fullmatch(r"/patients/([^/]+)", path)
        if m:
            return handle_by_id(event, method, m.group(1), uid, jti, cid)
        return json_out(404, msg(cid, "Patient endpoint not found", path=path))
    except Exception as exc:  # noqa: BLE001
        emit("error", "patient_domain_error", cid, error=type(exc).__name__, path=path, method=method,
             detail=str(exc)[:300], traceback=traceback.format_exc()[-1500:])
        return json_out(500, msg(cid, "Internal server error"))


def handle_by_id(event, method, raw_id, uid, jti, cid):
    """/patients/{id}: the only id that is ever served is the caller's own."""
    if method != "GET":
        return json_out(405, msg(cid, "Method not allowed"))
    if not (raw_id.isascii() and raw_id.isdigit() and int(raw_id) == uid):
        emit("warning", "patient_isolation_violation", cid, callerId=uid, requestedId=raw_id[:30])
        return json_out(403, msg(cid, "You can only access your own profile"))
    return handle_me(event, "GET", uid, jti, cid)


def handle_me(event, method, uid, jti, cid):
    if method == "GET":
        return read_profile(uid, cid)
    if method == "POST":
        return create_profile(event, uid, cid)
    if method in ("PUT", "PATCH"):
        return update_profile(event, uid, cid)
    if method == "DELETE":
        return delete_account(event, uid, jti, cid)
    return json_out(405, msg(cid, "Method not allowed"))


def account_problem(row, cid, need_profile=True):
    """Common gate: refuse closed accounts and (usually) accounts without a profile row."""
    if not row:
        return json_out(401, msg(cid, "Account not found"))
    if not row["is_active"]:
        return json_out(401, msg(cid, "This account has been closed"))
    if need_profile and row["patient_id"] is None:
        return json_out(404, msg(cid, "Patient profile not found; create it first"))
    return None


def read_profile(uid, cid):
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            row = load(cur, uid)
    problem = account_problem(row, cid)
    if problem:
        return problem
    emit("info", "patient_profile_read", cid, patientId=uid)
    return json_out(200, {"profile": profile_json(row), "correlationId": cid})


def parse_json_object(event, cid):
    data = body(event)
    if not isinstance(data, dict):
        return None, json_out(400, msg(cid, "Invalid JSON body"))
    return data, None


def validation_response(exc, cid):
    return json_out(422, msg(cid, "; ".join(exc.errors.values()), errors=exc.errors))


def create_profile(event, uid, cid):
    data, bad = parse_json_object(event, cid)
    if bad:
        return bad
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            row = load(cur, uid)
            problem = account_problem(row, cid, need_profile=False)
            if problem:
                return problem
            if row["patient_id"] is not None:
                return json_out(409, msg(cid, "Patient profile already exists"))
            try:
                fields = clean_patient_fields(data, required=tuple(f for f in PATIENT_MANDATORY if f != "phone"))
            except FieldErrors as exc:
                return validation_response(exc, cid)
            try:
                cur.execute(
                    """INSERT INTO patients (patient_id, first_name, last_name, date_of_birth, gender, address)
                       VALUES (%s, %s, %s, %s, %s, %s)""",
                    (uid, fields["firstName"], fields["lastName"], fields["dateOfBirth"], fields["gender"],
                     fields.get("address")),
                )
                if "phone" in fields:
                    cur.execute("UPDATE users SET phone = %s WHERE user_id = %s", (fields["phone"], uid))
                conn.commit()
            except pymysql.err.IntegrityError as exc:
                conn.rollback()
                if is_duplicate(exc):
                    return json_out(409, msg(cid, "That phone number is already registered"))
                raise
            row = load(cur, uid)
    emit("info", "patient_profile_created", cid, patientId=uid)
    return json_out(201, {"profile": profile_json(row), "correlationId": cid})


def update_profile(event, uid, cid):
    data, bad = parse_json_object(event, cid)
    if bad:
        return bad
    if not data:
        return json_out(400, msg(cid, "No fields supplied. You can change: " + ", ".join(PATIENT_FIELDS)))
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            problem = account_problem(load(cur, uid), cid)
            if problem:
                return problem
            try:
                fields = clean_patient_fields(data, required=())
            except FieldErrors as exc:
                return validation_response(exc, cid)
            try:
                sets = [f"{COLUMN_FOR[k]} = %s" for k in COLUMN_FOR if k in fields]
                if sets:
                    cur.execute(
                        f"UPDATE patients SET {', '.join(sets)}, updated_at = UTC_TIMESTAMP(6) WHERE patient_id = %s",
                        [fields[k] for k in COLUMN_FOR if k in fields] + [uid],
                    )
                if "phone" in fields:
                    cur.execute("UPDATE users SET phone = %s WHERE user_id = %s", (fields["phone"], uid))
                    if not sets:
                        cur.execute("UPDATE patients SET updated_at = UTC_TIMESTAMP(6) WHERE patient_id = %s", (uid,))
                conn.commit()
            except pymysql.err.IntegrityError as exc:
                conn.rollback()
                if is_duplicate(exc):
                    return json_out(409, msg(cid, "That phone number is already registered"))
                raise
            row = load(cur, uid)
    emit("info", "patient_profile_updated", cid, patientId=uid, fields=sorted(fields))
    return json_out(200, {"message": "Profile updated", "profile": profile_json(row), "correlationId": cid})


def delete_account(event, uid, jti, cid):
    """Close the account. The row is kept (future appointments will reference patient_id) but every
    personal detail is erased and the email / phone are freed so the person can register again."""
    data, bad = parse_json_object(event, cid)
    if bad:
        return bad
    password = data.get("password")
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            problem = account_problem(load(cur, uid), cid, need_profile=False)
            if problem:
                return problem
            if not isinstance(password, str) or not password:
                return json_out(422, msg(cid, "Enter your password to confirm closing the account"))
            cur.execute("SELECT password_hash FROM users WHERE user_id = %s", (uid,))
            stored = (cur.fetchone() or {}).get("password_hash") or ""
            try:
                ok = bcrypt.checkpw(password.encode(), stored.encode())
            except ValueError:
                ok = False
            if not ok:
                emit("warning", "account_close_rejected_wrong_password", cid, patientId=uid)
                return json_out(403, msg(cid, "That password is not correct"))
            try:
                cur.execute(
                    """UPDATE patients SET first_name = 'Closed', last_name = 'Account', date_of_birth = NULL,
                       gender = NULL, address = NULL, updated_at = UTC_TIMESTAMP(6) WHERE patient_id = %s""",
                    (uid,),
                )
                cur.execute(
                    """UPDATE users SET is_active = 0, email = %s, phone = NULL, password_hash = %s,
                       updated_at = UTC_TIMESTAMP(6) WHERE user_id = %s""",
                    (f"closed-{uid}-{secrets.token_hex(4)}@closed.medislot.invalid", unusable_password_hash(), uid),
                )
                conn.commit()
            except Exception:
                conn.rollback()
                raise
    revoke_tokens(uid, jti, data.get("refreshToken"), cid)
    emit("info", "patient_account_closed", cid, patientId=uid)
    return json_out(200, msg(cid, "Your account has been closed and your personal details erased"))
