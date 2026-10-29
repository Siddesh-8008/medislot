import os, re, time, uuid, bcrypt, jwt, secrets
from botocore.exceptions import ClientError
from common import config, dbconn, body, response, emit, correlation, auth_table, token_hash, get_param

LICENSE_RE = re.compile(r"^[A-Z][A-Z ]{1,29}-\d{4}$")  # Example: TS-1234 or Telangana-1234
EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")

def _issue_access(user_id, role, secret, minutes, now=None):
    now = int(now or time.time())
    claims = {"sub": str(user_id), "userId": str(user_id), "role": role,
              "iss": "medislot", "aud": "medislot-api", "iat": now,
              "exp": now + int(minutes) * 60, "jti": str(uuid.uuid4())}
    return jwt.encode(claims, secret, algorithm="HS256")

def _register(event, context, role, cid):
    data = body(event)
    if not isinstance(data, dict):
        return response(400, {"message": "Invalid JSON body", "correlationId": cid})

    first = str(data.get("firstName", "")).strip()
    last = str(data.get("lastName", "")).strip()
    # Accept fullName for older clients, but store the split name in the profile table.
    if not first and not last and str(data.get("fullName", "")).strip():
        parts = str(data.get("fullName", "")).strip().split(None, 1)
        first = parts[0]
        last = parts[1] if len(parts) > 1 else ""

    email = str(data.get("email", "")).strip().lower()
    password = data.get("password", "")
    phone = str(data.get("phone", "")).strip() or None
    dob = str(data.get("dateOfBirth", "")).strip() or None
    gender = str(data.get("gender", "")).strip() or None
    address = str(data.get("address", "")).strip() or None

    if (
        not first or not last or len(first) > 75 or len(last) > 75
        or not EMAIL_RE.fullmatch(email) or len(email) > 254
    ):
        return response(400, {
            "message": "firstName, lastName and a valid email are required",
            "correlationId": cid,
        })

    if not isinstance(password, str):
        password = ""
    min_len = int(get_param(
        f"/medislot/{os.environ.get('ENVIRONMENT', 'dev')}/auth/password-min-length"
    ))
    if (
        len(password) < min_len or len(password.encode()) > 72
        or not re.search(r"[a-z]", password)
        or not re.search(r"[A-Z]", password)
        or not re.search(r"\d", password)
        or not re.search(r"[^A-Za-z0-9]", password)
    ):
        return response(400, {
            "message": f"Password must be {min_len}-72 characters and include upper, lower, number and symbol",
            "correlationId": cid,
        })

    specialization = None
    license_number = None
    clinic_address = None
    years = None
    if role == "DOCTOR":
        specialization = str(data.get("specialization", "")).strip()
        license_number = str(data.get("licenseNumber", "")).strip().upper()
        clinic_address = str(data.get("clinicAddress", "")).strip() or None
        years = data.get("yearsExperience")

        if not specialization or len(specialization) > 120:
            return response(400, {
                "message": "specialization is required for doctor registration",
                "correlationId": cid,
            })
        if not LICENSE_RE.fullmatch(license_number):
            return response(400, {
                "message": "licenseNumber must match state-1234 format (for example, TS-1234 or Telangana-1234)",
                "correlationId": cid,
            })
        if years is not None:
            try:
                years = int(years)
            except (TypeError, ValueError):
                years = -1
            if years < 0 or years > 80:
                return response(400, {
                    "message": "yearsExperience must be between 0 and 80",
                    "correlationId": cid,
                })

    c = config()
    password_hash = bcrypt.hashpw(password.encode(), bcrypt.gensalt(rounds=12)).decode()
    conn = dbconn(c)
    try:
        with conn.cursor() as cur:
            # The users table stores identity/authentication fields only.
            cur.execute(
                "INSERT INTO users (email, password_hash, role) VALUES (%s, %s, %s)",
                (email, password_hash, role),
            )
            user_id = cur.lastrowid

            # Profile insert shares the same transaction. If it fails, the user insert rolls back too.
            if role == "PATIENT":
                cur.execute(
                    """INSERT INTO patients
                       (patient_id, first_name, last_name, phone, date_of_birth, gender, address)
                       VALUES (%s, %s, %s, %s, %s, %s, %s)""",
                    (user_id, first, last, phone, dob, gender, address),
                )
            else:
                cur.execute(
                    """INSERT INTO doctors
                       (doctor_id, first_name, last_name, phone, specialization,
                        license_number, years_experience, clinic_address)
                       VALUES (%s, %s, %s, %s, %s, %s, %s, %s)""",
                    (user_id, first, last, phone, specialization,
                     license_number, years, clinic_address),
                )
        conn.commit()
    except Exception as exc:
        conn.rollback()
        message = str(exc).lower()
        if "duplicate entry" in message:
            return response(409, {
                "message": "Unable to register with those details",
                "correlationId": cid,
            })
        if "incorrect date" in message or "invalid date" in message or "date value" in message:
            return response(400, {
                "message": "dateOfBirth must use YYYY-MM-DD format",
                "correlationId": cid,
            })
        raise
    finally:
        conn.close()

    emit("info", "registration_succeeded", cid, role=role, userId=str(user_id))
    return response(201, {
        "userId": str(user_id),
        "role": role,
        "message": "Registration successful",
    })

def _login(event, cid):
    data=body(event)
    if not isinstance(data,dict): return response(400,{"message":"Invalid JSON body","correlationId":cid})
    email=str(data.get("email"," ")).strip().lower(); password=data.get("password","")
    if not email or not isinstance(password,str): return response(400,{"message":"Email and password are required","correlationId":cid})
    c=config(); conn=dbconn(c)
    try:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id,password_hash,role FROM users WHERE email=%s", (email,)); user=cur.fetchone()
    finally: conn.close()
    if not user or not bcrypt.checkpw(password.encode(), user["password_hash"].encode()):
        emit("warning","login_rejected",cid); return response(401,{"message":"Invalid email or password","correlationId":cid})
    now=int(time.time()); minutes=int(get_param(f"{c['prefix']}/auth/access-token-minutes")); days=int(get_param(f"{c['prefix']}/auth/refresh-token-days"))
    access=_issue_access(user["user_id"],user["role"],c["jwt"],minutes,now)
    raw=secrets.token_urlsafe(48)
    auth_table().put_item(Item={"token_key":"REFRESH#"+token_hash(raw),"token_type":"REFRESH","user_id":str(user["user_id"]),"role":user["role"],"created_at":now,"expires_at":now+days*86400,"status":"ACTIVE"})
    emit("info","login_succeeded",cid,userId=str(user["user_id"]),role=user["role"])
    return response(200,{"accessToken":access,"tokenType":"Bearer","expiresIn":minutes*60,"refreshToken":raw,"refreshExpiresIn":days*86400,"userId":str(user["user_id"]),"role":user["role"]})

def _refresh(event, cid):
    data=body(event); raw=data.get("refreshToken") if isinstance(data,dict) else None
    if not isinstance(raw,str) or not raw: return response(400,{"message":"refreshToken is required","correlationId":cid})
    c=config(); key="REFRESH#"+token_hash(raw); table=auth_table(); item=table.get_item(Key={"token_key":key,"token_type":"REFRESH"}).get("Item"); now=int(time.time())
    if not item or item.get("status")!="ACTIVE" or int(item.get("expires_at",0))<=now:
        return response(401,{"message":"Invalid or revoked refresh token","correlationId":cid})
    # Keep the same refresh token and original fixed 7-day expiry. Only access JWT rotates.
    minutes=int(get_param(f"{c['prefix']}/auth/access-token-minutes"))
    access=_issue_access(item["user_id"],item["role"],c["jwt"],minutes,now)
    return response(200,{"accessToken":access,"tokenType":"Bearer","expiresIn":minutes*60,"refreshToken":raw,"refreshExpiresAt":int(item["expires_at"]),"userId":item["user_id"],"role":item["role"]})

def _logout(event, cid):
    data=body(event); raw=data.get("refreshToken") if isinstance(data,dict) else None
    headers=event.get("headers") or {}; auth=headers.get("authorization") or headers.get("Authorization","")
    if not auth.startswith("Bearer "): return response(401,{"message":"Bearer access token required","correlationId":cid})
    c=config(); claims=jwt.decode(auth[7:],c["jwt"],algorithms=["HS256"],issuer="medislot",audience="medislot-api")
    if isinstance(raw,str) and raw:
        key="REFRESH#"+token_hash(raw)
        try:
            table=auth_table(); item=table.get_item(Key={"token_key":key,"token_type":"REFRESH"}).get("Item")
            if item and item.get("user_id")==claims["sub"]:
                table.update_item(Key={"token_key":key,"token_type":"REFRESH"},UpdateExpression="SET #s=:r",ConditionExpression="attribute_exists(token_key)",ExpressionAttributeNames={"#s":"status"},ExpressionAttributeValues={":r":"REVOKED"})
        except Exception: pass
    auth_table().put_item(Item={"token_key":"JTI#"+claims["jti"],"token_type":"DENYLIST","user_id":claims["sub"],"created_at":int(time.time()),"expires_at":int(claims["exp"]),"status":"REVOKED"})
    return response(200,{"message":"Logged out"})

def handler(event, context):
    cid=correlation(event)
    path=(event.get("path") or event.get("rawPath") or "").rstrip("/").lower()
    method=((event.get("httpMethod") or (event.get("requestContext") or {}).get("http",{}).get("method") or "").upper())
    try:
        if method=="POST" and path.endswith("/auth/register/patient"): return _register(event,context,"PATIENT",cid)
        if method=="POST" and path.endswith("/auth/register/doctor"): return _register(event,context,"DOCTOR",cid)
        if method=="POST" and path.endswith("/auth/login"): return _login(event,cid)
        if method=="POST" and path.endswith("/auth/refresh"): return _refresh(event,cid)
        if method=="POST" and path.endswith("/auth/logout"): return _logout(event,cid)
        return response(404,{"message":"Unknown authentication route","correlationId":cid})
    except Exception:
        emit("error","authentication_request_failed",cid)
        if path.endswith("/auth/logout"):
            return response(401,{"message":"Invalid access token","correlationId":cid})
        if path.endswith("/auth/refresh"):
            return response(401,{"message":"Invalid or revoked refresh token","correlationId":cid})
        return response(500,{"message":"Internal server error","correlationId":cid})
