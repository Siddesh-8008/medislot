import json
import os
import time
import uuid
import logging
import functools
import traceback
import boto3
import pymysql
import hashlib
import re
import unicodedata
from datetime import date, datetime, time as dt_time, timedelta, timezone
from decimal import Decimal
from functools import lru_cache

log = logging.getLogger()
log.setLevel(logging.INFO)
ssm = boto3.client("ssm")
ddb = boto3.resource("dynamodb")


def emit(level, message, correlation_id=None, **fields):
    payload = {
        "level": level,
        "message": message,
        "correlationId": correlation_id or str(uuid.uuid4()),
        **fields,
    }
    getattr(log, level if level in ("info", "warning", "error") else "info")(
        json.dumps(payload)
    )


def response(status, body):
    return {
        "statusCode": status,
        "headers": {
            "content-type": "application/json",
            "cache-control": "no-store",
        },
        "body": json.dumps(body),
    }


def _json_default(o):
    """DynamoDB returns Decimal; MySQL returns Decimal / date / datetime / time / timedelta."""
    if isinstance(o, Decimal):
        return int(o) if o == o.to_integral_value() else float(o)
    if isinstance(o, (datetime, date, dt_time)):
        return o.isoformat()
    if isinstance(o, timedelta):
        total = int(o.total_seconds())
        return "%02d:%02d:%02d" % (total // 3600, total % 3600 // 60, total % 60)
    return str(o)


def json_out(status, data):
    """API Gateway proxy response that can serialise database / DynamoDB values."""
    return {
        "statusCode": status,
        "headers": {"content-type": "application/json", "cache-control": "no-store"},
        "body": json.dumps(data, default=_json_default),
    }


@lru_cache(maxsize=64)
def get_param(name, decrypt=False):
    return ssm.get_parameter(Name=name, WithDecryption=decrypt)["Parameter"]["Value"]


def config():
    env = os.environ.get("ENVIRONMENT", "dev")
    return {
        "env": env,
        "prefix": f"/medislot/{env}",
        "jwt": get_param(f"/medislot/{env}/auth/jwt-secret", True),
        "dbhost": get_param(f"/medislot/{env}/db/host"),
        "dbport": int(get_param(f"/medislot/{env}/db/port")),
        "dbname": get_param(f"/medislot/{env}/db/name"),
        "dbuser": get_param(f"/medislot/{env}/db/username"),
        "dbpass": get_param(f"/medislot/{env}/db/password", True),
    }


def db_config():
    """Database-only configuration (no JWT secret). Used by the read-only / profile Lambdas so their
    IAM role never needs access to the signing key."""
    env = os.environ.get("ENVIRONMENT", "dev")
    return {
        "env": env,
        "prefix": f"/medislot/{env}",
        "dbhost": get_param(f"/medislot/{env}/db/host"),
        "dbport": int(get_param(f"/medislot/{env}/db/port")),
        "dbname": get_param(f"/medislot/{env}/db/name"),
        "dbuser": get_param(f"/medislot/{env}/db/username"),
        "dbpass": get_param(f"/medislot/{env}/db/password", True),
    }


def dbconn(c):
    """Open a database connection. Schema changes are managed by schema.sql, not Lambda."""
    return pymysql.connect(
        host=c["dbhost"],
        port=c["dbport"],
        user=c["dbuser"],
        password=c["dbpass"],
        database=c["dbname"],
        connect_timeout=5,
        read_timeout=8,
        write_timeout=8,
        cursorclass=pymysql.cursors.DictCursor,
        autocommit=False,
    )


def table(name):
    return ddb.Table(name)


def auth_table():
    return table(f"MediSlot-{os.environ.get('ENVIRONMENT', 'dev')}-AuthTokens")


def token_hash(raw):
    return hashlib.sha256(raw.encode()).hexdigest()


def body(event):
    try:
        value = event.get("body") or "{}"
        if event.get("isBase64Encoded"):
            import base64
            value = base64.b64decode(value).decode()
        return json.loads(value)
    except Exception:
        return None


def correlation(event):
    return (
        (event.get("headers") or {}).get("x-correlation-id")
        or (event.get("requestContext") or {}).get("requestId")
        or str(uuid.uuid4())
    )


# --------------------------------------------------------------------------- CloudWatch event logging
# Every Lambda handler is wrapped with @instrument("<name>"). It writes one structured JSON log line
# when the function is invoked (what event arrived), one when it finishes (status code + duration),
# and one with the stack trace if it crashes. Secrets are never logged: request bodies, tokens,
# passwords and Authorization headers are dropped or masked.
_SENSITIVE = ("password", "token", "secret", "authorization", "cookie", "jwt")


def _redact(value, depth=0):
    if depth > 3:
        return "..."
    if isinstance(value, dict):
        return {
            k: ("***" if any(w in str(k).lower() for w in _SENSITIVE) else _redact(v, depth + 1))
            for k, v in list(value.items())[:30]
        }
    if isinstance(value, (list, tuple)):
        return [_redact(v, depth + 1) for v in list(value)[:20]]
    if isinstance(value, str):
        return value[:200]
    return value


def describe_event(event):
    """A log-safe summary of whatever triggered the Lambda."""
    if not isinstance(event, dict):
        return {"type": type(event).__name__}
    if "methodArn" in event:  # API Gateway TOKEN authorizer: never log authorizationToken
        return {"type": "apigateway-authorizer", "methodArn": event.get("methodArn")}
    if "httpMethod" in event or "requestContext" in event:
        rc = event.get("requestContext") or {}
        auth = rc.get("authorizer") or {}
        identity = rc.get("identity") or {}
        return {
            "type": "apigateway-http",
            "method": event.get("httpMethod"),
            "resource": event.get("resource"),
            "path": event.get("path") or event.get("rawPath"),
            # Names only: query values can be personal (a patient's doctor search says a lot about their health).
            "queryParameterNames": sorted((event.get("queryStringParameters") or {}))[:30],
            "requestId": rc.get("requestId"),
            "sourceIp": identity.get("sourceIp"),
            "userAgent": (identity.get("userAgent") or "")[:120],
            "userId": auth.get("userId"),
            "role": auth.get("role"),
            "hasBody": bool(event.get("body")),
        }
    return {"type": "direct-invoke", "event": _redact(event)}


def instrument(function_name):
    """Decorator that logs every invocation, result and failure of a Lambda handler to CloudWatch."""

    def decorator(fn):
        @functools.wraps(fn)
        def wrapper(event, context):
            cid = correlation(event) if isinstance(event, dict) else str(uuid.uuid4())
            started = time.time()
            request_id = getattr(context, "aws_request_id", None)
            emit("info", "lambda_invoked", cid, function=function_name,
                 awsRequestId=request_id, event=describe_event(event))
            try:
                result = fn(event, context)
            except Exception as exc:
                rejected = str(exc) == "Unauthorized"  # intentional authorizer rejection, not a crash
                emit("warning" if rejected else "error",
                     "lambda_rejected" if rejected else "lambda_failed", cid,
                     function=function_name, awsRequestId=request_id,
                     error=type(exc).__name__, durationMs=int((time.time() - started) * 1000),
                     **({} if rejected else {"traceback": traceback.format_exc()[-1800:]}))
                raise
            status = result.get("statusCode") if isinstance(result, dict) else None
            level = "info"
            if isinstance(status, int):
                level = "error" if status >= 500 else "warning" if status >= 400 else "info"
            emit(level, "lambda_completed", cid, function=function_name, awsRequestId=request_id,
                 statusCode=status, durationMs=int((time.time() - started) * 1000))
            return result

        return wrapper

    return decorator


# --------------------------------------------------------------------------- patient field rules
# One set of rules shared by registration (authentication Lambda) and the profile API (patient_domain
# Lambda), so a value that registers can always be edited and a value that is rejected is rejected everywhere.
GENDERS = ("Male", "Female", "Other", "Prefer not to say")
_GENDER_LOOKUP = {g.lower(): g for g in GENDERS}
_NAME_FORBIDDEN = set('<>@#$%^&*()=+[]{}|\\/;:"~`_?!0123456789')
_PHONE_RE = re.compile(r"[0-9]{10}")
_DOB_RE = re.compile(r"[0-9]{4}-[0-9]{2}-[0-9]{2}")
_EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PATIENT_FIELDS = ("firstName", "lastName", "dateOfBirth", "gender", "address", "phone")
PATIENT_MANDATORY = ("firstName", "lastName", "dateOfBirth", "gender", "phone")


class FieldErrors(ValueError):
    """One or more fields failed validation. `errors` maps field name -> human-readable message."""

    def __init__(self, errors):
        self.errors = dict(errors)
        super().__init__("; ".join(self.errors.values()))


def _has_control_chars(text):
    return any(unicodedata.category(ch).startswith("C") for ch in text)


def clean_person_name(value, label):
    if not isinstance(value, str):
        raise ValueError(f"{label} is required")
    text = " ".join(value.split())  # trims and collapses runs of whitespace
    if not text:
        raise ValueError(f"{label} is required")
    if len(text) > 75:
        raise ValueError(f"{label} must be 75 characters or fewer")
    if _has_control_chars(text) or any(ch in _NAME_FORBIDDEN for ch in text) or not any(ch.isalpha() for ch in text):
        raise ValueError(f"{label} can contain only letters, spaces, apostrophes, hyphens and full stops")
    return text


def clean_phone(value):
    text = value.strip() if isinstance(value, str) else ""
    if not _PHONE_RE.fullmatch(text):
        raise ValueError("phone must be exactly 10 digits")
    return text


def clean_date_of_birth(value, today=None):
    text = value.strip() if isinstance(value, str) else ""
    if not _DOB_RE.fullmatch(text):
        raise ValueError("dateOfBirth is required in YYYY-MM-DD format")
    try:
        dob = date(int(text[0:4]), int(text[5:7]), int(text[8:10]))
    except ValueError:
        raise ValueError("dateOfBirth must be a real calendar date (YYYY-MM-DD)")
    today = today or datetime.now(timezone.utc).date()
    if dob > today + timedelta(days=1):  # one day of slack for time zones ahead of UTC
        raise ValueError("dateOfBirth cannot be in the future")
    if (today - dob).days > 120 * 366:
        raise ValueError("dateOfBirth is more than 120 years ago; please check the year")
    return dob.isoformat()


def clean_gender(value):
    canonical = _GENDER_LOOKUP.get(value.strip().lower()) if isinstance(value, str) else None
    if not canonical:
        raise ValueError("gender must be one of: " + ", ".join(GENDERS))
    return canonical


def clean_address(value):
    """Optional. Blank / null clears it."""
    if value is None:
        return None
    if not isinstance(value, str):
        raise ValueError("address must be text")
    text = " ".join(value.split())
    if len(text) > 500:
        raise ValueError("address must be 500 characters or fewer")
    if _has_control_chars(text):
        raise ValueError("address contains characters that are not allowed")
    return text or None


def clean_patient_fields(data, required=(), allowed=PATIENT_FIELDS, today=None):
    """Validate the patient fields present in `data`.

    Returns {fieldName: cleaned value} for every allowed key present. Keys outside `allowed` are
    rejected (so email, role, patientId... can never be written through the profile API). Fields in
    `required` must be present and valid. Raises FieldErrors listing every problem at once."""
    errors = {}
    if not isinstance(data, dict):
        raise FieldErrors({"body": "A JSON object is required"})
    cleaners = {
        "firstName": lambda v: clean_person_name(v, "First name"),
        "lastName": lambda v: clean_person_name(v, "Last name"),
        "dateOfBirth": lambda v: clean_date_of_birth(v, today),
        "gender": clean_gender,
        "address": clean_address,
        "phone": clean_phone,
    }
    for key in data:
        if key not in allowed:
            errors[key] = f"{key} cannot be set here"
    out = {}
    for key in allowed:
        if key not in data:
            if key in required:
                errors[key] = {
                    "firstName": "First name is required", "lastName": "Last name is required",
                    "dateOfBirth": "dateOfBirth is required in YYYY-MM-DD format",
                    "gender": "gender must be one of: " + ", ".join(GENDERS),
                    "phone": "phone must be exactly 10 digits",
                }.get(key, f"{key} is required")
            continue
        try:
            out[key] = cleaners[key](data[key])
        except ValueError as exc:
            errors[key] = str(exc)
    if errors:
        raise FieldErrors(errors)
    return out


def valid_email(value):
    return isinstance(value, str) and len(value) <= 254 and bool(_EMAIL_RE.fullmatch(value))
