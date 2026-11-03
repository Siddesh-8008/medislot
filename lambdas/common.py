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
            "queryStringParameters": _redact(event.get("queryStringParameters")),
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
