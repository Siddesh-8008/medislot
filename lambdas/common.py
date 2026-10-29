import json
import os
import uuid
import logging
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
