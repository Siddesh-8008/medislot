"""RDS connection check.

Connectivity-only: opens a connection, runs SELECT 1 and lists the tables it can see.
It never creates, alters or seeds tables. The schema lives in scehma/schema.sql and is applied by hand.
"""
import socket
import time

from common import config, dbconn, emit, instrument

# Tables the application expects. Missing ones are reported (and logged as a warning) but do not
# fail the check, so the pipeline still passes while you are re-creating tables by hand.
EXPECTED_TABLES = {
    "users", "patients", "hospitals", "specializations", "doctors",
    "consultation_types", "doctor_consultation_types",
    "availability_patterns", "blackout_dates",
}


def _tcp_probe(host, port, timeout=3):
    """Diagnose DNS and TCP connectivity problems."""
    result = {"host": host, "port": port}

    try:
        result["resolved_ips"] = sorted({
            item[4][0]
            for item in socket.getaddrinfo(
                host, port, socket.AF_INET, socket.SOCK_STREAM
            )
        })
    except Exception as exc:
        result["dns_error"] = repr(exc)
        return result

    start = time.time()

    try:
        with socket.create_connection((host, port), timeout=timeout):
            result["tcp_connect"] = "ok"
    except Exception as exc:
        result["tcp_connect"] = f"failed: {exc!r}"

    result["tcp_ms"] = int((time.time() - start) * 1000)
    return result


def _list_tables(conn):
    # SHOW TABLES returns one column whose name varies, so read the first value.
    with conn.cursor() as cur:
        cur.execute("SELECT 1 AS ok")
        cur.fetchone()
        cur.execute("SHOW TABLES")
        return sorted(next(iter(row.values())) for row in cur.fetchall())


@instrument("rds_check")
def handler(event, context):
    cid = (event or {}).get("correlationId")
    c = None
    conn = None

    try:
        c = config()
        conn = dbconn(c)

        tables = _list_tables(conn)
        missing = sorted(EXPECTED_TABLES - {t.lower() for t in tables})
        if missing:
            emit("warning", "rds_expected_tables_missing", cid, missing=missing)

        emit("info", "rds_connection_check_succeeded", cid, tables=tables)

        return {
            "statusCode": 200,
            "body": {
                "connected": True,
                "tables": tables,
                "missing_tables": missing,
            },
        }

    except Exception as exc:
        probe = {}
        if c and c.get("dbhost") and c.get("dbport"):
            probe = _tcp_probe(c["dbhost"], c["dbport"])

        emit("error", "rds_connection_check_failed", cid,
             error=repr(exc), probe=probe)

        return {
            "statusCode": 500,
            "body": {
                "connected": False,
                "error": repr(exc),
                "probe": probe,
            },
        }

    finally:
        if conn:
            conn.close()
