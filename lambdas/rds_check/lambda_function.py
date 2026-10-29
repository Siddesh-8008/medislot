import socket, time
from common import config, dbconn, emit


def _tcp_probe(host, port, timeout=3):
    """Raw TCP test so we can tell DNS / routing / security-group problems from MySQL auth problems."""
    result = {"host": host, "port": port}
    try:
        result["resolved_ips"] = sorted({ai[4][0] for ai in socket.getaddrinfo(host, port, socket.AF_INET, socket.SOCK_STREAM)})
    except Exception as e:
        result["dns_error"] = repr(e)
        return result
    start = time.time()
    try:
        with socket.create_connection((host, port), timeout=timeout):
            result["tcp_connect"] = "ok"
    except Exception as e:
        result["tcp_connect"] = f"failed: {e!r}"
    result["tcp_ms"] = int((time.time() - start) * 1000)
    return result


def handler(event, context):
    cid = (event or {}).get("correlationId")
    conn = None
    c = None
    try:
        c = config()
        conn = dbconn(c)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS connected")
            row = cur.fetchone()
        emit("info", "rds_connection_check_succeeded", cid)
        return {"statusCode": 200, "body": {"connected": bool(row and row["connected"] == 1)}}
    except Exception as exc:
        probe = _tcp_probe(c["dbhost"], c["dbport"]) if c else {}
        emit("error", "rds_connection_check_failed", cid, error=repr(exc), probe=probe)
        # Return (not raise) so the workflow prints the diagnostics; its assertion still fails the job.
        return {"statusCode": 500, "body": {"connected": False, "error": repr(exc), "probe": probe}}
    finally:
        if conn:
            conn.close()