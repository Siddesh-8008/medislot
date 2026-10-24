from common import config, dbconn, emit
def handler(event, context):
    cid=(event or {}).get("correlationId")
    conn=None
    try:
        c=config()
        conn=dbconn(c)
        with conn.cursor() as cur:
            cur.execute("SELECT 1 AS connected")
            row=cur.fetchone()
        emit("info","rds_connection_check_succeeded",cid)
        return {"statusCode":200,"body":{"connected":bool(row and row["connected"]==1)}}
    except Exception:
        emit("error","rds_connection_check_failed",cid)
        raise
    finally:
        if conn: conn.close()
