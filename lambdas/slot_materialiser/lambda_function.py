import os, json, logging, hashlib
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo
import boto3
from boto3.dynamodb.conditions import Key
from common import dbconn, emit, get_param

log=logging.getLogger()
log.setLevel(logging.INFO)
ddb=boto3.resource("dynamodb")

def resolve_local(local_naive, zone):
    """Resolve wall time with PEP-495 fold=0; skip nonexistent spring-forward times."""
    aware=local_naive.replace(tzinfo=zone,fold=0)
    utc=aware.astimezone(timezone.utc)
    roundtrip=utc.astimezone(zone).replace(tzinfo=None)
    if roundtrip != local_naive:
        return None
    return utc

def stable_slot_id(doctor_id, consultation_type_id, start_iso):
    stable=f"{doctor_id}#{consultation_type_id}#{start_iso}"
    return "SLOT#"+hashlib.sha256(stable.encode()).hexdigest()[:40]

def handler(event, context):
    env=os.environ.get("ENVIRONMENT","dev")
    prefix=f"/medislot/{env}"
    c={"env":env,"prefix":prefix,
       "dbhost":get_param(f"{prefix}/db/host"),
       "dbport":int(get_param(f"{prefix}/db/port")),
       "dbname":get_param(f"{prefix}/db/name"),
       "dbuser":get_param(f"{prefix}/db/username"),
       "dbpass":get_param(f"{prefix}/db/password",True)}
    days=int(os.environ.get("MATERIALISE_DAYS","30"))
    today_utc=datetime.now(timezone.utc).date()
    now=datetime.now(timezone.utc)
    table=ddb.Table(f"MediSlot-{c['env']}-Slots")
    made=0; skipped=0; errors=[]
    try:
        with dbconn(c) as conn:
            with conn.cursor() as cur:
                cur.execute("""SELECT d.doctor_id,d.timezone,p.pattern_id,p.consultation_type_id,p.day_of_week,
                    p.start_time,p.end_time,ct.duration_minutes,ct.fee,ct.capacity
                    FROM doctors d JOIN availability_patterns p ON p.doctor_id=d.doctor_id
                    JOIN consultation_types ct ON ct.consultation_type_id=p.consultation_type_id
                    WHERE p.is_active=1 AND ct.is_active=1""")
                patterns=cur.fetchall()
                for p in patterns:
                    try: zone=ZoneInfo(p["timezone"])
                    except Exception:
                        errors.append({"doctorId":int(p["doctor_id"]),"reason":"invalid_timezone"}); continue
                    cur.execute("SELECT blackout_date FROM blackout_dates WHERE doctor_id=%s",(p["doctor_id"],))
                    blackouts={x["blackout_date"].isoformat() for x in cur.fetchall()}
                    for offset in range(days):
                        day=today_utc+timedelta(days=offset)
                        if day.isoweekday()!=int(p["day_of_week"]) or day.isoformat() in blackouts: continue
                        start_time=p["start_time"] if isinstance(p["start_time"],time) else time.fromisoformat(str(p["start_time"]))
                        end_time=p["end_time"] if isinstance(p["end_time"],time) else time.fromisoformat(str(p["end_time"]))
                        cursor=datetime.combine(day,start_time)
                        end_wall=datetime.combine(day,end_time)
                        step=timedelta(minutes=int(p["duration_minutes"]))
                        while cursor+step<=end_wall:
                            start_utc=resolve_local(cursor,zone)
                            end_utc=resolve_local(cursor+step,zone)
                            if start_utc is None or end_utc is None or end_utc<=start_utc:
                                skipped+=1; cursor+=step; continue
                            if start_utc<=now:
                                cursor+=step; continue
                            start_iso=start_utc.isoformat(timespec="seconds").replace("+00:00","Z")
                            end_iso=end_utc.isoformat(timespec="seconds").replace("+00:00","Z")
                            slot_id=stable_slot_id(p["doctor_id"],p["consultation_type_id"],start_iso)
                            item={"slot_id":slot_id,"doctor_id":int(p["doctor_id"]),"consultation_type_id":int(p["consultation_type_id"]),
                                "doctor_date":f"DOCTOR#{p['doctor_id']}#{day.isoformat()}","start_at":start_iso,"end_at":end_iso,
                                "timezone":p["timezone"],"capacity":int(p["capacity"]),"booked_count":0,"held_count":0,
                                "version":1,"status":"OPEN","materialised_at":now.isoformat(timespec="seconds").replace("+00:00","Z"),
                                "local_date":day.isoformat(),"local_start_time":cursor.time().isoformat(timespec="minutes"),
                                "utc_offset":int(cursor.replace(tzinfo=zone,fold=0).utcoffset().total_seconds())}
                            try:
                                table.put_item(Item=item,ConditionExpression="attribute_not_exists(slot_id)")
                                made+=1
                            except Exception as e:
                                if getattr(e,"response",{}).get("Error",{}).get("Code")=="ConditionalCheckFailedException":
                                    skipped+=1
                                else:
                                    errors.append({"doctorId":int(p["doctor_id"]),"date":day.isoformat(),"error":type(e).__name__})
                            cursor+=step
        return {"statusCode":200,"body":json.dumps({"created":made,"skippedOrExisting":skipped,"errors":errors[:20],"windowDays":days,"asOf":now.isoformat()})}
    except Exception as e:
        emit("error","slot_materialiser_failed",error=type(e).__name__)
        raise
