import json, os, re, uuid, logging, traceback
from decimal import Decimal
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import boto3
import pymysql
from botocore.exceptions import ClientError
from common import config, dbconn, body, response, emit, correlation, instrument

log=logging.getLogger()
log.setLevel(logging.INFO)
s3=boto3.client("s3", config=boto3.session.Config(signature_version="s3v4"))
lambda_client=boto3.client("lambda",config=boto3.session.Config(connect_timeout=2,read_timeout=3,retries={"max_attempts":1}))
# Synchronous call to the booking Lambda for the blackout sweep: it must finish inside the API Gateway limit (29 s).
booking_client=boto3.client("lambda",config=boto3.session.Config(connect_timeout=2,read_timeout=15,retries={"max_attempts":1}))

def cancel_blackout_day(uid,day,reason,cid):
    """A blackout must also cancel what is ALREADY booked on that day. The booking Lambda owns appointments, refunds and
    the slot counters, so it does the work (same code path as the doctor's 'Cancel slot' button: slot CANCELLED,
    every patient refunded 100 %). Returns its summary dict, or None when it could not be reached."""
    name=os.environ.get("BOOKING_FUNCTION")
    if not name:
        emit("warning","blackout_sweep_not_configured",cid,doctorId=uid); return None
    try:
        res=booking_client.invoke(FunctionName=name,InvocationType="RequestResponse",
            Payload=json.dumps({"action":"cancelBlackoutDay","doctorId":uid,"date":day.isoformat(),"reason":reason or "",
                                "headers":{"x-correlation-id":cid}}).encode())
        if res.get("FunctionError"):
            emit("error","blackout_sweep_failed",cid,doctorId=uid,date=day.isoformat(),error=res.get("FunctionError")); return None
        out=json.loads(res["Payload"].read() or b"{}")
        return out if isinstance(out,dict) else None
    except Exception as e:
        emit("error","blackout_sweep_failed",cid,doctorId=uid,date=day.isoformat(),error=type(e).__name__); return None

def doctor_today(cur,uid):
    """Today's date in the doctor's own timezone (blackout dates are local calendar days)."""
    cur.execute("SELECT timezone FROM doctors WHERE doctor_id=%s",(uid,))
    try: return datetime.now(ZoneInfo((cur.fetchone() or {}).get("timezone") or "Asia/Kolkata")).date()
    except (ZoneInfoNotFoundError,ValueError): return datetime.now(ZoneInfo("Asia/Kolkata")).date()

def blackout_reply(status,day,sweep,cid,already=False,base=None,**extra):
    base=base or ("This date was already blocked." if already else "Blackout date added.")
    if sweep is None:
        text=base+" Existing appointments on this day could NOT be cancelled automatically right now - add the same date again to retry."
    else:
        n=int(sweep.get("cancelledAppointments") or 0)
        text=base+(f" {n} booked appointment{'s' if n!=1 else ''} on this day {'were' if n!=1 else 'was'} cancelled and refunded in full." if n else " No booked appointments needed cancelling.")
        if sweep.get("skippedSlots"): text+=" Some slots could not be cancelled - add the same date again to retry."
    return out(status,{"message":text,"blackoutDate":day.isoformat(),"alreadyExisted":already,
                       "cancelledAppointments":(sweep or {}).get("cancelledAppointments",0),"sweepOk":bool(sweep and sweep.get("ok")),
                       **extra,"correlationId":cid})

def refresh_slots(uid,cid):
    """Ask the slot materialiser to rebuild this doctor's bookable slots. Fire-and-forget:
    the doctor's save has already been committed, so a failure here must never fail the request
    (the daily run still extends the window and a later change re-triggers the reconcile)."""
    name=os.environ.get("SLOT_MATERIALISER_FUNCTION")
    if not name: return
    try:
        lambda_client.invoke(FunctionName=name,InvocationType="Event",Payload=json.dumps({"doctorId":uid}).encode())
    except Exception as e:
        emit("warning","slot_refresh_trigger_failed",cid,error=type(e).__name__)

DAY_NAMES=["","Monday","Tuesday","Wednesday","Thursday","Friday","Saturday","Sunday"]

def as_time(v):
    """MySQL TIME columns arrive from pymysql as timedelta; API input arrives as 'HH:MM' strings."""
    if isinstance(v,time): return v
    if isinstance(v,timedelta):
        secs=int(v.total_seconds()); return time(secs//3600%24,secs%3600//60,secs%60)
    h,_,rest=str(v).strip().partition(":")
    return time.fromisoformat(f"{int(h):02d}:{rest}")

def hhmm(v):
    return as_time(v).strftime("%H:%M")

def pattern_problem(cur,uid,tid,dow,start,end,exclude_pid=None):
    """Validate a weekly pattern. Returns (http_status, message) or None when it is fine.
    1) the window must fit at least one visit of the consultation type;
    2) it must not overlap another active pattern of the same doctor on the same weekday
       (overlapping patterns are what produced overlapping / duplicate-looking slots)."""
    cur.execute("""SELECT ct.duration_minutes FROM doctor_consultation_types dct
        JOIN consultation_types ct ON ct.consultation_type_id=dct.consultation_type_id
        WHERE dct.consultation_type_id=%s AND dct.doctor_id=%s""",(tid,uid))
    ct=cur.fetchone()
    if not ct: return 422,"Consultation type not found"
    window=(end.hour*60+end.minute)-(start.hour*60+start.minute)
    need=int(ct["duration_minutes"])
    if window<need:
        return 422,f"This consultation type takes {need} minutes, but the window {hhmm(start)}-{hhmm(end)} is only {window} minutes. Choose a window of at least {need} minutes."
    sql="SELECT pattern_id,start_time,end_time FROM availability_patterns WHERE doctor_id=%s AND day_of_week=%s AND is_active=1 AND start_time<%s AND end_time>%s"
    args=[uid,dow,end.strftime("%H:%M:%S"),start.strftime("%H:%M:%S")]
    if exclude_pid: sql+=" AND pattern_id<>%s"; args.append(exclude_pid)
    cur.execute(sql+" LIMIT 1",args)
    o=cur.fetchone()
    if o:
        return 409,f"This overlaps your existing {DAY_NAMES[dow]} hours {hhmm(o['start_time'])}-{hhmm(o['end_time'])}. Remove or edit that pattern first, or choose different hours."
    return None

def ctx_user(event):
    c=(event.get("requestContext") or {}).get("authorizer") or {}
    return int(c.get("userId") or 0), c.get("role")

def route(event):
    p=event.get("pathParameters") or {}
    proxy=p.get("proxy")
    if proxy is not None:
        # The API resource is /doctors/{proxy+}, so proxy omits the "/doctors" prefix.
        return "/doctors/" + str(proxy).strip("/")
    path=event.get("path") or event.get("rawPath") or ""
    path=re.sub(r"^/dev(?=/|$)","",path)
    path=re.sub(r"^/api/v1","",path)
    return "/" + str(path).strip("/")

def _json_default(o):
    # DynamoDB returns Decimal; MySQL returns Decimal/datetime/date/time/timedelta.
    if isinstance(o,Decimal):
        return int(o) if o==o.to_integral_value() else float(o)
    if isinstance(o,(datetime,date,time)):
        return o.isoformat()
    if isinstance(o,timedelta):
        total=int(o.total_seconds()); return "%02d:%02d:%02d"%(total//3600,total%3600//60,total%60)
    return str(o)

def out(status, data):
    return {"statusCode":status,
            "headers":{"content-type":"application/json","cache-control":"no-store"},
            "body":json.dumps(data,default=_json_default)}

def parse_body(event):
    v=body(event)
    return v if isinstance(v,dict) else None

def doctor_exists(cur, uid):
    cur.execute("SELECT doctor_id FROM doctors WHERE doctor_id=%s", (uid,))
    return cur.fetchone() is not None

@instrument("doctor_domain")
def handler(event, context):
    cid=correlation(event)
    uid, role=ctx_user(event)
    if role != "DOCTOR" or uid <= 0:
        emit("warning","doctor_access_denied",cid,role=role)
        return out(403,{"message":"Doctor role required","correlationId":cid})
    method=(event.get("httpMethod") or event.get("requestContext",{}).get("http",{}).get("method") or "GET").upper()
    path=route(event)
    data=parse_body(event)
    try:
        c=config()
        if path == "/doctors/me/photo-upload-url" and method=="POST":
            if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
            filename=str(data.get("fileName","")).strip()
            content_type=str(data.get("contentType","")).lower().strip()
            allowed={"image/jpeg":"jpg","image/png":"png","image/webp":"webp"}
            if content_type not in allowed or not filename or len(filename)>180:
                return out(422,{"message":"Use JPEG, PNG or WebP image","correlationId":cid})
            key=f"doctors/{uid}/{uuid.uuid4().hex}.{allowed[content_type]}"
            emit("info","photo_upload_url_issued",cid,doctorId=uid,contentType=content_type)
            bucket=os.environ["DOCTOR_PHOTOS_BUCKET"]
            url=s3.generate_presigned_url("put_object",Params={"Bucket":bucket,"Key":key,"ContentType":content_type,"ServerSideEncryption":"AES256"},ExpiresIn=300)
            return out(200,{"uploadUrl":url,"photoKey":key,"expiresInSeconds":300,"contentType":content_type,"correlationId":cid})
        with dbconn(c) as conn:
            with conn.cursor() as cur:
                # Master list of the standard consultation types a doctor can choose from.
                if path=="/doctors/catalog/consultation-types" and method=="GET":
                    cur.execute("SELECT consultation_type_id,name,duration_minutes,min_capacity,max_capacity FROM consultation_types WHERE is_active=1 ORDER BY consultation_type_id")
                    items=cur.fetchall()
                    emit("info","consultation_type_catalog_read",cid,doctorId=uid,count=len(items))
                    return out(200,{"items":items,"correlationId":cid})
                if path=="/doctors/me" and method=="POST":
                    if doctor_exists(cur,uid):
                        return out(409,{"message":"Doctor profile already exists","correlationId":cid})
                    if data is None:
                        return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    required=("firstName","lastName","hospitalId","specializationId","licenseNumber")
                    if any(not str(data.get(k,"")).strip() for k in required):
                        return out(422,{"message":"firstName, lastName, hospitalId, specializationId and licenseNumber are required","correlationId":cid})
                    tz=str(data.get("timezone","Asia/Kolkata"))
                    try: ZoneInfo(tz)
                    except ZoneInfoNotFoundError: return out(422,{"message":"timezone must be a valid IANA timezone","correlationId":cid})
                    langs=json.dumps(data.get("languagesSpoken")) if data.get("languagesSpoken") is not None else None
                    cur.execute("""INSERT INTO doctors(doctor_id,first_name,last_name,hospital_id,specialization_id,license_number,
                        years_experience,clinic_address,qualifications,languages_spoken,timezone)
                        VALUES(%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s)""",
                        (uid,data["firstName"].strip(),data["lastName"].strip(),int(data["hospitalId"]),int(data["specializationId"]),
                         data["licenseNumber"].strip(),int(data.get("yearsExperience",0)),data.get("clinicAddress"),data.get("qualifications"),langs,tz))
                    conn.commit()
                    emit("info","doctor_profile_created",cid,doctorId=uid)
                    return out(201,{"message":"Doctor profile created","doctorId":uid,"correlationId":cid})
                if not doctor_exists(cur,uid):
                    return out(404,{"message":"Doctor profile not found; create it first","correlationId":cid})
                # Profile read/update
                if path=="/doctors/me" and method=="GET":
                    cur.execute("""SELECT d.doctor_id,d.first_name,d.last_name,d.hospital_id,h.hospital_name,
                        d.specialization_id,s.specialization_name,d.license_number,d.years_experience,
                        d.clinic_address,d.qualifications,d.languages_spoken,d.timezone,d.photo_key,
                        d.created_at,d.updated_at FROM doctors d
                        LEFT JOIN hospitals h ON h.hospital_id=d.hospital_id
                        LEFT JOIN specializations s ON s.specialization_id=d.specialization_id
                        WHERE d.doctor_id=%s""",(uid,))
                    row=cur.fetchone()
                    if row and row.get("languages_spoken") is not None and isinstance(row["languages_spoken"],str):
                        try: row["languages_spoken"]=json.loads(row["languages_spoken"])
                        except Exception: pass
                    return out(200,{"profile":row,"correlationId":cid})
                if path=="/doctors/me" and method in ("POST","PUT","PATCH"):
                    if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    allowed={"firstName":"first_name","lastName":"last_name","hospitalId":"hospital_id","specializationId":"specialization_id","licenseNumber":"license_number","yearsExperience":"years_experience","clinicAddress":"clinic_address","qualifications":"qualifications","languagesSpoken":"languages_spoken","timezone":"timezone","photoKey":"photo_key"}
                    sets=[]; vals=[]
                    for k,col in allowed.items():
                        if k in data:
                            v=data[k]
                            if k=="languagesSpoken": v=json.dumps(v) if v is not None else None
                            if k=="timezone":
                                try: ZoneInfo(str(v))
                                except (ZoneInfoNotFoundError,TypeError): return out(422,{"message":"timezone must be a valid IANA timezone","correlationId":cid})
                            if k in ("firstName","lastName","licenseNumber") and not str(v or "").strip():
                                return out(422,{"message":f"{k} cannot be empty","correlationId":cid})
                            sets.append(f"{col}=%s"); vals.append(v)
                    if not sets: return out(400,{"message":"No supported profile fields supplied","correlationId":cid})
                    vals.append(uid)
                    cur.execute(f"UPDATE doctors SET {', '.join(sets)}, updated_at=UTC_TIMESTAMP(6) WHERE doctor_id=%s",vals)
                    conn.commit()
                    emit("info","doctor_profile_updated",cid,doctorId=uid,fields=sorted(k for k in data if k in allowed))
                    if "timezone" in data: refresh_slots(uid,cid)
                    return out(200,{"message":"Doctor profile updated","correlationId":cid})
                # Consultation types the doctor offers: a choice from the master consultation_types table,
                # stored with the doctor's own fee and capacity in doctor_consultation_types.
                prefix="/doctors/me/consultation-types"
                if path==prefix and method=="GET":
                    cur.execute("""SELECT dct.doctor_consultation_type_id,dct.doctor_id,dct.consultation_type_id,ct.name,
                        ct.duration_minutes,ct.min_capacity,ct.max_capacity,dct.fee,dct.capacity,dct.is_active,
                        dct.created_at,dct.updated_at
                        FROM doctor_consultation_types dct
                        JOIN consultation_types ct ON ct.consultation_type_id=dct.consultation_type_id
                        WHERE dct.doctor_id=%s ORDER BY dct.consultation_type_id""",(uid,))
                    return out(200,{"items":cur.fetchall(),"correlationId":cid})
                if path==prefix and method=="POST":
                    if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    try: tid=int(data.get("consultationTypeId")); fee=float(data.get("fee"))
                    except (TypeError,ValueError): return out(422,{"message":"consultationTypeId and fee are required numbers","correlationId":cid})
                    cur.execute("SELECT consultation_type_id,name,duration_minutes,min_capacity,max_capacity FROM consultation_types WHERE consultation_type_id=%s AND is_active=1",(tid,))
                    master=cur.fetchone()
                    if not master: return out(422,{"message":"Choose one of the standard consultation types","correlationId":cid})
                    try: capacity=int(data.get("capacity") or master["min_capacity"])
                    except (TypeError,ValueError): return out(422,{"message":"capacity must be a number","correlationId":cid})
                    if fee<0: return out(422,{"message":"fee cannot be negative","correlationId":cid})
                    if not master["min_capacity"]<=capacity<=master["max_capacity"]:
                        return out(422,{"message":f"{master['name']} needs a capacity between {master['min_capacity']} and {master['max_capacity']}","correlationId":cid})
                    cur.execute("SELECT doctor_consultation_type_id,is_active FROM doctor_consultation_types WHERE doctor_id=%s AND consultation_type_id=%s",(uid,tid))
                    existing=cur.fetchone()
                    if existing and existing["is_active"]:
                        return out(409,{"message":f"You already offer {master['name']}","correlationId":cid})
                    if existing:
                        # Previously removed by this doctor: switch it back on with the new fee and capacity.
                        cur.execute("UPDATE doctor_consultation_types SET fee=%s,capacity=%s,is_active=1,updated_at=UTC_TIMESTAMP(6) WHERE doctor_consultation_type_id=%s",(fee,capacity,existing["doctor_consultation_type_id"]))
                        dct_id=existing["doctor_consultation_type_id"]
                    else:
                        cur.execute("INSERT INTO doctor_consultation_types(doctor_id,consultation_type_id,fee,capacity,is_active) VALUES(%s,%s,%s,%s,1)",(uid,tid,fee,capacity))
                        dct_id=cur.lastrowid
                    conn.commit()
                    emit("info","doctor_consultation_type_added",cid,doctorId=uid,consultationTypeId=tid,fee=fee,capacity=capacity)
                    refresh_slots(uid,cid)
                    return out(201,{"doctorConsultationTypeId":dct_id,"consultationTypeId":tid,"name":master["name"],"durationMinutes":master["duration_minutes"],"fee":fee,"capacity":capacity,"correlationId":cid})
                m=re.fullmatch(re.escape(prefix)+r"/(\d+)",path)
                if m:
                    # The id in the URL is the master consultation_type_id (unique per doctor).
                    tid=int(m.group(1))
                    cur.execute("""SELECT dct.doctor_consultation_type_id,ct.name,ct.min_capacity,ct.max_capacity
                        FROM doctor_consultation_types dct JOIN consultation_types ct ON ct.consultation_type_id=dct.consultation_type_id
                        WHERE dct.consultation_type_id=%s AND dct.doctor_id=%s""",(tid,uid))
                    mine=cur.fetchone()
                    if not mine: return out(404,{"message":"Consultation type not found","correlationId":cid})
                    if method=="DELETE":
                        cur.execute("UPDATE doctor_consultation_types SET is_active=0,updated_at=UTC_TIMESTAMP(6) WHERE consultation_type_id=%s AND doctor_id=%s",(tid,uid)); conn.commit()
                        emit("info","doctor_consultation_type_deactivated",cid,doctorId=uid,consultationTypeId=tid)
                        refresh_slots(uid,cid)
                        return out(200,{"message":"Consultation type deactivated","correlationId":cid})
                    if method in ("PATCH","PUT"):
                        if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                        # Name and duration belong to the master type and cannot be edited here.
                        mapping={"fee":"fee","capacity":"capacity","isActive":"is_active"}
                        sets=[]; vals=[]
                        for k,col in mapping.items():
                            if k in data:
                                v=data[k]
                                try:
                                    if k=="fee" and float(v)<0: return out(422,{"message":"fee cannot be negative","correlationId":cid})
                                    if k=="capacity" and not mine["min_capacity"]<=int(v)<=mine["max_capacity"]: return out(422,{"message":f"capacity must be {mine['min_capacity']}..{mine['max_capacity']}","correlationId":cid})
                                except (TypeError,ValueError): return out(422,{"message":f"{k} is invalid","correlationId":cid})
                                if k=="isActive": v=1 if v in (1,True,"1","true","True") else 0
                                sets.append(f"{col}=%s"); vals.append(v)
                        if not sets: return out(400,{"message":"No fields supplied","correlationId":cid})
                        vals.extend([tid,uid]); cur.execute(f"UPDATE doctor_consultation_types SET {', '.join(sets)},updated_at=UTC_TIMESTAMP(6) WHERE consultation_type_id=%s AND doctor_id=%s",vals); conn.commit()
                        emit("info","doctor_consultation_type_updated",cid,doctorId=uid,consultationTypeId=tid,fields=sorted(k for k in data if k in mapping))
                        refresh_slots(uid,cid)
                        return out(200,{"message":"Consultation type updated","correlationId":cid})
                # Weekly patterns
                prefix="/doctors/me/availability-patterns"
                if path==prefix and method=="GET":
                    cur.execute("""SELECT p.*,ct.name AS consultation_type_name,ct.duration_minutes
                        FROM availability_patterns p JOIN consultation_types ct ON ct.consultation_type_id=p.consultation_type_id
                        WHERE p.doctor_id=%s ORDER BY p.day_of_week,p.start_time,p.pattern_id""",(uid,))
                    return out(200,{"items":cur.fetchall(),"correlationId":cid})
                if path==prefix and method=="POST":
                    if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    try:
                        tid=int(data["consultationTypeId"]); dow=int(data["dayOfWeek"]); start=time.fromisoformat(data["startTime"]); end=time.fromisoformat(data["endTime"])
                    except Exception: return out(422,{"message":"consultationTypeId, dayOfWeek, startTime and endTime are required","correlationId":cid})
                    if not 1<=dow<=7 or end<=start: return out(422,{"message":"dayOfWeek is 1=Monday..7=Sunday and endTime must be after startTime","correlationId":cid})
                    cur.execute("SELECT consultation_type_id FROM doctor_consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1",(tid,uid))
                    if not cur.fetchone(): return out(422,{"message":"Choose one of your active consultation types. Add it under Consultation types first.","correlationId":cid})
                    problem=pattern_problem(cur,uid,tid,dow,start,end)
                    if problem: return out(problem[0],{"message":problem[1],"correlationId":cid})
                    cur.execute("INSERT INTO availability_patterns(doctor_id,consultation_type_id,day_of_week,start_time,end_time,is_active) VALUES(%s,%s,%s,%s,%s,1)",(uid,tid,dow,start.strftime("%H:%M:%S"),end.strftime("%H:%M:%S")))
                    conn.commit()
                    emit("info","availability_pattern_added",cid,doctorId=uid,patternId=cur.lastrowid,consultationTypeId=tid,dayOfWeek=dow,startTime=hhmm(start),endTime=hhmm(end))
                    refresh_slots(uid,cid)
                    return out(201,{"patternId":cur.lastrowid,"correlationId":cid})
                m=re.fullmatch(re.escape(prefix)+r"/(\d+)",path)
                if m:
                    pid=int(m.group(1))
                    if method=="DELETE":
                        cur.execute("UPDATE availability_patterns SET is_active=0,updated_at=UTC_TIMESTAMP(6) WHERE pattern_id=%s AND doctor_id=%s",(pid,uid)); conn.commit(); emit("info","availability_pattern_removed",cid,doctorId=uid,patternId=pid); refresh_slots(uid,cid)
                        return out(200,{"message":"Availability pattern deactivated; unbooked future slots are closed automatically, booked slots are kept","correlationId":cid})
                    if method in ("PATCH","PUT"):
                        if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                        mapping={"dayOfWeek":"day_of_week","startTime":"start_time","endTime":"end_time","isActive":"is_active"}
                        sets=[]; vals=[]
                        for k,col in mapping.items():
                            if k in data: sets.append(f"{col}=%s"); vals.append(data[k])
                        if not sets: return out(400,{"message":"No fields supplied","correlationId":cid})
                        cur.execute("SELECT * FROM availability_patterns WHERE pattern_id=%s AND doctor_id=%s",(pid,uid)); old=cur.fetchone()
                        if not old: return out(404,{"message":"Availability pattern not found","correlationId":cid})
                        try:
                            ndow=int(data.get("dayOfWeek",old["day_of_week"])); nstart=as_time(data.get("startTime",old["start_time"])); nend=as_time(data.get("endTime",old["end_time"]))
                        except Exception: return out(422,{"message":"dayOfWeek, startTime and endTime are invalid","correlationId":cid})
                        if not 1<=ndow<=7 or nend<=nstart: return out(422,{"message":"dayOfWeek is 1=Monday..7=Sunday and endTime must be after startTime","correlationId":cid})
                        if ("isActive" not in data) or data["isActive"] in (1,True,"1","true","True"):
                            problem=pattern_problem(cur,uid,int(old["consultation_type_id"]),ndow,nstart,nend,exclude_pid=pid)
                            if problem: return out(problem[0],{"message":problem[1],"correlationId":cid})
                        vals.extend([pid,uid]); cur.execute(f"UPDATE availability_patterns SET {', '.join(sets)},updated_at=UTC_TIMESTAMP(6) WHERE pattern_id=%s AND doctor_id=%s",vals); conn.commit(); emit("info","availability_pattern_updated",cid,doctorId=uid,patternId=pid,fields=sorted(k for k in data if k in mapping)); refresh_slots(uid,cid)
                        return out(200,{"message":"Availability pattern updated; future slots are being refreshed automatically","correlationId":cid})
                # Blackout dates
                prefix="/doctors/me/blackout-dates"
                if path==prefix and method=="GET":
                    cur.execute("SELECT blackout_id,doctor_id,blackout_date,reason,created_at FROM blackout_dates WHERE doctor_id=%s ORDER BY blackout_date",(uid,))
                    return out(200,{"items":cur.fetchall(),"correlationId":cid})
                if path==prefix and method=="POST":
                    if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    try: bd=date.fromisoformat(str(data["blackoutDate"]))
                    except Exception: return out(422,{"message":"blackoutDate must be YYYY-MM-DD","correlationId":cid})
                    if bd<doctor_today(cur,uid): return out(422,{"message":"A blackout date cannot be in the past","correlationId":cid})
                    reason=(str(data.get("reason") or "").strip()[:255]) or None
                    already=False
                    try:
                        cur.execute("INSERT INTO blackout_dates(doctor_id,blackout_date,reason) VALUES(%s,%s,%s)",(uid,bd,reason))
                        conn.commit()
                        emit("info","blackout_date_added",cid,doctorId=uid,blackoutDate=bd.isoformat())
                    except pymysql.err.IntegrityError as e:
                        conn.rollback()
                        if not (e.args and e.args[0]==1062): raise
                        # Already blocked: still run the sweep, so re-adding a date fixes appointments booked before.
                        already=True
                    sweep=cancel_blackout_day(uid,bd,reason,cid)
                    refresh_slots(uid,cid)
                    return blackout_reply(200 if already else 201,bd,sweep,cid,already)
                m=re.fullmatch(re.escape(prefix)+r"/(\d+)",path)
                if m and method in ("DELETE","PATCH","PUT"):
                    bid=int(m.group(1))
                    cur.execute("SELECT blackout_id,blackout_date,reason FROM blackout_dates WHERE blackout_id=%s AND doctor_id=%s",(bid,uid))
                    row=cur.fetchone()
                    if not row: return out(404,{"message":"Blackout date not found","correlationId":cid})
                    old=row["blackout_date"] if isinstance(row["blackout_date"],date) else date.fromisoformat(str(row["blackout_date"])[:10])
                    if method=="DELETE":
                        cur.execute("DELETE FROM blackout_dates WHERE blackout_id=%s AND doctor_id=%s",(bid,uid)); conn.commit()
                        emit("info","blackout_date_removed",cid,doctorId=uid,blackoutId=bid,blackoutDate=old.isoformat())
                        refresh_slots(uid,cid)     # recreates / reopens that day's slots from the weekly pattern
                        return out(200,{"message":f"Blackout on {old.isoformat()} removed. You are open again that day: slots from your weekly pattern are being reopened (refresh in a few seconds). Appointments already cancelled for the blackout stay cancelled and refunded.",
                                        "blackoutDate":old.isoformat(),"correlationId":cid})
                    # Edit: change the date and/or the reason.
                    if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    today=doctor_today(cur,uid)
                    if old<today: return out(409,{"message":"This blackout date is in the past and can no longer be edited","correlationId":cid})
                    new=old
                    if "blackoutDate" in data:
                        try: new=date.fromisoformat(str(data["blackoutDate"]))
                        except Exception: return out(422,{"message":"blackoutDate must be YYYY-MM-DD","correlationId":cid})
                        if new<today: return out(422,{"message":"A blackout date cannot be in the past","correlationId":cid})
                    reason=(str(data.get("reason") or "").strip()[:255] or None) if "reason" in data else row["reason"]
                    try:
                        cur.execute("UPDATE blackout_dates SET blackout_date=%s,reason=%s WHERE blackout_id=%s AND doctor_id=%s",(new,reason,bid,uid))
                        conn.commit()
                    except pymysql.err.IntegrityError as e:
                        conn.rollback()
                        if e.args and e.args[0]==1062: return out(409,{"message":f"{new.isoformat()} is already one of your blackout dates","correlationId":cid})
                        raise
                    emit("info","blackout_date_updated",cid,doctorId=uid,blackoutId=bid,oldDate=old.isoformat(),newDate=new.isoformat())
                    if new==old:
                        return out(200,{"message":"Blackout reason updated","blackoutDate":new.isoformat(),"alreadyExisted":True,"cancelledAppointments":0,"sweepOk":True,"correlationId":cid})
                    # Moved to another day: cancel what is booked on the NEW day, and reopen the OLD day's slots.
                    sweep=cancel_blackout_day(uid,new,reason,cid)
                    refresh_slots(uid,cid)
                    return blackout_reply(200,new,sweep,cid,previousDate=old.isoformat(),
                        base=f"Blackout moved from {old.isoformat()} to {new.isoformat()}. {old.isoformat()} is open again (its slots are being reopened).")
                # The schedule day view moved to GET /doctors/{doctorId}/slots (doctor_discovery, owner view).
                return out(404,{"message":"Doctor endpoint not found","path":path,"correlationId":cid})
    except Exception as e:
        emit("error","doctor_domain_error",cid,error=type(e).__name__,path=path,method=method,detail=str(e)[:300],traceback=traceback.format_exc()[-1500:])
        return out(500,{"message":"Internal server error","correlationId":cid})