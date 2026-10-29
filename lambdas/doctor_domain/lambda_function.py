import json, os, re, uuid, logging
from decimal import Decimal
from datetime import datetime, date, time, timedelta, timezone
from zoneinfo import ZoneInfo, ZoneInfoNotFoundError
import boto3
from boto3.dynamodb.conditions import Key
from botocore.exceptions import ClientError
from common import config, dbconn, body, response, emit, correlation

log=logging.getLogger()
log.setLevel(logging.INFO)
s3=boto3.client("s3", config=boto3.session.Config(signature_version="s3v4"))
ddb=boto3.resource("dynamodb")

def ctx_user(event):
    c=(event.get("requestContext") or {}).get("authorizer") or {}
    return int(c.get("userId") or 0), c.get("role")

def route(event):
    p=event.get("pathParameters") or {}
    path=p.get("proxy")
    if path is None:
        path=event.get("path") or event.get("rawPath") or ""
        path=re.sub(r"^/dev(?=/|$)","",path)
        path=re.sub(r"^/api/v1","",path)
    return "/" + str(path).strip("/")

def out(status, data):
    return response(status,data)

def parse_body(event):
    v=body(event)
    return v if isinstance(v,dict) else None

def doctor_exists(cur, uid):
    cur.execute("SELECT doctor_id FROM doctors WHERE doctor_id=%s", (uid,))
    return cur.fetchone() is not None

def handler(event, context):
    cid=correlation(event)
    uid, role=ctx_user(event)
    if role != "DOCTOR" or uid <= 0:
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
            bucket=os.environ["DOCTOR_PHOTOS_BUCKET"]
            url=s3.generate_presigned_url("put_object",Params={"Bucket":bucket,"Key":key,"ContentType":content_type,"ServerSideEncryption":"AES256"},ExpiresIn=300)
            return out(200,{"uploadUrl":url,"photoKey":key,"expiresInSeconds":300,"contentType":content_type,"correlationId":cid})
        with dbconn(c) as conn:
            with conn.cursor() as cur:
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
                    return out(200,{"message":"Doctor profile updated","correlationId":cid})
                # Consultation type endpoints
                prefix="/doctors/me/consultation-types"
                if path==prefix and method=="GET":
                    cur.execute("SELECT consultation_type_id,doctor_id,name,duration_minutes,fee,capacity,is_active,created_at,updated_at FROM consultation_types WHERE doctor_id=%s ORDER BY consultation_type_id",(uid,))
                    return out(200,{"items":cur.fetchall(),"correlationId":cid})
                if path==prefix and method=="POST":
                    if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    name=str(data.get("name","")).strip()
                    try: duration=int(data.get("durationMinutes")); capacity=int(data.get("capacity",1)); fee=float(data.get("fee"))
                    except (TypeError,ValueError): return out(422,{"message":"durationMinutes, fee and capacity are required numbers","correlationId":cid})
                    if not name or duration not in (15,30,45,60,90,120) or capacity<1 or capacity>20 or fee<0:
                        return out(422,{"message":"Invalid name, duration, fee or capacity","correlationId":cid})
                    cur.execute("INSERT INTO consultation_types(doctor_id,name,duration_minutes,fee,capacity,is_active) VALUES(%s,%s,%s,%s,%s,1)",(uid,name,duration,fee,capacity))
                    conn.commit()
                    return out(201,{"consultationTypeId":cur.lastrowid,"name":name,"durationMinutes":duration,"fee":fee,"capacity":capacity,"correlationId":cid})
                m=re.fullmatch(re.escape(prefix)+r"/(\d+)",path)
                if m:
                    tid=int(m.group(1))
                    cur.execute("SELECT consultation_type_id FROM consultation_types WHERE consultation_type_id=%s AND doctor_id=%s",(tid,uid))
                    if not cur.fetchone(): return out(404,{"message":"Consultation type not found","correlationId":cid})
                    if method=="DELETE":
                        cur.execute("UPDATE consultation_types SET is_active=0,updated_at=UTC_TIMESTAMP(6) WHERE consultation_type_id=%s AND doctor_id=%s",(tid,uid)); conn.commit()
                        return out(200,{"message":"Consultation type deactivated","correlationId":cid})
                    if method in ("PATCH","PUT"):
                        if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                        mapping={"name":"name","durationMinutes":"duration_minutes","fee":"fee","capacity":"capacity","isActive":"is_active"}
                        sets=[]; vals=[]
                        for k,col in mapping.items():
                            if k in data:
                                v=data[k]
                                if k=="name" and not str(v).strip(): return out(422,{"message":"name cannot be empty","correlationId":cid})
                                if k=="durationMinutes" and int(v) not in (15,30,45,60,90,120): return out(422,{"message":"Unsupported duration","correlationId":cid})
                                if k=="fee" and float(v)<0: return out(422,{"message":"fee cannot be negative","correlationId":cid})
                                if k=="capacity" and not 1<=int(v)<=20: return out(422,{"message":"capacity must be 1..20","correlationId":cid})
                                sets.append(f"{col}=%s"); vals.append(v)
                        if not sets: return out(400,{"message":"No fields supplied","correlationId":cid})
                        vals.extend([tid,uid]); cur.execute(f"UPDATE consultation_types SET {', '.join(sets)},updated_at=UTC_TIMESTAMP(6) WHERE consultation_type_id=%s AND doctor_id=%s",vals); conn.commit()
                        return out(200,{"message":"Consultation type updated","correlationId":cid})
                # Weekly patterns
                prefix="/doctors/me/availability-patterns"
                if path==prefix and method=="GET":
                    cur.execute("SELECT * FROM availability_patterns WHERE doctor_id=%s ORDER BY day_of_week,start_time,pattern_id",(uid,))
                    return out(200,{"items":cur.fetchall(),"correlationId":cid})
                if path==prefix and method=="POST":
                    if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    try:
                        tid=int(data["consultationTypeId"]); dow=int(data["dayOfWeek"]); start=time.fromisoformat(data["startTime"]); end=time.fromisoformat(data["endTime"])
                    except Exception: return out(422,{"message":"consultationTypeId, dayOfWeek, startTime and endTime are required","correlationId":cid})
                    if not 1<=dow<=7 or end<=start: return out(422,{"message":"dayOfWeek is 1=Monday..7=Sunday and endTime must be after startTime","correlationId":cid})
                    cur.execute("SELECT consultation_type_id FROM consultation_types WHERE consultation_type_id=%s AND doctor_id=%s AND is_active=1",(tid,uid))
                    if not cur.fetchone(): return out(422,{"message":"Consultation type must be active and owned by doctor","correlationId":cid})
                    cur.execute("INSERT INTO availability_patterns(doctor_id,consultation_type_id,day_of_week,start_time,end_time,is_active) VALUES(%s,%s,%s,%s,%s,1)",(uid,tid,dow,start.strftime("%H:%M:%S"),end.strftime("%H:%M:%S")))
                    conn.commit()
                    return out(201,{"patternId":cur.lastrowid,"correlationId":cid})
                m=re.fullmatch(re.escape(prefix)+r"/(\d+)",path)
                if m:
                    pid=int(m.group(1))
                    if method=="DELETE":
                        cur.execute("UPDATE availability_patterns SET is_active=0,updated_at=UTC_TIMESTAMP(6) WHERE pattern_id=%s AND doctor_id=%s",(pid,uid)); conn.commit()
                        return out(200,{"message":"Availability pattern deactivated; existing materialised slots are not automatically deleted","correlationId":cid})
                    if method in ("PATCH","PUT"):
                        if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                        mapping={"dayOfWeek":"day_of_week","startTime":"start_time","endTime":"end_time","isActive":"is_active"}
                        sets=[]; vals=[]
                        for k,col in mapping.items():
                            if k in data: sets.append(f"{col}=%s"); vals.append(data[k])
                        if not sets: return out(400,{"message":"No fields supplied","correlationId":cid})
                        vals.extend([pid,uid]); cur.execute(f"UPDATE availability_patterns SET {', '.join(sets)},updated_at=UTC_TIMESTAMP(6) WHERE pattern_id=%s AND doctor_id=%s",vals); conn.commit()
                        return out(200,{"message":"Availability pattern updated; rerun materialiser for future dates","correlationId":cid})
                # Blackout dates
                prefix="/doctors/me/blackout-dates"
                if path==prefix and method=="GET":
                    cur.execute("SELECT blackout_id,doctor_id,blackout_date,reason,created_at FROM blackout_dates WHERE doctor_id=%s ORDER BY blackout_date",(uid,))
                    return out(200,{"items":cur.fetchall(),"correlationId":cid})
                if path==prefix and method=="POST":
                    if data is None: return out(400,{"message":"Invalid JSON body","correlationId":cid})
                    try: bd=date.fromisoformat(data["blackoutDate"])
                    except Exception: return out(422,{"message":"blackoutDate must be YYYY-MM-DD","correlationId":cid})
                    cur.execute("INSERT INTO blackout_dates(doctor_id,blackout_date,reason) VALUES(%s,%s,%s)",(uid,bd,data.get("reason")))
                    conn.commit()
                    return out(201,{"blackoutDate":bd.isoformat(),"correlationId":cid})
                m=re.fullmatch(re.escape(prefix)+r"/(\d+)",path)
                if m and method=="DELETE":
                    cur.execute("DELETE FROM blackout_dates WHERE blackout_id=%s AND doctor_id=%s",(int(m.group(1)),uid)); conn.commit()
                    return out(200,{"message":"Blackout date removed; rerun materialiser if safe","correlationId":cid})
                # Schedule day view
                if path=="/doctors/me/schedule" and method=="GET":
                    q=event.get("queryStringParameters") or {}
                    try: day=date.fromisoformat(q.get("date",""))
                    except Exception: return out(422,{"message":"date query parameter must be YYYY-MM-DD","correlationId":cid})
                    local_start=datetime.combine(day,time.min,ZoneInfo("UTC"))
                    local_end=local_start+timedelta(days=1)
                    cur.execute("SELECT doctor_id,timezone FROM doctors WHERE doctor_id=%s",(uid,)); dr=cur.fetchone()
                    tz=ZoneInfo(dr["timezone"])
                    start_utc=datetime.combine(day,time.min,tzinfo=tz).astimezone(timezone.utc).replace(tzinfo=None)
                    end_utc=datetime.combine(day+timedelta(days=1),time.min,tzinfo=tz).astimezone(timezone.utc).replace(tzinfo=None)
                    cur.execute("""SELECT appointment_id,patient_id,consultation_type_id,slot_id,start_at,end_at,fee_snapshot,status
                        FROM appointments WHERE doctor_id=%s AND start_at >= %s AND start_at < %s ORDER BY start_at""",(uid,start_utc,end_utc))
                    appointments=cur.fetchall()
                    table=ddb.Table(f"MediSlot-{c['env']}-Slots")
                    items=[]; kwargs={"IndexName":"GSI_DoctorDate","KeyConditionExpression":Key("doctor_date").eq(f"DOCTOR#{uid}#{day.isoformat()}")}
                    try:
                        while True:
                            res=table.query(**kwargs); items.extend(res.get("Items",[]))
                            if "LastEvaluatedKey" not in res: break
                            kwargs["ExclusiveStartKey"]=res["LastEvaluatedKey"]
                    except Exception as e:
                        emit("warning","schedule_slots_read_failed",cid,error=type(e).__name__)
                    return out(200,{"date":day.isoformat(),"timezone":dr["timezone"],"slots":items,"appointments":appointments,"correlationId":cid})
                return out(404,{"message":"Doctor endpoint not found","path":path,"correlationId":cid})
    except Exception as e:
        emit("error","doctor_domain_error",cid,error=type(e).__name__,path=path,method=method)
        return out(500,{"message":"Internal server error","correlationId":cid})
