import os, re, json, secrets, time, uuid, logging, requests, boto3
from datetime import timedelta, datetime, timezone
from zoneinfo import ZoneInfo
from flask import Flask, request, session, redirect, url_for, render_template, flash, Response, g
import patient_portal
app=Flask(__name__)
# Log to the gunicorn error log (journald on the EC2 box) at INFO so every request and API call is recorded.
_gunicorn_log=logging.getLogger("gunicorn.error")
if _gunicorn_log.handlers: app.logger.handlers=_gunicorn_log.handlers
app.logger.setLevel(logging.INFO)
app.config.update(PERMANENT_SESSION_LIFETIME=timedelta(days=7),SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE","false").lower()=="true",SESSION_COOKIE_SAMESITE="Lax")
app.secret_key=os.environ.get("FLASK_SESSION_SECRET") or boto3.client("ssm").get_parameter(Name=os.environ.get("SESSION_SECRET_PARAMETER","/medislot/dev/web/session-secret"),WithDecryption=True)["Parameter"]["Value"]
API_BASE=os.environ["MEDISLOT_API_BASE"].rstrip("/")
LOGIN_URL=API_BASE+"/auth/login"
REFRESH_URL=API_BASE+"/auth/refresh"
REGISTER_URL=API_BASE+"/auth/register/doctor"
REGISTER_PATIENT_URL=API_BASE+"/auth/register/patient"

# Reference data. Mirrors the seed rows in scehma/schema.sql so doctors pick a name
# instead of typing a numeric ID. The API still validates every ID on its side.
HOSPITALS=[(1,"Apollo Hospitals Jubilee Hills"),(2,"Yashoda Hospitals Somajiguda"),(3,"KIMS Hospitals Secunderabad"),(4,"CARE Hospitals Banjara Hills"),(5,"AIG Hospitals Gachibowli"),(6,"Continental Hospitals"),(7,"Rainbow Children's Hospital Banjara Hills"),(8,"NIMS Hyderabad"),(9,"Osmania General Hospital"),(10,"Gandhi Hospital")]
SPECIALIZATIONS=[(1,"Cardiology"),(2,"Dermatology"),(3,"Endocrinology"),(4,"Family Medicine"),(5,"Gastroenterology"),(6,"General Medicine"),(7,"General Surgery"),(8,"Geriatrics"),(9,"Gynecology"),(10,"Hematology"),(11,"Nephrology"),(12,"Neurology"),(13,"Neurosurgery"),(14,"Oncology"),(15,"Ophthalmology"),(16,"Orthopedics"),(17,"Otolaryngology (ENT)"),(18,"Pediatrics"),(19,"Psychiatry"),(20,"Pulmonology"),(21,"Radiology"),(22,"Rheumatology"),(23,"Urology"),(24,"Anesthesiology"),(25,"Emergency Medicine"),(26,"Infectious Disease"),(27,"Physical Medicine and Rehabilitation"),(28,"Plastic Surgery"),(29,"Vascular Surgery"),(30,"Obstetrics")]
WEEKDAYS=[(1,"Monday"),(2,"Tuesday"),(3,"Wednesday"),(4,"Thursday"),(5,"Friday"),(6,"Saturday"),(7,"Sunday")]
# The standard consultation types live in the master `consultation_types` table (seeded in scehma/schema.sql).
# The dashboard reads them through the API, so there is a single source of truth.
# key, label, icon, accent colour, one-line description
MODULES=[
 ("overview","Overview","home","#0f6b73","Your practice at a glance and what to set up next."),
 ("profile","Profile","user","#7a4fd6","Your name, hospital, specialization and credentials."),
 ("photo","Profile photo","camera","#e0572f","The picture patients see when they find you."),
 ("consultation","Consultation types","list","#b9801c","What you offer, how long it takes and what it costs."),
 ("availability","Availability","clock","#1f7a4d","Weekly working hours and the days you are away."),
 ("appointments","Appointments","clipboard","#c2417a","Patients who have booked with you."),
 ("schedule","Schedule","calendar","#2563b8","Look up slots and appointments for any day."),
]
# Tabs of the Appointments module -> query for GET /appointments (the booking API filters by the signed-in doctor).
APPT_TABS={"upcoming":{"when":"upcoming","status":"CONFIRMED"},"past":{"when":"past"},"cancelled":{"status":"CANCELLED"}}
APPT_PAGE_SIZE=20
MODULE_KEYS={m[0] for m in MODULES}

class SessionExpired(Exception):
    pass

def notify(text,kind="info"):
    """Queue one banner message for the next page view. kind: info | success | error."""
    session["messages"]=[{"kind":kind,"text":text}]
def api_message(r):
    """Human-readable message from the API. Never exposes status codes or raw response bodies to the page."""
    try:
        msg=r.json().get("message")
        if msg: return str(msg)
    except (ValueError,AttributeError): pass
    return "Something went wrong. Please try again."
def report(r,ok_text,label):
    """Friendly success / error banner. Status codes go to the server log only."""
    if r.ok: notify(ok_text,"success")
    else:
        app.logger.warning("%s failed: HTTP %s %s",label,r.status_code,r.text[:300])
        notify(f"{label} failed: {api_message(r)}","error")

def refresh_tokens():
    """Exchange the stored refresh token for a new access token. Returns True on success."""
    rt=session.get("refresh_token")
    if not rt: return False
    try:
        r=requests.post(REFRESH_URL,json={"refreshToken":rt},timeout=25)
        payload=r.json() if r.ok else {}
    except (requests.RequestException,ValueError):
        return False
    if not payload.get("accessToken"): return False
    session["access_token"]=payload["accessToken"]
    if payload.get("refreshToken"): session["refresh_token"]=payload["refreshToken"]
    return True
def api(method,path,**kwargs):
    headers=kwargs.pop("headers",{})
    def send():
        h=dict(headers)
        # The same correlation id is sent to API Gateway and the Lambdas, so one click can be traced end to end.
        h["x-correlation-id"]=getattr(g,"cid",None) or uuid.uuid4().hex
        if session.get("access_token"): h["Authorization"]="Bearer "+session["access_token"]
        started=time.time()
        resp=requests.request(method,API_BASE+path,headers=h,timeout=25,**kwargs)
        app.logger.info("api_call cid=%s %s %s -> HTTP %s (%dms)",h["x-correlation-id"],method,path,resp.status_code,(time.time()-started)*1000)
        return resp
    r=send()
    if r.status_code==401 and session.get("refresh_token"):
        # Access token expired (15 min): get a new one silently and retry once.
        if not refresh_tokens(): raise SessionExpired()
        r=send()
    return r
def read_json(path):
    """Read-only GET used to fill the module pages. Returns parsed JSON, or None if it can't be loaded."""
    try:
        r=api("GET",path)
        return r.json() if r.ok else None
    except SessionExpired: raise
    except (requests.RequestException,ValueError): return None

@app.before_request
def _start_request():
    g.cid=uuid.uuid4().hex; g.started=time.time()
@app.after_request
def _log_request(resp):
    if request.endpoint!="static":
        app.logger.info("request cid=%s %s %s -> %s (%dms)",getattr(g,"cid","-"),request.method,request.path,resp.status_code,(time.time()-getattr(g,"started",time.time()))*1000)
    return resp

@app.errorhandler(SessionExpired)
def session_expired(e):
    was_patient=session.get("role")=="PATIENT"
    session.clear()
    notify("Your session expired. Please sign in again.","error")
    return redirect("/?mode=signin&role=patient" if was_patient else "/")

def photo_url_for(key):
    # The key is unique per upload, so using it as ?v= makes the browser reload a new photo immediately.
    return "/photo-image?v="+key.rsplit("/",1)[-1] if key else None
_photo_bucket=None
def photo_bucket():
    """Name of the private doctor-photos bucket (same naming as the CloudFormation stacks)."""
    global _photo_bucket
    if _photo_bucket is None:
        b=os.environ.get("DOCTOR_PHOTOS_BUCKET","")
        if not b:
            try:
                account=boto3.client("sts").get_caller_identity()["Account"]
                region=os.environ.get("AWS_DEFAULT_REGION") or os.environ.get("AWS_REGION")
                env=os.environ.get("SESSION_SECRET_PARAMETER","/medislot/dev/web/session-secret").split("/")[2]
                b=f"medislot-{env}-doctor-photos-{account}-{region}"
            except Exception as e:
                app.logger.warning("could not work out photo bucket: %s",e)
        _photo_bucket=b
    return _photo_bucket
def render_dashboard(module,**extra):
    """Shared renderer for every dashboard module."""
    ctx=dict(logged_in=True,module=module,modules=MODULES,messages=session.pop("messages",[]),hospitals=HOSPITALS,specializations=SPECIALIZATIONS,weekdays=WEEKDAYS,day_names=dict(WEEKDAYS),
             ctype_options=[],ctypes_ref=[],photo_url=photo_url_for(session.get("photo_key")),
             profile=None,p={},display_name=session.get("display_name"),last_name=None,greeting="Welcome",progress=None,steps=None,counts=None,types=None,active_types=None,type_names={},patterns=None,blackouts=None,sched=None,
             upcoming=None,appts=None)
    ctx.update(extra)
    return render_template("dashboard.html",**ctx)
def load_profile():
    data=read_json("/doctors/me")
    return (data or {}).get("profile") if isinstance(data,dict) else None
def load_catalog():
    """Master consultation types from the API, shaped for the template. None if it can't be loaded."""
    items=items_of(read_json("/doctors/catalog/consultation-types"))
    if items is None: return None
    return [dict(id=c["consultation_type_id"],name=c["name"],duration=c["duration_minutes"],cap_min=c["min_capacity"],cap_max=c["max_capacity"]) for c in items]
def items_of(data):
    return data.get("items",[]) if isinstance(data,dict) else None

@app.get("/")
def home():
    if session.get("access_token") and session.get("role")=="PATIENT": return redirect("/patient")
    if not session.get("access_token"):
        mode="signup" if request.args.get("mode")=="signup" else "signin"
        who="patient" if request.args.get("role")=="patient" else "doctor"
        return render_template("dashboard.html",logged_in=False,mode=mode,who=who,messages=session.pop("messages",[]),form=session.pop("signup_form",{}) if mode=="signup" else {},prefill_email=session.pop("prefill_email","") if mode=="signin" else "",hospitals=HOSPITALS,specializations=SPECIALIZATIONS,genders=patient_portal.GENDERS,today=datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat())
    module=request.args.get("m","overview")
    if module not in MODULE_KEYS: module="overview"
    if module=="schedule":
        # Open the schedule on today's date instead of an empty picker.
        return redirect("/schedule?date="+datetime.now(ZoneInfo("Asia/Kolkata")).date().isoformat())
    extra={}
    if module=="appointments":
        tab=request.args.get("tab","upcoming")
        if tab not in APPT_TABS: tab="upcoming"
        try: page=max(int(request.args.get("page","1")),1)
        except ValueError: page=1
        extra["appts"]=load_bookings(tab,page,APPT_PAGE_SIZE)
    if module=="overview":
        extra["upcoming"]=load_bookings("upcoming",1,5)
    if module in ("overview","profile","photo"):
        profile=load_profile()
        extra.update(profile=profile,p=profile or {})
        if profile:
            session["display_name"]=f"Dr. {profile.get('first_name','')} {profile.get('last_name','')}".strip()
            if profile.get("photo_key"): session["photo_key"]=profile["photo_key"]
            else: session.pop("photo_key",None)
            extra["last_name"]=profile.get("last_name")
    if module in ("overview","consultation","availability"):
        types=items_of(read_json("/doctors/me/consultation-types"))
        extra["types"]=types
        if types is not None:
            extra["active_types"]=[t for t in types if t.get("is_active")]
            extra["type_names"]={t["consultation_type_id"]:t["name"] for t in types}

    if module=="consultation":
        catalog=load_catalog()
        have={t["consultation_type_id"] for t in (extra.get("types") or []) if t.get("is_active")}
        extra.update(ctypes_ref=catalog or [],ctype_options=[c for c in (catalog or []) if c["id"] not in have],catalog_loaded=catalog is not None)
    if module in ("overview","availability"):
        extra["patterns"]=items_of(read_json("/doctors/me/availability-patterns"))
    if module=="availability":
        extra["blackouts"]=items_of(read_json("/doctors/me/blackout-dates"))
    if module=="overview":
        prof=extra.get("profile"); types=extra.get("types"); patterns=extra.get("patterns")
        try: hour=datetime.now(ZoneInfo((prof or {}).get("timezone") or "Asia/Kolkata")).hour
        except Exception: hour=datetime.now().hour
        extra["greeting"]="Good morning" if hour<12 else "Good afternoon" if hour<17 else "Good evening"
        if prof is not None and types is not None and patterns is not None:
            active_types=[t for t in types if t.get("is_active")]; active_patterns=[x for x in patterns if x.get("is_active")]
            steps=[
             dict(module="profile",title="Complete your profile",text="Add your qualifications and years of experience.",done=bool(prof.get("qualifications")) and prof.get("years_experience") is not None),
             dict(module="photo",title="Add a profile photo",text="Help patients recognise you.",done=bool(prof.get("photo_key"))),
             dict(module="consultation",title="Create a consultation type",text="Set the duration, fee and capacity of a visit.",done=bool(active_types)),
             dict(module="availability",title="Publish weekly availability",text="Choose the hours patients can book.",done=bool(active_patterns))]
            extra.update(steps=steps,progress=round(100*sum(s["done"] for s in steps)/len(steps)),counts=dict(types=len(active_types),patterns=len(active_patterns)))
    return render_dashboard(module,**extra)

@app.get("/login")
def login_page():
    return redirect("/")
@app.get("/signup")
def signup_page():
    return redirect("/?mode=signup")
@app.post("/login")
def login():
    email=request.form["email"]
    expect="PATIENT" if request.form.get("as")=="patient" else "DOCTOR"
    back="/?mode=signin&role=patient" if expect=="PATIENT" else "/"
    try:
        r=requests.post(LOGIN_URL,json={"email":email,"password":request.form["password"]},timeout=25)
    except requests.RequestException as e:
        app.logger.error("login upstream error: %s",e)
        session["prefill_email"]=email
        notify(f"Could not reach the MediSlot API ({type(e).__name__}). Check API URL and Lambda logs.","error")
        return redirect(back)
    try: payload=r.json()
    except ValueError: payload={}
    if r.ok and payload.get("accessToken"):
        role=payload.get("role","DOCTOR")
        session.clear(); session.permanent=True
        if role!=expect:
            session["prefill_email"]=email
            notify("This is a patient account. Use the patient sign in." if role=="PATIENT" else "This is a doctor account. Use the doctor sign in.","error")
            return redirect("/?mode=signin&role=patient" if role=="PATIENT" else "/")
        else:
            session["access_token"]=payload["accessToken"]; session["refresh_token"]=payload.get("refreshToken"); session["role"]=role
            app.logger.info("login_succeeded cid=%s userId=%s",g.cid,payload.get("userId"))
    else:
        app.logger.warning("login_failed cid=%s HTTP %s %s",g.cid,r.status_code,r.text[:300])
        session["prefill_email"]=email
        notify(f"Sign-in failed: {payload.get('message') or 'Please check your email and password.'}","error")
        return redirect(back)
    return redirect("/")
@app.post("/signup")
def signup():
    f=request.form
    keep=("firstName","lastName","email","phone","hospitalId","specializationId","licenseNumber","yearsExperience","clinicAddress","qualifications","languages")
    session["signup_form"]={k:f.get(k,"").strip() for k in keep}   # never includes the password
    def fail(text):
        notify(text,"error")
        return redirect("/?mode=signup")
    # Mandatory fields (same ones the register API requires).
    required={"firstName":"first name","lastName":"last name","email":"email","password":"password","hospitalId":"hospital","specializationId":"specialization","licenseNumber":"license number"}
    missing=[label for k,label in required.items() if not f.get(k,"").strip()]
    if missing: return fail("Please fill in: "+", ".join(missing)+".")
    if f.get("password")!=f.get("confirmPassword"): return fail("Passwords do not match.")
    try:
        data={"firstName":f["firstName"].strip(),"lastName":f["lastName"].strip(),"email":f["email"].strip(),"password":f["password"],
              "hospitalId":int(f["hospitalId"]),"specializationId":int(f["specializationId"]),"licenseNumber":f["licenseNumber"].strip()}
        if f.get("yearsExperience","").strip(): data["yearsExperience"]=int(f["yearsExperience"])
    except ValueError:
        return fail("Hospital, specialization and years of experience must be valid numbers.")
    for form_key,api_key in (("phone","phone"),("clinicAddress","clinicAddress"),("qualifications","qualifications"),("languages","languages_spoken")):
        if f.get(form_key,"").strip(): data[api_key]=f[form_key].strip()
    try:
        r=requests.post(REGISTER_URL,json=data,timeout=25)
    except requests.RequestException as e:
        app.logger.error("signup upstream error: %s",e)
        return fail(f"Could not reach the MediSlot API ({type(e).__name__}). Check API URL and Lambda logs.")
    if r.status_code==201:
        app.logger.info("signup_succeeded cid=%s",g.cid)
        session.pop("signup_form",None)
        session["prefill_email"]=data["email"]
        notify("Account created. Sign in to open your dashboard.","success")
        return redirect("/")
    app.logger.warning("signup failed: HTTP %s %s",r.status_code,r.text[:300])
    if r.status_code==409: return fail("An account with those details already exists. Try signing in, or check your email, phone and license number.")
    return fail(f"Sign-up failed: {api_message(r)}")
@app.post("/signup/patient")
def signup_patient():
    """Patient registration. The API validates everything again (shared rules in lambdas/common.py)."""
    f=request.form
    keep=("firstName","lastName","email","phone","dateOfBirth","gender","address")
    session["signup_form"]={k:f.get(k,"").strip() for k in keep}   # never includes the password
    def fail(text):
        notify(text,"error")
        return redirect("/?mode=signup&role=patient")
    required={"firstName":"first name","lastName":"last name","email":"email","phone":"phone","dateOfBirth":"date of birth","gender":"gender","password":"password"}
    missing=[label for k,label in required.items() if not f.get(k,"").strip()]
    if missing: return fail("Please fill in: "+", ".join(missing)+".")
    if f.get("password")!=f.get("confirmPassword"): return fail("Passwords do not match.")
    data={k:f[k].strip() for k in ("firstName","lastName","email","phone","dateOfBirth","gender")}
    data["password"]=f["password"]
    if f.get("address","").strip(): data["address"]=f["address"].strip()
    try:
        r=requests.post(REGISTER_PATIENT_URL,json=data,timeout=25)
    except requests.RequestException as e:
        app.logger.error("patient signup upstream error: %s",e)
        return fail(f"Could not reach the MediSlot API ({type(e).__name__}). Please try again.")
    if r.status_code==201:
        app.logger.info("patient_signup_succeeded cid=%s",g.cid)
        session.pop("signup_form",None)
        session["prefill_email"]=data["email"]
        notify("Account created. Sign in to find a doctor.","success")
        return redirect("/?mode=signin&role=patient")
    app.logger.warning("patient signup failed: HTTP %s %s",r.status_code,r.text[:300])
    if r.status_code==409: return fail("An account with that email or phone number already exists. Try signing in instead.")
    return fail(f"Sign-up failed: {api_message(r)}")

# Role separation in the shared dashboard: doctors never see patient pages and patients never see doctor pages.
OPEN_ENDPOINTS={"static","home","login_page","signup_page","login","signup","signup_patient","logout","patient.login_page","patient.register_page"}
@app.before_request
def _role_guard():
    ep=request.endpoint
    if ep is None or ep in OPEN_ENDPOINTS: return None
    role=session.get("role") if session.get("access_token") else None
    if ep.startswith("patient."):
        if role=="PATIENT": return None
        if role is None:
            notify("Please sign in to continue.","info")
            return redirect("/?mode=signin&role=patient")
        return redirect("/")
    if role=="PATIENT": return redirect("/patient")
    return None

@app.get("/logout")
def logout():
    app.logger.info("logout cid=%s",g.cid)
    if session.get("access_token"):
        try: api("POST","/auth/logout",json={"refreshToken":session.get("refresh_token")})
        except Exception: pass
    session.clear()
    return redirect("/")
@app.post("/profile")
def profile():
    fields=("firstName","lastName","hospitalId","specializationId","licenseNumber","yearsExperience","qualifications","timezone")
    data={k:request.form[k] for k in fields if request.form.get(k)}
    for k in ("hospitalId","specializationId","yearsExperience"):
        if k in data: data[k]=int(data[k])
    r=api("PATCH","/doctors/me",json=data)
    report(r,"Profile saved","Profile update")
    return redirect("/?m=profile")
@app.post("/availability")
def availability():
    data={"consultationTypeId":int(request.form["consultationTypeId"]),"dayOfWeek":int(request.form["dayOfWeek"]),"startTime":request.form["startTime"],"endTime":request.form["endTime"]}
    r=api("POST","/doctors/me/availability-patterns",json=data)
    report(r,"Weekly pattern added","Availability")
    return redirect("/?m=availability")
@app.post("/availability/<int:pid>/update")
def availability_update(pid):
    """Change the hours of an existing weekly pattern. The API then rebuilds this doctor's slots:
    unbooked, unheld slots outside the new hours are closed; booked and held slots are never touched."""
    data={"startTime":request.form["startTime"],"endTime":request.form["endTime"]}
    r=api("PATCH",f"/doctors/me/availability-patterns/{pid}",json=data)
    report(r,"Weekly pattern updated. Open slots outside the new hours are being closed; booked slots are kept.","Availability update")
    return redirect("/?m=availability")
@app.post("/availability/<int:pid>/remove")
def availability_remove(pid):
    r=api("DELETE",f"/doctors/me/availability-patterns/{pid}")
    report(r,"Weekly pattern removed. Its open slots are being closed; booked slots are kept.","Remove pattern")
    return redirect("/?m=availability")
@app.template_filter("hhmm")
def hhmm_filter(v):
    """'9:00:00' / '09:00:00' -> '09:00' (for display and <input type=time>)."""
    try:
        h,m=str(v).split(":")[:2]
        return f"{int(h):02d}:{m[:2]}"
    except (ValueError,TypeError):
        return str(v)
@app.post("/blackout")
def blackout():
    r=api("POST","/doctors/me/blackout-dates",json={"blackoutDate":request.form["blackoutDate"],"reason":request.form.get("reason","")})
    report(r,"Blackout date added","Blackout date")
    return redirect("/?m=availability")
@app.post("/consultation-types")
def consultation():
    """The doctor picks one of the standard types (by id) and sets only the fee, plus capacity for Health Camp.
    The API checks the capacity range against the master table and stores the choice in doctor_consultation_types."""
    try:
        data={"consultationTypeId":int(request.form["consultationTypeId"]),"fee":float(request.form["fee"])}
        if request.form.get("capacity","").strip(): data["capacity"]=int(request.form["capacity"])
    except (KeyError,ValueError):
        notify("Choose a consultation type and enter a valid fee.","error")
        return redirect("/?m=consultation")
    r=api("POST","/doctors/me/consultation-types",json=data)
    report(r,"Consultation type added","Consultation type")
    return redirect("/?m=consultation")

@app.post("/photo")
def photo():
    f=request.files.get("photo")
    allowed={"image/jpeg","image/png","image/webp"}
    if not f or f.mimetype not in allowed:
        notify("Choose a JPEG, PNG or WebP image.","error")
        return redirect("/?m=photo")
    content=f.read()
    if not content or len(content)>5*1024*1024:
        notify("Photo must be between 1 byte and 5 MB.","error")
        return redirect("/?m=photo")
    r=api("POST","/doctors/me/photo-upload-url",json={"fileName":f.filename or "photo","contentType":f.mimetype})
    if not r.ok:
        app.logger.warning("photo upload-url failed: HTTP %s",r.status_code)
        notify("Could not prepare the photo upload. Please try again.","error")
        return redirect("/?m=photo")
    upload=r.json()
    put=requests.put(upload["uploadUrl"],data=content,headers={"Content-Type":f.mimetype,"x-amz-server-side-encryption":"AES256"},timeout=20)
    if not put.ok:
        app.logger.warning("photo S3 upload failed: HTTP %s",put.status_code)
        notify("Photo upload failed. Please try again.","error")
        return redirect("/?m=photo")
    saved=api("PATCH","/doctors/me",json={"photoKey":upload["photoKey"]})
    if saved.ok: session["photo_key"]=upload["photoKey"]
    report(saved,"Photo uploaded and profile updated","Photo uploaded but profile update")
    return redirect("/?m=photo")

@app.get("/photo-image")
def photo_image():
    """Streams the signed-in doctor's own photo from the private S3 bucket. The key comes from the signed session, never from the URL."""
    key=session.get("photo_key")
    if not session.get("access_token") or not key or not key.startswith("doctors/") or not photo_bucket(): return ("",404)
    try:
        obj=boto3.client("s3").get_object(Bucket=photo_bucket(),Key=key)
        data=obj["Body"].read()
    except Exception as e:
        app.logger.warning("photo read failed for %s: %s",key,e)
        return ("",404)
    resp=Response(data,mimetype=obj.get("ContentType") or "image/jpeg")
    resp.headers["Cache-Control"]="private, max-age=3600"
    return resp

def _utc(iso):
    """'2026-11-09T04:00:00Z' -> aware UTC datetime (works on Python 3.9, where fromisoformat rejects 'Z').
    RDS DATETIME values arrive without an offset ('2026-11-09T04:00:00'); they are stored in UTC, so treat them as UTC
    (otherwise .astimezone() would read them in the server's own local time and shift every appointment)."""
    dt=datetime.fromisoformat(str(iso).replace("Z","+00:00"))
    return dt if dt.tzinfo else dt.replace(tzinfo=timezone.utc)
STATUS_LABELS={"CONFIRMED":("Confirmed","open"),"COMPLETED":("Completed","done"),"NO_SHOW":("No-show","closed"),
               "CANCELLED_BY_PATIENT":("Cancelled by patient","closed"),"CANCELLED_BY_DOCTOR":("Cancelled by you","closed")}
def booking_card(a):
    """One appointment from GET /appointments (doctor view), shaped for the template. Times are already in the doctor's timezone."""
    label,state=STATUS_LABELS.get(str(a.get("status")),(str(a.get("status") or "").replace("_"," ").title(),"closed"))
    pat=a.get("patient") or {}
    name=pat.get("name") or (f"Patient #{pat['id']}" if pat.get("id") else "Patient")
    booked_by=None
    if pat.get("bookedBy"):
        booked_by=f"Booked by {pat['bookedBy']}"+(f" ({pat['relation']})" if pat.get("relation") else "")
    try:
        d=datetime.strptime(a.get("localDate") or "","%Y-%m-%d").date(); pretty=f"{d:%A}, {d.day} {d:%B %Y}"
    except ValueError:
        d=None; pretty=a.get("localDate") or ""
    def t12(hm):
        try: return datetime.strptime(hm,"%H:%M").strftime("%I:%M %p").lstrip("0")
        except (TypeError,ValueError): return hm
    refund=a.get("refund") or {}
    return dict(id=a.get("appointmentId"),date=a.get("localDate"),pretty_date=pretty,start=t12(a.get("localStart")),end=t12(a.get("localEnd")),
                patient=name,booked_by=booked_by,type=(a.get("consultationType") or {}).get("name") or "Consultation",
                status=label,state=state,fee=_money(a.get("fee")),refund=_money(refund.get("refundAmount")) if refund else None,
                note=a.get("cancellationNote"))
def load_bookings(tab,page,size):
    """Doctor's appointments from the booking API. Returns dict(ok, items, groups, total, ...)."""
    out=dict(ok=False,tab=tab,page=page,items=[],groups=[],total=0,total_pages=1,has_prev=False,has_next=False)
    try:
        r=api("GET","/appointments",params={**APPT_TABS[tab],"page":page,"pageSize":size})
        data=r.json() if r.ok else None
    except SessionExpired: raise
    except (requests.RequestException,ValueError) as e:
        app.logger.warning("doctor appointments upstream error: %s",e); return out
    if not isinstance(data,dict):
        app.logger.warning("doctor appointments failed: HTTP %s %s",r.status_code,r.text[:300]); return out
    items=[]
    for raw in data.get("items") or []:
        try: items.append(booking_card(raw))
        except (KeyError,TypeError,AttributeError): app.logger.warning("skipped unreadable appointment: %s",str(raw)[:200])
    groups=[]
    for c in items:   # the API already sorts them; group consecutive rows by local date
        if groups and groups[-1]["date"]==c["date"]: groups[-1]["items"].append(c)
        else: groups.append(dict(date=c["date"],label=c["pretty_date"],items=[c]))
    out.update(ok=True,items=items,groups=groups,total=_num(data.get("total"),len(items)),total_pages=_num(data.get("totalPages"),1),
               has_prev=bool(data.get("hasPrevious")),has_next=bool(data.get("hasNext")))
    return out
def _clock(dt):
    return dt.strftime("%I:%M %p").lstrip("0")
def _money(v):
    try: return "\u20b9{:,.0f}".format(float(v)) if float(v)==int(float(v)) else "\u20b9{:,.2f}".format(float(v))
    except (TypeError,ValueError): return None
def _num(v,default=0):
    try: return int(v)
    except (TypeError,ValueError): return default
def slot_card(s,zone):
    """Turn a raw slot record into something a person can read at a glance (times in the doctor's own timezone)."""
    start=_utc(s["start_at"]).astimezone(zone); end=_utc(s["end_at"]).astimezone(zone)
    cap=max(_num(s.get("capacity"),1),1); booked=_num(s.get("booked_count")); held=_num(s.get("held_count"))
    raw=str(s.get("status") or "").upper()
    if raw=="CLOSED": state,label="closed","Closed"
    elif booked>=cap: state,label="booked","Fully booked"
    elif booked>0: state,label="partial","Partly booked"
    elif held>0: state,label="hold","On hold"
    elif raw in ("","OPEN"): state,label="open","Open"
    else: state,label="closed",raw.title()
    mins=s.get("duration_minutes") or int((end-start).total_seconds()//60)
    details=[("Slot ID",s.get("slot_id")),("Starts (UTC)",s.get("start_at")),("Ends (UTC)",s.get("end_at")),("Seats held",held),("Closed because","Schedule changed" if s.get("closed_reason")=="SCHEDULE_CHANGE" else s.get("closed_reason")),("Version",s.get("version"))]
    sid=str(s.get("slot_id") or "")
    # Milestone 5: a future slot that is not already cancelled can be cancelled by the doctor (patients refunded in full).
    cancellable=raw!="CANCELLED" and _utc(s["start_at"])>datetime.now(timezone.utc) and bool(re.fullmatch(r"SLOT#[0-9a-f]{40}",sid))
    if raw=="CANCELLED": state,label="closed","Cancelled"
    return dict(start=_clock(start),end=_clock(end),hour=start.hour,sort=start,state=state,label=label,minutes=mins,type=s.get("consultation_type_name") or "Consultation",
                fee=_money(s.get("fee")),capacity=cap,booked=booked,free=max(cap-booked-held,0),details=[(k,v) for k,v in details if v not in (None,"")],
                slot_hex=sid[5:] if cancellable else None,cancellable=cancellable)
def appointment_card(a,zone):
    start=_utc(a["start_at"]).astimezone(zone); end=_utc(a["end_at"]).astimezone(zone) if a.get("end_at") else None
    status=str(a.get("status") or "").replace("_"," ").title() or "Scheduled"
    details=[("Appointment ref",a.get("appointment_id")),("Patient ref",a.get("patient_id")),("Slot ID",a.get("slot_id"))]
    return dict(start=_clock(start),end=_clock(end) if end else None,type=a.get("consultation_type_name") or "Consultation",status=status,fee=_money(a.get("fee_snapshot") if a.get("fee_snapshot") is not None else a.get("fee")),details=[(k,v) for k,v in details if v not in (None,"")])
PARTS=[("Morning",0,12),("Afternoon",12,17),("Evening",17,24)]
@app.get("/schedule")
def schedule():
    day=request.args.get("date","")
    r=api("GET","/doctors/me/schedule",params={"date":day})
    # Rendered directly (not stored in the cookie session) so a long day can never overflow the 4 KB cookie.
    if not r.ok: app.logger.warning("schedule failed: HTTP %s %s",r.status_code,r.text[:300])
    sched=dict(date=day,ok=r.ok,data=None,error=f"Could not load the schedule: {api_message(r)}",groups=[],appointments=[],counts={},pretty_date=day)
    try:
        data=r.json()
        if r.ok and isinstance(data,dict):
            sched["data"]=data
            try: zone=ZoneInfo(data.get("timezone") or "Asia/Kolkata")
            except Exception: zone=ZoneInfo("Asia/Kolkata")
            try: d=datetime.strptime(data.get("date") or day,"%Y-%m-%d"); sched["pretty_date"]=f"{d:%A}, {d.day} {d:%B %Y}"
            except ValueError: pass
            cards=[]
            for raw in data.get("slots") or []:
                try: cards.append(slot_card(raw,zone))
                except (KeyError,ValueError,TypeError): app.logger.warning("skipped unreadable slot record: %s",str(raw)[:200])
            cards.sort(key=lambda c:c["sort"])
            sched["groups"]=[dict(label=lbl,slots=[c for c in cards if lo<=c["hour"]<hi]) for lbl,lo,hi in PARTS if any(lo<=c["hour"]<hi for c in cards)]
            sched["counts"]=dict(total=len(cards),**{k:sum(1 for c in cards if c["state"]==k) for k in ("open","booked","partial","hold","closed")})
            appts=[]
            for raw in data.get("appointments") or []:
                try: appts.append(appointment_card(raw,zone))
                except (KeyError,ValueError,TypeError): app.logger.warning("skipped unreadable appointment record: %s",str(raw)[:200])
            sched["appointments"]=appts
    except ValueError: pass
    return render_dashboard("schedule",sched=sched)
@app.post("/schedule/slots/cancel")
def schedule_cancel_slot():
    """Milestone 5: the doctor cancels one slot. Every patient booked in it is cancelled with a 100% refund (booking API)."""
    slot_hex=request.form.get("slot","")
    day=request.form.get("date","")
    back="/schedule?date="+day if re.fullmatch(r"\d{4}-\d{2}-\d{2}",day) else "/?m=schedule"
    if not re.fullmatch(r"[0-9a-f]{40}",slot_hex) or request.form.get("confirm")!="yes":
        notify("Tick the confirmation box to cancel the slot.","error")
        return redirect(back)
    reason=" ".join(request.form.get("reason","").split())[:255]
    r=api("POST",f"/doctors/me/slots/{slot_hex}/cancel",json={"reason":reason} if reason else {})
    if r.ok:
        n=r.json().get("cancelledAppointments",0)
        notify("Slot cancelled."+(f" {n} patient{'s' if n!=1 else ''} refunded in full." if n else " Nobody was booked in it."),"success")
    else:
        app.logger.warning("slot cancel failed: HTTP %s %s",r.status_code,r.text[:300])
        notify(f"Slot not cancelled: {api_message(r)}","error")
    return redirect(back)
patient_portal.init(app,api=api,notify=notify,api_message=api_message,read_json=read_json,photo_bucket=photo_bucket,HOSPITALS=HOSPITALS,SPECIALIZATIONS=SPECIALIZATIONS)
if __name__=="__main__":
    # Production runs under gunicorn behind an approved HTTPS reverse proxy.
    app.run(host="127.0.0.1",port=8000,debug=False)