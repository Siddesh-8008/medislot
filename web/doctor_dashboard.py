import os, json, secrets, requests, boto3
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, session, redirect, url_for, render_template, flash, Response
app=Flask(__name__)
app.config.update(PERMANENT_SESSION_LIFETIME=timedelta(days=7),SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE","false").lower()=="true",SESSION_COOKIE_SAMESITE="Lax")
app.secret_key=os.environ.get("FLASK_SESSION_SECRET") or boto3.client("ssm").get_parameter(Name=os.environ.get("SESSION_SECRET_PARAMETER","/medislot/dev/web/session-secret"),WithDecryption=True)["Parameter"]["Value"]
API_BASE=os.environ["MEDISLOT_API_BASE"].rstrip("/")
LOGIN_URL=API_BASE+"/auth/login"
REFRESH_URL=API_BASE+"/auth/refresh"
REGISTER_URL=API_BASE+"/auth/register/doctor"

# Reference data. Mirrors the seed rows in scehma/schema.sql so doctors pick a name
# instead of typing a numeric ID. The API still validates every ID on its side.
HOSPITALS=[(1,"Apollo Hospitals Jubilee Hills"),(2,"Yashoda Hospitals Somajiguda"),(3,"KIMS Hospitals Secunderabad"),(4,"CARE Hospitals Banjara Hills"),(5,"AIG Hospitals Gachibowli"),(6,"Continental Hospitals"),(7,"Rainbow Children's Hospital Banjara Hills"),(8,"NIMS Hyderabad"),(9,"Osmania General Hospital"),(10,"Gandhi Hospital")]
SPECIALIZATIONS=[(1,"Cardiology"),(2,"Dermatology"),(3,"Endocrinology"),(4,"Family Medicine"),(5,"Gastroenterology"),(6,"General Medicine"),(7,"General Surgery"),(8,"Geriatrics"),(9,"Gynecology"),(10,"Hematology"),(11,"Nephrology"),(12,"Neurology"),(13,"Neurosurgery"),(14,"Oncology"),(15,"Ophthalmology"),(16,"Orthopedics"),(17,"Otolaryngology (ENT)"),(18,"Pediatrics"),(19,"Psychiatry"),(20,"Pulmonology"),(21,"Radiology"),(22,"Rheumatology"),(23,"Urology"),(24,"Anesthesiology"),(25,"Emergency Medicine"),(26,"Infectious Disease"),(27,"Physical Medicine and Rehabilitation"),(28,"Plastic Surgery"),(29,"Vascular Surgery"),(30,"Obstetrics")]
WEEKDAYS=[(1,"Monday"),(2,"Tuesday"),(3,"Wednesday"),(4,"Thursday"),(5,"Friday"),(6,"Saturday"),(7,"Sunday")]
# The four standard consultation types from the project brief. Duration is fixed per type;
# capacity is fixed at 1 except Health Camp Session (2 to 10). The doctor only sets the fee.
CONSULT_TYPES=[dict(name="Quick Consult",duration=15,cap_min=1,cap_max=1),dict(name="Standard Consult",duration=30,cap_min=1,cap_max=1),dict(name="Detailed Consult",duration=60,cap_min=1,cap_max=1),dict(name="Health Camp Session",duration=60,cap_min=2,cap_max=10)]
CONSULT_BY_NAME={c["name"]:c for c in CONSULT_TYPES}
# key, label, icon, accent colour, one-line description
MODULES=[
 ("overview","Overview","home","#0f6b73","Your practice at a glance and what to set up next."),
 ("profile","Profile","user","#7a4fd6","Your name, hospital, specialization and credentials."),
 ("photo","Profile photo","camera","#e0572f","The picture patients see when they find you."),
 ("consultation","Consultation types","list","#b9801c","What you offer, how long it takes and what it costs."),
 ("availability","Availability","clock","#1f7a4d","Weekly working hours and the days you are away."),
 ("schedule","Schedule","calendar","#2563b8","Look up slots and appointments for any day."),
]
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
        if session.get("access_token"): h["Authorization"]="Bearer "+session["access_token"]
        return requests.request(method,API_BASE+path,headers=h,timeout=25,**kwargs)
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

@app.errorhandler(SessionExpired)
def session_expired(e):
    session.clear()
    notify("Your session expired. Please sign in again.","error")
    return redirect("/")

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
             ctype_options=CONSULT_TYPES,ctypes_ref=CONSULT_TYPES,photo_url=photo_url_for(session.get("photo_key")),
             profile=None,p={},display_name=session.get("display_name"),last_name=None,greeting="Welcome",progress=None,steps=None,counts=None,types=None,active_types=None,type_names={},patterns=None,blackouts=None,sched=None)
    ctx.update(extra)
    return render_template("dashboard.html",**ctx)
def load_profile():
    data=read_json("/doctors/me")
    return (data or {}).get("profile") if isinstance(data,dict) else None
def items_of(data):
    return data.get("items",[]) if isinstance(data,dict) else None

@app.get("/")
def home():
    if not session.get("access_token"):
        mode="signup" if request.args.get("mode")=="signup" else "signin"
        return render_template("dashboard.html",logged_in=False,mode=mode,messages=session.pop("messages",[]),form=session.pop("signup_form",{}) if mode=="signup" else {},prefill_email=session.pop("prefill_email","") if mode=="signin" else "",hospitals=HOSPITALS,specializations=SPECIALIZATIONS)
    module=request.args.get("m","overview")
    if module not in MODULE_KEYS: module="overview"
    extra={}
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
            have={t["name"] for t in types}
            extra["ctype_options"]=[c for c in CONSULT_TYPES if c["name"] not in have]
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
    try:
        r=requests.post(LOGIN_URL,json={"email":email,"password":request.form["password"]},timeout=25)
    except requests.RequestException as e:
        app.logger.error("login upstream error: %s",e)
        session["prefill_email"]=email
        notify(f"Could not reach the MediSlot API ({type(e).__name__}). Check API URL and Lambda logs.","error")
        return redirect("/")
    try: payload=r.json()
    except ValueError: payload={}
    if r.ok and payload.get("accessToken"):
        role=payload.get("role","DOCTOR")
        session.clear(); session.permanent=True
        if role!="DOCTOR":
            notify("Doctor account required.","error")
        else:
            session["access_token"]=payload["accessToken"]; session["refresh_token"]=payload.get("refreshToken"); session["role"]=role
    else:
        app.logger.warning("login failed: HTTP %s %s",r.status_code,r.text[:300])
        session["prefill_email"]=email
        notify(f"Sign-in failed: {payload.get('message') or 'Please check your email and password.'}","error")
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
        session.pop("signup_form",None)
        session["prefill_email"]=data["email"]
        notify("Account created. Sign in to open your dashboard.","success")
        return redirect("/")
    app.logger.warning("signup failed: HTTP %s %s",r.status_code,r.text[:300])
    if r.status_code==409: return fail("An account with those details already exists. Try signing in, or check your email, phone and license number.")
    return fail(f"Sign-up failed: {api_message(r)}")
@app.get("/logout")
def logout():
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
@app.post("/blackout")
def blackout():
    r=api("POST","/doctors/me/blackout-dates",json={"blackoutDate":request.form["blackoutDate"],"reason":request.form.get("reason","")})
    report(r,"Blackout date added","Blackout date")
    return redirect("/?m=availability")
@app.post("/consultation-types")
def consultation():
    ct=CONSULT_BY_NAME.get(request.form.get("name",""))
    if not ct:
        notify("Choose one of the standard consultation types.","error")
        return redirect("/?m=consultation")
    # Duration is fixed by the type; capacity is fixed at 1 except Health Camp Session (2 to 10).
    try: capacity=int(request.form.get("capacity") or ct["cap_min"])
    except ValueError: capacity=-1
    if ct["cap_min"]==ct["cap_max"]: capacity=ct["cap_min"]
    elif not ct["cap_min"]<=capacity<=ct["cap_max"]:
        notify(f"{ct['name']} needs a capacity between {ct['cap_min']} and {ct['cap_max']}.","error")
        return redirect("/?m=consultation")
    data={"name":ct["name"],"durationMinutes":ct["duration"],"fee":float(request.form["fee"]),"capacity":capacity}
    r=api("POST","/doctors/me/consultation-types",json=data)
    report(r,"Consultation type created","Consultation type")
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

@app.get("/schedule")
def schedule():
    day=request.args.get("date","")
    r=api("GET","/doctors/me/schedule",params={"date":day})
    # Rendered directly (not stored in the cookie session) so a long day can never overflow the 4 KB cookie.
    if not r.ok: app.logger.warning("schedule failed: HTTP %s %s",r.status_code,r.text[:300])
    sched=dict(date=day,ok=r.ok,data=None,error=f"Could not load the schedule: {api_message(r)}",slot_cols=[])
    try:
        data=r.json()
        if isinstance(data,dict):
            sched["data"]=data
            slots=data.get("slots") or []
            preferred=["start_at","end_at","status","slot_id","consultation_type_id","capacity","booked_count"]
            keys=[k for s in slots if isinstance(s,dict) for k in s]
            sched["slot_cols"]=[k for k in preferred if k in keys]+[k for k in dict.fromkeys(keys) if k not in preferred]
    except ValueError: pass
    return render_dashboard("schedule",sched=sched)
if __name__=="__main__":
    # Production runs under gunicorn behind an approved HTTPS reverse proxy.
    app.run(host="127.0.0.1",port=8000,debug=False)