import os, secrets, requests, boto3
from datetime import timedelta
from flask import Flask, request, session, redirect, url_for, render_template_string, flash
app=Flask(__name__)
app.config.update(PERMANENT_SESSION_LIFETIME=timedelta(days=7),SESSION_COOKIE_HTTPONLY=True,SESSION_COOKIE_SECURE=os.environ.get("SESSION_COOKIE_SECURE","false").lower()=="true",SESSION_COOKIE_SAMESITE="Lax")
app.secret_key=os.environ.get("FLASK_SESSION_SECRET") or boto3.client("ssm").get_parameter(Name=os.environ.get("SESSION_SECRET_PARAMETER","/medislot/dev/web/session-secret"),WithDecryption=True)["Parameter"]["Value"]
API_BASE=os.environ["MEDISLOT_API_BASE"].rstrip("/")
LOGIN_URL=API_BASE+"/auth/login"
REFRESH_URL=API_BASE+"/auth/refresh"

PAGE=r"""
<!doctype html><html><head><meta charset="utf-8"><meta name="viewport" content="width=device-width">
<title>MediSlot Doctor Dashboard</title><style>
body{font:16px system-ui;margin:0;background:#f4f7fb;color:#172033}header{background:#173b63;color:white;padding:20px 5%}
main{max-width:1000px;margin:24px auto;padding:0 16px}.grid{display:grid;grid-template-columns:repeat(auto-fit,minmax(280px,1fr));gap:16px}
section{background:white;padding:20px;border-radius:12px;box-shadow:0 2px 12px #17203312}label{display:block;margin-top:10px}
input,textarea,select,button{font:inherit;padding:9px;width:100%;box-sizing:border-box;margin-top:5px}button{background:#176b87;color:white;border:0;border-radius:6px;cursor:pointer;margin-top:14px}
.notice{padding:10px;background:#e8f5ee;border-radius:6px}.muted{color:#5b6575}
</style></head><body><header><h1>MediSlot · Doctor Dashboard</h1><p>Profile editor · Availability editor · Schedule day view</p></header><main>
{% for m in messages %}<p class="notice">{{m}}</p>{% endfor %}
{% if not logged_in %}
<section><h2>Doctor sign in</h2><form method="post" action="/login"><label>Email<input name="email" type="email" required></label><label>Password<input name="password" type="password" required></label><button>Sign in</button></form><p class="muted">Use your existing MediSlot doctor account.</p></section>
{% else %}
<p>Signed in as doctor. <a href="/logout">Log out</a></p><div class="grid">
<section><h2>Profile editor</h2><form method="post" action="/profile"><label>First name<input name="firstName" required></label><label>Last name<input name="lastName" required></label><label>Hospital ID<input name="hospitalId" type="number"></label><label>Specialization ID<input name="specializationId" type="number"></label><label>License number<input name="licenseNumber"></label><label>Experience (years)<input name="yearsExperience" type="number"></label><label>Qualifications<textarea name="qualifications"></textarea></label><label>Timezone (IANA)<input name="timezone" value="Asia/Kolkata" required></label><button>Save profile fields</button></form></section>
<section><h2>Profile photo</h2><form method="post" action="/photo" enctype="multipart/form-data"><label>JPEG, PNG or WebP<input type="file" name="photo" accept="image/jpeg,image/png,image/webp" required></label><button>Upload photo</button></form></section><section><h2>Availability editor</h2><form method="post" action="/availability"><label>Consultation type ID<input name="consultationTypeId" type="number" required></label><label>Weekday (1 Monday – 7 Sunday)<input name="dayOfWeek" type="number" min="1" max="7" required></label><label>Start time<input name="startTime" type="time" required></label><label>End time<input name="endTime" type="time" required></label><button>Add weekly pattern</button></form><hr><form method="post" action="/blackout"><label>Blackout date<input name="blackoutDate" type="date" required></label><label>Reason<input name="reason"></label><button>Add blackout</button></form></section>
<section><h2>Schedule day view</h2><form method="get" action="/schedule"><label>Date<input name="date" type="date" required></label><button>View schedule</button></form>{% if schedule %}<h3>Schedule response</h3><pre>{{schedule}}</pre>{% endif %}</section>
<section><h2>Consultation types</h2><form method="post" action="/consultation-types"><label>Name<input name="name" required></label><label>Duration minutes<select name="durationMinutes"><option>15</option><option>30</option><option>45</option><option>60</option><option>90</option><option>120</option></select></label><label>Fee<input name="fee" type="number" min="0" step="0.01" required></label><label>Capacity<input name="capacity" type="number" min="1" max="20" value="1"></label><button>Create consultation type</button></form></section>
</div>{% endif %}</main></body></html>
"""
class SessionExpired(Exception):
    pass
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
@app.errorhandler(SessionExpired)
def session_expired(e):
    session.clear()
    session["messages"]=["Your session expired. Please sign in again."]
    return redirect("/")
@app.get("/")
def home():
    schedule=session.pop("schedule",None)
    return render_template_string(PAGE,logged_in=bool(session.get("access_token")),messages=session.pop("messages",[]),schedule=schedule)
@app.get("/login")
def login_page():
    return redirect("/")
@app.post("/login")
def login():
    try:
        r=requests.post(LOGIN_URL,json={"email":request.form["email"],"password":request.form["password"]},timeout=25)
    except requests.RequestException as e:
        app.logger.error("login upstream error: %s",e)
        session["messages"]=[f"Could not reach the MediSlot API ({type(e).__name__}). Check API URL and Lambda logs."]
        return redirect("/")
    try: payload=r.json()
    except ValueError: payload={}
    if r.ok and payload.get("accessToken"):
        role=payload.get("role","DOCTOR")
        session.clear(); session.permanent=True
        if role!="DOCTOR":
            session["messages"]=["Doctor account required."]
        else:
            session["access_token"]=payload["accessToken"]; session["refresh_token"]=payload.get("refreshToken"); session["role"]=role
    else:
        app.logger.warning("login failed: HTTP %s %s",r.status_code,r.text[:300])
        session["messages"]=[f"Sign-in failed: HTTP {r.status_code} - {payload.get('message') or r.text[:120]}"]
    return redirect("/")
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
    session["messages"]=[f"Profile API: HTTP {r.status_code}"]
    return redirect("/")
@app.post("/availability")
def availability():
    data={"consultationTypeId":int(request.form["consultationTypeId"]),"dayOfWeek":int(request.form["dayOfWeek"]),"startTime":request.form["startTime"],"endTime":request.form["endTime"]}
    r=api("POST","/doctors/me/availability-patterns",json=data)
    session["messages"]=[f"Availability API: HTTP {r.status_code}"]
    return redirect("/")
@app.post("/blackout")
def blackout():
    r=api("POST","/doctors/me/blackout-dates",json={"blackoutDate":request.form["blackoutDate"],"reason":request.form.get("reason","")})
    session["messages"]=[f"Blackout API: HTTP {r.status_code}"]
    return redirect("/")
@app.post("/consultation-types")
def consultation():
    data={"name":request.form["name"],"durationMinutes":int(request.form["durationMinutes"]),"fee":float(request.form["fee"]),"capacity":int(request.form["capacity"])}
    r=api("POST","/doctors/me/consultation-types",json=data)
    session["messages"]=[f"Consultation type API: HTTP {r.status_code}"]
    return redirect("/")

@app.post("/photo")
def photo():
    f=request.files.get("photo")
    allowed={"image/jpeg","image/png","image/webp"}
    if not f or f.mimetype not in allowed:
        session["messages"]=["Choose a JPEG, PNG or WebP image."]
        return redirect("/")
    content=f.read()
    if not content or len(content)>5*1024*1024:
        session["messages"]=["Photo must be between 1 byte and 5 MB."]
        return redirect("/")
    r=api("POST","/doctors/me/photo-upload-url",json={"fileName":f.filename or "photo","contentType":f.mimetype})
    if not r.ok:
        session["messages"]=[f"Could not create upload URL: HTTP {r.status_code}"]
        return redirect("/")
    upload=r.json()
    put=requests.put(upload["uploadUrl"],data=content,headers={"Content-Type":f.mimetype,"x-amz-server-side-encryption":"AES256"},timeout=20)
    if not put.ok:
        session["messages"]=[f"S3 upload failed: HTTP {put.status_code}"]
        return redirect("/")
    saved=api("PATCH","/doctors/me",json={"photoKey":upload["photoKey"]})
    session["messages"]=[f"Photo uploaded; profile update HTTP {saved.status_code}"]
    return redirect("/")

@app.get("/schedule")
def schedule():
    r=api("GET","/doctors/me/schedule",params={"date":request.args.get("date","")})
    session["schedule"]=r.text[:12000]
    return redirect("/")
if __name__=="__main__":
    # Production runs under gunicorn behind an approved HTTPS reverse proxy.
    app.run(host="127.0.0.1",port=8000,debug=False)