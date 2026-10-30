import os, json, secrets, requests, boto3
from datetime import timedelta, datetime
from zoneinfo import ZoneInfo
from flask import Flask, request, session, redirect, url_for, render_template_string, flash
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

PAGE=r"""
<!doctype html><html lang="en"><head><meta charset="utf-8"><meta name="viewport" content="width=device-width,initial-scale=1">
<title>MediSlot Doctor Dashboard</title>
<link rel="preconnect" href="https://fonts.googleapis.com"><link rel="preconnect" href="https://fonts.gstatic.com" crossorigin>
<link href="https://fonts.googleapis.com/css2?family=DM+Sans:wght@400;500;600;700&family=Fraunces:opsz,wght@9..144,500;9..144,600;9..144,700&display=swap" rel="stylesheet">
<style>
:root{--bg:#f5f1e8;--paper:#fffdf8;--ink:#12303a;--ink2:#0b2229;--muted:#5d7078;--line:#e4ddcc;--teal:#0f6b73;--teal-d:#0a4f56;--mint:#dcf1ec;--coral:#e0572f;--ok:#1f7a4d;--okbg:#e3f4ea;--err:#b3261e;--errbg:#fbe9e7;--info:#1d5c8a;--infobg:#e6f0f8;--r:16px;--serif:'Fraunces',Georgia,serif;--sans:'DM Sans',system-ui,-apple-system,'Segoe UI',sans-serif}
*{box-sizing:border-box}html{-webkit-text-size-adjust:100%}
body{margin:0;font:16px/1.55 var(--sans);background:var(--bg);color:var(--ink)}
h1,h2,h3{font-family:var(--serif);font-weight:600;letter-spacing:-.01em;margin:0}
a{color:var(--teal)}
.ic{width:20px;height:20px;flex:none}
label{display:block;font-size:14px;font-weight:600;color:var(--ink);margin:0}
label .opt{font-weight:400;color:var(--muted);margin-left:6px;font-size:13px}
label .req{color:var(--coral);margin-left:2px}
input,textarea,select{font:inherit;color:var(--ink);width:100%;margin-top:8px;padding:13px 14px;background:#fff;border:1.5px solid var(--line);border-radius:12px;transition:border-color .15s,box-shadow .15s}
textarea{min-height:110px;resize:vertical}
input:focus,textarea:focus,select:focus{outline:0;border-color:var(--teal);box-shadow:0 0 0 4px #0f6b7322}
input[type=file]{padding:12px;background:var(--paper);border-style:dashed}
.hint{display:block;font-size:13px;font-weight:400;color:var(--muted);margin-top:6px}
button,.btn{font:inherit;font-weight:600;cursor:pointer;border:0;border-radius:12px;padding:14px 22px;background:var(--teal);color:#fff;transition:background .15s,transform .05s;text-decoration:none;display:inline-flex;align-items:center;justify-content:center;gap:8px}
button:hover,.btn:hover{background:var(--teal-d)}button:active{transform:translateY(1px)}
button.block{width:100%}
button.ghost,.btn.ghost{background:transparent;color:var(--ink);border:1.5px solid var(--line)}
button.ghost:hover,.btn.ghost:hover{background:#00000008}
.banner{display:flex;gap:12px;align-items:flex-start;padding:14px 16px;border-radius:12px;margin-bottom:24px;font-size:15px;border:1px solid transparent}
.banner.info{background:var(--infobg);color:var(--info);border-color:#c5dcee}.banner.success{background:var(--okbg);color:var(--ok);border-color:#bfe3cd}.banner.error{background:var(--errbg);color:var(--err);border-color:#f2c4bf}
.banner span{flex:1}.banner button{padding:0 4px;background:none;color:inherit;font-size:20px;line-height:1;opacity:.6}.banner button:hover{background:none;opacity:1}

/* ---------- Sign in / sign up ---------- */
.auth{display:grid;grid-template-columns:minmax(340px,5fr) minmax(0,7fr);min-height:100vh}
.brand{position:relative;overflow:hidden;color:#eaf6f3;padding:56px clamp(28px,4vw,64px);display:flex;flex-direction:column;justify-content:space-between;gap:40px;background:radial-gradient(900px 500px at 0% 0%,#14808a55,transparent 60%),linear-gradient(165deg,#0b3a41,#0a2a31 70%)}
.brand:before{content:"";position:absolute;inset:0;background-image:linear-gradient(#ffffff0a 1px,transparent 1px),linear-gradient(90deg,#ffffff0a 1px,transparent 1px);background-size:44px 44px;mask-image:linear-gradient(#000,transparent 85%)}
.brand>*{position:relative}
.logo{display:flex;align-items:center;gap:12px;font-family:var(--serif);font-size:24px;font-weight:700;color:#fff;text-decoration:none}
.logo i{width:38px;height:38px;border-radius:11px;background:var(--coral);display:grid;place-items:center}
.logo i svg{width:22px;height:22px}
.brand h2{font-size:clamp(30px,3.2vw,44px);line-height:1.1;color:#fff;margin-bottom:18px}
.brand h2 em{font-style:normal;color:#ffb199}
.brand p{color:#b8d3d0;max-width:420px;margin:0}
.ecg{width:100%;height:70px;margin-top:34px;opacity:.9}
.ecg path{fill:none;stroke:#ff8d6b;stroke-width:2.5;stroke-linecap:round;stroke-linejoin:round;stroke-dasharray:600;stroke-dashoffset:600;animation:draw 3.2s ease-out forwards}
@keyframes draw{to{stroke-dashoffset:0}}
.steps{list-style:none;margin:0;padding:0;display:grid;gap:16px}
.steps li{display:flex;gap:14px;align-items:flex-start;color:#cfe5e2;font-size:15px}
.steps b{display:block;color:#fff;font-weight:600}
.steps span.n{width:30px;height:30px;border-radius:50%;border:1.5px solid #ffffff55;display:grid;place-items:center;font-weight:600;font-size:14px;flex:none;color:#fff}
.auth-main{display:flex;justify-content:center;align-items:flex-start;padding:56px clamp(20px,5vw,80px)}
.auth-card{width:100%;max-width:680px}
.seg{display:inline-grid;grid-template-columns:1fr 1fr;background:#ebe5d6;border-radius:14px;padding:5px;margin-bottom:32px;width:100%;max-width:340px}
.seg a{text-align:center;padding:11px 16px;border-radius:10px;text-decoration:none;color:var(--muted);font-weight:600;font-size:15px}
.seg a.on{background:#fff;color:var(--ink);box-shadow:0 1px 4px #12303a1f}
.auth-card h1{font-size:34px;margin-bottom:6px}
.auth-card>p.sub{color:var(--muted);margin:0 0 32px}
.card{background:var(--paper);border:1px solid var(--line);border-radius:var(--r);padding:clamp(24px,3vw,40px)}
.fset{border:0;padding:0;margin:0 0 36px}
.fset legend{padding:0;display:flex;align-items:center;gap:12px;font-family:var(--serif);font-size:20px;font-weight:600;margin-bottom:22px}
.fset legend span{width:28px;height:28px;border-radius:50%;background:var(--ink);color:#fff;font:600 14px var(--sans);display:grid;place-items:center}
.fgrid{display:grid;grid-template-columns:1fr 1fr;gap:24px 22px}.span2{grid-column:1/-1}
.pwwrap{position:relative}.pwwrap input{padding-right:78px}
.pwwrap button{position:absolute;right:6px;top:14px;padding:7px 12px;font-size:13px;background:#ebe5d6;color:var(--ink);border-radius:8px}
.rules{list-style:none;margin:14px 0 0;padding:0;display:grid;grid-template-columns:1fr 1fr;gap:6px 16px;font-size:13px;color:var(--muted)}
.rules li:before{content:"○ ";color:#b6ae99}.rules li.ok{color:var(--ok)}.rules li.ok:before{content:"● ";color:var(--ok)}
.foot-note{color:var(--muted);font-size:14px;margin-top:22px}
.err-hint{margin:0 0 24px}

/* ---------- Dashboard shell ---------- */
.shell{display:grid;grid-template-columns:280px minmax(0,1fr);min-height:100vh}
.side{background:var(--ink2);color:#cfe0de;padding:28px 20px;display:flex;flex-direction:column;gap:30px;position:sticky;top:0;height:100vh}
.side .logo{padding:0 10px}
.nav{display:grid;gap:6px}
.nav a{--acc:#fff;display:flex;align-items:center;gap:14px;padding:13px 14px;border-radius:12px;color:#b5cbc9;text-decoration:none;font-weight:500;transition:background .15s,color .15s}
.nav a .ico{width:34px;height:34px;border-radius:10px;display:grid;place-items:center;background:#ffffff0f;color:var(--acc);transition:all .15s}
.nav a:hover{background:#ffffff0c;color:#fff}
.nav a.on{background:#ffffff14;color:#fff}.nav a.on .ico{background:var(--acc);color:#fff}
.me{margin-top:auto;padding:16px;border-radius:14px;background:#ffffff0c;display:grid;gap:12px}
.me small{color:#8fb0ad;display:block}.me strong{color:#fff;font-weight:600;word-break:break-word}
.me a{color:#ffb199;font-weight:600;text-decoration:none;font-size:14px}
.content{padding:clamp(28px,4vw,56px) clamp(20px,5vw,72px) 96px;max-width:1240px;width:100%}
.mod-head{display:flex;gap:20px;align-items:center;margin-bottom:40px}
.mod-head .chip-ic{width:60px;height:60px;border-radius:18px;display:grid;place-items:center;color:#fff;background:var(--acc);flex:none}
.mod-head .chip-ic .ic{width:28px;height:28px}
.mod-head h1{font-size:clamp(28px,3.4vw,40px);line-height:1.1}
.mod-head p{margin:6px 0 0;color:var(--muted)}
.cols{display:grid;grid-template-columns:minmax(0,1fr) minmax(0,1fr);gap:32px;align-items:start}
.cols.wide-left{grid-template-columns:minmax(0,3fr) minmax(0,2fr)}
.panel{background:var(--paper);border:1px solid var(--line);border-radius:var(--r);padding:clamp(24px,3vw,40px)}
.panel+.panel{margin-top:32px}.cols>.panel+.panel{margin-top:0}
.panel h2{font-size:22px;margin-bottom:6px}
.panel .lead{color:var(--muted);margin:0 0 28px;font-size:15px}
.panel form{display:grid;gap:24px}
.panel form .fgrid{gap:24px 22px}
.panel form button{justify-self:start}
.side-note{background:var(--mint);border-radius:var(--r);padding:28px;color:#0c4a50}
.side-note h3{font-size:18px;margin-bottom:10px}.side-note p,.side-note li{font-size:15px;margin:0 0 10px}.side-note ul{padding-left:18px;margin:0}
.tbl-wrap{overflow-x:auto;border:1px solid var(--line);border-radius:12px;background:#fff}
table{border-collapse:collapse;width:100%;font-size:14.5px}
th,td{text-align:left;padding:13px 16px;border-bottom:1px solid var(--line);white-space:nowrap}
th{font-size:12px;text-transform:uppercase;letter-spacing:.06em;color:var(--muted);background:#faf6ec;font-weight:600}
tr:last-child td{border-bottom:0}
.pill{display:inline-block;padding:3px 10px;border-radius:99px;font-size:12.5px;font-weight:600;background:#ebe5d6;color:var(--ink)}
.pill.ok{background:var(--okbg);color:var(--ok)}.pill.off{background:#eee;color:#777}
.empty{border:1.5px dashed var(--line);border-radius:14px;padding:32px;text-align:center;color:var(--muted)}
.empty b{display:block;color:var(--ink);margin-bottom:4px}
.list-title{font-family:var(--serif);font-size:18px;font-weight:600;margin:36px 0 14px}
.hello{background:linear-gradient(135deg,#0f6b73,#0a4f56);color:#fff;border-radius:22px;padding:clamp(28px,4vw,48px);display:flex;justify-content:space-between;gap:32px;flex-wrap:wrap;align-items:center;margin-bottom:36px}
.hello h2{font-size:clamp(26px,3vw,36px);color:#fff;margin-bottom:8px}.hello p{margin:0;color:#cde8e5;max-width:460px}
.ring{--p:0;width:112px;height:112px;border-radius:50%;background:conic-gradient(#ffb199 calc(var(--p)*1%),#ffffff2b 0);display:grid;place-items:center;flex:none}
.ring div{width:86px;height:86px;border-radius:50%;background:#0c5760;display:grid;place-items:center;text-align:center;line-height:1.1}
.ring strong{font:700 24px var(--serif)}.ring small{display:block;font-size:11px;color:#bfe0dc;letter-spacing:.06em;text-transform:uppercase}
.todo{list-style:none;margin:0;padding:0;display:grid;gap:14px}
.todo a{display:flex;align-items:center;gap:16px;padding:18px 20px;border:1px solid var(--line);border-radius:14px;background:var(--paper);text-decoration:none;color:var(--ink);transition:border-color .15s,transform .15s}
.todo a:hover{border-color:var(--teal);transform:translateX(3px)}
.todo .tick{width:30px;height:30px;border-radius:50%;border:2px solid var(--line);display:grid;place-items:center;flex:none;color:#fff;font-size:15px}
.todo .done .tick{background:var(--ok);border-color:var(--ok)}
.todo .t{flex:1}.todo .t b{display:block;font-weight:600}.todo .t span{color:var(--muted);font-size:14px}
.todo .go{color:var(--muted)}
.tiles{display:grid;grid-template-columns:repeat(auto-fit,minmax(200px,1fr));gap:20px;margin-top:36px}
.tile{background:var(--paper);border:1px solid var(--line);border-radius:var(--r);padding:24px}
.tile small{color:var(--muted);font-weight:600;text-transform:uppercase;letter-spacing:.06em;font-size:12px}
.tile strong{display:block;font:600 34px var(--serif);margin-top:6px}
.preview{width:132px;height:132px;border-radius:50%;background:#ebe5d6 center/cover;border:4px solid #fff;box-shadow:0 0 0 1.5px var(--line);display:grid;place-items:center;color:var(--muted);margin-bottom:6px;font-size:13px;text-align:center}
.kv{display:grid;grid-template-columns:auto 1fr;gap:10px 20px;margin:0}.kv dt{color:var(--muted);font-size:14px}.kv dd{margin:0;font-weight:500}
details{margin-top:24px}summary{cursor:pointer;color:var(--muted);font-weight:600}
pre{background:#12303a;color:#d6ece9;padding:20px;border-radius:12px;overflow:auto;font-size:13px;max-height:420px}

@media(min-width:861px){.auth .brand{position:sticky;top:0;height:100vh;align-self:start}}
@media(max-width:1020px){.cols,.cols.wide-left{grid-template-columns:minmax(0,1fr)}}
@media(max-width:860px){
 .auth{grid-template-columns:minmax(0,1fr)}.brand{padding:32px 24px;gap:24px}.brand .steps,.ecg{display:none}.brand h2{font-size:28px}
 .shell{grid-template-columns:minmax(0,1fr)}.content{min-width:0}.side{position:static;height:auto;flex-direction:row;flex-wrap:wrap;align-items:center;gap:12px;padding:14px 16px}
 .nav{display:flex;overflow-x:auto;order:3;width:100%;padding-bottom:4px}.nav a{white-space:nowrap;padding:8px 12px}.nav a .ico{width:28px;height:28px}
 .me{margin:0 0 0 auto;padding:8px 12px;display:flex;align-items:center;gap:12px;order:2}.me small{display:none}
 .mod-head .chip-ic{width:48px;height:48px;border-radius:14px}
}
@media(max-width:560px){.fgrid,.rules{grid-template-columns:1fr}.span2{grid-column:auto}}
</style></head><body>

{%- macro icon(n) -%}<svg class="ic" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="1.8" stroke-linecap="round" stroke-linejoin="round" aria-hidden="true">
{%- if n=='home' %}<path d="M3 11l9-8 9 8"/><path d="M5 10v10h14V10"/>
{%- elif n=='user' %}<circle cx="12" cy="8" r="4"/><path d="M4 21c0-4 4-6 8-6s8 2 8 6"/>
{%- elif n=='camera' %}<path d="M4 8h3l2-3h6l2 3h3v12H4z"/><circle cx="12" cy="13" r="4"/>
{%- elif n=='list' %}<path d="M8 6h13M8 12h13M8 18h13"/><path d="M3 6h.01M3 12h.01M3 18h.01"/>
{%- elif n=='clock' %}<circle cx="12" cy="12" r="9"/><path d="M12 7v5l3 2"/>
{%- elif n=='calendar' %}<rect x="3" y="5" width="18" height="16" rx="2"/><path d="M3 10h18M8 3v4M16 3v4"/>
{%- endif -%}</svg>{%- endmacro -%}

{%- macro hospital_options(sel,blank='Keep current') -%}<option value="">{{ blank }}</option>{% for i,n in hospitals %}<option value="{{i}}" {{ 'selected' if sel==i }}>{{n}}</option>{% endfor %}{%- endmacro -%}
{%- macro spec_options(sel,blank='Keep current') -%}<option value="">{{ blank }}</option>{% for i,n in specializations %}<option value="{{i}}" {{ 'selected' if sel==i }}>{{n}}</option>{% endfor %}{%- endmacro -%}
{%- macro banners() -%}{% for m in messages %}<div class="banner {{m.kind}}" role="status"><span>{{m.text}}</span><button type="button" aria-label="Dismiss" onclick="this.parentNode.remove()">×</button></div>{% endfor %}{%- endmacro -%}
{%- macro cell(v) -%}{% if v is none or v=='' %}—{% elif v is mapping or (v is iterable and v is not string) %}{{ v|tojson }}{% else %}{{v}}{% endif %}{%- endmacro -%}
{%- macro table(rows, columns) -%}<div class="tbl-wrap"><table><thead><tr>{% for c in columns %}<th>{{c|replace('_',' ')}}</th>{% endfor %}</tr></thead><tbody>{% for r in rows %}<tr>{% for c in columns %}<td>{{ cell(r.get(c)) }}</td>{% endfor %}</tr>{% endfor %}</tbody></table></div>{%- endmacro -%}

{% if not logged_in %}
<div class="auth">
 <aside class="brand">
  <a class="logo" href="/"><i><svg viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.4" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg></i>MediSlot</a>
  <div>
   <h2>Run your clinic hours <em>without the back-and-forth.</em></h2>
   <p>Set your consultation types, publish your weekly availability and see each day’s schedule in one calm workspace.</p>
   <svg class="ecg" viewBox="0 0 520 70" preserveAspectRatio="none" aria-hidden="true"><path d="M0 40 H150 L172 40 L190 10 L212 62 L232 26 L248 40 H340 L360 40 L376 22 L392 46 L408 40 H520"/></svg>
  </div>
  <ol class="steps">
   <li><span class="n">1</span><div><b>Create your doctor account</b>Name, hospital, specialization and license.</div></li>
   <li><span class="n">2</span><div><b>Add consultation types</b>Duration, fee and capacity.</div></li>
   <li><span class="n">3</span><div><b>Publish weekly availability</b>Patients can start booking.</div></li>
  </ol>
 </aside>
 <main class="auth-main"><div class="auth-card">
  <nav class="seg" aria-label="Sign in or sign up"><a href="/?mode=signin" class="{{ 'on' if mode=='signin' }}">Sign in</a><a href="/?mode=signup" class="{{ 'on' if mode=='signup' }}">Create account</a></nav>
  {{ banners() }}
  {% if mode=='signin' %}
  <h1>Doctor sign in</h1><p class="sub">Welcome back. Use your MediSlot doctor account.</p>
  <div class="card" style="max-width:520px"><form method="post" action="/login" class="fgrid" style="grid-template-columns:1fr">
   <label>Email<input name="email" type="email" value="{{ prefill_email }}" autocomplete="username" required {{ 'autofocus' if not prefill_email }}></label>
   <label>Password<input name="password" type="password" autocomplete="current-password" required {{ 'autofocus' if prefill_email }}></label>
   <button class="block">Sign in</button></form>
   <p class="foot-note">New to MediSlot? <a href="/?mode=signup">Create a doctor account</a></p></div>
  {% else %}
  <h1>Create your doctor account</h1><p class="sub">Fields marked <span style="color:var(--coral)">*</span> are required. You can sign in straight after.</p>
  <form method="post" action="/signup" class="card" autocomplete="on">
   <fieldset class="fset"><legend><span>1</span>Account</legend><div class="fgrid">
    <label class="span2">Email<span class="req">*</span><input name="email" type="email" value="{{ form.get('email','') }}" autocomplete="username" required></label>
    <label>Password<span class="req">*</span><div class="pwwrap"><input id="su-password" name="password" type="password" autocomplete="new-password" required><button type="button" data-toggle="su-password">Show</button></div>
     <ul class="rules" id="rules"><li data-r="len">12+ characters</li><li data-r="low">Lowercase letter</li><li data-r="up">Uppercase letter</li><li data-r="num">A number</li><li data-r="sym">A symbol</li></ul></label>
    <label>Confirm password<span class="req">*</span><div class="pwwrap"><input id="su-confirm" name="confirmPassword" type="password" autocomplete="new-password" required><button type="button" data-toggle="su-confirm">Show</button></div></label>
    <label class="span2">Phone<span class="opt">optional</span><input name="phone" type="tel" maxlength="25" value="{{ form.get('phone','') }}" autocomplete="tel"></label>
   </div></fieldset>
   <fieldset class="fset"><legend><span>2</span>Professional details</legend><div class="fgrid">
    <label>First name<span class="req">*</span><input name="firstName" maxlength="75" value="{{ form.get('firstName','') }}" autocomplete="given-name" required></label>
    <label>Last name<span class="req">*</span><input name="lastName" maxlength="75" value="{{ form.get('lastName','') }}" autocomplete="family-name" required></label>
    <label class="span2">Hospital<span class="req">*</span><select name="hospitalId" required>{{ hospital_options(form.get('hospitalId')|int(none) if form.get('hospitalId') else none,'Select your hospital') }}</select></label>
    <label class="span2">Specialization<span class="req">*</span><select name="specializationId" required>{{ spec_options(form.get('specializationId')|int(none) if form.get('specializationId') else none,'Select your specialization') }}</select></label>
    <label>License number<span class="req">*</span><input id="su-license" name="licenseNumber" placeholder="TS-1234" pattern="[A-Za-z][A-Za-z ]{1,29}-[0-9]{4}" title="State code or name, a hyphen, then 4 digits. For example TS-1234 or Telangana-1234." value="{{ form.get('licenseNumber','') }}" required><span class="hint">State, hyphen, 4 digits — e.g. TS-1234 or Telangana-1234</span></label>
    <label>Experience (years)<span class="opt">optional</span><input name="yearsExperience" type="number" min="0" max="80" value="{{ form.get('yearsExperience','') }}"></label>
   </div></fieldset>
   <fieldset class="fset" style="margin-bottom:28px"><legend><span>3</span>About you <small style="font:400 14px var(--sans);color:var(--muted)">optional</small></legend><div class="fgrid">
    <label class="span2">Clinic address<input name="clinicAddress" maxlength="500" value="{{ form.get('clinicAddress','') }}"></label>
    <label class="span2">Qualifications<textarea name="qualifications" maxlength="5000" placeholder="MBBS, MD (Cardiology)…">{{ form.get('qualifications','') }}</textarea></label>
    <label class="span2">Languages spoken<input name="languages" value="{{ form.get('languages','') }}" placeholder="English, Telugu, Hindi"><span class="hint">Separate with commas</span></label>
   </div></fieldset>
   <button class="block">Create doctor account</button>
   <p class="foot-note">Already registered? <a href="/?mode=signin">Sign in</a></p>
  </form>
  {% endif %}
 </div></main>
</div>

{% else %}
{% set cur = module %}
<div class="shell">
 <aside class="side">
  <a class="logo" href="/?m=overview"><i><svg viewBox="0 0 24 24" fill="none" stroke="#fff" stroke-width="2.4" stroke-linecap="round"><path d="M12 5v14M5 12h14"/></svg></i>MediSlot</a>
  <nav class="nav" aria-label="Dashboard modules">
   {% for key,label,ic,acc,desc in modules %}<a href="/?m={{key}}" class="{{ 'on' if key==cur }}" style="--acc:{{acc}}"><span class="ico">{{ icon(ic) }}</span>{{label}}</a>{% endfor %}
  </nav>
  <div class="me"><div><small>Signed in as doctor</small><strong>{{ display_name or 'Doctor' }}</strong></div><a href="/logout">Log out</a></div>
 </aside>
 <main class="content">
  {% for key,label,ic,acc,desc in modules if key==cur %}
  <header class="mod-head" style="--acc:{{acc}}"><span class="chip-ic">{{ icon(ic) }}</span><div><h1>{{label}}</h1><p>{{desc}}</p></div></header>
  {% endfor %}
  {{ banners() }}

  {% if cur=='overview' %}
   <section class="hello"><div><h2>{{ greeting }}{{ ', Dr. '+last_name if last_name }}</h2><p>{% if progress is not none %}{% if progress==100 %}Your practice is fully set up. Patients can find and book you.{% else %}Finish the steps below so patients can start booking with you.{% endif %}{% else %}Pick a module on the left to manage your practice.{% endif %}</p></div>
   {% if progress is not none %}<div class="ring" style="--p:{{progress}}"><div><span><strong>{{progress}}%</strong><small>set up</small></span></div></div>{% endif %}</section>
   {% if steps %}<ul class="todo">{% for s in steps %}<li><a href="/?m={{s.module}}" class="{{ 'done' if s.done }}"><span class="tick">{{ '✓' if s.done }}</span><span class="t"><b>{{s.title}}</b><span>{{s.text}}</span></span><span class="go">{{ 'Review' if s.done else 'Start' }} →</span></a></li>{% endfor %}</ul>{% endif %}
   {% if counts %}<div class="tiles"><div class="tile"><small>Consultation types</small><strong>{{counts.types}}</strong></div><div class="tile"><small>Weekly patterns</small><strong>{{counts.patterns}}</strong></div>{% if profile and profile.get('timezone') %}<div class="tile"><small>Timezone</small><strong style="font-size:22px;padding-top:10px">{{profile.get('timezone')}}</strong></div>{% endif %}</div>{% endif %}

  {% elif cur=='profile' %}
   <div class="cols wide-left"><section class="panel"><h2>Profile editor</h2><p class="lead">Only the fields you fill in are changed.</p>
   <form method="post" action="/profile"><div class="fgrid">
    <label>First name<input name="firstName" value="{{ p.get('first_name','') }}" required></label>
    <label>Last name<input name="lastName" value="{{ p.get('last_name','') }}" required></label>
    <label class="span2">Hospital<select name="hospitalId">{{ hospital_options(p.get('hospital_id')) }}</select></label>
    <label class="span2">Specialization<select name="specializationId">{{ spec_options(p.get('specialization_id')) }}</select></label>
    <label>License number<input name="licenseNumber" value="{{ p.get('license_number','') }}"></label>
    <label>Experience (years)<input name="yearsExperience" type="number" min="0" value="{{ p.get('years_experience') if p.get('years_experience') is not none else '' }}"></label>
    <label class="span2">Qualifications<textarea name="qualifications">{{ p.get('qualifications') or '' }}</textarea></label>
    <label class="span2">Timezone (IANA)<input name="timezone" list="tzs" value="{{ p.get('timezone') or 'Asia/Kolkata' }}" required><datalist id="tzs"><option>Asia/Kolkata</option><option>UTC</option><option>Asia/Dubai</option><option>Asia/Singapore</option><option>Europe/London</option><option>America/New_York</option></datalist></label>
   </div><button>Save profile</button></form></section>
   <aside class="side-note"><h3>Good to know</h3><ul><li>Patients see your name, hospital and specialization first.</li><li>Your timezone decides which hours count as a “day” in the schedule view.</li></ul></aside></div>

  {% elif cur=='photo' %}
   <div class="cols"><section class="panel"><h2>Upload a photo</h2><p class="lead">A clear, friendly headshot builds trust. JPEG, PNG or WebP up to 5&nbsp;MB.</p>
   <form method="post" action="/photo" enctype="multipart/form-data"><div class="preview" id="preview">{{ 'Photo on file' if p.get('photo_key') else 'No photo yet' }}</div>
    <label>Choose an image<input id="photo-input" type="file" name="photo" accept="image/jpeg,image/png,image/webp" required></label><button>Upload photo</button></form></section>
   <aside class="side-note"><h3>Tips</h3><ul><li>Use a square crop with your face centred.</li><li>Good light, plain background.</li><li>Uploading a new photo replaces the old one.</li></ul></aside></div>

  {% elif cur=='consultation' %}
   <div class="cols wide-left"><section class="panel"><h2>New consultation type</h2><p class="lead">Each type gets its own length, fee and capacity. You’ll use them when you set availability.</p>
   <form method="post" action="/consultation-types"><div class="fgrid">
    <label class="span2">Name<input name="name" placeholder="e.g. First visit" required></label>
    <label>Duration<select name="durationMinutes">{% for d in (15,30,45,60,90,120) %}<option value="{{d}}" {{ 'selected' if d==30 }}>{{d}} minutes</option>{% endfor %}</select></label>
    <label>Fee<input name="fee" type="number" min="0" step="0.01" placeholder="500" required></label>
    <label class="span2">Capacity<input name="capacity" type="number" min="1" max="20" value="1"><span class="hint">How many patients can book the same slot (1–20)</span></label>
   </div><button>Create consultation type</button></form></section>
   <aside class="side-note"><h3>How it fits</h3><p>1. Create a consultation type here.</p><p>2. Add weekly hours for it under Availability.</p><p>3. Patients can then book those slots.</p></aside></div>
   <h3 class="list-title">Your consultation types</h3>
   {% if types is none %}<div class="empty"><b>Couldn’t load your list</b>Your changes are still saved. Refresh to try again.</div>
   {% elif types %}<div class="tbl-wrap"><table><thead><tr><th>ID</th><th>Name</th><th>Duration</th><th>Fee</th><th>Capacity</th><th>Status</th></tr></thead><tbody>{% for t in types %}<tr><td>{{t.consultation_type_id}}</td><td>{{t.name}}</td><td>{{t.duration_minutes}} min</td><td>{{t.fee}}</td><td>{{t.capacity}}</td><td><span class="pill {{ 'ok' if t.is_active else 'off' }}">{{ 'Active' if t.is_active else 'Inactive' }}</span></td></tr>{% endfor %}</tbody></table></div>
   {% else %}<div class="empty"><b>Nothing here yet</b>Create your first consultation type above.</div>{% endif %}

  {% elif cur=='availability' %}
   <div class="cols"><section class="panel"><h2>Weekly pattern</h2><p class="lead">Repeat these hours every week for a consultation type.</p>
   {% if types is not none and not active_types %}<div class="empty"><b>Create a consultation type first</b>Availability is attached to a type. <a href="/?m=consultation">Go to Consultation types →</a></div>
   {% else %}<form method="post" action="/availability"><div class="fgrid">
    <label class="span2">Consultation type{% if active_types %}<select name="consultationTypeId" required>{% for t in active_types %}<option value="{{t.consultation_type_id}}">{{t.name}} · {{t.duration_minutes}} min (ID {{t.consultation_type_id}})</option>{% endfor %}</select>{% else %}<input name="consultationTypeId" type="number" required>{% endif %}</label>
    <label class="span2">Weekday<select name="dayOfWeek" required>{% for n,d in weekdays %}<option value="{{n}}">{{d}}</option>{% endfor %}</select></label>
    <label>Start time<input name="startTime" type="time" required></label>
    <label>End time<input name="endTime" type="time" required></label>
   </div><button>Add weekly pattern</button></form>{% endif %}</section>
   <section class="panel"><h2>Blackout date</h2><p class="lead">Block a day you’re away — holiday, conference, leave.</p>
   <form method="post" action="/blackout"><div class="fgrid"><label class="span2">Date<input name="blackoutDate" type="date" required></label><label class="span2">Reason<span class="opt">optional</span><input name="reason" placeholder="e.g. Conference"></label></div><button>Add blackout</button></form></section></div>
   <h3 class="list-title">Current weekly patterns</h3>
   {% if patterns is none %}<div class="empty"><b>Couldn’t load your patterns</b>Refresh to try again.</div>
   {% elif patterns %}<div class="tbl-wrap"><table><thead><tr><th>Day</th><th>Start</th><th>End</th><th>Consultation type</th><th>Status</th></tr></thead><tbody>{% for x in patterns %}<tr><td>{{ day_names.get(x.day_of_week|int, x.day_of_week) }}</td><td>{{ x.start_time }}</td><td>{{ x.end_time }}</td><td>{{ type_names.get(x.consultation_type_id|int, 'ID '~x.consultation_type_id) }}</td><td><span class="pill {{ 'ok' if x.is_active else 'off' }}">{{ 'Active' if x.is_active else 'Inactive' }}</span></td></tr>{% endfor %}</tbody></table></div>
   {% else %}<div class="empty"><b>No weekly hours yet</b>Add your first pattern above.</div>{% endif %}
   <h3 class="list-title">Blackout dates</h3>
   {% if blackouts is none %}<div class="empty"><b>Couldn’t load your blackout dates</b>Refresh to try again.</div>
   {% elif blackouts %}<div class="tbl-wrap"><table><thead><tr><th>Date</th><th>Reason</th></tr></thead><tbody>{% for b in blackouts %}<tr><td>{{b.blackout_date}}</td><td>{{ b.reason or '—' }}</td></tr>{% endfor %}</tbody></table></div>
   {% else %}<div class="empty"><b>No blackout dates</b>You’re open on every pattern day.</div>{% endif %}

  {% elif cur=='schedule' %}
   <section class="panel"><h2>Pick a day</h2><p class="lead">See every slot and appointment for that date, in your timezone.</p>
   <form method="get" action="/schedule" style="grid-template-columns:minmax(0,320px) auto;align-items:end;display:grid;gap:20px"><label>Date<input name="date" type="date" value="{{ sched.date if sched else '' }}" required></label><button style="margin-bottom:0">View schedule</button></form></section>
   {% if sched %}
    {% if sched.data and sched.ok %}
     <div class="tiles"><div class="tile"><small>Date</small><strong style="font-size:24px;padding-top:8px">{{sched.data.get('date')}}</strong></div><div class="tile"><small>Timezone</small><strong style="font-size:24px;padding-top:8px">{{sched.data.get('timezone')}}</strong></div><div class="tile"><small>Slots</small><strong>{{ (sched.data.get('slots') or [])|length }}</strong></div><div class="tile"><small>Appointments</small><strong>{{ (sched.data.get('appointments') or [])|length }}</strong></div></div>
     <h3 class="list-title">Appointments</h3>
     {% if sched.data.get('appointments') %}{{ table(sched.data.appointments, ['start_at','end_at','status','patient_id','consultation_type_id','fee_snapshot','appointment_id']) }}{% else %}<div class="empty"><b>No appointments</b>Nothing is booked for this day.</div>{% endif %}
     <h3 class="list-title">Slots</h3>
     {% if sched.data.get('slots') %}{{ table(sched.data.slots, sched.slot_cols) }}{% else %}<div class="empty"><b>No slots</b>No slots are open for this day. Check your weekly pattern and blackout dates.</div>{% endif %}
    {% else %}<div class="banner error" style="margin-top:28px"><span>{{ sched.error }}</span></div>{% endif %}
    <details><summary>Raw API response</summary><pre>{{ sched.raw }}</pre></details>
   {% endif %}
  {% endif %}
 </main>
</div>
{% endif %}
<script>
(function(){
 function $(s,r){return (r||document).querySelector(s)}
 document.querySelectorAll('[data-toggle]').forEach(function(b){b.addEventListener('click',function(){var i=document.getElementById(b.dataset.toggle);var show=i.type==='password';i.type=show?'text':'password';b.textContent=show?'Hide':'Show'})});
 var pw=document.getElementById('su-password'),cf=document.getElementById('su-confirm');
 if(pw){
  var tests={len:function(v){return v.length>=12},low:function(v){return /[a-z]/.test(v)},up:function(v){return /[A-Z]/.test(v)},num:function(v){return /\d/.test(v)},sym:function(v){return /[^A-Za-z0-9]/.test(v)}};
  function check(){document.querySelectorAll('#rules li').forEach(function(li){li.classList.toggle('ok',tests[li.dataset.r](pw.value))});
   cf.setCustomValidity(cf.value&&cf.value!==pw.value?'Passwords do not match':'')}
  pw.addEventListener('input',check);cf.addEventListener('input',check);check();
 }
 var lic=document.getElementById('su-license');
 if(lic)lic.addEventListener('blur',function(){lic.value=lic.value.trim().toUpperCase()});
 var pi=document.getElementById('photo-input');
 if(pi)pi.addEventListener('change',function(){var f=pi.files&&pi.files[0],pv=document.getElementById('preview');if(!f)return;
  var r=new FileReader();r.onload=function(e){pv.style.backgroundImage='url('+e.target.result+')';pv.textContent=''};r.readAsDataURL(f)});
})();
</script>
</body></html>
"""
class SessionExpired(Exception):
    pass

def notify(text,kind="info"):
    """Queue one banner message for the next page view. kind: info | success | error."""
    session["messages"]=[{"kind":kind,"text":text}]
def api_message(r):
    try: return r.json().get("message") or r.text[:160]
    except (ValueError,AttributeError): return r.text[:160]
def report(r,ok_text,label):
    """Same API status as before, now shown as a friendly success / error banner."""
    if r.ok: notify(f"{ok_text} (HTTP {r.status_code})","success")
    else: notify(f"{label} failed: HTTP {r.status_code} - {api_message(r)}","error")

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

def render_dashboard(module,**extra):
    """Shared renderer for every dashboard module."""
    ctx=dict(logged_in=True,module=module,modules=MODULES,messages=session.pop("messages",[]),hospitals=HOSPITALS,specializations=SPECIALIZATIONS,weekdays=WEEKDAYS,day_names=dict(WEEKDAYS),
             profile=None,p={},display_name=session.get("display_name"),last_name=None,greeting="Welcome",progress=None,steps=None,counts=None,types=None,active_types=None,type_names={},patterns=None,blackouts=None,sched=None)
    ctx.update(extra)
    return render_template_string(PAGE,**ctx)
def load_profile():
    data=read_json("/doctors/me")
    return (data or {}).get("profile") if isinstance(data,dict) else None
def items_of(data):
    return data.get("items",[]) if isinstance(data,dict) else None

@app.get("/")
def home():
    if not session.get("access_token"):
        mode="signup" if request.args.get("mode")=="signup" else "signin"
        return render_template_string(PAGE,logged_in=False,mode=mode,messages=session.pop("messages",[]),form=session.pop("signup_form",{}) if mode=="signup" else {},prefill_email=session.pop("prefill_email","") if mode=="signin" else "",hospitals=HOSPITALS,specializations=SPECIALIZATIONS)
    module=request.args.get("m","overview")
    if module not in MODULE_KEYS: module="overview"
    extra={}
    if module in ("overview","profile","photo"):
        profile=load_profile()
        extra.update(profile=profile,p=profile or {})
        if profile:
            session["display_name"]=f"Dr. {profile.get('first_name','')} {profile.get('last_name','')}".strip()
            extra["last_name"]=profile.get("last_name")
    if module in ("overview","consultation","availability"):
        types=items_of(read_json("/doctors/me/consultation-types"))
        extra["types"]=types
        if types is not None:
            extra["active_types"]=[t for t in types if t.get("is_active")]
            extra["type_names"]={t["consultation_type_id"]:t["name"] for t in types}
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
        notify(f"Sign-in failed: HTTP {r.status_code} - {payload.get('message') or r.text[:120]}","error")
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
    return fail(f"Sign-up failed: HTTP {r.status_code} - {api_message(r)}")
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
    data={"name":request.form["name"],"durationMinutes":int(request.form["durationMinutes"]),"fee":float(request.form["fee"]),"capacity":int(request.form["capacity"])}
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
        notify(f"Could not create upload URL: HTTP {r.status_code}","error")
        return redirect("/?m=photo")
    upload=r.json()
    put=requests.put(upload["uploadUrl"],data=content,headers={"Content-Type":f.mimetype,"x-amz-server-side-encryption":"AES256"},timeout=20)
    if not put.ok:
        notify(f"S3 upload failed: HTTP {put.status_code}","error")
        return redirect("/?m=photo")
    saved=api("PATCH","/doctors/me",json={"photoKey":upload["photoKey"]})
    report(saved,"Photo uploaded and profile updated","Photo uploaded but profile update")
    return redirect("/?m=photo")

@app.get("/schedule")
def schedule():
    day=request.args.get("date","")
    r=api("GET","/doctors/me/schedule",params={"date":day})
    # Rendered directly (not stored in the cookie session) so a long day can never overflow the 4 KB cookie.
    sched=dict(date=day,ok=r.ok,raw=r.text[:12000],data=None,error=f"Schedule API: HTTP {r.status_code} - {api_message(r)}",slot_cols=[])
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
