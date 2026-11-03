"""Patient side of the shared MediSlot dashboard.

This module adds the patient pages to the SAME Flask app, session handling and API client the doctor dashboard
uses (doctor_dashboard.py calls `init(app, ...)` once). Everything the patient sees comes from the API Gateway
methods - the dashboard holds no data of its own:

    /patient                      home
    /patient/profile              profile editor (GET /patients/me, PATCH /patients/me) + close account (DELETE)
    /patient/doctors              doctor search with filters and pagination (GET /doctors/search)
    /patient/doctors/<id>         public doctor profile + slot browser (GET /doctors/{id}, GET /doctors/{id}/slots)

Role separation: the guard in doctor_dashboard.py only lets a signed-in PATIENT reach this blueprint, and only a
DOCTOR reach the doctor pages. The API enforces it again (patient-domain Lambda checks the PATIENT role; the
patient id always comes from the verified token, never from the page).
"""
import re
import time
from datetime import date, datetime, timedelta
from urllib.parse import urlencode
from zoneinfo import ZoneInfo

import boto3
import requests
from flask import Blueprint, Response, redirect, render_template, request, session

bp = Blueprint("patient", __name__, url_prefix="/patient")
_deps = {}

PAGE_SIZE = 9
GENDERS = ["Male", "Female", "Other", "Prefer not to say"]
PHOTO_NAME = re.compile(r"^[0-9a-f]{32}\.(jpg|png|webp)$")
DATE_RE = re.compile(r"^\d{4}-\d{2}-\d{2}$")
FILTERS_TTL = 300
_filters = {"at": 0.0, "data": None}
NAV = [
    ("home", "Home", "home", "#0f6b73", "/patient", "Your account and where to start."),
    ("doctors", "Find a doctor", "search", "#2563b8", "/patient/doctors", "Search by name, hospital, specialization or city."),
    ("profile", "My profile", "user", "#7a4fd6", "/patient/profile", "Your details, kept private to you."),
]


def init(app, **deps):
    """Called once by doctor_dashboard.py with its shared helpers (api, notify, ...)."""
    _deps.update(deps, app=app)
    app.register_blueprint(bp)


def d(name):
    return _deps[name]


# --------------------------------------------------------------------------- small helpers
def rupees(value):
    try:
        number = float(value)
    except (TypeError, ValueError):
        return None
    return "₹{:,.0f}".format(number) if number == int(number) else "₹{:,.2f}".format(number)


def initials(first, last):
    return ((first or "?")[:1] + (last or "")[:1]).upper()


def photo_src(doctor_id, key):
    """Dashboard URL for a doctor's photo, or None. The key must be exactly doctors/<id>/<32 hex>.<ext>."""
    if not key or not isinstance(key, str):
        return None
    prefix = f"doctors/{doctor_id}/"
    name = key[len(prefix):] if key.startswith(prefix) else ""
    return f"/patient/doctor-photo/{doctor_id}/{name}" if PHOTO_NAME.match(name) else None


def render(page, **extra):
    ctx = dict(
        logged_in=True, role="PATIENT", page=page, nav=NAV, messages=session.pop("messages", []),
        display_name=session.get("display_name"), page_title="MediSlot", genders=GENDERS,
    )
    ctx.update(extra)
    return render_template("dashboard.html", **ctx)


def get_filters():
    """Hospitals / specializations / cities for the search form. Cached briefly; falls back to the built-in lists."""
    now = time.time()
    if _filters["data"] and now - _filters["at"] < FILTERS_TTL:
        return _filters["data"]
    data = d("read_json")("/doctors/filters")
    if isinstance(data, dict) and data.get("hospitals") is not None:
        _filters.update(at=now, data=data)
        return data
    return {
        "hospitals": [{"hospitalId": i, "name": n, "city": "Hyderabad"} for i, n in d("HOSPITALS")],
        "specializations": [{"specializationId": i, "name": n} for i, n in d("SPECIALIZATIONS")],
        "cities": ["Hyderabad"],
    }


def load_profile():
    data = d("read_json")("/patients/me")
    profile = (data or {}).get("profile") if isinstance(data, dict) else None
    if profile:
        session["display_name"] = f"{profile.get('firstName', '')} {profile.get('lastName', '')}".strip()
    return profile


# --------------------------------------------------------------------------- public convenience URLs
@bp.get("/login", endpoint="login_page")
def login_page():
    return redirect("/?mode=signin&role=patient")


@bp.get("/register", endpoint="register_page")
def register_page():
    return redirect("/?mode=signup&role=patient")


# --------------------------------------------------------------------------- home
@bp.get("/", strict_slashes=False)
def home():
    profile = load_profile()
    hour = datetime.now(ZoneInfo("Asia/Kolkata")).hour
    greeting = "Good morning" if hour < 12 else "Good afternoon" if hour < 17 else "Good evening"
    return render("home", profile=profile, greeting=greeting, active="home")


# --------------------------------------------------------------------------- profile
FORM_FIELDS = ("firstName", "lastName", "phone", "dateOfBirth", "gender", "address")


@bp.get("/profile")
def profile_page():
    return render("profile", profile=load_profile(), form=None, active="profile", today=date.today().isoformat())


@bp.post("/profile")
def profile_save():
    form = {k: request.form.get(k, "").strip() for k in FORM_FIELDS}
    payload = {k: form[k] for k in FORM_FIELDS if k != "address" and form[k] != ""}
    payload["address"] = form["address"]                      # an empty address clears it
    for key, label in (("firstName", "first name"), ("lastName", "last name"), ("phone", "phone"),
                       ("dateOfBirth", "date of birth"), ("gender", "gender")):
        if not form[key]:
            d("notify")(f"Please fill in your {label}.", "error")
            return render("profile", profile=load_profile(), form=form, active="profile", today=date.today().isoformat())
    r = d("api")("PATCH", "/patients/me", json=payload)
    if r.ok:
        d("notify")("Profile saved.", "success")
        return redirect("/patient/profile")
    d("app").logger.warning("patient profile update failed: HTTP %s %s", r.status_code, r.text[:300])
    d("notify")(f"Profile not saved: {d('api_message')(r)}", "error")
    return render("profile", profile=load_profile(), form=form, active="profile", today=date.today().isoformat())


@bp.post("/profile/delete")
def profile_delete():
    password = request.form.get("password", "")
    if not password or request.form.get("confirm") != "yes":
        d("notify")("To close your account, enter your password and tick the confirmation box.", "error")
        return redirect("/patient/profile")
    r = d("api")("DELETE", "/patients/me", json={"password": password, "refreshToken": session.get("refresh_token")})
    if not r.ok:
        d("app").logger.warning("patient account close failed: HTTP %s", r.status_code)
        d("notify")(f"Account not closed: {d('api_message')(r)}", "error")
        return redirect("/patient/profile")
    session.clear()
    d("notify")("Your account has been closed and your personal details erased.", "success")
    return redirect("/?mode=signin&role=patient")


# --------------------------------------------------------------------------- doctor search
def search_args(args):
    """Clean the search form values from the query string (never trust them; the API validates again)."""
    out = {}
    name = " ".join(args.get("name", "").split())[:100]
    if name:
        out["name"] = name
    for key in ("hospitalId", "specializationId"):
        value = args.get(key, "").strip()
        if re.fullmatch(r"[0-9]{1,9}", value) and int(value) > 0:
            out[key] = value
    city = " ".join(args.get("city", "").split())[:100]
    if city:
        out["city"] = city
    if args.get("acceptingOnly", "").lower() in ("1", "true", "on", "yes"):
        out["acceptingOnly"] = "1"
    return out


def page_number(args):
    raw = args.get("page", "").strip()
    return min(int(raw), 10000) if re.fullmatch(r"[0-9]{1,5}", raw) and int(raw) >= 1 else 1


def page_links(current, total_pages, base):
    """Numbered pager entries with gaps: 1 ... 4 5 [6] 7 8 ... 20"""
    wanted = sorted({1, total_pages} | {p for p in range(current - 2, current + 3) if 1 <= p <= total_pages})
    links, last = [], 0
    for p in wanted:
        if p - last > 1:
            links.append({"gap": True})
        links.append({"gap": False, "n": p, "current": p == current, "url": "/patient/doctors?" + urlencode({**base, "page": p})})
        last = p
    return links


def doctor_card(item):
    langs = item.get("languages") or []
    exp = item.get("yearsExperience")
    return {
        "id": item["doctorId"], "name": item["displayName"], "initials": initials(item.get("firstName"), item.get("lastName")),
        "photo": photo_src(item["doctorId"], item.get("photoKey")),
        "specialization": (item.get("specialization") or {}).get("name"),
        "hospital": (item.get("hospital") or {}).get("name"), "city": (item.get("hospital") or {}).get("city"),
        "experience": f"{exp} yr{'s' if exp != 1 else ''} experience" if isinstance(exp, int) and exp > 0 else None,
        "languages": ", ".join(langs[:4]) if langs else None,
        "fee": rupees(item.get("startingFee")), "types": item.get("consultationTypeCount", 0),
        "accepting": bool(item.get("acceptingBookings")),
    }


@bp.get("/doctors")
def doctors():
    base = search_args(request.args)
    page = page_number(request.args)
    filters = get_filters()
    result, error = None, None
    params = {**base, "page": page, "pageSize": PAGE_SIZE}
    if params.get("acceptingOnly"):
        params["acceptingOnly"] = "true"
    try:
        r = d("api")("GET", "/doctors/search", params=params)
        if r.ok:
            result = r.json()
        else:
            d("app").logger.warning("doctor search failed: HTTP %s %s", r.status_code, r.text[:300])
            error = d("api_message")(r) if r.status_code == 422 else "Search is unavailable right now. Please try again in a moment."
    except (requests.RequestException, ValueError) as exc:
        d("app").logger.error("doctor search upstream error: %s", exc)
        error = "Search is unavailable right now. Please try again in a moment."
    session["last_search"] = urlencode(base)[:300]
    cards = [doctor_card(i) for i in (result or {}).get("items", [])]
    total_pages = (result or {}).get("totalPages", 1)
    return render(
        "doctors", active="doctors", filters=filters, form=base, cards=cards, result=result, error=error,
        page_no=page, pager=page_links(page, total_pages, base) if result and total_pages > 1 else [],
        prev_url="/patient/doctors?" + urlencode({**base, "page": page - 1}) if result and result.get("hasPrevious") else None,
        next_url="/patient/doctors?" + urlencode({**base, "page": page + 1}) if result and result.get("hasNext") else None,
        reset_needed=bool(base), page_size=PAGE_SIZE, first_url="/patient/doctors?" + urlencode(base),
    )


# --------------------------------------------------------------------------- doctor page + slot browser
PARTS = [("Morning", 0, 12), ("Afternoon", 12, 17), ("Evening", 17, 24)]


def pretty_day(iso):
    try:
        day = datetime.strptime(iso, "%Y-%m-%d")
        return f"{day:%A}, {day.day} {day:%B}"
    except ValueError:
        return iso


def slot_card(s):
    cap, free = max(int(s.get("capacity") or 1), 1), max(int(s.get("freeSeats") or 0), 0)
    if free == 0:
        state, label, seats = "booked", "Full", "No seats left"
    elif cap == 1:
        state, label, seats = "open", "Free", "1 seat"
    elif free * 4 <= cap:
        state, label, seats = "partial", "Few seats left", f"{free} of {cap} seats free"
    else:
        state, label, seats = "open", "Free", f"{free} of {cap} seats free"
    try:
        hour = int(str(s.get("localStart", "0:0")).split(":")[0])
    except ValueError:
        hour = 0
    return {"start": s.get("localStart"), "end": s.get("localEnd"), "minutes": s.get("durationMinutes"), "type": s.get("consultationTypeName"),
            "fee": rupees(s.get("fee")), "state": state, "label": label, "seats": seats, "hour": hour}


def slot_days(data):
    """API slot response -> days with readable, part-of-day grouped cards. Empty days are listed in one line."""
    days, empty = [], []
    for day in data.get("days", []):
        cards = [slot_card(s) for s in day.get("slots", [])]
        if not cards:
            empty.append(pretty_day(day["date"]).split(",")[0][:3] + " " + day["date"][8:10].lstrip("0"))
            continue
        days.append({
            "title": pretty_day(day["date"]), "free": day.get("freeSlots", 0), "total": len(cards),
            "groups": [{"label": lbl, "cards": [c for c in cards if lo <= c["hour"] < hi]} for lbl, lo, hi in PARTS
                       if any(lo <= c["hour"] < hi for c in cards)],
        })
    return days, empty


def doctor_url(doctor_id, **params):
    clean = {k: v for k, v in params.items() if v not in (None, "", False)}
    return f"/patient/doctors/{doctor_id}" + ("?" + urlencode(clean) if clean else "")


@bp.get("/doctors/<int:doctor_id>")
def doctor_page(doctor_id):
    api = d("api")
    r = api("GET", f"/doctors/{doctor_id}")
    if r.status_code in (404, 422):
        d("notify")("That doctor could not be found.", "error")
        return redirect("/patient/doctors")
    if not r.ok:
        d("app").logger.warning("doctor profile failed: HTTP %s %s", r.status_code, r.text[:300])
        d("notify")("The doctor's page could not be loaded. Please try again.", "error")
        return redirect("/patient/doctors?" + session.get("last_search", ""))
    info = r.json()
    doc = info["doctor"]
    types = info.get("consultationTypes", [])

    start = request.args.get("from", "").strip()
    end = request.args.get("to", "").strip()
    type_id = request.args.get("type", "").strip()
    only_free = request.args.get("free") == "1"
    params = {}
    if DATE_RE.match(start):
        params["from"] = start
    if DATE_RE.match(end):
        params["to"] = end
    if re.fullmatch(r"[0-9]{1,9}", type_id):
        params["consultationTypeId"] = type_id
    if only_free:
        params["availableOnly"] = "true"

    slots, slot_error, days, empty, window = None, None, [], [], None
    if types:
        try:
            sr = api("GET", f"/doctors/{doctor_id}/slots", params=params)
            if sr.ok:
                slots = sr.json()
                days, empty = slot_days(slots)
                window = (slots["from"], slots["to"])
            else:
                d("app").logger.warning("slots failed: HTTP %s %s", sr.status_code, sr.text[:300])
                slot_error = d("api_message")(sr) if sr.status_code == 422 else "Slot availability is unavailable right now. Please try again in a moment."
        except (requests.RequestException, ValueError) as exc:
            d("app").logger.error("slots upstream error: %s", exc)
            slot_error = "Slot availability is unavailable right now. Please try again in a moment."

    prev_url = next_url = None
    if window:
        first, last = date.fromisoformat(window[0]), date.fromisoformat(window[1])
        span = (last - first).days + 1
        try:
            today = datetime.now(ZoneInfo(doc.get("timezone") or "Asia/Kolkata")).date()
        except Exception:  # noqa: BLE001
            today = date.today()
        common = {"type": params.get("consultationTypeId"), "free": "1" if only_free else None}
        if first > today:
            new_first = max(first - timedelta(days=span), today)
            prev_url = doctor_url(doctor_id, **{"from": new_first.isoformat(), "to": (first - timedelta(days=1)).isoformat()}, **common)
        if span <= 31 and (last - today).days < 44:
            next_url = doctor_url(doctor_id, **{"from": (last + timedelta(days=1)).isoformat(), "to": (last + timedelta(days=span)).isoformat()}, **common)

    languages = doc.get("languages") or []
    exp = doc.get("yearsExperience")
    return render(
        "doctor", active="doctors", doc=doc, photo=photo_src(doc["doctorId"], doc.get("photoKey")),
        initials=initials(doc.get("firstName"), doc.get("lastName")), languages=", ".join(languages) if languages else None,
        experience=f"{exp} years" if isinstance(exp, int) and exp > 0 else None,
        types=[dict(t, fee_label=rupees(t.get("fee"))) for t in types], days=days, empty_days=empty, slots=slots,
        slot_error=slot_error, window=window, selected_type=params.get("consultationTypeId", ""), only_free=only_free,
        prev_url=prev_url, next_url=next_url, back_url="/patient/doctors" + ("?" + session["last_search"] if session.get("last_search") else ""),
    )


@bp.get("/doctor-photo/<int:doctor_id>/<name>")
def doctor_photo(doctor_id, name):
    """Streams a doctor's photo from the private bucket. The key is rebuilt from the id and a validated file name."""
    bucket = d("photo_bucket")()
    if not PHOTO_NAME.match(name) or not bucket:
        return ("", 404)
    try:
        obj = boto3.client("s3").get_object(Bucket=bucket, Key=f"doctors/{doctor_id}/{name}")
        data = obj["Body"].read()
    except Exception as exc:  # noqa: BLE001
        d("app").logger.warning("doctor photo read failed for %s/%s: %s", doctor_id, name, type(exc).__name__)
        return ("", 404)
    resp = Response(data, mimetype=obj.get("ContentType") or "image/jpeg")
    resp.headers["Cache-Control"] = "private, max-age=3600"
    return resp
