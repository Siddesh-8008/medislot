"""MediSlot doctor discovery: what a patient (or any signed-in user) can see about doctors.

Routes (behind the JWT authorizer; any signed-in PATIENT or DOCTOR):
    GET /doctors/filters             reference lists for the search form (hospitals, specializations, cities)
    GET /doctors/search              multi-attribute search with pagination
    GET /doctors/{doctorId}          public doctor profile with consultation types and fees
    GET /doctors/{doctorId}/slots    free seats per slot for a date range (read from DynamoDB)

Only information a patient needs is returned. Licence number, email and phone are never exposed.

Search design (see MILESTONE4.md for the index justification)
    * Filters: name, hospital (id or text), specialization (id or text), city, and an optional
      "only doctors taking bookings" switch. They combine with AND; blank filters are ignored.
    * `name` is a word-prefix match on first or last name ("ravi", "kumar", "ravi kumar", "Dr. Ravi"):
      `LIKE 'word%'` can use the name indexes, `LIKE '%word%'` could not.
    * User text is escaped before it goes into LIKE, so % and _ are matched literally.
    * Results are ordered by (last_name, first_name, doctor_id): a total order, so pages never overlap or skip.
    * Everything is parameterised; no user text is ever concatenated into SQL.
"""
import json
import os
import re
import traceback
from datetime import date, datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3
from boto3.dynamodb.conditions import Key

from common import correlation, db_config, dbconn, emit, instrument, json_out

ddb = boto3.resource("dynamodb")

DEFAULT_PAGE_SIZE = 10
MAX_PAGE_SIZE = 50
MAX_PAGE = 10000
MAX_NAME_WORDS = 4
MAX_TEXT = 100
DEFAULT_RANGE_DAYS = 7
MAX_RANGE_DAYS = 31
WEEKDAYS = ["Monday", "Tuesday", "Wednesday", "Thursday", "Friday", "Saturday", "Sunday"]

# A "listed" doctor: account active, hospital active, specialization active.
LISTED_FROM = """FROM doctors d
    JOIN users u ON u.user_id = d.doctor_id AND u.is_active = 1
    JOIN hospitals h ON h.hospital_id = d.hospital_id AND h.is_active = 1
    JOIN specializations s ON s.specialization_id = d.specialization_id AND s.is_active = 1"""


class BadRequest(Exception):
    """Invalid query parameter; becomes HTTP 422 with the message."""


# --------------------------------------------------------------------------- request parsing
def query_params(event):
    return {k: (v if isinstance(v, str) else "") for k, v in (event.get("queryStringParameters") or {}).items()}


def int_param(q, name, default=None, lo=1, hi=None):
    raw = q.get(name, "").strip()
    if raw == "":
        return default
    if not re.fullmatch(r"[0-9]{1,9}", raw):
        raise BadRequest(f"{name} must be a whole number")
    value = int(raw)
    if value < lo or (hi is not None and value > hi):
        raise BadRequest(f"{name} must be between {lo} and {hi}" if hi is not None else f"{name} must be at least {lo}")
    return value


def text_param(q, name, max_len=MAX_TEXT):
    text = " ".join(q.get(name, "").split())
    if len(text) > max_len:
        raise BadRequest(f"{name} must be {max_len} characters or fewer")
    if any(ord(ch) < 32 for ch in text):
        raise BadRequest(f"{name} contains characters that are not allowed")
    return text


def bool_param(q, name):
    raw = q.get(name, "").strip().lower()
    if raw in ("", "false", "0", "no", "off"):
        return False
    if raw in ("true", "1", "yes", "on"):
        return True
    raise BadRequest(f"{name} must be true or false")


def date_param(q, name, default):
    raw = q.get(name, "").strip()
    if raw == "":
        return default
    if not re.fullmatch(r"[0-9]{4}-[0-9]{2}-[0-9]{2}", raw):
        raise BadRequest(f"{name} must be a date in YYYY-MM-DD format")
    try:
        return date(int(raw[0:4]), int(raw[5:7]), int(raw[8:10]))
    except ValueError:
        raise BadRequest(f"{name} is not a real calendar date")


def like_escape(text):
    """Make user text safe inside a LIKE pattern: backslash, % and _ lose their special meaning."""
    return text.replace("\\", "\\\\").replace("%", "\\%").replace("_", "\\_")


def route(event):
    path = event.get("path") or event.get("rawPath") or ""
    m = re.search(r"(/doctors(?:/[^/]+){0,2})/?$", path)
    return m.group(1) if m else path


def method_of(event):
    return (event.get("httpMethod") or (event.get("requestContext") or {}).get("http", {}).get("method") or "GET").upper()


def msg(cid, text, **extra):
    return {"message": text, "correlationId": cid, **extra}


# --------------------------------------------------------------------------- search SQL
def build_search_filters(q):
    """-> (where_sql, args, used_filter_names). Pure function: easy to unit test."""
    where, args, used = [], [], []

    name = text_param(q, "name")
    words = [w for w in name.split(" ") if w and w.lower().strip(".") != "dr"]
    if len(words) > MAX_NAME_WORDS:
        raise BadRequest(f"name can have at most {MAX_NAME_WORDS} words")
    for word in words:
        pattern = like_escape(word) + "%"
        where.append("(d.first_name LIKE %s OR d.last_name LIKE %s)")
        args += [pattern, pattern]
    if words:
        used.append("name")

    hospital_id = int_param(q, "hospitalId", hi=2_000_000_000)
    if hospital_id is not None:
        where.append("d.hospital_id = %s")
        args.append(hospital_id)
        used.append("hospitalId")
    hospital = text_param(q, "hospital")
    if hospital:
        where.append("h.hospital_name LIKE %s")
        args.append("%" + like_escape(hospital) + "%")
        used.append("hospital")

    specialization_id = int_param(q, "specializationId", hi=2_000_000_000)
    if specialization_id is not None:
        where.append("d.specialization_id = %s")
        args.append(specialization_id)
        used.append("specializationId")
    specialization = text_param(q, "specialization")
    if specialization:
        where.append("s.specialization_name LIKE %s")
        args.append("%" + like_escape(specialization) + "%")
        used.append("specialization")

    city = text_param(q, "city")
    if city:
        where.append("h.city = %s")
        args.append(city)
        used.append("city")

    if bool_param(q, "acceptingOnly"):
        where.append("EXISTS (SELECT 1 FROM doctor_consultation_types x WHERE x.doctor_id = d.doctor_id AND x.is_active = 1)")
        used.append("acceptingOnly")

    return (" WHERE " + " AND ".join(where)) if where else "", args, used


def parse_languages(value):
    if value is None:
        return []
    if isinstance(value, (list, tuple)):
        return list(value)
    try:
        parsed = json.loads(value)
        return parsed if isinstance(parsed, list) else []
    except (TypeError, ValueError):
        return []


def doctor_card(r, fee_by_doctor):
    fee = fee_by_doctor.get(int(r["doctor_id"]))
    return {
        "doctorId": int(r["doctor_id"]),
        "firstName": r["first_name"],
        "lastName": r["last_name"],
        "displayName": f"Dr. {r['first_name']} {r['last_name']}",
        "specialization": {"id": int(r["specialization_id"]), "name": r["specialization_name"]},
        "hospital": {"id": int(r["hospital_id"]), "name": r["hospital_name"], "city": r["city"]},
        "yearsExperience": r["years_experience"],
        "languages": parse_languages(r.get("languages_spoken")),
        "photoKey": r["photo_key"],
        "startingFee": fee["starting_fee"] if fee else None,
        "consultationTypeCount": int(fee["type_count"]) if fee else 0,
        "acceptingBookings": bool(fee),
    }


def search(event, cid):
    q = query_params(event)
    page = int_param(q, "page", default=1, hi=MAX_PAGE)
    page_size = int_param(q, "pageSize", default=DEFAULT_PAGE_SIZE, hi=MAX_PAGE_SIZE)
    where, args, used = build_search_filters(q)
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT COUNT(*) AS total {LISTED_FROM}{where}", args)
            total = int(cur.fetchone()["total"])
            rows = []
            if total > (page - 1) * page_size:
                cur.execute(
                    f"""SELECT d.doctor_id, d.first_name, d.last_name, d.years_experience, d.languages_spoken, d.photo_key,
                               h.hospital_id, h.hospital_name, h.city, s.specialization_id, s.specialization_name
                        {LISTED_FROM}{where}
                        ORDER BY d.last_name, d.first_name, d.doctor_id
                        LIMIT %s OFFSET %s""",
                    args + [page_size, (page - 1) * page_size],
                )
                rows = cur.fetchall()
            fees = {}
            if rows:
                ids = [int(r["doctor_id"]) for r in rows]
                cur.execute(
                    f"""SELECT doctor_id, MIN(fee) AS starting_fee, COUNT(*) AS type_count
                        FROM doctor_consultation_types
                        WHERE doctor_id IN ({', '.join(['%s'] * len(ids))}) AND is_active = 1
                        GROUP BY doctor_id""",
                    ids,
                )
                fees = {int(r["doctor_id"]): r for r in cur.fetchall()}
    total_pages = max((total + page_size - 1) // page_size, 1)
    emit("info", "doctor_search", cid, filters=used, page=page, pageSize=page_size, total=total, returned=len(rows))
    return json_out(200, {
        "items": [doctor_card(r, fees) for r in rows],
        "page": page,
        "pageSize": page_size,
        "total": total,
        "totalPages": total_pages,
        "hasPrevious": page > 1 and total > 0,
        "hasNext": page < total_pages,
        "correlationId": cid,
    })


def filters(cid):
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT hospital_id, hospital_name, city FROM hospitals WHERE is_active = 1 ORDER BY city, hospital_name")
            hospitals = cur.fetchall()
            cur.execute("SELECT specialization_id, specialization_name FROM specializations WHERE is_active = 1 ORDER BY specialization_name")
            specializations = cur.fetchall()
    cities = sorted({h["city"] for h in hospitals})
    return json_out(200, {
        "hospitals": [{"hospitalId": int(h["hospital_id"]), "name": h["hospital_name"], "city": h["city"]} for h in hospitals],
        "specializations": [{"specializationId": int(s["specialization_id"]), "name": s["specialization_name"]} for s in specializations],
        "cities": cities,
        "maxPageSize": MAX_PAGE_SIZE,
        "correlationId": cid,
    })


# --------------------------------------------------------------------------- doctor profile and slots
def offered_types(cur, doctor_id):
    cur.execute(
        """SELECT dct.consultation_type_id, ct.name, ct.duration_minutes, dct.fee, dct.capacity
           FROM doctor_consultation_types dct
           JOIN consultation_types ct ON ct.consultation_type_id = dct.consultation_type_id AND ct.is_active = 1
           WHERE dct.doctor_id = %s AND dct.is_active = 1
           ORDER BY dct.consultation_type_id""",
        (doctor_id,),
    )
    return cur.fetchall()


def profile(doctor_id, cid):
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(
                f"""SELECT d.doctor_id, d.first_name, d.last_name, d.years_experience, d.clinic_address, d.qualifications,
                           d.languages_spoken, d.timezone, d.photo_key, h.hospital_id, h.hospital_name,
                           h.address AS hospital_address, h.city, h.state, s.specialization_id, s.specialization_name
                    {LISTED_FROM} WHERE d.doctor_id = %s""",
                (doctor_id,),
            )
            r = cur.fetchone()
            if not r:
                return json_out(404, msg(cid, "Doctor not found"))
            types = offered_types(cur, doctor_id)
    emit("info", "doctor_profile_viewed", cid, doctorId=doctor_id, consultationTypes=len(types))
    return json_out(200, {
        "doctor": {
            "doctorId": int(r["doctor_id"]),
            "firstName": r["first_name"],
            "lastName": r["last_name"],
            "displayName": f"Dr. {r['first_name']} {r['last_name']}",
            "specialization": {"id": int(r["specialization_id"]), "name": r["specialization_name"]},
            "hospital": {"id": int(r["hospital_id"]), "name": r["hospital_name"], "address": r["hospital_address"],
                         "city": r["city"], "state": r["state"]},
            "yearsExperience": r["years_experience"],
            "qualifications": r["qualifications"],
            "languages": parse_languages(r.get("languages_spoken")),
            "clinicAddress": r["clinic_address"],
            "timezone": r["timezone"],
            "photoKey": r["photo_key"],
            "acceptingBookings": bool(types),
        },
        "consultationTypes": [
            {"consultationTypeId": int(t["consultation_type_id"]), "name": t["name"],
             "durationMinutes": int(t["duration_minutes"]), "fee": t["fee"], "capacity": int(t["capacity"])}
            for t in types
        ],
        "correlationId": cid,
    })


def read_day(table, doctor_id, day):
    items, kwargs = [], {"IndexName": "GSI_DoctorDate",
                         "KeyConditionExpression": Key("doctor_date").eq(f"DOCTOR#{doctor_id}#{day.isoformat()}")}
    while True:
        res = table.query(**kwargs)
        items.extend(res.get("Items", []))
        if "LastEvaluatedKey" not in res:
            return items
        kwargs["ExclusiveStartKey"] = res["LastEvaluatedKey"]


def parse_utc(text):
    return datetime.fromisoformat(str(text).replace("Z", "+00:00"))


def slots(event, doctor_id, cid, now=None):
    q = query_params(event)
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(f"SELECT d.doctor_id, d.timezone {LISTED_FROM} WHERE d.doctor_id = %s", (doctor_id,))
            doc = cur.fetchone()
            if not doc:
                return json_out(404, msg(cid, "Doctor not found"))
            types = {int(t["consultation_type_id"]): t for t in offered_types(cur, doctor_id)}
    try:
        zone = ZoneInfo(doc["timezone"])
    except Exception:  # noqa: BLE001 - a bad stored timezone must not break browsing
        zone = ZoneInfo("Asia/Kolkata")

    now = now or datetime.now(timezone.utc)
    today = now.astimezone(zone).date()
    first = date_param(q, "from", today)
    last = date_param(q, "to", first + timedelta(days=DEFAULT_RANGE_DAYS - 1))
    if last < today:
        raise BadRequest("Dates in the past have no bookable slots")
    if last < first:
        raise BadRequest("to must be on or after from")
    clamped = first < today
    if clamped:
        first = today
    if (last - first).days + 1 > MAX_RANGE_DAYS:
        raise BadRequest(f"Choose a range of at most {MAX_RANGE_DAYS} days")
    type_id = int_param(q, "consultationTypeId")
    if type_id is not None and type_id not in types:
        raise BadRequest("This doctor does not offer that consultation type")
    only_available = bool_param(q, "availableOnly")

    table = ddb.Table(f"MediSlot-{os.environ.get('ENVIRONMENT', 'dev')}-Slots")
    now_iso = now.isoformat(timespec="seconds").replace("+00:00", "Z")
    days, total_slots, total_free = [], 0, 0
    try:
        day = first
        while day <= last:
            out = []
            for it in read_day(table, doctor_id, day):
                t = types.get(int(it.get("consultation_type_id", 0)))
                if it.get("status") != "OPEN" or t is None or str(it.get("start_at", "")) <= now_iso:
                    continue
                if type_id is not None and int(it["consultation_type_id"]) != type_id:
                    continue
                capacity = max(int(it.get("capacity", 1)), 1)
                free = max(capacity - int(it.get("booked_count", 0)) - int(it.get("held_count", 0)), 0)
                if only_available and free == 0:
                    continue
                start, end = parse_utc(it["start_at"]), parse_utc(it["end_at"])
                out.append({
                    "slotId": it["slot_id"],
                    "startAt": it["start_at"],
                    "endAt": it["end_at"],
                    "localStart": start.astimezone(zone).strftime("%H:%M"),
                    "localEnd": end.astimezone(zone).strftime("%H:%M"),
                    "consultationTypeId": int(it["consultation_type_id"]),
                    "consultationTypeName": t["name"],
                    "durationMinutes": int(t["duration_minutes"]),
                    "fee": t["fee"],
                    "capacity": capacity,
                    "freeSeats": free,
                    "available": free > 0,
                })
            out.sort(key=lambda s: s["startAt"])
            days.append({"date": day.isoformat(), "weekday": WEEKDAYS[day.weekday()], "slots": out,
                         "freeSlots": sum(1 for s in out if s["available"])})
            total_slots += len(out)
            total_free += sum(s["freeSeats"] for s in out)
            day += timedelta(days=1)
    except Exception as exc:  # noqa: BLE001
        emit("error", "slots_read_failed", cid, doctorId=doctor_id, error=type(exc).__name__, detail=str(exc)[:200])
        return json_out(503, msg(cid, "Slot availability is temporarily unavailable. Please try again."))
    emit("info", "doctor_slots_viewed", cid, doctorId=doctor_id, fromDate=first.isoformat(), toDate=last.isoformat(),
         slots=total_slots, freeSeats=total_free)
    return json_out(200, {
        "doctorId": doctor_id,
        "timezone": str(zone),
        "from": first.isoformat(),
        "to": last.isoformat(),
        "fromWasInThePast": clamped,
        "days": days,
        "totalSlots": total_slots,
        "totalFreeSeats": total_free,
        "correlationId": cid,
    })


# --------------------------------------------------------------------------- entrypoint
@instrument("doctor_discovery")
def handler(event, context):
    cid = correlation(event)
    ctx = (event.get("requestContext") or {}).get("authorizer") or {}
    if ctx.get("role") not in ("PATIENT", "DOCTOR") or not str(ctx.get("userId") or "").isdigit():
        emit("warning", "discovery_access_denied", cid, role=ctx.get("role"))
        return json_out(403, msg(cid, "Sign in to browse doctors"))
    if method_of(event) != "GET":
        return json_out(405, msg(cid, "Method not allowed"))

    path = route(event)
    try:
        if path == "/doctors/search":
            return search(event, cid)
        if path == "/doctors/filters":
            return filters(cid)
        m = re.fullmatch(r"/doctors/([^/]+)(/slots)?", path)
        if m:
            raw = m.group(1)
            if not re.fullmatch(r"[0-9]{1,18}", raw):
                return json_out(422, msg(cid, "doctorId must be a positive number"))
            if int(raw) == 0:
                return json_out(404, msg(cid, "Doctor not found"))
            return slots(event, int(raw), cid) if m.group(2) else profile(int(raw), cid)
        return json_out(404, msg(cid, "Doctor endpoint not found", path=path))
    except BadRequest as exc:
        return json_out(422, msg(cid, str(exc)))
    except Exception as exc:  # noqa: BLE001
        emit("error", "doctor_discovery_error", cid, error=type(exc).__name__, path=path,
             detail=str(exc)[:300], traceback=traceback.format_exc()[-1500:])
        return json_out(500, msg(cid, "Internal server error"))
