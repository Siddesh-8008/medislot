"""MediSlot slot materialiser.

Turns each doctor's *weekly availability patterns* into concrete, bookable rows
in the DynamoDB Slots table for a rolling window (default 45 days).

Two ways it runs, same code path:
  1. Daily EventBridge Scheduler tick  -> event {}                  -> extend the window for ALL doctors.
  2. Doctor changed pattern/blackout   -> event {"doctorId": 42}    -> rebuild ONE doctor's window and
                                                                      reconcile (close/reopen) existing slots.

Safety rules (so booking/business logic is never disturbed):
  * Slot ids are deterministic, and creation uses attribute_not_exists, so re-running never
    overwrites a slot that already has bookings/holds.
  * A slot is only ever CLOSED if it is OPEN with booked_count = 0 and held_count = 0
    (checked atomically in the same DynamoDB write). Booked slots are never touched.
  * Only slots this module closed (closed_reason = SCHEDULE_CHANGE) are ever reopened.
"""
import os, json, logging, hashlib
from datetime import datetime, time, timedelta, timezone
from zoneinfo import ZoneInfo
import boto3
from boto3.dynamodb.conditions import Key
from common import dbconn, emit, get_param

log = logging.getLogger()
log.setLevel(logging.INFO)
ddb = boto3.resource("dynamodb")

CLOSED_REASON = "SCHEDULE_CHANGE"


# --------------------------------------------------------------------------- helpers (unchanged API)
def resolve_local(local_naive, zone):
    """Resolve wall time with PEP-495 fold=0; skip nonexistent spring-forward times."""
    aware = local_naive.replace(tzinfo=zone, fold=0)
    utc = aware.astimezone(timezone.utc)
    roundtrip = utc.astimezone(zone).replace(tzinfo=None)
    if roundtrip != local_naive:
        return None
    return utc


def stable_slot_id(doctor_id, consultation_type_id, start_iso):
    stable = f"{doctor_id}#{consultation_type_id}#{start_iso}"
    return "SLOT#" + hashlib.sha256(stable.encode()).hexdigest()[:40]


def iso_z(dt):
    return dt.isoformat(timespec="seconds").replace("+00:00", "Z")


def as_time(v):
    return v if isinstance(v, time) else time.fromisoformat(str(v))


# --------------------------------------------------------------------------- pure slot generation
def slots_for_pattern_day(p, day, zone, now):
    """Return (items, dst_skipped) for one pattern on one local calendar day. No I/O."""
    items, skipped = [], 0
    cursor = datetime.combine(day, as_time(p["start_time"]))
    end_wall = datetime.combine(day, as_time(p["end_time"]))
    step = timedelta(minutes=int(p["duration_minutes"]))
    while cursor + step <= end_wall:
        start_utc = resolve_local(cursor, zone)
        end_utc = resolve_local(cursor + step, zone)
        if start_utc is None or end_utc is None or end_utc <= start_utc:
            skipped += 1
        elif start_utc > now:
            start_iso, end_iso = iso_z(start_utc), iso_z(end_utc)
            items.append({
                "slot_id": stable_slot_id(p["doctor_id"], p["consultation_type_id"], start_iso),
                "doctor_id": int(p["doctor_id"]),
                "consultation_type_id": int(p["consultation_type_id"]),
                "doctor_date": f"DOCTOR#{p['doctor_id']}#{day.isoformat()}",
                "start_at": start_iso, "end_at": end_iso,
                "timezone": p["timezone"], "capacity": int(p["capacity"]),
                "booked_count": 0, "held_count": 0, "version": 1, "status": "OPEN",
                "materialised_at": iso_z(now),
                "local_date": day.isoformat(),
                "local_start_time": cursor.time().isoformat(timespec="minutes"),
                "utc_offset": int(cursor.replace(tzinfo=zone, fold=0).utcoffset().total_seconds()),
            })
        cursor += step
    return items, skipped


def desired_slots_for_doctor(patterns, blackouts, zone, now, days):
    """All slots a doctor SHOULD have in the window: {slot_id: item}. Pure function."""
    local_today = now.astimezone(zone).date()
    desired, skipped = {}, 0
    for p in patterns:
        for offset in range(days):
            day = local_today + timedelta(days=offset)
            if day.isoweekday() != int(p["day_of_week"]) or day.isoformat() in blackouts:
                continue
            items, s = slots_for_pattern_day(p, day, zone, now)
            skipped += s
            for it in items:
                desired[it["slot_id"]] = it
    return desired, skipped


# --------------------------------------------------------------------------- DynamoDB writes
def _code(e):
    return getattr(e, "response", {}).get("Error", {}).get("Code")


def create_missing(table, desired, errors):
    made = existing = 0
    for item in desired.values():
        try:
            table.put_item(Item=item, ConditionExpression="attribute_not_exists(slot_id)")
            made += 1
        except Exception as e:
            if _code(e) == "ConditionalCheckFailedException":
                existing += 1
            else:
                errors.append({"doctorId": item["doctor_id"], "date": item["local_date"], "error": type(e).__name__})
    return made, existing


def reconcile_doctor(table, doctor_id, zone, desired, now, days):
    """Close stale unbooked slots; reopen ones the doctor re-enabled. Booked slots are never touched."""
    closed = reopened = kept = 0
    now_iso = iso_z(now)
    local_today = now.astimezone(zone).date()
    for offset in range(days):
        day = local_today + timedelta(days=offset)
        kwargs = {"IndexName": "GSI_DoctorDate",
                  "KeyConditionExpression": Key("doctor_date").eq(f"DOCTOR#{doctor_id}#{day.isoformat()}")}
        existing = []
        while True:
            res = table.query(**kwargs)
            existing.extend(res.get("Items", []))
            if "LastEvaluatedKey" not in res:
                break
            kwargs["ExclusiveStartKey"] = res["LastEvaluatedKey"]
        for it in existing:
            if it["start_at"] <= now_iso:
                continue
            sid, status = it["slot_id"], it.get("status")
            try:
                if sid not in desired and status == "OPEN":
                    table.update_item(
                        Key={"slot_id": sid},
                        UpdateExpression="SET #s=:closed, closed_reason=:r, closed_at=:t, version=version+:one",
                        ConditionExpression="#s=:open AND booked_count=:zero AND held_count=:zero",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":closed": "CLOSED", ":open": "OPEN", ":r": CLOSED_REASON,
                                                   ":t": now_iso, ":zero": 0, ":one": 1})
                    closed += 1
                elif sid in desired and status == "CLOSED" and it.get("closed_reason") == CLOSED_REASON:
                    table.update_item(
                        Key={"slot_id": sid},
                        UpdateExpression="SET #s=:open, version=version+:one REMOVE closed_reason, closed_at",
                        ConditionExpression="#s=:closed AND closed_reason=:r",
                        ExpressionAttributeNames={"#s": "status"},
                        ExpressionAttributeValues={":closed": "CLOSED", ":open": "OPEN", ":r": CLOSED_REASON, ":one": 1})
                    reopened += 1
            except Exception as e:
                if _code(e) == "ConditionalCheckFailedException":
                    kept += 1          # a patient booked/held it meanwhile -> leave it alone
                else:
                    raise
    return closed, reopened, kept


# --------------------------------------------------------------------------- Lambda entrypoint
def handler(event, context):
    env = os.environ.get("ENVIRONMENT", "dev")
    prefix = f"/medislot/{env}"
    c = {"env": env, "prefix": prefix,
         "dbhost": get_param(f"{prefix}/db/host"),
         "dbport": int(get_param(f"{prefix}/db/port")),
         "dbname": get_param(f"{prefix}/db/name"),
         "dbuser": get_param(f"{prefix}/db/username"),
         "dbpass": get_param(f"{prefix}/db/password", True)}
    days = int(os.environ.get("MATERIALISE_DAYS", "45"))
    event = event if isinstance(event, dict) else {}
    only_doctor = int(event["doctorId"]) if event.get("doctorId") else None
    reconcile = only_doctor is not None or bool(event.get("reconcile"))
    now = datetime.now(timezone.utc)
    table = ddb.Table(f"MediSlot-{c['env']}-Slots")
    made = existing = skipped = closed = reopened = kept = 0
    errors = []
    tz_by_doctor = {}
    try:
        with dbconn(c) as conn:
            with conn.cursor() as cur:
                sql = """SELECT d.doctor_id,d.timezone,p.pattern_id,p.consultation_type_id,p.day_of_week,
                    p.start_time,p.end_time,ct.duration_minutes,ct.fee,ct.capacity
                    FROM doctors d JOIN availability_patterns p ON p.doctor_id=d.doctor_id
                    JOIN consultation_types ct ON ct.consultation_type_id=p.consultation_type_id
                    WHERE p.is_active=1 AND ct.is_active=1"""
                args = ()
                if only_doctor:
                    sql += " AND d.doctor_id=%s"; args = (only_doctor,)
                cur.execute(sql, args)
                by_doctor = {}
                for p in cur.fetchall():
                    by_doctor.setdefault(int(p["doctor_id"]), []).append(p)
                if only_doctor and only_doctor not in by_doctor:
                    # Doctor removed their last pattern: still must close their old unbooked slots.
                    cur.execute("SELECT doctor_id,timezone FROM doctors WHERE doctor_id=%s", (only_doctor,))
                    row = cur.fetchone()
                    if row:
                        by_doctor[only_doctor] = []
                        tz_by_doctor = {only_doctor: row["timezone"]}
                else:
                    tz_by_doctor = {d: ps[0]["timezone"] for d, ps in by_doctor.items()}
                for doctor_id, patterns in by_doctor.items():
                    try:
                        zone = ZoneInfo(tz_by_doctor[doctor_id])
                    except Exception:
                        errors.append({"doctorId": doctor_id, "reason": "invalid_timezone"}); continue
                    cur.execute("SELECT blackout_date FROM blackout_dates WHERE doctor_id=%s", (doctor_id,))
                    blackouts = {x["blackout_date"].isoformat() for x in cur.fetchall()}
                    desired, sk = desired_slots_for_doctor(patterns, blackouts, zone, now, days)
                    skipped += sk
                    m, ex = create_missing(table, desired, errors)
                    made += m; existing += ex
                    if reconcile:
                        cl, re_, kp = reconcile_doctor(table, doctor_id, zone, desired, now, days)
                        closed += cl; reopened += re_; kept += kp
        result = {"created": made, "alreadyExisting": existing, "dstSkipped": skipped,
                  "closed": closed, "reopened": reopened, "keptBecauseBooked": kept,
                  "errors": errors[:20], "windowDays": days, "doctorId": only_doctor, "asOf": now.isoformat()}
        emit("info", "slot_materialiser_done", **{k: v for k, v in result.items() if k != "errors"})
        return {"statusCode": 200, "body": json.dumps(result)}
    except Exception as e:
        emit("error", "slot_materialiser_failed", error=type(e).__name__)
        raise
