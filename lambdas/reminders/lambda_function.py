"""MediSlot reminder dispatcher and no-show sweeper (Milestone 6). Both run on EventBridge Scheduler.

    {"action": "reminders"}  every 15 minutes
        For each CONFIRMED appointment whose start falls in (now + lead - lookback, now + lead] - lead 24 h and 2 h,
        lookback 60 min (bigger than the schedule interval, so a late or skipped run never misses anyone) - publish
        AppointmentReminder24h / AppointmentReminder2h with
            dispatchKey = REMINDER_<24H|2H>#<appointmentId>#<startAt>
        The notification dispatcher sends each dispatchKey exactly once (DynamoDB NotificationLog), so overlapping
        windows, a second manual run or SQS duplicates can never notify twice. startAt is part of the key, so an
        appointment that is rescheduled is reminded again for its NEW time.
        An appointment booked less than 24 h ahead simply gets only the 2 h reminder.

    {"action": "noShowSweep"} every 30 minutes
        CONFIRMED appointments whose end is more than `no-show-grace-hours` (SSM, default 12) in the past were never
        marked by the doctor: they become NO_SHOW (conditional UPDATE, so a doctor marking it at the same moment wins)
        and AppointmentStatusChanged (status NO_SHOW, markedBy SYSTEM) is published -> "missed appointment" notice.

Metrics: RemindersQueued (+ per Kind), NoShowsMarked.
"""
import json
import os
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import boto3

from common import correlation, db_config, dbconn, emit, get_param, instrument, put_metric

events = boto3.client("events", config=boto3.session.Config(connect_timeout=2, read_timeout=3, retries={"max_attempts": 2}))
UTC = timezone.utc
REMINDERS = (("24H", "AppointmentReminder24h", timedelta(hours=24)), ("2H", "AppointmentReminder2h", timedelta(hours=2)))
LOOKBACK = timedelta(minutes=60)
BATCH = 500
NO_SHOW_LIMIT = 200
DEFAULT_GRACE_HOURS = 12


def utcnow():
    return datetime.now(UTC)


def metric(name, value=1, unit="Count", **dimensions):
    put_metric(name, value, unit, **dimensions)


def event_bus():
    return os.environ.get("EVENT_BUS_NAME") or f"medislot-{os.environ.get('ENVIRONMENT', 'dev')}-events"


def utc_of(value):
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    dt = datetime.fromisoformat(str(value).replace("Z", "+00:00"))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def iso(dt):
    return dt.astimezone(UTC).isoformat(timespec="seconds").replace("+00:00", "Z")


def zone_of(name):
    try:
        return ZoneInfo(name or "Asia/Kolkata")
    except Exception:  # noqa: BLE001
        return ZoneInfo("Asia/Kolkata")


def notify(row):
    zone = zone_of(row.get("timezone"))
    start, end = utc_of(row["start_at"]).astimezone(zone), utc_of(row["end_at"]).astimezone(zone)
    return {"doctorName": f"Dr. {row.get('doctor_first', '')} {row.get('doctor_last', '')}".strip(),
            "consultationType": row.get("type_name"), "hospital": row.get("hospital_name"),
            "localDate": start.date().isoformat(), "localStart": start.strftime("%H:%M"), "localEnd": end.strftime("%H:%M"),
            "timezone": str(zone)}


def publish(entries, source, cid):
    """[(detail_type, detail)] in batches of 10. Returns how many were accepted by EventBridge."""
    ok = 0
    for i in range(0, len(entries), 10):
        batch = [{"Source": source, "DetailType": t, "EventBusName": event_bus(),
                  "Detail": json.dumps({"eventId": uuid.uuid4().hex, "eventVersion": 1, "occurredAt": iso(utcnow()),
                                        "correlationId": cid, **d}, default=str)} for t, d in entries[i:i + 10]]
        for attempt in range(3):
            try:
                res = events.put_events(Entries=batch)
                failed = [e for e, r in zip(batch, res.get("Entries", [])) if r.get("ErrorCode")]
                ok += len(batch) - len(failed)
                batch = failed
                if not batch:
                    break
            except Exception as exc:  # noqa: BLE001
                emit("warning", "event_publish_retry", cid, attempt=attempt + 1, error=type(exc).__name__)
            time.sleep(0.2 * (attempt + 1))
        if batch:
            emit("error", "event_publish_failed", cid, detailTypes=[e["DetailType"] for e in batch])
            metric("EventPublishFailures", len(batch))
    return ok


SELECT = """SELECT a.appointment_id, a.patient_id, a.doctor_id, a.dependent_id, a.slot_id, a.start_at, a.end_at,
       d.first_name AS doctor_first, d.last_name AS doctor_last, d.timezone, ct.name AS type_name, h.hospital_name
  FROM appointments a
  JOIN doctors d ON d.doctor_id = a.doctor_id
  JOIN hospitals h ON h.hospital_id = d.hospital_id
  JOIN consultation_types ct ON ct.consultation_type_id = a.consultation_type_id"""


def send_reminders(cid, now=None):
    now = now or utcnow()
    entries, found = [], {}
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            for kind, detail_type, lead in REMINDERS:
                lo, hi = now + lead - LOOKBACK, now + lead
                cur.execute(SELECT + """ WHERE a.status = 'CONFIRMED' AND a.start_at > %s AND a.start_at <= %s
                                         ORDER BY a.start_at, a.appointment_id LIMIT %s""",
                            (lo.replace(tzinfo=None), hi.replace(tzinfo=None), BATCH))
                rows = cur.fetchall()
                found[kind] = len(rows)
                for r in rows:
                    start = iso(utc_of(r["start_at"]))
                    entries.append((detail_type, {
                        "appointmentId": int(r["appointment_id"]), "patientId": int(r["patient_id"]),
                        "doctorId": int(r["doctor_id"]), "dependentId": r.get("dependent_id"), "slotId": r["slot_id"],
                        "startAt": start, "reminder": kind,
                        "dispatchKey": f"REMINDER_{kind}#{int(r['appointment_id'])}#{start}", "notify": notify(r)}))
        conn.rollback()
    sent = publish(entries, "medislot.reminders", cid)
    for kind, _, _ in REMINDERS:
        if found[kind]:
            metric("RemindersQueued", found[kind], Kind=kind)
    emit("info", "reminders_dispatched", cid, found=found, published=sent)
    return {"ok": sent == len(entries), "found": found, "published": sent}


def grace_hours():
    try:
        n = int(get_param(f"/medislot/{os.environ.get('ENVIRONMENT', 'dev')}/booking/no-show-grace-hours"))
        return n if 1 <= n <= 168 else DEFAULT_GRACE_HOURS
    except Exception:  # noqa: BLE001
        return DEFAULT_GRACE_HOURS


def sweep_no_shows(cid, now=None, grace=None):
    now = now or utcnow()
    cutoff = now - timedelta(hours=grace if grace is not None else grace_hours())
    marked = []
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(SELECT + """ WHERE a.status = 'CONFIRMED' AND a.end_at <= %s
                                     ORDER BY a.end_at, a.appointment_id LIMIT %s""", (cutoff.replace(tzinfo=None), NO_SHOW_LIMIT))
            for r in cur.fetchall():
                cur.execute("""UPDATE appointments SET status = 'NO_SHOW', updated_at = %s,
                                      cancellation_note = COALESCE(cancellation_note, 'Marked as missed automatically')
                               WHERE appointment_id = %s AND status = 'CONFIRMED'""", (now.replace(tzinfo=None), r["appointment_id"]))
                if cur.rowcount == 1:
                    marked.append(r)
            conn.commit()
    entries = [("AppointmentStatusChanged", {
        "appointmentId": int(r["appointment_id"]), "patientId": int(r["patient_id"]), "doctorId": int(r["doctor_id"]),
        "dependentId": r.get("dependent_id"), "slotId": r["slot_id"], "startAt": iso(utc_of(r["start_at"])),
        "previousStatus": "CONFIRMED", "status": "NO_SHOW", "markedBy": "SYSTEM", "notify": notify(r)}) for r in marked]
    publish(entries, "medislot.booking", cid)
    if marked:
        metric("NoShowsMarked", len(marked))
    emit("info", "no_show_sweep_finished", cid, marked=len(marked), cutoff=iso(cutoff))
    return {"ok": True, "marked": len(marked)}


@instrument("reminders")
def handler(event, context):
    event = event if isinstance(event, dict) else {}
    cid = correlation(event)
    action = event.get("action") or "reminders"
    try:
        if action == "reminders":
            return send_reminders(cid)
        if action == "noShowSweep":
            return sweep_no_shows(cid)
        return {"ok": False, "message": "Unknown action"}
    except Exception as exc:
        emit("error", "scheduled_job_failed", cid, action=action, error=type(exc).__name__, detail=str(exc)[:300],
             traceback=traceback.format_exc()[-1500:])
        raise
