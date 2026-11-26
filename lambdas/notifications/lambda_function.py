"""MediSlot notification dispatcher (Milestone 6): EventBridge -> SQS -> this Lambda -> SNS.

The eight notification events (Frozen Rule Set, as implemented here - see NOTIFICATIONS below):
    AppointmentBooked        booking confirmed (direct booking or an accepted waitlist offer)
    AppointmentCancelled     cancelled by the patient or the doctor, with the refund
    AppointmentRescheduled   moved to a new time by the reschedule saga
    HoldOfferIssued          a seat is held for a waitlisted patient until <time>
    HoldOfferExpired         the hold ran out and the seat went to the next person
    AppointmentReminder24h   24 hours before the start (reminder dispatcher)
    AppointmentReminder2h    2 hours before the start (reminder dispatcher)
    AppointmentStatusChanged with status NO_SHOW (doctor marked it, or the no-show sweeper did)

Exactly-once dispatch (the DynamoDB NotificationLog table):
    dispatch_key = detail.dispatchKey (reminders: REMINDER_24H#<appointmentId>#<startAt>, so firing the reminder
    dispatcher twice can never notify twice) or <detail-type>#<detail.eventId> for everything else.
      1. conditional PutItem PENDING (lease 60 s) - only if the key was never seen, or a previous attempt died;
      2. SNS Publish;
      3. mark SENT (with the SNS MessageId).
    A duplicate SQS delivery / a second dispatcher run finds SENT and is acknowledged without publishing
    (NotificationsDuplicateSuppressed). An SNS failure frees the lease and fails only that record, so SQS retries
    it; after 5 receives it lands in the DLQ (alarmed).
This Lambda runs OUTSIDE the VPC: it needs only DynamoDB and SNS, and every fact a message needs travels in the
event (doctor name, local date and time) - never the patient's name, phone or email.
Metrics: NotificationsDispatched (+ per EventType), NotificationsDuplicateSuppressed, NotificationFailures.
"""
import json
import os
import time
import traceback
from datetime import datetime, timezone

import boto3

from common import aws_error_code, correlation, ddb_av, ddb_item, emit, instrument, put_metric

_cfg = boto3.session.Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 3, "mode": "standard"})
ddb = boto3.client("dynamodb", config=_cfg)
sns = boto3.client("sns", config=_cfg)

LEASE_SECONDS = 60
LOG_TTL_SECONDS = 90 * 24 * 3600


def utcnow():
    return datetime.now(timezone.utc)


def metric(name, value=1, unit="Count", **dimensions):
    put_metric(name, value, unit, **dimensions)


def log_table():
    return os.environ.get("NOTIFICATION_LOG_TABLE") or f"MediSlot-{os.environ.get('ENVIRONMENT', 'dev')}-NotificationLog"


def topic_arn():
    return os.environ.get("PATIENT_TOPIC_ARN", "")


def _vals(**kw):
    return {":" + k: ddb_av(v) for k, v in kw.items()}


# --------------------------------------------------------------------------- message templates
def _when(n):
    if not n.get("localDate"):
        return "your appointment time"
    try:
        d = datetime.strptime(n["localDate"], "%Y-%m-%d")
        day = f"{d:%a} {d.day} {d:%b %Y}"
    except ValueError:
        day = n["localDate"]
    return f"{day} at {n.get('localStart', '')}".strip() + (f" ({n['timezone']})" if n.get("timezone") else "")


def _doctor(n):
    return n.get("doctorName") or "your doctor"


def _ref(d):
    return f" Reference #{d['appointmentId']}." if d.get("appointmentId") else ""


def booked(d, n):
    via = " from the waitlist" if d.get("source") == "WAITLIST" else ""
    return ("Appointment confirmed", f"Your {n.get('consultationType') or 'appointment'} with {_doctor(n)} on {_when(n)} "
            f"is confirmed{via}.{_ref(d)}")


def cancelled(d, n):
    who = "The doctor cancelled" if d.get("cancelledBy") == "DOCTOR" else "You cancelled"
    refund = d.get("refundAmount")
    money = f" Refund: Rs {refund} ({d.get('refundPercent', 0)}%)." if refund is not None else ""
    return ("Appointment cancelled", f"{who} your appointment with {_doctor(n)} on {_when(n)}.{money}{_ref(d)}")


def rescheduled(d, n):
    before = _when({"localDate": n.get("previousLocalDate"), "localStart": n.get("previousLocalStart")}) if n.get("previousLocalDate") else "the old time"
    return ("Appointment rescheduled", f"Your appointment with {_doctor(n)} moved from {before} to {_when(n)}.{_ref(d)}")


def offer_issued(d, n):
    return ("A seat is held for you", f"A seat with {_doctor(n)} on {_when(n)} is free. It is held for you until "
            f"{n.get('holdUntilLocal') or d.get('expiresAt')} - open MediSlot and accept it before then.")


def offer_expired(d, n):
    return ("Your held seat has expired", f"The seat with {_doctor(n)} on {_when(n)} was held for you but the offer "
            "expired, so it went to the next person in line.")


def reminder(hours):
    def build(d, n):
        return (f"Reminder: appointment in {hours} hours", f"Reminder: you see {_doctor(n)} on {_when(n)}.{_ref(d)}")
    return build


def no_show(d, n):
    return ("Missed appointment", f"Your appointment with {_doctor(n)} on {_when(n)} was marked as missed.{_ref(d)}")


NOTIFICATIONS = {
    "AppointmentBooked": booked,
    "AppointmentCancelled": cancelled,
    "AppointmentRescheduled": rescheduled,
    "HoldOfferIssued": offer_issued,
    "HoldOfferExpired": offer_expired,
    "AppointmentReminder24h": reminder(24),
    "AppointmentReminder2h": reminder(2),
    "AppointmentStatusChanged": no_show,          # the EventBridge rule only forwards status NO_SHOW
}


def notification_type(detail_type, detail):
    """The notification an event produces, or None (e.g. a COMPLETED status change is not a patient notification)."""
    if detail_type == "AppointmentStatusChanged":
        return "AppointmentNoShow" if detail.get("status") == "NO_SHOW" else None
    return detail_type if detail_type in NOTIFICATIONS else None


# --------------------------------------------------------------------------- exactly-once dispatch
def claim(key, kind, now_s):
    """-> 'claimed' | 'sent' (already dispatched) | 'busy' (another attempt holds a live lease)."""
    try:
        ddb.put_item(TableName=log_table(), Item={k: ddb_av(v) for k, v in {
            "dispatch_key": key, "status": "PENDING", "event_type": kind, "lease_until": now_s + LEASE_SECONDS,
            "first_seen_at": now_s, "expires_at": now_s + LOG_TTL_SECONDS}.items()},
            ConditionExpression="attribute_not_exists(dispatch_key) OR (#st = :pending AND lease_until < :now)",
            ExpressionAttributeNames={"#st": "status"}, ExpressionAttributeValues=_vals(pending="PENDING", now=now_s))
        return "claimed"
    except Exception as exc:  # noqa: BLE001
        if aws_error_code(exc) != "ConditionalCheckFailedException":
            raise
    res = ddb.get_item(TableName=log_table(), Key={"dispatch_key": {"S": key}}, ConsistentRead=True)
    rec = ddb_item(res.get("Item"))
    return "sent" if rec.get("status") == "SENT" else "busy"


def mark(key, status, now_s, **fields):
    names = {"#st": "status"}
    values = _vals(s=status, t=now_s, **fields)
    sets = ["#st = :s", "updated_at = :t"] + [f"{k} = :{k}" for k in fields]
    ddb.update_item(TableName=log_table(), Key={"dispatch_key": {"S": key}}, UpdateExpression="SET " + ", ".join(sets),
                    ExpressionAttributeNames=names, ExpressionAttributeValues=values)


def dispatch(message, cid):
    """One EventBridge event (as delivered by SQS). -> 'sent' | 'duplicate' | 'ignored'. Raises to retry."""
    detail_type, detail = message.get("detail-type"), message.get("detail") or {}
    kind = notification_type(detail_type, detail)
    if not kind:
        return "ignored"
    patient = detail.get("patientId")
    if patient in (None, ""):
        raise ValueError(f"{detail_type} without patientId")
    key = detail.get("dispatchKey") or f"{detail_type}#{detail.get('eventId') or message.get('id')}"
    now_s = int(time.time())
    state = claim(key, kind, now_s)
    if state == "sent":
        metric("NotificationsDuplicateSuppressed", 1, EventType=kind)
        emit("info", "notification_duplicate_suppressed", cid, dispatchKey=key, eventType=kind)
        return "duplicate"
    if state == "busy":
        raise RuntimeError(f"{key} is being dispatched by another attempt")
    subject, text = NOTIFICATIONS[detail_type](detail, detail.get("notify") or {})
    try:
        res = sns.publish(TopicArn=topic_arn(), Subject=subject[:100], Message=text,
                          MessageAttributes={"eventType": {"DataType": "String", "StringValue": kind},
                                             "patientId": {"DataType": "String", "StringValue": str(patient)},
                                             "audience": {"DataType": "String", "StringValue": "PATIENT"}})
    except Exception:
        try:
            mark(key, "PENDING", now_s, lease_until=0)     # let the SQS retry take it over straight away
        except Exception:  # noqa: BLE001
            pass
        metric("NotificationFailures", 1, EventType=kind)
        raise
    mark(key, "SENT", int(time.time()), sns_message_id=res.get("MessageId", ""), patient_id=str(patient))
    metric("NotificationsDispatched", 1, EventType=kind)
    emit("info", "notification_dispatched", cid, dispatchKey=key, eventType=kind, patientId=patient,
         snsMessageId=res.get("MessageId"))
    return "sent"


@instrument("notifications")
def handler(event, context):
    event = event if isinstance(event, dict) else {}
    cid = correlation(event)
    failures, counts = [], {"sent": 0, "duplicate": 0, "ignored": 0, "failed": 0}
    for record in event.get("Records") or []:
        try:
            message = json.loads(record["body"])
            counts[dispatch(message, (message.get("detail") or {}).get("correlationId") or cid)] += 1
        except Exception as exc:  # noqa: BLE001 - only this record is retried (partial batch response)
            counts["failed"] += 1
            emit("error", "notification_failed", cid, messageId=record.get("messageId"), error=type(exc).__name__,
                 detail=str(exc)[:300], traceback=traceback.format_exc()[-1200:])
            failures.append({"itemIdentifier": record.get("messageId")})
    emit("info", "notification_batch_done", cid, **counts)
    return {"batchItemFailures": failures}
