"""MediSlot waitlist and hold offers (Milestone 6) - everything about "tell me when a seat frees up" in one place.

A patient waits for a whole DAY with one doctor (not for one slot): when any seat on that doctor's day frees up, the
first person in line gets a HOLD OFFER for it. Accepting the offer is done by the booking Lambda
(POST /offers/{offerId}/accept), because that is where the booking rules live.

Routes (behind the JWT authorizer; PATIENT role; the patient id always comes from the verified token):
    POST   /waitlists                    join   {"doctorId": 12, "date": "2026-11-30", "dependentId": null}
    GET    /waitlists                    my active waitlists (with my place in line) and my pending offers
    DELETE /waitlists/{waitlistId}       leave  (waitlistId = <doctorId>_<YYYY-MM-DD>_<dependentId or 0>)
    POST   /offers/{offerId}/decline     give the held seat to the next person in line

Event-driven (SQS queue fed by an EventBridge rule; partial batch failures, DLQ after 5 receives):
    SlotSeatFreed   -> issue ONE hold offer for that seat to the first ACTIVE patient on that doctor's day
    SlotCancelled   -> withdraw offers on that slot (patient goes back to the front of the line);
                       reason "blackout" closes the whole day's waitlist
Scheduled (EventBridge Scheduler, every minute):
    {"action": "sweep"} -> expire LIVE offers past expires_at and pass the seat to the next patient in line;
                           resolve offers stuck in ACCEPTING (the accepting Lambda died) from RDS

Exclusivity - why there is never more than one live offer for one freed seat:
    * A hold is a counter on the slot item (held_count). Seats shown to normal booking are
      capacity - booked_count - held_count, so a held seat is invisible to everyone else.
    * Issuing an offer is ONE DynamoDB transaction: held_count + 1 on the slot (optimistic concurrency on the slot's
      version, so it only succeeds while that seat is still free), the waitlist entry ACTIVE -> OFFERED, and the new
      offer item (attribute_not_exists, its id derived from the SlotSeatFreed event, so a duplicate delivery of the
      same event can never create a second offer).
    * Expiry / decline is ONE transaction too: old offer LIVE -> EXPIRED/DECLINED, old entry ended, and either the
      next entry ACTIVE -> OFFERED + a new offer (held_count unchanged: the hold is TRANSFERRED, never released in
      between) or, with nobody left in line, held_count - 1.

Limits: at most `max-active` (SSM, default 5) live waitlist entries per account, enforced with an optimistic-
concurrency summary item per patient. Hold length: `hold-minutes` (SSM, default 15).
Metrics (namespace MediSlot): WaitlistJoins, WaitlistPromotions (a waiting patient got an offer), OffersExpired,
OffersDeclined, OffersVoided, OffersOverdue (sweeper lag, alarmed), WaitlistLeaves.
"""
import hashlib
import json
import os
import re
import threading
import time
import traceback
import uuid
from datetime import date, datetime, timedelta, timezone
from urllib.parse import unquote
from zoneinfo import ZoneInfo

import boto3

from common import (SLOT_ID_RE, aws_error_code, body, cancellation_reasons, correlation, db_config, dbconn, ddb_av,
                    ddb_item, emit, instrument, iso_utc, json_out, local_day_bounds, parse_iso_utc, parse_slot_id,
                    put_metric)

_cfg = boto3.session.Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 3, "mode": "standard"})
ddb = boto3.client("dynamodb", config=_cfg)
ssm = boto3.client("ssm", config=_cfg)
events = boto3.client("events", config=boto3.session.Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 1}))

UTC = timezone.utc
ACTIVE, OFFERED = "ACTIVE", "OFFERED"
LIVE_ENTRY = (ACTIVE, OFFERED)
DEFAULTS = {"hold_minutes": 15, "max_active": 5, "max_per_doctor_day": 1}
RANGES = {"hold_minutes": (2, 120), "max_active": (1, 20), "max_per_doctor_day": (1, 100)}
MIN_LEAD = timedelta(minutes=5)        # an offer always ends at least 5 minutes before the slot starts
MIN_HOLD = timedelta(minutes=2)        # ... and is never shorter than 2 minutes (otherwise it is not offered)
HORIZON_DAYS = 45                      # the slot materialiser's window
SWEEP_LIMIT = 200
OVERDUE_AFTER = 300                    # seconds past expiry before an offer counts as "overdue" (sweeper lag)
SETTINGS_TTL = 60
WAITLIST_ID_RE = re.compile(r"([1-9][0-9]{0,17})_([0-9]{4}-[0-9]{2}-[0-9]{2})_(0|[1-9][0-9]{0,17})")
OFFER_ID_RE = re.compile(r"OFR-[0-9a-f]{32}")
TRANSIENT = ("TransactionConflictException", "ProvisionedThroughputExceededException", "ThrottlingException",
             "RequestLimitExceeded", "TransactionInProgressException", "InternalServerError")


# =========================================================================== small helpers (tests replace these)
def utcnow():
    return datetime.now(UTC)


def sleep(seconds):
    time.sleep(seconds)


def metric(name, value=1, unit="Count", **dimensions):
    put_metric(name, value, unit, **dimensions)


def env_name():
    return os.environ.get("ENVIRONMENT", "dev")


def slots_table():
    return os.environ.get("SLOTS_TABLE") or f"MediSlot-{env_name()}-Slots"


def waitlist_table():
    return os.environ.get("WAITLIST_TABLE") or f"MediSlot-{env_name()}-WaitlistEntries"


def offers_table():
    return os.environ.get("OFFERS_TABLE") or f"MediSlot-{env_name()}-Offers"


def event_bus():
    return os.environ.get("EVENT_BUS_NAME") or f"medislot-{env_name()}-events"


def msg(cid, text, code=None, **extra):
    out = {"message": text, "correlationId": cid, **extra}
    if code:
        out["code"] = code
    return out


def zone_of(name):
    try:
        return ZoneInfo(name or "Asia/Kolkata")
    except Exception:  # noqa: BLE001
        return ZoneInfo("Asia/Kolkata")


def iso(dt):
    return iso_utc(dt).replace(".000000Z", "Z") if dt else None


def backoff(attempt):
    import random
    window = min(0.4, 0.025 * (2 ** (attempt - 1)))
    return window / 2 + random.uniform(0, window / 2)


def _vals(**kw):
    return {":" + k: ddb_av(v) for k, v in kw.items()}


def set_clause(fields, prefix="f"):
    """{'a': 1, 'b': None} -> ('#f0 = :f0', names, values, ['b']) - every attribute aliased (no reserved-word surprises).
    None values are returned separately so the caller can REMOVE them."""
    parts, names, values, removes = [], {}, {}, []
    for i, (k, v) in enumerate(fields.items()):
        if v is None:
            removes.append(k)
            continue
        names[f"#{prefix}{i}"] = k
        values[f":{prefix}{i}"] = ddb_av(v)
        parts.append(f"#{prefix}{i} = :{prefix}{i}")
    return ", ".join(parts), names, values, removes


# =========================================================================== settings (SSM, cached 60 s)
_settings = {"at": 0.0, "value": None}
_settings_lock = threading.Lock()


def load_settings(cid=None):
    with _settings_lock:
        if _settings["value"] and time.time() - _settings["at"] < SETTINGS_TTL:
            return dict(_settings["value"])
    env = env_name()
    names = {"hold_minutes": f"/medislot/{env}/waitlist/hold-minutes", "max_active": f"/medislot/{env}/waitlist/max-active",
             "max_per_doctor_day": f"/medislot/{env}/booking/max-per-doctor-per-day"}
    raw = {}
    try:
        res = ssm.get_parameters(Names=list(names.values()))
        by_name = {p["Name"]: p["Value"] for p in res.get("Parameters", [])}
        raw = {k: by_name.get(n) for k, n in names.items()}
    except Exception as exc:  # noqa: BLE001 - safe defaults
        emit("warning", "waitlist_settings_unavailable", cid, error=type(exc).__name__)
    out = {}
    for key, default in DEFAULTS.items():
        lo, hi = RANGES[key]
        try:
            n = int(str(raw.get(key)).strip())
            out[key] = n if lo <= n <= hi else default
        except (TypeError, ValueError):
            out[key] = default
    with _settings_lock:
        _settings.update(at=time.time(), value=out)
    return dict(out)


# =========================================================================== keys and reads
def waitlist_key(doctor_id, day):
    return f"DOC#{int(doctor_id)}#DAY#{day}"


def entry_key(patient_id, dependent_id):
    return f"P#{int(patient_id)}#D#{int(dependent_id or 0)}"


def waitlist_id(doctor_id, day, dependent_id):
    return f"{int(doctor_id)}_{day}_{int(dependent_id or 0)}"


def summary_key(patient_id):
    return {"waitlist_key": {"S": f"PATIENT#{int(patient_id)}"}, "entry_key": {"S": "SUMMARY"}}


def entry_table_key(wk, ek):
    return {"waitlist_key": {"S": wk}, "entry_key": {"S": ek}}


def slot_table_key(slot_id):
    parsed = parse_slot_id(slot_id)
    if parsed is None:
        raise ValueError("not a slot id")
    return {"doctor_id": {"N": str(parsed[0])}, "slot_key": {"S": parsed[1]}}


def get_slot(slot_id):
    if parse_slot_id(slot_id) is None:
        return None
    res = ddb.get_item(TableName=slots_table(), Key=slot_table_key(slot_id), ConsistentRead=True)
    return ddb_item(res.get("Item")) or None


def free_seats(slot):
    return max(int(slot.get("capacity", 1) or 1) - int(slot.get("booked_count", 0) or 0) - int(slot.get("held_count", 0) or 0), 0)


def slot_day(slot):
    if slot.get("local_date"):
        return str(slot["local_date"])
    return parse_iso_utc(slot["start_at"]).astimezone(zone_of(slot.get("timezone"))).date().isoformat()


def get_entry(wk, ek):
    res = ddb.get_item(TableName=waitlist_table(), Key=entry_table_key(wk, ek), ConsistentRead=True)
    return ddb_item(res.get("Item")) or None


def get_offer(offer_id):
    res = ddb.get_item(TableName=offers_table(), Key={"offer_id": {"S": offer_id}}, ConsistentRead=True)
    return ddb_item(res.get("Item")) or None


def query_all(**kwargs):
    items = []
    while True:
        res = ddb.query(**kwargs)
        items.extend(ddb_item(i) for i in res.get("Items", []))
        if not res.get("LastEvaluatedKey"):
            return items
        kwargs["ExclusiveStartKey"] = res["LastEvaluatedKey"]


def day_entries(doctor_id, day):
    """Every entry of one doctor-day waitlist (one partition, strongly consistent)."""
    return query_all(TableName=waitlist_table(), KeyConditionExpression="waitlist_key = :k",
                     ExpressionAttributeValues=_vals(k=waitlist_key(doctor_id, day)), ConsistentRead=True)


def queue_order(e):
    return (str(e.get("joined_at", "")), str(e.get("entry_key", "")))


def active_queue(doctor_id, day):
    """ACTIVE entries, first come first served (joined_at, then the key as a stable tie-break)."""
    return sorted((e for e in day_entries(doctor_id, day) if e.get("status") == ACTIVE), key=queue_order)


def day_slots(doctor_id, lo_iso, hi_iso):
    return query_all(TableName=slots_table(), KeyConditionExpression="doctor_id = :d AND slot_key BETWEEN :lo AND :hi",
                     ExpressionAttributeValues=_vals(d=int(doctor_id), lo=lo_iso, hi=hi_iso), ConsistentRead=True)


# =========================================================================== events
def publish(entries, cid):
    """Best effort after the state change (a lost event never undoes an offer: the sweeper still expires it)."""
    if not entries:
        return True
    batch = [{"Source": "medislot.waitlist", "DetailType": t, "EventBusName": event_bus(),
              "Detail": json.dumps({"eventId": uuid.uuid4().hex, "eventVersion": 1, "occurredAt": iso(utcnow()),
                                    "correlationId": cid, **d}, default=str)} for t, d in entries]
    for attempt in range(1, 4):
        try:
            res = events.put_events(Entries=batch)
            if not res.get("FailedEntryCount"):
                emit("info", "events_published", cid, detailTypes=[t for t, _ in entries])
                return True
            batch = [e for e, r in zip(batch, res.get("Entries", [])) if r.get("ErrorCode")] or batch
        except Exception as exc:  # noqa: BLE001
            emit("warning", "event_publish_retry", cid, attempt=attempt, error=type(exc).__name__)
        if attempt < 3:
            sleep(backoff(attempt))
    emit("error", "event_publish_failed", cid, detailTypes=[e["DetailType"] for e in batch])
    metric("EventPublishFailures", len(batch))
    return False


def offer_notify(offer):
    start = parse_iso_utc(offer["start_at"]).astimezone(zone_of(offer.get("tz")))
    until = datetime.fromtimestamp(int(offer["expires_at"]), UTC).astimezone(zone_of(offer.get("tz")))
    return {"doctorName": offer.get("doctor_name"), "localDate": start.date().isoformat(), "localStart": start.strftime("%H:%M"),
            "timezone": str(zone_of(offer.get("tz"))), "holdUntilLocal": until.strftime("%H:%M")}


def offer_event(kind, offer):
    return (kind, {"offerId": offer["offer_id"], "patientId": int(offer["patient_id"]), "dependentId": offer.get("dependent_id"),
                   "doctorId": int(offer["doctor_id"]), "slotId": offer["slot_id"], "startAt": offer["start_at"],
                   "expiresAt": iso(datetime.fromtimestamp(int(offer["expires_at"]), UTC)),
                   "holdMinutes": offer.get("hold_minutes"), "notify": offer_notify(offer)})


# =========================================================================== offers: issue, transfer, withdraw
def new_offer(slot, entry, now, settings, offer_id=None):
    start = parse_iso_utc(slot["start_at"])
    expires = min(now + timedelta(minutes=settings["hold_minutes"]), start - MIN_LEAD)
    return {"offer_id": offer_id or "OFR-" + uuid.uuid4().hex, "slot_id": slot["slot_id"],
            "doctor_id": int(slot["doctor_id"]), "consultation_type_id": int(slot["consultation_type_id"]),
            "start_at": slot["start_at"], "end_at": slot["end_at"], "local_date": slot_day(slot),
            "tz": slot.get("timezone") or entry.get("tz") or "Asia/Kolkata", "patient_id": str(entry["patient_id"]),
            "dependent_id": entry.get("dependent_id"), "waitlist_key": entry["waitlist_key"], "entry_key": entry["entry_key"],
            "waitlist_id": entry.get("waitlist_id"), "doctor_name": entry.get("doctor_name"), "status": "LIVE",
            "created_at": iso_utc(now), "expires_at": int(expires.timestamp()), "hold_minutes": settings["hold_minutes"],
            "purge_at": int(start.timestamp()) + 30 * 86400}


def offer_create(offer):
    clause, names, values, _ = set_clause({k: v for k, v in offer.items() if k != "offer_id"}, "o")
    return {"Update": {"TableName": offers_table(), "Key": {"offer_id": {"S": offer["offer_id"]}},
                       "UpdateExpression": "SET " + clause, "ConditionExpression": "attribute_not_exists(offer_id)",
                       "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}


def entry_offered(entry, offer, now):
    return {"Update": {"TableName": waitlist_table(), "Key": entry_table_key(entry["waitlist_key"], entry["entry_key"]),
                       "UpdateExpression": "SET #st = :offered, offer_id = :oid, offer_slot_id = :sid, offered_at = :t",
                       "ConditionExpression": "#st = :active AND joined_at = :j", "ExpressionAttributeNames": {"#st": "status"},
                       "ExpressionAttributeValues": _vals(offered=OFFERED, oid=offer["offer_id"], sid=offer["slot_id"],
                                                          t=iso_utc(now), active=ACTIVE, j=entry["joined_at"])}}


def slot_version_update(slot, held_delta, require_open, now):
    """Optimistic-concurrency write on the slot: held_count + held_delta, only if nobody changed the slot meanwhile."""
    cond = "version = :v" + (" AND #st = :open" if require_open else "") + (" AND held_count > :zero" if held_delta <= 0 else "")
    values = {"h": int(slot.get("held_count", 0) or 0) + held_delta, "nv": int(slot["version"]) + 1, "v": int(slot["version"]),
              "t": iso_utc(now)}
    if require_open:
        values["open"] = "OPEN"
    if held_delta <= 0:
        values["zero"] = 0
    spec = {"TableName": slots_table(), "Key": slot_table_key(slot["slot_id"]),
            "UpdateExpression": "SET held_count = :h, version = :nv, updated_at = :t", "ConditionExpression": cond,
            "ExpressionAttributeValues": _vals(**values)}
    if require_open:
        spec["ExpressionAttributeNames"] = {"#st": "status"}
    return {"Update": spec}


def usable_for_offer(slot, now):
    return bool(slot) and slot.get("status") == "OPEN" and parse_iso_utc(slot["start_at"]) - now >= MIN_LEAD + MIN_HOLD


def issue_offer(slot_id, cid, dedupe=None):
    """A seat on slot_id is free: hold it for the first ACTIVE patient on that doctor's day. -> outcome dict."""
    settings = load_settings(cid)
    offer_id = "OFR-" + hashlib.sha256(dedupe.encode()).hexdigest()[:32] if dedupe else None
    for attempt in range(1, 7):
        now = utcnow()
        slot = get_slot(slot_id)
        if not usable_for_offer(slot, now):
            return {"outcome": "slot_not_offerable"}
        if free_seats(slot) <= 0:
            return {"outcome": "no_free_seat"}
        queue = active_queue(int(slot["doctor_id"]), slot_day(slot))
        if not queue:
            return {"outcome": "nobody_waiting"}
        entry = queue[0]
        offer = new_offer(slot, entry, now, settings, offer_id)
        try:
            ddb.transact_write_items(TransactItems=[offer_create(offer), entry_offered(entry, offer, now),
                                                    slot_version_update(slot, +1, True, now)])
        except Exception as exc:  # noqa: BLE001
            reasons = cancellation_reasons(exc) if aws_error_code(exc) == "TransactionCanceledException" else None
            if reasons is None and aws_error_code(exc) not in TRANSIENT:
                raise
            if reasons and reasons[0] == "ConditionalCheckFailed":
                emit("info", "offer_already_issued_for_event", cid, slotId=slot_id, offerId=offer_id)
                return {"outcome": "duplicate", "offerId": offer_id}
            sleep(backoff(attempt))         # the entry or the slot changed under us: look again
            continue
        metric("WaitlistPromotions", 1)
        emit("info", "hold_offer_issued", cid, offerId=offer["offer_id"], slotId=slot_id, patientId=entry["patient_id"],
             expiresAt=offer["expires_at"], queueLength=len(queue))
        publish([offer_event("HoldOfferIssued", offer)], cid)
        return {"outcome": "issued", "offerId": offer["offer_id"]}
    raise RuntimeError("could not issue a hold offer (contention); the message is retried")


def pass_on(offer, terminal, cid, require_expired=False):
    """End a LIVE offer (EXPIRED by the sweeper, DECLINED by the patient) and, in the SAME transaction, give the held
    seat to the next patient in line - or release it when nobody is waiting / the slot can no longer be offered.
    -> outcome dict. Safe to call twice (the second call finds the offer no longer LIVE)."""
    settings = load_settings(cid)
    for attempt in range(1, 7):
        now = utcnow()
        now_s = int(now.timestamp())
        slot = get_slot(offer["slot_id"])
        cond = "#st = :live" + (" AND expires_at <= :now" if require_expired else "")
        items = [{"Update": {"TableName": offers_table(), "Key": {"offer_id": {"S": offer["offer_id"]}},
                             "UpdateExpression": "SET #st = :term, ended_at = :t", "ConditionExpression": cond,
                             "ExpressionAttributeNames": {"#st": "status"},
                             "ExpressionAttributeValues": _vals(term=terminal, t=iso_utc(now), live="LIVE",
                                                                **({"now": now_s} if require_expired else {}))}},
                 {"Update": {"TableName": waitlist_table(), "Key": entry_table_key(offer["waitlist_key"], offer["entry_key"]),
                             "UpdateExpression": "SET #st = :term, ended_at = :t", "ConditionExpression": "offer_id = :oid",
                             "ExpressionAttributeNames": {"#st": "status"},
                             "ExpressionAttributeValues": _vals(term=terminal, t=iso_utc(now), oid=offer["offer_id"])}}]
        nxt, nxt_offer = None, None
        if usable_for_offer(slot, now):
            queue = [e for e in active_queue(int(offer["doctor_id"]), offer["local_date"]) if e["entry_key"] != offer["entry_key"]]
            nxt = queue[0] if queue else None
        if slot and nxt:
            nxt_offer = new_offer(slot, nxt, now, settings)
            items += [offer_create(nxt_offer), entry_offered(nxt, nxt_offer, now), slot_version_update(slot, 0, True, now)]
        elif slot and int(slot.get("held_count", 0) or 0) > 0:
            items.append(slot_version_update(slot, -1, False, now))
        elif slot:
            emit("error", "offer_without_held_seat", cid, offerId=offer["offer_id"], slotId=offer["slot_id"])
        try:
            ddb.transact_write_items(TransactItems=items)
        except Exception as exc:  # noqa: BLE001
            reasons = cancellation_reasons(exc) if aws_error_code(exc) == "TransactionCanceledException" else None
            if reasons is None and aws_error_code(exc) not in TRANSIENT:
                raise
            if reasons and reasons[0] == "ConditionalCheckFailed":
                return {"outcome": "already_ended"}
            sleep(backoff(attempt))
            continue
        out = []
        if terminal == "EXPIRED":
            metric("OffersExpired", 1)
            out.append(offer_event("HoldOfferExpired", offer))
        else:
            metric("OffersDeclined", 1)
        if nxt_offer:
            metric("WaitlistPromotions", 1)
            out.append(offer_event("HoldOfferIssued", nxt_offer))
        emit("info", "hold_offer_ended", cid, offerId=offer["offer_id"], status=terminal, slotId=offer["slot_id"],
             passedTo=nxt_offer["offer_id"] if nxt_offer else None, seatReleased=bool(slot and not nxt_offer))
        publish(out, cid)
        return {"outcome": "passed_on" if nxt_offer else "released", "nextOfferId": nxt_offer["offer_id"] if nxt_offer else None}
    raise RuntimeError("could not end the offer (contention); try again")


def withdraw_offer(offer, cid, entry_status=ACTIVE, offer_status="VOID"):
    """The slot is gone (doctor cancelled it): the offer is withdrawn and the held seat released. entry_status ACTIVE
    puts the patient back in line with their ORIGINAL joined_at (front of the queue); CLOSED ends the entry."""
    now = utcnow()
    items = [{"Update": {"TableName": offers_table(), "Key": {"offer_id": {"S": offer["offer_id"]}},
                         "UpdateExpression": "SET #st = :void, ended_at = :t", "ConditionExpression": "#st = :live",
                         "ExpressionAttributeNames": {"#st": "status"},
                         "ExpressionAttributeValues": _vals(void=offer_status, t=iso_utc(now), live="LIVE")}},
             {"Update": {"TableName": waitlist_table(), "Key": entry_table_key(offer["waitlist_key"], offer["entry_key"]),
                         "UpdateExpression": "SET #st = :es REMOVE offer_id, offer_slot_id",
                         "ConditionExpression": "offer_id = :oid", "ExpressionAttributeNames": {"#st": "status"},
                         "ExpressionAttributeValues": _vals(es=entry_status, oid=offer["offer_id"])}}]
    slot = get_slot(offer["slot_id"])
    if slot and int(slot.get("held_count", 0) or 0) > 0:
        items.append({"Update": {"TableName": slots_table(), "Key": slot_table_key(offer["slot_id"]),
                                 "UpdateExpression": "SET held_count = held_count - :one, version = version + :one",
                                 "ConditionExpression": "held_count > :zero", "ExpressionAttributeValues": _vals(one=1, zero=0)}})
    try:
        ddb.transact_write_items(TransactItems=items)
    except Exception as exc:  # noqa: BLE001
        if aws_error_code(exc) == "TransactionCanceledException" and (cancellation_reasons(exc) or [None])[0] == "ConditionalCheckFailed":
            return False
        raise
    metric("OffersVoided", 1)
    emit("info", "hold_offer_withdrawn", cid, offerId=offer["offer_id"], slotId=offer["slot_id"], entryStatus=entry_status)
    return True


def close_entry(entry, cid):
    try:
        ddb.update_item(TableName=waitlist_table(), Key=entry_table_key(entry["waitlist_key"], entry["entry_key"]),
                        UpdateExpression="SET #st = :closed, ended_at = :t", ConditionExpression="#st = :active",
                        ExpressionAttributeNames={"#st": "status"},
                        ExpressionAttributeValues=_vals(closed="CLOSED", t=iso_utc(utcnow()), active=ACTIVE))
        return True
    except Exception as exc:  # noqa: BLE001
        if aws_error_code(exc) == "ConditionalCheckFailedException":
            return False
        raise


def on_slot_cancelled(detail, cid):
    """SlotCancelled: offers on that slot are withdrawn (the patient keeps their place for other seats that day).
    reason 'blackout' - the doctor is away the whole day - closes every entry of that day's waitlist."""
    slot_id = detail.get("slotId")
    slot = get_slot(slot_id) if isinstance(slot_id, str) else None
    if not slot:
        return {"outcome": "slot_unknown"}
    blackout = str(detail.get("reason") or "").lower() == "blackout"
    withdrawn = closed = 0
    for entry in day_entries(int(slot["doctor_id"]), slot_day(slot)):
        if entry.get("status") == OFFERED and entry.get("offer_id"):
            offer = get_offer(entry["offer_id"])
            if offer and offer.get("status") == "LIVE" and (blackout or offer["slot_id"] == slot_id):
                withdrawn += withdraw_offer(offer, cid, "CLOSED" if blackout else ACTIVE, "CLOSED" if blackout else "VOID")
        elif entry.get("status") == ACTIVE and blackout:
            closed += close_entry(entry, cid)
    emit("info", "waitlist_slot_cancelled", cid, slotId=slot_id, blackout=blackout, offersWithdrawn=withdrawn, entriesClosed=closed)
    return {"outcome": "handled", "withdrawn": withdrawn, "closed": closed}


# =========================================================================== stuck ACCEPTING offers
def resolve_accepting(offer, cid):
    """The booking Lambda died between taking the seat (held -> booked) and finishing. RDS decides: if the
    appointment exists the offer is ACCEPTED, otherwise the seat goes back on hold and the offer back to LIVE."""
    key = f"OFFER#{offer['offer_id']}"
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT appointment_id FROM appointments WHERE idempotency_key = %s", (key,))
            row = cur.fetchone()
        conn.rollback()
    now = utcnow()
    if row:
        ddb.transact_write_items(TransactItems=[
            {"Update": {"TableName": offers_table(), "Key": {"offer_id": {"S": offer["offer_id"]}},
                        "UpdateExpression": "SET #st = :ok, appointment_id = :aid, accepted_at = :t",
                        "ConditionExpression": "#st = :acc", "ExpressionAttributeNames": {"#st": "status"},
                        "ExpressionAttributeValues": _vals(ok="ACCEPTED", aid=int(row["appointment_id"]), t=iso_utc(now), acc="ACCEPTING")}},
            {"Update": {"TableName": waitlist_table(), "Key": entry_table_key(offer["waitlist_key"], offer["entry_key"]),
                        "UpdateExpression": "SET #st = :b, appointment_id = :aid, ended_at = :t", "ConditionExpression": "offer_id = :oid",
                        "ExpressionAttributeNames": {"#st": "status"},
                        "ExpressionAttributeValues": _vals(b="BOOKED", aid=int(row["appointment_id"]), t=iso_utc(now), oid=offer["offer_id"])}}])
        emit("warning", "accepting_offer_resolved", cid, offerId=offer["offer_id"], outcome="accepted")
        return "accepted"
    ddb.transact_write_items(TransactItems=[
        {"Update": {"TableName": slots_table(), "Key": slot_table_key(offer["slot_id"]),
                    "UpdateExpression": "SET booked_count = booked_count - :one, held_count = held_count + :one, version = version + :one",
                    "ConditionExpression": "booked_count > :zero", "ExpressionAttributeValues": _vals(one=1, zero=0)}},
        {"Update": {"TableName": offers_table(), "Key": {"offer_id": {"S": offer["offer_id"]}},
                    "UpdateExpression": "SET #st = :live REMOVE attempt_id, accepting_lease",
                    "ConditionExpression": "#st = :acc AND accepting_lease < :now", "ExpressionAttributeNames": {"#st": "status"},
                    "ExpressionAttributeValues": _vals(live="LIVE", acc="ACCEPTING", now=int(now.timestamp()))}}])
    emit("warning", "accepting_offer_resolved", cid, offerId=offer["offer_id"], outcome="back_on_hold")
    return "reverted"


def offers_with_status(status, upper):
    return query_all(TableName=offers_table(), IndexName="GSI_Status",
                     KeyConditionExpression="#st = :s AND expires_at <= :hi", ExpressionAttributeNames={"#st": "status"},
                     ExpressionAttributeValues=_vals(s=status, hi=int(upper)), Limit=SWEEP_LIMIT)


def sweep(cid):
    """Every minute: expire due offers (passing the seat on) and resolve offers stuck in ACCEPTING."""
    now_s = int(utcnow().timestamp())
    due = offers_with_status("LIVE", now_s)
    overdue = sum(1 for o in due if int(o["expires_at"]) < now_s - OVERDUE_AFTER)
    results = {"expired": 0, "passedOn": 0, "released": 0, "errors": 0, "resolved": 0}
    for offer in due:
        try:
            out = pass_on(offer, "EXPIRED", cid, require_expired=True)
            if out["outcome"] != "already_ended":
                results["expired"] += 1
                results["passedOn" if out["outcome"] == "passed_on" else "released"] += 1
        except Exception as exc:  # noqa: BLE001 - one bad offer must not stop the sweep; it is retried next minute
            results["errors"] += 1
            emit("error", "offer_expiry_failed", cid, offerId=offer.get("offer_id"), error=type(exc).__name__, detail=str(exc)[:200])
    for offer in offers_with_status("ACCEPTING", 2 ** 40):
        if int(offer.get("accepting_lease", 0)) < now_s:
            try:
                resolve_accepting(offer, cid)
                results["resolved"] += 1
            except Exception as exc:  # noqa: BLE001
                results["errors"] += 1
                emit("error", "accepting_offer_resolve_failed", cid, offerId=offer.get("offer_id"), error=type(exc).__name__)
    metric("OffersOverdue", overdue)
    emit("info", "offer_sweep_finished", cid, due=len(due), overdue=overdue, **results)
    return {"ok": results["errors"] == 0, "due": len(due), "overdue": overdue, **results}


# =========================================================================== API
def patient_context(cur, uid, dependent_id):
    cur.execute("""SELECT u.is_active, p.patient_id FROM users u LEFT JOIN patients p ON p.patient_id = u.user_id
                   WHERE u.user_id = %s AND u.role = 'PATIENT'""", (uid,))
    me = cur.fetchone()
    if not me or not me["is_active"]:
        return "ACCOUNT_INACTIVE"
    if me["patient_id"] is None:
        return "PROFILE_REQUIRED"
    if dependent_id is not None:
        cur.execute("""SELECT dependent_id FROM patient_dependents WHERE dependent_id = %s AND patient_id = %s
                       AND is_active = 1""", (dependent_id, uid))
        if not cur.fetchone():
            return "DEPENDENT_NOT_FOUND"
    return None


def join(event, uid, cid):
    data = body(event)
    if not isinstance(data, dict):
        return json_out(400, msg(cid, "Invalid JSON body", "INVALID_BODY"))
    extra = sorted(k for k in data if k not in ("doctorId", "date", "dependentId"))
    if extra:
        return json_out(422, msg(cid, "Only doctorId, date and dependentId can be sent (not: " + ", ".join(extra)[:120] + ")",
                                 "UNKNOWN_FIELDS"))
    doctor_id, day_text, dep = data.get("doctorId"), data.get("date"), data.get("dependentId")
    if isinstance(doctor_id, bool) or not isinstance(doctor_id, int) or not 1 <= doctor_id < 10 ** 18:
        return json_out(422, msg(cid, "doctorId must be a positive number", "DOCTOR_ID_INVALID"))
    try:
        day = date.fromisoformat(day_text) if isinstance(day_text, str) and re.fullmatch(r"\d{4}-\d{2}-\d{2}", day_text) else None
    except ValueError:
        day = None
    if day is None:
        return json_out(422, msg(cid, "date must be a calendar date (YYYY-MM-DD)", "DATE_INVALID"))
    if dep is not None and (isinstance(dep, bool) or not isinstance(dep, int) or not 1 <= dep < 10 ** 18):
        return json_out(422, msg(cid, "dependentId must be one of your family members, or null for yourself", "DEPENDENT_ID_INVALID"))
    settings, now = load_settings(cid), utcnow()
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            problem = patient_context(cur, uid, dep)
            cur.execute("""SELECT d.timezone, d.first_name, d.last_name FROM doctors d
                           JOIN users u ON u.user_id = d.doctor_id AND u.is_active = 1 WHERE d.doctor_id = %s""", (doctor_id,))
            doc = cur.fetchone()
            blackout = booked = False
            if doc and not problem:
                cur.execute("SELECT blackout_id FROM blackout_dates WHERE doctor_id = %s AND blackout_date = %s",
                            (doctor_id, day.isoformat()))
                blackout = cur.fetchone() is not None
                zone = zone_of(doc["timezone"])
                lo = datetime.combine(day, datetime.min.time(), tzinfo=zone).astimezone(UTC).replace(tzinfo=None)
                hi = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=zone).astimezone(UTC).replace(tzinfo=None)
                cur.execute("""SELECT COUNT(*) AS n FROM appointments WHERE patient_id = %s AND doctor_id = %s AND status = 'CONFIRMED'
                               AND COALESCE(dependent_id, 0) = %s AND start_at >= %s AND start_at < %s""",
                            (uid, doctor_id, int(dep or 0), lo, hi))
                booked = int(cur.fetchone()["n"]) >= settings["max_per_doctor_day"]
        conn.rollback()
    if problem == "ACCOUNT_INACTIVE":
        return json_out(403, msg(cid, "This account cannot use waitlists", problem))
    if problem == "PROFILE_REQUIRED":
        return json_out(409, msg(cid, "Complete your patient profile first", problem))
    if problem == "DEPENDENT_NOT_FOUND":
        return json_out(404, msg(cid, "That family member is not on your account", problem))
    if not doc:
        return json_out(404, msg(cid, "Doctor not found", "DOCTOR_NOT_FOUND"))
    zone = zone_of(doc["timezone"])
    today = now.astimezone(zone).date()
    if day < today:
        return json_out(422, msg(cid, "That day is in the past", "DATE_IN_PAST"))
    if day > today + timedelta(days=HORIZON_DAYS):
        return json_out(422, msg(cid, f"You can join a waitlist up to {HORIZON_DAYS} days ahead", "DATE_TOO_FAR"))
    if blackout:
        return json_out(409, msg(cid, "The doctor is not available on that day", "DOCTOR_UNAVAILABLE"))
    if booked:
        return json_out(409, msg(cid, "You already have an appointment with this doctor that day" if dep is None else
                                 "This family member already has an appointment with this doctor that day", "ALREADY_BOOKED_THAT_DAY"))
    lo_iso, hi_iso = local_day_bounds(day, day, zone)
    open_future = [s for s in day_slots(doctor_id, lo_iso, hi_iso)
                   if s.get("status") == "OPEN" and parse_iso_utc(s["start_at"]) - now >= MIN_LEAD + MIN_HOLD]
    if not open_future:
        return json_out(409, msg(cid, "The doctor has no slots left that day", "NO_SLOTS_THAT_DAY"))
    if sum(free_seats(s) for s in open_future) > 0:
        return json_out(409, msg(cid, "There are still free slots that day - book one directly", "SLOTS_AVAILABLE"))
    return add_entry(uid, doctor_id, day.isoformat(), dep, f"Dr. {doc['first_name']} {doc['last_name']}", str(zone), settings, now, cid)


def entry_is_live(wid, uid, today_utc):
    m = WAITLIST_ID_RE.fullmatch(str(wid))
    if not m or date.fromisoformat(m.group(2)) < today_utc - timedelta(days=1):
        return False
    e = get_entry(waitlist_key(m.group(1), m.group(2)), entry_key(uid, int(m.group(3)) or None))
    return bool(e) and e.get("status") in LIVE_ENTRY


def add_entry(uid, doctor_id, day, dep, doctor_name, tz, settings, now, cid):
    wk, ek, wid = waitlist_key(doctor_id, day), entry_key(uid, dep), waitlist_id(doctor_id, day, dep)
    today_utc = now.date()
    for attempt in range(1, 6):
        res = ddb.get_item(TableName=waitlist_table(), Key=summary_key(uid), ConsistentRead=True)
        summary = ddb_item(res.get("Item"))
        existing = get_entry(wk, ek)
        if existing and existing.get("status") in LIVE_ENTRY:
            return json_out(409, msg(cid, "You are already on this waitlist", "ALREADY_ON_WAITLIST"))
        # Self-healing count: an id only counts while its entry is really ACTIVE/OFFERED and its day has not passed.
        live = [i for i in (summary.get("active") or []) if i != wid and entry_is_live(i, uid, today_utc)]
        if len(live) >= settings["max_active"]:
            return json_out(409, msg(cid, f"You can be on at most {settings['max_active']} waitlists at a time. Leave one to join another.",
                                     "WAITLIST_LIMIT_REACHED", maxActive=settings["max_active"]))
        version = int(summary.get("version") or 0)
        fields = {"status": ACTIVE, "joined_at": iso_utc(now), "patient_id": str(uid), "dependent_id": dep,
                  "doctor_id": int(doctor_id), "local_date": day, "waitlist_id": wid, "doctor_name": doctor_name, "tz": tz,
                  "purge_at": int(datetime.combine(date.fromisoformat(day), datetime.min.time(), tzinfo=UTC).timestamp()) + 9 * 86400}
        clause, names, values, removes = set_clause(fields)
        names["#st0"] = "status"
        values.update(_vals(active=ACTIVE, offered=OFFERED))
        removes += ["offer_id", "offer_slot_id", "ended_at", "appointment_id", "offered_at"]
        try:
            ddb.transact_write_items(TransactItems=[
                {"Update": {"TableName": waitlist_table(), "Key": summary_key(uid),
                            "UpdateExpression": "SET active = :ids, version = :nv",
                            "ConditionExpression": "attribute_not_exists(version) OR version = :v",
                            "ExpressionAttributeValues": _vals(ids=live + [wid], nv=version + 1, v=version)}},
                {"Update": {"TableName": waitlist_table(), "Key": entry_table_key(wk, ek),
                            "UpdateExpression": "SET " + clause + " REMOVE " + ", ".join(removes),
                            "ConditionExpression": "attribute_not_exists(#st0) OR (#st0 <> :active AND #st0 <> :offered)",
                            "ExpressionAttributeNames": names, "ExpressionAttributeValues": values}}])
        except Exception as exc:  # noqa: BLE001
            reasons = cancellation_reasons(exc) if aws_error_code(exc) == "TransactionCanceledException" else None
            if reasons is None and aws_error_code(exc) not in TRANSIENT:
                raise
            if reasons and len(reasons) > 1 and reasons[1] == "ConditionalCheckFailed":
                return json_out(409, msg(cid, "You are already on this waitlist", "ALREADY_ON_WAITLIST"))
            sleep(backoff(attempt))       # another join by the same patient won the summary race: count again
            continue
        position = sum(1 for e in active_queue(doctor_id, day) if queue_order(e) <= (fields["joined_at"], ek))
        metric("WaitlistJoins", 1)
        emit("info", "waitlist_joined", cid, patientId=uid, doctorId=doctor_id, date=day, dependentId=dep, position=position)
        return json_out(201, {"message": "You are on the waitlist. We will hold the next free seat that day for you.",
                              "waitlist": {"waitlistId": wid, "doctorId": int(doctor_id), "doctorName": doctor_name, "date": day,
                                           "dependentId": dep, "status": ACTIVE, "position": position, "joinedAt": fields["joined_at"]},
                              "correlationId": cid})
    return json_out(409, msg(cid, "Please try again in a moment", "WAITLIST_BUSY"))


def offer_json(o, now_s):
    return {"offerId": o["offer_id"], "status": o.get("status"), "slotId": o["slot_id"], "doctorId": int(o["doctor_id"]),
            "doctorName": o.get("doctor_name"), "consultationTypeId": int(o["consultation_type_id"]), "startAt": o["start_at"],
            "endAt": o["end_at"], "expiresAt": iso(datetime.fromtimestamp(int(o["expires_at"]), UTC)),
            "secondsLeft": max(int(o["expires_at"]) - now_s, 0), "dependentId": o.get("dependent_id"),
            "waitlistId": o.get("waitlist_id"), **{k: v for k, v in offer_notify(o).items() if k != "doctorName"}}


def list_mine(uid, cid):
    now = utcnow()
    now_s, today_utc = int(now.timestamp()), now.date()
    entries = query_all(TableName=waitlist_table(), IndexName="GSI_Patient", KeyConditionExpression="patient_id = :p",
                        ExpressionAttributeValues=_vals(p=str(uid)), ScanIndexForward=False)
    items, recent, offers = [], [], []
    for e in sorted(entries, key=lambda x: str(x.get("joined_at", "")), reverse=True):
        day = date.fromisoformat(str(e["local_date"]))
        view = {"waitlistId": e.get("waitlist_id"), "doctorId": int(e["doctor_id"]), "doctorName": e.get("doctor_name"),
                "date": e["local_date"], "dependentId": e.get("dependent_id"), "status": e.get("status"), "joinedAt": e.get("joined_at")}
        if e.get("status") in LIVE_ENTRY and day >= today_utc - timedelta(days=1):
            if e.get("status") == ACTIVE:
                view["position"] = sum(1 for x in active_queue(e["doctor_id"], e["local_date"]) if queue_order(x) <= queue_order(e))
            elif e.get("offer_id"):
                o = get_offer(e["offer_id"])
                if o and o.get("status") in ("LIVE", "ACCEPTING"):
                    view["offer"] = offer_json(o, now_s)
                    offers.append(view["offer"])
            items.append(view)
        elif len(recent) < 10:
            recent.append(view)
    settings = load_settings(cid)
    return json_out(200, {"items": items, "offers": offers, "recent": recent, "maxActive": settings["max_active"],
                          "holdMinutes": settings["hold_minutes"], "correlationId": cid})


def leave(wid, uid, cid):
    m = WAITLIST_ID_RE.fullmatch(wid)
    if not m:
        return json_out(422, msg(cid, "waitlistId is not valid", "WAITLIST_ID_INVALID"))
    wk, ek = waitlist_key(m.group(1), m.group(2)), entry_key(uid, int(m.group(3)) or None)
    entry = get_entry(wk, ek)
    if not entry or str(entry.get("patient_id")) != str(uid):
        return json_out(404, msg(cid, "You are not on that waitlist", "WAITLIST_NOT_FOUND"))
    if entry.get("status") == OFFERED and entry.get("offer_id"):
        offer = get_offer(entry["offer_id"])
        if offer and offer.get("status") == "ACCEPTING":
            return json_out(409, msg(cid, "Your offer is being accepted right now", "REQUEST_IN_PROGRESS"))
        if offer and offer.get("status") == "LIVE":
            pass_on(offer, "DECLINED", cid)
            metric("WaitlistLeaves", 1)
            return json_out(200, msg(cid, "You left the waitlist. The seat held for you was passed to the next person."))
    if entry.get("status") == ACTIVE:
        try:
            ddb.update_item(TableName=waitlist_table(), Key=entry_table_key(wk, ek),
                            UpdateExpression="SET #st = :left, ended_at = :t", ConditionExpression="#st = :active",
                            ExpressionAttributeNames={"#st": "status"},
                            ExpressionAttributeValues=_vals(left="LEFT", t=iso_utc(utcnow()), active=ACTIVE))
        except Exception as exc:  # noqa: BLE001
            if aws_error_code(exc) != "ConditionalCheckFailedException":
                raise
            return leave(wid, uid, cid)        # it just got an offer: leave through the offer
        metric("WaitlistLeaves", 1)
        emit("info", "waitlist_left", cid, patientId=uid, waitlistId=wid)
        return json_out(200, msg(cid, "You left the waitlist"))
    return json_out(200, msg(cid, "You are no longer on this waitlist", alreadyLeft=True))


def decline(offer_id, uid, cid):
    offer = get_offer(offer_id)
    if not offer or str(offer.get("patient_id")) != str(uid):
        if offer:
            emit("warning", "offer_isolation_violation", cid, callerId=uid, offerId=offer_id)
        return json_out(404, msg(cid, "Offer not found", "OFFER_NOT_FOUND"))
    if offer.get("status") == "ACCEPTING":
        return json_out(409, msg(cid, "This offer is being accepted right now", "REQUEST_IN_PROGRESS"))
    if offer.get("status") != "LIVE":
        return json_out(200, msg(cid, "This offer has already ended", alreadyEnded=True, status=offer.get("status")))
    out = pass_on(offer, "DECLINED", cid)
    return json_out(200, msg(cid, "Offer declined. The seat was passed to the next person in line." if out.get("nextOfferId")
                             else "Offer declined.", **({"alreadyEnded": True} if out["outcome"] == "already_ended" else {})))


# =========================================================================== SQS consumer
def handle_queue(event, cid):
    failures = []
    for record in event.get("Records") or []:
        try:
            message = json.loads(record["body"])
            kind, detail = message.get("detail-type"), message.get("detail") or {}
            if kind == "SlotSeatFreed":
                out = issue_offer(detail["slotId"], detail.get("correlationId") or cid,
                                  dedupe=f"{message.get('id')}:{detail.get('eventId')}")
            elif kind == "SlotCancelled":
                out = on_slot_cancelled(detail, detail.get("correlationId") or cid)
            else:
                out = {"outcome": "ignored"}
            emit("info", "waitlist_event_handled", cid, detailType=kind, slotId=detail.get("slotId"), **out)
        except Exception as exc:  # noqa: BLE001 - only this record is retried; after 5 receives it goes to the DLQ
            emit("error", "waitlist_event_failed", cid, messageId=record.get("messageId"), error=type(exc).__name__,
                 detail=str(exc)[:300], traceback=traceback.format_exc()[-1200:])
            failures.append({"itemIdentifier": record.get("messageId")})
    return {"batchItemFailures": failures}


# =========================================================================== entrypoint
def route(event):
    return (event.get("path") or event.get("rawPath") or "").rstrip("/") or "/"


@instrument("waitlist")
def handler(event, context):
    event = event if isinstance(event, dict) else {}
    cid = correlation(event)
    if event.get("Records"):
        return handle_queue(event, cid)
    if event.get("action") and "httpMethod" not in event and "requestContext" not in event:
        if event["action"] == "sweep":
            return sweep(cid)
        return {"ok": False, "message": "Unknown action"}
    ctx = (event.get("requestContext") or {}).get("authorizer") or {}
    try:
        uid = int(ctx.get("userId") or 0)
    except (TypeError, ValueError):
        uid = 0
    if ctx.get("role") != "PATIENT" or uid <= 0:
        emit("warning", "waitlist_access_denied", cid, role=ctx.get("role"))
        return json_out(403, msg(cid, "Patient role required"))
    method = (event.get("httpMethod") or "GET").upper()
    path = route(event)
    try:
        if re.search(r"/waitlists$", path):
            if method == "POST":
                return join(event, uid, cid)
            if method == "GET":
                return list_mine(uid, cid)
            return json_out(405, msg(cid, "Method not allowed"))
        m = re.search(r"/waitlists/([^/]+)$", path)
        if m:
            if method != "DELETE":
                return json_out(405, msg(cid, "Method not allowed"))
            return leave(unquote((event.get("pathParameters") or {}).get("waitlistId") or m.group(1)), uid, cid)
        m = re.search(r"/offers/([^/]+)/decline$", path)
        if m:
            if method != "POST":
                return json_out(405, msg(cid, "Method not allowed"))
            offer_id = unquote((event.get("pathParameters") or {}).get("offerId") or m.group(1))
            if not OFFER_ID_RE.fullmatch(offer_id):
                return json_out(422, msg(cid, "offerId is not valid", "OFFER_ID_INVALID"))
            return decline(offer_id, uid, cid)
        return json_out(404, msg(cid, "Waitlist endpoint not found", path=path))
    except Exception as exc:  # noqa: BLE001
        emit("error", "waitlist_error", cid, error=type(exc).__name__, path=path, method=method, detail=str(exc)[:300],
             traceback=traceback.format_exc()[-1500:])
        return json_out(500, msg(cid, "Internal server error"))
