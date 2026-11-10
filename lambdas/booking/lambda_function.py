"""MediSlot booking engine (Milestone 5): booking, concurrency, cancellation and refunds - all in one place.

Routes (behind the JWT authorizer; the role is checked here, the user id always comes from the verified token):
    POST /appointments                         PATIENT  book a slot. Header Idempotency-Key is REQUIRED.
    GET  /appointments                         PATIENT  my appointments    | DOCTOR  appointments with me
    GET  /appointments/{appointmentId}         PATIENT  one of mine        | DOCTOR  one with me
    POST /appointments/{appointmentId}/cancel  PATIENT  cancel; refund tier from the SSM refund policy
    POST /doctors/me/slots/{slotId}/cancel     DOCTOR   cancel a whole slot; every booked patient refunded 100 %
    GET  /patients/me/refunds                  PATIENT  my refund ledger
    GET  /patients/me/dependents               PATIENT  my family members (people I book for)
    POST /patients/me/dependents               PATIENT  add a family member
    PATCH/DELETE /patients/me/dependents/{id}  PATIENT  edit / remove a family member

Family members
    One account can book for the account holder AND for family members ("dependents"): POST /appointments with
    {"slotId": ..., "dependentId": 12}. The rules that are about one human being apply PER PERSON - the same person
    cannot be in two overlapping appointments, cannot hold the same slot twice, and is limited per doctor per day -
    so a patient and their mother CAN both be booked 11:00-11:30 with two different doctors. The cap on upcoming
    appointments stays per ACCOUNT (it limits how many seats one login can hold).

How a booking stays correct under contention (full write-up: MILESTONE5.md "Concurrency design note")
    1. Idempotency claim (DynamoDB Idempotency table, conditional put). The key is scoped to the patient:
       BOOK#<patientId>#<client key>. Same key + same body -> the stored original response is replayed.
       Same key + different body -> 422. Same key while the first request is still running -> 409.
    2. Read-only checks in RDS: patient account, the doctor still offers the slot's consultation type
       (fee snapshot comes from here), and the patient limits (duplicate, overlap, per-doctor-per-day, active cap).
    3. Seat claim = optimistic concurrency on the slot item. Read the slot (strongly consistent), compute
       free = capacity - booked_count - held_count, then write booked_count+1 / version+1 with the condition
       `version = <version I read> AND status = OPEN`. The write is a DynamoDB transaction together with the
       idempotency record (`seat_claimed = true`, owned by this attempt), so a crash can never leave a claimed
       seat that nobody knows about. If another request won the race the condition fails: re-read, back off
       (exponential + jitter) and retry, at most `max-conflict-retries` times. Every retry is counted in the
       BookingConflictRetries CloudWatch metric. When free reaches 0 -> 409 SLOT_FULL.
    4. RDS transaction (system of record): lock the patient's users row (SELECT ... FOR UPDATE) so two
       parallel bookings by the SAME patient are serialised, re-check the limits authoritatively, INSERT.
       Different patients never wait for each other in RDS - the slot race is decided only in step 3.
       Any failure after the seat was claimed releases the seat again (compensation).
    5. After COMMIT: re-read the slot. If the doctor cancelled it in the meantime, this appointment is
       cancelled with a full refund too (closes the claim-then-doctor-cancel race). Then AppointmentBooked
       is published to EventBridge.

Refunds
    * Tiers come from SSM /medislot/<env>/booking/refund-policy (JSON, re-read at most every 60 s).
      A tier applies when (start_at - now) >= minHoursBefore; boundaries are inclusive and compared as exact
      timedeltas (microseconds, no floating point). Cancelling at or after the start instant is refused.
    * Money is Decimal, rounded half-up to 0.01; deduction = fee - refund, so the two always add up to the fee.
    * One ledger row per appointment (UNIQUE appointment_id): a repeated cancel returns the same refund.
    * Doctor-initiated cancellation always refunds 100 % (tier DOCTOR_CANCELLED).
"""
import hashlib
import json
import os
import random
import re
import threading
import time
import traceback
import uuid
from datetime import datetime, timedelta, timezone
from decimal import ROUND_HALF_UP, Decimal, InvalidOperation
from urllib.parse import unquote
from zoneinfo import ZoneInfo

import boto3
import pymysql

from common import (FieldErrors, body, clean_date_of_birth, clean_gender, clean_person_name, correlation, db_config,
                    dbconn, emit, instrument, json_out)

_cfg = boto3.session.Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 3, "mode": "standard"})
ddb = boto3.client("dynamodb", config=_cfg)
ssm = boto3.client("ssm", config=_cfg)
# Short timeouts and no SDK retries: publish() does its own 3 attempts, and a slow EventBridge must never push a
# committed booking past the API Gateway timeout.
events = boto3.client("events", config=boto3.session.Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 1}))

UTC = timezone.utc
CENT = Decimal("0.01")
SLOT_ID_RE = re.compile(r"SLOT#[0-9a-f]{40}")
SLOT_HEX_RE = re.compile(r"[0-9a-f]{40}")
IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9._:\-]{8,128}")
TIER_NAME_RE = re.compile(r"[A-Z][A-Z0-9_]{0,29}")
CANCELLED = ("CANCELLED_BY_PATIENT", "CANCELLED_BY_DOCTOR")
STATUS_FILTERS = {
    "CONFIRMED": ("CONFIRMED",), "COMPLETED": ("COMPLETED",), "NO_SHOW": ("NO_SHOW",), "CANCELLED": CANCELLED,
    "CANCELLED_BY_PATIENT": ("CANCELLED_BY_PATIENT",), "CANCELLED_BY_DOCTOR": ("CANCELLED_BY_DOCTOR",),
}
DOCTOR_TIER = "DOCTOR_CANCELLED"
RELATIONS = ("Mother", "Father", "Spouse", "Child", "Sibling", "Grandparent", "Grandchild", "Other")
_RELATION_LOOKUP = {r.lower(): r for r in RELATIONS}
MAX_DEPENDENTS = 10
DEPENDENT_FIELDS = ("firstName", "lastName", "relation", "dateOfBirth", "gender")
DEFAULT_PAGE_SIZE, MAX_PAGE_SIZE = 10, 50
IDEMPOTENCY_TTL_SECONDS = 24 * 3600      # how long a key replays its original answer
LEASE_SECONDS = 30                        # > Lambda timeout (25 s): an IN_PROGRESS record older than this is abandoned
SETTINGS_TTL_SECONDS = 60
BACKOFF_BASE, BACKOFF_CAP = 0.025, 0.4    # seconds

DEFAULT_REFUND_POLICY = {"tiers": [
    {"name": "FULL", "minHoursBefore": 24, "refundPercent": 100},
    {"name": "PARTIAL", "minHoursBefore": 2, "refundPercent": 50},
    {"name": "NONE", "minHoursBefore": 0, "refundPercent": 0},
]}
DEFAULT_SETTINGS = {"max_active": 3, "max_per_doctor_day": 1, "max_retries": 10}


# =========================================================================== small helpers
class PolicyError(Exception):
    """The refund policy in SSM is missing or invalid. Cancellations fail closed (503) instead of guessing money."""


class TooLate(Exception):
    """The appointment has started (or is in the past): it can no longer be cancelled."""


def env_name():
    return os.environ.get("ENVIRONMENT", "dev")


def slots_table():
    return os.environ.get("SLOTS_TABLE") or f"MediSlot-{env_name()}-Slots"


def idempotency_table():
    return os.environ.get("IDEMPOTENCY_TABLE") or f"MediSlot-{env_name()}-Idempotency"


def event_bus():
    return os.environ.get("EVENT_BUS_NAME") or f"medislot-{env_name()}-events"


def utcnow():
    """The single clock of this module (tests replace it to hit exact refund boundaries)."""
    return datetime.now(UTC)


def sleep(seconds):
    time.sleep(seconds)


def parse_utc(value):
    """ISO text ('...Z' or '+00:00'), MySQL DATETIME (naive UTC) or SQLite text -> aware UTC datetime."""
    if value is None:
        return None
    if isinstance(value, datetime):
        return value.replace(tzinfo=UTC) if value.tzinfo is None else value.astimezone(UTC)
    dt = datetime.fromisoformat(str(value).strip().replace("Z", "+00:00"))
    return dt.replace(tzinfo=UTC) if dt.tzinfo is None else dt.astimezone(UTC)


def db_time(dt):
    """Aware datetime -> naive UTC datetime, the form stored in DATETIME(6) columns."""
    return dt.astimezone(UTC).replace(tzinfo=None)


def iso(dt):
    return None if dt is None else parse_utc(dt).isoformat().replace("+00:00", "Z")


def money(value):
    try:
        return Decimal(str(value)).quantize(CENT, rounding=ROUND_HALF_UP)
    except (InvalidOperation, ValueError, TypeError):
        raise ValueError(f"not an amount: {value!r}")


def num(d):
    """Decimal money -> JSON number (int when whole, like the rest of the API)."""
    if d is None:
        return None
    d = Decimal(str(d))
    return int(d) if d == d.to_integral_value() else float(d)


def msg(cid, text, code=None, **extra):
    out = {"message": text, "correlationId": cid, **extra}
    if code:
        out["code"] = code
    return out


def respond(status, data, headers=None):
    res = json_out(status, data)
    if headers:
        res["headers"].update(headers)
    return res


def header(event, name):
    wanted = name.lower()
    for source in (event.get("headers") or {}, {k: (v or [None])[-1] for k, v in (event.get("multiValueHeaders") or {}).items()}):
        for k, v in source.items():
            if str(k).lower() == wanted and v is not None:
                return str(v)
    return None


def backoff(attempt):
    """Exponential backoff with 'equal jitter': half fixed, half random, capped. attempt starts at 1."""
    window = min(BACKOFF_CAP, BACKOFF_BASE * (2 ** (attempt - 1)))
    return window / 2 + random.uniform(0, window / 2)


def metric(name, value=1, unit="Count", **dimensions):
    """CloudWatch metric via Embedded Metric Format: one JSON line on stdout, no API call, no extra IAM.
    Namespace MediSlot, always dimensioned by Environment (plus any extra dimension such as Tier)."""
    dims = {"Environment": env_name(), **{k: str(v) for k, v in dimensions.items()}}
    doc = {"_aws": {"Timestamp": int(time.time() * 1000), "CloudWatchMetrics": [{
        "Namespace": "MediSlot", "Dimensions": [list(dims)], "Metrics": [{"Name": name, "Unit": unit}]}]},
        **dims, name: value}
    print(json.dumps(doc), flush=True)


# =========================================================================== settings (SSM, cached 60 s)
_settings_cache = {"at": 0.0, "value": None}
_settings_lock = threading.Lock()


def _param_names():
    p = f"/medislot/{env_name()}/booking"
    return {"refund_policy": f"{p}/refund-policy", "max_active": f"{p}/max-active-appointments",
            "max_per_doctor_day": f"{p}/max-per-doctor-per-day", "max_retries": f"{p}/max-conflict-retries"}


def load_settings(cid=None):
    """{'max_active', 'max_per_doctor_day', 'max_retries', 'refund_policy_raw'} from SSM. Missing numbers fall
    back to safe defaults (logged); the refund policy is validated separately by refund_policy()."""
    with _settings_lock:
        cached = _settings_cache["value"]
        if cached and time.time() - _settings_cache["at"] < SETTINGS_TTL_SECONDS:
            return cached
    names = _param_names()
    values = {}
    try:
        res = ssm.get_parameters(Names=list(names.values()))
        by_name = {p["Name"]: p["Value"] for p in res.get("Parameters", [])}
        values = {key: by_name.get(name) for key, name in names.items()}
        if res.get("InvalidParameters"):
            emit("warning", "booking_settings_missing", cid, parameters=res["InvalidParameters"])
    except Exception as exc:  # noqa: BLE001 - SSM hiccup: use defaults for limits, policy fails closed later
        emit("warning", "booking_settings_unavailable", cid, error=type(exc).__name__)
        values = {key: None for key in names}
    out = {"refund_policy_raw": values.get("refund_policy")}
    for key, default in DEFAULT_SETTINGS.items():
        raw = values.get(key)
        try:
            n = int(str(raw).strip())
            ok = 1 <= n <= (50 if key == "max_retries" else 100)
        except (TypeError, ValueError):
            ok = False
        if raw is not None and not ok:
            emit("warning", "booking_setting_invalid", cid, setting=key, value=str(raw)[:40])
        out[key] = n if ok else default
    with _settings_lock:
        _settings_cache.update(at=time.time(), value=out)
    return out


def parse_refund_policy(raw):
    """JSON text or dict -> [{'name', 'min_before': timedelta, 'percent': Decimal, 'min_hours': str}] sorted from
    the longest notice to the shortest. Raises PolicyError for anything that could pay the wrong amount."""
    try:
        data = json.loads(raw) if isinstance(raw, str) else raw
        tiers = data["tiers"]
        if not isinstance(tiers, list) or not tiers:
            raise PolicyError("refund policy needs at least one tier")
    except PolicyError:
        raise
    except Exception as exc:  # noqa: BLE001
        raise PolicyError(f"refund policy is not valid JSON with a 'tiers' list ({type(exc).__name__})")
    out, seen_names = [], set()
    for t in tiers:
        if not isinstance(t, dict):
            raise PolicyError("each tier must be an object")
        name = str(t.get("name", "")).strip().upper()
        if not TIER_NAME_RE.fullmatch(name) or name == DOCTOR_TIER or name in seen_names:
            raise PolicyError(f"tier name {name!r} is invalid or repeated")
        seen_names.add(name)
        try:
            hours = Decimal(str(t["minHoursBefore"]))
            percent = Decimal(str(t["refundPercent"]))
        except (KeyError, InvalidOperation):
            raise PolicyError(f"tier {name} needs numeric minHoursBefore and refundPercent")
        if not hours.is_finite() or not percent.is_finite():
            raise PolicyError(f"tier {name} needs finite numbers")
        if hours < 0 or hours > 24 * 365:
            raise PolicyError(f"tier {name}: minHoursBefore must be between 0 and 8760")
        if percent < 0 or percent > 100 or percent != percent.quantize(CENT):
            raise PolicyError(f"tier {name}: refundPercent must be 0..100 with at most 2 decimals")
        micros = hours * 3600 * 1_000_000
        if micros != micros.to_integral_value():
            raise PolicyError(f"tier {name}: minHoursBefore is more precise than a microsecond")
        out.append({"name": name, "min_before": timedelta(microseconds=int(micros)), "percent": percent,
                    "min_hours": str(hours.normalize()) if hours else "0"})
    out.sort(key=lambda t: t["min_before"], reverse=True)
    if len({t["min_before"] for t in out}) != len(out):
        raise PolicyError("two tiers have the same minHoursBefore")
    if out[-1]["min_before"] != timedelta(0):
        raise PolicyError("the last tier must start at minHoursBefore 0 so every cancellation has a tier")
    for longer, shorter in zip(out, out[1:]):
        if shorter["percent"] > longer["percent"]:
            raise PolicyError("cancelling earlier can never refund less than cancelling later")
    return out


def refund_policy(cid=None):
    raw = load_settings(cid).get("refund_policy_raw")
    if raw is None:
        emit("warning", "refund_policy_parameter_missing_using_default", cid)
        raw = DEFAULT_REFUND_POLICY
    return parse_refund_policy(raw)


def policy_snapshot(tiers):
    return {"tiers": [{"name": t["name"], "minHoursBefore": t["min_hours"], "refundPercent": str(t["percent"])} for t in tiers]}


def refund_tier(start_at, now, tiers):
    """The tier for cancelling at `now`. Inclusive lower bound: exactly 24 h before start is still the 24 h tier.
    Raises TooLate at or after the start instant."""
    notice = start_at - now
    if notice <= timedelta(0):
        raise TooLate()
    for tier in tiers:
        if notice >= tier["min_before"]:
            return tier
    raise PolicyError("no tier matches")  # unreachable: parse_refund_policy guarantees a 0-hour tier


def refund_amounts(fee, percent):
    """(refund, deduction) in exact cents. refund + deduction == fee, always."""
    fee = money(fee)
    refund = (fee * Decimal(percent) / Decimal(100)).quantize(CENT, rounding=ROUND_HALF_UP)
    return refund, fee - refund


def cancellation_preview(start_at, fee, now, tiers):
    """What cancelling right now would refund, and until when that tier holds."""
    try:
        tier = refund_tier(start_at, now, tiers)
    except TooLate:
        return None
    refund, deduction = refund_amounts(fee, tier["percent"])
    return {"tier": tier["name"], "refundPercent": num(tier["percent"]), "refundAmount": num(refund),
            "deductionAmount": num(deduction), "validUntil": iso(start_at - tier["min_before"])}


# =========================================================================== DynamoDB (low-level client)
def _av(v):
    if v is None:
        return {"NULL": True}
    if isinstance(v, bool):
        return {"BOOL": v}
    if isinstance(v, (int, Decimal)):
        return {"N": str(v)}
    if isinstance(v, float):
        return {"N": repr(v)}
    if isinstance(v, dict):
        return {"M": {k: _av(x) for k, x in v.items()}}
    if isinstance(v, (list, tuple)):
        return {"L": [_av(x) for x in v]}
    return {"S": str(v)}


def _py(av):
    (kind, v), = av.items()
    if kind == "N":
        d = Decimal(v)
        return int(d) if d == d.to_integral_value() else d
    if kind == "M":
        return {k: _py(x) for k, x in v.items()}
    if kind == "L":
        return [_py(x) for x in v]
    if kind == "NULL":
        return None
    return v


def _item(raw):
    return {k: _py(v) for k, v in (raw or {}).items()}


def _vals(**kw):
    return {":" + k: _av(v) for k, v in kw.items()}


def _code(exc):
    return (getattr(exc, "response", None) or {}).get("Error", {}).get("Code")


def get_slot(slot_id):
    res = ddb.get_item(TableName=slots_table(), Key={"slot_id": {"S": slot_id}}, ConsistentRead=True)
    return _item(res["Item"]) if res.get("Item") else None


def free_seats(slot):
    return max(int(slot.get("capacity", 1) or 1) - int(slot.get("booked_count", 0) or 0) - int(slot.get("held_count", 0) or 0), 0)


def claim_seat(slot, idem_key, attempt_id, now):
    """One optimistic-concurrency attempt: +1 booked seat if the slot is still exactly the version we read.
    Atomic with marking the idempotency record (owned by this attempt) as seat_claimed.
    -> 'ok' | 'conflict' (someone else changed the slot; retry) | 'lost' (our idempotency record was taken over)."""
    version = slot.get("version")
    slot_update = {
        "TableName": slots_table(), "Key": {"slot_id": {"S": slot["slot_id"]}},
        "UpdateExpression": "SET booked_count = :nb, version = :nv, updated_at = :now",
        "ConditionExpression": ("version = :v" if version is not None else "attribute_not_exists(version)") + " AND #st = :open",
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": _vals(nb=int(slot.get("booked_count", 0) or 0) + 1, nv=int(version or 0) + 1,
                                           now=iso(now), open="OPEN", **({"v": int(version)} if version is not None else {})),
    }
    idem_update = {
        "TableName": idempotency_table(), "Key": {"idempotency_key": {"S": idem_key}},
        "UpdateExpression": "SET seat_claimed = :t, claimed_version = :nv",
        "ConditionExpression": "attempt_id = :aid AND #st = :prog",
        "ExpressionAttributeNames": {"#st": "status"},
        "ExpressionAttributeValues": _vals(t=True, nv=int(version or 0) + 1, aid=attempt_id, prog="IN_PROGRESS"),
    }
    try:
        ddb.transact_write_items(TransactItems=[{"Update": slot_update}, {"Update": idem_update}])
        return "ok"
    except Exception as exc:  # noqa: BLE001
        code = _code(exc)
        if code == "TransactionCanceledException":
            reasons = [r.get("Code") for r in (exc.response.get("CancellationReasons") or [])]
            if len(reasons) > 1 and reasons[1] == "ConditionalCheckFailed":
                return "lost"
            return "conflict"   # ConditionalCheckFailed on the slot, or TransactionConflict with a parallel writer
        if code in ("TransactionConflictException", "ProvisionedThroughputExceededException", "ThrottlingException",
                    "RequestLimitExceeded", "TransactionInProgressException"):
            return "conflict"
        raise


def release_seat(slot_id, cid, reason):
    """Give one booked seat back (compensation or cancellation). Atomic decrement; never below zero.
    Returns the slot after the update, or None if it could not be released (logged + metric)."""
    for attempt in range(1, 4):
        try:
            res = ddb.update_item(
                TableName=slots_table(), Key={"slot_id": {"S": slot_id}},
                UpdateExpression="SET booked_count = booked_count - :one, version = version + :one, updated_at = :now",
                ConditionExpression="booked_count > :zero",
                ExpressionAttributeValues=_vals(one=1, zero=0, now=iso(utcnow())), ReturnValues="ALL_NEW")
            emit("info", "seat_released", cid, slotId=slot_id, reason=reason)
            return _item(res.get("Attributes"))
        except Exception as exc:  # noqa: BLE001
            if _code(exc) == "ConditionalCheckFailedException":
                emit("error", "seat_release_inconsistent", cid, slotId=slot_id, reason=reason,
                     detail="booked_count was already 0")
                break
            if attempt == 3:
                emit("error", "seat_release_failed", cid, slotId=slot_id, reason=reason, error=type(exc).__name__)
                break
            sleep(backoff(attempt))
    metric("SeatReleaseFailures", 1)
    return None


def mark_slot_cancelled(slot_id, doctor_id, now):
    """Doctor cancels the slot: status CANCELLED so nobody can book it and the materialiser never reopens it.
    -> 'cancelled' | 'already' | 'started' | 'missing'"""
    try:
        ddb.update_item(
            TableName=slots_table(), Key={"slot_id": {"S": slot_id}},
            UpdateExpression="SET #st = :c, closed_reason = :r, cancelled_at = :now, version = version + :one",
            ConditionExpression="doctor_id = :d AND start_at > :now AND #st <> :c",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=_vals(c="CANCELLED", r="DOCTOR_CANCELLED", now=iso(now), one=1, d=int(doctor_id)))
        return "cancelled"
    except Exception as exc:  # noqa: BLE001
        if _code(exc) != "ConditionalCheckFailedException":
            raise
    slot = get_slot(slot_id)
    if not slot or int(slot.get("doctor_id", 0)) != int(doctor_id):
        return "missing"
    if slot.get("status") == "CANCELLED":
        return "already"
    return "started"


# --------------------------------------------------------------------------- idempotency store
def scoped_key(patient_id, client_key):
    return f"BOOK#{patient_id}#{client_key}"


def request_hash(slot_id, dependent_id=None):
    req = {"slotId": slot_id}
    if dependent_id is not None:
        req["dependentId"] = dependent_id
    return hashlib.sha256(json.dumps(req, sort_keys=True).encode()).hexdigest()


def idem_begin(key, req_hash, patient_id, slot_id):
    """-> (state, record, attempt_id). state: new | replay | mismatch | in_progress | takeover"""
    now = time.time()
    attempt_id = uuid.uuid4().hex
    item = {"idempotency_key": key, "status": "IN_PROGRESS", "request_hash": req_hash, "patient_id": str(patient_id),
            "slot_id": slot_id, "attempt_id": attempt_id, "seat_claimed": False, "created_at": iso(utcnow()),
            "lease_expires_at": int(now) + LEASE_SECONDS, "expires_at": int(now) + IDEMPOTENCY_TTL_SECONDS}
    for _ in range(3):
        try:
            # TTL deletion is lazy (can take hours), so an expired record counts as absent here.
            ddb.put_item(TableName=idempotency_table(), Item={k: _av(v) for k, v in item.items()},
                         ConditionExpression="attribute_not_exists(idempotency_key) OR expires_at < :now",
                         ExpressionAttributeValues=_vals(now=int(now)))
            return "new", item, attempt_id
        except Exception as exc:  # noqa: BLE001
            if _code(exc) != "ConditionalCheckFailedException":
                raise
        res = ddb.get_item(TableName=idempotency_table(), Key={"idempotency_key": {"S": key}}, ConsistentRead=True)
        rec = _item(res.get("Item"))
        if not rec:
            continue  # released between our put and get: try to claim again
        if rec.get("request_hash") != req_hash:
            return "mismatch", rec, None
        if rec.get("status") == "COMPLETED":
            return "replay", rec, None
        if int(rec.get("lease_expires_at", 0)) > now:
            return "in_progress", rec, None
        try:  # the earlier attempt died mid-way: take the record over (and any seat it already claimed)
            ddb.update_item(
                TableName=idempotency_table(), Key={"idempotency_key": {"S": key}},
                UpdateExpression="SET attempt_id = :new, lease_expires_at = :lease",
                ConditionExpression="attempt_id = :old AND #st = :prog",
                ExpressionAttributeNames={"#st": "status"},
                ExpressionAttributeValues=_vals(new=attempt_id, lease=int(now) + LEASE_SECONDS,
                                                old=rec.get("attempt_id", ""), prog="IN_PROGRESS"))
            rec["attempt_id"] = attempt_id
            return "takeover", rec, attempt_id
        except Exception as exc:  # noqa: BLE001
            if _code(exc) != "ConditionalCheckFailedException":
                raise
            return "in_progress", rec, None
    return "in_progress", item, None


def idem_complete(key, attempt_id, status, payload, cid):
    try:
        ddb.update_item(
            TableName=idempotency_table(), Key={"idempotency_key": {"S": key}},
            UpdateExpression="SET #st = :done, response_status = :code, response_body = :body, completed_at = :at"
                             + (", appointment_id = :aid" if payload.get("appointment") else ""),
            ConditionExpression="attempt_id = :me",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=_vals(done="COMPLETED", code=status, body=json.dumps(payload, default=str), at=iso(utcnow()),
                                            me=attempt_id, **({"aid": payload["appointment"]["appointmentId"]}
                                                              if payload.get("appointment") else {})))
    except Exception as exc:  # noqa: BLE001 - the booking itself is done; a replay will find it via RDS
        emit("warning", "idempotency_complete_failed", cid, error=type(exc).__name__)


def idem_release(key, attempt_id, cid):
    """Forget a key whose request did not reach a final answer, so the client can retry with the same key."""
    try:
        ddb.delete_item(TableName=idempotency_table(), Key={"idempotency_key": {"S": key}},
                        ConditionExpression="attempt_id = :me", ExpressionAttributeValues=_vals(me=attempt_id))
    except Exception as exc:  # noqa: BLE001
        emit("warning", "idempotency_release_failed", cid, error=type(exc).__name__)


# =========================================================================== events
def publish(entries, cid):
    """entries: [(detail_type, detail)]. Best effort with retries, after the database commit. A failure is
    logged and counted (EventPublishFailures) but never undoes a committed booking or cancellation."""
    if not entries:
        return True
    batch = [{"Source": "medislot.booking", "DetailType": t, "EventBusName": event_bus(),
              "Detail": json.dumps({"eventId": uuid.uuid4().hex, "eventVersion": 1, "occurredAt": iso(utcnow()),
                                    "correlationId": cid, **d}, default=str)} for t, d in entries]
    ok = True
    for start in range(0, len(batch), 10):
        pending = batch[start:start + 10]
        for attempt in range(1, 4):
            try:
                res = events.put_events(Entries=pending)
                failed = [e for e, r in zip(pending, res.get("Entries", [])) if r.get("ErrorCode")]
                if not res.get("FailedEntryCount"):
                    pending = []
                    break
                pending = failed or pending
            except Exception as exc:  # noqa: BLE001
                emit("warning", "event_publish_retry", cid, attempt=attempt, error=type(exc).__name__)
            if attempt < 3:
                sleep(backoff(attempt))
        if pending:
            ok = False
            emit("error", "event_publish_failed", cid, detailTypes=[e["DetailType"] for e in pending])
            metric("EventPublishFailures", len(pending))
    if ok:
        emit("info", "events_published", cid, detailTypes=[t for t, _ in entries])
    return ok


def booked_event(a):
    return ("AppointmentBooked", {**{k: a[k] for k in ("appointmentId", "patientId", "doctorId", "slotId", "consultationTypeId",
                                                        "startAt", "endAt", "fee", "status")},
                                  "dependentId": a["bookedFor"].get("dependentId")})


def cancelled_event(a, refund, cancelled_by):
    return ("AppointmentCancelled", {**{k: a[k] for k in ("appointmentId", "patientId", "doctorId", "slotId", "startAt", "endAt")},
                                     "dependentId": a["bookedFor"].get("dependentId"),
                                     "status": a["status"], "cancelledBy": cancelled_by, "refundTier": refund["tier"],
                                     "refundPercent": refund["refundPercent"], "refundAmount": refund["refundAmount"]})


# =========================================================================== SQL
APPT_SELECT = """SELECT a.appointment_id, a.patient_id, a.doctor_id, a.consultation_type_id, a.slot_id, a.start_at, a.end_at,
       a.fee_snapshot, a.status, a.reschedule_count, a.cancelled_at, a.cancellation_note, a.created_at, a.dependent_id,
       pd.first_name AS dependent_first, pd.last_name AS dependent_last, pd.relation AS dependent_relation,
       d.first_name AS doctor_first, d.last_name AS doctor_last, d.timezone AS doctor_timezone,
       h.hospital_name, h.city, ct.name AS type_name, ct.duration_minutes,
       p.first_name AS patient_first, p.last_name AS patient_last,
       r.refund_id, r.fee_amount, r.refund_amount, r.deduction_amount, r.refund_percentage, r.refund_tier,
       r.cancellation_reason, r.calculated_at
  FROM appointments a
  JOIN doctors d ON d.doctor_id = a.doctor_id
  JOIN hospitals h ON h.hospital_id = d.hospital_id
  JOIN consultation_types ct ON ct.consultation_type_id = a.consultation_type_id
  JOIN patients p ON p.patient_id = a.patient_id
  LEFT JOIN patient_dependents pd ON pd.dependent_id = a.dependent_id
  LEFT JOIN refund_ledger r ON r.appointment_id = a.appointment_id"""


def zone_of(name):
    try:
        return ZoneInfo(name or "Asia/Kolkata")
    except Exception:  # noqa: BLE001
        return ZoneInfo("Asia/Kolkata")


def refund_json(r):
    if r.get("refund_id") is None:
        return None
    return {"refundId": int(r["refund_id"]), "feeAmount": num(r["fee_amount"]), "refundAmount": num(r["refund_amount"]),
            "deductionAmount": num(r["deduction_amount"]), "refundPercent": num(r["refund_percentage"]),
            "tier": r["refund_tier"], "reason": r["cancellation_reason"], "calculatedAt": iso(r["calculated_at"])}


def booked_for(r):
    if r.get("dependent_id") is None:
        return {"type": "SELF"}
    return {"type": "DEPENDENT", "dependentId": int(r["dependent_id"]), "relation": r.get("dependent_relation"),
            "name": f"{r.get('dependent_first', '')} {r.get('dependent_last', '')}".strip()}


def appointment_json(r, role, now=None, tiers=None):
    start, end = parse_utc(r["start_at"]), parse_utc(r["end_at"])
    zone = zone_of(r.get("doctor_timezone"))
    local = start.astimezone(zone)
    out = {
        "appointmentId": int(r["appointment_id"]), "status": r["status"],
        "patientId": int(r["patient_id"]), "doctorId": int(r["doctor_id"]), "slotId": r["slot_id"],
        "consultationTypeId": int(r["consultation_type_id"]),
        "consultationType": {"id": int(r["consultation_type_id"]), "name": r.get("type_name"),
                             "durationMinutes": r.get("duration_minutes")},
        "startAt": iso(start), "endAt": iso(end), "timezone": str(zone),
        "localDate": local.date().isoformat(), "localStart": local.strftime("%H:%M"),
        "localEnd": end.astimezone(zone).strftime("%H:%M"),
        "fee": num(money(r["fee_snapshot"])), "rescheduleCount": int(r.get("reschedule_count") or 0),
        "doctor": {"id": int(r["doctor_id"]), "displayName": f"Dr. {r.get('doctor_first', '')} {r.get('doctor_last', '')}".strip(),
                   "hospital": r.get("hospital_name"), "city": r.get("city")},
        "createdAt": iso(r.get("created_at")), "cancelledAt": iso(r.get("cancelled_at")),
        "refund": refund_json(r), "bookedFor": booked_for(r),
    }
    if role == "DOCTOR":   # the doctor sees who is coming - name only, never contact details
        holder = f"{r.get('patient_first', '')} {r.get('patient_last', '')}".strip()
        out["patient"] = {"id": int(r["patient_id"]), "name": out["bookedFor"].get("name") or holder}
        if r.get("dependent_id") is not None:
            out["patient"].update(bookedBy=holder, relation=r.get("dependent_relation"))
        if r.get("cancellation_note"):
            out["cancellationNote"] = r["cancellation_note"]
    if role == "PATIENT" and r["status"] == "CONFIRMED" and now is not None:
        out["cancellable"] = start > now
        if tiers and start > now:
            out["cancellationPreview"] = cancellation_preview(start, r["fee_snapshot"], now, tiers)
    return out


def fetch_appointment(cur, appointment_id, lock=False):
    """The appointment with doctor / type / refund details. lock=True first takes the row lock (FOR UPDATE), so the
    read that follows sees the latest committed state (a parallel cancel waits here and then sees CANCELLED)."""
    if lock:
        cur.execute("SELECT appointment_id FROM appointments WHERE appointment_id = %s FOR UPDATE", (appointment_id,))
        if not cur.fetchone():
            return None
    cur.execute(APPT_SELECT + " WHERE a.appointment_id = %s", (appointment_id,))
    return cur.fetchone()


def check_limits(cur, uid, dependent_id, doctor_id, slot_id, start, end, zone, settings, now, who=None):
    """Patient booking rules. -> None, or (code, message). Uses only CONFIRMED appointments.
    PER PERSON (account holder = dependent 0): same slot twice, overlapping times, per doctor per day.
    PER ACCOUNT: the cap on upcoming appointments."""
    person = int(dependent_id or 0)
    subject = "You" if not who else who
    cur.execute("""SELECT appointment_id FROM appointments WHERE patient_id = %s AND COALESCE(dependent_id, 0) = %s
                   AND slot_id = %s AND status = 'CONFIRMED' LIMIT 1""", (uid, person, slot_id))
    if cur.fetchone():
        return "DUPLICATE_BOOKING", "You have already booked this slot" if not who else f"{who} is already booked in this slot"
    cur.execute("""SELECT appointment_id FROM appointments
                   WHERE patient_id = %s AND COALESCE(dependent_id, 0) = %s AND status = 'CONFIRMED'
                   AND start_at < %s AND end_at > %s LIMIT 1""",
                (uid, person, db_time(end), db_time(start)))
    if cur.fetchone():
        return "OVERLAPPING_APPOINTMENT", f"{subject} already {'have' if not who else 'has'} another appointment at that time"
    local_day = start.astimezone(zone).date()
    day_start = datetime.combine(local_day, datetime.min.time(), tzinfo=zone).astimezone(UTC)
    day_end = datetime.combine(local_day + timedelta(days=1), datetime.min.time(), tzinfo=zone).astimezone(UTC)
    cur.execute("""SELECT COUNT(*) AS n FROM appointments
                   WHERE patient_id = %s AND doctor_id = %s AND status = 'CONFIRMED' AND COALESCE(dependent_id, 0) = %s
                   AND start_at >= %s AND start_at < %s""",
                (uid, doctor_id, person, db_time(day_start), db_time(day_end)))
    if int(cur.fetchone()["n"]) >= settings["max_per_doctor_day"]:
        n = settings["max_per_doctor_day"]
        return "DOCTOR_DAILY_LIMIT_REACHED", (f"{'You' if not who else who} can have at most {n} appointment{'s' if n != 1 else ''} "
                                              "with the same doctor on one day")
    cur.execute("SELECT COUNT(*) AS n FROM appointments WHERE patient_id = %s AND status = 'CONFIRMED' AND start_at > %s",
                (uid, db_time(now)))
    if int(cur.fetchone()["n"]) >= settings["max_active"]:
        n = settings["max_active"]
        return "ACTIVE_LIMIT_REACHED", (f"Your account can hold at most {n} upcoming appointment{'s' if n != 1 else ''} "
                                        "(including family members). Cancel one to book another")
    return None


def active_dependent(cur, uid, dependent_id):
    cur.execute("""SELECT dependent_id, first_name, last_name, relation FROM patient_dependents
                   WHERE dependent_id = %s AND patient_id = %s AND is_active = 1""", (dependent_id, uid))
    return cur.fetchone()


def insert_ledger(cur, appointment_id, fee, refund, deduction, percent, tier, reason, tiers, now):
    cur.execute(
        """INSERT INTO refund_ledger (appointment_id, fee_amount, refund_amount, deduction_amount, refund_percentage,
                                      refund_tier, cancellation_reason, refund_policy, calculated_at)
           VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)""",
        (appointment_id, money(fee), refund, deduction, Decimal(percent).quantize(CENT), tier, reason,
         json.dumps(policy_snapshot(tiers)) if tiers else None, db_time(now)))


def cancel_confirmed_by_doctor(cur, slot_id, doctor_id, now, note, only_appointment_id=None):
    """Every CONFIRMED appointment in the slot -> CANCELLED_BY_DOCTOR with a 100 % refund. Idempotent: rows that
    are no longer CONFIRMED are skipped. Caller commits. Returns the cancelled appointment ids."""
    sql = "SELECT appointment_id, fee_snapshot FROM appointments WHERE slot_id = %s AND doctor_id = %s AND status = 'CONFIRMED'"
    args = [slot_id, doctor_id]
    if only_appointment_id is not None:
        sql += " AND appointment_id = %s"
        args.append(only_appointment_id)
    cur.execute(sql + " ORDER BY appointment_id FOR UPDATE", args)
    done = []
    for row in cur.fetchall():
        cur.execute("""UPDATE appointments SET status = 'CANCELLED_BY_DOCTOR', cancelled_at = %s, cancellation_note = %s,
                              updated_at = %s WHERE appointment_id = %s AND status = 'CONFIRMED'""",
                    (db_time(now), note, db_time(now), row["appointment_id"]))
        if cur.rowcount != 1:
            continue
        fee = money(row["fee_snapshot"])
        insert_ledger(cur, row["appointment_id"], fee, fee, Decimal("0.00"), 100, DOCTOR_TIER, "DOCTOR_CANCEL", None, now)
        done.append(int(row["appointment_id"]))
    return done


def load_appointments(cur, ids):
    if not ids:
        return []
    cur.execute(APPT_SELECT + f" WHERE a.appointment_id IN ({', '.join(['%s'] * len(ids))}) ORDER BY a.appointment_id", list(ids))
    return cur.fetchall()


def is_dup(exc, needle):
    return bool(exc.args) and exc.args[0] == 1062 and needle in str(exc.args[-1])


# =========================================================================== booking
def book(event, uid, cid):
    client_key = (header(event, "Idempotency-Key") or "").strip()
    if not client_key:
        return respond(400, msg(cid, "The Idempotency-Key header is required for bookings", "IDEMPOTENCY_KEY_REQUIRED"))
    if not IDEMPOTENCY_KEY_RE.fullmatch(client_key):
        return respond(400, msg(cid, "Idempotency-Key must be 8-128 characters: letters, digits, . _ : -", "IDEMPOTENCY_KEY_INVALID"))
    data = body(event)
    if not isinstance(data, dict):
        return respond(400, msg(cid, "Invalid JSON body", "INVALID_BODY"))
    extra = sorted(k for k in data if k not in ("slotId", "dependentId"))
    if extra:
        return respond(422, msg(cid, "Only slotId and dependentId can be sent when booking (not: " + ", ".join(extra)[:120] + ")",
                                "UNKNOWN_FIELDS"))
    slot_id = data.get("slotId")
    if not isinstance(slot_id, str) or not SLOT_ID_RE.fullmatch(slot_id):
        return respond(422, msg(cid, "slotId is required (as returned by GET /doctors/{doctorId}/slots)", "SLOT_ID_INVALID"))
    dependent_id = data.get("dependentId")
    if dependent_id is not None and (isinstance(dependent_id, bool) or not isinstance(dependent_id, int) or not 1 <= dependent_id < 10 ** 18):
        return respond(422, msg(cid, "dependentId must be the id of one of your family members, or null to book for yourself",
                                "DEPENDENT_ID_INVALID"))

    key = scoped_key(uid, client_key)
    state, rec, attempt_id = idem_begin(key, request_hash(slot_id, dependent_id), uid, slot_id)
    if state == "replay":
        emit("info", "idempotent_replay", cid, patientId=uid, slotId=slot_id)
        metric("IdempotentReplays", 1)
        stored = json.loads(rec.get("response_body") or "{}")
        stored["correlationId"] = cid
        return respond(int(rec.get("response_status", 200)), stored, {"Idempotent-Replayed": "true"})
    if state == "mismatch":
        return respond(422, msg(cid, "This Idempotency-Key was already used for a different booking request", "IDEMPOTENCY_KEY_REUSED"))
    if state == "in_progress":
        return respond(409, msg(cid, "A booking with this Idempotency-Key is still being processed. Retry shortly.",
                                "REQUEST_IN_PROGRESS"), {"Retry-After": "2"})
    try:
        outbox = []
        status, payload, final = do_book(uid, slot_id, dependent_id, key, attempt_id,
                                         state == "takeover" and bool(rec.get("seat_claimed")), cid, outbox)
    except Exception:
        idem_release(key, attempt_id, cid)
        raise
    if final:
        idem_complete(key, attempt_id, status, payload, cid)
    else:
        idem_release(key, attempt_id, cid)
    publish(outbox, cid)     # after the answer is safely stored: a slow EventBridge cannot break a replay
    headers = {"Retry-After": "1"} if payload.get("code") in ("SLOT_BUSY", "REQUEST_IN_PROGRESS") else None
    return respond(status, payload, headers)


def do_book(uid, slot_id, dependent_id, key, attempt_id, resumed_seat, cid, outbox):
    """-> (http status, body, final). final=True answers are stored for replay; others free the key for a retry.
    Events to publish are appended to `outbox` (published by the caller after the answer is stored)."""
    def reject(status, code, text, final=True, **extra):
        emit("info", "booking_rejected", cid, patientId=uid, slotId=slot_id, code=code)
        metric("BookingsRejected", 1, Reason=code)
        return status, msg(cid, text, code, **extra), final

    now = utcnow()
    settings = load_settings(cid)
    tiers = safe_tiers(cid)     # only for the "cancel before ... for a full refund" preview in the answer
    cfg = db_config()

    # -- the seat may already be ours (an earlier attempt with this key crashed after claiming it) ------------
    with dbconn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT appointment_id, slot_id, dependent_id FROM appointments WHERE idempotency_key = %s", (key,))
            existing = cur.fetchone()
            if existing:   # the booking was committed by an earlier attempt: answer with it
                if existing["slot_id"] != slot_id or existing["dependent_id"] != dependent_id:
                    return 422, msg(cid, "This Idempotency-Key was already used for a different booking request", "IDEMPOTENCY_KEY_REUSED"), True
                appt = appointment_json(fetch_appointment(cur, existing["appointment_id"]), "PATIENT", now, tiers)
                return 201, {"message": "Appointment confirmed", "appointment": appt, "correlationId": cid}, True
        conn.rollback()

    slot = get_slot(slot_id)
    if not slot:
        if resumed_seat:
            emit("error", "claimed_slot_vanished", cid, slotId=slot_id)
        return reject(404, "SLOT_NOT_FOUND", "That slot does not exist")
    start, end = parse_utc(slot["start_at"]), parse_utc(slot["end_at"])
    doctor_id, type_id = int(slot["doctor_id"]), int(slot["consultation_type_id"])

    if resumed_seat and start <= now:
        release_seat(slot_id, cid, "resumed_after_start")
        return reject(422, "SLOT_IN_PAST", "That slot has already started or is in the past")
    if not resumed_seat:
        if start <= now:
            return reject(422, "SLOT_IN_PAST", "That slot has already started or is in the past")
        if slot.get("status") != "OPEN":
            return reject(409, "SLOT_CLOSED", "That slot is no longer open for booking")

    # -- read-only checks (a short transaction, no locks held while we race for the seat) -------------------
    with dbconn(cfg) as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT u.is_active, p.patient_id FROM users u LEFT JOIN patients p ON p.patient_id = u.user_id
                           WHERE u.user_id = %s AND u.role = 'PATIENT'""", (uid,))
            me = cur.fetchone()
            cur.execute("""SELECT dct.fee, d.timezone FROM doctor_consultation_types dct
                           JOIN consultation_types ct ON ct.consultation_type_id = dct.consultation_type_id AND ct.is_active = 1
                           JOIN doctors d ON d.doctor_id = dct.doctor_id
                           JOIN users du ON du.user_id = d.doctor_id AND du.is_active = 1
                           WHERE dct.doctor_id = %s AND dct.consultation_type_id = %s AND dct.is_active = 1""", (doctor_id, type_id))
            offer = cur.fetchone()
            dependent = active_dependent(cur, uid, dependent_id) if dependent_id is not None else None
            who = f"{dependent['first_name']} {dependent['last_name']}" if dependent else None
            problem = None
            if me and me["is_active"] and me["patient_id"] is not None and offer and (dependent_id is None or dependent):
                problem = check_limits(cur, uid, dependent_id, doctor_id, slot_id, start, end, zone_of(offer["timezone"]),
                                       settings, now, who)
        conn.rollback()
    if not me or not me["is_active"]:
        return reject(403, "ACCOUNT_INACTIVE", "This account cannot book appointments", final=False)
    if me["patient_id"] is None:
        return reject(409, "PROFILE_REQUIRED", "Complete your patient profile before booking", final=False)
    if dependent_id is not None and not dependent:
        if resumed_seat:
            release_seat(slot_id, cid, "dependent_removed")
        return reject(404, "DEPENDENT_NOT_FOUND", "That family member is not on your account")
    if not offer:
        return reject(409, "SLOT_UNAVAILABLE", "This doctor is not taking bookings for that consultation type")
    if problem and not resumed_seat:
        return reject(409, *problem)
    fee, zone = money(offer["fee"]), zone_of(offer["timezone"])

    # -- the race: optimistic concurrency on the slot item ---------------------------------------------------
    retries = 0
    claimed_here = False
    if not resumed_seat:
        for attempt in range(settings["max_retries"] + 1):
            if attempt:
                slot = get_slot(slot_id)
                if not slot or slot.get("status") != "OPEN":
                    metric("BookingConflictRetries", retries)
                    return reject(409, "SLOT_CLOSED", "That slot is no longer open for booking")
            if parse_utc(slot["start_at"]) <= utcnow():
                metric("BookingConflictRetries", retries)
                return reject(422, "SLOT_IN_PAST", "That slot has already started or is in the past")
            if free_seats(slot) <= 0:
                metric("BookingConflictRetries", retries)
                return reject(409, "SLOT_FULL", "Sorry, this slot is fully booked", capacity=int(slot.get("capacity", 1)))
            outcome = claim_seat(slot, key, attempt_id, utcnow())
            if outcome == "ok":
                claimed_here = True
                break
            if outcome == "lost":
                metric("BookingConflictRetries", retries)
                return reject(409, "REQUEST_IN_PROGRESS", "A booking with this Idempotency-Key is still being processed. Retry shortly.", final=False)
            retries += 1
            emit("info", "booking_conflict_retry", cid, slotId=slot_id, attempt=retries, readVersion=slot.get("version"))
            if attempt < settings["max_retries"]:
                sleep(backoff(retries))
        metric("BookingConflictRetries", retries)
        if not claimed_here:
            return reject(409, "SLOT_BUSY", "This slot is in high demand right now. Please try again.", final=False)

    # -- system of record: RDS transaction, serialised per patient ---------------------------------------
    # The seat is given back on any failure BEFORE commit. Once commit() has been called the outcome may be
    # "committed", so the seat is never released after that point (a leaked seat is safe; a double booking is not).
    commit_attempted = False
    try:
        with dbconn(cfg) as conn:
            with conn.cursor() as cur:
                try:
                    # Lock MY users row: parallel bookings by the same patient queue here, so the limit re-check
                    # below always sees the other one's committed row. Other patients are not blocked.
                    cur.execute("SELECT user_id FROM users WHERE user_id = %s FOR UPDATE", (uid,))
                    # The family member may have been removed while we raced (removal takes the same lock).
                    if dependent_id is not None and not active_dependent(cur, uid, dependent_id):
                        conn.rollback()
                        release_seat(slot_id, cid, "dependent_removed")
                        return reject(404, "DEPENDENT_NOT_FOUND", "That family member is not on your account")
                    problem = check_limits(cur, uid, dependent_id, doctor_id, slot_id, start, end, zone, settings, now, who)
                    if problem:
                        conn.rollback()
                        release_seat(slot_id, cid, "limit_recheck")
                        return reject(409, *problem)
                    cur.execute(
                        """INSERT INTO appointments (patient_id, doctor_id, consultation_type_id, slot_id, start_at, end_at,
                                                     fee_snapshot, status, dependent_id, idempotency_key, created_at, updated_at)
                           VALUES (%s, %s, %s, %s, %s, %s, %s, 'CONFIRMED', %s, %s, %s, %s)""",
                        (uid, doctor_id, type_id, slot_id, db_time(start), db_time(end), fee, dependent_id, key,
                         db_time(now), db_time(now)))
                    appointment_id = int(cur.lastrowid)
                    commit_attempted = True
                    conn.commit()
                except pymysql.err.IntegrityError as exc:
                    conn.rollback()
                    if is_dup(exc, "idempotency_key"):   # an earlier attempt with this key already committed
                        if claimed_here:                 # ... with its own seat, so the one we just took goes back
                            release_seat(slot_id, cid, "duplicate_key")
                        cur.execute("SELECT appointment_id FROM appointments WHERE idempotency_key = %s", (key,))
                        row = cur.fetchone()
                        appt = appointment_json(fetch_appointment(cur, row["appointment_id"]), "PATIENT", now, tiers)
                        return 201, {"message": "Appointment confirmed", "appointment": appt, "correlationId": cid}, True
                    release_seat(slot_id, cid, "insert_rejected")
                    if is_dup(exc, "active_patient_slot"):
                        return reject(409, "DUPLICATE_BOOKING", "You have already booked this slot" if not who
                                      else f"{who} is already booked in this slot")
                    raise
                appt = appointment_json(fetch_appointment(cur, appointment_id), "PATIENT", now, tiers)
    except pymysql.err.IntegrityError:
        raise
    except Exception:
        if (claimed_here or resumed_seat) and not commit_attempted:
            release_seat(slot_id, cid, "rds_failure")
        raise

    # -- did the doctor cancel the slot while we were committing? -----------------------------------------
    try:
        after = get_slot(slot_id)
    except Exception as exc:  # noqa: BLE001
        after = None
        emit("warning", "post_commit_slot_check_failed", cid, slotId=slot_id, error=type(exc).__name__)
    if after and after.get("status") == "CANCELLED":
        with dbconn(cfg) as conn:
            with conn.cursor() as cur:
                done = cancel_confirmed_by_doctor(cur, slot_id, doctor_id, utcnow(), "Slot cancelled by the doctor", appointment_id)
                conn.commit()
                row = fetch_appointment(cur, appointment_id)
        appt = appointment_json(row, "PATIENT", utcnow())
        if done:
            outbox.append(cancelled_event(appt, appt["refund"], "DOCTOR"))
            metric("CancellationsByTier", 1, Tier=DOCTOR_TIER)
        return reject(409, "SLOT_CANCELLED", "The doctor cancelled this slot while you were booking. You have been refunded in full.",
                      appointment=appt)

    outbox.append(booked_event(appt))
    metric("BookingsConfirmed", 1)
    emit("info", "booking_confirmed", cid, appointmentId=appointment_id, patientId=uid, dependentId=dependent_id,
         doctorId=doctor_id, slotId=slot_id,
         conflictRetries=retries, resumed=resumed_seat)
    return 201, {"message": "Appointment confirmed", "appointment": appt, "correlationId": cid}, True


# =========================================================================== cancellation
def cancel_by_patient(appointment_id, uid, cid):
    try:
        tiers = refund_policy(cid)
    except PolicyError as exc:
        emit("error", "refund_policy_invalid", cid, detail=str(exc)[:200])
        return respond(503, msg(cid, "Cancellations are temporarily unavailable. Please try again later.", "REFUND_POLICY_UNAVAILABLE"))
    now = utcnow()
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            row = fetch_appointment(cur, appointment_id, lock=True)
            if not row or int(row["patient_id"]) != uid:
                conn.rollback()
                if row:
                    emit("warning", "appointment_isolation_violation", cid, callerId=uid, appointmentId=appointment_id)
                return respond(404, msg(cid, "Appointment not found", "APPOINTMENT_NOT_FOUND"))
            if row["status"] in CANCELLED:
                conn.rollback()
                row = fetch_appointment(cur, appointment_id)
                return respond(200, {"message": "This appointment was already cancelled", "alreadyCancelled": True,
                                     "appointment": appointment_json(row, "PATIENT", now), "refund": refund_json(row), "correlationId": cid})
            if row["status"] != "CONFIRMED":
                conn.rollback()
                return respond(409, msg(cid, f"A {row['status'].lower().replace('_', ' ')} appointment cannot be cancelled", "NOT_CANCELLABLE"))
            start = parse_utc(row["start_at"])
            try:
                tier = refund_tier(start, now, tiers)
            except TooLate:
                conn.rollback()
                return respond(409, msg(cid, "This appointment has already started and can no longer be cancelled", "ALREADY_STARTED"))
            refund, deduction = refund_amounts(row["fee_snapshot"], tier["percent"])
            try:
                cur.execute("""UPDATE appointments SET status = 'CANCELLED_BY_PATIENT', cancelled_at = %s, updated_at = %s
                               WHERE appointment_id = %s AND status = 'CONFIRMED'""", (db_time(now), db_time(now), appointment_id))
                if cur.rowcount != 1:
                    raise pymysql.err.IntegrityError(1062, "appointment changed concurrently (refund_ledger.appointment_id)")
                insert_ledger(cur, appointment_id, row["fee_snapshot"], refund, deduction, tier["percent"], tier["name"],
                              "PATIENT_CANCEL", tiers, now)
                conn.commit()
            except pymysql.err.IntegrityError:
                conn.rollback()      # a parallel cancel won; answer with its result
                row = fetch_appointment(cur, appointment_id)
                if not row or row["status"] not in CANCELLED:
                    raise
                return respond(200, {"message": "This appointment was already cancelled", "alreadyCancelled": True,
                                     "appointment": appointment_json(row, "PATIENT", now), "refund": refund_json(row), "correlationId": cid})
            row = fetch_appointment(cur, appointment_id)
    appt = appointment_json(row, "PATIENT", now)
    ref = appt["refund"]
    metric("CancellationsByTier", 1, Tier=tier["name"])
    emit("info", "appointment_cancelled_by_patient", cid, appointmentId=appointment_id, tier=tier["name"],
         refundPercent=str(tier["percent"]), noticeSeconds=int((start - now).total_seconds()))

    after = release_seat(row["slot_id"], cid, "patient_cancel")
    entries = [cancelled_event(appt, ref, "PATIENT")]
    if after and after.get("status") == "OPEN" and start > utcnow():
        entries.append(("SlotSeatFreed", {"slotId": row["slot_id"], "doctorId": int(row["doctor_id"]),
                                          "consultationTypeId": int(row["consultation_type_id"]), "startAt": iso(start),
                                          "localDate": after.get("local_date") or appt["localDate"],
                                          "freeSeats": free_seats(after), "capacity": int(after.get("capacity", 1))}))
    publish(entries, cid)
    return respond(200, {"message": "Appointment cancelled", "appointment": appt, "refund": ref, "correlationId": cid})


def cancel_slot_by_doctor(event, raw_slot, uid, cid):
    slot_id = unquote(raw_slot or "")
    if SLOT_HEX_RE.fullmatch(slot_id):
        slot_id = "SLOT#" + slot_id
    if not SLOT_ID_RE.fullmatch(slot_id):
        return respond(422, msg(cid, "slotId is not valid", "SLOT_ID_INVALID"))
    data = body(event)
    if data is None or not isinstance(data, dict):
        return respond(400, msg(cid, "Invalid JSON body", "INVALID_BODY"))
    note = " ".join(str(data.get("reason") or "").split())[:255] or "Slot cancelled by the doctor"
    now = utcnow()
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT doctor_id FROM doctors WHERE doctor_id = %s", (uid,))
            if not cur.fetchone():
                return respond(404, msg(cid, "Doctor profile not found; create it first", "DOCTOR_PROFILE_REQUIRED"))
        conn.rollback()
    slot = get_slot(slot_id)
    if not slot or int(slot.get("doctor_id", 0)) != uid:
        if slot:
            emit("warning", "slot_isolation_violation", cid, callerId=uid, slotId=slot_id)
        return respond(404, msg(cid, "Slot not found", "SLOT_NOT_FOUND"))
    if parse_utc(slot["start_at"]) <= now and slot.get("status") != "CANCELLED":
        return respond(409, msg(cid, "This slot has already started and cannot be cancelled", "SLOT_STARTED"))
    state = "already" if slot.get("status") == "CANCELLED" else mark_slot_cancelled(slot_id, uid, now)
    if state == "missing":
        return respond(404, msg(cid, "Slot not found", "SLOT_NOT_FOUND"))
    if state == "started":
        return respond(409, msg(cid, "This slot has already started and cannot be cancelled", "SLOT_STARTED"))

    # Sweep (also on a repeated call, so a retry finishes a sweep that failed half-way).
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            try:
                done = cancel_confirmed_by_doctor(cur, slot_id, uid, now, note)
                conn.commit()
            except Exception:
                conn.rollback()
                raise
            rows = load_appointments(cur, done)
    appts = [appointment_json(r, "DOCTOR", now) for r in rows]
    entries = [cancelled_event(a, a["refund"], "DOCTOR") for a in appts]
    if state == "cancelled" or appts:   # a repeated call that changed nothing publishes nothing
        entries.append(("SlotCancelled", {"slotId": slot_id, "doctorId": uid, "startAt": iso(slot["start_at"]),
                                          "cancelledAppointments": len(appts)}))
    publish(entries, cid)
    if appts:
        metric("CancellationsByTier", len(appts), Tier=DOCTOR_TIER)
    total = sum((money(a["refund"]["refundAmount"]) for a in appts), Decimal("0.00"))
    emit("info", "slot_cancelled_by_doctor", cid, doctorId=uid, slotId=slot_id, cancelledAppointments=len(appts),
         alreadyCancelled=state == "already")
    return respond(200, {
        "message": "Slot cancelled" + (f"; {len(appts)} patient{'s' if len(appts) != 1 else ''} refunded in full" if appts else ""),
        "slotId": slot_id, "status": "CANCELLED", "alreadyCancelled": state == "already",
        "cancelledAppointments": len(appts), "totalRefunded": num(total),
        "refunds": [{"appointmentId": a["appointmentId"], "patientId": a["patientId"], "refundAmount": a["refund"]["refundAmount"]}
                    for a in appts],
        "correlationId": cid})


# =========================================================================== reads
def query_params(event):
    return {k: (v if isinstance(v, str) else "") for k, v in (event.get("queryStringParameters") or {}).items()}


def int_param(q, name, default, lo, hi):
    raw = q.get(name, "").strip()
    if raw == "":
        return default
    if not re.fullmatch(r"[0-9]{1,6}", raw) or not lo <= int(raw) <= hi:
        raise ValueError(f"{name} must be a whole number between {lo} and {hi}")
    return int(raw)


def safe_tiers(cid):
    try:
        return refund_policy(cid)
    except PolicyError:
        return None


def list_appointments(event, uid, role, cid):
    q = query_params(event)
    try:
        page = int_param(q, "page", 1, 1, 10000)
        size = int_param(q, "pageSize", DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    except ValueError as exc:
        return respond(422, msg(cid, str(exc)))
    status = q.get("status", "").strip().upper()
    when = q.get("when", "").strip().lower() or "all"
    if status and status not in STATUS_FILTERS:
        return respond(422, msg(cid, "status must be one of: " + ", ".join(STATUS_FILTERS)))
    if when not in ("upcoming", "past", "all"):
        return respond(422, msg(cid, "when must be upcoming, past or all"))
    booked_for_q = q.get("bookedFor", "").strip().lower()
    if booked_for_q and (role != "PATIENT" or not (booked_for_q == "self" or re.fullmatch(r"[1-9][0-9]{0,17}", booked_for_q))):
        return respond(422, msg(cid, "bookedFor must be 'self' or one of your family member ids"))
    now = utcnow()
    owner = "a.patient_id" if role == "PATIENT" else "a.doctor_id"
    where, args = [f"{owner} = %s"], [uid]
    if booked_for_q:
        where.append("COALESCE(a.dependent_id, 0) = %s")
        args.append(0 if booked_for_q == "self" else int(booked_for_q))
    if status:
        where.append(f"a.status IN ({', '.join(['%s'] * len(STATUS_FILTERS[status]))})")
        args += list(STATUS_FILTERS[status])
    if when == "upcoming":
        where.append("a.start_at > %s")
        args.append(db_time(now))
    elif when == "past":
        where.append("a.start_at <= %s")
        args.append(db_time(now))
    order = "a.start_at ASC, a.appointment_id ASC" if when == "upcoming" else "a.start_at DESC, a.appointment_id DESC"
    clause = " WHERE " + " AND ".join(where)
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT COUNT(*) AS n FROM appointments a" + clause, args)
            total = int(cur.fetchone()["n"])
            rows = []
            if total > (page - 1) * size:
                cur.execute(APPT_SELECT + clause + f" ORDER BY {order} LIMIT %s OFFSET %s", args + [size, (page - 1) * size])
                rows = cur.fetchall()
    tiers = safe_tiers(cid) if role == "PATIENT" else None
    pages = max((total + size - 1) // size, 1)
    emit("info", "appointments_listed", cid, role=role, userId=uid, when=when, status=status or None, total=total)
    return respond(200, {"items": [appointment_json(r, role, now, tiers) for r in rows], "page": page, "pageSize": size,
                         "total": total, "totalPages": pages, "hasPrevious": page > 1 and total > 0, "hasNext": page < pages,
                         "correlationId": cid})


def get_appointment(appointment_id, uid, role, cid):
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            row = fetch_appointment(cur, appointment_id)
    owner = int(row["patient_id"] if role == "PATIENT" else row["doctor_id"]) if row else None
    if owner != uid:
        if row:   # exists but belongs to someone else: same answer as "does not exist", logged for audit
            emit("warning", "appointment_isolation_violation", cid, callerId=uid, role=role, appointmentId=appointment_id)
        return respond(404, msg(cid, "Appointment not found", "APPOINTMENT_NOT_FOUND"))
    now = utcnow()
    return respond(200, {"appointment": appointment_json(row, role, now, safe_tiers(cid) if role == "PATIENT" else None),
                         "correlationId": cid})


def list_refunds(event, uid, cid):
    q = query_params(event)
    try:
        page = int_param(q, "page", 1, 1, 10000)
        size = int_param(q, "pageSize", DEFAULT_PAGE_SIZE, 1, MAX_PAGE_SIZE)
    except ValueError as exc:
        return respond(422, msg(cid, str(exc)))
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT COUNT(*) AS n, COALESCE(SUM(r.refund_amount), 0) AS refunded,
                                  COALESCE(SUM(r.deduction_amount), 0) AS deducted
                           FROM refund_ledger r JOIN appointments a ON a.appointment_id = r.appointment_id
                           WHERE a.patient_id = %s""", (uid,))
            tot = cur.fetchone()
            total = int(tot["n"])
            rows = []
            if total > (page - 1) * size:
                cur.execute(APPT_SELECT + " WHERE a.patient_id = %s AND r.refund_id IS NOT NULL"
                            " ORDER BY r.calculated_at DESC, r.refund_id DESC LIMIT %s OFFSET %s", (uid, size, (page - 1) * size))
                rows = cur.fetchall()
    items = []
    for r in rows:
        a = appointment_json(r, "PATIENT")
        items.append({**a["refund"], "appointment": {k: a[k] for k in ("appointmentId", "status", "startAt", "localDate", "localStart",
                                                                        "timezone", "doctor", "consultationType", "fee", "bookedFor")}})
    pages = max((total + size - 1) // size, 1)
    return respond(200, {"items": items, "page": page, "pageSize": size, "total": total, "totalPages": pages,
                         "hasPrevious": page > 1 and total > 0, "hasNext": page < pages,
                         "totals": {"refunded": num(money(tot["refunded"])), "deducted": num(money(tot["deducted"]))},
                         "correlationId": cid})


# =========================================================================== family members (dependents)
def dependent_json(r, upcoming=0):
    return {"dependentId": int(r["dependent_id"]), "firstName": r["first_name"], "lastName": r["last_name"],
            "displayName": f"{r['first_name']} {r['last_name']}", "relation": r["relation"],
            "dateOfBirth": str(r["date_of_birth"])[:10] if r.get("date_of_birth") else None, "gender": r.get("gender"),
            "upcomingAppointments": int(upcoming), "createdAt": iso(r.get("created_at"))}


def clean_dependent(data, required):
    """Validate family-member fields with the SAME rules as patient registration (lambdas/common.py)."""
    if not isinstance(data, dict):
        raise FieldErrors({"body": "A JSON object is required"})
    errors, out = {}, {}
    for k in data:
        if k not in DEPENDENT_FIELDS:
            errors[k] = f"{k} cannot be set here"
    for k in DEPENDENT_FIELDS:
        if k not in data:
            if k in required:
                errors[k] = {"firstName": "First name is required", "lastName": "Last name is required",
                             "relation": "relation must be one of: " + ", ".join(RELATIONS),
                             "dateOfBirth": "dateOfBirth is required in YYYY-MM-DD format"}[k]
            continue
        v = data[k]
        try:
            if k == "firstName":
                out[k] = clean_person_name(v, "First name")
            elif k == "lastName":
                out[k] = clean_person_name(v, "Last name")
            elif k == "dateOfBirth":
                out[k] = clean_date_of_birth(v, utcnow().date())
            elif k == "relation":
                rel = _RELATION_LOOKUP.get(v.strip().lower()) if isinstance(v, str) else None
                if not rel:
                    raise ValueError("relation must be one of: " + ", ".join(RELATIONS))
                out[k] = rel
            elif k == "gender":
                out[k] = None if v in (None, "") else clean_gender(v)
        except ValueError as exc:
            errors[k] = str(exc)
    if errors:
        raise FieldErrors(errors)
    return out


DEP_COLUMNS = {"firstName": "first_name", "lastName": "last_name", "relation": "relation", "dateOfBirth": "date_of_birth",
               "gender": "gender"}
DEP_SELECT = "SELECT dependent_id, first_name, last_name, relation, date_of_birth, gender, created_at FROM patient_dependents"


def upcoming_counts(cur, uid, now):
    cur.execute("""SELECT dependent_id, COUNT(*) AS n FROM appointments
                   WHERE patient_id = %s AND dependent_id IS NOT NULL AND status = 'CONFIRMED' AND start_at > %s
                   GROUP BY dependent_id""", (uid, db_time(now)))
    return {int(r["dependent_id"]): int(r["n"]) for r in cur.fetchall()}


def patient_ready(cur, uid):
    """Lock the account row (serialises with bookings and other family changes) and check it can be used."""
    cur.execute("""SELECT u.is_active, p.patient_id FROM users u LEFT JOIN patients p ON p.patient_id = u.user_id
                   WHERE u.user_id = %s AND u.role = 'PATIENT' FOR UPDATE""", (uid,))
    me = cur.fetchone()
    return bool(me and me["is_active"] and me["patient_id"] is not None)


def is_same_person(cur, uid, fields, exclude_id=None):
    sql = """SELECT dependent_id FROM patient_dependents WHERE patient_id = %s AND is_active = 1
             AND LOWER(first_name) = LOWER(%s) AND LOWER(last_name) = LOWER(%s) AND date_of_birth = %s"""
    args = [uid, fields["firstName"], fields["lastName"], fields["dateOfBirth"]]
    if exclude_id is not None:
        sql += " AND dependent_id <> %s"
        args.append(exclude_id)
    cur.execute(sql + " LIMIT 1", args)
    return cur.fetchone() is not None


def list_dependents(uid, cid):
    now = utcnow()
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute(DEP_SELECT + " WHERE patient_id = %s AND is_active = 1 ORDER BY dependent_id", (uid,))
            rows = cur.fetchall()
            counts = upcoming_counts(cur, uid, now)
    return respond(200, {"items": [dependent_json(r, counts.get(int(r["dependent_id"]), 0)) for r in rows],
                         "maxDependents": MAX_DEPENDENTS, "relations": list(RELATIONS), "correlationId": cid})


def add_dependent(event, uid, cid):
    data = body(event)
    try:
        fields = clean_dependent(data, required=("firstName", "lastName", "relation", "dateOfBirth"))
    except FieldErrors as exc:
        return respond(422, msg(cid, "; ".join(exc.errors.values()), "INVALID_FIELDS", errors=exc.errors))
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            if not patient_ready(cur, uid):
                conn.rollback()
                return respond(409, msg(cid, "Complete your patient profile first", "PROFILE_REQUIRED"))
            cur.execute("SELECT COUNT(*) AS n FROM patient_dependents WHERE patient_id = %s AND is_active = 1", (uid,))
            if int(cur.fetchone()["n"]) >= MAX_DEPENDENTS:
                conn.rollback()
                return respond(409, msg(cid, f"You can add at most {MAX_DEPENDENTS} family members", "DEPENDENT_LIMIT_REACHED"))
            if is_same_person(cur, uid, fields):
                conn.rollback()
                return respond(409, msg(cid, "This family member is already on your account", "DEPENDENT_EXISTS"))
            cur.execute("""INSERT INTO patient_dependents (patient_id, first_name, last_name, relation, date_of_birth, gender)
                           VALUES (%s, %s, %s, %s, %s, %s)""",
                        (uid, fields["firstName"], fields["lastName"], fields["relation"], fields["dateOfBirth"], fields.get("gender")))
            new_id = int(cur.lastrowid)
            conn.commit()
            cur.execute(DEP_SELECT + " WHERE dependent_id = %s", (new_id,))
            row = cur.fetchone()
    emit("info", "dependent_added", cid, patientId=uid, dependentId=new_id, relation=fields["relation"])
    return respond(201, {"message": "Family member added", "dependent": dependent_json(row), "correlationId": cid})


def change_dependent(event, uid, dependent_id, method, cid):
    data = body(event) if method == "PATCH" else {}
    fields = {}
    if method == "PATCH":
        if not isinstance(data, dict) or not data:
            return respond(400, msg(cid, "No fields supplied. You can change: " + ", ".join(DEPENDENT_FIELDS), "INVALID_BODY"))
        try:
            fields = clean_dependent(data, required=())
        except FieldErrors as exc:
            return respond(422, msg(cid, "; ".join(exc.errors.values()), "INVALID_FIELDS", errors=exc.errors))
    now = utcnow()
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            patient_ready(cur, uid)     # takes the account lock
            cur.execute(DEP_SELECT + " WHERE dependent_id = %s AND patient_id = %s AND is_active = 1", (dependent_id, uid))
            row = cur.fetchone()
            if not row:
                conn.rollback()
                return respond(404, msg(cid, "Family member not found", "DEPENDENT_NOT_FOUND"))
            upcoming = upcoming_counts(cur, uid, now).get(dependent_id, 0)
            if method == "DELETE":
                if upcoming:
                    conn.rollback()
                    return respond(409, msg(cid, f"{row['first_name']} still has {upcoming} upcoming appointment"
                                                 f"{'s' if upcoming != 1 else ''}. Cancel them first.", "DEPENDENT_HAS_APPOINTMENTS"))
                cur.execute("UPDATE patient_dependents SET is_active = 0, updated_at = %s WHERE dependent_id = %s",
                            (db_time(now), dependent_id))
                conn.commit()
                emit("info", "dependent_removed", cid, patientId=uid, dependentId=dependent_id)
                return respond(200, msg(cid, "Family member removed"))
            merged = {"firstName": row["first_name"], "lastName": row["last_name"], "dateOfBirth": str(row["date_of_birth"])[:10],
                      **fields}
            if is_same_person(cur, uid, merged, exclude_id=dependent_id):
                conn.rollback()
                return respond(409, msg(cid, "This family member is already on your account", "DEPENDENT_EXISTS"))
            sets = [f"{DEP_COLUMNS[k]} = %s" for k in fields]
            cur.execute(f"UPDATE patient_dependents SET {', '.join(sets)}, updated_at = %s WHERE dependent_id = %s",
                        [fields[k] for k in fields] + [db_time(now), dependent_id])
            conn.commit()
            cur.execute(DEP_SELECT + " WHERE dependent_id = %s", (dependent_id,))
            row = cur.fetchone()
    emit("info", "dependent_updated", cid, patientId=uid, dependentId=dependent_id, fields=sorted(fields))
    return respond(200, {"message": "Family member updated", "dependent": dependent_json(row, upcoming), "correlationId": cid})


# =========================================================================== routing
ROUTE_RE = re.compile(r"(/appointments(?:/[^/]+){0,2}|/doctors/me/slots/[^/]+/cancel|/patients/me/refunds"
                      r"|/patients/me/dependents(?:/[^/]+)?)/?$")


def route(event):
    path = event.get("path") or event.get("rawPath") or ""
    m = ROUTE_RE.search(path)
    return m.group(1) if m else path


def method_of(event):
    return (event.get("httpMethod") or (event.get("requestContext") or {}).get("http", {}).get("method") or "GET").upper()


def appointment_id_of(raw, cid):
    if not re.fullmatch(r"[0-9]{1,18}", raw or "") or int(raw) == 0:
        return None, respond(422, msg(cid, "appointmentId must be a positive number", "APPOINTMENT_ID_INVALID"))
    return int(raw), None


@instrument("booking")
def handler(event, context):
    cid = correlation(event)
    ctx = (event.get("requestContext") or {}).get("authorizer") or {}
    role = ctx.get("role")
    try:
        uid = int(ctx.get("userId") or 0)
    except (TypeError, ValueError):
        uid = 0
    if role not in ("PATIENT", "DOCTOR") or uid <= 0:
        emit("warning", "booking_access_denied", cid, role=role)
        return respond(403, msg(cid, "Sign in to manage appointments"))
    method, path = method_of(event), route(event)
    try:
        if path == "/appointments":
            if method == "POST":
                return book(event, uid, cid) if role == "PATIENT" else respond(403, msg(cid, "Only patients can book appointments"))
            if method == "GET":
                return list_appointments(event, uid, role, cid)
            return respond(405, msg(cid, "Method not allowed"))
        m = re.fullmatch(r"/appointments/([^/]+)(/cancel)?", path)
        if m:
            appointment_id, bad = appointment_id_of(m.group(1), cid)
            if bad:
                return bad
            if m.group(2):
                if method != "POST":
                    return respond(405, msg(cid, "Method not allowed"))
                if role != "PATIENT":
                    return respond(403, msg(cid, "Doctors cancel by slot: POST /doctors/me/slots/{slotId}/cancel"))
                return cancel_by_patient(appointment_id, uid, cid)
            if method != "GET":
                return respond(405, msg(cid, "Method not allowed"))
            return get_appointment(appointment_id, uid, role, cid)
        m = re.fullmatch(r"/doctors/me/slots/([^/]+)/cancel", path)
        if m:
            if method != "POST":
                return respond(405, msg(cid, "Method not allowed"))
            if role != "DOCTOR":
                return respond(403, msg(cid, "Doctor role required"))
            raw = ((event.get("pathParameters") or {}).get("slotId")) or m.group(1)
            return cancel_slot_by_doctor(event, raw, uid, cid)
        m = re.fullmatch(r"/patients/me/dependents(?:/([^/]+))?", path)
        if m:
            if role != "PATIENT":
                return respond(403, msg(cid, "Patient role required"))
            if m.group(1) is None:
                if method == "GET":
                    return list_dependents(uid, cid)
                if method == "POST":
                    return add_dependent(event, uid, cid)
                return respond(405, msg(cid, "Method not allowed"))
            if not re.fullmatch(r"[1-9][0-9]{0,17}", m.group(1)):
                return respond(422, msg(cid, "dependentId must be a positive number", "DEPENDENT_ID_INVALID"))
            if method not in ("PATCH", "PUT", "DELETE"):
                return respond(405, msg(cid, "Method not allowed"))
            return change_dependent(event, uid, int(m.group(1)), "DELETE" if method == "DELETE" else "PATCH", cid)
        if path == "/patients/me/refunds":
            if method != "GET":
                return respond(405, msg(cid, "Method not allowed"))
            if role != "PATIENT":
                return respond(403, msg(cid, "Patient role required"))
            return list_refunds(event, uid, cid)
        return respond(404, msg(cid, "Booking endpoint not found", path=path))
    except Exception as exc:  # noqa: BLE001
        emit("error", "booking_error", cid, error=type(exc).__name__, path=path, method=method,
             detail=str(exc)[:300], traceback=traceback.format_exc()[-1500:])
        return respond(500, msg(cid, "Internal server error"))
