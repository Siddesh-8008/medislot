"""MediSlot booking engine (Milestone 5): booking, concurrency, cancellation and refunds - all in one place.

Routes (behind the JWT authorizer; the role is checked here, the user id always comes from the verified token):
    POST /appointments                         PATIENT  book a slot. Header Idempotency-Key is REQUIRED.
    GET  /appointments                         PATIENT  my appointments    | DOCTOR  appointments with me
    GET  /appointments/{appointmentId}         PATIENT  one of mine        | DOCTOR  one with me
    POST /appointments/{appointmentId}/cancel  PATIENT  cancel; refund tier from the SSM refund policy
    POST /appointments/{appointmentId}/status  DOCTOR   set COMPLETED / NO_SHOW (after start) or CANCELLED_BY_DOCTOR
                                                        (before start, 100 % refund) on one appointment with me
    POST /doctors/me/slots/{slotId}/cancel     DOCTOR   cancel a whole slot; every booked patient refunded 100 %
    GET  /patients/me/refunds                  PATIENT  my refund ledger
    POST /appointments/{appointmentId}/reschedule  PATIENT  move to another slot (same doctor + type) via the Step
                                                        Functions saga. Header Idempotency-Key is REQUIRED.  [M6]
    POST /offers/{offerId}/accept              PATIENT  turn a waitlist hold offer into a confirmed appointment [M6]
    GET  /appointments?from=&to=               DOCTOR   my appointments between two local dates (schedule view) [M7]

Internal (direct Lambda invoke, never reachable through API Gateway):
    {"action": "reschedule<Step>", "input": {...}}   one step of the reschedule saga, called by Step Functions [M6]
    {"action": "cancelBlackoutDay", "doctorId": 42, "date": "2026-11-30", "reason": "..."}
        The doctor blacked out a day: every future slot on that local day that still has CONFIRMED appointments is
        cancelled exactly like POST /doctors/me/slots/{slotId}/cancel (slot CANCELLED, patients refunded 100 %).
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
                    dbconn, emit, instrument, json_out, parse_slot_id)
# slotId = "SLOT#<doctorId>_<YYYYMMDDTHHMMSSZ>_<typeId>" carries the Slots table key; SLOT_REF_RE is the same id
# without "SLOT#", as used in the doctor's cancel URL. Both live in common.py so every Lambda agrees on the format.
from common import SLOT_ID_RE, SLOT_REF_RE  # noqa: E402

_cfg = boto3.session.Config(connect_timeout=3, read_timeout=5, retries={"max_attempts": 3, "mode": "standard"})
ddb = boto3.client("dynamodb", config=_cfg)
ssm = boto3.client("ssm", config=_cfg)
# Short timeouts and no SDK retries: publish() does its own 3 attempts, and a slow EventBridge must never push a
# committed booking past the API Gateway timeout.
events = boto3.client("events", config=boto3.session.Config(connect_timeout=2, read_timeout=2, retries={"max_attempts": 1}))

UTC = timezone.utc
CENT = Decimal("0.01")
IDEMPOTENCY_KEY_RE = re.compile(r"[A-Za-z0-9._:\-]{8,128}")
TIER_NAME_RE = re.compile(r"[A-Z][A-Z0-9_]{0,29}")
CANCELLED = ("CANCELLED_BY_PATIENT", "CANCELLED_BY_DOCTOR")
STATUS_FILTERS = {
    "CONFIRMED": ("CONFIRMED",), "COMPLETED": ("COMPLETED",), "NO_SHOW": ("NO_SHOW",), "CANCELLED": CANCELLED,
    "CANCELLED_BY_PATIENT": ("CANCELLED_BY_PATIENT",), "CANCELLED_BY_DOCTOR": ("CANCELLED_BY_DOCTOR",),
}
DOCTOR_TIER = "DOCTOR_CANCELLED"
# Statuses a doctor can set on one appointment (POST /appointments/{id}/status).
#   COMPLETED / NO_SHOW: only once the appointment has started; the doctor may switch between the two to fix a mis-click.
#   CANCELLED_BY_DOCTOR: only before the start; the patient is refunded 100 % and the seat goes back to the slot.
DOCTOR_SETTABLE = ("COMPLETED", "NO_SHOW", "CANCELLED_BY_DOCTOR")
ATTENDANCE = ("COMPLETED", "NO_SHOW")
BLACKOUT_NOTE = "The doctor is unavailable on this day"
BLACKOUT_REASON = "BLACKOUT"          # closed_reason on slots cancelled by a blackout (reopened when it is removed)
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
# Milestone 6: reschedule rules (SSM, same 60 s cache). The schema caps reschedule_count at 2 (chk_appointments_reschedules).
RESCHEDULE_DEFAULTS = {"max_reschedules": 2, "reschedule_cutoff_hours": 6}
RESCHEDULE_RANGES = {"max_reschedules": (0, 2), "reschedule_cutoff_hours": (0, 168)}
FAULTS = ("release-original", "move")          # fault injection for the saga checkpoint; never honoured in prod


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
            "max_per_doctor_day": f"{p}/max-per-doctor-per-day", "max_retries": f"{p}/max-conflict-retries",
            "max_reschedules": f"{p}/max-reschedules", "reschedule_cutoff_hours": f"{p}/reschedule-cutoff-hours",
            "fault_injection": f"{p}/fault-injection"}


OPTIONAL_PARAMETERS = ("fault_injection",)     # absent on purpose most of the time: never logged as missing


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
        missing = [n for n in res.get("InvalidParameters") or [] if n not in {names[k] for k in OPTIONAL_PARAMETERS}]
        if missing:
            emit("warning", "booking_settings_missing", cid, parameters=missing)
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
    for key, default in RESCHEDULE_DEFAULTS.items():
        raw = values.get(key)
        lo, hi = RESCHEDULE_RANGES[key]
        try:
            n = int(str(raw).strip())
            ok = lo <= n <= hi
        except (TypeError, ValueError):
            ok = False
        if raw is not None and not ok:
            emit("warning", "booking_setting_invalid", cid, setting=key, value=str(raw)[:40])
        out[key] = n if ok else default
    fault = str(values.get("fault_injection") or "").strip().lower()
    out["fault_injection"] = fault if fault in FAULTS and env_name() != "prod" else ""
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


def slot_table_key(slot_id):
    """Low-level DynamoDB key {doctor_id, slot_key} for a slotId (decoded from the id itself, no lookup)."""
    parsed = parse_slot_id(slot_id)
    if parsed is None:
        raise ValueError("not a slot id")
    doctor_id, sk = parsed
    return {"doctor_id": {"N": str(doctor_id)}, "slot_key": {"S": sk}}


def get_slot(slot_id):
    if parse_slot_id(slot_id) is None:
        return None
    res = ddb.get_item(TableName=slots_table(), Key=slot_table_key(slot_id), ConsistentRead=True)
    return _item(res["Item"]) if res.get("Item") else None


def free_seats(slot):
    return max(int(slot.get("capacity", 1) or 1) - int(slot.get("booked_count", 0) or 0) - int(slot.get("held_count", 0) or 0), 0)


def claim_seat(slot, idem_key, attempt_id, now):
    """One optimistic-concurrency attempt: +1 booked seat if the slot is still exactly the version we read.
    Atomic with marking the idempotency record (owned by this attempt) as seat_claimed.
    -> 'ok' | 'conflict' (someone else changed the slot; retry) | 'lost' (our idempotency record was taken over)."""
    version = slot.get("version")
    slot_update = {
        "TableName": slots_table(), "Key": slot_table_key(slot["slot_id"]),
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
                TableName=slots_table(), Key=slot_table_key(slot_id),
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


def mark_slot_cancelled(slot_id, doctor_id, now, reason="DOCTOR_CANCELLED"):
    """Doctor cancels the slot: status CANCELLED so nobody can book it.
    reason DOCTOR_CANCELLED (the 'Cancel slot' button): the materialiser never reopens it.
    reason BLACKOUT (the doctor blocked the whole day): the materialiser reopens it as an empty slot if the blackout
    is removed or moved and the weekly pattern still covers it.
    -> 'cancelled' | 'already' | 'started' | 'missing'"""
    try:
        ddb.update_item(
            TableName=slots_table(), Key=slot_table_key(slot_id),
            UpdateExpression="SET #st = :c, closed_reason = :r, cancelled_at = :now, version = version + :one",
            ConditionExpression="doctor_id = :d AND start_at > :now AND #st <> :c",
            ExpressionAttributeNames={"#st": "status"},
            ExpressionAttributeValues=_vals(c="CANCELLED", r=reason, now=iso(now), one=1, d=int(doctor_id)))
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


def notify_fields(a):
    """What a patient notification needs to read well (Milestone 6). Doctor and appointment facts only - never the
    patient's or family member's name, phone or email: the event bus and its log group are not a place for PII."""
    return {"doctorName": (a.get("doctor") or {}).get("displayName"), "hospital": (a.get("doctor") or {}).get("hospital"),
            "consultationType": (a.get("consultationType") or {}).get("name"), "localDate": a.get("localDate"),
            "localStart": a.get("localStart"), "localEnd": a.get("localEnd"), "timezone": a.get("timezone")}


def booked_event(a, **extra):
    return ("AppointmentBooked", {**{k: a[k] for k in ("appointmentId", "patientId", "doctorId", "slotId", "consultationTypeId",
                                                        "startAt", "endAt", "fee", "status")},
                                  "dependentId": a["bookedFor"].get("dependentId"), "notify": notify_fields(a), **extra})


def cancelled_event(a, refund, cancelled_by):
    return ("AppointmentCancelled", {**{k: a[k] for k in ("appointmentId", "patientId", "doctorId", "slotId", "startAt", "endAt")},
                                     "dependentId": a["bookedFor"].get("dependentId"),
                                     "status": a["status"], "cancelledBy": cancelled_by, "refundTier": refund["tier"],
                                     "refundPercent": refund["refundPercent"], "refundAmount": refund["refundAmount"],
                                     "notify": notify_fields(a)})


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
        if now is not None:
            out["allowedStatuses"] = doctor_status_options(r["status"], start, now)
    if role == "PATIENT" and r["status"] == "CONFIRMED" and now is not None:
        out["cancellable"] = start > now
        if tiers and start > now:
            out["cancellationPreview"] = cancellation_preview(start, r["fee_snapshot"], now, tiers)
    return out


def doctor_status_options(status, start, now):
    """Which statuses the doctor may set next (drives the 'Update status' button on the dashboard)."""
    if status == "CONFIRMED":
        return ["CANCELLED_BY_DOCTOR"] if start > now else list(ATTENDANCE)
    if status in ATTENDANCE:
        return [s for s in ATTENDANCE if s != status]
    return []          # cancelled appointments are final


def blackout_on(cur, doctor_id, start, zone, lock=False):
    """True when the doctor has blacked out the LOCAL day this appointment starts on.
    lock=True (inside the booking transaction) takes a locking read on the unique (doctor_id, blackout_date) key:
    on InnoDB that also locks the gap, so a blackout being added for that day waits until this booking commits and
    its sweep then sees (and cancels) it - a booking can never slip in between the blackout and its sweep."""
    day = start.astimezone(zone).date().isoformat()
    cur.execute("SELECT blackout_id FROM blackout_dates WHERE doctor_id = %s AND blackout_date = %s"
                + (" FOR UPDATE" if lock else ""), (doctor_id, day))
    return cur.fetchone() is not None


def fetch_appointment(cur, appointment_id, lock=False):
    """The appointment with doctor / type / refund details. lock=True first takes the row lock (FOR UPDATE), so the
    read that follows sees the latest committed state (a parallel cancel waits here and then sees CANCELLED)."""
    if lock:
        cur.execute("SELECT appointment_id FROM appointments WHERE appointment_id = %s FOR UPDATE", (appointment_id,))
        if not cur.fetchone():
            return None
    cur.execute(APPT_SELECT + " WHERE a.appointment_id = %s", (appointment_id,))
    return cur.fetchone()


def check_limits(cur, uid, dependent_id, doctor_id, slot_id, start, end, zone, settings, now, who=None, exclude_id=None):
    """Patient booking rules. -> None, or (code, message). Uses only CONFIRMED appointments.
    PER PERSON (account holder = dependent 0): same slot twice, overlapping times, per doctor per day.
    PER ACCOUNT: the cap on upcoming appointments.
    exclude_id (Milestone 6 reschedule): the appointment being MOVED is ignored by the per-person rules, and the
    account cap is skipped because moving an appointment never changes how many the account holds."""
    person = int(dependent_id or 0)
    subject = "You" if not who else who
    skip, skip_args = ("", ()) if exclude_id is None else (" AND appointment_id <> %s", (int(exclude_id),))
    cur.execute("""SELECT appointment_id FROM appointments WHERE patient_id = %s AND COALESCE(dependent_id, 0) = %s
                   AND slot_id = %s AND status = 'CONFIRMED'""" + skip + " LIMIT 1", (uid, person, slot_id) + skip_args)
    if cur.fetchone():
        return "DUPLICATE_BOOKING", "You have already booked this slot" if not who else f"{who} is already booked in this slot"
    cur.execute("""SELECT appointment_id FROM appointments
                   WHERE patient_id = %s AND COALESCE(dependent_id, 0) = %s AND status = 'CONFIRMED'
                   AND start_at < %s AND end_at > %s""" + skip + " LIMIT 1",
                (uid, person, db_time(end), db_time(start)) + skip_args)
    if cur.fetchone():
        return "OVERLAPPING_APPOINTMENT", f"{subject} already {'have' if not who else 'has'} another appointment at that time"
    local_day = start.astimezone(zone).date()
    day_start = datetime.combine(local_day, datetime.min.time(), tzinfo=zone).astimezone(UTC)
    day_end = datetime.combine(local_day + timedelta(days=1), datetime.min.time(), tzinfo=zone).astimezone(UTC)
    cur.execute("""SELECT COUNT(*) AS n FROM appointments
                   WHERE patient_id = %s AND doctor_id = %s AND status = 'CONFIRMED' AND COALESCE(dependent_id, 0) = %s
                   AND start_at >= %s AND start_at < %s""" + skip,
                (uid, doctor_id, person, db_time(day_start), db_time(day_end)) + skip_args)
    if int(cur.fetchone()["n"]) >= settings["max_per_doctor_day"]:
        n = settings["max_per_doctor_day"]
        return "DOCTOR_DAILY_LIMIT_REACHED", (f"{'You' if not who else who} can have at most {n} appointment{'s' if n != 1 else ''} "
                                              "with the same doctor on one day")
    if exclude_id is not None:
        return None
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
                if blackout_on(cur, doctor_id, start, zone_of(offer["timezone"])):
                    problem = ("DOCTOR_UNAVAILABLE", "The doctor is not available on this day. Please choose another date.")
                else:
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
                    if blackout_on(cur, doctor_id, start, zone, lock=True):
                        conn.rollback()
                        release_seat(slot_id, cid, "doctor_blackout")
                        return reject(409, "DOCTOR_UNAVAILABLE", "The doctor is not available on this day. Please choose another date.")
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
            if reschedule_lock_owner(appointment_id):   # Milestone 6: a reschedule is moving it right now
                conn.rollback()
                return respond(409, msg(cid, "This appointment is being rescheduled. Try again in a moment.",
                                        "RESCHEDULE_IN_PROGRESS"), {"Retry-After": "5"})
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


def doctor_cancel_slot(slot_id, uid, now, note, cid, source="slot"):
    """The doctor cancels ONE slot: mark it CANCELLED in DynamoDB (nobody can book it, the materialiser never reopens
    it), then cancel every CONFIRMED appointment in it with a 100 % refund. Shared by the cancel-slot endpoint and the
    blackout-day sweep. Safe to repeat: a second call finishes a sweep that failed half-way and changes nothing else.
    -> (state, appointments) with state 'cancelled' | 'already' | 'started' | 'missing'."""
    slot = get_slot(slot_id)
    if not slot or int(slot.get("doctor_id", 0)) != uid:
        if slot:
            emit("warning", "slot_isolation_violation", cid, callerId=uid, slotId=slot_id)
        return "missing", []
    if parse_utc(slot["start_at"]) <= now and slot.get("status") != "CANCELLED":
        return "started", []
    reason = BLACKOUT_REASON if source == "blackout" else "DOCTOR_CANCELLED"
    state = "already" if slot.get("status") == "CANCELLED" else mark_slot_cancelled(slot_id, uid, now, reason)
    if state in ("missing", "started"):
        return state, []

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
                                          "cancelledAppointments": len(appts), "reason": source}))
    publish(entries, cid)
    if appts:
        metric("CancellationsByTier", len(appts), Tier=DOCTOR_TIER)
    emit("info", "slot_cancelled_by_doctor", cid, doctorId=uid, slotId=slot_id, cancelledAppointments=len(appts),
         alreadyCancelled=state == "already", source=source)
    return state, appts


def cancel_day_by_doctor(doctor_id, day_text, reason, cid):
    """Internal action for a new blackout date: cancel every future slot of that LOCAL day that still has CONFIRMED
    appointments (patients refunded 100 %). Unbooked slots are closed by the slot materialiser as before.
    Idempotent - adding the same blackout again simply re-runs the sweep."""
    try:
        doctor_id = int(doctor_id)
        day = datetime.strptime(str(day_text), "%Y-%m-%d").date()
    except (TypeError, ValueError):
        return {"ok": False, "message": "doctorId and date (YYYY-MM-DD) are required"}
    reason = " ".join(str(reason or "").split())[:200]
    note = f"{BLACKOUT_NOTE} ({reason})"[:255] if reason else BLACKOUT_NOTE
    now = utcnow()
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT timezone FROM doctors WHERE doctor_id = %s", (doctor_id,))
            doc = cur.fetchone()
            if not doc:
                conn.rollback()
                return {"ok": False, "message": "Doctor not found"}
            zone = zone_of(doc["timezone"])
            lo = datetime.combine(day, datetime.min.time(), tzinfo=zone).astimezone(UTC)
            hi = datetime.combine(day + timedelta(days=1), datetime.min.time(), tzinfo=zone).astimezone(UTC)
            cur.execute("""SELECT DISTINCT slot_id FROM appointments
                           WHERE doctor_id = %s AND status = 'CONFIRMED' AND start_at >= %s AND start_at < %s AND start_at > %s
                           ORDER BY slot_id""", (doctor_id, db_time(lo), db_time(hi), db_time(now)))
            slot_ids = [r["slot_id"] for r in cur.fetchall()]
        conn.rollback()
    cancelled, refunded, skipped = 0, Decimal("0.00"), []
    for slot_id in slot_ids:
        try:
            state, appts = doctor_cancel_slot(slot_id, doctor_id, now, note, cid, source="blackout")
        except Exception as exc:  # noqa: BLE001 - one bad slot must not stop the rest; a re-run finishes it
            emit("error", "blackout_slot_cancel_failed", cid, doctorId=doctor_id, slotId=slot_id, error=type(exc).__name__)
            skipped.append(slot_id)
            continue
        if state in ("missing", "started"):
            skipped.append(slot_id)
        cancelled += len(appts)
        refunded += sum((money(a["refund"]["refundAmount"]) for a in appts), Decimal("0.00"))
    emit("info", "blackout_day_swept", cid, doctorId=doctor_id, date=day.isoformat(), slots=len(slot_ids),
         cancelledAppointments=cancelled, skippedSlots=len(skipped))
    return {"ok": not skipped, "date": day.isoformat(), "slots": len(slot_ids), "cancelledAppointments": cancelled,
            "totalRefunded": num(refunded), "skippedSlots": skipped}


def update_status_by_doctor(event, appointment_id, uid, cid):
    """The doctor sets the outcome of one appointment: COMPLETED / NO_SHOW once it has started, or
    CANCELLED_BY_DOCTOR before it starts (100 % refund, seat released)."""
    data = body(event)
    if not isinstance(data, dict):
        return respond(400, msg(cid, "Invalid JSON body", "INVALID_BODY"))
    new = str(data.get("status") or "").strip().upper()
    if new not in DOCTOR_SETTABLE:
        return respond(422, msg(cid, "status must be one of: " + ", ".join(DOCTOR_SETTABLE), "STATUS_INVALID"))
    note = " ".join(str(data.get("note") or data.get("reason") or "").split())[:255] or None
    now = utcnow()
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            row = fetch_appointment(cur, appointment_id, lock=True)
            if not row or int(row["doctor_id"]) != uid:
                conn.rollback()
                if row:
                    emit("warning", "appointment_isolation_violation", cid, callerId=uid, role="DOCTOR", appointmentId=appointment_id)
                return respond(404, msg(cid, "Appointment not found", "APPOINTMENT_NOT_FOUND"))
            old, start = row["status"], parse_utc(row["start_at"])
            if old == new:
                conn.rollback()
                return respond(200, {"message": "The appointment already has this status", "unchanged": True,
                                     "appointment": appointment_json(row, "DOCTOR", now), "correlationId": cid})
            if old in CANCELLED:
                conn.rollback()
                return respond(409, msg(cid, "A cancelled appointment cannot be changed", "NOT_CHANGEABLE"))
            if new in ATTENDANCE:
                if start > now:
                    conn.rollback()
                    return respond(409, msg(cid, "You can mark an appointment completed or no-show only once it has started",
                                            "NOT_STARTED"))
                cur.execute("""UPDATE appointments SET status = %s, cancellation_note = COALESCE(%s, cancellation_note),
                                      updated_at = %s WHERE appointment_id = %s AND status = %s""",
                            (new, note, db_time(now), appointment_id, old))
                if cur.rowcount != 1:
                    conn.rollback()
                    return respond(409, msg(cid, "The appointment changed meanwhile. Refresh and try again.", "CONFLICT"))
                conn.commit()
                row = fetch_appointment(cur, appointment_id)
                done = None
            else:   # CANCELLED_BY_DOCTOR
                if old != "CONFIRMED":
                    conn.rollback()
                    return respond(409, msg(cid, f"A {old.lower().replace('_', ' ')} appointment cannot be cancelled", "NOT_CANCELLABLE"))
                if start <= now:
                    conn.rollback()
                    return respond(409, msg(cid, "This appointment has already started. Mark it completed or no-show instead.",
                                            "ALREADY_STARTED"))
                done = cancel_confirmed_by_doctor(cur, row["slot_id"], uid, now, note or "Cancelled by the doctor", appointment_id)
                conn.commit()
                row = fetch_appointment(cur, appointment_id)
    appt = appointment_json(row, "DOCTOR", now)
    entries = []
    if new == "CANCELLED_BY_DOCTOR":
        if done:
            metric("CancellationsByTier", 1, Tier=DOCTOR_TIER)
            entries.append(cancelled_event(appt, appt["refund"], "DOCTOR"))
            after = release_seat(row["slot_id"], cid, "doctor_cancel_appointment")
            if after and after.get("status") == "OPEN" and start > utcnow():
                entries.append(("SlotSeatFreed", {"slotId": row["slot_id"], "doctorId": uid,
                                                  "consultationTypeId": int(row["consultation_type_id"]), "startAt": iso(start),
                                                  "localDate": after.get("local_date") or appt["localDate"],
                                                  "freeSeats": free_seats(after), "capacity": int(after.get("capacity", 1))}))
    else:
        entries.append(("AppointmentStatusChanged", {"appointmentId": appt["appointmentId"], "doctorId": uid,
                                                     "patientId": appt["patientId"], "slotId": appt["slotId"],
                                                     "startAt": appt["startAt"], "previousStatus": old, "status": new,
                                                     "markedBy": "DOCTOR", "notify": notify_fields(appt)}))
    publish(entries, cid)
    emit("info", "appointment_status_set_by_doctor", cid, doctorId=uid, appointmentId=appointment_id, previous=old, status=new)
    text = {"COMPLETED": "Appointment marked as completed", "NO_SHOW": "Appointment marked as no-show",
            "CANCELLED_BY_DOCTOR": "Appointment cancelled; the patient is refunded in full"}[new]
    return respond(200, {"message": text, "appointment": appt, "refund": appt.get("refund"), "correlationId": cid})


def cancel_slot_by_doctor(event, raw_slot, uid, cid):
    slot_id = unquote(raw_slot or "")
    if SLOT_REF_RE.fullmatch(slot_id):
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
    state, appts = doctor_cancel_slot(slot_id, uid, now, note, cid)
    if state == "missing":
        return respond(404, msg(cid, "Slot not found", "SLOT_NOT_FOUND"))
    if state == "started":
        return respond(409, msg(cid, "This slot has already started and cannot be cancelled", "SLOT_STARTED"))
    total = sum((money(a["refund"]["refundAmount"]) for a in appts), Decimal("0.00"))
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
    day_from, day_to = q.get("from", "").strip(), q.get("to", "").strip()
    if (day_from or day_to) and role != "DOCTOR":
        return respond(422, msg(cid, "from / to can only be used by doctors"))
    try:
        first = datetime.strptime(day_from, "%Y-%m-%d").date() if day_from else None
        last = datetime.strptime(day_to, "%Y-%m-%d").date() if day_to else first
    except ValueError:
        return respond(422, msg(cid, "from and to must be dates (YYYY-MM-DD)"))
    if first and last and (last < first or (last - first).days > 31):
        return respond(422, msg(cid, "to must be on or after from, at most 31 days later"))
    now = utcnow()
    owner = "a.patient_id" if role == "PATIENT" else "a.doctor_id"
    where, args = [f"{owner} = %s"], [uid]
    if first:   # Milestone 7: the doctor's day / week schedule asks for "my patients between these LOCAL dates"
        with dbconn(db_config()) as conn:
            with conn.cursor() as cur:
                cur.execute("SELECT timezone FROM doctors WHERE doctor_id = %s", (uid,))
                doc = cur.fetchone()
            conn.rollback()
        zone = zone_of((doc or {}).get("timezone"))
        lo = datetime.combine(first, datetime.min.time(), tzinfo=zone).astimezone(UTC)
        hi = datetime.combine(last + timedelta(days=1), datetime.min.time(), tzinfo=zone).astimezone(UTC)
        where.append("a.start_at >= %s AND a.start_at < %s")
        args += [db_time(lo), db_time(hi)]
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
    order = ("a.start_at ASC, a.appointment_id ASC" if when == "upcoming" or first else
             "a.start_at DESC, a.appointment_id DESC")
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


# =========================================================================== Milestone 6: reschedule saga
# POST /appointments/{id}/reschedule starts a STANDARD Step Functions execution (medislot-<env>-reschedule-saga) whose
# Task states call this Lambda with {"action": "reschedule<Step>", "input": <state>}. Every step is idempotent, so a
# Step Functions retry (or a crash half-way) can never claim or release a seat twice:
#
#   Validate -> ClaimTarget -> MoveAppointment -> ReleaseOriginal -> Finalize
#                   |               |                  |
#                   +---------------+------------------+--> CompensateRevertMove -> CompensateReleaseTarget
#                                                                               -> RecordCompensated (FAIL)
#
#   * ClaimTarget   +1 seat on the new slot (optimistic concurrency, same as booking) and mark target_claimed on the
#                   reschedule record IN THE SAME DynamoDB transaction.
#   * MoveAppointment  RDS: the one appointment row is updated in place (slot, times, reschedule_count + 1). The
#                   patient therefore always holds exactly ONE appointment - there is never a second row.
#   * ReleaseOriginal  -1 seat on the old slot, again together with original_released on the record.
#   * Compensation: move the row back (only if the old seat was NOT released) and give the new seat back.
# A DynamoDB lock item (LOCK#APPT#<id>, 10 minute lease) stops a patient cancellation or a second reschedule from
# changing the appointment while the saga runs.
sfn = boto3.client("stepfunctions", config=_cfg)
RESCHEDULE_LOCK_SECONDS = 600
RESCHEDULE_RECORD_TTL = 24 * 3600
SAGA_WAIT_SECONDS = 18              # the API answers synchronously when the saga finishes in time (it normally takes < 2 s)
TRANSIENT_DDB = ("TransactionConflictException", "ProvisionedThroughputExceededException", "ThrottlingException",
                 "RequestLimitExceeded", "TransactionInProgressException", "InternalServerError")


class SeatReleaseError(Exception):
    """A seat could not be given back. Step Functions retries the step, then runs the compensation branch."""


def state_machine_arn():
    return os.environ.get("RESCHEDULE_STATE_MACHINE_ARN", "")


def offers_table():
    return os.environ.get("OFFERS_TABLE") or f"MediSlot-{env_name()}-Offers"


def waitlist_table():
    return os.environ.get("WAITLIST_TABLE") or f"MediSlot-{env_name()}-WaitlistEntries"


def lock_key(appointment_id):
    return f"LOCK#APPT#{int(appointment_id)}"


def reschedule_lock_owner(appointment_id):
    """The saga execution that is moving this appointment right now, or None. An expired lease does not count."""
    try:
        res = ddb.get_item(TableName=idempotency_table(), Key={"idempotency_key": {"S": lock_key(appointment_id)}},
                           ConsistentRead=True)
    except Exception as exc:  # noqa: BLE001 - never block a cancellation because the lock could not be read
        emit("warning", "reschedule_lock_read_failed", None, appointmentId=appointment_id, error=type(exc).__name__)
        return None
    rec = _item(res.get("Item"))
    return rec.get("owner") if rec and int(rec.get("lease_expires_at", 0)) > time.time() else None


def acquire_reschedule_lock(appointment_id, owner):
    now = int(time.time())
    try:
        ddb.put_item(TableName=idempotency_table(),
                     Item={k: _av(v) for k, v in {"idempotency_key": lock_key(appointment_id), "owner": owner,
                                                   "lease_expires_at": now + RESCHEDULE_LOCK_SECONDS,
                                                   "expires_at": now + 2 * RESCHEDULE_LOCK_SECONDS}.items()},
                     ConditionExpression="attribute_not_exists(idempotency_key) OR lease_expires_at < :now OR #o = :me",
                     ExpressionAttributeNames={"#o": "owner"}, ExpressionAttributeValues=_vals(now=now, me=owner))
        return True
    except Exception as exc:  # noqa: BLE001
        if _code(exc) == "ConditionalCheckFailedException":
            return False
        raise


def release_reschedule_lock(appointment_id, owner):
    try:
        ddb.delete_item(TableName=idempotency_table(), Key={"idempotency_key": {"S": lock_key(appointment_id)}},
                        ConditionExpression="#o = :me", ExpressionAttributeNames={"#o": "owner"},
                        ExpressionAttributeValues=_vals(me=owner))
    except Exception as exc:  # noqa: BLE001 - not ours any more, or already gone: the lease expires anyway
        if _code(exc) != "ConditionalCheckFailedException":
            emit("warning", "reschedule_lock_release_failed", None, appointmentId=appointment_id, error=type(exc).__name__)


def resched_record(key):
    res = ddb.get_item(TableName=idempotency_table(), Key={"idempotency_key": {"S": key}}, ConsistentRead=True)
    return _item(res.get("Item"))


def resched_begin(key, req_hash, request):
    """-> (state, record). state: new | replay | mismatch | in_progress. The record stores the exact execution input,
    so a retry with the same key re-sends the SAME input (Step Functions treats same name + same input as idempotent)."""
    now = int(time.time())
    name = f"r-{hashlib.sha256(key.encode()).hexdigest()[:40]}-{now}"
    item = {"idempotency_key": key, "status": "IN_PROGRESS", "kind": "RESCHEDULE", "request_hash": req_hash,
            "patient_id": str(request["patientId"]), "appointment_id": int(request["appointmentId"]),
            "slot_id": request["targetSlotId"], "execution_name": name, "created_at": iso(utcnow()),
            "expires_at": now + RESCHEDULE_RECORD_TTL}
    item["request_json"] = json.dumps({**request, "executionName": name, "idempotencyKey": key}, sort_keys=True)
    for _ in range(3):
        try:
            ddb.put_item(TableName=idempotency_table(), Item={k: _av(v) for k, v in item.items()},
                         ConditionExpression="attribute_not_exists(idempotency_key) OR expires_at < :now",
                         ExpressionAttributeValues=_vals(now=now))
            return "new", item
        except Exception as exc:  # noqa: BLE001
            if _code(exc) != "ConditionalCheckFailedException":
                raise
        rec = resched_record(key)
        if not rec:
            continue
        if rec.get("request_hash") != req_hash:
            return "mismatch", rec
        return ("replay" if rec.get("status") == "COMPLETED" else "in_progress"), rec
    return "in_progress", item


def resched_complete(key, status, payload):
    ddb.update_item(TableName=idempotency_table(), Key={"idempotency_key": {"S": key}},
                    UpdateExpression="SET #st = :done, response_status = :code, response_body = :body, completed_at = :at",
                    ExpressionAttributeNames={"#st": "status"},
                    ExpressionAttributeValues=_vals(done="COMPLETED", code=int(status), body=json.dumps(payload, default=str),
                                                    at=iso(utcnow())))


def replay_stored(rec, cid, replayed=False):
    stored = json.loads(rec.get("response_body") or "{}")
    stored["correlationId"] = cid
    return respond(int(rec.get("response_status", 200)), stored, {"Idempotent-Replayed": "true"} if replayed else None)


def execution_arn(name):
    return state_machine_arn().replace(":stateMachine:", ":execution:") + ":" + name


def wait_execution(arn):
    deadline, delay = time.time() + SAGA_WAIT_SECONDS, 0.15
    while True:
        try:
            status = sfn.describe_execution(executionArn=arn).get("status", "RUNNING")
        except Exception as exc:  # noqa: BLE001 - a brand-new execution can briefly be invisible
            if _code(exc) not in ("ExecutionDoesNotExist", "ThrottlingException"):
                raise
            status = "RUNNING"
        if status != "RUNNING" or time.time() >= deadline:
            return status
        sleep(delay)
        delay = min(delay * 1.6, 1.5)


def reschedule(event, appointment_id, uid, cid):
    client_key = (header(event, "Idempotency-Key") or "").strip()
    if not client_key:
        return respond(400, msg(cid, "The Idempotency-Key header is required to reschedule", "IDEMPOTENCY_KEY_REQUIRED"))
    if not IDEMPOTENCY_KEY_RE.fullmatch(client_key):
        return respond(400, msg(cid, "Idempotency-Key must be 8-128 characters: letters, digits, . _ : -", "IDEMPOTENCY_KEY_INVALID"))
    data = body(event)
    if not isinstance(data, dict):
        return respond(400, msg(cid, "Invalid JSON body", "INVALID_BODY"))
    extra = sorted(k for k in data if k != "targetSlotId")
    if extra:
        return respond(422, msg(cid, "Only targetSlotId can be sent (not: " + ", ".join(extra)[:120] + ")", "UNKNOWN_FIELDS"))
    target = data.get("targetSlotId")
    if not isinstance(target, str) or not SLOT_ID_RE.fullmatch(target):
        return respond(422, msg(cid, "targetSlotId is required (as returned by GET /doctors/{doctorId}/slots)", "SLOT_ID_INVALID"))
    if not state_machine_arn():
        return respond(503, msg(cid, "Rescheduling is temporarily unavailable", "RESCHEDULE_UNAVAILABLE"))
    fault = (header(event, "X-Fault-Injection") or "").strip().lower()
    fault = fault if fault in FAULTS and env_name() != "prod" else (load_settings(cid).get("fault_injection") or "")
    key = f"RESCHED#{uid}#{client_key}"
    req_hash = hashlib.sha256(json.dumps({"appointmentId": appointment_id, "targetSlotId": target}, sort_keys=True).encode()).hexdigest()
    request = {"appointmentId": appointment_id, "patientId": uid, "targetSlotId": target, "faultInjection": fault,
               "correlationId": cid}
    state, rec = resched_begin(key, req_hash, request)
    if state == "mismatch":
        return respond(422, msg(cid, "This Idempotency-Key was already used for a different reschedule", "IDEMPOTENCY_KEY_REUSED"))
    if state == "replay":
        metric("IdempotentReplays", 1)
        return replay_stored(rec, cid, replayed=True)
    name = rec["execution_name"]
    try:
        sfn.start_execution(stateMachineArn=state_machine_arn(), name=name,
                            input=json.dumps({"request": json.loads(rec["request_json"])}, sort_keys=True))
    except Exception as exc:  # noqa: BLE001
        if _code(exc) != "ExecutionAlreadyExists":
            emit("error", "reschedule_start_failed", cid, appointmentId=appointment_id, error=type(exc).__name__)
            if state == "new":
                try:
                    ddb.delete_item(TableName=idempotency_table(), Key={"idempotency_key": {"S": key}},
                                    ConditionExpression="#st = :p", ExpressionAttributeNames={"#st": "status"},
                                    ExpressionAttributeValues=_vals(p="IN_PROGRESS"))
                except Exception:  # noqa: BLE001
                    pass
            return respond(503, msg(cid, "Rescheduling is temporarily unavailable. Your appointment was not changed.",
                                    "RESCHEDULE_UNAVAILABLE"))
    emit("info", "reschedule_started", cid, appointmentId=appointment_id, execution=name, fault=fault or None)
    status = wait_execution(execution_arn(name))
    rec = resched_record(key)
    if rec.get("status") == "COMPLETED":
        return replay_stored(rec, cid)
    if status == "RUNNING":
        return respond(202, {"message": "Your reschedule is still being processed. Send the same request again (same "
                                        "Idempotency-Key) in a few seconds to get the result.", "status": "IN_PROGRESS",
                             "execution": name, "correlationId": cid}, {"Retry-After": "3"})
    emit("error", "reschedule_ended_without_result", cid, appointmentId=appointment_id, execution=name, status=status)
    return respond(503, msg(cid, "The reschedule could not be completed. Please check your appointment before trying again.",
                            "RESCHEDULE_FAILED"))


# --------------------------------------------------------------------------- saga steps (called by Step Functions)
def _no(status, code, text):
    return {"ok": False, "status": status, "code": code, "message": text}


def _seat_update(slot_id, delta_booked, cond, values, delta_held=0):
    """Update spec for one seat counter change on a slot (used inside transactions)."""
    sets = ["booked_count = booked_count " + ("+" if delta_booked > 0 else "-") + " :one"] if delta_booked else []
    if delta_held:
        sets.append("held_count = held_count " + ("+" if delta_held > 0 else "-") + " :one")
    sets += ["version = version + :one", "updated_at = :now"]
    return {"TableName": slots_table(), "Key": slot_table_key(slot_id), "UpdateExpression": "SET " + ", ".join(sets),
            "ConditionExpression": cond, "ExpressionAttributeValues": _vals(one=1, now=iso(utcnow()), **values)}


def _flag_update(key, flag, cond, values=None):
    return {"TableName": idempotency_table(), "Key": {"idempotency_key": {"S": key}},
            "UpdateExpression": f"SET {flag} = :t, {flag}_at = :at", "ConditionExpression": cond,
            "ExpressionAttributeValues": _vals(**{"t": True, "at": iso(utcnow()), **(values or {})})}


def saga_validate(state, cid):
    req = state["request"]
    uid, aid, target, owner = int(req["patientId"]), int(req["appointmentId"]), req["targetSlotId"], req["executionName"]
    now, settings = utcnow(), load_settings(cid)

    def no(status, code, text):
        emit("info", "reschedule_rejected", cid, appointmentId=aid, code=code)
        return _no(status, code, text)

    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            row = fetch_appointment(cur, aid)
        conn.rollback()
    if not row or int(row["patient_id"]) != uid:
        if row:
            emit("warning", "appointment_isolation_violation", cid, callerId=uid, appointmentId=aid)
        return no(404, "APPOINTMENT_NOT_FOUND", "Appointment not found")
    if row["status"] != "CONFIRMED":
        return no(409, "NOT_RESCHEDULABLE", f"A {row['status'].lower().replace('_', ' ')} appointment cannot be rescheduled")
    count, limit = int(row.get("reschedule_count") or 0), int(settings.get("max_reschedules", 2))
    if count >= limit:
        return no(409, "RESCHEDULE_LIMIT_REACHED",
                  f"An appointment can be rescheduled at most {limit} time{'s' if limit != 1 else ''}" if limit else
                  "Rescheduling is switched off")
    start, hours = parse_utc(row["start_at"]), int(settings.get("reschedule_cutoff_hours", 6))
    if start - now < timedelta(hours=hours):
        return no(409, "RESCHEDULE_CUTOFF", f"Appointments can be rescheduled up to {hours} hours before they start")
    if target == row["slot_id"]:
        return no(422, "SAME_SLOT", "Pick a different time than the one you already have")
    slot = get_slot(target)
    if not slot:
        return no(404, "SLOT_NOT_FOUND", "That slot does not exist")
    if int(slot["doctor_id"]) != int(row["doctor_id"]):
        return no(422, "DIFFERENT_DOCTOR", "An appointment can only be moved to another time with the same doctor")
    if int(slot["consultation_type_id"]) != int(row["consultation_type_id"]):
        return no(422, "DIFFERENT_TYPE", "An appointment can only be moved to a slot of the same consultation type")
    tstart, tend = parse_utc(slot["start_at"]), parse_utc(slot["end_at"])
    if tstart <= now:
        return no(422, "SLOT_IN_PAST", "That slot has already started or is in the past")
    if slot.get("status") != "OPEN":
        return no(409, "SLOT_CLOSED", "That slot is no longer open for booking")
    if free_seats(slot) <= 0:
        return no(409, "SLOT_FULL", "Sorry, that time is fully booked")
    zone, dep = zone_of(row.get("doctor_timezone")), row.get("dependent_id")
    who = booked_for(row).get("name") if dep is not None else None
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("""SELECT dct.fee FROM doctor_consultation_types dct
                           JOIN consultation_types ct ON ct.consultation_type_id = dct.consultation_type_id AND ct.is_active = 1
                           JOIN users du ON du.user_id = dct.doctor_id AND du.is_active = 1
                           WHERE dct.doctor_id = %s AND dct.consultation_type_id = %s AND dct.is_active = 1""",
                        (int(row["doctor_id"]), int(row["consultation_type_id"])))
            if not cur.fetchone():
                problem = ("SLOT_UNAVAILABLE", "This doctor is not taking bookings for that consultation type")
            elif blackout_on(cur, int(row["doctor_id"]), tstart, zone):
                problem = ("DOCTOR_UNAVAILABLE", "The doctor is not available on that day. Please choose another date.")
            else:
                problem = check_limits(cur, uid, dep, int(row["doctor_id"]), target, tstart, tend, zone, settings, now, who,
                                       exclude_id=aid)
        conn.rollback()
    if problem:
        return no(409, *problem)
    if not acquire_reschedule_lock(aid, owner):
        return no(409, "RESCHEDULE_IN_PROGRESS", "This appointment is already being rescheduled. Try again in a moment.")
    return {"ok": True, "originalSlotId": row["slot_id"], "doctorId": int(row["doctor_id"]),
            "consultationTypeId": int(row["consultation_type_id"]), "dependentId": dep, "expectedCount": count,
            "originalStartAt": iso(start), "originalEndAt": iso(row["end_at"]), "targetStartAt": iso(tstart),
            "targetEndAt": iso(tend), "timezone": str(zone)}


def saga_claim_target(state, cid):
    req = state["request"]
    key, target = req["idempotencyKey"], req["targetSlotId"]
    if resched_record(key).get("target_claimed"):
        return {"ok": True, "already": True}
    settings, retries = load_settings(cid), 0
    for _ in range(settings["max_retries"] + 1):
        slot = get_slot(target)
        if not slot or slot.get("status") != "OPEN":
            return _no(409, "SLOT_CLOSED", "That slot is no longer open for booking")
        if parse_utc(slot["start_at"]) <= utcnow():
            return _no(422, "SLOT_IN_PAST", "That slot has already started or is in the past")
        if free_seats(slot) <= 0:
            return _no(409, "SLOT_FULL", "Sorry, that time is fully booked")
        version = slot.get("version")
        slot_update = {
            "TableName": slots_table(), "Key": slot_table_key(target),
            "UpdateExpression": "SET booked_count = :nb, version = :nv, updated_at = :now",
            "ConditionExpression": ("version = :v" if version is not None else "attribute_not_exists(version)") + " AND #st = :open",
            "ExpressionAttributeNames": {"#st": "status"},
            "ExpressionAttributeValues": _vals(nb=int(slot.get("booked_count", 0) or 0) + 1, nv=int(version or 0) + 1,
                                               now=iso(utcnow()), open="OPEN", **({"v": int(version)} if version is not None else {}))}
        try:
            ddb.transact_write_items(TransactItems=[
                {"Update": slot_update},
                {"Update": _flag_update(key, "target_claimed", "attribute_exists(idempotency_key) AND attribute_not_exists(target_claimed)")}])
            emit("info", "reschedule_target_claimed", cid, slotId=target, retries=retries)
            return {"ok": True}
        except Exception as exc:  # noqa: BLE001
            code = _code(exc)
            if code == "TransactionCanceledException":
                reasons = [r.get("Code") for r in (exc.response.get("CancellationReasons") or [])]
                if len(reasons) > 1 and reasons[1] == "ConditionalCheckFailed":
                    if resched_record(key).get("target_claimed"):
                        return {"ok": True, "already": True}
                    raise RuntimeError("reschedule record is missing")
            elif code not in TRANSIENT_DDB:
                raise
        retries += 1
        sleep(backoff(retries))
    return _no(409, "SLOT_BUSY", "That time is in high demand right now. Please try again.")


def saga_move(state, cid):
    req, v = state["request"], state["validate"]
    uid, aid, target, original = int(req["patientId"]), int(req["appointmentId"]), req["targetSlotId"], v["originalSlotId"]
    if req.get("faultInjection") == "move":
        emit("warning", "fault_injected", cid, step="MOVE_APPOINTMENT", appointmentId=aid)
        return _no(503, "FAULT_INJECTED", "Injected failure while moving the appointment")
    now, settings = utcnow(), load_settings(cid)
    slot = get_slot(target)
    if not slot or slot.get("status") != "OPEN":
        return _no(409, "SLOT_CLOSED", "That slot is no longer open for booking")
    tstart, tend, zone, expected = parse_utc(v["targetStartAt"]), parse_utc(v["targetEndAt"]), zone_of(v["timezone"]), int(v["expectedCount"])
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            try:
                cur.execute("SELECT user_id FROM users WHERE user_id = %s FOR UPDATE", (uid,))
                row = fetch_appointment(cur, aid, lock=True)
                if row and row["status"] == "CONFIRMED" and row["slot_id"] == target and int(row["reschedule_count"]) == expected + 1:
                    conn.rollback()
                    return {"ok": True, "already": True}            # a retried step: the move is already committed
                if (not row or row["status"] != "CONFIRMED" or row["slot_id"] != original
                        or int(row["reschedule_count"] or 0) != expected):
                    conn.rollback()
                    return _no(409, "APPOINTMENT_CHANGED", "The appointment changed while it was being moved")
                if blackout_on(cur, int(v["doctorId"]), tstart, zone, lock=True):
                    conn.rollback()
                    return _no(409, "DOCTOR_UNAVAILABLE", "The doctor is not available on that day. Please choose another date.")
                who = booked_for(row).get("name") if v.get("dependentId") is not None else None
                problem = check_limits(cur, uid, v.get("dependentId"), int(v["doctorId"]), target, tstart, tend, zone, settings,
                                       now, who, exclude_id=aid)
                if problem:
                    conn.rollback()
                    return _no(409, *problem)
                cur.execute("""UPDATE appointments SET slot_id = %s, start_at = %s, end_at = %s,
                                      reschedule_count = reschedule_count + 1, updated_at = %s
                               WHERE appointment_id = %s AND status = 'CONFIRMED' AND slot_id = %s AND reschedule_count = %s""",
                            (target, db_time(tstart), db_time(tend), db_time(now), aid, original, expected))
                if cur.rowcount != 1:
                    conn.rollback()
                    return _no(409, "APPOINTMENT_CHANGED", "The appointment changed while it was being moved")
                conn.commit()
            except pymysql.err.IntegrityError as exc:
                conn.rollback()
                if is_dup(exc, "active_patient_slot"):
                    return _no(409, "DUPLICATE_BOOKING", "That person is already booked in that slot")
                raise
    emit("info", "reschedule_moved", cid, appointmentId=aid, fromSlot=original, toSlot=target)
    return {"ok": True}


def _release_with_flag(slot_id, key, flag, flag_cond, cid, attempts):
    """-1 booked seat on slot_id together with `flag` on the reschedule record (one transaction, so a retried step
    never releases twice). -> 'released' | 'already' | 'inconsistent'. Raises SeatReleaseError when it keeps failing."""
    for attempt in range(1, attempts + 1):
        try:
            ddb.transact_write_items(TransactItems=[
                {"Update": _seat_update(slot_id, -1, "booked_count > :zero", {"zero": 0})},
                {"Update": _flag_update(key, flag, flag_cond, {"t": True})}])
            return "released"
        except Exception as exc:  # noqa: BLE001
            code = _code(exc)
            if code == "TransactionCanceledException":
                reasons = [r.get("Code") for r in (exc.response.get("CancellationReasons") or [])]
                if len(reasons) > 1 and reasons[1] == "ConditionalCheckFailed":
                    return "already"
                if reasons and reasons[0] == "ConditionalCheckFailed":
                    emit("error", "seat_release_inconsistent", cid, slotId=slot_id, detail="booked_count was already 0", step=flag)
                    metric("SeatReleaseFailures", 1)
                    return "inconsistent"
            elif code not in TRANSIENT_DDB:
                raise SeatReleaseError(f"{type(exc).__name__} while releasing {slot_id}")
            if attempt < attempts:
                sleep(backoff(attempt))
    raise SeatReleaseError(f"could not release a seat on {slot_id}")


def saga_release_original(state, cid):
    req, v = state["request"], state["validate"]
    if req.get("faultInjection") == "release-original":
        emit("warning", "fault_injected", cid, step="RELEASE_ORIGINAL", appointmentId=req["appointmentId"])
        raise SeatReleaseError("Injected failure while releasing the original seat")
    key = req["idempotencyKey"]
    if resched_record(key).get("original_released"):
        return {"ok": True, "already": True}
    outcome = _release_with_flag(v["originalSlotId"], key, "original_released",
                                 "attribute_exists(idempotency_key) AND attribute_not_exists(original_released)", cid, 3)
    if outcome == "inconsistent":
        raise SeatReleaseError("the original slot had no booked seat to release")
    emit("info", "reschedule_original_released", cid, slotId=v["originalSlotId"], outcome=outcome)
    return {"ok": True}


def saga_revert_move(state, cid):
    """Compensation 1/2: put the appointment back on its original slot. Idempotent; decides whether the new seat must
    be given back. If the OLD seat was already released the move is really finished - going back would leave the
    appointment without a seat - so the saga goes forward to Finalize instead."""
    req, v = state["request"], state.get("validate") or {}
    key, aid, target = req["idempotencyKey"], int(req["appointmentId"]), req["targetSlotId"]
    rec = resched_record(key)
    if not v.get("ok"):
        return {"outcome": "not_moved", "releaseTarget": bool(rec.get("target_claimed"))}
    if rec.get("original_released"):
        emit("warning", "reschedule_completed_forward", cid, appointmentId=aid)
        return {"outcome": "completed_forward", "releaseTarget": False}
    original, expected = v["originalSlotId"], int(v["expectedCount"])
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            cur.execute("SELECT user_id FROM users WHERE user_id = %s FOR UPDATE", (int(req["patientId"]),))
            row = fetch_appointment(cur, aid, lock=True)
            if not row:
                conn.rollback()
                out = {"outcome": "missing", "releaseTarget": True}
            elif row["status"] != "CONFIRMED":
                conn.rollback()
                # Cancelled meanwhile (only the doctor can - patients are locked out). On the NEW slot its seat was
                # already given back by that cancellation (or the slot itself is cancelled): never release it twice.
                out = {"outcome": "cancelled", "releaseTarget": row["slot_id"] != target}
            elif row["slot_id"] == original and int(row["reschedule_count"] or 0) == expected:
                conn.rollback()
                out = {"outcome": "not_moved", "releaseTarget": True}
            elif row["slot_id"] == target and int(row["reschedule_count"] or 0) == expected + 1:
                cur.execute("""UPDATE appointments SET slot_id = %s, start_at = %s, end_at = %s, reschedule_count = %s, updated_at = %s
                               WHERE appointment_id = %s AND slot_id = %s AND status = 'CONFIRMED'""",
                            (original, db_time(parse_utc(v["originalStartAt"])), db_time(parse_utc(v["originalEndAt"])),
                             expected, db_time(utcnow()), aid, target))
                if cur.rowcount != 1:
                    conn.rollback()
                    raise RuntimeError("appointment changed during compensation")       # retried by Step Functions
                conn.commit()
                out = {"outcome": "reverted", "releaseTarget": True}
            else:
                conn.rollback()
                emit("error", "reschedule_revert_unexpected_state", cid, appointmentId=aid, slotId=row["slot_id"])
                out = {"outcome": "unexpected", "releaseTarget": row["slot_id"] != target}
    emit("warning", "reschedule_compensation_revert", cid, appointmentId=aid, **out)
    return out


def saga_release_target(state, cid):
    """Compensation 2/2: give the claimed seat on the new slot back (exactly once)."""
    req = state["request"]
    key = req["idempotencyKey"]
    rec = resched_record(key)
    if not rec.get("target_claimed") or rec.get("target_released"):
        return {"ok": True, "already": True}
    outcome = _release_with_flag(req["targetSlotId"], key, "target_released",
                                 "target_claimed = :t AND attribute_not_exists(target_released)", cid, 5)
    emit("warning", "reschedule_compensation_release_target", cid, slotId=req["targetSlotId"], outcome=outcome)
    return {"ok": True, "outcome": outcome}


def saga_finalize(state, cid):
    req, v = state["request"], state["validate"]
    key, aid, uid, target, original = req["idempotencyKey"], int(req["appointmentId"]), int(req["patientId"]), req["targetSlotId"], v["originalSlotId"]
    now, tiers, rcid = utcnow(), safe_tiers(cid), req.get("correlationId") or cid
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            row = fetch_appointment(cur, aid)
        conn.rollback()
    entries = []
    try:
        target_slot = get_slot(target)
    except Exception as exc:  # noqa: BLE001
        target_slot = None
        emit("warning", "post_move_slot_check_failed", cid, slotId=target, error=type(exc).__name__)
    if row["status"] == "CONFIRMED" and target_slot and target_slot.get("status") == "CANCELLED":
        # The doctor cancelled the new slot while we were moving into it: the sweep may have missed this row.
        with dbconn(db_config()) as conn:
            with conn.cursor() as cur:
                done = cancel_confirmed_by_doctor(cur, target, int(v["doctorId"]), utcnow(), "Slot cancelled by the doctor", aid)
                conn.commit()
                row = fetch_appointment(cur, aid)
        appt = appointment_json(row, "PATIENT", utcnow())
        if done:
            entries.append(cancelled_event(appt, appt["refund"], "DOCTOR"))
            metric("CancellationsByTier", 1, Tier=DOCTOR_TIER)
        status, payload = 409, msg(rcid, "The doctor cancelled the new time while your appointment was being moved. "
                                         "You have been refunded in full.", "SLOT_CANCELLED", appointment=appt)
    elif row["status"] == "CONFIRMED":
        appt = appointment_json(row, "PATIENT", now, tiers)
        prev_local = parse_utc(v["originalStartAt"]).astimezone(zone_of(v["timezone"]))
        previous = {"slotId": original, "startAt": v["originalStartAt"], "localDate": prev_local.date().isoformat(),
                    "localStart": prev_local.strftime("%H:%M")}
        entries.append(("AppointmentRescheduled", {
            **{k: appt[k] for k in ("appointmentId", "patientId", "doctorId", "slotId", "consultationTypeId", "startAt", "endAt",
                                    "rescheduleCount", "status")},
            "dependentId": appt["bookedFor"].get("dependentId"), "previousSlotId": original,
            "previousStartAt": v["originalStartAt"], "notify": {**notify_fields(appt), "previousLocalDate": previous["localDate"],
                                                                "previousLocalStart": previous["localStart"]}}))
        metric("ReschedulesSucceeded", 1)
        status, payload = 200, {"message": "Appointment rescheduled", "appointment": appt, "previous": previous, "correlationId": rcid}
    else:
        appt = appointment_json(row, "PATIENT", now)
        text = "This appointment was cancelled while it was being moved."
        if row["status"] == "CANCELLED_BY_DOCTOR":
            text = "The doctor cancelled this appointment while it was being moved. You have been refunded in full."
        status, payload = 409, msg(rcid, text, "APPOINTMENT_CANCELLED", appointment=appt)
    try:
        after = get_slot(original)
    except Exception:  # noqa: BLE001
        after = None
    if after and after.get("status") == "OPEN" and parse_utc(after["start_at"]) > utcnow() and free_seats(after) > 0:
        entries.append(("SlotSeatFreed", {"slotId": original, "doctorId": int(v["doctorId"]),
                                          "consultationTypeId": int(v["consultationTypeId"]), "startAt": after["start_at"],
                                          "localDate": after.get("local_date"), "freeSeats": free_seats(after),
                                          "capacity": int(after.get("capacity", 1)), "reason": "RESCHEDULE"}))
    resched_complete(key, status, payload)            # stored before publishing, exactly like a booking
    release_reschedule_lock(aid, req["executionName"])
    publish(entries, cid)
    emit("info", "reschedule_finished", cid, appointmentId=aid, status=status, fromSlot=original, toSlot=target)
    return {"ok": status == 200, "status": status}


def saga_record_failure(state, cid):
    """Terminal step for a rejected request or a compensated failure: store the answer, free the lock, count it."""
    req, v = state["request"], state.get("validate") or {}
    key, aid, rcid = req["idempotencyKey"], int(req["appointmentId"]), req.get("correlationId") or cid
    if state.get("failedStep"):
        revert = (state.get("revert") or {}).get("outcome")
        metric("ReschedulesCompensated", 1)
        emit("warning", "reschedule_compensated", cid, appointmentId=aid, step=state["failedStep"], revert=revert,
             error=(state.get("error") or {}).get("Error"), detail=str((state.get("error") or {}).get("Cause", ""))[:300])
        appt = None
        try:
            with dbconn(db_config()) as conn:
                with conn.cursor() as cur:
                    row = fetch_appointment(cur, aid)
                conn.rollback()
            appt = appointment_json(row, "PATIENT", utcnow(), safe_tiers(cid)) if row else None
        except Exception as exc:  # noqa: BLE001
            emit("warning", "reschedule_failure_lookup_failed", cid, error=type(exc).__name__)
        text = ("This appointment was cancelled while it was being moved." if revert == "cancelled" else
                "Your appointment could not be moved, so it stays at its original time. Nothing else changed.")
        status, payload = 409, msg(rcid, text, "RESCHEDULE_COMPENSATED", appointment=appt, failedStep=state["failedStep"])
    else:
        failure = next((state[k] for k in ("validate", "claim") if isinstance(state.get(k), dict) and state[k].get("ok") is False), None)
        if failure:
            status, code, text = int(failure["status"]), failure["code"], failure["message"]
        else:
            status, code, text = 503, "RESCHEDULE_FAILED", "The reschedule could not be completed. Your appointment was not changed."
        metric("ReschedulesRejected", 1, Reason=code)
        payload = msg(rcid, text, code)
    resched_complete(key, status, payload)
    release_reschedule_lock(aid, req["executionName"])     # only removes the lock if THIS execution holds it
    return {"recorded": True, "status": status}


def saga_compensation_failed(state, cid):
    req = state["request"]
    metric("SagaCompensationFailures", 1)
    emit("error", "reschedule_compensation_failed", cid, appointmentId=req["appointmentId"], targetSlot=req["targetSlotId"],
         failedStep=state.get("failedStep"), error=(state.get("error") or {}).get("Error"),
         compensationError=(state.get("compensationError") or {}).get("Error"))
    try:
        resched_complete(req["idempotencyKey"], 500, msg(req.get("correlationId") or cid,
                         "Something went wrong while moving your appointment and our team has been alerted. "
                         "Please check your appointments before trying again.", "RESCHEDULE_NEEDS_ATTENTION"))
    except Exception as exc:  # noqa: BLE001
        emit("error", "reschedule_record_failed", cid, error=type(exc).__name__)
    # The lock is deliberately KEPT until its lease runs out: nobody should act on a half-moved appointment meanwhile.
    return {"recorded": True}


SAGA_STEPS = {"rescheduleValidate": saga_validate, "rescheduleClaimTarget": saga_claim_target, "rescheduleMove": saga_move,
              "rescheduleReleaseOriginal": saga_release_original, "rescheduleRevertMove": saga_revert_move,
              "rescheduleReleaseTarget": saga_release_target, "rescheduleFinalize": saga_finalize,
              "rescheduleRecordFailure": saga_record_failure, "rescheduleCompensationFailed": saga_compensation_failed}


# =========================================================================== Milestone 6: accept a waitlist offer
# The waitlist Lambda HOLDS a seat for the first patient in line (held_count + 1 on the slot, an Offers item LIVE until
# expires_at). Accepting turns that hold into a confirmed appointment:
#   1. one DynamoDB transaction: offer LIVE -> ACCEPTING (only if still unexpired and the caller's) AND the slot's
#      held seat becomes a booked seat (held - 1, booked + 1). The seat never becomes visible to anyone else.
#   2. RDS: the normal booking rules + INSERT, with idempotency_key OFFER#<offerId> as the duplicate backstop.
#   3. offer -> ACCEPTED, waitlist entry -> BOOKED, AppointmentBooked published.
# Anything that fails before the INSERT commits puts the seat back on hold and the offer back to LIVE.
OFFER_ID_RE = re.compile(r"OFR-[0-9a-f]{32}")
ACCEPT_LEASE_SECONDS = 60


def get_offer(offer_id):
    res = ddb.get_item(TableName=offers_table(), Key={"offer_id": {"S": offer_id}}, ConsistentRead=True)
    return _item(res.get("Item"))


def revert_accept(offer, attempt_id, cid):
    """Seat back on hold, offer back to LIVE (the patient may still accept before it expires)."""
    try:
        ddb.transact_write_items(TransactItems=[
            {"Update": _seat_update(offer["slot_id"], -1, "booked_count > :zero", {"zero": 0}, delta_held=+1)},
            {"Update": {"TableName": offers_table(), "Key": {"offer_id": {"S": offer["offer_id"]}},
                        "UpdateExpression": "SET #st = :live REMOVE attempt_id, accepting_lease",
                        "ConditionExpression": "#st = :acc AND attempt_id = :a", "ExpressionAttributeNames": {"#st": "status"},
                        "ExpressionAttributeValues": _vals(live="LIVE", acc="ACCEPTING", a=attempt_id)}}])
        emit("info", "offer_accept_reverted", cid, offerId=offer["offer_id"])
    except Exception as exc:  # noqa: BLE001 - the expiry sweeper resolves an ACCEPTING offer once its lease ends
        emit("error", "offer_accept_revert_failed", cid, offerId=offer["offer_id"], error=type(exc).__name__)


def appointment_by_key(cur, key):
    cur.execute("SELECT appointment_id FROM appointments WHERE idempotency_key = %s", (key,))
    row = cur.fetchone()
    return int(row["appointment_id"]) if row else None


def accept_offer(offer_id, uid, cid):
    now = utcnow()
    now_s = int(now.timestamp())
    offer = get_offer(offer_id)
    if not offer or str(offer.get("patient_id")) != str(uid):
        if offer:
            emit("warning", "offer_isolation_violation", cid, callerId=uid, offerId=offer_id)
        return respond(404, msg(cid, "Offer not found", "OFFER_NOT_FOUND"))
    key, status = f"OFFER#{offer_id}", offer.get("status")
    if status == "ACCEPTED":
        with dbconn(db_config()) as conn:
            with conn.cursor() as cur:
                aid = appointment_by_key(cur, key)
                row = fetch_appointment(cur, aid) if aid else None
        if row:
            return respond(200, {"message": "You already accepted this offer", "alreadyAccepted": True,
                                 "appointment": appointment_json(row, "PATIENT", now, safe_tiers(cid)), "correlationId": cid})
    if status in ("EXPIRED", "DECLINED", "VOID", "CLOSED", "ACCEPTED"):
        return respond(410, msg(cid, {"EXPIRED": "This offer has expired", "DECLINED": "You declined this offer",
                                      "VOID": "The doctor cancelled that slot, so the offer was withdrawn. You are back on the waitlist.",
                                      "CLOSED": "The doctor is no longer available that day"}.get(status, "This offer has ended"),
                                "OFFER_" + ("ENDED" if status == "ACCEPTED" else status)))
    if status == "ACCEPTING" and int(offer.get("accepting_lease", 0)) > now_s:
        return respond(409, msg(cid, "This offer is being accepted right now. Refresh in a moment.", "REQUEST_IN_PROGRESS"),
                       {"Retry-After": "2"})
    if status == "LIVE" and int(offer.get("expires_at", 0)) <= now_s:
        return respond(410, msg(cid, "This offer has expired", "OFFER_EXPIRED"))
    attempt_id = uuid.uuid4().hex
    if status == "LIVE":
        for _ in range(10):
            slot = get_slot(offer["slot_id"])
            if not slot or slot.get("status") != "OPEN" or parse_utc(slot["start_at"]) <= now:
                return respond(409, msg(cid, "This seat is no longer available", "SLOT_CLOSED"))
            if int(slot.get("held_count", 0) or 0) <= 0:
                emit("error", "offer_without_held_seat", cid, offerId=offer_id, slotId=offer["slot_id"])
                return respond(409, msg(cid, "This seat is no longer available", "SLOT_CLOSED"))
            try:
                ddb.transact_write_items(TransactItems=[
                    {"Update": {"TableName": offers_table(), "Key": {"offer_id": {"S": offer_id}},
                                "UpdateExpression": "SET #st = :acc, attempt_id = :a, accepting_lease = :l",
                                "ConditionExpression": "#st = :live AND expires_at > :now AND patient_id = :me",
                                "ExpressionAttributeNames": {"#st": "status"},
                                "ExpressionAttributeValues": _vals(acc="ACCEPTING", a=attempt_id, l=now_s + ACCEPT_LEASE_SECONDS,
                                                                   live="LIVE", now=now_s, me=str(uid))}},
                    {"Update": {"TableName": slots_table(), "Key": slot_table_key(offer["slot_id"]),
                                "UpdateExpression": "SET held_count = :h, booked_count = :b, version = :nv, updated_at = :t",
                                "ConditionExpression": "version = :v AND #st = :open",
                                "ExpressionAttributeNames": {"#st": "status"},
                                "ExpressionAttributeValues": _vals(h=int(slot["held_count"]) - 1, b=int(slot.get("booked_count", 0) or 0) + 1,
                                                                   nv=int(slot["version"]) + 1, v=int(slot["version"]),
                                                                   open="OPEN", t=iso(now))}}])
                break
            except Exception as exc:  # noqa: BLE001
                if _code(exc) == "TransactionCanceledException":
                    reasons = [r.get("Code") for r in (exc.response.get("CancellationReasons") or [])]
                    if reasons and reasons[0] == "ConditionalCheckFailed":     # expired / taken meanwhile: answer from the item
                        latest = get_offer(offer_id)
                        if latest and latest.get("status") != "LIVE":
                            return accept_offer(offer_id, uid, cid)
                        return respond(410, msg(cid, "This offer has expired", "OFFER_EXPIRED"))
                elif _code(exc) not in TRANSIENT_DDB:
                    raise
                sleep(backoff(1))
        else:
            return respond(409, msg(cid, "This seat is in high demand right now. Please try again.", "SLOT_BUSY"), {"Retry-After": "1"})
    else:   # ACCEPTING with an expired lease: an earlier attempt died after moving the seat. Take it over (no seat change).
        try:
            ddb.update_item(TableName=offers_table(), Key={"offer_id": {"S": offer_id}},
                            UpdateExpression="SET attempt_id = :a, accepting_lease = :l",
                            ConditionExpression="#st = :acc AND accepting_lease < :now", ExpressionAttributeNames={"#st": "status"},
                            ExpressionAttributeValues=_vals(a=attempt_id, l=now_s + ACCEPT_LEASE_SECONDS, acc="ACCEPTING", now=now_s))
        except Exception as exc:  # noqa: BLE001
            if _code(exc) == "ConditionalCheckFailedException":
                return respond(409, msg(cid, "This offer is being accepted right now. Refresh in a moment.", "REQUEST_IN_PROGRESS"))
            raise

    settings, tiers = load_settings(cid), safe_tiers(cid)
    doctor_id, type_id = int(offer["doctor_id"]), int(offer["consultation_type_id"])
    dep = offer.get("dependent_id")
    dep = int(dep) if dep not in (None, "", 0, "0") else None
    start, end = parse_utc(offer["start_at"]), parse_utc(offer["end_at"])
    problem, appointment_id, commit_attempted = None, None, False
    try:
        with dbconn(db_config()) as conn:
            with conn.cursor() as cur:
                try:
                    appointment_id = appointment_by_key(cur, key)
                    if appointment_id is None:
                        if not patient_ready(cur, uid):
                            problem = ("ACCOUNT_INACTIVE", "This account cannot book appointments")
                        cur.execute("""SELECT dct.fee, d.timezone FROM doctor_consultation_types dct
                                       JOIN consultation_types ct ON ct.consultation_type_id = dct.consultation_type_id AND ct.is_active = 1
                                       JOIN doctors d ON d.doctor_id = dct.doctor_id
                                       JOIN users du ON du.user_id = d.doctor_id AND du.is_active = 1
                                       WHERE dct.doctor_id = %s AND dct.consultation_type_id = %s AND dct.is_active = 1""",
                                    (doctor_id, type_id))
                        offered = cur.fetchone()
                        dependent = active_dependent(cur, uid, dep) if dep is not None else None
                        who = f"{dependent['first_name']} {dependent['last_name']}" if dependent else None
                        if problem:
                            pass
                        elif not offered:
                            problem = ("SLOT_UNAVAILABLE", "This doctor is not taking bookings for that consultation type")
                        elif dep is not None and not dependent:
                            problem = ("DEPENDENT_NOT_FOUND", "That family member is not on your account")
                        elif blackout_on(cur, doctor_id, start, zone_of(offered["timezone"]), lock=True):
                            problem = ("DOCTOR_UNAVAILABLE", "The doctor is not available on this day")
                        else:
                            problem = check_limits(cur, uid, dep, doctor_id, offer["slot_id"], start, end,
                                                   zone_of(offered["timezone"]), settings, now, who)
                        if problem:
                            conn.rollback()
                        else:
                            cur.execute(
                                """INSERT INTO appointments (patient_id, doctor_id, consultation_type_id, slot_id, start_at, end_at,
                                                             fee_snapshot, status, dependent_id, idempotency_key, created_at, updated_at)
                                   VALUES (%s, %s, %s, %s, %s, %s, %s, 'CONFIRMED', %s, %s, %s, %s)""",
                                (uid, doctor_id, type_id, offer["slot_id"], db_time(start), db_time(end), money(offered["fee"]),
                                 dep, key, db_time(now), db_time(now)))
                            appointment_id = int(cur.lastrowid)
                            commit_attempted = True
                            conn.commit()
                except pymysql.err.IntegrityError as exc:
                    conn.rollback()
                    if is_dup(exc, "idempotency_key"):
                        appointment_id = appointment_by_key(cur, key)
                    elif is_dup(exc, "active_patient_slot"):
                        problem = ("DUPLICATE_BOOKING", "You are already booked in this slot" if dep is None else "That person is already booked in this slot")
                    else:
                        raise
    except Exception:
        if not commit_attempted:
            revert_accept(offer, attempt_id, cid)
        raise
    if problem:
        revert_accept(offer, attempt_id, cid)
        emit("info", "offer_accept_rejected", cid, offerId=offer_id, code=problem[0])
        return respond(409, msg(cid, problem[1] + ". The seat stays held for you until the offer expires.", problem[0]))

    for upd in ({"TableName": offers_table(), "Key": {"offer_id": {"S": offer_id}},
                 "UpdateExpression": "SET #st = :ok, appointment_id = :aid, accepted_at = :t, purge_at = :p",
                 "ConditionExpression": "attempt_id = :a", "ExpressionAttributeNames": {"#st": "status"},
                 "ExpressionAttributeValues": _vals(ok="ACCEPTED", aid=appointment_id, t=iso(utcnow()), a=attempt_id,
                                                    p=now_s + 30 * 86400)},
                {"TableName": waitlist_table(), "Key": {"waitlist_key": {"S": offer["waitlist_key"]}, "entry_key": {"S": offer["entry_key"]}},
                 "UpdateExpression": "SET #st = :booked, appointment_id = :aid, ended_at = :t",
                 "ConditionExpression": "offer_id = :oid", "ExpressionAttributeNames": {"#st": "status"},
                 "ExpressionAttributeValues": _vals(booked="BOOKED", aid=appointment_id, t=iso(utcnow()), oid=offer_id)}):
        try:
            ddb.update_item(**upd)
        except Exception as exc:  # noqa: BLE001 - the booking is committed; the sweeper reconciles ACCEPTING offers
            emit("warning", "offer_finalise_failed", cid, offerId=offer_id, table=upd["TableName"], error=type(exc).__name__)
    metric("OffersAccepted", 1)
    with dbconn(db_config()) as conn:
        with conn.cursor() as cur:
            row = fetch_appointment(cur, appointment_id)
    appt = appointment_json(row, "PATIENT", now, tiers)
    try:
        after = get_slot(offer["slot_id"])
    except Exception:  # noqa: BLE001
        after = None
    if after and after.get("status") == "CANCELLED":
        with dbconn(db_config()) as conn:
            with conn.cursor() as cur:
                done = cancel_confirmed_by_doctor(cur, offer["slot_id"], doctor_id, utcnow(), "Slot cancelled by the doctor", appointment_id)
                conn.commit()
                row = fetch_appointment(cur, appointment_id)
        appt = appointment_json(row, "PATIENT", utcnow())
        if done:
            publish([cancelled_event(appt, appt["refund"], "DOCTOR")], cid)
        return respond(409, msg(cid, "The doctor cancelled this slot while you were accepting. You have been refunded in full.",
                                "SLOT_CANCELLED", appointment=appt))
    publish([booked_event(appt, source="WAITLIST", offerId=offer_id)], cid)
    emit("info", "offer_accepted", cid, offerId=offer_id, appointmentId=appointment_id, patientId=uid, slotId=offer["slot_id"])
    return respond(201, {"message": "Offer accepted. Your appointment is confirmed.", "appointment": appt, "correlationId": cid})


# =========================================================================== routing
ROUTE_RE = re.compile(r"(/appointments(?:/[^/]+){0,2}|/doctors/me/slots/[^/]+/cancel|/patients/me/refunds"
                      r"|/patients/me/dependents(?:/[^/]+)?|/offers/[^/]+/accept)/?$")


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
    event = event if isinstance(event, dict) else {}
    cid = correlation(event)
    # Internal action from doctor_domain (direct, IAM-authorised Lambda invoke). API Gateway events always carry
    # httpMethod + requestContext, so this branch can never be reached from the public API.
    if event.get("action") and "httpMethod" not in event and "requestContext" not in event:
        step = SAGA_STEPS.get(event.get("action"))
        if step:   # Milestone 6: a reschedule saga step from Step Functions. Errors propagate so the state machine
            return step(event.get("input") or {}, (event.get("input") or {}).get("request", {}).get("correlationId") or cid)
        if event.get("action") != "cancelBlackoutDay":
            return {"ok": False, "message": "Unknown action"}
        try:
            return cancel_day_by_doctor(event.get("doctorId"), event.get("date"), event.get("reason"), cid)
        except Exception as exc:  # noqa: BLE001
            emit("error", "blackout_day_sweep_failed", cid, error=type(exc).__name__, detail=str(exc)[:300],
                 traceback=traceback.format_exc()[-1500:])
            return {"ok": False, "message": "Blackout sweep failed"}
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
        m = re.fullmatch(r"/appointments/([^/]+)(/cancel|/status|/reschedule)?", path)
        if m:
            appointment_id, bad = appointment_id_of(m.group(1), cid)
            if bad:
                return bad
            if m.group(2) == "/reschedule":
                if method != "POST":
                    return respond(405, msg(cid, "Method not allowed"))
                if role != "PATIENT":
                    return respond(403, msg(cid, "Only the patient can reschedule an appointment"))
                return reschedule(event, appointment_id, uid, cid)
            if m.group(2) == "/status":
                if method != "POST":
                    return respond(405, msg(cid, "Method not allowed"))
                if role != "DOCTOR":
                    return respond(403, msg(cid, "Only the doctor can update an appointment's status"))
                return update_status_by_doctor(event, appointment_id, uid, cid)
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
        m = re.fullmatch(r"/offers/([^/]+)/accept", path)
        if m:
            if method != "POST":
                return respond(405, msg(cid, "Method not allowed"))
            if role != "PATIENT":
                return respond(403, msg(cid, "Patient role required"))
            offer_id = unquote((event.get("pathParameters") or {}).get("offerId") or m.group(1))
            if not OFFER_ID_RE.fullmatch(offer_id):
                return respond(422, msg(cid, "offerId is not valid", "OFFER_ID_INVALID"))
            return accept_offer(offer_id, uid, cid)
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
