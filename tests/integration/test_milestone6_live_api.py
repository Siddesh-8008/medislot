"""Milestone 6 against the DEPLOYED API (run by the deploy workflow; skipped offline).

* Mandatory test 3 live: a reschedule with a forced failure while releasing the original seat is compensated - the
  patient still holds exactly ONE appointment, on the original slot - and the same reschedule without the fault works.
* Waitlist rules live: a day that still has free seats cannot be waitlisted; the listing works.
The fault is injected with the X-Fault-Injection header, which the booking Lambda honours only outside prod.
"""
import os
import uuid
from datetime import datetime, timedelta, timezone

import pytest

pytestmark = pytest.mark.skipif(os.environ.get("ENVIRONMENT", "dev") == "prod", reason="fault injection is disabled in prod")


def free_slots_on_one_day(api, doctor, patient):
    """Two free slots on the same local day, at least 24 h ahead (clear of the 6-hour reschedule cutoff)."""
    resp = api.get(f"/doctors/{doctor.id}/slots", patient.token, params={"availableOnly": "true"})
    assert resp.status_code == 200, resp.text[:300]
    soon = datetime.now(timezone.utc) + timedelta(hours=24)
    for day in resp.json()["days"]:
        slots = [s for s in day["slots"] if datetime.fromisoformat(s["startAt"].replace("Z", "+00:00")) > soon and s["freeSeats"] > 0]
        if len(slots) >= 2:
            return day["date"], slots[0]["slotId"], slots[1]["slotId"]
    pytest.skip("no day with two free slots in the next week")


def confirmed(api, patient):
    resp = api.get("/appointments", patient.token, params={"status": "CONFIRMED", "when": "upcoming", "pageSize": 50})
    assert resp.status_code == 200, resp.text[:300]
    return resp.json()["items"]


def test_live_mandatory3_forced_release_failure_keeps_exactly_one_appointment(api, doctor, patient_a):
    day, original, target = free_slots_on_one_day(api, doctor, patient_a)
    booked = api.call("POST", "/appointments", patient_a.token, json={"slotId": original},
                      headers={"Idempotency-Key": uuid.uuid4().hex})
    assert booked.status_code == 201, booked.text[:300]
    appointment_id = booked.json()["appointment"]["appointmentId"]
    try:
        r = api.call("POST", f"/appointments/{appointment_id}/reschedule", patient_a.token, json={"targetSlotId": target},
                     headers={"Idempotency-Key": uuid.uuid4().hex, "X-Fault-Injection": "release-original"})
        assert r.status_code == 409 and r.json()["code"] == "RESCHEDULE_COMPENSATED", r.text[:300]
        mine = [a for a in confirmed(api, patient_a) if a["doctorId"] == doctor.id]
        assert len(mine) == 1 and mine[0]["appointmentId"] == appointment_id and mine[0]["slotId"] == original
        # the target seat was given back: the same move now succeeds
        ok = api.call("POST", f"/appointments/{appointment_id}/reschedule", patient_a.token, json={"targetSlotId": target},
                      headers={"Idempotency-Key": uuid.uuid4().hex})
        assert ok.status_code == 200 and ok.json()["appointment"]["slotId"] == target, ok.text[:300]
        mine = [a for a in confirmed(api, patient_a) if a["doctorId"] == doctor.id]
        assert len(mine) == 1 and mine[0]["slotId"] == target and mine[0]["rescheduleCount"] == 1
    finally:
        api.call("POST", f"/appointments/{appointment_id}/cancel", patient_a.token, json={})


def test_live_waitlist_rules(api, doctor, patient_b):
    day, _, _ = free_slots_on_one_day(api, doctor, patient_b)
    r = api.call("POST", "/waitlists", patient_b.token, json={"doctorId": doctor.id, "date": day})
    assert r.status_code == 409 and r.json()["code"] == "SLOTS_AVAILABLE", r.text[:300]
    listing = api.get("/waitlists", patient_b.token)
    assert listing.status_code == 200 and listing.json()["maxActive"] >= 1
    assert api.get("/waitlists", doctor.token).status_code == 403
