"""Blackout dates cancel what is already booked, and the doctor's "Update status" button.

Offline, on the same fakes as test_milestone5.py (SQLite for RDS, fake DynamoDB / EventBridge). The doctor_domain
Lambda's call to the booking Lambda is wired straight into booking.handler, so the blackout tests run the real
end-to-end path: POST /doctors/me/blackout-dates -> doctor_domain -> booking "cancelBlackoutDay" -> refunds.
"""
import io
import json
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from support import api_event, load_lambda
from test_milestone5 import World, at_api, book_api, booking, call_api  # noqa: F401

IST = ZoneInfo("Asia/Kolkata")
doctor_domain = load_lambda("doctor_domain", "doctor_domain_for_blackout")


@pytest.fixture
def w(monkeypatch):
    world = World(monkeypatch)
    world.doc = world.doctor("Ravi", "Kumar", types=((1, 300.0, 1), (4, 500.0, 3)))
    world.doc2 = world.doctor("Meena", "Rao", types=((2, 700.0, 1),))
    world.p1, world.p2, world.p3 = world.patient(1), world.patient(2), world.patient(3)

    class BookingLambda:          # doctor_domain -> booking, in-process
        def __init__(self):
            self.calls, self.fail = [], False

        def invoke(self, FunctionName, InvocationType, Payload):
            self.calls.append(json.loads(Payload))
            if self.fail:
                raise TimeoutError("booking Lambda unreachable (injected)")
            out = booking.handler(json.loads(Payload), None)
            return {"StatusCode": 200, "Payload": io.BytesIO(json.dumps(out).encode())}

    world.booking_lambda = BookingLambda()
    world.refreshes = []
    monkeypatch.setattr(doctor_domain, "config", lambda: {"env": "dev"})
    monkeypatch.setattr(doctor_domain, "dbconn", world.db.connect)
    monkeypatch.setattr(doctor_domain, "booking_client", world.booking_lambda)
    monkeypatch.setattr(doctor_domain, "refresh_slots", lambda uid, cid: world.refreshes.append(uid))
    monkeypatch.setenv("BOOKING_FUNCTION", "medislot-dev-booking")
    return world


def add_blackout(w, day, reason="Conference", doctor=None):
    ev = api_event("POST", "/doctors/me/blackout-dates", user_id=doctor or w.doc, role="DOCTOR",
                   body={"blackoutDate": day.isoformat() if hasattr(day, "isoformat") else day, "reason": reason})
    res = doctor_domain.handler(ev, None)
    return res["statusCode"], json.loads(res["body"])


def local_day(slot_start):
    return slot_start.astimezone(IST).date()


def set_status(w, appointment_id, status, doctor=None, note=None):
    body = {"status": status}
    if note:
        body["note"] = note
    return call_api("POST", f"/appointments/{appointment_id}/status", doctor or w.doc, role="DOCTOR", body=body)


# ================================================================================================ blackout dates
def test_blackout_cancels_every_booked_appointment_that_day_with_a_full_refund(w):
    start = at_api(5)                                     # 10:00 UTC = 15:30 IST, same calendar day
    s1 = w.slot(w.doc, start=start)
    s2 = w.slot(w.doc, type_id=4, start=start + timedelta(hours=2), minutes=60, capacity=3)
    other_day = w.slot(w.doc, start=at_api(6))
    other_doc = w.slot(w.doc2, type_id=2, start=start, minutes=30)
    a1 = book_api(w.p1, s1)[1]["appointment"]["appointmentId"]
    a2 = book_api(w.p2, s2)[1]["appointment"]["appointmentId"]
    a3 = book_api(w.p3, s2)[1]["appointment"]["appointmentId"]
    keep1 = book_api(w.p1, other_day)[1]["appointment"]["appointmentId"]
    keep2 = book_api(w.p3, other_doc)[1]["appointment"]["appointmentId"]

    st, data = add_blackout(w, local_day(start), reason="Medical conference")
    assert st == 201 and data["cancelledAppointments"] == 3 and data["sweepOk"]
    assert "3 booked appointments" in data["message"]
    rows = {r["appointment_id"]: r for r in w.appointments()}
    for a in (a1, a2, a3):
        assert rows[a]["status"] == "CANCELLED_BY_DOCTOR"
        assert "unavailable" in rows[a]["cancellation_note"] and "Medical conference" in rows[a]["cancellation_note"]
    assert rows[keep1]["status"] == "CONFIRMED" and rows[keep2]["status"] == "CONFIRMED"
    assert [(l["refund_percentage"], l["refund_tier"]) for l in w.ledger()] == [(100, "DOCTOR_CANCELLED")] * 3
    assert w.dynamo.slot(s1)["status"] == "CANCELLED" and w.dynamo.slot(s2)["status"] == "CANCELLED"
    assert w.dynamo.slot(other_day)["status"] == "OPEN"
    assert w.events.types().count("AppointmentCancelled") == 3
    assert w.refreshes == [w.doc]                         # unbooked slots are still closed by the materialiser
    # the patient sees it as cancelled by the doctor, with the refund
    mine = call_api("GET", f"/appointments/{a1}", w.p1)[1]["appointment"]
    assert mine["status"] == "CANCELLED_BY_DOCTOR" and mine["refund"]["refundAmount"] == 300


def test_re_adding_an_existing_blackout_runs_the_sweep_again(w):
    """The reported bug: the blackout was saved earlier, the appointment stayed. Adding the date again fixes it."""
    start = at_api(4)
    s = w.slot(w.doc, start=start)
    a = book_api(w.p1, s)[1]["appointment"]["appointmentId"]
    with w.db.lock:      # a blackout saved by the OLD code: the appointment was never cancelled
        w.db.db.execute("INSERT INTO blackout_dates (doctor_id, blackout_date, reason) VALUES (?,?,?)",
                        (w.doc, local_day(start).isoformat(), "old"))
        w.db.db.commit()
    assert w.appointments(appointment_id=a)[0]["status"] == "CONFIRMED"
    st, data = add_blackout(w, local_day(start))
    assert st == 200 and data["alreadyExisted"] and data["cancelledAppointments"] == 1
    assert w.appointments(appointment_id=a)[0]["status"] == "CANCELLED_BY_DOCTOR"
    # and once more: nothing left to do, no second refund
    st, data = add_blackout(w, local_day(start))
    assert st == 200 and data["cancelledAppointments"] == 0 and len(w.ledger()) == 1


def test_booking_on_a_blacked_out_day_is_refused(w):
    start = at_api(7)
    s = w.slot(w.doc, start=start)
    assert add_blackout(w, local_day(start))[0] == 201
    st, data = book_api(w.p1, s)
    assert st == 409 and data["code"] == "DOCTOR_UNAVAILABLE"
    assert w.appointments() == [] and w.dynamo.slot(s)["booked_count"] == 0


def test_blackout_uses_the_doctors_local_day(w):
    # 20:00 UTC on day N is 01:30 IST on day N+1: blacking out day N (IST) must NOT touch it.
    start = at_api(5, hour=20)
    s = w.slot(w.doc, start=start)
    a = book_api(w.p1, s)[1]["appointment"]["appointmentId"]
    utc_day = start.date()
    assert local_day(start) == utc_day + timedelta(days=1)
    assert add_blackout(w, utc_day)[1]["cancelledAppointments"] == 0
    assert w.appointments(appointment_id=a)[0]["status"] == "CONFIRMED"
    assert add_blackout(w, local_day(start))[1]["cancelledAppointments"] == 1


def test_blackout_in_the_past_is_refused_and_bad_dates_are_rejected(w):
    yesterday = datetime.now(IST).date() - timedelta(days=1)
    assert add_blackout(w, yesterday)[0] == 422
    assert add_blackout(w, "30-11-2026")[0] == 422
    assert w.booking_lambda.calls == []


def test_when_the_sweep_cannot_run_the_blackout_is_saved_and_the_doctor_is_told_to_retry(w):
    start = at_api(3)
    s = w.slot(w.doc, start=start)
    a = book_api(w.p1, s)[1]["appointment"]["appointmentId"]
    w.booking_lambda.fail = True
    st, data = add_blackout(w, local_day(start))
    assert st == 201 and not data["sweepOk"] and "add the same date again" in data["message"]
    assert w.appointments(appointment_id=a)[0]["status"] == "CONFIRMED"
    w.booking_lambda.fail = False
    assert add_blackout(w, local_day(start))[1]["cancelledAppointments"] == 1


def test_internal_action_is_not_reachable_through_the_api(w):
    s = w.slot(w.doc, start=at_api(3))
    book_api(w.p1, s)
    ev = api_event("POST", "/appointments", user_id=w.doc, role="DOCTOR", body={})
    ev["action"] = "cancelBlackoutDay"
    ev["doctorId"], ev["date"] = w.doc, at_api(3).date().isoformat()
    res = booking.handler(ev, None)
    assert res["statusCode"] == 403                       # handled as a normal API request, not the internal action
    assert w.appointments()[0]["status"] == "CONFIRMED"


# ================================================================================================ doctor status button
def test_doctor_marks_a_started_appointment_completed_or_no_show(w):
    start = at_api(2)
    s = w.slot(w.doc, start=start)
    a = book_api(w.p1, s)[1]["appointment"]["appointmentId"]
    w.now = start + timedelta(minutes=20)
    st, data = set_status(w, a, "COMPLETED")
    assert st == 200 and data["appointment"]["status"] == "COMPLETED"
    assert data["appointment"]["allowedStatuses"] == ["NO_SHOW"]
    assert "AppointmentStatusChanged" in w.events.types()
    st, data = set_status(w, a, "NO_SHOW", note="Did not arrive")        # fixing a mis-click
    assert st == 200 and w.appointments(appointment_id=a)[0]["status"] == "NO_SHOW"
    assert set_status(w, a, "NO_SHOW")[1].get("unchanged")
    assert w.ledger() == []                                              # no refund for attendance outcomes
    assert call_api("GET", f"/appointments/{a}", w.p1)[1]["appointment"]["status"] == "NO_SHOW"


def test_completed_or_no_show_before_the_start_is_refused(w):
    s = w.slot(w.doc, start=at_api(2))
    a = book_api(w.p1, s)[1]["appointment"]["appointmentId"]
    st, data = set_status(w, a, "COMPLETED")
    assert st == 409 and data["code"] == "NOT_STARTED"
    listed = call_api("GET", "/appointments", w.doc, role="DOCTOR")[1]["items"][0]
    assert listed["allowedStatuses"] == ["CANCELLED_BY_DOCTOR"]


def test_doctor_cancels_one_future_appointment_with_full_refund_and_the_seat_is_freed(w):
    s = w.slot(w.doc, type_id=4, start=at_api(3), minutes=60, capacity=3)
    a1 = book_api(w.p1, s)[1]["appointment"]["appointmentId"]
    a2 = book_api(w.p2, s)[1]["appointment"]["appointmentId"]
    st, data = set_status(w, a1, "CANCELLED_BY_DOCTOR", note="Patient asked to move")
    assert st == 200 and data["refund"]["refundAmount"] == 500 and data["refund"]["tier"] == "DOCTOR_CANCELLED"
    rows = {r["appointment_id"]: r for r in w.appointments()}
    assert rows[a1]["status"] == "CANCELLED_BY_DOCTOR" and rows[a1]["cancellation_note"] == "Patient asked to move"
    assert rows[a2]["status"] == "CONFIRMED"
    slot = w.dynamo.slot(s)
    assert slot["status"] == "OPEN" and slot["booked_count"] == 1
    assert "SlotSeatFreed" in w.events.types()
    # final: no further change, no second refund
    assert set_status(w, a1, "COMPLETED")[1]["code"] == "NOT_CHANGEABLE"
    assert set_status(w, a1, "CANCELLED_BY_DOCTOR")[1].get("unchanged") and len(w.ledger()) == 1


def test_doctor_cannot_cancel_a_started_appointment(w):
    start = at_api(1)
    s = w.slot(w.doc, start=start)
    a = book_api(w.p1, s)[1]["appointment"]["appointmentId"]
    w.now = start + timedelta(minutes=1)
    assert set_status(w, a, "CANCELLED_BY_DOCTOR")[1]["code"] == "ALREADY_STARTED"


def test_status_update_access_and_validation(w):
    s = w.slot(w.doc, start=at_api(2))
    a = book_api(w.p1, s)[1]["appointment"]["appointmentId"]
    assert set_status(w, a, "COMPLETED", doctor=w.doc2)[1]["code"] == "APPOINTMENT_NOT_FOUND"     # not their patient
    assert call_api("POST", f"/appointments/{a}/status", w.p1, body={"status": "COMPLETED"})[0] == 403
    assert set_status(w, a, "CONFIRMED")[1]["code"] == "STATUS_INVALID"
    assert set_status(w, a, "CANCELLED_BY_PATIENT")[1]["code"] == "STATUS_INVALID"
    assert call_api("GET", f"/appointments/{a}/status", w.doc, role="DOCTOR")[0] == 405
    assert set_status(w, 0, "COMPLETED")[1]["code"] == "APPOINTMENT_ID_INVALID"


# ================================================================================================ doctor dashboard (Flask)
from test_milestone5 import FakeResp, env, iso, login_doctor, owner_view  # noqa: E402,F401  (env is a fixture)


def appt_item(aid, status, start, allowed):
    return {"appointmentId": aid, "status": status, "patientId": 9, "startAt": iso(start), "localDate": start.date().isoformat(),
            "localStart": "09:00", "localEnd": "09:15", "fee": 300, "consultationType": {"name": "Quick Consult"},
            "patient": {"id": 9, "name": "Asha Rao"}, "allowedStatuses": allowed}


def test_appointments_page_shows_the_update_status_button_with_the_allowed_choices(env):
    login_doctor(env)
    now = datetime.now(timezone.utc)
    items = [appt_item(41, "CONFIRMED", now - timedelta(hours=1), ["COMPLETED", "NO_SHOW"]),
             appt_item(42, "CONFIRMED", now + timedelta(days=1), ["CANCELLED_BY_DOCTOR"]),
             appt_item(43, "CANCELLED_BY_PATIENT", now - timedelta(days=1), [])]
    env.route("GET", "/appointments", FakeResp(200, {"items": items, "total": 3, "totalPages": 1}))
    page = env.client.get("/?m=appointments&tab=past").get_data(as_text=True)
    assert page.count("Update status</summary>") == 2
    assert 'action="/appointments/41/status"' in page and 'action="/appointments/42/status"' in page
    assert 'action="/appointments/43/status"' not in page                  # cancelled is final
    assert "Mark as completed" in page and "Mark as no-show" in page and "Cancel (refund patient in full)" in page
    assert 'name="back" value="/?m=appointments&amp;tab=past"' in page


def test_update_status_posts_to_the_api_and_returns_to_the_same_page(env):
    login_doctor(env)
    env.route("POST", "/appointments/41/status", FakeResp(200, {"message": "Appointment marked as completed"}))
    r = env.client.post("/appointments/41/status", data={"status": "COMPLETED", "note": "  all  good ", "back": "/?m=appointments&tab=past"})
    assert r.headers["Location"].endswith("/?m=appointments&tab=past")
    call = [c for c in env.calls if c[1] == "/appointments/41/status"][0]
    assert call[2]["json"] == {"status": "COMPLETED", "note": "all good"}


def test_update_status_guards(env):
    login_doctor(env)
    r = env.client.post("/appointments/41/status", data={"status": "CANCELLED_BY_DOCTOR", "back": "https://evil.example/"})
    assert r.headers["Location"].endswith("/?m=appointments")              # no confirmation + unsafe back -> nothing sent
    env.client.post("/appointments/41/status", data={"status": "CONFIRMED"})
    assert not [c for c in env.calls if c[1].endswith("/status")]


def test_schedule_view_offers_the_status_button_for_its_appointments(env):
    login_doctor(env)
    past = datetime.now(timezone.utc) - timedelta(hours=1)
    day = past.date().isoformat()
    appts = [{"appointmentId": 77, "patientId": 9, "slotId": "SLOT#3_20301120T060000Z_1", "consultationTypeId": 1,
              "consultationTypeName": "Quick Consult", "startAt": iso(past), "endAt": iso(past + timedelta(minutes=15)),
              "fee": "300.00", "status": "CONFIRMED"}]
    env.route("GET", "/doctors/3/slots", FakeResp(200, owner_view(day, [], appts)))
    page = env.client.get("/schedule?date=" + day).get_data(as_text=True)
    assert 'action="/appointments/77/status"' in page and "Mark as completed" in page and "Cancel (refund" not in page


def test_blackout_banner_reports_the_cancelled_bookings(env):
    login_doctor(env)
    env.route("POST", "/doctors/me/blackout-dates", FakeResp(201, {"message": "Blackout date added. 2 booked appointments on this day were cancelled and refunded in full.", "sweepOk": True}))
    env.client.post("/blackout", data={"blackoutDate": "2030-11-30", "reason": "Leave"})
    env.route("GET", "/doctors/me/blackout-dates", FakeResp(200, {"items": []}))
    page = env.client.get("/?m=availability").get_data(as_text=True)
    assert "2 booked appointments on this day were cancelled" in page


# ================================================================================================ edit / remove a blackout
def blackout_row(w, day):
    rows = w.db.rows("SELECT * FROM blackout_dates WHERE doctor_id = ? AND blackout_date = ?", (w.doc, day.isoformat()))
    return rows[0] if rows else None


def edit_blackout(w, bid, body, method="PATCH", doctor=None):
    ev = api_event(method, f"/doctors/me/blackout-dates/{bid}", user_id=doctor or w.doc, role="DOCTOR", body=body)
    res = doctor_domain.handler(ev, None)
    return res["statusCode"], json.loads(res["body"])


def test_removing_a_blackout_opens_the_day_again(w):
    start = at_api(6)
    s = w.slot(w.doc, start=start)
    add_blackout(w, local_day(start))
    assert book_api(w.p1, s)[1]["code"] == "DOCTOR_UNAVAILABLE"
    bid = blackout_row(w, local_day(start))["blackout_id"]
    w.refreshes.clear()
    st, data = edit_blackout(w, bid, None, method="DELETE")
    assert st == 200 and "open again" in data["message"]
    assert blackout_row(w, local_day(start)) is None
    assert w.refreshes == [w.doc]                     # the materialiser rebuilds / reopens that day's slots
    assert book_api(w.p1, s)[0] == 201                # patients can book that day again
    assert edit_blackout(w, bid, None, method="DELETE")[0] == 404
    assert edit_blackout(w, 999, None, method="DELETE")[0] == 404


def test_another_doctor_cannot_edit_or_remove_my_blackout(w):
    day = local_day(at_api(6))
    add_blackout(w, day)
    bid = blackout_row(w, day)["blackout_id"]
    assert edit_blackout(w, bid, None, method="DELETE", doctor=w.doc2)[0] == 404
    assert edit_blackout(w, bid, {"reason": "x"}, doctor=w.doc2)[0] == 404
    assert blackout_row(w, day) is not None


def test_moving_a_blackout_cancels_the_new_day_and_opens_the_old_day(w):
    old_start, new_start = at_api(5), at_api(8)
    old_slot, new_slot = w.slot(w.doc, start=old_start), w.slot(w.doc, start=new_start)
    booked_new = book_api(w.p2, new_slot)[1]["appointment"]["appointmentId"]
    add_blackout(w, local_day(old_start))
    bid = blackout_row(w, local_day(old_start))["blackout_id"]
    st, data = edit_blackout(w, bid, {"blackoutDate": local_day(new_start).isoformat(), "reason": "Travel"})
    assert st == 200 and data["previousDate"] == local_day(old_start).isoformat()
    assert data["cancelledAppointments"] == 1 and "is open again" in data["message"]
    assert blackout_row(w, local_day(old_start)) is None and blackout_row(w, local_day(new_start))["reason"] == "Travel"
    assert w.appointments(appointment_id=booked_new)[0]["status"] == "CANCELLED_BY_DOCTOR"
    assert w.dynamo.slot(new_slot)["closed_reason"] == "BLACKOUT"             # reopenable if this blackout goes away
    assert book_api(w.p1, old_slot)[0] == 201                                  # old day bookable again


def test_editing_only_the_reason_does_not_cancel_anything_again(w):
    start = at_api(5)
    day = local_day(start)
    add_blackout(w, day, reason="Leave")
    bid = blackout_row(w, day)["blackout_id"]
    calls = len(w.booking_lambda.calls)
    st, data = edit_blackout(w, bid, {"blackoutDate": day.isoformat(), "reason": "Family function"})
    assert st == 200 and data["message"] == "Blackout reason updated"
    assert blackout_row(w, day)["reason"] == "Family function" and len(w.booking_lambda.calls) == calls


def test_edit_validation(w):
    d1, d2 = local_day(at_api(5)), local_day(at_api(6))
    add_blackout(w, d1)
    add_blackout(w, d2)
    bid = blackout_row(w, d1)["blackout_id"]
    assert edit_blackout(w, bid, {"blackoutDate": d2.isoformat()})[0] == 409          # already blocked
    yesterday = datetime.now(IST).date() - timedelta(days=1)
    assert edit_blackout(w, bid, {"blackoutDate": yesterday.isoformat()})[0] == 422
    assert edit_blackout(w, bid, {"blackoutDate": "soon"})[0] == 422
    with w.db.lock:      # a blackout that is already over can be removed but not edited
        w.db.db.execute("UPDATE blackout_dates SET blackout_date = ? WHERE blackout_id = ?", (yesterday.isoformat(), bid))
        w.db.db.commit()
    assert edit_blackout(w, bid, {"reason": "x"})[0] == 409
    assert edit_blackout(w, bid, None, method="DELETE")[0] == 200


# ------------------------------------------------------------------------------- slot materialiser reopens blackout slots
from test_slot_materialiser_window import IST as _IST, NOW, GuardedSlotsTable, materialiser, pattern  # noqa: E402


def test_materialiser_reopens_slots_a_removed_blackout_cancelled_but_not_hand_cancelled_ones():
    table = GuardedSlotsTable()
    desired, _ = materialiser.desired_slots_for_doctor([pattern()], set(), _IST, NOW, 7)   # Wed 25 Nov 09:00 + 09:30 IST
    materialiser.create_missing(table, desired, [])
    a, b = sorted(table.items, key=lambda i: i["slot_key"])
    a.update(status="CANCELLED", closed_reason="BLACKOUT", booked_count=1)
    b.update(status="CANCELLED", closed_reason="DOCTOR_CANCELLED", booked_count=1)
    # while 25 Nov is still blacked out the day is not desired: nothing is reopened
    blocked, _ = materialiser.desired_slots_for_doctor([pattern()], {"2026-11-25"}, _IST, NOW, 7)
    assert materialiser.reconcile_doctor(table, 12, _IST, blocked, NOW, 7) == (0, 0, 0)
    assert a["status"] == "CANCELLED"
    # blackout removed -> the blackout-cancelled slot comes back as an EMPTY open slot; the hand-cancelled one does not
    assert materialiser.reconcile_doctor(table, 12, _IST, desired, NOW, 7) == (0, 1, 0)
    assert a["status"] == "OPEN" and b["status"] == "CANCELLED"
    key, upd = table.updates[-1]
    assert key["slot_key"] == a["slot_key"] and "booked_count=:zero" in upd["UpdateExpression"]
    assert "REMOVE closed_reason" in upd["UpdateExpression"] and upd["ExpressionAttributeValues"][":r"] == "BLACKOUT"


def test_materialiser_creates_slots_for_a_day_that_was_blocked_before_they_were_ever_built():
    table = GuardedSlotsTable()
    blocked, _ = materialiser.desired_slots_for_doctor([pattern()], {"2026-11-25"}, _IST, NOW, 7)
    materialiser.create_missing(table, blocked, [])
    assert not [i for i in table.items if i["local_date"] == "2026-11-25"]
    desired, _ = materialiser.desired_slots_for_doctor([pattern()], set(), _IST, NOW, 7)
    assert materialiser.create_missing(table, desired, [])[0] == 2
    assert len([i for i in table.items if i["local_date"] == "2026-11-25" and i["status"] == "OPEN"]) == 2


# ------------------------------------------------------------------------------- dashboard: blackout table actions
def test_availability_page_has_edit_and_remove_for_each_blackout(env):
    login_doctor(env)
    tomorrow = (datetime.now(IST).date() + timedelta(days=1)).isoformat()
    yesterday = (datetime.now(IST).date() - timedelta(days=1)).isoformat()
    env.route("GET", "/doctors/me/blackout-dates", FakeResp(200, {"items": [
        {"blackout_id": 7, "blackout_date": yesterday, "reason": None},
        {"blackout_id": 8, "blackout_date": tomorrow, "reason": "Leave"}]}))
    page = env.client.get("/?m=availability").get_data(as_text=True)
    assert 'action="/blackout/8/update"' in page and 'action="/blackout/8/remove"' in page
    assert 'action="/blackout/7/update"' not in page and 'action="/blackout/7/remove"' in page    # past: remove only
    assert f'value="{tomorrow}"' in page and 'value="Leave"' in page and ">Past<" in page


def test_blackout_edit_and_remove_call_the_api(env):
    login_doctor(env)
    env.route("PATCH", "/doctors/me/blackout-dates/8", FakeResp(200, {"message": "Blackout moved from A to B. A is open again.", "sweepOk": True}))
    env.route("DELETE", "/doctors/me/blackout-dates/8", FakeResp(200, {"message": "Blackout on B removed. You are open again that day."}))
    r = env.client.post("/blackout/8/update", data={"blackoutDate": "2030-12-08", "reason": "Travel"})
    assert r.headers["Location"].endswith("/?m=availability")
    call = [c for c in env.calls if c[0] == "PATCH"][0]
    assert call[1] == "/doctors/me/blackout-dates/8" and call[2]["json"] == {"blackoutDate": "2030-12-08", "reason": "Travel"}
    env.client.post("/blackout/8/remove")
    assert [c for c in env.calls if c[0] == "DELETE" and c[1] == "/doctors/me/blackout-dates/8"]
