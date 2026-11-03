"""Patient side of the shared dashboard, driven with Flask's test client and a fake API (no network, no AWS)."""
import importlib
import json
import os
import sys
from pathlib import Path

import pytest

WEB = Path(__file__).resolve().parents[1] / "web"


class FakeResp:
    def __init__(self, status=200, body=None):
        self.status_code = status
        self._body = body if body is not None else {}
        self.text = json.dumps(self._body)

    @property
    def ok(self):
        return self.status_code < 400

    def json(self):
        return self._body


FILTERS = {"hospitals": [{"hospitalId": 1, "name": "Apollo", "city": "Hyderabad"}], "specializations": [{"specializationId": 1, "name": "Cardiology"}],
           "cities": ["Hyderabad"]}
PROFILE = {"patientId": 7, "firstName": "Asha", "lastName": "Rao", "email": "asha@example.com", "phone": "9876543210",
           "dateOfBirth": "1990-05-17", "gender": "Female", "address": "<b>MG Road</b>"}


def card(i, name="Rao", fee=300):
    return {"doctorId": i, "firstName": "Anil", "lastName": name, "displayName": f"Dr. Anil {name}", "specialization": {"id": 1, "name": "Cardiology"},
            "hospital": {"id": 1, "name": "Apollo", "city": "Hyderabad"}, "yearsExperience": 10, "languages": ["English"], "photoKey": None,
            "startingFee": fee, "consultationTypeCount": 1, "acceptingBookings": fee is not None}


@pytest.fixture()
def env(monkeypatch):
    monkeypatch.setenv("MEDISLOT_API_BASE", "https://api.test/dev")
    monkeypatch.setenv("FLASK_SESSION_SECRET", "x" * 32)
    monkeypatch.setenv("DOCTOR_PHOTOS_BUCKET", "bucket")
    monkeypatch.syspath_prepend(str(WEB))
    for m in ("doctor_dashboard", "patient_portal"):
        sys.modules.pop(m, None)
    dash = importlib.import_module("doctor_dashboard")
    portal = importlib.import_module("patient_portal")
    portal._filters.update(at=0.0, data=None)
    calls = []
    state = {"routes": {}, "post": {}}

    def route(method, path, resp):
        state["routes"][(method, path)] = resp

    def fake_request(method, url, headers=None, timeout=None, **kw):
        path = url.replace("https://api.test/dev", "")
        calls.append((method, path, kw))
        r = state["routes"].get((method, path))
        if r is None:
            return FakeResp(404, {"message": "no route"})
        return r(kw) if callable(r) else r

    def fake_post(url, json=None, timeout=None, **kw):
        path = url.replace("https://api.test/dev", "")
        calls.append(("POST", path, {"json": json}))
        r = state["routes"].get(("POST", path))
        return (r(json) if callable(r) else r) if r else FakeResp(404, {"message": "no route"})

    monkeypatch.setattr(dash.requests, "request", fake_request)
    monkeypatch.setattr(dash.requests, "post", fake_post)
    route("GET", "/doctors/filters", FakeResp(200, FILTERS))
    route("GET", "/patients/me", FakeResp(200, {"profile": PROFILE}))
    client = dash.app.test_client()
    return type("Env", (), {"dash": dash, "portal": portal, "client": client, "calls": calls, "route": staticmethod(route)})


def login_patient(env):
    env.route("POST", "/auth/login", FakeResp(200, {"accessToken": "a", "refreshToken": "r", "role": "PATIENT", "userId": 7}))
    return env.client.post("/login", data={"email": "asha@example.com", "password": "pw", "as": "patient"})


# ----------------------------------------------------------------------------- sign in / register pages
def test_signed_out_page_offers_patient_tab_and_register_form(env):
    page = env.client.get("/?mode=signup&role=patient").get_data(as_text=True)
    assert "Register as a patient" in page and 'action="/signup/patient"' in page
    for field in ("firstName", "lastName", "email", "phone", "dateOfBirth", "gender", "password", "confirmPassword"):
        assert f'name="{field}"' in page
    assert "I’m a doctor" in page and "I’m a patient" in page


def test_doctor_pages_are_unchanged(env):
    page = env.client.get("/").get_data(as_text=True)
    assert "Doctor sign in" in page and 'action="/login"' in page and "Run your clinic hours" in page
    assert "Create your doctor account" in env.client.get("/?mode=signup").get_data(as_text=True)


def test_patient_login_and_register_shortcuts_redirect(env):
    assert env.client.get("/patient/login").headers["Location"].endswith("/?mode=signin&role=patient")
    assert env.client.get("/patient/register").headers["Location"].endswith("/?mode=signup&role=patient")


# ----------------------------------------------------------------------------- registration -> patient login
def good_form(**over):
    f = {"firstName": "Asha", "lastName": "Rao", "email": "asha@example.com", "phone": "9876543210", "dateOfBirth": "1990-05-17",
         "gender": "Female", "address": "", "password": "Passw0rd!xyz1", "confirmPassword": "Passw0rd!xyz1"}
    f.update(over)
    return f


def test_register_success_redirects_to_patient_login_with_email_prefilled(env):
    env.route("POST", "/auth/register/patient", FakeResp(201, {"message": "ok"}))
    r = env.client.post("/signup/patient", data=good_form())
    assert r.headers["Location"].endswith("/?mode=signin&role=patient")
    sent = [c for c in env.calls if c[1] == "/auth/register/patient"][0][2]["json"]
    assert sent["phone"] == "9876543210" and sent["dateOfBirth"] == "1990-05-17" and "address" not in sent
    page = env.client.get("/?mode=signin&role=patient").get_data(as_text=True)
    assert "Patient sign in" in page and 'value="asha@example.com"' in page and "Account created" in page


@pytest.mark.parametrize("missing", ["firstName", "lastName", "email", "phone", "dateOfBirth", "gender", "password"])
def test_register_requires_every_mandatory_field_before_calling_the_api(env, missing):
    r = env.client.post("/signup/patient", data=good_form(**{missing: " "}))
    assert r.headers["Location"].endswith("/?mode=signup&role=patient")
    assert not [c for c in env.calls if c[1] == "/auth/register/patient"]
    assert "Please fill in" in env.client.get("/?mode=signup&role=patient").get_data(as_text=True)


def test_register_password_mismatch_keeps_the_form_but_never_the_password(env):
    env.client.post("/signup/patient", data=good_form(confirmPassword="nope"))
    page = env.client.get("/?mode=signup&role=patient").get_data(as_text=True)
    assert "Passwords do not match" in page and 'value="Asha"' in page and "Passw0rd" not in page


def test_register_api_errors_are_shown_without_status_codes(env):
    env.route("POST", "/auth/register/patient", FakeResp(400, {"message": "Phone must be 10 digits"}))
    env.client.post("/signup/patient", data=good_form())
    page = env.client.get("/?mode=signup&role=patient").get_data(as_text=True)
    assert "Phone must be 10 digits" in page and "HTTP 400" not in page and "Sign-up failed: Phone" in page


def test_register_duplicate_gets_a_friendly_message(env):
    env.route("POST", "/auth/register/patient", FakeResp(409, {"message": "exists"}))
    env.client.post("/signup/patient", data=good_form())
    assert "already exists" in env.client.get("/?mode=signup&role=patient").get_data(as_text=True)


def test_register_upstream_failure_is_handled(env):
    def boom(url, **kw):
        raise env.dash.requests.ConnectionError("down")
    env.dash.requests.post = boom
    r = env.client.post("/signup/patient", data=good_form())
    assert r.status_code == 302 and "Could not reach" in env.client.get("/?mode=signup&role=patient").get_data(as_text=True)


# ----------------------------------------------------------------------------- login and role separation
def test_patient_login_lands_on_patient_home(env):
    r = login_patient(env)
    assert r.headers["Location"].endswith("/")
    assert env.client.get("/").headers["Location"].endswith("/patient")
    page = env.client.get("/patient").get_data(as_text=True)
    assert "Signed in as patient" in page and "Find a doctor" in page and "Asha" in page


def test_patient_using_the_doctor_form_is_told_to_use_patient_sign_in(env):
    env.route("POST", "/auth/login", FakeResp(200, {"accessToken": "a", "refreshToken": "r", "role": "PATIENT"}))
    r = env.client.post("/login", data={"email": "a@b.co", "password": "pw"})
    assert r.headers["Location"].endswith("/?mode=signin&role=patient")
    with env.client.session_transaction() as s:
        assert "access_token" not in s
    assert "patient account" in env.client.get("/?mode=signin&role=patient").get_data(as_text=True)


def test_doctor_using_patient_form_is_rejected(env):
    env.route("POST", "/auth/login", FakeResp(200, {"accessToken": "a", "role": "DOCTOR"}))
    r = env.client.post("/login", data={"email": "a@b.co", "password": "pw", "as": "patient"})
    assert r.headers["Location"].endswith("/")
    with env.client.session_transaction() as s:
        assert "access_token" not in s


def test_wrong_password_returns_to_the_patient_tab(env):
    env.route("POST", "/auth/login", FakeResp(401, {"message": "Invalid email or password"}))
    r = env.client.post("/login", data={"email": "a@b.co", "password": "x", "as": "patient"})
    assert r.headers["Location"].endswith("/?mode=signin&role=patient")
    assert "Invalid email or password" in env.client.get("/?mode=signin&role=patient").get_data(as_text=True)


def test_signed_out_users_cannot_open_patient_pages(env):
    for path in ("/patient", "/patient/profile", "/patient/doctors", "/patient/doctors/1", "/patient/doctor-photo/1/" + "a" * 32 + ".jpg"):
        r = env.client.get(path)
        assert r.status_code == 302 and "role=patient" in r.headers["Location"], path
    assert not env.calls


def test_doctor_session_cannot_open_patient_pages(env):
    with env.client.session_transaction() as s:
        s.update(access_token="a", role="DOCTOR")
    r = env.client.get("/patient/profile")
    assert r.status_code == 302 and r.headers["Location"].endswith("/")
    assert not [c for c in env.calls if c[1].startswith("/patients")]


def test_patient_session_cannot_open_doctor_pages(env):
    login_patient(env)
    for path in ("/schedule?date=2026-11-09", "/photo-image", "/?m=profile"):
        r = env.client.get(path)
        assert r.status_code == 302 and r.headers["Location"].endswith("/patient"), path
    assert env.client.post("/profile", data={"firstName": "x"}).headers["Location"].endswith("/patient")
    assert not [c for c in env.calls if c[1].startswith("/doctors/me")]


def test_logout_clears_the_patient_session(env):
    login_patient(env)
    env.route("POST", "/auth/logout", FakeResp(200, {}))
    env.client.get("/logout")
    assert env.client.get("/patient").status_code == 302


# ----------------------------------------------------------------------------- profile
def test_profile_page_shows_values_and_escapes_html(env):
    login_patient(env)
    page = env.client.get("/patient/profile").get_data(as_text=True)
    assert 'value="9876543210"' in page and 'value="1990-05-17"' in page and "asha@example.com" in page
    assert "<b>MG Road</b>" not in page and "&lt;b&gt;MG Road&lt;/b&gt;" in page


def test_profile_save_sends_patch_and_flashes_success(env):
    login_patient(env)
    env.route("PATCH", "/patients/me", FakeResp(200, {"message": "Profile updated", "profile": PROFILE}))
    r = env.client.post("/patient/profile", data={"firstName": "Asha", "lastName": "Rao", "phone": "9876543210", "dateOfBirth": "1990-05-17",
                                                    "gender": "Female", "address": ""})
    assert r.headers["Location"].endswith("/patient/profile")
    sent = [c for c in env.calls if c[0] == "PATCH"][0][2]["json"]
    assert sent["address"] == "" and sent["phone"] == "9876543210" and "email" not in sent
    assert "Profile saved" in env.client.get("/patient/profile").get_data(as_text=True)


def test_profile_save_blank_required_field_does_not_call_api_and_keeps_input(env):
    login_patient(env)
    page = env.client.post("/patient/profile", data={"firstName": "Asha", "lastName": "Rao", "phone": "", "dateOfBirth": "1990-05-17", "gender": "Female"}).get_data(as_text=True)
    assert "Please fill in your phone" in page and 'value="Asha"' in page
    assert not [c for c in env.calls if c[0] == "PATCH"]


def test_profile_save_api_rejection_shows_message_and_keeps_input(env):
    login_patient(env)
    env.route("PATCH", "/patients/me", FakeResp(422, {"message": "phone: must be 10 digits"}))
    page = env.client.post("/patient/profile", data={"firstName": "Asha", "lastName": "Rao", "phone": "12", "dateOfBirth": "1990-05-17", "gender": "Female"}).get_data(as_text=True)
    assert "phone: must be 10 digits" in page and 'value="12"' in page


def test_profile_load_failure_shows_a_friendly_state(env):
    login_patient(env)
    env.route("GET", "/patients/me", FakeResp(500, {"message": "boom"}))
    page = env.client.get("/patient/profile").get_data(as_text=True)
    assert "could not be loaded" in page


def test_close_account_needs_password_and_confirmation(env):
    login_patient(env)
    r = env.client.post("/patient/profile/delete", data={"password": "", "confirm": "yes"})
    assert r.headers["Location"].endswith("/patient/profile")
    env.client.post("/patient/profile/delete", data={"password": "pw"})
    assert not [c for c in env.calls if c[0] == "DELETE"]


def test_close_account_wrong_password_keeps_the_session(env):
    login_patient(env)
    env.route("DELETE", "/patients/me", FakeResp(403, {"message": "That password is not correct"}))
    r = env.client.post("/patient/profile/delete", data={"password": "bad", "confirm": "yes"})
    assert r.headers["Location"].endswith("/patient/profile")
    assert "not correct" in env.client.get("/patient/profile").get_data(as_text=True)


def test_close_account_success_signs_out(env):
    login_patient(env)
    env.route("DELETE", "/patients/me", FakeResp(200, {"message": "closed"}))
    r = env.client.post("/patient/profile/delete", data={"password": "pw", "confirm": "yes"})
    assert r.headers["Location"].endswith("/?mode=signin&role=patient")
    sent = [c for c in env.calls if c[0] == "DELETE"][0][2]["json"]
    assert sent == {"password": "pw", "refreshToken": "r"}
    assert env.client.get("/patient").status_code == 302


# ----------------------------------------------------------------------------- doctor search
def search_route(env, items, total=None, pages=1, has_next=False, has_prev=False):
    def handler(kw):
        env.last_params = kw.get("params")
        return FakeResp(200, {"items": items, "page": 1, "pageSize": 9, "total": total if total is not None else len(items), "totalPages": pages,
                              "hasPrevious": has_prev, "hasNext": has_next})
    env.route("GET", "/doctors/search", handler)


def test_search_page_renders_cards_with_fee_and_filters(env):
    login_patient(env)
    search_route(env, [card(1), card(2, "Zed", fee=None)])
    page = env.client.get("/patient/doctors").get_data(as_text=True)
    assert "2 doctors found" in page and "Dr. Anil Rao" in page and "₹300" in page and "Not available" in page
    assert 'name="name"' in page and 'name="hospitalId"' in page and 'name="specializationId"' in page and 'name="city"' in page
    assert "Cardiology" in page and "Apollo" in page


def test_every_filter_is_forwarded_and_cleaned(env):
    login_patient(env)
    search_route(env, [card(1)])
    env.client.get("/patient/doctors?name=%20%20sha%20%20rma&hospitalId=1&specializationId=1&city=Hyderabad&acceptingOnly=1&page=2")
    assert env.last_params == {"name": "sha rma", "hospitalId": "1", "specializationId": "1", "city": "Hyderabad", "acceptingOnly": "true", "page": 2, "pageSize": 9}


@pytest.mark.parametrize("qs,expected_absent", [("hospitalId=abc", "hospitalId"), ("hospitalId=-3", "hospitalId"), ("specializationId=0", "specializationId"),
                                                 ("hospitalId=1;DROP", "hospitalId"), ("acceptingOnly=maybe", "acceptingOnly"), ("name=%20%20", "name")])
def test_garbage_filters_are_dropped_before_reaching_the_api(env, qs, expected_absent):
    login_patient(env)
    search_route(env, [])
    assert env.client.get("/patient/doctors?" + qs).status_code == 200
    assert expected_absent not in env.last_params


@pytest.mark.parametrize("raw,expected", [("abc", 1), ("0", 1), ("-4", 1), ("99999999", 1), ("3", 3), ("", 1)])
def test_page_number_is_sanitised(env, raw, expected):
    login_patient(env)
    search_route(env, [])
    env.client.get("/patient/doctors?page=" + raw)
    assert env.last_params["page"] == expected


def test_long_name_is_truncated(env):
    login_patient(env)
    search_route(env, [])
    env.client.get("/patient/doctors?name=" + "a" * 500)
    assert len(env.last_params["name"]) == 100


def test_search_with_no_results_explains_and_offers_clear(env):
    login_patient(env)
    search_route(env, [], total=0)
    page = env.client.get("/patient/doctors?name=zzz").get_data(as_text=True)
    assert "0 doctors found" in page and "No doctors match" in page and "Clear filters" in page


def test_page_past_the_end_links_back(env):
    login_patient(env)
    search_route(env, [], total=4, pages=1)
    page = env.client.get("/patient/doctors?name=a&page=9").get_data(as_text=True)
    assert "past the last result" in page and 'href="/patient/doctors?name=a"' in page


def test_pagination_links_keep_the_filters(env):
    login_patient(env)
    search_route(env, [card(i) for i in range(1, 10)], total=180, pages=20, has_next=True, has_prev=True)
    page = env.client.get("/patient/doctors?city=Hyderabad&page=10").get_data(as_text=True)
    assert "Next →" in page and "Previous" in page
    assert "/patient/doctors?city=Hyderabad&amp;page=11" in page and "/patient/doctors?city=Hyderabad&amp;page=9" in page
    assert "/patient/doctors?city=Hyderabad&amp;page=20" in page and "/patient/doctors?city=Hyderabad&amp;page=1\"" in page
    assert "…" in page and 'aria-current="page"' in page


def test_search_api_failure_shows_friendly_error_without_internals(env):
    login_patient(env)
    env.route("GET", "/doctors/search", FakeResp(500, {"message": "Traceback secret"}))
    page = env.client.get("/patient/doctors").get_data(as_text=True)
    assert "Search is unavailable" in page and "secret" not in page


def test_search_validation_message_from_api_is_shown(env):
    login_patient(env)
    env.route("GET", "/doctors/search", FakeResp(422, {"message": "hospitalId must be a positive number"}))
    assert "hospitalId must be a positive number" in env.client.get("/patient/doctors").get_data(as_text=True)


def test_search_network_error_is_handled(env):
    login_patient(env)

    def boom(method, url, **kw):
        raise env.dash.requests.ConnectionError("down")
    env.dash.requests.request = boom
    page = env.client.get("/patient/doctors")
    assert page.status_code == 200 and "Search is unavailable" in page.get_data(as_text=True)


def test_filter_list_falls_back_when_the_api_cannot_supply_it(env):
    login_patient(env)
    env.route("GET", "/doctors/filters", FakeResp(500, {}))
    search_route(env, [])
    page = env.client.get("/patient/doctors").get_data(as_text=True)
    assert "Apollo Hospitals Jubilee Hills" in page and "Cardiology" in page


def test_doctor_names_from_the_api_are_escaped(env):
    login_patient(env)
    search_route(env, [card(1, name="<script>alert(1)</script>")])
    page = env.client.get("/patient/doctors").get_data(as_text=True)
    assert "<script>alert(1)</script>" not in page and "&lt;script&gt;" in page


def test_photo_key_must_match_the_exact_pattern(env):
    p = env.portal
    good = "doctors/5/" + "a" * 32 + ".jpg"
    assert p.photo_src(5, good) == "/patient/doctor-photo/5/" + "a" * 32 + ".jpg"
    for bad in (None, "", "doctors/6/" + "a" * 32 + ".jpg", "doctors/5/../x.jpg", "doctors/5/" + "a" * 32 + ".exe", 123):
        assert p.photo_src(5, bad) is None


def test_photo_route_rejects_bad_names(env):
    login_patient(env)
    assert env.client.get("/patient/doctor-photo/5/evil.jpg").status_code == 404


# ----------------------------------------------------------------------------- doctor page + slots
def doc_profile(types=True):
    return {"doctor": {"doctorId": 1, "firstName": "Anil", "lastName": "Rao", "displayName": "Dr. Anil Rao", "specialization": {"id": 1, "name": "Cardiology"},
                       "hospital": {"id": 1, "name": "Apollo", "address": "Road 72", "city": "Hyderabad", "state": "Telangana"}, "yearsExperience": 10,
                       "qualifications": "MBBS, MD", "languages": ["English", "Telugu"], "clinicAddress": "Clinic", "timezone": "Asia/Kolkata", "photoKey": None,
                       "acceptingBookings": types},
            "consultationTypes": [{"consultationTypeId": 1, "name": "Quick Consult", "durationMinutes": 15, "fee": 300, "capacity": 1},
                                  {"consultationTypeId": 4, "name": "Health Camp Session", "durationMinutes": 60, "fee": "150.00", "capacity": 10}] if types else []}


def slots_payload(days=None):
    return {"doctorId": 1, "timezone": "Asia/Kolkata", "from": "2026-11-04", "to": "2026-11-10", "fromWasInThePast": False, "totalSlots": 4, "totalFreeSeats": 8,
            "days": days if days is not None else [
                {"date": "2026-11-04", "weekday": "Wednesday", "freeSlots": 2, "slots": [
                    {"slotId": "a", "localStart": "09:00", "localEnd": "09:15", "consultationTypeName": "Quick Consult", "durationMinutes": 15, "fee": 300, "capacity": 1, "freeSeats": 1, "available": True},
                    {"slotId": "b", "localStart": "14:00", "localEnd": "15:00", "consultationTypeName": "Health Camp Session", "durationMinutes": 60, "fee": 150, "capacity": 10, "freeSeats": 2, "available": True},
                    {"slotId": "c", "localStart": "18:00", "localEnd": "18:15", "consultationTypeName": "Quick Consult", "durationMinutes": 15, "fee": 300, "capacity": 1, "freeSeats": 0, "available": False}]},
                {"date": "2026-11-05", "weekday": "Thursday", "freeSlots": 0, "slots": []}]}


def open_doctor(env, slots=None, profile=None):
    login_patient(env)
    env.route("GET", "/doctors/1", FakeResp(200, profile or doc_profile()))

    def handler(kw):
        env.last_slot_params = kw.get("params")
        return slots if slots is not None else FakeResp(200, slots_payload())
    env.route("GET", "/doctors/1/slots", handler)


def test_doctor_page_shows_fees_and_slot_cards_by_part_of_day(env):
    open_doctor(env)
    page = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert "Dr. Anil Rao" in page and "Quick Consult" in page and "₹300" in page and "₹150" in page and "up to 10 patients" in page
    assert "Morning" in page and "Afternoon" in page and "Evening" in page
    assert "1 seat" in page and "2 of 10 seats free" in page and "No seats left" in page and "Few seats left" in page
    assert "slot open" in page and "slot partial" in page and "slot booked" in page
    assert "<table" not in page.split("Available slots", 1)[1]
    assert "No slots on: Thu 5" in page and "2 of 3 slots free" in page


def test_slot_filters_are_forwarded(env):
    open_doctor(env)
    env.client.get("/patient/doctors/1?from=2026-11-04&to=2026-11-08&type=4&free=1")
    assert env.last_slot_params == {"from": "2026-11-04", "to": "2026-11-08", "consultationTypeId": "4", "availableOnly": "true"}


def test_bad_slot_filter_values_are_ignored(env):
    open_doctor(env)
    env.client.get("/patient/doctors/1?from=tomorrow&to=%27%3Bdrop&type=x&free=2")
    assert env.last_slot_params == {}


def test_slot_api_validation_message_is_shown(env):
    open_doctor(env, slots=FakeResp(422, {"message": "The date range can span at most 31 days"}))
    assert "at most 31 days" in env.client.get("/patient/doctors/1?from=2026-11-01&to=2027-03-01").get_data(as_text=True)


def test_slot_outage_still_shows_the_doctor(env):
    open_doctor(env, slots=FakeResp(503, {"message": "internal detail"}))
    page = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert "Dr. Anil Rao" in page and "Slot availability is unavailable" in page and "internal detail" not in page


def test_no_slots_message(env):
    open_doctor(env, slots=FakeResp(200, slots_payload(days=[{"date": "2026-11-04", "weekday": "Wednesday", "freeSlots": 0, "slots": []}])))
    assert "No slots in this date range" in env.client.get("/patient/doctors/1").get_data(as_text=True)


def test_doctor_without_types_does_not_request_slots(env):
    open_doctor(env, profile=doc_profile(types=False))
    page = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert "Not taking bookings yet" in page
    assert not [c for c in env.calls if c[1].endswith("/slots")]


def test_unknown_doctor_goes_back_to_search_with_a_message(env):
    login_patient(env)
    env.route("GET", "/doctors/99", FakeResp(404, {"message": "Doctor not found"}))
    r = env.client.get("/patient/doctors/99")
    assert r.headers["Location"].endswith("/patient/doctors")
    assert "could not be found" in env.client.get("/patient/doctors").get_data(as_text=True)


def test_non_numeric_doctor_id_is_404(env):
    login_patient(env)
    assert env.client.get("/patient/doctors/abc").status_code == 404


def test_slot_window_links_page_forward_and_back(env):
    open_doctor(env)
    page = env.client.get("/patient/doctors/1").get_data(as_text=True)
    assert "Later →" in page and "from=2026-11-11" in page and "to=2026-11-17" in page


def test_expired_access_token_is_refreshed_silently(env):
    login_patient(env)
    seq = iter([FakeResp(401, {"message": "expired"}), FakeResp(200, {"profile": PROFILE})])
    env.route("GET", "/patients/me", lambda kw: next(seq))
    env.route("POST", "/auth/refresh", FakeResp(200, {"accessToken": "new", "refreshToken": "r2"}))
    assert env.client.get("/patient/profile").status_code == 200
    with env.client.session_transaction() as s:
        assert s["access_token"] == "new"


def test_dead_session_sends_a_patient_back_to_the_patient_sign_in(env):
    login_patient(env)
    env.route("GET", "/patients/me", FakeResp(401, {"message": "expired"}))
    env.route("POST", "/auth/refresh", FakeResp(401, {"message": "revoked"}))
    r = env.client.get("/patient/profile")
    assert r.status_code == 302 and "role=patient" in r.headers["Location"]
