"""Milestone 4 checkpoint, automated: patient profiles, isolation, doctor search, public profile and slot availability."""
import itertools
import math
from datetime import date, timedelta

import pytest

from conftest import (DOCTOR_FIRST, DOCTOR_LAST, PASSWORD, QUICK_FEE, close_account, login, make_patient, all_pages)


def ids(items):
    return {i["doctorId"] for i in items}


# --------------------------------------------------------------------------- profile CRUD
def test_registered_patient_can_read_own_profile(api, patient_a):
    r = api.get("/patients/me", patient_a.token)
    assert r.status_code == 200, r.text
    p = r.json()["profile"]
    assert p["patientId"] == patient_a.id and p["email"] == patient_a.email
    assert p["firstName"] == "Itest" and p["dateOfBirth"] == "1991-03-14" and p["gender"] == "Female"
    assert api.get(f"/patients/{patient_a.id}", patient_a.token).status_code == 200


def test_update_profile_persists(api, patient_a):
    r = api.call("PATCH", "/patients/me", patient_a.token, json={"address": "12 Integration Road", "lastName": "Updated"})
    assert r.status_code == 200, r.text
    p = api.get("/patients/me", patient_a.token).json()["profile"]
    assert p["address"] == "12 Integration Road" and p["lastName"] == "Updated"


@pytest.mark.parametrize("bad", [{"phone": "123"}, {"dateOfBirth": "2999-01-01"}, {"gender": "Banana"}, {"firstName": ""}, {"email": "x@y.z"}, {"patientId": 1}])
def test_invalid_updates_are_rejected_with_422(api, patient_a, bad):
    r = api.call("PATCH", "/patients/me", patient_a.token, json=bad)
    assert r.status_code in (400, 422), r.text
    assert api.get("/patients/me", patient_a.token).json()["profile"]["email"] == patient_a.email


def test_patient_endpoints_need_a_token(api):
    assert api.get("/patients/me").status_code in (401, 403)
    assert api.get("/patients/me", "not-a-token").status_code in (401, 403)


# --------------------------------------------------------------------------- isolation
def test_patient_cannot_read_another_patients_profile(api, patient_a, patient_b):
    r = api.get(f"/patients/{patient_b.id}", patient_a.token)
    assert r.status_code == 403, r.text
    assert patient_b.email not in r.text and "Itest" not in r.text
    r = api.get(f"/patients/{patient_a.id}", patient_b.token)
    assert r.status_code == 403
    # each still sees only their own profile through /me
    assert api.get("/patients/me", patient_a.token).json()["profile"]["patientId"] == patient_a.id
    assert api.get("/patients/me", patient_b.token).json()["profile"]["patientId"] == patient_b.id


def test_patient_cannot_write_to_another_patients_profile(api, patient_a, patient_b):
    before = api.get("/patients/me", patient_b.token).json()["profile"]
    for method in ("PATCH", "PUT", "DELETE"):
        r = api.call(method, f"/patients/{patient_b.id}", patient_a.token, json={"firstName": "Hacked", "password": PASSWORD})
        assert r.status_code in (403, 404, 405), (method, r.status_code)
    assert api.get("/patients/me", patient_b.token).json()["profile"] == before


def test_patient_cannot_use_doctor_endpoints(api, patient_a):
    assert api.get("/doctors/me", patient_a.token).status_code in (401, 403)


def test_doctor_cannot_use_patient_endpoints(api, doctor):
    assert api.get("/patients/me", doctor.token).status_code in (401, 403)


# --------------------------------------------------------------------------- search
def test_search_requires_sign_in(api):
    assert api.get("/doctors/search").status_code in (401, 403)


def test_filters_lists_hospitals_specializations_and_cities(api, patient_a):
    b = api.get("/doctors/filters", patient_a.token).json()
    assert b["hospitals"] and b["specializations"] and b["cities"]
    assert {"hospitalId", "name", "city"} <= set(b["hospitals"][0])


def test_search_by_each_single_filter(api, patient_a, doctor):
    for params in ({"name": DOCTOR_FIRST}, {"name": DOCTOR_LAST.lower()}, {"hospitalId": doctor.hospital_id},
                   {"specializationId": doctor.specialization_id}, {"city": doctor.city}, {"city": doctor.city.upper()}):
        items, _ = all_pages(api, patient_a.token, params)
        assert doctor.id in ids(items), params


def test_search_every_filter_combination_finds_the_doctor_and_a_wrong_value_excludes_it(api, patient_a, doctor):
    right = {"name": f"{DOCTOR_FIRST} {DOCTOR_LAST}", "hospitalId": doctor.hospital_id, "specializationId": doctor.specialization_id, "city": doctor.city}
    wrong = {"name": "Zzzzqq", "hospitalId": 10 if doctor.hospital_id != 10 else 9, "specializationId": 30 if doctor.specialization_id != 30 else 29,
             "city": "Nowhereville"}
    keys = list(right)
    for size in range(1, len(keys) + 1):
        for combo in itertools.combinations(keys, size):
            items, _ = all_pages(api, patient_a.token, {k: right[k] for k in combo})
            assert doctor.id in ids(items), f"expected the doctor for {combo}"
            for bad in combo:
                params = {k: (wrong[k] if k == bad else right[k]) for k in combo}
                items, _ = all_pages(api, patient_a.token, params)
                assert doctor.id not in ids(items), f"{bad} was wrong but the doctor still matched {combo}"


def test_search_matches_word_prefixes_and_is_case_insensitive(api, patient_a, doctor):
    for term in (DOCTOR_FIRST[:4], DOCTOR_LAST.upper(), f"{DOCTOR_LAST[:3].lower()} {DOCTOR_FIRST[:3].lower()}"):
        items, _ = all_pages(api, patient_a.token, {"name": term})
        assert doctor.id in ids(items), term


@pytest.mark.parametrize("term", ["%", "_", "\\", "' OR 1=1 --", "<script>", "Integration%", "%%%%%%"])
def test_search_treats_wildcards_and_sql_as_plain_text(api, patient_a, doctor, term):
    r = api.get("/doctors/search", patient_a.token, params={"name": term})
    assert r.status_code in (200, 422), r.text[:300]
    if r.status_code == 200 and term in ("%", "_", "\\", "' OR 1=1 --", "%%%%%%"):
        assert doctor.id not in ids(r.json()["items"])


def test_search_with_no_match_returns_an_empty_page(api, patient_a):
    b = api.get("/doctors/search", patient_a.token, params={"name": "Qqqqzzzz"}).json()
    assert b["items"] == [] and b["total"] == 0 and b["hasNext"] is False and b["hasPrevious"] is False


@pytest.mark.parametrize("params", [{"hospitalId": "abc"}, {"hospitalId": "-1"}, {"specializationId": "0"}, {"page": "0"}, {"page": "x"},
                                    {"pageSize": "0"}, {"pageSize": "51"}, {"pageSize": "1000"}])
def test_search_rejects_malformed_parameters(api, patient_a, params):
    assert api.get("/doctors/search", patient_a.token, params=params).status_code == 422


def test_pagination_is_consistent_and_stable(api, patient_a, doctor):
    first = api.get("/doctors/search", patient_a.token, params={"pageSize": 1, "page": 1}).json()
    assert first["pageSize"] == 1 and len(first["items"]) <= 1 and first["page"] == 1
    assert first["totalPages"] == max(math.ceil(first["total"] / 1), 1)
    seen = []
    page = 1
    while page <= min(first["totalPages"], 5):
        b = api.get("/doctors/search", patient_a.token, params={"pageSize": 1, "page": page}).json()
        seen += [i["doctorId"] for i in b["items"]]
        assert b["hasPrevious"] == (page > 1) and b["hasNext"] == (page < b["totalPages"])
        page += 1
    assert len(seen) == len(set(seen)), "a doctor appeared on two pages"
    again = api.get("/doctors/search", patient_a.token, params={"pageSize": 1, "page": 1}).json()
    assert again["items"] == first["items"], "ordering is not deterministic"
    beyond = api.get("/doctors/search", patient_a.token, params={"page": first["totalPages"] + 5}).json()
    assert beyond["items"] == []


def test_search_cards_show_fee_and_never_leak_private_fields(api, patient_a, doctor):
    items, _ = all_pages(api, patient_a.token, {"name": DOCTOR_LAST})
    card = next(i for i in items if i["doctorId"] == doctor.id)
    assert card["startingFee"] is not None and float(card["startingFee"]) <= QUICK_FEE and card["acceptingBookings"] is True
    for field in ("email", "phone", "licenseNumber", "license_number", "passwordHash"):
        assert field not in card


# --------------------------------------------------------------------------- public doctor profile
def test_public_profile_returns_consultation_types_and_fees(api, patient_a, doctor):
    r = api.get(f"/doctors/{doctor.id}", patient_a.token)
    assert r.status_code == 200, r.text
    b = r.json()
    assert b["doctor"]["displayName"] == f"Dr. {DOCTOR_FIRST} {DOCTOR_LAST}"
    quick = next(t for t in b["consultationTypes"] if t["consultationTypeId"] == 1)
    assert float(quick["fee"]) == QUICK_FEE and quick["durationMinutes"] > 0
    for field in ("email", "phone", "licenseNumber", "license_number"):
        assert field not in b["doctor"]


def test_unknown_or_malformed_doctor_ids(api, patient_a):
    assert api.get("/doctors/999999999", patient_a.token).status_code == 404
    assert api.get("/doctors/abc", patient_a.token).status_code in (404, 422)
    assert api.get("/doctors/0", patient_a.token).status_code in (404, 422)


def test_doctor_pages_need_sign_in(api, doctor):
    assert api.get(f"/doctors/{doctor.id}").status_code in (401, 403)
    assert api.get(f"/doctors/{doctor.id}/slots").status_code in (401, 403)


# --------------------------------------------------------------------------- slots
def test_slot_availability_returns_free_seats_per_slot(api, patient_a, doctor):
    today = date.today()
    r = api.get(f"/doctors/{doctor.id}/slots", patient_a.token, params={"from": today.isoformat(), "to": (today + timedelta(days=6)).isoformat()})
    assert r.status_code == 200, r.text
    b = r.json()
    assert len(b["days"]) >= 6 and b["totalSlots"] > 0
    slots = [s for d in b["days"] for s in d["slots"]]
    assert slots
    for s in slots:
        assert s["capacity"] >= 1 and 0 <= s["freeSeats"] <= s["capacity"] and s["available"] == (s["freeSeats"] > 0)
        assert s["localStart"] < s["localEnd"] and s["consultationTypeName"] and s["fee"] is not None
    assert b["totalFreeSeats"] == sum(s["freeSeats"] for s in slots)
    starts = [s["startAt"] for d in b["days"] for s in d["slots"]]
    assert starts == sorted(starts)


def test_slot_filters(api, patient_a, doctor):
    base = {"from": date.today().isoformat(), "to": (date.today() + timedelta(days=3)).isoformat()}
    quick = api.get(f"/doctors/{doctor.id}/slots", patient_a.token, params={**base, "consultationTypeId": 1}).json()
    assert all(s["consultationTypeId"] == 1 for d in quick["days"] for s in d["slots"])
    not_offered = api.get(f"/doctors/{doctor.id}/slots", patient_a.token, params={**base, "consultationTypeId": 4})
    assert not_offered.status_code == 422, "a type this doctor does not offer is a validation error, not an empty list"
    free = api.get(f"/doctors/{doctor.id}/slots", patient_a.token, params={**base, "availableOnly": "true"}).json()
    assert all(s["available"] for d in free["days"] for s in d["slots"])


def test_slot_default_window_and_past_dates(api, patient_a, doctor):
    r = api.get(f"/doctors/{doctor.id}/slots", patient_a.token)
    assert r.status_code == 200 and r.json()["days"]
    past = api.get(f"/doctors/{doctor.id}/slots", patient_a.token, params={"from": "2020-01-01", "to": "2020-01-03"})
    assert past.status_code in (200, 422)
    if past.status_code == 200:
        assert past.json()["totalSlots"] == 0 or all(d["date"] >= date.today().isoformat() for d in past.json()["days"])


@pytest.mark.parametrize("params", [{"from": "tomorrow"}, {"to": "2026-13-45"}, {"from": "2026-12-10", "to": "2026-12-01"},
                                    {"from": "2026-12-01", "to": "2027-03-01"}, {"consultationTypeId": "abc"}])
def test_slot_range_validation(api, patient_a, doctor, params):
    assert api.get(f"/doctors/{doctor.id}/slots", patient_a.token, params=params).status_code == 422


def test_slots_for_unknown_doctor_is_404(api, patient_a):
    assert api.get("/doctors/999999999/slots", patient_a.token).status_code == 404


# --------------------------------------------------------------------------- close account
def test_close_account_flow(api, run_id):
    p = make_patient(api, f"it-{run_id}-c@example.com")
    assert api.call("DELETE", "/patients/me", p.token, json={}).status_code in (400, 422)
    wrong = api.call("DELETE", "/patients/me", p.token, json={"password": "Wrong-password-1!"})
    assert wrong.status_code == 403
    assert api.get("/patients/me", p.token).status_code == 200, "a wrong password must not close or sign out the account"
    ok = close_account(api, p)
    assert ok.status_code == 200, ok.text
    assert api.get("/patients/me", p.token).status_code in (401, 403), "the old token must stop working"
    resp, _ = login(api, p.email)
    assert resp.status_code in (401, 403), "a closed account cannot sign in"


def test_closed_account_refresh_token_is_dead(api, run_id):
    p = make_patient(api, f"it-{run_id}-d@example.com")
    assert close_account(api, p).status_code == 200
    r = api.call("POST", "/auth/refresh", json={"refreshToken": p.refresh})
    assert r.status_code == 401


# --------------------------------------------------------------------------- registration edge cases
@pytest.mark.parametrize("override,expect", [({"phone": "12345"}, 400), ({"gender": "x"}, 400), ({"dateOfBirth": "31-12-1990"}, 400),
                                             ({"firstName": ""}, 400), ({"email": "not-an-email"}, 400), ({"password": "short"}, 400)])
def test_registration_rejects_bad_input(api, run_id, override, expect):
    data = {"firstName": "Itest", "lastName": "Bad", "email": f"it-{run_id}-bad@example.com", "password": PASSWORD, "phone": "9123456780",
            "dateOfBirth": "1991-03-14", "gender": "Male"}
    data.update(override)
    r = api.call("POST", "/auth/register/patient", json=data)
    assert r.status_code == expect, r.text
    assert "errors" in r.json() or "message" in r.json()


def test_registration_requires_every_mandatory_field(api, run_id):
    base = {"firstName": "Itest", "lastName": "Missing", "email": f"it-{run_id}-miss@example.com", "password": PASSWORD, "phone": "9123456781",
            "dateOfBirth": "1991-03-14", "gender": "Male"}
    for field in ("firstName", "lastName", "email", "password", "phone", "dateOfBirth", "gender"):
        data = {k: v for k, v in base.items() if k != field}
        assert api.call("POST", "/auth/register/patient", json=data).status_code == 400, field


def test_duplicate_email_is_a_conflict(api, patient_a):
    data = {**patient_a.data, "phone": "9123456782"}
    assert api.call("POST", "/auth/register/patient", json=data).status_code == 409