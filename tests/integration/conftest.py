"""Integration tests against the DEPLOYED MediSlot API (API Gateway -> Lambda -> RDS / DynamoDB).

They run in the deploy workflow right after the stacks are rolled out, with MEDISLOT_API_BASE set from the
auth stack's ApiUrl output. Without that variable every test here is skipped, so `pytest` stays fast and offline
for pull requests and for local runs.

Test data:
  * Patients are registered fresh for every run with unique e-mails and phone numbers, and closed again at the end
    (which also exercises the delete flow). Closing anonymises the row, so nothing identifiable is left behind.
  * One persistent "Integration TestDoctor" is created (or re-used) so the search and slot endpoints have a known
    doctor to find. It is a real, listed doctor in the dev database and will appear in search results.
"""
import math
import os
import random
import time
import uuid
from datetime import date, timedelta

import pytest
import requests

API = os.environ.get("MEDISLOT_API_BASE", "").rstrip("/")
PASSWORD = "Integr@tion-Test-9x"
DOCTOR_EMAIL = "integration.doctor@medislot-ci.example.com"
DOCTOR_PHONE = "9000099901"
DOCTOR_LICENSE = "TS-9901"
DOCTOR_FIRST, DOCTOR_LAST = "Integration", "TestDoctor"
HOSPITAL_ID, SPECIALIZATION_ID = 1, 1
QUICK_TYPE_ID, QUICK_FEE = 1, 250


def pytest_collection_modifyitems(config, items):
    if API:
        return
    skip = pytest.mark.skip(reason="MEDISLOT_API_BASE is not set (integration tests need the deployed API)")
    for item in items:
        if "tests/integration" in str(item.fspath).replace("\\", "/"):
            item.add_marker(skip)


class Api:
    """Tiny client: JSON in/out, retries a GET once or twice when API Gateway or a cold Lambda hiccups."""

    def call(self, method, path, token=None, json=None, params=None, retries=2):
        headers = {"x-correlation-id": "it-" + uuid.uuid4().hex[:12]}
        if token:
            headers["Authorization"] = "Bearer " + token
        for attempt in range(retries + 1):
            resp = requests.request(method, API + path, headers=headers, json=json, params=params, timeout=40)
            if method == "GET" and resp.status_code in (502, 503, 504) and attempt < retries:
                time.sleep(2 + attempt * 2)
                continue
            return resp
        return resp

    def get(self, path, token=None, **kw):
        return self.call("GET", path, token, **kw)

    def body(self, resp):
        try:
            return resp.json()
        except ValueError:
            return {}


@pytest.fixture(scope="session")
def api():
    return Api()


@pytest.fixture(scope="session")
def run_id():
    return uuid.uuid4().hex[:10]


def new_phone():
    return "9" + "".join(random.choice("0123456789") for _ in range(9))


def register_patient(api, email, **over):
    for _ in range(6):
        data = {"firstName": "Itest", "lastName": "Patient", "email": email, "password": PASSWORD, "phone": new_phone(),
                "dateOfBirth": "1991-03-14", "gender": "Female"}
        data.update(over)
        resp = api.call("POST", "/auth/register/patient", json=data)
        if resp.status_code == 201:
            return data
        if resp.status_code != 409 or "phone" in over:
            raise AssertionError(f"patient registration failed: {resp.status_code} {resp.text[:300]}")
    raise AssertionError("could not find a free phone number for the test patient")


def login(api, email, password=PASSWORD):
    resp = api.call("POST", "/auth/login", json={"email": email, "password": password})
    return resp, api.body(resp)


class Patient:
    def __init__(self, data, payload):
        self.data, self.email = data, data["email"]
        self.token, self.refresh = payload["accessToken"], payload.get("refreshToken")
        self.id = int(payload["userId"])


def make_patient(api, email):
    data = register_patient(api, email)
    resp, payload = login(api, email)
    assert resp.status_code == 200 and payload.get("role") == "PATIENT", resp.text[:300]
    return Patient(data, payload)


def close_account(api, patient):
    return api.call("DELETE", "/patients/me", patient.token, json={"password": PASSWORD, "refreshToken": patient.refresh})


@pytest.fixture(scope="session")
def patient_a(api, run_id):
    p = make_patient(api, f"it-{run_id}-a@example.com")
    yield p
    close_account(api, p)


@pytest.fixture(scope="session")
def patient_b(api, run_id):
    p = make_patient(api, f"it-{run_id}-b@example.com")
    yield p
    close_account(api, p)


class Doctor:
    pass


@pytest.fixture(scope="session")
def doctor(api):
    """The persistent integration doctor, with a consultation type and weekly hours on every day."""
    reg = api.call("POST", "/auth/register/doctor", json={
        "firstName": DOCTOR_FIRST, "lastName": DOCTOR_LAST, "email": DOCTOR_EMAIL, "password": PASSWORD, "phone": DOCTOR_PHONE,
        "hospitalId": HOSPITAL_ID, "specializationId": SPECIALIZATION_ID, "licenseNumber": DOCTOR_LICENSE})
    assert reg.status_code in (201, 409), f"doctor registration: {reg.status_code} {reg.text[:300]}"
    resp, payload = login(api, DOCTOR_EMAIL)
    assert resp.status_code == 200 and payload.get("role") == "DOCTOR", f"doctor login: {resp.status_code} {resp.text[:300]}"
    d = Doctor()
    d.token, d.id = payload["accessToken"], int(payload["userId"])

    t = api.call("POST", "/doctors/me/consultation-types", d.token, json={"consultationTypeId": QUICK_TYPE_ID, "fee": QUICK_FEE})
    assert t.status_code in (200, 201, 409), f"consultation type: {t.status_code} {t.text[:300]}"
    for weekday in range(1, 8):
        p = api.call("POST", "/doctors/me/availability-patterns", d.token,
                     json={"consultationTypeId": QUICK_TYPE_ID, "dayOfWeek": weekday, "startTime": "06:00", "endTime": "22:00"})
        assert p.status_code in (200, 201, 409), f"availability pattern {weekday}: {p.status_code} {p.text[:300]}"

    # Slots are materialised asynchronously into DynamoDB; wait until the first ones can be read.
    deadline = time.time() + 120
    while time.time() < deadline:
        s = api.get(f"/doctors/{d.id}/slots", d.token)
        if s.status_code == 200 and api.body(s).get("totalSlots", 0) > 0:
            break
        time.sleep(5)
    else:
        pytest.fail("the integration doctor's slots were not materialised within 2 minutes")

    prof = api.body(api.get(f"/doctors/{d.id}", d.token))["doctor"]
    d.city, d.hospital_id, d.specialization_id = prof["hospital"]["city"], prof["hospital"]["id"], prof["specialization"]["id"]
    return d


def all_pages(api, token, params, cap=60):
    """Every search result across every page (the dev database can hold other doctors too)."""
    items, page = [], 1
    while page <= cap:
        resp = api.get("/doctors/search", token, params={**params, "page": page, "pageSize": 50})
        assert resp.status_code == 200, resp.text[:300]
        body = resp.json()
        items += body["items"]
        if not body["hasNext"]:
            return items, body
        page += 1
    raise AssertionError("search never ended")
