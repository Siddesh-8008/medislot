"""MediSlot Milestone 7 - every offline test in ONE file.

  * Web app      - patient waitlists / offers / reschedule pages, the doctor's week view and patients per slot, and the
                   role guards in both directions (Flask test client + fake API, no network)
  * Observability - the medislot-<env>-operations dashboard shows every custom business metric; six alarm categories,
                   all delivering to SNS
  * Hardening    - IAM audit (no wildcard actions anywhere, '*' resources only where AWS allows nothing narrower),
                   backups / point-in-time recovery, cost allocation tags, budgets, drift / DR / teardown tooling
"""
import json
import re
from datetime import date, datetime, timedelta, timezone
from pathlib import Path

import yaml

from test_milestone5 import SLOT_A, _Loader, capture_headers, iso, login_doctor, owner_view
from test_patient_dashboard import FakeResp, env, login_patient  # noqa: F401 (env is a fixture)

ROOT = Path(__file__).resolve().parents[1]
OFFER = "OFR-" + "a" * 32


def template(name):
    return yaml.load((ROOT / "cloudformation" / name).read_text(), Loader=_Loader)


# ==============================================================================================================
# Web app - patient
# ==============================================================================================================
def waitlists_payload(offer=True):
    items = [{"waitlistId": "12_2026-12-01_0", "doctorId": 12, "doctorName": "Dr. Ravi Kumar", "date": "2026-12-01",
              "dependentId": None, "status": "ACTIVE", "position": 2}]
    if offer:
        items.append({"waitlistId": "13_2026-12-02_0", "doctorId": 13, "doctorName": "Dr. Meena Rao", "date": "2026-12-02",
                      "dependentId": None, "status": "OFFERED",
                      "offer": {"offerId": OFFER, "status": "LIVE", "localDate": "2026-12-02", "localStart": "11:00",
                                "timezone": "Asia/Kolkata", "expiresAt": "2026-12-01T05:45:00Z", "secondsLeft": 754}})
    return {"items": items, "offers": [i["offer"] for i in items if i.get("offer")], "recent": [
        {"waitlistId": "12_2026-11-20_0", "doctorId": 12, "doctorName": "Dr. Ravi Kumar", "date": "2026-11-20", "status": "EXPIRED"}],
        "maxActive": 5, "holdMinutes": 15}


def test_waitlists_page_shows_held_seats_with_accept_and_decline(env):
    login_patient(env)
    env.route("GET", "/waitlists", FakeResp(200, waitlists_payload()))
    env.route("GET", "/patients/me/dependents", FakeResp(200, {"items": []}))
    page = env.client.get("/patient/waitlists").get_data(as_text=True)
    assert "Seats held for you" in page and "12 min 34 s left" in page
    assert f'action="/patient/offers/{OFFER}/accept"' in page and f'action="/patient/offers/{OFFER}/decline"' in page
    assert 'action="/patient/waitlists/12_2026-12-01_0/leave"' in page and ">2<" in page          # place in line
    assert "Offer expired" in page and "up to 5 waitlists" in page and "Waitlists &amp; offers" in page


def test_waitlists_page_outage_is_friendly(env):
    login_patient(env)
    env.route("GET", "/waitlists", FakeResp(503, {"message": "down"}))
    page = env.client.get("/patient/waitlists").get_data(as_text=True)
    assert "could not be loaded" in page


def test_full_day_offers_join_the_waitlist_and_joining_sends_the_right_body(env):
    login_patient(env)
    day = (date.today() + timedelta(days=3)).isoformat()
    env.route("GET", "/doctors/12", FakeResp(200, {"doctor": {"doctorId": 12, "displayName": "Dr. Ravi Kumar", "firstName": "Ravi",
                                                              "lastName": "Kumar", "timezone": "Asia/Kolkata", "specialization": {"name": "Cardiology"},
                                                              "hospital": {"name": "Apollo"}},
                                                   "consultationTypes": [{"consultationTypeId": 1, "name": "Quick Consult", "fee": 300,
                                                                          "durationMinutes": 15, "capacity": 1}]}))
    env.route("GET", "/doctors/12/slots", FakeResp(200, {"from": day, "to": day, "timezone": "Asia/Kolkata", "totalFreeSeats": 0, "days": [
        {"date": day, "freeSlots": 0, "slots": [{"slotId": SLOT_A, "localStart": "11:00", "localEnd": "11:15", "capacity": 1, "freeSeats": 0,
                                                "consultationTypeName": "Quick Consult", "durationMinutes": 15, "fee": 300}]}]}))
    env.route("GET", "/patients/me/dependents", FakeResp(200, {"items": [{"dependentId": 5, "displayName": "Lakshmi Rao", "relation": "Mother"}]}))
    page = env.client.get(f"/patient/doctors/12?on={day}").get_data(as_text=True)
    assert "Join the waitlist for this day" in page and f'name="date" value="{day}"' in page
    env.route("POST", "/waitlists", lambda kw: FakeResp(201, {"waitlist": {"position": 3}}))
    r = env.client.post("/patient/waitlists", data={"doctorId": "12", "date": day, "dependentId": "5", "back": f"/patient/doctors/12?on={day}"})
    assert r.headers["Location"].endswith("/patient/waitlists")
    sent = [c for c in env.calls if c[0] == "POST" and c[1] == "/waitlists"][0][2]["json"]
    assert sent == {"doctorId": 12, "date": day, "dependentId": 5}


def test_tampered_waitlist_and_offer_forms_never_reach_the_api(env):
    login_patient(env)
    env.client.post("/patient/waitlists", data={"doctorId": "x", "date": "2026-12-01"})
    env.client.post("/patient/waitlists/../../x/leave")
    env.client.post("/patient/offers/OFR-nothex/accept")
    env.client.post(f"/patient/offers/{OFFER}/steal")
    assert not [c for c in env.calls if c[1].startswith(("/waitlists", "/offers"))]


def test_accepting_an_offer_opens_the_new_appointment(env):
    login_patient(env)
    env.route("POST", f"/offers/{OFFER}/accept", lambda kw: FakeResp(201, {"appointment": {
        "appointmentId": 77, "localDate": "2026-12-02", "localStart": "11:00", "localEnd": "11:15", "doctor": {"displayName": "Dr. Meena Rao"}}}))
    r = env.client.post(f"/patient/offers/{OFFER}/accept")
    assert r.headers["Location"].endswith("/patient/appointments/77")
    env.route("POST", f"/offers/{OFFER}/decline", lambda kw: FakeResp(200, {"message": "Offer declined."}))
    assert env.client.post(f"/patient/offers/{OFFER}/decline").headers["Location"].endswith("/patient/waitlists")


def appointment(start, count=0, status="CONFIRMED"):
    return {"appointment": {"appointmentId": 41, "status": status, "slotId": SLOT_A, "doctorId": 12, "startAt": start, "localDate": "2026-12-01",
                            "localStart": "11:00", "localEnd": "11:15", "timezone": "Asia/Kolkata", "fee": 300, "rescheduleCount": count,
                            "consultationType": {"id": 1, "name": "Quick Consult", "durationMinutes": 15}, "cancellable": status == "CONFIRMED",
                            "doctor": {"displayName": "Dr. Ravi Kumar", "hospital": "Apollo", "city": "Hyderabad"}, "bookedFor": {"type": "SELF"}}}


def test_appointment_page_offers_free_times_of_the_same_doctor_and_type(env):
    login_patient(env)
    start = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    env.route("GET", "/appointments/41", FakeResp(200, appointment(start)))
    other = "SLOT#12_20301201T060000Z_1"
    env.route("GET", "/doctors/12/slots", FakeResp(200, {"days": [{"date": "2026-12-03", "slots": [
        {"slotId": SLOT_A, "localStart": "11:00", "localEnd": "11:15", "freeSeats": 1},
        {"slotId": other, "localStart": "12:00", "localEnd": "12:15", "freeSeats": 1},
        {"slotId": "SLOT#12_20301201T070000Z_1", "localStart": "13:00", "localEnd": "13:15", "freeSeats": 0}]}]}))
    page = env.client.get("/patient/appointments/41").get_data(as_text=True)
    assert "Reschedule to another time" in page and f'value="{other}"' in page and "Move here" in page
    assert page.count("Move here") == 1                                  # not the current slot, not a full one
    params = [c for c in env.calls if c[1] == "/doctors/12/slots"][0][2]["params"]
    assert params["consultationTypeId"] == 1 and params["availableOnly"] == "true"


def test_reschedule_is_not_offered_inside_the_cutoff_or_after_two_moves(env):
    login_patient(env)
    soon = (datetime.now(timezone.utc) + timedelta(hours=5)).strftime("%Y-%m-%dT%H:%M:%SZ")
    env.route("GET", "/appointments/41", FakeResp(200, appointment(soon)))
    page = env.client.get("/patient/appointments/41").get_data(as_text=True)
    assert "up to 6 hours before" in page and "Move here" not in page
    later = (datetime.now(timezone.utc) + timedelta(days=2)).strftime("%Y-%m-%dT%H:%M:%SZ")
    env.route("GET", "/appointments/41", FakeResp(200, appointment(later, count=2)))
    assert "already been rescheduled twice" in env.client.get("/patient/appointments/41").get_data(as_text=True)


def test_reschedule_form_sends_the_key_and_reports_the_outcome(env):
    login_patient(env)
    seen = capture_headers(env)
    other = "SLOT#12_20301201T060000Z_1"
    env.route("POST", "/appointments/41/reschedule", lambda kw: FakeResp(409, {"message": "Your appointment could not be moved, so it stays at its original time.",
                                                                              "code": "RESCHEDULE_COMPENSATED"}))
    r = env.client.post("/patient/appointments/41/reschedule", data={"targetSlotId": other, "key": "k" * 32})
    assert r.headers["Location"].endswith("/patient/appointments/41")
    call = [c for c in env.calls if c[1] == "/appointments/41/reschedule"][0]
    sent_headers = [h for m, p, h, _ in seen if p == "/appointments/41/reschedule"][0]
    assert call[2]["json"] == {"targetSlotId": other} and sent_headers["Idempotency-Key"] == "k" * 32
    with env.client.session_transaction() as s:
        assert "stays at its original time" in s["messages"][0]["text"]
    env.client.post("/patient/appointments/41/reschedule", data={"targetSlotId": "bad", "key": "k" * 32})
    assert len([c for c in env.calls if c[1] == "/appointments/41/reschedule"]) == 1


# ==============================================================================================================
# Web app - doctor, and the guards
# ==============================================================================================================
def test_doctor_day_view_lists_the_patients_in_each_slot(env):
    login_doctor(env)
    future = datetime.now(timezone.utc) + timedelta(days=2)
    day = future.date().isoformat()
    env.route("GET", "/doctors/3/slots", FakeResp(200, owner_view(day, [
        {"slotId": SLOT_A, "startAt": iso(future), "endAt": iso(future + timedelta(minutes=60)), "capacity": 3, "bookedCount": 2,
         "heldCount": 1, "status": "OPEN"}])))
    env.route("GET", "/appointments", FakeResp(200, {"items": [
        {"appointmentId": 51, "slotId": SLOT_A, "status": "CONFIRMED", "localDate": day, "localStart": "10:00", "localEnd": "11:00",
         "startAt": iso(future), "patient": {"id": 9, "name": "Lakshmi Rao", "bookedBy": "Asha Rao", "relation": "Mother"},
         "consultationType": {"name": "Health Camp Session"}, "allowedStatuses": ["CANCELLED_BY_DOCTOR"]},
        {"appointmentId": 52, "slotId": SLOT_A, "status": "CONFIRMED", "localDate": day, "localStart": "10:00", "localEnd": "11:00",
         "startAt": iso(future), "patient": {"id": 10, "name": "Kiran Das"}, "consultationType": {"name": "Health Camp Session"},
         "allowedStatuses": ["CANCELLED_BY_DOCTOR"]}], "hasNext": False}))
    page = env.client.get("/schedule?date=" + day).get_data(as_text=True)
    assert "Lakshmi Rao" in page and "Booked by Asha Rao (Mother)" in page and "Kiran Das" in page
    assert "On hold for a waitlisted patient" in page
    params = [c for c in env.calls if c[1] == "/appointments"][0][2]["params"]
    assert params["from"] == params["to"] == day


def test_doctor_week_view(env):
    login_doctor(env)
    first = date.today() + timedelta(days=1)
    days = []
    for i in range(7):
        d = first + timedelta(days=i)
        start = datetime(d.year, d.month, d.day, 5, 0, tzinfo=timezone.utc)
        days.append({"date": d.isoformat(), "slots": [{"slotId": SLOT_A, "startAt": iso(start), "endAt": iso(start + timedelta(minutes=15)),
                                                        "capacity": 1, "bookedCount": 1 if i == 0 else 0, "status": "OPEN"}]})
    env.route("GET", "/doctors/3/slots", FakeResp(200, {"timezone": "UTC", "days": days}))
    env.route("GET", "/appointments", FakeResp(200, {"items": [
        {"appointmentId": 61, "slotId": SLOT_A, "status": "CONFIRMED", "localDate": first.isoformat(), "localStart": "05:00",
         "localEnd": "05:15", "startAt": "2030-01-01T05:00:00Z", "patient": {"id": 9, "name": "Kiran Das"}, "consultationType": {"name": "Quick Consult"}}],
        "hasNext": False}))
    page = env.client.get(f"/schedule?date={first.isoformat()}&view=week").get_data(as_text=True)
    assert page.count('class="wd"') == 7 and "Kiran Das" in page and "Next week" in page
    slots_call = [c for c in env.calls if c[1] == "/doctors/3/slots"][0][2]["params"]
    assert slots_call == {"from": first.isoformat(), "to": (first + timedelta(days=6)).isoformat()}


def test_guards_work_in_both_directions(env):
    # signed out: neither area
    for path in ("/?m=appointments", "/schedule?date=2026-12-01", "/patient/waitlists"):
        r = env.client.get(path)
        assert r.status_code in (200, 302)
        if r.status_code == 302:
            assert r.headers["Location"].endswith(("/", "/?mode=signin&role=patient"))
    r = env.client.post("/schedule/slots/cancel", data={"slot": SLOT_A[5:], "confirm": "yes"})
    assert r.status_code == 302 and r.headers["Location"].endswith("/")
    assert not [c for c in env.calls if "/cancel" in c[1]]
    # patient -> doctor pages
    login_patient(env)
    assert env.client.get("/schedule?date=2026-12-01").headers["Location"].endswith("/patient")
    assert env.client.post("/appointments/41/status", data={"status": "NO_SHOW"}).headers["Location"].endswith("/patient")
    env.client.get("/logout")
    # doctor -> patient pages
    login_doctor(env)
    assert env.client.get("/patient/waitlists").headers["Location"].endswith("/")
    assert env.client.post(f"/patient/offers/{OFFER}/accept").headers["Location"].endswith("/")
    assert not [c for c in env.calls if c[1].startswith("/offers")]


def test_landing_page_offers_both_roles(env):
    page = env.client.get("/").get_data(as_text=True)
    assert "I’m a doctor" in page and "I’m a patient" in page and 'href="/?mode=signin&role=patient"' in page


# ==============================================================================================================
# Observability
# ==============================================================================================================
OBS = template("observability-stack.yaml")["Resources"]


def dashboard_body():
    body = OBS["OperationsDashboard"]["Properties"]["DashboardBody"]
    return json.loads(re.sub(r"\$\{[^}]+\}", "X", body)), body


def test_operations_dashboard_has_every_business_metric():
    assert OBS["OperationsDashboard"]["Properties"]["DashboardName"] == "medislot-${Environment}-operations"
    data, raw = dashboard_body()
    for metric in ("ReschedulesCompensated", "WaitlistPromotions", "OffersExpired", "NotificationsDispatched",
                   "BookingsConfirmed", "BookingsRejected", "BookingConflictRetries", "CancellationsByTier", "ReschedulesSucceeded",
                   "ReschedulesRejected", "SagaCompensationFailures", "OffersAccepted", "OffersDeclined", "OffersOverdue",
                   "WaitlistJoins", "NotificationsDuplicateSuppressed", "NotificationFailures", "RemindersQueued", "NoShowsMarked",
                   "EventPublishFailures", "SeatReleaseFailures"):
        assert metric in raw, metric
    kinds = {w["type"] for w in data["widgets"]}
    assert {"metric", "text", "alarm"} <= kinds and len(data["widgets"]) >= 20
    for w in data["widgets"]:
        assert 0 <= w["x"] and w["x"] + w["width"] <= 24
        # CloudWatch rule: "metrics" is an array of ARRAYS - a math / SEARCH expression object must sit inside its own array
        for m in w.get("properties", {}).get("metrics", []):
            assert isinstance(m, list) and all(isinstance(x, str) for x in m[:-1]) and isinstance(m[-1], (str, dict)), (w["properties"]["title"], m)
    alarm_refs = set(re.findall(r"\$\{(\w+)\.Arn\}", raw))
    assert alarm_refs == {k for k, v in OBS.items() if v["Type"] == "AWS::CloudWatch::Alarm"}


def test_every_metric_the_lambdas_emit_is_on_the_dashboard():
    _, raw = dashboard_body()
    emitted = set()
    for f in (ROOT / "lambdas").glob("*/lambda_function.py"):
        emitted |= set(re.findall(r'metric\("([A-Za-z]+)"', f.read_text()))
    assert emitted and not (emitted - set(re.findall(r"[A-Z][A-Za-z]+", raw))), emitted


def test_six_alarm_categories_all_notify_sns():
    alarms = {k: v["Properties"] for k, v in OBS.items() if v["Type"] == "AWS::CloudWatch::Alarm"}
    namespaces = set()
    for name, p in alarms.items():
        assert p["AlarmActions"] == ["OpsAlarmTopic"] and p["TreatMissingData"] == "notBreaching", name
        if "Namespace" in p:
            namespaces.add(p["Namespace"])
        for m in p.get("Metrics", []):
            if "MetricStat" in m:
                namespaces.add(m["MetricStat"]["Metric"]["Namespace"])
    assert {"AWS/ApiGateway", "AWS/Lambda", "AWS/SQS", "AWS/RDS", "AWS/DynamoDB", "AWS/States", "MediSlot"} <= namespaces
    for must in ("ReschedulesCompensatedAlarm", "SagaCompensationFailedAlarm", "OffersOverdueAlarm", "NotificationsDLQAlarm",
                 "WaitlistDLQAlarm", "SeatReleaseFailuresAlarm"):
        assert must in alarms
    assert OBS["OpsAlarmTopicPolicy"]["Properties"]["PolicyDocument"]["Statement"][0]["Principal"]["Service"] == \
        ["cloudwatch.amazonaws.com", "budgets.amazonaws.com"]


def test_budgets_and_cost_tags():
    budgets = [v["Properties"] for v in OBS.values() if v["Type"] == "AWS::Budgets::Budget"]
    assert len(budgets) == 2 and any(b["Budget"].get("CostFilters", {}).get("TagKeyValue") == ["user:Project$MediSlot"] for b in budgets)
    deployer = (ROOT / "scripts" / "deploy_stack.py").read_text()
    assert "Key=Project,Value=MediSlot" in deployer and "Key=Environment" in deployer
    deploy = (ROOT / ".github" / "workflows" / "deploy.yaml").read_text()
    assert '--tags "Project=MediSlot" "Environment=${ENVIRONMENT}"' in deploy and "observability-stack.yaml" in deploy


def test_xray_tracing_end_to_end():
    auth = template("auth-stack.yaml")["Resources"]
    fns = {k: v["Properties"] for k, v in auth.items() if v["Type"] == "AWS::Lambda::Function" and "ZipFile" not in v["Properties"]["Code"]}
    untraced = [k for k, p in fns.items() if p.get("TracingConfig", {}).get("Mode") != "Active" and k != "RdsConnectionCheckFunction"]
    assert not untraced, untraced
    assert auth["ApiStage"]["Properties"]["TracingEnabled"] is True
    assert auth["RescheduleStateMachine"]["Properties"]["TracingConfiguration"]["Enabled"] is True


# ==============================================================================================================
# Hardening
# ==============================================================================================================
STAR_OK = ("xray:", "ec2:CreateNetworkInterface", "ec2:DescribeNetworkInterfaces", "ec2:DeleteNetworkInterface",
           "ec2:AssignPrivateIpAddresses", "ec2:UnassignPrivateIpAddresses")


def statements(doc):
    for name, res in doc["Resources"].items():
        props = res.get("Properties", {})
        docs = [props["PolicyDocument"]] if isinstance(props.get("PolicyDocument"), dict) else []
        docs += [p["PolicyDocument"] for p in props.get("Policies", [])]
        for d in docs:
            for s in d["Statement"]:
                yield name, s


def test_iam_audit_no_wildcard_actions_anywhere():
    for f in (ROOT / "cloudformation").glob("*.yaml"):
        for name, s in statements(yaml.load(f.read_text(), Loader=_Loader)):
            actions = s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
            assert not any(a == "*" or a.endswith(":*") for a in actions), (f.name, name, actions)


def test_iam_audit_application_roles_scope_every_resource():
    doc = template("auth-stack.yaml")
    for name, s in statements(doc):
        if s.get("Principal"):                              # resource policies (queues, logs) are not role policies
            continue
        resources = s["Resource"] if isinstance(s["Resource"], list) else [s["Resource"]]
        actions = s["Action"] if isinstance(s["Action"], list) else [s["Action"]]
        if "*" in resources:
            assert all(a.startswith(STAR_OK) for a in actions), (name, actions)
        if "kms:Decrypt" in actions:
            assert "kms:ViaService" in json.dumps(s.get("Condition", {})), name


def test_backups_and_point_in_time_recovery():
    data = template("data-stack.yaml")["Resources"]
    db = data["MediSlotDatabase"]
    assert db["Properties"]["BackupRetentionPeriod"] >= 1 and db["DeletionPolicy"] == "Snapshot"
    for k, v in data.items():
        if v["Type"] == "AWS::DynamoDB::Table":
            assert v["Properties"]["PointInTimeRecoverySpecification"]["PointInTimeRecoveryEnabled"] is True, k


def test_ops_tooling_for_drift_dr_and_teardown():
    ops = (ROOT / ".github" / "workflows" / "ops.yaml").read_text()
    for action in ("drift", "dr-drill", "teardown"):
        assert action in ops
    drift = (ROOT / "scripts" / "ops" / "drift_check.sh").read_text()
    assert "detect-stack-drift" in drift and "IN_SYNC" in drift and "observability" in drift
    dr = (ROOT / "scripts" / "ops" / "dr_drill.sh").read_text()
    assert "restore-db-instance-to-point-in-time" in dr and "restore-table-to-point-in-time" in dr and "rds_secs" in dr
    down = (ROOT / "scripts" / "ops" / "teardown.sh").read_text()
    assert "CONFIRM" in down and "delete_stack iam" not in down
    iam = template("iam-stack.yaml")["Resources"]["MediSlotGitHubActionsDeploymentPolicy"]["Properties"]["PolicyDocument"]
    sids = {s["Sid"] for s in iam["Statement"]}
    assert {"DetectMediSlotDrift", "TeardownApplicationStacks", "DisasterRecoveryDrillRds", "ManageMediSlotAlarms"} <= sids
    text = json.dumps(iam, separators=(",", ":"))
    for k, v in (("${AWS::Partition}", "aws"), ("${AWS::Region}", "ap-south-1"), ("${AWS::AccountId}", "123456789012"), ("${Environment}", "prod")):
        text = text.replace(k, v)
    size = len(text)
    assert size < 10240, f"inline IAM policies are limited to 10,240 characters ({size})"
