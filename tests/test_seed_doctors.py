"""scripts/seed_doctors.py: the 15 demo doctors are valid for the real API rules, and re-running is harmless."""
import importlib.util
import re

from support import ROOT, load_lambda

spec = importlib.util.spec_from_file_location("seed_doctors", ROOT / "scripts" / "seed_doctors.py")
seed = importlib.util.module_from_spec(spec)
spec.loader.exec_module(seed)
rds = load_lambda("rds_check", "rds_check_for_seed")
auth = load_lambda("authentication", "authentication_for_seed")


def minutes(hhmm):
    h, m = hhmm.split(":")
    return int(h) * 60 + int(m)


def test_fifteen_doctors_with_unique_logins_and_valid_licenses():
    assert len(seed.DOCTORS) == 15
    accounts = [seed.account(n, d[0], d[1]) for n, d in enumerate(seed.DOCTORS, start=1)]
    assert len({a["email"] for a in accounts}) == 15 and len({a["license"] for a in accounts}) == 15
    assert all(auth.LICENSE_RE.fullmatch(a["license"]) for a in accounts)


def test_passwords_meet_the_sign_up_rules():
    for n, d in enumerate(seed.DOCTORS, start=1):
        pw = seed.account(n, d[0], d[1])["password"]
        assert 12 <= len(pw) <= 72 and re.search("[a-z]", pw) and re.search("[A-Z]", pw)
        assert re.search(r"\d", pw) and re.search("[^A-Za-z0-9]", pw), pw


def test_reference_ids_match_what_rds_check_seeds():
    hospitals = {h[0]: h[1] for h in rds.HOSPITAL_SEED}
    specs = dict(enumerate(rds.SPECIALIZATION_SEED, start=1))
    types = {t[0]: t[1:] for t in rds.CONSULTATION_TYPE_SEED}
    assert all(hospitals[i] == name for i, name in seed.HOSPITALS.items())
    assert all(specs[i] == name for i, name in seed.SPECIALIZATIONS.items())
    assert all(types[i][:4] == v for i, v in seed.TYPES.items())
    used = {(d[2], d[3]) for d in seed.DOCTORS}
    assert all(h in hospitals and s in specs for h, s in used)


def test_every_doctor_has_varied_types_and_weekly_hours_the_api_accepts():
    all_types = set()
    for first, last, _h, _s, years, _l, _q, types, patterns in seed.DOCTORS:
        offered = {t for t, _fee, _cap in types}
        all_types |= offered
        assert 0 <= years <= 80 and len(types) >= 2 and len(patterns) >= 2, (first, last)
        for t, fee, cap in types:
            _name, _mins, lo, hi = seed.TYPES[t]
            assert fee > 0 and lo <= cap <= hi, (first, t)
        by_day = {}
        for t, dow, start, end in patterns:
            assert t in offered and 1 <= dow <= 7, (first, t, dow)
            assert minutes(end) - minutes(start) >= seed.TYPES[t][1], (first, dow, start)      # fits one visit
            by_day.setdefault(dow, []).append((minutes(start), minutes(end)))
        for dow, spans in by_day.items():                                                     # no overlaps per day
            spans.sort()
            assert all(a_end <= b_start for (_, a_end), (b_start, _) in zip(spans, spans[1:])), (first, dow)
    assert all_types == {1, 2, 3, 4}                                                          # all four types used
    assert {dow for d in seed.DOCTORS for _t, dow, _s, _e in d[8]} == set(range(1, 8))        # every weekday covered


class FakeApi:
    """Records calls; remembers what each doctor created so a second run can be checked."""

    def __init__(self):
        self.users, self.types, self.patterns, self.calls = {}, {}, {}, []

    def __call__(self, api, method, path, token=None, body=None):
        self.calls.append((method, path))
        if path == "/auth/register/doctor":
            if body["email"] in self.users:
                return 409, {"message": "Unable to register with those details"}
            self.users[body["email"]] = body["password"]
            return 201, {}
        if path == "/auth/login":
            return (200, {"accessToken": body["email"]}) if self.users.get(body["email"]) == body["password"] else (401, {})
        if path == "/doctors/me/consultation-types":
            if method == "GET":
                return 200, {"items": [{"consultation_type_id": t} for t in self.types.get(token, [])]}
            self.types.setdefault(token, []).append(body["consultationTypeId"])
            return 201, {}
        if path == "/doctors/me/availability-patterns":
            if method == "GET":
                return 200, {"items": self.patterns.get(token, [])}
            self.patterns.setdefault(token, []).append({
                "consultation_type_id": body["consultationTypeId"], "day_of_week": body["dayOfWeek"],
                "start_time": body["startTime"] + ":00", "end_time": body["endTime"] + ":00", "is_active": 1})
            return 201, {}
        raise AssertionError(path)


def test_seeding_creates_everything_once_and_a_rerun_changes_nothing(monkeypatch, capsys):
    fake = FakeApi()
    monkeypatch.setattr(seed, "call", fake)
    assert all(seed.seed_one("http://api", n, d) for n, d in enumerate(seed.DOCTORS, start=1))
    assert len(fake.users) == 15
    assert sum(len(v) for v in fake.types.values()) == sum(len(d[7]) for d in seed.DOCTORS)
    assert sum(len(v) for v in fake.patterns.values()) == sum(len(d[8]) for d in seed.DOCTORS)
    fake.calls.clear()
    assert all(seed.seed_one("http://api", n, d) for n, d in enumerate(seed.DOCTORS, start=1))
    assert not [c for c in fake.calls if c[0] == "POST" and c[1].startswith("/doctors/me")]      # nothing re-created
    assert "account already exists" in capsys.readouterr().out


def test_dry_run_prints_every_password(capsys, monkeypatch):
    monkeypatch.setattr("sys.argv", ["seed_doctors.py", "--dry-run"])
    assert seed.main() == 0
    out = capsys.readouterr().out
    assert all(seed.account(n, d[0], d[1])["password"] in out for n, d in enumerate(seed.DOCTORS, start=1))


# ---------------------------------------------------------------------------------------------------------------
# End to end against the REAL Lambda code: the seed script's HTTP calls are routed into the authentication and
# doctor-domain handlers, backed by the SQLite stand-in that runs their real SQL. This catches any payload the API
# would reject (the fake API above cannot).
import json as _json  # noqa: E402

import pytest  # noqa: E402

from support import FakeAuthTable, FakeMysql, api_event  # noqa: E402

doctor_api = load_lambda("doctor_domain", "doctor_domain_for_seed")
EXTRA_SCHEMA = """
ALTER TABLE doctors ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE doctors ADD COLUMN updated_at TEXT DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE doctor_consultation_types ADD COLUMN created_at TEXT DEFAULT CURRENT_TIMESTAMP;
ALTER TABLE doctor_consultation_types ADD COLUMN updated_at TEXT DEFAULT CURRENT_TIMESTAMP;
CREATE TABLE availability_patterns (pattern_id INTEGER PRIMARY KEY AUTOINCREMENT, doctor_id INTEGER NOT NULL,
    consultation_type_id INTEGER NOT NULL, day_of_week INTEGER NOT NULL, start_time TEXT NOT NULL, end_time TEXT NOT NULL,
    is_active INTEGER NOT NULL DEFAULT 1, created_at TEXT DEFAULT CURRENT_TIMESTAMP, updated_at TEXT DEFAULT CURRENT_TIMESTAMP);
CREATE TABLE blackout_dates (blackout_id INTEGER PRIMARY KEY AUTOINCREMENT, doctor_id INTEGER NOT NULL, blackout_date TEXT NOT NULL,
    reason TEXT, created_at TEXT DEFAULT CURRENT_TIMESTAMP, UNIQUE (doctor_id, blackout_date));
"""


class RealApi:
    def __init__(self, monkeypatch):
        self.db = FakeMysql()
        self.db.db.executescript(EXTRA_SCHEMA)
        # every hospital / specialization the seed uses, exactly as rds_check seeds them
        self.db.db.executemany("INSERT OR IGNORE INTO hospitals (hospital_id, hospital_name, address) VALUES (?,?,?)",
                               [(i, n, "Hyderabad") for i, n, *_ in rds.HOSPITAL_SEED])
        self.db.db.executemany("INSERT OR IGNORE INTO specializations (specialization_id, specialization_name) VALUES (?,?)",
                               list(enumerate(rds.SPECIALIZATION_SEED, start=1)))
        self.db.db.executemany("INSERT OR IGNORE INTO consultation_types VALUES (?,?,?,?,?,?)", rds.CONSULTATION_TYPE_SEED)
        self.db.db.commit()
        self.tokens, self.statuses = {}, []
        cfg = {"env": "dev", "prefix": "/medislot/dev", "jwt": "unit-test-signing-secret-0123456789"}
        monkeypatch.setattr(auth, "config", lambda: cfg)
        monkeypatch.setattr(auth, "dbconn", lambda c: self.db)
        monkeypatch.setattr(auth, "get_param", lambda name, *a, **k: "12" if "password-min-length" in name else "15")
        monkeypatch.setattr(auth, "auth_table", lambda: FakeAuthTable())
        monkeypatch.setattr(doctor_api, "config", lambda: cfg)
        monkeypatch.setattr(doctor_api, "dbconn", lambda c: self.db)
        monkeypatch.setattr(doctor_api, "refresh_slots", lambda uid, cid: None)       # slot materialiser not under test

    def __call__(self, api, method, path, token=None, body=None):
        if path.startswith("/auth/"):
            res = auth.handler(api_event(method, path, body=body), None)
        else:
            res = doctor_api.handler(api_event(method, path, user_id=self.tokens[token], role="DOCTOR", body=body), None)
        data = _json.loads(res["body"]) if res.get("body") else {}
        if path == "/auth/login" and res["statusCode"] == 200:
            self.tokens[data["accessToken"]] = int(data["userId"])
        self.statuses.append((method, path, res["statusCode"], data.get("message")))
        return res["statusCode"], data


@pytest.fixture
def real_api(monkeypatch):
    api = RealApi(monkeypatch)
    monkeypatch.setattr(seed, "call", api)
    return api


def test_all_fifteen_doctors_are_accepted_by_the_real_api(real_api):
    assert all(seed.seed_one("http://api", n, d) for n, d in enumerate(seed.DOCTORS, start=1))
    rejected = [s for s in real_api.statuses if s[2] >= 400]
    assert rejected == [], rejected                                          # no request was refused
    one = real_api.db.one
    assert one("SELECT COUNT(*) AS n FROM doctors")["n"] == 15
    assert one("SELECT COUNT(*) AS n FROM users WHERE role='DOCTOR' AND phone IS NOT NULL")["n"] == 15
    assert one("SELECT COUNT(*) AS n FROM doctor_consultation_types")["n"] == sum(len(d[7]) for d in seed.DOCTORS)
    assert one("SELECT COUNT(*) AS n FROM availability_patterns")["n"] == sum(len(d[8]) for d in seed.DOCTORS)
    # a second deploy changes nothing and is not refused anywhere
    real_api.statuses.clear()
    assert all(seed.seed_one("http://api", n, d) for n, d in enumerate(seed.DOCTORS, start=1))
    assert [s[:3] for s in real_api.statuses if s[2] >= 400] == [("POST", "/auth/register/doctor", 409)] * 15
    assert one("SELECT COUNT(*) AS n FROM availability_patterns")["n"] == sum(len(d[8]) for d in seed.DOCTORS)
