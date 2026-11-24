"""scripts/seed_doctors.py: the 15 demo doctors are valid for the real API rules, and re-running is harmless."""
import importlib.util
import re
from pathlib import Path

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
    assert Path(seed.__file__).name == "seed_doctors.py"
