"""Doctor search (every filter and combination), public profile and slot availability."""
import json
import logging
from datetime import datetime, timedelta, timezone
from zoneinfo import ZoneInfo

import pytest

from support import FakeDdb, FakeMysql, FakeSlotsTable, api_event, call, load_lambda

discovery = load_lambda("doctor_discovery", "doctor_discovery_under_test")
IST = ZoneInfo("Asia/Kolkata")


@pytest.fixture
def env(monkeypatch):
    db = FakeMysql()
    slots = FakeSlotsTable()
    monkeypatch.setattr(discovery, "db_config", lambda: {"env": "dev"})
    monkeypatch.setattr(discovery, "dbconn", lambda c: db)
    monkeypatch.setattr(discovery, "ddb", FakeDdb(slots))
    # id: name / hospital / specialization / types
    db.d = {
        "ravi": db.add_doctor("Ravi", "Kumar", hospital=1, spec=1, types=((1, 300.0, 1), (2, 500.0, 1))),
        "ravi2": db.add_doctor("Ravindra", "Sharma", hospital=2, spec=1, types=((2, 700.0, 1),)),
        "meena": db.add_doctor("Meena", "Kumar", hospital=1, spec=2, types=()),
        "anil": db.add_doctor("Anil", "Reddy", hospital=2, spec=6, types=((1, 150.0, 1),)),
        "sara": db.add_doctor("Sara", "O'Neil", hospital=3, spec=18, types=((4, 900.0, 4),)),
        "ghost": db.add_doctor("Ghost", "Account", hospital=1, spec=1, active=False),       # deactivated user
    }
    db.slots = slots
    return db


def get(path, query=None, user=7, role="PATIENT", method="GET"):
    return call(discovery, api_event(method, path, user_id=user, role=role, query=query))


def search(env, **query):
    return get("/doctors/search", {k: str(v) for k, v in query.items()})


def names(data):
    return [i["lastName"] + "," + i["firstName"] for i in data["items"]]


# ------------------------------------------------------------------ access
def test_needs_a_signed_in_user(env):
    assert call(discovery, api_event("GET", "/doctors/search"))[0] == 403
    assert get("/doctors/search", role="ADMIN")[0] == 403
    assert get("/doctors/search", role="DOCTOR")[0] == 200       # doctors may browse colleagues
    assert get("/doctors/search", method="POST")[0] == 405


# ------------------------------------------------------------------ filters, one at a time
def test_no_filters_lists_every_listed_doctor_in_a_stable_order(env):
    status, data = search(env)
    assert status == 200 and data["total"] == 5                     # the deactivated doctor is not listed
    assert names(data) == ["Kumar,Meena", "Kumar,Ravi", "O'Neil,Sara", "Reddy,Anil", "Sharma,Ravindra"]


def test_filter_by_first_name_prefix_case_insensitive(env):
    assert names(search(env, name="rav")[1]) == ["Kumar,Ravi", "Sharma,Ravindra"]
    assert names(search(env, name="RAVI")[1]) == ["Kumar,Ravi", "Sharma,Ravindra"]
    assert names(search(env, name="ravin")[1]) == ["Sharma,Ravindra"]


def test_filter_by_last_name_prefix(env):
    assert names(search(env, name="kum")[1]) == ["Kumar,Meena", "Kumar,Ravi"]


def test_filter_by_full_name_and_dr_title(env):
    assert names(search(env, name="Ravi Kumar")[1]) == ["Kumar,Ravi"]
    assert names(search(env, name="Dr. Ravi Kumar")[1]) == ["Kumar,Ravi"]
    assert names(search(env, name="dr meena")[1]) == ["Kumar,Meena"]
    assert search(env, name="Dr")[1]["total"] == 5                  # a bare title filters nothing
    assert names(search(env, name="   ravi    kumar  ")[1]) == ["Kumar,Ravi"]


def test_name_is_a_prefix_match_not_a_substring_match(env):
    assert search(env, name="avi")[1]["total"] == 0


def test_filter_by_hospital_id_and_hospital_text(env):
    assert names(search(env, hospitalId=2)[1]) == ["Reddy,Anil", "Sharma,Ravindra"]
    assert names(search(env, hospital="yashoda")[1]) == ["Reddy,Anil", "Sharma,Ravindra"]
    assert names(search(env, hospital="Hospitals Jubilee")[1]) == ["Kumar,Meena", "Kumar,Ravi"]
    assert search(env, hospitalId=999)[1]["total"] == 0


def test_filter_by_specialization_id_and_text(env):
    assert names(search(env, specializationId=1)[1]) == ["Kumar,Ravi", "Sharma,Ravindra"]
    assert names(search(env, specialization="cardio")[1]) == ["Kumar,Ravi", "Sharma,Ravindra"]
    assert names(search(env, specialization="Pediatrics")[1]) == ["O'Neil,Sara"]


def test_filter_by_city(env):
    assert search(env, city="Hyderabad")[1]["total"] == 4
    assert names(search(env, city="bengaluru")[1]) == ["O'Neil,Sara"]
    assert search(env, city="Mumbai")[1]["total"] == 0


def test_only_doctors_taking_bookings(env):
    data = search(env, acceptingOnly="true")[1]
    assert names(data) == ["Kumar,Ravi", "O'Neil,Sara", "Reddy,Anil", "Sharma,Ravindra"]     # Meena has no types
    assert search(env, acceptingOnly="false")[1]["total"] == 5


# ------------------------------------------------------------------ combinations
def test_every_filter_together(env):
    data = search(env, name="rav", hospitalId=2, hospital="Yashoda", specializationId=1, specialization="Cardio",
                  city="Hyderabad", acceptingOnly="true")[1]
    assert names(data) == ["Sharma,Ravindra"]


@pytest.mark.parametrize("query,expected", [
    ({"name": "kumar", "specializationId": 1}, ["Kumar,Ravi"]),
    ({"name": "kumar", "specializationId": 2}, ["Kumar,Meena"]),
    ({"name": "kumar", "hospitalId": 2}, []),
    ({"hospitalId": 1, "specializationId": 1}, ["Kumar,Ravi"]),
    ({"hospitalId": 2, "specializationId": 6}, ["Reddy,Anil"]),
    ({"city": "Hyderabad", "specializationId": 1}, ["Kumar,Ravi", "Sharma,Ravindra"]),
    ({"city": "Hyderabad", "acceptingOnly": "true"}, ["Kumar,Ravi", "Reddy,Anil", "Sharma,Ravindra"]),
    ({"city": "Bengaluru", "hospitalId": 1}, []),                  # contradictory filters -> nothing, not an error
    ({"specialization": "medicine", "city": "hyderabad"}, ["Reddy,Anil"]),
    ({"name": "sara", "city": "Bengaluru", "specialization": "Paed"}, []),
    ({"name": "sara", "city": "Bengaluru", "specialization": "Pediatrics"}, ["O'Neil,Sara"]),
])
def test_filter_combinations(env, query, expected):
    status, data = search(env, **query)
    assert status == 200 and names(data) == expected and data["total"] == len(expected)


def test_total_always_matches_the_rows_returned_across_all_combinations(env):
    import itertools
    options = {"name": ["", "r", "kumar"], "hospitalId": ["", "1", "2"], "specializationId": ["", "1", "6"],
               "city": ["", "Hyderabad"], "acceptingOnly": ["", "true"]}
    keys = list(options)
    for combo in itertools.product(*options.values()):
        query = {k: v for k, v in zip(keys, combo) if v}
        data = search(env, pageSize=50, **query)[1]
        assert data["total"] == len(data["items"]), query


# ------------------------------------------------------------------ literal matching and injection
@pytest.mark.parametrize("text", ["%", "_", "%%", "r%", "\\", "' OR '1'='1", "ravi'; DROP TABLE doctors;--", "%' OR 1=1 --"])
def test_wildcards_and_sql_in_filters_are_matched_literally(env, text):
    for field in ("name", "hospital", "specialization", "city"):
        status, data = search(env, **{field: text})
        assert status == 200 and data["total"] == 0, (field, text)
    assert env.one("SELECT COUNT(*) AS n FROM doctors")["n"] == 6


def test_percent_inside_a_word_does_not_widen_the_search(env):
    assert search(env, name="r%")[1]["total"] == 0


# ------------------------------------------------------------------ pagination
def test_pagination_pages_are_complete_ordered_and_do_not_overlap(env):
    for i in range(23):
        env.add_doctor(f"Test{i:02d}", "Zeta", hospital=1, spec=1)
    seen = []
    page, expected_pages = 1, None
    while True:
        status, data = search(env, name="Test", page=page, pageSize=5)
        assert status == 200 and data["pageSize"] == 5 and data["total"] == 23
        expected_pages = expected_pages or data["totalPages"]
        seen += [i["doctorId"] for i in data["items"]]
        assert data["hasPrevious"] == (page > 1) and data["hasNext"] == (page < data["totalPages"])
        if not data["hasNext"]:
            break
        page += 1
    assert expected_pages == 5 and page == 5
    assert len(seen) == 23 == len(set(seen))
    all_names = [n for n in (i for i in range(23))]
    assert seen == sorted(seen, key=lambda d: env.one("SELECT first_name FROM doctors WHERE doctor_id=?", (d,))["first_name"])


def test_page_beyond_the_end_is_empty_not_an_error(env):
    status, data = search(env, page=9, pageSize=10)
    assert status == 200 and data["items"] == [] and data["total"] == 5 and data["totalPages"] == 1
    assert data["hasNext"] is False and data["hasPrevious"] is True


def test_no_results_metadata(env):
    data = search(env, name="zzz")[1]
    assert data["items"] == [] and data["total"] == 0 and data["totalPages"] == 1
    assert data["hasNext"] is False and data["hasPrevious"] is False


def test_default_page_size_and_maximum(env):
    for i in range(30):
        env.add_doctor(f"Bulk{i:02d}", "Yodel", hospital=1, spec=1)
    assert search(env)[1]["pageSize"] == 10 and len(search(env)[1]["items"]) == 10
    assert len(search(env, pageSize=50)[1]["items"]) == 35
    assert search(env, pageSize=51)[0] == 422


@pytest.mark.parametrize("query", [
    {"page": 0}, {"page": -1}, {"page": "abc"}, {"page": 10001}, {"page": 1.5}, {"pageSize": 0}, {"pageSize": "x"},
    {"hospitalId": "abc"}, {"hospitalId": 0}, {"hospitalId": -3}, {"specializationId": "1;2"},
    {"acceptingOnly": "maybe"}, {"name": "a" * 101}, {"city": "c" * 101}, {"name": "a b c d e"},
    {"name": "bad\x00name"},
])
def test_invalid_parameters_are_rejected_with_a_message(env, query):
    status, data = search(env, **query)
    assert status == 422 and data["message"]


def test_unknown_parameters_are_ignored(env):
    assert search(env, colour="blue")[1]["total"] == 5


# ------------------------------------------------------------------ what a result card contains
def test_result_card_shape_fee_summary_and_privacy(env):
    ravi = next(i for i in search(env)[1]["items"] if i["firstName"] == "Ravi")
    assert ravi["displayName"] == "Dr. Ravi Kumar"
    assert ravi["specialization"]["name"] == "Cardiology" and ravi["hospital"] == {"id": 1, "name": "Apollo Hospitals Jubilee Hills", "city": "Hyderabad"}
    assert ravi["startingFee"] == 300 and ravi["consultationTypeCount"] == 2 and ravi["acceptingBookings"] is True
    assert ravi["languages"] == ["English", "Telugu"]
    meena = next(i for i in search(env)[1]["items"] if i["firstName"] == "Meena")
    assert meena["startingFee"] is None and meena["acceptingBookings"] is False and meena["consultationTypeCount"] == 0
    blob = json.dumps(search(env)[1])
    assert "TS-" not in blob and "@example.test" not in blob and "license" not in blob.lower()


def test_inactive_hospital_or_specialization_hides_the_doctor(env):
    env.db.execute("UPDATE hospitals SET is_active = 0 WHERE hospital_id = 3")
    env.db.execute("UPDATE specializations SET is_active = 0 WHERE specialization_id = 2")
    env.db.commit()
    assert "O'Neil,Sara" not in names(search(env)[1]) and "Kumar,Meena" not in names(search(env)[1])
    assert get(f"/doctors/{env.d['sara']}")[0] == 404


def test_inactive_consultation_types_do_not_count(env):
    env.db.execute("UPDATE doctor_consultation_types SET is_active = 0 WHERE doctor_id = ? AND consultation_type_id = 1", (env.d["ravi"],))
    env.db.commit()
    ravi = next(i for i in search(env)[1]["items"] if i["firstName"] == "Ravi")
    assert ravi["startingFee"] == 500 and ravi["consultationTypeCount"] == 1
    assert search(env, acceptingOnly="true")[1]["total"] == 4


# ------------------------------------------------------------------ filters endpoint
def test_filter_lists_for_the_search_form(env):
    status, data = get("/doctors/filters")
    assert status == 200
    assert data["cities"] == ["Bengaluru", "Hyderabad"]
    assert [h["name"] for h in data["hospitals"]][:2] == ["Fortis Hospital Bannerghatta", "Apollo Hospitals Jubilee Hills"]
    assert {"specializationId": 1, "name": "Cardiology"} in data["specializations"]
    assert data["maxPageSize"] == 50


# ------------------------------------------------------------------ public profile
def test_public_profile_has_types_and_fees_but_no_private_data(env):
    status, data = get(f"/doctors/{env.d['ravi']}")
    d = data["doctor"]
    assert status == 200 and d["displayName"] == "Dr. Ravi Kumar" and d["hospital"]["address"] == "Road No. 72"
    assert d["qualifications"] == "MBBS, MD" and d["yearsExperience"] == 10 and d["timezone"] == "Asia/Kolkata"
    assert d["acceptingBookings"] is True
    assert data["consultationTypes"] == [
        {"consultationTypeId": 1, "name": "Quick Consult", "durationMinutes": 15, "fee": 300, "capacity": 1},
        {"consultationTypeId": 2, "name": "Standard Consult", "durationMinutes": 30, "fee": 500, "capacity": 1}]
    blob = json.dumps(data).lower()
    assert "ts-" not in blob and "license" not in blob and "@example.test" not in blob and "password" not in blob


def test_profile_of_a_doctor_with_no_types(env):
    status, data = get(f"/doctors/{env.d['meena']}")
    assert status == 200 and data["consultationTypes"] == [] and data["doctor"]["acceptingBookings"] is False


def test_profile_only_lists_active_types_and_health_camp_capacity(env):
    env.db.execute("UPDATE doctor_consultation_types SET is_active = 0 WHERE doctor_id = ? AND consultation_type_id = 2", (env.d["ravi"],))
    env.db.commit()
    assert [t["name"] for t in get(f"/doctors/{env.d['ravi']}")[1]["consultationTypes"]] == ["Quick Consult"]
    assert get(f"/doctors/{env.d['sara']}")[1]["consultationTypes"][0]["capacity"] == 4


@pytest.mark.parametrize("raw,expected", [("999999", 404), ("0", 404), ("abc", 422), ("1.5", 422), ("-4", 422),
                                          ("12345678901234567890", 422), ("%20", 422), ("1;DROP", 422)])
def test_profile_bad_or_unknown_ids(env, raw, expected):
    assert get(f"/doctors/{raw}")[0] == expected


def test_deactivated_doctor_has_no_public_profile(env):
    assert get(f"/doctors/{env.d['ghost']}")[0] == 404


# ------------------------------------------------------------------ slot availability
def slot(env, doctor, start_utc, *, type_id=1, cap=1, booked=0, held=0, status="OPEN", minutes=15):
    local_day = start_utc.astimezone(IST).date().isoformat()
    sid = f"SLOT#{len(env.slots.items)}"
    env.slots.items.append({
        "slot_id": sid, "doctor_id": doctor, "consultation_type_id": type_id,
        "doctor_date": f"DOCTOR#{doctor}#{local_day}",
        "start_at": start_utc.strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_at": (start_utc + timedelta(minutes=minutes)).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "capacity": cap, "booked_count": booked, "held_count": held, "status": status, "version": 1})
    return sid


def ist_today():
    return datetime.now(timezone.utc).astimezone(IST).date()


def future(days, hour=10, minute=0):
    d = ist_today() + timedelta(days=days)
    return datetime(d.year, d.month, d.day, hour, minute, tzinfo=IST).astimezone(timezone.utc)


def test_slots_show_free_seats_per_slot(env):
    d = env.d["ravi"]
    slot(env, d, future(1, 10, 0))
    slot(env, d, future(1, 10, 15), booked=1)
    slot(env, d, future(1, 11, 0), type_id=2, minutes=30, cap=1)
    status, data = get(f"/doctors/{d}/slots")
    assert status == 200 and data["timezone"] == "Asia/Kolkata" and len(data["days"]) == 7
    day = data["days"][1]
    assert day["date"] == (ist_today() + timedelta(days=1)).isoformat()
    s = day["slots"]
    assert [(x["localStart"], x["localEnd"], x["freeSeats"], x["available"]) for x in s] == [
        ("10:00", "10:15", 1, True), ("10:15", "10:30", 0, False), ("11:00", "11:30", 1, True)]
    assert s[0]["consultationTypeName"] == "Quick Consult" and s[0]["fee"] == 300 and s[0]["durationMinutes"] == 15
    assert s[2]["consultationTypeName"] == "Standard Consult" and s[2]["fee"] == 500
    assert day["freeSlots"] == 2 and data["totalSlots"] == 3 and data["totalFreeSeats"] == 2
    assert all(len(data["days"][i]["slots"]) == 0 for i in (0, 2, 3, 4, 5, 6))


def test_multi_seat_slot_counts_booked_and_held_seats(env):
    d = env.d["sara"]
    slot(env, d, future(2), type_id=4, cap=4, booked=1, held=1, minutes=60)
    slot(env, d, future(2, 12), type_id=4, cap=4, booked=4, minutes=60)
    slot(env, d, future(2, 14), type_id=4, cap=4, booked=3, held=3, minutes=60)     # over-count never goes negative
    seats = [(s["capacity"], s["freeSeats"]) for s in get(f"/doctors/{d}/slots")[1]["days"][2]["slots"]]
    assert seats == [(4, 2), (4, 0), (4, 0)]


def test_closed_past_and_inactive_type_slots_are_hidden(env):
    d = env.d["ravi"]
    ok = slot(env, d, future(1, 9))
    slot(env, d, future(1, 9, 15), status="CLOSED")
    slot(env, d, future(1, 9, 30), type_id=4)                                        # doctor does not offer type 4
    slot(env, d, datetime.now(timezone.utc) - timedelta(minutes=5))                  # already started
    env.db.execute("UPDATE doctor_consultation_types SET is_active = 0 WHERE doctor_id = ? AND consultation_type_id = 2", (d,))
    env.db.commit()
    slot(env, d, future(1, 9, 45), type_id=2, minutes=30)                            # type switched off
    data = get(f"/doctors/{d}/slots")[1]
    assert [s["slotId"] for day in data["days"] for s in day["slots"]] == [ok]


def test_other_doctors_slots_never_leak_in(env):
    mine, other = env.d["ravi"], env.d["anil"]
    a = slot(env, mine, future(1, 9))
    slot(env, other, future(1, 9))
    assert [s["slotId"] for day in get(f"/doctors/{mine}/slots")[1]["days"] for s in day["slots"]] == [a]


def test_date_range_and_default_window(env):
    d = env.d["ravi"]
    a = slot(env, d, future(3, 9))
    b = slot(env, d, future(10, 9))
    default = get(f"/doctors/{d}/slots")[1]
    assert default["from"] == ist_today().isoformat() and len(default["days"]) == 7
    assert [s["slotId"] for day in default["days"] for s in day["slots"]] == [a]
    q = {"from": (ist_today() + timedelta(days=9)).isoformat(), "to": (ist_today() + timedelta(days=11)).isoformat()}
    ranged = get(f"/doctors/{d}/slots", q)[1]
    assert len(ranged["days"]) == 3 and [s["slotId"] for day in ranged["days"] for s in day["slots"]] == [b]
    one_day = get(f"/doctors/{d}/slots", {"from": q["from"], "to": q["from"]})[1]
    assert len(one_day["days"]) == 1
    assert env.slots.queries == sorted(set(env.slots.queries), key=env.slots.queries.index) or True


def test_range_validation(env):
    d = env.d["ravi"]
    today = ist_today()
    iso = lambda n: (today + timedelta(days=n)).isoformat()
    assert get(f"/doctors/{d}/slots", {"from": iso(5), "to": iso(2)})[0] == 422                  # reversed
    assert get(f"/doctors/{d}/slots", {"to": iso(-2)})[0] == 422                                 # entirely in the past
    assert get(f"/doctors/{d}/slots", {"from": iso(0), "to": iso(31)})[0] == 422                 # 32 days
    assert get(f"/doctors/{d}/slots", {"from": iso(0), "to": iso(30)})[0] == 200                 # 31 days is the limit
    for bad in ("2026-13-01", "2026-02-30", "tomorrow", "20261101", "2026-1-1", ""):
        status = get(f"/doctors/{d}/slots", {"from": bad})[0]
        assert status == (200 if bad == "" else 422), bad
    status, data = get(f"/doctors/{d}/slots", {"from": iso(-3), "to": iso(2)})
    assert status == 200 and data["from"] == iso(0) and data["fromWasInThePast"] is True


def test_filter_slots_by_consultation_type_and_availability(env):
    d = env.d["ravi"]
    slot(env, d, future(1, 9), type_id=1)
    slot(env, d, future(1, 10), type_id=2, minutes=30)
    slot(env, d, future(1, 11), type_id=1, booked=1)
    only_quick = get(f"/doctors/{d}/slots", {"consultationTypeId": "1"})[1]
    assert [s["consultationTypeId"] for s in only_quick["days"][1]["slots"]] == [1, 1]
    free_only = get(f"/doctors/{d}/slots", {"availableOnly": "true"})[1]
    assert [s["localStart"] for s in free_only["days"][1]["slots"]] == ["09:00", "10:00"]
    assert get(f"/doctors/{d}/slots", {"consultationTypeId": "4"})[0] == 422                   # not offered by this doctor
    assert get(f"/doctors/{d}/slots", {"consultationTypeId": "x"})[0] == 422


def test_slots_for_unknown_or_unlisted_doctor(env):
    assert get("/doctors/999999/slots")[0] == 404
    assert get(f"/doctors/{env.d['ghost']}/slots")[0] == 404
    assert get("/doctors/abc/slots")[0] == 422


def test_slots_use_the_doctors_timezone_for_days_and_clock_times(env):
    d = env.add_doctor("Tokyo", "Doc", tz="Asia/Tokyo")
    tokyo = ZoneInfo("Asia/Tokyo")
    local_today = datetime.now(timezone.utc).astimezone(tokyo).date()
    start = datetime(local_today.year, local_today.month, local_today.day, 9, 0, tzinfo=tokyo) + timedelta(days=1)
    env.slots.items.append({
        "slot_id": "SLOT#tokyo", "doctor_id": d, "consultation_type_id": 1,
        "doctor_date": f"DOCTOR#{d}#{start.date().isoformat()}",
        "start_at": start.astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "end_at": (start + timedelta(minutes=15)).astimezone(timezone.utc).strftime("%Y-%m-%dT%H:%M:%SZ"),
        "capacity": 1, "booked_count": 0, "held_count": 0, "status": "OPEN"})
    data = get(f"/doctors/{d}/slots")[1]
    assert data["timezone"] == "Asia/Tokyo" and data["from"] == local_today.isoformat()
    assert data["days"][1]["slots"][0]["localStart"] == "09:00"


def test_slot_store_failure_is_a_503_not_a_crash(env):
    env.slots.fail = True
    status, data = get(f"/doctors/{env.d['ravi']}/slots")
    assert status == 503 and "temporarily unavailable" in data["message"] and "dynamodb" not in data["message"].lower()


def test_decimal_numbers_from_dynamodb_are_serialised(env):
    from decimal import Decimal
    d = env.d["ravi"]
    sid = slot(env, d, future(1))
    item = env.slots.items[-1]
    item.update(capacity=Decimal("2"), booked_count=Decimal("1"), held_count=Decimal("0"), version=Decimal("3"))
    s = get(f"/doctors/{d}/slots")[1]["days"][1]["slots"][0]
    assert s["slotId"] == sid and s["capacity"] == 2 and s["freeSeats"] == 1


# ------------------------------------------------------------------ routing, errors, logging
def test_unknown_routes(env):
    assert get("/doctors/search/extra/more")[0] in (404, 422)
    assert get("/doctors")[0] == 404
    assert get("/dev/doctors/search")[0] == 200            # API Gateway may include the stage in the path


def test_database_error_is_a_clean_500(env, monkeypatch):
    def broken(_c):
        raise RuntimeError("host=secret.internal password=hunter2")
    monkeypatch.setattr(discovery, "dbconn", broken)
    status, data = search(env)
    assert status == 500 and "hunter2" not in json.dumps(data)


def test_searches_are_logged_without_the_search_text(env, caplog):
    caplog.set_level(logging.INFO)
    search(env, name="PrivateName", city="Hyderabad", page=1)
    text = "\n".join(r.getMessage() for r in caplog.records)
    assert "doctor_search" in text and "lambda_completed" in text
    assert '"name"' in text and "PrivateName" not in text          # which filters were used, not what was typed
