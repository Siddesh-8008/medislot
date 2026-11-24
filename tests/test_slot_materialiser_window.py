"""Slot materialiser against the Slots table layout: partition key doctor_id, sort key slot_key, no GSI.

(This file used to hold a stale copy of the materialiser source with no tests in it; it now tests the real module.)
"""
from datetime import datetime, time, timezone
from zoneinfo import ZoneInfo

from botocore.exceptions import ClientError

from support import FakeSlotsTable, load_lambda   # support puts lambdas/ on sys.path
from common import parse_slot_id  # noqa: E402

materialiser = load_lambda("slot_materialiser", "slot_materialiser_window_under_test")
IST = ZoneInfo("Asia/Kolkata")
NOW = datetime(2026, 11, 24, 3, 0, tzinfo=timezone.utc)          # Tue 24 Nov 2026, 08:30 IST


def pattern(day_of_week=3, start="09:00", end="10:00", minutes=30, type_id=2, capacity=1, doctor=12):
    return {"doctor_id": doctor, "timezone": "Asia/Kolkata", "pattern_id": 1, "consultation_type_id": type_id,
            "day_of_week": day_of_week, "start_time": time.fromisoformat(start), "end_time": time.fromisoformat(end),
            "duration_minutes": minutes, "fee": 500, "capacity": capacity}


class GuardedSlotsTable(FakeSlotsTable):
    """Applies the materialiser's close condition: a slot with bookings or holds is never closed."""

    def update_item(self, Key, **kwargs):
        super().update_item(Key, **kwargs)
        item = next(i for i in self.items if i["doctor_id"] == Key["doctor_id"] and i["slot_key"] == Key["slot_key"])
        if "booked_count=:zero" in kwargs.get("ConditionExpression", "") and (item["booked_count"] or item["held_count"]):
            self.updates.pop()
            raise ClientError({"Error": {"Code": "ConditionalCheckFailedException", "Message": "booked"}}, "UpdateItem")
        item["status"] = kwargs["ExpressionAttributeValues"][":closed" if "=:closed," in kwargs["UpdateExpression"] else ":open"]
        return {}


def test_items_carry_the_table_key_and_a_slot_id_that_encodes_it():
    desired, skipped = materialiser.desired_slots_for_doctor([pattern()], set(), IST, NOW, 7)
    assert skipped == 0 and len(desired) == 2                                 # Wed 25 Nov 09:00 and 09:30 IST
    for sid, item in desired.items():
        assert item["doctor_id"] == 12 and item["slot_key"] == f"{item['start_at']}#2"
        assert parse_slot_id(sid) == (item["doctor_id"], item["slot_key"])  # any Lambda can address it from the id
        assert "doctor_date" not in item                                      # the old GSI attribute is gone
    assert sorted(i["start_at"] for i in desired.values()) == ["2026-11-25T03:30:00Z", "2026-11-25T04:00:00Z"]


def test_same_start_different_consultation_types_are_separate_items():
    desired, _ = materialiser.desired_slots_for_doctor([pattern(type_id=1, minutes=60), pattern(type_id=2, minutes=60)],
                                                       set(), IST, NOW, 7)
    assert sorted(i["slot_key"] for i in desired.values()) == ["2026-11-25T03:30:00Z#1", "2026-11-25T03:30:00Z#2"]


def test_rerunning_never_overwrites_existing_slots():
    table = FakeSlotsTable()
    desired, _ = materialiser.desired_slots_for_doctor([pattern()], set(), IST, NOW, 7)
    assert materialiser.create_missing(table, desired, []) == (2, 0)
    table.items[0]["booked_count"] = 1                                        # a patient booked meanwhile
    assert materialiser.create_missing(table, desired, []) == (0, 2)
    assert table.items[0]["booked_count"] == 1


def test_reconcile_reads_the_whole_window_with_one_query_and_closes_only_unbooked_slots():
    table = GuardedSlotsTable(page_size=1000)
    old, _ = materialiser.desired_slots_for_doctor([pattern(start="09:00", end="11:00")], set(), IST, NOW, 45)
    materialiser.create_missing(table, old, [])
    booked = sorted(table.items, key=lambda i: i["slot_key"])[1]
    booked["booked_count"] = 1
    new, _ = materialiser.desired_slots_for_doctor([pattern(start="09:00", end="09:30")], set(), IST, NOW, 45)
    closed, reopened, kept = materialiser.reconcile_doctor(table, 12, IST, new, NOW, 45)
    assert len(table.queries) == 1                                            # was one Query per day (45)
    assert (closed, reopened, kept) == (len(old) - len(new) - 1, 0, 1)
    assert booked["status"] == "OPEN"                                         # booked slots are never closed
    assert all(set(k) == {"doctor_id", "slot_key"} for k, _ in table.updates)


def test_reconcile_reopens_slots_it_closed_itself():
    table = GuardedSlotsTable()
    desired, _ = materialiser.desired_slots_for_doctor([pattern()], set(), IST, NOW, 7)
    materialiser.create_missing(table, desired, [])
    for i in table.items:
        i.update(status="CLOSED", closed_reason=materialiser.CLOSED_REASON)
    assert materialiser.reconcile_doctor(table, 12, IST, desired, NOW, 7) == (0, 2, 0)
    assert {i["status"] for i in table.items} == {"OPEN"}


def test_reconcile_ignores_other_doctors_and_past_slots():
    table = GuardedSlotsTable()
    desired, _ = materialiser.desired_slots_for_doctor([pattern(doctor=99)], set(), IST, NOW, 7)
    materialiser.create_missing(table, desired, [])
    table.add({"doctor_id": 12, "slot_id": "SLOT#12_20261124T020000Z_2", "consultation_type_id": 2,
               "start_at": "2026-11-24T02:00:00Z", "status": "OPEN", "booked_count": 0, "held_count": 0})
    assert materialiser.reconcile_doctor(table, 12, IST, {}, NOW, 7) == (0, 0, 0)
    assert table.updates == []
#