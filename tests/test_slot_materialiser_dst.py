import os, sys, importlib.util
from pathlib import Path
os.environ.setdefault("AWS_DEFAULT_REGION","ap-south-1")
os.environ.setdefault("AWS_REGION","ap-south-1")
ROOT=Path(__file__).resolve().parents[1]
sys.path.insert(0,str(ROOT/"lambdas"))
spec=importlib.util.spec_from_file_location("slot_materialiser_under_test",ROOT/"lambdas/slot_materialiser/lambda_function.py")
materialiser=importlib.util.module_from_spec(spec)
spec.loader.exec_module(materialiser)
from datetime import datetime, timezone
from zoneinfo import ZoneInfo

def test_spring_forward_nonexistent_wall_time_is_skipped():
    # New York jumps from 01:59:59 to 03:00 on 2026-03-08.
    assert materialiser.resolve_local(datetime(2026,3,8,2,30),ZoneInfo("America/New_York")) is None

def test_fall_back_ambiguous_time_uses_first_fold_deterministically():
    # New York repeats 01:30 on 2026-11-01; policy is fold=0 (first occurrence).
    result=materialiser.resolve_local(datetime(2026,11,1,1,30),ZoneInfo("America/New_York"))
    assert result == datetime(2026,11,1,5,30,tzinfo=timezone.utc)

def test_normal_local_time_round_trips_across_dst_season():
    result=materialiser.resolve_local(datetime(2026,11,2,9,0),ZoneInfo("America/New_York"))
    assert result == datetime(2026,11,2,14,0,tzinfo=timezone.utc)

def test_slot_id_is_stable_for_reruns():
    first=materialiser.stable_slot_id(42,3,"2026-11-02T14:00:00Z")
    second=materialiser.stable_slot_id(42,3,"2026-11-02T14:00:00Z")
    assert first == second
    assert first.startswith("SLOT#")
    assert first != materialiser.stable_slot_id(42,3,"2026-11-02T14:30:00Z")
