import importlib.util
from pathlib import Path

SCRIPT = Path(__file__).parents[1] / "scripts" / "live_sliding_sync_check.py"
SPEC = importlib.util.spec_from_file_location("live_sliding_sync_check", SCRIPT)
assert SPEC and SPEC.loader
live_check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(live_check)


def test_recovery_order_allows_live_events_to_overtake_history():
    canonical = ["$history1", "$history2", "$live1", "$live2"]
    observed = ["$live1", "$live2", "$history1", "$history2"]
    is_live = {
        "$history1": False,
        "$history2": False,
        "$live1": True,
        "$live2": True,
    }

    assert live_check.recovery_lane_orders(canonical, observed, is_live) == {
        "recovered": (["$history1", "$history2"], ["$history1", "$history2"]),
        "live": (["$live1", "$live2"], ["$live1", "$live2"]),
    }


def test_recovery_order_rejects_reordering_inside_a_lane():
    canonical = ["$history1", "$history2", "$live1", "$live2"]
    observed = ["$live2", "$live1", "$history2", "$history1"]
    is_live = {
        "$history1": False,
        "$history2": False,
        "$live1": True,
        "$live2": True,
    }

    assert live_check.recovery_lane_orders(canonical, observed, is_live) == {
        "recovered": (["$history1", "$history2"], ["$history2", "$history1"]),
        "live": (["$live1", "$live2"], ["$live2", "$live1"]),
    }
