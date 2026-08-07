import importlib.util
from pathlib import Path
from types import SimpleNamespace

import pytest

from nio import TimelineEventProvenance

SCRIPT = Path(__file__).parents[1] / "scripts" / "live_sliding_sync_check.py"
SPEC = importlib.util.spec_from_file_location("live_sliding_sync_check", SCRIPT)
assert SPEC and SPEC.loader
live_check = importlib.util.module_from_spec(SPEC)
SPEC.loader.exec_module(live_check)


@pytest.mark.asyncio
async def test_recorder_observe_recovery_forwards_all_dispatch_arguments():
    recorder = live_check.Recorder("test")
    forwarded = []
    result = object()

    async def original(
        room_id,
        event,
        was_completed,
        kind,
        provenance,
        sync_origin,
        apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        forwarded.append(
            (
                room_id,
                event,
                was_completed,
                kind,
                provenance,
                sync_origin,
                apply_room_state,
                admission_accepted,
                mark_admission_accepted,
            )
        )
        return result

    client = SimpleNamespace(_dispatch_timeline_event=original)
    recorder.observe_recovery(client)

    positional_event = SimpleNamespace(event_id="$positional")
    positional_was_completed = object()
    positional_sync_origin = object()
    positional_apply_room_state = object()
    positional_admission_accepted = object()
    positional_mark = lambda: None
    positional = (
        "!positional:example.org",
        positional_event,
        positional_was_completed,
        "timeline",
        TimelineEventProvenance.LIVE,
        positional_sync_origin,
        positional_apply_room_state,
        positional_admission_accepted,
        positional_mark,
    )
    assert await client._dispatch_timeline_event(*positional) is result

    keyword_event = SimpleNamespace(event_id="$keyword")
    keyword_was_completed = object()
    keyword_sync_origin = object()
    keyword_apply_room_state = object()
    keyword_admission_accepted = object()
    keyword_mark = lambda: None
    keyword = (
        "!keyword:example.org",
        keyword_event,
        keyword_was_completed,
        "timeline",
        TimelineEventProvenance.RECOVERED,
        keyword_sync_origin,
        keyword_apply_room_state,
        keyword_admission_accepted,
        keyword_mark,
    )

    assert (
        await client._dispatch_timeline_event(
            room_id="!keyword:example.org",
            event=keyword_event,
            was_completed=keyword_was_completed,
            kind="timeline",
            provenance=TimelineEventProvenance.RECOVERED,
            sync_origin=keyword_sync_origin,
            apply_room_state=keyword_apply_room_state,
            admission_accepted=keyword_admission_accepted,
            mark_admission_accepted=keyword_mark,
        )
        is result
    )
    assert forwarded == [positional, keyword]
    assert recorder.recovery_live == {
        "$positional": positional_sync_origin,
        "$keyword": keyword_sync_origin,
    }


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
