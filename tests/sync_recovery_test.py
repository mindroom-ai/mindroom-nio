"""Owning-seam tests for durable room recovery state."""

import asyncio
import threading
import time

import nio.client.sync_recovery as sync_recovery
import pytest

from nio import Event, RoomMessageText
from nio.client.sync_recovery import (
    PendingTimelineEvent,
    RecoveryGap,
    RecoveryOptions,
    RecoveryPlan,
    RecoveryState,
    apply_plan,
    persist_response_plan,
    plan_room_timeline,
    pump_recovery,
)
from nio.responses import RoomMessagesResponse

ROOM = "!room:example.org"
ROOM_B = "!other:example.org"


def event(event_id: str, ts: int, room_id: str = ROOM) -> RoomMessageText:
    value = RoomMessageText.from_dict(
        {
            "content": {"body": event_id, "msgtype": "m.text"},
            "event_id": event_id,
            "sender": "@sender:example.org",
            "origin_server_ts": ts,
            "room_id": room_id,
            "type": "m.room.message",
        }
    )
    assert isinstance(value, RoomMessageText)
    return value


def pending(event_id: str, sequence: int) -> PendingTimelineEvent:
    value = PendingTimelineEvent.from_event(
        ROOM, 1, sequence, event(event_id, sequence), event_id.startswith("$live")
    )
    assert value
    return value


def test_no_id_keys_are_scoped_to_the_sliding_connection():
    state = RecoveryState()
    bad = Event.parse_event({"type": "broken"})
    first = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[bad],
        user_id="@me:example.org",
        membership="join",
        batch_id="sliding:first:pos",
    )
    apply_plan(state, first)
    second = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[bad],
        user_id="@me:example.org",
        membership="join",
        batch_id="sliding:second:pos",
    )
    assert first.events[0].event_id != second.events[0].event_id


class InlineStore:
    supports_threaded_writes = False

    def __init__(self):
        self.thread_ids: list[int] = []

    def save_recovery(self, *args):
        self.thread_ids.append(threading.get_ident())

    def finish_recovery(self, room_id, generation, event_id, was_encrypted):
        self.thread_ids.append(threading.get_ident())


@pytest.mark.asyncio
async def test_callback_crash_replays_only_active_row():
    state = RecoveryState(
        gaps={
            ROOM: [
                RecoveryGap(
                    ROOM,
                    1,
                    "p1",
                    None,
                )
            ]
        },
        events={
            (ROOM, 1): [pending("$one", 0), pending("$two", 1), pending("$live", 2)]
        },
    )
    calls: list[str] = []
    failed = False

    async def dispatch(_room, value, _is_live, _was_completed, _kind):
        nonlocal failed
        calls.append(value.event_id)
        if value.event_id == "$two" and not failed:
            failed = True
            raise RuntimeError("crash after callback effect")
        return value

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    options = RecoveryOptions(1, 10, 10, 10)
    kwargs = {
        "user_id": "@me:example.org",
        "options": options,
        "fetch_messages": unused_fetch,
        "dispatch_event": dispatch,
        "store": None,
    }
    await pump_recovery(state, **kwargs)
    assert calls == ["$one", "$two"]
    assert [item.event_id for item in state.events[(ROOM, 1)]] == [
        "$two",
        "$live",
    ]

    await pump_recovery(state, **kwargs)
    assert calls == ["$one", "$two", "$two", "$live"]
    assert not state.gaps


@pytest.mark.asyncio
async def test_later_generation_suffix_does_not_deadlock_older_gap():
    gaps = [
        RecoveryGap(ROOM, 1, "p1", "s1"),
        RecoveryGap(ROOM, 2, "p2", "s2"),
    ]
    state = RecoveryState(
        gaps={ROOM: gaps},
        events={
            (ROOM, 1): [pending("$live1", 0)],
            (ROOM, 2): [
                PendingTimelineEvent.from_event(ROOM, 2, 0, event("$live2", 4), True)
            ],
        },
    )
    seen: list[str] = []

    async def fetch(_room, start, *_args):
        values = (
            [event("$gap", 1), event("$live1", 2), event("$live2", 4)]
            if start == "s1"
            else [event("$live2", 4)]
        )
        return RoomMessagesResponse.from_dict(
            {
                "start": start,
                "end": "p1" if start == "s1" else "p2",
                "chunk": [value.source for value in values],
            },
            ROOM,
        )

    async def dispatch(_room, value, _is_live, _was_completed, _kind):
        seen.append(value.event_id)
        return value

    kwargs = {
        "user_id": "@me:example.org",
        "options": RecoveryOptions(1, 10, 10, 10),
        "fetch_messages": fetch,
        "dispatch_event": dispatch,
        "store": None,
    }
    await pump_recovery(state, **kwargs)
    assert seen == ["$gap", "$live1"]
    assert [gap.generation for gap in state.gaps[ROOM]] == [2]
    await pump_recovery(state, **kwargs)
    assert seen == ["$gap", "$live1", "$live2"]
    assert not state.gaps


@pytest.mark.asyncio
async def test_slow_room_does_not_consume_another_rooms_budget():
    state = RecoveryState(
        gaps={
            ROOM: [RecoveryGap(ROOM, 1, "p1", "s1")],
            ROOM_B: [RecoveryGap(ROOM_B, 1, "p2", "s2")],
        },
        events={(ROOM, 1): [], (ROOM_B, 1): []},
    )
    seen: list[str] = []

    async def fetch(room_id, start, *_args):
        if room_id == ROOM:
            await asyncio.Event().wait()
        value = event("$other", 1, ROOM_B)
        return RoomMessagesResponse.from_dict(
            {"start": start, "end": "p2", "chunk": [value.source]},
            ROOM_B,
        )

    async def dispatch(_room, value, _is_live, _was_completed, _kind):
        seen.append(value.event_id)

    kwargs = {
        "user_id": "@me:example.org",
        "options": RecoveryOptions(1, 10, 10, 0.02),
        "fetch_messages": fetch,
        "dispatch_event": dispatch,
        "store": None,
    }
    await pump_recovery(state, **kwargs)
    assert seen == ["$other"]
    assert ROOM in state.gaps
    assert ROOM_B not in state.gaps


@pytest.mark.asyncio
async def test_ready_callback_does_not_consume_recovering_room_budget():
    other_live = PendingTimelineEvent.from_event(
        ROOM_B, 1, 0, event("$other-live", 2, ROOM_B), True
    )
    assert other_live
    state = RecoveryState(
        gaps={
            ROOM: [RecoveryGap(ROOM, 1, "p1", None)],
            ROOM_B: [RecoveryGap(ROOM_B, 1, "p2", "s2")],
        },
        events={(ROOM, 1): [pending("$live", 0)], (ROOM_B, 1): [other_live]},
    )
    seen: list[str] = []

    async def fetch(room_id, start, *_args):
        assert room_id == ROOM_B
        value = event("$other", 1, ROOM_B)
        return RoomMessagesResponse.from_dict(
            {"start": start, "end": "p2", "chunk": [value.source]},
            ROOM_B,
        )

    async def dispatch(room_id, value, _is_live, _was_completed, _kind):
        if room_id == ROOM:
            await asyncio.Event().wait()
        seen.append(value.event_id)

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 0.04),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert seen == ["$other", "$other-live"]
    assert ROOM in state.gaps
    assert ROOM_B not in state.gaps


@pytest.mark.asyncio
async def test_expired_budget_commits_exhausted_page_without_dispatch():
    """An exhausted bounded page is kept, but an expired budget defers it.

    The page carries no ``end`` token while the walk was bounded by the
    window's token, so the server has no further events to give and the
    slice is complete. Committing it durably is what lets the next pump
    dispatch it; the expired callback budget only forbids dispatching now.
    """
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", "s1")]},
        events={(ROOM, 1): [value]},
    )

    async def fetch(*_args):
        time.sleep(0.02)
        return RoomMessagesResponse.from_dict(
            {"start": "s1", "chunk": [event("$untrusted", 1).source]},
            ROOM,
        )

    async def dispatch(*_args):
        raise AssertionError("expired callback budget must not dispatch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 0.01),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert state.gaps[ROOM][0].cursor_token is None
    assert [item.event_id for item in state.events[(ROOM, 1)]] == [
        "$untrusted",
        "$live",
    ]
    assert value in state.events[(ROOM, 1)]


@pytest.mark.asyncio
async def test_unbounded_page_without_end_stays_unverifiable():
    """Without a `to` bound an absent `end` proves nothing.

    A walk with no target token cannot tell "nothing further before the
    window" from "the live edge", so the page is still discarded.
    """
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "", "s1")]},
        events={(ROOM, 1): [value]},
    )

    async def fetch(*_args):
        return RoomMessagesResponse.from_dict(
            {"start": "s1", "chunk": [event("$untrusted", 1).source]},
            ROOM,
        )

    seen: list[str] = []

    async def dispatch(_room, item, _is_live, _was_completed, _kind):
        seen.append(item.event_id)

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 10),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert seen == ["$live"]
    assert ROOM not in state.gaps


@pytest.mark.asyncio
async def test_repeated_target_cursor_abandons_unverifiable_page():
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "s1", "s1")]},
        events={(ROOM, 1): [pending("$live", 0)]},
    )
    seen: list[str] = []

    async def fetch(*_args):
        return RoomMessagesResponse.from_dict(
            {
                "start": "s1",
                "end": "s1",
                "chunk": [event("$untrusted", 1).source],
            },
            ROOM,
        )

    async def dispatch(_room, value, _is_live, _was_completed, _kind):
        seen.append(value.event_id)

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 10),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert seen == ["$live"]
    assert not state.gaps


@pytest.mark.asyncio
async def test_corrupt_persisted_event_is_discarded_and_acknowledged():
    corrupt = PendingTimelineEvent(ROOM, 1, 0, "$bad", "{", False, True)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "", None)]},
        events={(ROOM, 1): [corrupt]},
    )

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    async def unused_dispatch(*args):
        raise AssertionError("corrupt event must not dispatch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 10),
        fetch_messages=unused_fetch,
        dispatch_event=unused_dispatch,
        store=None,
    )
    assert not state.gaps
    assert state.completed[ROOM] == {"$bad": True}


@pytest.mark.asyncio
async def test_hanging_callback_leaves_active_row_pending():
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [value]},
    )

    async def dispatch(_room, _event, _is_live, _was_completed, _kind):
        await asyncio.Event().wait()

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 0.02),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert state.events[(ROOM, 1)] == [value]
    assert ROOM in state.gaps


@pytest.mark.asyncio
async def test_default_store_commit_stays_on_event_loop_thread():
    state = RecoveryState()
    store = InlineStore()
    thread_id = threading.get_ident()
    persist_response_plan(
        state,
        store,
        token="s2",
        plan=RecoveryPlan(gaps=(RecoveryGap(ROOM, 1, "p1", "s1"),)),
    )
    assert store.thread_ids == [thread_id]


@pytest.mark.asyncio
async def test_repeated_end_overlap_does_not_recover_suffix():
    present_one = PendingTimelineEvent.from_event(
        ROOM, 1, 0, event("$present-one", 2), True
    )
    present_two = PendingTimelineEvent.from_event(
        ROOM, 1, 1, event("$present-two", 4), True
    )
    assert present_one and present_two
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]},
        events={(ROOM, 1): [present_one, present_two]},
    )
    seen: list[str] = []

    async def fetch(*_args):
        return RoomMessagesResponse.from_dict(
            {
                "start": "cursor",
                "end": "cursor",
                "chunk": [
                    event("$present-two", 4).source,
                    event("$gap-two", 3).source,
                    event("$present-one", 2).source,
                    event("$gap-one", 1).source,
                ],
            },
            ROOM,
        )

    async def dispatch(_room, value, _is_live, _was_completed, _kind):
        seen.append(value.event_id)
        return value

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 10),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert seen == ["$present-one", "$present-two"]
    assert not state.gaps


@pytest.mark.asyncio
async def test_room_cap_abandons_existing_unverified_prefix():
    recovered = PendingTimelineEvent.from_event(
        ROOM, 1, 0, event("$recovered", 1), False
    )
    assert recovered
    live = pending("$live", 1)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]},
        events={(ROOM, 1): [recovered, live]},
    )
    seen: list[str] = []

    async def fetch(*_args):
        return RoomMessagesResponse.from_dict(
            {
                "start": "cursor",
                "end": "more",
                "chunk": [event("$overflow", 2).source],
            },
            ROOM,
        )

    async def dispatch(_room, value, _is_live, _was_completed, _kind):
        seen.append(value.event_id)
        return value

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 1, 10, 10),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert seen == ["$live"]
    assert not state.gaps


@pytest.mark.parametrize("clear_mode", ["recovered", "room"])
def test_abandonment_restores_promoted_completed_marker(clear_mode):
    retry = PendingTimelineEvent.from_event(
        ROOM,
        1,
        0,
        event("$encrypted", 1),
        False,
        was_completed=True,
    )
    assert retry
    gap = RecoveryGap(ROOM, 1, "target", "cursor")
    state = RecoveryState(
        gaps={ROOM: [gap]},
        events={(ROOM, 1): [retry]},
    )
    plan = (
        RecoveryPlan(clear_recovered=gap)
        if clear_mode == "recovered"
        else RecoveryPlan(clear_rooms=frozenset({ROOM}))
    )

    apply_plan(state, plan)

    assert state.completed[ROOM]["$encrypted"]


@pytest.mark.asyncio
@pytest.mark.parametrize("overlap_first", [False, True])
async def test_room_cap_abandons_over_cap_page_despite_overlap(overlap_first):
    held = PendingTimelineEvent.from_event(ROOM, 1, 0, event("$held", 3), True)
    assert held
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]},
        events={(ROOM, 1): [held]},
    )
    seen: list[str] = []
    history = [event("$gap-one", 1), event("$gap-two", 2)]
    page = (
        [event("$held", 3), *history]
        if overlap_first
        else [*history, event("$held", 3)]
    )

    async def fetch(*_args):
        return RoomMessagesResponse.from_dict(
            {
                "start": "cursor",
                "end": "cursor",
                "chunk": [value.source for value in page],
            },
            ROOM,
        )

    async def dispatch(_room, value, _is_live, _was_completed, _kind):
        seen.append(value.event_id)
        return value

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 1, 10, 10),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert seen == ["$held"]
    assert not state.gaps


@pytest.mark.asyncio
async def test_room_cap_counts_recovered_rows_in_other_generations():
    later_recovered = PendingTimelineEvent.from_event(
        ROOM, 2, 0, event("$later-recovered", 3), False
    )
    assert later_recovered
    live = pending("$live", 0)
    state = RecoveryState(
        gaps={
            ROOM: [
                RecoveryGap(ROOM, 1, "target-one", "cursor-one"),
                RecoveryGap(ROOM, 2, "target-two", "cursor-two"),
            ]
        },
        events={(ROOM, 1): [live], (ROOM, 2): [later_recovered]},
    )
    seen: list[str] = []

    async def fetch(_room, start, *_args):
        assert start == "cursor-one"
        return RoomMessagesResponse.from_dict(
            {
                "start": start,
                "end": "more",
                "chunk": [event("$overflow", 2).source],
            },
            ROOM,
        )

    async def dispatch(_room, value, _is_live, _was_completed, _kind):
        seen.append(value.event_id)
        return value

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 1, 10, 10),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert seen == ["$live"]
    assert [gap.generation for gap in state.gaps[ROOM]] == [2]
    assert [
        queued.event_id
        for (room_id, _generation), queued_events in state.events.items()
        if room_id == ROOM
        for queued in queued_events
        if not queued.is_live
    ] == ["$later-recovered"]


def test_recovery_outcome_keeps_open_real_gap_unrecovered():
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 2, "p2", "s2")]},
    )
    state.outcomes = {ROOM: True, ROOM_B: False}

    assert sync_recovery.take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM, ROOM_B}),
    )
    assert sync_recovery.take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
    )


def test_clearing_real_gap_is_unrecovered_but_synthetic_gap_is_not():
    state = RecoveryState(
        gaps={
            ROOM: [RecoveryGap(ROOM, 1, "p1", "s1")],
            ROOM_B: [RecoveryGap(ROOM_B, 1, "", None)],
        },
        events={(ROOM, 1): [], (ROOM_B, 1): []},
    )

    apply_plan(state, RecoveryPlan(clear_rooms=frozenset({ROOM, ROOM_B})))

    assert sync_recovery.take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
    )
