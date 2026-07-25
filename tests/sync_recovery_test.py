"""Owning-seam tests for durable room recovery state."""

import asyncio
import threading
import time

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
from nio.store import MatrixStore

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


class BlockingStore:
    supports_threaded_writes = True

    def __init__(self):
        self.started = threading.Event()
        self.release = threading.Event()
        self.writes: list[str] = []

    def save_recovery(self, token, clear_rooms, gaps, events, clear_recovered):
        if token == "s2":
            self.started.set()
            self.release.wait()
        self.writes.append(token)


class BlockingMutationStore:
    supports_threaded_writes = True

    def __init__(self, operation):
        self.operation = operation
        self.started = threading.Event()
        self.release = threading.Event()

    def _run(self, operation):
        if self.operation == operation:
            self.started.set()
            self.release.wait()

    def save_recovery(self, *args):
        self._run("progress")

    def finish_recovery(self, room_id, generation, event_id, was_encrypted):
        self._run("acknowledge" if event_id else "delete")


class InlineStore:
    supports_threaded_writes = False

    def __init__(self):
        self.thread_ids: list[int] = []

    def save_recovery(self, *args):
        self.thread_ids.append(threading.get_ident())

    def finish_recovery(self, room_id, generation, event_id, was_encrypted):
        self.thread_ids.append(threading.get_ident())


@pytest.mark.asyncio
async def test_cancelled_old_commit_stays_ahead_of_newer_commit():
    state = RecoveryState()
    store = BlockingStore()
    gap = RecoveryGap(ROOM, 1, "p1", "s1")
    first = asyncio.create_task(
        persist_response_plan(
            state,
            store,
            token="s2",
            plan=RecoveryPlan(gaps=(gap,)),
        )
    )
    await asyncio.to_thread(store.started.wait)
    first.cancel()
    second = asyncio.create_task(
        persist_response_plan(
            state,
            store,
            token="s3",
            plan=RecoveryPlan(),
        )
    )
    await asyncio.sleep(0)
    assert store.writes == []

    store.release.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert state.gaps == {ROOM: [gap]}
    await second
    assert store.writes == ["s2", "s3"]


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

    async def dispatch(_room, value):
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

    async def dispatch(_room, value):
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

    async def dispatch(_room, value):
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

    async def dispatch(room_id, value):
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
async def test_expired_budget_commits_unverifiable_page():
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
    assert state.events[(ROOM, 1)] == [value]


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

    async def dispatch(_room, value):
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

    async def dispatch(_room, _event):
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
    store.supports_threaded_writes = MatrixStore.supports_threaded_writes
    thread_id = threading.get_ident()
    await persist_response_plan(
        state,
        store,
        token="s2",
        plan=RecoveryPlan(gaps=(RecoveryGap(ROOM, 1, "p1", "s1"),)),
    )
    assert store.thread_ids == [thread_id]


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["progress", "acknowledge", "delete"])
async def test_committed_mutation_updates_memory_before_cancellation(operation):
    cursor = "s1" if operation == "progress" else None
    queued = [] if operation == "delete" else [pending("$live", 0)]
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", cursor)]},
        events={(ROOM, 1): queued},
    )
    store = BlockingMutationStore(operation)

    async def fetch(*args):
        return RoomMessagesResponse.from_dict(
            {
                "start": "s1",
                "end": "p1",
                "chunk": [event("$gap", 1).source],
            },
            ROOM,
        )

    async def dispatch(_room, value):
        return value

    task = asyncio.create_task(
        pump_recovery(
            state,
            user_id="@me:example.org",
            options=RecoveryOptions(1, 10, 10, 10),
            fetch_messages=fetch,
            dispatch_event=dispatch,
            store=store,
        )
    )
    await asyncio.to_thread(store.started.wait)
    task.cancel()
    store.release.set()
    with pytest.raises(asyncio.CancelledError):
        await task

    if operation == "progress":
        assert state.gaps[ROOM][0].cursor_token is None
        assert [item.event_id for item in state.events[(ROOM, 1)]] == [
            "$gap",
            "$live",
        ]
    elif operation == "acknowledge":
        assert state.events[(ROOM, 1)] == []
        assert state.gaps
    else:
        assert not state.gaps
