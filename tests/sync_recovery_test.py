"""Owning-seam tests for durable room recovery state."""

import asyncio
import json
import threading
import time
from collections import OrderedDict
from types import SimpleNamespace

import nio.client.sync_recovery as sync_recovery
import pytest

from nio import (
    AsyncClient,
    Event,
    LocalProtocolError,
    MegolmEvent,
    RoomMemberEvent,
    RoomMessageText,
    RoomInfo,
    Timeline,
    TimelineEventProvenance,
    TypingNoticeEvent,
)
from nio.client import async_client as async_client_module
from nio.client.sync_recovery import (
    PendingTimelineEvent,
    RecoveryGap,
    RecoveryOptions,
    RecoveryPlan,
    RecoveryState,
    apply_plan,
    persist_response_plan,
    plan_room_timeline,
    plan_sync_response,
    pump_recovery,
    record_completed_timeline_event,
    take_recovery_outcomes,
)
from nio.responses import RoomMessagesError, RoomMessagesResponse, SlidingSyncResponse

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


def own_join(event_id: str) -> RoomMemberEvent:
    value = RoomMemberEvent.from_dict(
        {
            "content": {"membership": "join"},
            "event_id": event_id,
            "sender": "@me:example.org",
            "state_key": "@me:example.org",
            "origin_server_ts": 1,
            "room_id": ROOM,
            "type": "m.room.member",
        }
    )
    assert isinstance(value, RoomMemberEvent)
    return value


def encrypted_event(event_id: str) -> MegolmEvent:
    value = Event.parse_event(
        {
            "event_id": event_id,
            "sender": "@sender:example.org",
            "origin_server_ts": 1,
            "room_id": ROOM,
            "type": "m.room.encrypted",
            "content": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "ciphertext": "AwgAEnACgAkLmt6q",
                "device_id": "DEVICEID",
                "sender_key": "sender-key",
                "session_id": "session-id",
            },
        }
    )
    assert isinstance(value, MegolmEvent)
    return value


def pending(event_id: str, sequence: int) -> PendingTimelineEvent:
    is_live = event_id.startswith("$live")
    value = PendingTimelineEvent.from_event(
        ROOM,
        1,
        sequence,
        event(event_id, sequence),
        is_live,
    )
    assert value
    return value


def test_loaded_recovery_keeps_page_chronology_across_live_boundary():
    def row(value, sequence, is_live):
        return SimpleNamespace(
            room_id=ROOM,
            generation=1,
            sequence=sequence,
            event_id=value.event_id,
            source_json=json.dumps(value.source),
            is_live=is_live,
            was_encrypted=False,
            was_completed=False,
            kind="timeline",
            admission_accepted=False,
            provenance=(
                TimelineEventProvenance.LIVE
                if is_live
                else TimelineEventProvenance.HISTORY
            ),
            apply_room_state=is_live,
        )

    state = RecoveryState()
    events = [
        row(event("$gap", 1), 0, False),
        row(event("$held", 2), 1, True),
        row(event("$overflow", 3), 2, False),
    ]

    sync_recovery.load_recovery_state(state, (), events)

    assert [item.event_id for item in state.events[(ROOM, 1)]] == [
        "$gap",
        "$held",
        "$overflow",
    ]


def test_loaded_legacy_abandoned_room_ids_become_unknown():
    state = RecoveryState()

    sync_recovery.load_recovery_state(state, (), (), [ROOM])

    assert state.abandoned == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.UNKNOWN})
    }


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


def test_received_timeline_stays_after_a_recovered_only_prefix():
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [pending("$gap", 0)]},
    )

    plan = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[event("$live", 2)],
        user_id="@me:example.org",
        membership="join",
    )

    assert len(plan.events) == 1
    assert plan.events[0].sequence == 1
    assert plan.events[0].is_live


def test_received_ancillary_event_stays_after_a_recovered_only_prefix():
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [pending("$gap", 7)]},
    )
    notice = TypingNoticeEvent(["@sender:example.org"])
    notice.source = {
        "type": "m.typing",
        "content": {"user_ids": ["@sender:example.org"]},
    }

    plan = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[],
        user_id="@me:example.org",
        membership="join",
        batch_id="sync:s2",
        ephemeral_events=[notice],
    )
    apply_plan(state, plan)

    assert [item.event_id for item in state.events[(ROOM, 1)]] == [
        "$gap",
        "~sync:s2:ephemeral:0",
    ]


def test_classic_initial_own_join_names_stale_real_gap_as_baseline_lost():
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]},
        events={(ROOM, 1): [pending("$live-old", 0)]},
    )

    plan = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[own_join("$join")],
        user_id="@me:example.org",
        membership="join",
        live_event_count=None,
        provenance_live_event_count=0,
    )

    assert plan.clear_rooms == frozenset({ROOM})
    assert plan.clear_room_reasons == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.BASELINE_LOST})
    }


def test_membership_reset_names_the_real_gap_loss_in_the_plan():
    """The producer says why, rather than leaving persistence to guess.

    A reset drops the baseline the walk was anchored to, which nio cannot
    resume -- but it establishes nothing about the history itself, so it must
    not be reported the same way as a walk proven unable to advance.
    """
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]},
    )

    plan = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[],
        user_id="@me:example.org",
        membership="leave",
    )

    assert plan.clear_rooms == frozenset({ROOM})
    assert plan.clear_room_reasons == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.BASELINE_LOST})
    }


def test_membership_reset_of_a_synthetic_gap_is_not_a_loss():
    """No real gap means no work was lost, so nothing is abandoned."""
    state = RecoveryState(gaps={ROOM: [RecoveryGap(ROOM, 1, "", None)]})

    plan = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[],
        user_id="@me:example.org",
        membership="leave",
    )

    assert plan.clear_rooms == frozenset({ROOM})
    assert plan.abandoned_rooms == {}
    assert plan.clear_room_reasons == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.BASELINE_LOST})
    }


def test_sync_history_counts_toward_held_cap():
    state = RecoveryState(max_held_events=1)
    first = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[event("$history-1", 1)],
        user_id="@me:example.org",
        membership="join",
        provenance_live_event_count=0,
    )
    apply_plan(state, first)

    second = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[event("$history-2", 2)],
        user_id="@me:example.org",
        membership="join",
        provenance_live_event_count=0,
    )

    assert second.clear_rooms == frozenset({ROOM})
    assert second.abandoned_rooms == {}


def test_room_reset_preserves_unaccepted_sync_history():
    state = RecoveryState()
    first = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[event("$history", 1)],
        user_id="@me:example.org",
        membership="join",
        provenance_live_event_count=0,
    )
    apply_plan(state, first)

    reset = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[],
        user_id="@me:example.org",
        membership="leave",
    )

    assert [item.event_id for item in reset.events] == ["$history"]


def test_stale_classic_sync_remains_live_without_applying_room_state():
    plan = plan_sync_response(
        RecoveryState(),
        user_id="@me:example.org",
        request_since="s0",
        response_token="s1",
        joined_rooms={
            ROOM: RoomInfo(
                Timeline([event("$history", 1)], False, None),
                [],
                [],
                [],
            )
        },
        current_room_ids=frozenset(),
    )

    assert len(plan.events) == 1
    assert plan.events[0].is_live
    assert plan.events[0].provenance is TimelineEventProvenance.LIVE
    assert not plan.events[0].apply_room_state


def test_a_limited_classic_timeline_records_a_membership_bounded_gap():
    """A limited classic sync knows both ends of the gap it just opened.

    Everything at or before ``since`` arrived in an earlier sync, so a
    backfill that runs out of pages before matching the target token has
    still seen all of it. Without this the walk can only ever be proven by
    the end token happening to equal the target, and a burst of genuinely
    new messages stays classified as history.
    """
    plan = plan_sync_response(
        RecoveryState(),
        user_id="@me:example.org",
        request_since="s0",
        response_token="s1",
        joined_rooms={
            ROOM: RoomInfo(
                Timeline([event("$new", 1)], True, "p1"),
                [],
                [],
                [],
            )
        },
    )

    assert [gap.membership_bound for gap in plan.gaps] == [True]
    assert [gap.cursor_token for gap in plan.gaps] == ["s0"]


def test_an_unlimited_classic_timeline_records_no_bounded_gap():
    """Nothing was skipped, so there is no bound to claim."""
    plan = plan_sync_response(
        RecoveryState(),
        user_id="@me:example.org",
        request_since="s0",
        response_token="s1",
        joined_rooms={
            ROOM: RoomInfo(
                Timeline([event("$new", 1)], False, "p1"),
                [],
                [],
                [],
            )
        },
    )

    assert not any(gap.membership_bound for gap in plan.gaps)


class InlineStore:
    supports_threaded_writes = False

    def __init__(self):
        self.thread_ids: list[int] = []
        self.finished: list[tuple[str, int, str | None, bool]] = []
        self.finished_batches: list[tuple[list[tuple[str, int, str]], list]] = []
        self.accepted: list[tuple[str, int, str]] = []
        self.accepted_batches: list[list[tuple[str, int, str]]] = []
        self.saved_abandoned_rooms: list[tuple[str, ...]] = []
        self.actions: list[str] = []

    def save_recovery(
        self,
        _token,
        _clear_rooms,
        _gaps,
        _events,
        _clear_recovered,
        _window_tokens=None,
        _forgotten_rooms=(),
        _abandoned_rooms=None,
    ):
        self.thread_ids.append(threading.get_ident())
        self.saved_abandoned_rooms.append(tuple(_abandoned_rooms or ()))
        self.actions.append("save")

    def finish_recovery(self, room_id, generation, event_id, was_encrypted):
        self.thread_ids.append(threading.get_ident())
        self.finished.append((room_id, generation, event_id, was_encrypted))
        self.actions.append("finish")

    def accept_recovery_event(self, room_id, generation, event_id):
        self.thread_ids.append(threading.get_ident())
        self.accepted.append((room_id, generation, event_id))

    def accept_recovery_events(self, keys):
        self.thread_ids.append(threading.get_ident())
        self.accepted_batches.append(list(keys))

    def finish_recovery_events(self, keys, completed):
        self.thread_ids.append(threading.get_ident())
        self.finished_batches.append((list(keys), list(completed)))


def accept_admission(admission_accepted, mark_admission_accepted):
    if not admission_accepted:
        mark_admission_accepted()


@pytest.mark.asyncio
async def test_batch_dispatch_groups_store_transitions():
    gap = RecoveryGap(ROOM, 1, "", None)
    queued = [pending("$one", 0), pending("$two", 1), pending("$live-three", 2)]
    state = RecoveryState(
        gaps={ROOM: [gap]},
        events={(ROOM, 1): queued},
    )
    store = InlineStore()

    async def reject_individual(*_args):
        raise AssertionError("timeline rows must use the batch dispatcher")

    async def dispatch_batch(pending_events, events, mark_admission_accepted):
        assert [item.event_id for item in pending_events] == [
            "$one",
            "$two",
            "$live-three",
        ]
        mark_admission_accepted()
        return events

    async def unused_fetch(*_args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 10),
        fetch_messages=unused_fetch,
        dispatch_event=reject_individual,
        dispatch_event_batch=dispatch_batch,
        store=store,
    )

    expected_keys = [
        (ROOM, 1, "$one"),
        (ROOM, 1, "$two"),
        (ROOM, 1, "$live-three"),
    ]
    assert store.accepted_batches == [expected_keys]
    assert [item[0] for item in store.finished_batches] == [expected_keys]
    assert [
        (item.was_encrypted, item.provenance) for item in store.finished_batches[0][1]
    ] == [
        (False, TimelineEventProvenance.HISTORY),
        (False, TimelineEventProvenance.HISTORY),
        (False, TimelineEventProvenance.LIVE),
    ]
    assert store.accepted == []
    assert store.finished == [(ROOM, 1, None, False)]
    assert not state.gaps


@pytest.mark.parametrize(
    ("page_size", "event_count", "expected_batch_sizes"),
    [(10, 25, [10, 10, 5]), (0, 1, [1])],
)
@pytest.mark.asyncio
async def test_batch_dispatch_is_bounded_by_page_size(
    page_size, event_count, expected_batch_sizes
):
    gap = RecoveryGap(ROOM, 1, "", None)
    queued = [pending(f"${index}", index) for index in range(event_count)]
    state = RecoveryState(gaps={ROOM: [gap]}, events={(ROOM, 1): queued})
    batch_sizes: list[int] = []

    async def reject_individual(*_args):
        raise AssertionError("timeline rows must use the batch dispatcher")

    async def dispatch_batch(pending_events, events, mark_admission_accepted):
        batch_sizes.append(len(pending_events))
        mark_admission_accepted()
        return events

    async def unused_fetch(*_args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, event_count, page_size, 10),
        fetch_messages=unused_fetch,
        dispatch_event=reject_individual,
        dispatch_event_batch=dispatch_batch,
        store=None,
    )

    assert batch_sizes == expected_batch_sizes
    assert not state.gaps


@pytest.mark.asyncio
async def test_batch_dispatch_live_failure_completes_failed_live_event():
    gap = RecoveryGap(ROOM, 1, "", None)
    queued = [pending("$one", 0), pending("$live-two", 1), pending("$three", 2)]
    state = RecoveryState(gaps={ROOM: [gap]}, events={(ROOM, 1): queued})
    store = InlineStore()

    async def dispatch_batch(pending_events, events, mark_admission_accepted):
        mark_admission_accepted()
        raise sync_recovery._BatchCallbackError(
            RuntimeError("live callback failed"),
            ((0, events[0]), (1, events[1])),
            accepted=True,
            failed_is_live=True,
        )

    async def unused(*_args):
        raise AssertionError("individual path must not run")

    with pytest.raises(RuntimeError, match="live callback failed"):
        await pump_recovery(
            state,
            user_id="@me:example.org",
            options=RecoveryOptions(1, 10, 10, 10),
            fetch_messages=unused,
            dispatch_event=unused,
            dispatch_event_batch=dispatch_batch,
            store=store,
        )

    assert [item.event_id for item in state.events[(ROOM, 1)]] == ["$three"]
    assert store.finished_batches[0][0] == [
        (ROOM, 1, "$one"),
        (ROOM, 1, "$live-two"),
    ]
    assert state.outcomes == {}


@pytest.mark.asyncio
async def test_batch_dispatch_history_failure_keeps_failed_suffix_pending():
    gap = RecoveryGap(ROOM, 1, "target", None)
    queued = [pending("$one", 0), pending("$two", 1), pending("$three", 2)]
    state = RecoveryState(gaps={ROOM: [gap]}, events={(ROOM, 1): queued})
    store = InlineStore()

    async def dispatch_batch(pending_events, events, mark_admission_accepted):
        mark_admission_accepted()
        raise sync_recovery._BatchCallbackError(
            RuntimeError("history callback failed"),
            ((0, events[0]),),
            accepted=True,
            failed_is_live=False,
        )

    async def unused(*_args):
        raise AssertionError("individual path must not run")

    with pytest.raises(RuntimeError, match="history callback failed"):
        await pump_recovery(
            state,
            user_id="@me:example.org",
            options=RecoveryOptions(1, 10, 10, 10),
            fetch_messages=unused,
            dispatch_event=unused,
            dispatch_event_batch=dispatch_batch,
            store=store,
        )

    assert [item.event_id for item in state.events[(ROOM, 1)]] == [
        "$two",
        "$three",
    ]
    assert store.finished_batches[0][0] == [(ROOM, 1, "$one")]
    assert state.outcomes == {ROOM: False}


@pytest.mark.asyncio
async def test_batch_dispatch_keeps_mixed_kinds_on_individual_path():
    gap = RecoveryGap(ROOM, 1, "", None)
    notice = TypingNoticeEvent(["@sender:example.org"])
    notice.source = {
        "type": "m.typing",
        "content": {"user_ids": ["@sender:example.org"]},
    }
    ephemeral = PendingTimelineEvent(
        ROOM,
        1,
        0,
        "~sync:s1:ephemeral:0",
        json.dumps(notice.source),
        False,
        False,
        kind="ephemeral",
    )
    queued = [ephemeral, pending("$one", 1), pending("$two", 2)]
    state = RecoveryState(gaps={ROOM: [gap]}, events={(ROOM, 1): queued})
    seen: list[tuple[str, str]] = []

    async def dispatch_individual(
        _room,
        value,
        _was_completed,
        kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        _admission_accepted,
        _mark_admission_accepted,
    ):
        seen.append((kind, getattr(value, "event_id", "~ephemeral")))
        return value

    async def reject_batch(*_args):
        raise AssertionError("mixed-kind suffix must not enter a timeline batch")

    async def unused_fetch(*_args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 10),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch_individual,
        dispatch_event_batch=reject_batch,
        store=None,
    )

    assert seen == [
        ("ephemeral", "~ephemeral"),
        ("timeline", "$one"),
        ("timeline", "$two"),
    ]
    assert not state.gaps


def test_batch_completion_validates_memory_before_store_commit():
    gap = RecoveryGap(ROOM, 1, "", None)
    existing = pending("$one", 0)
    missing = pending("$missing", 1)
    state = RecoveryState(
        gaps={ROOM: [gap]},
        events={(ROOM, 1): [existing]},
    )
    store = InlineStore()

    with pytest.raises(
        ValueError,
        match=r"Pending recovery event disappeared: \$missing",
    ):
        sync_recovery._finish_recovery_event_batch(
            state,
            store,
            gap,
            (existing, missing),
            (event("$one", 0), event("$missing", 1)),
        )

    assert store.finished_batches == []
    assert state.events[(ROOM, 1)] == [existing]


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

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        nonlocal failed
        accept_admission(admission_accepted, mark_admission_accepted)
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
                PendingTimelineEvent.from_event(
                    ROOM,
                    2,
                    0,
                    event("$live2", 4),
                    True,
                )
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

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        seen.append(value.event_id)

    kwargs = {
        "user_id": "@me:example.org",
        "options": RecoveryOptions(1, 10, 10, 0.1),
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
        ROOM_B,
        1,
        0,
        event("$other-live", 2, ROOM_B),
        True,
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

    async def dispatch(
        room_id,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", "s1", membership_bound=True)]},
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
    assert state.events[(ROOM, 1)][-1].source_json == value.source_json


@pytest.mark.asyncio
async def test_targeted_unbound_page_without_end_is_sticky_unverifiable():
    """Exhaustion proves continuity only when membership supplied the bound."""
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", "s1")]},
        events={(ROOM, 1): [pending("$live", 0)]},
    )

    async def fetch(*_args):
        return RoomMessagesResponse.from_dict(
            {"start": "s1", "chunk": [event("$untrusted", 1).source]},
            ROOM,
        )

    seen: list[str] = []

    async def dispatch(
        _room,
        item,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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
    expected = {ROOM: frozenset({sync_recovery.RecoveryAbandonment.UNVERIFIABLE})}
    assert take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
        expected,
    )
    assert take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
        expected,
    )


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

    async def dispatch(
        _room,
        item,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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
async def test_unbounded_page_without_end_and_event_cap_retains_both_causes():
    """An exhausted cursor and a local cap are independent terminal facts."""
    state = RecoveryState(gaps={ROOM: [RecoveryGap(ROOM, 1, "", "cursor")]})

    await _pump_one_page(
        state,
        RoomMessagesResponse.from_dict(
            {
                "start": "cursor",
                "chunk": [event("$overflow", 2).source],
            },
            ROOM,
        ),
        options=RecoveryOptions(1, 0, 10, 10),
    )

    assert state.abandoned == {
        ROOM: frozenset(
            {
                sync_recovery.RecoveryAbandonment.EVENT_LIMIT,
                sync_recovery.RecoveryAbandonment.UNVERIFIABLE,
            }
        )
    }


def test_direct_response_construction_normalizes_singular_causes():
    """Response annotations match their runtime values for direct callers."""
    response = SlidingSyncResponse(
        "pos",
        abandoned_rooms={ROOM: sync_recovery.RecoveryAbandonment.BASELINE_LOST},
    )

    assert response.abandoned_rooms == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.BASELINE_LOST})
    }


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

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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
    corrupt = PendingTimelineEvent(
        ROOM,
        1,
        0,
        "$bad",
        "{",
        False,
        True,
        provenance=TimelineEventProvenance.HISTORY,
        apply_room_state=False,
    )
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
    completed = state.completed[ROOM]["$bad"]
    assert completed.was_encrypted
    assert completed.provenance is TimelineEventProvenance.HISTORY


@pytest.mark.asyncio
async def test_corrupt_real_gap_event_is_sticky_abandonment():
    corrupt = PendingTimelineEvent(
        ROOM,
        1,
        0,
        "$bad",
        "{",
        False,
        True,
        provenance=TimelineEventProvenance.HISTORY,
        apply_room_state=False,
    )
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", None)]},
        events={(ROOM, 1): [corrupt]},
    )

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    async def unused_dispatch(*args):
        raise AssertionError("corrupt event must not dispatch")

    store = InlineStore()

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 10),
        fetch_messages=unused_fetch,
        dispatch_event=unused_dispatch,
        store=store,
    )

    assert ROOM not in state.gaps
    assert store.saved_abandoned_rooms == [(ROOM,)]
    assert store.finished == [
        (ROOM, 1, "$bad", True),
        (ROOM, 1, None, False),
    ]
    assert store.actions == ["save", "finish", "finish"]
    expected = {ROOM: frozenset({sync_recovery.RecoveryAbandonment.CORRUPT_EVENT})}
    assert take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
        expected,
    )
    assert take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
        expected,
    )


@pytest.mark.asyncio
async def test_callback_overrunning_deadline_is_not_restarted():
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [value]},
    )
    calls = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def dispatch(
        _room,
        event,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        calls.append("early")
        started.set()
        await release.wait()
        calls.append("late")
        return event

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    first_pump = asyncio.create_task(
        pump_recovery(
            state,
            user_id="@me:example.org",
            options=RecoveryOptions(1, 10, 10, 0.02),
            fetch_messages=unused_fetch,
            dispatch_event=dispatch,
            store=None,
        )
    )
    await started.wait()
    await asyncio.wait_for(first_pump, 1)
    assert calls == ["early"]
    assert [item.event_id for item in state.events[(ROOM, 1)]] == ["$live"]
    assert state.events[(ROOM, 1)][0].admission_accepted
    assert len(state._active_dispatches) == 1
    release.set()
    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 1),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert calls == ["early", "late"]
    assert not state.gaps
    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_completed_timeout_dispatch_is_acknowledged_without_next_pump():
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [value]},
    )
    store = InlineStore()
    started = asyncio.Event()
    release = asyncio.Event()

    async def dispatch(
        _room,
        event,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        started.set()
        await release.wait()
        return event

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 0.01),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch,
        store=store,
    )
    await started.wait()
    task = next(iter(state._active_dispatches.values()))
    release.set()
    await asyncio.wait_for(task, 1)
    await asyncio.sleep(0)

    assert store.finished == [(ROOM, 1, "$live", False)]
    assert not state.events[(ROOM, 1)]
    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_live_callback_failure_after_deadline_is_deferred(caplog):
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [value]},
    )
    store = InlineStore()
    started = asyncio.Event()
    release = asyncio.Event()

    async def dispatch(
        _room,
        _event,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        started.set()
        await release.wait()
        raise sync_recovery._LiveCallbackError(RuntimeError("late failure"), False)

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 0.01),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch,
        store=store,
    )
    await started.wait()
    task = next(iter(state._active_dispatches.values()))
    release.set()
    await asyncio.wait_for(task, 1)
    await asyncio.sleep(0)

    assert not state.events[(ROOM, 1)]
    assert not state._active_dispatches
    assert store.finished == [(ROOM, 1, "$live", False)]
    record = next(
        record
        for record in caplog.records
        if record.message
        == "Recovered event callback failed after its recovery pump returned: $live"
    )
    assert isinstance(record.exc_info[1], RuntimeError)

    with pytest.raises(RuntimeError, match="late failure"):
        await pump_recovery(
            state,
            user_id="@me:example.org",
            options=RecoveryOptions(1, 10, 10, 1),
            fetch_messages=unused_fetch,
            dispatch_event=dispatch,
            store=store,
        )


@pytest.mark.asyncio
async def test_reset_preserves_in_flight_dispatch_without_replay():
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [value]},
    )
    calls: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def dispatch(
        _room,
        event,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        calls.append("early")
        started.set()
        try:
            await release.wait()
        except asyncio.CancelledError:
            calls.append("cancelled")
            await release.wait()
        return event

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 0.01),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch,
        store=None,
    )
    await started.wait()
    active = next(iter(state._active_dispatches.values()))

    apply_plan(
        state,
        plan_room_timeline(
            state,
            room_id=ROOM,
            timeline_events=(),
            user_id="@me:example.org",
            membership="leave",
        ),
    )
    release.set()
    await asyncio.wait_for(active, 1)
    await asyncio.sleep(0)

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 1),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch,
        store=None,
    )
    assert calls == ["early"]
    assert not state.gaps
    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_dispatch_drain_reaches_tasks_registered_while_waiting():
    state = RecoveryState()
    nested_started = asyncio.Event()
    nested_release = asyncio.Event()
    nested_finished = asyncio.Event()

    async def nested_dispatch():
        nested_started.set()
        await nested_release.wait()
        nested_finished.set()

    async def first_dispatch():
        state._active_dispatches[(ROOM, "$live", "timeline")] = asyncio.create_task(
            nested_dispatch()
        )

    state._active_dispatches[(ROOM, "$live", "timeline")] = asyncio.create_task(
        first_dispatch()
    )
    drain = asyncio.create_task(sync_recovery.drain_recovery_dispatches(state))
    await nested_started.wait()

    with pytest.raises(asyncio.TimeoutError):
        await asyncio.wait_for(asyncio.shield(drain), 0.01)
    nested_release.set()
    await drain

    assert nested_finished.is_set()
    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_room_drain_clears_every_failed_batch_alias():
    state = RecoveryState()

    async def failed_batch():
        return sync_recovery._LiveCallbackError(
            RuntimeError("batch callback failed"),
            False,
            accepted=False,
        )

    task = asyncio.create_task(failed_batch())
    await task
    for event_id in ("$one", "$two", "$three"):
        state._active_dispatches[(ROOM, event_id, "timeline")] = task

    with pytest.raises(RuntimeError, match="batch callback failed"):
        await sync_recovery.drain_recovery_room_dispatches(state, {ROOM})

    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_close_drains_without_caller_exclusion(monkeypatch):
    calls = 0

    async def drain(_state):
        nonlocal calls
        calls += 1

    monkeypatch.setattr(async_client_module, "drain_recovery_dispatches", drain)
    client = AsyncClient("https://example.org")

    await client.close()

    assert calls == 1


@pytest.mark.asyncio
async def test_close_rejects_retained_dispatch_caller(monkeypatch):
    callback_task = asyncio.create_task(asyncio.sleep(0))
    await callback_task
    monkeypatch.setattr(
        async_client_module.asyncio,
        "current_task",
        lambda: callback_task,
    )
    client = AsyncClient("https://example.org")
    key = (ROOM, "$live", "timeline")
    client._recovery._active_dispatches[key] = callback_task

    try:
        with pytest.raises(
            LocalProtocolError,
            match=r"AsyncClient\.close\(\) cannot run from a timeline callback\.",
        ):
            await client.close()
    finally:
        client._recovery._active_dispatches.clear()


@pytest.mark.asyncio
async def test_close_drains_cleared_dispatch_without_cancelling_it():
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [value]},
    )
    started = asyncio.Event()
    release = asyncio.Event()
    finished = asyncio.Event()

    async def dispatch(
        _room,
        event,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        started.set()
        await release.wait()
        finished.set()
        return event

    async def unused_fetch(*_args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 0.01),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch,
        store=None,
    )
    await started.wait()
    apply_plan(
        state,
        RecoveryPlan(
            clear_rooms=frozenset({ROOM}),
            clear_room_reasons={ROOM: sync_recovery.RecoveryAbandonment.BASELINE_LOST},
        ),
    )
    assert state._active_dispatches

    client = AsyncClient("https://example.org")
    client._recovery = state
    close = asyncio.create_task(client.close())
    await asyncio.sleep(0)
    assert not close.done()

    release.set()
    await close

    assert finished.is_set()
    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_close_surfaces_deferred_error_after_dispatch_drain():
    state = RecoveryState()
    state._deferred_dispatch_errors.append(RuntimeError("late callback failure"))
    finished = asyncio.Event()

    async def dispatch():
        await asyncio.sleep(0)
        finished.set()

    key = (ROOM, "$live", "timeline")
    state._active_dispatches[key] = asyncio.create_task(dispatch())
    client = AsyncClient("https://example.org")
    client._recovery = state

    with pytest.raises(RuntimeError, match="late callback failure"):
        await client.close()

    assert finished.is_set()
    assert not state._active_dispatches
    assert not state._deferred_dispatch_errors


@pytest.mark.asyncio
async def test_close_surfaces_error_returned_by_retained_dispatch():
    state = RecoveryState()

    async def dispatch():
        await asyncio.sleep(0)
        return sync_recovery._LiveCallbackError(
            RuntimeError("retained callback failure"),
            False,
        )

    key = (ROOM, "$live", "timeline")
    state._active_dispatches[key] = asyncio.create_task(dispatch())
    client = AsyncClient("https://example.org")
    client._recovery = state

    with pytest.raises(RuntimeError, match="retained callback failure"):
        await client.close()

    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_room_drain_ignores_cancelled_retained_dispatch():
    state = RecoveryState()
    task = asyncio.create_task(asyncio.sleep(10))
    key = (ROOM, "$cancelled", "timeline")
    state._active_dispatches[key] = task
    task.cancel()
    await asyncio.gather(task, return_exceptions=True)

    await sync_recovery.drain_recovery_room_dispatches(state, (ROOM,))

    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_close_drains_dispatch_after_repeated_cancellation():
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "", None)]},
        events={(ROOM, 1): [value]},
    )
    store = InlineStore()
    calls: list[str] = []
    started = asyncio.Event()
    release = asyncio.Event()

    async def dispatch(
        _room,
        event,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        calls.append("early")
        started.set()
        await release.wait()
        calls.append("late")
        return event

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    pump = asyncio.create_task(
        pump_recovery(
            state,
            user_id="@me:example.org",
            options=RecoveryOptions(1, 10, 10, 1),
            fetch_messages=unused_fetch,
            dispatch_event=dispatch,
            store=store,
        )
    )
    await started.wait()
    pump.cancel()
    pump.cancel()
    with pytest.raises(asyncio.CancelledError):
        await pump

    active = next(iter(state._active_dispatches.values()))
    client = AsyncClient("https://example.org")
    client._recovery = state
    close = asyncio.create_task(client.close())
    try:
        await asyncio.sleep(0)
        assert not close.done()
        close.cancel()
        await asyncio.sleep(0)
        close.cancel()
        await asyncio.sleep(0)
        assert not close.done()
    finally:
        release.set()
        await asyncio.wait_for(active, 1)

    with pytest.raises(asyncio.CancelledError):
        await close
    assert calls == ["early", "late"]
    assert store.finished == [(ROOM, 1, "$live", False)]
    assert not state.events[(ROOM, 1)]
    assert not state._active_dispatches


@pytest.mark.asyncio
async def test_clearing_room_drains_active_dispatch_before_reset():
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [value]},
    )
    store = InlineStore()
    started = asyncio.Event()
    release = asyncio.Event()
    calls: list[str] = []

    async def dispatch(
        _room,
        event,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        calls.append(f"start:{event.event_id}")
        started.set()
        await release.wait()
        calls.append(f"finish:{event.event_id}")
        return event

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    pump = asyncio.create_task(
        pump_recovery(
            state,
            user_id="@me:example.org",
            options=RecoveryOptions(1, 10, 10, 0.01),
            fetch_messages=unused_fetch,
            dispatch_event=dispatch,
            store=store,
            ready_room_id=ROOM,
        )
    )
    await started.wait()
    pump.cancel()
    with pytest.raises(asyncio.CancelledError):
        await asyncio.wait_for(pump, 1)

    async def reset():
        await sync_recovery.drain_recovery_room_dispatches(state, {ROOM})
        apply_plan(
            state,
            RecoveryPlan(
                clear_rooms=frozenset({ROOM}),
                clear_room_reasons={
                    ROOM: sync_recovery.RecoveryAbandonment.BASELINE_LOST
                },
            ),
        )

    task = asyncio.create_task(reset())
    await asyncio.sleep(0)
    assert not task.done()
    assert calls == ["start:$live"]

    release.set()
    await asyncio.wait_for(task, 1)

    assert calls == ["start:$live", "finish:$live"]
    assert store.finished == [(ROOM, 1, "$live", False)]
    assert not state._active_dispatches
    assert ROOM not in state.gaps


@pytest.mark.asyncio
async def test_clearing_failed_dispatch_aborts_before_plan():
    value = pending("$live", 0)
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "p1", None)]},
        events={(ROOM, 1): [value]},
    )
    release = asyncio.Event()

    async def dispatch(
        _room,
        _event,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        await release.wait()
        raise RuntimeError("failed after timeout")

    async def unused_fetch(*args):
        raise AssertionError("closed gap must not fetch")

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 0.01),
        fetch_messages=unused_fetch,
        dispatch_event=dispatch,
        store=None,
    )
    task = next(iter(state._active_dispatches.values()))
    release.set()
    done, _ = await asyncio.wait((task,), timeout=1)
    assert done

    with pytest.raises(RuntimeError, match="failed after timeout"):
        await sync_recovery.drain_recovery_room_dispatches(state, {ROOM})

    assert ROOM in state.gaps
    assert not state._active_dispatches


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

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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
async def test_conflicting_overlap_order_keeps_gap_open_without_target():
    held_a = PendingTimelineEvent.from_event(ROOM, 1, 0, event("$held-a", 2), True)
    held_b = PendingTimelineEvent.from_event(ROOM, 1, 1, event("$held-b", 4), True)
    assert held_a and held_b
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]},
        events={(ROOM, 1): [held_a, held_b]},
    )
    admissions: list[tuple[str, TimelineEventProvenance]] = []

    async def fetch(*_args):
        return RoomMessagesResponse.from_dict(
            {
                "start": "cursor",
                "end": "more",
                "chunk": [
                    event("$gap-one", 1).source,
                    event("$held-b", 4).source,
                    event("$gap-two", 3).source,
                    event("$held-a", 2).source,
                ],
            },
            ROOM,
        )

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        admissions.append((value.event_id, provenance))

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=RecoveryOptions(1, 10, 10, 10),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )

    assert admissions == []
    [gap] = state.gaps[ROOM]
    assert gap.cursor_token == "more"
    assert gap.target_token == "target"
    assert take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
        {},
    )


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

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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
        else RecoveryPlan(
            clear_rooms=frozenset({ROOM}),
            clear_room_reasons={ROOM: sync_recovery.RecoveryAbandonment.BASELINE_LOST},
        )
    )

    apply_plan(state, plan)

    assert state.completed[ROOM]["$encrypted"]


@pytest.mark.asyncio
@pytest.mark.parametrize("overlap_first", [False, True])
async def test_room_cap_abandons_over_cap_page_despite_overlap(overlap_first):
    held = PendingTimelineEvent.from_event(
        ROOM,
        1,
        0,
        event("$held", 3),
        True,
    )
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

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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


@pytest.mark.asyncio
async def test_completed_plaintext_does_not_consume_encrypted_retry_budget():
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]},
    )
    record_completed_timeline_event(
        state,
        ROOM,
        "$completed",
        False,
        TimelineEventProvenance.HISTORY,
    )
    seen: list[str] = []

    async def fetch(*_args):
        return RoomMessagesResponse.from_dict(
            {
                "start": "cursor",
                "end": "target",
                "chunk": [
                    encrypted_event("$completed").source,
                    event("$unseen", 2).source,
                ],
            },
            ROOM,
        )

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
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

    assert seen == ["$unseen"]


def test_recovery_outcome_keeps_open_real_gap_unrecovered():
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 2, "p2", "s2")]},
    )
    state.outcomes = {ROOM: True, ROOM_B: False}

    assert sync_recovery.take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM, ROOM_B}),
        {},
    )
    assert sync_recovery.take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
        {},
    )


def test_clearing_real_gap_is_unrecovered_but_synthetic_gap_is_not():
    state = RecoveryState(
        gaps={
            ROOM: [RecoveryGap(ROOM, 1, "p1", "s1")],
            ROOM_B: [RecoveryGap(ROOM_B, 1, "", None)],
        },
        events={(ROOM, 1): [], (ROOM_B, 1): []},
    )

    apply_plan(
        state,
        RecoveryPlan(
            clear_rooms=frozenset({ROOM, ROOM_B}),
            clear_room_reasons={ROOM: sync_recovery.RecoveryAbandonment.UNKNOWN},
        ),
    )

    expected = {ROOM: frozenset({sync_recovery.RecoveryAbandonment.UNKNOWN})}
    assert sync_recovery.take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
        expected,
    )
    assert sync_recovery.take_recovery_outcomes(state) == (
        frozenset(),
        frozenset({ROOM}),
        expected,
    )


async def _pump_one_page(state: RecoveryState, page, *, options=None) -> list[str]:
    """Run one recovery pump against a scripted ``/messages`` reply."""
    seen: list[str] = []

    async def fetch(*_args):
        return page

    async def dispatch(
        _room,
        value,
        _was_completed,
        _kind,
        _provenance,
        _sync_origin,
        _apply_room_state,
        admission_accepted,
        mark_admission_accepted,
    ):
        accept_admission(admission_accepted, mark_admission_accepted)
        seen.append(value.event_id)
        return value

    await pump_recovery(
        state,
        user_id="@me:example.org",
        options=options or RecoveryOptions(1, 10, 10, 10),
        fetch_messages=fetch,
        dispatch_event=dispatch,
        store=None,
    )
    return seen


@pytest.mark.asyncio
async def test_room_event_cap_is_reported_as_a_budget_not_a_loss():
    """Running out of nio's event budget is not evidence the history is gone.

    The cap is a bound nio sets on itself, exactly like an application-side
    page limit. An application told only that the room was abandoned would
    mark it permanently incompletable for what is really a spending decision,
    so the reason has to say which of the two happened.
    """
    state = RecoveryState(gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]})

    await _pump_one_page(
        state,
        RoomMessagesResponse.from_dict(
            {
                "start": "cursor",
                "end": "more",
                "chunk": [event("$overflow", 2).source],
            },
            ROOM,
        ),
        options=RecoveryOptions(1, 0, 10, 10),
    )

    assert state.abandoned == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.EVENT_LIMIT})
    }


@pytest.mark.asyncio
async def test_a_refused_backfill_is_reported_apart_from_a_broken_walk():
    """A server that says no is a different finding from a walk that cannot work.

    Nothing about a refusal shows the events are unreachable: a later session
    with different permissions may fetch them. Folding it in with the
    unverifiable walks would overstate what nio actually established.
    """
    state = RecoveryState(gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]})
    refusal = RoomMessagesError.from_dict(
        {"errcode": "M_FORBIDDEN", "error": "not allowed"},
        ROOM,
    )
    refusal.transport_response = SimpleNamespace(status=403)

    await _pump_one_page(state, refusal)

    assert state.abandoned == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.FETCH_FAILED})
    }


@pytest.mark.asyncio
async def test_a_walk_that_cannot_advance_is_reported_as_permanent():
    """A cursor the server will not move past is the one genuinely lost case.

    Retrying cannot help, so this is the reason an application may treat as
    permanent -- and the only one. Reporting it the same way as a budget stop
    would throw the distinction away in the other direction.
    """
    state = RecoveryState(gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]})

    await _pump_one_page(
        state,
        RoomMessagesResponse.from_dict(
            {"start": "cursor", "end": "cursor", "chunk": []},
            ROOM,
        ),
    )

    assert state.abandoned == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.UNVERIFIABLE})
    }


def test_held_event_cap_is_reported_as_a_budget_not_a_loss():
    """The other cost ceiling has to answer the same way as the first.

    Dropping a room because too many of its events are held is the same
    spending decision as the per-gap cap, reached from the ingress side.
    """
    state = RecoveryState(max_held_events=0)

    plan = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[event("$live", 1)],
        user_id="@me:example.org",
        membership="join",
        cursor_token="since",
        target_token="p1",
    )

    assert plan.abandoned_rooms == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.EVENT_LIMIT})
    }


def test_existing_gap_held_event_cap_stays_a_budget_stop():
    """Continuing a real gap must not turn the same ceiling into permanent loss."""
    state = RecoveryState(
        gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]},
        events={(ROOM, 1): [pending("$live-old", 0)]},
        max_held_events=1,
    )

    plan = plan_room_timeline(
        state,
        room_id=ROOM,
        timeline_events=[event("$live-new", 1)],
        user_id="@me:example.org",
        membership="join",
    )
    persist_response_plan(state, None, token=None, plan=plan)

    assert state.abandoned == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.EVENT_LIMIT})
    }


def test_recovery_plan_normalizes_singular_causes_to_frozen_sets():
    """Public plans never expose the legacy singular cause representation."""
    plan = RecoveryPlan(
        abandoned_rooms={ROOM: sync_recovery.RecoveryAbandonment.BASELINE_LOST},
        clear_room_reasons={ROOM_B: sync_recovery.RecoveryAbandonment.CORRUPT_EVENT},
    )

    assert plan.abandoned_rooms == {
        ROOM: frozenset({sync_recovery.RecoveryAbandonment.BASELINE_LOST})
    }
    assert plan.clear_room_reasons == {
        ROOM_B: frozenset({sync_recovery.RecoveryAbandonment.CORRUPT_EVENT})
    }
    assert sync_recovery.normalize_abandonment_reasons(object()) == frozenset(
        {sync_recovery.RecoveryAbandonment.UNKNOWN}
    )


def test_empty_legacy_causes_become_unknown():
    """A named abandoned room never exposes an empty cause set."""
    plan = RecoveryPlan(abandoned_rooms={ROOM: []})
    state = RecoveryState()
    sync_recovery.load_recovery_state(state, (), (), {ROOM: []})

    expected = {ROOM: frozenset({sync_recovery.RecoveryAbandonment.UNKNOWN})}
    assert plan.abandoned_rooms == expected
    assert state.abandoned == expected


def test_sequential_abandonments_retain_every_cause_until_acknowledged():
    """A later cause must not replace the room's earlier loss."""
    state = RecoveryState()

    apply_plan(
        state,
        RecoveryPlan(
            abandoned_rooms={ROOM: sync_recovery.RecoveryAbandonment.BASELINE_LOST}
        ),
    )
    apply_plan(
        state,
        RecoveryPlan(
            abandoned_rooms={ROOM: sync_recovery.RecoveryAbandonment.CORRUPT_EVENT}
        ),
    )

    assert state.abandoned == {
        ROOM: frozenset(
            {
                sync_recovery.RecoveryAbandonment.BASELINE_LOST,
                sync_recovery.RecoveryAbandonment.CORRUPT_EVENT,
            }
        )
    }
    assert sync_recovery.acknowledge_unrecovered_rooms(state, [ROOM]) == frozenset(
        {ROOM}
    )
    assert state.abandoned == {}


def test_merged_plans_union_causes_for_one_room():
    """Combining independently planned work cannot discard either cause."""
    plan = sync_recovery.merge_recovery_plans(
        (
            RecoveryPlan(
                abandoned_rooms={ROOM: sync_recovery.RecoveryAbandonment.BASELINE_LOST}
            ),
            RecoveryPlan(
                abandoned_rooms={ROOM: sync_recovery.RecoveryAbandonment.CORRUPT_EVENT}
            ),
        )
    )

    assert plan.abandoned_rooms == {
        ROOM: frozenset(
            {
                sync_recovery.RecoveryAbandonment.BASELINE_LOST,
                sync_recovery.RecoveryAbandonment.CORRUPT_EVENT,
            }
        )
    }


def test_corrupt_recovery_event_has_a_named_abandonment_reason():
    """A known local loss must not be published as an unknown cause."""
    assert "corrupt_event" in {
        reason.value for reason in sync_recovery.RecoveryAbandonment
    }


def test_an_undiagnosed_loss_keeps_later_named_causes():
    """Unknown legacy data is retained beside every later named cause."""
    state = RecoveryState()
    unknown = sync_recovery.RecoveryAbandonment.UNKNOWN

    for later in (
        sync_recovery.RecoveryAbandonment.EVENT_LIMIT,
        sync_recovery.RecoveryAbandonment.FETCH_FAILED,
        sync_recovery.RecoveryAbandonment.BASELINE_LOST,
        sync_recovery.RecoveryAbandonment.CORRUPT_EVENT,
    ):
        state.abandoned = {ROOM: unknown}
        apply_plan(state, RecoveryPlan(abandoned_rooms={ROOM: later}))
        assert state.abandoned == {ROOM: frozenset({unknown, later})}, later

    state.abandoned = {ROOM: unknown}
    apply_plan(
        state,
        RecoveryPlan(
            abandoned_rooms={ROOM: sync_recovery.RecoveryAbandonment.UNVERIFIABLE}
        ),
    )
    assert state.abandoned == {
        ROOM: frozenset({unknown, sync_recovery.RecoveryAbandonment.UNVERIFIABLE})
    }


@pytest.mark.asyncio
async def test_a_stalled_cursor_and_event_cap_on_one_page_retain_both_causes():
    """One terminal page records every cause it establishes."""
    state = RecoveryState(gaps={ROOM: [RecoveryGap(ROOM, 1, "target", "cursor")]})

    await _pump_one_page(
        state,
        RoomMessagesResponse.from_dict(
            {
                "start": "cursor",
                "end": "cursor",
                "chunk": [event("$overflow", 2).source],
            },
            ROOM,
        ),
        options=RecoveryOptions(1, 0, 10, 10),
    )

    assert state.abandoned == {
        ROOM: frozenset(
            {
                sync_recovery.RecoveryAbandonment.EVENT_LIMIT,
                sync_recovery.RecoveryAbandonment.UNVERIFIABLE,
            }
        )
    }


def test_published_recovery_outcomes_stay_frozen():
    """``frozenset`` operators return ``NotImplemented`` against ``dict_keys``.

    The reflected ``dict_keys`` operator answers instead and yields a mutable
    ``set``, which compares equal to the frozen set every assertion here uses.
    The published fields are declared frozen, so the type is checked directly.
    """
    state = RecoveryState()
    state.outcomes = {ROOM: True, ROOM_B: False}
    state.abandoned = {ROOM_B: sync_recovery.RecoveryAbandonment.UNVERIFIABLE}

    recovered, unrecovered, _abandoned = take_recovery_outcomes(state)

    assert isinstance(recovered, frozenset)
    assert isinstance(unrecovered, frozenset)


def test_a_second_loss_is_added_until_the_room_is_settled():
    """Acknowledgement is the only operation that removes a room's full set."""
    state = RecoveryState()
    unverifiable = sync_recovery.RecoveryAbandonment.UNVERIFIABLE
    budget = sync_recovery.RecoveryAbandonment.EVENT_LIMIT

    apply_plan(state, RecoveryPlan(abandoned_rooms={ROOM: unverifiable}))
    apply_plan(state, RecoveryPlan(abandoned_rooms={ROOM: budget}))

    assert state.abandoned == {ROOM: frozenset({unverifiable, budget})}

    assert sync_recovery.acknowledge_unrecovered_rooms(state, [ROOM]) == frozenset(
        {ROOM}
    )
    apply_plan(state, RecoveryPlan(abandoned_rooms={ROOM: budget}))

    assert state.abandoned == {ROOM: frozenset({budget})}
