"""Durable room-local recovery behind a monotonic Matrix sync cursor."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any, Literal

from ..api import MessageDirection
from ..events import (
    AccountDataEvent,
    BadEventType,
    EphemeralEvent,
    Event,
    MegolmEvent,
    RoomMemberEvent,
)
from ..responses import RoomMessagesError, RoomMessagesResponse

if TYPE_CHECKING:
    from ..sliding_sync_tokens import SlidingWindowToken
    from ..store.database import MatrixStore

logger = logging.getLogger(__name__)


FetchMessages = Callable[[str, str, str | None, MessageDirection, int], Awaitable]
PendingEventKind = Literal["timeline", "ephemeral", "account_data", "boundary"]
_DispatchResult = Event | BadEventType | EphemeralEvent | AccountDataEvent | None
DispatchEvent = Callable[
    [
        str,
        Event | BadEventType | EphemeralEvent | AccountDataEvent,
        bool,
        bool,
        PendingEventKind,
    ],
    Awaitable[_DispatchResult],
]
_DispatchKey = tuple[str, str, PendingEventKind]


class _LiveCallbackError(Exception):
    def __init__(self, error: Exception, was_encrypted: bool):
        super().__init__(error)
        self.error = error
        self.was_encrypted = was_encrypted


class _DispatchFinishError(Exception):
    def __init__(self, error: Exception):
        super().__init__(error)
        self.error = error


@dataclass(frozen=True)
class RecoveryOptions:
    max_pages: int
    max_events: int
    page_size: int
    timeout: float


@dataclass(frozen=True)
class RecoveryGap:
    room_id: str
    generation: int
    target_token: str
    cursor_token: str | None


@dataclass(frozen=True)
class PendingTimelineEvent:
    room_id: str
    generation: int
    sequence: int
    event_id: str
    source_json: str
    is_live: bool
    was_encrypted: bool
    was_completed: bool = False
    kind: PendingEventKind = "timeline"

    @classmethod
    def from_event(
        cls,
        room_id: str,
        generation: int,
        sequence: int,
        event: Event | BadEventType,
        is_live: bool,
        fallback_event_id: str | None = None,
        was_completed: bool = False,
        kind: PendingEventKind = "timeline",
    ) -> PendingTimelineEvent | None:
        event_id = getattr(event, "event_id", None) or fallback_event_id
        if not event_id:
            return None
        source = json.dumps(event.source, sort_keys=True, separators=(",", ":"))
        return cls(
            room_id,
            generation,
            sequence,
            event_id,
            source,
            is_live,
            isinstance(event, MegolmEvent),
            was_completed,
            kind,
        )

    def parse(
        self,
    ) -> Event | BadEventType | EphemeralEvent | AccountDataEvent:
        if self.kind == "boundary":
            raise ValueError("Boundary markers cannot be parsed as events")
        source = json.loads(self.source_json)
        if self.kind == "ephemeral":
            event = EphemeralEvent.parse_event(source)
            if event is None:
                raise ValueError("Invalid pending ephemeral event")
            return event
        if self.kind == "account_data":
            return AccountDataEvent.parse_event(source)
        return Event.parse_event(source)


@dataclass(frozen=True)
class RecoveryPlan:
    clear_rooms: frozenset[str] = frozenset()
    gaps: tuple[RecoveryGap, ...] = ()
    events: tuple[PendingTimelineEvent, ...] = ()
    clear_recovered: RecoveryGap | None = None
    # Explicit real-gap failures; synthetic empty-token drains never enter this set.
    unrecovered_room_ids: frozenset[str] = frozenset()


@dataclass
class RecoveryState:
    gaps: dict[str, list[RecoveryGap]] = field(default_factory=dict)
    events: dict[tuple[str, int], list[PendingTimelineEvent]] = field(
        default_factory=dict
    )
    completed: dict[str, OrderedDict[str, bool]] = field(default_factory=dict)
    # Outcomes since the last take; False stays sticky when _finish records True.
    outcomes: dict[str, bool] = field(default_factory=dict)
    room_offset: int = 0
    max_held_events: int = 200
    _active_dispatches: dict[_DispatchKey, asyncio.Task[_LiveCallbackError | None]] = (
        field(default_factory=dict, init=False, repr=False, compare=False)
    )
    _dispatch_waiters: dict[asyncio.Task[_LiveCallbackError | None], int] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    _orphaned_dispatches: dict[
        asyncio.Task[_LiveCallbackError | None], _DispatchKey
    ] = field(default_factory=dict, init=False, repr=False, compare=False)
    _deferred_dispatch_errors: list[Exception] = field(
        default_factory=list, init=False, repr=False, compare=False
    )


def _dispatch_key(pending: PendingTimelineEvent) -> _DispatchKey:
    return pending.room_id, pending.event_id, pending.kind


def _pending_dispatch(
    state: RecoveryState, key: _DispatchKey
) -> tuple[RecoveryGap, PendingTimelineEvent] | None:
    for (room_id, generation), queued in state.events.items():
        if room_id != key[0]:
            continue
        pending = next(
            (
                event
                for event in queued
                if event.event_id == key[1] and event.kind == key[2]
            ),
            None,
        )
        if pending is None:
            continue
        gap = next(
            (
                gap
                for gap in state.gaps.get(room_id, ())
                if gap.generation == generation
            ),
            None,
        )
        if gap:
            return gap, pending
    return None


def _has_pending_dispatch(state: RecoveryState, key: _DispatchKey) -> bool:
    return _pending_dispatch(state, key) is not None


def _dispatch_finished(
    state: RecoveryState,
    key: _DispatchKey,
    task: asyncio.Task[_LiveCallbackError | None],
) -> None:
    if state._active_dispatches.get(key) is not task:
        if not task.cancelled():
            task.exception()
        return
    if state._dispatch_waiters.get(task):
        return
    if _has_pending_dispatch(state, key):
        if not task.cancelled():
            task.exception()
        return
    state._active_dispatches.pop(key, None)
    state._dispatch_waiters.pop(task, None)
    if task.cancelled():
        return
    try:
        dispatch_error = task.result()
    except Exception:
        _report_orphaned_dispatch(key, task)
    else:
        if dispatch_error:
            state._deferred_dispatch_errors.append(dispatch_error.error)


def _report_orphaned_dispatch(
    key: _DispatchKey, task: asyncio.Task[_LiveCallbackError | None]
) -> None:
    _report_dispatch_error(
        key,
        task,
        "Recovered event callback failed after its row was cleared: %s",
    )


def _report_dispatch_error(
    key: _DispatchKey,
    task: asyncio.Task[_LiveCallbackError | None],
    message: str,
) -> None:
    if task.cancelled():
        return
    error = task.exception()
    if error:
        logger.error(message, key[1], exc_info=error)


def _discard_orphaned_dispatches(state: RecoveryState) -> None:
    for key, task in tuple(state._active_dispatches.items()):
        if _has_pending_dispatch(state, key):
            continue
        if state._active_dispatches.pop(key, None) is not task:
            continue
        state._dispatch_waiters.pop(task, None)
        if task.done():
            _report_orphaned_dispatch(key, task)
        else:
            state._orphaned_dispatches[task] = key
            task.cancel()


async def _run_dispatch(
    state: RecoveryState,
    store: MatrixStore | None,
    gap: RecoveryGap,
    pending: PendingTimelineEvent,
    key: _DispatchKey,
    dispatch_event: DispatchEvent,
    event: Event | BadEventType | EphemeralEvent | AccountDataEvent,
    *,
    retain_boundary: bool,
) -> _LiveCallbackError | None:
    error: _LiveCallbackError | None = None
    try:
        delivered = await dispatch_event(
            gap.room_id,
            event,
            pending.is_live,
            pending.was_completed,
            pending.kind,
        )
    except _LiveCallbackError as dispatch_error:
        error = dispatch_error
        delivered = None

    task = asyncio.current_task()
    target = _pending_dispatch(state, key)
    if target and state._active_dispatches.get(key) is task:
        current_gap, current_pending = target
        was_encrypted = (
            error.was_encrypted
            if error
            else (
                isinstance(delivered, MegolmEvent)
                if delivered
                else current_pending.was_encrypted
            )
        )
        try:
            _finish(
                state,
                store,
                current_gap,
                current_pending,
                was_encrypted,
                retain_boundary=retain_boundary and error is None,
            )
        except Exception as finish_error:
            raise _DispatchFinishError(finish_error) from finish_error

    if error:
        if task is None or not state._dispatch_waiters.get(task):
            logger.error(
                "Recovered event callback failed after its recovery pump returned: %s",
                pending.event_id,
                exc_info=error.error,
            )
        return error
    return None


async def drain_recovery_dispatches(state: RecoveryState) -> None:
    """Wait for retained callback work and release its in-memory task entries."""
    finish_error: Exception | None = None
    while state._active_dispatches or state._orphaned_dispatches:
        tasks = set(state._active_dispatches.values())
        tasks.update(state._orphaned_dispatches)
        await asyncio.wait(tuple(tasks))
        for key, task in tuple(state._active_dispatches.items()):
            if not task.done() or state._active_dispatches.get(key) is not task:
                continue
            state._active_dispatches.pop(key)
            state._dispatch_waiters.pop(task, None)
            error = None if task.cancelled() else task.exception()
            if isinstance(error, _DispatchFinishError):
                finish_error = finish_error or error.error
                continue
            _report_dispatch_error(
                key,
                task,
                "Recovered event callback failed while the client was closing: %s",
            )
        for task, key in tuple(state._orphaned_dispatches.items()):
            if not task.done():
                continue
            state._orphaned_dispatches.pop(task)
            _report_orphaned_dispatch(key, task)
    if finish_error:
        raise finish_error
    if state._deferred_dispatch_errors:
        raise state._deferred_dispatch_errors.pop(0)


def is_own_join(event: Event | BadEventType, user_id: str | None) -> bool:
    """Whether this event is our own transition into the room."""
    return bool(
        user_id
        and isinstance(event, RoomMemberEvent)
        and event.state_key == user_id
        and event.membership == "join"
        and event.prev_membership != "join"
    )


def _timeline_clears_recovery(
    timeline_events: Sequence[Event | BadEventType],
    user_id: str | None,
    live_event_count: int | None,
) -> bool:
    last_join = max(
        (
            index
            for index, event in enumerate(timeline_events)
            if is_own_join(event, user_id)
        ),
        default=-1,
    )
    live_start = (
        0
        if live_event_count is None
        else max(0, len(timeline_events) - live_event_count)
    )
    return last_join >= live_start


def would_plan_real_gap(
    *,
    timeline_events: Sequence[Event | BadEventType],
    user_id: str | None,
    membership: str,
    live_event_count: int | None = None,
    cursor_token: str | None = None,
) -> bool:
    """Return whether these inputs create a targeted recovery gap."""
    return (
        membership not in {"leave", "ban", "invite"}
        and cursor_token is not None
        and not _timeline_clears_recovery(
            timeline_events,
            user_id,
            live_event_count,
        )
    )


def should_dispatch_timeline_event(
    state: RecoveryState,
    room_id: str,
    event: Event | BadEventType,
) -> bool:
    event_id = getattr(event, "event_id", None)
    if not event_id:
        return True
    was_encrypted = state.completed.get(room_id, {}).get(event_id)
    return was_encrypted is None or (
        was_encrypted and not isinstance(event, MegolmEvent)
    )


def record_completed_timeline_event(
    state: RecoveryState,
    room_id: str,
    event_id: str,
    was_encrypted: bool,
) -> None:
    completed = state.completed.setdefault(room_id, OrderedDict())
    previous = completed.pop(event_id, None)
    completed[event_id] = (
        was_encrypted if previous is None else previous and was_encrypted
    )
    if len(completed) > 512:
        completed.popitem(last=False)


def _plan_live_events(
    state: RecoveryState,
    room_id: str,
    generation: int,
    timeline_events: Iterable[Event | BadEventType],
    *,
    include_pending: bool,
    batch_id: str | None,
) -> list[PendingTimelineEvent]:
    known = (
        {
            event.event_id: event
            for (event_room, _), queued in state.events.items()
            if event_room == room_id
            for event in queued
        }
        if include_pending
        else {}
    )
    sequence = 1 + max(
        (
            event.sequence
            for event in state.events.get((room_id, generation), ())
            if event.is_live
        ),
        default=-1,
    )
    planned = []
    for index, event in enumerate(timeline_events):
        event_id = getattr(event, "event_id", None) or (
            f"~{batch_id}:{index}" if batch_id is not None else None
        )
        pending = PendingTimelineEvent.from_event(
            room_id, generation, sequence, event, True, event_id
        )
        if not event_id or not pending:
            continue
        existing = known.get(event_id)
        if existing:
            continue
        was_completed = bool(state.completed.get(room_id, {}).get(event_id))
        if not should_dispatch_timeline_event(state, room_id, event) and not (
            was_completed and isinstance(event, MegolmEvent)
        ):
            continue
        pending = replace(pending, was_completed=was_completed)
        planned.append(pending)
        known[event_id] = pending
        sequence += 1
    return planned


def _plan_ancillary_events(
    room_id: str,
    generation: int,
    sequence: int,
    batch_id: str,
    kind: Literal["ephemeral", "account_data"],
    events: Iterable[EphemeralEvent | AccountDataEvent | BadEventType],
) -> list[PendingTimelineEvent]:
    return [
        PendingTimelineEvent(
            room_id,
            generation,
            sequence + index,
            f"~{batch_id}:{kind}:{index}",
            json.dumps(event.source, sort_keys=True, separators=(",", ":")),
            True,
            False,
            kind=kind,
        )
        for index, event in enumerate(events)
    ]


def _plan_room_reset(
    state: RecoveryState,
    room_id: str,
    additional_events: Iterable[PendingTimelineEvent] = (),
    *,
    unrecovered: bool = False,
) -> RecoveryPlan:
    gaps = state.gaps.get(room_id, ())
    live = [
        event
        for gap in gaps
        for event in state.events.get((room_id, gap.generation), ())
        if event.is_live and event.kind != "boundary"
    ] + list(additional_events)
    clear = frozenset({room_id})
    unrecovered_room_ids = frozenset({room_id}) if unrecovered else frozenset()
    if not live:
        return RecoveryPlan(
            clear_rooms=clear,
            unrecovered_room_ids=unrecovered_room_ids,
        )
    generation = max((gap.generation for gap in gaps), default=0) + 1
    events = tuple(
        replace(event, generation=generation, sequence=index)
        for index, event in enumerate(live)
    )
    return RecoveryPlan(
        clear,
        (RecoveryGap(room_id, generation, "", None),),
        events,
        unrecovered_room_ids=unrecovered_room_ids,
    )


def plan_room_timeline(
    state: RecoveryState,
    *,
    room_id: str,
    timeline_events: Sequence[Event | BadEventType],
    user_id: str | None,
    membership: str,
    live_event_count: int | None = None,
    cursor_token: str | None = None,
    target_token: str = "",
    batch_id: str | None = None,
    ephemeral_events: Sequence[EphemeralEvent] = (),
    account_data_events: Sequence[AccountDataEvent | BadEventType] = (),
) -> RecoveryPlan:
    if membership in {"leave", "ban", "invite"}:
        return _plan_room_reset(state, room_id)

    clear = _timeline_clears_recovery(
        timeline_events,
        user_id,
        live_event_count,
    )
    existing = () if clear else state.gaps.get(room_id, ())
    new_gap = would_plan_real_gap(
        timeline_events=timeline_events,
        user_id=user_id,
        membership=membership,
        live_event_count=live_event_count,
        cursor_token=cursor_token,
    )
    generation = existing[-1].generation if existing else 0
    if new_gap or not existing:
        generation += 1
    events = _plan_live_events(
        state,
        room_id,
        generation,
        timeline_events,
        include_pending=not clear,
        batch_id=batch_id,
    )
    if ephemeral_events or account_data_events:
        if batch_id is None:
            raise ValueError("Ancillary recovery events require a batch ID")
        next_sequence = 1 + max(
            (event.sequence for event in events),
            default=max(
                (
                    event.sequence
                    for event in state.events.get((room_id, generation), ())
                    if event.is_live
                ),
                default=-1,
            ),
        )
        deferred = _plan_ancillary_events(
            room_id,
            generation,
            next_sequence,
            batch_id,
            "ephemeral",
            ephemeral_events,
        )
        events.extend(deferred)
        events.extend(
            _plan_ancillary_events(
                room_id,
                generation,
                next_sequence + len(deferred),
                batch_id,
                "account_data",
                account_data_events,
            )
        )
    held_count = sum(
        event.is_live and event.kind != "boundary"
        for gap in existing
        for event in state.events.get((room_id, gap.generation), ())
    )
    if (new_gap or existing) and held_count + len(events) > state.max_held_events:
        logger.error("Abandoning recovery with too many held events in %s", room_id)
        return _plan_room_reset(
            state,
            room_id,
            events,
            unrecovered=new_gap or any(gap.target_token for gap in existing),
        )
    gap = (
        RecoveryGap(
            room_id,
            generation,
            target_token if new_gap else "",
            cursor_token if new_gap else None,
        )
        if new_gap or events and not existing
        else None
    )
    return RecoveryPlan(
        frozenset({room_id}) if clear else frozenset(),
        (gap,) if gap else (),
        tuple(events),
    )


def merge_recovery_plans(plans: Iterable[RecoveryPlan]) -> RecoveryPlan:
    clear_rooms: set[str] = set()
    gaps: list[RecoveryGap] = []
    events: list[PendingTimelineEvent] = []
    unrecovered_room_ids: set[str] = set()
    for plan in plans:
        clear_rooms.update(plan.clear_rooms)
        gaps.extend(plan.gaps)
        events.extend(plan.events)
        unrecovered_room_ids.update(plan.unrecovered_room_ids)
    return RecoveryPlan(
        frozenset(clear_rooms),
        tuple(gaps),
        tuple(events),
        unrecovered_room_ids=frozenset(unrecovered_room_ids),
    )


def plan_sync_response(
    state: RecoveryState,
    *,
    user_id: str | None,
    request_since: str | None,
    response_token: str,
    joined_rooms: Mapping[str, Any],
    reset_room_ids: Iterable[str] = (),
) -> RecoveryPlan:
    plans = [
        plan_room_timeline(
            state,
            room_id=room_id,
            timeline_events=tuple(room_info.timeline.events),
            user_id=user_id,
            membership="join",
            cursor_token=(
                request_since if room_info.timeline.limited and request_since else None
            ),
            target_token=room_info.timeline.prev_batch or response_token,
            batch_id=f"sync:{response_token}",
            ephemeral_events=room_info.ephemeral,
            account_data_events=room_info.account_data,
        )
        for room_id, room_info in joined_rooms.items()
    ]
    plans.extend(_plan_room_reset(state, room_id) for room_id in reset_room_ids)
    return merge_recovery_plans(plans)


def apply_plan(state: RecoveryState, plan: RecoveryPlan) -> None:
    for room_id in plan.unrecovered_room_ids:
        state.outcomes[room_id] = False

    for room_id in plan.clear_rooms:
        gaps = state.gaps.pop(room_id, ())
        if any(gap.target_token for gap in gaps):
            state.outcomes[room_id] = False
        for gap in gaps:
            for event in state.events.pop((room_id, gap.generation), ()):
                if event.was_completed and not event.event_id.startswith("~"):
                    record_completed_timeline_event(
                        state, room_id, event.event_id, True
                    )

    for gap in plan.gaps:
        gaps = state.gaps.setdefault(gap.room_id, [])
        existing = next(
            (item for item in gaps if item.generation == gap.generation), None
        )
        if existing:
            gaps[gaps.index(existing)] = gap
        else:
            gaps.append(gap)
        state.events.setdefault((gap.room_id, gap.generation), [])

    if plan.clear_recovered:
        key = (plan.clear_recovered.room_id, plan.clear_recovered.generation)
        for event in state.events[key]:
            if (
                not event.is_live
                and event.was_completed
                and not event.event_id.startswith("~")
            ):
                record_completed_timeline_event(
                    state, plan.clear_recovered.room_id, event.event_id, True
                )
        state.events[key][:] = [event for event in state.events[key] if event.is_live]

    for event in plan.events:
        state.completed.get(event.room_id, {}).pop(event.event_id, None)
        key = (event.room_id, event.generation)
        queued = state.events.setdefault(key, [])
        if not any(item.event_id == event.event_id for item in queued):
            queued.append(event)
    for key in {(event.room_id, event.generation) for event in plan.events}:
        state.events[key].sort(key=lambda item: (item.is_live, item.sequence))
    _discard_orphaned_dispatches(state)


def take_recovery_outcomes(
    state: RecoveryState,
) -> tuple[frozenset[str], frozenset[str]]:
    outcomes = state.outcomes
    state.outcomes = {}
    pending = frozenset(
        room_id
        for room_id, gaps in state.gaps.items()
        if any(gap.target_token for gap in gaps)
    )
    recovered = (
        frozenset(room_id for room_id, complete in outcomes.items() if complete)
        - pending
    )
    unrecovered = (
        frozenset(room_id for room_id, complete in outcomes.items() if not complete)
        | pending
    )
    return recovered, unrecovered


def load_recovery_state(
    state: RecoveryState,
    gaps: Iterable[Any],
    events: Iterable[Any],
) -> None:
    state.gaps.clear()
    state.events.clear()
    state.completed.clear()
    state.outcomes.clear()
    for row in gaps:
        gap = RecoveryGap(
            row.room_id, row.generation, row.target_token, row.cursor_token
        )
        state.gaps.setdefault(row.room_id, []).append(gap)
    for room_gaps in state.gaps.values():
        room_gaps.sort(key=lambda gap: gap.generation)

    for row in events:
        if row.generation == 0:
            state.completed.setdefault(row.room_id, OrderedDict())[
                row.event_id
            ] = row.was_encrypted
            continue
        event = PendingTimelineEvent(
            row.room_id,
            row.generation,
            row.sequence,
            row.event_id,
            row.source_json,
            row.is_live,
            row.was_encrypted,
            row.was_completed,
            row.kind,
        )
        state.events.setdefault((row.room_id, row.generation), []).append(event)
    for queued in state.events.values():
        queued.sort(key=lambda event: (event.is_live, event.sequence))
    _discard_orphaned_dispatches(state)


def persist_response_plan(
    state: RecoveryState,
    store: MatrixStore | None,
    *,
    token: str | None,
    plan: RecoveryPlan,
    window_tokens: Mapping[str, SlidingWindowToken] | None = None,
    forgotten_rooms: Iterable[str] = (),
) -> None:
    try:
        if store:
            store.save_recovery(
                token,
                set(plan.clear_rooms),
                plan.gaps,
                plan.events,
                plan.clear_recovered,
                window_tokens,
                forgotten_rooms,
            )
        apply_plan(state, plan)
    except BaseException:
        for room_id in plan.unrecovered_room_ids:
            state.outcomes[room_id] = False
        for gap in plan.gaps:
            if gap.target_token:
                state.outcomes[gap.room_id] = False
        raise


def _finish(
    state: RecoveryState,
    store: MatrixStore | None,
    gap: RecoveryGap,
    event: PendingTimelineEvent | None = None,
    was_encrypted: bool = False,
    *,
    retain_boundary: bool = False,
) -> None:
    # Keep the oldest live ID as the durable gap anchor; later live rows use
    # completed-event deduplication instead of adding more boundary markers.
    boundary = (
        PendingTimelineEvent(
            gap.room_id,
            gap.generation,
            event.sequence,
            f"~boundary:{gap.generation}",
            event.event_id,
            True,
            False,
            kind="boundary",
        )
        if event and retain_boundary
        else None
    )
    if store:
        kwargs = {"boundary": boundary} if boundary else {}
        store.finish_recovery(
            gap.room_id,
            gap.generation,
            event.event_id if event else None,
            was_encrypted,
            **kwargs,
        )
    key = (gap.room_id, gap.generation)
    if event:
        state.events[key].remove(event)
        if not event.event_id.startswith("~"):
            record_completed_timeline_event(
                state, gap.room_id, event.event_id, was_encrypted
            )
        if boundary:
            state.events[key].append(boundary)
            state.events[key].sort(key=lambda item: (item.is_live, item.sequence))
        return
    state.events.pop(key, None)
    gaps = state.gaps[gap.room_id]
    gaps.remove(gap)
    if not gaps:
        state.gaps.pop(gap.room_id)
    if gap.target_token:
        state.outcomes.setdefault(gap.room_id, True)


async def _collect_slice(
    state: RecoveryState,
    gap: RecoveryGap,
    *,
    user_id: str | None,
    options: RecoveryOptions,
    fetch_messages: FetchMessages,
    store: MatrixStore | None,
    deadline: float,
) -> RecoveryGap:
    if gap.cursor_token is None:
        return gap

    pages = 0
    cursor = gap.cursor_token
    pending = [
        event
        for (event_room, generation), queued in state.events.items()
        if event_room == gap.room_id and generation > 0
        for event in queued
    ]
    pending_ids = {event.event_id for event in pending}
    recovered_count = sum(not event.is_live for event in pending)

    while cursor and pages < options.max_pages:
        clear_recovered = False
        abandoned = False
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        try:
            response = await asyncio.wait_for(
                fetch_messages(
                    gap.room_id,
                    cursor,
                    gap.target_token,
                    MessageDirection.front,
                    options.page_size,
                ),
                timeout=remaining,
            )
        except asyncio.TimeoutError:
            logger.warning("Limited-timeline recovery timed out in %s", gap.room_id)
            break
        except Exception:
            logger.exception("Limited-timeline recovery failed in %s", gap.room_id)
            break
        pages += 1
        if not isinstance(response, RoomMessagesResponse):
            if isinstance(response, RoomMessagesError) and response.transport_response:
                status = response.transport_response.status
                if status in (408, 429) or status >= 500:
                    break
            logger.error("Abandoning failed gap in %s", gap.room_id)
            gap = replace(gap, cursor_token=None)
            persist_response_plan(
                state,
                store,
                token=None,
                plan=RecoveryPlan(
                    gaps=(gap,),
                    clear_recovered=gap,
                    unrecovered_room_ids=frozenset({gap.room_id}),
                ),
            )
            return gap

        recovered: list[PendingTimelineEvent] = []
        next_sequence = 1 + max(
            (
                event.sequence
                for event in state.events.get((gap.room_id, gap.generation), ())
                if not event.is_live
            ),
            default=-1,
        )
        for event in response.chunk:
            event_id = getattr(event, "event_id", None)
            if not event_id:
                continue
            if is_own_join(event, user_id):
                recovered.clear()
                clear_recovered = True
                next_sequence = 0
            was_completed = bool(state.completed.get(gap.room_id, {}).get(event_id))
            if event_id in pending_ids or (
                not should_dispatch_timeline_event(state, gap.room_id, event)
                and not (was_completed and isinstance(event, MegolmEvent))
            ):
                continue
            pending = PendingTimelineEvent.from_event(
                gap.room_id,
                gap.generation,
                next_sequence,
                event,
                False,
                was_completed=was_completed,
            )
            if pending:
                recovered.append(pending)
                pending_ids.add(event_id)
                next_sequence += 1

        current_recovered_count = sum(
            not event.is_live
            for event in state.events.get((gap.room_id, gap.generation), ())
        )
        retained_recovered_count = recovered_count - (
            current_recovered_count if clear_recovered else 0
        )
        if retained_recovered_count + len(recovered) > options.max_events:
            logger.error("Abandoning recovery at the room event cap in %s", gap.room_id)
            recovered.clear()
            clear_recovered = True
            abandoned = True
            next_cursor = None
        elif response.end is None and gap.target_token:
            # A bounded walk that runs out of events has reached the sync
            # window: the spec omits `end` exactly when no further events
            # are available in the requested direction, and the request
            # was bounded by the token the window starts at. Both Synapse
            # and Tuwunel answer the last page of a `to`-bounded forward
            # walk this way — with an empty chunk and no token — and they
            # stop short of the window's own events, so the live overlap
            # above never gets the chance to close the gap. Treating the
            # exhausted page as failure discarded every recovered event
            # instead.
            #
            # The spec also omits `end` when the user may not see any more
            # events, which this cannot distinguish; a gap truncated by
            # history visibility is dispatched as if complete rather than
            # dropped whole.
            next_cursor = None
        elif response.end in (None, cursor):
            logger.error("Abandoning unverifiable gap in %s", gap.room_id)
            recovered.clear()
            clear_recovered = True
            abandoned = True
            next_cursor = None
        elif response.end == gap.target_token:
            next_cursor = None
        else:
            next_cursor = response.end

        updated = replace(gap, cursor_token=next_cursor)
        persist_response_plan(
            state,
            store,
            token=None,
            plan=RecoveryPlan(
                gaps=(updated,),
                events=tuple(recovered),
                clear_recovered=updated if clear_recovered else None,
                unrecovered_room_ids=(
                    frozenset({gap.room_id}) if abandoned else frozenset()
                ),
            ),
        )
        gap = updated
        recovered_count = retained_recovered_count + len(recovered)
        cursor = next_cursor

    return gap


async def _drain_gap(
    state: RecoveryState,
    gap: RecoveryGap,
    *,
    dispatch_event: DispatchEvent,
    store: MatrixStore | None,
    deadline: float | None,
    live_timeline_only: bool = False,
) -> None:
    if gap.cursor_token is not None and not live_timeline_only:
        return
    queued = state.events.get((gap.room_id, gap.generation), ())
    for pending in tuple(queued):
        if live_timeline_only and (not pending.is_live or pending.kind != "timeline"):
            continue
        if pending.kind == "boundary":
            _finish(state, store, gap, pending)
            continue
        try:
            event = pending.parse()
        except Exception:
            logger.exception("Discarding corrupt recovered event: %s", pending.event_id)
            if gap.target_token:
                state.outcomes[gap.room_id] = False
            _finish(state, store, gap, pending, pending.was_encrypted)
            continue
        try:
            key = _dispatch_key(pending)
            task = state._active_dispatches.get(key)
            loop = asyncio.get_running_loop()
            if task is None:
                if deadline is not None and deadline <= loop.time():
                    logger.warning(
                        "Recovered event callback timed out: %s", pending.event_id
                    )
                    return
                task = loop.create_task(
                    _run_dispatch(
                        state,
                        store,
                        gap,
                        pending,
                        key,
                        dispatch_event,
                        event,
                        retain_boundary=live_timeline_only
                        and not any(item.kind == "boundary" for item in queued),
                    )
                )
                # A timeout must stop waiting without cancelling callbacks that
                # may already have side effects. The next pump reuses this task.
                state._active_dispatches[key] = task
                task.add_done_callback(
                    lambda done, state=state, key=key: _dispatch_finished(
                        state, key, done
                    )
                )
            timeout = None if deadline is None else max(0, deadline - loop.time())
            state._dispatch_waiters[task] = state._dispatch_waiters.get(task, 0) + 1
            done: set[asyncio.Task[_LiveCallbackError | None]] = set()
            try:
                done, _ = await asyncio.wait((task,), timeout=timeout)
            finally:
                waiters = state._dispatch_waiters.get(task, 0)
                if waiters <= 1:
                    state._dispatch_waiters.pop(task, None)
                else:
                    state._dispatch_waiters[task] = waiters - 1
                if task.done() and task not in done:
                    _dispatch_finished(state, key, task)
            if not done:
                logger.warning(
                    "Recovered event callback timed out: %s", pending.event_id
                )
                return
            if state._active_dispatches.get(key) is not task:
                return
            state._active_dispatches.pop(key, None)
            error = task.result()
            if error:
                raise error
        except _LiveCallbackError as error:
            raise error.error from error
        except _DispatchFinishError as error:
            raise error.error from error
        except asyncio.TimeoutError:
            logger.warning("Recovered event callback timed out: %s", pending.event_id)
            return
        except Exception:
            logger.exception("Recovered event callback failed: %s", pending.event_id)
            return

    if not live_timeline_only:
        _finish(state, store, gap)


async def pump_recovery(
    state: RecoveryState,
    *,
    user_id: str | None,
    options: RecoveryOptions,
    fetch_messages: FetchMessages,
    dispatch_event: DispatchEvent,
    store: MatrixStore | None,
    ready_room_id: str | None = None,
) -> None:
    if state._deferred_dispatch_errors:
        raise state._deferred_dispatch_errors.pop(0)
    if ready_room_id is not None:
        gaps = state.gaps.get(ready_room_id)
        if not gaps:
            return
        room_ids = [ready_room_id]
        for gap in gaps:
            await _drain_gap(
                state,
                gap,
                dispatch_event=dispatch_event,
                store=store,
                deadline=None,
                live_timeline_only=True,
            )
        gaps = state.gaps.get(ready_room_id)
        if not gaps or gaps[0].cursor_token is not None:
            return
    else:
        room_ids = list(state.gaps)
    if not room_ids:
        return
    if ready_room_id is None:
        offset = state.room_offset % len(room_ids)
        room_ids = room_ids[offset:] + room_ids[:offset]
        state.room_offset = (offset + 1) % len(room_ids)
    room_ids.sort(key=lambda room_id: state.gaps[room_id][0].cursor_token is not None)
    deadline = asyncio.get_running_loop().time() + options.timeout
    for index, room_id in enumerate(room_ids):
        gaps = state.gaps.get(room_id)
        if not gaps:
            continue
        gap = gaps[0]
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            if gap.cursor_token is not None or gap.target_token:
                continue
            room_deadline = deadline
        else:
            room_deadline = asyncio.get_running_loop().time() + remaining / (
                len(room_ids) - index
            )
        if gap.cursor_token is not None:
            gap = await _collect_slice(
                state,
                gap,
                user_id=user_id,
                options=options,
                fetch_messages=fetch_messages,
                store=store,
                deadline=room_deadline,
            )
        await _drain_gap(
            state,
            gap,
            dispatch_event=dispatch_event,
            store=store,
            deadline=None if not gap.target_token else room_deadline,
        )
