"""Durable room-local recovery behind a monotonic Matrix sync cursor."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from itertools import pairwise
from types import MappingProxyType
from typing import TYPE_CHECKING, Any, Literal

from ..api import MessageDirection
from ..event_provenance import TimelineEventProvenance
from ..events import (
    AccountDataEvent,
    BadEventType,
    EphemeralEvent,
    Event,
    MegolmEvent,
    RoomMemberEvent,
)
from ..exceptions import LocalProtocolError
from ..recovery_abandonment import (
    RecoveryAbandonment,
    most_conservative_abandonment,
)
from ..responses import RoomMessagesError, RoomMessagesResponse

if TYPE_CHECKING:
    from ..sliding_sync_tokens import SlidingWindowToken
    from ..store.database import MatrixStore

logger = logging.getLogger(__name__)


FetchMessages = Callable[[str, str, str | None, MessageDirection, int], Awaitable]
PendingEventKind = Literal["timeline", "ephemeral", "account_data"]
_DispatchResult = Event | BadEventType | EphemeralEvent | AccountDataEvent | None
DispatchEvent = Callable[
    [
        str,
        Event | BadEventType | EphemeralEvent | AccountDataEvent,
        bool,
        PendingEventKind,
        TimelineEventProvenance,
        bool,
        bool,
        bool,
        Callable[[], None],
    ],
    Awaitable[_DispatchResult],
]
_DispatchKey = tuple[str, str, PendingEventKind]


class _LiveCallbackError(Exception):
    def __init__(
        self,
        error: Exception,
        was_encrypted: bool,
        *,
        accepted: bool = True,
    ):
        super().__init__(error)
        self.error = error
        self.was_encrypted = was_encrypted
        self.accepted = accepted


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
    # The bounded tokens were issued under one proven own-membership identity.
    membership_bound: bool = False


@dataclass(frozen=True)
class PendingTimelineEvent:
    """One durable callback obligation.

    ``is_live`` distinguishes sync-origin rows from ``/messages`` recovery
    rows. Sync-origin rows survive room resets, count toward the held-event
    cap, and acknowledge ordinary callback failures. ``provenance`` is the
    independent live, recovered, or history classification exposed at admission.
    ``apply_room_state`` keeps historical Sliding Sync expansions from
    replacing current room state; Classic Sync timelines always apply state.
    """

    room_id: str
    generation: int
    sequence: int
    event_id: str
    source_json: str
    is_live: bool
    was_encrypted: bool
    was_completed: bool = False
    kind: PendingEventKind = "timeline"
    admission_accepted: bool = False
    provenance: TimelineEventProvenance = TimelineEventProvenance.LIVE
    apply_room_state: bool = True

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
        provenance: TimelineEventProvenance | None = None,
        apply_room_state: bool | None = None,
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
            False,
            (
                provenance
                if provenance is not None
                else (
                    TimelineEventProvenance.LIVE
                    if is_live
                    else TimelineEventProvenance.HISTORY
                )
            ),
            is_live if apply_room_state is None else apply_room_state,
        )

    def parse(
        self,
    ) -> Event | BadEventType | EphemeralEvent | AccountDataEvent:
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
    # Explicit real-gap failures and why each one was given up on; synthetic
    # empty-token drains never enter this mapping.
    abandoned_rooms: Mapping[str, RecoveryAbandonment] = field(default_factory=dict)
    # Why a room is being cleared, used only if that clear destroys a real gap.
    # Keeping this separate from ``abandoned_rooms`` lets the store make the
    # decision atomically when the persisted gap is not present in memory.
    clear_room_reasons: Mapping[str, RecoveryAbandonment] = field(default_factory=dict)


@dataclass(frozen=True)
class CompletedTimelineEvent:
    was_encrypted: bool
    provenance: TimelineEventProvenance


@dataclass
class RecoveryState:
    gaps: dict[str, list[RecoveryGap]] = field(default_factory=dict)
    events: dict[tuple[str, int], list[PendingTimelineEvent]] = field(
        default_factory=dict
    )
    completed: dict[str, OrderedDict[str, CompletedTimelineEvent]] = field(
        default_factory=dict
    )
    # Outcomes since the last take; False stays sticky when _finish records True.
    outcomes: dict[str, bool] = field(default_factory=dict)
    # Rooms whose gap was given up on, mapped to why. Unlike ``outcomes`` this
    # is not drained by a read: a loss that is announced once and then forgotten
    # is indistinguishable from a recovery, so the application would have no way
    # to tell a room with permanently missing work from a healthy one.
    abandoned: dict[str, RecoveryAbandonment] = field(default_factory=dict)
    room_offset: int = 0
    max_held_events: int = 200
    _active_dispatches: dict[_DispatchKey, asyncio.Task[_LiveCallbackError | None]] = (
        field(default_factory=dict, init=False, repr=False, compare=False)
    )
    _dispatch_waiters: dict[asyncio.Task[_LiveCallbackError | None], int] = field(
        default_factory=dict, init=False, repr=False, compare=False
    )
    _deferred_dispatch_errors: list[Exception] = field(
        default_factory=list, init=False, repr=False, compare=False
    )


def has_pending_recovery_work(state: RecoveryState) -> bool:
    """Whether a recovery pump has gaps or deferred callback failures."""
    return bool(state.gaps or state._deferred_dispatch_errors)


def acknowledge_unrecovered_rooms(
    state: RecoveryState,
    room_ids: Iterable[str],
) -> frozenset[str]:
    """Stop reporting these rooms as degraded and return the ones that were.

    Abandonment is sticky precisely so that it cannot be missed, which means
    something has to clear it. Only the application can, because only it knows
    whether it has recorded the loss somewhere durable.
    """
    settled = state.abandoned.keys() & set(room_ids)
    for room_id in settled:
        del state.abandoned[room_id]
    return frozenset(settled)


def _materialize_memory_clear_abandonments(
    state: RecoveryState,
    plan: RecoveryPlan,
) -> RecoveryPlan:
    """Turn structural clear causes into losses visible in memory.

    The store performs the same check against persisted gaps in its transaction;
    this side handles memory-only recovery and supplies immediate response state.
    """
    abandoned_rooms = dict(plan.abandoned_rooms)
    for room_id in plan.clear_rooms:
        if not any(gap.target_token for gap in state.gaps.get(room_id, ())):
            continue
        reason = plan.clear_room_reasons.get(room_id)
        if reason is None:
            if room_id in abandoned_rooms:
                continue
            logger.error("Clearing a real gap in %s without naming a cause", room_id)
            reason = RecoveryAbandonment.UNKNOWN
        abandoned_rooms[room_id] = most_conservative_abandonment(
            abandoned_rooms.get(room_id),
            reason,
        )
    return replace(plan, abandoned_rooms=abandoned_rooms)


def is_recovery_dispatch_task(
    state: RecoveryState,
    task: asyncio.Task[Any] | None,
    room_id: str | None = None,
) -> bool:
    """Whether task owns a retained recovery callback, optionally for one room."""
    return task is not None and any(
        active is task and (room_id is None or key[0] == room_id)
        for key, active in state._active_dispatches.items()
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


def is_recovered_dispatch_task(
    state: RecoveryState,
    task: asyncio.Task[Any] | None,
    room_id: str,
) -> bool:
    """Whether task owns the room's only continuity-proven callback gap."""
    if task is None:
        return False
    for key, active in state._active_dispatches.items():
        if active is not task or key[0] != room_id:
            continue
        target = _pending_dispatch(state, key)
        if target is None:
            continue
        gap, pending = target
        if (
            pending.provenance is TimelineEventProvenance.RECOVERED
            and gap.cursor_token is None
            and not any(
                other.generation != gap.generation
                and (other.cursor_token is not None or other.target_token)
                for other in state.gaps.get(room_id, ())
            )
        ):
            return True
    return False


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
    if _pending_dispatch(state, key) is not None:
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
        _report_dispatch_error(
            key,
            task,
            "Recovered event callback failed after its row was cleared: %s",
        )
    else:
        if dispatch_error:
            state._deferred_dispatch_errors.append(dispatch_error.error)


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


async def _run_dispatch(
    state: RecoveryState,
    store: MatrixStore | None,
    gap: RecoveryGap,
    pending: PendingTimelineEvent,
    key: _DispatchKey,
    dispatch_event: DispatchEvent,
    event: Event | BadEventType | EphemeralEvent | AccountDataEvent,
) -> _LiveCallbackError | None:
    error: _LiveCallbackError | None = None

    def mark_admission_accepted() -> None:
        _mark_admission_accepted(state, store, gap, pending)

    try:
        delivered = await dispatch_event(
            gap.room_id,
            event,
            pending.was_completed,
            pending.kind,
            pending.provenance,
            pending.is_live,
            pending.apply_room_state,
            pending.admission_accepted,
            mark_admission_accepted,
        )
    except _LiveCallbackError as dispatch_error:
        error = dispatch_error
        delivered = None

    task = asyncio.current_task()
    target = _pending_dispatch(state, key)
    if target and state._active_dispatches.get(key) is task:
        current_gap, current_pending = target
        if error and not error.accepted:
            pass
        elif error and not current_pending.is_live:
            state.outcomes[current_gap.room_id] = False
        else:
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


def _mark_admission_accepted(
    state: RecoveryState,
    store: MatrixStore | None,
    gap: RecoveryGap,
    pending: PendingTimelineEvent,
) -> None:
    key = (gap.room_id, gap.generation)
    queued = state.events[key]
    index = next(
        (
            index
            for index, current in enumerate(queued)
            if current.event_id == pending.event_id and current.kind == pending.kind
        ),
        None,
    )
    if index is None:
        raise ValueError(f"Pending recovery event disappeared: {pending.event_id}")
    if store:
        store.accept_recovery_event(
            gap.room_id,
            gap.generation,
            pending.event_id,
        )
    queued[index] = replace(queued[index], admission_accepted=True)


async def drain_recovery_dispatches(state: RecoveryState) -> None:
    """Wait for retained callback work and release its in-memory task entries."""
    drain_error: Exception | None = None
    while state._active_dispatches:
        tasks = set(state._active_dispatches.values())
        await asyncio.wait(tuple(tasks))
        for key, task in tuple(state._active_dispatches.items()):
            if not task.done() or state._active_dispatches.get(key) is not task:
                continue
            state._active_dispatches.pop(key)
            state._dispatch_waiters.pop(task, None)
            if task.cancelled():
                continue
            try:
                callback_error = task.result()
            except _DispatchFinishError as error:
                drain_error = drain_error or error.error
            except Exception as error:
                drain_error = drain_error or error
            else:
                if callback_error:
                    drain_error = drain_error or callback_error.error
    if drain_error:
        raise drain_error
    if state._deferred_dispatch_errors:
        raise state._deferred_dispatch_errors.pop(0)


async def drain_recovery_room_dispatches(
    state: RecoveryState, room_ids: Iterable[str]
) -> None:
    """Finish retained callback work before replacing room recovery state."""
    selected_rooms = set(room_ids)
    while True:
        selected = {
            key: task
            for key, task in state._active_dispatches.items()
            if key[0] in selected_rooms
        }
        if not selected:
            return
        tasks = set(selected.values())
        for task in tasks:
            state._dispatch_waiters[task] = state._dispatch_waiters.get(task, 0) + 1
        try:
            await asyncio.wait(tuple(tasks))
        finally:
            for task in tasks:
                waiters = state._dispatch_waiters.get(task, 0)
                if waiters <= 1:
                    state._dispatch_waiters.pop(task, None)
                else:
                    state._dispatch_waiters[task] = waiters - 1
        for key, task in selected.items():
            if state._active_dispatches.get(key) is not task:
                continue
            state._active_dispatches.pop(key)
            if task.cancelled():
                continue
            error = task.exception()
            if isinstance(error, _DispatchFinishError):
                raise error.error
            if error:
                raise error
            dispatch_error = task.result()
            if dispatch_error:
                raise dispatch_error.error


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
    completed = state.completed.get(room_id, {}).get(event_id)
    return completed is None or (
        completed.was_encrypted and not isinstance(event, MegolmEvent)
    )


def record_completed_timeline_event(
    state: RecoveryState,
    room_id: str,
    event_id: str,
    was_encrypted: bool,
    provenance: TimelineEventProvenance,
) -> None:
    completed = state.completed.setdefault(room_id, OrderedDict())
    previous = completed.pop(event_id, None)
    completed[event_id] = CompletedTimelineEvent(
        was_encrypted=(
            was_encrypted
            if previous is None
            else previous.was_encrypted and was_encrypted
        ),
        provenance=provenance if previous is None else previous.provenance,
    )
    if len(completed) > 512:
        completed.popitem(last=False)


def _plan_timeline_events(
    state: RecoveryState,
    room_id: str,
    generation: int,
    timeline_events: Sequence[Event | BadEventType],
    *,
    include_pending: bool,
    batch_id: str | None,
    provenance_live_event_count: int | None,
    apply_state_live_event_count: int | None,
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
        (event.sequence for event in state.events.get((room_id, generation), ())),
        default=-1,
    )
    provenance_live_start = (
        0
        if provenance_live_event_count is None
        else len(timeline_events) - provenance_live_event_count
    )
    apply_state_start = (
        0
        if apply_state_live_event_count is None
        else len(timeline_events) - apply_state_live_event_count
    )
    planned = []
    for index, event in enumerate(timeline_events):
        event_id = getattr(event, "event_id", None) or (
            f"~{batch_id}:{index}" if batch_id is not None else None
        )
        completed = state.completed.get(room_id, {}).get(event_id) if event_id else None
        provenance = (
            completed.provenance
            if completed
            else (
                TimelineEventProvenance.LIVE
                if index >= provenance_live_start
                else TimelineEventProvenance.HISTORY
            )
        )
        pending = PendingTimelineEvent.from_event(
            room_id,
            generation,
            sequence,
            event,
            True,
            event_id,
            provenance=provenance,
            apply_room_state=index >= apply_state_start,
        )
        if not event_id or not pending:
            continue
        existing = known.get(event_id)
        if existing:
            continue
        was_completed = bool(completed and completed.was_encrypted)
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
    abandoned_rooms: Mapping[str, RecoveryAbandonment] = MappingProxyType({}),
    clear_reason: RecoveryAbandonment | None = None,
) -> RecoveryPlan:
    gaps = state.gaps.get(room_id, ())
    live = [
        event
        for gap in gaps
        for event in state.events.get((room_id, gap.generation), ())
        if event.is_live
    ] + list(additional_events)
    clear = frozenset({room_id})
    abandoned_rooms = dict(abandoned_rooms)
    if not live:
        return RecoveryPlan(
            clear_rooms=clear,
            abandoned_rooms=abandoned_rooms,
            clear_room_reasons=({room_id: clear_reason} if clear_reason else {}),
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
        abandoned_rooms=abandoned_rooms,
        clear_room_reasons=({room_id: clear_reason} if clear_reason else {}),
    )


def plan_room_timeline(
    state: RecoveryState,
    *,
    room_id: str,
    timeline_events: Sequence[Event | BadEventType],
    user_id: str | None,
    membership: str,
    reset_recovery: bool = False,
    live_event_count: int | None = None,
    provenance_live_event_count: int | None = None,
    apply_state_live_event_count: int | None = None,
    cursor_token: str | None = None,
    target_token: str = "",
    membership_bound: bool = False,
    batch_id: str | None = None,
    ephemeral_events: Sequence[EphemeralEvent] = (),
    account_data_events: Sequence[AccountDataEvent | BadEventType] = (),
) -> RecoveryPlan:
    if membership in {"leave", "ban", "invite"}:
        return _plan_room_reset(
            state,
            room_id,
            clear_reason=RecoveryAbandonment.BASELINE_LOST,
        )

    if live_event_count is not None and not (
        0 <= live_event_count <= len(timeline_events)
    ):
        live_event_count = 0
    if provenance_live_event_count is not None and not (
        0 <= provenance_live_event_count <= len(timeline_events)
    ):
        provenance_live_event_count = 0
    if apply_state_live_event_count is not None and not (
        0 <= apply_state_live_event_count <= len(timeline_events)
    ):
        apply_state_live_event_count = 0

    previous_gaps = state.gaps.get(room_id, ())
    clear = reset_recovery or _timeline_clears_recovery(
        timeline_events,
        user_id,
        live_event_count,
    )
    existing = () if clear else previous_gaps
    new_gap = would_plan_real_gap(
        timeline_events=timeline_events,
        user_id=user_id,
        membership=membership,
        live_event_count=live_event_count,
        cursor_token=cursor_token,
    )
    separate_history = (
        bool(existing)
        and not new_gap
        and provenance_live_event_count is not None
        and provenance_live_event_count < len(timeline_events)
    )
    generation = existing[-1].generation if existing else 0
    if new_gap or not existing or separate_history:
        generation += 1
    events = _plan_timeline_events(
        state,
        room_id,
        generation,
        timeline_events,
        include_pending=not clear,
        batch_id=batch_id,
        provenance_live_event_count=provenance_live_event_count,
        apply_state_live_event_count=apply_state_live_event_count,
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
        event.is_live
        for gap in existing
        for event in state.events.get((room_id, gap.generation), ())
    )
    if (new_gap or existing) and held_count + len(events) > state.max_held_events:
        logger.error("Abandoning recovery with too many held events in %s", room_id)
        abandons_real_gap = new_gap or any(gap.target_token for gap in existing)
        return _plan_room_reset(
            state,
            room_id,
            events,
            abandoned_rooms=(
                {room_id: RecoveryAbandonment.EVENT_LIMIT} if new_gap else {}
            ),
            clear_reason=(
                RecoveryAbandonment.EVENT_LIMIT if abandons_real_gap else None
            ),
        )
    gap = (
        RecoveryGap(
            room_id,
            generation,
            target_token if new_gap else "",
            cursor_token if new_gap else None,
            membership_bound if new_gap else False,
        )
        if new_gap or events and (not existing or separate_history)
        else None
    )
    return RecoveryPlan(
        frozenset({room_id}) if clear else frozenset(),
        (gap,) if gap else (),
        tuple(events),
        clear_room_reasons=(
            {room_id: RecoveryAbandonment.BASELINE_LOST} if clear else {}
        ),
    )


def merge_recovery_plans(plans: Iterable[RecoveryPlan]) -> RecoveryPlan:
    clear_rooms: set[str] = set()
    gaps: list[RecoveryGap] = []
    events: list[PendingTimelineEvent] = []
    abandoned_rooms: dict[str, RecoveryAbandonment] = {}
    clear_room_reasons: dict[str, RecoveryAbandonment] = {}
    for plan in plans:
        clear_rooms.update(plan.clear_rooms)
        gaps.extend(plan.gaps)
        events.extend(plan.events)
        for room_id, reason in plan.abandoned_rooms.items():
            abandoned_rooms[room_id] = most_conservative_abandonment(
                abandoned_rooms.get(room_id),
                reason,
            )
        for room_id, reason in plan.clear_room_reasons.items():
            clear_room_reasons[room_id] = most_conservative_abandonment(
                clear_room_reasons.get(room_id),
                reason,
            )
    return RecoveryPlan(
        frozenset(clear_rooms),
        tuple(gaps),
        tuple(events),
        abandoned_rooms=abandoned_rooms,
        clear_room_reasons=clear_room_reasons,
    )


def plan_sync_response(
    state: RecoveryState,
    *,
    user_id: str | None,
    request_since: str | None,
    response_token: str,
    joined_rooms: Mapping[str, Any],
    reset_room_ids: Iterable[str] = (),
    current_room_ids: frozenset[str] | None = None,
) -> RecoveryPlan:
    def recovery_cursor(room_id: str, room_info: Any) -> str | None:
        """Return the token a limited classic timeline must backfill down to.

        This is a genuine lower bound, not a hint: everything at or before
        ``since`` was delivered by an earlier sync and is already processed.
        A gap that carries one is bounded at both ends, which is what lets
        running out of pages count as having seen all of it.
        """
        if current_room_ids is not None and room_id not in current_room_ids:
            return None
        if not room_info.timeline.limited or not request_since:
            return None
        return request_since

    plans = [
        plan_room_timeline(
            state,
            room_id=room_id,
            timeline_events=tuple(room_info.timeline.events),
            user_id=(
                user_id
                if current_room_ids is None or room_id in current_room_ids
                else None
            ),
            membership="join",
            cursor_token=recovery_cursor(room_id, room_info),
            # Derived from the same cursor as the sliding path, and for the
            # same reason. Without it ``bounded_exhausted`` can never be true
            # on this path, so a backfill that reaches the start of the
            # server's history without the end token happening to equal the
            # target leaves every recovered event classified as history --
            # which means a burst of messages the bot has never seen arrives
            # as context it is not allowed to answer.
            membership_bound=recovery_cursor(room_id, room_info) is not None,
            target_token=(
                room_info.timeline.prev_batch or response_token
                if current_room_ids is None or room_id in current_room_ids
                else ""
            ),
            provenance_live_event_count=None if request_since is not None else 0,
            # Classic timeline events advance room state even when the initial
            # response classifies their callback provenance as history.
            apply_state_live_event_count=(
                None if current_room_ids is None or room_id in current_room_ids else 0
            ),
            batch_id=f"sync:{response_token}",
            ephemeral_events=room_info.ephemeral,
            account_data_events=room_info.account_data,
        )
        for room_id, room_info in joined_rooms.items()
    ]
    plans.extend(
        _plan_room_reset(
            state,
            room_id,
            clear_reason=RecoveryAbandonment.BASELINE_LOST,
        )
        for room_id in reset_room_ids
    )
    return merge_recovery_plans(plans)


def apply_plan(state: RecoveryState, plan: RecoveryPlan) -> None:
    plan = _materialize_memory_clear_abandonments(state, plan)
    for room_id in plan.abandoned_rooms:
        state.outcomes[room_id] = False

    for room_id in plan.clear_rooms:
        gaps = state.gaps.pop(room_id, ())
        if any(gap.target_token for gap in gaps):
            state.outcomes[room_id] = False
        for gap in gaps:
            for event in state.events.pop((room_id, gap.generation), ()):
                if event.was_completed and not event.event_id.startswith("~"):
                    record_completed_timeline_event(
                        state,
                        room_id,
                        event.event_id,
                        True,
                        event.provenance,
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
                    state,
                    plan.clear_recovered.room_id,
                    event.event_id,
                    True,
                    event.provenance,
                )
        state.events[key][:] = [event for event in state.events[key] if event.is_live]

    for event in plan.events:
        state.completed.get(event.room_id, {}).pop(event.event_id, None)
        key = (event.room_id, event.generation)
        queued = state.events.setdefault(key, [])
        existing = next(
            (item for item in queued if item.event_id == event.event_id),
            None,
        )
        if existing is None:
            queued.append(event)
        else:
            queued[queued.index(existing)] = event
    for key in {(event.room_id, event.generation) for event in plan.events}:
        state.events[key].sort(key=lambda item: item.sequence)

    # Clearing room recovery does not settle a loss. Only the application can
    # do that after recording the missing work somewhere it owns.
    for room_id, reason in plan.abandoned_rooms.items():
        state.abandoned[room_id] = most_conservative_abandonment(
            state.abandoned.get(room_id),
            reason,
        )


def take_recovery_outcomes(
    state: RecoveryState,
) -> tuple[frozenset[str], frozenset[str], dict[str, RecoveryAbandonment]]:
    """Drain this response's outcomes into recovered, unrecovered, abandoned.

    The third result is reported alongside the second as well as folded into
    it, because the two mean different things to an application: an unrecovered
    room may still deliver its events on a later pump, while an abandoned one
    never will. Telling them apart by calling
    :func:`acknowledge_unrecovered_rooms` and reading back what it cleared
    would force the application to forget a loss in order to learn of it, which
    inverts the store-first ordering the rest of this module keeps.
    """
    outcomes = state.outcomes
    state.outcomes = {}
    pending = frozenset(
        room_id
        for room_id, gaps in state.gaps.items()
        if any(gap.target_token for gap in gaps)
    )
    abandoned = dict(state.abandoned)
    # ``frozenset`` operators return ``NotImplemented`` against ``dict_keys``,
    # so the reflected ``dict_keys`` operator answers instead and yields a
    # mutable ``set``. Both fields are published as frozen sets, so the result
    # is re-frozen rather than relying on the operand types.
    abandoned_ids = frozenset(abandoned)
    recovered = (
        frozenset(room_id for room_id, complete in outcomes.items() if complete)
        - pending
        - abandoned_ids
    )
    unrecovered = (
        frozenset(room_id for room_id, complete in outcomes.items() if not complete)
        | pending
        | abandoned_ids
    )
    return recovered, unrecovered, abandoned


def load_recovery_state(
    state: RecoveryState,
    gaps: Iterable[Any],
    events: Iterable[Any],
    abandoned: Mapping[str, RecoveryAbandonment] | None = None,
) -> None:
    state.gaps.clear()
    state.events.clear()
    state.completed.clear()
    state.outcomes.clear()
    state.abandoned.clear()
    state.abandoned.update(abandoned or {})
    for row in gaps:
        gap = RecoveryGap(
            row.room_id,
            row.generation,
            row.target_token,
            row.cursor_token,
            row.membership_bound,
        )
        state.gaps.setdefault(row.room_id, []).append(gap)
    for room_gaps in state.gaps.values():
        room_gaps.sort(key=lambda gap: gap.generation)

    for row in events:
        if row.generation == 0:
            state.completed.setdefault(row.room_id, OrderedDict())[row.event_id] = (
                CompletedTimelineEvent(
                    row.was_encrypted,
                    row.provenance,
                )
            )
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
            row.admission_accepted,
            row.provenance,
            row.apply_room_state,
        )
        state.events.setdefault((row.room_id, row.generation), []).append(event)
    for queued in state.events.values():
        queued.sort(key=lambda event: event.sequence)


def persist_response_plan(
    state: RecoveryState,
    store: MatrixStore | None,
    *,
    token: str | None,
    plan: RecoveryPlan,
    window_tokens: Mapping[str, SlidingWindowToken] | None = None,
    forgotten_rooms: Iterable[str] = (),
) -> None:
    real_gap_clear_rooms = frozenset(
        room_id
        for room_id in plan.clear_rooms
        if any(gap.target_token for gap in state.gaps.get(room_id, ()))
    )
    # A structural cause means the store may discover a real gap that is absent
    # from memory. Reject a non-atomic implementation before validation logs or
    # any mutation; deleting the gap and recording its loss is one transition.
    potentially_destructive_clear = bool(
        real_gap_clear_rooms or plan.clear_room_reasons
    )
    if (
        store
        and potentially_destructive_clear
        and not getattr(store, "supports_atomic_recovery", False)
    ):
        raise LocalProtocolError(
            "The configured store does not support atomic recovery writes."
        )
    plan = _materialize_memory_clear_abandonments(state, plan)
    # Fold in what is already known before the row is written, not after: the
    # store replaces the room's reason outright, so a plan that claims less
    # than the standing loss would leave a restart reading the weaker one.
    plan = replace(
        plan,
        abandoned_rooms={
            room_id: most_conservative_abandonment(state.abandoned.get(room_id), reason)
            for room_id, reason in plan.abandoned_rooms.items()
        },
    )
    try:
        if store:
            save_args = (
                token,
                set(plan.clear_rooms),
                plan.gaps,
                plan.events,
                plan.clear_recovered,
                window_tokens,
                forgotten_rooms,
                plan.abandoned_rooms,
            )
            if plan.clear_room_reasons:
                store.save_recovery(*save_args, plan.clear_room_reasons)
            else:
                store.save_recovery(*save_args)
        apply_plan(state, plan)
    except BaseException:
        for room_id in plan.abandoned_rooms:
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
    abandonment: RecoveryAbandonment | None = None,
) -> None:
    if store:
        finish_args = (
            gap.room_id,
            gap.generation,
            event.event_id if event else None,
            was_encrypted,
        )
        if abandonment is None:
            store.finish_recovery(*finish_args)
        else:
            store.finish_recovery(*finish_args, abandonment)
    if abandonment is not None:
        state.outcomes[gap.room_id] = False
        state.abandoned[gap.room_id] = most_conservative_abandonment(
            state.abandoned.get(gap.room_id),
            abandonment,
        )
    key = (gap.room_id, gap.generation)
    if event:
        state.events[key].remove(event)
        if not event.event_id.startswith("~"):
            record_completed_timeline_event(
                state,
                gap.room_id,
                event.event_id,
                was_encrypted,
                event.provenance,
            )
        return
    state.events.pop(key, None)
    gaps = state.gaps[gap.room_id]
    gaps.remove(gap)
    if not gaps:
        state.gaps.pop(gap.room_id)
    if gap.target_token:
        state.outcomes.setdefault(gap.room_id, True)


def _merge_recovery_page_order(
    queued: Iterable[PendingTimelineEvent],
    page: Iterable[PendingTimelineEvent],
) -> tuple[PendingTimelineEvent, ...]:
    """Place page overlap anchors between recovered history and later live work."""
    page_events = list(page)
    queued_events = list(queued)
    if not page_events:
        return tuple(
            replace(event, sequence=index) for index, event in enumerate(queued_events)
        )

    queued_indexes = {
        event.event_id: index for index, event in enumerate(queued_events)
    }
    anchors = [
        (page_index, queued_indexes[event.event_id])
        for page_index, event in enumerate(page_events)
        if event.event_id in queued_indexes
    ]
    # Conflicting overlap order cannot be spliced safely. Insert only the
    # page's new events at the normal recovered/live boundary instead.
    if any(previous[1] >= current[1] for previous, current in pairwise(anchors)):
        page_events = [
            event for event in page_events if event.event_id not in queued_indexes
        ]
        anchors = []
    page_ids = {event.event_id for event in page_events}
    if not anchors:
        insert_at = 1 + max(
            (index for index, event in enumerate(queued_events) if not event.is_live),
            default=-1,
        )
        ordered = [
            *queued_events[:insert_at],
            *page_events,
            *queued_events[insert_at:],
        ]
    else:
        ordered = []
        queued_start = 0
        page_start = 0
        for page_end, queued_end in anchors:
            ordered.extend(
                event
                for event in queued_events[queued_start:queued_end]
                if event.event_id not in page_ids
            )
            ordered.extend(page_events[page_start : page_end + 1])
            queued_start = queued_end + 1
            page_start = page_end + 1
        ordered.extend(page_events[page_start:])
        ordered.extend(
            event
            for event in queued_events[queued_start:]
            if event.event_id not in page_ids
        )
    return tuple(replace(event, sequence=index) for index, event in enumerate(ordered))


def _promote_recovered_continuity(
    events: Iterable[PendingTimelineEvent],
) -> tuple[PendingTimelineEvent, ...]:
    return tuple(
        (
            replace(event, provenance=TimelineEventProvenance.RECOVERED)
            if event.kind == "timeline"
            and event.provenance is TimelineEventProvenance.HISTORY
            and not event.was_completed
            else event
        )
        for event in events
    )


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
    if gap.target_token and cursor == gap.target_token:
        updated = replace(gap, cursor_token=None)
        persist_response_plan(
            state,
            store,
            token=None,
            plan=RecoveryPlan(
                gaps=(updated,),
                events=_promote_recovered_continuity(
                    state.events.get((gap.room_id, gap.generation), ())
                ),
            ),
        )
        return updated

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
        abandoned: RecoveryAbandonment | None = None
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
                    abandoned_rooms={gap.room_id: RecoveryAbandonment.FETCH_FAILED},
                ),
            )
            return gap

        recovered: list[PendingTimelineEvent] = []
        page_events: list[PendingTimelineEvent] = []
        page_ids: set[str] = set()
        queued = state.events.get((gap.room_id, gap.generation), ())
        queued_by_id = {event.event_id: event for event in queued}
        for event in response.chunk:
            event_id = getattr(event, "event_id", None)
            if not event_id:
                continue
            if is_own_join(event, user_id):
                recovered.clear()
                page_events.clear()
                page_ids.clear()
                clear_recovered = True
            completed = state.completed.get(gap.room_id, {}).get(event_id)
            was_completed = bool(completed and completed.was_encrypted)
            if event_id in pending_ids:
                existing = queued_by_id.get(event_id)
                if (
                    existing
                    and (existing.is_live or not clear_recovered)
                    and event_id not in page_ids
                ):
                    page_events.append(existing)
                    page_ids.add(event_id)
                continue
            if not should_dispatch_timeline_event(state, gap.room_id, event) and not (
                was_completed and isinstance(event, MegolmEvent)
            ):
                continue
            pending_event = PendingTimelineEvent.from_event(
                gap.room_id,
                gap.generation,
                0,
                event,
                False,
                was_completed=was_completed,
                provenance=(
                    completed.provenance
                    if completed is not None
                    else TimelineEventProvenance.HISTORY
                ),
                apply_room_state=False,
            )
            if pending_event:
                recovered.append(pending_event)
                page_events.append(pending_event)
                page_ids.add(event_id)
                pending_ids.add(event_id)

        current_recovered_count = sum(
            not event.is_live
            for event in state.events.get((gap.room_id, gap.generation), ())
        )
        retained_recovered_count = recovered_count - (
            current_recovered_count if clear_recovered else 0
        )
        target_reached = bool(gap.target_token and response.end == gap.target_token)
        bounded_exhausted = bool(
            gap.membership_bound
            and gap.target_token
            and response.end is None
            and not clear_recovered
        )
        continuity_proven = target_reached or bounded_exhausted
        # A cursor the server will not move past is checked before the event
        # cap. Both can be true of one page, and only one reason is recorded,
        # so the cap would report a budget stop for a walk nio just watched
        # fail to advance -- the under-claim the merge order exists to prevent.
        if response.end == cursor:
            logger.error("Abandoning unverifiable gap in %s", gap.room_id)
            recovered.clear()
            clear_recovered = True
            abandoned = RecoveryAbandonment.UNVERIFIABLE
            next_cursor = None
        elif retained_recovered_count + len(recovered) > options.max_events:
            logger.error("Abandoning recovery at the room event cap in %s", gap.room_id)
            recovered.clear()
            page_events.clear()
            clear_recovered = True
            abandoned = RecoveryAbandonment.EVENT_LIMIT
            next_cursor = None
        elif continuity_proven:
            next_cursor = None
        elif response.end is None:
            logger.error("Abandoning unverifiable gap in %s", gap.room_id)
            recovered.clear()
            clear_recovered = True
            abandoned = RecoveryAbandonment.UNVERIFIABLE
            next_cursor = None
        else:
            next_cursor = response.end

        retained = (
            [event for event in queued if event.is_live] if clear_recovered else queued
        )
        ordered_events = (
            () if abandoned else _merge_recovery_page_order(retained, page_events)
        )
        if continuity_proven and next_cursor is None and not abandoned:
            ordered_events = _promote_recovered_continuity(ordered_events)
        updated = replace(gap, cursor_token=next_cursor)
        persist_response_plan(
            state,
            store,
            token=None,
            plan=RecoveryPlan(
                gaps=(updated,),
                events=ordered_events,
                clear_recovered=updated if clear_recovered else None,
                abandoned_rooms={gap.room_id: abandoned} if abandoned else {},
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
) -> None:
    if gap.cursor_token is not None:
        return
    queued = state.events.get((gap.room_id, gap.generation), ())
    for pending in tuple(queued):
        callback_error: Exception | None = None
        try:
            event = pending.parse()
        except Exception:
            logger.exception("Discarding corrupt recovered event: %s", pending.event_id)
            _finish(
                state,
                store,
                gap,
                pending,
                pending.was_encrypted,
                (RecoveryAbandonment.CORRUPT_EVENT if gap.target_token else None),
            )
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
            callback_error = error.error
        except _DispatchFinishError as error:
            raise error.error from error
        except asyncio.TimeoutError:
            logger.warning("Recovered event callback timed out: %s", pending.event_id)
            return
        except Exception:
            logger.exception("Recovered event callback failed: %s", pending.event_id)
            return
        if callback_error is not None:
            raise callback_error

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
        if not gaps or gaps[0].cursor_token is not None:
            return
        room_ids = [ready_room_id]
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
