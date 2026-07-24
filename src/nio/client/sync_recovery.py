"""Durable room-local recovery behind a monotonic Matrix sync cursor."""

from __future__ import annotations

import asyncio
import json
import logging
from collections import OrderedDict
from collections.abc import Awaitable, Callable, Iterable, Mapping, Sequence
from dataclasses import dataclass, field, replace
from typing import TYPE_CHECKING, Any

from ..api import MessageDirection
from ..events import BadEventType, Event, MegolmEvent, RoomMemberEvent
from ..responses import RoomMessagesResponse

if TYPE_CHECKING:
    from ..store.database import MatrixStore

logger = logging.getLogger(__name__)


class CommittedCancellation(asyncio.CancelledError):
    pass


FetchMessages = Callable[
    [str, str, str | None, MessageDirection, int], Awaitable[object]
]
DispatchEvent = Callable[[str, Event | BadEventType], Awaitable[Event | None]]


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

    @classmethod
    def from_event(
        cls,
        room_id: str,
        generation: int,
        sequence: int,
        event: Event | BadEventType,
        is_live: bool,
    ) -> PendingTimelineEvent | None:
        event_id = getattr(event, "event_id", None)
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
        )

    def parse(self) -> Event | BadEventType:
        return Event.parse_event(json.loads(self.source_json))


@dataclass(frozen=True)
class RecoveryPlan:
    clear_rooms: frozenset[str] = frozenset()
    gaps: tuple[RecoveryGap, ...] = ()
    events: tuple[PendingTimelineEvent, ...] = ()
    clear_recovered: RecoveryGap | None = None


@dataclass
class RecoveryState:
    gaps: dict[str, list[RecoveryGap]] = field(default_factory=dict)
    events: dict[tuple[str, int], list[PendingTimelineEvent]] = field(
        default_factory=dict
    )
    completed: dict[str, OrderedDict[str, bool]] = field(default_factory=dict)
    room_offset: int = 0
    write_lock: asyncio.Lock = field(default_factory=asyncio.Lock)


def _is_own_join(event: Event | BadEventType, user_id: str | None) -> bool:
    return bool(
        user_id
        and isinstance(event, RoomMemberEvent)
        and event.state_key == user_id
        and event.membership == "join"
        and event.prev_membership != "join"
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
    for event in timeline_events:
        event_id = getattr(event, "event_id", None)
        pending = PendingTimelineEvent.from_event(
            room_id, generation, sequence, event, True
        )
        if not event_id or not pending:
            continue
        existing = known.get(event_id)
        if existing:
            if existing.was_encrypted and not pending.was_encrypted:
                planned.append(
                    replace(
                        pending,
                        generation=existing.generation,
                        sequence=existing.sequence,
                        is_live=existing.is_live,
                    )
                )
            continue
        if not should_dispatch_timeline_event(state, room_id, event):
            continue
        planned.append(pending)
        known[event_id] = pending
        sequence += 1
    return planned


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
) -> RecoveryPlan:
    if membership in {"leave", "ban", "invite"}:
        return RecoveryPlan(clear_rooms=frozenset({room_id}))

    last_join = max(
        (
            index
            for index, event in enumerate(timeline_events)
            if _is_own_join(event, user_id)
        ),
        default=-1,
    )
    start = last_join + 1
    live_start = (
        0
        if live_event_count is None
        else max(0, len(timeline_events) - live_event_count)
    )
    clear = last_join >= live_start
    existing = () if clear else state.gaps.get(room_id, ())
    new_gap = cursor_token is not None and not clear
    generation = existing[-1].generation if existing else 0
    if new_gap or not existing:
        generation += 1
    events = _plan_live_events(
        state,
        room_id,
        generation,
        timeline_events[start:],
        include_pending=not clear,
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
    clear_recovered = None
    for plan in plans:
        clear_rooms.update(plan.clear_rooms)
        gaps.extend(plan.gaps)
        events.extend(plan.events)
        clear_recovered = plan.clear_recovered or clear_recovered
    return RecoveryPlan(
        frozenset(clear_rooms), tuple(gaps), tuple(events), clear_recovered
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
    plan = merge_recovery_plans(
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
        )
        for room_id, room_info in joined_rooms.items()
    )
    return RecoveryPlan(
        plan.clear_rooms | frozenset(reset_room_ids),
        plan.gaps,
        plan.events,
    )


def apply_plan(state: RecoveryState, plan: RecoveryPlan) -> None:
    for room_id in plan.clear_rooms:
        for gap in state.gaps.pop(room_id, ()):
            state.events.pop((room_id, gap.generation), None)

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
        state.events[key][:] = [event for event in state.events[key] if event.is_live]

    for event in plan.events:
        state.completed.get(event.room_id, {}).pop(event.event_id, None)
        key = (event.room_id, event.generation)
        queued = state.events.setdefault(key, [])
        _merge_event(queued, event)
    for key in {(event.room_id, event.generation) for event in plan.events}:
        state.events[key].sort(key=lambda item: (item.is_live, item.sequence))


def _merge_event(
    queued: list[PendingTimelineEvent], event: PendingTimelineEvent
) -> None:
    existing = next((item for item in queued if item.event_id == event.event_id), None)
    if existing is None:
        queued.append(event)
    elif existing.was_encrypted and not event.was_encrypted:
        queued[queued.index(existing)] = replace(
            event,
            generation=existing.generation,
            sequence=existing.sequence,
            is_live=existing.is_live,
        )


def load_recovery_state(
    state: RecoveryState,
    gaps: Iterable[Any],
    events: Iterable[Any],
) -> None:
    state.gaps.clear()
    state.events.clear()
    state.completed.clear()
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
        )
        state.events.setdefault((row.room_id, row.generation), []).append(event)
    for queued in state.events.values():
        queued.sort(key=lambda event: (event.is_live, event.sequence))


async def _commit(
    state: RecoveryState,
    store: MatrixStore | None,
    operation: Callable[..., None] | None,
    apply: Callable[[], None],
    *args: Any,
) -> None:
    cancelled = False
    if store:
        assert operation
        async with state.write_lock:
            if not store.supports_threaded_writes:
                operation(*args)
            else:
                task = asyncio.create_task(asyncio.to_thread(operation, *args))
                try:
                    await asyncio.shield(task)
                except asyncio.CancelledError:
                    await asyncio.shield(task)
                    cancelled = True
    apply()
    if cancelled:
        raise CommittedCancellation


async def persist_response_plan(
    state: RecoveryState,
    store: MatrixStore | None,
    *,
    token: str | None,
    plan: RecoveryPlan,
) -> None:
    await _commit(
        state,
        store,
        store.save_recovery if store else None,
        lambda: apply_plan(state, plan),
        token,
        set(plan.clear_rooms),
        plan.gaps,
        plan.events,
        plan.clear_recovered,
    )


def _finish_memory(
    state: RecoveryState,
    gap: RecoveryGap,
    event: PendingTimelineEvent | None,
    was_encrypted: bool,
) -> None:
    key = (gap.room_id, gap.generation)
    if event:
        state.events[key].remove(event)
        record_completed_timeline_event(
            state, gap.room_id, event.event_id, was_encrypted
        )
        return
    state.events.pop(key, None)
    gaps = state.gaps[gap.room_id]
    gaps.remove(gap)
    if not gaps:
        state.gaps.pop(gap.room_id)


async def _finish(
    state: RecoveryState,
    store: MatrixStore | None,
    gap: RecoveryGap,
    event: PendingTimelineEvent | None = None,
    was_encrypted: bool = False,
) -> None:
    await _commit(
        state,
        store,
        store.finish_recovery if store else None,
        lambda: _finish_memory(state, gap, event, was_encrypted),
        gap.room_id,
        gap.generation,
        event.event_id if event else None,
        was_encrypted,
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
    recovered_count = 0
    clear_recovered = False
    cursor = gap.cursor_token
    pending_ids = {
        event.event_id
        for (event_room, generation), queued in state.events.items()
        if event_room == gap.room_id and generation > 0
        for event in queued
    }

    while cursor and pages < options.max_pages and recovered_count < options.max_events:
        remaining = deadline - asyncio.get_running_loop().time()
        if remaining <= 0:
            break
        limit = min(options.page_size, options.max_events - recovered_count)
        try:
            response = await asyncio.wait_for(
                fetch_messages(
                    gap.room_id,
                    cursor,
                    gap.target_token,
                    MessageDirection.front,
                    limit,
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
            logger.warning("Limited-timeline recovery stopped in %s", gap.room_id)
            break

        recovered: list[PendingTimelineEvent] = []
        next_sequence = 1 + max(
            (
                event.sequence
                for event in state.events.get((gap.room_id, gap.generation), ())
                if not event.is_live
            ),
            default=-1,
        )
        chunk = response.chunk[: options.max_events - recovered_count]
        truncated = len(chunk) < len(response.chunk)
        for event in chunk:
            event_id = getattr(event, "event_id", None)
            if not event_id:
                continue
            if event_id in pending_ids or not should_dispatch_timeline_event(
                state, gap.room_id, event
            ):
                continue
            if _is_own_join(event, user_id):
                recovered.clear()
                clear_recovered = True
                next_sequence = 0
                continue
            pending = PendingTimelineEvent.from_event(
                gap.room_id, gap.generation, next_sequence, event, False
            )
            if pending:
                recovered.append(pending)
                pending_ids.add(event_id)
                next_sequence += 1

        if response.end == gap.target_token:
            next_cursor = None
        elif truncated:
            break
        elif response.end is None or response.end == cursor:
            logger.error("Abandoning unverifiable gap in %s", gap.room_id)
            recovered.clear()
            clear_recovered = True
            next_cursor = None
        else:
            next_cursor = response.end

        updated = replace(gap, cursor_token=next_cursor)
        if deadline - asyncio.get_running_loop().time() <= 0:
            break
        await persist_response_plan(
            state,
            store,
            token=None,
            plan=RecoveryPlan(
                gaps=(updated,),
                events=tuple(recovered),
                clear_recovered=updated if clear_recovered else None,
            ),
        )
        gap = updated
        recovered_count += len(recovered)
        clear_recovered = False
        if next_cursor is None:
            break
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
        try:
            event = pending.parse()
        except Exception:
            logger.exception("Discarding corrupt recovered event: %s", pending.event_id)
            await _finish(state, store, gap, pending, pending.was_encrypted)
            continue
        try:
            dispatch = dispatch_event(gap.room_id, event)
            delivered = (
                await dispatch
                if deadline is None
                else await asyncio.wait_for(
                    dispatch, timeout=deadline - asyncio.get_running_loop().time()
                )
            )
        except asyncio.TimeoutError:
            logger.warning("Recovered event callback timed out: %s", pending.event_id)
            return
        except Exception:
            logger.exception("Recovered event callback failed: %s", pending.event_id)
            return
        await _finish(
            state,
            store,
            gap,
            pending,
            isinstance(delivered, MegolmEvent) if delivered else pending.was_encrypted,
        )

    await _finish(state, store, gap)


async def pump_recovery(
    state: RecoveryState,
    *,
    user_id: str | None,
    options: RecoveryOptions,
    fetch_messages: FetchMessages,
    dispatch_event: DispatchEvent,
    store: MatrixStore | None,
) -> None:
    room_ids = list(state.gaps)
    if not room_ids:
        return
    offset = state.room_offset % len(room_ids)
    room_ids = room_ids[offset:] + room_ids[:offset]
    state.room_offset = (offset + 1) % len(room_ids)
    room_ids.sort(key=lambda room_id: state.gaps[room_id][0].cursor_token is not None)
    recovering = sum(
        state.gaps[room_id][0].cursor_token is not None for room_id in room_ids
    )
    deadline = asyncio.get_running_loop().time() + options.timeout
    for room_id in room_ids:
        gaps = state.gaps.get(room_id)
        if not gaps:
            continue
        gap = gaps[0]
        room_deadline = deadline
        if gap.cursor_token is not None:
            remaining = deadline - asyncio.get_running_loop().time()
            if remaining <= 0:
                continue
            room_deadline = asyncio.get_running_loop().time() + remaining / recovering
            recovering -= 1
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
