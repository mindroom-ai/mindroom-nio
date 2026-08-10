from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal
from uuid import UUID, uuid5

from ..ingest._json import canonical_json
from ..ingest.model import EventRecord, LossBoundary, LossReason, LossRecord, RecordKind
from ..ingest.reducer import (
    DescriptorRoute,
    LossProposal,
    RecoveryRelease,
    reduce_staged_frame,
)
from ..ingest.serialization import _loss_id, _record_to_dict
from ..ingest.source import SyncFrame
from ._sync_journal_values import MaterializerLimits, RoomAggregateValue

type PlannedWork = tuple[EventRecord | LossRecord, bytes, int | None]


@dataclass(frozen=True, slots=True)
class AuthenticatedWork:
    value: EventRecord | LossRecord
    status: Literal["ready", "held"]
    canonical_size: int


@dataclass(frozen=True, slots=True)
class MaterializationPlan:
    room_values: tuple[RoomAggregateValue, ...]
    work_inserts: tuple[PlannedWork, ...]
    work_releases: tuple[tuple[EventRecord, bytes, int], ...]
    crypto_deferred: bool


def _canonical_work_plaintext(kind: str, value: EventRecord | LossRecord) -> bytes:
    if type(kind) is not str:
        raise TypeError("work kind must be str")
    expected = "event" if type(value) is EventRecord else "loss"
    if type(value) not in (EventRecord, LossRecord) or kind != expected:
        raise ValueError("Work kind and value type do not agree")
    return canonical_json({"kind": kind, "value": _record_to_dict(value)})


def _work_id(value: EventRecord | LossRecord) -> str:
    return value.record_id if isinstance(value, EventRecord) else value.loss_id


def _planned_work(
    value: EventRecord | LossRecord,
    ordinal: int | None,
    used_ids: set[str],
    max_bytes: int | None,
) -> PlannedWork:
    work_id = _work_id(value)
    plaintext = _canonical_work_plaintext(
        "event" if type(value) is EventRecord else "loss", value
    )
    if max_bytes is not None and len(plaintext) > max_bytes:
        raise ValueError("planned Work record exceeds the canonical byte limit")
    if work_id in used_ids:
        raise ValueError("planned Work identity collides")
    used_ids.add(work_id)
    return value, plaintext, ordinal


def plan_frame_materialization(
    *,
    stream_id: UUID,
    frame: SyncFrame,
    aggregates: tuple[RoomAggregateValue, ...],
    work: tuple[AuthenticatedWork, ...],
    revision: int,
    limits: MaterializerLimits,
) -> MaterializationPlan | None:
    aggregate_by_room = {item.continuity.room_id: item for item in aggregates}
    if len(aggregate_by_room) != len(aggregates):
        raise ValueError("room Aggregates must have unique room IDs")
    continuities = tuple(aggregate.continuity for aggregate in aggregates)
    proposal = reduce_staged_frame(stream_id, frame.frame_id, frame, continuities)
    room_plans = {room.after.room_id: room for room in proposal.room_proposals}
    descriptor_rooms = {
        descriptor.room_id
        for descriptor in proposal.descriptors
        if descriptor.room_id is not None
    }
    ephemeral_room_id = (
        next(iter(descriptor_rooms))
        if not room_plans
        and len(descriptor_rooms) == 1
        and descriptor_rooms == aggregate_by_room.keys()
        else None
    )
    if len(room_plans) != len(proposal.room_proposals) or (
        ephemeral_room_id is None and aggregate_by_room.keys() - room_plans.keys()
    ):
        raise ValueError("selected frame has inconsistent room ownership")
    retirements = tuple(
        room
        for room in proposal.room_proposals
        if room.retirement_epoch is not None
        or room.losses
        or room.release is not RecoveryRelease.NONE
    )
    if len(retirements) > 1:
        raise ValueError("this checkpoint retires exactly one room")
    retirement = retirements[0] if retirements else None
    retirement_room_id = retirement.after.room_id if retirement is not None else None
    if retirement is not None:
        before = aggregate_by_room[retirement.after.room_id].continuity
        pending = aggregate_by_room[retirement.after.room_id].pending_hydration
        after = replace(
            before,
            membership_epoch=before.membership_epoch + 1,
            membership=retirement.after.membership,
            hydration_id=None,
        )
        expected_loss = LossProposal(
            before.room_id,
            before.membership_epoch,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
        )
        if (
            pending is None
            or before.hydration_id != pending.hydration_id
            or after.membership == before.membership
            or retirement.before != before
            or retirement.after != after
            or retirement.recovery is not None
            or retirement.hydration is not None
            or retirement.retirement_epoch != before.membership_epoch
            or retirement.losses != (expected_loss,)
            or retirement.release is not RecoveryRelease.LOSS_THEN_HELD
        ):
            raise ValueError("invalid pending-hydration retirement")
    elif any(room.recovery is not None for room in proposal.room_proposals):
        raise ValueError("selected frame requires a later room path")

    used_ids = {_work_id(item.value) for item in work}
    room_sequences: dict[str, int] = {}
    pending_hydrations = {}
    for room_id, room in room_plans.items():
        if room_id == retirement_room_id:
            room_sequences[room_id] = aggregate_by_room[room_id].next_room_sequence
            continue
        if (hydration := room.hydration) is None:
            raise ValueError("selected frame requires a later room path")
        aggregate = aggregate_by_room.get(room_id)
        if aggregate is None:
            if room.before is not None:
                raise ValueError("room proposal is missing its Aggregate")
            room_sequences[room_id] = 0
            pending_hydrations[room_id] = hydration
        else:
            pending = aggregate.pending_hydration
            if (
                room.before != aggregate.continuity
                or pending is None
                or hydration.hydration_id != pending.hydration_id
            ):
                raise ValueError("room proposal does not continue its Aggregate")
            room_sequences[room_id] = aggregate.next_room_sequence
            pending_hydrations[room_id] = pending

    if ephemeral_room_id is not None:
        aggregate = aggregate_by_room[ephemeral_room_id]
        pending = aggregate.pending_hydration
        route = (
            DescriptorRoute.HOLD_FOR_HYDRATION
            if pending is not None
            else DescriptorRoute.READY
        )
        if aggregate.continuity.gap is not None or any(
            descriptor.kind is not RecordKind.EPHEMERAL
            or descriptor.route is not route
            or descriptor.provenance is not None
            for descriptor in proposal.descriptors
            if descriptor.room_id is not None
        ):
            raise ValueError("invalid ephemeral-only room ownership")
        room_sequences[ephemeral_room_id] = aggregate.next_room_sequence
        if pending is not None:
            pending_hydrations[ephemeral_room_id] = pending

    candidate = proposal.room_proposals[0] if len(room_plans) == 1 else None
    capacity_room_id = ephemeral_room_id
    if (
        capacity_room_id is None
        and retirement is None
        and candidate is not None
        and candidate.before == candidate.after
        and candidate.after.room_id in aggregate_by_room
        and candidate.hydration is not None
        and candidate.hydration.origin == frame.origin
    ):
        capacity_room_id = candidate.after.room_id

    held_count = 0
    held_bytes = 0
    retired_work: list[EventRecord] = []
    seen_sequences: set[tuple[str, int, int]] = set()
    for item in work:
        value = item.value
        if item.status != "held":
            continue
        if type(value) is not EventRecord:
            raise ValueError("HELD Work must be an event")
        held_count += 1
        held_bytes += item.canonical_size
        held_room_id = value.room_id
        if held_room_id is None or (
            held_room_id not in room_plans and held_room_id != ephemeral_room_id
        ):
            continue
        aggregate = aggregate_by_room.get(held_room_id)
        if aggregate is None:
            raise ValueError("new Aggregate has orphan HELD Work")
        membership_epoch = value.membership_epoch
        sequence = value.room_sequence
        if (
            membership_epoch is None
            or sequence is None
            or membership_epoch != aggregate.continuity.membership_epoch
            or sequence >= aggregate.next_room_sequence
        ):
            raise ValueError("HELD Work does not match its Aggregate")
        key = (held_room_id, membership_epoch, sequence)
        if key in seen_sequences:
            raise ValueError("HELD Work does not match its Aggregate")
        seen_sequences.add(key)
        if held_room_id == ephemeral_room_id and held_room_id not in pending_hydrations:
            raise ValueError("READY ephemeral room has orphan HELD Work")
        if held_room_id in {retirement_room_id, capacity_room_id}:
            retired_work.append(value)

    inserts: list[PlannedWork] = []
    releases: list[tuple[EventRecord, bytes, int]] = []
    ready_ordinal = 0
    room_additions = 0
    held_additions = 0
    oversized_room = False
    max_record_bytes = limits.max_record_canonical_bytes
    if retirement is not None:
        before = aggregate_by_room[retirement.after.room_id].continuity
        loss = LossRecord(
            "",
            frame.origin,
            before.room_id,
            before.membership_epoch,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        loss = replace(loss, loss_id=_loss_id(stream_id, loss))
        inserts.append(_planned_work(loss, ready_ordinal, used_ids, max_record_bytes))
        ready_ordinal += 1
        retired_work.sort(key=lambda value: (value.room_sequence or 0, value.record_id))
        releases = [
            (value, _canonical_work_plaintext("event", value), ordinal)
            for ordinal, value in enumerate(retired_work, 1)
        ]
        ready_ordinal += len(releases)
        lifecycle_key = (
            f"lifecycle:{before.room_id}:{before.membership_epoch}:"
            f"{retirement.after.membership_epoch}"
        )
        lifecycle = EventRecord(
            str(uuid5(frame.frame_id, lifecycle_key)),
            RecordKind.ROOM_LIFECYCLE,
            frame.origin,
            before.room_id,
            retirement.after.membership_epoch,
            room_sequences[before.room_id],
            None,
            None,
            canonical_json(
                {
                    "membership": retirement.after.membership,
                    "membership_epoch": retirement.after.membership_epoch,
                    "previous_membership_epoch": before.membership_epoch,
                }
            ),
            None,
        )
        room_sequences[before.room_id] += 1
        inserts.append(
            _planned_work(lifecycle, ready_ordinal, used_ids, max_record_bytes)
        )
        ready_ordinal += 1
    for index, descriptor in enumerate(proposal.descriptors):
        descriptor_room_id = descriptor.room_id
        if descriptor_room_id is None:
            if (
                descriptor.route is not DescriptorRoute.READY
                or descriptor.kind
                not in (RecordKind.GLOBAL_ACCOUNT_DATA, RecordKind.PRESENCE)
                or descriptor.provenance is not None
            ):
                raise ValueError("unsupported account-wide descriptor")
            epoch = sequence = None
            ordinal: int | None = ready_ordinal
            ready_ordinal += 1
        else:
            is_ephemeral_room = descriptor_room_id == ephemeral_room_id
            route = (
                DescriptorRoute.HOLD_FOR_RETIREMENT
                if descriptor_room_id == retirement_room_id
                else (
                    DescriptorRoute.READY
                    if is_ephemeral_room
                    and descriptor_room_id not in pending_hydrations
                    else DescriptorRoute.HOLD_FOR_HYDRATION
                )
            )
            if (
                (descriptor_room_id not in room_plans and not is_ephemeral_room)
                or descriptor.route is not route
                or descriptor.kind
                not in (
                    RecordKind.STATE,
                    RecordKind.TIMELINE,
                    RecordKind.EPHEMERAL,
                    RecordKind.ROOM_ACCOUNT_DATA,
                )
                or (
                    (descriptor.kind is RecordKind.TIMELINE)
                    != (descriptor.provenance is not None)
                )
            ):
                raise ValueError("unsupported room descriptor")
            epoch = (
                aggregate_by_room[descriptor_room_id].continuity.membership_epoch
                if is_ephemeral_room
                else room_plans[descriptor_room_id].after.membership_epoch
            )
            sequence = room_sequences[descriptor_room_id]
            room_sequences[descriptor_room_id] += 1
            ordinal = (
                ready_ordinal
                if descriptor_room_id == retirement_room_id
                or descriptor.route is DescriptorRoute.READY
                else None
            )
            if ordinal is not None:
                ready_ordinal += 1
        work_id = str(uuid5(frame.frame_id, f"event:{descriptor.descriptor_key}"))
        value = EventRecord(
            work_id,
            descriptor.kind,
            replace(frame.origin, frame_index=index),
            descriptor_room_id,
            epoch,
            sequence,
            None,
            descriptor.provenance,
            descriptor.source_json,
            None,
        )
        planned = _planned_work(value, ordinal, used_ids, None)
        if len(planned[1]) > max_record_bytes:
            if descriptor_room_id is None or capacity_room_id is None:
                raise ValueError("planned Work record exceeds the canonical byte limit")
            oversized_room = True
        inserts.append(planned)
        if descriptor_room_id is not None and retirement is None:
            room_additions += 1
            if ordinal is None:
                held_additions += 1
                held_count += 1
                held_bytes += len(planned[1])

    hard = MaterializerLimits()
    addition_bytes = sum(len(item[1]) for item in inserts)
    hard_addition = (
        len(inserts) > hard.max_held_work_count
        or addition_bytes > hard.max_held_work_canonical_bytes
    )
    capacity_reason = None
    if capacity_room_id is not None and room_additions:
        if oversized_room:
            capacity_reason = LossReason.OVERSIZED_EVENT
        elif (
            held_additions
            and (
                held_count > limits.max_held_work_count
                or held_bytes > limits.max_held_work_canonical_bytes
            )
        ) or hard_addition:
            capacity_reason = LossReason.EVENT_LIMIT
    if capacity_reason is not None and capacity_room_id is not None:
        before = aggregate_by_room[capacity_room_id].continuity
        loss = LossRecord(
            "",
            frame.origin,
            before.room_id,
            before.membership_epoch,
            capacity_reason,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        loss = replace(loss, loss_id=_loss_id(stream_id, loss))
        loss_work = _planned_work(loss, 0, used_ids, max_record_bytes)
        retired_work.sort(key=lambda value: (value.room_sequence or 0, value.record_id))
        releases = [
            (value, _canonical_work_plaintext("event", value), ordinal)
            for ordinal, value in enumerate(retired_work, 1)
        ]
        inserts = [loss_work] + [
            (value, plaintext, index + len(releases) + 1)
            for index, (value, plaintext, _) in enumerate(
                item for item in inserts if item[0].room_id is None
            )
        ]
        room_sequences[before.room_id] = aggregate_by_room[
            before.room_id
        ].next_room_sequence
        addition_bytes = sum(len(item[1]) for item in inserts)

    if (
        retirement is None
        and capacity_reason is None
        and held_additions
        and (
            held_count > limits.max_held_work_count
            or held_bytes > limits.max_held_work_canonical_bytes
        )
    ):
        raise ValueError("planned HELD Work exceeds global HELD capacity")
    if capacity_reason is None and hard_addition:
        raise ValueError("selected frame Work exceeds the hard addition envelope")

    planned_ready = tuple(item for item in inserts if item[2] is not None)
    existing_ready = tuple(item for item in work if item.status == "ready")
    if (
        retirement is None
        and capacity_reason is None
        and planned_ready
        and (
            len(existing_ready) + len(planned_ready) > limits.max_ready_work_count
            or sum(item.canonical_size for item in existing_ready)
            + sum(len(item[1]) for item in planned_ready)
            > limits.max_ready_work_canonical_bytes
        )
    ):
        return None
    capacity = hard if retirement is not None or capacity_reason is not None else limits
    if capacity_reason is not None and (
        len(inserts) > hard.max_held_work_count
        or addition_bytes > hard.max_held_work_canonical_bytes
    ):
        raise ValueError("selected frame Work exceeds the hard addition envelope")
    if len(work) + len(inserts) > capacity.max_total_work_count or (
        sum(item.canonical_size for item in work) + addition_bytes
        > capacity.max_total_work_canonical_bytes
    ):
        return None

    room_values = tuple(
        RoomAggregateValue(
            (
                replace(room.after, hydration_id=None)
                if capacity_reason is not None
                else room.after
            ),
            room_sequences[room.after.room_id],
            revision,
            (
                None
                if room.after.room_id == retirement_room_id
                or capacity_reason is not None
                else pending_hydrations[room.after.room_id]
            ),
        )
        for room in proposal.room_proposals
        if (
            capacity_reason is not None
            or (stored := aggregate_by_room.get(room.after.room_id)) is None
            or room.after != stored.continuity
            or room_sequences[room.after.room_id] != stored.next_room_sequence
        )
    )
    if ephemeral_room_id is not None:
        aggregate = aggregate_by_room[ephemeral_room_id]
        pending = aggregate.pending_hydration
        if capacity_reason is None or pending is not None:
            room_values += (
                RoomAggregateValue(
                    (
                        replace(aggregate.continuity, hydration_id=None)
                        if capacity_reason is not None
                        else aggregate.continuity
                    ),
                    room_sequences[ephemeral_room_id],
                    revision,
                    None if capacity_reason is not None else pending,
                ),
            )
    return MaterializationPlan(
        room_values,
        tuple(inserts),
        tuple(releases),
        proposal.crypto_deferred,
    )
