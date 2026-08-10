from __future__ import annotations

from dataclasses import dataclass, replace
from typing import Literal
from uuid import UUID, uuid5

from ..ingest._json import canonical_json
from ..ingest.model import EventRecord, RecordKind
from ..ingest.reducer import DescriptorRoute, RecoveryRelease, reduce_staged_frame
from ..ingest.serialization import _record_to_dict
from ..ingest.source import SyncFrame
from ._sync_journal_values import MaterializerLimits, RoomAggregateValue

type PlannedWork = tuple[EventRecord, bytes, int | None]


@dataclass(frozen=True, slots=True)
class AuthenticatedWork:
    value: EventRecord
    status: Literal["ready", "held"]
    canonical_size: int


@dataclass(frozen=True, slots=True)
class MaterializationPlan:
    room_values: tuple[RoomAggregateValue, ...]
    work_inserts: tuple[PlannedWork, ...]
    crypto_deferred: bool


def _canonical_work_plaintext(kind: str, value: EventRecord) -> bytes:
    if type(kind) is not str:
        raise TypeError("work kind must be str")
    if kind != "event" or type(value) is not EventRecord:
        raise ValueError("Task 3 supports only event Work values")
    return canonical_json({"kind": kind, "value": _record_to_dict(value)})


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
    if (
        len(room_plans) != len(proposal.room_proposals)
        or aggregate_by_room.keys() - room_plans.keys()
        or any(
            room.recovery is not None
            or room.retirement_epoch is not None
            or room.losses
            or room.release is not RecoveryRelease.NONE
            for room in proposal.room_proposals
        )
    ):
        raise ValueError("selected frame requires a later room path")

    existing_ids = {item.value.record_id for item in work}
    room_sequences: dict[str, int] = {}
    pending_hydrations = {}
    for room_id, room in room_plans.items():
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

    room_counts = dict.fromkeys(room_plans, 0)
    room_bytes = dict.fromkeys(room_plans, 0)
    seen_sequences: set[tuple[str, int, int]] = set()
    for item in work:
        value = item.value
        if item.status != "held":
            continue
        held_room_id = value.room_id
        if held_room_id is None or held_room_id not in room_plans:
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
        room_counts[held_room_id] += 1
        room_bytes[held_room_id] += item.canonical_size

    inserts: list[PlannedWork] = []
    planned_ids: set[str] = set()
    ready_ordinal = 0
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
            if (
                descriptor_room_id not in room_plans
                or descriptor.route is not DescriptorRoute.HOLD_FOR_HYDRATION
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
            epoch = room_plans[descriptor_room_id].after.membership_epoch
            sequence = room_sequences[descriptor_room_id]
            room_sequences[descriptor_room_id] += 1
            ordinal = None
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
        plaintext = _canonical_work_plaintext("event", value)
        if len(plaintext) > limits.max_record_canonical_bytes:
            raise ValueError("planned Work record exceeds the canonical byte limit")
        if work_id in planned_ids or work_id in existing_ids:
            raise ValueError("planned Work identity collides")
        planned_ids.add(work_id)
        inserts.append((value, plaintext, ordinal))
        if descriptor_room_id is not None:
            room_counts[descriptor_room_id] += 1
            room_bytes[descriptor_room_id] += len(plaintext)

    if any(
        room_counts[room_id] > limits.max_held_records_per_room
        or room_bytes[room_id] > limits.max_held_canonical_bytes_per_room
        for room_id in room_plans
    ):
        raise ValueError("planned HELD Work exceeds room capacity")

    planned_ready = tuple(item for item in inserts if item[2] is not None)
    existing_ready = tuple(item for item in work if item.status == "ready")
    if (
        len(existing_ready) + len(planned_ready) > limits.max_ready_work_count
        or sum(item.canonical_size for item in existing_ready)
        + sum(len(item[1]) for item in planned_ready)
        > limits.max_ready_work_canonical_bytes
        or len(work) + len(inserts) > limits.max_total_work_count
        or sum(item.canonical_size for item in work)
        + sum(len(item[1]) for item in inserts)
        > limits.max_total_work_canonical_bytes
    ):
        return None

    room_values: list[RoomAggregateValue] = []
    for room in proposal.room_proposals:
        room_id = room.after.room_id
        aggregate = aggregate_by_room.get(room_id)
        if (
            aggregate is not None
            and room.after == aggregate.continuity
            and room_sequences[room_id] == aggregate.next_room_sequence
        ):
            continue
        room_values.append(
            RoomAggregateValue(
                room.after,
                room_sequences[room_id],
                revision,
                pending_hydrations[room_id],
            )
        )
    return MaterializationPlan(
        tuple(room_values), tuple(inserts), proposal.crypto_deferred
    )
