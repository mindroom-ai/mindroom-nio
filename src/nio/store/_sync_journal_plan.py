from __future__ import annotations

from collections import defaultdict
from dataclasses import dataclass, replace
from operator import itemgetter
from typing import Literal, NamedTuple, cast
from uuid import UUID

from ..ingest._json import canonical_json, load_json
from ..ingest.errors import JournalCapacityError
from ..ingest.model import (
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RoomSnapshot,
    TransportKind,
    _CallbackRoute,
    _DecryptedToDeviceKind,
    _DecryptionDisposition,
    _PreparationPhase,
    _PreparedIngestionFrame,
)
from ..ingest.reducer import (
    DescriptorRoute,
    HydrationIntent,
    PreparedRecordStep,
    PreparedRecoveryStep,
    PreparedTransitionStep,
    RecoveryRelease,
    reduce_prepared_frame,
)
from ..ingest.serialization import _loss_id, _record_to_dict
from ..ingest.source import SyncFrame
from ._sync_journal_format import _canonical_internal, _row
from ._sync_journal_values import (
    _HELD_PROMOTION_RESERVE_BYTES,
    MaterializerLimits,
    RoomAggregateValue,
)


class _PreparedWorkMetadata(NamedTuple):
    record_id: str
    preparation_phase: _PreparationPhase
    effective_event_type: str
    decryption: _DecryptionDisposition
    decryption_verified: bool | None
    decrypted_to_device_kind: _DecryptedToDeviceKind | None
    callback_route: _CallbackRoute | None


class PlannedWork(NamedTuple):
    value: EventRecord | LossRecord
    plaintext: bytes
    ready_ordinal: int | None
    metadata: _PreparedWorkMetadata | None = None


@dataclass(frozen=True, slots=True)
class _StoredWorkRow:
    work_id: str
    kind: Literal["event", "loss"]
    status: Literal["ready", "held"]
    frame_id: UUID
    room_id: str | None
    membership_epoch: int | None
    room_sequence: int | None
    ready_revision: int | None
    ready_ordinal: int | None
    created_revision: int
    plaintext: bytes

    @property
    def clear_values(self) -> tuple[object, ...]:
        return (
            self.work_id,
            self.kind,
            self.status,
            str(self.frame_id),
            self.room_id,
            self.membership_epoch,
            self.room_sequence,
            self.ready_revision,
            self.ready_ordinal,
            self.created_revision,
        )


@dataclass(frozen=True, slots=True)
class AuthenticatedWork:
    """Immutable Work already validated at the journal's disk boundary."""

    value: EventRecord | LossRecord
    status: Literal["ready", "held"]
    canonical_size: int
    metadata: _PreparedWorkMetadata | None = None
    plaintext: bytes | None = None
    frame_id: UUID | None = None
    created_revision: int | None = None


@dataclass(frozen=True, slots=True)
class MaterializationPlan:
    room_values: tuple[RoomAggregateValue, ...]
    work_inserts: tuple[PlannedWork, ...]
    work_releases: tuple[PlannedWork, ...]
    crypto_deferred: bool


def _stored_work_insert_row(
    item: PlannedWork,
    frame_id: UUID,
    revision: int,
) -> _StoredWorkRow:
    value, plaintext, ordinal = item.value, item.plaintext, item.ready_ordinal
    is_event = type(value) is EventRecord
    return _StoredWorkRow(
        _work_id(value),
        "event" if is_event else "loss",
        "held" if ordinal is None else "ready",
        frame_id,
        value.room_id,
        value.membership_epoch,
        cast("EventRecord", value).room_sequence if is_event else None,
        None if ordinal is None else revision,
        ordinal,
        revision,
        plaintext,
    )


def _stored_work_release_row(
    item: PlannedWork,
    existing: AuthenticatedWork,
    revision: int,
) -> _StoredWorkRow:
    value, plaintext, ordinal = item.value, item.plaintext, item.ready_ordinal
    if (
        type(value) is not EventRecord
        or type(existing.value) is not EventRecord
        or value.record_id != existing.value.record_id
        or existing.frame_id is None
        or existing.created_revision is None
    ):
        raise ValueError("released Work lacks authenticated row identity")
    return _StoredWorkRow(
        existing.value.record_id,
        "event",
        "ready",
        existing.frame_id,
        existing.value.room_id,
        existing.value.membership_epoch,
        existing.value.room_sequence,
        revision,
        ordinal,
        existing.created_revision,
        plaintext,
    )


def _stored_work_size(
    owner: tuple[str, UUID, TransportKind],
    stored: _StoredWorkRow,
) -> int:
    return len(
        _row(
            owner,
            "NioIngestWork",
            stored.plaintext,
            header=_canonical_internal(stored.clear_values),
        )[0]
    )


def _canonical_work_plaintext(
    kind: str,
    value: EventRecord | LossRecord,
    metadata: _PreparedWorkMetadata | None = None,
) -> bytes:
    if type(kind) is not str:
        raise TypeError("work kind must be str")
    expected = "event" if type(value) is EventRecord else "loss"
    if type(value) not in (EventRecord, LossRecord) or kind != expected:
        raise ValueError("Work kind and value type do not agree")
    wrapper: dict[str, object] = {
        "kind": kind,
        "value": _record_to_dict(value),
    }
    if metadata is not None:
        if type(metadata) is not _PreparedWorkMetadata:
            raise TypeError("prepared Work metadata has an invalid type")
        if type(value) is not EventRecord or metadata.record_id != value.record_id:
            raise ValueError("prepared Work metadata identity is invalid")
        if (
            type(metadata.record_id) is not str
            or type(metadata.preparation_phase) is not _PreparationPhase
            or type(metadata.effective_event_type) is not str
            or not metadata.effective_event_type
            or type(metadata.decryption) is not _DecryptionDisposition
            or metadata.decryption_verified is not None
            and type(metadata.decryption_verified) is not bool
            or metadata.decrypted_to_device_kind is not None
            and type(metadata.decrypted_to_device_kind) is not _DecryptedToDeviceKind
            or metadata.callback_route is not None
            and type(metadata.callback_route) is not _CallbackRoute
        ):
            raise ValueError("prepared Work metadata is invalid")
        wrapper["preparation"] = {
            "record_id": metadata.record_id,
            "preparation_phase": metadata.preparation_phase.value,
            "effective_event_type": metadata.effective_event_type,
            "decryption": metadata.decryption.value,
            "decryption_verified": metadata.decryption_verified,
            "decrypted_to_device_kind": (
                metadata.decrypted_to_device_kind.value
                if metadata.decrypted_to_device_kind is not None
                else None
            ),
            "callback_route": (
                metadata.callback_route.value
                if metadata.callback_route is not None
                else None
            ),
        }
    return canonical_json(wrapper)


def _validate_prepared_metadata_semantics(
    value: EventRecord,
    metadata: _PreparedWorkMetadata,
) -> None:
    try:
        if metadata.record_id != value.record_id or metadata.record_id != str(
            UUID(metadata.record_id)
        ):
            raise ValueError
        source = load_json(value.source_json, "prepared Work source")
        clear = (
            load_json(value.clear_json, "prepared Work clear")
            if value.clear_json is not None
            else None
        )
        if (
            type(source) is not dict
            or canonical_json(source) != value.source_json
            or clear is not None
            and (type(clear) is not dict or canonical_json(clear) != value.clear_json)
        ):
            raise ValueError
        visible = clear if clear is not None else source
        visible_type = visible.get("type")
        if visible_type is None:
            if not (
                metadata.preparation_phase is _PreparationPhase.EXPIRED_VERIFICATION
                and metadata.effective_event_type == "m.key.verification.cancel"
            ):
                raise ValueError
        elif visible_type != metadata.effective_event_type:
            raise ValueError
        if value.event_id != source.get("event_id"):
            raise ValueError
        if (value.kind is RecordKind.TIMELINE) != (value.provenance is not None):
            raise ValueError
        if value.kind is RecordKind.ROOM_LIFECYCLE:
            raise ValueError
        if metadata.preparation_phase is not _PreparationPhase.SOURCE and (
            value.kind is not RecordKind.TO_DEVICE
            or metadata.callback_route is not _CallbackRoute.TO_DEVICE
            or metadata.preparation_phase is _PreparationPhase.EXPIRED_VERIFICATION
            and metadata.effective_event_type != "m.key.verification.cancel"
            or metadata.preparation_phase is _PreparationPhase.COLLECTED_KEY_REQUEST
            and metadata.effective_event_type != "m.room_key_request"
        ):
            raise ValueError
        if metadata.decryption is _DecryptionDisposition.DECRYPTED:
            if value.kind not in (RecordKind.TIMELINE, RecordKind.TO_DEVICE) or (
                clear is None
            ):
                raise ValueError
        elif clear is not None or (
            metadata.decryption is _DecryptionDisposition.MEGOLM_FAILED
            and value.kind is not RecordKind.TIMELINE
        ):
            raise ValueError
        if metadata.decryption_verified is not None and (
            value.kind is not RecordKind.TIMELINE
            or metadata.decryption is not _DecryptionDisposition.DECRYPTED
        ):
            raise ValueError
        if (metadata.decrypted_to_device_kind is not None) != (
            value.kind is RecordKind.TO_DEVICE
            and metadata.decryption is _DecryptionDisposition.DECRYPTED
        ):
            raise ValueError
        route_kinds = {
            _CallbackRoute.EVENT: {RecordKind.STATE, RecordKind.TIMELINE},
            _CallbackRoute.EPHEMERAL: {RecordKind.EPHEMERAL},
            _CallbackRoute.ROOM_ACCOUNT_DATA: {RecordKind.ROOM_ACCOUNT_DATA},
            _CallbackRoute.GLOBAL_ACCOUNT_DATA: {RecordKind.GLOBAL_ACCOUNT_DATA},
            _CallbackRoute.TO_DEVICE: {RecordKind.TO_DEVICE},
        }
        if (
            metadata.callback_route is not None
            and value.kind not in route_kinds[metadata.callback_route]
        ):
            raise ValueError
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("prepared Work metadata is invalid") from error


def _decode_prepared_metadata(
    value: EventRecord,
    preparation: object,
) -> _PreparedWorkMetadata:
    try:
        if type(preparation) is not dict or set(preparation) != {
            "record_id",
            "preparation_phase",
            "effective_event_type",
            "decryption",
            "decryption_verified",
            "decrypted_to_device_kind",
            "callback_route",
        }:
            raise ValueError
        verified = preparation["decryption_verified"]
        if verified is not None and type(verified) is not bool:
            raise ValueError
        decrypted_kind = preparation["decrypted_to_device_kind"]
        callback_route = preparation["callback_route"]
        metadata = _PreparedWorkMetadata(
            preparation["record_id"],
            _PreparationPhase(preparation["preparation_phase"]),
            preparation["effective_event_type"],
            _DecryptionDisposition(preparation["decryption"]),
            verified,
            (
                _DecryptedToDeviceKind(decrypted_kind)
                if decrypted_kind is not None
                else None
            ),
            _CallbackRoute(callback_route) if callback_route is not None else None,
        )
        _validate_prepared_metadata_semantics(value, metadata)
        return metadata
    except (KeyError, TypeError, ValueError) as error:
        raise ValueError("Work plaintext metadata is invalid") from error


def _work_id(value: EventRecord | LossRecord) -> str:
    return value.record_id if isinstance(value, EventRecord) else value.loss_id


def plan_prepared_frame_materialization(
    *,
    account_id: str,
    stream_id: UUID,
    frame: SyncFrame,
    prepared: _PreparedIngestionFrame,
    aggregates: tuple[RoomAggregateValue, ...],
    work: tuple[AuthenticatedWork, ...],
    revision: int,
    limits: MaterializerLimits,
) -> MaterializationPlan:
    """Plan Task4C output without regenerating records or transition order."""
    if type(account_id) is not str or not account_id:
        raise ValueError("account_id must be a nonempty string")
    if type(stream_id) is not UUID or type(frame) is not SyncFrame:
        raise TypeError("prepared planner owner inputs are invalid")
    if type(prepared) is not _PreparedIngestionFrame:
        raise TypeError("prepared must be _PreparedIngestionFrame")
    if type(aggregates) is not tuple or any(
        type(item) is not RoomAggregateValue for item in aggregates
    ):
        raise TypeError("aggregates must contain RoomAggregateValue values")
    if type(work) is not tuple or any(
        type(item) is not AuthenticatedWork for item in work
    ):
        raise TypeError("work must contain AuthenticatedWork values")
    if type(revision) is not int or revision < 1:
        raise ValueError("revision must be a positive integer")
    if type(limits) is not MaterializerLimits:
        raise TypeError("limits must be MaterializerLimits")
    if prepared.staged_revision >= revision:
        raise ValueError("prepared revision chronology is invalid")
    if any(aggregate.updated_revision >= revision for aggregate in aggregates):
        raise ValueError("Aggregate revision chronology is invalid")

    aggregate_by_room = {
        aggregate.continuity.room_id: aggregate for aggregate in aggregates
    }
    if len(aggregate_by_room) != len(aggregates):
        raise ValueError("room Aggregates must have unique room IDs")
    continuities = tuple(aggregate.continuity for aggregate in aggregates)
    proposal = reduce_prepared_frame(stream_id, frame, prepared, continuities)

    for transition in prepared.membership_transitions:
        if transition.source_record_id is None:
            continue
        evidence_source = transition.source_json
        if evidence_source is None:
            raise ValueError("prepared membership evidence is unavailable")
        evidence = load_json(
            evidence_source,
            "prepared membership evidence",
        )
        if type(evidence) is not dict or evidence.get("state_key") != account_id:
            raise ValueError("prepared membership evidence state_key is not the owner")

    room_result_by_id = {
        result.after.room_id: result for result in proposal.room_results
    }
    if len(room_result_by_id) != len(proposal.room_results):
        raise ValueError("prepared reduction has duplicate room results")
    snapshot_by_room = {
        snapshot.room_id: snapshot for snapshot in prepared.room_snapshots
    }
    if len(snapshot_by_room) != len(prepared.room_snapshots):
        raise ValueError("prepared room snapshots must have unique room IDs")
    for room_id, snapshot in snapshot_by_room.items():
        result = room_result_by_id.get(room_id)
        if result is None or (
            snapshot.own_user_id != account_id
            or snapshot.membership_epoch != result.after.membership_epoch
            or snapshot.own_membership != result.after.membership
        ):
            raise ValueError("prepared room snapshot does not match final continuity")
    room_sequences: dict[str, int] = {}
    pending_hydrations: dict[str, HydrationIntent | None] = {}
    for room_id, result in room_result_by_id.items():
        stored = aggregate_by_room.get(room_id)
        if stored is None:
            if result.before is not None:
                raise ValueError("prepared room result is missing its Aggregate")
            room_sequences[room_id] = 0
        else:
            if result.before != stored.continuity:
                raise ValueError("prepared room result disagrees with its Aggregate")
            room_sequences[room_id] = stored.next_room_sequence
        if result.after.hydration_id is None:
            pending = None
        elif (
            stored is not None
            and stored.pending_hydration is not None
            and stored.pending_hydration.hydration_id == result.after.hydration_id
        ):
            pending = stored.pending_hydration
        elif result.hydration is not None:
            if result.hydration.hydration_id != result.after.hydration_id:
                raise ValueError("prepared hydration result is inconsistent")
            pending = result.hydration
        else:
            raise ValueError("prepared hydration result has no durable owner")
        pending_hydrations[room_id] = pending

    used_ids: set[str] = set()
    existing_by_id: dict[str, AuthenticatedWork] = {}
    existing_held: dict[tuple[str, int], list[AuthenticatedWork]] = defaultdict(list)
    seen_room_sequences: set[tuple[str, int, int]] = set()
    for item in work:
        work_id = _work_id(item.value)
        if work_id in used_ids:
            raise ValueError("authenticated Work identities are not unique")
        used_ids.add(work_id)
        existing_by_id[work_id] = item
        if item.status != "held":
            continue
        value = item.value
        if (
            type(value) is not EventRecord
            or value.room_id is None
            or value.membership_epoch is None
            or value.room_sequence is None
        ):
            raise ValueError("HELD Work is not room-owned Event Work")
        aggregate = aggregate_by_room.get(value.room_id)
        if value.room_id in room_result_by_id and (
            aggregate is None
            or value.membership_epoch != aggregate.continuity.membership_epoch
            or not 0 <= value.room_sequence < aggregate.next_room_sequence
        ):
            raise ValueError("HELD Work does not match its Aggregate")
        if (
            value.room_id in room_result_by_id
            and aggregate is not None
            and (
                aggregate.continuity.gap is None
                and aggregate.continuity.hydration_id is None
            )
        ):
            raise ValueError("selected HELD Work has no Aggregate barrier")
        key = (value.room_id, value.membership_epoch, value.room_sequence)
        if key in seen_room_sequences:
            raise ValueError("HELD Work room sequence collides")
        seen_room_sequences.add(key)
        existing_held[(value.room_id, value.membership_epoch)].append(item)

    def stored_size(
        value: EventRecord | LossRecord,
        plaintext: bytes,
        ordinal: int | None,
    ) -> int:
        return _stored_work_size(
            (account_id, stream_id, frame.origin.transport),
            _stored_work_insert_row(
                PlannedWork(value, plaintext, ordinal), frame.frame_id, revision
            ),
        )

    def append_planned(
        value: EventRecord | LossRecord,
        metadata: _PreparedWorkMetadata | None,
        ordinal: int | None,
    ) -> int:
        work_id = _work_id(value)
        if work_id in used_ids:
            raise ValueError("planned Work identity collides")
        used_ids.add(work_id)
        plaintext = _canonical_work_plaintext(
            "event" if type(value) is EventRecord else "loss",
            value,
            metadata,
        )
        item = PlannedWork(value, plaintext, ordinal, metadata)
        if (
            stored_size(value, plaintext, ordinal)
            > hard_limits.max_record_canonical_bytes
        ):
            raise JournalCapacityError(
                "planned Work record exceeds the immutable byte limit"
            )
        inserts.append(item)
        if metadata is None:
            mandatory_work_ids.add(work_id)
        else:
            source_work_ids.add(work_id)
        return len(inserts) - 1

    hard_limits = MaterializerLimits()
    inserts: list[PlannedWork] = []
    releases: list[PlannedWork] = []
    source_work_ids: set[str] = set()
    source_ready_anchors: dict[str, int] = {}
    mandatory_work_ids: set[str] = set()
    released_existing_ids: set[str] = set()
    buffered: dict[tuple[str, int], list[int]] = defaultdict(list)
    ready_ordinal = 0

    def release_barrier(room_id: str, membership_epoch: int) -> None:
        nonlocal ready_ordinal
        candidates: list[tuple[int, str, AuthenticatedWork | int]] = []
        key = (room_id, membership_epoch)
        for held_item in existing_held.get(key, ()):
            held_value = held_item.value
            assert type(held_value) is EventRecord
            assert held_value.room_sequence is not None
            candidates.append(
                (held_value.room_sequence, held_value.record_id, held_item)
            )
        for insert_index in buffered.pop(key, ()):
            insert_value = inserts[insert_index].value
            assert type(insert_value) is EventRecord
            assert insert_value.room_sequence is not None
            candidates.append(
                (insert_value.room_sequence, insert_value.record_id, insert_index)
            )
        for _sequence, work_id, candidate in sorted(
            candidates,
            key=itemgetter(0, 1),
        ):
            if type(candidate) is AuthenticatedWork:
                if candidate.frame_id is None or candidate.created_revision is None:
                    raise ValueError(
                        "prepared HELD release lacks authenticated row identity"
                    )
                if work_id in released_existing_ids:
                    raise ValueError("HELD Work is released more than once")
                released_existing_ids.add(work_id)
                plaintext = candidate.plaintext or _canonical_work_plaintext(
                    "event", candidate.value
                )
                releases.append(
                    PlannedWork(
                        candidate.value, plaintext, ready_ordinal, candidate.metadata
                    )
                )
            else:
                if type(candidate) is not int:
                    raise ValueError("prepared HELD release candidate is invalid")
                inserts[candidate] = inserts[candidate]._replace(
                    ready_ordinal=ready_ordinal
                )
            ready_ordinal += 1

    for step in proposal.linear_steps:
        if type(step) is PreparedRecoveryStep:
            recovery_result = room_result_by_id.get(step.room_id)
            if (
                recovery_result is None
                or recovery_result.before is None
                or recovery_result.before.membership_epoch != step.membership_epoch
                or recovery_result.losses != step.losses
                or recovery_result.release is not RecoveryRelease.LOSS_THEN_HELD
            ):
                raise ValueError("prepared recovery step is invalid")
            for proposal_loss in step.losses:
                loss = LossRecord(
                    "",
                    frame.origin,
                    proposal_loss.room_id,
                    proposal_loss.membership_epoch,
                    proposal_loss.reason,
                    proposal_loss.boundary,
                    b"{}",
                )
                loss = replace(loss, loss_id=_loss_id(stream_id, loss))
                append_planned(loss, None, ready_ordinal)
                ready_ordinal += 1
            release_barrier(step.room_id, step.membership_epoch)
            continue

        if type(step) is PreparedTransitionStep:
            transition = step.transition
            if step.loss is not None:
                loss = LossRecord(
                    "",
                    transition.origin,
                    step.loss.room_id,
                    step.loss.membership_epoch,
                    step.loss.reason,
                    step.loss.boundary,
                    b"{}",
                )
                loss = replace(loss, loss_id=_loss_id(stream_id, loss))
                append_planned(loss, None, ready_ordinal)
                ready_ordinal += 1
                assert step.release_epoch is not None
                release_barrier(transition.room_id, step.release_epoch)
            sequence = room_sequences[transition.room_id]
            lifecycle = EventRecord(
                transition.transition_id,
                RecordKind.ROOM_LIFECYCLE,
                transition.origin,
                transition.room_id,
                transition.current_epoch,
                sequence,
                None,
                None,
                canonical_json(
                    {
                        "event_id": transition.event_id,
                        "membership": transition.current_membership,
                        "membership_epoch": transition.current_epoch,
                        "membership_provenance": (
                            transition.membership_provenance.value
                        ),
                        "previous_membership": transition.previous_membership,
                        "previous_membership_epoch": transition.previous_epoch,
                        "source_kind": transition.source_kind.value,
                        "source_record_id": transition.source_record_id,
                        "timeline_provenance": (
                            transition.timeline_provenance.value
                            if transition.timeline_provenance is not None
                            else None
                        ),
                    }
                ),
                None,
            )
            room_sequences[transition.room_id] += 1
            append_planned(lifecycle, None, ready_ordinal)
            ready_ordinal += 1
            continue

        if type(step) is not PreparedRecordStep:
            raise ValueError("prepared reduction contains an invalid linear step")
        record = step.record
        if record.room_id is None:
            if (
                step.route is not DescriptorRoute.READY
                or step.membership_epoch is not None
            ):
                raise ValueError("prepared account-wide route is invalid")
            record_sequence: int | None = None
        else:
            if record.room_id not in room_sequences:
                raise ValueError("prepared record has no room result")
            record_sequence = room_sequences[record.room_id]
            room_sequences[record.room_id] += 1
        record_value = EventRecord(
            record.record_id,
            record.kind,
            record.origin,
            record.room_id,
            step.membership_epoch,
            record_sequence,
            record.event_id,
            record.provenance,
            record.source_json,
            record.clear_json,
        )
        record_metadata = _PreparedWorkMetadata(
            record.record_id,
            record.preparation_phase,
            record.effective_event_type,
            record.decryption,
            record.decryption_verified,
            record.decrypted_to_device_kind,
            record.callback_route,
        )
        source_ready_anchors[record.record_id] = ready_ordinal
        record_ordinal: int | None = (
            ready_ordinal
            if step.route in (DescriptorRoute.READY, DescriptorRoute.RELEASE_AFTER_LOSS)
            else None
        )
        insert_index = append_planned(
            record_value,
            record_metadata,
            record_ordinal,
        )
        if record_ordinal is None:
            if (
                record.room_id is None
                or step.membership_epoch is None
                or step.route
                not in (
                    DescriptorRoute.HOLD_FOR_GAP,
                    DescriptorRoute.HOLD_FOR_HYDRATION,
                )
            ):
                raise ValueError("prepared HELD route is invalid")
            buffered[(record.room_id, step.membership_epoch)].append(insert_index)
        else:
            ready_ordinal += 1

    preliminary_sizes = {
        _work_id(planned_item.value): stored_size(
            planned_item.value,
            planned_item.plaintext,
            planned_item.ready_ordinal,
        )
        for planned_item in inserts
    }
    capacity_reasons: dict[str, LossReason] = {}
    for candidate_insert in inserts:
        work_id = _work_id(candidate_insert.value)
        if (
            work_id not in source_work_ids
            or preliminary_sizes[work_id]
            + (
                _HELD_PROMOTION_RESERVE_BYTES
                if candidate_insert.ready_ordinal is None
                else 0
            )
            <= limits.max_record_canonical_bytes
        ):
            continue
        candidate_value = candidate_insert.value
        assert type(candidate_value) is EventRecord
        if candidate_value.room_id is None:
            raise JournalCapacityError(
                "planned Work record exceeds the canonical byte limit"
            )
        capacity_reasons[candidate_value.room_id] = LossReason.OVERSIZED_EVENT

    preliminary_remaining_held = tuple(
        existing_item
        for existing_item in work
        if existing_item.status == "held"
        and _work_id(existing_item.value) not in released_existing_ids
    )
    preliminary_planned_held = tuple(
        planned_item for planned_item in inserts if planned_item.ready_ordinal is None
    )
    if not mandatory_work_ids:
        existing_held_by_room: dict[str, list[AuthenticatedWork]] = defaultdict(list)
        for existing_held_item in preliminary_remaining_held:
            existing_held_value = existing_held_item.value
            if (
                type(existing_held_value) is EventRecord
                and existing_held_value.room_id is not None
            ):
                existing_held_by_room[existing_held_value.room_id].append(
                    existing_held_item
                )
        planned_held_by_room: dict[str, list[PlannedWork]] = defaultdict(list)
        for planned_held_item in preliminary_planned_held:
            planned_held_value = planned_held_item.value
            if (
                type(planned_held_value) is EventRecord
                and planned_held_value.room_id in room_result_by_id
                and _work_id(planned_held_value) in source_work_ids
            ):
                assert planned_held_value.room_id is not None
                planned_held_by_room[planned_held_value.room_id].append(
                    planned_held_item
                )

        admitted_count = len(preliminary_remaining_held)
        admitted_bytes = sum(
            existing_item.canonical_size for existing_item in preliminary_remaining_held
        )
        for room_id in capacity_reasons:
            admitted_count -= len(existing_held_by_room[room_id])
            admitted_bytes -= sum(
                existing_item.canonical_size
                for existing_item in existing_held_by_room[room_id]
            )
        for room_id, room_items in planned_held_by_room.items():
            if room_id in capacity_reasons:
                continue
            addition_count = len(room_items)
            addition_bytes = sum(
                preliminary_sizes[_work_id(room_item.value)] for room_item in room_items
            )
            if (
                admitted_count + addition_count > limits.max_held_work_count
                or admitted_bytes + addition_bytes
                > limits.max_held_work_canonical_bytes
            ):
                capacity_reasons[room_id] = LossReason.EVENT_LIMIT
                admitted_count -= len(existing_held_by_room[room_id])
                admitted_bytes -= sum(
                    existing_item.canonical_size
                    for existing_item in existing_held_by_room[room_id]
                )
            else:
                admitted_count += addition_count
                admitted_bytes += addition_bytes
        if (
            admitted_count > limits.max_held_work_count
            or admitted_bytes > limits.max_held_work_canonical_bytes
        ):
            raise JournalCapacityError("planned HELD Work exceeds global HELD capacity")

    if capacity_reasons:
        original_insert_positions = {
            _work_id(positioned_insert.value): index
            for index, positioned_insert in enumerate(inserts)
        }
        terminal_specs: list[
            tuple[
                int,
                int,
                PlannedWork,
                tuple[PlannedWork, ...],
            ]
        ] = []
        dropped_source_ids: set[str] = set()
        for room_id, reason in sorted(
            capacity_reasons.items(),
            key=lambda pair: min(
                original_insert_positions[_work_id(positioned_insert.value)]
                for positioned_insert in inserts
                if type(positioned_insert.value) is EventRecord
                and positioned_insert.value.room_id == pair[0]
                and _work_id(positioned_insert.value) in source_work_ids
            ),
        ):
            room_sources = tuple(
                room_source
                for room_source in inserts
                if type(room_source.value) is EventRecord
                and room_source.value.room_id == room_id
                and _work_id(room_source.value) in source_work_ids
            )
            if not room_sources:
                raise ValueError("capacity room has no prepared source Work")
            room_order = min(
                original_insert_positions[_work_id(room_source.value)]
                for room_source in room_sources
            )
            anchor = max(
                (
                    room_source.ready_ordinal
                    if room_source.ready_ordinal is not None
                    else source_ready_anchors[_work_id(room_source.value)]
                )
                for room_source in room_sources
            )
            dropped_source_ids.update(
                _work_id(room_source.value) for room_source in room_sources
            )

            capacity_result = room_result_by_id[room_id]
            room_result_by_id[room_id] = replace(
                capacity_result,
                after=replace(capacity_result.after, gap=None, hydration_id=None),
                recovery=None,
                hydration=None,
                retirement_epoch=None,
                losses=(),
                release=RecoveryRelease.NONE,
            )
            pending_hydrations[room_id] = None

            loss = LossRecord(
                "",
                frame.origin,
                room_id,
                capacity_result.after.membership_epoch,
                reason,
                LossBoundary(None, None, None, None),
                b"{}",
            )
            loss = replace(loss, loss_id=_loss_id(stream_id, loss))
            loss_id = _work_id(loss)
            if loss_id in used_ids:
                raise ValueError("planned Work identity collides")
            used_ids.add(loss_id)
            mandatory_work_ids.add(loss_id)
            loss_plaintext = _canonical_work_plaintext("loss", loss)
            loss_item = PlannedWork(loss, loss_plaintext, anchor)
            if (
                stored_size(loss, loss_plaintext, anchor)
                > hard_limits.max_record_canonical_bytes
            ):
                raise JournalCapacityError(
                    "planned Work record exceeds the immutable byte limit"
                )

            terminal_releases: list[PlannedWork] = []

            def release_candidate_key(
                candidate: AuthenticatedWork,
            ) -> tuple[int, str]:
                candidate_value = candidate.value
                if (
                    type(candidate_value) is not EventRecord
                    or candidate_value.room_sequence is None
                ):
                    raise ValueError("capacity release candidate is invalid")
                return candidate_value.room_sequence, candidate_value.record_id

            release_candidates = sorted(
                (
                    release_candidate
                    for release_candidate in work
                    if release_candidate.status == "held"
                    and type(release_candidate.value) is EventRecord
                    and release_candidate.value.room_id == room_id
                    and _work_id(release_candidate.value) not in released_existing_ids
                ),
                key=release_candidate_key,
            )
            for offset, release_candidate in enumerate(release_candidates, 1):
                if (
                    release_candidate.frame_id is None
                    or release_candidate.created_revision is None
                ):
                    raise ValueError(
                        "prepared HELD release lacks authenticated row identity"
                    )
                released_existing_ids.add(_work_id(release_candidate.value))
                terminal_releases.append(
                    PlannedWork(
                        release_candidate.value,
                        release_candidate.plaintext
                        or _canonical_work_plaintext("event", release_candidate.value),
                        anchor + offset,
                        release_candidate.metadata,
                    )
                )
            terminal_specs.append(
                (anchor, room_order, loss_item, tuple(terminal_releases))
            )

        source_work_ids.difference_update(dropped_source_ids)
        inserts = [
            retained_insert
            for retained_insert in inserts
            if _work_id(retained_insert.value) not in dropped_source_ids
        ]

        def room_item_key(
            pair: tuple[int, PlannedWork],
        ) -> tuple[int, str]:
            pair_value = pair[1].value
            if type(pair_value) is not EventRecord or pair_value.room_sequence is None:
                raise ValueError("capacity room Work sequence is invalid")
            return pair_value.room_sequence, pair_value.record_id

        for room_id in capacity_reasons:
            aggregate = aggregate_by_room.get(room_id)
            next_sequence = aggregate.next_room_sequence if aggregate is not None else 0
            sequenced_room_items = sorted(
                (
                    (index, room_insert)
                    for index, room_insert in enumerate(inserts)
                    if type(room_insert.value) is EventRecord
                    and room_insert.value.room_id == room_id
                ),
                key=room_item_key,
            )
            for index, room_item in sequenced_room_items:
                room_value = room_item.value
                assert type(room_value) is EventRecord
                room_value = replace(room_value, room_sequence=next_sequence)
                next_sequence += 1
                inserts[index] = PlannedWork(
                    room_value,
                    _canonical_work_plaintext("event", room_value, room_item.metadata),
                    room_item.ready_ordinal,
                    room_item.metadata,
                )
            room_sequences[room_id] = next_sequence

        ordered: list[
            tuple[tuple[int, int, int, int], Literal["insert", "release"], PlannedWork]
        ] = []
        for index, ordered_insert in enumerate(inserts):
            if ordered_insert.ready_ordinal is not None:
                ordered.append(
                    (
                        (ordered_insert.ready_ordinal, 1, index, 0),
                        "insert",
                        ordered_insert,
                    )
                )
        for index, ordered_release in enumerate(releases):
            assert ordered_release.ready_ordinal is not None
            ordered.append(
                (
                    (ordered_release.ready_ordinal, 1, index, 1),
                    "release",
                    ordered_release,
                )
            )
        for (
            anchor,
            room_order,
            terminal_loss,
            terminal_release_tuple,
        ) in terminal_specs:
            ordered.append(((anchor, 0, room_order, 0), "insert", terminal_loss))
            for offset, terminal_release in enumerate(terminal_release_tuple, 1):
                ordered.append(
                    (
                        (anchor, 0, room_order, offset),
                        "release",
                        terminal_release,
                    )
                )
        ready_inserts: list[PlannedWork] = []
        releases = []
        for ordinal, (_key, owner, ordered_item) in enumerate(sorted(ordered)):
            reordered_item = ordered_item._replace(ready_ordinal=ordinal)
            if owner == "insert":
                ready_inserts.append(reordered_item)
            else:
                releases.append(reordered_item)
        held_inserts = [
            held_insert for held_insert in inserts if held_insert.ready_ordinal is None
        ]
        inserts = ready_inserts + held_inserts
        buffered = defaultdict(list)
        for index, buffered_insert in enumerate(inserts):
            buffered_value = buffered_insert.value
            if buffered_insert.ready_ordinal is None:
                assert type(buffered_value) is EventRecord
                assert buffered_value.room_id is not None
                assert buffered_value.membership_epoch is not None
                buffered[
                    (buffered_value.room_id, buffered_value.membership_epoch)
                ].append(index)

    release_sizes: dict[str, int] = {}
    for final_release in releases:
        final_release_value = final_release.value
        if type(final_release_value) is not EventRecord:
            raise ValueError("prepared release is not Event Work")
        existing_release = existing_by_id[_work_id(final_release_value)]
        assert existing_release.frame_id is not None
        assert existing_release.created_revision is not None
        release_size = _stored_work_size(
            (account_id, stream_id, frame.origin.transport),
            _stored_work_release_row(final_release, existing_release, revision),
        )
        if release_size > hard_limits.max_record_canonical_bytes:
            raise JournalCapacityError(
                "released Work record exceeds the immutable byte limit"
            )
        release_sizes[_work_id(final_release_value)] = release_size

    planned_sizes = {
        _work_id(final_insert.value): stored_size(
            final_insert.value,
            final_insert.plaintext,
            final_insert.ready_ordinal,
        )
        for final_insert in inserts
    }
    if any(
        planned_sizes[_work_id(item.value)]
        + (_HELD_PROMOTION_RESERVE_BYTES if item.ready_ordinal is None else 0)
        > hard_limits.max_record_canonical_bytes
        for item in inserts
    ):
        raise JournalCapacityError(
            "final planned Work exceeds the immutable record limit"
        )
    final_limits = hard_limits if mandatory_work_ids or releases else limits
    remaining_existing_held = tuple(
        held_item
        for held_item in work
        if held_item.status == "held"
        and _work_id(held_item.value) not in released_existing_ids
    )
    planned_held = tuple(
        held_insert for held_insert in inserts if held_insert.ready_ordinal is None
    )

    def validate_final_held(final_held_value: EventRecord | LossRecord) -> None:
        if (
            type(final_held_value) is not EventRecord
            or final_held_value.room_id not in room_result_by_id
        ):
            return
        assert final_held_value.room_id is not None
        after = room_result_by_id[final_held_value.room_id].after
        if (
            after.gap is None
            and after.hydration_id is None
            or final_held_value.membership_epoch != after.membership_epoch
            or final_held_value.room_sequence is None
            or final_held_value.room_sequence
            >= room_sequences[final_held_value.room_id]
        ):
            raise ValueError("selected HELD Work has no final Aggregate barrier")

    for existing_held_item in remaining_existing_held:
        validate_final_held(existing_held_item.value)
    for planned_held_item in planned_held:
        validate_final_held(planned_held_item.value)
    buffered_indexes = {index for indexes in buffered.values() for index in indexes}
    planned_held_indexes = {
        index
        for index, held_insert in enumerate(inserts)
        if held_insert.ready_ordinal is None
    }
    if buffered_indexes != planned_held_indexes:
        raise ValueError("prepared HELD Work buffering is inconsistent")
    held_count = len(remaining_existing_held) + len(planned_held)
    held_bytes = sum(
        held_item.canonical_size for held_item in remaining_existing_held
    ) + sum(planned_sizes[_work_id(held_insert.value)] for held_insert in planned_held)
    if held_count > final_limits.max_held_work_count or (
        held_bytes > final_limits.max_held_work_canonical_bytes
    ):
        suffix = "immutable envelope" if final_limits is hard_limits else "capacity"
        raise JournalCapacityError(f"planned HELD Work exceeds global HELD {suffix}")
    total_count = len(work) + len(inserts)
    total_bytes = (
        sum(work_item.canonical_size for work_item in work)
        - sum(
            existing_by_id[work_id].canonical_size for work_id in released_existing_ids
        )
        + sum(release_sizes.values())
        + sum(planned_sizes.values())
        + held_count * _HELD_PROMOTION_RESERVE_BYTES
    )
    if total_count > final_limits.max_total_work_count or (
        total_bytes > final_limits.max_total_work_canonical_bytes
    ):
        if final_limits is hard_limits:
            raise JournalCapacityError(
                "mandatory Work exceeds the immutable total envelope"
            )
        raise JournalCapacityError("planned Work exceeds global total capacity")

    room_values_list: list[RoomAggregateValue] = []
    for room_id, result in room_result_by_id.items():
        stored = aggregate_by_room.get(room_id)
        room_snapshot: RoomSnapshot | None = snapshot_by_room.get(room_id)
        if room_snapshot is None and stored is not None:
            room_snapshot = stored.room_snapshot
        if (
            stored is None
            or result.after != stored.continuity
            or room_sequences[room_id] != stored.next_room_sequence
            or pending_hydrations[room_id] != stored.pending_hydration
            or room_snapshot != stored.room_snapshot
        ):
            room_values_list.append(
                RoomAggregateValue(
                    result.after,
                    room_sequences[room_id],
                    revision,
                    pending_hydrations[room_id],
                    room_snapshot,
                )
            )
    room_values = tuple(room_values_list)
    return MaterializationPlan(
        room_values,
        tuple(inserts),
        tuple(releases),
        proposal.crypto_deferred,
    )
