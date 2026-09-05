"""Pure proposals derived from source-certified staged sync frames."""

from collections import defaultdict
from dataclasses import dataclass, fields, replace
from enum import StrEnum
from types import UnionType
from typing import get_args, get_origin
from uuid import UUID, uuid5

from ..event_provenance import TimelineEventProvenance
from ._json import canonical_json, load_json
from .model import (
    LossBoundary,
    LossReason,
    RecordKind,
    RecordOrigin,
    RoomSnapshot,
    TransportKind,
    _CallbackRoute,
    _DecryptedToDeviceKind,
    _DecryptionDisposition,
    _MembershipProvenance,
    _MembershipSourceKind,
    _PreparationPhase,
    _PreparedCryptoDelta,
    _PreparedIngestionFrame,
    _PreparedIngestionRecord,
    _PreparedKeyClaim,
    _PreparedMegolmRerequest,
    _PreparedMembershipTransition,
    _PreparedQueuedToDeviceMessage,
    _PreparedWaitingKeyRequest,
    _QueuedToDeviceSubtype,
)
from .recovery import recovery_progress
from .source import (
    RoomSegment,
    SyncFrame,
    _classic_cursor_from_json,
    _continuity_bounds,
    _normalized_ephemeral_envelopes,
)


def _matches_exact_shape(value: object, shape: object) -> bool:
    origin = get_origin(shape)
    if origin is UnionType:
        return any(_matches_exact_shape(value, member) for member in get_args(shape))
    if origin is tuple:
        item_shape, ellipsis = get_args(shape)
        return (
            ellipsis is Ellipsis
            and type(value) is tuple
            and all(_matches_exact_shape(item, item_shape) for item in value)
        )
    return type(value) is shape


def _exact(value: object, expected: type, name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{name} must be {expected.__name__}")


class _ValidatedValue:
    __slots__ = ()

    def __post_init__(self) -> None:
        for field in fields(self):  # type: ignore[arg-type]
            value = getattr(self, field.name)
            if not _matches_exact_shape(value, field.type):
                raise TypeError(f"{field.name} has an invalid type")
            if type(value) is str and not value:
                raise ValueError(f"{field.name} must not be empty")
            if type(value) is int and value < 0:
                raise ValueError(f"{field.name} must be nonnegative")


class DescriptorRoute(StrEnum):
    READY = "ready"
    HOLD_FOR_GAP = "hold_for_gap"
    HOLD_FOR_HYDRATION = "hold_for_hydration"
    HOLD_FOR_RETIREMENT = "hold_for_retirement"
    RELEASE_AFTER_LOSS = "release_after_loss"


class RecoveryRelease(StrEnum):
    NONE = "none"
    LOSS_THEN_HELD = "loss_then_held"


class ReducerInputError(ValueError):
    """Raised before a proposal when immutable reducer inputs disagree."""


@dataclass(frozen=True, slots=True)
class MembershipBaseline(_ValidatedValue):
    membership_event_id: str | None
    window_token: str | None


@dataclass(frozen=True, slots=True)
class RecoveryGap(_ValidatedValue):
    gap_id: UUID
    room_id: str
    membership_epoch: int
    origin: RecordOrigin
    start_token: str
    target_token: str


@dataclass(frozen=True, slots=True)
class RoomContinuity(_ValidatedValue):
    room_id: str
    membership_epoch: int
    membership: str | None
    baseline: MembershipBaseline | None
    gap: RecoveryGap | None
    hydration_id: UUID | None
    last_timeline_event_id: str | None = None

    def __post_init__(self) -> None:
        _ValidatedValue.__post_init__(self)
        if self.gap is not None and (
            self.gap.room_id != self.room_id
            or self.gap.membership_epoch != self.membership_epoch
        ):
            raise ValueError("gap must belong to this room epoch")
        if self.gap is not None and self.hydration_id is not None:
            raise ValueError("gap and hydration_id are mutually exclusive")


@dataclass(frozen=True, slots=True)
class LossProposal(_ValidatedValue):
    room_id: str
    membership_epoch: int
    reason: LossReason
    boundary: LossBoundary


@dataclass(frozen=True, slots=True)
class HydrationIntent(_ValidatedValue):
    hydration_id: UUID
    origin: RecordOrigin


@dataclass(frozen=True, slots=True)
class RoomProposal(_ValidatedValue):
    before: RoomContinuity | None
    after: RoomContinuity
    recovery: RecoveryGap | None
    hydration: HydrationIntent | None
    retirement_epoch: int | None
    losses: tuple[LossProposal, ...]
    release: RecoveryRelease


def _membership(segment: RoomSegment) -> str | None:
    observation = segment.membership_observation
    if observation.is_unparsed:
        return observation.room_membership
    return observation.event_membership or observation.room_membership


def _has_trusted_baseline(state: RoomContinuity, segment: RoomSegment) -> bool:
    baseline = state.baseline
    if (
        state.membership is None
        or baseline is None
        or baseline.membership_event_id is None
    ):
        return False
    observation = segment.membership_observation
    if observation.is_unparsed:
        return False
    if observation.event_membership is None:
        return not observation.is_initial and not observation.is_expanded_timeline
    return (
        observation.event_id == baseline.membership_event_id
        or observation.is_live
        or (
            observation.previous_membership == state.membership
            and observation.replaces_state == baseline.membership_event_id
        )
    )


def _recovered_membership_baseline(
    frame: SyncFrame, segment: RoomSegment, before: RoomContinuity
) -> MembershipBaseline | None:
    observation = segment.membership_observation
    if (
        frame.origin.transport is not TransportKind.CLASSIC
        or before.baseline is None
        or before.baseline.membership_event_id is None
        or before.gap is not None
        or before.hydration_id is not None
        or not segment.recovered_event_count
        or segment.recovered_event_count != len(segment.timeline_json)
        or observation.is_unparsed
        or observation.event_membership is None
        or not observation.event_id
    ):
        return None
    progress = recovery_progress(frame.request_cursor_json)
    if (
        progress is None
        or progress["phase"] != "page"
        or segment.room_id not in progress["rooms"]
    ):
        return None
    return MembershipBaseline(
        observation.event_id,
        _classic_cursor_from_json(frame.request_cursor_json).next_batch,
    )


def _loss(
    state: RoomContinuity,
    reason: LossReason,
    start_token: str | None = None,
    target_token: str | None = None,
) -> LossProposal:
    return LossProposal(
        state.room_id,
        state.membership_epoch,
        reason,
        LossBoundary(None, None, start_token, target_token),
    )


def _timeline_event_ids(segment: RoomSegment) -> tuple[str | None, ...]:
    event_ids: list[str | None] = []
    for payload in segment.timeline_json:
        try:
            event = load_json(payload, "timeline event")
            if type(event) is not dict or canonical_json(event) != payload:
                raise ValueError("timeline event is not a canonical object")
        except (TypeError, ValueError) as error:
            raise ReducerInputError("invalid canonical timeline event") from error
        event_id = event.get("event_id")
        event_ids.append(event_id if type(event_id) is str and event_id else None)
    return tuple(event_ids)


def _restart_recovery_start(
    frame: SyncFrame,
    segment: RoomSegment,
    before: RoomContinuity | None,
    event_ids: tuple[str | None, ...],
) -> int | None:
    if (
        before is None
        or before.last_timeline_event_id is None
        or before.gap is not None
        or before.hydration_id is not None
        or not _has_trusted_baseline(before, segment)
        or frame.origin.transport is not TransportKind.SLIDING
        or frame.origin.source_epoch == 0
        or not segment.initial
        or segment.expanded_timeline
    ):
        return None
    matches = tuple(
        index
        for index, event_id in enumerate(event_ids)
        if event_id == before.last_timeline_event_id
    )
    return matches[0] + 1 if len(matches) == 1 else None


def _timeline_provenance(
    index: int,
    history_count: int,
    recovery_start: int | None,
    recovered_event_count: int = 0,
) -> TimelineEventProvenance:
    if index < recovered_event_count:
        return TimelineEventProvenance.RECOVERED
    if index >= history_count:
        return TimelineEventProvenance.LIVE
    if recovery_start is not None and index >= recovery_start:
        return TimelineEventProvenance.RECOVERED
    return TimelineEventProvenance.HISTORY


def _next_timeline_event_id(
    before: RoomContinuity | None,
    after: RoomContinuity,
    segment: RoomSegment,
    event_ids: tuple[str | None, ...],
) -> str | None:
    previous = (
        before.last_timeline_event_id
        if before is not None
        and (before.membership_epoch, before.membership)
        == (after.membership_epoch, after.membership)
        else None
    )
    if not event_ids or (segment.expanded_timeline and not segment.live_event_count):
        return previous
    return event_ids[-1]


def _plan_room(
    stream_id: UUID,
    frame: SyncFrame,
    segment: RoomSegment,
    before: RoomContinuity | None,
    *,
    verified_restart_boundary: bool = False,
) -> tuple[RoomProposal, DescriptorRoute]:
    claim = _membership(segment)
    if before is None:
        epoch = 0
        membership = claim
    else:
        epoch = before.membership_epoch
        membership = claim if claim is not None else before.membership

        if before.gap is not None and segment.membership_observation.is_unparsed:
            hydration_id = uuid5(
                stream_id,
                f"hydrate:{segment.room_id}:{epoch}:{frame.frame_id}",
            )
            after = RoomContinuity(
                segment.room_id, epoch, membership, None, None, hydration_id
            )
            gap = before.gap
            return (
                RoomProposal(
                    before,
                    after,
                    None,
                    HydrationIntent(hydration_id, frame.origin),
                    None,
                    (
                        _loss(
                            before,
                            LossReason.BASELINE_LOST,
                            gap.start_token,
                            gap.target_token,
                        ),
                    ),
                    RecoveryRelease.LOSS_THEN_HELD,
                ),
                DescriptorRoute.HOLD_FOR_HYDRATION,
            )

        if (
            claim is not None
            and before.membership is not None
            and claim != before.membership
        ):
            after = RoomContinuity(segment.room_id, epoch + 1, claim, None, None, None)
            losses: tuple[LossProposal, ...] = ()
            release = RecoveryRelease.NONE
            if before.gap is not None:
                gap = before.gap
                losses = (
                    _loss(
                        before,
                        LossReason.BASELINE_LOST,
                        gap.start_token,
                        gap.target_token,
                    ),
                )
                release = RecoveryRelease.LOSS_THEN_HELD
            elif before.hydration_id is not None:
                losses = (_loss(before, LossReason.UNVERIFIABLE),)
                release = RecoveryRelease.LOSS_THEN_HELD
            return (
                RoomProposal(before, after, None, None, epoch, losses, release),
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            )

        if before.gap is not None:
            return (
                RoomProposal(
                    before,
                    before,
                    None,
                    None,
                    None,
                    (),
                    RecoveryRelease.NONE,
                ),
                DescriptorRoute.HOLD_FOR_GAP,
            )

        if before.hydration_id is not None:
            after = RoomContinuity(
                segment.room_id,
                epoch,
                membership,
                before.baseline,
                None,
                before.hydration_id,
            )
            return (
                RoomProposal(
                    before,
                    after,
                    None,
                    HydrationIntent(before.hydration_id, frame.origin),
                    None,
                    (),
                    RecoveryRelease.NONE,
                ),
                DescriptorRoute.HOLD_FOR_HYDRATION,
            )

        if (
            _has_trusted_baseline(before, segment)
            or _recovered_membership_baseline(frame, segment, before) is not None
        ):
            assert before.baseline is not None
            observation = segment.membership_observation
            event_id = (
                observation.event_id
                if not observation.is_unparsed and observation.event_id is not None
                else before.baseline.membership_event_id
            )
            baseline = MembershipBaseline(
                event_id,
                segment.timeline_prev_batch or before.baseline.window_token,
            )
            if segment.history_discontinuity and not verified_restart_boundary:
                bounds = _continuity_bounds(
                    frame, segment, before.baseline.window_token
                )
                if bounds is not None:
                    start_token, target_token = bounds
                    gap = RecoveryGap(
                        uuid5(
                            stream_id,
                            f"gap:{segment.room_id}:{epoch}:{frame.frame_id}:"
                            f"{start_token}:{target_token}",
                        ),
                        segment.room_id,
                        epoch,
                        frame.origin,
                        start_token,
                        target_token,
                    )
                    after = RoomContinuity(
                        segment.room_id, epoch, membership, baseline, gap, None
                    )
                    return (
                        RoomProposal(
                            before,
                            after,
                            gap,
                            None,
                            None,
                            (),
                            RecoveryRelease.NONE,
                        ),
                        DescriptorRoute.HOLD_FOR_GAP,
                    )
                after = RoomContinuity(
                    segment.room_id, epoch, membership, baseline, None, None
                )
                return (
                    RoomProposal(
                        before,
                        after,
                        None,
                        None,
                        None,
                        (_loss(before, LossReason.UNVERIFIABLE),),
                        RecoveryRelease.LOSS_THEN_HELD,
                    ),
                    DescriptorRoute.RELEASE_AFTER_LOSS,
                )
            after = RoomContinuity(
                segment.room_id, epoch, membership, baseline, None, None
            )
            return (
                RoomProposal(
                    before,
                    after,
                    None,
                    None,
                    None,
                    (),
                    RecoveryRelease.NONE,
                ),
                DescriptorRoute.READY,
            )

    if membership != "join":
        after = RoomContinuity(segment.room_id, epoch, membership, None, None, None)
        return (
            RoomProposal(
                before,
                after,
                None,
                None,
                None,
                (),
                RecoveryRelease.NONE,
            ),
            DescriptorRoute.READY,
        )

    hydration_id = uuid5(
        stream_id,
        f"hydrate:{segment.room_id}:{epoch}:{frame.frame_id}",
    )
    after = RoomContinuity(segment.room_id, epoch, membership, None, None, hydration_id)
    return (
        RoomProposal(
            before,
            after,
            None,
            HydrationIntent(hydration_id, frame.origin),
            None,
            (),
            RecoveryRelease.NONE,
        ),
        DescriptorRoute.HOLD_FOR_HYDRATION,
    )


@dataclass(frozen=True, slots=True)
class PreparedRecordStep:
    record: _PreparedIngestionRecord
    route: DescriptorRoute
    membership_epoch: int | None

    def __post_init__(self) -> None:
        _exact(self.record, _PreparedIngestionRecord, "record")
        _exact(self.route, DescriptorRoute, "route")
        if self.membership_epoch is not None:
            _exact(self.membership_epoch, int, "membership_epoch")
            if self.membership_epoch < 0:
                raise ValueError("membership_epoch must be nonnegative")


@dataclass(frozen=True, slots=True)
class PreparedTransitionStep:
    transition: _PreparedMembershipTransition
    membership_epoch: int
    loss: LossProposal | None
    release_epoch: int | None

    def __post_init__(self) -> None:
        _exact(self.transition, _PreparedMembershipTransition, "transition")
        _exact(self.membership_epoch, int, "membership_epoch")
        if self.loss is not None:
            _exact(self.loss, LossProposal, "loss")
        if self.release_epoch is not None:
            _exact(self.release_epoch, int, "release_epoch")
        if self.membership_epoch < 0 or (
            self.release_epoch is not None and self.release_epoch < 0
        ):
            raise ValueError("prepared transition epochs must be nonnegative")
        if (self.loss is None) != (self.release_epoch is None):
            raise ValueError("loss and release_epoch must be present together")


@dataclass(frozen=True, slots=True)
class PreparedRecoveryStep:
    room_id: str
    membership_epoch: int
    losses: tuple[LossProposal, ...]

    def __post_init__(self) -> None:
        _exact(self.room_id, str, "room_id")
        _exact(self.membership_epoch, int, "membership_epoch")
        _exact(self.losses, tuple, "losses")
        if not self.room_id or self.membership_epoch < 0 or not self.losses:
            raise ValueError("prepared recovery step is invalid")
        if any(
            type(loss) is not LossProposal
            or loss.room_id != self.room_id
            or loss.membership_epoch != self.membership_epoch
            for loss in self.losses
        ):
            raise ValueError("prepared recovery losses are invalid")


@dataclass(frozen=True, slots=True)
class PreparedFrameProposal:
    frame_id: UUID
    source_sha256: bytes
    room_results: tuple[RoomProposal, ...]
    linear_steps: tuple[
        PreparedRecordStep | PreparedTransitionStep | PreparedRecoveryStep, ...
    ]
    crypto_deferred: bool

    def __post_init__(self) -> None:
        _exact(self.frame_id, UUID, "frame_id")
        _exact(self.source_sha256, bytes, "source_sha256")
        _exact(self.room_results, tuple, "room_results")
        _exact(self.linear_steps, tuple, "linear_steps")
        _exact(self.crypto_deferred, bool, "crypto_deferred")
        if len(self.source_sha256) != 32:
            raise ValueError("source_sha256 must be exactly 32 bytes")
        if any(type(result) is not RoomProposal for result in self.room_results):
            raise TypeError("room_results must contain RoomProposal values")
        if any(
            type(step)
            not in (PreparedRecordStep, PreparedTransitionStep, PreparedRecoveryStep)
            for step in self.linear_steps
        ):
            raise TypeError("linear_steps contain an invalid value")


type _PreparedSourceRecord = tuple[
    RecordKind,
    str | None,
    bytes,
    TimelineEventProvenance | None,
]


def _prepared_source_records(
    frame: SyncFrame,
) -> tuple[tuple[_PreparedSourceRecord, ...], frozenset[tuple[str, int]]]:
    ephemeral_by_room: dict[str, list[bytes]] = defaultdict(list)
    for room_id, source_json in _normalized_ephemeral_envelopes(frame.ephemeral_json):
        ephemeral_by_room[room_id].append(source_json)

    records: list[_PreparedSourceRecord] = [
        (RecordKind.TO_DEVICE, None, source_json, None)
        for source_json in frame.to_device_json
    ]
    section_boundaries: set[tuple[str, int]] = set()
    for segment in frame.room_segments:
        section_boundaries.add((segment.room_id, len(records)))
        records.extend(
            (RecordKind.STATE, segment.room_id, source_json, None)
            for source_json in segment.state_json
        )
        history_count = len(segment.timeline_json) - segment.live_event_count
        records.extend(
            (
                RecordKind.TIMELINE,
                segment.room_id,
                source_json,
                (
                    TimelineEventProvenance.RECOVERED
                    if index < segment.recovered_event_count
                    else (
                        TimelineEventProvenance.HISTORY
                        if index < history_count
                        else TimelineEventProvenance.LIVE
                    )
                ),
            )
            for index, source_json in enumerate(segment.timeline_json)
        )
        records.extend(
            (RecordKind.EPHEMERAL, segment.room_id, source_json, None)
            for source_json in ephemeral_by_room.pop(segment.room_id, ())
        )
        records.extend(
            (RecordKind.ROOM_ACCOUNT_DATA, segment.room_id, source_json, None)
            for source_json in segment.room_account_data_json
        )
    for room_id, events in ephemeral_by_room.items():
        records.extend(
            (RecordKind.EPHEMERAL, room_id, source_json, None) for source_json in events
        )
    records.extend(
        (RecordKind.GLOBAL_ACCOUNT_DATA, None, source_json, None)
        for source_json in frame.global_account_data_json
    )
    return tuple(records), frozenset(section_boundaries)


def _canonical_prepared_object(source_json: bytes, label: str) -> dict[str, object]:
    try:
        value = load_json(source_json, label)
        if type(value) is not dict or canonical_json(value) != source_json:
            raise ValueError
        return value
    except (TypeError, ValueError) as error:
        raise ReducerInputError(f"{label} is not a canonical object") from error


def _validate_prepared_record_shape(record: _PreparedIngestionRecord) -> None:
    if type(record) is not _PreparedIngestionRecord:
        raise ReducerInputError("prepared record carrier is invalid")
    if (
        type(record.record_id) is not str
        or not record.record_id
        or type(record.kind) is not RecordKind
        or type(record.origin) is not RecordOrigin
        or type(record.preparation_phase) is not _PreparationPhase
        or type(record.effective_event_type) is not str
        or not record.effective_event_type
        or record.room_id is not None
        and type(record.room_id) is not str
        or record.event_id is not None
        and type(record.event_id) is not str
        or record.provenance is not None
        and type(record.provenance) is not TimelineEventProvenance
        or type(record.source_json) is not bytes
        or record.clear_json is not None
        and type(record.clear_json) is not bytes
        or type(record.decryption) is not _DecryptionDisposition
        or record.decryption_verified is not None
        and type(record.decryption_verified) is not bool
        or record.decrypted_to_device_kind is not None
        and type(record.decrypted_to_device_kind) is not _DecryptedToDeviceKind
        or record.callback_route is not None
        and type(record.callback_route) is not _CallbackRoute
    ):
        raise ReducerInputError("prepared record carrier is invalid")


def _validate_prepared_record(
    frame: SyncFrame,
    record: _PreparedIngestionRecord,
    index: int,
    phase_index: int,
) -> None:
    expected_id = str(
        uuid5(frame.frame_id, f"record:{record.preparation_phase.value}:{phase_index}")
    )
    if record.record_id != expected_id:
        raise ReducerInputError("prepared record identity is invalid")
    if record.origin != replace(frame.origin, frame_index=index):
        raise ReducerInputError("prepared record origin is invalid")
    if record.kind is RecordKind.ROOM_LIFECYCLE:
        raise ReducerInputError("Task4C records cannot contain room lifecycle Work")
    if record.preparation_phase is not _PreparationPhase.SOURCE:
        expected_type = {
            _PreparationPhase.EXPIRED_VERIFICATION: "m.key.verification.cancel",
            _PreparationPhase.COLLECTED_KEY_REQUEST: "m.room_key_request",
        }[record.preparation_phase]
        if (
            record.kind is not RecordKind.TO_DEVICE
            or record.callback_route is not _CallbackRoute.TO_DEVICE
            or record.effective_event_type != expected_type
        ):
            raise ReducerInputError("prepared synthetic event type or route is invalid")
    room_kind = record.kind in {
        RecordKind.TIMELINE,
        RecordKind.STATE,
        RecordKind.EPHEMERAL,
        RecordKind.ROOM_ACCOUNT_DATA,
    }
    if room_kind != (record.room_id is not None):
        raise ReducerInputError("prepared record room ownership is invalid")
    if (record.kind is RecordKind.TIMELINE) != (record.provenance is not None):
        raise ReducerInputError("prepared record timeline provenance is invalid")

    source = _canonical_prepared_object(record.source_json, "prepared source event")
    clear = (
        _canonical_prepared_object(record.clear_json, "prepared clear event")
        if record.clear_json is not None
        else None
    )
    effective = clear if clear is not None else source
    visible_type = effective.get("type")
    if visible_type is None:
        if not (
            record.preparation_phase is _PreparationPhase.EXPIRED_VERIFICATION
            and record.effective_event_type == "m.key.verification.cancel"
        ):
            raise ReducerInputError("prepared source event type is unavailable")
    elif type(visible_type) is not str or visible_type != record.effective_event_type:
        raise ReducerInputError("prepared effective event type is invalid")
    event_id = source.get("event_id")
    if event_id is not None and type(event_id) is not str:
        raise ReducerInputError("prepared event_id is invalid")
    if record.event_id != event_id:
        raise ReducerInputError("prepared event_id disagrees with source JSON")

    if record.decryption is _DecryptionDisposition.DECRYPTED:
        if clear is None or record.kind not in (
            RecordKind.TIMELINE,
            RecordKind.TO_DEVICE,
        ):
            raise ReducerInputError("decrypted prepared record has no clear JSON")
    elif clear is not None:
        raise ReducerInputError("non-decrypted prepared record has clear JSON")
    if record.decryption is _DecryptionDisposition.MEGOLM_FAILED and (
        record.kind is not RecordKind.TIMELINE
    ):
        raise ReducerInputError("Megolm failure must be a timeline record")
    if record.decryption_verified is not None and (
        record.kind is not RecordKind.TIMELINE
        or record.decryption is not _DecryptionDisposition.DECRYPTED
    ):
        raise ReducerInputError("prepared verification marker is invalid")
    if (record.decrypted_to_device_kind is not None) != (
        record.kind is RecordKind.TO_DEVICE
        and record.decryption is _DecryptionDisposition.DECRYPTED
    ):
        raise ReducerInputError("prepared to-device class is invalid")
    route_kinds = {
        _CallbackRoute.EVENT: {RecordKind.STATE, RecordKind.TIMELINE},
        _CallbackRoute.EPHEMERAL: {RecordKind.EPHEMERAL},
        _CallbackRoute.ROOM_ACCOUNT_DATA: {RecordKind.ROOM_ACCOUNT_DATA},
        _CallbackRoute.GLOBAL_ACCOUNT_DATA: {RecordKind.GLOBAL_ACCOUNT_DATA},
        _CallbackRoute.TO_DEVICE: {RecordKind.TO_DEVICE},
    }
    if (
        record.callback_route is not None
        and record.kind not in route_kinds[record.callback_route]
    ):
        raise ReducerInputError("prepared callback route is invalid")


def _carrier_fields_are_exact(
    value: tuple[object, ...], shapes: tuple[object, ...]
) -> bool:
    return len(value) == len(shapes) and all(map(_matches_exact_shape, value, shapes))


def _validate_prepared_rerequest(value: _PreparedMegolmRerequest) -> None:
    if not _carrier_fields_are_exact(
        value,
        (bytes, str, str, str, str, str, str, str),
    ):
        raise ReducerInputError("prepared crypto carrier is invalid")


def _validate_prepared_crypto_delta(delta: _PreparedCryptoDelta) -> None:
    if type(delta) is not _PreparedCryptoDelta or not _carrier_fields_are_exact(
        delta,
        (
            tuple[str, ...],
            tuple[str, ...],
            int | None,
            bytes,
            bytes,
            tuple[_PreparedKeyClaim, ...],
            tuple[_PreparedQueuedToDeviceMessage, ...],
        ),
    ):
        raise ReducerInputError("prepared crypto carrier is invalid")
    for claim in delta.key_claims:
        if not _carrier_fields_are_exact(
            claim,
            (
                str,
                str,
                bool,
                bool,
                tuple[_PreparedWaitingKeyRequest, ...],
                tuple[_PreparedMegolmRerequest, ...],
            ),
        ):
            raise ReducerInputError("prepared crypto claim carrier is invalid")
        for waiting in claim.waiting_key_requests:
            if not _carrier_fields_are_exact(
                waiting,
                (bytes, str, str, str, str, str, str, str),
            ):
                raise ReducerInputError(
                    "prepared waiting-key-request carrier is invalid"
                )
        for rerequest in claim.rerequest_events:
            _validate_prepared_rerequest(rerequest)

    for message in delta.queued_to_device_messages:
        if not _carrier_fields_are_exact(
            message,
            (
                _QueuedToDeviceSubtype,
                str,
                str,
                str,
                bytes,
                str | None,
                str | None,
                str | None,
                str | None,
                tuple[_PreparedMegolmRerequest, ...],
            ),
        ):
            raise ReducerInputError("prepared queued-message carrier is invalid")
        for rerequest in message.rerequest_events:
            _validate_prepared_rerequest(rerequest)


def _validate_prepared_frame_shape(
    frame: SyncFrame,
    prepared: _PreparedIngestionFrame,
) -> None:
    if type(prepared) is not _PreparedIngestionFrame or not _carrier_fields_are_exact(
        prepared,
        (
            UUID,
            TransportKind,
            int,
            int,
            int,
            bytes,
            bytes,
            bytes,
            str | None,
            tuple[_PreparedIngestionRecord, ...],
            tuple[_PreparedMembershipTransition, ...],
            tuple[RoomSnapshot, ...],
            _PreparedCryptoDelta,
        ),
    ):
        raise ReducerInputError("prepared frame carrier is invalid")
    if len(prepared.source_sha256) != 32:
        raise ReducerInputError("prepared frame carrier is invalid")
    snapshot_rooms = tuple(snapshot.room_id for snapshot in prepared.room_snapshots)
    segment_rooms = {segment.room_id for segment in frame.room_segments}
    if (
        len(snapshot_rooms) != len(set(snapshot_rooms))
        or set(snapshot_rooms) - segment_rooms
    ):
        raise ReducerInputError("prepared snapshot ownership is invalid")
    _validate_prepared_crypto_delta(prepared.crypto_delta)


def _validate_prepared_frame_identity(
    frame: SyncFrame,
    prepared: _PreparedIngestionFrame,
) -> tuple[frozenset[tuple[str, int]], dict[str, _PreparedIngestionRecord]]:
    _validate_prepared_frame_shape(frame, prepared)
    if (
        prepared.frame_id,
        prepared.transport,
        prepared.source_epoch,
        prepared.request_id,
        prepared.request_cursor_json,
        prepared.candidate_cursor_json,
        prepared.source_sha256,
    ) != (
        frame.frame_id,
        frame.origin.transport,
        frame.origin.source_epoch,
        frame.origin.request_id,
        frame.request_cursor_json,
        frame.candidate_cursor_json,
        frame.source_sha256,
    ):
        raise ReducerInputError("prepared frame identity does not match source frame")
    minimum_staged_revision = frame.origin.source_epoch + frame.origin.request_id + 1
    if prepared.staged_revision < minimum_staged_revision:
        raise ReducerInputError("prepared staged revision is invalid")
    _canonical_prepared_object(
        prepared.candidate_cursor_json,
        "prepared candidate cursor",
    )
    expected_token: str | None
    if prepared.transport is TransportKind.CLASSIC:
        try:
            token = _classic_cursor_from_json(prepared.candidate_cursor_json).next_batch
        except (TypeError, ValueError) as error:
            raise ReducerInputError(
                "prepared Classic candidate cursor is invalid"
            ) from error
        if type(token) is not str or not token:
            raise ReducerInputError("prepared Classic compatibility token is invalid")
        expected_token = token
    else:
        expected_token = None
    if prepared.compatibility_token != expected_token:
        raise ReducerInputError("prepared compatibility token is invalid")

    phase_order = {
        _PreparationPhase.SOURCE: 0,
        _PreparationPhase.EXPIRED_VERIFICATION: 1,
        _PreparationPhase.COLLECTED_KEY_REQUEST: 2,
    }
    phase_indexes: dict[_PreparationPhase, int] = defaultdict(int)
    previous_phase = -1
    for index, record in enumerate(prepared.records):
        _validate_prepared_record_shape(record)
        current_phase = phase_order[record.preparation_phase]
        if current_phase < previous_phase:
            raise ReducerInputError("prepared record phases are out of order")
        _validate_prepared_record(
            frame,
            record,
            index,
            phase_indexes[record.preparation_phase],
        )
        phase_indexes[record.preparation_phase] += 1
        previous_phase = current_phase
    record_by_id = {record.record_id: record for record in prepared.records}
    if len(record_by_id) != len(prepared.records):
        raise ReducerInputError("prepared record identities are not unique")

    raw_source, section_boundaries = _prepared_source_records(frame)
    prepared_source = tuple(
        record
        for record in prepared.records
        if record.preparation_phase is _PreparationPhase.SOURCE
    )
    if len(prepared_source) != len(raw_source):
        raise ReducerInputError("prepared source record count is invalid")
    for record, expected in zip(prepared_source, raw_source, strict=True):
        kind, room_id, source_json, provenance = expected
        if (
            record.kind,
            record.room_id,
            record.source_json,
            record.provenance,
        ) != (kind, room_id, source_json, provenance):
            raise ReducerInputError(
                "prepared source record disagrees with source frame"
            )
    if any(
        record.kind is not RecordKind.TO_DEVICE
        for record in prepared.records[len(prepared_source) :]
    ):
        raise ReducerInputError("synthetic prepared records must be to-device events")
    return section_boundaries, record_by_id


def _validate_prepared_transitions(
    frame: SyncFrame,
    prepared: _PreparedIngestionFrame,
    section_boundaries: frozenset[tuple[str, int]],
    record_by_id: dict[str, _PreparedIngestionRecord],
) -> dict[int, tuple[_PreparedMembershipTransition, ...]]:
    transitions_at: dict[int, list[_PreparedMembershipTransition]] = defaultdict(list)
    linked_ids: set[str] = set()
    linked_rooms: set[str] = set()
    section_rooms: set[str] = set()
    transition_ids: set[str] = set()
    previous_position = -1
    segment_by_room = {segment.room_id: segment for segment in frame.room_segments}
    memberships = {"invite", "join", "knock", "leave", "ban"}
    for index, transition in enumerate(prepared.membership_transitions):
        if type(
            transition
        ) is not _PreparedMembershipTransition or not _carrier_fields_are_exact(
            transition,
            (
                str,
                str | None,
                str,
                str | None,
                str | None,
                str,
                int,
                int,
                _MembershipSourceKind,
                TimelineEventProvenance | None,
                _MembershipProvenance,
                RecordOrigin,
                bytes | None,
            ),
        ):
            raise ReducerInputError("prepared transition carrier is invalid")
        if (
            transition.transition_id
            != str(uuid5(frame.frame_id, f"record:transition:{index}"))
            or transition.transition_id in transition_ids
        ):
            raise ReducerInputError("prepared transition identity is invalid")
        transition_ids.add(transition.transition_id)
        if transition.membership_provenance is not _MembershipProvenance.REPORTED:
            raise ReducerInputError("prepared transition provenance is invalid")
        if min(transition.previous_epoch, transition.current_epoch) < 0:
            raise ReducerInputError("prepared transition epoch is invalid")
        if (
            transition.origin.transport,
            transition.origin.source_epoch,
            transition.origin.request_id,
        ) != (
            frame.origin.transport,
            frame.origin.source_epoch,
            frame.origin.request_id,
        ):
            raise ReducerInputError("prepared transition origin is invalid")
        if transition.current_membership not in memberships or (
            transition.previous_membership is not None
            and transition.previous_membership not in memberships
        ):
            raise ReducerInputError(
                "prepared transition membership is outside the domain"
            )
        if transition.current_membership == transition.previous_membership:
            raise ReducerInputError("prepared transition must change membership")

        if transition.source_record_id is None:
            segment = segment_by_room.get(transition.room_id)
            if (
                transition.source_kind is not _MembershipSourceKind.SECTION
                or transition.event_id is not None
                or transition.source_json is not None
                or transition.timeline_provenance is not None
                or (transition.room_id, transition.origin.frame_index)
                not in section_boundaries
                or segment is None
                or segment.membership_observation.room_membership
                != transition.current_membership
                or transition.room_id in section_rooms
                or transition.room_id in linked_rooms
            ):
                raise ReducerInputError("prepared section transition is invalid")
            section_rooms.add(transition.room_id)
            position = transition.origin.frame_index
        else:
            if transition.room_id in section_rooms:
                raise ReducerInputError(
                    "prepared section and linked transitions cannot coexist"
                )
            if (
                transition.source_record_id in linked_ids
                or transition.source_kind is _MembershipSourceKind.SECTION
            ):
                raise ReducerInputError("prepared transition source is invalid")
            linked_ids.add(transition.source_record_id)
            linked_rooms.add(transition.room_id)
            record = record_by_id.get(transition.source_record_id)
            if (
                record is None
                or record.preparation_phase is not _PreparationPhase.SOURCE
            ):
                raise ReducerInputError("prepared transition source record is invalid")
            if (
                transition.source_kind is _MembershipSourceKind.STATE
                and (
                    record.kind is not RecordKind.STATE
                    or transition.timeline_provenance is not None
                )
            ) or (
                transition.source_kind is _MembershipSourceKind.TIMELINE
                and (
                    record.kind is not RecordKind.TIMELINE
                    or transition.timeline_provenance is not record.provenance
                )
            ):
                raise ReducerInputError("prepared transition source kind is invalid")
            if (
                transition.room_id,
                transition.event_id,
                transition.origin,
                transition.source_json,
            ) != (
                record.room_id,
                record.event_id,
                record.origin,
                record.clear_json or record.source_json,
            ):
                raise ReducerInputError("prepared transition evidence is invalid")
            evidence_source = transition.source_json
            if evidence_source is None:
                raise ReducerInputError("prepared transition evidence is invalid")
            evidence = _canonical_prepared_object(
                evidence_source,
                "prepared transition evidence",
            )
            content = evidence.get("content")
            if (
                evidence.get("type") != "m.room.member"
                or evidence.get("event_id") != transition.event_id
                or type(evidence.get("state_key")) is not str
                or type(content) is not dict
                or content.get("membership") != transition.current_membership
            ):
                raise ReducerInputError("prepared transition evidence is invalid")
            position = record.origin.frame_index
        if not 0 <= position <= len(prepared.records):
            raise ReducerInputError("prepared transition position is invalid")
        if position < previous_position:
            raise ReducerInputError("prepared transition order is invalid")
        previous_position = position
        transitions_at[position].append(transition)
    return {position: tuple(values) for position, values in transitions_at.items()}


def _barrier_loss(continuity: RoomContinuity) -> LossProposal | None:
    if continuity.gap is not None:
        return _loss(
            continuity,
            LossReason.BASELINE_LOST,
            continuity.gap.start_token,
            continuity.gap.target_token,
        )
    if continuity.hydration_id is not None:
        return _loss(continuity, LossReason.UNVERIFIABLE)
    return None


def _crypto_deferred(prepared: _PreparedIngestionFrame) -> bool:
    delta = prepared.crypto_delta
    return bool(
        delta.encrypted_room_ids
        or delta.users_for_key_query
        or delta.uploaded_key_count is not None
        or delta.key_claims
        or delta.queued_to_device_messages
        or any(record.kind is RecordKind.TO_DEVICE for record in prepared.records)
    )


def reduce_prepared_frame(
    stream_id: UUID,
    frame: SyncFrame,
    prepared: _PreparedIngestionFrame,
    rooms: tuple[RoomContinuity, ...],
) -> PreparedFrameProposal:
    """Validate Task4C output and preserve its exact global transition order."""
    _exact(stream_id, UUID, "stream_id")
    _exact(frame, SyncFrame, "frame")
    _exact(rooms, tuple, "rooms")
    if any(type(room) is not RoomContinuity for room in rooms):
        raise TypeError("rooms elements must be RoomContinuity")
    room_ids = tuple(room.room_id for room in rooms)
    if len(room_ids) != len(set(room_ids)):
        raise ReducerInputError("rooms must have unique room IDs")

    section_boundaries, record_by_id = _validate_prepared_frame_identity(
        frame, prepared
    )
    transitions_at = _validate_prepared_transitions(
        frame,
        prepared,
        section_boundaries,
        record_by_id,
    )
    segments = {segment.room_id: segment for segment in frame.room_segments}
    if len(segments) != len(frame.room_segments):
        raise ReducerInputError("source frame room segments are not unique")
    if {transition.room_id for transition in prepared.membership_transitions} - set(
        segments
    ):
        raise ReducerInputError("prepared transition has no source room segment")

    prior = {room.room_id: room for room in rooms}
    event_ids_by_room = {
        segment.room_id: _timeline_event_ids(segment) for segment in frame.room_segments
    }
    recovery_start_by_room = {
        segment.room_id: _restart_recovery_start(
            frame,
            segment,
            prior.get(segment.room_id),
            event_ids_by_room[segment.room_id],
        )
        for segment in frame.room_segments
    }
    section_position_by_room = dict(section_boundaries)
    timeline_provenance_at: dict[int, TimelineEventProvenance] = {}
    for segment in frame.room_segments:
        timeline_start = section_position_by_room[segment.room_id] + len(
            segment.state_json
        )
        history_count = len(segment.timeline_json) - segment.live_event_count
        for index in range(len(segment.timeline_json)):
            timeline_provenance_at[timeline_start + index] = _timeline_provenance(
                index,
                history_count,
                recovery_start_by_room[segment.room_id],
                segment.recovered_event_count,
            )
    timeline_provenance_by_record_id = {
        prepared.records[position].record_id: provenance
        for position, provenance in timeline_provenance_at.items()
    }
    room_transitions: dict[str, list[_PreparedMembershipTransition]] = defaultdict(list)
    for transition in prepared.membership_transitions:
        room_transitions[transition.room_id].append(transition)

    states: dict[str, tuple[int, str | None]] = {}
    routes_before_change: dict[str, DescriptorRoute] = {}
    first_seen: set[str] = set()
    outstanding_loss: dict[str, LossProposal] = {}
    active_hydrations: dict[str, UUID] = {}
    room_results: dict[str, RoomProposal] = {}
    delegated_routes: dict[str, DescriptorRoute] = {}
    for segment in frame.room_segments:
        before = prior.get(segment.room_id)
        transitions = room_transitions.get(segment.room_id, [])
        if not transitions:
            if before is None or (
                (claim := _membership(segment)) is not None
                and claim != before.membership
            ):
                raise ReducerInputError(
                    "source membership change has no prepared transition"
                )
            result, route = _plan_room(
                stream_id,
                frame,
                segment,
                before,
                verified_restart_boundary=(
                    recovery_start_by_room[segment.room_id] is not None
                ),
            )
            if result.recovery is not None:
                gap = result.recovery
                if (
                    result.before != before
                    or result.after.gap != gap
                    or result.after.hydration_id is not None
                    or result.hydration is not None
                    or result.retirement_epoch is not None
                    or result.losses
                    or result.release is not RecoveryRelease.NONE
                    or route is not DescriptorRoute.HOLD_FOR_GAP
                ):
                    raise ReducerInputError(
                        "prepared new-gap recovery result is invalid"
                    )
                result = replace(
                    result,
                    after=replace(result.after, gap=None),
                    recovery=None,
                    losses=(
                        _loss(
                            before,
                            LossReason.BASELINE_LOST,
                            gap.start_token,
                            gap.target_token,
                        ),
                    ),
                    release=RecoveryRelease.LOSS_THEN_HELD,
                )
                route = DescriptorRoute.RELEASE_AFTER_LOSS
            result = replace(
                result,
                after=replace(
                    result.after,
                    last_timeline_event_id=_next_timeline_event_id(
                        before,
                        result.after,
                        segment,
                        event_ids_by_room[segment.room_id],
                    ),
                ),
            )
            room_results[segment.room_id] = result
            delegated_routes[segment.room_id] = route
            states[segment.room_id] = (
                result.after.membership_epoch,
                result.after.membership,
            )
            continue

        if before is None:
            first_seen.add(segment.room_id)
            states[segment.room_id] = (0, None)
            routes_before_change[segment.room_id] = (
                DescriptorRoute.HOLD_FOR_HYDRATION
                if transitions[0].current_membership == "join"
                else DescriptorRoute.READY
            )
        else:
            states[segment.room_id] = (
                before.membership_epoch,
                before.membership,
            )
            routes_before_change[segment.room_id] = (
                DescriptorRoute.HOLD_FOR_GAP
                if before.gap is not None
                else (
                    DescriptorRoute.HOLD_FOR_HYDRATION
                    if before.hydration_id is not None
                    else DescriptorRoute.READY
                )
            )
            loss = _barrier_loss(before)
            if loss is not None:
                outstanding_loss[segment.room_id] = loss

    touched_room_ids = list(segments)
    for record in prepared.records:
        room_id = record.room_id
        if room_id is None or room_id in touched_room_ids:
            continue
        before = prior.get(room_id)
        if before is None:
            raise ReducerInputError(
                "prepared room record has no segment or prior Aggregate"
            )
        touched_room_ids.append(room_id)
        states[room_id] = (before.membership_epoch, before.membership)
        route = (
            DescriptorRoute.HOLD_FOR_GAP
            if before.gap is not None
            else (
                DescriptorRoute.HOLD_FOR_HYDRATION
                if before.hydration_id is not None
                else DescriptorRoute.READY
            )
        )
        delegated_routes[room_id] = route
        room_results[room_id] = RoomProposal(
            before,
            before,
            None,
            None,
            None,
            (),
            RecoveryRelease.NONE,
        )

    recovery_by_room: dict[str, PreparedRecoveryStep] = {}
    for room_id, result in room_results.items():
        if not result.losses:
            continue
        if (
            result.before is None
            or result.release is not RecoveryRelease.LOSS_THEN_HELD
            or room_id not in section_position_by_room
        ):
            raise ReducerInputError("prepared recovery result is invalid")
        recovery_by_room[room_id] = PreparedRecoveryStep(
            room_id,
            result.before.membership_epoch,
            result.losses,
        )

    actions_at: dict[
        int, list[PreparedRecoveryStep | _PreparedMembershipTransition]
    ] = defaultdict(list)
    for segment in frame.room_segments:
        position = section_position_by_room[segment.room_id]
        actions_at[position].extend(
            transition
            for transition in transitions_at.get(position, ())
            if transition.source_record_id is None
            and transition.room_id == segment.room_id
        )
        recovery = recovery_by_room.get(segment.room_id)
        if recovery is not None:
            actions_at[position].append(recovery)
    for position, anchored_transitions in transitions_at.items():
        actions_at[position].extend(
            transition
            for transition in anchored_transitions
            if transition.source_record_id is not None
        )
    for position, anchored_transitions in transitions_at.items():
        if (
            tuple(
                action
                for action in actions_at[position]
                if type(action) is _PreparedMembershipTransition
            )
            != anchored_transitions
        ):
            raise ReducerInputError("prepared transition tie-break order is invalid")

    changed_rooms: set[str] = set()
    linear_steps: list[
        PreparedRecordStep | PreparedTransitionStep | PreparedRecoveryStep
    ] = []
    for position in range(len(prepared.records) + 1):
        for action in actions_at.get(position, ()):
            if type(action) is PreparedRecoveryStep:
                linear_steps.append(action)
                continue
            if type(action) is not _PreparedMembershipTransition:
                raise ReducerInputError("prepared linear action is invalid")
            transition_action: _PreparedMembershipTransition = action
            if transition_action.source_record_id is not None:
                desired_provenance = timeline_provenance_by_record_id.get(
                    transition_action.source_record_id
                )
                if transition_action.timeline_provenance is not desired_provenance:
                    transition_action = transition_action._replace(
                        timeline_provenance=desired_provenance
                    )
            epoch, membership = states[transition_action.room_id]
            if (
                transition_action.previous_epoch,
                transition_action.previous_membership,
            ) != (epoch, membership):
                raise ReducerInputError(
                    "prepared membership transition chain is invalid"
                )
            expected_epoch = epoch + int(
                membership == "join" and transition_action.current_membership != "join"
            )
            if transition_action.current_epoch != expected_epoch:
                raise ReducerInputError(
                    "prepared membership transition epoch is invalid"
                )
            loss = outstanding_loss.pop(transition_action.room_id, None)
            release_epoch = loss.membership_epoch if loss is not None else None
            states[transition_action.room_id] = (
                transition_action.current_epoch,
                transition_action.current_membership,
            )
            changed_rooms.add(transition_action.room_id)
            if loss is not None:
                active_hydrations.pop(transition_action.room_id, None)
                routes_before_change[transition_action.room_id] = DescriptorRoute.READY
            elif (
                transition_action.room_id in first_seen
                and membership is None
                and transition_action.current_membership == "join"
            ):
                hydration_id = uuid5(
                    stream_id,
                    (
                        f"hydrate:{transition_action.room_id}:"
                        f"{transition_action.current_epoch}:{frame.frame_id}"
                    ),
                )
                active_hydrations[transition_action.room_id] = hydration_id
                outstanding_loss[transition_action.room_id] = LossProposal(
                    transition_action.room_id,
                    transition_action.current_epoch,
                    LossReason.UNVERIFIABLE,
                    LossBoundary(None, None, None, None),
                )
                routes_before_change[transition_action.room_id] = (
                    DescriptorRoute.HOLD_FOR_HYDRATION
                )
            linear_steps.append(
                PreparedTransitionStep(
                    transition_action,
                    transition_action.current_epoch,
                    loss,
                    release_epoch,
                )
            )
        if position == len(prepared.records):
            continue
        record = prepared.records[position]
        if (
            provenance := timeline_provenance_at.get(position)
        ) is not None and record.provenance is not provenance:
            record = record._replace(provenance=provenance)
        if record.room_id is None:
            linear_steps.append(PreparedRecordStep(record, DescriptorRoute.READY, None))
            continue
        epoch, _membership_value = states[record.room_id]
        route = delegated_routes.get(
            record.room_id,
            routes_before_change.get(record.room_id, DescriptorRoute.READY),
        )
        if record.room_id in changed_rooms and record.room_id not in active_hydrations:
            route = DescriptorRoute.READY
        linear_steps.append(PreparedRecordStep(record, route, epoch))

    for segment in frame.room_segments:
        if segment.room_id not in room_transitions:
            continue
        epoch, membership = states[segment.room_id]
        if (claim := _membership(segment)) is not None and claim != membership:
            raise ReducerInputError(
                "prepared membership chain disagrees with source claim"
            )
        before = prior.get(segment.room_id)
        if segment.room_id in first_seen:
            final_hydration_id = active_hydrations.get(segment.room_id)
            after = RoomContinuity(
                segment.room_id,
                epoch,
                membership,
                None,
                None,
                final_hydration_id,
            )
            after = replace(
                after,
                last_timeline_event_id=_next_timeline_event_id(
                    before,
                    after,
                    segment,
                    event_ids_by_room[segment.room_id],
                ),
            )
            room_results[segment.room_id] = RoomProposal(
                None,
                after,
                None,
                (
                    HydrationIntent(final_hydration_id, frame.origin)
                    if final_hydration_id is not None
                    else None
                ),
                None,
                (),
                RecoveryRelease.NONE,
            )
        else:
            assert before is not None
            after = RoomContinuity(
                segment.room_id,
                epoch,
                membership,
                _recovered_membership_baseline(frame, segment, before),
                None,
                None,
            )
            after = replace(
                after,
                last_timeline_event_id=_next_timeline_event_id(
                    before,
                    after,
                    segment,
                    event_ids_by_room[segment.room_id],
                ),
            )
            room_results[segment.room_id] = RoomProposal(
                before,
                after,
                None,
                None,
                None,
                (),
                RecoveryRelease.NONE,
            )
    if set(outstanding_loss) != set(active_hydrations):
        raise ReducerInputError("prepared transition did not settle its room barrier")

    return PreparedFrameProposal(
        frame.frame_id,
        frame.source_sha256,
        tuple(room_results[room_id] for room_id in touched_room_ids),
        tuple(linear_steps),
        _crypto_deferred(prepared),
    )
