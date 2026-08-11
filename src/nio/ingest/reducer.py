"""Pure proposals derived from source-certified staged sync frames."""

from dataclasses import dataclass, fields
from enum import StrEnum
from types import UnionType
from typing import get_args, get_origin
from uuid import UUID, uuid5

from ..event_provenance import TimelineEventProvenance
from ._json import canonical_json, load_json
from .model import LossBoundary, LossReason, RecordKind, RecordOrigin
from .source import (
    RoomSegment,
    SyncFrame,
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

    def __post_init__(self) -> None:
        super().__post_init__()
        if self.gap is not None and (
            self.gap.room_id != self.room_id
            or self.gap.membership_epoch != self.membership_epoch
        ):
            raise ValueError("gap must belong to this room epoch")
        if self.gap is not None and self.hydration_id is not None:
            raise ValueError("gap and hydration_id are mutually exclusive")


@dataclass(frozen=True, slots=True)
class RecordDescriptor(_ValidatedValue):
    kind: RecordKind
    room_id: str | None
    source_json: bytes
    provenance: TimelineEventProvenance | None
    descriptor_key: str
    route: DescriptorRoute


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


@dataclass(frozen=True, slots=True)
class FrameProposal(_ValidatedValue):
    frame_id: UUID
    source_sha256: bytes
    room_proposals: tuple[RoomProposal, ...]
    descriptors: tuple[RecordDescriptor, ...]
    crypto_deferred: bool

    def __post_init__(self) -> None:
        super().__post_init__()
        if len(self.source_sha256) != 32:
            raise ValueError("source_sha256 must be exactly 32 bytes")


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


def _plan_room(
    stream_id: UUID,
    frame: SyncFrame,
    segment: RoomSegment,
    before: RoomContinuity | None,
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

        if _has_trusted_baseline(before, segment):
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
            if segment.history_discontinuity:
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


def reduce_staged_frame(
    stream_id: UUID,
    staged_frame_id: UUID,
    frame: SyncFrame,
    rooms: tuple[RoomContinuity, ...],
) -> FrameProposal:
    _exact(stream_id, UUID, "stream_id")
    _exact(staged_frame_id, UUID, "staged_frame_id")
    _exact(frame, SyncFrame, "frame")
    _exact(rooms, tuple, "rooms")
    if any(type(room) is not RoomContinuity for room in rooms):
        raise TypeError("rooms elements must be RoomContinuity")
    if staged_frame_id != frame.frame_id:
        raise ReducerInputError("staged frame identity does not match frame")
    room_ids = tuple(room.room_id for room in rooms)
    if len(room_ids) != len(set(room_ids)):
        raise ReducerInputError("rooms must have unique room IDs")
    states = {room.room_id: room for room in rooms}
    plans: dict[str, tuple[RoomProposal, DescriptorRoute]] = {}
    descriptors: list[RecordDescriptor] = []
    encrypted_timeline = False

    for segment in frame.room_segments:
        plan = _plan_room(stream_id, frame, segment, states.get(segment.room_id))
        plans[segment.room_id] = plan
        states[segment.room_id] = plan[0].after

    def append(
        kind: RecordKind,
        room_id: str | None,
        source_json: bytes,
        provenance: TimelineEventProvenance | None = None,
    ) -> None:
        state = states.get(room_id) if room_id is not None else None
        if room_id is None:
            route = DescriptorRoute.READY
        elif room_id in plans:
            route = plans[room_id][1]
        elif state is None:
            raise ReducerInputError("room descriptor has no continuity")
        elif state.gap is not None:
            route = DescriptorRoute.HOLD_FOR_GAP
        elif state.hydration_id is not None:
            route = DescriptorRoute.HOLD_FOR_HYDRATION
        else:
            route = DescriptorRoute.READY
        descriptors.append(
            RecordDescriptor(
                kind,
                room_id,
                source_json,
                provenance,
                f"frame:{frame.frame_id}:{len(descriptors)}",
                route,
            )
        )

    for segment in frame.room_segments:
        for payload in segment.state_json:
            append(RecordKind.STATE, segment.room_id, payload)
        history_count = len(segment.timeline_json) - segment.live_event_count
        for index, payload in enumerate(segment.timeline_json):
            try:
                event = load_json(payload, "timeline event")
                if type(event) is not dict or canonical_json(event) != payload:
                    raise ValueError("timeline event is not a canonical object")
            except (TypeError, ValueError) as error:
                raise ReducerInputError("invalid canonical timeline event") from error
            encrypted_timeline = encrypted_timeline or event.get("type") == "m.room.encrypted"  # fmt: skip
            append(
                RecordKind.TIMELINE,
                segment.room_id,
                payload,
                (
                    TimelineEventProvenance.HISTORY
                    if index < history_count
                    else TimelineEventProvenance.LIVE
                ),
            )
        for payload in segment.room_account_data_json:
            append(RecordKind.ROOM_ACCOUNT_DATA, segment.room_id, payload)
    try:
        ephemeral = _normalized_ephemeral_envelopes(frame.ephemeral_json)
    except (TypeError, ValueError) as error:
        raise ReducerInputError("invalid canonical ephemeral envelope") from error
    for room_id, source_json in ephemeral:
        append(RecordKind.EPHEMERAL, room_id, source_json)
    for payload in frame.global_account_data_json:
        append(RecordKind.GLOBAL_ACCOUNT_DATA, None, payload)
    for payload in frame.presence_json:
        append(RecordKind.PRESENCE, None, payload)
    room_proposals = tuple(plans[segment.room_id][0] for segment in frame.room_segments)
    crypto_deferred = (
        encrypted_timeline
        or bool(frame.to_device_json)
        or frame.device_list_delta_json != b'{"changed":[],"left":[]}'
        or frame.one_time_key_counts_json != b"{}"
        or frame.unused_fallback_key_types_json not in (b"null", b"[]")
    )
    return FrameProposal(
        frame.frame_id,
        frame.source_sha256,
        room_proposals,
        tuple(descriptors),
        crypto_deferred,
    )
