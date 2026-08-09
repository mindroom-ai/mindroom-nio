from dataclasses import dataclass, field, replace
from enum import StrEnum
from uuid import UUID

from ..event_provenance import TimelineEventProvenance
from .membership import MembershipBaseline
from .model import (
    ConsumerBinding,
    EventRecord,
    LossRecord,
    RecordKind,
    RoomHydrationStatus,
    RoomSnapshot,
    SyncBatch,
    SystemOrigin,
    TransportKind,
)
from .recovery import RecoveryGap


class LaneStatus(StrEnum):
    ACTIVE = "active"
    RETIRING = "retiring"


class ReleasePhase(StrEnum):
    IDLE = "idle"
    RECOVERING = "recovering"
    RELEASING_RECOVERED = "releasing_recovered"
    RELEASING_TERMINAL = "releasing_terminal"


class AckOutcome(StrEnum):
    ACKNOWLEDGED = "acknowledged"
    ALREADY_ACKNOWLEDGED = "already_acknowledged"


class ConsumerAttachStatus(StrEnum):
    UNBOUND = "unbound"
    ATTACHING = "attaching"
    ATTACHED = "attached"


@dataclass(frozen=True, slots=True)
class OwnerView:
    account_id: str
    device_id: str
    schema_version: int
    stream_id: UUID
    transport_kind: TransportKind
    binding_operation_id: UUID
    consumer_attach_status: ConsumerAttachStatus
    consumer_attach_next_room_ordinal: int
    binding: ConsumerBinding | None
    consumer_first_sequence: int | None
    baseline_rooms_sha256: bytes | None
    consumer_attached_revision: int | None
    revision: int
    writer_epoch: UUID
    next_ready_order: int
    next_batch_sequence: int
    last_acked_sequence: int


@dataclass(frozen=True, slots=True)
class SourceState:
    source_epoch: int
    transport_kind: TransportKind
    cursor_json: bytes
    next_request_id: int
    active: bool

    def __post_init__(self) -> None:
        if type(self.source_epoch) is not int:
            raise TypeError("source_epoch must be int")
        if type(self.transport_kind) is not TransportKind:
            raise TypeError("transport_kind must be TransportKind")
        if type(self.cursor_json) is not bytes:
            raise TypeError("cursor_json must be bytes")
        if type(self.next_request_id) is not int:
            raise TypeError("next_request_id must be int")
        if type(self.active) is not bool:
            raise TypeError("active must be bool")


@dataclass(frozen=True, slots=True)
class RoomState:
    room_id: str
    current_membership_epoch: int
    next_room_sequence: int
    hydration_status: RoomHydrationStatus
    snapshot: RoomSnapshot | None
    membership_baseline: MembershipBaseline | None = None
    updated_revision: int = field(default=0, compare=False)

    def __post_init__(self) -> None:
        if type(self.room_id) is not str:
            raise TypeError("room_id must be str")
        if not self.room_id:
            raise ValueError("room_id must not be empty")
        if type(self.current_membership_epoch) is not int:
            raise TypeError("current_membership_epoch must be int")
        if type(self.next_room_sequence) is not int:
            raise TypeError("next_room_sequence must be int")
        if type(self.updated_revision) is not int:
            raise TypeError("updated_revision must be int")
        if any(
            value < 0
            for value in (
                self.current_membership_epoch,
                self.next_room_sequence,
                self.updated_revision,
            )
        ):
            raise ValueError("RoomState counters must be nonnegative")
        if type(self.hydration_status) is not RoomHydrationStatus:
            raise TypeError("hydration_status must be RoomHydrationStatus")
        if self.snapshot is not None and type(self.snapshot) is not RoomSnapshot:
            raise TypeError("snapshot must be RoomSnapshot or None")
        if (
            self.membership_baseline is not None
            and type(self.membership_baseline) is not MembershipBaseline
        ):
            raise TypeError("membership_baseline must be MembershipBaseline or None")
        if self.snapshot is not None and (
            self.snapshot.room_id != self.room_id
            or self.snapshot.membership_epoch != self.current_membership_epoch
        ):
            raise ValueError("snapshot room/epoch does not match RoomState")
        if self.hydration_status is RoomHydrationStatus.READY:
            if self.snapshot is None:
                raise ValueError("READY RoomState requires a snapshot")
        elif self.snapshot is not None:
            raise ValueError(
                f"{self.hydration_status.name} RoomState cannot contain a snapshot"
            )
        if self.membership_baseline is not None and (
            self.membership_baseline.room_id != self.room_id
            or self.membership_baseline.membership_epoch
            != self.current_membership_epoch
        ):
            raise ValueError("membership baseline room/epoch does not match RoomState")


@dataclass(frozen=True, slots=True)
class RoomLane:
    room_id: str
    membership_epoch: int
    lane_status: LaneStatus
    held_record_count: int = 0
    held_canonical_bytes: int = 0
    release_phase: ReleasePhase = ReleasePhase.IDLE
    ready_order: int | None = None
    next_held_ordinal: int = 0
    successor_membership_epoch: int | None = None
    recovery_gap: RecoveryGap | None = None
    pending_lifecycle: EventRecord | None = None
    updated_revision: int = field(default=0, compare=False)

    def __post_init__(self) -> None:
        if type(self.room_id) is not str:
            raise TypeError("room_id must be str")
        if not self.room_id:
            raise ValueError("room_id must not be empty")
        if type(self.membership_epoch) is not int:
            raise TypeError("membership_epoch must be int")
        if type(self.lane_status) is not LaneStatus:
            raise TypeError("lane_status must be LaneStatus")
        if type(self.release_phase) is not ReleasePhase:
            raise TypeError("release_phase must be ReleasePhase")
        for name in (
            "held_record_count",
            "held_canonical_bytes",
            "next_held_ordinal",
            "updated_revision",
        ):
            value = getattr(self, name)
            if type(value) is not int:
                raise TypeError(f"{name} must be int")
        for name in ("ready_order", "successor_membership_epoch"):
            value = getattr(self, name)
            if value is not None and type(value) is not int:
                raise TypeError(f"{name} must be int or None")
        counters = (
            self.membership_epoch,
            self.held_record_count,
            self.held_canonical_bytes,
            self.next_held_ordinal,
            self.updated_revision,
        )
        optional_counters = (self.ready_order, self.successor_membership_epoch)
        if any(value < 0 for value in counters) or any(
            value is not None and value < 0 for value in optional_counters
        ):
            raise ValueError("RoomLane counters must be nonnegative")
        if (self.held_record_count == 0) is not (self.held_canonical_bytes == 0):
            raise ValueError("held record count and bytes must be zero together")
        if self.next_held_ordinal < self.held_record_count:
            raise ValueError("next held ordinal cannot precede held record count")
        if self.recovery_gap is not None and type(self.recovery_gap) is not RecoveryGap:
            raise TypeError("recovery_gap must be RecoveryGap or None")
        if self.recovery_gap is not None and (
            self.recovery_gap.room_id != self.room_id
            or self.recovery_gap.membership_epoch != self.membership_epoch
        ):
            raise ValueError("recovery gap room/epoch does not match RoomLane")
        if self.release_phase is ReleasePhase.RECOVERING:
            if self.recovery_gap is None:
                raise ValueError("RECOVERING RoomLane requires a recovery gap")
        elif self.recovery_gap is not None:
            raise ValueError("a recovery gap requires RECOVERING release phase")
        has_ready_head = self.release_phase in (
            ReleasePhase.RELEASING_RECOVERED,
            ReleasePhase.RELEASING_TERMINAL,
        )
        if (self.ready_order is not None) is not has_ready_head:
            raise ValueError("ready_order must exist exactly for a releasing lane")
        if (
            self.pending_lifecycle is not None
            and type(self.pending_lifecycle) is not EventRecord
        ):
            raise TypeError("pending_lifecycle must be EventRecord or None")
        if self.lane_status is LaneStatus.ACTIVE:
            if (
                self.successor_membership_epoch is not None
                or self.pending_lifecycle is not None
            ):
                raise ValueError("active lane cannot have a successor lifecycle")
        else:
            lifecycle = self.pending_lifecycle
            if self.recovery_gap is not None:
                raise ValueError("retiring lane cannot retain a recovery gap")
            if self.successor_membership_epoch is None or lifecycle is None:
                raise ValueError("retiring lane requires a successor lifecycle")
            if (
                lifecycle.kind is not RecordKind.ROOM_LIFECYCLE
                or lifecycle.room_id != self.room_id
                or lifecycle.membership_epoch != self.successor_membership_epoch
            ):
                raise ValueError("retiring lane lifecycle barrier is invalid")


@dataclass(frozen=True, slots=True)
class RoomAggregate:
    state: RoomState
    active_lane: RoomLane
    retiring_lanes: tuple[RoomLane, ...]

    def __post_init__(self) -> None:
        if type(self.state) is not RoomState:
            raise TypeError("state must be RoomState")
        if type(self.active_lane) is not RoomLane:
            raise TypeError("active_lane must be RoomLane")
        if type(self.retiring_lanes) is not tuple:
            raise TypeError("retiring_lanes must be a tuple")
        if any(type(lane) is not RoomLane for lane in self.retiring_lanes):
            raise TypeError("retiring_lanes must contain RoomLane values")

        lanes = (*self.retiring_lanes, self.active_lane)
        try:
            replace(self.state)
            for lane in lanes:
                replace(lane)
        except (TypeError, ValueError) as error:
            raise ValueError(str(error)) from error
        if any(lane.room_id != self.state.room_id for lane in lanes):
            raise ValueError("room membership lane belongs to a different room")
        epochs = tuple(lane.membership_epoch for lane in lanes)
        if epochs != tuple(sorted(epochs)):
            raise ValueError("room membership lanes are not ordered")
        if any(
            right != left + 1 for left, right in zip(epochs, epochs[1:], strict=False)
        ):
            raise ValueError("room membership lane chain is not gap-free")
        if self.active_lane.lane_status is not LaneStatus.ACTIVE:
            raise ValueError("room must have exactly one final active lane")
        if self.active_lane.membership_epoch != self.state.current_membership_epoch:
            raise ValueError("active lane does not match current membership epoch")

        for lane, successor in zip(lanes, lanes[1:], strict=False):
            lifecycle = lane.pending_lifecycle
            if lane.lane_status is not LaneStatus.RETIRING:
                raise ValueError("predecessor lane must be retiring")
            if lane.recovery_gap is not None:
                raise ValueError("retiring lane cannot retain a recovery gap")
            if lane.successor_membership_epoch != successor.membership_epoch:
                raise ValueError("retiring lane successor is not gap-free")
            if (
                lifecycle is None
                or lifecycle.kind is not RecordKind.ROOM_LIFECYCLE
                or lifecycle.room_id != self.state.room_id
                or lifecycle.membership_epoch != successor.membership_epoch
            ):
                raise ValueError("retiring lane lifecycle barrier is invalid")
        if (
            self.active_lane.successor_membership_epoch is not None
            or self.active_lane.pending_lifecycle is not None
        ):
            raise ValueError("active lane cannot have a successor barrier")

        gap = self.active_lane.recovery_gap
        if gap is not None:
            baseline = self.state.membership_baseline
            if baseline is None:
                raise ValueError("active recovery gap requires a membership baseline")
            if gap.start_token != baseline.prev_batch:
                raise ValueError("recovery gap start token does not match baseline")


class LaneRecordSection(StrEnum):
    LOSS = "loss"
    RECOVERED = "recovered"
    HELD = "held"


@dataclass(frozen=True, slots=True)
class LaneRecordKey:
    room_id: str
    membership_epoch: int
    section: LaneRecordSection
    page_ordinal: int
    record_ordinal: int

    def __post_init__(self) -> None:
        if type(self.room_id) is not str:
            raise TypeError("room_id must be str")
        if not self.room_id:
            raise ValueError("room_id must not be empty")
        for name in ("membership_epoch", "page_ordinal", "record_ordinal"):
            value = getattr(self, name)
            if type(value) is not int:
                raise TypeError(f"{name} must be int")
            if value < 0:
                raise ValueError(f"{name} must be nonnegative")
        if type(self.section) is not LaneRecordSection:
            raise TypeError("section must be LaneRecordSection")
        if self.section is LaneRecordSection.HELD and self.page_ordinal != 0:
            raise ValueError("HELD lane record page_ordinal must be zero")


@dataclass(frozen=True, slots=True)
class LaneRecord:
    key: LaneRecordKey
    record: EventRecord | LossRecord
    source_frame_id: UUID | None
    source_effect_id: UUID | None
    canonical_bytes: int

    def __post_init__(self) -> None:
        if type(self.key) is not LaneRecordKey:
            raise TypeError("key must be LaneRecordKey")
        if type(self.record) not in (EventRecord, LossRecord):
            raise TypeError("record must be EventRecord or LossRecord")
        if self.key.section is LaneRecordSection.LOSS:
            if type(self.record) is not LossRecord:
                raise ValueError("LOSS lane record must contain a LossRecord")
        elif type(self.record) is not EventRecord:
            raise ValueError(
                f"{self.key.section.name} lane record must contain an EventRecord"
            )
        elif self.key.section is LaneRecordSection.RECOVERED and (
            self.record.kind is not RecordKind.TIMELINE
            or self.record.provenance is not TimelineEventProvenance.RECOVERED
        ):
            raise ValueError(
                "RECOVERED lane record requires TIMELINE/RECOVERED provenance"
            )
        elif self.key.section is LaneRecordSection.HELD and (
            self.record.kind is not RecordKind.TIMELINE
            or self.record.provenance is not TimelineEventProvenance.LIVE
        ):
            raise ValueError("HELD lane record requires TIMELINE/LIVE provenance")
        if (
            self.record.room_id != self.key.room_id
            or self.record.membership_epoch != self.key.membership_epoch
        ):
            raise ValueError("lane record room/epoch does not match its key")
        item_id = (
            self.record.record_id
            if type(self.record) is EventRecord
            else self.record.loss_id
        )
        if not item_id:
            raise ValueError("lane record item identity must not be empty")
        for name in ("source_frame_id", "source_effect_id"):
            value = getattr(self, name)
            if value is not None and type(value) is not UUID:
                raise TypeError(f"{name} must be UUID or None")
        if self.source_frame_id is not None and self.source_effect_id is not None:
            raise ValueError("lane record source identity must have at most one owner")
        if self.key.section is LaneRecordSection.LOSS:
            if type(self.record.origin) is SystemOrigin and (
                self.source_frame_id is not None or self.source_effect_id is not None
            ):
                raise ValueError(
                    "system-derived LOSS lane record cannot have a source pointer"
                )
            if type(self.record.origin) is not SystemOrigin and (
                (self.source_frame_id is None) == (self.source_effect_id is None)
            ):
                raise ValueError(
                    "transport-derived LOSS lane record requires exactly one source pointer"
                )
        if self.key.section is LaneRecordSection.HELD and (
            self.source_frame_id is None or self.source_effect_id is not None
        ):
            raise ValueError("HELD lane record requires source_frame_id only")
        if self.key.section is LaneRecordSection.RECOVERED and (
            self.source_effect_id is None or self.source_frame_id is not None
        ):
            raise ValueError("RECOVERED lane record requires source_effect_id only")
        if type(self.canonical_bytes) is not int:
            raise TypeError("canonical_bytes must be int")
        if self.canonical_bytes <= 0:
            raise ValueError("canonical_bytes must be positive")


@dataclass(frozen=True, slots=True)
class ReadyRecord:
    ready_order: int
    record: EventRecord | LossRecord
    source_frame_id: UUID | None = None
    canonical_bytes: int = 0
    created_revision: int = field(default=0, compare=False)

    def __post_init__(self) -> None:
        if (
            type(self.record.origin) is SystemOrigin
            and self.source_frame_id is not None
        ):
            raise ValueError("SystemOrigin ReadyRecord source_frame_id must be None")


@dataclass(frozen=True, slots=True)
class StagedFrame:
    frame_id: UUID
    source_epoch: int
    request_id: int
    payload: bytes
    staged_revision: int = field(default=0, compare=False)


@dataclass(frozen=True, slots=True)
class JournalTransition:
    source_state: SourceState | None = None
    room_states: tuple[RoomState, ...] = ()
    room_lanes: tuple[RoomLane, ...] = ()
    lane_record_inserts: tuple[LaneRecord, ...] = ()
    lane_record_deletes: tuple[LaneRecordKey, ...] = ()
    ready_records: tuple[ReadyRecord, ...] = ()
    frames: tuple[StagedFrame, ...] = ()
    batches: tuple[SyncBatch, ...] = ()
    losses: tuple[LossRecord, ...] = ()
    delete_frame_ids: tuple[UUID, ...] = ()

    def __post_init__(self) -> None:
        if self.source_state is not None and type(self.source_state) is not SourceState:
            raise TypeError("source_state must be SourceState or None")
        fields = (
            ("room_states", self.room_states, RoomState),
            ("room_lanes", self.room_lanes, RoomLane),
            ("lane_record_inserts", self.lane_record_inserts, LaneRecord),
            ("lane_record_deletes", self.lane_record_deletes, LaneRecordKey),
            ("ready_records", self.ready_records, ReadyRecord),
            ("frames", self.frames, StagedFrame),
            ("batches", self.batches, SyncBatch),
            ("losses", self.losses, LossRecord),
            ("delete_frame_ids", self.delete_frame_ids, UUID),
        )
        for name, values, expected in fields:
            if type(values) is not tuple:
                raise TypeError(f"{name} must be a tuple")
            if any(type(value) is not expected for value in values):
                raise TypeError(f"{name} must contain {expected.__name__} values")


@dataclass(frozen=True, slots=True)
class CommitResult:
    revision: int
