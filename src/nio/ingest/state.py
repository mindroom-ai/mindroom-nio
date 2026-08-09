from dataclasses import dataclass, field
from enum import StrEnum
from uuid import UUID

from .model import (
    ConsumerBinding,
    EventRecord,
    LossRecord,
    RoomHydrationStatus,
    RoomSnapshot,
    SyncBatch,
    TransportKind,
)


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


@dataclass(frozen=True, slots=True)
class OwnerView:
    account_id: str
    device_id: str
    schema_version: int
    stream_id: UUID
    transport_kind: TransportKind
    binding_operation_id: UUID
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
    updated_revision: int = field(default=0, compare=False)

    def __post_init__(self) -> None:
        if type(self.room_id) is not str:
            raise TypeError("room_id must be str")
        if type(self.current_membership_epoch) is not int:
            raise TypeError("current_membership_epoch must be int")
        if type(self.next_room_sequence) is not int:
            raise TypeError("next_room_sequence must be int")
        if type(self.hydration_status) is not RoomHydrationStatus:
            raise TypeError("hydration_status must be RoomHydrationStatus")
        if self.snapshot is not None and type(self.snapshot) is not RoomSnapshot:
            raise TypeError("snapshot must be RoomSnapshot or None")
        if self.snapshot is not None and (
            self.snapshot.room_id != self.room_id
            or self.snapshot.membership_epoch != self.current_membership_epoch
        ):
            raise ValueError("snapshot room/epoch does not match RoomState")


@dataclass(frozen=True, slots=True)
class RoomLane:
    room_id: str
    membership_epoch: int
    lane_status: LaneStatus
    held_record_count: int = 0
    held_canonical_bytes: int = 0
    release_phase: ReleasePhase = ReleasePhase.IDLE
    release_loss_id: str | None = None
    ready_order: int | None = None
    next_held_ordinal: int = 0
    next_recovery_page: int = 0
    successor_membership_epoch: int | None = None
    pending_lifecycle: EventRecord | None = None
    updated_revision: int = field(default=0, compare=False)

    def __post_init__(self) -> None:
        if type(self.room_id) is not str:
            raise TypeError("room_id must be str")
        if type(self.membership_epoch) is not int:
            raise TypeError("membership_epoch must be int")
        if type(self.lane_status) is not LaneStatus:
            raise TypeError("lane_status must be LaneStatus")
        if type(self.release_phase) is not ReleasePhase:
            raise TypeError("release_phase must be ReleasePhase")
        if (
            self.pending_lifecycle is not None
            and type(self.pending_lifecycle) is not EventRecord
        ):
            raise TypeError("pending_lifecycle must be EventRecord or None")


@dataclass(frozen=True, slots=True)
class RoomAggregate:
    state: RoomState
    active_lane: RoomLane
    retiring_lanes: tuple[RoomLane, ...]


@dataclass(frozen=True, slots=True)
class ReadyRecord:
    ready_order: int
    record: EventRecord | LossRecord
    source_frame_id: UUID | None = None
    canonical_bytes: int = 0
    created_revision: int = field(default=0, compare=False)


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
    ready_records: tuple[ReadyRecord, ...] = ()
    frames: tuple[StagedFrame, ...] = ()
    batches: tuple[SyncBatch, ...] = ()
    losses: tuple[LossRecord, ...] = ()
    delete_frame_ids: tuple[UUID, ...] = ()


@dataclass(frozen=True, slots=True)
class CommitResult:
    revision: int
