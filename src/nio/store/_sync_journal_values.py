from __future__ import annotations

from dataclasses import dataclass, fields
from enum import StrEnum
from typing import NamedTuple
from uuid import UUID

from ..ingest.model import RoomSnapshot
from ..ingest.reducer import HydrationIntent, RoomContinuity

SQLITE_INT_MAX = 2**63 - 1


class DeliveryState(NamedTuple):
    next_sequence: int
    acknowledged_sha256: bytes | None
    outstanding_work_id: str | None
    outstanding_ready_revision: int | None
    outstanding_ready_ordinal: int | None
    outstanding_batch_sha256: bytes | None


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _require_bounded_positive(
    value: object,
    field_name: str,
    ceiling: int,
) -> None:
    if type(value) is not int:
        raise TypeError(f"{field_name} must be int")
    if not 1 <= value <= ceiling:
        raise ValueError(f"{field_name} must be between 1 and {ceiling}")


class MaterializeStatus(StrEnum):
    IDLE = "idle"
    AT_CAPACITY = "at_capacity"
    BLOCKED = "blocked"
    MATERIALIZED = "materialized"


@dataclass(frozen=True, slots=True)
class MaterializerLimits:
    max_record_canonical_bytes: int = 1 * 1024 * 1024
    max_held_work_count: int = 10_000
    max_held_work_canonical_bytes: int = 32 * 1024 * 1024
    max_ready_work_count: int = 2_048
    max_ready_work_canonical_bytes: int = 16 * 1024 * 1024
    max_total_work_count: int = 20_000
    max_total_work_canonical_bytes: int = 64 * 1024 * 1024

    def __post_init__(self) -> None:
        for field, ceiling in zip(
            fields(self),
            (
                1 * 1024 * 1024,
                10_000,
                32 * 1024 * 1024,
                20_000,
                64 * 1024 * 1024,
                20_000,
                64 * 1024 * 1024,
            ),
            strict=True,
        ):
            _require_bounded_positive(getattr(self, field.name), field.name, ceiling)


@dataclass(frozen=True, slots=True)
class MaterializeResult:
    status: MaterializeStatus
    frame_id: UUID | None
    revision: int | None

    def __post_init__(self) -> None:
        _require_exact(self.status, MaterializeStatus, "status")
        if self.frame_id is not None:
            _require_exact(self.frame_id, UUID, "frame_id")
        if self.revision is not None:
            _require_exact(self.revision, int, "revision")
            if self.revision < 1:
                raise ValueError("revision must be positive")

        if self.status is MaterializeStatus.IDLE:
            if self.frame_id is not None or self.revision is not None:
                raise ValueError("idle materialization has no frame or revision")
        elif self.status in (MaterializeStatus.AT_CAPACITY, MaterializeStatus.BLOCKED):
            if self.frame_id is None or self.revision is not None:
                raise ValueError(f"{self.status.value.replace('_', '-')} materialization has only a frame")  # fmt: skip
        elif self.frame_id is None or self.revision is None:
            raise ValueError("materialized result requires a frame and revision")


@dataclass(frozen=True, slots=True)
class RoomAggregateValue:
    continuity: RoomContinuity
    next_room_sequence: int
    updated_revision: int
    pending_hydration: HydrationIntent | None
    room_snapshot: RoomSnapshot | None = None

    def __post_init__(self) -> None:
        _require_exact(self.continuity, RoomContinuity, "continuity")
        _require_exact(self.next_room_sequence, int, "next_room_sequence")
        _require_exact(self.updated_revision, int, "updated_revision")
        if self.pending_hydration is not None:
            _require_exact(
                self.pending_hydration,
                HydrationIntent,
                "pending_hydration",
            )
        if self.room_snapshot is not None:
            _require_exact(self.room_snapshot, RoomSnapshot, "room_snapshot")
        if self.next_room_sequence < 0:
            raise ValueError("next_room_sequence must be nonnegative")
        if self.updated_revision < 1:
            raise ValueError("updated_revision must be positive")

        gap = self.continuity.gap
        hydration_id = self.continuity.hydration_id
        if gap is not None:
            if self.pending_hydration is not None:
                raise ValueError("recovery and hydration intents are exclusive")
        elif hydration_id is None:
            if self.pending_hydration is not None:
                raise ValueError("pending hydration requires a hydration barrier")
        elif self.pending_hydration is None or (
            self.pending_hydration.hydration_id != hydration_id
        ):
            raise ValueError("hydration barrier and pending intent must agree")
