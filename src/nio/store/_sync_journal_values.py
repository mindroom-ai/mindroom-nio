from __future__ import annotations

from dataclasses import dataclass, fields
from enum import StrEnum
from uuid import UUID


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
    MATERIALIZED = "materialized"


@dataclass(frozen=True, slots=True)
class MaterializerLimits:
    max_record_canonical_bytes: int = 1 * 1024 * 1024
    max_held_records_per_room: int = 10_000
    max_held_canonical_bytes_per_room: int = 32 * 1024 * 1024
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
                2_048,
                16 * 1024 * 1024,
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
        elif self.status is MaterializeStatus.AT_CAPACITY:
            if self.frame_id is None or self.revision is not None:
                raise ValueError("at-capacity materialization has only a frame")
        elif self.frame_id is None or self.revision is None:
            raise ValueError("materialized result requires a frame and revision")
