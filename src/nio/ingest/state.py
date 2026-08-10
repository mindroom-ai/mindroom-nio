from dataclasses import dataclass, field
from uuid import UUID

from .model import TransportKind
from .ports import (
    StagedSourceResponse,
    _frame_id_for_response,
)


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _require_nonnegative(value: object, field_name: str) -> None:
    _require_exact(value, int, field_name)
    if value < 0:
        raise ValueError(f"{field_name} must be nonnegative")


@dataclass(frozen=True, slots=True)
class OwnerView:
    account_id: str
    device_id: str
    schema_version: int
    stream_id: UUID
    transport_kind: TransportKind
    revision: int
    writer_epoch: UUID
    next_source_epoch: int

    def __post_init__(self) -> None:
        _require_exact(self.account_id, str, "account_id")
        _require_exact(self.device_id, str, "device_id")
        _require_exact(self.schema_version, int, "schema_version")
        _require_exact(self.stream_id, UUID, "stream_id")
        _require_exact(self.transport_kind, TransportKind, "transport_kind")
        _require_nonnegative(self.revision, "revision")
        _require_exact(self.writer_epoch, UUID, "writer_epoch")
        _require_exact(self.next_source_epoch, int, "next_source_epoch")
        if not self.account_id:
            raise ValueError("account_id must not be empty")
        if not self.device_id:
            raise ValueError("device_id must not be empty")
        if self.schema_version != 1:
            raise ValueError("schema_version must be 1")
        if self.next_source_epoch < 1:
            raise ValueError("next_source_epoch must be positive")


@dataclass(frozen=True, slots=True)
class SourceState:
    source_epoch: int
    transport_kind: TransportKind
    cursor_json: bytes
    next_request_id: int
    active: bool

    def __post_init__(self) -> None:
        _require_nonnegative(self.source_epoch, "source_epoch")
        _require_exact(self.transport_kind, TransportKind, "transport_kind")
        _require_exact(self.cursor_json, bytes, "cursor_json")
        _require_nonnegative(self.next_request_id, "next_request_id")
        _require_exact(self.active, bool, "active")


@dataclass(frozen=True, slots=True)
class StagedFrame:
    frame_id: UUID
    response: StagedSourceResponse
    staged_revision: int = field(default=0, compare=False)

    def __post_init__(self) -> None:
        _require_exact(self.frame_id, UUID, "frame_id")
        _require_exact(self.response, StagedSourceResponse, "response")
        _require_nonnegative(self.staged_revision, "staged_revision")
        expected = _frame_id_for_response(
            self.response.request,
            self.response.source_sha256,
        )
        if self.frame_id != expected:
            raise ValueError("frame_id does not match staged source response")


@dataclass(frozen=True, slots=True)
class CommitResult:
    revision: int

    def __post_init__(self) -> None:
        _require_nonnegative(self.revision, "revision")
