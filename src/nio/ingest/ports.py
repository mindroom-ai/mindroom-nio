from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from .model import TransportKind


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


@dataclass(frozen=True, slots=True)
class NetworkRequest:
    stream_id: UUID
    transport: TransportKind
    source_epoch: int
    request_id: int
    method: str
    path: str
    query: tuple[tuple[str, str], ...]
    body: bytes | None
    timeout_ms: int
    request_cursor_json: bytes

    def __post_init__(self) -> None:
        _require_exact(self.stream_id, UUID, "stream_id")
        _require_exact(self.transport, TransportKind, "transport")
        _require_exact(self.source_epoch, int, "source_epoch")
        _require_exact(self.request_id, int, "request_id")
        _require_exact(self.method, str, "method")
        _require_exact(self.path, str, "path")
        _require_exact(self.query, tuple, "query")
        for item in self.query:
            if type(item) is not tuple or len(item) != 2:
                raise TypeError("query elements must be two-item tuples")
            _require_exact(item[0], str, "query key")
            _require_exact(item[1], str, "query value")
        if self.body is not None:
            _require_exact(self.body, bytes, "body")
        _require_exact(self.timeout_ms, int, "timeout_ms")
        _require_exact(self.request_cursor_json, bytes, "request_cursor_json")
        if self.source_epoch < 0:
            raise ValueError("source_epoch must be nonnegative")
        if self.request_id < 0:
            raise ValueError("request_id must be nonnegative")
        if self.timeout_ms < 0:
            raise ValueError("timeout_ms must be nonnegative")
        if not self.method:
            raise ValueError("method must not be empty")
        if not self.path.startswith("/"):
            raise ValueError("path must be absolute")


class NetworkFailureKind(StrEnum):
    TIMEOUT = "timeout"
    CONNECTION = "connection"
    PROTOCOL = "protocol"


@dataclass(frozen=True, slots=True)
class NetworkResult:
    stream_id: UUID
    transport: TransportKind
    source_epoch: int
    request_id: int
    status_code: int | None
    body: bytes
    failure: NetworkFailureKind | None
    retry_after_ms: int | None

    def __post_init__(self) -> None:
        _require_exact(self.stream_id, UUID, "stream_id")
        _require_exact(self.transport, TransportKind, "transport")
        _require_exact(self.source_epoch, int, "source_epoch")
        _require_exact(self.request_id, int, "request_id")
        _require_exact(self.body, bytes, "body")
        if self.source_epoch < 0:
            raise ValueError("source_epoch must be nonnegative")
        if self.request_id < 0:
            raise ValueError("request_id must be nonnegative")

        if self.status_code is None:
            _require_exact(self.failure, NetworkFailureKind, "failure")
            if self.body:
                raise ValueError("transport failure body must be empty")
            if self.retry_after_ms is not None:
                raise ValueError("transport failures cannot carry Retry-After")
        else:
            _require_exact(self.status_code, int, "status_code")
            if self.failure is not None:
                raise ValueError("HTTP results cannot carry a transport failure")
            if not 100 <= self.status_code <= 599:
                raise ValueError("status_code must be between 100 and 599")

        if self.retry_after_ms is not None:
            _require_exact(self.retry_after_ms, int, "retry_after_ms")
            if self.retry_after_ms < 0:
                raise ValueError("retry_after_ms must be nonnegative")
