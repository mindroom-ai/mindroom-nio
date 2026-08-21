import hashlib
from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID, uuid5

from ._json import canonical_json, load_json
from .model import TransportKind

MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES = 16 * 1024 * 1024


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


def _frame_id_for_response(request: NetworkRequest, source_sha256: bytes) -> UUID:
    """Return the universal deterministic identity for one staged response."""
    _require_exact(request, NetworkRequest, "request")
    _require_exact(source_sha256, bytes, "source_sha256")
    if len(source_sha256) != 32:
        raise ValueError("source_sha256 must be exactly 32 bytes")
    return uuid5(
        request.stream_id,
        f"{request.source_epoch}:{request.request_id}:{source_sha256.hex()}",
    )


@dataclass(frozen=True, slots=True)
class StagedSourceResponse:
    """The one canonical successful Matrix response retained for restart."""

    request: NetworkRequest
    response_body: bytes
    source_sha256: bytes

    def __post_init__(self) -> None:
        _require_exact(self.request, NetworkRequest, "request")
        _require_exact(self.response_body, bytes, "response_body")
        if len(self.response_body) > MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES:
            raise ValueError("response_body exceeds 16 MiB")
        _require_exact(self.source_sha256, bytes, "source_sha256")
        if len(self.source_sha256) != 32:
            raise ValueError("source_sha256 must be exactly 32 bytes")
        if hashlib.sha256(self.response_body).digest() != self.source_sha256:
            raise ValueError("source_sha256 does not match response_body digest")
        try:
            value = load_json(self.response_body, "response body")
        except (TypeError, ValueError) as error:
            raise ValueError(
                "response_body must be canonical Matrix JSON object"
            ) from error
        if type(value) is not dict or canonical_json(value) != self.response_body:
            raise ValueError("response_body must be canonical Matrix JSON object")


def _revalidated_staged_source_response(
    response: StagedSourceResponse,
) -> StagedSourceResponse:
    """Deeply reconstruct a mutable-by-escape-hatch frozen carrier."""
    _require_exact(response, StagedSourceResponse, "response")
    request = response.request
    _require_exact(request, NetworkRequest, "request")
    request = NetworkRequest(
        request.stream_id,
        request.transport,
        request.source_epoch,
        request.request_id,
        request.method,
        request.path,
        request.query,
        request.body,
        request.timeout_ms,
        request.request_cursor_json,
    )
    return StagedSourceResponse(
        request,
        response.response_body,
        response.source_sha256,
    )


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
