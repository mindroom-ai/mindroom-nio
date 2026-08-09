import json
from dataclasses import dataclass
from enum import StrEnum
from typing import Any, Protocol, runtime_checkable
from uuid import UUID

from .model import RecordOrigin, TransportKind
from .ports import NetworkFailureKind, NetworkRequest, NetworkResult
from .state import SourceState

MATRIX_CANONICAL_INTEGER_MAX = (1 << 53) - 1
_MAX_JSON_CONTAINER_DEPTH = 257


def _reject_json_constant(value: str) -> None:
    raise ValueError(f"invalid JSON constant: {value}")


def _reject_json_float(value: str) -> None:
    raise ValueError(f"JSON floats are not canonical: {value}")


def _parse_json_integer(value: str) -> int:
    parsed = int(value)
    if not -MATRIX_CANONICAL_INTEGER_MAX <= parsed <= MATRIX_CANONICAL_INTEGER_MAX:
        raise ValueError("JSON integer exceeds the Matrix canonical range")
    return parsed


def _object_from_pairs(pairs: list[tuple[str, Any]]) -> dict[str, Any]:
    result: dict[str, Any] = {}
    for key, value in pairs:
        if key in result:
            raise ValueError(f"duplicate JSON key: {key}")
        result[key] = value
    return result


def _validate_json_nesting(text: str, field_name: str) -> None:
    depth = 0
    in_string = False
    escaped = False
    for character in text:
        if in_string:
            if escaped:
                escaped = False
            elif character == "\\":
                escaped = True
            elif character == '"':
                in_string = False
            continue
        if character == '"':
            in_string = True
        elif character in "[{":
            depth += 1
            if depth > _MAX_JSON_CONTAINER_DEPTH:
                raise ValueError(f"{field_name} exceeds the JSON nesting limit")
        elif character in "]}":
            depth -= 1


def load_json(data: bytes, field_name: str) -> Any:
    if type(data) is not bytes:
        raise TypeError(f"{field_name} must be bytes")
    try:
        text = data.decode("utf-8")
        _validate_json_nesting(text, field_name)
        return json.loads(
            text,
            parse_constant=_reject_json_constant,
            parse_float=_reject_json_float,
            parse_int=_parse_json_integer,
            object_pairs_hook=_object_from_pairs,
        )
    except (UnicodeDecodeError, json.JSONDecodeError, RecursionError) as error:
        raise ValueError(f"{field_name} must contain valid UTF-8 JSON") from error


def canonical_json(value: Any) -> bytes:
    try:
        return json.dumps(
            value,
            ensure_ascii=False,
            allow_nan=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode("utf-8")
    except RecursionError as error:
        raise ValueError("JSON value exceeds the canonical nesting limit") from error


def require_json_object(value: Any, field_name: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise ValueError(f"{field_name} must be an object")
    return value


def require_json_array(value: Any, field_name: str) -> list[Any]:
    if type(value) is not list:
        raise ValueError(f"{field_name} must be an array")
    return value


def require_json_events(value: Any, field_name: str) -> list[dict[str, Any]]:
    events = require_json_array(value, field_name)
    if any(type(event) is not dict for event in events):
        raise ValueError(f"{field_name} must contain event objects")
    return events


def require_json_strings(value: Any, field_name: str) -> list[str]:
    strings = require_json_array(value, field_name)
    if any(type(item) is not str for item in strings):
        raise ValueError(f"{field_name} must contain strings")
    return strings


def require_json_string(value: Any, field_name: str) -> str:
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a string")
    return value


def require_optional_json_string(value: Any, field_name: str) -> str | None:
    if value is not None and type(value) is not str:
        raise ValueError(f"{field_name} must be a string or null")
    return value


def require_json_bool(value: Any, field_name: str) -> bool:
    if type(value) is not bool:
        raise ValueError(f"{field_name} must be bool")
    return value


def require_json_event_container(
    value: Any,
    field_name: str,
) -> list[dict[str, Any]]:
    container = require_json_object(value, field_name)
    return require_json_events(container.get("events", []), f"{field_name}.events")


@dataclass(frozen=True, slots=True)
class ClassicCursor:
    next_batch: str | None

    def __post_init__(self) -> None:
        if self.next_batch is not None and type(self.next_batch) is not str:
            raise TypeError("next_batch must be str or None")


def canonical_classic_cursor(cursor: ClassicCursor) -> bytes:
    if type(cursor) is not ClassicCursor:
        raise TypeError("cursor must be ClassicCursor")
    return canonical_json({"next_batch": cursor.next_batch})


def _classic_cursor_from_json(data: bytes) -> ClassicCursor:
    value = load_json(data, "classic cursor")
    if type(value) is not dict or set(value) != {"next_batch"}:
        raise ValueError("classic cursor must contain only next_batch")
    next_batch = value["next_batch"]
    if next_batch is not None and type(next_batch) is not str:
        raise ValueError("classic cursor next_batch must be a string or null")
    return ClassicCursor(next_batch)


class RoomSection(StrEnum):
    INVITE = "invite"
    KNOCK = "knock"
    JOIN = "join"
    LEAVE = "leave"
    UNCHANGED = "unchanged"


class SourceResultKind(StrEnum):
    FRAME = "frame"
    RETRYABLE_ERROR = "retryable_error"
    TERMINAL_ERROR = "terminal_error"
    RESET_REQUIRED = "reset_required"


@runtime_checkable
class SyncSource(Protocol):
    def plan_request(
        self,
        state: SourceState,
        request_id: int,
    ) -> NetworkRequest | None: ...

    def normalize(
        self,
        request: NetworkRequest,
        result: NetworkResult,
    ) -> "SourceResult": ...


def _is_retryable_http_status(status_code: int) -> bool:
    return status_code in {408, 429} or status_code >= 500


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _require_json_bytes_tuple(value: object, field_name: str) -> None:
    _require_exact(value, tuple, field_name)
    if any(type(item) is not bytes for item in value):
        raise TypeError(f"{field_name} elements must be bytes")


@dataclass(frozen=True, slots=True)
class RoomSegment:
    room_id: str
    section: RoomSection
    state_json: tuple[bytes, ...]
    timeline_json: tuple[bytes, ...]
    room_account_data_json: tuple[bytes, ...]
    timeline_limited: bool
    timeline_prev_batch: str | None
    initial: bool
    expanded_timeline: bool
    live_event_count: int

    @property
    def history_discontinuity(self) -> bool:
        return self.timeline_limited or self.initial or self.expanded_timeline

    def __post_init__(self) -> None:
        _require_exact(self.room_id, str, "room_id")
        _require_exact(self.section, RoomSection, "section")
        _require_json_bytes_tuple(self.state_json, "state_json")
        _require_json_bytes_tuple(self.timeline_json, "timeline_json")
        _require_json_bytes_tuple(
            self.room_account_data_json,
            "room_account_data_json",
        )
        _require_exact(self.timeline_limited, bool, "timeline_limited")
        if self.timeline_prev_batch is not None:
            _require_exact(
                self.timeline_prev_batch,
                str,
                "timeline_prev_batch",
            )
        _require_exact(self.initial, bool, "initial")
        _require_exact(self.expanded_timeline, bool, "expanded_timeline")
        _require_exact(self.live_event_count, int, "live_event_count")
        if not self.room_id:
            raise ValueError("room_id must not be empty")
        if not 0 <= self.live_event_count <= len(self.timeline_json):
            raise ValueError("live_event_count must index the timeline suffix")
        if self.section is RoomSection.UNCHANGED and (
            self.state_json
            or self.timeline_json
            or not self.room_account_data_json
            or self.timeline_limited
            or self.timeline_prev_batch is not None
            or self.initial
            or self.expanded_timeline
            or self.live_event_count
        ):
            raise ValueError("unchanged room segments must be account-data-only")


@dataclass(frozen=True, slots=True)
class SyncFrame:
    frame_id: UUID
    origin: RecordOrigin
    request_cursor_json: bytes
    candidate_cursor_json: bytes
    source_json: bytes
    to_device_json: tuple[bytes, ...]
    device_list_delta_json: bytes
    one_time_key_counts_json: bytes
    unused_fallback_key_types_json: bytes
    room_segments: tuple[RoomSegment, ...]
    ephemeral_json: tuple[bytes, ...]
    global_account_data_json: tuple[bytes, ...]
    presence_json: tuple[bytes, ...]

    def __post_init__(self) -> None:
        _require_exact(self.frame_id, UUID, "frame_id")
        _require_exact(self.origin, RecordOrigin, "origin")
        _require_exact(self.request_cursor_json, bytes, "request_cursor_json")
        _require_exact(self.candidate_cursor_json, bytes, "candidate_cursor_json")
        _require_exact(self.source_json, bytes, "source_json")
        _require_json_bytes_tuple(self.to_device_json, "to_device_json")
        _require_exact(
            self.device_list_delta_json,
            bytes,
            "device_list_delta_json",
        )
        _require_exact(
            self.one_time_key_counts_json,
            bytes,
            "one_time_key_counts_json",
        )
        _require_exact(
            self.unused_fallback_key_types_json,
            bytes,
            "unused_fallback_key_types_json",
        )
        _require_exact(self.room_segments, tuple, "room_segments")
        if any(type(segment) is not RoomSegment for segment in self.room_segments):
            raise TypeError("room_segments elements must be RoomSegment")
        _require_json_bytes_tuple(self.ephemeral_json, "ephemeral_json")
        _require_json_bytes_tuple(
            self.global_account_data_json,
            "global_account_data_json",
        )
        _require_json_bytes_tuple(self.presence_json, "presence_json")
        if self.origin.frame_index != 0:
            raise ValueError("frame origin frame_index must be zero")
        room_ids = tuple(segment.room_id for segment in self.room_segments)
        if len(room_ids) != len(set(room_ids)):
            raise ValueError("a room may appear in only one frame section")


@dataclass(frozen=True, slots=True)
class SourceResult:
    kind: SourceResultKind
    request: NetworkRequest
    frame: SyncFrame | None
    status_code: int | None
    network_failure: NetworkFailureKind | None
    error_code: str | None
    retry_after_ms: int | None
    response_body: bytes
    detail: str | None

    def __post_init__(self) -> None:
        _require_exact(self.kind, SourceResultKind, "kind")
        _require_exact(self.request, NetworkRequest, "request")
        _require_exact(self.response_body, bytes, "response_body")
        if self.status_code is not None:
            _require_exact(self.status_code, int, "status_code")
            if not 100 <= self.status_code <= 599:
                raise ValueError("status_code must be between 100 and 599")
        if self.network_failure is not None:
            _require_exact(
                self.network_failure,
                NetworkFailureKind,
                "network_failure",
            )
        if self.error_code is not None:
            _require_exact(self.error_code, str, "error_code")
        if self.retry_after_ms is not None:
            _require_exact(self.retry_after_ms, int, "retry_after_ms")
            if self.retry_after_ms < 0:
                raise ValueError("retry_after_ms must be nonnegative")
        if self.detail is not None:
            _require_exact(self.detail, str, "detail")

        if self.kind is SourceResultKind.FRAME:
            _require_exact(self.frame, SyncFrame, "frame")
            if (
                self.status_code != 200
                or self.network_failure is not None
                or self.error_code is not None
                or self.retry_after_ms is not None
                or self.response_body
                or self.detail is not None
            ):
                raise ValueError("frame result contains error metadata")
            if (
                self.frame.origin.transport is not self.request.transport
                or self.frame.origin.source_epoch != self.request.source_epoch
                or self.frame.origin.request_id != self.request.request_id
                or self.frame.request_cursor_json != self.request.request_cursor_json
            ):
                raise ValueError("frame does not match its network request")
            return

        if self.frame is not None:
            raise ValueError("error result cannot contain a frame")
        if (self.status_code is None) == (self.network_failure is None):
            raise ValueError("error result requires exactly one failure source")
        if self.network_failure is not None and (
            self.response_body
            or self.error_code is not None
            or self.retry_after_ms is not None
        ):
            raise ValueError("network error classification has HTTP metadata")

        if self.kind is SourceResultKind.RETRYABLE_ERROR:
            valid = (
                self.network_failure
                in {NetworkFailureKind.TIMEOUT, NetworkFailureKind.CONNECTION}
                if self.network_failure is not None
                else _is_retryable_http_status(self.status_code)  # type: ignore[arg-type]
            )
        elif self.kind is SourceResultKind.TERMINAL_ERROR:
            valid = (
                self.network_failure is NetworkFailureKind.PROTOCOL
                if self.network_failure is not None
                else not _is_retryable_http_status(self.status_code)  # type: ignore[arg-type]
            ) and self.retry_after_ms is None
        else:
            valid = (
                self.status_code == 400
                and self.network_failure is None
                and self.request.transport is TransportKind.SLIDING
                and self.error_code == "M_UNKNOWN_POS"
                and self.retry_after_ms is None
            )
        if not valid:
            raise ValueError("source result contradicts its error classification")


def canonical_json_or_raw(data: bytes) -> bytes:
    try:
        return canonical_json(load_json(data, "response body"))
    except (TypeError, ValueError):
        return data


def validate_network_result_identity(
    request: NetworkRequest,
    result: NetworkResult,
) -> None:
    _require_exact(request, NetworkRequest, "request")
    _require_exact(result, NetworkResult, "result")
    if (
        result.stream_id != request.stream_id
        or result.transport is not request.transport
        or result.source_epoch != request.source_epoch
        or result.request_id != request.request_id
    ):
        raise ValueError("network result does not match its request")


def normalize_source_error(
    request: NetworkRequest,
    result: NetworkResult,
) -> SourceResult | None:
    """Normalize transport/non-200 failures shared by every sync source."""
    validate_network_result_identity(request, result)
    if result.failure is not None:
        kind = (
            SourceResultKind.RETRYABLE_ERROR
            if result.failure
            in {NetworkFailureKind.TIMEOUT, NetworkFailureKind.CONNECTION}
            else SourceResultKind.TERMINAL_ERROR
        )
        return SourceResult(
            kind=kind,
            request=request,
            frame=None,
            status_code=None,
            network_failure=result.failure,
            error_code=None,
            retry_after_ms=None,
            response_body=result.body,
            detail=f"network {result.failure.value}",
        )

    assert result.status_code is not None
    if result.status_code == 200:
        return None

    parsed: Any = None
    try:
        parsed = load_json(result.body, "response body")
        response_body = canonical_json(parsed)
    except (TypeError, ValueError):
        response_body = result.body

    error_code = None
    body_retry_after = None
    if type(parsed) is dict:
        value = parsed.get("errcode")
        if type(value) is str:
            error_code = value
        retry_value = parsed.get("retry_after_ms")
        if type(retry_value) is int and retry_value >= 0:
            body_retry_after = retry_value

    reset_required = (
        request.transport is TransportKind.SLIDING
        and result.status_code == 400
        and error_code == "M_UNKNOWN_POS"
    )
    retryable = _is_retryable_http_status(result.status_code)
    retry_after_ms = None
    if retryable:
        retry_after_ms = (
            result.retry_after_ms
            if result.retry_after_ms is not None
            else body_retry_after
        )
    return SourceResult(
        kind=(
            SourceResultKind.RESET_REQUIRED
            if reset_required
            else (
                SourceResultKind.RETRYABLE_ERROR
                if retryable
                else SourceResultKind.TERMINAL_ERROR
            )
        ),
        request=request,
        frame=None,
        status_code=result.status_code,
        network_failure=None,
        error_code=error_code,
        retry_after_ms=retry_after_ms,
        response_body=response_body,
        detail=(
            "source reset required" if reset_required else f"HTTP {result.status_code}"
        ),
    )


def malformed_success_result(
    request: NetworkRequest,
    body: bytes,
    error: TypeError | ValueError,
) -> SourceResult:
    return SourceResult(
        kind=SourceResultKind.TERMINAL_ERROR,
        request=request,
        frame=None,
        status_code=200,
        network_failure=None,
        error_code=None,
        retry_after_ms=None,
        response_body=canonical_json_or_raw(body),
        detail=str(error),
    )
