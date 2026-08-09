import hashlib
from dataclasses import dataclass
from typing import Any
from uuid import UUID, uuid5

from .config import SlidingSourceConfig
from .model import RecordOrigin, TransportKind
from .ports import NetworkRequest, NetworkResult
from .source import (
    SourceResult,
    SourceResultKind,
    SyncFrame,
    canonical_json,
    load_json,
    malformed_success_result,
    normalize_source_error,
    validate_network_result_identity,
)
from .state import SourceState

RESERVED_ALL_ROOMS_LIST = "__nio_all_rooms_v1"
_RESERVED_TIMELINE_LIMIT = 1
_SLIDING_SYNC_PATH = "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
_RESERVED_REQUIRED_STATE = (
    ("m.room.member", "*"),
    ("m.room.encryption", ""),
    ("m.room.name", ""),
    ("m.room.canonical_alias", ""),
    ("m.room.topic", ""),
    ("m.room.avatar", ""),
    ("m.room.join_rules", ""),
    ("m.room.create", ""),
    ("m.room.guest_access", ""),
    ("m.room.power_levels", ""),
)
_ROOM_SCOPED_EXTENSIONS = ("account_data", "typing", "receipts")


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _optional_string(value: object, field_name: str) -> None:
    if value is not None:
        _require_exact(value, str, field_name)


@dataclass(frozen=True, slots=True)
class SlidingCursor:
    pos: str | None
    to_device_since: str | None
    connection_instance: UUID
    all_rooms_range_end: int
    all_rooms_coverage_complete: bool

    def __post_init__(self) -> None:
        _optional_string(self.pos, "pos")
        _optional_string(self.to_device_since, "to_device_since")
        _require_exact(self.connection_instance, UUID, "connection_instance")
        _require_exact(self.all_rooms_range_end, int, "all_rooms_range_end")
        _require_exact(
            self.all_rooms_coverage_complete,
            bool,
            "all_rooms_coverage_complete",
        )
        if self.all_rooms_range_end < 0:
            raise ValueError("all_rooms_range_end must be a nonnegative range end")
        if self.pos is None and self.all_rooms_coverage_complete:
            raise ValueError("a cursor without pos cannot have complete coverage")


@dataclass(frozen=True, slots=True)
class SlidingConnectionReset:
    cursor: SlidingCursor
    history_uncertain: bool

    def __post_init__(self) -> None:
        _require_exact(self.cursor, SlidingCursor, "cursor")
        _require_exact(self.history_uncertain, bool, "history_uncertain")
        if not self.history_uncertain:
            raise ValueError("a sliding connection reset must mark history uncertain")


def canonical_sliding_cursor(cursor: SlidingCursor) -> bytes:
    _require_exact(cursor, SlidingCursor, "cursor")
    return canonical_json(
        {
            "pos": cursor.pos,
            "to_device_since": cursor.to_device_since,
            "connection_instance": str(cursor.connection_instance),
            "all_rooms_range_end": cursor.all_rooms_range_end,
            "all_rooms_coverage_complete": cursor.all_rooms_coverage_complete,
        }
    )


def _sliding_cursor_from_json(data: bytes) -> SlidingCursor:
    value = load_json(data, "sliding cursor")
    if type(value) is not dict or set(value) != {
        "pos",
        "to_device_since",
        "connection_instance",
        "all_rooms_range_end",
        "all_rooms_coverage_complete",
    }:
        raise ValueError("sliding cursor has an invalid field set")
    instance = value["connection_instance"]
    if type(instance) is not str:
        raise ValueError("sliding cursor connection_instance must be a UUID string")
    try:
        connection_instance = UUID(instance)
    except ValueError as error:
        raise ValueError(
            "sliding cursor connection_instance must be a UUID string"
        ) from error
    try:
        return SlidingCursor(
            value["pos"],
            value["to_device_since"],
            connection_instance,
            value["all_rooms_range_end"],
            value["all_rooms_coverage_complete"],
        )
    except TypeError as error:
        raise ValueError(str(error)) from error


def reset_sliding_connection(
    cursor: SlidingCursor,
    new_connection_instance: UUID,
) -> SlidingConnectionReset:
    _require_exact(cursor, SlidingCursor, "cursor")
    _require_exact(new_connection_instance, UUID, "new_connection_instance")
    if new_connection_instance == cursor.connection_instance:
        raise ValueError("new_connection_instance must be new")
    return SlidingConnectionReset(
        SlidingCursor(
            None,
            cursor.to_device_since,
            new_connection_instance,
            cursor.all_rooms_range_end,
            False,
        ),
        history_uncertain=True,
    )


def _object(value: Any, field_name: str) -> dict[str, Any]:
    if type(value) is not dict:
        raise ValueError(f"{field_name} must be an object")
    return value


def _array(value: Any, field_name: str) -> list[Any]:
    if type(value) is not list:
        raise ValueError(f"{field_name} must be an array")
    return value


def _request_transaction_id(cursor: SlidingCursor) -> str:
    return str(
        uuid5(
            cursor.connection_instance,
            f"{RESERVED_ALL_ROOMS_LIST}:{cursor.all_rooms_range_end}",
        )
    )


def _connection_id(connection_name: str, connection_instance: UUID) -> str:
    return uuid5(connection_instance, connection_name).hex[:16]


@dataclass(frozen=True, slots=True)
class SlidingSource:
    stream_id: UUID
    config: SlidingSourceConfig
    bootstrap_range_size: int
    own_user_id: str

    def __post_init__(self) -> None:
        _require_exact(self.stream_id, UUID, "stream_id")
        _require_exact(self.config, SlidingSourceConfig, "config")
        _require_exact(
            self.bootstrap_range_size,
            int,
            "bootstrap_range_size",
        )
        _require_exact(self.own_user_id, str, "own_user_id")
        if self.bootstrap_range_size <= 0:
            raise ValueError("bootstrap_range_size must be positive")
        if not self.own_user_id:
            raise ValueError("own_user_id must not be empty")

    def initial_cursor(self, connection_instance: UUID) -> SlidingCursor:
        _require_exact(connection_instance, UUID, "connection_instance")
        return SlidingCursor(
            None,
            None,
            connection_instance,
            self.bootstrap_range_size - 1,
            False,
        )

    def plan_request(
        self,
        state: SourceState,
        request_id: int,
    ) -> NetworkRequest | None:
        _require_exact(state, SourceState, "state")
        if state.transport_kind is not TransportKind.SLIDING:
            raise ValueError("SlidingSource requires sliding source state")
        _require_exact(request_id, int, "request_id")
        if request_id != state.next_request_id:
            raise ValueError("request_id does not match source state")
        cursor = _sliding_cursor_from_json(state.cursor_json)
        request_cursor_json = canonical_sliding_cursor(cursor)
        if request_cursor_json != state.cursor_json:
            raise ValueError("state cursor_json must be a canonical sliding cursor")
        if not state.active:
            return None

        body = self._request_body(cursor)
        return NetworkRequest(
            stream_id=self.stream_id,
            transport=TransportKind.SLIDING,
            source_epoch=state.source_epoch,
            request_id=request_id,
            method="POST",
            path=_SLIDING_SYNC_PATH,
            query=self._request_query(cursor, self.config.timeout_ms),
            body=canonical_json(body),
            timeout_ms=self.config.timeout_ms,
            request_cursor_json=request_cursor_json,
        )

    def normalize(
        self,
        request: NetworkRequest,
        result: NetworkResult,
    ) -> SourceResult:
        self._validate_result_identity(request, result)
        error_result = normalize_source_error(request, result)
        if error_result is not None:
            return error_result
        try:
            frame = self._normalize_frame(request, result.body)
        except (TypeError, ValueError) as error:
            return malformed_success_result(request, result.body, error)
        return SourceResult(
            kind=SourceResultKind.FRAME,
            request=request,
            frame=frame,
            status_code=200,
            network_failure=None,
            error_code=None,
            retry_after_ms=None,
            response_body=b"",
            detail=None,
        )

    def _request_body(self, cursor: SlidingCursor) -> dict[str, Any]:
        lists = _object(load_json(self.config.lists_json, "lists_json"), "lists_json")
        if RESERVED_ALL_ROOMS_LIST in lists:
            raise ValueError(f"{RESERVED_ALL_ROOMS_LIST} is a reserved list name")
        subscriptions = _object(
            load_json(
                self.config.room_subscriptions_json,
                "room_subscriptions_json",
            ),
            "room_subscriptions_json",
        )
        extensions = _object(
            load_json(self.config.extensions_json, "extensions_json"),
            "extensions_json",
        )
        planned_lists = dict(lists)
        planned_lists[RESERVED_ALL_ROOMS_LIST] = self._reserved_list(cursor)
        return {
            "conn_id": _connection_id(
                self.config.connection_name,
                cursor.connection_instance,
            ),
            "txn_id": _request_transaction_id(cursor),
            "lists": planned_lists,
            "room_subscriptions": subscriptions,
            "extensions": self._planned_extensions(extensions, cursor),
        }

    @staticmethod
    def _reserved_list(cursor: SlidingCursor) -> dict[str, Any]:
        return {
            "ranges": [[0, cursor.all_rooms_range_end]],
            "sort": ["by_recency"],
            "required_state": [list(selector) for selector in _RESERVED_REQUIRED_STATE],
            "timeline_limit": _RESERVED_TIMELINE_LIMIT,
        }

    @staticmethod
    def _planned_extensions(
        configured: dict[str, Any],
        cursor: SlidingCursor,
    ) -> dict[str, Any]:
        planned = dict(configured)
        to_device = dict(_object(planned.get("to_device", {}), "extensions.to_device"))
        to_device["enabled"] = True
        if cursor.to_device_since is None:
            to_device.pop("since", None)
        else:
            to_device["since"] = cursor.to_device_since
        planned["to_device"] = to_device

        e2ee = dict(_object(planned.get("e2ee", {}), "extensions.e2ee"))
        e2ee["enabled"] = True
        planned["e2ee"] = e2ee
        for name in _ROOM_SCOPED_EXTENSIONS:
            extension = dict(_object(planned.get(name, {}), f"extensions.{name}"))
            extension["enabled"] = True
            scope = extension.get("lists")
            if scope == ["*"]:
                pass
            else:
                if scope is None:
                    scoped_lists: list[str] = []
                else:
                    scoped_lists = list(_array(scope, f"extensions.{name}.lists"))
                    if any(type(item) is not str for item in scoped_lists):
                        raise ValueError(
                            f"extensions.{name}.lists must contain strings"
                        )
                    if "*" in scoped_lists:
                        raise ValueError(
                            f"extensions.{name}.lists wildcard must be exactly ['*']"
                        )
                if RESERVED_ALL_ROOMS_LIST not in scoped_lists:
                    scoped_lists.append(RESERVED_ALL_ROOMS_LIST)
                extension["lists"] = scoped_lists
            planned[name] = extension
        return planned

    @staticmethod
    def _request_query(
        cursor: SlidingCursor,
        timeout_ms: int,
    ) -> tuple[tuple[str, str], ...]:
        query: list[tuple[str, str]] = []
        if cursor.pos is not None:
            query.append(("pos", cursor.pos))
        if timeout_ms:
            query.append(("timeout", str(timeout_ms)))
        return tuple(query)

    def _validate_result_identity(
        self,
        request: NetworkRequest,
        result: NetworkResult,
    ) -> None:
        _require_exact(request, NetworkRequest, "request")
        _require_exact(result, NetworkResult, "result")
        if request.transport is not TransportKind.SLIDING:
            raise ValueError("SlidingSource can normalize only sliding requests")
        if request.stream_id != self.stream_id:
            raise ValueError("sliding request stream does not match source stream")
        cursor = _sliding_cursor_from_json(request.request_cursor_json)
        if canonical_sliding_cursor(cursor) != request.request_cursor_json:
            raise ValueError("request_cursor_json must be a canonical sliding cursor")
        if (
            request.method != "POST"
            or request.path != _SLIDING_SYNC_PATH
            or request.body is None
            or request.query != self._request_query(cursor, request.timeout_ms)
        ):
            raise ValueError("request is not the planned sliding sync request")
        body = _object(
            load_json(request.body, "sliding request body"), "sliding request body"
        )
        if canonical_json(body) != request.body:
            raise ValueError("sliding request body must be canonical JSON")
        if set(body) != {
            "conn_id",
            "txn_id",
            "lists",
            "room_subscriptions",
            "extensions",
        }:
            raise ValueError("request is not the planned sliding sync request")
        conn_id = body["conn_id"]
        if type(conn_id) is not str or not conn_id or len(conn_id.encode("utf-8")) > 16:
            raise ValueError("sliding conn_id must be at most 16 UTF-8 bytes")
        if conn_id != _connection_id(
            self.config.connection_name,
            cursor.connection_instance,
        ):
            raise ValueError("sliding request conn_id does not match its connection")
        if body["txn_id"] != _request_transaction_id(cursor):
            raise ValueError("sliding request txn_id does not match its cursor")
        lists = _object(body["lists"], "sliding request lists")
        if lists.get(RESERVED_ALL_ROOMS_LIST) != self._reserved_list(cursor):
            raise ValueError("sliding request weakens its reserved list")
        _object(body["room_subscriptions"], "sliding request room_subscriptions")
        extensions = _object(body["extensions"], "sliding request extensions")
        to_device = _object(
            extensions.get("to_device"),
            "sliding request extensions.to_device",
        )
        if to_device.get("enabled") is not True:
            raise ValueError("sliding request must enable to_device")
        if to_device.get("since") != cursor.to_device_since:
            if cursor.to_device_since is not None or "since" in to_device:
                raise ValueError("sliding to-device since does not match its cursor")
        e2ee = _object(extensions.get("e2ee"), "sliding request extensions.e2ee")
        if e2ee.get("enabled") is not True:
            raise ValueError("sliding request must enable the e2ee extension")
        for name in _ROOM_SCOPED_EXTENSIONS:
            extension = _object(
                extensions.get(name),
                f"sliding request extensions.{name}",
            )
            if extension.get("enabled") is not True:
                raise ValueError(f"sliding request must enable the {name} extension")
            scope = _array(
                extension.get("lists"),
                f"sliding request extensions.{name}.lists",
            )
            if any(type(item) is not str for item in scope):
                raise ValueError(
                    f"sliding request extensions.{name}.lists must contain strings"
                )
            if "*" in scope and scope != ["*"]:
                raise ValueError(
                    f"sliding request {name} extension wildcard is not exact"
                )
            if scope != ["*"] and RESERVED_ALL_ROOMS_LIST not in scope:
                raise ValueError(
                    f"sliding request {name} extension excludes reserved coverage"
                )
        validate_network_result_identity(request, result)

    def _normalize_frame(self, request: NetworkRequest, body: bytes) -> SyncFrame:
        root = _object(
            load_json(body, "sliding sync response"), "sliding sync response"
        )
        pos = root.get("pos")
        if type(pos) is not str or not pos:
            raise ValueError("sliding response pos must be a nonempty string")
        response_txn = root.get("txn_id")
        if type(response_txn) is not str:
            raise ValueError("sliding response txn_id must be a string")
        request_body = _object(
            load_json(request.body or b"", "sliding request body"),
            "sliding request body",
        )
        request_txn = request_body["txn_id"]
        lists = _object(root.get("lists"), "sliding response lists")
        reserved = _object(
            lists.get(RESERVED_ALL_ROOMS_LIST),
            f"sliding response lists.{RESERVED_ALL_ROOMS_LIST}",
        )
        count = reserved.get("count")
        if type(count) is not int or count < 0:
            raise ValueError("reserved list count must be a nonnegative integer")

        extensions = _object(root.get("extensions", {}), "extensions")
        to_device = _object(extensions.get("to_device"), "extensions.to_device")
        next_to_device = to_device.get("next_batch")
        if type(next_to_device) is not str:
            raise ValueError("extensions.to_device.next_batch must be a string")
        to_device_events = _array(
            to_device.get("events", []),
            "extensions.to_device.events",
        )
        for event in to_device_events:
            _object(event, "extensions.to_device event")

        cursor = _sliding_cursor_from_json(request.request_cursor_json)
        end = cursor.all_rooms_range_end
        complete = cursor.all_rooms_coverage_complete
        growth = count > end + 1
        if response_txn == request_txn:
            if growth:
                end = min(count - 1, end + self.bootstrap_range_size)
                complete = False
            else:
                complete = True
        elif growth:
            complete = False
        candidate = SlidingCursor(
            pos,
            next_to_device,
            cursor.connection_instance,
            end,
            complete,
        )
        source_json = canonical_json(root)
        digest = hashlib.sha256(source_json).hexdigest()
        frame_id = uuid5(
            self.stream_id,
            f"{request.source_epoch}:{request.request_id}:{digest}",
        )
        return SyncFrame(
            frame_id=frame_id,
            origin=RecordOrigin(
                TransportKind.SLIDING,
                request.source_epoch,
                request.request_id,
                0,
            ),
            request_cursor_json=request.request_cursor_json,
            candidate_cursor_json=canonical_sliding_cursor(candidate),
            source_json=source_json,
            to_device_json=tuple(canonical_json(event) for event in to_device_events),
            device_list_delta_json=b'{"changed":[],"left":[]}',
            one_time_key_counts_json=b"{}",
            unused_fallback_key_types_json=b"null",
            room_segments=(),
            ephemeral_json=(),
            global_account_data_json=(),
            presence_json=(),
        )
