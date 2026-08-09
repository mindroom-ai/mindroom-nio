import hashlib
from dataclasses import dataclass
from enum import StrEnum
from typing import Any
from uuid import UUID, uuid5

from .config import SlidingSourceConfig
from .membership import MembershipObservation
from .model import RecordOrigin, TransportKind
from .ports import NetworkRequest, NetworkResult
from .source import (
    RoomSection,
    RoomSegment,
    SourceResult,
    SourceResultKind,
    SyncFrame,
    canonical_json,
    load_json,
    malformed_success_result,
    normalize_source_error,
    require_json_events,
    require_json_strings,
    validate_network_result_identity,
)
from .source import (
    require_json_array as _array,
)
from .source import (
    require_json_bool as _boolean,
)
from .source import (
    require_json_object as _object,
)
from .source import (
    require_optional_json_string as _optional_response_string,
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
_TO_DEVICE_LIMIT = 100


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _optional_string(value: object, field_name: str) -> None:
    if value is not None:
        _require_exact(value, str, field_name)


class SlidingRangeAckMode(StrEnum):
    UNKNOWN = "unknown"
    TXN_ECHO = "txn_echo"
    RESPONSE_BOUND = "response_bound"


@dataclass(frozen=True, slots=True)
class SlidingCursor:
    pos: str | None
    to_device_since: str | None
    connection_instance: UUID
    connection_name: str
    all_rooms_range_end: int
    all_rooms_page_size: int
    all_rooms_range_ack_mode: SlidingRangeAckMode
    all_rooms_coverage_complete: bool

    def __post_init__(self) -> None:
        _optional_string(self.pos, "pos")
        _optional_string(self.to_device_since, "to_device_since")
        _require_exact(self.connection_instance, UUID, "connection_instance")
        _require_exact(self.connection_name, str, "connection_name")
        _require_exact(self.all_rooms_range_end, int, "all_rooms_range_end")
        _require_exact(self.all_rooms_page_size, int, "all_rooms_page_size")
        _require_exact(
            self.all_rooms_range_ack_mode,
            SlidingRangeAckMode,
            "all_rooms_range_ack_mode",
        )
        _require_exact(
            self.all_rooms_coverage_complete,
            bool,
            "all_rooms_coverage_complete",
        )
        if self.all_rooms_range_end < 0:
            raise ValueError("all_rooms_range_end must be a nonnegative range end")
        if not self.connection_name:
            raise ValueError("connection_name must not be empty")
        if self.all_rooms_page_size <= 0:
            raise ValueError("all_rooms_page_size must be positive")
        if self.pos is None and self.all_rooms_coverage_complete:
            raise ValueError("a cursor without pos cannot have complete coverage")
        if (
            self.pos is None
            and self.all_rooms_range_ack_mode is not SlidingRangeAckMode.UNKNOWN
        ):
            raise ValueError("a cursor without pos must have unknown range ack mode")


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
            "connection_name": cursor.connection_name,
            "all_rooms_range_end": cursor.all_rooms_range_end,
            "all_rooms_page_size": cursor.all_rooms_page_size,
            "all_rooms_range_ack_mode": cursor.all_rooms_range_ack_mode.value,
            "all_rooms_coverage_complete": cursor.all_rooms_coverage_complete,
        }
    )


def _sliding_cursor_from_json(data: bytes) -> SlidingCursor:
    value = load_json(data, "sliding cursor")
    if type(value) is not dict or set(value) != {
        "pos",
        "to_device_since",
        "connection_instance",
        "connection_name",
        "all_rooms_range_end",
        "all_rooms_page_size",
        "all_rooms_range_ack_mode",
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
    ack_mode = value["all_rooms_range_ack_mode"]
    if type(ack_mode) is not str:
        raise ValueError("sliding cursor all_rooms_range_ack_mode must be a string")
    try:
        parsed_ack_mode = SlidingRangeAckMode(ack_mode)
    except ValueError as error:
        raise ValueError("sliding cursor has an unknown range ack mode") from error
    try:
        return SlidingCursor(
            value["pos"],
            value["to_device_since"],
            connection_instance,
            value["connection_name"],
            value["all_rooms_range_end"],
            value["all_rooms_page_size"],
            parsed_ack_mode,
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
            cursor.connection_name,
            cursor.all_rooms_range_end,
            cursor.all_rooms_page_size,
            SlidingRangeAckMode.UNKNOWN,
            False,
        ),
        history_uncertain=True,
    )


def _request_transaction_id(cursor: SlidingCursor) -> str:
    return str(
        uuid5(
            cursor.connection_instance,
            f"{RESERVED_ALL_ROOMS_LIST}:{cursor.all_rooms_range_end}",
        )
    )


def _connection_id(connection_name: str, connection_instance: UUID) -> str:
    return uuid5(connection_instance, connection_name).hex[:16]


_ROOM_SECTION_ORDER = (
    RoomSection.INVITE,
    RoomSection.KNOCK,
    RoomSection.JOIN,
    RoomSection.LEAVE,
    RoomSection.UNCHANGED,
)


def _section_for_membership(membership: Any) -> RoomSection:
    if type(membership) is not str:
        raise ValueError("room membership must be a string")
    try:
        return {
            "invite": RoomSection.INVITE,
            "knock": RoomSection.KNOCK,
            "join": RoomSection.JOIN,
            "leave": RoomSection.LEAVE,
            "ban": RoomSection.LEAVE,
        }[membership]
    except KeyError as error:
        raise ValueError(f"unknown room membership: {membership}") from error


@dataclass(frozen=True, slots=True)
class _OwnMembershipEvidence:
    membership: str | None
    event_id: str | None
    previous_membership: str | None
    replaces_state: str | None
    is_live: bool
    is_unparsed: bool


def _latest_own_membership(
    state_events: list[dict[str, Any]],
    timeline_events: list[dict[str, Any]],
    live_event_count: int,
    own_user_id: str,
) -> _OwnMembershipEvidence | None:
    candidates: list[tuple[dict[str, Any], bool]] = [
        (event, False) for event in state_events
    ]
    live_timeline = timeline_events[-live_event_count:] if live_event_count else ()
    candidates.extend((event, True) for event in live_timeline)
    for event, is_live in reversed(candidates):
        if (
            event.get("type") != "m.room.member"
            or event.get("state_key") != own_user_id
        ):
            continue
        if "content" not in event:
            return _OwnMembershipEvidence(None, None, None, None, is_live, True)
        content = _object(event["content"], "own membership event content")
        membership = content.get("membership")
        if membership is not None and type(membership) is not str:
            raise ValueError("own membership event membership must be a string")
        event_id = event.get("event_id")
        if event_id is not None and type(event_id) is not str:
            raise ValueError("own membership event event_id must be a string")
        unsigned = _object(event.get("unsigned", {}), "own membership event unsigned")
        prev_content = _object(
            unsigned.get("prev_content", {}),
            "own membership event unsigned.prev_content",
        )
        return _OwnMembershipEvidence(
            membership,
            event_id,
            prev_content.get("membership"),
            unsigned.get("replaces_state"),
            is_live,
            membership is None,
        )
    return None


def _room_section(
    membership: Any,
    has_stripped_state: bool,
    own_membership: _OwnMembershipEvidence | None,
) -> RoomSection:
    derived = (
        None
        if own_membership is None or own_membership.is_unparsed
        else _section_for_membership(own_membership.membership)
    )
    if membership is None:
        if derived is not None:
            return derived
        if has_stripped_state:
            raise ValueError("stripped room has no parseable own membership evidence")
        return RoomSection.JOIN
    explicit = _section_for_membership(membership)
    if derived is not None and explicit is not derived:
        raise ValueError("room membership contradicts exact own membership evidence")
    return explicit


def _membership_observation(
    segment: RoomSegment,
    own_membership: _OwnMembershipEvidence | None,
) -> MembershipObservation:
    room_membership = {
        RoomSection.INVITE: "invite",
        RoomSection.KNOCK: "knock",
        RoomSection.JOIN: "join",
        RoomSection.LEAVE: "leave",
        RoomSection.UNCHANGED: None,
    }[segment.section]
    if own_membership is None:
        return MembershipObservation(
            room_membership=room_membership,
            event_membership=None,
            event_id=None,
            previous_membership=None,
            replaces_state=None,
            is_live=False,
            is_initial=segment.initial,
            is_expanded_timeline=segment.expanded_timeline,
            is_unparsed=False,
        )
    return MembershipObservation(
        room_membership=room_membership,
        event_membership=own_membership.membership,
        event_id=own_membership.event_id,
        previous_membership=own_membership.previous_membership,
        replaces_state=own_membership.replaces_state,
        is_live=own_membership.is_live,
        is_initial=segment.initial,
        is_expanded_timeline=segment.expanded_timeline,
        is_unparsed=own_membership.is_unparsed,
    )


def sliding_membership_observation(
    segment: RoomSegment,
    own_user_id: str,
) -> MembershipObservation:
    _require_exact(segment, RoomSegment, "segment")
    _require_exact(own_user_id, str, "own_user_id")
    if not own_user_id:
        raise ValueError("own_user_id must not be empty")

    state_events = [
        _object(load_json(event, "room state event"), "room state event")
        for event in segment.state_json
    ]
    timeline_events = [
        _object(
            load_json(event, "room timeline event"),
            "room timeline event",
        )
        for event in segment.timeline_json
    ]
    return _membership_observation(
        segment,
        _latest_own_membership(
            state_events,
            timeline_events,
            segment.live_event_count,
            own_user_id,
        ),
    )


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
            self.config.connection_name,
            self.bootstrap_range_size - 1,
            self.bootstrap_range_size,
            SlidingRangeAckMode.UNKNOWN,
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
        if len(lists) > 99:
            raise ValueError("caller lists must contain at most 99 entries")
        subscriptions = _object(
            load_json(
                self.config.room_subscriptions_json,
                "room_subscriptions_json",
            ),
            "room_subscriptions_json",
        )
        if len(subscriptions) > 100:
            raise ValueError("room_subscriptions must contain at most 100 entries")
        extensions = _object(
            load_json(self.config.extensions_json, "extensions_json"),
            "extensions_json",
        )
        planned_lists = dict(lists)
        planned_lists[RESERVED_ALL_ROOMS_LIST] = self._reserved_list(cursor)
        return {
            "conn_id": _connection_id(
                cursor.connection_name,
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
        to_device["limit"] = _TO_DEVICE_LIMIT
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
                scoped_lists = [RESERVED_ALL_ROOMS_LIST]
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
            cursor.connection_name, cursor.connection_instance
        ):
            raise ValueError("sliding request conn_id does not match its connection")
        if body["txn_id"] != _request_transaction_id(cursor):
            raise ValueError("sliding request txn_id does not match its cursor")
        lists = _object(body["lists"], "sliding request lists")
        if len(lists) > 100:
            raise ValueError("sliding request exceeds the 100-list wire limit")
        if lists.get(RESERVED_ALL_ROOMS_LIST) != self._reserved_list(cursor):
            raise ValueError("sliding request weakens its reserved list")
        subscriptions = _object(
            body["room_subscriptions"],
            "sliding request room_subscriptions",
        )
        if len(subscriptions) > 100:
            raise ValueError("sliding request exceeds the room subscription limit")
        extensions = _object(body["extensions"], "sliding request extensions")
        to_device = _object(
            extensions.get("to_device"),
            "sliding request extensions.to_device",
        )
        if to_device.get("enabled") is not True:
            raise ValueError("sliding request must enable to_device")
        if to_device.get("limit") != _TO_DEVICE_LIMIT:
            raise ValueError("sliding request must use the internal to-device limit")
        if cursor.to_device_since is None:
            if "since" in to_device:
                raise ValueError("sliding to-device since does not match its cursor")
        elif to_device.get("since") != cursor.to_device_since:
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
            if "*" in scope:
                raise ValueError(
                    f"sliding request {name} extension cannot use a literal wildcard"
                )
            if RESERVED_ALL_ROOMS_LIST not in scope:
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
        response_txn_missing = "txn_id" not in root
        response_txn = root.get("txn_id")
        if not response_txn_missing and type(response_txn) is not str:
            raise ValueError("sliding response txn_id must be a string when present")
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

        cursor = _sliding_cursor_from_json(request.request_cursor_json)
        extensions = _object(root.get("extensions", {}), "extensions")
        if "to_device" not in extensions:
            next_to_device = cursor.to_device_since
            to_device_events: list[dict[str, Any]] = []
        else:
            to_device = _object(
                extensions["to_device"],
                "extensions.to_device",
            )
            next_to_device = to_device.get("next_batch")
            if type(next_to_device) is not str:
                raise ValueError("extensions.to_device.next_batch must be a string")
            to_device_events = require_json_events(
                _array(
                    to_device.get("events", []),
                    "extensions.to_device.events",
                ),
                "extensions.to_device.events",
            )

        ack_mode = cursor.all_rooms_range_ack_mode
        if ack_mode is SlidingRangeAckMode.UNKNOWN:
            if response_txn_missing:
                next_ack_mode = SlidingRangeAckMode.RESPONSE_BOUND
                request_applied = True
            else:
                next_ack_mode = SlidingRangeAckMode.TXN_ECHO
                request_applied = response_txn == request_txn
        elif ack_mode is SlidingRangeAckMode.TXN_ECHO:
            if response_txn_missing:
                raise ValueError("sliding response txn_id is missing after negotiation")
            next_ack_mode = SlidingRangeAckMode.TXN_ECHO
            request_applied = response_txn == request_txn
        else:
            if response_txn_missing:
                next_ack_mode = SlidingRangeAckMode.RESPONSE_BOUND
                request_applied = True
            else:
                next_ack_mode = SlidingRangeAckMode.TXN_ECHO
                request_applied = response_txn == request_txn
        end = cursor.all_rooms_range_end
        complete = cursor.all_rooms_coverage_complete
        growth = count > end
        if request_applied:
            if growth:
                end = min(count, end + cursor.all_rooms_page_size)
                complete = False
            else:
                complete = True
        elif growth:
            complete = False
        candidate = SlidingCursor(
            pos,
            next_to_device,
            cursor.connection_instance,
            cursor.connection_name,
            end,
            cursor.all_rooms_page_size,
            next_ack_mode,
            complete,
        )

        account_data = _object(
            extensions.get("account_data", {}),
            "extensions.account_data",
        )
        global_account_data = require_json_events(
            account_data.get("global", []),
            "extensions.account_data.global",
        )
        account_data_rooms = _object(
            account_data.get("rooms", {}),
            "extensions.account_data.rooms",
        )
        room_account_data: dict[str, list[dict[str, Any]]] = {}
        for room_id, events in account_data_rooms.items():
            self._validate_room_id(room_id)
            room_account_data[room_id] = require_json_events(
                events,
                f"extensions.account_data.rooms.{room_id}",
            )

        rooms = _object(root.get("rooms", {}), "rooms")
        by_section: dict[RoomSection, list[RoomSegment]] = {
            section: [] for section in _ROOM_SECTION_ORDER
        }
        seen_room_ids: set[str] = set()
        for room_id in sorted(rooms):
            self._validate_room_id(room_id)
            room = _object(rooms[room_id], f"rooms.{room_id}")
            required_state = require_json_events(
                room.get("required_state", []),
                f"rooms.{room_id}.required_state",
            )
            timeline = require_json_events(
                room.get("timeline", []),
                f"rooms.{room_id}.timeline",
            )
            stripped_key = (
                "stripped_state" if "stripped_state" in room else "invite_state"
            )
            stripped_state = require_json_events(
                room.get(stripped_key, []),
                f"rooms.{room_id}.{stripped_key}",
            )
            initial = (
                _boolean(
                    room.get("initial", False),
                    f"rooms.{room_id}.initial",
                )
                or cursor.pos is None
            )
            expanded = self._expanded_timeline(room, room_id)
            limited = _boolean(
                room.get("limited", False),
                f"rooms.{room_id}.limited",
            )
            prev_batch = _optional_response_string(
                room.get("prev_batch"),
                f"rooms.{room_id}.prev_batch",
            )
            live_event_count = self._live_event_count(
                room.get("num_live"),
                len(timeline),
                initial=initial,
                expanded=expanded,
                room_id=room_id,
            )
            own_membership = _latest_own_membership(
                [*required_state, *stripped_state],
                timeline,
                live_event_count,
                self.own_user_id,
            )
            section = _room_section(
                room.get("membership"),
                bool(stripped_state),
                own_membership,
            )
            state = (
                stripped_state
                if section in {RoomSection.INVITE, RoomSection.KNOCK} and stripped_state
                else required_state
            )
            segment = RoomSegment(
                room_id=room_id,
                section=section,
                state_json=tuple(canonical_json(event) for event in state),
                timeline_json=tuple(canonical_json(event) for event in timeline),
                room_account_data_json=tuple(
                    canonical_json(event)
                    for event in room_account_data.get(room_id, ())
                ),
                timeline_limited=limited,
                timeline_prev_batch=prev_batch,
                initial=initial,
                expanded_timeline=expanded,
                live_event_count=live_event_count,
            )
            _membership_observation(segment, own_membership)
            by_section[section].append(segment)
            seen_room_ids.add(room_id)

        for room_id in sorted(set(room_account_data) - seen_room_ids):
            if not room_account_data[room_id]:
                continue
            by_section[RoomSection.UNCHANGED].append(
                RoomSegment(
                    room_id=room_id,
                    section=RoomSection.UNCHANGED,
                    state_json=(),
                    timeline_json=(),
                    room_account_data_json=tuple(
                        canonical_json(event) for event in room_account_data[room_id]
                    ),
                    timeline_limited=False,
                    timeline_prev_batch=None,
                    initial=False,
                    expanded_timeline=False,
                    live_event_count=0,
                )
            )
        room_segments = tuple(
            segment
            for section in _ROOM_SECTION_ORDER
            for segment in by_section[section]
        )

        e2ee = _object(extensions.get("e2ee", {}), "extensions.e2ee")
        device_lists = _object(
            e2ee.get("device_lists", {}),
            "extensions.e2ee.device_lists",
        )
        changed = sorted(
            set(
                require_json_strings(
                    device_lists.get("changed", []),
                    "extensions.e2ee.device_lists.changed",
                )
            )
        )
        left = sorted(
            set(
                require_json_strings(
                    device_lists.get("left", []),
                    "extensions.e2ee.device_lists.left",
                )
            )
        )
        key_counts = _object(
            e2ee.get("device_one_time_keys_count", {}),
            "extensions.e2ee.device_one_time_keys_count",
        )
        for algorithm, value in key_counts.items():
            if type(algorithm) is not str or type(value) is not int or value < 0:
                raise ValueError(
                    "one-time key counts require string keys and nonnegative integers"
                )
        fallback: list[str] | None = None
        if "device_unused_fallback_key_types" in e2ee:
            fallback = sorted(
                set(
                    require_json_strings(
                        e2ee["device_unused_fallback_key_types"],
                        "extensions.e2ee.device_unused_fallback_key_types",
                    )
                )
            )

        ephemeral = self._ephemeral_events(extensions)
        presence_extension = _object(
            extensions.get("presence", {}),
            "extensions.presence",
        )
        presence = require_json_events(
            presence_extension.get("events", []),
            "extensions.presence.events",
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
            device_list_delta_json=canonical_json({"changed": changed, "left": left}),
            one_time_key_counts_json=canonical_json(key_counts),
            unused_fallback_key_types_json=canonical_json(fallback),
            room_segments=room_segments,
            ephemeral_json=tuple(ephemeral),
            global_account_data_json=tuple(
                canonical_json(event) for event in global_account_data
            ),
            presence_json=tuple(canonical_json(event) for event in presence),
        )

    @staticmethod
    def _validate_room_id(room_id: Any) -> None:
        if type(room_id) is not str or not room_id:
            raise ValueError("room IDs must be nonempty strings")

    @staticmethod
    def _expanded_timeline(room: dict[str, Any], room_id: str) -> bool:
        stable_present = "expanded_timeline" in room
        unstable_present = "unstable_expanded_timeline" in room
        stable = (
            _boolean(
                room["expanded_timeline"],
                f"rooms.{room_id}.expanded_timeline",
            )
            if stable_present
            else False
        )
        unstable = (
            _boolean(
                room["unstable_expanded_timeline"],
                f"rooms.{room_id}.unstable_expanded_timeline",
            )
            if unstable_present
            else False
        )
        if stable_present and unstable_present and stable != unstable:
            raise ValueError("conflicting stable and unstable expanded timeline flags")
        return unstable if unstable_present else stable

    @staticmethod
    def _live_event_count(
        num_live: Any,
        timeline_length: int,
        *,
        initial: bool,
        expanded: bool,
        room_id: str,
    ) -> int:
        if num_live is None:
            return 0 if initial or expanded else timeline_length
        if type(num_live) is not int or not 0 <= num_live <= timeline_length:
            raise ValueError(f"rooms.{room_id}.num_live must index the timeline suffix")
        return num_live

    @staticmethod
    def _ephemeral_events(extensions: dict[str, Any]) -> list[bytes]:
        by_extension: dict[str, dict[str, dict[str, Any]]] = {}
        for extension_name in ("typing", "receipts"):
            extension = _object(
                extensions.get(extension_name, {}),
                f"extensions.{extension_name}",
            )
            rooms = _object(
                extension.get("rooms", {}),
                f"extensions.{extension_name}.rooms",
            )
            events: dict[str, dict[str, Any]] = {}
            for room_id, event in rooms.items():
                SlidingSource._validate_room_id(room_id)
                events[room_id] = _object(
                    event,
                    f"extensions.{extension_name}.rooms.{room_id}",
                )
            by_extension[extension_name] = events
        room_ids = sorted(set(by_extension["typing"]) | set(by_extension["receipts"]))
        return [
            canonical_json({"room_id": room_id, "event": event})
            for room_id in room_ids
            for extension_name in ("typing", "receipts")
            if (event := by_extension[extension_name].get(room_id)) is not None
        ]
