import hashlib
from dataclasses import dataclass
from uuid import UUID

from .config import ClassicSourceConfig
from .membership import _latest_own_membership, _membership_observation
from .model import RecordOrigin, TransportKind
from .ports import NetworkRequest, NetworkResult, _frame_id_for_response
from .source import (
    ClassicCursor,
    RoomSection,
    RoomSegment,
    SourceResult,
    SourceResultKind,
    SyncFrame,
    _classic_cursor_from_json,
    canonical_classic_cursor,
    canonical_json,
    load_json,
    malformed_success_result,
    normalize_source_error,
    require_json_event_container,
    require_json_events,
    require_json_object,
    require_json_string,
    require_json_strings,
    validate_network_result_identity,
)
from .state import SourceState

_CLASSIC_ROOM_SECTIONS = (
    RoomSection.INVITE,
    RoomSection.KNOCK,
    RoomSection.JOIN,
    RoomSection.LEAVE,
)


@dataclass(frozen=True, slots=True)
class ClassicSource:
    stream_id: UUID
    config: ClassicSourceConfig
    own_user_id: str

    def __post_init__(self) -> None:
        if type(self.stream_id) is not UUID:
            raise TypeError("stream_id must be UUID")
        if type(self.config) is not ClassicSourceConfig:
            raise TypeError("config must be ClassicSourceConfig")
        if type(self.own_user_id) is not str:
            raise TypeError("own_user_id must be str")
        if not self.own_user_id:
            raise ValueError("own_user_id must not be empty")

    def plan_request(
        self,
        state: SourceState,
        request_id: int,
    ) -> NetworkRequest | None:
        if type(state) is not SourceState:
            raise TypeError("state must be SourceState")
        if state.transport_kind is not TransportKind.CLASSIC:
            raise ValueError("ClassicSource requires classic source state")
        if type(request_id) is not int:
            raise TypeError("request_id must be int")
        if request_id != state.next_request_id:
            raise ValueError("request_id does not match source state")

        cursor = _classic_cursor_from_json(state.cursor_json)
        request_cursor_json = canonical_classic_cursor(cursor)
        if request_cursor_json != state.cursor_json:
            raise ValueError("state cursor_json must be a canonical classic cursor")
        if not state.active:
            return None

        return NetworkRequest(
            stream_id=self.stream_id,
            transport=TransportKind.CLASSIC,
            source_epoch=state.source_epoch,
            request_id=request_id,
            method="GET",
            path="/_matrix/client/v3/sync",
            query=self._request_query(cursor),
            body=None,
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
            frame, response_body = self._normalize_frame(request, result.body)
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
            response_body=response_body,
            detail=None,
        )

    def _validate_result_identity(
        self,
        request: NetworkRequest,
        result: NetworkResult,
    ) -> None:
        if type(request) is not NetworkRequest:
            raise TypeError("request must be NetworkRequest")
        if type(result) is not NetworkResult:
            raise TypeError("result must be NetworkResult")
        if request.transport is not TransportKind.CLASSIC:
            raise ValueError("ClassicSource can normalize only classic requests")
        if request.stream_id != self.stream_id:
            raise ValueError("classic request stream does not match source stream")
        cursor = _classic_cursor_from_json(request.request_cursor_json)
        if canonical_classic_cursor(cursor) != request.request_cursor_json:
            raise ValueError("request_cursor_json must be a canonical classic cursor")
        if (
            request.method != "GET"
            or request.path != "/_matrix/client/v3/sync"
            or request.body is not None
        ):
            raise ValueError("request is not the planned classic sync request")
        self._validate_request_query(request, cursor)
        validate_network_result_identity(request, result)

    def _request_query(
        self,
        cursor: ClassicCursor,
    ) -> tuple[tuple[str, str], ...]:
        if type(cursor) is not ClassicCursor:
            raise TypeError("cursor must be ClassicCursor")
        filter_value = load_json(self.config.filter_json, "filter_json")
        if type(filter_value) is not dict:
            raise ValueError("filter_json must contain a JSON object")
        query: list[tuple[str, str]] = []
        if cursor.next_batch is not None:
            query.append(("since", cursor.next_batch))
        else:
            query.append(("full_state", "true"))
        if self.config.timeout_ms:
            query.append(("timeout", str(self.config.timeout_ms)))
        query.append(("filter", canonical_json(filter_value).decode("utf-8")))
        return tuple(query)

    @staticmethod
    def _validate_request_query(
        request: NetworkRequest,
        cursor: ClassicCursor,
    ) -> None:
        if not request.query or request.query[-1][0] != "filter":
            raise ValueError("request is not the planned classic sync request")
        try:
            filter_json = request.query[-1][1].encode("utf-8")
        except UnicodeEncodeError as error:
            raise ValueError("request filter must be canonical UTF-8 JSON") from error
        filter_value = load_json(filter_json, "request filter")
        if type(filter_value) is not dict:
            raise ValueError("request filter must contain a JSON object")

        expected: list[tuple[str, str]] = []
        if cursor.next_batch is not None:
            expected.append(("since", cursor.next_batch))
        else:
            expected.append(("full_state", "true"))
        if request.timeout_ms:
            expected.append(("timeout", str(request.timeout_ms)))
        expected.append(("filter", canonical_json(filter_value).decode("utf-8")))
        if request.query != tuple(expected):
            raise ValueError("request is not the planned classic sync request")

    def _normalize_frame(
        self, request: NetworkRequest, body: bytes
    ) -> tuple[SyncFrame, bytes]:
        root = load_json(body, "sync response")
        root = require_json_object(root, "sync response")
        next_batch = require_json_string(root.get("next_batch"), "next_batch")
        source_json = canonical_json(root)
        request_cursor = _classic_cursor_from_json(request.request_cursor_json)
        request_cursor_json = canonical_classic_cursor(request_cursor)

        rooms = require_json_object(root.get("rooms", {}), "rooms")
        unknown_sections = set(rooms) - {
            section.value for section in _CLASSIC_ROOM_SECTIONS
        }
        if unknown_sections:
            raise ValueError(f"unsupported room sections: {sorted(unknown_sections)!r}")

        segments: list[RoomSegment] = []
        ephemeral: list[bytes] = []
        seen_room_ids: set[str] = set()
        initial = request_cursor.next_batch is None
        for section in _CLASSIC_ROOM_SECTIONS:
            section_rooms = require_json_object(
                rooms.get(section.value, {}),
                section.value,
            )
            for room_id in sorted(section_rooms):
                if type(room_id) is not str or not room_id:
                    raise ValueError("room IDs must be nonempty strings")
                if room_id in seen_room_ids:
                    raise ValueError(f"room appears in multiple sections: {room_id}")
                seen_room_ids.add(room_id)
                room = require_json_object(
                    section_rooms[room_id],
                    f"rooms.{section.value}.{room_id}",
                )
                if "state_after" in room:
                    raise ValueError("state_after requires unsupported use_state_after")

                state_key = {
                    RoomSection.INVITE: "invite_state",
                    RoomSection.KNOCK: "knock_state",
                    RoomSection.JOIN: "state",
                    RoomSection.LEAVE: "state",
                }[section]
                state = require_json_event_container(
                    room.get(state_key, {}),
                    state_key,
                )
                timeline_container = require_json_object(
                    room.get("timeline", {}),
                    "timeline",
                )
                timeline = require_json_events(
                    timeline_container.get("events", []),
                    "timeline.events",
                )
                limited = timeline_container.get("limited", False)
                if type(limited) is not bool:
                    raise ValueError("timeline.limited must be bool")
                prev_batch = timeline_container.get("prev_batch")
                if prev_batch is not None and type(prev_batch) is not str:
                    raise ValueError("timeline.prev_batch must be string or null")
                account_data = require_json_event_container(
                    room.get("account_data", {}),
                    "account_data",
                )
                ephemeral_events = require_json_event_container(
                    room.get("ephemeral", {}),
                    "ephemeral",
                )
                ephemeral.extend(
                    canonical_json({"room_id": room_id, "event": event})
                    for event in ephemeral_events
                )
                own_membership = _latest_own_membership(
                    state,
                    timeline,
                    0 if initial else len(timeline),
                    self.own_user_id,
                )
                room_membership = {
                    RoomSection.INVITE: "invite",
                    RoomSection.KNOCK: "knock",
                    RoomSection.JOIN: "join",
                    RoomSection.LEAVE: "leave",
                }[section]
                if (
                    own_membership is not None
                    and not own_membership.is_unparsed
                    and own_membership.membership != room_membership
                    and not (
                        room_membership == "leave"
                        and own_membership.membership == "ban"
                    )
                ):
                    raise ValueError(
                        "room section contradicts exact own membership evidence"
                    )
                segments.append(
                    RoomSegment(
                        room_id=room_id,
                        section=section,
                        state_json=tuple(canonical_json(event) for event in state),
                        timeline_json=tuple(
                            canonical_json(event) for event in timeline
                        ),
                        room_account_data_json=tuple(
                            canonical_json(event) for event in account_data
                        ),
                        timeline_limited=limited,
                        timeline_prev_batch=prev_batch,
                        initial=initial,
                        expanded_timeline=False,
                        live_event_count=0 if initial else len(timeline),
                        membership_observation=_membership_observation(
                            room_membership,
                            own_membership,
                            is_initial=initial,
                            is_expanded_timeline=False,
                        ),
                    )
                )

        device_lists = require_json_object(
            root.get("device_lists", {}),
            "device_lists",
        )
        changed = sorted(
            set(
                require_json_strings(
                    device_lists.get("changed", []),
                    "device_lists.changed",
                )
            )
        )
        left = sorted(
            set(
                require_json_strings(
                    device_lists.get("left", []),
                    "device_lists.left",
                )
            )
        )
        key_counts = require_json_object(
            root.get("device_one_time_keys_count", {}),
            "device_one_time_keys_count",
        )
        for algorithm, count in key_counts.items():
            if type(algorithm) is not str or type(count) is not int or count < 0:
                raise ValueError(
                    "one-time key counts require string keys and nonnegative integers"
                )

        fallback = root.get("device_unused_fallback_key_types")
        if "device_unused_fallback_key_types" in root:
            fallback = sorted(
                set(
                    require_json_strings(
                        fallback,
                        "device_unused_fallback_key_types",
                    )
                )
            )

        source_sha256 = hashlib.sha256(source_json).digest()
        frame_id = _frame_id_for_response(request, source_sha256)
        return (
            SyncFrame(
                frame_id=frame_id,
                origin=RecordOrigin(
                    TransportKind.CLASSIC,
                    request.source_epoch,
                    request.request_id,
                    0,
                ),
                request_cursor_json=request_cursor_json,
                candidate_cursor_json=canonical_classic_cursor(
                    type(request_cursor)(next_batch)
                ),
                source_sha256=source_sha256,
                to_device_json=tuple(
                    canonical_json(event)
                    for event in require_json_event_container(
                        root.get("to_device", {}),
                        "to_device",
                    )
                ),
                device_list_delta_json=canonical_json(
                    {"changed": changed, "left": left}
                ),
                one_time_key_counts_json=canonical_json(key_counts),
                unused_fallback_key_types_json=canonical_json(fallback),
                room_segments=tuple(segments),
                ephemeral_json=tuple(ephemeral),
                global_account_data_json=tuple(
                    canonical_json(event)
                    for event in require_json_event_container(
                        root.get("account_data", {}),
                        "account_data",
                    )
                ),
                presence_json=tuple(
                    canonical_json(event)
                    for event in require_json_event_container(
                        root.get("presence", {}),
                        "presence",
                    )
                ),
            ),
            source_json,
        )
