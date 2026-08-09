import hashlib
import inspect
import json
from dataclasses import FrozenInstanceError, replace
from enum import StrEnum
from pathlib import Path
from uuid import UUID
from uuid import uuid5

import pytest

from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig
from nio.ingest.model import TransportKind
from nio.ingest.ports import (
    NetworkFailureKind,
    NetworkRequest,
    NetworkResult,
)
from nio.ingest.source import (
    ClassicCursor,
    RoomSegment,
    RoomSection,
    SourceResult,
    SourceResultKind,
    canonical_classic_cursor,
)
from nio.ingest.state import SourceState

STREAM_ID = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")


@pytest.fixture
def classic_source() -> ClassicSource:
    return ClassicSource(
        STREAM_ID,
        ClassicSourceConfig(
            timeout_ms=30_000,
            filter_json=b'{"room": {"timeline": {"limit": 20}}}',
        ),
    )


@pytest.fixture
def classic_sync_body() -> bytes:
    return Path("tests/data/ingest/classic_sync.json").read_bytes()


def _source_state(
    cursor: ClassicCursor,
    *,
    transport: TransportKind = TransportKind.CLASSIC,
    request_id: int = 9,
    active: bool = True,
) -> SourceState:
    return SourceState(
        source_epoch=4,
        transport_kind=transport,
        cursor_json=canonical_classic_cursor(cursor),
        next_request_id=request_id,
        active=active,
    )


def test_classic_cursor_is_frozen_slotted_and_canonical() -> None:
    cursor = ClassicCursor(None)

    assert canonical_classic_cursor(cursor) == b'{"next_batch":null}'
    assert not hasattr(cursor, "__dict__")
    with pytest.raises(FrozenInstanceError):
        cursor.next_batch = "s1"  # type: ignore[misc]


def test_initial_request_has_full_state_and_no_since(
    classic_source: ClassicSource,
) -> None:
    state = _source_state(ClassicCursor(None))

    request = classic_source.plan_request(state, request_id=9)

    assert request == NetworkRequest(
        transport=TransportKind.CLASSIC,
        source_epoch=4,
        request_id=9,
        method="GET",
        path="/_matrix/client/v3/sync",
        query=(
            ("full_state", "true"),
            ("timeout", "30000"),
            ("filter", '{"room":{"timeline":{"limit":20}}}'),
        ),
        body=None,
        timeout_ms=30_000,
        request_cursor_json=b'{"next_batch":null}',
    )
    assert state.cursor_json == b'{"next_batch":null}'


def test_continuation_request_retains_prior_cursor_without_full_state(
    classic_source: ClassicSource,
) -> None:
    state = _source_state(ClassicCursor("s1"))

    request = classic_source.plan_request(state, request_id=9)

    assert request == NetworkRequest(
        transport=TransportKind.CLASSIC,
        source_epoch=4,
        request_id=9,
        method="GET",
        path="/_matrix/client/v3/sync",
        query=(
            ("since", "s1"),
            ("timeout", "30000"),
            ("filter", '{"room":{"timeline":{"limit":20}}}'),
        ),
        body=None,
        timeout_ms=30_000,
        request_cursor_json=b'{"next_batch":"s1"}',
    )


def test_inactive_source_does_not_plan_network_work(
    classic_source: ClassicSource,
) -> None:
    state = _source_state(ClassicCursor(None), active=False)

    assert classic_source.plan_request(state, request_id=9) is None


@pytest.mark.parametrize(
    "state, request_id",
    [
        (_source_state(ClassicCursor(None), transport=TransportKind.SLIDING), 9),
        (_source_state(ClassicCursor(None), request_id=10), 9),
        (
            SourceState(
                source_epoch=4,
                transport_kind=TransportKind.CLASSIC,
                cursor_json=b'{"pos":"sliding"}',
                next_request_id=9,
                active=True,
            ),
            9,
        ),
    ],
)
def test_request_planning_fails_closed_on_wrong_source_contract(
    classic_source: ClassicSource,
    state: SourceState,
    request_id: int,
) -> None:
    with pytest.raises(ValueError):
        classic_source.plan_request(state, request_id)


def test_request_planning_rejects_noncanonical_durable_cursor_bytes(
    classic_source: ClassicSource,
) -> None:
    state = replace(
        _source_state(ClassicCursor(None)),
        cursor_json=b'{"next_batch": null}',
    )

    with pytest.raises(ValueError, match="canonical classic cursor"):
        classic_source.plan_request(state, request_id=9)


def _network_result(request: NetworkRequest, body: bytes) -> NetworkResult:
    return NetworkResult(
        transport=request.transport,
        source_epoch=request.source_epoch,
        request_id=request.request_id,
        status_code=200,
        body=body,
        failure=None,
        retry_after_ms=None,
    )


def test_initial_sync_normalizes_every_classic_section_in_fixed_order(
    classic_source: ClassicSource,
    classic_sync_body: bytes,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor(None)),
        request_id=9,
    )
    assert request is not None

    result = classic_source.normalize(
        request,
        _network_result(request, classic_sync_body),
    )

    assert result.kind is SourceResultKind.FRAME
    assert result.request is request
    assert result.status_code == 200
    assert result.response_body == b""
    assert result.frame is not None
    frame = result.frame
    assert frame.origin.transport is TransportKind.CLASSIC
    assert frame.origin.source_epoch == 4
    assert frame.origin.request_id == 9
    assert frame.origin.frame_index == 0
    assert frame.request_cursor_json == b'{"next_batch":null}'
    assert frame.candidate_cursor_json == b'{"next_batch":"s-next"}'

    parsed = json.loads(classic_sync_body)
    expected_source = json.dumps(
        parsed,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()
    assert frame.source_json == expected_source
    expected_digest = hashlib.sha256(expected_source).hexdigest()
    assert frame.frame_id == uuid5(STREAM_ID, f"4:9:{expected_digest}")

    assert tuple(
        (segment.section, segment.room_id) for segment in frame.room_segments
    ) == (
        (RoomSection.INVITE, "!a-invite:example.org"),
        (RoomSection.INVITE, "!z-invite:example.org"),
        (RoomSection.KNOCK, "!knock:example.org"),
        (RoomSection.JOIN, "!a-join:example.org"),
        (RoomSection.JOIN, "!z-join:example.org"),
        (RoomSection.LEAVE, "!leave:example.org"),
    )
    assert all(segment.initial for segment in frame.room_segments)
    assert all(segment.live_event_count == 0 for segment in frame.room_segments)

    invite = frame.room_segments[1]
    assert invite.state_json == (
        b'{"content":{"membership":"invite"},"state_key":'
        b'"@me:example.org","type":"m.room.member"}',
    )
    knock = frame.room_segments[2]
    assert knock.state_json == (
        b'{"content":{"membership":"knock"},"state_key":'
        b'"@me:example.org","type":"m.room.member"}',
    )

    z_join = frame.room_segments[4]
    assert z_join.timeline_limited is True
    assert z_join.timeline_prev_batch == "z-prev"
    assert z_join.state_json == (
        b'{"content":{"name":"Zed"},"event_id":"$z-name",'
        b'"state_key":"","type":"m.room.name"}',
    )
    assert z_join.timeline_json == (
        b'{"content":{"body":"first","msgtype":"m.text"},'
        b'"event_id":"$z-live-1","type":"m.room.message"}',
        b'{"content":{"body":"second","msgtype":"m.text"},'
        b'"event_id":"$z-live-2","type":"m.room.message"}',
    )
    assert z_join.room_account_data_json == (
        b'{"content":{"event_id":"$z-live-2"},"type":"m.fully_read"}',
    )

    left = frame.room_segments[-1]
    assert left.state_json == (
        b'{"content":{"membership":"leave"},"event_id":"$left",'
        b'"state_key":"@me:example.org","type":"m.room.member"}',
    )
    assert left.timeline_json == (
        b'{"content":{"body":"left","msgtype":"m.text"},'
        b'"event_id":"$leave-message","type":"m.room.message"}',
    )
    assert left.room_account_data_json == (
        b'{"content":{"tags":{"u.left":{}}},"type":"m.tag"}',
    )
    assert frame.ephemeral_json == (
        b'{"event":{"content":{"user_ids":["@bob:example.org"]},'
        b'"type":"m.typing"},"room_id":"!z-join:example.org"}',
        b'{"event":{"content":{"$z-live-2":{"m.read":'
        b'{"@me:example.org":{"ts":7}}}},"type":"m.receipt"},'
        b'"room_id":"!z-join:example.org"}',
        b'{"event":{"content":{"$z-live-2":{"m.read":'
        b'{"@me:example.org":{"ts":7}}}},"type":"m.receipt"},'
        b'"room_id":"!z-join:example.org"}',
    )
    assert frame.to_device_json == (
        b'{"content":{"room_id":"!z-join:example.org",'
        b'"session_id":"SESSION"},"sender":"@alice:example.org",'
        b'"type":"m.room_key"}',
        b'{"content":{"action":"request_cancellation","request_id":"REQ"},'
        b'"sender":"@bob:example.org","type":"m.room_key_request"}',
        b'{"content":{"action":"request_cancellation","request_id":"REQ"},'
        b'"sender":"@bob:example.org","type":"m.room_key_request"}',
    )
    assert frame.device_list_delta_json == (
        b'{"changed":["@alice:example.org","@bob:example.org"],'
        b'"left":["@old:example.org"]}'
    )
    assert frame.one_time_key_counts_json == (b'{"curve25519":3,"signed_curve25519":5}')
    assert frame.unused_fallback_key_types_json == b'["signed_curve25519"]'
    assert frame.global_account_data_json == (
        b'{"content":{"ignored_users":{"@spam:example.org":{}}},'
        b'"type":"m.ignored_user_list"}',
        b'{"content":{"@alice:example.org":["!a-join:example.org"]},'
        b'"type":"m.direct"}',
        b'{"content":{"@alice:example.org":["!a-join:example.org"]},'
        b'"type":"m.direct"}',
    )
    assert frame.presence_json == (
        b'{"content":{"presence":"online","status_msg":"Caf\xc3\xa9"},'
        b'"sender":"@alice:example.org","type":"m.presence"}',
        b'{"content":{"last_active_ago":42,"presence":"unavailable"},'
        b'"sender":"@bob:example.org","type":"m.presence"}',
        b'{"content":{"last_active_ago":42,"presence":"unavailable"},'
        b'"sender":"@bob:example.org","type":"m.presence"}',
    )


def test_continuation_marks_all_classic_timeline_events_live(
    classic_source: ClassicSource,
    classic_sync_body: bytes,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None

    result = classic_source.normalize(
        request,
        _network_result(request, classic_sync_body),
    )

    assert result.frame is not None
    assert result.frame.request_cursor_json == b'{"next_batch":"s-prior"}'
    assert all(not segment.initial for segment in result.frame.room_segments)
    assert tuple(
        segment.live_event_count for segment in result.frame.room_segments
    ) == (0, 0, 0, 1, 2, 1)


@pytest.mark.parametrize(
    ("fallback_field", "expected"),
    [
        (None, b"null"),
        ([], b"[]"),
    ],
)
def test_empty_sync_is_a_durable_frame_and_preserves_fallback_capability(
    classic_source: ClassicSource,
    fallback_field: list[str] | None,
    expected: bytes,
) -> None:
    payload: dict[str, object] = {"next_batch": "s-empty"}
    if fallback_field is not None:
        payload["device_unused_fallback_key_types"] = fallback_field
    body = json.dumps(payload).encode()
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None

    result = classic_source.normalize(request, _network_result(request, body))

    assert result.kind is SourceResultKind.FRAME
    assert result.frame is not None
    assert result.frame.candidate_cursor_json == b'{"next_batch":"s-empty"}'
    assert result.frame.room_segments == ()
    assert result.frame.to_device_json == ()
    assert result.frame.ephemeral_json == ()
    assert result.frame.global_account_data_json == ()
    assert result.frame.presence_json == ()
    assert result.frame.device_list_delta_json == b'{"changed":[],"left":[]}'
    assert result.frame.one_time_key_counts_json == b"{}"
    assert result.frame.unused_fallback_key_types_json == expected


def test_semantically_identical_object_order_produces_identical_frame_bytes(
    classic_source: ClassicSource,
    classic_sync_body: bytes,
) -> None:
    payload = json.loads(classic_sync_body)
    reordered_body = json.dumps(dict(reversed(tuple(payload.items())))).encode()
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None

    first = classic_source.normalize(
        request,
        _network_result(request, classic_sync_body),
    )
    second = classic_source.normalize(
        request,
        _network_result(request, reordered_body),
    )

    assert first == second
    assert first.frame is not None
    assert second.frame is not None
    assert first.frame.source_json == second.frame.source_json


@pytest.mark.parametrize(
    ("failure", "expected"),
    [
        (NetworkFailureKind.TIMEOUT, SourceResultKind.RETRYABLE_ERROR),
        (NetworkFailureKind.CONNECTION, SourceResultKind.RETRYABLE_ERROR),
        (NetworkFailureKind.PROTOCOL, SourceResultKind.TERMINAL_ERROR),
    ],
)
def test_transport_failures_have_an_explicit_classification(
    classic_source: ClassicSource,
    failure: NetworkFailureKind,
    expected: SourceResultKind,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    network_result = NetworkResult(
        request.transport,
        request.source_epoch,
        request.request_id,
        None,
        b"",
        failure,
        None,
    )

    result = classic_source.normalize(request, network_result)

    assert result.kind is expected
    assert result.frame is None
    assert result.network_failure is failure
    assert result.status_code is None
    assert result.response_body == b""


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (201, SourceResultKind.TERMINAL_ERROR),
        (301, SourceResultKind.TERMINAL_ERROR),
        (400, SourceResultKind.TERMINAL_ERROR),
        (401, SourceResultKind.TERMINAL_ERROR),
        (403, SourceResultKind.TERMINAL_ERROR),
        (404, SourceResultKind.TERMINAL_ERROR),
        (408, SourceResultKind.RETRYABLE_ERROR),
        (429, SourceResultKind.RETRYABLE_ERROR),
        (500, SourceResultKind.RETRYABLE_ERROR),
        (502, SourceResultKind.RETRYABLE_ERROR),
        (503, SourceResultKind.RETRYABLE_ERROR),
        (599, SourceResultKind.RETRYABLE_ERROR),
    ],
)
def test_http_statuses_are_classified_without_becoming_empty_frames(
    classic_source: ClassicSource,
    status_code: int,
    expected: SourceResultKind,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    body = b'{"retry_after_ms":250,"error":"later","errcode":"M_TEST"}'
    network_result = NetworkResult(
        request.transport,
        request.source_epoch,
        request.request_id,
        status_code,
        body,
        None,
        None,
    )

    result = classic_source.normalize(request, network_result)

    assert result.kind is expected
    assert result.frame is None
    assert result.status_code == status_code
    assert result.error_code == "M_TEST"
    assert result.response_body == (
        b'{"errcode":"M_TEST","error":"later","retry_after_ms":250}'
    )
    assert result.retry_after_ms == (
        250 if expected is SourceResultKind.RETRYABLE_ERROR else None
    )


def test_retry_after_header_takes_precedence_over_error_body(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    network_result = NetworkResult(
        request.transport,
        request.source_epoch,
        request.request_id,
        429,
        b'{"errcode":"M_LIMIT_EXCEEDED","error":"slow","retry_after_ms":250}',
        None,
        900,
    )

    result = classic_source.normalize(request, network_result)

    assert result.kind is SourceResultKind.RETRYABLE_ERROR
    assert result.retry_after_ms == 900


def test_non_json_retryable_http_body_is_retained_exactly(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    body = b"<html>upstream unavailable</html>"

    result = classic_source.normalize(
        request,
        NetworkResult(
            request.transport,
            request.source_epoch,
            request.request_id,
            503,
            body,
            None,
            None,
        ),
    )

    assert result.kind is SourceResultKind.RETRYABLE_ERROR
    assert result.response_body == body


@pytest.mark.parametrize(
    "body",
    [
        b"not JSON",
        b'\xff{"next_batch":"s"}',
        '{"next_batch":"s"}'.encode("utf-16"),
        b'{"next_batch":"s","value":NaN}',
        b'{"next_batch":"s","value":1.5}',
        b'{"next_batch":"s","value":"\\ud800"}',
        b'{"next_batch":"s","value":9007199254740992}',
        b'{"next_batch":"s","value":-9007199254740992}',
        pytest.param(
            b'{"next_batch":"s","value":' + (b"[" * 50_000) + (b"]" * 50_000) + b"}",
            id="deeply-nested-json",
        ),
        b'{"next_batch":"s","next_batch":"other"}',
        b"[]",
        b"{}",
        b'{"next_batch":7}',
        b'{"next_batch":"s","rooms":[]}',
        b'{"next_batch":"s","rooms":{"future":{}}}',
        (
            b'{"next_batch":"s","rooms":{"join":{"!r:example.org":'
            b'{"state_after":{"events":[]}}}}}'
        ),
        (
            b'{"next_batch":"s","rooms":{"invite":{"!r:example.org":{}},'
            b'"join":{"!r:example.org":{}}}}'
        ),
        b'{"next_batch":"s","device_one_time_keys_count":{"alg":1.5}}',
        b'{"next_batch":"s","device_one_time_keys_count":{"alg":true}}',
        b'{"next_batch":"s","device_one_time_keys_count":{"alg":-1}}',
        (
            b'{"next_batch":"s","rooms":{"join":{"!r:example.org":'
            b'{"timeline":{"events":[7]}}}}}'
        ),
    ],
)
def test_malformed_or_unsupported_success_is_terminal_and_retains_evidence(
    classic_source: ClassicSource,
    body: bytes,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None

    result = classic_source.normalize(request, _network_result(request, body))

    assert result.kind is SourceResultKind.TERMINAL_ERROR
    assert result.frame is None
    assert result.status_code == 200
    assert result.response_body
    assert result.detail


def test_arbitrary_nonnegative_one_time_key_algorithms_are_retained(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    body = (
        b'{"next_batch":"s","device_one_time_keys_count":'
        b'{"com.example.custom":2,"signed_curve25519":0}}'
    )

    result = classic_source.normalize(request, _network_result(request, body))

    assert result.frame is not None
    assert result.frame.one_time_key_counts_json == (
        b'{"com.example.custom":2,"signed_curve25519":0}'
    )


def test_deeply_nested_http_error_body_is_classified_and_retained(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    body = (b"[" * 50_000) + b"0" + (b"]" * 50_000)
    network_result = NetworkResult(
        request.transport,
        request.source_epoch,
        request.request_id,
        503,
        body,
        None,
        None,
    )

    result = classic_source.normalize(request, network_result)

    assert result.kind is SourceResultKind.RETRYABLE_ERROR
    assert result.response_body == body


def test_semantic_device_sets_are_sorted_and_deduplicated(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    body = (
        b'{"next_batch":"s","device_lists":'
        b'{"changed":["@z:e","@a:e","@z:e"],'
        b'"left":["@m:e","@a:e","@m:e"]},'
        b'"device_unused_fallback_key_types":'
        b'["z.example","a.example","z.example"]}'
    )

    result = classic_source.normalize(request, _network_result(request, body))

    assert result.frame is not None
    assert result.frame.device_list_delta_json == (
        b'{"changed":["@a:e","@z:e"],"left":["@a:e","@m:e"]}'
    )
    assert result.frame.unused_fallback_key_types_json == (b'["a.example","z.example"]')


def test_reset_required_is_reserved_for_sliding_unknown_position(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    request = replace(request, transport=TransportKind.SLIDING)

    result = SourceResult(
        kind=SourceResultKind.RESET_REQUIRED,
        request=request,
        frame=None,
        status_code=400,
        network_failure=None,
        error_code="M_UNKNOWN_POS",
        retry_after_ms=None,
        response_body=b'{"errcode":"M_UNKNOWN_POS"}',
        detail="source reset required",
    )

    assert result.kind is SourceResultKind.RESET_REQUIRED


@pytest.mark.parametrize(
    ("transport", "error_code"),
    [
        (TransportKind.CLASSIC, "M_UNKNOWN_POS"),
        (TransportKind.SLIDING, "M_FORBIDDEN"),
    ],
)
def test_reset_required_rejects_the_wrong_transport_or_error_code(
    classic_source: ClassicSource,
    transport: TransportKind,
    error_code: str,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    request = replace(request, transport=transport)

    with pytest.raises(ValueError, match="classification"):
        SourceResult(
            kind=SourceResultKind.RESET_REQUIRED,
            request=request,
            frame=None,
            status_code=400,
            network_failure=None,
            error_code=error_code,
            retry_after_ms=None,
            response_body=b'{"errcode":"M_UNKNOWN_POS"}',
            detail="source reset required",
        )


def test_transport_failure_cannot_smuggle_an_http_body() -> None:
    with pytest.raises(ValueError, match="body must be empty"):
        NetworkResult(
            TransportKind.CLASSIC,
            1,
            1,
            None,
            b"diagnostic body",
            NetworkFailureKind.TIMEOUT,
            None,
        )


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("transport", TransportKind.SLIDING),
        ("source_epoch", 5),
        ("request_id", 10),
    ],
)
def test_network_result_cannot_be_cross_wired_to_another_request(
    classic_source: ClassicSource,
    field: str,
    value: object,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    result = replace(_network_result(request, b'{"next_batch":"s"}'), **{field: value})

    with pytest.raises(ValueError, match="does not match"):
        classic_source.normalize(request, result)


@pytest.mark.parametrize(
    ("field", "value"),
    [
        ("method", "POST"),
        ("path", "/_matrix/client/v3/rooms"),
        ("body", b"{}"),
        ("timeout_ms", 1),
    ],
)
def test_normalize_rejects_a_classic_tagged_non_sync_request(
    classic_source: ClassicSource,
    field: str,
    value: object,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    wrong_request = replace(request, **{field: value})
    result = _network_result(wrong_request, b'{"next_batch":"s"}')

    with pytest.raises(ValueError, match="classic sync request"):
        classic_source.normalize(wrong_request, result)


def test_normalize_rejects_query_cursor_cross_wiring(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    wrong_request = replace(
        request,
        query=(("since", "other"), *request.query[1:]),
    )

    with pytest.raises(ValueError, match="classic sync request"):
        classic_source.normalize(
            wrong_request,
            _network_result(wrong_request, b'{"next_batch":"s"}'),
        )


def test_corrupt_request_cursor_is_an_integrity_error_before_http_classification(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    corrupt_request = replace(
        request,
        request_cursor_json=b'{"next_batch": "s-prior"}',
    )
    result = NetworkResult(
        corrupt_request.transport,
        corrupt_request.source_epoch,
        corrupt_request.request_id,
        503,
        b"unavailable",
        None,
        None,
    )

    with pytest.raises(ValueError, match="canonical classic cursor"):
        classic_source.normalize(corrupt_request, result)


def test_restart_normalizes_a_durable_request_using_its_original_config() -> None:
    original_source = ClassicSource(
        STREAM_ID,
        ClassicSourceConfig(
            timeout_ms=1_000,
            filter_json=b'{"room":{"timeline":{"limit":5}}}',
            full_state_on_cold_start=True,
        ),
    )
    request = original_source.plan_request(
        _source_state(ClassicCursor(None)),
        request_id=9,
    )
    assert request is not None
    restarted_source = ClassicSource(
        STREAM_ID,
        ClassicSourceConfig(
            timeout_ms=45_000,
            filter_json=b'{"room":{"timeline":{"limit":100}}}',
            full_state_on_cold_start=False,
        ),
    )

    normalized = restarted_source.normalize(
        request,
        _network_result(request, b'{"next_batch":"s-after-restart"}'),
    )

    assert normalized.kind is SourceResultKind.FRAME
    assert normalized.frame is not None
    assert normalized.frame.request_cursor_json == b'{"next_batch":null}'


def test_frozen_source_values_reject_mutable_nested_inputs(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    good = classic_source.normalize(
        request,
        _network_result(request, b'{"next_batch":"s"}'),
    )
    assert good.frame is not None

    with pytest.raises(TypeError):
        replace(request, query=list(request.query))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        replace(request, body=bytearray(b"{}"))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        replace(
            _network_result(request, b'{"next_batch":"s"}'),
            body=bytearray(b"{}"),  # type: ignore[arg-type]
        )
    with pytest.raises(TypeError):
        replace(good.frame, source_json=bytearray(good.frame.source_json))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        replace(good.frame, room_segments=list(good.frame.room_segments))  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        RoomSegment(
            "!room:example.org",
            RoomSection.JOIN,
            (bytearray(b"{}"),),  # type: ignore[arg-type]
            (),
            (),
            False,
            None,
            False,
            0,
        )

    mismatched_request = replace(request, request_id=request.request_id + 1)
    with pytest.raises(ValueError, match="frame does not match"):
        replace(good, request=mismatched_request)


def test_source_types_reject_foreign_string_enums() -> None:
    class ForeignTransport(StrEnum):
        CLASSIC = "classic"

    with pytest.raises(TypeError):
        NetworkResult(
            ForeignTransport.CLASSIC,  # type: ignore[arg-type]
            1,
            1,
            200,
            b"{}",
            None,
            None,
        )


@pytest.mark.parametrize(
    ("kind", "status", "failure"),
    [
        (SourceResultKind.RETRYABLE_ERROR, None, NetworkFailureKind.PROTOCOL),
        (SourceResultKind.TERMINAL_ERROR, None, NetworkFailureKind.TIMEOUT),
        (SourceResultKind.TERMINAL_ERROR, None, NetworkFailureKind.CONNECTION),
        (SourceResultKind.RETRYABLE_ERROR, 400, None),
        (SourceResultKind.TERMINAL_ERROR, 408, None),
        (SourceResultKind.TERMINAL_ERROR, 429, None),
        (SourceResultKind.TERMINAL_ERROR, 503, None),
    ],
)
def test_source_result_rejects_contradictory_error_discriminants(
    classic_source: ClassicSource,
    kind: SourceResultKind,
    status: int | None,
    failure: NetworkFailureKind | None,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None

    with pytest.raises(ValueError, match="classification"):
        SourceResult(
            kind=kind,
            request=request,
            frame=None,
            status_code=status,
            network_failure=failure,
            error_code=None,
            retry_after_ms=None,
            response_body=b"",
            detail="contradiction",
        )


@pytest.mark.parametrize("status", [99, 600])
def test_source_result_rejects_invalid_http_status(
    classic_source: ClassicSource,
    status: int,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None

    with pytest.raises(ValueError, match="status_code"):
        SourceResult(
            kind=SourceResultKind.RETRYABLE_ERROR,
            request=request,
            frame=None,
            status_code=status,
            network_failure=None,
            error_code=None,
            retry_after_ms=None,
            response_body=b"",
            detail="invalid status",
        )


def test_transport_source_result_rejects_even_zero_retry_delay(
    classic_source: ClassicSource,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None

    with pytest.raises(ValueError, match="HTTP metadata"):
        SourceResult(
            kind=SourceResultKind.RETRYABLE_ERROR,
            request=request,
            frame=None,
            status_code=None,
            network_failure=NetworkFailureKind.TIMEOUT,
            error_code=None,
            retry_after_ms=0,
            response_body=b"",
            detail="timeout",
        )


@pytest.mark.parametrize("status", [408, 429, 503])
def test_reset_required_rejects_retryable_http_statuses(
    classic_source: ClassicSource,
    status: int,
) -> None:
    request = classic_source.plan_request(
        _source_state(ClassicCursor("s-prior")),
        request_id=9,
    )
    assert request is not None
    request = replace(request, transport=TransportKind.SLIDING)

    with pytest.raises(ValueError, match="classification"):
        SourceResult(
            kind=SourceResultKind.RESET_REQUIRED,
            request=request,
            frame=None,
            status_code=status,
            network_failure=None,
            error_code="M_UNKNOWN_POS",
            retry_after_ms=None,
            response_body=b'{"errcode":"M_UNKNOWN_POS"}',
            detail="source reset required",
        )


def test_classic_adapter_is_synchronous_and_owns_no_runtime_resources(
    classic_source: ClassicSource,
) -> None:
    import nio.ingest.classic as classic_module
    import nio.ingest.ports as ports_module
    import nio.ingest.source as source_module

    assert not inspect.iscoroutinefunction(classic_source.plan_request)
    assert not inspect.iscoroutinefunction(classic_source.normalize)
    for module in (classic_module, ports_module, source_module):
        assert "asyncio" not in vars(module)
        imported_modules = {
            getattr(value, "__module__", "") for value in vars(module).values()
        }
        assert not any(name.startswith("nio.client") for name in imported_modules)
        assert not any(name.startswith("nio.store") for name in imported_modules)

    for value in (
        classic_source,
        ClassicCursor(None),
        RoomSegment(
            "!room:example.org",
            RoomSection.JOIN,
            (),
            (),
            (),
            False,
            None,
            True,
            0,
        ),
    ):
        assert not hasattr(value, "__dict__")
