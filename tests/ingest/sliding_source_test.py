import json
from dataclasses import FrozenInstanceError, replace
from uuid import UUID, uuid5

import pytest

from nio.ingest.config import SlidingSourceConfig
from nio.ingest.model import TransportKind
from nio.ingest.ports import NetworkRequest, NetworkResult
from nio.ingest.sliding import (
    SlidingConnectionReset,
    SlidingCursor,
    SlidingSource,
    canonical_sliding_cursor,
    reset_sliding_connection,
)
from nio.ingest.source import SourceResultKind, SyncSource
from nio.ingest.state import SourceState

STREAM_ID = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")
CONNECTION = UUID("236f12d0-c282-4594-8654-948a60a73ee9")
NEXT_CONNECTION = UUID("f70b2ed8-68d0-4ebd-9222-23f3e5fe44b7")
OWN_USER_ID = "@own:example.org"
RESERVED_LIST = "__nio_all_rooms_v1"


@pytest.fixture
def sliding_source() -> SlidingSource:
    return SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            timeout_ms=30_000,
            connection_name="worker λ",
            lists_json=(
                b'{"foreground":{"ranges":[[0,9]],"required_state":'
                b'[["m.room.name",""]],"timeline_limit":100}}'
            ),
            room_subscriptions_json=b'{"!focus:example.org":{"timeline_limit":100}}',
            extensions_json=(
                b'{"account_data":{"enabled":false,"lists":[],"rooms":'
                b'["!manual:example.org"]},"custom":{"x":1},'
                b'"e2ee":{"enabled":false,"vendor":7},'
                b'"receipts":{"enabled":false,"lists":[],"rooms":[]},'
                b'"to_device":{"enabled":false,"limit":9},'
                b'"typing":{"enabled":false,"lists":[],"rooms":[]}}'
            ),
        ),
        bootstrap_range_size=2,
        own_user_id=OWN_USER_ID,
    )


def _state(
    cursor: SlidingCursor,
    *,
    request_id: int = 7,
    active: bool = True,
) -> SourceState:
    return SourceState(
        source_epoch=4,
        transport_kind=TransportKind.SLIDING,
        cursor_json=canonical_sliding_cursor(cursor),
        next_request_id=request_id,
        active=active,
    )


def _result(request: NetworkRequest, body: dict[str, object]) -> NetworkResult:
    return NetworkResult(
        stream_id=request.stream_id,
        transport=request.transport,
        source_epoch=request.source_epoch,
        request_id=request.request_id,
        status_code=200,
        body=json.dumps(body).encode(),
        failure=None,
        retry_after_ms=None,
    )


def _request_body(request: NetworkRequest) -> dict[str, object]:
    assert request.body is not None
    parsed = json.loads(request.body)
    assert isinstance(parsed, dict)
    return parsed


def _candidate(result) -> dict[str, object]:
    assert result.frame is not None
    parsed = json.loads(result.frame.candidate_cursor_json)
    assert isinstance(parsed, dict)
    return parsed


def _success_body(
    request: NetworkRequest,
    *,
    pos: str,
    count: int,
    txn_id: object | None = None,
    to_device_since: str | None = None,
) -> dict[str, object]:
    request_txn = _request_body(request)["txn_id"]
    body: dict[str, object] = {
        "pos": pos,
        "txn_id": request_txn if txn_id is None else txn_id,
        "lists": {RESERVED_LIST: {"count": count}},
    }
    if to_device_since is not None:
        body["extensions"] = {
            "to_device": {"events": [], "next_batch": to_device_since}
        }
    return body


def test_sliding_cursor_is_frozen_slotted_exact_and_canonical() -> None:
    cursor = SlidingCursor(None, "td0", CONNECTION, 1, False)

    assert canonical_sliding_cursor(cursor) == (
        b'{"all_rooms_coverage_complete":false,"all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"pos":null,"to_device_since":"td0"}'
    )
    assert not hasattr(cursor, "__dict__")
    with pytest.raises(FrozenInstanceError):
        cursor.pos = "p"  # type: ignore[misc]
    with pytest.raises(ValueError, match="range"):
        SlidingCursor(None, None, CONNECTION, -1, False)
    with pytest.raises(ValueError, match="complete"):
        SlidingCursor(None, None, CONNECTION, 1, True)
    with pytest.raises(TypeError, match="coverage"):
        SlidingCursor(None, None, CONNECTION, 1, 1)  # type: ignore[arg-type]


def test_source_constructs_initial_cursor_without_creating_an_identity(
    sliding_source: SlidingSource,
) -> None:
    assert sliding_source.initial_cursor(CONNECTION) == SlidingCursor(
        None,
        None,
        CONNECTION,
        1,
        False,
    )
    assert isinstance(sliding_source, SyncSource)
    with pytest.raises(ValueError, match="positive"):
        replace(sliding_source, bootstrap_range_size=0)


def test_reset_is_coordinator_seeded_and_preserves_only_independent_state() -> None:
    cursor = SlidingCursor("p9", "td8", CONNECTION, 31, True)

    transition = reset_sliding_connection(cursor, NEXT_CONNECTION)

    assert transition == SlidingConnectionReset(
        SlidingCursor(None, "td8", NEXT_CONNECTION, 31, False),
        history_uncertain=True,
    )
    assert transition.history_uncertain is True
    assert cursor == SlidingCursor("p9", "td8", CONNECTION, 31, True)
    with pytest.raises(ValueError, match="new"):
        reset_sliding_connection(cursor, CONNECTION)


def test_initial_request_reserves_non_weakenable_all_room_coverage(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(None, "td0", CONNECTION, 1, False)

    request = sliding_source.plan_request(_state(cursor), request_id=7)

    assert request is not None
    assert request.stream_id == STREAM_ID
    assert request.transport is TransportKind.SLIDING
    assert request.method == "POST"
    assert request.path == (
        "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
    )
    assert request.query == (("timeout", "30000"),)
    assert request.timeout_ms == 30_000
    body = _request_body(request)
    assert body["conn_id"] == uuid5(CONNECTION, "worker λ").hex[:16]
    assert len(str(body["conn_id"]).encode("utf-8")) == 16
    assert body["txn_id"] == str(uuid5(CONNECTION, f"{RESERVED_LIST}:1"))
    assert body["room_subscriptions"] == {"!focus:example.org": {"timeline_limit": 100}}
    lists = body["lists"]
    assert isinstance(lists, dict)
    assert lists["foreground"] == {
        "ranges": [[0, 9]],
        "required_state": [["m.room.name", ""]],
        "timeline_limit": 100,
    }
    assert lists[RESERVED_LIST] == {
        "ranges": [[0, 1]],
        "required_state": [
            ["m.room.member", "*"],
            ["m.room.encryption", ""],
            ["m.room.name", ""],
            ["m.room.canonical_alias", ""],
            ["m.room.topic", ""],
            ["m.room.avatar", ""],
            ["m.room.join_rules", ""],
            ["m.room.create", ""],
            ["m.room.guest_access", ""],
            ["m.room.power_levels", ""],
        ],
        "sort": ["by_recency"],
        "timeline_limit": 1,
    }
    extensions = body["extensions"]
    assert isinstance(extensions, dict)
    assert extensions["to_device"] == {
        "enabled": True,
        "limit": 9,
        "since": "td0",
    }
    assert extensions["e2ee"] == {"enabled": True, "vendor": 7}
    assert extensions["account_data"] == {
        "enabled": True,
        "lists": [RESERVED_LIST],
        "rooms": ["!manual:example.org"],
    }
    for name in ("typing", "receipts"):
        assert extensions[name] == {
            "enabled": True,
            "lists": [RESERVED_LIST],
            "rooms": [],
        }
    assert extensions["custom"] == {"x": 1}
    assert (
        request.body
        == json.dumps(
            body,
            ensure_ascii=False,
            separators=(",", ":"),
            sort_keys=True,
        ).encode()
    )


def test_planning_does_not_mutate_frozen_config_json(
    sliding_source: SlidingSource,
) -> None:
    before = (
        sliding_source.config.lists_json,
        sliding_source.config.room_subscriptions_json,
        sliding_source.config.extensions_json,
    )

    request = sliding_source.plan_request(
        _state(SlidingCursor(None, None, CONNECTION, 1, False)),
        7,
    )

    assert request is not None
    assert before == (
        sliding_source.config.lists_json,
        sliding_source.config.room_subscriptions_json,
        sliding_source.config.extensions_json,
    )


@pytest.mark.parametrize(
    ("configured_scope", "expected_scope"),
    [
        (None, [RESERVED_LIST]),
        ([], [RESERVED_LIST]),
        (["foreground"], ["foreground", RESERVED_LIST]),
        (["*"], ["*"]),
    ],
)
def test_room_extension_scope_cannot_exclude_the_reserved_list(
    configured_scope: object,
    expected_scope: list[str],
) -> None:
    extension: dict[str, object] = {"enabled": False}
    if configured_scope is not None:
        extension["lists"] = configured_scope
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            0,
            "worker",
            b"{}",
            b"{}",
            json.dumps({"typing": extension}).encode(),
        ),
        bootstrap_range_size=2,
        own_user_id=OWN_USER_ID,
    )
    request = source.plan_request(
        _state(SlidingCursor(None, None, CONNECTION, 1, False)),
        7,
    )
    assert request is not None

    extensions = _request_body(request)["extensions"]
    assert isinstance(extensions, dict)
    typing = extensions["typing"]
    assert isinstance(typing, dict)
    assert typing["lists"] == expected_scope


def test_non_exact_wildcard_extension_scope_is_rejected() -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            0,
            "worker",
            b"{}",
            b"{}",
            b'{"typing":{"lists":["*","foreground"]}}',
        ),
        bootstrap_range_size=2,
        own_user_id=OWN_USER_ID,
    )

    with pytest.raises(ValueError, match="wildcard"):
        source.plan_request(
            _state(SlidingCursor(None, None, CONNECTION, 1, False)),
            7,
        )


@pytest.mark.parametrize(
    ("extension", "mutation"),
    [
        ("e2ee", {"enabled": False}),
        ("account_data", {"enabled": True, "lists": []}),
        ("typing", {"enabled": True, "lists": ["foreground"]}),
        ("receipts", {"enabled": False, "lists": [RESERVED_LIST]}),
    ],
)
def test_normalize_authenticates_mandatory_extension_coverage(
    sliding_source: SlidingSource,
    extension: str,
    mutation: dict[str, object],
) -> None:
    request = sliding_source.plan_request(
        _state(SlidingCursor("p1", "td1", CONNECTION, 1, False)),
        7,
    )
    assert request is not None
    body = _request_body(request)
    extensions = body["extensions"]
    assert isinstance(extensions, dict)
    extensions[extension] = mutation
    tampered = replace(
        request,
        body=json.dumps(
            extensions and body, separators=(",", ":"), sort_keys=True
        ).encode(),
    )

    with pytest.raises(ValueError, match="extension"):
        sliding_source.normalize(
            tampered,
            _result(tampered, _success_body(tampered, pos="p2", count=2)),
        )


def test_normalize_rejects_a_tampered_connection_id(
    sliding_source: SlidingSource,
) -> None:
    request = sliding_source.plan_request(
        _state(SlidingCursor("p1", "td1", CONNECTION, 1, False)),
        7,
    )
    assert request is not None
    body = _request_body(request)
    body["conn_id"] = "wrong-connection"
    tampered = replace(
        request,
        body=json.dumps(body, separators=(",", ":"), sort_keys=True).encode(),
    )

    with pytest.raises(ValueError, match="conn_id"):
        sliding_source.normalize(
            tampered,
            _result(tampered, _success_body(tampered, pos="p2", count=2)),
        )


def test_continuation_threads_pos_but_txn_is_stable_across_http_retries(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 3, False)
    first = sliding_source.plan_request(_state(cursor, request_id=7), 7)
    retry = sliding_source.plan_request(_state(cursor, request_id=8), 8)
    assert first is not None
    assert retry is not None

    assert first.query == (("pos", "p1"), ("timeout", "30000"))
    assert _request_body(first)["txn_id"] == _request_body(retry)["txn_id"]
    assert _request_body(first)["txn_id"] == str(
        uuid5(CONNECTION, f"{RESERVED_LIST}:3")
    )
    changed_end = sliding_source.plan_request(
        _state(replace(cursor, all_rooms_range_end=4), request_id=9),
        9,
    )
    changed_instance = sliding_source.plan_request(
        _state(
            replace(cursor, connection_instance=NEXT_CONNECTION),
            request_id=10,
        ),
        10,
    )
    assert changed_end is not None
    assert changed_instance is not None
    assert _request_body(changed_end)["txn_id"] != _request_body(first)["txn_id"]
    assert _request_body(changed_instance)["txn_id"] != _request_body(first)["txn_id"]
    assert _request_body(changed_instance)["conn_id"] != _request_body(first)["conn_id"]


def test_reserved_range_expands_in_bounded_echo_confirmed_steps(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(None, "td0", CONNECTION, 1, False)
    expected = ((3, False), (4, False), (4, True))

    for request_id, (end, complete) in enumerate(expected, start=7):
        request = sliding_source.plan_request(
            _state(cursor, request_id=request_id),
            request_id,
        )
        assert request is not None
        normalized = sliding_source.normalize(
            request,
            _result(
                request,
                _success_body(
                    request,
                    pos=f"p{request_id}",
                    count=5,
                    to_device_since=f"td{request_id}",
                ),
            ),
        )
        assert normalized.kind is SourceResultKind.FRAME
        candidate = _candidate(normalized)
        assert candidate["all_rooms_range_end"] == end
        assert candidate["all_rooms_coverage_complete"] is complete
        assert candidate["pos"] == f"p{request_id}"
        assert candidate["to_device_since"] == f"td{request_id}"
        cursor = SlidingCursor(
            str(candidate["pos"]),
            str(candidate["to_device_since"]),
            CONNECTION,
            end,
            complete,
        )


@pytest.mark.parametrize(
    ("complete", "count", "expected_complete"),
    [(False, 2, False), (False, 5, False), (True, 2, True), (True, 5, False)],
)
def test_stale_echo_maps_payload_but_cannot_claim_current_coverage(
    sliding_source: SlidingSource,
    complete: bool,
    count: int,
    expected_complete: bool,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 1, complete)
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p2",
        count=count,
        txn_id="older-transaction",
        to_device_since="td2",
    )

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.FRAME
    assert _candidate(normalized) == {
        "all_rooms_coverage_complete": expected_complete,
        "all_rooms_range_end": 1,
        "connection_instance": str(CONNECTION),
        "pos": "p2",
        "to_device_since": "td2",
    }


@pytest.mark.parametrize("count", [0, 1, 2])
def test_current_echo_never_shrinks_range_when_count_falls(
    sliding_source: SlidingSource,
    count: int,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 3, False)
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None

    normalized = sliding_source.normalize(
        request,
        _result(
            request,
            _success_body(
                request,
                pos="p2",
                count=count,
                to_device_since="td2",
            ),
        ),
    )

    candidate = _candidate(normalized)
    assert candidate["all_rooms_range_end"] == 3
    assert candidate["all_rooms_coverage_complete"] is True


def test_current_echo_reopens_and_expands_coverage_after_growth(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 1, True)
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None

    normalized = sliding_source.normalize(
        request,
        _result(
            request,
            _success_body(
                request,
                pos="p2",
                count=5,
                to_device_since="td2",
            ),
        ),
    )

    candidate = _candidate(normalized)
    assert candidate["all_rooms_range_end"] == 3
    assert candidate["all_rooms_coverage_complete"] is False


@pytest.mark.parametrize("txn_id", [None, 7, True])
def test_missing_or_non_string_echo_is_a_malformed_success(
    sliding_source: SlidingSource,
    txn_id: object | None,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 1, False)
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(request, pos="p2", count=2)
    if txn_id is None:
        del body["txn_id"]
    else:
        body["txn_id"] = txn_id

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.status_code == 200
    assert normalized.detail is not None
    assert "txn_id" in normalized.detail


@pytest.mark.parametrize("count", [None, -1, True, "5"])
def test_missing_or_invalid_reserved_count_is_a_malformed_success(
    sliding_source: SlidingSource,
    count: object,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 1, False)
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p2",
        count=2,
        to_device_since="td2",
    )
    reserved = body["lists"]
    assert isinstance(reserved, dict)
    if count is None:
        reserved[RESERVED_LIST] = {}
    else:
        reserved[RESERVED_LIST] = {"count": count}

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.detail is not None
    assert "count" in normalized.detail


def test_enabled_to_device_response_requires_its_independent_next_batch(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 1, False)
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(request, pos="p2", count=2)
    body["extensions"] = {"to_device": {"events": []}}

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.detail is not None
    assert "next_batch" in normalized.detail


def test_enabled_to_device_response_requires_the_extension_object(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 1, False)
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(request, pos="p2", count=2)

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.detail is not None
    assert "to_device" in normalized.detail


def test_restart_normalizes_durable_request_using_original_caller_payload(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor("p1", "td1", CONNECTION, 1, False)
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    restarted = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            45_000,
            "worker λ",
            b'{"different":{"timeline_limit":7}}',
            b"{}",
            b'{"custom":{"after_restart":true}}',
        ),
        bootstrap_range_size=2,
        own_user_id=OWN_USER_ID,
    )

    normalized = restarted.normalize(
        request,
        _result(
            request,
            _success_body(
                request,
                pos="p2",
                count=2,
                to_device_since="td2",
            ),
        ),
    )

    assert normalized.kind is SourceResultKind.FRAME


@pytest.mark.parametrize(
    "cursor_json",
    [
        b'{"all_rooms_coverage_complete":false,"all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"pos":null,"to_device_since":null,"extra":0}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"pos":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_range_end":true,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"pos":null,"to_device_since":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_range_end":1,'
        b'"connection_instance":"not-a-uuid","pos":null,'
        b'"to_device_since":null}',
        b'{ "all_rooms_coverage_complete":false,"all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"pos":null,"to_device_since":null}',
    ],
)
def test_request_planning_rejects_invalid_or_noncanonical_cursor_bytes(
    sliding_source: SlidingSource,
    cursor_json: bytes,
) -> None:
    state = replace(
        _state(SlidingCursor(None, None, CONNECTION, 1, False)),
        cursor_json=cursor_json,
    )

    with pytest.raises(ValueError):
        sliding_source.plan_request(state, 7)


def test_reserved_name_collision_is_rejected_without_silent_override() -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(0, "worker", b'{"__nio_all_rooms_v1":{}}', b"{}", b"{}"),
        bootstrap_range_size=2,
        own_user_id=OWN_USER_ID,
    )

    with pytest.raises(ValueError, match="reserved"):
        source.plan_request(
            _state(SlidingCursor(None, None, CONNECTION, 1, False)),
            7,
        )


def test_inactive_source_does_not_plan_network_work(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(None, None, CONNECTION, 1, False)
    assert sliding_source.plan_request(_state(cursor, active=False), 7) is None
