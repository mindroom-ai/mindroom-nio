import hashlib
import inspect
import json
from dataclasses import FrozenInstanceError, replace
from uuid import UUID, uuid5

import pytest

from nio.ingest.config import SlidingSourceConfig
from nio.ingest.membership import (
    MembershipBaseline,
    MembershipObservation,
    MembershipProof,
    MembershipProofKind,
    membership_recovery_cursor,
    prove_membership,
)
from nio.ingest.model import TransportKind
from nio.ingest.ports import NetworkRequest, NetworkResult
from nio.ingest.sliding import (
    SlidingConnectionReset,
    SlidingCursor,
    SlidingRangeAckMode,
    SlidingSource,
    canonical_sliding_cursor,
    reset_sliding_connection,
    sliding_membership_observation,
)
from nio.ingest.source import RoomSection, SourceResultKind, SyncSource
from nio.ingest.state import SourceState

STREAM_ID = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")
CONNECTION = UUID("236f12d0-c282-4594-8654-948a60a73ee9")
NEXT_CONNECTION = UUID("f70b2ed8-68d0-4ebd-9222-23f3e5fe44b7")
CONNECTION_NAME = "worker λ"
OWN_USER_ID = "@own:example.org"
RESERVED_LIST = "__nio_all_rooms_v1"
UNKNOWN_ACK = SlidingRangeAckMode.UNKNOWN
TXN_ACK = SlidingRangeAckMode.TXN_ECHO
RESPONSE_ACK = SlidingRangeAckMode.RESPONSE_BOUND


def _baseline(
    prev_batch: str,
    membership_event_id: str,
) -> MembershipBaseline:
    return MembershipBaseline(
        "!room:example.org",
        4,
        1,
        prev_batch,
        membership_event_id,
    )


@pytest.fixture
def sliding_source() -> SlidingSource:
    return SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            timeout_ms=30_000,
            connection_name=CONNECTION_NAME,
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
            all_rooms_page_size=2,
        ),
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


def _canonical(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        separators=(",", ":"),
        sort_keys=True,
    ).encode()


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
    cursor = SlidingCursor(
        None,
        "td0",
        CONNECTION,
        CONNECTION_NAME,
        1,
        2,
        UNKNOWN_ACK,
        False,
    )

    assert canonical_sliding_cursor(cursor) == _canonical(
        {
            "all_rooms_coverage_complete": False,
            "all_rooms_page_size": 2,
            "all_rooms_range_ack_mode": "unknown",
            "all_rooms_range_end": 1,
            "connection_instance": str(CONNECTION),
            "connection_name": CONNECTION_NAME,
            "pos": None,
            "to_device_since": "td0",
        }
    )
    assert not hasattr(cursor, "__dict__")
    with pytest.raises(FrozenInstanceError):
        cursor.pos = "p"  # type: ignore[misc]
    with pytest.raises(ValueError, match="range"):
        SlidingCursor(
            None, None, CONNECTION, CONNECTION_NAME, -1, 2, UNKNOWN_ACK, False
        )
    with pytest.raises(ValueError, match="page"):
        SlidingCursor(None, None, CONNECTION, CONNECTION_NAME, 1, 0, UNKNOWN_ACK, False)
    with pytest.raises(ValueError, match="complete"):
        SlidingCursor(None, None, CONNECTION, CONNECTION_NAME, 1, 2, UNKNOWN_ACK, True)
    with pytest.raises(ValueError, match="ack mode"):
        SlidingCursor(
            None,
            None,
            CONNECTION,
            CONNECTION_NAME,
            1,
            2,
            RESPONSE_ACK,
            False,
        )
    with pytest.raises(TypeError, match="coverage"):
        SlidingCursor(
            None,
            None,
            CONNECTION,
            CONNECTION_NAME,
            1,
            2,
            UNKNOWN_ACK,
            1,  # type: ignore[arg-type]
        )


def test_source_constructs_initial_cursor_without_creating_an_identity(
    sliding_source: SlidingSource,
) -> None:
    assert sliding_source.initial_cursor(CONNECTION) == SlidingCursor(
        None,
        None,
        CONNECTION,
        CONNECTION_NAME,
        1,
        2,
        UNKNOWN_ACK,
        False,
    )
    assert isinstance(sliding_source, SyncSource)
    with pytest.raises(ValueError, match="positive"):
        replace(sliding_source.config, all_rooms_page_size=0)


def test_reset_is_coordinator_seeded_and_preserves_only_independent_state() -> None:
    cursor = SlidingCursor(
        "p9", "td8", CONNECTION, CONNECTION_NAME, 31, 2, TXN_ACK, True
    )

    transition = reset_sliding_connection(cursor, NEXT_CONNECTION)

    assert transition == SlidingConnectionReset(
        SlidingCursor(
            None,
            "td8",
            NEXT_CONNECTION,
            CONNECTION_NAME,
            31,
            2,
            UNKNOWN_ACK,
            False,
        ),
        history_uncertain=True,
    )
    assert transition.history_uncertain is True
    assert cursor == SlidingCursor(
        "p9",
        "td8",
        CONNECTION,
        CONNECTION_NAME,
        31,
        2,
        TXN_ACK,
        True,
    )
    with pytest.raises(ValueError, match="new"):
        reset_sliding_connection(cursor, CONNECTION)


def test_initial_request_reserves_non_weakenable_all_room_coverage(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        None,
        "td0",
        CONNECTION,
        CONNECTION_NAME,
        1,
        2,
        UNKNOWN_ACK,
        False,
    )

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
        "limit": 100,
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
        _state(
            SlidingCursor(
                None,
                None,
                CONNECTION,
                CONNECTION_NAME,
                1,
                2,
                UNKNOWN_ACK,
                False,
            )
        ),
        7,
    )

    assert request is not None
    assert before == (
        sliding_source.config.lists_json,
        sliding_source.config.room_subscriptions_json,
        sliding_source.config.extensions_json,
    )


@pytest.mark.parametrize("configured_limit", [0, -1, 10_000, "many", None])
def test_to_device_limit_is_an_internal_non_weakenable_constant(
    configured_limit: object,
) -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            0,
            CONNECTION_NAME,
            b"{}",
            b"{}",
            json.dumps(
                {"to_device": {"enabled": False, "limit": configured_limit}}
            ).encode(),
            all_rooms_page_size=2,
        ),
        own_user_id=OWN_USER_ID,
    )
    request = source.plan_request(_state(source.initial_cursor(CONNECTION)), 7)
    assert request is not None

    extensions = _request_body(request)["extensions"]
    assert isinstance(extensions, dict)
    assert extensions["to_device"] == {"enabled": True, "limit": 100}


def test_normalize_authenticates_internal_to_device_limit(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _request_body(request)
    extensions = body["extensions"]
    assert isinstance(extensions, dict)
    to_device = extensions["to_device"]
    assert isinstance(to_device, dict)
    del to_device["limit"]
    tampered = replace(
        request,
        body=json.dumps(body, separators=(",", ":"), sort_keys=True).encode(),
    )

    with pytest.raises(ValueError, match="to-device limit"):
        sliding_source.normalize(
            tampered,
            _result(tampered, _success_body(tampered, pos="p2", count=2)),
        )


def test_normalize_rejects_explicit_null_since_when_cursor_has_no_token(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p1", None, CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _request_body(request)
    extensions = body["extensions"]
    assert isinstance(extensions, dict)
    to_device = extensions["to_device"]
    assert isinstance(to_device, dict)
    to_device["since"] = None
    tampered = replace(request, body=_canonical(body))

    with pytest.raises(ValueError, match="since"):
        sliding_source.normalize(
            tampered,
            _result(tampered, _success_body(tampered, pos="p2", count=2)),
        )


@pytest.mark.parametrize(
    ("configured_scope", "expected_scope"),
    [
        (None, [RESERVED_LIST]),
        ([], [RESERVED_LIST]),
        (["foreground"], ["foreground", RESERVED_LIST]),
        (["*"], [RESERVED_LIST]),
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
            all_rooms_page_size=2,
        ),
        own_user_id=OWN_USER_ID,
    )
    request = source.plan_request(
        _state(
            SlidingCursor(
                None,
                None,
                CONNECTION,
                CONNECTION_NAME,
                1,
                2,
                UNKNOWN_ACK,
                False,
            )
        ),
        7,
    )
    assert request is not None

    extensions = _request_body(request)["extensions"]
    assert isinstance(extensions, dict)
    typing = extensions["typing"]
    assert isinstance(typing, dict)
    assert typing["lists"] == expected_scope


def test_exact_wildcard_is_normalized_for_all_tuwunel_room_extensions() -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            0,
            "worker",
            b"{}",
            b"{}",
            _canonical(
                {
                    name: {"enabled": False, "lists": ["*"], "vendor": name}
                    for name in ("account_data", "typing", "receipts")
                }
            ),
            all_rooms_page_size=2,
        ),
        own_user_id=OWN_USER_ID,
    )
    request = source.plan_request(_state(source.initial_cursor(CONNECTION)), 7)
    assert request is not None

    extensions = _request_body(request)["extensions"]
    assert isinstance(extensions, dict)
    for name in ("account_data", "typing", "receipts"):
        extension = extensions[name]
        assert isinstance(extension, dict)
        assert extension == {
            "enabled": True,
            "lists": [RESERVED_LIST],
            "vendor": name,
        }


def test_non_exact_wildcard_extension_scope_is_rejected() -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            0,
            "worker",
            b"{}",
            b"{}",
            b'{"typing":{"lists":["*","foreground"]}}',
            all_rooms_page_size=2,
        ),
        own_user_id=OWN_USER_ID,
    )

    with pytest.raises(ValueError, match="wildcard"):
        source.plan_request(
            _state(
                SlidingCursor(
                    None,
                    None,
                    CONNECTION,
                    CONNECTION_NAME,
                    1,
                    2,
                    UNKNOWN_ACK,
                    False,
                )
            ),
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
        _state(
            SlidingCursor(
                "p1",
                "td1",
                CONNECTION,
                CONNECTION_NAME,
                1,
                2,
                TXN_ACK,
                False,
            )
        ),
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


@pytest.mark.parametrize("extension_name", ["account_data", "typing", "receipts"])
def test_normalize_rejects_literal_wildcard_room_extension_scope(
    sliding_source: SlidingSource,
    extension_name: str,
) -> None:
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _request_body(request)
    extensions = body["extensions"]
    assert isinstance(extensions, dict)
    extension = extensions[extension_name]
    assert isinstance(extension, dict)
    extension["lists"] = ["*"]
    tampered = replace(request, body=_canonical(body))

    with pytest.raises(ValueError, match="reserved|wildcard"):
        sliding_source.normalize(
            tampered,
            _result(tampered, _success_body(tampered, pos="p2", count=2)),
        )


def test_normalize_rejects_a_tampered_connection_id(
    sliding_source: SlidingSource,
) -> None:
    request = sliding_source.plan_request(
        _state(
            SlidingCursor(
                "p1",
                "td1",
                CONNECTION,
                CONNECTION_NAME,
                1,
                2,
                TXN_ACK,
                False,
            )
        ),
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
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 3, 2, TXN_ACK, False
    )
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
    cursor = SlidingCursor(
        None,
        "td0",
        CONNECTION,
        CONNECTION_NAME,
        1,
        2,
        UNKNOWN_ACK,
        False,
    )
    expected = ((3, False), (5, False), (5, True))

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
        assert candidate["all_rooms_range_ack_mode"] == "txn_echo"
        assert candidate["pos"] == f"p{request_id}"
        assert candidate["to_device_since"] == f"td{request_id}"
        cursor = SlidingCursor(
            str(candidate["pos"]),
            str(candidate["to_device_since"]),
            CONNECTION,
            CONNECTION_NAME,
            end,
            2,
            TXN_ACK,
            complete,
        )


@pytest.mark.parametrize(
    ("complete", "count", "expected_complete"),
    [(False, 2, False), (False, 5, False), (True, 2, False), (True, 5, False)],
)
def test_stale_echo_maps_payload_but_cannot_claim_current_coverage(
    sliding_source: SlidingSource,
    complete: bool,
    count: int,
    expected_complete: bool,
) -> None:
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, complete
    )
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
        "all_rooms_page_size": 2,
        "all_rooms_range_ack_mode": "txn_echo",
        "all_rooms_range_end": 1,
        "connection_instance": str(CONNECTION),
        "connection_name": CONNECTION_NAME,
        "pos": "p2",
        "to_device_since": "td2",
    }


@pytest.mark.parametrize("count", [0, 1, 2])
def test_current_echo_never_shrinks_range_when_count_falls(
    sliding_source: SlidingSource,
    count: int,
) -> None:
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 3, 2, TXN_ACK, False
    )
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
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, True
    )
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


def test_conduwuit_exclusive_end_requires_one_slot_of_overscan(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    first = sliding_source.plan_request(_state(cursor), 7)
    assert first is not None

    first_result = sliding_source.normalize(
        first,
        _result(
            first,
            _success_body(
                first,
                pos="p2",
                count=2,
                to_device_since="td2",
            ),
        ),
    )

    first_candidate = _candidate(first_result)
    assert first_candidate["all_rooms_range_end"] == 2
    assert first_candidate["all_rooms_coverage_complete"] is False
    overscanned = SlidingCursor(
        "p2", "td2", CONNECTION, CONNECTION_NAME, 2, 2, TXN_ACK, False
    )
    second = sliding_source.plan_request(_state(overscanned, request_id=8), 8)
    assert second is not None
    assert _request_body(second)["lists"][RESERVED_LIST]["ranges"] == [[0, 2]]  # type: ignore[index]

    second_result = sliding_source.normalize(
        second,
        _result(
            second,
            _success_body(
                second,
                pos="p3",
                count=2,
                to_device_since="td3",
            ),
        ),
    )

    second_candidate = _candidate(second_result)
    assert second_candidate["all_rooms_range_end"] == 2
    assert second_candidate["all_rooms_coverage_complete"] is True


@pytest.mark.parametrize("txn_id", [None, 7, True])
def test_missing_after_echo_negotiation_or_non_string_echo_is_malformed(
    sliding_source: SlidingSource,
    txn_id: object | None,
) -> None:
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p2",
        count=2,
        to_device_since="td2",
    )
    if txn_id is None:
        del body["txn_id"]
    else:
        body["txn_id"] = txn_id

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.status_code == 200
    assert normalized.detail is not None
    assert "txn_id" in normalized.detail


def test_synapse_missing_echo_negotiates_response_bound_and_expands() -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(30_000, CONNECTION_NAME, b"{}", b"{}", b"{}", 2),
        own_user_id=OWN_USER_ID,
    )
    cursor = source.initial_cursor(CONNECTION)
    request = source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=5,
        to_device_since="td1",
    )
    del body["txn_id"]

    normalized = source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.FRAME
    candidate = _candidate(normalized)
    assert candidate["all_rooms_range_ack_mode"] == "response_bound"
    assert candidate["all_rooms_range_end"] == 3
    assert candidate["all_rooms_coverage_complete"] is False


def test_synapse_response_bound_continuation_without_echo_can_complete(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p1",
        "td1",
        CONNECTION,
        CONNECTION_NAME,
        3,
        2,
        RESPONSE_ACK,
        False,
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p2",
        count=3,
        to_device_since="td2",
    )
    del body["txn_id"]

    normalized = sliding_source.normalize(request, _result(request, body))

    candidate = _candidate(normalized)
    assert candidate["all_rooms_range_ack_mode"] == "response_bound"
    assert candidate["all_rooms_range_end"] == 3
    assert candidate["all_rooms_coverage_complete"] is True


@pytest.mark.parametrize(
    ("start_mode", "echo", "expected_mode", "expected_end"),
    [
        (UNKNOWN_ACK, "older", TXN_ACK, 1),
        (RESPONSE_ACK, "older", TXN_ACK, 1),
        (RESPONSE_ACK, "current", TXN_ACK, 3),
    ],
)
def test_echo_observation_promotes_mode_without_trusting_stale_coverage(
    sliding_source: SlidingSource,
    start_mode: SlidingRangeAckMode,
    echo: str,
    expected_mode: SlidingRangeAckMode,
    expected_end: int,
) -> None:
    cursor = SlidingCursor(
        "p1",
        "td1",
        CONNECTION,
        CONNECTION_NAME,
        1,
        2,
        start_mode,
        False,
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    request_txn = _request_body(request)["txn_id"]
    body = _success_body(
        request,
        pos="p2",
        count=5,
        txn_id=request_txn if echo == "current" else "older-transaction",
        to_device_since="td2",
    )

    normalized = sliding_source.normalize(request, _result(request, body))

    candidate = _candidate(normalized)
    assert candidate["all_rooms_range_ack_mode"] == expected_mode.value
    assert candidate["all_rooms_range_end"] == expected_end
    assert candidate["all_rooms_coverage_complete"] is False


@pytest.mark.parametrize("start_mode", [UNKNOWN_ACK, RESPONSE_ACK])
@pytest.mark.parametrize("invalid_txn", [7, True, []])
def test_non_string_txn_is_malformed_in_every_negotiable_mode(
    sliding_source: SlidingSource,
    start_mode: SlidingRangeAckMode,
    invalid_txn: object,
) -> None:
    cursor = (
        sliding_source.initial_cursor(CONNECTION)
        if start_mode is UNKNOWN_ACK
        else SlidingCursor(
            "p1",
            "td1",
            CONNECTION,
            CONNECTION_NAME,
            1,
            2,
            RESPONSE_ACK,
            False,
        )
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p2",
        count=2,
        txn_id=invalid_txn,
        to_device_since="td2",
    )

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.detail is not None
    assert "txn_id" in normalized.detail


@pytest.mark.parametrize("start_mode", [UNKNOWN_ACK, RESPONSE_ACK, TXN_ACK])
def test_explicit_null_txn_is_malformed_in_every_ack_mode(
    sliding_source: SlidingSource,
    start_mode: SlidingRangeAckMode,
) -> None:
    cursor = (
        sliding_source.initial_cursor(CONNECTION)
        if start_mode is UNKNOWN_ACK
        else SlidingCursor(
            "p1",
            "td1",
            CONNECTION,
            CONNECTION_NAME,
            1,
            2,
            start_mode,
            False,
        )
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p2",
        count=2,
        to_device_since="td2",
    )
    body["txn_id"] = None

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
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
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
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(request, pos="p2", count=2)
    body["extensions"] = {"to_device": {"events": []}}

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.detail is not None
    assert "next_batch" in normalized.detail


@pytest.mark.parametrize("to_device_since", [None, "td1"])
@pytest.mark.parametrize("extensions", [None, {}])
def test_quiet_tuwunel_omission_preserves_independent_to_device_cursor(
    sliding_source: SlidingSource,
    to_device_since: str | None,
    extensions: dict[str, object] | None,
) -> None:
    cursor = SlidingCursor(
        "p1",
        to_device_since,
        CONNECTION,
        CONNECTION_NAME,
        1,
        2,
        TXN_ACK,
        False,
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(request, pos="p2", count=2)
    if extensions is not None:
        body["extensions"] = extensions

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.FRAME
    assert normalized.frame is not None
    assert normalized.frame.to_device_json == ()
    assert _candidate(normalized)["to_device_since"] == to_device_since


def test_restart_replays_with_durable_payload_and_page_size(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p1", "td1", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    restarted = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            45_000,
            "new runtime name",
            b'{"different":{"timeline_limit":7}}',
            b"{}",
            b'{"custom":{"after_restart":true}}',
            all_rooms_page_size=7,
        ),
        own_user_id=OWN_USER_ID,
    )

    normalized = restarted.normalize(
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

    assert normalized.kind is SourceResultKind.FRAME
    assert _candidate(normalized)["all_rooms_range_end"] == 3
    assert _candidate(normalized)["all_rooms_page_size"] == 2


@pytest.mark.parametrize(
    "cursor_json",
    [
        b'{"all_rooms_coverage_complete":false,"all_rooms_page_size":2,'
        b'"all_rooms_range_ack_mode":"unknown","all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"connection_name":"worker","extra":0,"pos":null,'
        b'"to_device_since":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_page_size":2,'
        b'"all_rooms_range_ack_mode":"unknown","all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"connection_name":"worker","pos":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_page_size":2,'
        b'"all_rooms_range_ack_mode":"unknown","all_rooms_range_end":true,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"connection_name":"worker","pos":null,"to_device_since":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_page_size":2,'
        b'"all_rooms_range_ack_mode":"unknown","all_rooms_range_end":1,'
        b'"connection_instance":"not-a-uuid","connection_name":"worker","pos":null,'
        b'"to_device_since":null}',
        b'{ "all_rooms_coverage_complete":false,"all_rooms_page_size":2,'
        b'"all_rooms_range_ack_mode":"unknown","all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"connection_name":"worker","pos":null,"to_device_since":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_page_size":true,'
        b'"all_rooms_range_ack_mode":"unknown","all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"connection_name":"worker","pos":null,"to_device_since":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_page_size":2,'
        b'"all_rooms_range_ack_mode":"future","all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"connection_name":"worker","pos":null,"to_device_since":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_page_size":2,'
        b'"all_rooms_range_ack_mode":"response_bound","all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"connection_name":"worker","pos":null,"to_device_since":null}',
        b'{"all_rooms_coverage_complete":false,"all_rooms_page_size":2,'
        b'"all_rooms_range_ack_mode":"unknown","all_rooms_range_end":1,'
        b'"connection_instance":"236f12d0-c282-4594-8654-948a60a73ee9",'
        b'"connection_name":"","pos":null,"to_device_since":null}',
    ],
)
def test_request_planning_rejects_invalid_or_noncanonical_cursor_bytes(
    sliding_source: SlidingSource,
    cursor_json: bytes,
) -> None:
    state = replace(
        _state(
            SlidingCursor(
                None,
                None,
                CONNECTION,
                CONNECTION_NAME,
                1,
                2,
                UNKNOWN_ACK,
                False,
            )
        ),
        cursor_json=cursor_json,
    )

    with pytest.raises(ValueError):
        sliding_source.plan_request(state, 7)


def test_reserved_name_collision_is_rejected_without_silent_override() -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            0,
            "worker",
            b'{"__nio_all_rooms_v1":{}}',
            b"{}",
            b"{}",
            2,
        ),
        own_user_id=OWN_USER_ID,
    )

    with pytest.raises(ValueError, match="reserved"):
        source.plan_request(
            _state(
                SlidingCursor(
                    None,
                    None,
                    CONNECTION,
                    CONNECTION_NAME,
                    1,
                    2,
                    UNKNOWN_ACK,
                    False,
                )
            ),
            7,
        )


@pytest.mark.parametrize(("caller_count", "allowed"), [(99, True), (100, False)])
def test_reserved_list_slot_is_kept_within_wire_limit(
    caller_count: int,
    allowed: bool,
) -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            0,
            CONNECTION_NAME,
            json.dumps({f"list-{index}": {} for index in range(caller_count)}).encode(),
            b"{}",
            b"{}",
            all_rooms_page_size=2,
        ),
        own_user_id=OWN_USER_ID,
    )
    state = _state(source.initial_cursor(CONNECTION))

    if allowed:
        request = source.plan_request(state, 7)
        assert request is not None
        lists = _request_body(request)["lists"]
        assert isinstance(lists, dict)
        assert len(lists) == 100
        assert RESERVED_LIST in lists
    else:
        with pytest.raises(ValueError, match="99"):
            source.plan_request(state, 7)


@pytest.mark.parametrize(("subscription_count", "allowed"), [(100, True), (101, False)])
def test_room_subscription_wire_limit_is_validated(
    subscription_count: int,
    allowed: bool,
) -> None:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            0,
            CONNECTION_NAME,
            b"{}",
            json.dumps(
                {
                    f"!room-{index}:example.org": {}
                    for index in range(subscription_count)
                }
            ).encode(),
            b"{}",
            all_rooms_page_size=2,
        ),
        own_user_id=OWN_USER_ID,
    )
    state = _state(source.initial_cursor(CONNECTION))

    if allowed:
        assert source.plan_request(state, 7) is not None
    else:
        with pytest.raises(ValueError, match="100"):
            source.plan_request(state, 7)


def test_inactive_source_does_not_plan_network_work(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        None,
        None,
        CONNECTION,
        CONNECTION_NAME,
        1,
        2,
        UNKNOWN_ACK,
        False,
    )
    assert sliding_source.plan_request(_state(cursor, active=False), 7) is None


def test_rooms_and_extensions_normalize_without_losing_order_or_duplicates(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 5, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    own_join = {
        "content": {"membership": "join"},
        "event_id": "$own",
        "state_key": OWN_USER_ID,
        "type": "m.room.member",
    }
    live = {
        "content": {"body": "live", "msgtype": "m.text"},
        "event_id": "$live",
        "type": "m.room.message",
    }
    tag = {"content": {"tags": {"u.work": {}}}, "type": "m.tag"}
    account_only = {"content": {"event_id": "$seen"}, "type": "m.fully_read"}
    global_data = {"content": {"theme": "dark"}, "type": "org.example.theme"}
    typing_a = {"content": {"user_ids": ["@a:example.org"]}, "type": "m.typing"}
    receipt_a = {"content": {"$e": {"m.read": {}}}, "type": "m.receipt"}
    typing_z = {"content": {"user_ids": ["@z:example.org"]}, "type": "m.typing"}
    receipt_z = {"content": {"$z": {"m.read": {}}}, "type": "m.receipt"}
    to_device = {"content": {"request_id": "r"}, "type": "m.room_key_request"}
    presence = {
        "content": {"presence": "online"},
        "sender": "@a:example.org",
        "type": "m.presence",
    }
    body = _success_body(request, pos="p1", count=6)
    body["rooms"] = {
        "!z:example.org": {
            "membership": "join",
            "required_state": [own_join],
            "timeline": [own_join, live],
            "num_live": 1,
            "limited": True,
            "prev_batch": "z-prev",
        },
        "!leave:example.org": {
            "membership": "ban",
            "required_state": [
                {
                    "content": {"membership": "ban"},
                    "event_id": "$ban",
                    "state_key": OWN_USER_ID,
                    "type": "m.room.member",
                }
            ],
        },
        "!implicit:example.org": {"required_state": []},
        "!knock:example.org": {
            "membership": "knock",
            "stripped_state": [
                {
                    "content": {"membership": "knock"},
                    "state_key": OWN_USER_ID,
                    "type": "m.room.member",
                }
            ],
        },
        "!invite:example.org": {
            "invite_state": [
                {
                    "content": {"membership": "invite"},
                    "state_key": OWN_USER_ID,
                    "type": "m.room.member",
                }
            ]
        },
        "!expanded:example.org": {
            "membership": "join",
            "expanded_timeline": True,
            "timeline": [live],
        },
    }
    body["extensions"] = {
        "to_device": {
            "events": [to_device, to_device],
            "next_batch": "td1",
        },
        "e2ee": {
            "device_lists": {
                "changed": ["@z:example.org", "@a:example.org", "@z:example.org"],
                "left": ["@old:example.org"],
            },
            "device_one_time_keys_count": {
                "signed_curve25519": 5,
                "curve25519": 3,
            },
            "device_unused_fallback_key_types": [
                "signed_curve25519",
                "signed_curve25519",
            ],
        },
        "account_data": {
            "global": [global_data, global_data],
            "rooms": {
                "!z:example.org": [tag, tag],
                "!account-only:example.org": [account_only],
            },
        },
        "typing": {
            "rooms": {
                "!z:example.org": typing_z,
                "!a:example.org": typing_a,
            }
        },
        "receipts": {
            "rooms": {
                "!z:example.org": receipt_z,
                "!a:example.org": receipt_a,
            }
        },
        "presence": {"events": [presence, presence]},
    }

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.FRAME
    assert normalized.frame is not None
    frame = normalized.frame
    assert tuple((room.section, room.room_id) for room in frame.room_segments) == (
        (RoomSection.INVITE, "!invite:example.org"),
        (RoomSection.KNOCK, "!knock:example.org"),
        (RoomSection.JOIN, "!expanded:example.org"),
        (RoomSection.JOIN, "!implicit:example.org"),
        (RoomSection.JOIN, "!z:example.org"),
        (RoomSection.LEAVE, "!leave:example.org"),
        (RoomSection.UNCHANGED, "!account-only:example.org"),
    )
    invite = frame.room_segments[0]
    assert invite.state_json[0] == _canonical(body["rooms"]["!invite:example.org"]["invite_state"][0])  # type: ignore[index]
    expanded = frame.room_segments[2]
    assert expanded.initial is False
    assert expanded.expanded_timeline is True
    assert expanded.history_discontinuity is True
    assert expanded.live_event_count == 0
    joined = frame.room_segments[4]
    assert joined.timeline_json == (_canonical(own_join), _canonical(live))
    assert joined.timeline_limited is True
    assert joined.timeline_prev_batch == "z-prev"
    assert joined.live_event_count == 1
    assert joined.room_account_data_json == (_canonical(tag), _canonical(tag))
    unchanged = frame.room_segments[-1]
    assert unchanged.state_json == ()
    assert unchanged.timeline_json == ()
    assert unchanged.room_account_data_json == (_canonical(account_only),)
    assert unchanged.initial is False
    assert unchanged.expanded_timeline is False
    assert unchanged.history_discontinuity is False
    assert unchanged.membership_observation == MembershipObservation(
        None, None, None, None, None, False, False, False, False
    )
    with pytest.raises(ValueError, match="account-data-only"):
        replace(
            unchanged,
            timeline_json=(b"{}",),
            live_event_count=1,
        )
    assert frame.to_device_json == (_canonical(to_device), _canonical(to_device))
    assert frame.device_list_delta_json == _canonical(
        {
            "changed": ["@a:example.org", "@z:example.org"],
            "left": ["@old:example.org"],
        }
    )
    assert frame.one_time_key_counts_json == _canonical(
        {"curve25519": 3, "signed_curve25519": 5}
    )
    assert frame.unused_fallback_key_types_json == _canonical(["signed_curve25519"])
    assert frame.global_account_data_json == (
        _canonical(global_data),
        _canonical(global_data),
    )
    assert frame.presence_json == (_canonical(presence), _canonical(presence))
    assert frame.ephemeral_json == tuple(
        _canonical({"event": event, "room_id": room_id})
        for room_id, event in (
            ("!a:example.org", typing_a),
            ("!a:example.org", receipt_a),
            ("!z:example.org", typing_z),
            ("!z:example.org", receipt_z),
        )
    )


def test_empty_account_data_only_room_is_ignored_but_nonempty_room_is_retained(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    account_event = {"content": {"event_id": "$seen"}, "type": "m.fully_read"}
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    extensions = body["extensions"]
    assert isinstance(extensions, dict)
    extensions["account_data"] = {
        "rooms": {
            "!empty:example.org": [],
            "!nonempty:example.org": [account_event],
        }
    }

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.FRAME
    assert normalized.frame is not None
    assert tuple(
        (segment.section, segment.room_id, segment.room_account_data_json)
        for segment in normalized.frame.room_segments
    ) == (
        (
            RoomSection.UNCHANGED,
            "!nonempty:example.org",
            (_canonical(account_event),),
        ),
    )


def test_connection_initial_request_marks_room_history_non_live(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        None,
        None,
        CONNECTION,
        CONNECTION_NAME,
        1,
        2,
        UNKNOWN_ACK,
        False,
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    event = {"event_id": "$history", "type": "m.room.message"}
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {"!room:example.org": {"timeline": [event]}}

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.frame is not None
    segment = normalized.frame.room_segments[0]
    assert segment.initial is True
    assert segment.expanded_timeline is False
    assert segment.live_event_count == 0


@pytest.mark.parametrize(
    "room",
    [
        {"membership": "joined"},
        {"num_live": True},
        {"timeline": [], "num_live": -1},
        {"timeline": [], "num_live": 1},
        {"initial": 1},
        {"limited": 0},
        {"expanded_timeline": True, "unstable_expanded_timeline": False},
    ],
)
def test_invalid_room_membership_window_or_flags_fail_closed(
    sliding_source: SlidingSource,
    room: dict[str, object],
) -> None:
    request = sliding_source.plan_request(
        _state(
            SlidingCursor(
                "p0",
                "td0",
                CONNECTION,
                CONNECTION_NAME,
                1,
                2,
                TXN_ACK,
                False,
            )
        ),
        7,
    )
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {"!room:example.org": room}

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.status_code == 200


def test_membership_extraction_uses_exact_own_identity_and_live_suffix(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    other_join = {
        "content": {"membership": "join"},
        "event_id": "$other",
        "state_key": "@other:example.org",
        "type": "m.room.member",
    }
    historical_own_join = {
        "content": {"membership": "join"},
        "event_id": "$own-history",
        "state_key": OWN_USER_ID,
        "type": "m.room.member",
    }
    live = {"event_id": "$live", "type": "m.room.message"}
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {
        "!room:example.org": {
            "membership": "join",
            "required_state": [other_join],
            "timeline": [historical_own_join, live],
            "num_live": 1,
        }
    }
    normalized = sliding_source.normalize(request, _result(request, body))
    assert normalized.frame is not None

    observation = sliding_membership_observation(
        normalized.frame.room_segments[0],
        OWN_USER_ID,
    )

    assert observation.room_membership == "join"
    assert observation.event_membership is None
    assert observation.event_id is None
    assert observation.is_live is False


def test_membership_extraction_marks_live_own_join_and_unparsed_stub(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    own_join = {
        "content": {"membership": "join"},
        "event_id": "$own-live",
        "state_key": OWN_USER_ID,
        "type": "m.room.member",
        "unsigned": {
            "prev_content": {"membership": "join"},
            "replaces_state": "$old",
        },
    }
    body = _success_body(
        request,
        pos="p1",
        count=2,
        to_device_since="td1",
    )
    body["rooms"] = {
        "!live:example.org": {"timeline": [own_join], "num_live": 1},
        "!stub:example.org": {
            "required_state": [{"state_key": OWN_USER_ID, "type": "m.room.member"}]
        },
    }
    normalized = sliding_source.normalize(request, _result(request, body))
    assert normalized.frame is not None
    by_id = {segment.room_id: segment for segment in normalized.frame.room_segments}

    live_observation = sliding_membership_observation(
        by_id["!live:example.org"],
        OWN_USER_ID,
    )
    stub_observation = sliding_membership_observation(
        by_id["!stub:example.org"],
        OWN_USER_ID,
    )

    assert live_observation.event_membership == "join"
    assert live_observation.event_id == "$own-live"
    assert live_observation.previous_membership == "join"
    assert live_observation.replaces_state == "$old"
    assert live_observation.is_live is True
    assert stub_observation.is_unparsed is True


def test_membership_observation_is_stored_not_reparsed_from_segment_bytes(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(request, pos="p1", count=1, to_device_since="td1")
    body["rooms"] = {
        "!room:example.org": {
            "membership": "join",
            "required_state": [_own_member("$own")],
        }
    }

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.frame is not None
    segment = normalized.frame.room_segments[0]
    corrupted = replace(segment, state_json=(b"not json",))
    assert (
        sliding_membership_observation(corrupted, OWN_USER_ID)
        is segment.membership_observation
    )


def _own_member(
    event_id: str,
    membership: str = "join",
    *,
    previous_membership: str | None = None,
    replaces_state: str | None = None,
) -> dict[str, object]:
    event: dict[str, object] = {
        "content": {"membership": membership},
        "event_id": event_id,
        "state_key": OWN_USER_ID,
        "type": "m.room.member",
    }
    unsigned: dict[str, object] = {}
    if previous_membership is not None:
        unsigned["prev_content"] = {"membership": previous_membership}
    if replaces_state is not None:
        unsigned["replaces_state"] = replaces_state
    if unsigned:
        event["unsigned"] = unsigned
    return event


@pytest.mark.parametrize(
    ("room", "expected_section"),
    [
        pytest.param(
            {"required_state": [_own_member("$join", "join")]},
            RoomSection.JOIN,
            id="required-state-join",
        ),
        pytest.param(
            {"required_state": [_own_member("$leave", "leave")]},
            RoomSection.LEAVE,
            id="required-state-leave",
        ),
        pytest.param(
            {"stripped_state": [_own_member("$knock", "knock")]},
            RoomSection.KNOCK,
            id="stripped-state-knock",
        ),
        pytest.param(
            {"invite_state": [_own_member("$invite", "invite")]},
            RoomSection.INVITE,
            id="invite-state-invite",
        ),
        pytest.param(
            {"timeline": [_own_member("$join", "join")], "num_live": 1},
            RoomSection.JOIN,
            id="live-timeline-join",
        ),
        pytest.param(
            {
                "timeline": [
                    _own_member("$historical-leave", "leave"),
                    {"event_id": "$live", "type": "m.room.message"},
                ],
                "num_live": 1,
            },
            RoomSection.JOIN,
            id="historical-membership-outside-live-suffix",
        ),
    ],
)
def test_native_room_without_top_level_membership_derives_exact_own_section(
    sliding_source: SlidingSource,
    room: dict[str, object],
    expected_section: RoomSection,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {"!room:example.org": room}

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.FRAME
    assert normalized.frame is not None
    segment = normalized.frame.room_segments[0]
    assert segment.section is expected_section
    observation = sliding_membership_observation(segment, OWN_USER_ID)
    if expected_section is RoomSection.JOIN and "required_state" not in room:
        assert observation.event_membership in {None, "join"}
    else:
        assert observation.event_membership == expected_section.value


@pytest.mark.parametrize(
    "stripped_state",
    [
        [
            {
                "content": {"membership": "invite"},
                "state_key": "@other:example.org",
                "type": "m.room.member",
            }
        ],
        [{"state_key": OWN_USER_ID, "type": "m.room.member"}],
    ],
)
def test_native_stripped_room_without_parseable_own_membership_fails_closed(
    sliding_source: SlidingSource,
    stripped_state: list[dict[str, object]],
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {"!room:example.org": {"stripped_state": stripped_state}}

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is SourceResultKind.TERMINAL_ERROR
    assert normalized.status_code == 200
    assert normalized.detail is not None
    assert "membership" in normalized.detail


@pytest.mark.parametrize(
    ("top_level", "event_membership", "expected_kind", "expected_section"),
    [
        pytest.param(
            "join",
            "leave",
            SourceResultKind.TERMINAL_ERROR,
            None,
            id="join-contradicts-own-leave",
        ),
        pytest.param(
            "leave",
            "join",
            SourceResultKind.TERMINAL_ERROR,
            None,
            id="leave-contradicts-own-join",
        ),
        pytest.param(
            "invite",
            "invite",
            SourceResultKind.FRAME,
            RoomSection.INVITE,
            id="explicit-invite-agrees",
        ),
        pytest.param(
            "ban",
            "leave",
            SourceResultKind.FRAME,
            RoomSection.LEAVE,
            id="ban-and-leave-share-departed-section",
        ),
    ],
)
def test_explicit_room_membership_must_agree_with_exact_own_evidence(
    sliding_source: SlidingSource,
    top_level: str,
    event_membership: str,
    expected_kind: SourceResultKind,
    expected_section: RoomSection | None,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {
        "!room:example.org": {
            "membership": top_level,
            "required_state": [_own_member("$own", event_membership)],
        }
    }

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.kind is expected_kind
    if expected_section is not None:
        assert normalized.frame is not None
        assert normalized.frame.room_segments[0].section is expected_section


@pytest.mark.parametrize(
    ("baseline", "room", "expected"),
    [
        pytest.param(
            None,
            {"required_state": [_own_member("$leave", "leave")]},
            MembershipProof(MembershipProofKind.DEPARTED, None),
            id="explicit-membership-loss",
        ),
        pytest.param(
            None,
            {"required_state": [_own_member("$m1")]},
            MembershipProof(MembershipProofKind.ESTABLISHED, "$m1"),
            id="first-current-membership",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {"required_state": [_own_member("$m1")]},
            MembershipProof(MembershipProofKind.CONTINUES, "$m1"),
            id="exact-held-membership",
        ),
        pytest.param(
            _baseline("w1", "$old"),
            {"timeline": [_own_member("$new")], "num_live": 1},
            MembershipProof(MembershipProofKind.CHANGED, "$new"),
            id="live-own-join",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {
                "required_state": [
                    _own_member(
                        "$m2",
                        previous_membership="join",
                        replaces_state="$m1",
                    )
                ]
            },
            MembershipProof(MembershipProofKind.CONTINUES, "$m2"),
            id="linked-join-rotation",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {
                "required_state": [
                    _own_member(
                        "$m2",
                        previous_membership="join",
                        replaces_state="$other",
                    )
                ]
            },
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="wrong-predecessor",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {"required_state": [_own_member("$m2")]},
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="unlinked-required-state-rejoin",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {"required_state": [{"state_key": OWN_USER_ID, "type": "m.room.member"}]},
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="unparsed-own-state",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {},
            MembershipProof(MembershipProofKind.CONTINUES, "$m1"),
            id="ordinary-delta",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {"initial": True},
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="initial-without-proof",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {"expanded_timeline": True},
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="expanded-without-proof",
        ),
        pytest.param(
            _baseline("w1", "$m1"),
            {"membership": "invite"},
            MembershipProof(MembershipProofKind.DEPARTED, None),
            id="invite",
        ),
        pytest.param(
            _baseline("w1", "$old"),
            {
                "required_state": [_own_member("$current")],
                "timeline": [_own_member("$historical"), {"type": "m.room.message"}],
                "initial": True,
                "num_live": 1,
            },
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="historical-join-outside-live-suffix",
        ),
    ],
)
def test_sliding_adapter_ports_legacy_membership_proof_cases(
    sliding_source: SlidingSource,
    baseline: MembershipBaseline | None,
    room: dict[str, object],
    expected: MembershipProof,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {"!room:example.org": room}
    normalized = sliding_source.normalize(request, _result(request, body))
    assert normalized.frame is not None
    observation = sliding_membership_observation(
        normalized.frame.room_segments[0],
        OWN_USER_ID,
    )

    assert prove_membership(baseline, observation) == expected


def test_expanded_timeline_is_a_distinct_recoverable_discontinuity(
    sliding_source: SlidingSource,
) -> None:
    baseline = _baseline("old-prev", "$m1")
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {
        "!room:example.org": {
            "expanded_timeline": True,
            "required_state": [_own_member("$m1")],
            "prev_batch": "new-prev",
        }
    }
    normalized = sliding_source.normalize(request, _result(request, body))
    assert normalized.frame is not None
    segment = normalized.frame.room_segments[0]
    proof = prove_membership(
        baseline,
        sliding_membership_observation(segment, OWN_USER_ID),
    )

    assert segment.initial is False
    assert segment.expanded_timeline is True
    assert (
        membership_recovery_cursor(
            baseline,
            proof,
            discontinuity=segment.history_discontinuity,
            current_prev_batch=segment.timeline_prev_batch,
        )
        == "old-prev"
    )


def test_expanded_incomplete_room_stores_unsafe_membership_evidence(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    body["rooms"] = {
        "!room:example.org": {
            "membership": "join",
            "expanded_timeline": True,
        }
    }

    normalized = sliding_source.normalize(request, _result(request, body))

    assert normalized.frame is not None
    observation = normalized.frame.room_segments[0].membership_observation
    assert observation.is_expanded_timeline is True
    assert prove_membership(
        _baseline("old-prev", "$member"), observation
    ) == MembershipProof(
        MembershipProofKind.UNSAFE,
        None,
    )


@pytest.mark.parametrize(
    ("status_code", "expected"),
    [
        (400, SourceResultKind.RESET_REQUIRED),
        (401, SourceResultKind.TERMINAL_ERROR),
        (403, SourceResultKind.TERMINAL_ERROR),
        (404, SourceResultKind.TERMINAL_ERROR),
    ],
)
def test_unknown_position_requires_reset_only_at_exact_http_400(
    sliding_source: SlidingSource,
    status_code: int,
    expected: SourceResultKind,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, True
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    network_result = NetworkResult(
        stream_id=request.stream_id,
        transport=request.transport,
        source_epoch=request.source_epoch,
        request_id=request.request_id,
        status_code=status_code,
        body=b'{"error":"expired","errcode":"M_UNKNOWN_POS"}',
        failure=None,
        retry_after_ms=None,
    )

    normalized = sliding_source.normalize(request, network_result)

    assert normalized.kind is expected
    if expected is SourceResultKind.RESET_REQUIRED:
        transition = reset_sliding_connection(cursor, NEXT_CONNECTION)
        assert transition.history_uncertain is True
        assert transition.cursor.pos is None
        assert transition.cursor.to_device_since == "td0"
        assert transition.cursor.connection_instance == NEXT_CONNECTION
        assert transition.cursor.all_rooms_range_end == 1
        assert transition.cursor.all_rooms_page_size == 2
        assert transition.cursor.all_rooms_coverage_complete is False


def test_same_payload_has_deterministic_but_request_bound_frame_identity(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request_seven = sliding_source.plan_request(_state(cursor, request_id=7), 7)
    request_eight = sliding_source.plan_request(_state(cursor, request_id=8), 8)
    assert request_seven is not None
    assert request_eight is not None
    body = _success_body(
        request_seven,
        pos="p1",
        count=1,
        to_device_since="td1",
    )

    frame_seven = sliding_source.normalize(
        request_seven,
        _result(request_seven, body),
    ).frame
    frame_eight = sliding_source.normalize(
        request_eight,
        _result(request_eight, body),
    ).frame

    assert frame_seven is not None
    assert frame_eight is not None
    digest = frame_seven.source_sha256.hex()
    assert frame_seven.frame_id == uuid5(STREAM_ID, f"4:7:{digest}")
    assert frame_eight.frame_id == uuid5(STREAM_ID, f"4:8:{digest}")
    assert frame_seven.frame_id != frame_eight.frame_id


def test_sliding_result_from_another_stream_is_rejected(
    sliding_source: SlidingSource,
) -> None:
    cursor = SlidingCursor(
        "p0", "td0", CONNECTION, CONNECTION_NAME, 1, 2, TXN_ACK, False
    )
    request = sliding_source.plan_request(_state(cursor), 7)
    assert request is not None
    body = _success_body(
        request,
        pos="p1",
        count=1,
        to_device_since="td1",
    )
    foreign = replace(
        _result(request, body),
        stream_id=UUID("dbfca53e-13ca-4b4a-b83d-0a3d838ac82f"),
    )

    with pytest.raises(ValueError, match="does not match"):
        sliding_source.normalize(request, foreign)


def test_sliding_adapter_is_synchronous_and_owns_no_runtime_resources(
    sliding_source: SlidingSource,
) -> None:
    import nio.ingest.sliding as sliding_module

    assert not inspect.iscoroutinefunction(sliding_source.plan_request)
    assert not inspect.iscoroutinefunction(sliding_source.normalize)
    assert "asyncio" not in vars(sliding_module)
    assert "uuid4" not in vars(sliding_module)
    imported_modules = {
        getattr(value, "__module__", "") for value in vars(sliding_module).values()
    }
    assert not any(name.startswith("nio.client") for name in imported_modules)
    assert not any(name.startswith("nio.store") for name in imported_modules)
