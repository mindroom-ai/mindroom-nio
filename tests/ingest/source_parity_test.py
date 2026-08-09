import json
from dataclasses import fields
from uuid import UUID

import pytest

from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.model import EventRecord, RecordKind, RecordOrigin, TransportKind
from nio.ingest.ports import NetworkRequest, NetworkResult
from nio.ingest.sliding import (
    SlidingCursor,
    SlidingRangeAckMode,
    SlidingSource,
    canonical_sliding_cursor,
)
from nio.ingest.source import ClassicCursor, SyncFrame, canonical_classic_cursor
from nio.ingest.state import SourceState

STREAM_ID = UUID("34e80daa-8fe8-45aa-bb79-bd2c2496b99f")
CONNECTION = UUID("c31f578f-e556-48a8-9e28-81161261037d")
OWN_USER_ID = "@own:example.org"
ROOM_ID = "!room:example.org"
RESERVED_LIST = "__nio_all_rooms_v1"


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


def _shape(frame: SyncFrame) -> tuple[tuple[RecordKind, str | None, bytes], ...]:
    records: list[tuple[RecordKind, str | None, bytes]] = []
    records.extend(
        (RecordKind.TO_DEVICE, None, event) for event in frame.to_device_json
    )
    for segment in frame.room_segments:
        records.extend(
            (RecordKind.STATE, segment.room_id, event) for event in segment.state_json
        )
        records.extend(
            (RecordKind.TIMELINE, segment.room_id, event)
            for event in segment.timeline_json
        )
        records.extend(
            (RecordKind.ROOM_ACCOUNT_DATA, segment.room_id, event)
            for event in segment.room_account_data_json
        )
    records.extend(
        (RecordKind.EPHEMERAL, None, event) for event in frame.ephemeral_json
    )
    records.extend(
        (RecordKind.GLOBAL_ACCOUNT_DATA, None, event)
        for event in frame.global_account_data_json
    )
    records.extend((RecordKind.PRESENCE, None, event) for event in frame.presence_json)
    return tuple(records)


def test_sync_frame_keeps_only_a_digest_while_event_records_keep_source_json() -> None:
    sync_frame_fields = {field.name for field in fields(SyncFrame)}
    event_record_fields = {field.name for field in fields(EventRecord)}
    source_json = b'{"content":{"body":"record-owned"},"type":"m.room.message"}'
    record = EventRecord(
        "$record",
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 1, 2, 0),
        ROOM_ID,
        1,
        1,
        "$record",
        None,
        source_json,
        None,
    )

    assert "source_sha256" in sync_frame_fields
    assert "source_json" not in sync_frame_fields
    assert "response_body" not in sync_frame_fields
    assert "source_json" in event_record_fields
    assert record.source_json == source_json


def _membership_observation_pair(
    *,
    classic_section: str,
    sliding_membership: str,
    state: list[dict[str, object]],
    timeline: list[dict[str, object]] | None = None,
    live: bool = False,
    initial: bool = False,
) -> tuple[object, object]:
    classic = ClassicSource(STREAM_ID, ClassicSourceConfig(30_000, b"{}"), OWN_USER_ID)
    classic_cursor = ClassicCursor(None if initial else "s0")
    classic_request = classic.plan_request(
        SourceState(
            2,
            TransportKind.CLASSIC,
            canonical_classic_cursor(classic_cursor),
            4,
            True,
        ),
        4,
    )
    assert classic_request is not None
    state_key = {"invite": "invite_state", "knock": "knock_state"}.get(
        classic_section, "state"
    )
    timeline = timeline or []
    classic_body = {
        "next_batch": "s1",
        "rooms": {
            classic_section: {
                ROOM_ID: {
                    state_key: {"events": state},
                    "timeline": {"events": timeline},
                }
            }
        },
    }
    classic_result = classic.normalize(
        classic_request, _result(classic_request, classic_body)
    )
    assert classic_result.frame is not None

    sliding = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(30_000, "worker", b"{}", b"{}", b"{}", 2),
        OWN_USER_ID,
    )
    sliding_cursor = SlidingCursor(
        None if initial else "p0",
        "td0",
        CONNECTION,
        "worker",
        1,
        2,
        SlidingRangeAckMode.UNKNOWN if initial else SlidingRangeAckMode.TXN_ECHO,
        False,
    )
    sliding_request = sliding.plan_request(
        SourceState(
            2,
            TransportKind.SLIDING,
            canonical_sliding_cursor(sliding_cursor),
            4,
            True,
        ),
        4,
    )
    assert sliding_request is not None
    assert sliding_request.body is not None
    room: dict[str, object] = {
        "membership": sliding_membership,
        "timeline": timeline,
        "num_live": len(timeline) if live else 0,
    }
    if sliding_membership in {"invite", "knock"}:
        room["stripped_state"] = state
    else:
        room["required_state"] = state
    sliding_body = {
        "pos": "p1",
        "txn_id": json.loads(sliding_request.body)["txn_id"],
        "lists": {RESERVED_LIST: {"count": 1}},
        "rooms": {ROOM_ID: room},
    }
    sliding_result = sliding.normalize(
        sliding_request, _result(sliding_request, sliding_body)
    )
    assert sliding_result.frame is not None
    return (
        classic_result.frame.room_segments[0].membership_observation,
        sliding_result.frame.room_segments[0].membership_observation,
    )


def _own_member(
    membership: str,
    event_id: str,
    *,
    previous_membership: str | None = None,
    replaces_state: str | None = None,
) -> dict[str, object]:
    event: dict[str, object] = {
        "type": "m.room.member",
        "state_key": OWN_USER_ID,
        "event_id": event_id,
        "content": {"membership": membership},
    }
    if previous_membership is not None or replaces_state is not None:
        event["unsigned"] = {
            "prev_content": {"membership": previous_membership},
            "replaces_state": replaces_state,
        }
    return event


@pytest.mark.parametrize(
    ("classic_section", "sliding_membership", "state", "timeline", "live"),
    [
        pytest.param(
            "join",
            "join",
            [_own_member("join", "$same")],
            [_own_member("join", "$same")],
            True,
            id="same-id-live-join",
        ),
        pytest.param(
            "join",
            "join",
            [
                _own_member(
                    "join", "$new", previous_membership="join", replaces_state="$old"
                )
            ],
            None,
            False,
            id="linked-non-live-join",
        ),
        pytest.param(
            "invite",
            "invite",
            [_own_member("invite", "$invite")],
            None,
            False,
            id="invite",
        ),
        pytest.param(
            "knock", "knock", [_own_member("knock", "$knock")], None, False, id="knock"
        ),
        pytest.param(
            "leave", "leave", [_own_member("leave", "$leave")], None, False, id="leave"
        ),
        pytest.param(
            "leave", "ban", [_own_member("ban", "$ban")], None, False, id="ban"
        ),
        pytest.param(
            "join",
            "join",
            [{"type": "m.room.member", "state_key": OWN_USER_ID}],
            None,
            False,
            id="malformed-own-member",
        ),
    ],
)
def test_classic_and_sliding_preserve_equal_membership_observations(
    classic_section: str,
    sliding_membership: str,
    state: list[dict[str, object]],
    timeline: list[dict[str, object]] | None,
    live: bool,
) -> None:
    classic_observation, sliding_observation = _membership_observation_pair(
        classic_section=classic_section,
        sliding_membership=sliding_membership,
        state=state,
        timeline=timeline,
        live=live,
    )

    assert classic_observation == sliding_observation
    observation = classic_observation
    assert observation.room_membership == classic_section
    assert observation.is_live is live
    if "content" not in state[-1]:
        assert observation.is_unparsed is True
        assert observation.event_membership is None
        assert observation.event_id is None
    else:
        event = (timeline or state)[-1] if live else state[-1]
        content = event["content"]
        assert type(content) is dict
        assert observation.is_unparsed is False
        assert observation.event_membership == content["membership"]
        assert observation.event_id == event["event_id"]


def test_classic_and_sliding_preserve_initial_incomplete_observations() -> None:
    classic_observation, sliding_observation = _membership_observation_pair(
        classic_section="join",
        sliding_membership="join",
        state=[],
        initial=True,
    )

    assert classic_observation == sliding_observation
    assert classic_observation.is_initial is True


def test_equivalent_classic_and_sliding_payloads_have_equal_record_shape() -> None:
    state_event = {
        "content": {"membership": "join"},
        "event_id": "$member",
        "state_key": OWN_USER_ID,
        "type": "m.room.member",
    }
    timeline_event = {
        "content": {"body": "hello", "msgtype": "m.text"},
        "event_id": "$message",
        "type": "m.room.message",
    }
    room_account_data = {"content": {"tags": {"u.work": {}}}, "type": "m.tag"}
    typing = {"content": {"user_ids": [OWN_USER_ID]}, "type": "m.typing"}
    receipt = {"content": {"$message": {"m.read": {}}}, "type": "m.receipt"}
    global_account_data = {"content": {"theme": "dark"}, "type": "org.example.theme"}
    presence = {
        "content": {"presence": "online"},
        "sender": OWN_USER_ID,
        "type": "m.presence",
    }
    to_device = {"content": {"request_id": "r"}, "type": "m.room_key_request"}
    device_lists = {
        "changed": ["@z:example.org", "@a:example.org"],
        "left": ["@old:example.org"],
    }
    key_counts = {"curve25519": 3, "signed_curve25519": 5}
    fallback = ["signed_curve25519"]

    classic = ClassicSource(
        STREAM_ID,
        ClassicSourceConfig(30_000, b"{}"),
        own_user_id=OWN_USER_ID,
    )
    classic_state = SourceState(
        2,
        TransportKind.CLASSIC,
        canonical_classic_cursor(ClassicCursor("s0")),
        4,
        True,
    )
    classic_request = classic.plan_request(classic_state, 4)
    assert classic_request is not None
    classic_body = {
        "next_batch": "s1",
        "rooms": {
            "join": {
                ROOM_ID: {
                    "state": {"events": [state_event]},
                    "timeline": {
                        "events": [timeline_event],
                        "limited": True,
                        "prev_batch": "prev",
                    },
                    "account_data": {"events": [room_account_data]},
                    "ephemeral": {"events": [typing, receipt]},
                }
            }
        },
        "account_data": {"events": [global_account_data]},
        "presence": {"events": [presence]},
        "to_device": {"events": [to_device]},
        "device_lists": device_lists,
        "device_one_time_keys_count": key_counts,
        "device_unused_fallback_key_types": fallback,
    }
    classic_frame = classic.normalize(
        classic_request,
        _result(classic_request, classic_body),
    ).frame
    assert classic_frame is not None

    sliding = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(30_000, "worker", b"{}", b"{}", b"{}", 2),
        own_user_id=OWN_USER_ID,
    )
    sliding_cursor = SlidingCursor(
        "p0",
        "td0",
        CONNECTION,
        "worker",
        1,
        2,
        SlidingRangeAckMode.TXN_ECHO,
        False,
    )
    sliding_state = SourceState(
        2,
        TransportKind.SLIDING,
        canonical_sliding_cursor(sliding_cursor),
        4,
        True,
    )
    sliding_request = sliding.plan_request(sliding_state, 4)
    assert sliding_request is not None
    assert sliding_request.body is not None
    txn_id = json.loads(sliding_request.body)["txn_id"]
    sliding_body = {
        "pos": "p1",
        "txn_id": txn_id,
        "lists": {RESERVED_LIST: {"count": 1}},
        "rooms": {
            ROOM_ID: {
                "membership": "join",
                "required_state": [state_event],
                "timeline": [timeline_event],
                "num_live": 1,
                "limited": True,
                "prev_batch": "prev",
            }
        },
        "extensions": {
            "to_device": {"events": [to_device], "next_batch": "td1"},
            "e2ee": {
                "device_lists": device_lists,
                "device_one_time_keys_count": key_counts,
                "device_unused_fallback_key_types": fallback,
            },
            "account_data": {
                "global": [global_account_data],
                "rooms": {ROOM_ID: [room_account_data]},
            },
            "typing": {"rooms": {ROOM_ID: typing}},
            "receipts": {"rooms": {ROOM_ID: receipt}},
            "presence": {"events": [presence]},
        },
    }
    sliding_frame = sliding.normalize(
        sliding_request,
        _result(sliding_request, sliding_body),
    ).frame
    assert sliding_frame is not None

    assert _shape(sliding_frame) == _shape(classic_frame)
    assert sliding_frame.device_list_delta_json == classic_frame.device_list_delta_json
    assert (
        sliding_frame.one_time_key_counts_json == classic_frame.one_time_key_counts_json
    )
    assert (
        sliding_frame.unused_fallback_key_types_json
        == classic_frame.unused_fallback_key_types_json
    )
    sliding_room = sliding_frame.room_segments[0]
    classic_room = classic_frame.room_segments[0]
    assert (
        sliding_room.section,
        sliding_room.timeline_limited,
        sliding_room.timeline_prev_batch,
        sliding_room.initial,
        sliding_room.expanded_timeline,
        sliding_room.live_event_count,
    ) == (
        classic_room.section,
        classic_room.timeline_limited,
        classic_room.timeline_prev_batch,
        classic_room.initial,
        classic_room.expanded_timeline,
        classic_room.live_event_count,
    )
    assert sliding_room.membership_observation == classic_room.membership_observation
