import json
from uuid import UUID

from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.model import RecordKind, TransportKind
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
