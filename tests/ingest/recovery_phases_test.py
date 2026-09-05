"""Phase boundaries preserve the frozen sync while bounding recovered pages."""

from dataclasses import replace
from uuid import UUID

import pytest

from nio.ingest._json import canonical_json, load_json
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig
from nio.ingest.model import TransportKind
from nio.ingest.ports import NetworkResult
from nio.ingest.recovery import (
    apply_classic_recovery,
    capture_classic_recovery,
    recovery_progress,
    start_classic_recovery,
)
from nio.ingest.source import ClassicCursor, RoomSection, canonical_classic_cursor
from nio.ingest.state import SourceState

OWN = "@me:example.org"
ROOMS = ("!one:example.org", "!two:example.org")


def _event(identity, **extra):
    return {
        "event_id": identity,
        "type": "m.room.message",
        "sender": OWN,
        "content": {"msgtype": "m.text", "body": identity},
        **extra,
    }


def _root():
    source = ClassicSource(UUID(int=11), ClassicSourceConfig(30000, b"{}"), OWN)
    request = source.plan_request(
        SourceState(
            0,
            TransportKind.CLASSIC,
            canonical_classic_cursor(ClassicCursor("s1")),
            0,
            True,
        ),
        0,
    )
    body = canonical_json(
        {
            "next_batch": "s2",
            "to_device": {"events": [{"type": "m.test", "content": {}}]},
            "device_lists": {"changed": [OWN], "left": []},
            "device_one_time_keys_count": {"signed_curve25519": 4},
            "account_data": {"events": [{"type": "m.test", "content": {}}]},
            "rooms": {
                "join": {
                    room: {
                        "state": {
                            "events": [
                                {
                                    "type": "m.room.name",
                                    "state_key": "",
                                    "content": {"name": "final"},
                                }
                            ]
                        },
                        "timeline": {
                            "limited": True,
                            "prev_batch": "target" + str(index),
                            "events": [_event("$fresh" + str(index))],
                        },
                    }
                    for index, room in enumerate(ROOMS)
                }
            },
        }
    )
    result = NetworkResult(
        request.stream_id,
        request.transport,
        request.source_epoch,
        request.request_id,
        200,
        body,
        None,
        None,
    )
    frame = source.normalize(request, result).frame
    assert frame is not None
    return request, frame, body


def _at(frame, cursor):
    return replace(frame, request_cursor_json=cursor)


def test_phases_defer_state_and_fresh_tails_until_all_rooms_complete():
    _, root, _ = _root()
    initial = start_classic_recovery(root, ROOMS)
    prologue = apply_classic_recovery(_at(root, initial), None, OWN)
    assert prologue.to_device_json == root.to_device_json
    assert prologue.room_segments == ()
    assert prologue.global_account_data_json == ()
    first = apply_classic_recovery(
        _at(root, prologue.candidate_cursor_json),
        canonical_json(
            {
                "start": "s1",
                "end": "middle",
                "chunk": [_event("$old"), _event("$fresh0")],
            }
        ),
        OWN,
    )
    assert first.to_device_json == ()
    assert first.device_list_delta_json == b'{"changed":[],"left":[]}'
    segment = first.room_segments[0]
    assert segment.section is RoomSection.UNCHANGED
    assert segment.state_json == ()
    assert [load_json(item, "event")["event_id"] for item in segment.timeline_json] == [
        "$old"
    ]
    assert segment.recovered_event_count == 1
    second = apply_classic_recovery(
        _at(root, first.candidate_cursor_json),
        canonical_json(
            {
                "start": "middle",
                "end": "target0",
                "chunk": [_event("$old"), _event("$last")],
            }
        ),
        OWN,
    )
    assert [
        load_json(item, "event")["event_id"]
        for item in second.room_segments[0].timeline_json
    ] == ["$last"]
    assert recovery_progress(second.candidate_cursor_json)["room_index"] == 1
    empty = apply_classic_recovery(
        _at(root, second.candidate_cursor_json),
        canonical_json({"start": "s1", "end": "target1", "chunk": []}),
        OWN,
    )
    assert empty.room_segments[0].timeline_json == ()
    assert recovery_progress(empty.candidate_cursor_json)["phase"] == "tail"
    tail = apply_classic_recovery(_at(root, empty.candidate_cursor_json), None, OWN)
    assert tail.candidate_cursor_json == root.candidate_cursor_json
    assert tail.global_account_data_json == root.global_account_data_json
    assert tail.to_device_json == ()
    assert all(not segment.timeline_limited for segment in tail.room_segments)
    assert tail.room_segments[0].state_json == root.room_segments[0].state_json


@pytest.mark.asyncio
async def test_oversized_page_retries_same_position_with_smaller_limit():
    request, root, body = _root()
    prologue = apply_classic_recovery(
        _at(root, start_classic_recovery(root, ROOMS)), None, OWN
    )
    frame = _at(root, prologue.candidate_cursor_json)
    request = replace(request, request_cursor_json=frame.request_cursor_json)
    queries = []

    async def fetch(page_request):
        query = dict(page_request.query)
        queries.append(query)
        count = int(query["limit"])
        page = {
            "start": query["from"],
            "end": "target0",
            "chunk": [
                _event("$" + str(index), content={"body": "x" * 30000})
                for index in range(count)
            ],
        }
        return NetworkResult(
            page_request.stream_id,
            page_request.transport,
            page_request.source_epoch,
            page_request.request_id,
            200,
            canonical_json(page),
            None,
            None,
        )

    evidence = await capture_classic_recovery(request, frame, body, ROOMS, fetch)
    assert [query["limit"] for query in queries] == ["100", "50"]
    assert {query["from"] for query in queries} == {"s1"}
    assert len(load_json(evidence, "page")["chunk"]) == 50


def test_recovery_pins_reject_another_response_and_pagination_cycles():
    _, root, _ = _root()
    cursor = start_classic_recovery(root, ROOMS)
    with pytest.raises(ValueError, match="source"):
        apply_classic_recovery(
            replace(_at(root, cursor), source_sha256=b"x" * 32), None, OWN
        )
    page_frame = apply_classic_recovery(_at(root, cursor), None, OWN)
    with pytest.raises(ValueError, match="advance"):
        apply_classic_recovery(
            _at(root, page_frame.candidate_cursor_json),
            canonical_json({"start": "s1", "end": "s1", "chunk": []}),
            OWN,
        )


def test_recovered_membership_pages_do_not_fabricate_final_room_membership():
    _, root, _ = _root()
    frame = apply_classic_recovery(
        _at(root, start_classic_recovery(root, (ROOMS[0],))), None, OWN
    )
    for membership, token, end in (
        ("leave", "s1", "middle"),
        ("join", "middle", "target0"),
    ):
        event = _event(
            "$" + membership,
            type="m.room.member",
            state_key=OWN,
            content={"membership": membership},
        )
        frame = apply_classic_recovery(
            _at(root, frame.candidate_cursor_json),
            canonical_json({"start": token, "end": end, "chunk": [event]}),
            OWN,
        )
        segment = frame.room_segments[0]
        assert segment.section is RoomSection.UNCHANGED
        assert segment.membership_observation.room_membership is None
        assert segment.membership_observation.event_membership == membership
        assert segment.membership_observation.event_id == "$" + membership
        assert segment.recovered_event_count == 1
        assert segment.live_event_count == 0


@pytest.mark.parametrize(
    "changes",
    [
        {"phase": "unknown"},
        {"room_index": True},
        {"page_count": 1001},
        {"seen_tokens": ["s1", "s1"]},
        {"previous_event_ids": ["$duplicate", "$duplicate"]},
        {"source_sha256": "z" * 64},
        {"rooms": [ROOMS[0], ROOMS[0]]},
    ],
)
def test_malformed_recovery_continuations_are_rejected(changes):
    _, root, _ = _root()
    cursor = load_json(start_classic_recovery(root, ROOMS), "cursor")
    cursor["recovery"].update(changes)
    with pytest.raises(ValueError):
        recovery_progress(canonical_json(cursor))


@pytest.mark.asyncio
async def test_non_page_phases_need_no_network_and_empty_pages_advance():
    request, root, body = _root()
    cursor = start_classic_recovery(root, (ROOMS[0],))

    async def no_fetch(_):
        pytest.fail("prologue and tail must not fetch")

    prologue_source = _at(root, cursor)
    assert (
        await capture_classic_recovery(
            replace(request, request_cursor_json=cursor),
            prologue_source,
            body,
            (ROOMS[0],),
            no_fetch,
        )
        is None
    )
    prologue = apply_classic_recovery(prologue_source, None, OWN)
    empty = apply_classic_recovery(
        _at(root, prologue.candidate_cursor_json),
        canonical_json({"start": "s1", "end": "middle", "chunk": []}),
        OWN,
    )
    assert recovery_progress(empty.candidate_cursor_json)["phase"] == "page"
    last = apply_classic_recovery(
        _at(root, empty.candidate_cursor_json),
        canonical_json({"start": "middle", "chunk": []}),
        OWN,
    )
    assert recovery_progress(last.candidate_cursor_json)["phase"] == "tail"
    assert (
        await capture_classic_recovery(
            replace(request, request_cursor_json=last.candidate_cursor_json),
            _at(root, last.candidate_cursor_json),
            body,
            (ROOMS[0],),
            no_fetch,
        )
        is None
    )
