from nio import (
    DeviceList,
    DeviceOneTimeKeyCount,
    RoomInfo,
    Rooms,
    SlidingSyncRoom,
    SyncResponse,
    Timeline,
    UnknownAccountDataEvent,
)
from nio.client.sync_reset_fence import (
    SyncResetFence,
    issue_sync_request,
    mark_room_reset,
)
from nio.client.sync_response_ordering import (
    account_data_kind,
    ordered_response_view,
    response_room_ids,
)
from nio.responses import SlidingSyncResponse

ROOM_A = "!a:example.org"
ROOM_B = "!b:example.org"
ROOM_C = "!c:example.org"


def room_info() -> RoomInfo:
    return RoomInfo(Timeline([], False, None), [], [], [])


def test_account_data_kind_keeps_unknown_wire_types_distinct() -> None:
    first = UnknownAccountDataEvent.from_dict(
        {"type": "org.example.first", "content": {}}
    )
    second = UnknownAccountDataEvent.from_dict(
        {"type": "org.example.second", "content": {}}
    )

    assert account_data_kind(first) == (
        "UnknownAccountDataEvent",
        "org.example.first",
    )
    assert account_data_kind(second) == (
        "UnknownAccountDataEvent",
        "org.example.second",
    )


def test_response_room_ids_include_room_account_data_outside_the_window() -> None:
    classic = SyncResponse(
        "next",
        Rooms({}, {ROOM_A: room_info()}, {ROOM_B: room_info()}),
        DeviceOneTimeKeyCount(None, None),
        DeviceList([], []),
        [],
        [],
    )
    sliding = SlidingSyncResponse(
        "pos",
        rooms={ROOM_A: SlidingSyncRoom()},
        room_account_data={ROOM_B: []},
    )

    assert response_room_ids(classic) == frozenset({ROOM_A, ROOM_B})
    assert response_room_ids(sliding) == frozenset({ROOM_A, ROOM_B})


def test_ordered_response_view_rejects_room_reset_after_request() -> None:
    state = SyncResetFence()
    request_id = issue_sync_request(state, "classic")
    mark_room_reset(state, ROOM_A)
    response = SyncResponse(
        "next",
        Rooms({}, {ROOM_A: room_info(), ROOM_B: room_info()}, {}),
        DeviceOneTimeKeyCount(None, None),
        DeviceList([], []),
        [],
        [],
    )

    ordered = ordered_response_view(state, response, request_id)

    assert ordered.current_room_ids == frozenset({ROOM_B})
    assert set(ordered.response.rooms.join) == {ROOM_B}


def test_ordered_response_view_does_not_duplicate_stream_ordering_for_rooms() -> None:
    state = SyncResetFence()
    older = issue_sync_request(state, "classic")
    newer = issue_sync_request(state, "classic")
    rooms = Rooms(
        {ROOM_B: room_info()},
        {ROOM_A: room_info()},
        {ROOM_C: room_info()},
    )
    newer_response = SyncResponse(
        "newer",
        rooms,
        DeviceOneTimeKeyCount(None, None),
        DeviceList([], []),
        [],
        [],
    )
    older_response = SyncResponse(
        "older",
        rooms,
        DeviceOneTimeKeyCount(None, None),
        DeviceList([], []),
        [],
        [],
    )

    ordered_response_view(state, newer_response, newer)
    ordered = ordered_response_view(state, older_response, older)

    assert ordered.current_room_ids == frozenset({ROOM_A, ROOM_B, ROOM_C})
    assert set(ordered.response.rooms.join) == {ROOM_A}
    assert set(ordered.response.rooms.invite) == {ROOM_B}
    assert set(ordered.response.rooms.leave) == {ROOM_C}


def test_ordered_response_view_does_not_duplicate_account_data_ordering() -> None:
    state = SyncResetFence()
    older = issue_sync_request(state, "classic")
    newer = issue_sync_request(state, "classic")
    newer_event = UnknownAccountDataEvent.from_dict(
        {"type": "org.example.settings", "content": {"value": "newer"}}
    )
    older_event = UnknownAccountDataEvent.from_dict(
        {"type": "org.example.settings", "content": {"value": "older"}}
    )
    newer_response = SyncResponse(
        "newer",
        Rooms(
            {}, {ROOM_A: RoomInfo(Timeline([], False, None), [], [], [newer_event])}, {}
        ),
        DeviceOneTimeKeyCount(None, None),
        DeviceList([], []),
        [],
        [],
        [newer_event],
    )
    older_response = SyncResponse(
        "older",
        Rooms(
            {}, {ROOM_A: RoomInfo(Timeline([], False, None), [], [], [older_event])}, {}
        ),
        DeviceOneTimeKeyCount(None, None),
        DeviceList([], []),
        [],
        [],
        [older_event],
    )

    ordered_response_view(state, newer_response, newer)
    ordered = ordered_response_view(state, older_response, older)

    assert ordered.response.account_data_events == [older_event]
    assert ordered.response.rooms.join[ROOM_A].account_data == [older_event]
