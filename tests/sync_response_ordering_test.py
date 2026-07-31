from nio import (
    DeviceList,
    DeviceOneTimeKeyCount,
    RoomInfo,
    Rooms,
    SlidingSyncResponse,
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

ROOM_A = "!a:example.org"
ROOM_B = "!b:example.org"


def room_info() -> RoomInfo:
    return RoomInfo(Timeline([], False, None), [], [], [])


def test_account_data_kind_falls_back_to_the_parsed_class() -> None:
    event = UnknownAccountDataEvent.from_dict(
        {"type": "org.example.first", "content": {}}
    )

    assert account_data_kind(event) == ("UnknownAccountDataEvent", None)


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
