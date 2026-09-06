"""Sliding Sync requests and protocol models, without client recovery policy."""

import copy
import json
from urllib.parse import parse_qs, urlsplit

import pytest

import nio
from nio import responses
from nio.api import Api


def test_request_uses_deployed_query_and_body() -> None:
    method, path, body = Api.sliding_sync(
        "secret & token",
        conn_id="agent-1",
        pos="position/1+2",
        timeout=0,
        set_presence="unavailable",
        lists={"main": {"ranges": [[0, 99]], "required_state": [["m.room.name", ""]]}},
        room_subscriptions={"!room:example.org": {"timeline_limit": 20}},
        extensions={"to_device": {"enabled": True, "since": "device-1"}},
    )

    assert method == "POST"
    assert (
        urlsplit(path).path
        == "/_matrix/client/unstable/org.matrix.simplified_msc3575/sync"
    )
    assert parse_qs(urlsplit(path).query) == {
        "access_token": ["secret & token"],
        "pos": ["position/1+2"],
        "timeout": ["0"],
        "set_presence": ["unavailable"],
    }
    assert json.loads(body) == {
        "conn_id": "agent-1",
        "lists": {
            "main": {"ranges": [[0, 99]], "required_state": [["m.room.name", ""]]}
        },
        "room_subscriptions": {"!room:example.org": {"timeline_limit": 20}},
        "extensions": {"to_device": {"enabled": True, "since": "device-1"}},
    }


def test_request_preserves_empty_updates_and_omits_unspecified_fields() -> None:
    assert Api.sliding_sync("token", unstable=False) == (
        "POST",
        "/_matrix/client/v4/sync?access_token=token",
        "{}",
    )
    _, path, body = Api.sliding_sync(
        "token", conn_id="", pos="", lists={}, room_subscriptions={}, extensions={}
    )
    assert parse_qs(urlsplit(path).query, keep_blank_values=True) == {
        "access_token": ["token"],
        "pos": [""],
    }
    assert json.loads(body) == {
        "conn_id": "",
        "lists": {},
        "room_subscriptions": {},
        "extensions": {},
    }


def test_minimal_response_and_public_models() -> None:
    response = responses.SlidingSyncResponse.from_dict({"pos": "connection-1"})
    assert isinstance(response, nio.SlidingSyncResponse)
    assert response.pos == "connection-1"
    assert response.rooms == {}
    assert response.lists == {}
    assert response.extensions == {}
    assert response.to_device_events == []
    assert response.to_device_next_batch is None
    assert response.device_key_count.curve25519 is None
    assert response.device_key_count.signed_curve25519 is None
    assert response.device_list.changed == []
    assert response.device_list.left == []
    assert response.account_data_events == []
    assert response.room_account_data == {}


def test_room_protocol_fields_state_stubs_and_parsed_events() -> None:
    response = responses.SlidingSyncResponse.from_dict(
        {
            "pos": "connection-2",
            "lists": {"main": {"count": 123}},
            "rooms": {
                "!room:example.org": {
                    "bump_stamp": 10,
                    "membership": "join",
                    "lists": ["main"],
                    "name": "Example",
                    "avatar": None,
                    "heroes": [
                        {
                            "user_id": "@alice:example.org",
                            "displayname": "Alice",
                            "avatar_url": "mxc://example.org/alice",
                        }
                    ],
                    "is_dm": True,
                    "initial": True,
                    "limited": True,
                    "required_state": [
                        {"type": "m.room.topic", "state_key": ""},
                        {
                            "type": "m.room.name",
                            "state_key": "",
                            "event_id": "$name",
                            "sender": "@alice:example.org",
                            "origin_server_ts": 1,
                            "content": {"name": "Example"},
                        },
                    ],
                    "timeline": [
                        {
                            "type": "m.room.message",
                            "event_id": "$message",
                            "sender": "@alice:example.org",
                            "origin_server_ts": 2,
                            "content": {"msgtype": "m.text", "body": "hello"},
                        }
                    ],
                    "prev_batch": "history-1",
                    "num_live": 1,
                    "joined_count": 2,
                    "invited_count": 0,
                    "notification_count": 3,
                    "highlight_count": 1,
                }
            },
        }
    )
    assert isinstance(response, nio.SlidingSyncResponse)
    assert isinstance(response.lists["main"], nio.SlidingSyncList)
    assert response.lists["main"].count == 123
    room = response.rooms["!room:example.org"]
    assert isinstance(room, nio.SlidingSyncRoom)
    assert room.bump_stamp == 10
    assert room.membership == "join"
    assert room.lists == ["main"]
    assert room.name == "Example"
    assert room.avatar is None
    assert room.heroes == [
        nio.SlidingSyncHero("@alice:example.org", "Alice", "mxc://example.org/alice")
    ]
    assert room.is_dm and room.initial and room.limited
    assert room.required_state[0] == nio.SlidingSyncStateStub("m.room.topic", "")
    assert isinstance(room.required_state[1], nio.RoomNameEvent)
    assert room.required_state[1].name == "Example"
    assert isinstance(room.timeline[0], nio.RoomMessageText)
    assert room.timeline[0].body == "hello"
    assert room.prev_batch == "history-1"
    assert room.num_live == 1
    assert (room.joined_count, room.invited_count) == (2, 0)
    assert (room.notification_count, room.highlight_count) == (3, 1)


@pytest.mark.parametrize(
    ("fields", "expected"), [({}, None), ({"heroes": []}, []), ({"heroes": None}, None)]
)
def test_heroes_omission_differs_from_empty(fields, expected) -> None:
    response = responses.SlidingSyncResponse.from_dict(
        {"pos": "p", "rooms": {"!r:x": fields}}
    )
    assert response.rooms["!r:x"].heroes == expected


@pytest.mark.parametrize("num_live", [None, -1, 0, 12])
def test_num_live_remains_raw_for_client_classification(num_live) -> None:
    response = responses.SlidingSyncResponse.from_dict(
        {"pos": "p", "rooms": {"!r:x": {"num_live": num_live, "timeline": []}}}
    )
    assert response.rooms["!r:x"].num_live == num_live


@pytest.mark.parametrize(
    ("fields", "expected"),
    [
        ({}, False),
        ({"expanded_timeline": True}, True),
        ({"unstable_expanded_timeline": True}, True),
        ({"unstable_expanded_timeline": False, "expanded_timeline": True}, False),
    ],
)
def test_expanded_timeline_spellings(fields, expected) -> None:
    response = responses.SlidingSyncResponse.from_dict(
        {"pos": "p", "rooms": {"!r:x": fields}}
    )
    assert response.rooms["!r:x"].expanded_timeline is expected


@pytest.mark.parametrize("field", ["stripped_state", "invite_state"])
def test_invite_state_spellings(field) -> None:
    response = responses.SlidingSyncResponse.from_dict(
        {
            "pos": "p",
            "rooms": {
                "!r:x": {
                    field: [
                        {
                            "type": "m.room.member",
                            "state_key": "@bob:example.org",
                            "sender": "@alice:example.org",
                            "content": {"membership": "invite"},
                        },
                    ]
                }
            },
        }
    )
    event = response.rooms["!r:x"].stripped_state[0]
    assert isinstance(event, nio.InviteMemberEvent)
    assert event.membership == "invite"


def test_extensions_parse_without_changing_wire_data() -> None:
    payload = {
        "pos": "connection-9",
        "extensions": {
            "to_device": {
                "next_batch": "delivery-4",
                "events": [
                    {
                        "sender": "@alice:example.org",
                        "type": "org.example.custom",
                        "content": {"body": "ping"},
                    },
                    {
                        "sender": "@alice:example.org",
                        "type": "org.example.custom",
                        "content": {},
                    },
                ],
            },
            "e2ee": {
                "device_one_time_keys_count": {"curve25519": 1, "signed_curve25519": 0},
                "device_lists": {
                    "changed": ["@alice:example.org"],
                    "left": ["@bob:example.org"],
                },
            },
            "account_data": {
                "global": [
                    {"type": "m.direct", "content": {"@alice:example.org": ["!r:x"]}}
                ],
                "rooms": {
                    "!outside:x": [
                        {"type": "m.fully_read", "content": {"event_id": "$read"}}
                    ]
                },
            },
            "org.example.extension": {"nested": [1, 2]},
        },
    }
    original = copy.deepcopy(payload)
    response = responses.SlidingSyncResponse.from_dict(payload)
    assert isinstance(response, nio.SlidingSyncResponse)
    assert response.to_device_next_batch == "delivery-4"
    assert len(response.to_device_events) == 1
    assert response.to_device_events[0].source["content"] == {"body": "ping"}
    assert response.device_key_count.curve25519 == 1
    assert response.device_key_count.signed_curve25519 == 0
    assert response.device_list.changed == ["@alice:example.org"]
    assert response.device_list.left == ["@bob:example.org"]
    assert response.account_data_events[0].content == {"@alice:example.org": ["!r:x"]}
    assert response.room_account_data["!outside:x"][0].event_id == "$read"
    assert response.rooms == {}
    assert payload == original
    assert response.extensions == original["extensions"]
    response.to_device_events[0].source["content"]["body"] = "changed"
    response.device_list.changed.clear()
    assert response.extensions == original["extensions"]


@pytest.mark.parametrize(
    "payload",
    [
        {},
        None,
        [],
        {"pos": 1},
        {"pos": "p", "lists": []},
        {"pos": "p", "lists": {"main": {}}},
        {"pos": "p", "lists": {"main": {"count": True}}},
        {"pos": "p", "lists": {"main": {"count": "2"}}},
        {"pos": "p", "rooms": []},
        {"pos": "p", "rooms": {"!r:x": None}},
    ],
)
def test_malformed_response_shapes_return_errors(payload) -> None:
    assert isinstance(
        responses.SlidingSyncResponse.from_dict(payload), nio.SlidingSyncError
    )


@pytest.mark.parametrize(
    "room",
    [
        {"required_state": {}},
        {"required_state": [42]},
        {"required_state": [{"type": "m.room.name"}]},
        {"required_state": [{"type": "m.room.name", "state_key": 1}]},
        {"timeline": None},
        {"timeline": [42]},
        {"heroes": "Alice"},
        {"heroes": [{}]},
        {"heroes": [{"user_id": 1}]},
        {"num_live": True},
        {"num_live": "1"},
        {"num_live": 1.5},
        {"initial": "true"},
        {"expanded_timeline": []},
        {"unstable_expanded_timeline": 1},
        {"stripped_state": {}},
        {"invite_state": [42]},
        {"lists": [1]},
        {"joined_count": "2"},
        {"membership": []},
        {"name": {}},
    ],
)
def test_malformed_room_shapes_return_errors(room) -> None:
    response = responses.SlidingSyncResponse.from_dict(
        {"pos": "p", "rooms": {"!r:x": room}}
    )
    assert isinstance(response, nio.SlidingSyncError)


@pytest.mark.parametrize(
    "extensions",
    [
        [],
        None,
        {"to_device": "junk"},
        {"to_device": {"events": "junk"}},
        {"to_device": {"events": [42]}},
        {"to_device": {"next_batch": 4}},
        {"e2ee": []},
        {"e2ee": {"device_one_time_keys_count": None}},
        {"e2ee": {"device_one_time_keys_count": {"signed_curve25519": "50"}}},
        {"e2ee": {"device_lists": {"changed": [42]}}},
        {"account_data": {"global": {}}},
        {"account_data": {"global": [42]}},
        {"account_data": {"rooms": {"!r:x": "junk"}}},
        {"account_data": {"rooms": {"!r:x": [42]}}},
    ],
)
def test_malformed_extension_shapes_return_errors(extensions) -> None:
    response = responses.SlidingSyncResponse.from_dict(
        {"pos": "p", "extensions": extensions}
    )
    assert isinstance(response, nio.SlidingSyncError)


def test_server_error_preserves_retry_and_logout_fields() -> None:
    response = responses.SlidingSyncResponse.from_dict(
        {
            "errcode": "M_UNKNOWN_POS",
            "error": "Expired connection",
            "retry_after_ms": 50,
            "soft_logout": True,
        }
    )
    assert isinstance(response, nio.SlidingSyncError)
    assert response.status_code == "M_UNKNOWN_POS"
    assert response.message == "Expired connection"
    assert response.retry_after_ms == 50
    assert response.soft_logout


@pytest.mark.parametrize(
    "payload",
    [
        {
            "pos": "p",
            "rooms": {
                "!r:x": {
                    "timeline": [
                        {
                            "event_id": "$broken",
                            "sender": "@alice:example.org",
                            "origin_server_ts": 1,
                            "type": "m.room.message",
                        }
                    ]
                }
            },
        },
        {
            "pos": "p",
            "rooms": {
                "!r:x": {
                    "required_state": [
                        {
                            "event_id": "$broken",
                            "sender": "@alice:example.org",
                            "origin_server_ts": None,
                            "type": "m.room.name",
                            "state_key": "",
                            "content": {"name": "Example"},
                        }
                    ]
                }
            },
        },
        {
            "pos": "p",
            "extensions": {
                "account_data": {
                    "global": [
                        {
                            "origin_server_ts": None,
                            "type": "m.direct",
                        }
                    ]
                }
            },
        },
        {
            "pos": "p",
            "extensions": {
                "to_device": {
                    "events": [
                        {
                            "origin_server_ts": None,
                            "type": "org.example.custom",
                        }
                    ]
                }
            },
        },
    ],
)
def test_nested_event_parse_failures_return_errors(payload) -> None:
    response = responses.SlidingSyncResponse.from_dict(payload)
    assert isinstance(response, nio.SlidingSyncError)
