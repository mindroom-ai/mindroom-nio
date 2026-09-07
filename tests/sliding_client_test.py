"""Sliding windows share the ordinary event interpreter and state ownership."""

import asyncio
import json
import re

import nio
import pytest
from nio.event_provenance import TimelineEventProvenance

ROOM = "!room:example.org"
USER = "@alice:example.org"
OTHER = "@bob:example.org"


def event(kind, content, event_id="$state", state_key=""):
    value = {
        "type": kind,
        "content": content,
        "event_id": event_id,
        "sender": USER,
        "origin_server_ts": 1,
    }
    if state_key is not None:
        value["state_key"] = state_key
    return value


def member(user=USER, membership="join", event_id="$join"):
    return event("m.room.member", {"membership": membership}, event_id, user)


def message(event_id="$message"):
    return event(
        "m.room.message", {"msgtype": "m.text", "body": event_id}, event_id, None
    )


def response(room=None, pos="p1", extensions=None):
    assert hasattr(
        nio, "SlidingSyncResponse"
    ), "Sliding response model must be restored"
    parsed = nio.SlidingSyncResponse.from_dict(
        {
            "pos": pos,
            "rooms": {} if room is None else {ROOM: room},
            "extensions": extensions or {},
        }
    )
    assert isinstance(parsed, nio.SlidingSyncResponse)
    return parsed


def client():
    value = nio.Client(USER, config=nio.ClientConfig(encryption_enabled=False))
    value.user_id = USER
    return value


def test_initial_snapshot_rebuild_preserves_external_data():
    value = client()
    value.receive_response(
        response(
            {
                "initial": True,
                "required_state": [
                    member(),
                    member(OTHER),
                    event("m.room.topic", {"topic": "stale"}),
                ],
            }
        )
    )
    room = value.rooms[ROOM]
    room.tags = {"m.favourite": {"order": 0.5}}
    room.fully_read_marker = "$read"
    room.typing_users = [USER]
    room.members_synced = True
    value.receive_response(
        response({"initial": True, "required_state": [member()]}, "p2")
    )
    assert value.rooms[ROOM].topic is None
    assert OTHER not in value.rooms[ROOM].users
    assert not value.rooms[ROOM].members_synced
    assert value.rooms[ROOM].tags == {"m.favourite": {"order": 0.5}}
    assert value.rooms[ROOM].fully_read_marker == "$read"
    assert value.rooms[ROOM].typing_users == [USER]


def test_initial_history_does_not_rewind_current_state():
    value = client()
    observed = []
    value.add_event_callback(
        lambda room, item: observed.append(
            (item.event_id, room.topic, sorted(room.users))
        ),
        nio.Event,
    )
    value.receive_response(
        response(
            {
                "initial": True,
                "required_state": [member(), event("m.room.topic", {"topic": "now"})],
                "timeline": [
                    member(OTHER, event_id="$old-join"),
                    event("m.room.topic", {"topic": "old"}, "$old-topic"),
                    message(),
                ],
            }
        )
    )
    assert observed == [
        ("$old-join", "now", [USER]),
        ("$old-topic", "now", [USER]),
        ("$message", "now", [USER]),
    ]
    assert value.rooms[ROOM].topic == "now"


def test_known_delta_applies_membership_in_timeline_order_before_snapshot():
    value = client()
    value.receive_response(response({"initial": True, "required_state": [member()]}))
    observed = []
    value.add_event_callback(
        lambda room, item: observed.append((item.event_id, OTHER in room.users)),
        nio.RoomMessageText,
    )
    value.receive_response(
        response(
            {
                "required_state": [member(OTHER)],
                "timeline": [message("$before"), member(OTHER), message("$after")],
            },
            "p2",
        )
    )
    assert observed == [("$before", False), ("$after", True)]


def test_state_stubs_clear_membership_and_represented_state():
    value = client()
    value.receive_response(
        response(
            {
                "initial": True,
                "required_state": [
                    member(),
                    member(OTHER),
                    event("m.room.name", {"name": "Named"}),
                    event("m.room.power_levels", {"users": {USER: 75}}),
                ],
            }
        )
    )
    value.receive_response(
        response(
            {
                "required_state": [
                    {"type": "m.room.member", "state_key": OTHER},
                    {"type": "m.room.name", "state_key": ""},
                    {"type": "m.room.power_levels", "state_key": ""},
                ]
            },
            "p2",
        )
    )
    assert OTHER not in value.rooms[ROOM].users
    assert value.rooms[ROOM].name is None
    assert value.rooms[ROOM].users[USER].power_level == 0


def test_summary_omission_empty_and_departed_hero():
    value = client()
    value.receive_response(
        response(
            {
                "initial": True,
                "required_state": [member()],
                "heroes": [{"user_id": OTHER, "displayname": "Bob"}],
                "joined_count": 2,
                "name": "Calculated",
                "avatar": "mxc://calculated/avatar",
            }
        )
    )
    room = value.rooms[ROOM]
    assert room.name is None and room.room_avatar_url is None
    assert room.summary.heroes == [OTHER]
    value.receive_response(
        response(
            {
                "required_state": [member(OTHER, "leave")],
                "heroes": [{"user_id": OTHER, "displayname": "Bob"}],
                "joined_count": 2,
            },
            "p2",
        )
    )
    assert OTHER not in room.users
    value.receive_response(response({}, "p3"))
    assert room.summary.heroes == [OTHER]
    value.receive_response(response({"heroes": []}, "p4"))
    assert room.summary.heroes == []


def test_account_data_before_discovery_coalesces_original_type():
    value = client()
    observed = []
    value.add_room_account_data_callback(
        lambda room, item: observed.append(item.source["type"]), nio.AccountDataEvent
    )
    value.receive_response(
        response(
            extensions={
                "account_data": {
                    "rooms": {
                        ROOM: [
                            {"type": "m.tag", "content": {"tags": {"old": {}}}},
                            {"type": "m.fully_read", "content": {"event_id": "$read"}},
                            {"type": "org.example.one", "content": {}},
                            {"type": "org.example.two", "content": {}},
                        ]
                    }
                }
            }
        )
    )
    value.receive_response(
        response(
            extensions={
                "account_data": {
                    "rooms": {
                        ROOM: [{"type": "m.tag", "content": {"tags": {"new": {}}}}]
                    }
                }
            },
            pos="p2",
        )
    )
    value.receive_response(
        response({"initial": True, "required_state": [member()]}, "p3")
    )
    assert value.rooms[ROOM].tags == {"new": {}}
    assert value.rooms[ROOM].fully_read_marker == "$read"
    assert observed == ["m.tag", "m.fully_read", "org.example.one", "org.example.two"]


def test_shared_iterator_classifies_live_tail_and_suppresses_overlap():
    value = client()
    parsed = response(
        {
            "initial": True,
            "num_live": 1,
            "timeline": [message("$old"), message("$live")],
        }
    )
    from nio.client.sliding_sync import iter_sliding_sync

    items = [item for item in iter_sliding_sync(value, parsed) if item.route == "event"]
    assert [item.provenance for item in items] == [
        TimelineEventProvenance.HISTORY,
        TimelineEventProvenance.LIVE,
    ]
    parsed = response(
        {"initial": True, "timeline": [message("$old"), message("$new")]}, "p2"
    )
    items = [
        item
        for item in iter_sliding_sync(
            value, parsed, recovered_rooms={ROOM}, suppress_ids={ROOM: {"$old"}}
        )
        if item.route == "event"
    ]
    assert [(item.event.event_id, item.provenance) for item in items] == [
        ("$new", TimelineEventProvenance.RECOVERED)
    ]


@pytest.mark.asyncio
async def test_async_sliding_extensions_callbacks_and_cursor(aioresponse):
    value = nio.AsyncClient(
        "https://example.org",
        USER,
        config=nio.AsyncClientConfig(encryption_enabled=False),
    )
    value.access_token = "token"
    value.user_id = USER
    seen = []

    async def observe(room, item):
        seen.append(item.event_id)

    value.add_event_callback(observe, nio.RoomMessageText)
    aioresponse.post(
        re.compile(r"https://example.org/.*"),
        payload={
            "pos": "p1",
            "rooms": {ROOM: {"initial": True, "timeline": [message()]}},
            "extensions": {"to_device": {"next_batch": "device1", "events": []}},
        },
    )
    assert hasattr(value, "sliding_sync"), "ordinary Sliding sync must be restored"
    result = await value.sliding_sync(
        conn_id="desktop", extensions={"to_device": {"enabled": True}}
    )
    assert isinstance(result, nio.SlidingSyncResponse)
    assert seen == ["$message"]
    assert value.next_batch == ""
    with pytest.raises(nio.LocalProtocolError, match="transport|Classic|cursor"):
        await value.sync()
    await value.close()


@pytest.mark.asyncio
async def test_sliding_forever_restarts_unknown_position_and_keeps_device_cursor(
    aioresponse,
):
    value = nio.AsyncClient(
        "https://example.org",
        USER,
        config=nio.AsyncClientConfig(encryption_enabled=False),
    )
    value.access_token = "token"
    value.user_id = USER
    calls = []
    from aioresponses import CallbackResult

    async def server(url, **kwargs):
        calls.append((dict(url.query), json.loads(kwargs["data"])))
        if len(calls) == 1:
            return CallbackResult(
                payload={
                    "pos": "p1",
                    "extensions": {
                        "to_device": {"next_batch": "device1", "events": []}
                    },
                }
            )
        if len(calls) == 2:
            return CallbackResult(
                status=400, payload={"errcode": "M_UNKNOWN_POS", "error": "expired"}
            )
        return CallbackResult(payload={"pos": "p2"})

    aioresponse.post(
        re.compile(r"https://example.org/.*"), callback=server, repeat=True
    )

    async def stop(result):
        if getattr(result, "pos", None) == "p2":
            value.stop_sync_forever()

    value.add_response_callback(stop)
    assert hasattr(
        value, "sliding_sync_forever"
    ), "Sliding forever loop must be restored"
    await asyncio.wait_for(
        value.sliding_sync_forever(
            conn_id="desktop", extensions={"to_device": {"enabled": True}}
        ),
        5,
    )
    assert "pos" not in calls[0][0]
    assert calls[1][0]["pos"] == "p1"
    assert "pos" not in calls[2][0]
    assert calls[2][1]["extensions"]["to_device"]["since"] == "device1"
    await value.close()


@pytest.mark.asyncio
async def test_sliding_cancel_releases_poll_and_rejects_concurrent_poll(aioresponse):
    value = nio.AsyncClient(
        "https://example.org",
        USER,
        config=nio.AsyncClientConfig(encryption_enabled=False),
    )
    value.access_token = "token"
    entered = asyncio.Event()
    from aioresponses import CallbackResult

    async def hold(url, **kwargs):
        if entered.is_set():
            return CallbackResult(payload={"pos": "after-cancel"})
        entered.set()
        await asyncio.Event().wait()

    aioresponse.post(re.compile(r"https://example.org/.*"), callback=hold)
    assert hasattr(value, "sliding_sync"), "ordinary Sliding sync must be restored"
    task = asyncio.create_task(value.sliding_sync())
    await asyncio.wait_for(entered.wait(), 1)
    with pytest.raises(nio.LocalProtocolError, match="in flight"):
        await value.sliding_sync()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    result = await value.sliding_sync()
    assert result.pos == "after-cancel"
    await value.close()


def test_http_sliding_builds_request_and_fences_classic_cursor():
    value = nio.HttpClient(
        "example.org", USER, config=nio.ClientConfig(encryption_enabled=False)
    )
    value.access_token = "token"
    value.connect()
    assert hasattr(value, "sliding_sync"), "HTTP Sliding sync must be restored"
    request_id, wire = value.sliding_sync(
        conn_id="desktop", pos="p1", extensions={"to_device": {"enabled": True}}
    )
    assert b"POST /_matrix/client/unstable/org.matrix.simplified_msc3575/sync?" in wire
    assert b"pos=p1" in wire
    assert value.requests_made[request_id].request_class is nio.SlidingSyncResponse
    with pytest.raises(nio.LocalProtocolError, match="transport|Classic|cursor"):
        value.sync()
    value.disconnect()


def test_ordinary_sliding_does_not_repeat_window_callbacks():
    value = client()
    seen = []
    value.add_event_callback(
        lambda room, item: seen.append(item.event_id), nio.RoomMessageText
    )
    value.receive_response(response({"initial": True, "timeline": [message("$old")]}))
    value.receive_response(
        response(
            {"initial": True, "timeline": [message("$old"), message("$new")]}, "p2"
        )
    )
    assert seen == ["$old", "$new"]


def test_extension_callback_order_places_account_data_after_room_events():
    value = client()
    seen = []
    value.add_to_device_callback(lambda item: seen.append("device"), nio.ToDeviceEvent)
    value.add_event_callback(
        lambda room, item: seen.append("timeline"), nio.RoomMessageText
    )
    value.add_ephemeral_callback(
        lambda room, item: seen.append("typing"), nio.TypingNoticeEvent
    )
    value.add_global_account_data_callback(
        lambda item: seen.append("global"), nio.AccountDataEvent
    )
    value.add_room_account_data_callback(
        lambda room, item: seen.append("room-data"), nio.AccountDataEvent
    )
    value.receive_response(
        response(
            {"initial": True, "timeline": [message()]},
            extensions={
                "to_device": {
                    "events": [
                        {
                            "type": "org.example.control",
                            "sender": OTHER,
                            "content": {"value": 1},
                        }
                    ]
                },
                "typing": {
                    "rooms": {
                        ROOM: {"type": "m.typing", "content": {"user_ids": [OTHER]}}
                    }
                },
                "account_data": {
                    "global": [{"type": "org.example.setting", "content": {}}],
                    "rooms": {ROOM: [{"type": "m.tag", "content": {"tags": {}}}]},
                },
            },
        )
    )
    assert seen == ["device", "timeline", "typing", "global", "room-data"]


def test_stub_observation_preserves_wire_source():
    value = client()
    parsed = response(
        {"required_state": [{"type": "m.room.member", "state_key": OTHER}]}
    )
    from nio.client.sliding_sync import iter_sliding_sync

    items = [
        item for item in iter_sliding_sync(value, parsed) if item.event is not None
    ]
    assert items[0].source == {"type": "m.room.member", "state_key": OTHER}


@pytest.mark.parametrize(
    "fields, count",
    [
        ({}, 3),
        ({"initial": True}, 0),
        ({"expanded_timeline": True}, 0),
        ({"unstable_expanded_timeline": True}, 0),
        ({"num_live": -4}, 0),
        ({"num_live": 8}, 3),
        ({"initial": True, "num_live": 1}, 1),
    ],
)
def test_live_tail_classification_clamps_and_accepts_expanded_aliases(fields, count):
    from nio.client.sliding_sync import live_event_count

    parsed = response(
        {**fields, "timeline": [message("$a"), message("$b"), message("$c")]}
    )
    assert live_event_count(parsed.rooms[ROOM]) == count


@pytest.mark.asyncio
async def test_encrypted_initial_history_decrypts_and_snapshot_rotates_session(
    async_client,
):
    value = async_client
    encryption = event(
        "m.room.encryption", {"algorithm": "m.megolm.v1.aes-sha2"}, "$encryption"
    )
    await value.receive_response(
        response(
            {
                "initial": True,
                "required_state": [member(value.user_id), member(OTHER), encryption],
            }
        )
    )
    value.olm.create_outbound_group_session(ROOM)
    value.olm.outbound_group_sessions[ROOM].shared = True
    encrypted = value.olm.group_encrypt(
        ROOM,
        {"type": "m.room.message", "content": {"msgtype": "m.text", "body": "secret"}},
    )
    seen = []
    value.add_event_callback(
        lambda room, item: seen.append(item.body), nio.RoomMessageText
    )
    parsed = response(
        {
            "initial": True,
            "required_state": [member(value.user_id), encryption],
            "timeline": [event("m.room.encrypted", encrypted, "$secret", None)],
        },
        "p2",
    )
    parsed.rooms[ROOM].timeline[0].sender = value.user_id
    await value.receive_response(parsed)
    assert seen == ["secret"]
    assert isinstance(parsed.rooms[ROOM].timeline[0], nio.RoomMessageText)
    assert OTHER not in value.rooms[ROOM].users
    assert ROOM not in value.olm.outbound_group_sessions


@pytest.mark.asyncio
async def test_sliding_request_preserves_optional_extensions_and_caller_input(
    aioresponse,
):
    value = nio.AsyncClient(
        "https://example.org",
        USER,
        config=nio.AsyncClientConfig(encryption_enabled=False),
    )
    value.access_token = "token"
    from aioresponses import CallbackResult

    bodies = []

    async def server(url, **kwargs):
        bodies.append(json.loads(kwargs["data"]))
        return CallbackResult(payload={"pos": "p1"})

    aioresponse.post(
        re.compile(r"https://example.org/.*"), callback=server, repeat=True
    )
    lists = {"main": {"ranges": [[0, 19]], "timeline_limit": 1}}
    await value.sliding_sync(conn_id="desktop", lists=lists)
    assert bodies == [
        {
            "conn_id": "desktop",
            "lists": {"main": {"ranges": [[0, 19]], "timeline_limit": 1}},
        }
    ]
    assert lists == {"main": {"ranges": [[0, 19]], "timeline_limit": 1}}
    await value.close()


def test_hero_profile_does_not_override_membership_state():
    value = client()
    value.receive_response(
        response(
            {
                "required_state": [
                    member(),
                    event(
                        "m.room.member",
                        {
                            "membership": "join",
                            "displayname": "Current",
                            "avatar_url": "mxc://current/avatar",
                        },
                        "$other",
                        OTHER,
                    ),
                ],
                "joined_count": 2,
                "heroes": [
                    {
                        "user_id": OTHER,
                        "displayname": "Old",
                        "avatar_url": "mxc://old/avatar",
                    }
                ],
            }
        )
    )
    assert value.rooms[ROOM].user_name(OTHER) == "Current"
    assert value.rooms[ROOM].avatar_url(OTHER) == "mxc://current/avatar"


@pytest.mark.parametrize(
    "joined, invited, expected", [(1, 0, None), (1, 1, True), (2, 0, False)]
)
def test_heroes_seed_only_current_joined_or_invited_rooms(joined, invited, expected):
    value = client()
    value.receive_response(
        response(
            {
                "joined_count": joined,
                "invited_count": invited,
                "heroes": [{"user_id": OTHER, "displayname": "Bob"}],
            }
        )
    )
    user = value.rooms[ROOM].users.get(OTHER)
    assert (user.invited if user else None) == expected


@pytest.mark.asyncio
async def test_replayed_ciphertext_emits_plaintext_once_after_to_device_key(
    async_client_pair,
):
    alice, bob = async_client_pair
    from nio.crypto import OlmDevice

    alice_device = OlmDevice(
        alice.user_id, alice.device_id, alice.olm.account.identity_keys
    )
    bob_device = OlmDevice(bob.user_id, bob.device_id, bob.olm.account.identity_keys)
    alice.olm.device_store.add(bob_device)
    bob.olm.device_store.add(alice_device)
    alice.olm.account.generate_one_time_keys(1)
    one_time_key = next(iter(alice.olm.account.one_time_keys["curve25519"].values()))
    bob.olm.create_session(one_time_key, alice_device.curve25519)
    _, key_message = bob.olm.share_group_session(
        ROOM, [alice.user_id], ignore_unverified_devices=True
    )
    bob.olm.outbound_group_sessions[ROOM].shared = True
    ciphertext = bob.olm.group_encrypt(
        ROOM,
        {"type": "m.room.message", "content": {"msgtype": "m.text", "body": "secret"}},
    )
    encrypted_event = event("m.room.encrypted", ciphertext, "$secret", None)
    encrypted_event["sender"] = bob.user_id
    observed = []
    alice.add_event_callback(
        lambda room, item: observed.append(type(item)),
        (nio.MegolmEvent, nio.RoomMessageText),
    )
    await alice.receive_response(
        response(
            {
                "initial": True,
                "required_state": [member(alice.user_id), member(bob.user_id)],
            }
        )
    )
    for pos in ("p2", "p3"):
        await alice.receive_response(response({"timeline": [encrypted_event]}, pos))
    assert observed == [nio.MegolmEvent]
    parsed = response(
        {"timeline": [encrypted_event]},
        "p4",
        {
            "to_device": {
                "next_batch": "device1",
                "events": [
                    {
                        "type": "m.room.encrypted",
                        "sender": bob.user_id,
                        "content": key_message["messages"][alice.user_id][
                            alice.device_id
                        ],
                    }
                ],
            }
        },
    )
    await alice.receive_response(parsed)
    assert observed == [nio.MegolmEvent, nio.RoomMessageText]
    assert isinstance(parsed.to_device_events[0], nio.RoomKeyEvent)
    assert isinstance(parsed.rooms[ROOM].timeline[0], nio.RoomMessageText)
    await alice.receive_response(response({"timeline": [encrypted_event]}, "p5"))
    assert observed == [nio.MegolmEvent, nio.RoomMessageText]


def test_historical_leave_does_not_override_current_hero_summary():
    value = client()
    value.receive_response(
        response(
            {
                "initial": True,
                "joined_count": 2,
                "heroes": [{"user_id": OTHER, "displayname": "Bob"}],
                "timeline": [member(OTHER, "leave", "$historical-leave")],
            }
        )
    )
    assert OTHER in value.rooms[ROOM].users
