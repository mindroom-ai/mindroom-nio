"""Event-time observations shared by ordinary and durable sync consumers."""

import inspect
import json
from copy import deepcopy
from dataclasses import replace
from datetime import timedelta
from pathlib import Path

import pytest

from nio import (
    AsyncClient,
    Client,
    Event,
    InviteInfo,
    InviteNameEvent,
    KeyVerificationCancel,
    MatrixRoom,
    RoomKeyRequestCancellation,
    SyncResponse,
    ToDeviceEvent,
)
from nio.crypto import OlmDevice
from nio.store import SqliteMemoryStore
from to_device_client_test import _malformed_room_key_event

ROOM_ID = "!room:example.org"


def name_event(event_id, name):
    return {
        "type": "m.room.name",
        "event_id": event_id,
        "sender": "@alice:example.org",
        "origin_server_ts": 1,
        "state_key": "",
        "content": {"name": name},
    }


def message_event(event_id):
    return {
        "type": "m.room.message",
        "event_id": event_id,
        "sender": "@alice:example.org",
        "origin_server_ts": 1,
        "content": {"msgtype": "m.text", "body": event_id},
    }


def sync_response(*, state=(), timeline=(), section="join"):
    response = SyncResponse.from_dict(
        {
            "next_batch": "next",
            "rooms": {
                section: {
                    ROOM_ID: {
                        "state": {"events": list(state)},
                        "timeline": {"events": list(timeline)},
                    }
                }
            },
        }
    )
    assert isinstance(response, SyncResponse)
    return response


@pytest.fixture(params=[Client, AsyncClient], ids=["sync", "async"])
def client(request):
    if request.param is AsyncClient:
        return AsyncClient("https://example.org", "@alice:example.org")
    return Client("@alice:example.org")


async def receive(client, response):
    result = client.receive_response(response)
    if inspect.isawaitable(result):
        await result


@pytest.mark.asyncio
async def test_callback_observes_event_time_room_state(client):
    response = sync_response(
        state=[name_event("$state", "Initial")],
        timeline=[
            name_event("$rename1", "First"),
            message_event("$first"),
            name_event("$rename2", "Second"),
            message_event("$second"),
        ],
    )
    observed = []
    client.add_event_callback(
        lambda room, event: observed.append((event.event_id, room.name)), Event
    )

    await receive(client, response)

    assert observed == [
        ("$rename1", "First"),
        ("$first", "First"),
        ("$rename2", "Second"),
        ("$second", "Second"),
    ]


@pytest.mark.asyncio
async def test_callback_exception_stops_later_projection(client):
    response = sync_response(
        timeline=[name_event("$first", "First"), name_event("$second", "Second")]
    )
    observed = []

    def callback(room, event):
        observed.append(event.event_id)
        raise RuntimeError("callback stopped")

    client.add_event_callback(callback, Event)

    with pytest.raises(RuntimeError, match="callback stopped"):
        await receive(client, response)

    assert observed == ["$first"]
    assert client.rooms[ROOM_ID].name == "First"
    assert client.next_batch == "next"


def test_iterator_is_synchronous_callback_free_and_preserves_cursor(client):
    response = sync_response(
        timeline=[message_event("$first"), message_event("$second")]
    )
    callback_events = []
    client.add_event_callback(lambda room, event: callback_events.append(event), Event)
    client.next_batch = "before"

    items = list(client._iter_sync(response))

    assert [item.event.event_id for item in items if item.route == "event"] == [
        "$first",
        "$second",
    ]
    assert callback_events == []
    assert client.next_batch == "before"


def test_state_observations_follow_each_applied_event(client):
    response = sync_response(
        state=[name_event("$first", "First"), name_event("$second", "Second")]
    )
    iterator = client._iter_sync(response)
    marker = next(iterator)
    assert (marker.route, marker.event, marker.section) == (None, None, "join")
    assert marker.room is client.rooms[ROOM_ID]
    assert marker.room.name is None
    first = next(iterator)
    assert first.event.event_id == "$first"
    assert first.route is None
    assert first.room.name == "First"
    second = next(iterator)
    assert second.event.event_id == "$second"
    assert second.room.name == "Second"
    assert list(iterator) == []


def test_left_room_is_opt_in_and_removed_after_observations(client):
    client.rooms[ROOM_ID] = MatrixRoom(ROOM_ID, client.user_id)
    response = sync_response(
        section="leave",
        state=[name_event("$state", "Leaving")],
        timeline=[message_event("$last")],
    )
    assert list(client._iter_sync(response)) == []
    assert ROOM_ID in client.rooms
    iterator = client._iter_sync(response, include_left=True)
    marker = next(iterator)
    assert marker.section == "leave"
    assert marker.room is client.rooms[ROOM_ID]
    state = next(iterator)
    assert state.event.event_id == "$state"
    assert state.room.name == "Leaving"
    event = next(iterator)
    assert event.route == "event"
    assert event.event.event_id == "$last"
    assert ROOM_ID in client.rooms
    assert list(iterator) == []
    assert ROOM_ID not in client.rooms


def test_invite_marker_precedes_invited_room_state(client):
    response = sync_response()
    response.rooms.join.clear()
    response.rooms.invite[ROOM_ID] = InviteInfo(
        [InviteNameEvent.from_dict(name_event("$invite", "Invitation"))]
    )
    iterator = client._iter_sync(response)
    marker = next(iterator)
    assert marker.section == "invite"
    assert marker.room is client.invited_rooms[ROOM_ID]
    assert marker.room.name is None
    item = next(iterator)
    assert item.route == "invite"
    assert item.room.name == "Invitation"
    assert list(iterator) == []


@pytest.fixture
def crypto_client(client):
    client.config = replace(client.config, store=SqliteMemoryStore)
    client.device_id = "ALICE"
    client.restore_login(client.user, client.device_id, "access-token")
    try:
        yield client
    finally:
        client.olm.store.database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_to_device_decryption_response_timing(crypto_client, raises):
    client = crypto_client
    sender = Client("@bob:example.org", "BOB", config=client.config)
    sender.restore_login(sender.user, sender.device_id, "sender-token")
    try:
        response = sync_response()
        encrypted = _malformed_room_key_event(sender, client)
        response.to_device_events = [encrypted]
        observed = []

        def callback(event):
            observed.append((event, response.to_device_events[0]))
            if raises:
                raise RuntimeError("stop decrypted callback")

        client.add_to_device_callback(callback, object)
        if raises:
            with pytest.raises(RuntimeError, match="stop decrypted callback"):
                await receive(client, response)
        else:
            await receive(client, response)

        assert len(observed) == 1
        decrypted, during_callback = observed[0]
        assert decrypted is not encrypted
        immediate = isinstance(client, AsyncClient)
        assert during_callback is (decrypted if immediate else encrypted)
        assert response.to_device_events[0] is (
            decrypted if immediate or not raises else encrypted
        )
    finally:
        sender.olm.store.database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("iterate", [False, True])
async def test_expired_and_collected_key_events_follow_olm_state(
    crypto_client, iterate
):
    client = crypto_client
    olm = client.olm
    device = OlmDevice("@bob:example.org", "BOB", olm.account.identity_keys)
    olm.create_sas(device)
    sas = olm.get_active_sas(device.user_id, device.id)
    sas.creation_time -= timedelta(minutes=5)
    cancellation = RoomKeyRequestCancellation.from_dict(
        {
            "sender": client.user_id,
            "type": "m.room_key_request",
            "content": {
                "action": "request_cancellation",
                "request_id": "pending-request",
                "requesting_device_id": "OTHER",
            },
        }
    )
    # A cancellation for a previously exposed request is collected for users.
    olm.received_key_requests[cancellation.request_id] = cancellation
    response = sync_response()
    response.device_key_count.signed_curve25519 = 7
    before_count = olm.uploaded_key_count
    observed = []
    client.add_to_device_callback(
        lambda event: observed.append((type(event), olm.uploaded_key_count)),
        ToDeviceEvent,
    )

    if iterate:
        items = [
            (item.route, type(item.event), olm.uploaded_key_count)
            for item in client._iter_sync(response)
            if item.route
        ]
        assert items == [
            ("expired_verification", KeyVerificationCancel, before_count),
            ("to_device", RoomKeyRequestCancellation, 7),
        ]
        assert observed == []
    else:
        await receive(client, response)
        assert observed == [
            (KeyVerificationCancel, before_count),
            (RoomKeyRequestCancellation, 7),
        ]
    assert not olm.key_verifications
    assert not olm.received_key_requests


@pytest.mark.asyncio
async def test_standalone_joined_handler_preserves_callback_order(client):
    response = sync_response(
        timeline=[name_event("$first", "First"), name_event("$second", "Second")]
    )
    observed = []
    client.add_event_callback(
        lambda room, event: observed.append((event.event_id, room.name)), Event
    )
    result = client._handle_joined_rooms(response)
    if inspect.isawaitable(result):
        await result
    assert observed == [("$first", "First"), ("$second", "Second")]
    assert client.next_batch == ""


@pytest.mark.asyncio
@pytest.mark.parametrize("raises", [False, True])
async def test_subclass_decryption_hook_and_delayed_timeline_replacement(
    client, raises
):
    replacement = Event.parse_event(name_event("$decrypted", "Decrypted"))

    class DecryptionHook(type(client)):
        def _handle_timeline_event(self, event, room_id, room, encrypted_rooms):
            super()._handle_timeline_event(replacement, room_id, room, encrypted_rooms)
            return replacement

    if isinstance(client, AsyncClient):
        hooked = DecryptionHook("https://example.org", "@alice:example.org")
    else:
        hooked = DecryptionHook("@alice:example.org")
    response = sync_response(timeline=[message_event("$encrypted")])
    timeline = response.rooms.join[ROOM_ID].timeline.events
    original = timeline[0]
    observed = []

    def callback(room, event):
        observed.append((event, room.name, timeline[0]))
        if raises:
            raise RuntimeError("stop timeline callback")

    hooked.add_event_callback(callback, Event)
    if raises:
        with pytest.raises(RuntimeError, match="stop timeline callback"):
            await receive(hooked, response)
    else:
        await receive(hooked, response)
    assert observed == [(replacement, "Decrypted", original)]
    assert timeline[0] is (original if raises else replacement)


def test_iterator_retains_encrypted_to_device_source(crypto_client):
    client = crypto_client
    sender = Client("@bob:example.org", "BOB", config=client.config)
    sender.restore_login(sender.user, sender.device_id, "sender-token")
    try:
        response = sync_response()
        encrypted = _malformed_room_key_event(sender, client)
        wire_source = deepcopy(encrypted.source)
        response.to_device_events = [encrypted]
        items = [
            item for item in client._iter_sync(response) if item.route == "to_device"
        ]
        assert len(items) == 1
        assert items[0].event is not encrypted
        assert items[0].source == wire_source
        assert items[0].source["type"] == "m.room.encrypted"
    finally:
        sender.olm.store.database.close()


def test_iterator_retains_timeline_source_before_subclass_hook(client):
    replacement = Event.parse_event(message_event("$clear"))

    class DecryptionHook(type(client)):
        def _handle_timeline_event(self, event, room_id, room, encrypted_rooms):
            event.source["content"]["ciphertext"] = "hook changed source"
            return replacement

    if isinstance(client, AsyncClient):
        hooked = DecryptionHook("https://example.org", "@alice:example.org")
    else:
        hooked = DecryptionHook("@alice:example.org")
    wire_event = json.loads(Path("tests/data/events/megolm.json").read_text())
    response = sync_response(timeline=[wire_event])
    original_source = deepcopy(wire_event)
    items = [item for item in hooked._iter_sync(response) if item.route == "event"]
    assert items[0].event is replacement
    assert items[0].source == original_source
