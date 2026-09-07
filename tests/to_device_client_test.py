import asyncio

import pytest

from nio import (
    AsyncClient,
    AsyncClientConfig,
    Client,
    ClientConfig,
    DeviceList,
    DeviceOneTimeKeyCount,
    RoomKeyEvent,
    Rooms,
    SyncResponse,
    ToDeviceEvent,
    UnknownBadEvent,
)
from nio.crypto import OlmDevice
from nio.store import SqliteMemoryStore


def _malformed_room_key_event(sender: Client, recipient: Client) -> ToDeviceEvent:
    assert sender.olm
    assert recipient.olm

    recipient_device = OlmDevice(
        recipient.user_id,
        recipient.device_id,
        recipient.olm.account.identity_keys,
    )
    recipient.olm.account.generate_one_time_keys(1)
    one_time_key = next(
        iter(recipient.olm.account.one_time_keys["curve25519"].values())
    )
    session = sender.olm.create_session(one_time_key, recipient_device.curve25519)
    content = sender.olm._olm_encrypt(
        session,
        recipient_device,
        "m.room_key",
        {"algorithm": "m.megolm.v1.aes-sha2"},
    )
    event = ToDeviceEvent.parse_event(
        {
            "sender": sender.user_id,
            "type": "m.room.encrypted",
            "content": content,
        }
    )
    assert isinstance(event, ToDeviceEvent)
    return event


def _sync_response(event: ToDeviceEvent) -> SyncResponse:
    return SyncResponse(
        "next",
        Rooms({}, {}, {}),
        DeviceOneTimeKeyCount(None, None),
        DeviceList([], []),
        [event],
        [],
    )


def _add_ordered_callbacks(client: Client, calls: list[tuple[str, object]]) -> None:
    def filtered(event: RoomKeyEvent) -> None:
        calls.append(("filtered", event))

    async def first(event: UnknownBadEvent) -> None:
        calls.append(("first-start", event))
        await asyncio.sleep(0)
        calls.append(("first-end", event))

    def second(event: UnknownBadEvent) -> None:
        calls.append(("second", event))

    client.add_to_device_callback(filtered, RoomKeyEvent)
    client.add_to_device_callback(first, UnknownBadEvent)
    client.add_to_device_callback(second, UnknownBadEvent)


def _close_stores(*clients: Client) -> None:
    for client in clients:
        assert client.olm
        client.olm.store.database.close()


def test_client_delivers_malformed_decrypted_to_device_event_in_order() -> None:
    config = ClientConfig(store=SqliteMemoryStore)
    sender = Client("@alice:example.org", "ALICE", config=config)
    recipient = Client("@bob:example.org", "BOB", config=config)
    sender.restore_login(sender.user, sender.device_id, "sender-token")
    recipient.restore_login(recipient.user, recipient.device_id, "recipient-token")

    try:
        response = _sync_response(_malformed_room_key_event(sender, recipient))
        calls: list[tuple[str, object]] = []
        _add_ordered_callbacks(recipient, calls)

        recipient.receive_response(response)

        event = response.to_device_events[0]
        assert isinstance(event, UnknownBadEvent)
        assert calls == [
            ("first-start", event),
            ("first-end", event),
            ("second", event),
        ]
    finally:
        _close_stores(sender, recipient)


@pytest.mark.asyncio
async def test_async_client_delivers_malformed_decrypted_event_in_order() -> None:
    config = AsyncClientConfig(store=SqliteMemoryStore)
    sender = AsyncClient(
        "https://example.org",
        "@alice:example.org",
        "ALICE",
        config=config,
    )
    recipient = AsyncClient(
        "https://example.org",
        "@bob:example.org",
        "BOB",
        config=config,
    )
    sender.restore_login(sender.user, sender.device_id, "sender-token")
    recipient.restore_login(recipient.user, recipient.device_id, "recipient-token")

    try:
        response = _sync_response(_malformed_room_key_event(sender, recipient))
        calls: list[tuple[str, object]] = []
        _add_ordered_callbacks(recipient, calls)

        await recipient.receive_response(response)

        event = response.to_device_events[0]
        assert isinstance(event, UnknownBadEvent)
        assert calls == [
            ("first-start", event),
            ("first-end", event),
            ("second", event),
        ]
    finally:
        await sender.close()
        await recipient.close()
        _close_stores(sender, recipient)
