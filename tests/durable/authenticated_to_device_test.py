"""Custom event identity is captured at decryption, never inferred from JSON."""

from copy import deepcopy
from dataclasses import replace

import pytest

import nio
from nio.client.base_client import _SyncItem
from nio.crypto import OlmDevice
from nio.durable.codec import freeze_event, restore_event
from nio.durable.model import decode_records, encode_records
from nio.store import SqliteMemoryStore


@pytest.fixture
def peers():
    config = nio.ClientConfig(store=SqliteMemoryStore)
    sender = nio.Client("@peer:example.org", "PEER", config=config)
    recipient = nio.Client("@alice:example.org", "ALICE", config=config)
    for peer in (sender, recipient):
        peer.restore_login(peer.user, peer.device_id, "token")
    recipient.receive_response(
        nio.KeysQueryResponse(
            {
                sender.user_id: {
                    sender.device_id: sender.olm.share_keys()["device_keys"]
                }
            },
            {},
        )
    )
    try:
        yield sender, recipient
    finally:
        for peer in (sender, recipient):
            peer.store.database.close()


def custom_event(sender, recipient, content=None, *, event_type="org.example.notice"):
    recipient_device = OlmDevice(
        recipient.user_id, recipient.device_id, recipient.olm.account.identity_keys
    )
    recipient.olm.account.generate_one_time_keys(1)
    key = next(iter(recipient.olm.account.one_time_keys["curve25519"].values()))
    session = sender.olm.create_session(key, recipient_device.curve25519)
    return nio.ToDeviceEvent.parse_event(
        {
            "sender": sender.user_id,
            "type": "m.room.encrypted",
            "content": sender.olm._olm_encrypt(
                session,
                recipient_device,
                event_type,
                {"value": 1} if content is None else content,
            ),
        }
    )


@pytest.mark.parametrize("content", [{"value": 1}, {}])
def test_custom_event_has_signed_identity_and_replays_without_device_cache(
    peers, content
):
    sender, recipient = peers
    encrypted = custom_event(sender, recipient, content)
    clear = recipient.olm.decrypt_event(encrypted)
    assert hasattr(nio, "AuthenticatedToDeviceEvent")
    assert isinstance(clear, nio.AuthenticatedToDeviceEvent)
    evidence = clear.authenticated_sender
    assert evidence.user_id == "@peer:example.org"
    assert evidence.device_id == "PEER"
    assert evidence.ed25519 == sender.olm.account.identity_keys["ed25519"]
    assert evidence.curve25519 == sender.olm.account.identity_keys["curve25519"]
    record = freeze_event(_SyncItem("to_device", clear, source=encrypted.source))
    (restored_record,) = decode_records(encode_records((record,)))
    recipient.olm.device_store[sender.user_id].clear()
    restored = restore_event(restored_record)
    assert isinstance(restored, nio.AuthenticatedToDeviceEvent)
    assert restored.authenticated_sender == evidence
    assert restored.source["content"] == content


@pytest.mark.parametrize("change", ["unknown", "ambiguous", "signing_key", "device_id"])
def test_custom_event_cannot_authenticate_mismatched_signed_identity(peers, change):
    sender, recipient = peers
    encrypted = custom_event(sender, recipient)
    device = recipient.olm.device_store[sender.user_id][sender.device_id]
    if change == "unknown":
        recipient.olm.device_store[sender.user_id].clear()
    elif change == "ambiguous":
        recipient.olm.device_store.add(
            OlmDevice(sender.user_id, "OTHER", dict(device.keys))
        )
    elif change == "signing_key":
        device.ed25519 = "different"
    else:
        recipient.olm.device_store[sender.user_id].clear()
        recipient.olm.device_store.add(
            OlmDevice(sender.user_id, "OTHER", dict(device.keys))
        )
    clear = recipient.olm.decrypt_event(encrypted)
    assert type(clear) is nio.UnknownToDeviceEvent
    assert not hasattr(clear, "authenticated_sender")


def test_plain_wire_fields_do_not_grant_authentication(peers):
    sender, recipient = peers
    encrypted = custom_event(sender, recipient)
    clear = recipient.olm.decrypt_event(encrypted)
    payload = deepcopy(clear.source)
    payload["authenticated_sender"] = {
        "user_id": sender.user_id,
        "device_id": sender.device_id,
    }
    parsed = nio.ToDeviceEvent.parse_event(payload)
    assert type(parsed) is nio.UnknownToDeviceEvent


def test_invalid_stored_identity_is_rejected(peers):
    sender, recipient = peers
    encrypted = custom_event(sender, recipient)
    clear = recipient.olm.decrypt_event(encrypted)
    record = freeze_event(_SyncItem("to_device", clear, source=encrypted.source))
    encoded = encode_records((record,))
    assert "authenticated_sender" in encoded
    with pytest.raises(ValueError):
        decode_records(encoded.replace('"device_id":"PEER"', '"device_id":7'))


def test_restored_identity_must_match_original_encrypted_envelope(peers):
    sender, recipient = peers
    encrypted = custom_event(sender, recipient)
    clear = recipient.olm.decrypt_event(encrypted)
    record = freeze_event(_SyncItem("to_device", clear, source=encrypted.source))
    source = deepcopy(record.source)
    source["sender"] = "@different:example.org"
    with pytest.raises(ValueError, match="identity"):
        restore_event(replace(record, source=source))


@pytest.mark.parametrize(
    "event_type,content",
    [
        (
            "m.key.verification.cancel",
            {"transaction_id": "test", "code": "m.user", "reason": "cancelled"},
        ),
        (
            "m.room_key_request",
            {
                "action": "request_cancellation",
                "requesting_device_id": "PEER",
                "request_id": "test",
            },
        ),
        ("m.room.encrypted", {"custom": "clear payload"}),
    ],
)
def test_authenticated_replay_preserves_decrypted_wrapper_for_reserved_types(
    peers, event_type, content
):
    """Replay must preserve the decrypt fallback type instead of reclassifying its payload."""
    sender, recipient = peers
    encrypted = custom_event(sender, recipient, content, event_type=event_type)
    clear = recipient.olm.decrypt_event(encrypted)
    assert type(clear) is nio.AuthenticatedToDeviceEvent
    record = freeze_event(_SyncItem("to_device", clear, source=encrypted.source))
    (record,) = decode_records(encode_records((record,)))
    restored = restore_event(record)
    assert type(restored) is nio.AuthenticatedToDeviceEvent
    assert restored.authenticated_sender == clear.authenticated_sender
    assert restored.type == event_type
    assert restored.source == clear.source


@pytest.mark.parametrize(
    "field,value", [("sender", 7), ("type", None), ("content", [])]
)
def test_authenticated_replay_rejects_malformed_clear_shapes(peers, field, value):
    """Retained identity cannot turn malformed clear event structure into authentication."""
    sender, recipient = peers
    encrypted = custom_event(sender, recipient)
    clear = recipient.olm.decrypt_event(encrypted)
    record = freeze_event(_SyncItem("to_device", clear, source=encrypted.source))
    payload = deepcopy(record.clear)
    payload[field] = value
    with pytest.raises(ValueError, match="identity"):
        restore_event(replace(record, clear=payload))
