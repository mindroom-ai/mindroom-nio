"""Committed event replay uses parsers without repeating crypto processing."""

import copy
import json
from pathlib import Path

import pytest

from nio.client.base_client import _SyncItem
from nio.durable.codec import freeze_event, restore_event
from nio.durable.model import decode_records, encode_records
from nio.events import (
    DummyEvent,
    Event,
    ForwardedRoomKeyEvent,
    InviteEvent,
    InviteMemberEvent,
    KeyVerificationCancel,
    RoomEncryptedImage,
    RoomKeyEvent,
    ToDeviceEvent,
)
from nio.rooms import MatrixInvitedRoom, MatrixRoom


def fixture(name):
    return json.loads(
        (Path(__file__).parents[1] / "data" / "events" / name).read_text()
    )


def roundtrip(item):
    record = freeze_event(item)
    return record, restore_event(decode_records(encode_records((record,)))[0])


def test_decrypted_media_keeps_subtype_envelope_and_event_time_trust():
    event = Event.parse_decrypted_event(fixture("room_encrypted_image.json"))
    event.decrypted, event.verified = True, False
    event.sender_key, event.session_id = "sender-key", "session-id"
    envelope = {"type": "m.room.encrypted", "content": {"ciphertext": "sealed"}}
    record = freeze_event(
        _SyncItem("event", event, MatrixRoom("!r:x", "@me:x"), "join", envelope)
    )
    event.verified = True
    event.source["content"]["body"] = "changed"
    envelope["content"]["ciphertext"] = "changed"
    restored = restore_event(decode_records(encode_records((record,)))[0])
    assert type(restored) is RoomEncryptedImage
    assert restored.body == "orange_cat.jpg"
    assert restored.decrypted is True
    assert restored.verified is False
    assert restored.sender_key == "sender-key"
    assert restored.session_id == "session-id"
    assert record.source["content"]["ciphertext"] == "sealed"
    assert restore_event(record).body == "orange_cat.jpg"


@pytest.mark.parametrize(
    "name,cls",
    [
        ("room_key.json", RoomKeyEvent),
        ("forwarded_room_key.json", ForwardedRoomKeyEvent),
        ("dummy.json", DummyEvent),
    ],
)
def test_sanitized_to_device_preserves_concrete_type(name, cls):
    event = cls.from_dict(fixture(name), "@alice:example.org", "sender-key")
    record, restored = roundtrip(
        _SyncItem(
            "to_device",
            event,
            source={"type": "m.room.encrypted", "content": {"ciphertext": "sealed"}},
        )
    )
    assert type(restored) is cls
    assert restored.sender == "@alice:example.org"
    assert restored.sender_key == "sender-key"
    assert record.crypto.verified is None
    if isinstance(restored, RoomKeyEvent):
        assert restored.session_id == "X3lUlvLELLYxeTx4yOVu6UDpasGEVO0Jbu+QFnm0cKQ"
        assert "session_key" not in restored.source["content"]
    else:
        assert restored.sender_device == "DEVICEID"


def test_invitation_restores_content_removed_by_upstream_parser():
    event = InviteEvent.parse_event(
        {
            "type": "m.room.member",
            "sender": "@alice:x",
            "state_key": "@me:x",
            "content": {"membership": "invite", "displayname": "Me"},
        }
    )
    record, restored = roundtrip(
        _SyncItem("invite", event, MatrixInvitedRoom("!r:x", "@me:x"), "invite")
    )
    assert type(restored) is InviteMemberEvent
    assert restored.membership == "invite"
    assert restored.content["displayname"] == "Me"
    assert restore_event(record).content["displayname"] == "Me"


def test_generated_verification_cancel_without_source_replays():
    event = KeyVerificationCancel(
        {}, "@alice:x", "transaction", "m.timeout", "Timed out"
    )
    _, restored = roundtrip(_SyncItem("expired_verification", event))
    assert type(restored) is KeyVerificationCancel
    assert restored.transaction_id == "transaction"
    assert restored.code == "m.timeout"
    assert restored.reason == "Timed out"


@pytest.mark.parametrize("route", ["presence", "ephemeral"])
def test_transient_routes_are_excluded(route):
    with pytest.raises(ValueError, match="transient"):
        freeze_event(_SyncItem(route, object()))


@pytest.mark.parametrize(
    "field,value",
    [
        ("crypto", {"verified": "false", "sender_key": "key"}),
        ("crypto", {"verified": False, "sender_key": 7}),
        ("crypto", {"verified": False, "sender_key": "key", "session_id": []}),
        ("membership_epoch", True),
        ("membership_epoch", -1),
        (
            "membership",
            {
                "previous": None,
                "current": "other",
                "previous_epoch": 0,
                "current_epoch": 1,
            },
        ),
        (
            "membership",
            {
                "previous": None,
                "current": "join",
                "previous_epoch": "0",
                "current_epoch": 1,
            },
        ),
        (
            "membership",
            {
                "previous": None,
                "current": "join",
                "previous_epoch": 0,
                "current_epoch": 1,
                "source": "other",
            },
        ),
    ],
)
def test_disk_decoder_rejects_invalid_authorization_metadata(field, value):
    raw = {"kind": "timeline", "room_id": "!r:x", "source": {}, field: value}
    with pytest.raises(ValueError):
        decode_records(json.dumps([raw]))


@pytest.mark.parametrize(
    "route,kind",
    [
        ("room_account_data", "room_account_data"),
        ("global_account_data", "global_account_data"),
    ],
)
def test_account_data_roundtrip_keeps_original_content(route, kind):
    from nio.events import AccountDataEvent, TagEvent

    event = AccountDataEvent.parse_event(
        {"type": "m.tag", "content": {"tags": {"m.favourite": {"order": 0.5}}}}
    )
    record, restored = roundtrip(_SyncItem(route, event))
    assert record.kind == kind
    assert type(restored) is TagEvent
    assert restored.tags["m.favourite"]["order"] == 0.5


def test_custom_to_device_clear_payload_keeps_original_envelope():
    from nio.events import UnknownToDeviceEvent

    event = ToDeviceEvent.parse_event(
        {
            "sender": "@alice:x",
            "type": "org.example.signed",
            "content": {"signed": "payload"},
        }
    )
    record, restored = roundtrip(
        _SyncItem(
            "to_device",
            event,
            source={
                "sender": "@alice:x",
                "type": "m.room.encrypted",
                "content": {"ciphertext": "sealed"},
            },
        )
    )
    assert type(restored) is UnknownToDeviceEvent
    assert restored.source["content"]["signed"] == "payload"
    assert record.source["content"]["ciphertext"] == "sealed"


def test_plaintext_timeline_membership_and_provenance_metadata_survive_disk():
    from dataclasses import replace
    from nio.durable.model import OwnMembership
    from nio.event_provenance import TimelineEventProvenance
    from nio.events import RoomMessageText

    event = Event.parse_event(
        {
            "type": "m.room.message",
            "sender": "@alice:x",
            "event_id": "$e:x",
            "origin_server_ts": 1,
            "content": {"msgtype": "m.text", "body": "hello"},
        }
    )
    record = replace(
        freeze_event(_SyncItem("event", event)),
        membership_epoch=2,
        provenance=TimelineEventProvenance.RECOVERED,
        membership=OwnMembership("leave", "join", 1, 2),
    )
    decoded = decode_records(encode_records((record,)))[0]
    assert decoded.membership_epoch == 2
    assert decoded.membership.current == "join"
    assert decoded.provenance is TimelineEventProvenance.RECOVERED
    assert decoded.clear is None
    assert type(restore_event(decoded)) is RoomMessageText


def test_generic_decrypted_to_device_freezes_envelope_sender_key_evidence():
    from nio.events import UnknownToDeviceEvent

    event = ToDeviceEvent.parse_event(
        {
            "sender": "@alice:x",
            "type": "org.example.signed",
            "content": {"signed": "payload"},
        }
    )
    envelope = {
        "sender": "@alice:x",
        "type": "m.room.encrypted",
        "content": {"sender_key": "envelope-key", "ciphertext": "sealed"},
    }
    record = freeze_event(_SyncItem("to_device", event, source=envelope))
    envelope["content"]["sender_key"] = "later-key"
    restored = restore_event(decode_records(encode_records((record,)))[0])
    assert type(restored) is UnknownToDeviceEvent
    assert restored.source["content"]["signed"] == "payload"
    assert record.crypto is not None
    assert record.crypto.sender_key == "envelope-key"
    assert record.crypto.verified is None
    assert record.source["content"]["sender_key"] == "envelope-key"
