"""Event trees from other users may nest deeper than Python-level recursion allows."""

import json
from pathlib import Path

import pytest

import nio
from nio import responses
from nio.events import ForwardedRoomKeyEvent, MegolmEvent, OlmEvent, RoomKeyEvent
from nio.rooms import MatrixRoom


def _nested(depth=2000):
    value = 1
    for _ in range(depth):
        value = {"a": value}
    return value


def _fixture(name):
    return json.loads((Path(__file__).parent / "data" / "events" / name).read_text())


@pytest.mark.parametrize(
    "name,cls",
    [
        ("room_key.json", RoomKeyEvent),
        ("forwarded_room_key.json", ForwardedRoomKeyEvent),
    ],
)
def test_room_key_events_parse_deeply_nested_content(name, cls):
    event_dict = _fixture(name)
    event_dict["content"]["nested"] = _nested()

    event = cls.from_dict(event_dict, "@alice:example.org", "sender-key")

    assert type(event) is cls


def test_sliding_sync_parses_deeply_nested_to_device_events():
    response = responses.SlidingSyncResponse.from_dict(
        {
            "pos": "p",
            "extensions": {
                "to_device": {
                    "next_batch": "n",
                    "events": [
                        {
                            "type": "org.example.custom",
                            "sender": "@alice:example.org",
                            "content": {"nested": _nested()},
                        }
                    ],
                }
            },
        }
    )

    assert len(response.to_device_events) == 1


def _encrypted_envelope(algorithm, **content):
    return {
        "type": "m.room.encrypted",
        "sender": "@alice:example.org",
        "event_id": "$e:example.org",
        "origin_server_ts": 1,
        "content": {
            "algorithm": algorithm,
            "sender_key": "sender-key",
            "nested": _nested(),
            **content,
        },
    }


def test_client_snapshots_deeply_nested_encrypted_envelopes():
    client = nio.AsyncClient("https://example.org", "@me:example.org")
    olm_event = OlmEvent.parse_event(
        _encrypted_envelope(
            "m.olm.v1.curve25519-aes-sha2",
            ciphertext={"key": {"type": 0, "body": "sealed"}},
        )
    )
    megolm_event = MegolmEvent.parse_event(
        _encrypted_envelope(
            "m.megolm.v1.aes-sha2",
            ciphertext="sealed",
            session_id="session",
            device_id="DEVICE",
        )
    )
    assert isinstance(olm_event, OlmEvent)
    assert isinstance(megolm_event, MegolmEvent)
    room = MatrixRoom("!r:example.org", "@me:example.org")
    info = responses.RoomInfo(
        responses.Timeline([megolm_event], False, None), [], [], []
    )
    sync = responses.SlidingSyncResponse.from_dict({"pos": "p"})
    sync.to_device_events.append(olm_event)

    to_device = list(client._iter_to_device(sync))
    timeline = list(client._iter_room_timeline(room.room_id, info, room, set(), "join"))

    assert to_device[0].source == olm_event.source
    assert timeline[0].source == megolm_event.source
