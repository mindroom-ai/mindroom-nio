"""Integration tests for durable room-local limited-sync recovery."""

import asyncio
import json
import re
from dataclasses import replace
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
import pytest_asyncio
from aioresponses import CallbackResult

import nio.client.async_client as async_client_module
from nio import (
    AsyncClient,
    AsyncClientConfig,
    DeviceList,
    DeviceOneTimeKeyCount,
    Event,
    FullyReadEvent,
    InviteInfo,
    InviteNameEvent,
    LocalProtocolError,
    LoginResponse,
    MegolmEvent,
    PresenceEvent,
    RoomEncryptedImage,
    RoomEncryptionEvent,
    RoomForgetResponse,
    RoomInfo,
    RoomLeaveResponse,
    RoomMemberEvent,
    RoomMessageText,
    RoomNameEvent,
    Rooms,
    SendRetryError,
    SlidingSyncResponse,
    SyncResponse,
    Timeline,
    ToDeviceEvent,
    TypingNoticeEvent,
    UnknownBadEvent,
    UnknownToDeviceEvent,
)
from nio.api import MATRIX_API_PATH_V3, Api
from nio.client.sync_recovery import (
    PendingTimelineEvent,
    RecoveryGap,
    record_completed_timeline_event,
    should_dispatch_timeline_event,
)
from nio.sliding_sync_tokens import SlidingWindowToken

BASE_URL = f"https://example.org{MATRIX_API_PATH_V3}"
MESSAGES_URL = re.compile(rf"^{BASE_URL}/rooms/.+/messages")
SEND_URL = re.compile(rf"^{BASE_URL}/rooms/.+/send/m.room.message/.+")
SYNC_URL = re.compile(rf"^{BASE_URL}/sync")
ROOM_A = "!a:example.org"
ROOM_B = "!b:example.org"
LOGIN = json.loads(Path("tests/data/login_response.json").read_text())
OWN_ID = LOGIN["user_id"]


def window_token(
    token: str, membership_event_id: str = "$membership"
) -> SlidingWindowToken:
    return SlidingWindowToken(token, membership_event_id)


def text_event(event_id: str, ts: int, room_id: str = ROOM_A) -> RoomMessageText:
    event = RoomMessageText.from_dict(
        {
            "content": {"body": event_id, "msgtype": "m.text"},
            "event_id": event_id,
            "sender": "@sender:example.org",
            "origin_server_ts": ts,
            "room_id": room_id,
            "type": "m.room.message",
        }
    )
    assert isinstance(event, RoomMessageText)
    return event


def member_event(
    event_id: str,
    ts: int,
    membership: str,
    user_id: str = OWN_ID,
) -> RoomMemberEvent:
    event = RoomMemberEvent.from_dict(
        {
            "content": {"membership": membership},
            "event_id": event_id,
            "sender": user_id,
            "state_key": user_id,
            "origin_server_ts": ts,
            "room_id": ROOM_A,
            "type": "m.room.member",
        }
    )
    assert isinstance(event, RoomMemberEvent)
    return event


def name_event(event_id: str, ts: int, name: str) -> RoomNameEvent:
    event = RoomNameEvent.from_dict(
        {
            "content": {"name": name},
            "event_id": event_id,
            "sender": "@sender:example.org",
            "origin_server_ts": ts,
            "room_id": ROOM_A,
            "state_key": "",
            "type": "m.room.name",
        }
    )
    assert isinstance(event, RoomNameEvent)
    return event


def encryption_event(room_id: str = ROOM_B) -> RoomEncryptionEvent:
    event = Event.parse_event(
        {
            "content": {"algorithm": "m.megolm.v1.aes-sha2"},
            "event_id": "$encryption",
            "origin_server_ts": 0,
            "room_id": room_id,
            "sender": "@sender:example.org",
            "state_key": "",
            "type": "m.room.encryption",
        }
    )
    assert isinstance(event, RoomEncryptionEvent)
    return event


def megolm_event(event_id: str, ts: int) -> MegolmEvent:
    event = Event.parse_event(
        {
            "event_id": event_id,
            "sender": "@sender:example.org",
            "origin_server_ts": ts,
            "room_id": ROOM_A,
            "type": "m.room.encrypted",
            "content": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "ciphertext": "AwgAEnACgAkLmt6q",
                "device_id": "DEVICEID",
                "sender_key": "IlRMeOPX2e0MurIyfWEucYBRVOEEUMrOHqn/8mLqMjA",
                "session_id": "X3lUlvLELLYxeTx4yOVu6UDpasGEVO0J",
            },
        }
    )
    assert isinstance(event, MegolmEvent)
    return event


def room_info(
    events: list,
    *,
    limited: bool,
    prev_batch: str | None,
    state: list | None = None,
) -> RoomInfo:
    return RoomInfo(Timeline(events, limited, prev_batch), state or [], [], [])


def sync_response(
    token: str,
    joined: dict[str, RoomInfo],
    *,
    invited: dict[str, InviteInfo] | None = None,
    left: dict | None = None,
    presence: list | None = None,
) -> SyncResponse:
    return SyncResponse(
        token,
        Rooms(invited or {}, joined, left or {}),
        DeviceOneTimeKeyCount(49, 50),
        DeviceList([], []),
        [],
        presence or [],
    )


def sync_json(token: str, joined: dict[str, RoomInfo]) -> dict:
    return {
        "next_batch": token,
        "device_one_time_keys_count": {"signed_curve25519": 50},
        "device_lists": {"changed": [], "left": []},
        "rooms": {
            "invite": {},
            "leave": {},
            "join": {
                room_id: {
                    "timeline": {
                        "events": [event.source for event in info.timeline.events],
                        "limited": info.timeline.limited,
                        "prev_batch": info.timeline.prev_batch,
                    },
                    "state": {"events": []},
                    "ephemeral": {"events": []},
                    "account_data": {"events": []},
                }
                for room_id, info in joined.items()
            },
        },
        "to_device": {"events": []},
        "presence": {"events": []},
        "account_data": {"events": []},
    }


def timeline_response(
    protocol: str,
    token: str,
    events: list[RoomMessageText],
) -> SyncResponse | SlidingSyncResponse:
    if protocol == "classic":
        return sync_response(
            token,
            {ROOM_A: room_info(events, limited=False, prev_batch="p0")},
        )
    response = SlidingSyncResponse.from_dict(
        {
            "pos": token,
            "rooms": {
                ROOM_A: {
                    "membership": "join",
                    "timeline": [event.source for event in events],
                }
            },
        }
    )
    assert isinstance(response, SlidingSyncResponse)
    return response


def messages(events: list, end: str | None) -> dict:
    payload = {"start": "start", "chunk": [event.source for event in events]}
    if end is not None:
        payload["end"] = end
    return payload


class Pages:
    def __init__(self, pages: dict[str, dict]):
        self.pages = pages
        self.from_tokens: list[str | None] = []
        self.to_tokens: list[str | None] = []

    def __call__(self, url, **kwargs) -> CallbackResult:
        query = parse_qs(urlparse(str(url)).query)
        start = query.get("from", [None])[0]
        self.from_tokens.append(start)
        self.to_tokens.append(query.get("to", [None])[0])
        return CallbackResult(status=200, payload=self.pages[start])


def record_events(client: AsyncClient) -> list[str]:
    seen: list[str] = []

    async def callback(_room, event):
        seen.append(event.event_id)

    client.add_event_callback(callback, RoomMessageText)
    return seen


def block_next_recovery_plan(client: AsyncClient, monkeypatch) -> asyncio.Event:
    started = asyncio.Event()
    block_once = True

    class BlockBeforePlan:
        async def __aenter__(self):
            nonlocal block_once
            if block_once:
                block_once = False
                started.set()
                await asyncio.Event().wait()

        async def __aexit__(self, *_args):
            return False

    monkeypatch.setattr(
        client,
        "_recovery_room_state",
        lambda _room_ids: BlockBeforePlan(),
    )
    return started


@pytest_asyncio.fixture
async def client(tempdir):
    value = AsyncClient(
        "https://example.org",
        OWN_ID,
        "DEVICEID",
        tempdir,
        config=AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_max_pages=1,
        ),
    )
    await value.receive_response(LoginResponse.from_dict(LOGIN))
    yield value
    await value.close()


@pytest.mark.asyncio
class TestRoomLocalRecovery:
    async def test_deferred_callback_error_surfaces_without_recovery_gap(self, client):
        client._recovery._deferred_dispatch_errors.append(
            RuntimeError("late callback failure")
        )

        with pytest.raises(RuntimeError, match="late callback failure"):
            await client._pump_sync_recovery()

    async def test_disabled_preserves_short_circuit(self, tempdir):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        calls: list[str] = []

        async def first(_room, _event):
            calls.append("first")
            raise RuntimeError("stop")

        async def second(_room, _event):
            calls.append("second")

        client.add_event_callback(first, RoomMessageText)
        client.add_event_callback(second, RoomMessageText)
        with pytest.raises(RuntimeError, match="stop"):
            await client.receive_response(
                sync_response(
                    "s1",
                    {
                        ROOM_A: room_info(
                            [text_event("$a", 1)], limited=True, prev_batch="p"
                        )
                    },
                )
            )
        assert calls == ["first"]
        await client.close()

    async def test_disabled_store_token_stays_eager_on_callback_error(self, tempdir):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(store_sync_tokens=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))

        async def fail(_room, _event):
            raise RuntimeError("callback failed")

        client.add_event_callback(fail, RoomMessageText)
        with pytest.raises(RuntimeError, match="callback failed"):
            await client.receive_response(
                sync_response(
                    "s1",
                    {
                        ROOM_A: room_info(
                            [text_event("$a", 1)], limited=True, prev_batch="p"
                        )
                    },
                )
            )
        assert client.next_batch == "s1"
        assert client.store.load_sync_token() == "s1"
        await client.close()

    async def test_disabled_preserves_upstream_cross_room_order(self, tempdir):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = []

        async def record(room, event):
            seen.append((room.room_id, type(event).__name__))

        client.add_event_callback(record, RoomMessageText)
        client.add_ephemeral_callback(record, TypingNoticeEvent)
        rooms = {
            ROOM_A: room_info([text_event("$a", 1)], limited=False, prev_batch="a"),
            ROOM_B: room_info(
                [text_event("$b", 2, ROOM_B)], limited=False, prev_batch="b"
            ),
        }
        for info in rooms.values():
            info.ephemeral.append(TypingNoticeEvent([OWN_ID]))
        await client.receive_response(sync_response("s1", rooms))
        assert seen == [
            (ROOM_A, "RoomMessageText"),
            (ROOM_A, "TypingNoticeEvent"),
            (ROOM_B, "RoomMessageText"),
            (ROOM_B, "TypingNoticeEvent"),
        ]
        await client.close()

    async def test_disabled_preserves_to_device_processing_order(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        events = [
            ToDeviceEvent.parse_event(
                {
                    "content": {"body": body},
                    "sender": "@sender:example.org",
                    "type": "org.example.test",
                }
            )
            for body in ("one", "two")
        ]
        assert all(isinstance(event, UnknownToDeviceEvent) for event in events)
        processed = []
        seen = []

        def process(event):
            processed.append(event.source["content"]["body"])

        async def callback(event):
            seen.append((event.source["content"]["body"], tuple(processed)))

        monkeypatch.setattr(client, "_handle_decrypt_to_device", process)
        client.add_to_device_callback(callback, UnknownToDeviceEvent)
        response = sync_response("s1", {})
        response.to_device_events = events
        await client.receive_response(response)
        assert seen == [("one", ("one",)), ("two", ("one", "two"))]
        await client.close()

    async def test_ordinary_sync_ignores_recovery_deadline(self, tempdir):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_timeout=0,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$a", 1)], limited=False, prev_batch="p"
                    )
                },
            )
        )
        assert seen == ["$a"]
        await client.close()

    async def test_first_sync_needs_no_recovery(self, client):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$a", 1)], limited=True, prev_batch="p"
                    )
                },
            )
        )
        assert seen == ["$a"]
        assert not client._recovery.gaps

    async def test_no_id_bad_event_keeps_timeline_position(self, client):
        seen: list[str] = []

        async def record(_room, event):
            seen.append(getattr(event, "event_id", None) or "bad")

        client.add_event_callback(record, RoomMessageText)
        client.add_event_callback(record, UnknownBadEvent)
        malformed = Event.parse_event({"type": "broken"})
        assert isinstance(malformed, UnknownBadEvent)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [
                            text_event("$before", 1),
                            malformed,
                            text_event("$after", 2),
                        ],
                        limited=False,
                        prev_batch="p0",
                    )
                },
            )
        )
        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "s1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [malformed.source],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await client.receive_response(sliding)
        await client.receive_response(sliding)
        assert seen == ["$before", "bad", "$after", "bad", "bad"]
        assert all(
            not event_id.startswith("~")
            for event_id in client._recovery.completed[ROOM_A]
        )

    async def test_complete_timeline_precedes_other_sync_surfaces(self, client):
        seen: list[str] = []

        async def room_callback(_room, event):
            seen.append(type(event).__name__)

        async def presence_callback(event):
            seen.append(type(event).__name__)

        client.add_event_callback(room_callback, RoomMessageText)
        client.add_ephemeral_callback(room_callback, TypingNoticeEvent)
        client.add_room_account_data_callback(room_callback, FullyReadEvent)
        client.add_presence_callback(presence_callback, PresenceEvent)
        info = room_info([text_event("$event", 1)], limited=False, prev_batch="p0")
        payload = sync_json("s1", {ROOM_A: info})
        payload["rooms"]["join"][ROOM_A]["ephemeral"]["events"] = [
            {"content": {"user_ids": [OWN_ID]}, "type": "m.typing"}
        ]
        payload["rooms"]["join"][ROOM_A]["account_data"]["events"] = [
            {"content": {"event_id": "$event"}, "type": "m.fully_read"}
        ]
        payload["presence"] = {
            "events": [
                {
                    "content": {"presence": "online"},
                    "sender": OWN_ID,
                    "type": "m.presence",
                }
            ]
        }
        response = SyncResponse.from_dict(payload)
        assert isinstance(response, SyncResponse)
        await client.receive_response(response)
        assert seen == [
            "RoomMessageText",
            "TypingNoticeEvent",
            "FullyReadEvent",
            "PresenceEvent",
        ]

    async def test_live_window_dispatches_before_backfill_fetch(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        pages = Pages(
            {
                "s1": messages(
                    [text_event("$gap", 2), text_event("$live", 3)],
                    "p1",
                ),
            }
        )
        seen_at_fetch: list[list[str]] = []

        def fetch_page(url, **kwargs):
            seen_at_fetch.append(seen.copy())
            return pages(url, **kwargs)

        aioresponse.get(MESSAGES_URL, callback=fetch_page, repeat=True)
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$live", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen_at_fetch == [["$old", "$live"]]
        assert seen == ["$old", "$live", "$gap"]
        assert pages.from_tokens == ["s1"]
        assert pages.to_tokens == ["p1"]
        assert client.next_batch == "s2"

        await client.receive_response(
            sync_response(
                "s3",
                {
                    ROOM_A: room_info(
                        [text_event("$gap", 2), text_event("$newer", 4)],
                        limited=False,
                        prev_batch="p2",
                    )
                },
            )
        )
        assert seen == ["$old", "$live", "$gap", "$newer"]

    async def test_recovered_encryption_event_updates_room_and_store(
        self, client, aioresponse
    ):
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [encryption_event(ROOM_A), text_event("$held", 3)],
                "p1",
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )

        assert client.rooms[ROOM_A].encrypted
        assert ROOM_A in client.encrypted_rooms
        assert ROOM_A in client.store.load_encrypted_rooms()

    async def test_empty_page_and_page_bound_resume(self, client, aioresponse):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        pages = Pages(
            {
                "s1": messages([], "more"),
                "more": messages(
                    [text_event("$gap", 2), text_event("$held", 3)],
                    "p1",
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        limited = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 3)], limited=True, prev_batch="p1"
                )
            },
        )

        await client.receive_response(limited)
        assert seen == ["$old", "$held"]
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "more"

        await client.receive_response(limited)
        assert pages.from_tokens == ["s1", "more"]
        assert seen == ["$old", "$held", "$gap"]
        assert not client._recovery.gaps

    async def test_sliding_room_account_data_waits_for_pending_classic_gap(
        self, client, aioresponse
    ):
        seen: list[str] = []

        async def record(_room, event):
            seen.append(
                event.event_id
                if isinstance(event, RoomMessageText)
                else type(event).__name__
            )

        client.add_event_callback(record, RoomMessageText)
        client.add_room_account_data_callback(record, FullyReadEvent)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        pages = Pages(
            {
                "s1": messages([text_event("$gap", 2)], "more"),
                "more": messages(
                    [text_event("$gap2", 3), text_event("$held", 4)],
                    "p1",
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 4)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held"]

        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [],
                    }
                },
                "extensions": {
                    "account_data": {
                        "rooms": {
                            ROOM_A: [
                                {
                                    "content": {"event_id": "$held"},
                                    "type": "m.fully_read",
                                }
                            ]
                        }
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await client.receive_response(sliding)

        assert seen == [
            "$old",
            "$held",
            "$gap",
            "$gap2",
            "FullyReadEvent",
        ]
        assert not client._recovery.gaps

    async def test_event_bound_abandons_prefix_at_room_wide_cap(
        self, tempdir, aioresponse
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_max_events=1,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        pages = Pages(
            {
                "s1": messages([text_event("$gap1", 2)], "more"),
                "more": messages([text_event("$gap2", 3)], "p1"),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        limited = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 4)], limited=True, prev_batch="p1"
                )
            },
        )

        await client.receive_response(limited)
        assert pages.from_tokens == ["s1", "more"]
        assert seen == ["$old", "$held"]
        assert not client._recovery.gaps
        await client.close()

    async def test_room_messages_error_keeps_gap_pending(self, client, aioresponse):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(MESSAGES_URL, status=500)
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held"]
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "s1"

    async def test_request_timeout_keeps_gap_pending(self, client, aioresponse):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(MESSAGES_URL, status=408)
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held"]
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "s1"

        with pytest.raises(SendRetryError, match="recovery is still pending"):
            await client.room_send(
                ROOM_A,
                "m.room.message",
                {"body": "secret", "msgtype": "m.text"},
            )

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_live_callback_can_send_without_recovery_gap(
        self, client, aioresponse, protocol
    ):
        sent = []
        aioresponse.put(SEND_URL, payload={"event_id": "$sent"})

        async def send(_room, event):
            sent.append(
                await client.room_send(
                    ROOM_A,
                    "m.room.message",
                    {"body": event.event_id, "msgtype": "m.text"},
                    "tx",
                )
            )

        client.add_event_callback(send, RoomMessageText)
        await client.receive_response(
            timeline_response(protocol, "s1", [text_event("$live", 1)])
        )
        assert [response.event_id for response in sent] == ["$sent"]
        assert not client._recovery.gaps

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    @pytest.mark.parametrize(
        "error_type",
        [RuntimeError, asyncio.TimeoutError],
        ids=["runtime-error", "timeout-error"],
    )
    async def test_live_callback_error_is_acked_and_does_not_wedge(
        self, client, protocol, error_type
    ):
        calls = []
        failed = False

        async def first(_room, event):
            calls.append(f"first:{event.event_id}")

        async def fail_once(_room, event):
            nonlocal failed
            calls.append(f"failing:{event.event_id}")
            if event.event_id == "$bad" and not failed:
                failed = True
                raise error_type("callback failed")

        async def last(_room, event):
            calls.append(f"last:{event.event_id}")

        for callback in (first, fail_once, last):
            client.add_event_callback(callback, RoomMessageText)
        response = timeline_response(
            protocol,
            "s1",
            [text_event("$bad", 1), text_event("$after", 2)],
        )
        with pytest.raises(error_type, match="callback failed"):
            await client.receive_response(response)
        assert [event.event_id for event in client._recovery.events[(ROOM_A, 1)]] == [
            "$after"
        ]

        await client.receive_response(response)
        await client.receive_response(
            timeline_response(protocol, "s2", [text_event("$next", 3)])
        )
        assert calls == [
            "first:$bad",
            "failing:$bad",
            "first:$after",
            "failing:$after",
            "last:$after",
            "first:$next",
            "failing:$next",
            "last:$next",
        ]
        assert not client._recovery.gaps

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_all_room_state_is_ready_before_live_callbacks(
        self, client, protocol
    ):
        failed = False
        seen = []

        async def callback(room, event):
            nonlocal failed
            if event.event_id == "$a" and not failed:
                failed = True
                raise RuntimeError("callback failed")
            if event.event_id == "$b":
                seen.append(room.encrypted)

        client.add_event_callback(callback, RoomMessageText)
        if protocol == "classic":
            response = sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$a", 1)], limited=False, prev_batch="a"
                    ),
                    ROOM_B: room_info(
                        [text_event("$b", 2, ROOM_B)],
                        limited=False,
                        prev_batch="b",
                        state=[encryption_event()],
                    ),
                },
            )
        else:
            response = SlidingSyncResponse.from_dict(
                {
                    "pos": "s1",
                    "rooms": {
                        ROOM_A: {
                            "membership": "join",
                            "timeline": [text_event("$a", 1).source],
                        },
                        ROOM_B: {
                            "membership": "join",
                            "required_state": [encryption_event().source],
                            "timeline": [text_event("$b", 2, ROOM_B).source],
                        },
                    },
                }
            )
            assert isinstance(response, SlidingSyncResponse)

        with pytest.raises(RuntimeError, match="callback failed"):
            await client.receive_response(response)
        await client.receive_response(response)
        assert seen == [True]
        assert client.rooms[ROOM_B].encrypted

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_live_events_do_not_wait_for_stuck_gap(self, tempdir, protocol):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_max_events=2,
                backfill_timeout=0,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        client.next_batch = "s0"
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$held1", 1)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        await client.receive_response(
            timeline_response(protocol, "s2", [text_event("$held2", 2)])
        )
        assert seen == ["$held1", "$held2"]

        await client.receive_response(
            timeline_response(protocol, "s3", [text_event("$held3", 3)])
        )
        assert seen == ["$held1", "$held2", "$held3"]
        assert client._recovery.gaps
        await client.close()

    async def test_first_limited_window_honors_held_live_bound(self, tempdir):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_max_events=2,
                backfill_timeout=0,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        client.next_batch = "s0"
        seen = record_events(client)
        response = sync_response(
            "s1",
            {
                ROOM_A: room_info(
                    [text_event(f"$held{index}", index) for index in range(3)],
                    limited=True,
                    prev_batch="p1",
                )
            },
        )
        await client.receive_response(response)
        assert seen == ["$held0", "$held1", "$held2"]
        assert not client._recovery.gaps
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        await client.close()

    async def test_repeated_end_token_abandons_gap_and_releases_live(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "s1"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held"]
        assert not client._recovery.gaps

    @pytest.mark.parametrize("end", [None, "s1"])
    async def test_live_overlap_closes_gap_without_end_progress(
        self, client, aioresponse, end
    ):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 2), text_event("$held", 3)],
                end,
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held", "$gap"]
        assert not client._recovery.gaps

    async def test_exhausted_bounded_page_closes_gap(self, tempdir, aioresponse):
        """A `to`-bounded walk that runs dry has reached the window.

        Deployed servers stop the forward walk before the window's own
        events and answer the following request with an empty chunk and
        no `end` token, so the live overlap never closes the gap. The
        recovered history has to survive that, not be discarded.
        """
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_max_pages=2,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        pages = Pages(
            {
                "s1": messages([text_event("$gap1", 2), text_event("$gap2", 3)], "m1"),
                "m1": messages([], None),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 4)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held", "$gap1", "$gap2"]
        assert pages.from_tokens == ["s1", "m1"]
        assert pages.to_tokens == ["p1", "p1"]
        assert not client._recovery.gaps
        await client.close()

    async def test_single_exhausted_page_closes_gap(self, client, aioresponse):
        """The first page may itself be the last one."""
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(MESSAGES_URL, payload=messages([text_event("$gap", 2)], None))
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held", "$gap"]
        assert not client._recovery.gaps

    async def test_empty_bounded_gap_closes_without_recovery(self, client, aioresponse):
        """Nothing between the tokens is a complete walk, not a failure."""
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(MESSAGES_URL, payload=messages([], None))
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 2)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held"]
        assert not client._recovery.gaps

    async def test_live_overlap_defers_events_after_held_window(
        self, tempdir, aioresponse
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_max_events=2,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [
                    text_event("$gap", 1),
                    text_event("$held", 2),
                    text_event("$overflow", 3),
                ],
                "more",
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 2)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert seen == ["$held", "$gap"]
        assert not client._recovery.gaps
        await client.receive_response(
            sync_response(
                "s3",
                {
                    ROOM_A: room_info(
                        [text_event("$overflow", 3)],
                        limited=False,
                        prev_batch="p2",
                    )
                },
            )
        )
        assert seen == ["$held", "$gap", "$overflow"]
        await client.close()

    @pytest.mark.parametrize("second_protocol", ["classic", "sliding"])
    async def test_concurrent_response_pumps_dispatch_once(
        self, client, second_protocol
    ):
        pending = PendingTimelineEvent.from_event(
            ROOM_A,
            1,
            0,
            text_event("$one", 1),
            True,
        )
        assert pending is not None
        client._recovery.gaps[ROOM_A] = [RecoveryGap(ROOM_A, 1, "", None)]
        client._recovery.events[(ROOM_A, 1)] = [pending]
        client.next_batch = "s1"
        entered = asyncio.Event()
        release = asyncio.Event()
        seen = []

        async def callback(_room, event):
            seen.append(event.event_id)
            entered.set()
            await release.wait()

        client.add_event_callback(callback, RoomMessageText)
        first = asyncio.create_task(client.receive_response(sync_response("s1", {})))
        await entered.wait()
        if second_protocol == "classic":
            second_response = sync_response("s1", {})
        else:
            second_response = SlidingSyncResponse.from_dict({"pos": "s2", "rooms": {}})
        second = asyncio.create_task(client.receive_response(second_response))
        await asyncio.sleep(0)
        release.set()
        await asyncio.gather(first, second)
        assert seen == ["$one"]
        assert not client._recovery.gaps

    async def test_concurrent_sync_requests_keep_their_own_cursors(
        self, client, monkeypatch
    ):
        class Transport:
            status = 200

            def __init__(self, request_since):
                self.request_since = request_since

        responses = {
            "a": sync_response(
                "response-a",
                {
                    ROOM_A: room_info(
                        [text_event("$a", 1)], limited=True, prev_batch="target-a"
                    )
                },
            ),
            "b": sync_response(
                "response-b",
                {
                    ROOM_B: room_info(
                        [text_event("$b", 2, ROOM_B)],
                        limited=True,
                        prev_batch="target-b",
                    )
                },
            ),
        }
        b_processed = asyncio.Event()

        async def send(_method, path, *_args, **_kwargs):
            request_since = parse_qs(urlparse(path).query)["since"][0]
            if request_since == "a":
                await b_processed.wait()
            return Transport(request_since)

        async def create_matrix_response(*, transport_response, **_kwargs):
            return responses[transport_response.request_since]

        original_receive = client._receive_sync_family

        async def receive(response):
            await original_receive(response)
            if ROOM_B in client._recovery.gaps:
                b_processed.set()

        monkeypatch.setattr(client, "send", send)
        monkeypatch.setattr(client, "create_matrix_response", create_matrix_response)
        monkeypatch.setattr(client, "_receive_sync_family", receive)
        first, second = await asyncio.gather(
            client.sync(since="a"),
            client.sync(since="b"),
        )
        assert {first.next_batch, second.next_batch} == {"response-a", "response-b"}
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "a"
        assert client._recovery.gaps[ROOM_B][0].cursor_token == "b"

    @pytest.mark.parametrize(
        "nested_kind",
        ["sync", "sliding_sync", "receive_sync", "receive_sliding"],
    )
    @pytest.mark.parametrize("outer_protocol", ["classic", "sliding"])
    @pytest.mark.parametrize("child_task", [False, True])
    async def test_callback_sync_reentry_fails_before_io_or_mutation(
        self, client, monkeypatch, nested_kind, outer_protocol, child_task
    ):
        async def unexpected_send(*_args, **_kwargs):
            raise AssertionError("callback reentry must fail before HTTP")

        monkeypatch.setattr(client, "_send", unexpected_send)
        seen: list[str] = []
        failures: list[str] = []

        async def nested():
            if nested_kind == "sync":
                return await client.sync(since="nested")
            if nested_kind == "sliding_sync":
                return await client.sliding_sync(pos="nested")
            if nested_kind == "receive_sync":
                return await client.receive_response(
                    timeline_response("classic", "nested", [text_event("$nested", 2)])
                )
            return await client.receive_response(
                timeline_response("sliding", "nested", [text_event("$nested", 2)])
            )

        async def callback(_room, value):
            seen.append(value.event_id)
            if value.event_id != "$outer":
                return
            try:
                operation = nested()
                if child_task:
                    await asyncio.create_task(operation)
                else:
                    await operation
            except LocalProtocolError as error:
                failures.append(str(error))

        client.add_event_callback(callback, RoomMessageText)
        await client.receive_response(
            timeline_response(
                outer_protocol,
                "outer",
                [text_event("$outer", 1), text_event("$suffix", 3)],
            )
        )
        assert failures == ["Sync-family requests cannot run from a timeline callback."]
        assert seen == ["$outer", "$suffix"]
        assert client.next_batch == ("outer" if outer_protocol == "classic" else "")

    async def test_stale_inherited_executor_token_may_proceed(self, client):
        release = asyncio.Event()
        child: asyncio.Task | None = None
        seen: list[str] = []

        async def delayed_nested():
            await release.wait()
            await client.receive_response(
                timeline_response("classic", "nested", [text_event("$nested", 2)])
            )

        async def callback(_room, value):
            nonlocal child
            seen.append(value.event_id)
            if value.event_id == "$outer":
                child = asyncio.create_task(delayed_nested())

        client.add_event_callback(callback, RoomMessageText)
        await client.receive_response(
            timeline_response("classic", "outer", [text_event("$outer", 1)])
        )
        assert child is not None
        release.set()
        await child
        assert seen == ["$outer", "$nested"]
        assert client.next_batch == "nested"

    @pytest.mark.parametrize("nested_protocol", ["classic", "sliding"])
    async def test_disabled_callback_sync_reentry_keeps_upstream_behavior(
        self, tempdir, nested_protocol
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen: list[str] = []

        async def callback(_room, value):
            seen.append(value.event_id)
            if value.event_id == "$outer":
                await client.receive_response(
                    timeline_response(
                        nested_protocol,
                        "nested",
                        [text_event("$nested", 2)],
                    )
                )

        client.add_event_callback(callback, RoomMessageText)
        await client.receive_response(
            timeline_response("classic", "outer", [text_event("$outer", 1)])
        )
        assert seen == ["$outer", "$nested"]
        await client.close()

    async def test_room_send_linearizes_with_concurrent_gap_install(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_max_pages=0,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            sync_response(
                "s1",
                {ROOM_A: room_info([], limited=False, prev_batch="p0")},
            )
        )
        send_started = asyncio.Event()
        release_send = asyncio.Event()
        sent = object()

        async def send(*_args, **_kwargs):
            send_started.set()
            await release_send.wait()
            return sent

        monkeypatch.setattr(client, "_send", send)
        send_task = asyncio.create_task(
            client.room_send(
                ROOM_A,
                "m.room.message",
                {"body": "safe", "msgtype": "m.text"},
            )
        )
        await send_started.wait()
        response_task = asyncio.create_task(
            client.receive_response(
                sync_response(
                    "s2",
                    {
                        ROOM_A: room_info(
                            [text_event("$held", 2)],
                            limited=True,
                            prev_batch="p1",
                        )
                    },
                )
            )
        )
        await asyncio.sleep(0)
        await response_task
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "s1"
        assert not send_task.done()
        release_send.set()
        assert await send_task is sent
        await client.close()

    async def test_same_room_plan_waits_for_immutable_send_request(
        self, client, monkeypatch
    ):
        client.config = replace(client.config, backfill_max_pages=0)
        await client.receive_response(
            sync_response(
                "s1",
                {ROOM_A: room_info([], limited=False, prev_batch="p0")},
            )
        )
        prepare_started = asyncio.Event()
        release_prepare = asyncio.Event()
        sent = object()
        original_prepare = client._prepare_room_send

        async def prepare(*args, **kwargs):
            prepare_started.set()
            await release_prepare.wait()
            return await original_prepare(*args, **kwargs)

        async def send(*_args, **_kwargs):
            assert not client._recovery_room_gate(ROOM_A).locked()
            return sent

        monkeypatch.setattr(client, "_prepare_room_send", prepare)
        monkeypatch.setattr(client, "_send", send)
        send_task = asyncio.create_task(
            client.room_send(
                ROOM_A,
                "m.room.message",
                {"body": "safe", "msgtype": "m.text"},
            )
        )
        await prepare_started.wait()
        response_task = asyncio.create_task(
            client.receive_response(
                sync_response(
                    "s2",
                    {
                        ROOM_A: room_info(
                            [text_event("$held", 2)],
                            limited=True,
                            prev_batch="p1",
                        )
                    },
                )
            )
        )
        await asyncio.sleep(0)
        assert not client._recovery.gaps
        assert not response_task.done()

        release_prepare.set()
        assert await send_task is sent
        await response_task
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "s1"

    async def test_blocked_room_preparation_does_not_block_other_room(
        self, client, monkeypatch
    ):
        client.config = replace(client.config, backfill_max_pages=0)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info([], limited=False, prev_batch="a0"),
                    ROOM_B: room_info([], limited=False, prev_batch="b0"),
                },
            )
        )
        room_a_started = asyncio.Event()
        release_room_a = asyncio.Event()
        room_b_sent = asyncio.Event()
        original_prepare = client._prepare_room_send

        async def prepare(room_id, *args, **kwargs):
            if room_id == ROOM_A:
                room_a_started.set()
                await release_room_a.wait()
            return await original_prepare(room_id, *args, **kwargs)

        async def send(_response_class, _method, _path, _data, response_data):
            if response_data == (ROOM_B,):
                room_b_sent.set()
            return object()

        monkeypatch.setattr(client, "_prepare_room_send", prepare)
        monkeypatch.setattr(client, "_send", send)
        room_a_send = asyncio.create_task(
            client.room_send(
                ROOM_A,
                "m.room.message",
                {"body": "a", "msgtype": "m.text"},
            )
        )
        await room_a_started.wait()
        room_b_send = asyncio.create_task(
            client.room_send(
                ROOM_B,
                "m.room.message",
                {"body": "b", "msgtype": "m.text"},
            )
        )
        await asyncio.wait_for(room_b_sent.wait(), 1)
        await room_b_send

        await asyncio.wait_for(
            client.receive_response(
                sync_response(
                    "s2",
                    {
                        ROOM_B: room_info(
                            [text_event("$held-b", 2, ROOM_B)],
                            limited=True,
                            prev_batch="b1",
                        )
                    },
                )
            ),
            1,
        )
        assert client._recovery.gaps[ROOM_B][0].cursor_token == "s1"
        assert not room_a_send.done()
        release_room_a.set()
        await room_a_send

    async def test_multi_room_plan_acquires_gates_in_sorted_order(self, client):
        client.next_batch = "s1"
        room_a_gate = client._recovery_room_gate(ROOM_A)
        room_b_gate = client._recovery_room_gate(ROOM_B)
        await room_a_gate.acquire()
        response_task = asyncio.create_task(
            client.receive_response(
                sync_response(
                    "s2",
                    {
                        ROOM_B: room_info([], limited=False, prev_batch="b1"),
                        ROOM_A: room_info([], limited=False, prev_batch="a1"),
                    },
                )
            )
        )
        await asyncio.sleep(0)
        assert not response_task.done()
        assert not room_b_gate.locked()
        room_a_gate.release()
        await response_task

    async def test_room_send_builds_request_under_gate_and_sends_after(
        self, client, monkeypatch
    ):
        await client.receive_response(
            sync_response(
                "s1",
                {ROOM_A: room_info([], limited=False, prev_batch="p0")},
            )
        )
        original_room_send = Api.room_send
        built = False
        sent = object()

        def build(*args, **kwargs):
            nonlocal built
            assert client._recovery_room_gate(ROOM_A).locked()
            built = True
            return original_room_send(*args, **kwargs)

        async def send(*_args, **_kwargs):
            assert built
            assert not client._recovery_room_gate(ROOM_A).locked()
            return sent

        monkeypatch.setattr(Api, "room_send", build)
        monkeypatch.setattr(client, "_send", send)
        assert (
            await client.room_send(
                ROOM_A,
                "m.room.message",
                {"body": "safe", "msgtype": "m.text"},
            )
            is sent
        )

    async def test_missing_prev_batch_uses_response_target(self, client, aioresponse):
        seen = record_events(client)
        client.next_batch = "s1"
        pages = Pages({"s1": messages([text_event("$gap", 2)], "s2")})
        aioresponse.get(MESSAGES_URL, callback=pages)
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch=None
                    )
                },
            )
        )
        assert pages.to_tokens == ["s2"]
        assert seen == ["$held", "$gap"]
        assert not client._recovery.gaps

    async def test_incomplete_room_keeps_transport_monotonic(
        self, tempdir, aioresponse
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_max_pages=1,
                store_sync_tokens=True,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "more"),
            repeat=True,
        )
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 3)], limited=True, prev_batch="p1"
                )
            },
        )
        await client.receive_response(response)
        assert client.next_batch == "s2"
        assert client.store.load_sync_token() == "s2"
        assert client._recovery.gaps.get(ROOM_A)
        assert seen == ["$old", "$held"]

        requested: list[str | None] = []

        def next_sync(url, **kwargs):
            requested.append(parse_qs(urlparse(str(url)).query).get("since", [None])[0])
            return CallbackResult(status=200, payload=sync_json("s3", {}))

        aioresponse.get(SYNC_URL, callback=next_sync)
        await client.sync()
        assert requested == ["s2"]
        await client.close()

    async def test_unrelated_room_and_presence_do_not_wait(self, client, aioresponse):
        seen = record_events(client)
        presence_seen: list[str] = []

        async def on_presence(event):
            presence_seen.append(event.user_id)

        client.add_presence_callback(on_presence, PresenceEvent)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "more"),
            repeat=True,
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    ),
                    ROOM_B: room_info(
                        [text_event("$free", 4, ROOM_B)],
                        limited=False,
                        prev_batch="q1",
                    ),
                },
                presence=[PresenceEvent("@sender:example.org", "online")],
            )
        )
        assert seen == ["$old", "$held", "$free"]
        assert presence_seen == ["@sender:example.org"]

    async def test_newer_same_room_event_dispatches_before_pending_gap(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "more"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        pages = Pages(
            {
                "more": messages(
                    [text_event("$gap2", 3), text_event("$held", 4)],
                    "p1",
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages)
        await client.receive_response(
            sync_response(
                "s3",
                {
                    ROOM_A: room_info(
                        [text_event("$later", 5)], limited=False, prev_batch="p2"
                    )
                },
            )
        )
        assert seen == ["$old", "$held", "$later", "$gap", "$gap2"]

    async def test_ignored_to_live_boundary_preserves_recovered_prefix(
        self, client, aioresponse
    ):
        seen = record_events(client)
        client.next_batch = "s1"
        recovered = [text_event(f"${index}", index) for index in range(14)]
        live = [text_event(f"${index}", index) for index in range(14, 64)]
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(recovered + live[:36], "ignored-to-bound"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        live,
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert seen == [f"${index}" for index in range(14, 64)] + [
            f"${index}" for index in range(14)
        ]
        assert not client._recovery.gaps

    async def test_non_json_messages_4xx_abandons_gap_and_releases_live(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(MESSAGES_URL, status=403, body="forbidden")
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held"]
        assert not client._recovery.gaps

    async def test_oversized_page_abandons_durably(self, tempdir, aioresponse):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_max_events=1,
            backfill_page_size=1,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 2), text_event("$overflow", 3)], "more"
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 4)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$old", "$held"]
        assert not client._recovery.gaps
        await client.close()
        client.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        assert not restarted._recovery.gaps
        assert list(restarted._recovery.completed[ROOM_A]) == ["$old", "$held"]
        await restarted.close()

    async def test_target_page_recovers_concurrent_dag_branches(
        self, client, aioresponse
    ):
        seen = record_events(client)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [
                    text_event("$present2", 4),
                    text_event("$gap2", 2),
                    text_event("$present1", 3),
                    text_event("$gap1", 1),
                ],
                "p1",
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$present1", 3), text_event("$present2", 4)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert seen == ["$present1", "$present2", "$gap2", "$gap1"]
        assert not client._recovery.gaps

    async def test_two_rooms_close_independently(self, client, aioresponse):
        seen = record_events(client)
        client.next_batch = "s1"
        page_index = 0

        def page(url, **kwargs):
            nonlocal page_index
            page_index += 1
            if page_index == 1:
                events = [text_event("$gap-a", 1), text_event("$live-a", 2)]
            else:
                events = [
                    text_event("$gap-b", 1, ROOM_B),
                    text_event("$live-b", 2, ROOM_B),
                ]
            return CallbackResult(
                status=200,
                payload=messages(events, "p1" if page_index == 1 else "p2"),
            )

        aioresponse.get(MESSAGES_URL, callback=page, repeat=True)
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$live-a", 2)], limited=True, prev_batch="p1"
                    ),
                    ROOM_B: room_info(
                        [text_event("$live-b", 2, ROOM_B)],
                        limited=True,
                        prev_batch="p2",
                    ),
                },
            )
        )
        assert set(seen) == {"$gap-a", "$live-a", "$gap-b", "$live-b"}
        assert not client._recovery.gaps

    async def test_own_rejoin_discards_prejoin_history(self, client, aioresponse):
        seen = record_events(client)
        client.add_event_callback(
            lambda _room, event: seen.append(event.event_id), RoomMemberEvent
        )
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [
                    text_event("$prejoin", 1),
                    member_event("$join", 2, "join"),
                    text_event("$after", 3),
                    text_event("$live", 4),
                ],
                "p1",
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$live", 4)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$live", "$join", "$after"]

    async def test_bounded_prefix_waits_for_later_own_join(self, client, aioresponse):
        seen = record_events(client)
        client.add_event_callback(
            lambda _room, event: seen.append(event.event_id), RoomMemberEvent
        )
        client.next_batch = "s1"
        pages = Pages(
            {
                "s1": messages([text_event("$prejoin", 1)], "more"),
                "more": messages(
                    [
                        member_event("$join", 2, "join"),
                        text_event("$after", 3),
                        text_event("$held", 4),
                    ],
                    "p1",
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        limited = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 4)], limited=True, prev_batch="p1"
                )
            },
        )
        await client.receive_response(limited)
        assert seen == ["$held"]
        assert [
            event.event_id
            for event in client._recovery.events[(ROOM_A, 1)]
            if event.kind != "boundary"
        ] == ["$prejoin"]

        await client.receive_response(limited)
        assert seen == ["$held", "$join", "$after"]
        assert "$prejoin" not in seen
        assert not client._recovery.gaps

    async def test_recent_overlap_and_encrypted_replay(self, client, aioresponse):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$seen", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$seen", 1), text_event("$gap", 2), text_event("$live", 3)],
                "p1",
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$live", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$seen", "$live", "$gap"]

        encrypted = megolm_event("$encrypted", 4)
        record_completed_timeline_event(
            client._recovery, ROOM_A, encrypted.event_id, True
        )
        clear = text_event("$encrypted", 4)
        assert should_dispatch_timeline_event(client._recovery, ROOM_A, clear)

    async def test_live_decryption_upgrades_completed_encrypted_event(
        self, client, monkeypatch
    ):
        seen = record_events(client)
        encrypted = megolm_event("$encrypted", 1)
        clear = text_event("$encrypted", 1)
        record_completed_timeline_event(
            client._recovery, ROOM_A, encrypted.event_id, True
        )

        def decrypt(event, *_args):
            return clear if event.event_id == encrypted.event_id else None

        monkeypatch.setattr(client, "_handle_timeline_event", decrypt)
        await client.receive_response(
            sync_response(
                "s1",
                {ROOM_A: room_info([encrypted], limited=False, prev_batch="p0")},
            )
        )
        assert seen == ["$encrypted"]
        assert client._recovery.completed[ROOM_A]["$encrypted"] is False

    async def test_live_decryption_preserves_callback_metadata(
        self, client, monkeypatch
    ):
        encrypted = megolm_event("$encrypted-image", 1)
        source = json.loads(
            Path("tests/data/events/room_encrypted_image.json").read_text()
        )
        source.update(
            {
                "event_id": encrypted.event_id,
                "origin_server_ts": encrypted.server_timestamp,
                "room_id": ROOM_A,
                "sender": encrypted.sender,
            }
        )
        decrypted = Event.parse_decrypted_event(source)
        assert isinstance(decrypted, RoomEncryptedImage)
        decrypted.decrypted = True
        decrypted.verified = True
        decrypted.sender_key = "sender-key"
        decrypted.session_id = "session-id"
        decrypted.room_id = ROOM_A
        decrypt_calls = 0

        def decrypt(event):
            nonlocal decrypt_calls
            assert isinstance(event, MegolmEvent)
            decrypt_calls += 1
            return decrypted

        assert client.olm
        monkeypatch.setattr(client.olm, "_decrypt_megolm_no_error", decrypt)
        seen = []

        async def record(_room, event):
            seen.append(
                (
                    type(event),
                    event.decrypted,
                    event.verified,
                    event.sender_key,
                    event.session_id,
                )
            )

        client.add_event_callback(record, RoomEncryptedImage)
        await client.receive_response(
            sync_response(
                "s1",
                {ROOM_A: room_info([encrypted], limited=False, prev_batch="p0")},
            )
        )

        assert decrypt_calls == 2
        assert seen == [(RoomEncryptedImage, True, True, "sender-key", "session-id")]

    async def test_recovery_decryption_preserves_callback_metadata(
        self, client, aioresponse, monkeypatch
    ):
        encrypted = megolm_event("$recovered-image", 1)
        source = json.loads(
            Path("tests/data/events/room_encrypted_image.json").read_text()
        )
        source.update(
            {
                "event_id": encrypted.event_id,
                "origin_server_ts": encrypted.server_timestamp,
                "room_id": ROOM_A,
                "sender": encrypted.sender,
            }
        )
        decrypted = Event.parse_decrypted_event(source)
        assert isinstance(decrypted, RoomEncryptedImage)
        decrypted.decrypted = True
        decrypted.verified = True
        decrypted.sender_key = "sender-key"
        decrypted.session_id = "session-id"
        decrypted.room_id = ROOM_A
        decrypt_calls = 0

        def decrypt(event):
            nonlocal decrypt_calls
            assert isinstance(event, MegolmEvent)
            decrypt_calls += 1
            return decrypted

        assert client.olm
        monkeypatch.setattr(client.olm, "_decrypt_megolm_no_error", decrypt)
        seen = []

        async def record(_room, event):
            seen.append(
                (
                    type(event),
                    event.decrypted,
                    event.verified,
                    event.sender_key,
                    event.session_id,
                )
            )

        client.add_event_callback(record, RoomEncryptedImage)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([encrypted], "p1"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 2)], limited=True, prev_batch="p1"
                    )
                },
            )
        )

        assert decrypt_calls == 1
        assert seen == [(RoomEncryptedImage, True, True, "sender-key", "session-id")]

    async def test_restart_resumes_room_without_replaying_response_surfaces(
        self, tempdir, aioresponse
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_max_pages=1,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        first_seen = record_events(first)
        presence_seen: list[str] = []
        response_seen: list[str] = []
        room_surface_seen: list[str] = []

        async def on_presence(event):
            presence_seen.append(event.user_id)

        async def on_room_surface(_room, event):
            room_surface_seen.append(type(event).__name__)

        async def on_response(response):
            if isinstance(response, SyncResponse):
                response_seen.append(response.next_batch)

        first.add_presence_callback(on_presence, PresenceEvent)
        first.add_ephemeral_callback(on_room_surface, TypingNoticeEvent)
        first.add_room_account_data_callback(
            on_room_surface, (FullyReadEvent, UnknownBadEvent)
        )
        first.add_response_callback(on_response, SyncResponse)
        await first.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "more"),
        )
        limited_payload = sync_json(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 4)], limited=True, prev_batch="p1"
                )
            },
        )
        limited_payload["rooms"]["join"][ROOM_A]["ephemeral"]["events"] = [
            {"content": {"user_ids": [OWN_ID]}, "type": "m.typing"}
        ]
        limited_payload["rooms"]["join"][ROOM_A]["account_data"]["events"] = [
            {"content": {"event_id": "$held"}, "type": "m.fully_read"},
            {"type": "m.tag"},
        ]
        limited = SyncResponse.from_dict(limited_payload)
        assert isinstance(limited, SyncResponse)
        limited.presence_events = [PresenceEvent("@sender:example.org", "online")]
        await first.receive_response(limited)
        await first.run_response_callbacks([limited])
        assert first_seen == ["$old", "$held"]
        assert room_surface_seen == []
        assert presence_seen == ["@sender:example.org"]
        assert response_seen == ["s2"]
        await first.close()
        first.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        restarted_seen = record_events(restarted)
        restarted.add_ephemeral_callback(on_room_surface, TypingNoticeEvent)
        restarted.add_room_account_data_callback(
            on_room_surface, (FullyReadEvent, UnknownBadEvent)
        )
        assert restarted.loaded_sync_token == "s2"
        assert restarted._recovery.gaps.get(ROOM_A)
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap2", 3), text_event("$held", 4)],
                "p1",
            ),
        )
        await restarted.receive_response(
            sync_response(
                "s3",
                {
                    ROOM_A: room_info(
                        [text_event("$later", 5)], limited=False, prev_batch="p2"
                    )
                },
            )
        )
        assert restarted_seen == ["$later", "$gap", "$gap2"]
        assert room_surface_seen == [
            "TypingNoticeEvent",
            "FullyReadEvent",
            "UnknownBadEvent",
        ]
        assert presence_seen == ["@sender:example.org"]
        assert response_seen == ["s2"]
        assert not restarted._recovery.gaps
        await restarted.close()

    async def test_restart_first_limited_sync_uses_loaded_transport(
        self, tempdir, aioresponse
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        first.store.save_sync_token("s1")
        await first.close()
        first.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(restarted)
        pages = Pages(
            {
                "s1": messages(
                    [text_event("$gap", 2), text_event("$held", 3)],
                    "p1",
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages)
        await restarted.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert pages.from_tokens == ["s1"]
        assert seen == ["$held", "$gap"]
        assert not restarted._recovery.gaps
        await restarted.close()

    async def test_explicit_since_bounds_first_recovery(self, client, aioresponse):
        seen = record_events(client)
        pages = Pages(
            {
                "explicit": messages(
                    [text_event("$gap", 2), text_event("$held", 3)],
                    "p1",
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages)
        aioresponse.get(
            SYNC_URL,
            payload=sync_json(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            ),
        )
        response = await client.sync(since="explicit")
        assert isinstance(response, SyncResponse)
        assert pages.from_tokens == ["explicit"]
        assert seen == ["$held", "$gap"]

    async def test_full_state_join_does_not_cancel_timeline_recovery(
        self, client, aioresponse
    ):
        seen = record_events(client)
        client.next_batch = "s1"
        own_join = member_event("$old-join", 1, "join")
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 2), text_event("$held", 3)],
                "p1",
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)],
                        limited=True,
                        prev_batch="p1",
                        state=[own_join],
                    )
                },
            )
        )
        assert seen == ["$held", "$gap"]
        assert not client._recovery.gaps

    async def test_recovered_state_does_not_regress_live_room_state(
        self, client, aioresponse
    ):
        names: list[str] = []

        async def record_name(_room, event):
            names.append(event.name)

        client.add_event_callback(record_name, RoomNameEvent)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [name_event("$before", 1, "Before")],
                        limited=False,
                        prev_batch="p0",
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [
                    name_event("$gap-name", 2, "Gap"),
                    name_event("$after", 3, "After"),
                    text_event("$held", 4),
                ],
                "p1",
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [name_event("$after", 3, "After"), text_event("$held", 4)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert names == ["Before", "After", "Gap"]
        assert client.rooms[ROOM_A].name == "After"

    async def test_live_callbacks_see_each_events_room_state(self, client):
        seen = []

        async def record_name(room, event):
            seen.append((event.name, room.name))

        client.add_event_callback(record_name, RoomNameEvent)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [
                            name_event("$first", 1, "First"),
                            name_event("$second", 2, "Second"),
                        ],
                        limited=False,
                        prev_batch="p0",
                    )
                },
            )
        )
        assert seen == [("First", "First"), ("Second", "Second")]

    async def test_same_token_response_pumps_pending_room(self, client, aioresponse):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "more"),
        )
        limited = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 4)], limited=True, prev_batch="p1"
                )
            },
        )
        await client.receive_response(limited)
        assert seen == ["$old", "$held"]

        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap2", 3), text_event("$held", 4)],
                "p1",
            ),
        )
        await client.receive_response(limited)
        assert seen == ["$old", "$held", "$gap", "$gap2"]
        assert not client._recovery.gaps

    async def test_recovery_attempts_all_callbacks_once(self, client, aioresponse):
        calls: list[str] = []

        async def first(_room, event):
            if event.event_id == "$gap":
                calls.append("first")

        async def failing(_room, event):
            if event.event_id == "$gap":
                calls.append("failing")
                raise RuntimeError("callback failed")

        async def last(_room, event):
            if event.event_id == "$gap":
                calls.append("last")

        for callback in (first, failing, last):
            client.add_event_callback(callback, RoomMessageText)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 2), text_event("$held", 3)],
                "p1",
            ),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert calls == ["first", "failing", "last"]
        assert not client._recovery.gaps

    async def test_callback_fanout_failure_is_terminal_across_restart(
        self, tempdir, aioresponse
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        first.next_batch = "s1"
        calls: list[str] = []

        async def before(_room, event):
            calls.append(f"before:{event.event_id}")

        async def fail(_room, event):
            calls.append(f"fail:{event.event_id}")
            if event.event_id == "$gap":
                raise RuntimeError("callback failed")

        async def after(_room, event):
            calls.append(f"after:{event.event_id}")

        for callback in (before, fail, after):
            first.add_event_callback(callback, RoomMessageText)
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 2), text_event("$held", 3)],
                "p1",
            ),
        )
        await first.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert calls == [
            "before:$held",
            "fail:$held",
            "after:$held",
            "before:$gap",
            "fail:$gap",
            "after:$gap",
        ]
        assert not first._recovery.gaps
        await first.close()
        first.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        assert not restarted._recovery.gaps
        assert calls == [
            "before:$held",
            "fail:$held",
            "after:$held",
            "before:$gap",
            "fail:$gap",
            "after:$gap",
        ]
        await restarted.close()

    async def test_classic_recovery_deduplicates_sliding_replay_after_restart(
        self, tempdir, aioresponse
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        first.next_batch = "s1"
        seen = record_events(first)
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 2), text_event("$held", 3)],
                "p1",
            ),
        )
        await first.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$held", "$gap"]
        await first.close()
        first.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))

        async def record(_room, event):
            seen.append(event.event_id)

        restarted.add_event_callback(record, RoomMessageText)
        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [
                            text_event("$gap", 2).source,
                            text_event("$held", 3).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await restarted.receive_response(sliding)
        assert seen == ["$held", "$gap"]
        await restarted.close()

    async def test_sliding_window_token_survives_a_restart(self, tempdir, aioresponse):
        """A restarted client walks the gap its downtime left behind.

        The walk baseline is the token the room's last window carried, so
        persisting it is what lets the first limited window after a restart
        be recovered instead of dropped.
        """
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        await first.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        assert first.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}
        await first.close()

        # A fresh process against the same store: nothing in memory, the
        # baseline comes back from disk.
        second = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await second.receive_response(LoginResponse.from_dict(LOGIN))
        assert second._sliding_room_prev_batch == {ROOM_A: window_token("w1")}

        seen = record_events(second)
        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        await second.receive_response(
            self._sliding("s2", [text_event("$held", 3)], limited=True, prev_batch="w2")
        )

        assert seen == ["$held", "$gap"]
        assert pages.from_tokens == ["w1"]
        assert not second._recovery.gaps
        await second.close()

    @pytest.mark.parametrize(
        ("membership_event_id", "expected_token"),
        [(None, None), ("$new-membership", "w2")],
    )
    async def test_restart_token_requires_current_membership_identity(
        self,
        tempdir,
        aioresponse,
        membership_event_id,
        expected_token,
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        await first.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        await first.close()

        second = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await second.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(second)
        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await second.receive_response(
            self._sliding(
                "s2",
                [text_event("$held", 3)],
                limited=True,
                prev_batch="w2",
                membership_event_id=membership_event_id,
            )
        )

        assert seen == ["$held"]
        assert pages.from_tokens == []
        stored = second.store.load_sliding_window_tokens().get(ROOM_A)
        assert (stored.token if stored else None) == expected_token
        assert (stored.membership_event_id if stored else None) == membership_event_id
        await second.close()

    async def test_sliding_sync_requests_own_membership_without_mutating_inputs(
        self, tempdir, monkeypatch
    ):
        config = AsyncClientConfig(backfill_limited_timelines=True)
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        lists = {
            "main": {
                "ranges": [[0, 9]],
                "required_state": [["m.room.create", ""]],
            }
        }
        subscriptions = {
            ROOM_A: {
                "required_state": [["m.room.name", ""]],
            }
        }
        sent: list[dict] = []

        async def send(*_args, **kwargs):
            sent.append(json.loads(_args[3]))
            return SlidingSyncResponse.from_dict({"pos": "s1", "rooms": {}})

        monkeypatch.setattr(client, "_send", send)

        await client.sliding_sync(
            lists=lists,
            room_subscriptions=subscriptions,
        )

        own_membership = ["m.room.member", "$ME"]
        assert sent[0]["lists"]["main"]["required_state"] == [
            ["m.room.create", ""],
            own_membership,
        ]
        assert sent[0]["room_subscriptions"][ROOM_A]["required_state"] == [
            ["m.room.name", ""],
            own_membership,
        ]
        assert lists["main"]["required_state"] == [["m.room.create", ""]]
        assert subscriptions[ROOM_A]["required_state"] == [["m.room.name", ""]]
        await client.close()

    async def test_forgetting_a_room_drops_its_window_token(self, tempdir):
        """A stale baseline must not outlive the membership it was taken under."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}

        await client.receive_response(RoomForgetResponse.from_dict({}, ROOM_A))

        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    async def test_classic_leave_clears_the_sliding_window_token(self, tempdir):
        """A departure seen on /v3/sync invalidates the sliding baseline too."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}

        left = sync_response("s2", {})
        left.rooms.leave[ROOM_A] = RoomInfo(Timeline([], False, None), [], [], [])
        await client.receive_response(left)

        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    async def test_token_is_not_adopted_when_the_write_fails(self, tempdir):
        """A rolled back write must not leave memory past the stored gap."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$one", 1)], prev_batch="w1")
        )

        def fail(*args, **kwargs):
            raise RuntimeError("store is full")

        client.store.save_recovery = fail
        with pytest.raises(RuntimeError):
            await client.receive_response(
                self._sliding("s2", [text_event("$two", 2)], prev_batch="w2")
            )

        # The advanced token was never committed, so the next walk still
        # starts from the last baseline that was.
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w1")}
        await client.close()

    async def test_response_crossing_a_leave_does_not_restore_the_token(self, tempdir):
        """A join response older than the leave must not revive the baseline."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        client._forget_sliding_window_token(ROOM_A)

        # In flight before the leave, handled after it.
        await client.receive_response(
            self._sliding("s2", [text_event("$stale", 2)], prev_batch="w2")
        )
        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sliding_window_tokens() == {}

        # A fresh snapshot belongs to the membership held now, so it counts.
        await client.receive_response(
            self._sliding(
                "s3", [text_event("$rejoined", 3)], prev_batch="w3", initial=True
            )
        )
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w3")}
        await client.close()

    async def test_sync_token_is_stored_without_recovery_persistence(self, tempdir):
        """Turning recovery rows off must not stop the sync token being saved."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
            backfill_persist_recovery=False,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(sync_response("s1", {}))

        assert client.store.load_sync_token() == "s1"
        await client.close()

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_membership_reset_deletes_a_token_stored_by_an_earlier_run(
        self, tempdir, protocol
    ):
        """A token on disk outlives the run that wrote it, so deletion must too."""
        persisting = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=persisting
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        await first.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        assert first.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}
        await first.close()

        # This run refuses to write recovery rows, but the stale baseline
        # from the previous one still has to go.
        not_persisting = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
            backfill_persist_recovery=False,
        )
        second = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=not_persisting
        )
        await second.receive_response(LoginResponse.from_dict(LOGIN))
        if protocol == "classic":
            response = sync_response("s2", {})
            response.rooms.leave[ROOM_A] = RoomInfo(
                Timeline([], False, None), [], [], []
            )
        else:
            response = self._sliding("s2", [], membership="leave")
        await second.receive_response(response)

        assert second.store.load_sliding_window_tokens() == {}
        await second.close()

    async def test_initial_response_older_than_the_reset_is_rejected(self, tempdir):
        """`initial` says snapshot, not that the snapshot postdates the leave."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))

        # A request issued now, whose response arrives after a leave.
        stale_epoch = client._sliding_reset_epoch
        client._forget_sliding_window_token(ROOM_A)

        token = client._sliding_request_epoch.set(stale_epoch)
        try:
            await client.receive_response(
                self._sliding(
                    "s2", [text_event("$stale", 2)], prev_batch="w2", initial=True
                )
            )
        finally:
            client._sliding_request_epoch.reset(token)
        assert client._sliding_room_prev_batch == {}

        # A request issued after the reset carries a usable token.
        token = client._sliding_request_epoch.set(client._sliding_reset_epoch)
        try:
            await client.receive_response(
                self._sliding(
                    "s3", [text_event("$fresh", 3)], prev_batch="w3", initial=True
                )
            )
        finally:
            client._sliding_request_epoch.reset(token)
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w3")}
        await client.close()

    async def test_rejoin_in_the_timeline_discards_the_restored_token(
        self, tempdir, monkeypatch
    ):
        """A join inside the timeline ends the membership the token described."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}

        seen = record_events(client)
        rejoin = SlidingSyncResponse.from_dict(
            {
                "pos": "s2",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "initial": True,
                        "limited": True,
                        "prev_batch": "w2",
                        "timeline": [
                            member_event("$join", 2, "join", OWN_ID).source,
                            text_event("$after", 3).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(rejoin, SlidingSyncResponse)

        async def send(*_args, **_kwargs):
            await client.receive_response(rejoin)
            return rejoin

        monkeypatch.setattr(client, "_send", send)
        await client.sliding_sync()

        # No walk from the pre-departure token; the snapshot's own token
        # becomes the baseline for the membership that exists now.
        assert seen == ["$after"]
        assert not client._recovery.gaps
        assert client.store.load_sliding_window_tokens() == {
            ROOM_A: window_token("w2", "$join")
        }
        await client.close()

    async def test_cancelled_rejoin_planning_keeps_the_committed_baseline(
        self, tempdir, monkeypatch
    ):
        """A cancelled response must not mutate state before its plan commits."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        rejoin = SlidingSyncResponse.from_dict(
            {
                "pos": "s2",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "initial": True,
                        "limited": True,
                        "prev_batch": "w2",
                        "timeline": [
                            member_event("$join", 2, "join", OWN_ID).source,
                            text_event("$after", 3).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(rejoin, SlidingSyncResponse)

        async def send(*_args, **_kwargs):
            await client.receive_response(rejoin)
            return rejoin

        def cancel_planning(*_args, **_kwargs):
            raise asyncio.CancelledError

        monkeypatch.setattr(client, "_send", send)
        monkeypatch.setattr(async_client_module, "plan_room_timeline", cancel_planning)

        with pytest.raises(asyncio.CancelledError):
            await client.sliding_sync()

        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w1")}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}
        await client.close()

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_membership_change_waits_for_durable_token_invalidation(
        self, tempdir, monkeypatch, operation
    ):
        """A remote departure must not happen while its old baseline survives."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        sent = []

        async def send(response_class, *_args, **_kwargs):
            sent.append(response_class)
            if response_class is RoomForgetResponse:
                response = RoomForgetResponse.from_dict({}, ROOM_A)
                await client.receive_response(response)
                return response
            return RoomLeaveResponse.from_dict({})

        def fail(_room_id):
            raise RuntimeError("store unavailable")

        monkeypatch.setattr(client, "_send", send)
        monkeypatch.setattr(client.store, "forget_sliding_window_token", fail)

        with pytest.raises(RuntimeError, match="store unavailable"):
            await getattr(client, operation)(ROOM_A)

        assert sent == []
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w1")}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}
        await client.close()

    async def test_restart_snapshot_still_walks_from_the_stored_token(
        self, tempdir, aioresponse
    ):
        """required_state carries our membership on every snapshot.

        A restart re-enters every room `initial` with our own member event
        in required_state. Reading that as a fresh membership would drop
        the baseline and skip the walk the restart exists to perform.
        """
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        await first.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        await first.close()

        second = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await second.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(second)
        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        snapshot = SlidingSyncResponse.from_dict(
            {
                "pos": "s2",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "initial": True,
                        "limited": True,
                        "prev_batch": "w2",
                        "required_state": [
                            member_event("$membership", 0, "join", OWN_ID).source
                        ],
                        "timeline": [text_event("$held", 3).source],
                    }
                },
            }
        )
        assert isinstance(snapshot, SlidingSyncResponse)
        await second.receive_response(snapshot)

        assert seen == ["$held", "$gap"]
        assert pages.from_tokens == ["w1"]
        await second.close()

    async def test_window_token_is_written_with_its_plan(self, tempdir):
        """The baseline and the plan it belongs to share one transaction.

        Storing either alone would send a restarted walk from a position
        that does not match the pending rows it finds.
        """
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))

        calls: list[str] = []
        original = client.store.save_recovery
        seen_tokens: list[dict] = []

        def record(*args, **kwargs):
            calls.append("save_recovery")
            seen_tokens.append(
                args[5] if len(args) > 5 else kwargs.get("window_tokens")
            )
            return original(*args, **kwargs)

        client.store.save_recovery = record
        client.store.save_sliding_window_tokens = lambda *a, **k: calls.append(
            "save_sliding_window_tokens"
        )

        await client.receive_response(
            self._sliding("s1", [text_event("$one", 1)], prev_batch="w1")
        )

        assert calls == ["save_recovery"]
        assert seen_tokens == [{ROOM_A: window_token("w1")}]
        await client.close()

    async def test_left_room_drops_the_persisted_window_token(self, tempdir):
        """Leaving clears the stored baseline, not just the in-memory one."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        await client.receive_response(
            self._sliding("s2", [], membership="leave", prev_batch="w2")
        )
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    async def test_persist_recovery_without_owning_the_sync_token(self, tempdir):
        """Recovery state can be durable while the caller owns next_batch."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=False,
            backfill_persist_recovery=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )

        # The window token is durable...
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}
        # ...while nio has recorded no sync token of its own.
        assert client.store.load_sync_token() is None
        await client.close()

    async def test_sliding_replay_deduplicates_classic_recovery_after_restart(
        self, tempdir, aioresponse
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        await first.receive_response(sync_response("s1", {}))
        seen = record_events(first)
        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [text_event("$shared", 2).source],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await first.receive_response(sliding)
        assert seen == ["$shared"]
        await first.close()
        first.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))

        async def record(_room, event):
            seen.append(event.event_id)

        restarted.add_event_callback(record, RoomMessageText)
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 1), text_event("$shared", 2)],
                "p1",
            ),
        )
        await restarted.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$shared", 2)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert seen == ["$shared", "$gap"]
        await restarted.close()

    async def test_sliding_encrypted_restart_allows_classic_plaintext_upgrade(
        self, tempdir, aioresponse
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        await first.receive_response(sync_response("s1", {}))
        seen: list[type[Event]] = []

        async def record_first(_room, event):
            seen.append(type(event))

        first.add_event_callback(record_first, (MegolmEvent, RoomMessageText))
        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [megolm_event("$shared", 2).source],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await first.receive_response(sliding)
        assert seen == [MegolmEvent]
        await first.close()
        first.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))

        async def record_restarted(_room, event):
            seen.append(type(event))

        restarted.add_event_callback(
            record_restarted,
            (MegolmEvent, RoomMessageText),
        )
        clear = text_event("$shared", 2)
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([clear], "p1"),
        )
        await restarted.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [clear],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert seen == [MegolmEvent, RoomMessageText]
        await restarted.close()

    async def test_recovered_encrypted_event_upgrades_after_keys_arrive(
        self, client, aioresponse, monkeypatch
    ):
        encrypted = megolm_event("$shared", 2)
        clear = text_event("$shared", 2)
        record_completed_timeline_event(client._recovery, ROOM_A, "$shared", True)
        assert client.olm
        monkeypatch.setattr(
            client.olm,
            "_decrypt_megolm_no_error",
            lambda event: clear if event.event_id == "$shared" else None,
        )
        seen = record_events(client)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([encrypted, text_event("$held", 3)], "p1"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert seen == ["$held", "$shared"]
        assert client._recovery.completed[ROOM_A]["$shared"] is False

    async def test_encrypted_upgrade_needs_no_post_decrypt_commit(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                store_sync_tokens=True,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        encrypted = megolm_event("$shared", 1)
        clear = text_event("$shared", 1)
        seen = []

        async def record(_room, event):
            seen.append(event.event_id)

        client.add_event_callback(record, (MegolmEvent, RoomMessageText))
        await client.receive_response(
            sync_response(
                "s1",
                {ROOM_A: room_info([encrypted], limited=False, prev_batch="p0")},
            )
        )
        assert seen == ["$shared"]
        assert client.olm
        monkeypatch.setattr(
            client.olm, "_decrypt_megolm_no_error", lambda _event: clear
        )
        original_save = client.store.save_recovery
        saves = 0

        def fail_second_save(*args):
            nonlocal saves
            saves += 1
            if saves == 2:
                raise RuntimeError("post-decrypt commit failed")
            original_save(*args)

        monkeypatch.setattr(client.store, "save_recovery", fail_second_save)
        await client.receive_response(
            sync_response(
                "s2",
                {ROOM_A: room_info([encrypted], limited=False, prev_batch="p1")},
            )
        )
        assert saves == 1
        assert seen == ["$shared", "$shared"]
        assert client._recovery.completed[ROOM_A]["$shared"] is False
        await client.close()

    async def test_restart_replays_only_active_unacknowledged_row(
        self, tempdir, aioresponse, monkeypatch
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(LOGIN))
        first.next_batch = "s1"
        seen = record_events(first)

        original_finish = first.store.finish_recovery
        acknowledgements = 0

        def fail_third_ack(room_id, generation, event_id, was_encrypted, boundary=None):
            nonlocal acknowledgements
            if event_id:
                acknowledgements += 1
            if acknowledgements == 3:
                raise RuntimeError("ack failed")
            original_finish(
                room_id,
                generation,
                event_id,
                was_encrypted,
                boundary=boundary,
            )

        monkeypatch.setattr(first.store, "finish_recovery", fail_third_ack)
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [
                    text_event("$gap1", 2),
                    text_event("$gap2", 3),
                    text_event("$held", 4),
                ],
                "p1",
            ),
        )
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 4)],
                    limited=True,
                    prev_batch="p1",
                )
            },
        )
        with pytest.raises(RuntimeError, match="ack failed"):
            await first.receive_response(response)
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        assert seen == ["$held", "$gap1", "$gap2"]
        await first.close()
        first.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))

        async def record(_room, event):
            seen.append(event.event_id)

        restarted.add_event_callback(record, RoomMessageText)
        await restarted.receive_response(sync_response("s2", {}))
        assert seen == ["$held", "$gap1", "$gap2", "$gap2"]
        assert not restarted._recovery.gaps
        await restarted.close()

    async def test_sliding_timeline_joins_classic_recovery_lane(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "more"),
        )
        limited = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 4)], limited=True, prev_batch="p1"
                )
            },
        )
        await client.receive_response(limited)

        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [text_event("$slide", 5).source],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await client.receive_response(sliding)
        assert seen == ["$old", "$held", "$slide"]

        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap2", 3), text_event("$held", 4)],
                "p1",
            ),
        )
        await client.receive_response(limited)
        assert seen == ["$old", "$held", "$slide", "$gap", "$gap2"]
        assert not client._recovery.gaps

    @staticmethod
    def _sliding(
        pos: str,
        events: list,
        *,
        limited: bool = False,
        prev_batch: str | None = None,
        initial: bool = False,
        membership: str = "join",
        membership_event_id: str | None = "$membership",
    ) -> SlidingSyncResponse:
        room: dict = {
            "membership": membership,
            "timeline": [event.source for event in events],
            "limited": limited,
        }
        if membership_event_id is not None:
            room["required_state"] = [
                member_event(membership_event_id, 0, membership).source
            ]
        if prev_batch is not None:
            room["prev_batch"] = prev_batch
        if initial:
            room["initial"] = True
        response = SlidingSyncResponse.from_dict({"pos": pos, "rooms": {ROOM_A: room}})
        assert isinstance(response, SlidingSyncResponse)
        return response

    async def test_limited_sliding_window_walks_between_prev_batches(
        self, client, aioresponse
    ):
        """Consecutive window tokens bound a forward walk over the gap.

        A sliding `pos` is not a /messages token, but `prev_batch` is, so
        the walk runs from the previous window's token to this one's.
        """
        seen = record_events(client)
        await client.receive_response(
            self._sliding("s1", [text_event("$first", 1)], prev_batch="w1")
        )
        assert seen == ["$first"]

        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        seen_at_fetch: list[list[str]] = []

        def fetch_page(url, **kwargs):
            seen_at_fetch.append(seen.copy())
            return pages(url, **kwargs)

        aioresponse.get(MESSAGES_URL, callback=fetch_page, repeat=True)
        await client.receive_response(
            self._sliding("s2", [text_event("$held", 3)], limited=True, prev_batch="w2")
        )

        assert seen_at_fetch == [["$first", "$held"]]
        assert seen == ["$first", "$held", "$gap"]
        assert pages.from_tokens == ["w1"]
        assert pages.to_tokens == ["w2"]
        assert not client._recovery.gaps

    async def test_first_sliding_window_plans_no_walk(self, client, aioresponse):
        """Without a previous window there is no token to walk from."""
        seen = record_events(client)
        await client.receive_response(
            self._sliding("s1", [text_event("$held", 1)], limited=True, prev_batch="w1")
        )
        assert seen == ["$held"]
        assert not client._recovery.gaps

    async def test_initial_sliding_room_walks_from_the_held_token(
        self, client, aioresponse
    ):
        """A snapshot for a known room is a discontinuity like `limited`.

        A room re-entering a list window, or arriving on a connection the
        server expired, comes back flagged `initial`; the events since the
        held token are just as gone as after a limited window.
        """
        seen = record_events(client)
        await client.receive_response(
            self._sliding("s1", [text_event("$first", 1)], prev_batch="w1")
        )
        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        await client.receive_response(
            self._sliding(
                "s2",
                [text_event("$held", 3)],
                prev_batch="w2",
                initial=True,
            )
        )
        assert seen == ["$first", "$held", "$gap"]
        assert pages.from_tokens == ["w1"]
        assert not client._recovery.gaps

    async def test_initial_sliding_room_without_baseline_plans_no_walk(
        self, client, aioresponse
    ):
        """A room seen for the first time has no token to walk from."""
        seen = record_events(client)
        await client.receive_response(
            self._sliding(
                "s1",
                [text_event("$held", 1)],
                prev_batch="w1",
                initial=True,
            )
        )
        assert seen == ["$held"]
        assert not client._recovery.gaps

    async def test_left_room_drops_the_sliding_walk_token(self, client, aioresponse):
        """A stale token must not make a rejoin walk pre-departure history."""
        seen = record_events(client)
        await client.receive_response(
            self._sliding("s1", [text_event("$first", 1)], prev_batch="w1")
        )
        await client.receive_response(
            self._sliding("s2", [], membership="leave", prev_batch="w2")
        )
        await client.receive_response(
            self._sliding("s3", [text_event("$held", 3)], limited=True, prev_batch="w3")
        )
        assert seen == ["$first", "$held"]
        assert not client._recovery.gaps

    async def test_sliding_own_join_resets_classic_history_not_current_timeline(
        self, client, aioresponse
    ):
        seen = record_events(client)
        client.add_event_callback(
            lambda _room, event: seen.append(event.event_id), RoomMemberEvent
        )
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$prejoin", 1)], "more"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 4)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$held"]
        assert client._recovery.gaps

        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [
                            text_event("$prejoin2", 1).source,
                            member_event("$join", 2, "join").source,
                            text_event("$after", 3).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await client.receive_response(sliding)
        assert seen == ["$held", "$prejoin2", "$join", "$after"]
        assert not client._recovery.gaps
        assert sliding.recovered_room_ids == frozenset()
        assert sliding.unrecovered_room_ids == frozenset({ROOM_A})

    async def test_sliding_initial_historical_join_keeps_classic_gap(
        self, client, aioresponse
    ):
        seen = record_events(client)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 1)], "more"),
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 1)], "more2"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 4)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "initial": True,
                        "membership": "join",
                        "num_live": 1,
                        "timeline": [
                            member_event("$join", 2, "join").source,
                            text_event("$after", 3).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await client.receive_response(sliding)
        assert seen == ["$held", "$after"]
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "more2"
        assert [
            event.event_id
            for event in client._recovery.events[(ROOM_A, 1)]
            if event.kind != "boundary"
        ] == ["$gap"]

    @pytest.mark.parametrize("membership", ["leave", "ban", "invite"])
    async def test_sliding_membership_reset_clears_classic_recovery_durably(
        self, tempdir, aioresponse, membership
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_max_pages=1,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$seen", 0)],
                        limited=False,
                        prev_batch="p0",
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 1)], "more"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 2)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert client._recovery.gaps
        assert client.store.load_sync_recovery()[0]

        reset = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {ROOM_A: {"membership": membership, "timeline": []}},
            }
        )
        assert isinstance(reset, SlidingSyncResponse)
        await client.receive_response(reset)
        assert not client._recovery.gaps
        assert seen == ["$seen", "$held"]
        assert reset.recovered_room_ids == frozenset()
        assert reset.unrecovered_room_ids == frozenset({ROOM_A})
        gaps, events = client.store.load_sync_recovery()
        assert gaps == []
        assert list(client._recovery.completed[ROOM_A]) == ["$seen", "$held"]
        assert [(event.event_id, event.generation) for event in events] == [
            ("$seen", 0),
            ("$held", 0),
        ]
        await client.close()
        client.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        assert not restarted._recovery.gaps
        assert list(restarted._recovery.completed[ROOM_A]) == ["$seen", "$held"]
        await restarted.close()

    @pytest.mark.parametrize("membership", ["leave", "invite"])
    async def test_classic_membership_reset_clears_recovery_durably(
        self, tempdir, aioresponse, membership
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_max_pages=1,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$seen", 0)],
                        limited=False,
                        prev_batch="p0",
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 1)], "more"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 2)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert client._recovery.gaps
        assert client.store.load_sync_recovery()[0]

        reset = sync_response(
            "s3",
            {},
            invited=({ROOM_A: InviteInfo([])} if membership == "invite" else None),
            left=(
                {ROOM_A: room_info([], limited=False, prev_batch=None)}
                if membership == "leave"
                else None
            ),
        )
        await client.receive_response(reset)
        assert not client._recovery.gaps
        assert seen == ["$seen", "$held"]
        assert reset.recovered_room_ids == frozenset()
        assert reset.unrecovered_room_ids == frozenset({ROOM_A})
        gaps, events = client.store.load_sync_recovery()
        assert gaps == []
        assert list(client._recovery.completed[ROOM_A]) == ["$seen", "$held"]
        assert [(event.event_id, event.generation) for event in events] == [
            ("$seen", 0),
            ("$held", 0),
        ]
        await client.close()

    @pytest.mark.parametrize("sliding", [False, True])
    async def test_invite_state_waits_for_preserved_live(
        self, client, aioresponse, sliding
    ):
        seen = []

        async def record(_room, event):
            seen.append(
                event.event_id if isinstance(event, RoomMessageText) else "invite"
            )

        client.add_event_callback(record, (RoomMessageText, InviteNameEvent))
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "more"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        invite = InviteNameEvent.from_dict(
            {
                "content": {"name": "Invite"},
                "sender": "@sender:example.org",
                "state_key": "",
                "type": "m.room.name",
            }
        )
        assert isinstance(invite, InviteNameEvent)
        if sliding:
            reset = SlidingSyncResponse.from_dict(
                {
                    "pos": "slide1",
                    "rooms": {
                        ROOM_A: {
                            "membership": "invite",
                            "stripped_state": [invite.source],
                            "timeline": [],
                        }
                    },
                }
            )
            assert isinstance(reset, SlidingSyncResponse)
        else:
            reset = sync_response("s3", {}, invited={ROOM_A: InviteInfo([invite])})
        await client.receive_response(reset)
        assert seen == ["$old", "$held", "invite"]

    async def test_sliding_live_callback_failure_propagates(self, client):
        calls: list[str] = []

        async def first(_room, event):
            calls.append(f"first:{event.event_id}")

        async def failing(_room, event):
            calls.append(f"failing:{event.event_id}")
            if event.event_id == "$b":
                raise RuntimeError("callback failed")

        async def last(_room, event):
            calls.append(f"last:{event.event_id}")

        for callback in (first, failing, last):
            client.add_event_callback(callback, RoomMessageText)
        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [
                            text_event("$a", 1).source,
                            text_event("$b", 2).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        with pytest.raises(RuntimeError, match="callback failed"):
            await client.receive_response(sliding)
        assert calls == [
            "first:$a",
            "failing:$a",
            "last:$a",
            "first:$b",
            "failing:$b",
        ]
        assert all(
            event.kind == "boundary" for event in client._recovery.events[(ROOM_A, 1)]
        )
        assert "$b" in client._recovery.completed[ROOM_A]

    async def test_sliding_sync_dedup_stays_bounded(self, client):
        seen = record_events(client)
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$shared", 1)],
                        limited=False,
                        prev_batch="p0",
                    )
                },
            )
        )
        unique = [text_event(f"$slide-{index}", index + 2) for index in range(520)]
        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [
                            text_event("$shared", 1).source,
                            *(event.source for event in unique),
                        ],
                    }
                },
            }
        )
        assert isinstance(sliding, SlidingSyncResponse)
        await client.receive_response(sliding)
        assert seen == ["$shared", *(event.event_id for event in unique)]
        assert len(client._recovery.completed[ROOM_A]) == 512

    async def test_plaintext_overlap_state_wins_over_later_encrypted_copy(self, client):
        record_completed_timeline_event(client._recovery, ROOM_A, "$same", False)
        record_completed_timeline_event(client._recovery, ROOM_A, "$same", True)
        assert not should_dispatch_timeline_event(
            client._recovery, ROOM_A, text_event("$same", 1)
        )

    async def test_failed_plan_commit_restores_transport_cursor(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                store_sync_tokens=True,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$old", 1)], limited=False, prev_batch="p0"
                    )
                },
            )
        )

        def fail(*args, **kwargs):
            raise RuntimeError("commit failed")

        handled = []
        original_handle = client._handle_to_device

        async def handle(response):
            handled.append(response)
            await original_handle(response)

        monkeypatch.setattr(client, "_handle_to_device", handle)
        monkeypatch.setattr(client.store, "save_recovery", fail)
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 3)], limited=True, prev_batch="p1"
                )
            },
        )
        with pytest.raises(RuntimeError, match="commit failed"):
            await client.receive_response(response)
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        assert client.next_batch == "s1"
        assert client.store.load_sync_token() == "s1"
        assert not client._recovery.gaps
        assert handled == []
        await client.close()

    async def test_failed_sliding_plan_commit_keeps_to_device_cursor(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                store_sync_tokens=True,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        client._sliding_sync_to_device_since = "td0"
        handled = []

        async def handle(response):
            handled.append(response)

        monkeypatch.setattr(client, "_handle_to_device", handle)

        def fail(*_args):
            raise RuntimeError("commit failed")

        monkeypatch.setattr(client.store, "save_recovery", fail)
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [text_event("$held", 1).source],
                    }
                },
                "extensions": {
                    "to_device": {"next_batch": "td1", "events": []},
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)
        with pytest.raises(RuntimeError, match="commit failed"):
            await client.receive_response(response)
        assert client._sliding_sync_to_device_since == "td0"
        assert handled == []
        assert not client._recovery.gaps
        await client.close()

    async def test_sliding_timeline_retries_decryption_after_to_device(
        self, client, monkeypatch
    ):
        ready = False
        attempts = []

        async def process(_room_id, _room, timeline, _encrypted_rooms, **_kwargs):
            attempts.append(ready)
            if ready:
                timeline[:] = [text_event("$decrypted", 1)]

        async def handle(_response):
            nonlocal ready
            ready = True

        monkeypatch.setattr(client, "_process_timeline", process)
        monkeypatch.setattr(client, "_handle_to_device", handle)
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "slide1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [text_event("$encrypted", 1).source],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)
        await client.receive_response(response)
        assert attempts == [False, True]
        assert response.rooms[ROOM_A].timeline[0].event_id == "$decrypted"

    async def test_target_cursor_closes_without_live_echo(self, client, aioresponse):
        seen = record_events(client)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$gap", 2)], "p1"),
        )
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$held", 3)], limited=True, prev_batch="p1"
                    )
                },
            )
        )
        assert seen == ["$held", "$gap"]
        assert not client._recovery.gaps

    @pytest.mark.parametrize("limited", [False, True])
    async def test_current_timeline_preserves_events_before_own_join(
        self, client, limited
    ):
        seen = record_events(client)
        client.add_event_callback(
            lambda _room, event: seen.append(event.event_id), RoomMemberEvent
        )
        client.next_batch = "s1"
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [
                            text_event("$prejoin", 1),
                            member_event("$join", 2, "join"),
                            text_event("$after", 3),
                        ],
                        limited=limited,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert seen == ["$prejoin", "$join", "$after"]
        assert not client._recovery.gaps


@pytest.mark.asyncio
class TestRecoveryOutcome:
    async def test_sync_response_types_default_to_no_recovery(self, client):
        response = sync_response("s1", {})
        sliding = SlidingSyncResponse.from_dict({"pos": "p1"})
        assert isinstance(sliding, SlidingSyncResponse)

        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset()
        assert sliding.recovered_room_ids == frozenset()
        assert sliding.unrecovered_room_ids == frozenset()

    async def test_restored_token_first_sync_reports_recovered_room(
        self, tempdir, aioresponse
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        seed = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await seed.receive_response(LoginResponse.from_dict(LOGIN))
        await seed.receive_response(sync_response("s1", {}))
        await seed.close()
        seed.store.database.close()

        restored = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restored.receive_response(LoginResponse.from_dict(LOGIN))
        assert restored.loaded_sync_token == "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 1), text_event("$live", 2)],
                "p1",
            ),
        )
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$live", 2)],
                    limited=True,
                    prev_batch="p1",
                )
            },
        )

        await restored.receive_response(response)

        assert response.recovered_room_ids == frozenset({ROOM_A})
        assert response.unrecovered_room_ids == frozenset()
        assert response.rooms.join[ROOM_A].timeline.limited is True
        await restored.close()

    async def test_restored_token_incomplete_recovery_reports_unrecovered(
        self, client, aioresponse
    ):
        client.loaded_sync_token = "s1"
        aioresponse.get(MESSAGES_URL, status=500)
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$live", 2)],
                    limited=True,
                    prev_batch="p1",
                )
            },
        )

        await client.receive_response(response)

        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})

    async def test_cancelled_recovery_preserves_unrecovered_outcome(
        self, client, aioresponse
    ):
        started = asyncio.Event()
        release = asyncio.Event()

        async def block_recovered(_room, event):
            if event.event_id == "$gap":
                started.set()
                await release.wait()

        client.add_event_callback(block_recovered, RoomMessageText)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 1), text_event("$live", 2)],
                "p1",
            ),
        )
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$live", 2)],
                    limited=True,
                    prev_batch="p1",
                )
            },
        )
        task = asyncio.create_task(client.receive_response(response))
        await asyncio.wait_for(started.wait(), 1)

        try:
            task.cancel()

            with pytest.raises(asyncio.CancelledError):
                await task
            assert response.recovered_room_ids == frozenset()
            assert response.unrecovered_room_ids == frozenset({ROOM_A})
        finally:
            release.set()

    async def test_cancelled_before_plan_keeps_response_retryable(
        self, client, aioresponse, monkeypatch
    ):
        started = block_next_recovery_plan(client, monkeypatch)
        client.next_batch = "s1"
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$live", 2)],
                    limited=True,
                    prev_batch="p1",
                )
            },
        )
        task = asyncio.create_task(client.receive_response(response))
        await asyncio.wait_for(started.wait(), 1)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert client.next_batch == "s1"
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})

        aioresponse.get(MESSAGES_URL, status=500)
        await client.receive_response(response)

        assert client.next_batch == "s2"
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "s1"
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})

    async def test_cancelled_sliding_before_plan_reports_unrecovered(
        self, client, aioresponse, monkeypatch
    ):
        baseline = SlidingSyncResponse.from_dict(
            {
                "pos": "s1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [text_event("$old", 1).source],
                        "prev_batch": "w1",
                    }
                },
            }
        )
        assert isinstance(baseline, SlidingSyncResponse)
        await client.receive_response(baseline)

        started = block_next_recovery_plan(client, monkeypatch)
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s2",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [text_event("$live", 2).source],
                        "limited": True,
                        "prev_batch": "w2",
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)
        task = asyncio.create_task(client.receive_response(response))
        await asyncio.wait_for(started.wait(), 1)

        task.cancel()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})

        aioresponse.get(MESSAGES_URL, status=500)
        await client.receive_response(response)

        assert client._recovery.gaps[ROOM_A][0].cursor_token == "w1"
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_cancelled_while_waiting_for_sync_executor_reports_unrecovered(
        self, client, protocol
    ):
        if protocol == "classic":
            client.next_batch = "s1"
            response = sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$live", 2)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        else:
            baseline = SlidingSyncResponse.from_dict(
                {
                    "pos": "s1",
                    "rooms": {
                        ROOM_A: {
                            "membership": "join",
                            "timeline": [text_event("$old", 1).source],
                            "prev_batch": "w1",
                        }
                    },
                }
            )
            assert isinstance(baseline, SlidingSyncResponse)
            await client.receive_response(baseline)
            response = SlidingSyncResponse.from_dict(
                {
                    "pos": "s2",
                    "rooms": {
                        ROOM_A: {
                            "membership": "join",
                            "timeline": [text_event("$live", 2).source],
                            "limited": True,
                            "prev_batch": "w2",
                        }
                    },
                }
            )
            assert isinstance(response, SlidingSyncResponse)

        client._recovery.outcomes[ROOM_B] = True
        await client._sync_response_lock.acquire()
        task = asyncio.create_task(client.receive_response(response))
        await asyncio.sleep(0)
        assert not task.done()

        task.cancel()
        client._sync_response_lock.release()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        assert client._recovery.outcomes == {ROOM_B: True}
        if protocol == "classic":
            assert client.next_batch == "s1"
        else:
            assert client._sliding_room_prev_batch[ROOM_A] == "w1"

    @pytest.mark.parametrize("stage", ["executor", "room"])
    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    @pytest.mark.parametrize("scenario", ["no_token", "own_join"])
    async def test_cancelled_before_plan_ignores_non_gap_transport_hints(
        self, client, monkeypatch, stage, protocol, scenario
    ):
        events = (
            [member_event("$join", 2, "join")]
            if scenario == "own_join"
            else [text_event("$live", 2)]
        )
        if protocol == "classic":
            if scenario == "own_join":
                client.next_batch = "s1"
            response = sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        events,
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        else:
            if scenario == "own_join":
                baseline = SlidingSyncResponse.from_dict(
                    {
                        "pos": "s1",
                        "rooms": {
                            ROOM_A: {
                                "membership": "join",
                                "timeline": [text_event("$old", 1).source],
                                "prev_batch": "w1",
                            }
                        },
                    }
                )
                assert isinstance(baseline, SlidingSyncResponse)
                await client.receive_response(baseline)
            response = SlidingSyncResponse.from_dict(
                {
                    "pos": "s2",
                    "rooms": {
                        ROOM_A: {
                            "membership": "join",
                            "timeline": [event.source for event in events],
                            "limited": True,
                            "prev_batch": "w2",
                        }
                    },
                }
            )
            assert isinstance(response, SlidingSyncResponse)

        if stage == "executor":
            await client._sync_response_lock.acquire()
            started = None
        else:
            started = block_next_recovery_plan(client, monkeypatch)
        task = asyncio.create_task(client.receive_response(response))
        if started:
            await asyncio.wait_for(started.wait(), 1)
        else:
            await asyncio.sleep(0)
            assert not task.done()

        task.cancel()
        if stage == "executor":
            client._sync_response_lock.release()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset()

    async def test_cancelled_queued_duplicate_classic_response_is_not_a_gap(
        self, client
    ):
        client.next_batch = "s2"
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$duplicate", 2)],
                    limited=True,
                    prev_batch="p1",
                )
            },
        )
        await client._sync_response_lock.acquire()
        task = asyncio.create_task(client.receive_response(response))
        await asyncio.sleep(0)
        assert not task.done()

        task.cancel()
        client._sync_response_lock.release()

        with pytest.raises(asyncio.CancelledError):
            await task
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset()
