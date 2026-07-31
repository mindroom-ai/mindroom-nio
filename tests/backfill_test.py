"""Integration tests for durable room-local limited-sync recovery."""

import asyncio
import json
import re
import sys
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
    CallbackNotAcceptedError,
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
    RoomForgetError,
    RoomForgetResponse,
    RoomInfo,
    RoomLeaveError,
    RoomLeaveResponse,
    RoomMemberEvent,
    RoomMessageText,
    RoomNameEvent,
    Rooms,
    SendRetryError,
    SlidingSyncResponse,
    SlidingSyncRoom,
    SlidingSyncStateStub,
    SyncResponse,
    Timeline,
    TimelineEventProvenance,
    ToDeviceEvent,
    TypingNoticeEvent,
    UnknownAccountDataEvent,
    UnknownBadEvent,
    UnknownToDeviceEvent,
)
from nio.api import MATRIX_API_PATH_V3, Api
from nio.client.sync_recovery import (
    PendingTimelineEvent,
    RecoveryGap,
    RecoveryPlan,
    persist_response_plan,
    record_completed_timeline_event,
    should_dispatch_timeline_event,
)
from nio.client.sync_reset_fence import finish_sync_request, issue_sync_request
from nio.responses import RoomMessagesResponse, SlidingSyncError
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
    token: str, membership_event_id: str | None = "$membership"
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
                    "num_live": len(events),
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


def record_admissions(
    client: AsyncClient,
    event_filter: type[Event] | tuple[type[Event], ...] = RoomMessageText,
) -> list[tuple[str, TimelineEventProvenance]]:
    seen: list[tuple[str, TimelineEventProvenance]] = []

    async def callback(_room, event, provenance):
        seen.append((event.event_id, provenance))

    client.add_event_admission_callback(callback, event_filter)
    return seen


def block_next_recovery_plan(client: AsyncClient, monkeypatch) -> asyncio.Event:
    started = asyncio.Event()
    block_once = True
    original = client._recovery_room_state

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
        lambda room_ids: BlockBeforePlan() if block_once else original(room_ids),
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

    async def test_disabled_does_not_drop_out_of_order_sliding_response(self, tempdir):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        seen = record_events(client)
        await client.receive_response(self._sliding("newer", [text_event("$newer", 2)]))
        await client.receive_response(self._sliding("older", [text_event("$older", 1)]))

        assert seen == ["$newer", "$older"]
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

    async def test_backfill_dispatches_before_live_window(self, client, aioresponse):
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
        assert seen_at_fetch == [["$old"]]
        assert seen == ["$old", "$gap", "$live"]
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
        assert seen == ["$old", "$gap", "$live", "$newer"]

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
        assert seen == ["$old"]
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "more"

        await client.receive_response(limited)
        assert pages.from_tokens == ["s1", "more"]
        assert seen == ["$old", "$gap", "$held"]
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
        assert seen == ["$old"]

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
            "$gap",
            "$gap2",
            "$held",
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
        assert seen == ["$old"]
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
        assert seen == ["$old"]
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
        assert [
            (event.kind, event.event_id)
            for event in client._recovery.events[(ROOM_A, 1)]
        ] == [("timeline", "$after")]

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
    async def test_callback_non_acceptance_keeps_event_redispatchable(
        self, client, protocol
    ):
        calls: list[str] = []
        storage_error = OSError("storage unavailable")

        async def admit(_room, event, _provenance):
            calls.append(event.event_id)
            if len(calls) == 1:
                raise CallbackNotAcceptedError(
                    "durable admission failed"
                ) from storage_error

        client.add_event_admission_callback(admit, RoomMessageText)
        response = timeline_response(
            protocol,
            "s1",
            [text_event("$retry", 1)],
        )

        with pytest.raises(
            CallbackNotAcceptedError,
            match="durable admission failed",
        ) as rejected:
            await client.receive_response(response)

        assert rejected.value.__cause__ is storage_error
        assert [
            (event.kind, event.event_id)
            for event in client._recovery.events[(ROOM_A, 1)]
        ] == [("timeline", "$retry")]
        assert "$retry" not in client._recovery.completed.get(ROOM_A, {})

        await client.receive_response(response)

        assert calls == ["$retry", "$retry"]
        assert not client._recovery.gaps

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_admission_marker_failure_keeps_event_redispatchable(
        self, tempdir, monkeypatch, protocol
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
        assert client.store
        original_accept = client.store.accept_recovery_event
        fail_marker = True
        calls: list[str] = []

        def accept_recovery_event(room_id, generation, event_id):
            nonlocal fail_marker
            if fail_marker:
                fail_marker = False
                raise OSError("admission marker unavailable")
            original_accept(room_id, generation, event_id)

        async def admit(_room, event):
            calls.append(f"admit:{event.event_id}")

        async def ordinary(_room, event):
            calls.append(f"ordinary:{event.event_id}")

        monkeypatch.setattr(
            client.store,
            "accept_recovery_event",
            accept_recovery_event,
        )
        client.add_event_admission_callback(admit, RoomMessageText)
        client.add_event_callback(ordinary, RoomMessageText)
        response = timeline_response(protocol, "s1", [text_event("$retry", 1)])

        with pytest.raises(OSError, match="admission marker unavailable"):
            await client.receive_response(response)

        assert calls == ["admit:$retry"]
        assert any(
            pending.event_id == "$retry"
            for pending_events in client._recovery.events.values()
            for pending in pending_events
        )

        await client.receive_response(response)

        assert calls == [
            "admit:$retry",
            "admit:$retry",
            "ordinary:$retry",
        ]
        assert not client._recovery.gaps
        await client.close()

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_untyped_admission_failure_does_not_replay_live_event(
        self, client, protocol
    ):
        calls: list[str] = []

        async def admit(_room, event):
            calls.append(event.event_id)
            raise RuntimeError("unexpected admission failure")

        client.add_event_admission_callback(admit, RoomMessageText)
        response = timeline_response(
            protocol,
            "s1",
            [text_event("$once", 1)],
        )

        with pytest.raises(RuntimeError, match="unexpected admission failure"):
            await client.receive_response(response)

        await client.receive_response(response)

        assert calls == ["$once"]
        assert not client._recovery.gaps

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_callback_admission_precedes_event_fanout(self, client, protocol):
        calls: list[str] = []

        async def first_admission(_room, event, _provenance):
            calls.append(f"admit-first:{event.event_id}")
            if calls == ["admit-first:$retry"]:
                raise CallbackNotAcceptedError("durable admission failed")

        async def first(_room, event):
            calls.append(f"first:{event.event_id}")

        async def second(_room, event):
            calls.append(f"second:{event.event_id}")

        client.add_event_admission_callback(first_admission, RoomMessageText)
        client.add_event_callback(first, RoomMessageText)
        client.add_event_callback(second, RoomMessageText)
        response = timeline_response(
            protocol,
            "s1",
            [text_event("$retry", 1)],
        )

        with pytest.raises(
            CallbackNotAcceptedError,
            match="durable admission failed",
        ):
            await client.receive_response(response)

        assert calls == ["admit-first:$retry"]

        await client.receive_response(response)

        assert calls == [
            "admit-first:$retry",
            "admit-first:$retry",
            "first:$retry",
            "second:$retry",
        ]
        assert not client._recovery.gaps

    async def test_only_one_event_admission_owner_can_be_registered(self, client):
        async def first(_room, _event, _provenance):
            pass

        async def second(_room, _event, _provenance):
            pass

        client.add_event_admission_callback(first, cb_filter=RoomMessageText)

        with pytest.raises(
            LocalProtocolError,
            match="admission callback is already registered",
        ):
            client.add_event_admission_callback(second, RoomMessageText)

    async def test_event_admission_requires_limited_timeline_recovery(self):
        client = AsyncClient("https://example.org", OWN_ID, "DEVICEID")

        with pytest.raises(
            LocalProtocolError,
            match="requires limited-timeline recovery",
        ):
            client.add_event_admission_callback(lambda _room, _event, _provenance: None)

    async def test_classic_initial_timeline_is_history(self, client):
        admissions = record_admissions(client)

        await client.receive_response(
            timeline_response("classic", "s1", [text_event("$history", 1)])
        )

        assert admissions == [
            ("$history", TimelineEventProvenance.HISTORY),
        ]

    async def test_classic_continuation_timeline_is_live(self, client):
        admissions = record_admissions(client)
        client.next_batch = "s0"

        await client.receive_response(
            timeline_response("classic", "s1", [text_event("$live", 1)])
        )

        assert admissions == [
            ("$live", TimelineEventProvenance.LIVE),
        ]

    async def test_recovered_history_precedes_retained_live_provenance(
        self,
        client,
        aioresponse,
    ):
        admissions = record_admissions(client)
        client.next_batch = "s0"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages([text_event("$history", 1)], "p1"),
        )

        await client.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [text_event("$live", 2)],
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )

        assert admissions == [
            ("$history", TimelineEventProvenance.HISTORY),
            ("$live", TimelineEventProvenance.LIVE),
        ]

    async def test_sliding_steady_timeline_is_live(self, client):
        admissions = record_admissions(client)

        await client.receive_response(
            timeline_response("sliding", "s1", [text_event("$live", 1)])
        )

        assert admissions == [
            ("$live", TimelineEventProvenance.LIVE),
        ]

    async def test_sliding_initial_num_live_marks_only_exact_tail_live(self, client):
        admissions = record_admissions(client)
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s1",
                "rooms": {
                    ROOM_A: {
                        "initial": True,
                        "membership": "join",
                        "num_live": 2,
                        "timeline": [
                            text_event("$history-1", 1).source,
                            text_event("$history-2", 2).source,
                            text_event("$live-1", 3).source,
                            text_event("$live-2", 4).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)

        await client.receive_response(response)

        assert admissions == [
            ("$history-1", TimelineEventProvenance.HISTORY),
            ("$history-2", TimelineEventProvenance.HISTORY),
            ("$live-1", TimelineEventProvenance.LIVE),
            ("$live-2", TimelineEventProvenance.LIVE),
        ]

    async def test_sliding_expanded_timeline_uses_num_live_without_regressing_state(
        self,
        client,
    ):
        admissions = record_admissions(client, (RoomMessageText, RoomNameEvent))
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s1",
                "rooms": {
                    ROOM_A: {
                        "expanded_timeline": True,
                        "membership": "join",
                        "num_live": 1,
                        "required_state": [
                            name_event("$current", 3, "Current").source,
                        ],
                        "timeline": [
                            name_event("$history", 1, "Historic").source,
                            text_event("$live", 2).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)

        await client.receive_response(response)

        assert admissions == [
            ("$history", TimelineEventProvenance.HISTORY),
            ("$live", TimelineEventProvenance.LIVE),
        ]
        assert client.rooms[ROOM_A].name == "Current"

    @pytest.mark.parametrize(
        ("initial", "expanded_timeline", "num_live", "expected"),
        [
            (False, False, None, TimelineEventProvenance.LIVE),
            (True, False, None, TimelineEventProvenance.HISTORY),
            (False, True, None, TimelineEventProvenance.HISTORY),
            (False, False, -1, TimelineEventProvenance.HISTORY),
            (False, False, 3, TimelineEventProvenance.LIVE),
            (True, False, 3, TimelineEventProvenance.LIVE),
        ],
    )
    async def test_sliding_live_count_matches_deployed_response_shapes(
        self,
        client,
        initial,
        expanded_timeline,
        num_live,
        expected,
    ):
        admissions = record_admissions(client)
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s1",
                "rooms": {
                    ROOM_A: {
                        "initial": initial,
                        "expanded_timeline": expanded_timeline,
                        "membership": "join",
                        "num_live": num_live,
                        "timeline": [
                            text_event("$history-1", 1).source,
                            text_event("$history-2", 2).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)

        await client.receive_response(response)

        assert admissions == [
            ("$history-1", expected),
            ("$history-2", expected),
        ]

    @pytest.mark.parametrize(
        ("request_since", "expected"),
        [
            (None, TimelineEventProvenance.HISTORY),
            ("s0", TimelineEventProvenance.LIVE),
        ],
    )
    async def test_late_decryption_after_restart_keeps_original_provenance(
        self,
        tempdir,
        monkeypatch,
        request_since,
        expected,
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
        first.next_batch = request_since
        encrypted = megolm_event("$encrypted", 1)
        first_admissions = record_admissions(
            first,
            (MegolmEvent, RoomMessageText),
        )

        await first.receive_response(
            sync_response(
                "s1",
                {
                    ROOM_A: room_info(
                        [encrypted],
                        limited=False,
                        prev_batch="p0",
                    )
                },
            )
        )

        assert first_admissions == [("$encrypted", expected)]
        await first.close()
        assert first.store
        first.store.database.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        clear = text_event("$encrypted", 1)
        assert restarted.olm
        monkeypatch.setattr(
            restarted.olm,
            "_decrypt_megolm_no_error",
            lambda _event: clear,
        )
        restarted_admissions = record_admissions(
            restarted,
            (MegolmEvent, RoomMessageText),
        )

        await restarted.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [encrypted],
                        limited=False,
                        prev_batch="p1",
                    )
                },
            )
        )

        assert restarted_admissions == [("$encrypted", expected)]
        await restarted.close()

    async def test_no_admission_owner_skips_durable_acceptance_marker(self, client):
        marked = False

        def mark():
            nonlocal marked
            marked = True

        await client._dispatch_timeline_event(
            ROOM_A,
            text_event("$plain", 1),
            False,
            "timeline",
            TimelineEventProvenance.LIVE,
            True,
            False,
            mark,
        )

        assert not marked

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_non_acceptance_from_event_fanout_does_not_replay(
        self, client, protocol
    ):
        calls: list[str] = []

        async def admit(_room, event, _provenance):
            calls.append(f"admit:{event.event_id}")

        async def first(_room, event):
            calls.append(f"first:{event.event_id}")

        async def second(_room, event):
            calls.append(f"second:{event.event_id}")
            raise CallbackNotAcceptedError("too late to reject")

        client.add_event_admission_callback(admit, RoomMessageText)
        client.add_event_callback(first, RoomMessageText)
        client.add_event_callback(second, RoomMessageText)
        response = timeline_response(
            protocol,
            "s1",
            [text_event("$once", 1)],
        )

        with pytest.raises(CallbackNotAcceptedError, match="too late to reject"):
            await client.receive_response(response)

        await client.receive_response(response)

        assert calls == ["admit:$once", "first:$once", "second:$once"]
        assert not client._recovery.gaps

    async def test_callback_non_acceptance_preserves_implicit_context(self, client):
        storage_error = OSError("storage unavailable")

        async def admit(_room, _event, _provenance):
            try:
                raise storage_error
            except OSError:
                raise CallbackNotAcceptedError("durable admission failed")

        client.add_event_admission_callback(admit, RoomMessageText)
        response = timeline_response(
            "classic",
            "s1",
            [text_event("$retry", 1)],
        )

        with pytest.raises(
            CallbackNotAcceptedError,
            match="durable admission failed",
        ) as rejected:
            await client.receive_response(response)

        assert rejected.value.__cause__ is None
        assert rejected.value.__context__ is storage_error

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

    @pytest.mark.parametrize("end", [None, "p1"])
    async def test_token_or_exhaustion_closes_gap_without_end_progress(
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
        assert seen == ["$old", "$gap", "$held"]
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
        assert seen == ["$old", "$gap1", "$gap2", "$held"]
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
        assert seen == ["$old", "$gap", "$held"]
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

    async def test_live_overlap_recovers_events_after_held_window(
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
        aioresponse.get(MESSAGES_URL, payload=messages([], "p1"))
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
        assert seen == ["$gap", "$held", "$overflow"]
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
        assert seen == ["$gap", "$held", "$overflow"]
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
        a_started = asyncio.Event()
        b_started = asyncio.Event()
        release_a = asyncio.Event()

        async def send(_method, path, *_args, **_kwargs):
            request_since = parse_qs(urlparse(path).query)["since"][0]
            if request_since == "a":
                a_started.set()
                await release_a.wait()
            else:
                b_started.set()
            return Transport(request_since)

        async def create_matrix_response(*, transport_response, **_kwargs):
            return responses[transport_response.request_since]

        monkeypatch.setattr(client, "send", send)
        monkeypatch.setattr(client, "create_matrix_response", create_matrix_response)
        first_task = asyncio.create_task(client.sync(since="a"))
        await a_started.wait()
        second_task = asyncio.create_task(client.sync(since="b"))
        await asyncio.sleep(0)
        assert not b_started.is_set()
        release_a.set()
        first, second = await asyncio.gather(first_task, second_task)
        assert {first.next_batch, second.next_batch} == {"response-a", "response-b"}
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "a"
        assert client._recovery.gaps[ROOM_B][0].cursor_token == "b"

    async def test_concurrent_implicit_sync_reads_cursor_after_serialization(
        self, client, monkeypatch
    ):
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        request_cursors: list[str | None] = []

        async def send(*_args, sync_request_context=None, **_kwargs):
            request_cursors.append(sync_request_context.request_since)
            if len(request_cursors) == 1:
                first_started.set()
                await release_first.wait()
                response = sync_response("s1", {})
            else:
                response = sync_response("s2", {})
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        first = asyncio.create_task(client.sync())
        await first_started.wait()
        second = asyncio.create_task(client.sync())
        await asyncio.sleep(0)
        release_first.set()
        await asyncio.gather(first, second)

        assert request_cursors == [None, "s1"]

    async def test_fence_rejected_classic_join_without_room_is_ignored(self, client):
        older = issue_sync_request(client._sync_reset_fence, "classic")
        newer = issue_sync_request(client._sync_reset_fence, "classic")
        empty_room = RoomInfo(Timeline([], False, None), [], [], [])
        newer_leave = sync_response("s2", {}, left={ROOM_A: empty_room})
        older_join = sync_response("s1", {ROOM_A: empty_room})

        try:
            await client._receive_sync_family(
                async_client_module._SyncResponseEnvelope(
                    newer_leave,
                    None,
                    newer,
                )
            )
            await client._receive_sync_family(
                async_client_module._SyncResponseEnvelope(
                    older_join,
                    None,
                    older,
                )
            )
        finally:
            finish_sync_request(client._sync_reset_fence, older)
            finish_sync_request(client._sync_reset_fence, newer)

        assert ROOM_A not in client.rooms

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

    async def test_retained_callback_sync_reentry_fails_after_executor_clears(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_timeout=0.02,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        callback_started = asyncio.Event()
        release_callback = asyncio.Event()
        failures: list[str] = []

        async def unexpected_send(*_args, **_kwargs):
            raise AssertionError("retained callback reentry must fail before HTTP")

        async def fetch_messages(*_args, **_kwargs):
            return RoomMessagesResponse.from_dict(
                {"start": "previous", "end": "target", "chunk": []},
                ROOM_A,
            )

        async def callback(_room, event):
            callback_started.set()
            await release_callback.wait()
            try:
                await client.sync(since="nested")
            except LocalProtocolError as error:
                failures.append(str(error))

        monkeypatch.setattr(client, "_send", unexpected_send)
        monkeypatch.setattr(client, "_recovery_room_messages", fetch_messages)
        client.add_event_callback(callback, RoomMessageText)
        client.next_batch = "previous"
        response_task = asyncio.create_task(
            client.receive_response(
                sync_response(
                    "outer",
                    {
                        ROOM_A: room_info(
                            [text_event("$outer", 1)],
                            limited=True,
                            prev_batch="target",
                        )
                    },
                )
            )
        )

        try:
            await callback_started.wait()
            await asyncio.wait_for(response_task, 1)
            assert client._active_sync_executor_token is None
            assert client._recovery._active_dispatches
        finally:
            release_callback.set()
        await client._pump_sync_recovery()

        assert failures == ["Sync-family requests cannot run from a timeline callback."]
        assert not client._recovery.gaps
        await client.close()

    async def test_callback_close_reentry_fails_before_session_close(self, client):
        failures: list[str] = []

        class Session:
            closed = False

            async def close(self):
                self.closed = True

        session = Session()
        client.client_session = session

        async def callback(_room, value):
            if value.event_id != "$outer":
                return
            try:
                await client.close()
            except LocalProtocolError as error:
                failures.append(str(error))

        client.add_event_callback(callback, RoomMessageText)
        await client.receive_response(
            timeline_response("classic", "outer", [text_event("$outer", 1)])
        )

        assert failures == ["AsyncClient.close() cannot run from a timeline callback."]
        assert not session.closed

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

    async def test_disabled_callback_may_start_nested_sync_request(
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
        requests: list[str] = []
        seen = record_events(client)

        async def callback(_room, event):
            if event.event_id == "$outer":
                await client.sync(since="nested")

        async def send(_response_class, _method, path, *_args, **_kwargs):
            since = parse_qs(urlparse(path).query).get("since", ["outer"])[0]
            requests.append(since)
            response = sync_response(
                f"{since}-response",
                {
                    ROOM_A: room_info(
                        [text_event(f"${since}", len(requests))],
                        limited=False,
                        prev_batch=None,
                    )
                },
            )
            await client.receive_response(response)
            return response

        client.add_event_callback(callback, RoomMessageText)
        monkeypatch.setattr(client, "_send", send)
        await asyncio.wait_for(client.sync(since="outer"), 0.5)

        assert requests == ["outer", "nested"]
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

    async def test_response_drain_does_not_hold_room_gate_needed_by_callback_send(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(self._sliding("s1", []))
        callback_started = asyncio.Event()
        release_callback = asyncio.Event()
        callback_finished = asyncio.Event()
        sent = object()

        async def callback():
            callback_started.set()
            await release_callback.wait()
            assert (
                await client.room_send(
                    ROOM_A,
                    "m.room.message",
                    {"body": "reply", "msgtype": "m.text"},
                )
                is sent
            )
            callback_finished.set()
            return None

        async def send(*_args, **_kwargs):
            return sent

        monkeypatch.setattr(client, "_send", send)
        retained = asyncio.create_task(callback())
        client._recovery._active_dispatches[(ROOM_A, "$retained", "timeline")] = (
            retained
        )
        await callback_started.wait()

        second = asyncio.create_task(client.receive_response(self._sliding("s2", [])))
        await asyncio.sleep(0)
        release_callback.set()
        done, _ = await asyncio.wait((second,), timeout=5)
        if second in done:
            await second
            assert callback_finished.is_set()
            await client.close()
            return

        second.cancel()
        retained.cancel()
        await asyncio.gather(second, retained, return_exceptions=True)
        client._recovery._active_dispatches.clear()
        pytest.fail("response drain deadlocked with the callback's room_send()")

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
        assert seen == ["$gap", "$held"]
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
        assert seen == ["$old"]

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
        assert seen == ["$old", "$free"]
        assert presence_seen == ["@sender:example.org"]

    async def test_newer_same_room_event_waits_for_pending_gap(
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
        assert seen == ["$old", "$gap", "$gap2", "$held", "$later"]

    async def test_target_token_preserves_recovered_prefix(self, client, aioresponse):
        seen = record_events(client)
        client.next_batch = "s1"
        recovered = [text_event(f"${index}", index) for index in range(14)]
        live = [text_event(f"${index}", index) for index in range(14, 64)]
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(recovered + live[:36], "p1"),
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
        assert seen == [f"${index}" for index in range(64)]
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
        assert seen == ["$gap2", "$gap1", "$present1", "$present2"]
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
        assert seen == ["$join", "$after", "$live"]

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
        assert seen == []
        assert [event.event_id for event in client._recovery.events[(ROOM_A, 1)]] == [
            "$prejoin",
            "$held",
        ]

        await client.receive_response(limited)
        assert seen == ["$join", "$after", "$held"]
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
        assert seen == ["$seen", "$gap", "$live"]

        encrypted = megolm_event("$encrypted", 4)
        record_completed_timeline_event(
            client._recovery,
            ROOM_A,
            encrypted.event_id,
            True,
            TimelineEventProvenance.LIVE,
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
            client._recovery,
            ROOM_A,
            encrypted.event_id,
            True,
            TimelineEventProvenance.LIVE,
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
        assert client._recovery.completed[ROOM_A]["$encrypted"].was_encrypted is False

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
        assert first_seen == ["$old"]
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
        assert restarted_seen == ["$gap", "$gap2", "$held", "$later"]
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
        assert seen == ["$gap", "$held"]
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
        assert seen == ["$gap", "$held"]

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
        assert seen == ["$gap", "$held"]
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
        assert names == ["Before", "Gap", "After"]
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
        assert seen == ["$old"]

        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap2", 3), text_event("$held", 4)],
                "p1",
            ),
        )
        await client.receive_response(limited)
        assert seen == ["$old", "$gap", "$gap2", "$held"]
        assert not client._recovery.gaps

    async def test_recovery_stops_callback_fanout_on_failure(self, client, aioresponse):
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
        with pytest.raises(RuntimeError, match="callback failed"):
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
        assert calls == ["first", "failing"]
        assert ROOM_A in client._recovery.gaps

    async def test_recovered_callback_failure_keeps_gap_unrecovered(
        self, client, aioresponse
    ):
        async def fail(_room, event):
            if event.event_id == "$gap":
                raise RuntimeError("durable callback failed")

        client.add_event_callback(fail, RoomMessageText)
        client.next_batch = "s1"
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap", 2), text_event("$held", 3)],
                "p1",
            ),
        )
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 3)], limited=True, prev_batch="p1"
                )
            },
        )

        with pytest.raises(RuntimeError, match="durable callback failed"):
            await client.receive_response(response)

        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        assert "$gap" in {
            event.event_id for event in client._recovery.events[(ROOM_A, 1)]
        }

    async def test_callback_fanout_failure_retries_across_restart(
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
        admissions: list[str] = []

        async def admit_first(_room, event, _provenance):
            admissions.append(f"first:{event.event_id}")

        async def before(_room, event):
            calls.append(f"before:{event.event_id}")

        async def fail(_room, event):
            calls.append(f"fail:{event.event_id}")
            if event.event_id == "$gap1":
                raise RuntimeError("callback failed")

        async def after(_room, event):
            calls.append(f"after:{event.event_id}")

        for callback in (before, fail, after):
            first.add_event_callback(callback, RoomMessageText)
        first.add_event_admission_callback(admit_first, RoomMessageText)
        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [
                    text_event("$gap1", 1),
                    text_event("$gap2", 2),
                    text_event("$held", 3),
                ],
                "p1",
            ),
        )
        response = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 3)], limited=True, prev_batch="p1"
                )
            },
        )
        with pytest.raises(RuntimeError, match="callback failed"):
            await first.receive_response(response)
        assert calls == [
            "before:$gap1",
            "fail:$gap1",
        ]
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        assert ROOM_A in first._recovery.gaps
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
        assert ROOM_A in restarted._recovery.gaps
        replayed: list[str] = []

        async def replay(_room, event):
            replayed.append(event.event_id)

        async def admit_restarted(_room, event, _provenance):
            admissions.append(f"restarted:{event.event_id}")

        restarted.add_event_callback(replay, RoomMessageText)
        restarted.add_event_admission_callback(admit_restarted, RoomMessageText)
        await restarted.receive_response(sync_response("s2", {}))

        assert replayed == ["$gap1", "$gap2", "$held"]
        assert admissions == [
            "first:$gap1",
            "restarted:$gap2",
            "restarted:$held",
        ]
        assert not restarted._recovery.gaps
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
        assert seen == ["$gap", "$held"]
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
        assert seen == ["$gap", "$held"]
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

        assert seen == ["$gap", "$held"]
        assert pages.from_tokens == ["w1"]
        assert not second._recovery.gaps
        await second.close()

    @pytest.mark.parametrize(
        "membership_event_id",
        [None, "$new-membership"],
    )
    async def test_restart_token_requires_current_membership_identity(
        self,
        tempdir,
        aioresponse,
        membership_event_id,
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

        response = self._sliding(
            "s2",
            [text_event("$held", 3)],
            limited=True,
            prev_batch="w2",
            initial=True,
            membership_event_id=membership_event_id,
        )
        await second.receive_response(response)

        assert seen == ["$held"]
        assert pages.from_tokens == []
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        stored = second.store.load_sliding_window_tokens().get(ROOM_A)
        assert stored is None
        await second.close()

    async def test_own_membership_deletion_stub_invalidates_sliding_baseline(
        self, client, aioresponse
    ):
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s2",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "limited": True,
                        "prev_batch": "w2",
                        "required_state": [
                            {
                                "type": "m.room.member",
                                "state_key": OWN_ID,
                            }
                        ],
                        "timeline": [text_event("$held", 3).source],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)

        await client.receive_response(response)

        assert pages.from_tokens == []
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        assert ROOM_A not in client._sliding_room_prev_batch

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

    async def test_disabled_sliding_sync_keeps_upstream_request_shape(
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

        async def send(*args, **_kwargs):
            sent.append(json.loads(args[3]))
            return SlidingSyncResponse.from_dict({"pos": "s1", "rooms": {}})

        monkeypatch.setattr(client, "_send", send)
        await client.sliding_sync(
            lists=lists,
            room_subscriptions=subscriptions,
        )

        assert sent[0]["lists"] == lists
        assert sent[0]["room_subscriptions"] == subscriptions
        await client.close()

    async def test_forgetting_a_room_drops_its_window_token(self, tempdir, monkeypatch):
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

        async def send(*_args, **_kwargs):
            return RoomForgetResponse.from_dict({}, ROOM_A)

        monkeypatch.setattr(client, "_send", send)
        await client.room_forget(ROOM_A)

        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sliding_window_tokens() == {}
        assert client._sync_reset_fence.room_cutoffs == {}
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

    async def test_direct_response_after_reset_seeds_a_fresh_baseline(self, tempdir):
        """A direct response has no stale request context and seeds current state."""
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

        await client.receive_response(
            self._sliding("s2", [text_event("$fresh", 2)], prev_batch="w2")
        )
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w2")}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w2")}

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

    async def test_initial_direct_response_after_reset_is_accepted(self, tempdir):
        """A direct snapshot has no old request generation to reject."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))

        client._forget_sliding_window_token(ROOM_A)
        await client.receive_response(
            self._sliding(
                "s3", [text_event("$fresh", 3)], prev_batch="w3", initial=True
            )
        )
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w3")}
        await client.close()

    async def test_independent_sliding_connections_apply_deltas_in_arrival_order(
        self, tempdir, monkeypatch
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        older_started = asyncio.Event()
        newer_started = asyncio.Event()
        release_older = asyncio.Event()
        reverse_walks: list[tuple[str | None, str | None]] = []
        invite_names: list[str] = []
        global_values: list[str] = []
        room_markers: list[str] = []
        room_extras: list[str] = []
        to_device_values: list[str] = []
        e2ee_positions: list[str] = []
        seen = record_events(client)

        async def invite_callback(_room, event):
            invite_names.append(event.name)

        async def global_callback(event):
            global_values.append(event.source["content"]["value"])

        async def room_callback(_room, event):
            room_markers.append(event.event_id)

        async def room_extra_callback(_room, event):
            room_extras.append(event.content["value"])

        async def to_device_callback(event):
            to_device_values.append(event.source["content"]["body"])

        def handle_olm(response):
            e2ee_positions.append(response.pos)

        client.add_event_callback(invite_callback, InviteNameEvent)
        client.add_global_account_data_callback(
            global_callback, UnknownAccountDataEvent
        )
        client.add_room_account_data_callback(room_callback, FullyReadEvent)
        client.add_room_account_data_callback(
            room_extra_callback, UnknownAccountDataEvent
        )
        client.add_to_device_callback(to_device_callback, UnknownToDeviceEvent)
        monkeypatch.setattr(client, "_handle_olm_events", handle_olm)

        def sliding_response(label: str, token: str, *, limited: bool):
            invite = {
                "content": {"name": f"{label} invite"},
                "sender": "@sender:example.org",
                "state_key": "",
                "type": "m.room.name",
            }
            response = SlidingSyncResponse.from_dict(
                {
                    "pos": f"{label}-response",
                    "rooms": {
                        ROOM_A: {
                            "membership": "join",
                            "limited": limited,
                            "prev_batch": token,
                            "required_state": [
                                member_event("$membership", 0, "join", OWN_ID).source,
                                member_event(
                                    f"${label}-member",
                                    1,
                                    "join" if label == "newer" else "leave",
                                    "@surface:example.org",
                                ).source,
                                name_event(f"${label}-name", 2, f"{label} room").source,
                                encryption_event(ROOM_A).source,
                            ],
                            "timeline": [
                                *(
                                    [
                                        member_event(
                                            "$stale-own-membership",
                                            2,
                                            "join",
                                        ).source
                                    ]
                                    if label == "older"
                                    else []
                                ),
                                text_event(f"${label}", 3).source,
                            ],
                        },
                        ROOM_B: {
                            "membership": "invite",
                            "stripped_state": [invite],
                        },
                    },
                    "extensions": {
                        "to_device": {
                            "next_batch": f"{label}-to-device",
                            "events": [
                                {
                                    "content": {"body": label},
                                    "sender": "@sender:example.org",
                                    "type": "org.example.test",
                                }
                            ],
                        },
                        "e2ee": {
                            "device_lists": {
                                "changed": [f"@{label}:example.org"],
                                "left": [],
                            }
                        },
                        "account_data": {
                            "global": [
                                {
                                    "content": {"value": label},
                                    "type": "org.example.settings",
                                },
                                *(
                                    [
                                        {
                                            "content": {"value": "older-extra"},
                                            "type": "org.example.extra",
                                        }
                                    ]
                                    if label == "older"
                                    else []
                                ),
                            ],
                            "rooms": {
                                ROOM_A: [
                                    {
                                        "content": {"event_id": f"${label}-read"},
                                        "type": "m.fully_read",
                                    },
                                    *(
                                        [
                                            {
                                                "content": {
                                                    "value": "older-room-extra"
                                                },
                                                "type": "org.example.extra",
                                            }
                                        ]
                                        if label == "older"
                                        else []
                                    ),
                                ]
                            },
                        },
                    },
                }
            )
            assert isinstance(response, SlidingSyncResponse)
            return response

        async def send(*args, **_kwargs):
            if args[0] is RoomMessagesResponse:
                query = parse_qs(urlparse(args[2]).query)
                reverse_walks.append(
                    (
                        query.get("from", [None])[0],
                        query.get("to", [None])[0],
                    )
                )
                return RoomMessagesResponse.from_dict(
                    {"start": "w2", "end": "w1", "chunk": []},
                    ROOM_A,
                )
            request_pos = parse_qs(urlparse(args[2]).query)["pos"][0]
            if request_pos == "older":
                older_started.set()
                await release_older.wait()
                response = sliding_response("older", "w1", limited=False)
            else:
                newer_started.set()
                response = sliding_response("newer", "w2", limited=False)
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        older = asyncio.create_task(client.sliding_sync(conn_id="older", pos="older"))
        await older_started.wait()
        newer = asyncio.create_task(client.sliding_sync(conn_id="newer", pos="newer"))
        await newer_started.wait()
        await newer
        release_older.set()
        await older

        assert reverse_walks == []
        assert not client._recovery.gaps
        expected_window = window_token("w1", "$stale-own-membership")
        assert client._sliding_room_prev_batch == {ROOM_A: expected_window}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: expected_window}
        assert client.rooms[ROOM_A].name == "older room"
        assert "@surface:example.org" not in client.rooms[ROOM_A].users
        assert client.rooms[ROOM_A].encrypted
        assert client.invited_rooms[ROOM_B].name == "older invite"
        assert invite_names == ["newer invite", "older invite"]
        assert seen == ["$newer", "$older"]
        assert global_values == ["newer", "older", "older-extra"]
        assert room_markers == ["$newer-read", "$older-read"]
        assert room_extras == ["older-room-extra"]
        assert to_device_values == ["newer", "older"]
        assert client._sliding_sync_to_device_since == "newer-to-device"
        assert e2ee_positions == ["newer-response", "older-response"]
        assert client._sync_reset_fence.active_request_ids == set()
        assert client._sync_reset_fence.room_component_floors == {}
        assert client._sync_reset_fence.account_data_floors == {}
        assert client._sync_reset_fence.to_device_floor == 0
        await client.close()

    async def test_independent_sliding_connections_keep_same_type_deltas(
        self, client, monkeypatch
    ):
        lower_started = asyncio.Event()
        release_lower = asyncio.Event()
        values: list[str] = []

        async def account_data_callback(event):
            values.append(event.source["content"]["value"])

        def response(label):
            value = SlidingSyncResponse.from_dict(
                {
                    "pos": label,
                    "rooms": {},
                    "extensions": {
                        "account_data": {
                            "global": [
                                {
                                    "content": {"value": label},
                                    "type": "org.example.settings",
                                }
                            ]
                        }
                    },
                }
            )
            assert isinstance(value, SlidingSyncResponse)
            return value

        async def send(*args, **_kwargs):
            request_pos = parse_qs(urlparse(args[2]).query)["pos"][0]
            if request_pos == "lower":
                lower_started.set()
                await release_lower.wait()
            value = response(request_pos)
            await client.receive_response(value)
            return value

        client.add_global_account_data_callback(
            account_data_callback,
            UnknownAccountDataEvent,
        )
        monkeypatch.setattr(client, "_send", send)

        lower = asyncio.create_task(client.sliding_sync(conn_id="lower", pos="lower"))
        await lower_started.wait()
        await client.sliding_sync(conn_id="higher", pos="higher")
        release_lower.set()
        await lower

        assert values == ["higher", "lower"]

    async def test_older_sliding_response_cannot_rewind_one_time_key_count(
        self, client, monkeypatch
    ):
        lower_started = asyncio.Event()
        release_lower = asyncio.Event()
        uploaded_key_count = 50

        async def no_expired_verifications():
            pass

        async def no_key_requests():
            pass

        def handle_olm(response):
            nonlocal uploaded_key_count
            count = response.device_key_count.signed_curve25519
            if count is not None:
                uploaded_key_count = count

        def response(label, count):
            value = SlidingSyncResponse.from_dict(
                {
                    "pos": label,
                    "rooms": {},
                    "extensions": {
                        "e2ee": {
                            "device_one_time_keys_count": {"signed_curve25519": count}
                        }
                    },
                }
            )
            assert isinstance(value, SlidingSyncResponse)
            return value

        async def send(*args, **_kwargs):
            request_pos = parse_qs(urlparse(args[2]).query)["pos"][0]
            if request_pos == "lower":
                lower_started.set()
                await release_lower.wait()
                value = response("lower", 50)
            else:
                value = response("higher", 0)
            await client.receive_response(value)
            return value

        client.olm = object()
        monkeypatch.setattr(
            client,
            "_handle_expired_verifications",
            no_expired_verifications,
        )
        monkeypatch.setattr(client, "_collect_key_requests", no_key_requests)
        monkeypatch.setattr(client, "_handle_olm_events", handle_olm)
        monkeypatch.setattr(client, "_send", send)

        lower = asyncio.create_task(client.sliding_sync(conn_id="lower", pos="lower"))
        await lower_started.wait()
        await client.sliding_sync(conn_id="higher", pos="higher")
        release_lower.set()
        await lower

        assert uploaded_key_count == 0

    async def test_curve_only_one_time_key_count_reaches_olm(self, client, monkeypatch):
        counts = []

        async def no_expired_verifications():
            pass

        async def no_key_requests():
            pass

        def handle_olm(response):
            counts.append(response.device_key_count)

        response = SlidingSyncResponse.from_dict(
            {
                "pos": "curve-only",
                "rooms": {},
                "extensions": {
                    "e2ee": {"device_one_time_keys_count": {"curve25519": 7}}
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)

        async def send(*_args, **_kwargs):
            await client.receive_response(response)
            return response

        client.olm = object()
        monkeypatch.setattr(
            client,
            "_handle_expired_verifications",
            no_expired_verifications,
        )
        monkeypatch.setattr(client, "_collect_key_requests", no_key_requests)
        monkeypatch.setattr(client, "_handle_olm_events", handle_olm)
        monkeypatch.setattr(client, "_send", send)

        await client.sliding_sync(pos="before")

        assert counts == [DeviceOneTimeKeyCount(7, None)]

    async def test_failed_newer_response_does_not_suppress_older_key_count(
        self, client, monkeypatch
    ):
        lower_started = asyncio.Event()
        release_lower = asyncio.Event()
        uploaded_key_count = None

        async def no_expired_verifications():
            pass

        async def no_key_requests():
            pass

        def handle_olm(response):
            nonlocal uploaded_key_count
            count = response.device_key_count.signed_curve25519
            if count is not None:
                uploaded_key_count = count

        async def fail_account_data(_event):
            raise RuntimeError("callback failed")

        def response(label, count, *, fail=False):
            value = SlidingSyncResponse.from_dict(
                {
                    "pos": label,
                    "rooms": {},
                    "extensions": {
                        "account_data": {
                            "global": (
                                [
                                    {
                                        "content": {},
                                        "type": "org.example.fail",
                                    }
                                ]
                                if fail
                                else []
                            )
                        },
                        "e2ee": {
                            "device_one_time_keys_count": {"signed_curve25519": count}
                        },
                    },
                }
            )
            assert isinstance(value, SlidingSyncResponse)
            return value

        async def send(*args, **_kwargs):
            request_pos = parse_qs(urlparse(args[2]).query)["pos"][0]
            if request_pos == "lower":
                lower_started.set()
                await release_lower.wait()
                value = response("lower", 50)
            else:
                value = response("higher", 0, fail=True)
            await client.receive_response(value)
            return value

        client.olm = object()
        client.add_global_account_data_callback(
            fail_account_data,
            UnknownAccountDataEvent,
        )
        monkeypatch.setattr(
            client,
            "_handle_expired_verifications",
            no_expired_verifications,
        )
        monkeypatch.setattr(client, "_collect_key_requests", no_key_requests)
        monkeypatch.setattr(client, "_handle_olm_events", handle_olm)
        monkeypatch.setattr(client, "_send", send)

        lower = asyncio.create_task(client.sliding_sync(conn_id="lower", pos="lower"))
        await lower_started.wait()
        with pytest.raises(RuntimeError, match="callback failed"):
            await client.sliding_sync(conn_id="higher", pos="higher")
        release_lower.set()
        await lower

        assert uploaded_key_count == 50

    async def test_newer_curve_count_does_not_suppress_older_signed_count(
        self, client, monkeypatch
    ):
        lower_started = asyncio.Event()
        release_lower = asyncio.Event()
        counts = []

        async def no_expired_verifications():
            pass

        async def no_key_requests():
            pass

        def handle_olm(response):
            counts.append(response.device_key_count)

        def response(label, counts):
            value = SlidingSyncResponse.from_dict(
                {
                    "pos": label,
                    "rooms": {},
                    "extensions": {"e2ee": {"device_one_time_keys_count": counts}},
                }
            )
            assert isinstance(value, SlidingSyncResponse)
            return value

        async def send(*args, **_kwargs):
            request_pos = parse_qs(urlparse(args[2]).query)["pos"][0]
            if request_pos == "lower":
                lower_started.set()
                await release_lower.wait()
                value = response("lower", {"signed_curve25519": 50})
            else:
                value = response("higher", {"curve25519": 7})
            await client.receive_response(value)
            return value

        client.olm = object()
        monkeypatch.setattr(
            client,
            "_handle_expired_verifications",
            no_expired_verifications,
        )
        monkeypatch.setattr(client, "_collect_key_requests", no_key_requests)
        monkeypatch.setattr(client, "_handle_olm_events", handle_olm)
        monkeypatch.setattr(client, "_send", send)

        lower = asyncio.create_task(client.sliding_sync(conn_id="lower", pos="lower"))
        await lower_started.wait()
        await client.sliding_sync(conn_id="higher", pos="higher")
        release_lower.set()
        await lower

        assert counts == [
            DeviceOneTimeKeyCount(7, None),
            DeviceOneTimeKeyCount(None, 50),
        ]

    async def test_applied_key_count_stays_ordered_when_key_callback_fails(
        self, client, monkeypatch
    ):
        lower_started = asyncio.Event()
        release_lower = asyncio.Event()
        uploaded_key_count = None
        key_request_collections = 0

        async def no_expired_verifications():
            pass

        def handle_olm(response):
            nonlocal uploaded_key_count
            count = response.device_key_count.signed_curve25519
            if count is not None:
                uploaded_key_count = count

        async def collect_key_requests():
            nonlocal key_request_collections
            key_request_collections += 1
            if key_request_collections == 1:
                raise RuntimeError("key callback failed")

        def response(label, count):
            value = SlidingSyncResponse.from_dict(
                {
                    "pos": label,
                    "rooms": {},
                    "extensions": {
                        "e2ee": {
                            "device_one_time_keys_count": {"signed_curve25519": count}
                        }
                    },
                }
            )
            assert isinstance(value, SlidingSyncResponse)
            return value

        async def send(*args, **_kwargs):
            request_pos = parse_qs(urlparse(args[2]).query)["pos"][0]
            if request_pos == "lower":
                lower_started.set()
                await release_lower.wait()
                value = response("lower", 50)
            else:
                value = response("higher", 0)
            await client.receive_response(value)
            return value

        client.olm = object()
        monkeypatch.setattr(
            client,
            "_handle_expired_verifications",
            no_expired_verifications,
        )
        monkeypatch.setattr(client, "_collect_key_requests", collect_key_requests)
        monkeypatch.setattr(client, "_handle_olm_events", handle_olm)
        monkeypatch.setattr(client, "_send", send)

        lower = asyncio.create_task(client.sliding_sync(conn_id="lower", pos="lower"))
        await lower_started.wait()
        with pytest.raises(RuntimeError, match="key callback failed"):
            await client.sliding_sync(conn_id="higher", pos="higher")
        release_lower.set()
        await lower

        assert uploaded_key_count == 0

    async def test_independent_transport_deltas_apply_in_arrival_order(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("seed", [text_event("$seed", 0)], prev_batch="w0")
        )
        older_started = asyncio.Event()
        release_older = asyncio.Event()
        seen = record_events(client)
        admissions = record_admissions(
            client,
            (RoomMessageText, RoomNameEvent),
        )
        names: list[str] = []

        async def name_callback(_room, event):
            names.append(event.name)

        client.add_event_callback(name_callback, RoomNameEvent)

        async def send(response_class, *_args, **_kwargs):
            if response_class is SlidingSyncResponse:
                older_started.set()
                await release_older.wait()
                response = self._sliding(
                    "older",
                    [
                        name_event("$older-history-name", 2, "Historic"),
                        name_event("$older-live-name", 3, "Stale live"),
                    ],
                    prev_batch="w1",
                    expanded_timeline=True,
                    num_live=1,
                )
                response.rooms[ROOM_A].required_state.append(
                    name_event("$older-name", 2, "older room")
                )
            else:
                response = sync_response(
                    "newer",
                    {
                        ROOM_A: RoomInfo(
                            Timeline([text_event("$newer", 1)], False, None),
                            [name_event("$newer-name", 1, "newer room")],
                            [],
                            [],
                        )
                    },
                )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)
        older = asyncio.create_task(client.sliding_sync(conn_id="older", pos="older"))
        await older_started.wait()
        await client.sync(since="classic")
        release_older.set()
        await older

        assert seen == ["$newer"]
        assert names == ["Historic", "Stale live"]
        assert admissions == [
            ("$newer", TimelineEventProvenance.HISTORY),
            ("$older-history-name", TimelineEventProvenance.HISTORY),
            ("$older-live-name", TimelineEventProvenance.LIVE),
        ]
        assert client.rooms[ROOM_A].name == "newer room"
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w0")}
        await client.close()

    async def test_stale_sync_event_retries_after_membership_reset(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("seed", [text_event("$seed", 0)], prev_batch="w0")
        )
        older_started = asyncio.Event()
        release_older = asyncio.Event()
        calls: list[tuple[str, TimelineEventProvenance]] = []

        async def admit(_room, event, provenance):
            calls.append((event.event_id, provenance))
            if event.event_id == "$older":
                raise CallbackNotAcceptedError("durable admission failed")

        client.add_event_admission_callback(admit, RoomMessageText)

        async def send(response_class, *_args, **_kwargs):
            if response_class is SlidingSyncResponse:
                older_started.set()
                await release_older.wait()
                response = self._sliding(
                    "older",
                    [text_event("$older", 2)],
                    prev_batch="w1",
                )
            else:
                response = sync_response(
                    "newer",
                    {
                        ROOM_A: RoomInfo(
                            Timeline([text_event("$newer", 1)], False, None),
                            [],
                            [],
                            [],
                        )
                    },
                )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)
        older = asyncio.create_task(client.sliding_sync(conn_id="older", pos="older"))
        await older_started.wait()
        await client.sync(since="classic")
        release_older.set()
        with pytest.raises(
            CallbackNotAcceptedError,
            match="durable admission failed",
        ):
            await older

        leave = self._sliding("leave", [], membership="leave")
        with pytest.raises(
            CallbackNotAcceptedError,
            match="durable admission failed",
        ):
            await client.receive_response(leave)

        assert calls == [
            ("$newer", TimelineEventProvenance.HISTORY),
            ("$older", TimelineEventProvenance.LIVE),
            ("$older", TimelineEventProvenance.LIVE),
        ]
        await client.close()

    async def test_independent_initial_snapshot_does_not_clear_existing_gap(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("seed", [text_event("$seed", 0)], prev_batch="w0")
        )
        older_started = asyncio.Event()
        release_older = asyncio.Event()

        async def send(response_class, *args, **_kwargs):
            if response_class is RoomMessagesResponse:
                raise RuntimeError("keep the newer recovery gap open")
            request_pos = parse_qs(urlparse(args[1]).query)["pos"][0]
            if request_pos == "older":
                older_started.set()
                await release_older.wait()
                response = self._sliding(
                    "older-response",
                    [
                        member_event("$historical-rejoin", 1, "join", OWN_ID),
                        text_event("$historical", 2),
                    ],
                    initial=True,
                    num_live=0,
                    prev_batch="w1",
                )
            else:
                response = self._sliding(
                    "newer-response",
                    [text_event("$newer", 3)],
                    limited=True,
                    prev_batch="w2",
                )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)
        older = asyncio.create_task(client.sliding_sync(conn_id="older", pos="older"))
        await older_started.wait()
        await client.sliding_sync(conn_id="newer", pos="newer")
        release_older.set()
        await older

        assert client._recovery.gaps[ROOM_A][0].target_token == "w2"
        assert client._sliding_room_prev_batch[ROOM_A] == window_token("w1")
        await client.close()

    async def test_independent_classic_leave_clears_sliding_baseline_on_arrival(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("seed", [text_event("$seed", 0)], prev_batch="w0")
        )
        older_started = asyncio.Event()
        release_older = asyncio.Event()

        async def send(response_class, *_args, **_kwargs):
            if response_class is SyncResponse:
                older_started.set()
                await release_older.wait()
                response = sync_response("older", {})
                response.rooms.leave[ROOM_A] = RoomInfo(
                    Timeline([], False, None),
                    [],
                    [],
                    [],
                )
            else:
                response = self._sliding(
                    "newer",
                    [text_event("$newer", 1)],
                    prev_batch="w2",
                )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)
        older = asyncio.create_task(client.sync(since="classic"))
        await older_started.wait()
        await client.sliding_sync(conn_id="newer", pos="newer")
        release_older.set()
        await older

        assert ROOM_A not in client._sliding_room_prev_batch
        assert ROOM_A in client.rooms
        await client.close()

    async def test_to_device_connections_serialize_cursor_selection(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        first_started = asyncio.Event()
        release_first = asyncio.Event()
        request_count = 0
        seen: list[str] = []

        async def callback(event):
            seen.append(event.source["content"]["body"])

        async def send(*args, **_kwargs):
            nonlocal request_count
            request_count += 1
            request = json.loads(args[3])
            since = request["extensions"]["to_device"].get("since")
            if request_count == 1:
                first_started.set()
                await release_first.wait()
            event = (
                []
                if since == "td1"
                else [
                    {
                        "content": {"body": "once"},
                        "sender": "@sender:example.org",
                        "type": "org.example.test",
                    }
                ]
            )
            response = SlidingSyncResponse.from_dict(
                {
                    "pos": f"s{request_count}",
                    "rooms": {},
                    "extensions": {"to_device": {"next_batch": "td1", "events": event}},
                }
            )
            await client.receive_response(response)
            return response

        client.add_to_device_callback(callback, UnknownToDeviceEvent)
        monkeypatch.setattr(client, "_send", send)
        extensions = {"to_device": {"enabled": True}}
        first = asyncio.create_task(
            client.sliding_sync(conn_id="first", extensions=extensions)
        )
        await first_started.wait()
        second = asyncio.create_task(
            client.sliding_sync(conn_id="second", extensions=extensions)
        )
        await asyncio.sleep(0)
        assert request_count == 1
        release_first.set()
        await first
        await second

        assert seen == ["once"]
        await client.close()

    @pytest.mark.parametrize("first_transport", ("classic", "sliding"))
    async def test_to_device_transport_cannot_change_within_client_generation(
        self, tempdir, monkeypatch, first_transport
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        requests: list[type[SyncResponse] | type[SlidingSyncResponse]] = []

        async def send(response_class, *_args, **_kwargs):
            requests.append(response_class)
            response = (
                sync_response("classic", {})
                if response_class is SyncResponse
                else self._sliding("sliding", [])
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)
        extensions = {"to_device": {"enabled": True}}
        if first_transport == "classic":
            await client.sync()
            conflicting_request = client.sliding_sync(
                conn_id="sliding",
                extensions=extensions,
            )
        else:
            await client.sliding_sync(conn_id="sliding", extensions=extensions)
            conflicting_request = client.sync()

        with pytest.raises(
            LocalProtocolError,
            match="cannot both consume to-device messages",
        ):
            await conflicting_request

        assert len(requests) == 1
        await client.close()

    async def test_sliding_without_to_device_can_follow_classic_sync(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        requests: list[type[SyncResponse] | type[SlidingSyncResponse]] = []

        async def send(response_class, *_args, **_kwargs):
            requests.append(response_class)
            response = (
                sync_response("classic", {})
                if response_class is SyncResponse
                else self._sliding("sliding", [])
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)
        await client.sync()
        await client.sliding_sync(conn_id="sliding")

        assert requests == [SyncResponse, SlidingSyncResponse]
        await client.close()

    async def test_concurrent_sliding_requests_deliver_each_response_in_order(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        release_first = asyncio.Event()
        release_second = asyncio.Event()
        request_count = 0
        recovery_scopes: list[str] = []
        seen = record_events(client)
        global_values: list[str] = []
        to_device_values: list[str] = []
        original_plan = async_client_module.plan_room_timeline

        def record_plan(*args, **kwargs):
            recovery_scopes.append(kwargs["batch_id"].split(":")[1])
            return original_plan(*args, **kwargs)

        async def global_callback(event):
            global_values.append(event.content["value"])

        async def to_device_callback(event):
            to_device_values.append(event.source["content"]["body"])

        client.add_global_account_data_callback(
            global_callback, UnknownAccountDataEvent
        )
        client.add_to_device_callback(to_device_callback, UnknownToDeviceEvent)

        async def send(*_args, **_kwargs):
            nonlocal request_count
            request_count += 1
            if request_count == 1:
                label = "first"
                first_started.set()
                await release_first.wait()
            else:
                label = "second"
                second_started.set()
                await release_second.wait()
            response = self._sliding(label, [text_event(f"${label}", request_count)])
            response.to_device_next_batch = f"{label}-to-device"
            response.to_device_events = [
                UnknownToDeviceEvent.from_dict(
                    {
                        "content": {"body": label},
                        "sender": "@sender:example.org",
                        "type": "org.example.test",
                    }
                )
            ]
            response.account_data_events = [
                UnknownAccountDataEvent.from_dict(
                    {
                        "content": {"value": label},
                        "type": "org.example.settings",
                    }
                )
            ]
            await client.receive_response(response)
            return response

        monkeypatch.setattr(async_client_module, "plan_room_timeline", record_plan)
        monkeypatch.setattr(client, "_send", send)

        first = asyncio.create_task(client.sliding_sync(conn_id="first"))
        await first_started.wait()
        second = asyncio.create_task(client.sliding_sync(conn_id="second"))
        await second_started.wait()
        release_first.set()
        release_second.set()
        await asyncio.gather(first, second)

        assert len(recovery_scopes) == 2
        assert recovery_scopes[0] != recovery_scopes[1]
        assert seen == ["$first", "$second"]
        assert global_values == ["first", "second"]
        assert to_device_values == ["first", "second"]
        assert client._sliding_sync_to_device_since == "second-to-device"
        await client.close()

    async def test_classic_and_sliding_requests_may_overlap(self, tempdir, monkeypatch):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        classic_started = asyncio.Event()
        sliding_started = asyncio.Event()
        release = asyncio.Event()
        seen = record_events(client)

        async def send(response_class, *_args, **_kwargs):
            if response_class is SyncResponse:
                classic_started.set()
                await release.wait()
                response = sync_response(
                    "classic",
                    {
                        ROOM_A: room_info(
                            [text_event("$classic", 1)],
                            limited=False,
                            prev_batch=None,
                        )
                    },
                )
            else:
                sliding_started.set()
                await release.wait()
                response = self._sliding(
                    "sliding",
                    [text_event("$sliding", 2)],
                )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)
        classic = asyncio.create_task(client.sync())
        await classic_started.wait()
        sliding = asyncio.create_task(client.sliding_sync(conn_id="sliding"))
        await asyncio.wait_for(sliding_started.wait(), 0.5)
        release.set()
        await asyncio.gather(classic, sliding)

        assert set(seen) == {"$classic", "$sliding"}
        await client.close()

    async def test_default_mode_concurrent_sliding_preserves_late_unique_room_slice(
        self, monkeypatch
    ):
        client = AsyncClient("https://example.org", OWN_ID, "DEVICEID")
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        older_started = asyncio.Event()
        release_older = asyncio.Event()
        seen = record_events(client)

        async def send(*args, **_kwargs):
            request_pos = parse_qs(urlparse(args[2]).query)["pos"][0]
            if request_pos == "older":
                older_started.set()
                await release_older.wait()
            response = self._sliding(
                f"{request_pos}-response",
                [text_event(f"${request_pos}", 1)],
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        older = asyncio.create_task(client.sliding_sync(pos="older"))
        await older_started.wait()
        await client.sliding_sync(pos="newer")
        release_older.set()
        await older

        assert seen == ["$newer", "$older"]
        await client.close()

    async def test_late_continuation_preserves_unique_events_across_connections(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        a_continuation_started = asyncio.Event()
        release_a_continuation = asyncio.Event()
        seen = record_events(client)

        async def send(*args, **_kwargs):
            body = json.loads(args[3])
            conn_id = body["conn_id"]
            request_pos = parse_qs(urlparse(args[2]).query).get("pos", [None])[0]
            if conn_id == "a" and request_pos is not None:
                a_continuation_started.set()
                await release_a_continuation.wait()
            label = f"{conn_id}{'2' if request_pos else '1'}"
            response = self._sliding(
                f"{label}-pos",
                [text_event(f"${label}", len(seen))],
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        await client.sliding_sync(conn_id="a")
        await client.sliding_sync(conn_id="b")
        a_continuation = asyncio.create_task(
            client.sliding_sync(conn_id="a", pos="a1-pos")
        )
        await a_continuation_started.wait()
        b_continuation = asyncio.create_task(
            client.sliding_sync(conn_id="b", pos="b1-pos")
        )
        await b_continuation
        assert not a_continuation.done()
        release_a_continuation.set()
        await a_continuation

        assert seen == ["$a1", "$b1", "$b2", "$a2"]
        await client.close()

    async def test_independent_connections_update_shared_baseline_on_arrival(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        release_first = asyncio.Event()
        walk_starts: list[str | None] = []

        async def send(response_class, *_args, **_kwargs):
            if response_class is RoomMessagesResponse:
                query = parse_qs(urlparse(_args[1]).query)
                start = query.get("from", [None])[0]
                walk_starts.append(start)
                return RoomMessagesResponse.from_dict(
                    {"start": start, "end": "w3", "chunk": []},
                    ROOM_A,
                )

            body = json.loads(_args[2])
            conn_id = body["conn_id"]
            if conn_id == "a":
                first_started.set()
                await release_first.wait()
                token = "w1"
            elif conn_id == "b":
                second_started.set()
                token = "w2"
            else:
                token = "w3"
            response = self._sliding(
                f"{conn_id}-pos",
                [],
                limited=conn_id == "probe",
                prev_batch=token,
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        first = asyncio.create_task(client.sliding_sync(conn_id="a"))
        await first_started.wait()
        second = asyncio.create_task(client.sliding_sync(conn_id="b"))
        await second_started.wait()
        await second
        release_first.set()
        await first
        await client.sliding_sync(conn_id="probe")

        assert walk_starts == ["w1"]
        assert client._sliding_room_prev_batch == {
            ROOM_A: window_token("w3"),
        }
        await client.close()

    async def test_sequential_sliding_requests_advance_room_baseline(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        request_count = 0

        async def send(*_args, **_kwargs):
            nonlocal request_count
            request_count += 1
            response = self._sliding(
                f"p{request_count}",
                [],
                prev_batch=f"w{request_count}",
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        await client.sliding_sync(conn_id="main")
        await client.sliding_sync(conn_id="main")
        await client.sliding_sync(conn_id="main")

        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w3")}
        assert not client._sync_generation.classic_request_lock.locked()
        await client.close()

    async def test_named_connection_serializes_in_flight_requests(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        old_started = asyncio.Event()
        release_old = asyncio.Event()
        fresh_count = 0
        seen = record_events(client)

        async def send(*args, **_kwargs):
            nonlocal fresh_count
            request_pos = parse_qs(urlparse(args[2]).query).get("pos", [None])[0]
            if request_pos is not None:
                old_started.set()
                await release_old.wait()
                label, token = "old", "w-old"
            else:
                fresh_count += 1
                label = "seed" if fresh_count == 1 else "new"
                token = "w1" if fresh_count == 1 else "w2"
            response = self._sliding(
                f"{label}-pos",
                [text_event(f"${label}", fresh_count)],
                prev_batch=token,
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        await client.sliding_sync(conn_id="main")
        old = asyncio.create_task(client.sliding_sync(conn_id="main", pos="seed-pos"))
        await old_started.wait()
        new = asyncio.create_task(client.sliding_sync(conn_id="main"))
        await asyncio.sleep(0)
        assert not new.done()
        release_old.set()
        await old
        await new

        assert seen == ["$seed", "$old", "$new"]
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w2")}
        assert client._sync_generation.sliding_request_locks == {}
        await client.close()

    async def test_default_connection_serializes_in_flight_requests(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        old_started = asyncio.Event()
        release_old = asyncio.Event()
        fresh_count = 0
        seen = record_events(client)

        async def send(*args, **_kwargs):
            nonlocal fresh_count
            request_pos = parse_qs(urlparse(args[2]).query).get("pos", [None])[0]
            if request_pos is not None:
                old_started.set()
                await release_old.wait()
                label, token = "old", "w-old"
            else:
                fresh_count += 1
                label = "seed" if fresh_count == 1 else "new"
                token = "w1" if fresh_count == 1 else "w2"
            response = self._sliding(
                f"{label}-pos",
                [text_event(f"${label}", fresh_count)],
                prev_batch=token,
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        await client.sliding_sync()
        old = asyncio.create_task(client.sliding_sync(pos="seed-pos"))
        await old_started.wait()
        new = asyncio.create_task(client.sliding_sync())
        await asyncio.sleep(0)
        assert not new.done()
        release_old.set()
        await old
        await new

        assert seen == ["$seed", "$old", "$new"]
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w2")}
        assert client._sync_generation.sliding_request_locks == {}
        await client.close()

    async def test_many_sliding_connections_leave_no_request_lock_held(
        self, tempdir, monkeypatch
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        request_count = 0

        async def send(*_args, **_kwargs):
            nonlocal request_count
            request_count += 1
            response = self._sliding(
                f"p{request_count}",
                [],
                prev_batch=f"w{request_count}",
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        for index in range(1000):
            await client.sliding_sync(conn_id=f"connection-{index}")

        assert request_count == 1000
        assert not client._sync_generation.classic_request_lock.locked()
        assert client._sync_generation.sliding_request_locks == {}
        await client.close()

    async def test_independent_sliding_membership_delta_clears_existing_gap(
        self, tempdir, monkeypatch
    ):
        config = AsyncClientConfig(backfill_limited_timelines=True)
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("seed", [text_event("$seed", 0)], prev_batch="w0")
        )
        older_started = asyncio.Event()
        release_older = asyncio.Event()

        async def send(*args, **_kwargs):
            if args[0] is RoomMessagesResponse:
                raise RuntimeError("keep the newer recovery gap open")
            request_pos = parse_qs(urlparse(args[2]).query)["pos"][0]
            if request_pos == "older":
                older_started.set()
                await release_older.wait()
                response = self._sliding(
                    "older-response",
                    [],
                    membership="leave",
                )
            else:
                response = self._sliding(
                    "newer-response",
                    [text_event("$newer", 2)],
                    limited=True,
                    prev_batch="w2",
                )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        older = asyncio.create_task(client.sliding_sync(conn_id="older", pos="older"))
        await older_started.wait()
        newer = asyncio.create_task(client.sliding_sync(conn_id="newer", pos="newer"))
        await newer
        assert not older.done()
        release_older.set()
        await older

        assert ROOM_A not in client._recovery.gaps
        assert ROOM_A not in client._sliding_room_prev_batch
        await client.close()

    async def test_sliding_retry_stays_in_one_request_generation(
        self, tempdir, monkeypatch
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            max_limit_exceeded=1,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        generations = []
        responses = [
            SlidingSyncError.from_dict(
                {
                    "errcode": "M_LIMIT_EXCEEDED",
                    "error": "retry",
                    "retry_after_ms": 1,
                }
            ),
            SlidingSyncResponse.from_dict({"pos": "s1", "rooms": {}}),
        ]

        class Transport:
            def __init__(self, status):
                self.status = status

        async def send(*_args, **_kwargs):
            generations.append(client._sync_request_generation.get())
            return Transport(429 if len(generations) == 1 else 200)

        async def create_matrix_response(**_kwargs):
            return responses.pop(0)

        monkeypatch.setattr(client, "send", send)
        monkeypatch.setattr(client, "create_matrix_response", create_matrix_response)

        await client.sliding_sync()

        assert None not in generations
        assert generations[0] is generations[1]
        await client.close()

    async def test_sliding_retry_after_membership_reset_accepts_fresh_room(
        self, tempdir, monkeypatch
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
            max_limit_exceeded=1,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))

        def response(
            pos: str,
            membership_event_id: str,
            room_name: str,
            event_id: str,
            prev_batch: str,
        ) -> SlidingSyncResponse:
            value = SlidingSyncResponse.from_dict(
                {
                    "pos": pos,
                    "rooms": {
                        ROOM_A: {
                            "membership": "join",
                            "initial": True,
                            "limited": False,
                            "prev_batch": prev_batch,
                            "num_live": 1,
                            "required_state": [
                                member_event(
                                    membership_event_id,
                                    0,
                                    "join",
                                ).source,
                                name_event(
                                    f"${room_name.lower()}-name",
                                    1,
                                    room_name,
                                ).source,
                            ],
                            "timeline": [text_event(event_id, 2).source],
                        }
                    },
                }
            )
            assert isinstance(value, SlidingSyncResponse)
            return value

        await client.receive_response(response("seed", "$join1", "Old", "$seed", "w1"))
        seen = record_events(client)
        sliding_attempts = 0

        class Transport:
            def __init__(self, status):
                self.status = status

        async def send(_method, path, *_args, **_kwargs):
            nonlocal sliding_attempts
            if path.endswith("/leave"):
                return Transport(200)
            sliding_attempts += 1
            return Transport(429 if sliding_attempts == 1 else 200)

        async def create_matrix_response(*, response_class, **_kwargs):
            if response_class is RoomLeaveResponse:
                return RoomLeaveResponse.from_dict({})
            if sliding_attempts == 1:
                return SlidingSyncError.from_dict(
                    {
                        "errcode": "M_LIMIT_EXCEEDED",
                        "error": "retry",
                        "retry_after_ms": 1,
                    }
                )
            return response("s2", "$join2", "New", "$new", "w2")

        async def leave_on_retry(_responses):
            result = await client.room_leave(ROOM_A)
            assert isinstance(result, RoomLeaveResponse)

        monkeypatch.setattr(client, "send", send)
        monkeypatch.setattr(client, "create_matrix_response", create_matrix_response)
        monkeypatch.setattr(client, "run_response_callbacks", leave_on_retry)

        result = await client.sliding_sync(conn_id="main", pos="s1")

        expected_token = window_token("w2", "$join2")
        assert result.pos == "s2"
        assert seen == ["$new"]
        assert client.rooms[ROOM_A].name == "New"
        assert client._sliding_room_prev_batch == {ROOM_A: expected_token}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: expected_token}
        assert client._sync_reset_fence.active_request_ids == set()
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
                        "num_live": 500,
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
        assert rejoin.recovered_room_ids == frozenset()
        assert rejoin.unrecovered_room_ids == frozenset()
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
    async def test_membership_change_invalidates_only_after_server_success(
        self, tempdir, monkeypatch, operation
    ):
        """A successful remote departure applies local invalidation afterward."""
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

        def fail(*_args):
            raise RuntimeError("store unavailable")

        monkeypatch.setattr(client, "_send", send)
        monkeypatch.setattr(client.store, "save_recovery", fail)

        with pytest.raises(RuntimeError, match="store unavailable"):
            await getattr(client, operation)(ROOM_A)

        assert len(sent) == 1
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w1")}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}
        await client.close()

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_recovery_membership_change_from_callback_rejects_before_network(
        self, tempdir, monkeypatch, operation
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        membership_requests = 0

        async def callback(_room, _event):
            await getattr(client, operation)(ROOM_A)

        async def send(response_class, *_args, **_kwargs):
            nonlocal membership_requests
            if response_class is SyncResponse:
                response = sync_response(
                    "s1",
                    {
                        ROOM_A: room_info(
                            [text_event("$leave", 1)],
                            limited=False,
                            prev_batch=None,
                        )
                    },
                )
                await client.receive_response(response)
                return response
            membership_requests += 1
            if response_class is RoomForgetResponse:
                return RoomForgetResponse.from_dict({}, ROOM_A)
            return RoomLeaveResponse.from_dict({})

        client.add_event_callback(callback, RoomMessageText)
        monkeypatch.setattr(client, "_send", send)

        with pytest.raises(
            LocalProtocolError,
            match="cannot run from an event callback",
        ):
            await asyncio.wait_for(client.sync(), 0.5)

        assert membership_requests == 0
        await client.close()

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_awaited_callback_child_membership_change_rejects_before_network(
        self, tempdir, monkeypatch, operation
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        membership_requests = 0

        async def callback(_room, _event):
            await asyncio.create_task(getattr(client, operation)(ROOM_A))

        async def send(response_class, *_args, **_kwargs):
            nonlocal membership_requests
            if response_class is SyncResponse:
                response = sync_response(
                    "s1",
                    {
                        ROOM_A: room_info(
                            [text_event("$leave", 1)],
                            limited=False,
                            prev_batch=None,
                        )
                    },
                )
                await client.receive_response(response)
                return response
            membership_requests += 1
            if response_class is RoomForgetResponse:
                return RoomForgetError.from_dict(
                    {"errcode": "M_FORBIDDEN", "error": "rejected"},
                    ROOM_A,
                )
            return RoomLeaveError.from_dict(
                {"errcode": "M_FORBIDDEN", "error": "rejected"}
            )

        client.add_event_callback(callback, RoomMessageText)
        monkeypatch.setattr(client, "_send", send)

        with pytest.raises(
            LocalProtocolError,
            match="cannot run from an event callback",
        ):
            await asyncio.wait_for(client.sync(), 0.5)

        assert membership_requests == 0
        await client.close()

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_recovery_membership_change_in_detached_callback_child_runs_later(
        self, client, monkeypatch, operation
    ):
        membership_requests = 0
        membership_task: asyncio.Task | None = None

        async def callback(_room, _event):
            nonlocal membership_task
            membership_task = asyncio.create_task(getattr(client, operation)(ROOM_A))

        async def send(response_class, *_args, **_kwargs):
            nonlocal membership_requests
            if response_class is SyncResponse:
                response = sync_response(
                    "s1",
                    {
                        ROOM_A: room_info(
                            [text_event("$leave", 1)],
                            limited=False,
                            prev_batch=None,
                        )
                    },
                )
                await client.receive_response(response)
                return response
            membership_requests += 1
            if response_class is RoomForgetResponse:
                return RoomForgetResponse.from_dict({}, ROOM_A)
            return RoomLeaveResponse.from_dict({})

        client.add_event_callback(callback, RoomMessageText)
        monkeypatch.setattr(client, "_send", send)

        await asyncio.wait_for(client.sync(), 0.5)
        assert membership_task is not None
        await asyncio.wait_for(membership_task, 0.5)

        assert membership_requests == 1

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_default_membership_change_from_callback_keeps_upstream_behavior(
        self, tempdir, monkeypatch, operation
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        membership_requests = 0

        async def callback(_room, _event):
            await getattr(client, operation)(ROOM_A)

        async def send(response_class, *_args, **_kwargs):
            nonlocal membership_requests
            if response_class is SyncResponse:
                response = sync_response(
                    "s1",
                    {
                        ROOM_A: room_info(
                            [text_event("$leave", 1)],
                            limited=False,
                            prev_batch=None,
                        )
                    },
                )
                await client.receive_response(response)
                return response
            membership_requests += 1
            if response_class is RoomForgetResponse:
                return RoomForgetResponse.from_dict({}, ROOM_A)
            return RoomLeaveResponse.from_dict({})

        client.add_event_callback(callback, RoomMessageText)
        monkeypatch.setattr(client, "_send", send)

        await asyncio.wait_for(client.sync(), 0.5)

        assert membership_requests == 1
        await client.close()

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_default_membership_change_does_not_wait_for_sync_long_poll(
        self, tempdir, monkeypatch, operation
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        sync_started = asyncio.Event()
        release_sync = asyncio.Event()

        async def send(response_class, *_args, **_kwargs):
            if response_class is SyncResponse:
                sync_started.set()
                await release_sync.wait()
                response = sync_response("s1", {})
                await client.receive_response(response)
                return response
            if response_class is RoomForgetResponse:
                return RoomForgetResponse.from_dict({}, ROOM_A)
            return RoomLeaveResponse.from_dict({})

        monkeypatch.setattr(client, "_send", send)
        sync_task = asyncio.create_task(client.sync(timeout=30000))
        await sync_started.wait()
        membership_task = asyncio.create_task(getattr(client, operation)(ROOM_A))
        done, _ = await asyncio.wait((membership_task,), timeout=0.05)
        release_sync.set()
        await asyncio.gather(sync_task, membership_task)

        assert done == {membership_task}
        await client.close()

    @pytest.mark.parametrize("status", [403, 500])
    @pytest.mark.parametrize(
        ("operation", "error_type"),
        [
            ("room_leave", RoomLeaveError),
            ("room_forget", RoomForgetError),
        ],
    )
    async def test_membership_change_error_preserves_token(
        self, tempdir, monkeypatch, status, operation, error_type
    ):
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

        class Transport:
            def __init__(self, response_status):
                self.status = response_status

        error = {"errcode": "M_FORBIDDEN", "error": "membership change rejected"}
        response = (
            error_type.from_dict(error, ROOM_A)
            if error_type is RoomForgetError
            else error_type.from_dict(error)
        )
        response.transport_response = Transport(status)

        async def send(*_args, **_kwargs):
            return response

        monkeypatch.setattr(client, "_send", send)

        result = await getattr(client, operation)(ROOM_A)

        assert result is response
        expected = {ROOM_A: window_token("w1")}
        assert client._sliding_room_prev_batch == expected
        assert client.store.load_sliding_window_tokens() == expected
        await client.close()

    async def test_successful_leave_wins_over_crossing_sliding_response(
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
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        leave_started = asyncio.Event()
        release_leave = asyncio.Event()
        sliding_sent = asyncio.Event()

        async def send(response_class, *_args, **_kwargs):
            if response_class is RoomLeaveResponse:
                leave_started.set()
                await release_leave.wait()
                return RoomLeaveResponse.from_dict({})
            response = self._sliding(
                "s2",
                [text_event("$crossing", 2)],
                prev_batch="w2",
            )
            sliding_sent.set()
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        leave = asyncio.create_task(client.room_leave(ROOM_A))
        await leave_started.wait()
        sliding = asyncio.create_task(client.sliding_sync(pos="crossing"))
        await sliding_sent.wait()
        await asyncio.wait_for(sliding, 1)
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w2")}
        release_leave.set()
        await leave

        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    @pytest.mark.parametrize("protocol", ["classic", "sliding"])
    async def test_pre_reset_response_cannot_restore_target_room(
        self, tempdir, monkeypatch, protocol
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
            self._sliding("seed", [text_event("$seed", 1)], prev_batch="w1")
        )
        seen = record_events(client)
        global_values: list[str] = []
        sync_started = asyncio.Event()
        release_sync = asyncio.Event()

        async def global_callback(event):
            global_values.append(event.content["value"])

        client.add_global_account_data_callback(
            global_callback,
            UnknownAccountDataEvent,
        )

        async def send(response_class, *_args, **_kwargs):
            if response_class is RoomLeaveResponse:
                return RoomLeaveResponse.from_dict({})
            sync_started.set()
            await release_sync.wait()
            if response_class is SyncResponse:
                response = sync_response(
                    "s2",
                    {
                        ROOM_A: room_info(
                            [text_event("$stale", 2)],
                            limited=False,
                            prev_batch="p2",
                        ),
                        ROOM_B: room_info(
                            [text_event("$other", 3, ROOM_B)],
                            limited=False,
                            prev_batch="q2",
                        ),
                    },
                )
            else:
                response = SlidingSyncResponse.from_dict(
                    {
                        "pos": "slide2",
                        "rooms": {
                            ROOM_A: {
                                "membership": "join",
                                "prev_batch": "w2",
                                "required_state": [
                                    member_event("$membership", 0, "join").source
                                ],
                                "timeline": [text_event("$stale", 2).source],
                            },
                            ROOM_B: {
                                "membership": "join",
                                "prev_batch": "q2",
                                "required_state": [
                                    member_event(
                                        "$other-membership",
                                        0,
                                        "join",
                                    ).source
                                ],
                                "timeline": [text_event("$other", 3, ROOM_B).source],
                            },
                        },
                    }
                )
                assert isinstance(response, SlidingSyncResponse)
            response.account_data_events = [
                UnknownAccountDataEvent.from_dict(
                    {
                        "content": {"value": protocol},
                        "type": "org.example.settings",
                    }
                )
            ]
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        sync_task = asyncio.create_task(
            client.sync() if protocol == "classic" else client.sliding_sync()
        )
        await sync_started.wait()
        await client.room_leave(ROOM_A)
        release_sync.set()
        await sync_task

        assert "$stale" not in seen
        assert "$other" in seen
        assert global_values == [protocol]
        assert client._sliding_room_prev_batch.get(ROOM_A) is None
        assert client.store.load_sliding_window_tokens().get(ROOM_A) is None
        await client.close()

    async def test_successful_forget_invalidates_sliding_baseline(
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
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )

        async def send(*_args, **_kwargs):
            response = RoomForgetResponse.from_dict({}, ROOM_A)
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        await client.room_forget(ROOM_A)

        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_successful_membership_change_clears_all_room_recovery_durably(
        self, tempdir, monkeypatch, operation
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
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        pending = PendingTimelineEvent.from_event(
            ROOM_A,
            1,
            0,
            text_event("$pending", 2),
            False,
        )
        assert pending is not None
        persist_response_plan(
            client._recovery,
            client.store,
            token=None,
            plan=RecoveryPlan(
                gaps=(RecoveryGap(ROOM_A, 1, "target", "cursor"),),
                events=(pending,),
            ),
        )

        async def send(response_class, *_args, **_kwargs):
            if response_class is RoomForgetResponse:
                return RoomForgetResponse.from_dict({}, ROOM_A)
            return RoomLeaveResponse.from_dict({})

        monkeypatch.setattr(client, "_send", send)

        await getattr(client, operation)(ROOM_A)

        assert ROOM_A not in client._recovery.gaps
        assert not any(room_id == ROOM_A for room_id, _ in client._recovery.events)
        assert client._sliding_room_prev_batch == {}
        gaps, events = client.store.load_sync_recovery()
        assert [gap for gap in gaps if gap.room_id == ROOM_A] == []
        assert [
            event
            for event in events
            if event.room_id == ROOM_A and event.generation > 0
        ] == []
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_disabled_membership_change_clears_prior_recovery_durably(
        self, tempdir, monkeypatch, operation
    ):
        enabled = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                store_sync_tokens=True,
            ),
        )
        await enabled.receive_response(LoginResponse.from_dict(LOGIN))
        pending = PendingTimelineEvent.from_event(
            ROOM_A,
            1,
            0,
            text_event("$pending", 2),
            False,
        )
        assert pending is not None
        persist_response_plan(
            enabled._recovery,
            enabled.store,
            token=None,
            plan=RecoveryPlan(
                gaps=(RecoveryGap(ROOM_A, 1, "target", "cursor"),),
                events=(pending,),
            ),
        )
        enabled.store.save_sliding_window_tokens({ROOM_A: window_token("w1")})
        await enabled.close()

        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(),
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))

        async def send(response_class, *_args, **_kwargs):
            if response_class is RoomForgetResponse:
                return RoomForgetResponse.from_dict({}, ROOM_A)
            return RoomLeaveResponse.from_dict({})

        monkeypatch.setattr(client, "_send", send)
        await getattr(client, operation)(ROOM_A)

        gaps, events = client.store.load_sync_recovery()
        assert [gap for gap in gaps if gap.room_id == ROOM_A] == []
        assert [
            event
            for event in events
            if event.room_id == ROOM_A and event.generation > 0
        ] == []
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    async def test_cancelled_successful_leave_drains_atomic_room_reset(
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
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        pending = PendingTimelineEvent.from_event(
            ROOM_A,
            1,
            0,
            text_event("$pending", 2),
            False,
        )
        assert pending is not None
        persist_response_plan(
            client._recovery,
            client.store,
            token=None,
            plan=RecoveryPlan(
                gaps=(RecoveryGap(ROOM_A, 1, "target", "cursor"),),
                events=(pending,),
            ),
        )
        reset_started = asyncio.Event()
        release_reset = asyncio.Event()
        original_drain = async_client_module.drain_recovery_room_dispatches

        async def blocked_drain(state, room_ids):
            reset_started.set()
            await release_reset.wait()
            await original_drain(state, room_ids)

        async def send(*_args, **_kwargs):
            return RoomLeaveResponse.from_dict({})

        monkeypatch.setattr(
            async_client_module,
            "drain_recovery_room_dispatches",
            blocked_drain,
        )
        monkeypatch.setattr(client, "_send", send)

        leave = asyncio.create_task(client.room_leave(ROOM_A))
        await reset_started.wait()
        leave.cancel()
        await asyncio.sleep(0)
        leave.cancel()
        await asyncio.sleep(0)
        assert not leave.done()
        release_reset.set()
        with pytest.raises(asyncio.CancelledError):
            await leave

        assert ROOM_A not in client._recovery.gaps
        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sync_recovery()[0] == []
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    async def test_owned_failure_clears_caught_cancellation_requests(self):
        started = asyncio.Event()
        release = asyncio.Event()

        async def operation():
            started.set()
            await release.wait()
            raise RuntimeError("owned failure")

        async def run():
            with pytest.raises(RuntimeError, match="owned failure"):
                await async_client_module._run_to_completion(operation())
            if sys.version_info < (3, 11):
                return None
            task = asyncio.current_task()
            assert task is not None
            return task.cancelling()

        task = asyncio.create_task(run())
        await started.wait()
        task.cancel()
        await asyncio.sleep(0)
        task.cancel()
        await asyncio.sleep(0)
        release.set()

        expected_cancellations = 0 if sys.version_info >= (3, 11) else None
        assert await task == expected_cancellations

    async def test_run_to_completion_drains_before_propagating_base_exception(
        self, monkeypatch
    ):
        class CallerExit(BaseException):
            pass

        finished = asyncio.Event()
        original_shield = asyncio.shield
        shield_calls = 0

        async def operation():
            await asyncio.sleep(0)
            finished.set()

        def interrupt_once(awaitable):
            nonlocal shield_calls
            shield_calls += 1
            if shield_calls == 1:
                raise CallerExit
            return original_shield(awaitable)

        monkeypatch.setattr(async_client_module.asyncio, "shield", interrupt_once)

        with pytest.raises(CallerExit):
            await async_client_module._run_to_completion(operation())

        assert finished.is_set()

    async def test_rejected_leave_does_not_affect_in_flight_response(
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
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )
        sliding_started = asyncio.Event()
        release_sliding = asyncio.Event()

        class Transport:
            status = 403

        rejected = RoomLeaveError.from_dict(
            {"errcode": "M_FORBIDDEN", "error": "leave rejected"}
        )
        rejected.transport_response = Transport()

        async def send(response_class, *_args, **_kwargs):
            if response_class is RoomLeaveResponse:
                return rejected
            sliding_started.set()
            await release_sliding.wait()
            response = self._sliding(
                "s2",
                [text_event("$valid", 2)],
                prev_batch="w2",
            )
            await client.receive_response(response)
            return response

        monkeypatch.setattr(client, "_send", send)

        sliding = asyncio.create_task(client.sliding_sync(pos="in-flight"))
        await sliding_started.wait()
        await client.room_leave(ROOM_A)
        release_sliding.set()
        await sliding

        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w2")}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w2")}
        await client.close()

    async def test_concurrent_rejected_leaves_do_not_serialize_network(
        self, tempdir, monkeypatch
    ):
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
        first_started = asyncio.Event()
        second_started = asyncio.Event()
        release_first = asyncio.Event()
        calls = 0

        class Transport:
            status = 403

        response = RoomLeaveError.from_dict(
            {"errcode": "M_FORBIDDEN", "error": "leave rejected"}
        )
        response.transport_response = Transport()

        async def send(*_args, **_kwargs):
            nonlocal calls
            calls += 1
            if calls == 1:
                first_started.set()
                await release_first.wait()
            else:
                second_started.set()
            return response

        monkeypatch.setattr(client, "_send", send)
        first = asyncio.create_task(client.room_leave(ROOM_A))
        await first_started.wait()
        second = asyncio.create_task(client.room_leave(ROOM_A))
        await asyncio.sleep(0)
        second_started_before_first_finished = second_started.is_set()
        release_first.set()
        await asyncio.gather(first, second)

        assert second_started_before_first_finished
        assert calls == 2
        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w1")}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w1")}
        await client.close()

    async def test_rejected_leave_does_not_persist_when_recovery_store_is_disabled(
        self, tempdir, monkeypatch
    ):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
            backfill_persist_recovery=False,
        )
        client = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await client.receive_response(
            self._sliding("s1", [text_event("$before", 1)], prev_batch="w1")
        )

        class Transport:
            status = 403

        response = RoomLeaveError.from_dict(
            {"errcode": "M_FORBIDDEN", "error": "leave rejected"}
        )
        response.transport_response = Transport()

        async def send(*_args, **_kwargs):
            return response

        monkeypatch.setattr(client, "_send", send)
        await client.room_leave(ROOM_A)

        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w1")}
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    async def test_rejected_leave_does_not_replace_newer_accepted_token(
        self, tempdir, monkeypatch
    ):
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
        leave_started = asyncio.Event()
        release_leave = asyncio.Event()

        class Transport:
            status = 403

        response = RoomLeaveError.from_dict(
            {"errcode": "M_FORBIDDEN", "error": "leave rejected"}
        )
        response.transport_response = Transport()

        async def send(*_args, **_kwargs):
            leave_started.set()
            await release_leave.wait()
            return response

        monkeypatch.setattr(client, "_send", send)
        leave = asyncio.create_task(client.room_leave(ROOM_A))
        await leave_started.wait()

        sliding = asyncio.create_task(
            client.receive_response(
                self._sliding(
                    "s2",
                    [text_event("$newer", 2)],
                    prev_batch="w2",
                )
            )
        )

        await asyncio.wait_for(sliding, 1)
        release_leave.set()
        await leave

        assert client._sliding_room_prev_batch == {ROOM_A: window_token("w2")}
        assert client.store.load_sliding_window_tokens() == {ROOM_A: window_token("w2")}
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

        assert seen == ["$gap", "$held"]
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
        record_completed_timeline_event(
            client._recovery,
            ROOM_A,
            "$shared",
            True,
            TimelineEventProvenance.LIVE,
        )
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
        assert seen == ["$shared", "$held"]
        assert client._recovery.completed[ROOM_A]["$shared"].was_encrypted is False

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
        assert client._recovery.completed[ROOM_A]["$shared"].was_encrypted is False
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

        def fail_third_ack(room_id, generation, event_id, was_encrypted):
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
        assert seen == ["$gap1", "$gap2", "$held"]
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
        assert seen == ["$gap1", "$gap2", "$held", "$held"]
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
        # The sliding response pumps the open gap too; without a page for it
        # the walk burns the whole backfill deadline before moving on.
        aioresponse.get(MESSAGES_URL, payload=messages([], "more2"))
        await client.receive_response(sliding)
        assert seen == ["$old"]

        aioresponse.get(
            MESSAGES_URL,
            payload=messages(
                [text_event("$gap2", 3), text_event("$held", 4)],
                "p1",
            ),
        )
        await client.receive_response(limited)
        assert seen == ["$old", "$gap", "$gap2", "$held", "$slide"]
        assert not client._recovery.gaps

    @staticmethod
    def _sliding(
        pos: str,
        events: list,
        *,
        limited: bool = False,
        prev_batch: str | None = None,
        initial: bool = False,
        expanded_timeline: bool = False,
        num_live: int | None = None,
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
        if expanded_timeline:
            room["expanded_timeline"] = True
        if num_live is not None:
            room["num_live"] = num_live
        response = SlidingSyncResponse.from_dict({"pos": pos, "rooms": {ROOM_A: room}})
        assert isinstance(response, SlidingSyncResponse)
        return response

    async def test_sliding_seed_ranges_accept_tuple_ranges(self):
        assert AsyncClient._sliding_seed_ranges(((10, 20), (30, 40)), 5) == [
            [0, 4],
            [10, 20],
            [30, 40],
        ]

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

        assert seen_at_fetch == [["$first"]]
        assert seen == ["$first", "$gap", "$held"]
        assert pages.from_tokens == ["w1"]
        assert pages.to_tokens == ["w2"]
        assert not client._recovery.gaps

    async def test_sliding_without_own_membership_has_no_unsafe_baseline(
        self, tempdir, aioresponse
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
        seen = record_events(client)
        await client.receive_response(
            self._sliding(
                "s1",
                [text_event("$first", 1)],
                prev_batch="w1",
                membership_event_id=None,
            )
        )

        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sliding_window_tokens() == {}

        response = self._sliding(
            "s2",
            [text_event("$held", 3)],
            limited=True,
            prev_batch="w2",
            membership_event_id=None,
        )
        await client.receive_response(response)

        assert seen == ["$first", "$held"]
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})
        assert client._sliding_room_prev_batch == {}
        assert client.store.load_sliding_window_tokens() == {}
        await client.close()

    async def test_noninitial_membership_delta_preserves_exact_held_proof(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            self._sliding(
                "s1",
                [text_event("$first", 1)],
                prev_batch="w1",
                membership_event_id="$member-1",
            )
        )
        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        response = self._sliding(
            "s2",
            [text_event("$held", 3)],
            limited=True,
            prev_batch="w2",
            membership_event_id=None,
        )

        await client.receive_response(response)

        assert pages.from_tokens == ["w1"]
        assert seen == ["$first", "$gap", "$held"]
        assert client._sliding_room_prev_batch == {
            ROOM_A: window_token("w2", "$member-1")
        }
        assert response.recovered_room_ids == frozenset({ROOM_A})
        assert response.unrecovered_room_ids == frozenset()

    async def test_initial_membership_omission_discards_held_proof(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            self._sliding(
                "s1",
                [text_event("$first", 1)],
                prev_batch="w1",
                membership_event_id="$member-1",
            )
        )
        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        response = self._sliding(
            "s2",
            [text_event("$held", 3)],
            limited=True,
            prev_batch="w2",
            initial=True,
            membership_event_id=None,
        )

        await client.receive_response(response)

        assert pages.from_tokens == []
        assert seen == ["$first", "$held"]
        assert client._sliding_room_prev_batch == {}
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})

    async def test_unverified_baseline_is_not_restored_after_restart(self, tempdir):
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        seed = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await seed.receive_response(LoginResponse.from_dict(LOGIN))
        await seed.receive_response(
            self._sliding(
                "s1",
                [text_event("$first", 1)],
                prev_batch="w1",
                membership_event_id=None,
            )
        )
        await seed.close()
        seed.store.database.close()

        restarted = AsyncClient(
            "https://example.org", OWN_ID, "DEVICEID", tempdir, config=config
        )
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        response = self._sliding(
            "s2",
            [text_event("$held", 3)],
            limited=True,
            prev_batch="w2",
            membership_event_id=None,
        )
        await restarted.receive_response(response)

        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset()
        await restarted.close()

    async def test_unlinked_profile_change_rejects_held_baseline(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            self._sliding(
                "s1",
                [text_event("$first", 1)],
                prev_batch="w1",
                membership_event_id="$member-1",
            )
        )
        profile = member_event("$member-2", 2, "join").source
        profile["unsigned"] = {"prev_content": {"membership": "join"}}
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s2",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "limited": True,
                        "prev_batch": "w2",
                        "required_state": [profile],
                        "timeline": [text_event("$held", 3).source],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)
        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await client.receive_response(response)

        assert seen == ["$first", "$held"]
        assert pages.from_tokens == []
        assert client._sliding_room_prev_batch == {}
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset({ROOM_A})

    async def test_exact_linked_profile_change_rotates_held_membership_proof(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            self._sliding(
                "s1",
                [text_event("$first", 1)],
                prev_batch="w1",
                membership_event_id="$member-1",
            )
        )
        profile = member_event("$member-2", 2, "join").source
        profile["unsigned"] = {
            "prev_content": {"membership": "join"},
            "replaces_state": "$member-1",
        }
        update = SlidingSyncResponse.from_dict(
            {
                "pos": "s2",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "required_state": [profile],
                        "timeline": [],
                    }
                },
            }
        )
        assert isinstance(update, SlidingSyncResponse)
        await client.receive_response(update)
        assert client._sliding_room_prev_batch == {
            ROOM_A: window_token("w1", "$member-2")
        }

        pages = Pages({"w1": messages([text_event("$gap", 2)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        limited = self._sliding(
            "s3",
            [text_event("$held", 3)],
            limited=True,
            prev_batch="w2",
            membership_event_id=None,
        )
        await client.receive_response(limited)

        assert pages.from_tokens == ["w1"]
        assert seen == ["$first", "$gap", "$held"]
        assert client._sliding_room_prev_batch == {
            ROOM_A: window_token("w2", "$member-2")
        }
        assert limited.recovered_room_ids == frozenset({ROOM_A})

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
        assert seen == ["$first", "$gap", "$held"]
        assert pages.from_tokens == ["w1"]
        assert not client._recovery.gaps

    async def test_initial_sliding_room_without_baseline_plans_no_walk(
        self, client, aioresponse
    ):
        """A room seen for the first time has no token to walk from."""
        seen = record_events(client)
        response = self._sliding(
            "s1",
            [text_event("$held", 1)],
            prev_batch="w1",
            initial=True,
        )
        await client.receive_response(response)
        assert seen == ["$held"]
        assert not client._recovery.gaps
        assert response.recovered_room_ids == frozenset()
        assert response.unrecovered_room_ids == frozenset()

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
        assert seen == []
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
        assert seen == ["$prejoin2", "$join", "$after"]
        assert not client._recovery.gaps
        assert sliding.recovered_room_ids == frozenset()
        assert sliding.unrecovered_room_ids == frozenset({ROOM_A})

    @pytest.mark.parametrize(
        "timeline_shape",
        [{"initial": True}, {"expanded_timeline": True}],
        ids=["initial", "expanded"],
    )
    async def test_sliding_historical_join_keeps_classic_gap(
        self, client, aioresponse, timeline_shape
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
                        **timeline_shape,
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
        assert seen == []
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "more2"
        assert [event.event_id for event in client._recovery.events[(ROOM_A, 1)]] == [
            "$gap",
            "$held",
            "$join",
            "$after",
        ]

    async def test_sliding_initial_historical_join_does_not_hide_window_gap(
        self, client, aioresponse
    ):
        seen = record_events(client)
        await client.receive_response(
            self._sliding(
                "s1",
                [text_event("$first", 1)],
                prev_batch="w1",
                membership_event_id="$member",
            )
        )
        pages = Pages({"w1": messages([text_event("$gap", 3)], "w2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s2",
                "rooms": {
                    ROOM_A: {
                        "initial": True,
                        "membership": "join",
                        "num_live": 1,
                        "limited": True,
                        "prev_batch": "w2",
                        "required_state": [member_event("$member", 0, "join").source],
                        "timeline": [
                            member_event("$historical-join", 2, "join").source,
                            text_event("$held", 4).source,
                        ],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)

        await client.receive_response(response)

        assert seen == ["$first", "$gap", "$held"]
        assert pages.from_tokens == ["w1"]
        assert response.recovered_room_ids == frozenset({ROOM_A})
        assert response.unrecovered_room_ids == frozenset()

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
        assert client._recovery.events[(ROOM_A, 1)] == []
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
        record_completed_timeline_event(
            client._recovery,
            ROOM_A,
            "$same",
            False,
            TimelineEventProvenance.LIVE,
        )
        record_completed_timeline_event(
            client._recovery,
            ROOM_A,
            "$same",
            True,
            TimelineEventProvenance.LIVE,
        )
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
        assert seen == ["$gap", "$held"]
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
                        "required_state": [
                            member_event("$membership", 0, "join").source
                        ],
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
                        "required_state": [
                            member_event("$membership", 0, "join").source
                        ],
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
                            "required_state": [
                                member_event("$membership", 0, "join").source
                            ],
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
                            "required_state": [
                                member_event("$membership", 0, "join").source
                            ],
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
            assert client._sliding_room_prev_batch[ROOM_A] == window_token("w1")

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

    async def test_close_waits_for_active_sync_before_closing_session(
        self, client, monkeypatch
    ):
        planning_started = asyncio.Event()
        release_planning = asyncio.Event()
        callback_saw_closed: list[bool] = []
        original_room_state = client._recovery_room_state

        class Session:
            closed = False

            async def close(self):
                self.closed = True

        class BlockBeforePlan:
            def __init__(self, room_ids):
                self.room_ids = room_ids
                self.inner = None

            async def __aenter__(self):
                planning_started.set()
                await release_planning.wait()
                self.inner = original_room_state(self.room_ids)
                return await self.inner.__aenter__()

            async def __aexit__(self, *args):
                assert self.inner is not None
                return await self.inner.__aexit__(*args)

        session = Session()
        client.client_session = session
        monkeypatch.setattr(
            client,
            "_recovery_room_state",
            lambda room_ids: BlockBeforePlan(room_ids),
        )

        async def callback(_room, _event):
            callback_saw_closed.append(session.closed)

        client.add_event_callback(callback, RoomMessageText)
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [text_event("$live", 1).source],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)
        sync = asyncio.create_task(client.receive_response(response))
        await planning_started.wait()

        close = asyncio.create_task(client.close())
        await asyncio.sleep(0)
        closed_before_release = session.closed
        release_planning.set()
        await sync
        await close

        assert not closed_before_release
        assert callback_saw_closed == [False]

    async def test_close_fences_retry_from_old_sync_generation(
        self, client, monkeypatch
    ):
        request_started = asyncio.Event()
        release_request = asyncio.Event()
        sessions = []

        class Connector:
            connect = None

        class Transport:
            status = 200
            content_type = "application/json"
            content_disposition = None

            async def json(self):
                return {"next_batch": "late", "rooms": {}}

        class Session:
            def __init__(self, **_kwargs):
                self.closed = False
                self.connector = Connector()
                sessions.append(self)

            async def request(self, *_args, **_kwargs):
                if len(sessions) == 1:
                    request_started.set()
                    await release_request.wait()
                    raise asyncio.TimeoutError
                return Transport()

            async def close(self):
                self.closed = True

        monkeypatch.setattr(async_client_module, "ClientSession", Session)

        sync = asyncio.create_task(client.sync())
        await request_started.wait()
        await client.close()
        release_request.set()

        with pytest.raises(asyncio.TimeoutError):
            await asyncio.wait_for(sync, 1)

        assert len(sessions) == 1
        assert sessions[0].closed
        assert client.client_session is None

    async def test_sync_response_processing_resumes_after_close(self, client):
        seen = record_events(client)
        await client.close()
        response = SlidingSyncResponse.from_dict(
            {
                "pos": "s1",
                "rooms": {
                    ROOM_A: {
                        "membership": "join",
                        "timeline": [text_event("$reopened", 1).source],
                    }
                },
            }
        )
        assert isinstance(response, SlidingSyncResponse)

        await client.receive_response(response)

        assert seen == ["$reopened"]

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


def own_member_event(
    event_id: str,
    membership: str = "join",
    *,
    prev_membership: str | None = None,
    replaces_state: str | None = None,
) -> RoomMemberEvent:
    """Our own membership event, optionally linked to the one it replaces."""
    unsigned: dict = {}
    if prev_membership is not None:
        unsigned["prev_content"] = {"membership": prev_membership}
    if replaces_state is not None:
        unsigned["replaces_state"] = replaces_state
    source = {
        "content": {"membership": membership},
        "event_id": event_id,
        "sender": OWN_ID,
        "state_key": OWN_ID,
        "origin_server_ts": 0,
        "room_id": ROOM_A,
        "type": "m.room.member",
    }
    if unsigned:
        source["unsigned"] = unsigned
    event = RoomMemberEvent.from_dict(source)
    assert isinstance(event, RoomMemberEvent)
    return event


def proof_room(
    *,
    required_state: list | None = None,
    timeline: list | None = None,
    initial: bool = False,
    num_live: int | None = None,
) -> SlidingSyncRoom:
    return SlidingSyncRoom(
        membership="join",
        initial=initial,
        required_state=list(required_state or []),
        timeline=list(timeline or []),
        num_live=num_live,
    )


@pytest.mark.asyncio
class TestSlidingMembershipProof:
    """Direct coverage for the predicate guarding persisted walk baselines.

    A wrong answer here lets a restarted client walk a room's history from a
    token taken under a membership that has since ended.
    """

    @pytest.mark.parametrize(
        ("held", "room", "expected"),
        [
            pytest.param(
                None,
                proof_room(required_state=[own_member_event("$leave", "leave")]),
                None,
                id="explicit_membership_loss_fails_closed",
            ),
            pytest.param(
                None,
                proof_room(required_state=[own_member_event("$m1")]),
                "$m1",
                id="no_held_baseline_accepts_current_membership",
            ),
            pytest.param(
                "$m1",
                proof_room(required_state=[own_member_event("$m1")]),
                "$m1",
                id="exact_match_with_held_baseline",
            ),
            pytest.param(
                "$old",
                proof_room(timeline=[own_member_event("$new")]),
                "$new",
                id="live_own_join_supersedes_mismatched_baseline",
            ),
            pytest.param(
                "$m1",
                proof_room(
                    required_state=[
                        own_member_event(
                            "$m2", prev_membership="join", replaces_state="$m1"
                        )
                    ]
                ),
                "$m2",
                id="linked_join_to_join_rotation",
            ),
            pytest.param(
                "$m1",
                proof_room(
                    required_state=[
                        own_member_event(
                            "$m2", prev_membership="join", replaces_state="$other"
                        )
                    ]
                ),
                None,
                id="rotation_replacing_a_different_event_fails_closed",
            ),
            pytest.param(
                "$m1",
                proof_room(
                    required_state=[
                        own_member_event(
                            "$m2", prev_membership="invite", replaces_state="$m1"
                        )
                    ]
                ),
                None,
                id="rotation_from_non_join_fails_closed",
            ),
            pytest.param(
                "$m1",
                proof_room(required_state=[own_member_event("$m2")]),
                None,
                id="required_state_join_alone_does_not_date_a_rejoin",
            ),
            pytest.param(
                "$m1",
                proof_room(
                    required_state=[SlidingSyncStateStub("m.room.member", OWN_ID)]
                ),
                None,
                id="unparsed_membership_stub_fails_closed",
            ),
            pytest.param(
                "$m1",
                proof_room(),
                "$m1",
                id="no_membership_delta_keeps_held_baseline",
            ),
            pytest.param(
                "$m1",
                proof_room(initial=True),
                None,
                id="initial_window_without_membership_proof_fails_closed",
            ),
            pytest.param(
                None,
                proof_room(),
                None,
                id="no_membership_delta_and_no_held_baseline",
            ),
            pytest.param(
                "$m1",
                proof_room(
                    required_state=[own_member_event("$m2")],
                    timeline=[own_member_event("$historical"), text_event("$t", 0)],
                    initial=True,
                    num_live=1,
                ),
                None,
                id="historical_join_outside_num_live_is_not_a_live_join",
            ),
        ],
    )
    async def test_membership_proof(self, client, held, room, expected):
        if held is not None:
            client._sliding_room_prev_batch[ROOM_A] = window_token("w1", held)

        assert client._sliding_membership_proof(ROOM_A, room) == expected

    async def test_live_own_join_proves_membership_but_breaks_continuity(self, client):
        """A rejoin dates the new membership yet invalidates the old baseline."""
        client._sliding_room_prev_batch[ROOM_A] = window_token("w1", "$old")
        room = proof_room(timeline=[own_member_event("$new")])

        assert client._sliding_membership_proof(ROOM_A, room) == "$new"
        assert client._sliding_membership_continues(ROOM_A, room) is False

    async def test_unchanged_membership_continues(self, client):
        client._sliding_room_prev_batch[ROOM_A] = window_token("w1", "$m1")
        room = proof_room(required_state=[own_member_event("$m1")])

        assert client._sliding_membership_continues(ROOM_A, room) is True
