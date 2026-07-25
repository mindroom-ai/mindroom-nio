"""Integration tests for durable room-local limited-sync recovery."""

import asyncio
import json
import re
from pathlib import Path
from urllib.parse import parse_qs, urlparse

import pytest
import pytest_asyncio
from aioresponses import CallbackResult

from nio import (
    AsyncClient,
    AsyncClientConfig,
    DeviceList,
    DeviceOneTimeKeyCount,
    Event,
    FullyReadEvent,
    InviteInfo,
    LoginResponse,
    MegolmEvent,
    PresenceEvent,
    RoomEncryptedImage,
    RoomInfo,
    RoomMemberEvent,
    RoomMessageText,
    RoomNameEvent,
    Rooms,
    SlidingSyncResponse,
    SyncResponse,
    Timeline,
    TypingNoticeEvent,
    UnknownBadEvent,
)
from nio.api import MATRIX_API_PATH_V3
from nio.client.sync_recovery import (
    record_completed_timeline_event,
    should_dispatch_timeline_event,
)

BASE_URL = f"https://example.org{MATRIX_API_PATH_V3}"
MESSAGES_URL = re.compile(rf"^{BASE_URL}/rooms/.+/messages")
SYNC_URL = re.compile(rf"^{BASE_URL}/sync")
ROOM_A = "!a:example.org"
ROOM_B = "!b:example.org"
LOGIN = json.loads(Path("tests/data/login_response.json").read_text())
OWN_ID = LOGIN["user_id"]


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
        info.ephemeral.append(TypingNoticeEvent([OWN_ID]))
        info.account_data.append(FullyReadEvent(event_id="$event"))
        await client.receive_response(
            sync_response(
                "s1",
                {ROOM_A: info},
                presence=[PresenceEvent(OWN_ID, "online")],
            )
        )
        assert seen == [
            "RoomMessageText",
            "TypingNoticeEvent",
            "FullyReadEvent",
            "PresenceEvent",
        ]

    async def test_gap_and_live_window_dispatch_in_order(self, client, aioresponse):
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
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
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

    async def test_event_bound_resumes_from_persisted_cursor(
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
        assert seen == ["$old"]
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "more"

        await client.receive_response(limited)
        assert pages.from_tokens == ["s1", "more"]
        assert seen == ["$old", "$gap1", "$gap2", "$held"]
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

    async def test_newer_same_room_event_cannot_overtake_gap(self, client, aioresponse):
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

    async def test_ignored_to_abandons_untrusted_page_and_releases_live(
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
            payload=messages(
                [
                    text_event("$gap", 2),
                    text_event("$held", 3),
                    text_event("$future", 4),
                ],
                None,
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
        assert seen == ["$old", "$held"]
        assert "$future" not in seen
        assert not client._recovery.gaps

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

        async def on_presence(event):
            presence_seen.append(event.user_id)

        async def on_response(response):
            if isinstance(response, SyncResponse):
                response_seen.append(response.next_batch)

        first.add_presence_callback(on_presence, PresenceEvent)
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
        limited = sync_response(
            "s2",
            {
                ROOM_A: room_info(
                    [text_event("$held", 4)], limited=True, prev_batch="p1"
                )
            },
            presence=[PresenceEvent("@sender:example.org", "online")],
        )
        await first.receive_response(limited)
        await first.run_response_callbacks([limited])
        assert first_seen == ["$old"]
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
            "before:$gap",
            "fail:$gap",
            "after:$gap",
            "before:$held",
            "fail:$held",
            "after:$held",
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
            "before:$gap",
            "fail:$gap",
            "after:$gap",
            "before:$held",
            "fail:$held",
            "after:$held",
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

        def fail_second_ack(room_id, generation, event_id, was_encrypted):
            nonlocal acknowledgements
            if event_id:
                acknowledgements += 1
            if acknowledgements == 2:
                raise RuntimeError("ack failed")
            original_finish(room_id, generation, event_id, was_encrypted)

        monkeypatch.setattr(first.store, "finish_recovery", fail_second_ack)
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
        with pytest.raises(RuntimeError, match="ack failed"):
            await first.receive_response(
                sync_response(
                    "s2",
                    {
                        ROOM_A: room_info(
                            [text_event("$held", 4)],
                            limited=True,
                            prev_batch="p1",
                        )
                    },
                )
            )
        assert seen == ["$gap1", "$gap2"]
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
        assert seen == ["$gap1", "$gap2", "$gap2", "$held"]
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

    async def test_sliding_own_join_discards_classic_prejoin_rows(
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
        assert seen == ["$join", "$after"]
        assert not client._recovery.gaps

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
        assert seen == []
        assert client._recovery.gaps[ROOM_A][0].cursor_token == "more2"
        assert [event.event_id for event in client._recovery.events[(ROOM_A, 1)]] == [
            "$gap",
            "$held",
            "$join",
            "$after",
        ]

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
        gaps, events = client.store.load_sync_recovery()
        assert gaps == []
        assert list(client._recovery.completed[ROOM_A]) == ["$seen"]
        assert [(event.event_id, event.generation) for event in events] == [
            ("$seen", 0)
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
        assert list(restarted._recovery.completed[ROOM_A]) == ["$seen"]
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

        await client.receive_response(
            sync_response(
                "s3",
                {},
                invited=({ROOM_A: InviteInfo([])} if membership == "invite" else None),
                left=(
                    {ROOM_A: room_info([], limited=False, prev_batch=None)}
                    if membership == "leave"
                    else None
                ),
            )
        )
        assert not client._recovery.gaps
        gaps, events = client.store.load_sync_recovery()
        assert gaps == []
        assert list(client._recovery.completed[ROOM_A]) == ["$seen"]
        assert [(event.event_id, event.generation) for event in events] == [
            ("$seen", 0)
        ]
        await client.close()

    async def test_sliding_callback_failure_is_terminal(self, client):
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
        await client.receive_response(sliding)
        await client.receive_response(sliding)
        assert calls == [
            "first:$a",
            "failing:$a",
            "last:$a",
            "first:$b",
            "failing:$b",
            "last:$b",
        ]

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

        prepared = []
        original_prepare = client._prepare_to_device

        def prepare(response):
            prepared.append(response)
            return original_prepare(response)

        monkeypatch.setattr(client, "_prepare_to_device", prepare)
        monkeypatch.setattr(client.store, "save_recovery", fail)
        with pytest.raises(RuntimeError, match="commit failed"):
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
        assert client.next_batch == "s1"
        assert client.store.load_sync_token() == "s1"
        assert not client._recovery.gaps
        assert prepared == []
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
        prepared = []
        monkeypatch.setattr(
            client,
            "_prepare_to_device",
            lambda response: prepared.append(response) or [],
        )

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
        assert prepared == []
        assert not client._recovery.gaps
        await client.close()

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

    async def test_current_timeline_starts_at_last_own_join(self, client):
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
                        limited=True,
                        prev_batch="p1",
                    )
                },
            )
        )
        assert seen == ["$join", "$after"]
        assert not client._recovery.gaps
