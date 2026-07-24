"""Tests for limited-timeline backfill in the async client.

These exercise the opt-in ``backfill_limited_timelines`` behaviour end to end:
a limited sync timeline should page ``/messages`` backwards from
``prev_batch`` to the request's ``since`` token and dispatch the recovered gap
through the normal event callbacks, while a disabled client behaves exactly
like upstream nio.
"""

import asyncio
import json
import logging
import re
import time
from pathlib import Path
from typing import List, Optional
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
    LoginResponse,
    MegolmEvent,
    RoomInfo,
    RoomMemberEvent,
    RoomMessageText,
    RoomNameEvent,
    Rooms,
    SyncResponse,
    Timeline,
)
from nio.api import MATRIX_API_PATH_V3
from nio.client.async_client import _MAX_DISPATCHED_EVENT_IDS

BASE_URL_V3 = f"https://example.org{MATRIX_API_PATH_V3}"
MESSAGES_URL = re.compile(
    rf"^https://example\.org{MATRIX_API_PATH_V3}/rooms/.+/messages"
)
SYNC_URL = re.compile(rf"^https://example\.org{MATRIX_API_PATH_V3}/sync")

TEST_ROOM_ID = "!flooded:example.org"
OTHER_ROOM_ID = "!second:example.org"
OWN_ID = "@example:example.org"

login_response: dict = json.loads(Path("tests/data/login_response.json").read_text())


def text_event(
    event_id: str, *, ts: int, room_id: str = TEST_ROOM_ID
) -> RoomMessageText:
    """Build a real text event so event_id, source and timestamp are populated."""
    event = RoomMessageText.from_dict(
        {
            "content": {"body": f"body {event_id}", "msgtype": "m.text"},
            "event_id": event_id,
            "sender": "@user:example.org",
            "origin_server_ts": ts,
            "room_id": room_id,
            "type": "m.room.message",
        }
    )
    assert isinstance(event, RoomMessageText)
    return event


def member_event(
    event_id: str,
    *,
    ts: int,
    membership: str,
    user_id: str = "@member:example.org",
) -> RoomMemberEvent:
    """Build a membership state event as it appears in timelines and /messages."""
    event = RoomMemberEvent.from_dict(
        {
            "content": {"membership": membership, "displayname": "Member"},
            "event_id": event_id,
            "sender": user_id,
            "state_key": user_id,
            "origin_server_ts": ts,
            "room_id": TEST_ROOM_ID,
            "type": "m.room.member",
        }
    )
    assert isinstance(event, RoomMemberEvent)
    return event


def megolm_event(event_id: str, *, ts: int) -> MegolmEvent:
    """Build an undecryptable megolm event (no session for it exists)."""
    event = Event.parse_event(
        {
            "event_id": event_id,
            "sender": "@user:example.org",
            "origin_server_ts": ts,
            "room_id": TEST_ROOM_ID,
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


def name_event(event_id: str, *, ts: int, name: str) -> RoomNameEvent:
    """Build a room name state event as it appears in timelines and /messages."""
    event = RoomNameEvent.from_dict(
        {
            "content": {"name": name},
            "event_id": event_id,
            "sender": "@user:example.org",
            "state_key": "",
            "origin_server_ts": ts,
            "room_id": TEST_ROOM_ID,
            "type": "m.room.name",
        }
    )
    assert isinstance(event, RoomNameEvent)
    return event


def sync_response(
    next_batch: str,
    room_id: str,
    events: List[RoomMessageText],
    *,
    limited: bool,
    prev_batch: Optional[str],
    state: Optional[list] = None,
) -> SyncResponse:
    """Build a sync response carrying one joined room with the given timeline."""
    timeline = Timeline(list(events), limited, prev_batch)
    room_info = RoomInfo(timeline, list(state or []), [], [])
    rooms = Rooms({}, {room_id: room_info}, {})
    return SyncResponse(
        next_batch,
        rooms,
        DeviceOneTimeKeyCount(49, 50),
        DeviceList([], []),
        [],
        [],
    )


def sync_json(
    next_batch: str,
    room_id: str,
    events: List[RoomMessageText],
    *,
    limited: bool,
    prev_batch: Optional[str],
) -> dict:
    """Build a raw /sync body for tests that drive the real sync() request."""
    return {
        "next_batch": next_batch,
        "device_one_time_keys_count": {"signed_curve25519": 50},
        "device_lists": {"changed": [], "left": []},
        "rooms": {
            "invite": {},
            "leave": {},
            "join": {
                room_id: {
                    "timeline": {
                        "events": [event.source for event in events],
                        "limited": limited,
                        "prev_batch": prev_batch,
                    },
                    "state": {"events": []},
                    "ephemeral": {"events": []},
                    "account_data": {"events": []},
                }
            },
        },
        "to_device": {"events": []},
        "presence": {"events": []},
        "account_data": {"events": []},
    }


def messages_payload(events: List[RoomMessageText], *, end: Optional[str]) -> dict:
    """Build a /messages response body, ordered as the server sends it.

    The chunk follows the requested direction: newest first for ``dir=b``
    walks, oldest first for ``dir=f``. ``end=None`` omits the key entirely,
    which is how a server signals that the pagination walk has reached its
    end.
    """
    payload: dict = {
        "start": "start_token",
        "chunk": [event.source for event in events],
    }
    if end is not None:
        payload["end"] = end
    return payload


class PagedMessages:
    """A /messages callback that serves pre-canned pages keyed by their `from` token.

    aioresponse invokes ``__call__`` for every matching request; the ``from``
    query parameter selects which page to return, so the pagination walk can be
    driven deterministically without depending on request ordering.
    """

    def __init__(self, pages: dict):
        self.pages = pages
        self.requested_tokens: List[Optional[str]] = []
        self.requested_dir: List[Optional[str]] = []
        self.requested_to: List[Optional[str]] = []

    def __call__(self, url, **kwargs) -> CallbackResult:
        query = parse_qs(urlparse(str(url)).query)
        token = query.get("from", [None])[0]
        self.requested_tokens.append(token)
        self.requested_dir.append(query.get("dir", [None])[0])
        self.requested_to.append(query.get("to", [None])[0])
        return CallbackResult(status=200, payload=self.pages[token])


@pytest_asyncio.fixture
async def backfill_client(tempdir) -> AsyncClient:
    """An authed client with limited-timeline backfill enabled."""
    client = AsyncClient(
        "https://example.org",
        OWN_ID,
        "DEVICEID",
        tempdir,
        config=AsyncClientConfig(backfill_limited_timelines=True),
    )
    await client.receive_response(LoginResponse.from_dict(login_response))
    yield client
    await client.close()


@pytest_asyncio.fixture
async def disabled_client(tempdir) -> AsyncClient:
    """An authed client with the default (disabled) backfill behaviour."""
    client = AsyncClient(
        "https://example.org",
        OWN_ID,
        "DEVICEID",
        tempdir,
        config=AsyncClientConfig(),
    )
    await client.receive_response(LoginResponse.from_dict(login_response))
    yield client
    await client.close()


@pytest.mark.asyncio
class TestLimitedTimelineBackfill:
    def _record_callback(self, client: AsyncClient) -> List[str]:
        """Register an event callback and return the list it appends event ids to."""
        dispatched: List[str] = []

        async def cb(_room, event):
            dispatched.append(event.event_id)

        client.add_event_callback(cb, RoomMessageText)
        return dispatched

    async def test_disabled_makes_no_requests(self, disabled_client, aioresponse):
        """With the flag off, a limited timeline triggers no /messages requests."""
        dispatched = self._record_callback(disabled_client)

        await disabled_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$a", ts=100)],
                limited=False,
                prev_batch="p0",
            )
        )
        await disabled_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$b", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$a", "$b"]

    async def test_disabled_keeps_eager_store_token_on_callback_error(self, tempdir):
        """Backfill-disabled persistence remains identical to upstream nio."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(store_sync_tokens=True),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))

        async def fail(_room, _event):
            raise RuntimeError("callback failed")

        client.add_event_callback(fail, RoomMessageText)

        with pytest.raises(RuntimeError, match="callback failed"):
            await client.receive_response(
                sync_response(
                    "s1",
                    TEST_ROOM_ID,
                    [text_event("$event", ts=100)],
                    limited=False,
                    prev_batch="p0",
                )
            )

        assert client.store
        assert client.store.load_sync_token() == "s1"
        await client.close()

    async def test_first_sync_is_skipped(self, backfill_client, aioresponse):
        """A room's first (always limited) sync must not trigger backfill."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$a", ts=100)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$a"]

    async def test_limited_timeline_recovers_gap_in_order(
        self, backfill_client, aioresponse
    ):
        """A limited timeline backfills the gap and dispatches chronologically.

        The recovered gap events are delivered before the sync response's own
        (newer) events, so callbacks observe the room in chronological order.
        """
        dispatched = self._record_callback(backfill_client)

        # First (non-limited) sync establishes the delivered position at $old.
        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        # The gap holds $gap1,$gap2; the surviving window is $new. The room
        # walks backwards from prev_batch to the previous sync position.
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap2", ts=200), text_event("$gap1", ts=100)],
                    end="back2",
                ),
                "back2": messages_payload([text_event("$old", ts=50)], end=None),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", "$gap1", "$gap2", "$new"]
        assert pages.requested_tokens == ["p1", "back2"]
        assert pages.requested_dir == ["b", "b"]
        assert pages.requested_to == ["s1", "s1"]

    async def test_default_recovery_has_no_page_or_event_loss_bound(
        self, backfill_client, aioresponse
    ):
        """Default recovery closes gaps beyond the former hard bounds."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=1)],
                limited=False,
                prev_batch="p0",
            )
        )

        gap = [text_event(f"$gap{i}", ts=1000 + i) for i in range(525)]
        newest_first = list(reversed(gap))
        payloads = {}
        for page in range(10):
            start = "p1" if page == 0 else f"back{page}"
            payloads[start] = messages_payload(
                newest_first[page * 50 : (page + 1) * 50],
                end=f"back{page + 1}",
            )
        payloads["back10"] = messages_payload(
            [*newest_first[500:], text_event("$old", ts=1)],
            end=None,
        )
        pages = PagedMessages(payloads)
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=10_000)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", *(event.event_id for event in gap), "$new"]
        assert len(pages.requested_tokens) == 11

    async def test_events_in_sync_response_are_not_redispatched(
        self, backfill_client, aioresponse
    ):
        """Events present in the sync response are boundaries, never re-dispatched."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        # /messages overlaps the sync window: $present is skipped and never
        # dispatched a second time.
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$present", ts=300),
                        text_event("$gap1", ts=200),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$present", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", "$gap1", "$present"]

    async def test_boundary_page_keeps_gap_events_after_sync_overlap(
        self, backfill_client, aioresponse
    ):
        """Concurrent DAG ordering after the first overlap does not lose events."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        # /sync and /messages may order concurrent room-DAG branches
        # differently. Backward recovery ignores sync overlap and continues to
        # the server-provided since bound.
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$present2", ts=400),
                        text_event("$gap2", ts=200),
                        text_event("$present1", ts=300),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [
                    text_event("$present1", ts=300),
                    text_event("$present2", ts=400),
                ],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == [
            "$old",
            "$gap1",
            "$gap2",
            "$present1",
            "$present2",
        ]

    async def test_gap_events_on_pages_after_sync_overlap_are_recovered(
        self, backfill_client, aioresponse
    ):
        """A sync overlap does not stop later concurrent DAG branches."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$present2", ts=400),
                        text_event("$gap2", ts=200),
                    ],
                    end="back2",
                ),
                "back2": messages_payload(
                    [
                        text_event("$present1", ts=300),
                        text_event("$gap1", ts=100),
                    ],
                    end="back3",
                ),
                "back3": messages_payload(
                    [text_event("$old", ts=50)],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [
                    text_event("$present1", ts=300),
                    text_event("$present2", ts=400),
                ],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == [
            "$old",
            "$gap1",
            "$gap2",
            "$present1",
            "$present2",
        ]
        assert pages.requested_tokens == ["p1", "back2", "back3"]

    async def test_pagination_stops_at_page_bound(self, tempdir, aioresponse, caplog):
        """A never-terminating history walk stops at the configured page bound."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_max_pages=3
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=1)],
                limited=False,
                prev_batch="p0",
            )
        )

        # Every page yields a fresh event and a fresh end token, so only the page
        # bound can stop the walk.
        counter = {"n": 0}

        def endless(url, **kwargs) -> CallbackResult:
            counter["n"] += 1
            n = counter["n"]
            return CallbackResult(
                status=200,
                payload=messages_payload(
                    [text_event(f"$gap{n}", ts=1000 + n)], end=f"tok{n}"
                ),
            )

        aioresponse.get(MESSAGES_URL, callback=endless, repeat=True)

        with caplog.at_level(logging.WARNING):
            await client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$new", ts=10_000)],
                    limited=True,
                    prev_batch="p1",
                )
            )

        # Exactly three pages were fetched, then the walk hit the page bound
        # without reaching the sync window — so its buffer is discarded (it
        # might predate our membership) and the loss is surfaced.
        assert dispatched == ["$old", "$new"]
        assert counter["n"] == 3
        assert "without fully closing the gap" in caplog.text
        await client.close()

    async def test_event_bound_discards_incomplete_recovery(
        self, tempdir, aioresponse, caplog
    ):
        """Hitting the event bound before the window discards the buffer.

        An incomplete bounded walk cannot prove it covered the whole gap, so
        nothing from it may be dispatched and the loss is surfaced.
        """
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_max_events=5
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=1)],
                limited=False,
                prev_batch="p0",
            )
        )

        huge = [text_event(f"$gap{i}", ts=1000 + i) for i in range(20)]
        pages = PagedMessages({"p1": messages_payload(huge, end="back2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        with caplog.at_level(logging.WARNING):
            await client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$new", ts=10_000)],
                    limited=True,
                    prev_batch="p1",
                )
            )

        assert dispatched == ["$old", "$new"]
        assert "without fully closing the gap" in caplog.text
        await client.close()

    async def test_event_bound_after_sync_overlap_discards_boundary_page(
        self, tempdir, aioresponse, caplog
    ):
        """A bound later in a reordered boundary page keeps the gap unverified."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_max_events=1
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=1)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap2", ts=200),
                        text_event("$present", ts=300),
                        text_event("$gap1", ts=100),
                    ],
                    end="back2",
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        with caplog.at_level(logging.WARNING):
            await client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$present", ts=300)],
                    limited=True,
                    prev_batch="p1",
                )
            )

        assert dispatched == ["$old", "$present"]
        assert "without fully closing the gap" in caplog.text
        await client.close()

    async def test_room_messages_failure_is_tolerated(
        self, backfill_client, aioresponse
    ):
        """A /messages error is logged and does not break sync handling."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        aioresponse.get(MESSAGES_URL, status=500, repeat=True)

        # The sync response's own events are still delivered even though the
        # backfill request fails.
        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", "$new"]

    async def test_missing_prev_batch_skips_recovery(
        self, backfill_client, aioresponse
    ):
        """A limited timeline without a pagination start fails closed."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch=None,
            )
        )

        assert dispatched == ["$old", "$new"]

    async def test_backfilled_state_events_do_not_regress_room_state(
        self, backfill_client, aioresponse
    ):
        """Old state events in the gap are dispatched but never applied.

        Backfilled events are older than the state the sync response already
        applied, so replaying them into the room would roll back the current
        name and membership (and feed stale members into E2EE tracking).
        """
        name_ids: List[str] = []

        async def name_cb(_room, event):
            name_ids.append(event.event_id)

        backfill_client.add_event_callback(name_cb, RoomNameEvent)

        # Live sync 1: @member joins and the room is named "Before".
        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [
                    text_event("$old", ts=50),
                    member_event("$join1", ts=60, membership="join"),
                    name_event("$name1", ts=70, name="Before"),
                ],
                limited=False,
                prev_batch="p0",
            )
        )
        room = backfill_client.rooms[TEST_ROOM_ID]
        assert "@member:example.org" in room.users
        assert room.name == "Before"

        # The gap holds an older rename and an older re-join that must lose
        # against the state the limited sync below applies.
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        member_event("$leave", ts=400, membership="leave"),
                        name_event("$name2", ts=200, name="Gap name"),
                        member_event("$join2", ts=150, membership="join"),
                        name_event("$name1", ts=70, name="Before"),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        # Live sync 2 (limited): @member leaves and the room is renamed.
        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [
                    member_event("$leave", ts=400, membership="leave"),
                    name_event("$name3", ts=500, name="After"),
                ],
                limited=True,
                prev_batch="p1",
            )
        )

        # The gap's state events reached the callbacks...
        assert "$name2" in name_ids
        # ...but did not roll back the room's current state.
        assert room.name == "After"
        assert "@member:example.org" not in room.users

    async def test_backfill_timeout_is_tolerated(self, tempdir, aioresponse):
        """A hanging /messages request is abandoned at backfill_timeout."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_timeout=0.05
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        # The mocked homeserver never answers; only backfill_timeout can stop
        # the backfill (room_messages retries rate limits and timeouts
        # internally, so no error response ever surfaces).
        async def hang(url, **kwargs) -> CallbackResult:
            await asyncio.sleep(30)
            return CallbackResult(status=200, payload=messages_payload([], end=None))

        aioresponse.get(MESSAGES_URL, callback=hang, repeat=True)

        await client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", "$new"]
        assert client._last_processed_sync_token == "s1"
        await client.close()

    async def test_restart_resume_backfills_first_limited_sync(
        self, backfill_client, aioresponse
    ):
        """A since-token resume recovers a gap even with no delivered ids.

        This is the restart scenario: the client continues from a stored sync
        token, so a room's first (limited) sync of this run can still hide a
        gap. The gap is paged backwards from prev_batch to the stored sync
        token, so nothing delivered before the restart can be re-dispatched.
        """
        dispatched = self._record_callback(backfill_client)
        # Simulate resuming from a stored token: no room has been seen yet,
        # but the client knows the position the sync continued from.
        backfill_client.next_batch = "since_restart"

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap2", ts=200), text_event("$gap1", ts=100)],
                    end=None,
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$gap1", "$gap2", "$new"]
        assert pages.requested_tokens == ["p1"]
        assert pages.requested_dir == ["b"]
        assert pages.requested_to == ["since_restart"]

    async def test_newly_joined_room_is_not_backfilled(
        self, backfill_client, aioresponse
    ):
        """A freshly joined room's history is not a gap, even on a since resume.

        The discriminator is our own join transition in the sync timeline: a
        genuinely resumed room never carries one (our join predates the since
        token), while a fresh join always does — in the timeline if it survived
        the window, otherwise in the gap, where the backward walk stops before
        pre-join history.
        """
        dispatched = self._record_callback(backfill_client)
        backfill_client.next_batch = "since_token"

        calls = {"n": 0}

        def spy(url, **kwargs) -> CallbackResult:
            calls["n"] += 1
            return CallbackResult(status=200, payload=messages_payload([], end=None))

        aioresponse.get(MESSAGES_URL, callback=spy, repeat=True)

        own_join = member_event(
            "$ownjoin", ts=250, membership="join", user_id=backfill_client.user_id
        )
        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [own_join, text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$new"]
        assert calls["n"] == 0

    async def test_full_state_resume_still_backfills(
        self, backfill_client, aioresponse
    ):
        """Our old join in the state block must not defeat restart recovery.

        A ``full_state=True`` resume (and lazy loading) re-sends our original
        join event in the room's state block for every room, so only a join in
        the timeline may mark a room as freshly joined.
        """
        dispatched = self._record_callback(backfill_client)
        backfill_client.next_batch = "since_restart"

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100)],
                    end=None,
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        old_join = member_event(
            "$oldjoin", ts=10, membership="join", user_id=backfill_client.user_id
        )
        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
                state=[old_join],
            )
        )

        assert dispatched == ["$gap1", "$new"]
        assert pages.requested_dir == ["b"]
        assert pages.requested_to == ["since_restart"]

    async def test_walk_stops_at_own_join(self, backfill_client, aioresponse):
        """A fresh join whose join event fell into the gap recovers only
        post-join events; pre-join history is never dispatched."""
        dispatched = self._record_callback(backfill_client)
        backfill_client.next_batch = "since_token"

        own_join = member_event(
            "$ownjoin", ts=250, membership="join", user_id=backfill_client.user_id
        )
        pages = PagedMessages(
            {
                # Backward page from prev_batch sees post-join events before
                # the join boundary and never reaches pre-join history.
                "p1": messages_payload(
                    [
                        text_event("$post2", ts=260),
                        text_event("$post1", ts=255),
                        own_join,
                        text_event("$prejoin", ts=240),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$post1", "$post2", "$new"]
        assert "$prejoin" not in dispatched

    async def test_empty_page_continues_pagination(self, backfill_client, aioresponse):
        """An empty chunk with an advancing end token continues the walk."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                # An empty page mid-walk (e.g. filtered or purged history) is
                # not the end of pagination while the end token advances.
                "p1": messages_payload([], end="back2"),
                "back2": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$old", ts=50)],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", "$gap1", "$new"]
        assert pages.requested_tokens == ["p1", "back2"]

    async def test_explicit_since_reaches_backfill(self, backfill_client, aioresponse):
        """A caller-supplied sync(since=...) token bounds restart recovery.

        This drives the real sync() request path, not receive_response(), so
        it checks that the request's actual since token is the one handed to
        the backfill as the server-side walk bound.
        """
        dispatched = self._record_callback(backfill_client)

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$new", ts=300)],
                    end=None,
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        aioresponse.get(
            SYNC_URL,
            payload=sync_json(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            ),
        )

        response = await backfill_client.sync(since="tok0")

        assert isinstance(response, SyncResponse)
        assert dispatched == ["$gap1", "$new"]
        assert pages.requested_tokens == ["p1"]
        assert pages.requested_dir == ["b"]
        assert pages.requested_to == ["tok0"]

    async def test_explicit_since_is_persisted_before_callback_failure(
        self, tempdir, aioresponse
    ):
        """An explicit safe boundary survives failure in its first response."""
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
        await client.receive_response(LoginResponse.from_dict(login_response))

        async def fail(_room, event):
            if event.event_id == "$new":
                raise RuntimeError("callback failed")

        client.add_event_callback(fail, RoomMessageText)

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100)],
                    end="more",
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        aioresponse.get(
            SYNC_URL,
            payload=sync_json(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            ),
        )

        with pytest.raises(RuntimeError, match="callback failed"):
            await client.sync(since="explicit-since")

        assert client.next_batch == "s2"
        assert client._last_processed_sync_token == "explicit-since"
        assert client.loaded_sync_token == "explicit-since"
        assert client.store
        assert client.store.load_sync_token() == "explicit-since"
        await client.close()

    async def test_public_token_reset_preserves_recovery_bound(
        self, backfill_client, aioresponse
    ):
        """A forced full resync does not erase callback-delivery continuity."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        # Applications may clear the public token after processing a response
        # to force the next request to return full state. The limited timeline
        # in that response still needs to recover from the last token whose
        # callbacks completed.
        backfill_client.next_batch = ""
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$old", ts=50)],
                    end=None,
                )
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", "$gap1", "$new"]
        assert pages.requested_to == ["s1"]

    async def test_incomplete_backfill_keeps_checkpoint_until_recovered(
        self, tempdir, aioresponse
    ):
        """Later syncs cannot certify a checkpoint past an unrecovered gap."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_max_pages=1
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100)],
                    end="more",
                ),
                "p-reset": messages_payload(
                    [
                        text_event("$newer", ts=400),
                        text_event("$new", ts=300),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert client._last_processed_sync_token == "s1"

        await client.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$newer", ts=400)],
                limited=False,
                prev_batch="p2",
            )
        )

        assert client._last_processed_sync_token == "s1"

        client.next_batch = ""
        await client.receive_response(
            sync_response(
                "s4",
                TEST_ROOM_ID,
                [text_event("$latest", ts=500)],
                limited=True,
                prev_batch="p-reset",
            )
        )

        assert pages.requested_to == ["s1", "s1"]
        assert dispatched == ["$old", "$new", "$newer", "$gap1", "$latest"]
        assert client._last_processed_sync_token == "s4"
        await client.close()

    async def test_restart_loads_checkpoint_before_incomplete_backfill(
        self, tempdir, aioresponse
    ):
        """A process restart must resume before an incompletely recovered gap."""
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
        await first.receive_response(LoginResponse.from_dict(login_response))
        dispatched: List[str] = []

        async def record(_room, event):
            dispatched.append(event.event_id)

        first.add_event_callback(record, RoomMessageText)

        await first.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100)],
                    end="more",
                ),
                "p-reset": messages_payload(
                    [
                        text_event("$newer", ts=400),
                        text_event("$new", ts=300),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        await first.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert first.store
        assert first.next_batch == "s2"
        assert first._last_processed_sync_token == "s1"
        assert first.loaded_sync_token == "s1"
        assert first.store.load_sync_token() == "s1"

        await first.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$newer", ts=400)],
                limited=False,
                prev_batch="p2",
            )
        )

        assert first.next_batch == "s3"
        assert first._last_processed_sync_token == "s1"
        assert first.loaded_sync_token == "s1"
        assert first.store.load_sync_token() == "s1"
        await first.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(login_response))
        restarted.add_event_callback(record, RoomMessageText)

        assert restarted.loaded_sync_token == "s1"

        await restarted.receive_response(
            sync_response(
                "s4",
                TEST_ROOM_ID,
                [text_event("$latest", ts=500)],
                limited=True,
                prev_batch="p-reset",
            )
        )

        assert pages.requested_to == ["s1", "s1"]
        assert dispatched == ["$old", "$new", "$newer", "$gap1", "$latest"]
        assert restarted.store
        assert restarted._last_processed_sync_token == "s4"
        assert restarted.loaded_sync_token == "s4"
        assert restarted.store.load_sync_token() == "s4"
        await restarted.close()

    async def test_restart_journal_exceeds_memory_dedup_limit(
        self, tempdir, aioresponse
    ):
        """A held durable checkpoint retains every delivered id, not only 512."""
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
        await first.receive_response(LoginResponse.from_dict(login_response))
        dispatched: List[str] = []

        async def record(_room, event):
            dispatched.append(event.event_id)

        first.add_event_callback(record, RoomMessageText)
        await first.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        later_events = [
            text_event(f"$later-{index}", ts=300 + index) for index in range(600)
        ]
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100)],
                    end="more",
                ),
                "p-reset": messages_payload(
                    [
                        *reversed(later_events),
                        text_event("$new", ts=200),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        await first.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=200)],
                limited=True,
                prev_batch="p1",
            )
        )
        await first.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                later_events,
                limited=False,
                prev_batch="p2",
            )
        )
        await first.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(login_response))
        restarted.add_event_callback(record, RoomMessageText)
        await restarted.receive_response(
            sync_response(
                "s4",
                TEST_ROOM_ID,
                [text_event("$latest", ts=1000)],
                limited=True,
                prev_batch="p-reset",
            )
        )

        expected = [
            "$old",
            "$new",
            *(event.event_id for event in later_events),
            "$gap1",
            "$latest",
        ]
        assert dispatched == expected
        assert len(dispatched) == len(set(dispatched))
        await restarted.close()

    async def test_restart_journal_allows_one_decrypted_upgrade(
        self, tempdir, aioresponse
    ):
        """A persisted encrypted delivery upgrades once, then remains deduplicated."""
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
        await first.receive_response(LoginResponse.from_dict(login_response))
        await first.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [megolm_event("$enc", ts=90)],
                limited=False,
                prev_batch="p0",
            )
        )
        await first.close()

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap1", ts=100),
                        text_event("$enc", ts=90),
                    ],
                    end=None,
                ),
                "p2": messages_payload(
                    [
                        text_event("$new", ts=300),
                        text_event("$gap1", ts=100),
                        text_event("$enc", ts=90),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)
        dispatched: List[str] = []

        async def record(_room, event):
            dispatched.append(event.event_id)

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(login_response))
        restarted.add_event_callback(record, RoomMessageText)
        await restarted.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )
        assert dispatched == ["$enc", "$gap1", "$new"]
        await restarted.close()

        third = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await third.receive_response(LoginResponse.from_dict(login_response))
        third.add_event_callback(record, RoomMessageText)
        await third.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$latest", ts=400)],
                limited=True,
                prev_batch="p2",
            )
        )

        assert dispatched == ["$enc", "$gap1", "$new", "$latest"]
        await third.close()

    async def test_sliding_sync_dedup_stays_bounded_with_durable_journal(self, tempdir):
        """Sliding replays cannot disable their bound through /sync persistence."""
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
        await client.receive_response(LoginResponse.from_dict(login_response))
        durable = text_event("$durable", ts=1)
        client._record_dispatched_events(TEST_ROOM_ID, [durable], "s1")

        sliding = [
            text_event(f"$sliding-{index}", ts=index + 2)
            for index in range(_MAX_DISPATCHED_EVENT_IDS + 100)
        ]
        client._record_dispatched_events(TEST_ROOM_ID, sliding)

        assert len(client._sliding_dispatched_event_ids[TEST_ROOM_ID]) == (
            _MAX_DISPATCHED_EVENT_IDS
        )
        assert list(client._dispatched_event_ids[TEST_ROOM_ID]) == ["$durable"]
        assert not client._should_dispatch_timeline_event(TEST_ROOM_ID, sliding[-1])
        assert not client._should_dispatch_timeline_event(TEST_ROOM_ID, durable)
        assert client.store
        assert [
            event_id
            for _room_id, event_id, _encrypted, _token in client.store.load_dispatched_events()
        ] == ["$durable"]
        await client.close()

    async def test_straggler_already_delivered_is_not_redispatched(
        self, backfill_client, aioresponse
    ):
        """An event delivered by an earlier sync never fires twice.

        Federation ordering can place an already-delivered event inside a
        later walked gap topologically (/sync uses stream order, /messages
        topological order); the spec requires clients to de-duplicate the
        overlap. The straggler is skipped, but it is not a boundary — the walk
        continues past it.
        """
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$straggler", ts=90)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap2", ts=200),
                        text_event("$straggler", ts=90),
                    ],
                    end="back2",
                ),
                "back2": messages_payload(
                    [text_event("$gap1", ts=100)],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$straggler", "$gap1", "$gap2", "$new"]

    async def test_later_sync_does_not_redispatch_recovered_event(
        self, backfill_client, aioresponse
    ):
        """A gap event later repeated by /sync reaches callbacks only once."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )
        await backfill_client.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [
                    text_event("$gap1", ts=100),
                    text_event("$newer", ts=400),
                ],
                limited=False,
                prev_batch="p2",
            )
        )

        assert dispatched == ["$old", "$gap1", "$new", "$newer"]

    async def test_walk_upgrades_previously_encrypted_event(
        self, backfill_client, aioresponse
    ):
        """A copy that decrypted is dispatched although the encrypted form was.

        Mirrors the sliding sync loop's rule: an event that could only be
        handed to the callbacks in encrypted form (its room key had not
        arrived) goes through once more when a later copy of it — here
        returned by the recovery walk — decrypts.
        """
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [megolm_event("$enc", ts=90)],
                limited=False,
                prev_batch="p0",
            )
        )

        # Only the encrypted form was dispatched (invisible to the
        # RoomMessageText callback), and it was remembered as such.
        assert dispatched == []

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap1", ts=100),
                        text_event("$enc", ts=90),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$enc", "$gap1", "$new"]

    async def test_event_bound_counts_decrypted_replay(
        self, tempdir, aioresponse, caplog
    ):
        """A decrypted replay cannot bypass the configured event cap."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_max_events=1
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [megolm_event("$enc", ts=90)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap1", ts=100),
                        text_event("$enc", ts=90),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        with caplog.at_level(logging.WARNING):
            await client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$new", ts=300)],
                    limited=True,
                    prev_batch="p1",
                )
            )

        assert dispatched == ["$new"]
        assert "without fully closing the gap" in caplog.text
        await client.close()

    async def test_dispatch_respects_backfill_budget(
        self, tempdir, aioresponse, caplog
    ):
        """A slow event callback cannot stall dispatch past the budget.

        The budget covers not just the /messages walk but also handing the
        recovered events to callbacks; a callback that is slow for each of
        many recovered events would otherwise stall sync handling far beyond
        backfill_timeout.
        """
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_timeout=0.3
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))

        dispatched: List[str] = []

        async def slow_cb(_room, event):
            dispatched.append(event.event_id)
            await asyncio.sleep(0.4)

        client.add_event_callback(slow_cb, RoomMessageText)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap3", ts=120),
                        text_event("$gap2", ts=110),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        with caplog.at_level(logging.WARNING):
            await client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$new", ts=300)],
                    limited=True,
                    prev_batch="p1",
                )
            )

        # The first gap event's slow callback exhausts the budget; dispatch
        # stops instead of stalling through the remaining recovered events.
        # The sync's own event is still delivered by the live path.
        assert dispatched == ["$old", "$gap1", "$new"]
        assert "budget is exhausted" in caplog.text
        assert client._last_processed_sync_token == "s1"
        await client.close()

    async def test_hanging_callback_cannot_stall_dispatch(
        self, tempdir, aioresponse, caplog
    ):
        """A callback that never returns is cancelled at the budget.

        A deadline check between events cannot regain control once a hanging
        callback is awaited, so the await itself must be bounded.
        """
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_timeout=0.2
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))

        dispatched: List[str] = []

        async def cb(_room, event):
            dispatched.append(event.event_id)
            if event.event_id.startswith("$gap"):
                # Hang forever, but only for backfilled events, so the live
                # dispatch of the sync's own events still completes.
                await asyncio.Event().wait()

        client.add_event_callback(cb, RoomMessageText)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap2", ts=110),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        with caplog.at_level(logging.WARNING):
            await asyncio.wait_for(
                client.receive_response(
                    sync_response(
                        "s2",
                        TEST_ROOM_ID,
                        [text_event("$new", ts=300)],
                        limited=True,
                        prev_batch="p1",
                    )
                ),
                timeout=5,
            )

        assert dispatched == ["$old", "$gap1", "$new"]
        assert "inside an event callback" in caplog.text
        assert client._last_processed_sync_token == "s1"
        await client.close()

    @pytest.mark.parametrize(
        "error_type",
        [asyncio.TimeoutError, RuntimeError],
        ids=["timeout-error", "runtime-error"],
    )
    async def test_callback_error_skips_only_that_event(
        self, backfill_client, aioresponse, caplog, error_type
    ):
        """A callback error is that event's failure alone.

        TimeoutError in particular must not be mistaken for the dispatch
        budget expiring, which would abort the remaining recovered events.
        """
        dispatched = self._record_callback(backfill_client)

        async def flaky(_room, event):
            if event.event_id == "$gap1":
                raise error_type("the callback failed")

        backfill_client.add_event_callback(flaky, RoomMessageText)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap2", ts=110),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        with caplog.at_level(logging.WARNING):
            await backfill_client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$new", ts=300)],
                    limited=True,
                    prev_batch="p1",
                )
            )

        # $gap1 is terminally skipped (and logged), but $gap2 still arrived —
        # the budget warning must not fire and the completed gap advances.
        assert dispatched == ["$old", "$gap1", "$gap2", "$new"]
        assert "Failed to dispatch backfilled event $gap1" in caplog.text
        assert "budget is exhausted" not in caplog.text
        assert backfill_client._last_processed_sync_token == "s2"

    async def test_callback_fanout_failure_is_terminal_across_restart(
        self, tempdir, aioresponse
    ):
        """A later callback failure cannot replay an earlier successful callback."""
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
        await first.receive_response(LoginResponse.from_dict(login_response))
        dispatched: List[str] = []

        async def record(_room, event):
            dispatched.append(event.event_id)

        async def fail_after_record(_room, event):
            if event.event_id == "$gap1":
                raise RuntimeError("second callback failed")

        first.add_event_callback(record, RoomMessageText)
        first.add_event_callback(fail_after_record, RoomMessageText)
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$old", ts=50)],
                    end=None,
                ),
                "p-reset": messages_payload(
                    [
                        text_event("$new", ts=300),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await first.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )
        await first.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert first.store
        assert first.store.load_sync_token() == "s2"
        assert {
            event_id
            for _room_id, event_id, _encrypted, _token in first.store.load_dispatched_events()
        } == {"$old", "$gap1", "$new"}
        await first.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(login_response))
        restarted.add_event_callback(record, RoomMessageText)
        await restarted.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$latest", ts=400)],
                limited=True,
                prev_batch="p-reset",
            )
        )

        assert dispatched == ["$old", "$gap1", "$new", "$latest"]
        assert len(dispatched) == len(set(dispatched))
        await restarted.close()

    async def test_since_bound_closes_without_delivered_event(
        self, backfill_client, aioresponse
    ):
        """The server token, not a delivered event, closes the gap."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$unknown", ts=80)],
                    end=None,
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", "$unknown", "$gap1", "$new"]
        assert pages.requested_to == ["s1"]

    async def test_backfill_budget_is_shared_across_rooms(self, tempdir, aioresponse):
        """Stalled rooms cannot stack timeouts; one budget covers the sync."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_timeout=0.2
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)
        client.next_batch = "since_x"

        calls = {"n": 0}

        async def hang(url, **kwargs) -> CallbackResult:
            calls["n"] += 1
            await asyncio.sleep(30)
            return CallbackResult(status=200, payload=messages_payload([], end=None))

        aioresponse.get(MESSAGES_URL, callback=hang, repeat=True)

        rooms = Rooms(
            {},
            {
                TEST_ROOM_ID: RoomInfo(
                    Timeline([text_event("$a", ts=1)], True, "p1"), [], [], []
                ),
                OTHER_ROOM_ID: RoomInfo(
                    Timeline(
                        [text_event("$b", ts=2, room_id=OTHER_ROOM_ID)], True, "p2"
                    ),
                    [],
                    [],
                    [],
                ),
            },
            {},
        )
        start = time.monotonic()
        await client.receive_response(
            SyncResponse(
                "s2",
                rooms,
                DeviceOneTimeKeyCount(49, 50),
                DeviceList([], []),
                [],
                [],
            )
        )
        elapsed = time.monotonic() - start

        # Both windows were still delivered; the first room consumed the whole
        # budget, so the second room skipped its backfill without a request.
        assert dispatched == ["$a", "$b"]
        assert calls["n"] == 1
        assert elapsed < 5
        assert client._last_processed_sync_token == "since_x"
        await client.close()

    async def test_one_incomplete_room_keeps_multi_room_checkpoint(
        self, tempdir, aioresponse
    ):
        """Every limited room must complete before the sync is certified."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_max_pages=1
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)
        client.next_batch = "since_x"

        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap-a", ts=100)],
                    end=None,
                ),
                "p2": messages_payload(
                    [text_event("$gap-b", ts=110, room_id=OTHER_ROOM_ID)],
                    end="more",
                ),
            }
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        rooms = Rooms(
            {},
            {
                TEST_ROOM_ID: RoomInfo(
                    Timeline([text_event("$live-a", ts=300)], True, "p1"),
                    [],
                    [],
                    [],
                ),
                OTHER_ROOM_ID: RoomInfo(
                    Timeline(
                        [text_event("$live-b", ts=310, room_id=OTHER_ROOM_ID)],
                        True,
                        "p2",
                    ),
                    [],
                    [],
                    [],
                ),
            },
            {},
        )
        await client.receive_response(
            SyncResponse(
                "s2",
                rooms,
                DeviceOneTimeKeyCount(49, 50),
                DeviceList([], []),
                [],
                [],
            )
        )

        assert dispatched == ["$gap-a", "$live-a", "$live-b"]
        assert client._last_processed_sync_token == "since_x"
        await client.close()

    async def test_restart_walk_discards_incomplete_buffer(
        self, tempdir, aioresponse, caplog
    ):
        """A restart walk cut off by a bound must not dispatch its buffer.

        The buffer is incomplete, so it must be discarded rather than
        partially dispatched.
        """
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True, backfill_max_pages=1
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)
        client.next_batch = "since_token"

        # Page 1 buffers one event and offers more pages; the page bound stops
        # the walk before it reaches the server-side since bound.
        pages = PagedMessages(
            {"p1": messages_payload([text_event("$prejoin", ts=240)], end="back2")}
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        with caplog.at_level(logging.WARNING):
            await client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$new", ts=300)],
                    limited=True,
                    prev_batch="p1",
                )
            )

        assert dispatched == ["$new"]
        assert "$prejoin" not in dispatched
        assert "without fully closing the gap" in caplog.text
        await client.close()

    async def test_repeated_end_token_leaves_gap_open(
        self, backfill_client, aioresponse, caplog
    ):
        """A page whose end token repeats the from token is a stall, not proof
        that the history was exhausted; the gap must stay open and warn."""
        dispatched = self._record_callback(backfill_client)

        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {"p1": messages_payload([text_event("$gap1", ts=100)], end="p1")}
        )
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        with caplog.at_level(logging.WARNING):
            await backfill_client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$new", ts=300)],
                    limited=True,
                    prev_batch="p1",
                )
            )

        # A stalled walk cannot prove its buffer is complete or post-join, so
        # nothing from it is dispatched, and the stall is surfaced.
        assert dispatched == ["$old", "$new"]
        assert "without fully closing the gap" in caplog.text
