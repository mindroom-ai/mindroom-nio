"""Tests for limited-timeline backfill in the async client.

These exercise the opt-in ``backfill_limited_timelines`` behaviour end to end:
a limited sync timeline should cause the client to page ``/messages`` backwards
and dispatch the recovered events through the normal event callbacks, while a
disabled client behaves exactly like upstream nio.
"""

import json
import re
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
    LoginResponse,
    RoomInfo,
    RoomMessageText,
    Rooms,
    SyncResponse,
    Timeline,
)
from nio.api import MATRIX_API_PATH_V3

BASE_URL_V3 = f"https://example.org{MATRIX_API_PATH_V3}"
MESSAGES_URL = re.compile(
    rf"^https://example\.org{MATRIX_API_PATH_V3}/rooms/.+/messages"
)

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


def sync_response(
    next_batch: str,
    room_id: str,
    events: List[RoomMessageText],
    *,
    limited: bool,
    prev_batch: Optional[str],
) -> SyncResponse:
    """Build a sync response carrying one joined room with the given timeline."""
    timeline = Timeline(list(events), limited, prev_batch)
    room_info = RoomInfo(timeline, [], [], [])
    rooms = Rooms({}, {room_id: room_info}, {})
    return SyncResponse(
        next_batch,
        rooms,
        DeviceOneTimeKeyCount(49, 50),
        DeviceList([], []),
        [],
        [],
    )


def messages_payload(events: List[RoomMessageText], *, end: Optional[str]) -> dict:
    """Build a /messages response body; the chunk is newest first as the server sends."""
    return {
        "start": "start_token",
        "end": end,
        "chunk": [event.source for event in events],
    }


class PagedMessages:
    """A /messages callback that serves pre-canned pages keyed by their `from` token.

    aioresponse invokes ``__call__`` for every matching request; the ``from``
    query parameter selects which page to return, so the pagination walk can be
    driven deterministically without depending on request ordering.
    """

    def __init__(self, pages: dict):
        self.pages = pages
        self.requested_tokens: List[Optional[str]] = []

    def __call__(self, url, **kwargs) -> CallbackResult:
        token = parse_qs(urlparse(str(url)).query).get("from", [None])[0]
        self.requested_tokens.append(token)
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
        """A limited timeline backfills the gap and dispatches it oldest first."""
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

        # The gap holds $gap1,$gap2; the surviving window is $new; $old is the
        # boundary the walk must stop at.
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [
                        text_event("$gap2", ts=200),
                        text_event("$gap1", ts=100),
                        text_event("$old", ts=50),
                    ],
                    end="p2",
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

        assert dispatched == ["$old", "$new", "$gap1", "$gap2"]
        assert pages.requested_tokens == ["p1"]

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

        # /messages overlaps the sync window: $present must stop the walk and is
        # never dispatched a second time.
        pages = PagedMessages(
            {
                "p1": messages_payload(
                    [text_event("$gap1", ts=200), text_event("$present", ts=300)],
                    end="p2",
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

        assert dispatched == ["$old", "$present", "$gap1"]

    async def test_already_delivered_events_are_not_redispatched(
        self, backfill_client, aioresponse
    ):
        """A gap event delivered on an earlier round is skipped on a later one."""
        dispatched = self._record_callback(backfill_client)

        # First sync delivers $gap1 directly.
        await backfill_client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$gap1", ts=100)],
                limited=False,
                prev_batch="p0",
            )
        )

        pages = PagedMessages(
            {"p1": messages_payload([text_event("$gap1", ts=100)], end="p2")}
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

        assert dispatched == ["$gap1", "$new"]

    async def test_pagination_stops_at_page_bound(self, tempdir, aioresponse):
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

        await client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=10_000)],
                limited=True,
                prev_batch="p1",
            )
        )

        # $old and $new from the syncs, plus exactly three backfilled pages.
        # The walk fetches newest first ($gap1, $gap2, $gap3) and dispatches the
        # recovered events oldest first, reversing that order.
        assert dispatched == ["$old", "$new", "$gap3", "$gap2", "$gap1"]
        assert counter["n"] == 3
        await client.close()

    async def test_event_bound_caps_recovered_events(self, tempdir, aioresponse):
        """A single huge chunk is capped at the configured event bound."""
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

        huge = [text_event(f"$gap{i}", ts=1000 - i) for i in range(20)]
        pages = PagedMessages({"p1": messages_payload(huge, end="p2")})
        aioresponse.get(MESSAGES_URL, callback=pages, repeat=True)

        await client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=10_000)],
                limited=True,
                prev_batch="p1",
            )
        )

        backfilled = [e for e in dispatched if e.startswith("$gap")]
        assert len(backfilled) == 5
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

    async def test_missing_prev_batch_is_skipped(self, backfill_client, aioresponse):
        """A limited room with no prev_batch token cannot be paged and is skipped."""
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

    async def test_delivered_ids_are_bounded(self, backfill_client, aioresponse):
        """The per-room delivered-id memory does not grow without bound."""
        from nio.client.async_client import _MAX_BACKFILL_DELIVERED_IDS

        events = [
            text_event(f"$e{i}", ts=i) for i in range(_MAX_BACKFILL_DELIVERED_IDS + 100)
        ]
        await backfill_client.receive_response(
            sync_response("s1", TEST_ROOM_ID, events, limited=False, prev_batch="p0")
        )

        delivered = backfill_client._backfill_delivered_ids[TEST_ROOM_ID]
        assert len(delivered) == _MAX_BACKFILL_DELIVERED_IDS
