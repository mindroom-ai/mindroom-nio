"""Tests for limited-timeline backfill in the async client.

These exercise the opt-in ``backfill_limited_timelines`` behaviour end to end:
a limited sync timeline should cause the client to page ``/messages`` forwards
from the token the sync continued from and dispatch the recovered gap through
the normal event callbacks, while a disabled client behaves exactly like
upstream nio.
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
    LoginResponse,
    RoomInfo,
    RoomMemberEvent,
    RoomMessageText,
    RoomNameEvent,
    Rooms,
    SyncResponse,
    Timeline,
)
from nio.api import MATRIX_API_PATH_V3

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

        # The gap holds $gap1,$gap2; the surviving window is $new. The walk
        # pages forwards from "s1" (the token sync 2 continued from) and stops
        # when it reaches the window.
        pages = PagedMessages(
            {
                "s1": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$gap2", ts=200)],
                    end="fwd2",
                ),
                "fwd2": messages_payload([text_event("$new", ts=300)], end="fwd3"),
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
        assert pages.requested_tokens == ["s1", "fwd2"]
        assert pages.requested_dir == ["f", "f"]
        # The sync's next_batch clamps the walk on servers that honour `to`.
        assert pages.requested_to == ["s2", "s2"]

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
                "s1": messages_payload(
                    [text_event("$gap1", ts=200), text_event("$present", ts=300)],
                    end="fwd2",
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

        An incomplete forward walk cannot prove its buffer postdates our
        membership, so nothing from it may be dispatched, and the loss is
        surfaced instead of silently accepted.
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
        pages = PagedMessages({"s1": messages_payload(huge, end="fwd2")})
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

    async def test_missing_prev_batch_does_not_block_recovery(
        self, backfill_client, aioresponse
    ):
        """The forward walk starts from the since token, not prev_batch."""
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
                "s1": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$new", ts=300)],
                    end="fwd2",
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
                prev_batch=None,
            )
        )

        assert dispatched == ["$old", "$gap1", "$new"]

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
                "s1": messages_payload(
                    [
                        member_event("$join2", ts=150, membership="join"),
                        name_event("$name2", ts=200, name="Gap name"),
                        member_event("$leave", ts=400, membership="leave"),
                    ],
                    end="fwd2",
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
        await client.close()

    async def test_restart_resume_backfills_first_limited_sync(
        self, backfill_client, aioresponse
    ):
        """A since-token resume recovers a gap even with no delivered ids.

        This is the restart scenario: the client continues from a stored sync
        token, so a room's first (limited) sync of this run can still hide a
        gap. The gap is paged forwards from the since token instead of
        backwards to delivered event ids, so nothing delivered before the
        restart can be re-dispatched — verified live against Tuwunel, which
        silently ignores backwards ``to`` bounds.
        """
        dispatched = self._record_callback(backfill_client)
        # Simulate resuming from a stored token: no room has been seen yet,
        # but the client knows the position the sync continued from.
        backfill_client.next_batch = "since_restart"

        pages = PagedMessages(
            {
                # Forward pages are chronological, starting at the since token.
                "since_restart": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$gap2", ts=200)],
                    end="fwd2",
                ),
                # The next page reaches an event from the sync window, the
                # near edge of the gap.
                "fwd2": messages_payload([text_event("$new", ts=300)], end="fwd3"),
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
        # The walk paged forwards from the resume position.
        assert pages.requested_tokens == ["since_restart", "fwd2"]
        assert pages.requested_dir == ["f", "f"]

    async def test_newly_joined_room_is_not_backfilled(
        self, backfill_client, aioresponse
    ):
        """A freshly joined room's history is not a gap, even on a since resume.

        The discriminator is our own join transition in the sync timeline: a
        genuinely resumed room never carries one (our join predates the since
        token), while a fresh join always does — in the timeline if it survived
        the window, otherwise in the gap, where the forward walk discards
        everything collected before it.
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
                "since_restart": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$new", ts=300)],
                    end="fwd2",
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
        assert pages.requested_dir == ["f"]

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
                # Forward page from the since position: the walk collects
                # $prejoin, then discards it when it reaches our own join.
                "since_token": messages_payload(
                    [
                        text_event("$prejoin", ts=240),
                        own_join,
                        text_event("$post1", ts=255),
                        text_event("$post2", ts=260),
                        text_event("$new", ts=300),
                    ],
                    end="fwd2",
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
                "s1": messages_payload([], end="fwd2"),
                "fwd2": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$new", ts=300)],
                    end="fwd3",
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
        assert pages.requested_tokens == ["s1", "fwd2"]

    async def test_explicit_since_reaches_backfill(self, backfill_client, aioresponse):
        """A caller-supplied sync(since=...) token bounds restart recovery.

        This drives the real sync() request path, not receive_response(), so
        it checks that the request's actual since token is the one handed to
        the backfill as the server-side walk bound.
        """
        dispatched = self._record_callback(backfill_client)

        pages = PagedMessages(
            {
                "tok0": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$new", ts=300)],
                    end="fwd2",
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
        # The forward walk starts exactly at the caller's since token, clamped
        # by the response's next_batch on servers that honour `to`.
        assert pages.requested_tokens == ["tok0"]
        assert pages.requested_dir == ["f"]
        assert pages.requested_to == ["s2"]

    async def test_live_edge_does_not_close_the_gap(
        self, backfill_client, aioresponse, caplog
    ):
        """Reaching the live edge without meeting the sync window discards.

        An absent end token proves the walk hit the room's *current* live
        edge — not the sync position. Events that arrived after the sync
        response was generated may sit in the buffer; dispatching them here
        would deliver them again on the next sync.
        """
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

        # $future arrived during the backfill: it is newer than the sync
        # window ($new) yet reachable by the walk; the window's own event
        # never appears, so the gap is unverified.
        pages = PagedMessages(
            {
                "s1": messages_payload(
                    [text_event("$gap1", ts=100), text_event("$future", ts=400)],
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

        assert dispatched == ["$old", "$new"]
        assert "$future" not in dispatched
        assert "without fully closing the gap" in caplog.text

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
        await client.close()

    async def test_forward_walk_discards_unverified_buffer(
        self, tempdir, aioresponse, caplog
    ):
        """A forward walk cut off by a bound must not dispatch its buffer.

        If the walk ends before reaching the sync window, a join of ours may
        lie in the unwalked remainder — everything buffered would then be
        pre-join history, visible under shared history visibility. The buffer
        must be discarded, not dispatched, and the loss surfaced.
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

        # Page 1 buffers $prejoin and offers more pages; the page bound stops
        # the walk before it can learn whether a join lies further ahead.
        pages = PagedMessages(
            {
                "since_token": messages_payload(
                    [text_event("$prejoin", ts=240)], end="fwd2"
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
            {"s1": messages_payload([text_event("$gap1", ts=100)], end="s1")}
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
