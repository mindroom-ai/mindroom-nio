"""Tests for limited-timeline backfill in the async client.

These exercise the opt-in ``backfill_limited_timelines`` behaviour end to end:
a limited sync timeline should page ``/messages`` forwards from the request's
safe ``since`` token to the response token and dispatch the recovered gap
through the normal event callbacks, while a disabled client behaves exactly
like upstream nio.
"""

import asyncio
import json
import logging
import re
import threading
import time
from collections import OrderedDict
from pathlib import Path
from typing import List, Optional
from urllib.parse import parse_qs, unquote, urlparse

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
    SlidingSyncResponse,
    SyncResponse,
    Timeline,
    ToDeviceEvent,
    UnknownToDeviceEvent,
)
from nio.api import MATRIX_API_PATH_V3
from nio.client.async_client import _MAX_DISPATCHED_EVENT_IDS
from nio.events import AccountDataEvent, UnknownAccountDataEvent
from nio.store import SqliteMemoryStore

BASE_URL_V3 = f"https://example.org{MATRIX_API_PATH_V3}"
MESSAGES_URL = re.compile(
    rf"^https://example\.org{MATRIX_API_PATH_V3}/rooms/.+/messages"
)
SYNC_URL = re.compile(rf"^https://example\.org{MATRIX_API_PATH_V3}/sync")

TEST_ROOM_ID = "!flooded:example.org"
OTHER_ROOM_ID = "!second:example.org"
OWN_ID = "@example:example.org"

login_response: dict = json.loads(Path("tests/data/login_response.json").read_text())
_SYNC_WINDOWS: dict[tuple[str, str], tuple[Optional[str], List[RoomMessageText]]] = {}


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
    _SYNC_WINDOWS[(next_batch, room_id)] = (prev_batch, list(events))
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
    _SYNC_WINDOWS[(next_batch, room_id)] = (prev_batch, list(events))
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
        self.forward_pages: dict[tuple[str, str, str], dict] = {}
        self.requested_tokens: List[Optional[str]] = []
        self.requested_dir: List[Optional[str]] = []
        self.requested_to: List[Optional[str]] = []
        self.membership_requested_tokens: List[Optional[str]] = []

    def __call__(self, url, **kwargs) -> CallbackResult:
        query = parse_qs(urlparse(str(url)).query)
        room_id = unquote(
            urlparse(str(url)).path.split("/rooms/", 1)[1].split("/", 1)[0]
        )
        token = query.get("from", [None])[0]
        direction = query.get("dir", [None])[0]
        end = query.get("to", [None])[0]
        if "filter" in query:
            self.membership_requested_tokens.append(token)
        else:
            self.requested_tokens.append(token)
            self.requested_dir.append(direction)
            self.requested_to.append(end)

        if (
            direction == "f"
            and token
            and end
            and (room_id, token, end) in self.forward_pages
        ):
            return CallbackResult(
                status=200,
                payload=self.forward_pages[(room_id, token, end)],
            )

        if direction == "f" and token not in self.pages:
            assert token
            assert end
            prev_batch, present = _SYNC_WINDOWS[(end, room_id)]
            backward_events: List[RoomMessageText] = []
            page_token = prev_batch
            visited = set()
            chain_complete = False
            while page_token and page_token not in visited:
                visited.add(page_token)
                page = self.pages.get(page_token)
                if page is None:
                    break
                backward_events.extend(
                    Event.parse_event(source) for source in page["chunk"]
                )
                page_token = page.get("end")
            else:
                chain_complete = page_token is None

            since_ids = {
                event.event_id
                for event in _SYNC_WINDOWS.get((token, room_id), (None, []))[1]
            }
            forward_events = [
                event
                for event in reversed(backward_events)
                if event.event_id not in since_ids
            ]
            forward_events.extend(present)
            limit = int(query.get("limit", ["50"])[0])
            generated_token = token
            for index in range(0, len(forward_events), limit):
                next_token = (
                    f"forward:{end}:{index + limit}"
                    if index + limit < len(forward_events)
                    else None if chain_complete else f"forward-missing:{end}"
                )
                self.forward_pages[(room_id, generated_token, end)] = messages_payload(
                    forward_events[index : index + limit], end=next_token
                )
                generated_token = next_token

            return CallbackResult(
                status=200,
                payload=self.forward_pages[(room_id, token, end)],
            )

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

    async def test_disabled_callback_error_keeps_upstream_short_circuit(
        self, disabled_client
    ):
        """Attempt-all fanout is scoped to recovery-enabled clients."""
        later_callbacks: List[str] = []

        async def fail_first(_room, _event):
            raise RuntimeError("first callback failed")

        async def record_later(_room, event):
            later_callbacks.append(event.event_id)

        disabled_client.add_event_callback(fail_first, RoomMessageText)
        disabled_client.add_event_callback(record_later, RoomMessageText)

        with pytest.raises(RuntimeError, match="first callback failed"):
            await disabled_client.receive_response(
                sync_response(
                    "s1",
                    TEST_ROOM_ID,
                    [text_event("$event", ts=100)],
                    limited=False,
                    prev_batch="p0",
                )
            )

        assert later_callbacks == []

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
        assert not aioresponse.requests

    async def test_ordinary_sync_is_not_subject_to_backfill_deadline(self, tempdir):
        """The recovery budget cannot time out an ordinary callback commit."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_timeout=0,
                store_sync_tokens=True,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$event", ts=100)],
                limited=False,
                prev_batch="p0",
            )
        )

        assert dispatched == ["$event"]
        assert client.store
        assert client.store.load_sync_token() == "s1"
        await client.close()

    async def test_since_less_callback_error_restores_full_sync_cursor(
        self, backfill_client
    ):
        """A failed first response retries instead of losing untouched events."""
        dispatched = self._record_callback(backfill_client)
        failed = False

        async def fail_once(_room, event):
            nonlocal failed
            if event.event_id == "$a" and not failed:
                failed = True
                raise RuntimeError("first response failed")

        backfill_client.add_event_callback(fail_once, RoomMessageText)
        response = sync_response(
            "s1",
            TEST_ROOM_ID,
            [text_event("$a", ts=100), text_event("$b", ts=200)],
            limited=False,
            prev_batch="p0",
        )

        with pytest.raises(RuntimeError, match="first response failed"):
            await backfill_client.receive_response(response)

        assert backfill_client.next_batch is None
        assert dispatched == ["$a"]

        await backfill_client.receive_response(response)

        assert dispatched == ["$a", "$b"]
        assert backfill_client.next_batch == "s1"

    async def test_sticky_gap_closes_on_non_limited_retry(self, tempdir, aioresponse):
        """A complete safe-cursor retry needs no extra /messages walk."""
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
        dispatched = self._record_callback(client)
        account_data_callbacks: List[str] = []

        async def record_account_data(event):
            account_data_callbacks.append(event.content["value"])

        client.add_global_account_data_callback(
            record_account_data,
            UnknownAccountDataEvent,
        )
        account_data = AccountDataEvent.parse_event(
            {"type": "m.test", "content": {"value": "once"}}
        )
        assert isinstance(account_data, UnknownAccountDataEvent)

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )
        aioresponse.get(MESSAGES_URL, status=500)
        incomplete = sync_response(
            "s2",
            TEST_ROOM_ID,
            [text_event("$new", ts=300)],
            limited=True,
            prev_batch="p1",
        )
        incomplete.account_data_events = [account_data]
        await client.receive_response(incomplete)

        assert client.next_batch == "s1"
        assert client._has_unrecovered_sync_gap
        assert account_data_callbacks == ["once"]

        complete = sync_response(
            "s3",
            TEST_ROOM_ID,
            [
                text_event("$gap", ts=100),
                text_event("$new", ts=300),
                text_event("$newer", ts=400),
            ],
            limited=False,
            prev_batch="p2",
        )
        complete.account_data_events = [account_data]
        await client.receive_response(complete)

        assert dispatched == ["$old", "$gap", "$new", "$newer"]
        assert account_data_callbacks == ["once"]
        assert client.next_batch == "s3"
        assert client._last_processed_sync_token == "s3"
        assert not client._has_unrecovered_sync_gap
        assert client.store
        assert client.store.load_sync_token() == "s3"
        assert not client.store.load_sync_recovery_pending()

        # Replay suppression belongs only to the uncertified interval. The
        # same state payload in a later certified response is a new callback.
        later = sync_response(
            "s4",
            TEST_ROOM_ID,
            [text_event("$later", ts=500)],
            limited=False,
            prev_batch="p3",
        )
        later.account_data_events = [account_data]
        await client.receive_response(later)

        assert account_data_callbacks == ["once", "once"]
        await client.close()

    async def test_live_callback_error_is_terminal_across_process_restart(
        self, tempdir
    ):
        """A restarted safe-cursor retry resumes after attempted fan-out."""
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

        async def fail_on_bad(_room, event):
            if event.event_id == "$bad":
                raise RuntimeError("callback failed")

        first.add_event_callback(record, RoomMessageText)
        first.add_event_callback(fail_on_bad, RoomMessageText)
        await first.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )
        retry = sync_response(
            "s2",
            TEST_ROOM_ID,
            [text_event("$bad", ts=100), text_event("$later", ts=200)],
            limited=False,
            prev_batch="p1",
        )

        with pytest.raises(RuntimeError, match="callback failed"):
            await first.receive_response(retry)

        assert dispatched == ["$old", "$bad"]
        assert first.next_batch == "s1"
        assert first.store
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
        await restarted.receive_response(retry)

        assert dispatched == ["$old", "$bad", "$later"]
        assert restarted.next_batch == "s2"
        assert restarted.store
        assert restarted.store.load_sync_token() == "s2"
        await restarted.close()

    async def test_ancillary_callback_is_terminal_across_process_restart(
        self, tempdir, aioresponse
    ):
        """A safe-cursor retry cannot repeat non-timeline callback delivery."""
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
        callbacks: List[str] = []

        async def record(event):
            callbacks.append(event.content["value"])

        account_data = AccountDataEvent.parse_event(
            {"type": "m.test", "content": {"value": "once"}}
        )
        assert isinstance(account_data, UnknownAccountDataEvent)
        first.add_global_account_data_callback(record, UnknownAccountDataEvent)
        await first.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )
        aioresponse.get(MESSAGES_URL, status=500)
        incomplete = sync_response(
            "s2",
            TEST_ROOM_ID,
            [text_event("$new", ts=300)],
            limited=True,
            prev_batch="p1",
        )
        incomplete.account_data_events = [account_data]
        await first.receive_response(incomplete)

        assert callbacks == ["once"]
        assert first.next_batch == "s1"
        await first.close()

        restarted = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await restarted.receive_response(LoginResponse.from_dict(login_response))
        restarted.add_global_account_data_callback(record, UnknownAccountDataEvent)
        complete = sync_response(
            "s3",
            TEST_ROOM_ID,
            [
                text_event("$gap", ts=100),
                text_event("$new", ts=300),
            ],
            limited=False,
            prev_batch="p2",
        )
        complete.account_data_events = [account_data]
        await restarted.receive_response(complete)

        assert callbacks == ["once"]
        assert restarted.store
        assert restarted.store.load_sync_token() == "s3"
        await restarted.close()

    async def test_to_device_replay_uses_pre_decryption_identity(
        self, backfill_client, monkeypatch
    ):
        """A replay stays suppressed when only the first copy decrypts."""
        callbacks: List[str] = []
        decryptions = 0

        async def record(event):
            callbacks.append(event.type)

        raw_source = {
            "type": "m.raw",
            "sender": "@sender:example.org",
            "content": {"value": "ciphertext"},
        }
        decrypted_source = {
            "type": "m.decrypted",
            "sender": "@sender:example.org",
            "content": {"value": "plaintext"},
        }

        def decrypt(_event):
            nonlocal decryptions
            decryptions += 1
            if decryptions == 1:
                return UnknownToDeviceEvent.from_dict(dict(decrypted_source))
            return None

        backfill_client.add_to_device_callback(record, ToDeviceEvent)
        monkeypatch.setattr(backfill_client, "_handle_decrypt_to_device", decrypt)

        def response() -> SyncResponse:
            result = sync_response(
                "s2",
                TEST_ROOM_ID,
                [],
                limited=False,
                prev_batch="p0",
            )
            result.to_device_events = [UnknownToDeviceEvent.from_dict(dict(raw_source))]
            return result

        await backfill_client._handle_to_device(response(), "s1")
        await backfill_client._handle_to_device(response(), "s1")

        assert callbacks == ["m.decrypted"]

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
        # walks forwards from the previous sync position.
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
        assert pages.requested_tokens == ["s1"]
        assert pages.requested_dir == ["f"]
        assert pages.requested_to == ["s2"]

    async def test_default_recovery_progresses_in_bounded_slices(
        self, backfill_client, aioresponse
    ):
        """Large gaps close over automatic retries without unbounded buffers."""
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

        assert dispatched == [
            "$old",
            *(event.event_id for event in gap[:200]),
        ]
        assert backfill_client.next_batch == "s1"

        await backfill_client.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$new", ts=10_000)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == [
            "$old",
            *(event.event_id for event in gap[:400]),
        ]
        assert backfill_client.next_batch == "s1"

        await backfill_client.receive_response(
            sync_response(
                "s4",
                TEST_ROOM_ID,
                [text_event("$new", ts=10_000)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", *(event.event_id for event in gap), "$new"]
        assert backfill_client.next_batch == "s4"
        assert len(pages.requested_tokens) == 25

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
        # differently. Forward recovery ignores sync overlap and continues to
        # the server-provided pagination boundary.
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
        assert pages.requested_tokens == ["s1"]

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
            query = parse_qs(urlparse(str(url)).query)
            if "filter" in query:
                return CallbackResult(
                    status=200,
                    payload=messages_payload([], end=None),
                )
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

        # Exactly three pages were fetched. Their verified forward prefix is
        # delivered, while the live window waits for an automatic retry.
        assert dispatched == ["$old", "$gap1", "$gap2", "$gap3"]
        assert counter["n"] == 3
        assert "without fully closing the gap" in caplog.text
        assert client.next_batch == "s1"
        await client.close()

    async def test_page_bound_retry_resumes_after_completed_prefix(
        self, tempdir, aioresponse
    ):
        """Automatic retry advances beyond pages already durably dispatched."""
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

        requested_tokens: List[str] = []
        responses = {
            "s1": messages_payload([text_event("$gap1", ts=100)], end="t1"),
            "t1": messages_payload([text_event("$gap2", ts=200)], end="t2"),
            "t2": messages_payload([text_event("$gap3", ts=300)], end="t3"),
            "t3": messages_payload([text_event("$new", ts=400)], end=None),
        }

        def paged(url, **kwargs) -> CallbackResult:
            query = parse_qs(urlparse(str(url)).query)
            token = query["from"][0]
            if "filter" not in query:
                requested_tokens.append(token)
            return CallbackResult(status=200, payload=responses[token])

        aioresponse.get(MESSAGES_URL, callback=paged, repeat=True)

        await client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=400)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old", "$gap1", "$gap2"]
        assert client.next_batch == "s1"

        await client.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$new", ts=400)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert requested_tokens == ["s1", "t1", "t2", "t3"]
        assert dispatched == ["$old", "$gap1", "$gap2", "$gap3", "$new"]
        assert client.next_batch == "s3"
        await client.close()

    async def test_event_bound_slices_incomplete_recovery(
        self, tempdir, aioresponse, caplog
    ):
        """Hitting the event bound delivers one safe prefix and schedules retry."""
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
        pages = PagedMessages(
            {
                "p1": messages_payload(list(reversed(huge)), end="back2"),
                "back2": messages_payload([], end=None),
            }
        )
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

        assert dispatched == [
            "$old",
            *(event.event_id for event in huge[:5]),
        ]
        assert "without fully closing the gap" in caplog.text
        assert client.next_batch == "s1"
        await client.close()

    async def test_event_bound_after_sync_overlap_keeps_safe_prefix(
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
                ),
                "back2": messages_payload([], end=None),
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

        assert dispatched == ["$old", "$gap1"]
        assert "without fully closing the gap" in caplog.text
        assert client.next_batch == "s1"
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

        # The live window waits because its earlier gap could not be verified.
        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$old"]
        assert backfill_client.next_batch == "s1"

    async def test_forward_recovery_does_not_require_prev_batch(
        self, backfill_client, aioresponse
    ):
        """The safe `since` token is the forward pagination start."""
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

        aioresponse.get(
            MESSAGES_URL,
            payload=messages_payload(
                [text_event("$gap", ts=100), text_event("$new", ts=300)],
                end=None,
            ),
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

        assert dispatched == ["$old", "$gap", "$new"]

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

        assert dispatched == ["$old"]
        assert client._last_processed_sync_token == "s1"
        assert client.next_batch == "s1"
        await client.close()

    async def test_restart_resume_backfills_first_limited_sync(
        self, backfill_client, aioresponse
    ):
        """A since-token resume recovers a gap even with no delivered ids.

        This is the restart scenario: the client continues from a stored sync
        token, so a room's first (limited) sync of this run can still hide a
        gap. The gap is paged forwards from the stored sync
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
        assert pages.requested_tokens == ["since_restart"]
        assert pages.requested_dir == ["f"]
        assert pages.requested_to == ["s2"]

    async def test_newly_joined_room_is_not_backfilled(
        self, backfill_client, aioresponse
    ):
        """A freshly joined room's history is not a gap, even on a since resume.

        The discriminator is our own join transition in the sync timeline: a
        genuinely resumed room never carries one (our join predates the since
        token), while a fresh join always does — in the timeline if it survived
        the window, otherwise in the gap, where the forward walk resets at
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
        assert pages.requested_dir == ["f"]
        assert pages.requested_to == ["s2"]

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
                # The forward walk discards history before our own join and
                # retains only the events that follow it.
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

    async def test_bounded_prefix_waits_for_later_own_join(self, tempdir, aioresponse):
        """A page bound cannot expose pre-join history before a later page."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_max_pages=1,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)
        client.next_batch = "since_token"
        own_join = member_event(
            "$ownjoin",
            ts=250,
            membership="join",
            user_id=client.user_id,
        )
        post_join_data_calls = 0

        def paged(url, **kwargs) -> CallbackResult:
            nonlocal post_join_data_calls
            query = parse_qs(urlparse(str(url)).query)
            token = query["from"][0]
            is_membership_scan = "filter" in query
            if token == "since_token":
                events = [text_event("$prejoin", ts=200)]
                end = "after-prejoin"
            elif is_membership_scan:
                events = [own_join]
                end = None
            else:
                post_join_data_calls += 1
                events = [text_event("$postjoin", ts=260), text_event("$new", ts=300)]
                if post_join_data_calls > 1:
                    events.insert(0, own_join)
                end = None
            return CallbackResult(
                status=200,
                payload=messages_payload(events, end=end),
            )

        aioresponse.get(MESSAGES_URL, callback=paged, repeat=True)
        limited = sync_response(
            "s2",
            TEST_ROOM_ID,
            [text_event("$new", ts=300)],
            limited=True,
            prev_batch="p1",
        )

        await client.receive_response(limited)

        assert dispatched == []
        assert client.next_batch == "since_token"

        await client.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        # The membership-only page found the join, but an unstable first data
        # retry omitted it. That response still cannot certify the interval.
        assert dispatched == []
        assert client.next_batch == "since_token"

        await client.receive_response(
            sync_response(
                "s4",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert dispatched == ["$postjoin", "$new"]
        assert "$prejoin" not in dispatched
        await client.close()

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
        assert pages.requested_tokens == ["s1"]

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
        assert pages.requested_tokens == ["tok0"]
        assert pages.requested_dir == ["f"]
        assert pages.requested_to == ["s2"]

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

        with pytest.raises(RuntimeError, match="callback failed"):
            await client.sync(since="explicit-since")

        assert client.next_batch == "explicit-since"
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
        assert pages.requested_to == ["s2"]

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

        # This response was already in flight from the speculative transport
        # cursor when the earlier limited response pinned the safe checkpoint.
        # Its non-limited timeline cannot certify the missing interval.
        client._sync_since = "s2"
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

        assert pages.requested_to == ["s2", "s4"]
        assert dispatched == ["$old", "$newer", "$gap1", "$new", "$latest"]
        assert len(dispatched) == len(set(dispatched))
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
        assert first.next_batch == "s1"
        assert first._last_processed_sync_token == "s1"
        assert first.loaded_sync_token == "s1"
        assert first.store.load_sync_token() == "s1"
        assert first.store.load_sync_recovery_pending()
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
        assert restarted._has_unrecovered_sync_gap

        # Model a response that was already in flight from the speculative
        # cursor when the prior process established the durable gap marker.
        restarted._sync_since = "s2"
        await restarted.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$newer", ts=400)],
                limited=False,
                prev_batch="p2",
            )
        )

        assert restarted.next_batch == "s1"
        assert restarted._last_processed_sync_token == "s1"
        assert restarted.loaded_sync_token == "s1"
        assert restarted.store
        assert restarted.store.load_sync_token() == "s1"
        assert restarted.store.load_sync_recovery_pending()

        # Even an explicit speculative transport cursor cannot bypass the
        # durable callback checkpoint while the gap marker is sticky.
        restarted._sync_since = "s3"
        await restarted.receive_response(
            sync_response(
                "s4",
                TEST_ROOM_ID,
                [text_event("$latest", ts=500)],
                limited=True,
                prev_batch="p-reset",
            )
        )

        assert pages.requested_to == ["s2", "s4"]
        assert pages.requested_tokens == ["s1", "s1"]
        assert dispatched == ["$old", "$newer", "$gap1", "$new", "$latest"]
        assert len(dispatched) == len(set(dispatched))
        assert restarted.store
        assert restarted._last_processed_sync_token == "s4"
        assert restarted.loaded_sync_token == "s4"
        assert restarted.store.load_sync_token() == "s4"
        assert not restarted.store.load_sync_recovery_pending()
        await restarted.close()

    async def test_restart_journal_exceeds_memory_dedup_limit(
        self, tempdir, aioresponse
    ):
        """A held durable checkpoint retains every delivered id, not only 512."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_max_pages=20,
            backfill_max_events=None,
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
        first._sync_since = "s2"
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
        assert restarted.store
        journal_batch_sizes: List[int] = []
        save_dispatched_events = restarted.store.save_dispatched_events

        def record_journal_batch(room_id, sync_token, events):
            journal_batch_sizes.append(len(events))
            save_dispatched_events(room_id, sync_token, events)

        restarted.store.save_dispatched_events = record_journal_batch
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
            *(event.event_id for event in later_events),
            "$gap1",
            "$new",
            "$latest",
        ]
        assert dispatched == expected
        assert len(dispatched) == len(set(dispatched))
        # Callback completion is journaled one event at a time; already
        # journaled overlap is safely retagged in one bulk write.
        assert set(journal_batch_sizes) == {1, len(later_events)}
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
                [
                    text_event("$enc", ts=90),
                    text_event("$new", ts=300),
                ],
                limited=True,
                prev_batch="p1",
            )
        )
        assert dispatched == ["$gap1", "$enc", "$new"]
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

        assert dispatched == ["$gap1", "$enc", "$new", "$latest"]
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

    async def test_sliding_sync_call_path_keeps_dedup_bounded(self, tempdir):
        """The public sliding response path enforces the 512-event bound."""
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
        dispatched = self._record_callback(client)
        events = [
            text_event(f"$sliding-{index}", ts=index)
            for index in range(_MAX_DISPATCHED_EVENT_IDS + 100)
        ]

        def sliding(pos: str, timeline: List[RoomMessageText]) -> SlidingSyncResponse:
            return SlidingSyncResponse.from_dict(
                {
                    "pos": pos,
                    "rooms": {
                        TEST_ROOM_ID: {
                            "membership": "join",
                            "required_state": [],
                            "timeline": [event.source for event in timeline],
                        }
                    },
                }
            )

        await client.receive_response(sliding("p1", events))

        assert len(client._sliding_dispatched_event_ids[TEST_ROOM_ID]) == (
            _MAX_DISPATCHED_EVENT_IDS
        )
        await client.receive_response(sliding("p2", [events[-1]]))

        assert dispatched.count(events[-1].event_id) == 1
        await client.close()

    async def test_cross_cache_plaintext_state_wins(self, backfill_client):
        """A stale encrypted recent entry cannot override durable plaintext."""
        backfill_client._dispatched_event_ids[TEST_ROOM_ID] = OrderedDict(
            [("$same", False)]
        )
        backfill_client._sliding_dispatched_event_ids[TEST_ROOM_ID] = OrderedDict(
            [("$same", True)]
        )

        assert not backfill_client._dispatched_event_state(TEST_ROOM_ID, "$same")

    async def test_live_timeline_commits_each_durable_journal_event(
        self, tempdir, monkeypatch
    ):
        """A crash can expose at most the currently executing callback."""
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
        assert client.store
        journal_batch_sizes: List[int] = []
        save_dispatched_events = client.store.save_dispatched_events

        def record_journal_batch(room_id, sync_token, events):
            journal_batch_sizes.append(len(events))
            save_dispatched_events(room_id, sync_token, events)

        monkeypatch.setattr(
            client.store,
            "save_dispatched_events",
            record_journal_batch,
        )
        events = [
            text_event(f"$live-{index}", ts=index)
            for index in range(_MAX_DISPATCHED_EVENT_IDS + 100)
        ]

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                events,
                limited=False,
                prev_batch="p0",
            )
        )

        assert journal_batch_sizes == [1] * len(events)
        assert len(client._dispatched_event_ids[TEST_ROOM_ID]) == len(events)
        await client.close()

    async def test_memory_store_journal_stays_on_owning_connection(self):
        """In-memory SQLite writes cannot move to a worker connection."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                store_sync_tokens=True,
                store=SqliteMemoryStore,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))

        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$event", ts=1)],
                limited=False,
                prev_batch="p0",
            )
        )

        assert client.store
        assert client.store.load_sync_token() == "s1"
        assert client.store.load_dispatched_events() == [
            (TEST_ROOM_ID, "$event", False, "s1")
        ]
        await client.close()

    @pytest.mark.parametrize(
        "failed_operation",
        ["event-journal", "checkpoint"],
    )
    async def test_store_write_failure_keeps_response_retryable(
        self, tempdir, monkeypatch, failed_operation
    ):
        """A failed durable write cannot lose or duplicate a callback."""
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
        dispatched = self._record_callback(client)
        assert client.store
        failures = 0

        if failed_operation == "event-journal":
            original = client.store.save_dispatched_events

            def fail_once(*args):
                nonlocal failures
                failures += 1
                if failures == 1:
                    raise RuntimeError("journal write failed")
                return original(*args)

            monkeypatch.setattr(client.store, "save_dispatched_events", fail_once)
        else:
            original = client.store.save_sync_token_and_prune_dispatched_events

            def fail_once(*args):
                nonlocal failures
                failures += 1
                if failures == 1:
                    raise RuntimeError("checkpoint write failed")
                return original(*args)

            monkeypatch.setattr(
                client.store,
                "save_sync_token_and_prune_dispatched_events",
                fail_once,
            )

        response = sync_response(
            "s1",
            TEST_ROOM_ID,
            [text_event("$event", ts=1)],
            limited=False,
            prev_batch="p0",
        )
        with pytest.raises(RuntimeError, match="write failed"):
            await client.receive_response(response)

        assert client.next_batch is None

        await client.receive_response(response)

        assert dispatched == ["$event"]
        assert client.next_batch == "s1"
        assert client.store.load_sync_token() == "s1"
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

        aioresponse.get(
            MESSAGES_URL,
            payload=messages_payload(
                [
                    text_event("$enc", ts=90),
                    text_event("$gap1", ts=100),
                    text_event("$new", ts=300),
                ],
                end=None,
            ),
        )

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

        aioresponse.get(
            MESSAGES_URL,
            payload=messages_payload(
                [
                    text_event("$enc", ts=90),
                    text_event("$gap1", ts=100),
                    text_event("$new", ts=300),
                ],
                end=None,
            ),
        )

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

        assert dispatched == ["$enc"]
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
        # The sync's own event waits for a later safe-token recovery.
        assert dispatched == ["$old", "$gap1"]
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
                # Hang forever, but only for backfilled events.
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

        assert dispatched == ["$old", "$gap1"]
        assert "inside an event callback" in caplog.text
        assert client._last_processed_sync_token == "s1"
        await client.close()

    async def test_live_callback_cancellation_restores_safe_cursor(self, tempdir):
        """Cancelling sync handling cannot strand the transport cursor ahead."""
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
        callback_started = asyncio.Event()

        async def block_new(_room, event):
            if event.event_id == "$new":
                callback_started.set()
                await asyncio.Event().wait()

        client.add_event_callback(block_new, RoomMessageText)
        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$old", ts=50)],
                limited=False,
                prev_batch="p0",
            )
        )

        handling = asyncio.create_task(
            client.receive_response(
                sync_response(
                    "s2",
                    TEST_ROOM_ID,
                    [text_event("$new", ts=300)],
                    limited=False,
                    prev_batch="p1",
                )
            )
        )
        await asyncio.wait_for(callback_started.wait(), timeout=2)
        handling.cancel()
        with pytest.raises(asyncio.CancelledError):
            await handling

        assert client.next_batch == "s1"
        assert client._last_processed_sync_token == "s1"
        assert client.store
        assert client.store.load_sync_token() == "s1"
        assert not client.store.load_sync_recovery_pending()
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
        after_error: List[str] = []

        async def record(_room, event):
            dispatched.append(event.event_id)

        async def fail_after_record(_room, event):
            if event.event_id == "$gap1":
                raise RuntimeError("second callback failed")

        async def record_after_error(_room, event):
            after_error.append(event.event_id)

        first.add_event_callback(record, RoomMessageText)
        first.add_event_callback(fail_after_record, RoomMessageText)
        first.add_event_callback(record_after_error, RoomMessageText)
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
        } == {"$gap1", "$new"}
        assert after_error == ["$old", "$gap1", "$new"]
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

    async def test_silent_backward_to_ignore_cannot_replay_old_history(
        self, backfill_client, aioresponse
    ):
        """Recovery uses the safe token as `from`, never as a trusted `to`."""
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
        requests: List[dict[str, Optional[str]]] = []

        def server_ignoring_backward_to(url, **kwargs) -> CallbackResult:
            query = parse_qs(urlparse(str(url)).query)
            request = {key: query.get(key, [None])[0] for key in ("from", "to", "dir")}
            requests.append(request)
            if request["dir"] == "b":
                events = [
                    text_event("$ancient", ts=1),
                    text_event("$old", ts=50),
                    text_event("$gap", ts=100),
                ]
            else:
                events = [
                    text_event("$gap", ts=100),
                    text_event("$new", ts=300),
                ]
            return CallbackResult(
                status=200,
                payload=messages_payload(events, end=None),
            )

        aioresponse.get(
            MESSAGES_URL,
            callback=server_ignoring_backward_to,
            repeat=True,
        )
        await backfill_client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$new", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert requests == [{"from": "s1", "to": "s2", "dir": "f"}]
        assert dispatched == ["$old", "$gap", "$new"]
        assert "$ancient" not in dispatched

    async def test_completed_prefix_is_durable_while_next_callback_blocks(
        self, tempdir
    ):
        """A hard kill during event two can replay at most event two."""
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
        second_started = asyncio.Event()
        release_second = asyncio.Event()

        async def block_second(_room, event):
            if event.event_id == "$second":
                second_started.set()
                await release_second.wait()

        client.add_event_callback(block_second, RoomMessageText)
        handling = asyncio.create_task(
            client.receive_response(
                sync_response(
                    "s1",
                    TEST_ROOM_ID,
                    [
                        text_event("$first", ts=1),
                        text_event("$second", ts=2),
                    ],
                    limited=False,
                    prev_batch="p0",
                )
            )
        )
        await asyncio.wait_for(second_started.wait(), timeout=2)

        assert client.store
        assert [
            event_id
            for _room_id, event_id, _encrypted, _token in client.store.load_dispatched_events()
        ] == ["$first"]

        release_second.set()
        await handling
        await client.close()

    async def test_durable_journal_writes_run_off_event_loop(
        self, tempdir, monkeypatch
    ):
        """Per-event crash safety does not synchronously stall asyncio."""
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
        assert client.store
        caller_thread = threading.get_ident()
        writer_threads: List[int] = []
        checkpoint_threads: List[int] = []
        save_dispatched_events = client.store.save_dispatched_events
        save_sync_token_and_prune = (
            client.store.save_sync_token_and_prune_dispatched_events
        )

        def record_writer_thread(room_id, sync_token, events):
            writer_threads.append(threading.get_ident())
            time.sleep(0.01)
            save_dispatched_events(room_id, sync_token, events)

        def record_checkpoint_thread(token):
            checkpoint_threads.append(threading.get_ident())
            time.sleep(0.01)
            save_sync_token_and_prune(token)

        monkeypatch.setattr(
            client.store,
            "save_dispatched_events",
            record_writer_thread,
        )
        monkeypatch.setattr(
            client.store,
            "save_sync_token_and_prune_dispatched_events",
            record_checkpoint_thread,
        )
        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event(f"$event-{index}", ts=index) for index in range(5)],
                limited=False,
                prev_batch="p0",
            )
        )

        assert writer_threads
        assert checkpoint_threads
        assert caller_thread not in writer_threads
        assert caller_thread not in checkpoint_threads
        await client.close()

    async def test_overlap_only_recovery_is_durable_before_checkpoint_prune(
        self, tempdir, aioresponse
    ):
        """An empty recovered list still retags observed DAG overlap."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_max_pages=1,
            store_sync_tokens=True,
        )
        dispatched: List[str] = []

        async def record(_room, event):
            dispatched.append(event.event_id)

        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(login_response))
        first.add_event_callback(record, RoomMessageText)
        await first.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [text_event("$straggler", ts=200)],
                limited=False,
                prev_batch="p0",
            )
        )

        aioresponse.get(
            MESSAGES_URL,
            payload=messages_payload(
                [
                    text_event("$straggler", ts=200),
                    text_event("$live2", ts=300),
                ],
                end=None,
            ),
        )
        await first.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [text_event("$live2", ts=300)],
                limited=True,
                prev_batch="p1",
            )
        )

        assert first.store
        assert {
            event_id
            for _room_id, event_id, _encrypted, _token in first.store.load_dispatched_events()
        } == {"$straggler", "$live2"}
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
        aioresponse.get(
            MESSAGES_URL,
            payload=messages_payload(
                [
                    text_event("$straggler", ts=200),
                    text_event("$latest", ts=400),
                ],
                end=None,
            ),
        )
        await restarted.receive_response(
            sync_response(
                "s3",
                TEST_ROOM_ID,
                [text_event("$latest", ts=400)],
                limited=True,
                prev_batch="p2",
            )
        )

        assert dispatched == ["$straggler", "$live2", "$latest"]
        assert len(dispatched) == len(set(dispatched))
        await restarted.close()

    async def test_sliding_sync_uses_recent_normal_sync_dedup(self, tempdir):
        """Checkpoint pruning cannot make sliding sync replay a recent event."""
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
        dispatched = self._record_callback(client)
        event_a = text_event("$a", ts=1)
        event_b = text_event("$b", ts=2)
        await client.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [event_a],
                limited=False,
                prev_batch="p0",
            )
        )
        await client.receive_response(
            sync_response(
                "s2",
                TEST_ROOM_ID,
                [event_b],
                limited=False,
                prev_batch="p1",
            )
        )
        sliding = SlidingSyncResponse.from_dict(
            {
                "pos": "p1",
                "rooms": {
                    TEST_ROOM_ID: {
                        "membership": "join",
                        "required_state": [],
                        "timeline": [event_a.source, event_b.source],
                    }
                },
            }
        )
        await client.receive_response(sliding)

        assert dispatched == ["$a", "$b"]
        await client.close()

    async def test_sliding_decrypted_upgrade_updates_durable_journal(self, tempdir):
        """A cross-mode decrypted replay remains decrypted after restart."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            store_sync_tokens=True,
        )
        dispatched: List[str] = []

        async def record(_room, event):
            dispatched.append(event.event_id)

        first = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=config,
        )
        await first.receive_response(LoginResponse.from_dict(login_response))
        first.add_event_callback(record, RoomMessageText)
        await first.receive_response(
            sync_response(
                "s1",
                TEST_ROOM_ID,
                [megolm_event("$same", ts=1)],
                limited=False,
                prev_batch="p0",
            )
        )
        decrypted = text_event("$same", ts=1)
        await first.receive_response(
            SlidingSyncResponse.from_dict(
                {
                    "pos": "p1",
                    "rooms": {
                        TEST_ROOM_ID: {
                            "membership": "join",
                            "required_state": [],
                            "timeline": [decrypted.source],
                        }
                    },
                }
            )
        )

        assert first.store
        assert first.store.load_dispatched_events() == [
            (TEST_ROOM_ID, "$same", False, "s1")
        ]
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
            SlidingSyncResponse.from_dict(
                {
                    "pos": "p2",
                    "rooms": {
                        TEST_ROOM_ID: {
                            "membership": "join",
                            "required_state": [],
                            "timeline": [decrypted.source],
                        }
                    },
                }
            )
        )

        assert dispatched == ["$same"]
        await restarted.close()

    async def test_sliding_callback_error_records_prefix_and_attempts_all(
        self, tempdir
    ):
        """A replay cannot repeat callback successes before or after an error."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        first_callback: List[str] = []
        third_callback: List[str] = []

        async def record_first(_room, event):
            first_callback.append(event.event_id)

        async def fail_second(_room, event):
            if event.event_id == "$b":
                raise RuntimeError("second callback failed")

        async def record_third(_room, event):
            third_callback.append(event.event_id)

        client.add_event_callback(record_first, RoomMessageText)
        client.add_event_callback(fail_second, RoomMessageText)
        client.add_event_callback(record_third, RoomMessageText)
        events = [text_event("$a", ts=1), text_event("$b", ts=2)]

        def sliding(pos: str) -> SlidingSyncResponse:
            return SlidingSyncResponse.from_dict(
                {
                    "pos": pos,
                    "rooms": {
                        TEST_ROOM_ID: {
                            "membership": "join",
                            "required_state": [],
                            "timeline": [event.source for event in events],
                        }
                    },
                }
            )

        with pytest.raises(RuntimeError, match="second callback failed"):
            await client.receive_response(sliding("p1"))

        assert first_callback == ["$a", "$b"]
        assert third_callback == ["$a", "$b"]

        await client.receive_response(sliding("p2"))

        assert first_callback == ["$a", "$b"]
        assert third_callback == ["$a", "$b"]
        await client.close()

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
        assert pages.requested_to == ["s2"]

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

        # First room consumed the whole budget. Both live windows remain held
        # until a safe-token recovery reaches them.
        assert dispatched == []
        assert calls["n"] == 1
        assert elapsed < 5
        assert client._last_processed_sync_token == "since_x"
        await client.close()

    async def test_incomplete_retries_rotate_past_stalled_room(
        self, tempdir, aioresponse
    ):
        """A stable first room cannot starve later rooms across retries."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                backfill_limited_timelines=True,
                backfill_timeout=0.05,
            ),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)
        client.next_batch = "since_x"
        requested_rooms: List[str] = []

        async def stall_first_room(url, **kwargs) -> CallbackResult:
            room_id = unquote(
                urlparse(str(url)).path.split("/rooms/", 1)[1].split("/", 1)[0]
            )
            requested_rooms.append(room_id)
            if room_id == TEST_ROOM_ID:
                await asyncio.sleep(30)
            return CallbackResult(
                status=200,
                payload=messages_payload(
                    [
                        text_event("$gap-b", ts=100, room_id=OTHER_ROOM_ID),
                        text_event("$live-b", ts=300, room_id=OTHER_ROOM_ID),
                    ],
                    end=None,
                ),
            )

        aioresponse.get(
            MESSAGES_URL,
            callback=stall_first_room,
            repeat=True,
        )

        def two_room_response(token: str) -> SyncResponse:
            return SyncResponse(
                token,
                Rooms(
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
                                [
                                    text_event(
                                        "$live-b",
                                        ts=300,
                                        room_id=OTHER_ROOM_ID,
                                    )
                                ],
                                True,
                                "p2",
                            ),
                            [],
                            [],
                            [],
                        ),
                    },
                    {},
                ),
                DeviceOneTimeKeyCount(49, 50),
                DeviceList([], []),
                [],
                [],
            )

        await client.receive_response(two_room_response("s2"))

        assert requested_rooms == [TEST_ROOM_ID]
        assert client.next_batch == "since_x"

        await client.receive_response(two_room_response("s3"))

        assert requested_rooms[:2] == [TEST_ROOM_ID, OTHER_ROOM_ID]
        assert "$gap-b" in dispatched
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
        for room_id, room_info in rooms.join.items():
            _SYNC_WINDOWS[("s2", room_id)] = (
                room_info.timeline.prev_batch,
                list(room_info.timeline.events),
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

        assert dispatched == ["$gap-a", "$live-a"]
        assert client._last_processed_sync_token == "since_x"
        await client.close()

    async def test_incomplete_room_does_not_block_complete_room_retry(
        self, tempdir, aioresponse
    ):
        """One limited room cannot lose or duplicate another complete room."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(backfill_limited_timelines=True),
        )
        await client.receive_response(LoginResponse.from_dict(login_response))
        dispatched = self._record_callback(client)
        client.next_batch = "since_x"

        aioresponse.get(MESSAGES_URL, status=500)
        first_rooms = Rooms(
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
                        [
                            text_event(
                                "$complete-b",
                                ts=200,
                                room_id=OTHER_ROOM_ID,
                            )
                        ],
                        False,
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
                first_rooms,
                DeviceOneTimeKeyCount(49, 50),
                DeviceList([], []),
                [],
                [],
            )
        )

        assert dispatched == ["$complete-b"]
        assert client.next_batch == "since_x"

        retry_rooms = Rooms(
            {},
            {
                TEST_ROOM_ID: RoomInfo(
                    Timeline(
                        [
                            text_event("$gap-a", ts=100),
                            text_event("$live-a", ts=300),
                        ],
                        False,
                        "p3",
                    ),
                    [],
                    [],
                    [],
                ),
                OTHER_ROOM_ID: RoomInfo(
                    Timeline(
                        [
                            text_event(
                                "$complete-b",
                                ts=200,
                                room_id=OTHER_ROOM_ID,
                            )
                        ],
                        False,
                        "p4",
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
                "s3",
                retry_rooms,
                DeviceOneTimeKeyCount(49, 50),
                DeviceList([], []),
                [],
                [],
            )
        )

        assert dispatched == ["$complete-b", "$gap-a", "$live-a"]
        assert client.next_batch == "s3"
        assert not client._has_unrecovered_sync_gap
        await client.close()

    async def test_restart_walk_holds_prefix_until_membership_is_known(
        self, tempdir, aioresponse, caplog
    ):
        """A bounded walk cannot expose history before a possible later join."""
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

        # Page 1 yields one event and offers more pages. A later page may still
        # contain our own join, so this prefix is not safe to expose yet.
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

        assert dispatched == []
        assert "$new" not in dispatched
        assert "remaining membership boundary is known" in caplog.text
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

        # The repeated token cannot prove either the membership boundary or
        # the data walk complete, so both the prefix and live delivery wait.
        assert dispatched == ["$old"]
        assert "remaining membership boundary is known" in caplog.text
