"""Contract tests for per-room limited-timeline recovery.

Nio-owned Classic commits its cursor and per-room recovery obligations in one
transaction. Application-owned Classic keeps both in memory and cannot
acknowledge a cursor while any recovery obligation remains.

Each test names the invariant it protects and what breaks without it, because
the failure modes here are silent -- a missed event is never delivered, and no
error is raised anywhere for the loss.
"""

import asyncio
import json
from pathlib import Path

import pytest
from playhouse.sqliteq import SqliteQueueDatabase

from nio import (
    AsyncClient,
    AsyncClientConfig,
    DeviceList,
    DeviceOneTimeKeyCount,
    LocalProtocolError,
    LoginResponse,
    RoomInfo,
    RoomMessageText,
    Rooms,
    SyncResponse,
    Timeline,
    TimelineEventProvenance,
)
from nio.responses import RoomMessagesError, RoomMessagesResponse
from nio.client.sync_recovery import (
    RecoveryGap,
    RecoveryPlan,
    persist_response_plan,
)
from nio.store import SqliteStore

ROOM_A = "!a:example.org"
ROOM_B = "!b:example.org"
LOGIN = json.loads(Path("tests/data/login_response.json").read_text())
OWN_ID = LOGIN["user_id"]


class Transport:
    """The minimum of an aiohttp response that recovery classification reads."""

    def __init__(self, status: int) -> None:
        self.status = status


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


def room_info(events: list, *, limited: bool, prev_batch: str | None) -> RoomInfo:
    return RoomInfo(Timeline(events, limited, prev_batch), [], [], [])


def sync_response(
    token: str,
    joined: dict[str, RoomInfo],
    *,
    left: dict[str, RoomInfo] | None = None,
) -> SyncResponse:
    return SyncResponse(
        token,
        Rooms({}, joined, left or {}),
        DeviceOneTimeKeyCount(49, 50),
        DeviceList([], []),
        [],
        [],
    )


def messages_page(
    events: list[RoomMessageText],
    *,
    start: str,
    end: str | None,
    room_id: str = ROOM_A,
) -> RoomMessagesResponse:
    """One ``/messages`` page. ``end=None`` is the server saying it has no more."""
    body: dict = {"start": start, "chunk": [event.source for event in events]}
    if end is not None:
        body["end"] = end
    response = RoomMessagesResponse.from_dict(body, room_id)
    assert isinstance(response, RoomMessagesResponse)
    return response


def messages_error(status: int, room_id: str = ROOM_A) -> RoomMessagesError:
    response = RoomMessagesError.from_dict(
        {"errcode": "M_UNKNOWN", "error": "recovery fetch failed"},
        room_id,
    )
    assert isinstance(response, RoomMessagesError)
    response.transport_response = Transport(status)
    return response


def application_owned_client(tempdir: str) -> AsyncClient:
    """Return a memory-only Classic client whose application owns the cursor."""
    return AsyncClient(
        "https://example.org",
        OWN_ID,
        "DEVICEID",
        tempdir,
        config=AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_persist_recovery=False,
            store_sync_tokens=False,
        ),
    )


def nio_owned_client(tempdir: str) -> AsyncClient:
    """Return a Classic client that atomically owns its cursor and recovery."""
    return AsyncClient(
        "https://example.org",
        OWN_ID,
        "DEVICEID",
        tempdir,
        config=AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_persist_recovery=True,
            store_sync_tokens=True,
        ),
    )


class RecordingFetch:
    """A scripted ``/messages`` endpoint that records what recovery asked for."""

    def __init__(self, pages: list) -> None:
        self.pages = list(pages)
        self.calls: list[tuple[str, str, str | None]] = []

    async def __call__(self, room_id, start, end, _direction, _limit):
        self.calls.append((room_id, start, end))
        if not self.pages:
            pytest.fail("unscripted recovery fetch")
        return self.pages.pop(0)


class QueueStore(SqliteStore):
    """A real-file queued store whose multi-statement writes are not atomic."""

    supports_atomic_recovery = False

    def _create_database(self):
        return SqliteQueueDatabase(
            self.database_path,
            pragmas={"foreign_keys": 1, "journal_mode": "wal"},
            autostart=True,
        )


class Delivered:
    """Every event the application saw, in the order the transport dispatched it."""

    def __init__(self) -> None:
        self.events: list[tuple[str, str]] = []
        self.provenance: dict[str, TimelineEventProvenance] = {}

    def install(self, client: AsyncClient) -> None:
        async def on_event(room, event):
            self.events.append((room.room_id, event.event_id))

        async def on_admission(_room, event, provenance):
            self.provenance[event.event_id] = provenance

        client.add_event_callback(on_event, RoomMessageText)
        client.add_event_admission_callback(on_admission, RoomMessageText)

    def ids(self, room_id: str) -> list[str]:
        return [event_id for room, event_id in self.events if room == room_id]


@pytest.fixture
def delivered() -> Delivered:
    return Delivered()


async def open_a_stuck_gap(
    client: AsyncClient,
    fetch: RecordingFetch,
    monkeypatch,
    *,
    token: str = "s1",
    other_rooms: dict[str, RoomInfo] | None = None,
) -> SyncResponse:
    """Drive one limited response for ROOM_A whose recovery walk does not close.

    Returns the response so a caller can assert on its published outcome.
    """
    monkeypatch.setattr(client, "_recovery_room_messages", fetch)
    client.next_batch = "s0"
    response = sync_response(
        token,
        {
            ROOM_A: room_info(
                [text_event("$live", 2)],
                limited=True,
                prev_batch="p1",
            ),
            **(other_rooms or {}),
        },
    )
    await client.receive_response(response)
    return response


@pytest.mark.asyncio
class TestPerRoomRecoveryContract:
    async def test_application_owned_classic_replays_after_reset_and_fences_open_gap(
        self,
        tempdir,
        monkeypatch,
        delivered,
    ):
        """Memory-only Classic can rewind, replay, and cannot commit past a gap."""
        client = application_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        delivered.install(client)
        fetch = RecordingFetch([messages_error(429)])
        monkeypatch.setattr(client, "_recovery_room_messages", fetch)
        client.next_batch = "s0"
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

        with pytest.raises(LocalProtocolError, match="does not match"):
            client.acknowledge_classic_sync("s1")

        await client.reset_classic_sync_state()
        client.next_batch = "s0"
        fetch.pages.append(
            messages_page([text_event("$missed", 1)], start="s0", end="p1")
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

        assert delivered.ids(ROOM_A) == ["$missed", "$live"]
        client.acknowledge_classic_sync("s1")
        await client.close()

    async def test_nio_owned_classic_atomically_persists_token_and_gap_for_restart(
        self,
        tempdir,
        monkeypatch,
    ):
        """Nio commits a Classic cursor with its gap and restores both together."""
        config = AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_persist_recovery=True,
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
        assert client.store
        writes: list[tuple[str | None, tuple[RecoveryGap, ...]]] = []
        original_save = client.store.save_recovery

        def record_save(token, clear_rooms, gaps, events, clear_recovered, *args):
            writes.append((token, tuple(gaps)))
            return original_save(
                token,
                clear_rooms,
                gaps,
                events,
                clear_recovered,
                *args,
            )

        monkeypatch.setattr(client.store, "save_recovery", record_save)
        fetch = RecordingFetch([messages_error(429)])
        await open_a_stuck_gap(client, fetch, monkeypatch)
        gap = client._recovery.gaps[ROOM_A][0]

        assert writes[0] == ("s1", (gap,))
        assert client.store.load_sync_token() == "s1"
        [stored_gap] = client.store.load_sync_recovery()[0]
        assert (
            stored_gap.room_id,
            stored_gap.generation,
            stored_gap.target_token,
            stored_gap.cursor_token,
            stored_gap.membership_bound,
        ) == (
            gap.room_id,
            gap.generation,
            gap.target_token,
            gap.cursor_token,
            gap.membership_bound,
        )
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

        assert restarted.loaded_sync_token == "s1"
        assert restarted._recovery.gaps[ROOM_A] == [gap]
        await restarted.close()

    async def test_clearing_a_real_gap_persists_sticky_abandonment(self, tempdir):
        """Every real-gap deletion records loss in memory and on disk."""
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        assert client.store

        persist_response_plan(
            client._recovery,
            client.store,
            token=None,
            plan=RecoveryPlan(
                gaps=(RecoveryGap(ROOM_A, 1, "target", "cursor"),),
            ),
        )
        persist_response_plan(
            client._recovery,
            client.store,
            token=None,
            plan=RecoveryPlan(clear_rooms=frozenset({ROOM_A})),
        )

        assert ROOM_A not in client._recovery.gaps
        assert client._recovery.abandoned == {ROOM_A}
        gaps, _events, abandoned = client.store.load_sync_recovery()
        assert gaps == []
        assert abandoned == [ROOM_A]
        await client.close()

    async def test_unresolved_gap_does_not_block_the_global_cursor(
        self,
        tempdir,
        monkeypatch,
    ):
        """Contract 1: one room's open gap blocks that room, not the checkpoint.

        The gap carries its own bounded endpoints -- ``cursor_token`` is the
        ``since`` the limited response was measured from and ``target_token`` is
        its ``prev_batch``. Both are pinned the moment the gap is planned, so
        advancing the global cursor cannot enlarge the walk that still has to
        happen. Refusing to advance is what enlarges it: the next request asks
        for the same ``since`` against a live position that moved, so the gap
        grows every cycle and never converges.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        fetch = RecordingFetch([messages_error(429)])
        await open_a_stuck_gap(client, fetch, monkeypatch)

        assert client._recovery.gaps[ROOM_A], "the gap must stay open for this room"
        assert fetch.pages == []
        assert client.store
        assert client.store.load_sync_token() == "s1"
        await client.close()

    async def test_gap_without_durability_still_fences_the_cursor(
        self,
        tempdir,
        monkeypatch,
    ):
        """The safety half of contract 1: only a *durable* gap may be passed.

        Advancing the checkpoint past an in-memory gap is not forward progress,
        it is silent loss -- the committed cursor is already beyond the limited
        response, so nothing in a replay reaches back to plan the walk again and
        the only record that those events were owed dies with the process.
        """
        client = application_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        fetch = RecordingFetch([messages_error(429)])
        await open_a_stuck_gap(client, fetch, monkeypatch)

        assert client._recovery_store is None
        assert fetch.pages == []
        with pytest.raises(LocalProtocolError, match="does not match"):
            client.acknowledge_classic_sync("s1")
        await client.close()

    async def test_non_atomic_queue_store_rejects_abandonment_before_mutation(
        self,
        tempdir,
    ):
        """Multi-chunk loss settlement requires an explicitly atomic store."""
        bootstrap = SqliteStore(OWN_ID, LOGIN["device_id"], tempdir)
        bootstrap.database.close()
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                store=QueueStore,
                backfill_limited_timelines=True,
                backfill_persist_recovery=True,
                store_sync_tokens=True,
            ),
        )
        store = None
        try:
            await client.receive_response(LoginResponse.from_dict(LOGIN))
            assert isinstance(client.store, QueueStore)
            store = client.store
            room_ids = frozenset(
                f"!abandoned-{index}:example.org" for index in range(81)
            )
            store.save_recovery(
                None,
                set(),
                [],
                [],
                None,
                abandoned_rooms=room_ids,
            )
            store.database.stop()
            client._recovery.abandoned.update(room_ids)
            before_memory = client._recovery.abandoned.copy()
            before_rows = store.load_sync_recovery()[2]
            assert len(before_rows) == 81
            assert set(before_rows) == room_ids
            store.database.start()

            with pytest.raises(LocalProtocolError, match="atomic recovery"):
                client.acknowledge_unrecovered_rooms(room_ids)

            assert client._recovery.abandoned == before_memory
            assert store.load_sync_recovery()[2] == before_rows
        finally:
            try:
                await client.close()
            finally:
                if store is not None:
                    if not store.database.is_stopped():
                        store.database.stop()
                    if not store.database.is_closed():
                        store.database.close()

    async def test_non_atomic_queue_store_rejects_real_gap_clear_before_mutation(
        self,
        tempdir,
    ):
        """Deleting a real gap and recording its loss must be one transaction."""
        bootstrap = SqliteStore(OWN_ID, LOGIN["device_id"], tempdir)
        bootstrap.database.close()
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                store=QueueStore,
                backfill_limited_timelines=True,
                backfill_persist_recovery=True,
                store_sync_tokens=True,
            ),
        )
        store = None
        try:
            await client.receive_response(LoginResponse.from_dict(LOGIN))
            assert isinstance(client.store, QueueStore)
            store = client.store
            gap = RecoveryGap(ROOM_A, 1, "target", "cursor")
            persist_response_plan(
                client._recovery,
                store,
                token=None,
                plan=RecoveryPlan(gaps=(gap,)),
            )
            store.database.stop()

            def persisted_recovery():
                gaps, events, abandoned = store.load_sync_recovery()
                return (
                    [
                        (
                            item.room_id,
                            item.generation,
                            item.target_token,
                            item.cursor_token,
                            bool(item.membership_bound),
                        )
                        for item in gaps
                    ],
                    events,
                    abandoned,
                )

            expected_rows = ([(ROOM_A, 1, "target", "cursor", False)], [], [])
            assert persisted_recovery() == expected_rows
            before_gaps = {
                room_id: list(room_gaps)
                for room_id, room_gaps in client._recovery.gaps.items()
            }
            before_abandoned = client._recovery.abandoned.copy()
            before_outcomes = client._recovery.outcomes.copy()
            store.database.start()

            with pytest.raises(LocalProtocolError, match="atomic recovery"):
                persist_response_plan(
                    client._recovery,
                    store,
                    token=None,
                    plan=RecoveryPlan(clear_rooms=frozenset({ROOM_A})),
                )

            assert client._recovery.gaps == before_gaps
            assert client._recovery.abandoned == before_abandoned
            assert client._recovery.outcomes == before_outcomes
            assert persisted_recovery() == expected_rows
        finally:
            try:
                await client.close()
            finally:
                if store is not None:
                    if not store.database.is_stopped():
                        store.database.stop()
                    if not store.database.is_closed():
                        store.database.close()

    async def test_other_rooms_keep_delivering_while_one_room_is_blocked(
        self,
        tempdir,
        monkeypatch,
        delivered,
    ):
        """Contract 2: a blocked room must not stall unrelated rooms.

        Without this, one wedged room silences an entire principal: every other
        conversation stops being answered even though nothing is wrong with it.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        delivered.install(client)
        fetch = RecordingFetch([messages_error(429), messages_error(429)])
        await open_a_stuck_gap(
            client,
            fetch,
            monkeypatch,
            other_rooms={
                ROOM_B: room_info(
                    [text_event("$b1", 3, ROOM_B)],
                    limited=False,
                    prev_batch="p0",
                )
            },
        )

        client.next_batch = "s1"
        second = sync_response(
            "s2",
            {
                ROOM_B: room_info(
                    [text_event("$b2", 4, ROOM_B)],
                    limited=False,
                    prev_batch="p0",
                )
            },
        )
        await client.receive_response(second)

        assert delivered.ids(ROOM_B) == ["$b1", "$b2"]
        assert delivered.provenance["$b2"] is TimelineEventProvenance.LIVE
        assert ROOM_B not in second.unrecovered_room_ids
        assert ROOM_A in second.unrecovered_room_ids
        assert fetch.pages == []
        await client.close()

    async def test_recovery_obligation_survives_a_restart(self, tempdir, monkeypatch):
        """Contract 3: the obligation is recoverable from disk, not only memory.

        A process that dies with an open gap must not lose it. Nio's committed
        checkpoint is already past the limited response, so nothing in
        a replay from that checkpoint would ever ask for those events again --
        the only record that they are owed is the persisted gap row.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        fetch = RecordingFetch([messages_error(429)])
        await open_a_stuck_gap(client, fetch, monkeypatch)
        gap = client._recovery.gaps[ROOM_A][0]
        assert fetch.pages == []
        await client.close()
        assert client.store
        client.store.database.close()

        restarted = nio_owned_client(tempdir)
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))

        assert ROOM_A in restarted._recovery.gaps, "the gap must outlive the process"
        restored = restarted._recovery.gaps[ROOM_A][0]
        assert restored.cursor_token == gap.cursor_token
        assert restored.target_token == gap.target_token
        await restarted.close()

    async def test_later_events_do_not_overtake_an_open_gap(
        self,
        tempdir,
        monkeypatch,
        delivered,
    ):
        """Contract 4: same-room ordering holds across the gap.

        Delivering a later message before the ones it replies to gives the
        application a conversation it cannot read: an answer arrives before its
        question, and any thread built from delivery order is wrong.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        delivered.install(client)
        fetch = RecordingFetch([messages_error(429), messages_error(429)])
        await open_a_stuck_gap(client, fetch, monkeypatch)

        client.next_batch = "s1"
        await client.receive_response(
            sync_response(
                "s2",
                {
                    ROOM_A: room_info(
                        [text_event("$later", 5)],
                        limited=False,
                        prev_batch="p2",
                    )
                },
            )
        )

        assert (
            delivered.ids(ROOM_A) == []
        ), "nothing from this room may be delivered while its gap is open"
        assert fetch.pages == []
        await client.close()

    async def test_recovered_events_arrive_once_in_order_with_provenance(
        self,
        tempdir,
        monkeypatch,
        delivered,
    ):
        """Contract 5: a closed gap delivers the missed events as work, in order.

        RECOVERED is the only signal that separates "the bot never saw this" from
        "this is backdrop the bot is reading". A missed question delivered as
        HISTORY is never answered, and one delivered twice is answered twice.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        delivered.install(client)
        fetch = RecordingFetch(
            [
                messages_error(429),
                messages_page([text_event("$missed", 1)], start="s0", end="p1"),
            ]
        )
        await open_a_stuck_gap(client, fetch, monkeypatch)

        client.next_batch = "s1"
        third = sync_response(
            "s2",
            {ROOM_A: room_info([], limited=False, prev_batch="p2")},
        )
        await client.receive_response(third)

        assert delivered.ids(ROOM_A) == ["$missed", "$live"]
        assert delivered.provenance["$missed"] is TimelineEventProvenance.RECOVERED
        assert delivered.provenance["$live"] is TimelineEventProvenance.LIVE
        assert ROOM_A in third.recovered_room_ids
        assert ROOM_A not in third.unrecovered_room_ids
        assert fetch.pages == []
        await client.close()

    async def test_abandoned_gap_stays_explicitly_degraded(
        self,
        tempdir,
        monkeypatch,
        delivered,
    ):
        """Contract 6: giving up is reported for as long as the loss stands.

        A hard ``/messages`` rejection makes nio drop the gap and report the room
        unrecovered exactly once. Every later response then describes the room as
        healthy, so an application that missed that single edge cannot tell a room
        with permanently lost work from one that recovered. The room must stay
        distinguishable until the missed events are actually delivered.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        delivered.install(client)
        fetch = RecordingFetch([messages_error(403)])
        first = await open_a_stuck_gap(client, fetch, monkeypatch)
        assert ROOM_A in first.unrecovered_room_ids

        client.next_batch = "s1"
        second = sync_response(
            "s2",
            {ROOM_A: room_info([], limited=False, prev_batch="p2")},
        )
        await client.receive_response(second)

        assert delivered.ids(ROOM_A) == ["$live"]
        assert (
            ROOM_A in second.unrecovered_room_ids
        ), "an unrepaid loss must not read as a healthy room on the next response"
        assert fetch.pages == []
        await client.close()

    async def test_abandonment_outlives_a_restart_until_acknowledged(
        self,
        tempdir,
        monkeypatch,
        delivered,
    ):
        """A loss is durable, and only the application may retire it.

        Restarting is the easiest way to lose a degraded-room signal, and the
        one most likely to happen while nobody is looking. Clearing has to be an
        explicit act because only the application knows whether it recorded the
        loss anywhere.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        delivered.install(client)
        fetch = RecordingFetch([messages_error(403)])
        await open_a_stuck_gap(client, fetch, monkeypatch)
        assert client._recovery.abandoned == {ROOM_A}
        assert fetch.pages == []
        await client.close()
        assert client.store
        client.store.database.close()

        restarted = nio_owned_client(tempdir)
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        assert restarted._recovery.abandoned == {ROOM_A}

        restarted.next_batch = "s1"
        after_restart = sync_response(
            "s2",
            {ROOM_A: room_info([], limited=False, prev_batch="p2")},
        )
        await restarted.receive_response(after_restart)
        assert ROOM_A in after_restart.unrecovered_room_ids

        assert restarted.acknowledge_unrecovered_rooms([ROOM_A]) == frozenset({ROOM_A})
        assert restarted.acknowledge_unrecovered_rooms([ROOM_A]) == frozenset()

        restarted.next_batch = "s2"
        settled = sync_response(
            "s3",
            {ROOM_A: room_info([], limited=False, prev_batch="p3")},
        )
        await restarted.receive_response(settled)
        assert ROOM_A not in settled.unrecovered_room_ids
        await restarted.close()
        assert restarted.store
        restarted.store.database.close()

        reopened = nio_owned_client(tempdir)
        await reopened.receive_response(LoginResponse.from_dict(LOGIN))
        assert (
            reopened._recovery.abandoned == set()
        ), "acknowledgement must be durable too, or the loss returns on restart"
        await reopened.close()

    @pytest.mark.parametrize("recovery_status", [403, 429])
    async def test_membership_reset_never_settles_or_hides_a_real_loss(
        self,
        tempdir,
        monkeypatch,
        recovery_status,
    ):
        """Resetting room recovery cannot stand in for application settlement."""
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        fetch = RecordingFetch([messages_error(recovery_status)])
        await open_a_stuck_gap(client, fetch, monkeypatch)
        assert fetch.pages == []

        reset = sync_response(
            "s2",
            {},
            left={ROOM_A: room_info([], limited=False, prev_batch=None)},
        )
        await client.receive_response(reset)

        assert client._recovery.abandoned == {ROOM_A}
        assert client.store
        assert client.store.load_sync_recovery()[2] == [ROOM_A]

        later = sync_response("s3", {})
        await client.receive_response(later)
        assert later.unrecovered_room_ids == frozenset({ROOM_A})
        await client.close()

    async def test_failed_abandonment_acknowledgement_is_retryable(
        self,
        tempdir,
        monkeypatch,
    ):
        """A failed durable clear must leave memory available for a retry."""
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        fetch = RecordingFetch([messages_error(403)])
        await open_a_stuck_gap(client, fetch, monkeypatch)
        assert fetch.pages == []
        assert client.store
        store = client.store
        original = store.clear_recovery_abandonment

        def fail(_room_ids):
            raise OSError("disk failure")

        monkeypatch.setattr(store, "clear_recovery_abandonment", fail)
        with pytest.raises(OSError, match="disk failure"):
            client.acknowledge_unrecovered_rooms([ROOM_A])
        assert client._recovery.abandoned == {ROOM_A}

        monkeypatch.setattr(store, "clear_recovery_abandonment", original)
        assert client.acknowledge_unrecovered_rooms([ROOM_A]) == frozenset({ROOM_A})
        assert client._recovery.abandoned == set()
        assert store.load_sync_recovery()[2] == []
        await client.close()

    @pytest.mark.parametrize("last_page", ["empty", "final_events"])
    async def test_page_without_end_closes_a_bounded_walk_as_exhausted(
        self,
        tempdir,
        monkeypatch,
        delivered,
        last_page,
    ):
        """Contract 7: ``end=None`` on a bounded walk is a terminus, not a failure.

        A server with nothing older to serve omits ``end``. Because the gap is
        bounded below by the ``since`` it was measured from, running out of pages
        proves the walk saw everything between the two endpoints. Treating that
        as a failure would wedge every room whose history the server has trimmed.

        Both shapes matter and only the second one observes the classification:
        an empty last page has nothing to reclassify, so it proves only that the
        walk terminates cleanly. A last page that still carries events proves the
        stronger half -- exhausting a bounded walk is continuity *proof*, so its
        events are work the bot missed, not backdrop.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        delivered.install(client)
        pages = (
            [messages_page([], start="s0", end=None)]
            if last_page == "empty"
            else [
                messages_page([text_event("$missed", 1)], start="s0", end="mid"),
                messages_page([text_event("$older", 0)], start="mid", end=None),
            ]
        )
        fetch = RecordingFetch(pages)
        response = await open_a_stuck_gap(client, fetch, monkeypatch)

        expected = ["$live"] if last_page == "empty" else ["$missed", "$older", "$live"]
        assert delivered.ids(ROOM_A) == expected
        for event_id in expected[:-1]:
            assert (
                delivered.provenance[event_id] is TimelineEventProvenance.RECOVERED
            ), "an exhausted bounded walk proves continuity, so its events are work"
        assert ROOM_A in response.recovered_room_ids
        assert ROOM_A not in response.unrecovered_room_ids
        assert ROOM_A not in client._recovery.gaps
        assert fetch.pages == []
        await client.close()

    @pytest.mark.parametrize("transient_status", [408, 429, 503])
    async def test_transient_failure_retries_without_loss_or_duplication(
        self,
        tempdir,
        monkeypatch,
        delivered,
        transient_status,
    ):
        """Contract 8: transient failures retry, without replaying delivery.

        A retry that re-dispatches an already-delivered event makes the bot answer
        twice; one that skips the un-dispatched remainder loses the question
        entirely. Both are invisible without pinning the exact delivered sequence.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        delivered.install(client)
        fetch = RecordingFetch(
            [
                messages_error(transient_status),
                messages_page([text_event("$missed", 1)], start="s0", end="p1"),
            ]
        )
        await open_a_stuck_gap(client, fetch, monkeypatch)
        assert delivered.ids(ROOM_A) == []

        await client._pump_sync_recovery()

        assert delivered.ids(ROOM_A) == ["$missed", "$live"]
        assert delivered.provenance["$missed"] is TimelineEventProvenance.RECOVERED
        assert [call[1] for call in fetch.calls] == ["s0", "s0"]
        assert fetch.pages == []
        await client.close()
