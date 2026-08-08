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
import logging
from pathlib import Path

import pytest
from peewee import SqliteDatabase
from playhouse.sqliteq import SqliteQueueDatabase

from nio import (
    AsyncClient,
    AsyncClientConfig,
    DeviceList,
    DeviceOneTimeKeyCount,
    LocalProtocolError,
    LoginResponse,
    RecoveryAbandonment,
    RoomForgetResponse,
    RoomInfo,
    RoomLeaveResponse,
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


class UndeclaredAtomicStore(SqliteStore):
    """A SQLite-backed subclass that has not declared its recovery contract."""

    def _create_database(self):
        return SqliteDatabase(":memory:", pragmas={"foreign_keys": 1})


class LegacyRecoveryStore(UndeclaredAtomicStore):
    """A non-atomic custom store implementing the released eight-argument API."""

    def save_recovery(
        self,
        token,
        clear_rooms,
        gaps,
        events,
        clear_recovered,
        window_tokens=None,
        forgotten_rooms=(),
        abandoned_rooms=(),
    ):
        return super().save_recovery(
            token,
            clear_rooms,
            gaps,
            events,
            clear_recovered,
            window_tokens,
            forgotten_rooms,
            abandoned_rooms,
        )

    def finish_recovery(self, room_id, generation, event_id, was_encrypted):
        return super().finish_recovery(room_id, generation, event_id, was_encrypted)


class LegacyAtomicRecoveryStore(LegacyRecoveryStore):
    """An atomic custom store still using the released save signature."""

    supports_atomic_recovery = True


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

        def record_save(
            token, clear_rooms, gaps, events, clear_recovered, *args, **kwargs
        ):
            writes.append((token, tuple(gaps)))
            return original_save(
                token,
                clear_rooms,
                gaps,
                events,
                clear_recovered,
                *args,
                **kwargs,
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
            plan=RecoveryPlan(
                clear_rooms=frozenset({ROOM_A}),
                abandoned_rooms={ROOM_A: RecoveryAbandonment.BASELINE_LOST},
            ),
        )

        assert ROOM_A not in client._recovery.gaps
        assert client._recovery.abandoned == {
            ROOM_A: frozenset({RecoveryAbandonment.BASELINE_LOST})
        }
        gaps, _events, abandoned = client.store.load_sync_recovery()
        assert gaps == []
        assert abandoned == {ROOM_A: frozenset({RecoveryAbandonment.BASELINE_LOST})}
        await client.close()

    @pytest.mark.expects_anonymous_recovery_clear
    async def test_a_clear_that_names_no_cause_is_recorded_as_unknown(
        self,
        tempdir,
        caplog,
    ):
        """Persistence records the loss but refuses to invent the cause.

        Whether a clear drops a real gap is a fact about persisted state, so
        persistence reads it; *why* is known only to whoever cleared the room.
        A producer that fails to say is a defect -- but a durable sync path is
        the wrong place to raise, so the loss is still recorded, under a reason
        that claims nothing, and the defect is surfaced in the log.

        No production path reaches this: every real-gap-clearing site names its
        cause, which is what
        ``test_no_production_path_clears_a_real_gap_anonymously`` pins.
        """
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        assert client.store

        persist_response_plan(
            client._recovery,
            client.store,
            token=None,
            plan=RecoveryPlan(gaps=(RecoveryGap(ROOM_A, 1, "target", "cursor"),)),
        )
        with caplog.at_level(logging.ERROR, logger="nio.client.sync_recovery"):
            persist_response_plan(
                client._recovery,
                client.store,
                token=None,
                plan=RecoveryPlan(clear_rooms=frozenset({ROOM_A})),
            )

        assert client._recovery.abandoned == {
            ROOM_A: frozenset({RecoveryAbandonment.UNKNOWN})
        }
        assert client.store.load_sync_recovery()[2] == {
            ROOM_A: frozenset({RecoveryAbandonment.UNKNOWN})
        }
        assert "without naming a cause" in caplog.text
        await client.close()

    @pytest.mark.parametrize("operation", ["room_leave", "room_forget"])
    async def test_no_production_path_clears_a_real_gap_anonymously(
        self,
        tempdir,
        monkeypatch,
        caplog,
        operation,
    ):
        """The ``UNKNOWN`` fallback is a backstop, not a live code path."""
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        persist_response_plan(
            client._recovery,
            client.store,
            token=None,
            plan=RecoveryPlan(gaps=(RecoveryGap(ROOM_A, 1, "target", "cursor"),)),
        )

        async def send(response_class, *_args, **_kwargs):
            if response_class is RoomForgetResponse:
                return RoomForgetResponse.from_dict({}, ROOM_A)
            return RoomLeaveResponse.from_dict({})

        monkeypatch.setattr(client, "_send", send)
        with caplog.at_level(logging.ERROR, logger="nio.client.sync_recovery"):
            await getattr(client, operation)(ROOM_A)

        assert client._recovery.abandoned == {
            ROOM_A: frozenset({RecoveryAbandonment.BASELINE_LOST})
        }
        assert "without naming a cause" not in caplog.text
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

    async def test_non_atomic_store_rejects_sync_response_before_mutation(
        self,
        tempdir,
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                store=UndeclaredAtomicStore,
                backfill_limited_timelines=True,
                backfill_persist_recovery=True,
                store_sync_tokens=True,
            ),
        )
        try:
            await client.receive_response(LoginResponse.from_dict(LOGIN))
            assert client.store
            assert client.store.supports_atomic_recovery is False
            before_next_batch = client.next_batch

            with pytest.raises(LocalProtocolError, match="atomic recovery"):
                await client.receive_response(
                    sync_response(
                        "s1",
                        {ROOM_A: room_info([], limited=False, prev_batch="p0")},
                    )
                )

            assert not client._sync_response_seen
            assert client.next_batch == before_next_batch
            assert client.rooms == {}
            assert client._recovery.gaps == {}
            assert client._recovery.events == {}
            assert client._recovery.outcomes == {}
            assert client.store.load_sync_token() is None
            assert client.store.load_sync_recovery() == ([], [], {})
        finally:
            await client.close()
            if client.store:
                client.store.database.close()

    async def test_non_atomic_store_rejects_membership_reset_before_network_io(
        self,
        tempdir,
    ):
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(
                store=UndeclaredAtomicStore,
                backfill_limited_timelines=True,
                backfill_persist_recovery=True,
                store_sync_tokens=True,
            ),
        )
        network_called = False

        async def send_request():
            nonlocal network_called
            network_called = True
            return object()

        try:
            await client.receive_response(LoginResponse.from_dict(LOGIN))
            assert client.store

            with pytest.raises(LocalProtocolError, match="atomic recovery"):
                await client._run_room_membership_reset(ROOM_A, send_request)

            assert not network_called
            assert client._recovery.gaps == {}
            assert client._recovery.abandoned == {}
            assert client.store.load_sync_recovery() == ([], [], {})
        finally:
            await client.close()
            if client.store:
                client.store.database.close()

    @pytest.mark.parametrize(
        "response",
        [RoomLeaveResponse(), RoomForgetResponse(ROOM_A)],
    )
    @pytest.mark.parametrize("synthetic_gap", [False, True])
    @pytest.mark.parametrize(
        "store_class",
        [LegacyRecoveryStore, LegacyAtomicRecoveryStore],
    )
    async def test_legacy_store_allows_disabled_reset_without_a_real_gap(
        self,
        tempdir,
        response,
        synthetic_gap,
        store_class,
    ):
        """Ordinary leave/forget stays available when no real gap is at risk."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(store=store_class),
        )
        network_called = False

        async def send_request():
            nonlocal network_called
            network_called = True
            return response

        try:
            await client.receive_response(LoginResponse.from_dict(LOGIN))
            assert client.store
            assert not client.config.backfill_limited_timelines
            if synthetic_gap:
                gap = RecoveryGap(ROOM_A, 1, "", None)
                client.store.save_recovery(None, set(), [gap], [], None)

            result = await client._run_room_membership_reset(ROOM_A, send_request)

            assert result is response
            assert network_called
            assert client.store.load_sync_recovery() == ([], [], {})
        finally:
            await client.close()
            if client.store:
                client.store.database.close()

    @pytest.mark.parametrize(
        "response",
        [RoomLeaveResponse(), RoomForgetResponse(ROOM_A)],
    )
    async def test_non_atomic_store_rejects_disabled_reset_with_a_persisted_gap(
        self,
        tempdir,
        response,
    ):
        """A store-only real gap still requires atomic loss recording."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(store=LegacyRecoveryStore),
        )
        network_called = False

        async def send_request():
            nonlocal network_called
            network_called = True
            return response

        try:
            await client.receive_response(LoginResponse.from_dict(LOGIN))
            assert client.store
            gap = RecoveryGap(ROOM_A, 1, "target", "cursor")
            client.store.save_recovery(None, set(), [gap], [], None)

            with pytest.raises(LocalProtocolError, match="atomic recovery"):
                await client._run_room_membership_reset(ROOM_A, send_request)

            assert not network_called
            gaps, _, abandoned = client.store.load_sync_recovery()
            assert [(item.room_id, item.target_token) for item in gaps] == [
                (ROOM_A, "target")
            ]
            assert abandoned == {}
        finally:
            await client.close()
            if client.store:
                client.store.database.close()

    @pytest.mark.parametrize(
        "response",
        [RoomLeaveResponse(), RoomForgetResponse(ROOM_A)],
    )
    @pytest.mark.expects_anonymous_recovery_clear
    async def test_legacy_atomic_store_receives_released_call_shapes_after_reset(
        self,
        tempdir,
        response,
    ):
        """Server success is followed by eight-argument save compatibility."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(store=LegacyAtomicRecoveryStore),
        )
        network_called = False

        async def send_request():
            nonlocal network_called
            network_called = True
            return response

        try:
            await client.receive_response(LoginResponse.from_dict(LOGIN))
            assert client.store
            client.store.save_recovery(
                None,
                set(),
                [RecoveryGap(ROOM_A, 1, "target", "cursor")],
                [],
                None,
            )

            result = await client._run_room_membership_reset(ROOM_A, send_request)

            assert result is response
            assert network_called
            gaps, _, abandoned = client.store.load_sync_recovery()
            assert gaps == []
            assert abandoned == {ROOM_A: frozenset({RecoveryAbandonment.UNKNOWN})}
        finally:
            await client.close()
            if client.store:
                client.store.database.close()

    async def test_legacy_atomic_store_ignores_gap_free_structural_candidate(
        self,
        tempdir,
    ):
        """A structural reason is not itself an actual abandoned room."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(store=LegacyAtomicRecoveryStore),
        )
        try:
            await client.receive_response(LoginResponse.from_dict(LOGIN))
            assert client.store

            persist_response_plan(
                client._recovery,
                client.store,
                token=None,
                plan=RecoveryPlan(
                    clear_rooms=frozenset({ROOM_A}),
                    clear_room_reasons={
                        ROOM_A: RecoveryAbandonment.BASELINE_LOST,
                    },
                ),
            )

            assert client._recovery.abandoned == {}
            assert client.store.load_sync_recovery() == ([], [], {})
        finally:
            await client.close()
            if client.store:
                client.store.database.close()

    async def test_legacy_atomic_store_receives_materialized_real_gap_abandonment(
        self,
        tempdir,
    ):
        """A real in-memory gap still becomes an eighth-argument room ID."""
        client = AsyncClient(
            "https://example.org",
            OWN_ID,
            "DEVICEID",
            tempdir,
            config=AsyncClientConfig(store=LegacyAtomicRecoveryStore),
        )
        try:
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
                plan=RecoveryPlan(
                    clear_rooms=frozenset({ROOM_A}),
                    clear_room_reasons={
                        ROOM_A: RecoveryAbandonment.BASELINE_LOST,
                    },
                ),
            )

            assert client._recovery.abandoned == {
                ROOM_A: frozenset({RecoveryAbandonment.BASELINE_LOST})
            }
            assert client.store.load_sync_recovery()[2] == {
                ROOM_A: frozenset({RecoveryAbandonment.UNKNOWN})
            }
        finally:
            await client.close()
            if client.store:
                client.store.database.close()

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
            room_ids = {
                f"!abandoned-{index}:example.org": RecoveryAbandonment.UNVERIFIABLE
                for index in range(81)
            }
            store.save_recovery(
                None,
                set(),
                [],
                [],
                None,
                abandoned_room_reasons=room_ids,
            )
            store.database.stop()
            room_reasons = {
                room_id: frozenset({reason}) for room_id, reason in room_ids.items()
            }
            client._recovery.abandoned.update(room_reasons)
            before_memory = client._recovery.abandoned.copy()
            before_rows = store.load_sync_recovery()[2]
            assert len(before_rows) == 81
            assert before_rows == room_reasons
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
        caplog,
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

            expected_rows = ([(ROOM_A, 1, "target", "cursor", False)], [], {})
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

            assert "without naming a cause" not in caplog.text
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

    async def test_builtin_store_preserves_multiple_causes(
        self,
        tempdir,
    ):
        """An opted-in store persists every independently discovered cause."""
        client = nio_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        assert client.store

        persist_response_plan(
            client._recovery,
            client.store,
            token=None,
            plan=RecoveryPlan(
                abandoned_rooms={ROOM_A: RecoveryAbandonment.UNVERIFIABLE}
            ),
        )
        persist_response_plan(
            client._recovery,
            client.store,
            token=None,
            plan=RecoveryPlan(
                abandoned_rooms={ROOM_A: RecoveryAbandonment.EVENT_LIMIT}
            ),
        )

        assert client.store.load_sync_recovery()[2] == {
            ROOM_A: frozenset(
                {
                    RecoveryAbandonment.EVENT_LIMIT,
                    RecoveryAbandonment.UNVERIFIABLE,
                }
            )
        }
        await client.close()

    async def test_abandonment_is_named_apart_from_a_walk_still_running(
        self,
        tempdir,
        monkeypatch,
    ):
        """A permanent loss is nameable without having to settle it first.

        ``unrecovered_room_ids`` fuses two states an application must treat
        differently: a room whose walk is still running will deliver its events
        on some later pump, while an abandoned room never will. With only the
        union published, the sole way to tell them apart is to call
        ``acknowledge_unrecovered_rooms`` and read back what it cleared -- which
        forgets the loss in order to learn of it, and so inverts the store-first
        ordering every other durable transition here keeps.
        """
        client = application_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        walking = RecordingFetch([messages_error(429)])
        open_gap = await open_a_stuck_gap(client, walking, monkeypatch)

        assert client._recovery.gaps[ROOM_A], "the walk must still be pending"
        assert ROOM_A in open_gap.unrecovered_room_ids
        assert (
            open_gap.abandoned_rooms == {}
        ), "a retryable walk is degraded, not given up on"
        await client.close()

    async def test_abandonment_is_republished_with_its_reason_until_settled(
        self,
        tempdir,
        monkeypatch,
    ):
        """The published reason is as sticky as the union it is drawn from.

        Reporting the loss once and then dropping it would be the exact defect
        stickiness exists to prevent, moved to a narrower field an application
        is more likely to read on its own.
        """
        client = application_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        first = await open_a_stuck_gap(
            client,
            RecordingFetch([messages_error(403)]),
            monkeypatch,
        )
        assert first.abandoned_rooms == {
            ROOM_A: frozenset({RecoveryAbandonment.FETCH_FAILED})
        }
        assert isinstance(first.abandoned_rooms[ROOM_A], frozenset)
        assert (
            ROOM_A in first.unrecovered_room_ids
        ), "an abandoned room stays in the union, so existing readers keep working"

        client.next_batch = "s1"
        second = sync_response("s2", {})
        await client.receive_response(second)
        assert second.abandoned_rooms == {
            ROOM_A: frozenset({RecoveryAbandonment.FETCH_FAILED})
        }

        assert client.acknowledge_unrecovered_rooms([ROOM_A]) == frozenset({ROOM_A})

        client.next_batch = "s2"
        settled = sync_response("s3", {})
        await client.receive_response(settled)
        assert settled.abandoned_rooms == {}
        assert settled.unrecovered_room_ids == frozenset()
        await client.close()

    async def test_a_cancelled_sync_still_names_an_abandoned_room(
        self,
        tempdir,
        monkeypatch,
    ):
        """The fail-closed snapshot cannot be the one response that forgets.

        ``_publish_waiting_sync_cancellation`` builds its degraded set from open
        gap rows, and an abandoned room no longer has one -- the row is deleted
        when the walk is given up on. Without the sticky set folded in, a
        cancellation would publish a permanently lost room as healthy.
        """
        client = application_owned_client(tempdir)
        await client.receive_response(LoginResponse.from_dict(LOGIN))
        await open_a_stuck_gap(
            client,
            RecordingFetch([messages_error(403)]),
            monkeypatch,
        )
        assert not client._recovery.gaps.get(ROOM_A)

        cancelled = sync_response("s2", {})
        client._publish_waiting_sync_cancellation(cancelled, "s1")

        assert cancelled.unrecovered_room_ids == frozenset({ROOM_A})
        assert isinstance(
            cancelled.unrecovered_room_ids, frozenset
        ), "a mutable set compares equal to the frozen one this field promises"
        assert cancelled.abandoned_rooms == {
            ROOM_A: frozenset({RecoveryAbandonment.FETCH_FAILED})
        }
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
        assert client._recovery.abandoned == {
            ROOM_A: frozenset({RecoveryAbandonment.FETCH_FAILED})
        }
        assert fetch.pages == []
        await client.close()
        assert client.store
        client.store.database.close()

        restarted = nio_owned_client(tempdir)
        await restarted.receive_response(LoginResponse.from_dict(LOGIN))
        assert restarted._recovery.abandoned == {
            ROOM_A: frozenset({RecoveryAbandonment.FETCH_FAILED})
        }

        restarted.next_batch = "s1"
        after_restart = sync_response(
            "s2",
            {ROOM_A: room_info([], limited=False, prev_batch="p2")},
        )
        await restarted.receive_response(after_restart)
        assert ROOM_A in after_restart.unrecovered_room_ids
        assert after_restart.abandoned_rooms == {
            ROOM_A: frozenset({RecoveryAbandonment.FETCH_FAILED})
        }, "the reason has to outlive the restart, not just the loss"

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
            reopened._recovery.abandoned == {}
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

        # 403 is refused outright; 429 leaves the walk open until the leave
        # drops the gap it was walking. Neither shows the history is gone: a
        # rate-limited walk that loses its baseline was never proven unable to
        # advance, and reporting it as such would be the over-claim in reverse.
        expected = (
            RecoveryAbandonment.FETCH_FAILED
            if recovery_status == 403
            else RecoveryAbandonment.BASELINE_LOST
        )
        assert client._recovery.abandoned == {ROOM_A: frozenset({expected})}
        assert client.store
        assert client.store.load_sync_recovery()[2] == {ROOM_A: frozenset({expected})}

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
        assert client._recovery.abandoned == {
            ROOM_A: frozenset({RecoveryAbandonment.FETCH_FAILED})
        }

        monkeypatch.setattr(store, "clear_recovery_abandonment", original)
        assert client.acknowledge_unrecovered_rooms([ROOM_A]) == frozenset({ROOM_A})
        assert client._recovery.abandoned == {}
        assert store.load_sync_recovery()[2] == {}
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
