import asyncio
import json
import sqlite3
from collections.abc import Callable
from dataclasses import replace
from uuid import UUID, uuid4

import pytest
from aiohttp import ClientConnectionError, ClientPayloadError

import nio
from nio import AsyncClient, AsyncClientConfig
from nio.exceptions import LocalProtocolError
from nio.ingest.classic import ClassicSource
from nio.ingest.config import (
    ClassicSourceConfig,
    IngestionConfig,
    SlidingSourceConfig,
)
from nio.ingest.ports import NetworkResult, StagedSourceResponse
from nio.ingest.sliding import (
    RESERVED_ALL_ROOMS_LIST,
    SlidingSource,
    _sliding_cursor_from_json,
    canonical_sliding_cursor,
)
from nio.ingest.source import ClassicCursor, canonical_classic_cursor, canonical_json
from nio.ingest.state import SourceState, StagedFrame
from nio.store import SqliteStore, open_ingestion_store
from nio.store._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
)

ACCOUNT = "@alice:example.org"
DEVICE = "ALICE"
ROOM = "!diagnostic:example.org"


class RecordingClient(AsyncClient):
    def __init__(
        self,
        account: str = ACCOUNT,
        device: str = DEVICE,
        config: AsyncClientConfig | None = None,
    ) -> None:
        super().__init__("https://example.org", account, device, config=config)
        self.restore_login(account, device, "secret-token")
        self.send_count = 0
        self.responses: list[FakeResponse | BaseException] = []
        self.send_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.on_send: Callable[[], None] | None = None

    async def send(self, *args: object, **kwargs: object) -> "FakeResponse":
        self.send_count += 1
        self.send_calls.append((args, kwargs))
        if self.on_send is not None:
            self.on_send()
        if not self.responses:
            raise AssertionError("unexpected HTTP request")
        result = self.responses.pop(0)
        if isinstance(result, BaseException):
            raise result
        return result


class FakeContent:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.position = 0
        self.read_sizes: list[int] = []

    async def read(self, size: int = -1) -> bytes:
        self.read_sizes.append(size)
        end = len(self.body) if size < 0 else self.position + size
        chunk = self.body[self.position : end]
        self.position += len(chunk)
        return chunk


class FakeResponse:
    def __init__(self, status: int, body: bytes, headers: dict[str, str] | None = None):
        self.status = status
        self.content = FakeContent(body)
        self.headers = headers or {}
        self.released = False

    def release(self) -> None:
        self.released = True


class SecondRequestHoldingClient(RecordingClient):
    def __init__(self, response: FakeResponse) -> None:
        super().__init__()
        self.responses.append(response)
        self.second_request_entered = asyncio.Event()
        self.second_request_cancelled = asyncio.Event()

    async def send(self, *args: object, **kwargs: object) -> FakeResponse:
        if self.responses:
            return await super().send(*args, **kwargs)
        self.send_count += 1
        self.send_calls.append((args, kwargs))
        self.second_request_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.second_request_cancelled.set()
            raise
        raise AssertionError("unreachable")


class FailingContent:
    def __init__(self, error: BaseException | None = None) -> None:
        self.error = error or ClientConnectionError()

    async def read(self, size: int) -> bytes:
        raise self.error


class OverreadContent:
    async def read(self, size: int) -> bytes:
        assert size >= 0
        return b"x" * (size + 1)


class HoldingContent:
    def __init__(self) -> None:
        self.entered = asyncio.Event()

    async def read(self, size: int) -> bytes:
        self.entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class FakeClientSession:
    def __init__(self, responses: list[FakeResponse]) -> None:
        self.responses = responses
        self.calls: list[tuple[tuple[object, ...], dict[str, object]]] = []

    async def request(self, *args: object, **kwargs: object) -> FakeResponse:
        self.calls.append((args, kwargs))
        return self.responses.pop(0)


def classic_config() -> IngestionConfig:
    return IngestionConfig(ClassicSourceConfig(30_000, b"{}"))


def sliding_config() -> IngestionConfig:
    return IngestionConfig(SlidingSourceConfig(30_000, "diag", b"{}", b"{}", b"{}"))


def _blocked_classic_success() -> dict[str, object]:
    body: dict[str, object] = {
        "next_batch": "s1",
        "rooms": {"join": {"!other:example.org": {}}},
    }
    assert canonical_json(body) == (
        b'{"next_batch":"s1","rooms":{"join":{"!other:example.org":{}}}}'
    )
    return body


def open_bootstrap(
    tmp_path: object,
    generation: UUID,
    *,
    transition_statement_hook: Callable[[str], None] | None = None,
):
    return open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=classic_config().source,
        transition_statement_hook=transition_statement_hook,
    )


def stage_classic(bootstrap, body: dict[str, object]) -> StagedFrame:
    journal = bootstrap._journal
    owner = journal.load_owner()
    prior = journal.load_source()
    adapter = ClassicSource(owner.stream_id, classic_config().source, owner.account_id)
    request = adapter.plan_request(prior, prior.next_request_id)
    assert request is not None
    normalized = adapter.normalize(
        request,
        NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            canonical_json(body),
            None,
            None,
        ),
    )
    assert normalized.frame is not None
    frame = StagedFrame(
        normalized.frame.frame_id,
        StagedSourceResponse(
            request,
            normalized.response_body,
            normalized.frame.source_sha256,
        ),
    )
    journal.stage_source_response(
        source=SourceState(
            prior.source_epoch,
            prior.transport_kind,
            normalized.frame.candidate_cursor_json,
            request.request_id + 1,
            prior.active,
        ),
        frame=frame,
    )
    return frame


def _sliding_frame(
    bootstrap,
    request,
    *,
    pos: str,
    to_device_since: str,
) -> tuple[SourceState, StagedFrame]:
    owner = bootstrap._journal.load_owner()
    adapter = SlidingSource(owner.stream_id, sliding_config().source, owner.account_id)
    assert request.body is not None
    body = canonical_json(
        {
            "pos": pos,
            "txn_id": json.loads(request.body)["txn_id"],
            "lists": {RESERVED_ALL_ROOMS_LIST: {"count": 0}},
            "rooms": {},
            "extensions": {
                "to_device": {
                    "events": [],
                    "next_batch": to_device_since,
                }
            },
        }
    )
    normalized = adapter.normalize(
        request,
        NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            body,
            None,
            None,
        ),
    )
    assert normalized.frame is not None
    return (
        SourceState(
            request.source_epoch,
            request.transport,
            normalized.frame.candidate_cursor_json,
            request.request_id + 1,
            True,
        ),
        StagedFrame(
            normalized.frame.frame_id,
            StagedSourceResponse(
                request,
                normalized.response_body,
                normalized.frame.source_sha256,
            ),
        ),
    )


def stage_sliding_position(bootstrap, *, sequence: int = 1) -> StagedFrame:
    journal = bootstrap._journal
    owner = journal.load_owner()
    prior = journal.load_source()
    request = SlidingSource(
        owner.stream_id,
        sliding_config().source,
        owner.account_id,
    ).plan_request(prior, prior.next_request_id)
    assert request is not None
    successor, frame = _sliding_frame(
        bootstrap,
        request,
        pos=f"p{sequence}",
        to_device_since=f"td{sequence}",
    )
    journal.stage_source_response(source=successor, frame=frame)
    return frame


def _raw_ingestion_rows(database_path) -> tuple[tuple[object, ...], ...]:
    tables = (
        "NioIngestMeta",
        "NioIngestSourceState",
        "NioIngestFrame",
        "NioIngestRoomAggregate",
        "NioIngestWork",
    )
    with sqlite3.connect(database_path) as connection:
        return tuple(
            (table, *row)
            for table in tables
            for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
        )


def hydration_state() -> bytes:
    return canonical_json(
        [
            {
                "content": {"membership": "join"},
                "event_id": "$member",
                "state_key": ACCOUNT,
                "type": "m.room.member",
            }
        ]
    )


def stage_pending(bootstrap) -> StagedFrame:
    return stage_classic(bootstrap, {"next_batch": "s1", "rooms": {"join": {ROOM: {}}}})


def test_ingestion_session_is_public() -> None:
    assert hasattr(nio, "open_ingestion")
    assert hasattr(nio, "IngestionSession")


def test_ingestion_session_is_exported_by_ingest_package() -> None:
    namespace: dict[str, object] = {}
    exec("from nio.ingest import *", namespace)
    assert namespace["open_ingestion"] is nio.open_ingestion
    assert namespace["IngestionSession"] is nio.IngestionSession


@pytest.mark.parametrize(
    "binding",
    ["null_stream", "wrong_stream", "wrong_generation"],
)
def test_open_ingestion_rejects_incomplete_or_mismatched_binding(
    tmp_path, binding: str
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    owner = bootstrap._journal.load_owner()
    before = (
        owner,
        bootstrap._journal.load_source(),
        bootstrap._journal.list_frames(1),
    )
    supplied_generation = uuid4() if binding == "wrong_generation" else generation
    supplied_stream = (
        None
        if binding == "null_stream"
        else uuid4() if binding == "wrong_stream" else bootstrap.stream_id
    )

    with pytest.raises(LocalProtocolError):
        nio.open_ingestion(
            client,
            bootstrap,
            config=classic_config(),
            consumer_generation=supplied_generation,
            stream_id=supplied_stream,
        )

    assert client.send_count == 0
    assert before == (
        bootstrap._journal.load_owner(),
        bootstrap._journal.load_source(),
        bootstrap._journal.list_frames(1),
    )
    bootstrap.close()


@pytest.mark.parametrize(
    ("account", "device", "token"),
    [
        ("@mallory:example.org", DEVICE, "token"),
        (ACCOUNT, "OTHER", "token"),
        (ACCOUNT, DEVICE, ""),
    ],
)
def test_open_ingestion_rejects_wrong_or_unauthenticated_client(
    tmp_path, account: str, device: str, token: str
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient(account, device)
    client.access_token = token

    with pytest.raises(LocalProtocolError):
        nio.open_ingestion(
            client,
            bootstrap,
            config=classic_config(),
            consumer_generation=generation,
            stream_id=bootstrap.stream_id,
        )

    assert client.send_count == 0
    bootstrap.close()


def test_open_ingestion_rejects_transport_mismatch_without_consuming_bootstrap(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    sliding = IngestionConfig(SlidingSourceConfig(30_000, "diag", b"{}", b"{}", b"{}"))

    with pytest.raises(LocalProtocolError):
        nio.open_ingestion(
            client,
            bootstrap,
            config=sliding,
            consumer_generation=generation,
            stream_id=bootstrap.stream_id,
        )

    assert client.send_count == 0
    bootstrap.close()


@pytest.mark.asyncio
async def test_successful_open_transfers_bootstrap_once(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )

    with pytest.raises(LocalProtocolError):
        nio.open_ingestion(
            client,
            bootstrap,
            config=classic_config(),
            consumer_generation=generation,
            stream_id=bootstrap.stream_id,
        )
    with pytest.raises(LocalProtocolError):
        bootstrap.open_matrix_store(SqliteStore)

    assert client.send_count == 0
    await session.close()
    await session.close()


def test_store_first_rejects_session_claim_without_consuming_bootstrap(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    store = bootstrap.open_matrix_store(SqliteStore)
    client = RecordingClient()
    assert store.load_account() is None

    with pytest.raises(LocalProtocolError):
        nio.open_ingestion(
            client,
            bootstrap,
            config=classic_config(),
            consumer_generation=generation,
            stream_id=bootstrap.stream_id,
        )

    assert bootstrap._session_claimed is False
    assert store.load_account() is None
    assert client.send_count == 0
    bootstrap.close()


@pytest.mark.parametrize("header", ["Authorization", "aUtHoRiZaTiOn"])
@pytest.mark.asyncio
async def test_custom_authorization_rejected_without_consuming_bootstrap(
    tmp_path, header: str
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient(
        config=AsyncClientConfig(custom_headers={header: "Bearer attacker"})
    )

    with pytest.raises(LocalProtocolError):
        nio.open_ingestion(
            client,
            bootstrap,
            config=classic_config(),
            consumer_generation=generation,
            stream_id=bootstrap.stream_id,
        )

    assert bootstrap._session_claimed is False
    assert client.send_count == 0
    safe_client = RecordingClient()
    session = nio.open_ingestion(
        safe_client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    await session.close()


@pytest.mark.asyncio
async def test_run_drains_supported_frame_before_first_http(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    frame = stage_classic(
        bootstrap,
        {"next_batch": "s1", "rooms": {"join": {ROOM: {}}}},
    )
    client = RecordingClient()
    client.responses.append(FakeResponse(401, b"{}"))

    def assert_drained() -> None:
        assert bootstrap._journal.load_frame(frame.frame_id) is None
        owner = bootstrap._journal.load_owner()
        assert bootstrap._journal._load_room_aggregate(owner, ROOM) is not None

    client.on_send = assert_drained
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await session.run()
    assert client.send_count == 1
    assert "/rooms/%21diagnostic%3Aexample.org/state" in client.send_calls[0][0][1]
    await session.close()


@pytest.mark.asyncio
async def test_run_hydrates_fresh_live_room_before_later_sync(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_classic(
        bootstrap,
        {"next_batch": "s1", "rooms": {"join": {ROOM: {}}}},
    )
    frame = stage_classic(
        bootstrap,
        {
            "next_batch": "s2",
            "rooms": {
                "join": {
                    ROOM: {
                        "timeline": {
                            "events": [
                                {
                                    "content": {"body": "hello", "msgtype": "m.text"},
                                    "event_id": "$message",
                                    "origin_server_ts": 1,
                                    "sender": ACCOUNT,
                                    "type": "m.room.message",
                                }
                            ],
                        }
                    }
                }
            },
        },
    )
    state = canonical_json(
        [
            {
                "content": {"membership": "join"},
                "event_id": "$member",
                "state_key": ACCOUNT,
                "type": "m.room.member",
            }
        ]
    )
    hydration = FakeResponse(200, state)
    client = RecordingClient(config=AsyncClientConfig(custom_headers={"X-Test": "yes"}))
    client.responses.extend([hydration, FakeResponse(401, b"{}")])

    def assert_owned_before_get() -> None:
        if client.send_count != 1:
            return
        assert bootstrap._journal.load_frame(frame.frame_id) is None
        with bootstrap._journal._owner.read():
            rows = bootstrap._journal._execute(
                "SELECT status FROM NioIngestWork"
            ).fetchall()
        assert [row[0] for row in rows] == ["held"]

    client.on_send = assert_owned_before_get
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()

    assert client.send_count == 2
    assert client.send_calls[0][0][:2] == (
        "GET",
        "/_matrix/client/v3/rooms/%21diagnostic%3Aexample.org/state",
    )
    assert client.send_calls[0][0][3] == {
        "Authorization": "Bearer secret-token",
    }
    assert hydration.released
    assert bootstrap._journal.load_pending_hydrations(limit=2) == ()
    with bootstrap._journal._owner.read():
        row = bootstrap._journal._execute(
            "SELECT status, ready_ordinal FROM NioIngestWork"
        ).fetchone()
    assert tuple(row) == ("ready", 0)
    await session.close()


@pytest.mark.asyncio
async def test_hydration_protocol_failure_is_focused_and_retains_owner(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_classic(
        bootstrap,
        {"next_batch": "s1", "rooms": {"join": {ROOM: {}}}},
    )
    response = FakeResponse(200, b"")
    response.content = OverreadContent()  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await session.run()
    assert response.released
    assert len(bootstrap._journal.load_pending_hydrations(limit=2)) == 1
    await session.close()


@pytest.mark.asyncio
async def test_hydration_payload_disconnect_releases_and_retries(
    tmp_path, monkeypatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    failed = FakeResponse(200, b"")
    failed.content = FailingContent(ClientPayloadError())  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.extend(
        [failed, FakeResponse(200, hydration_state()), FakeResponse(401, b"{}")]
    )

    async def no_wait(delay: float) -> None:
        pass

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", no_wait)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert failed.released and client.send_count == 3
    await session.close()


@pytest.mark.asyncio
async def test_hydration_real_public_send_bypasses_callbacks(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    responses = [FakeResponse(200, hydration_state()), FakeResponse(401, b"{}")]
    transport = FakeClientSession(responses)
    client = AsyncClient("https://example.org", ACCOUNT, DEVICE)
    client.restore_login(ACCOUNT, DEVICE, "secret-token")
    client.client_session = transport  # type: ignore[assignment]

    async def callback(*args: object) -> None:
        raise AssertionError("hydration must not invoke callbacks")

    client.add_response_callback(callback)
    client.add_event_callback(callback, None)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert "/rooms/%21diagnostic%3Aexample.org/state" in transport.calls[0][0][1]
    assert all(response.released for response in responses)
    client.client_session = None
    await session.close()


@pytest.mark.parametrize(
    "first",
    [
        TimeoutError(),
        ClientConnectionError(),
        FakeResponse(408, b"{}"),
        FakeResponse(429, b"{}"),
        FakeResponse(503, b"{}"),
    ],
    ids=("timeout", "connection", "408", "429", "5xx"),
)
@pytest.mark.asyncio
async def test_hydration_retryable_fates_reload_then_release_before_sync(
    tmp_path, monkeypatch: pytest.MonkeyPatch, first: FakeResponse | BaseException
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    client = RecordingClient()
    client.responses.extend(
        [first, FakeResponse(200, hydration_state()), FakeResponse(401, b"{}")]
    )

    async def no_wait(delay: float) -> None:
        pass

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", no_wait)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    paths = [call[0][1] for call in client.send_calls]
    assert paths[0] == paths[1] and "/state" in paths[0] and "/sync" in paths[2]
    assert bootstrap._journal.load_pending_hydrations(limit=2) == ()
    await session.close()


@pytest.mark.parametrize("status", (400, 401, 403, 404))
@pytest.mark.asyncio
async def test_hydration_terminal_status_is_sticky_without_source_sync(
    tmp_path, status: int
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    client = RecordingClient()
    client.responses.append(FakeResponse(status, b"{}"))
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await session.run()
    assert client.send_count == 1 and "/state" in client.send_calls[0][0][1]
    assert len(bootstrap._journal.load_pending_hydrations(limit=2)) == 1
    await session.close()


@pytest.mark.asyncio
async def test_cancelling_held_hydration_get_retains_pending_owner(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    response = FakeResponse(200, b"")
    content = HoldingContent()
    response.content = content  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    task = asyncio.create_task(session.run())
    await content.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert response.released
    assert len(bootstrap._journal.load_pending_hydrations(limit=2)) == 1
    await session.close()


@pytest.mark.asyncio
async def test_hydration_http_holds_no_sqlite_writer_transaction(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    client = RecordingClient()
    client.responses.append(FakeResponse(401, b"{}"))

    def take_writer() -> None:
        connection = sqlite3.connect(bootstrap.database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.commit()
        finally:
            connection.close()

    client.on_send = take_writer
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await session.run()
    await session.close()


@pytest.mark.asyncio
async def test_pending_hydrations_for_multiple_rooms_are_processed_in_sequence(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    assert bootstrap._journal.materialize_oldest_frame(
        limits=MaterializerLimits()
    ).revision
    other = "!other:example.org"
    stage_classic(
        bootstrap,
        {"next_batch": "s2", "rooms": {"join": {other: {}}}},
    )
    assert bootstrap._journal.materialize_oldest_frame(
        limits=MaterializerLimits()
    ).revision
    client = RecordingClient()
    client.responses.extend(
        [
            FakeResponse(200, hydration_state()),
            FakeResponse(200, hydration_state()),
            FakeResponse(401, b"{}"),
        ]
    )
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert [call[0][1] for call in client.send_calls] == [
        "/_matrix/client/v3/rooms/%21diagnostic%3Aexample.org/state",
        "/_matrix/client/v3/rooms/%21other%3Aexample.org/state",
        "/_matrix/client/v3/sync?since=s2&timeout=30000&filter=%7B%7D",
    ]
    assert bootstrap._journal.load_pending_hydrations(limit=2) == ()
    await session.close()


@pytest.mark.asyncio
async def test_cancelling_hydration_retry_sleep_retains_pending_owner(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    client = RecordingClient()
    client.responses.append(FakeResponse(429, b"{}"))
    sleeping = asyncio.Event()

    async def hold_sleep(delay: float) -> None:
        sleeping.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", hold_sleep)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    task = asyncio.create_task(session.run())
    await sleeping.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert len(bootstrap._journal.load_pending_hydrations(limit=2)) == 1
    await session.close()


@pytest.mark.asyncio
async def test_success_retires_empty_frame_before_second_http(tmp_path) -> None:
    generation = uuid4()
    transitions: list[str] = []
    bootstrap = open_bootstrap(
        tmp_path,
        generation,
        transition_statement_hook=transitions.append,
    )
    before = bootstrap._journal.load_source()
    response = FakeResponse(200, b'{"next_batch":"s1","rooms":{}}')
    client = SecondRequestHoldingClient(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    run_task = asyncio.create_task(session.run())
    request_task = asyncio.create_task(client.second_request_entered.wait())
    try:
        async with asyncio.timeout(5):
            completed, _ = await asyncio.wait(
                {run_task, request_task},
                return_when=asyncio.FIRST_COMPLETED,
            )
        if run_task in completed:
            with pytest.raises(nio.IngestionBlockedError):
                await run_task
            after = bootstrap._journal.load_source()
            frames = bootstrap._journal.list_frames(2)
            assert response.released
            assert after == SourceState(
                before.source_epoch,
                before.transport_kind,
                canonical_classic_cursor(ClassicCursor("s1")),
                before.next_request_id + 1,
                before.active,
            )
            assert len(frames) == 1
            assert frames[0].response.response_body == response.content.body
            assert client.send_count == 1
            assert client.send_calls[0] == (
                (
                    "GET",
                    "/_matrix/client/v3/sync?full_state=true&timeout=30000&filter=%7B%7D",
                    None,
                    {"Authorization": "Bearer secret-token"},
                    None,
                    30.0,
                ),
                {},
            )
            assert transitions == [
                "frame_collision_probe",
                "meta_revision_epoch_cas",
                "source_state_upsert",
                "frame_insert",
                "commit",
            ]
            pytest.fail("Task7 empty Classic frame blocked before second HTTP")
        assert request_task in completed
        assert not run_task.done()
        assert response.released
        assert bootstrap._journal.load_source() == SourceState(
            before.source_epoch,
            before.transport_kind,
            canonical_classic_cursor(ClassicCursor("s1")),
            before.next_request_id + 1,
            before.active,
        )
        assert bootstrap._journal.list_frames(2) == ()
        assert transitions == [
            "frame_collision_probe",
            "meta_revision_epoch_cas",
            "source_state_upsert",
            "frame_insert",
            "commit",
            "meta_revision_epoch_cas",
            "frame_delete",
            "before_commit",
            "commit",
        ]
        assert client.send_count == 2
        assert client.send_calls[0] == (
            (
                "GET",
                "/_matrix/client/v3/sync?full_state=true&timeout=30000&filter=%7B%7D",
                None,
                {"Authorization": "Bearer secret-token"},
                None,
                30.0,
            ),
            {},
        )
        assert client.send_calls[1] == (
            (
                "GET",
                "/_matrix/client/v3/sync?since=s1&timeout=30000&filter=%7B%7D",
                None,
                {"Authorization": "Bearer secret-token"},
                None,
                30.0,
            ),
            {},
        )
    finally:
        if not request_task.done():
            request_task.cancel()
        await asyncio.gather(request_task, return_exceptions=True)
        await session.close()
        await asyncio.gather(run_task, return_exceptions=True)
        await client.close()
    assert client.second_request_cancelled.is_set()


@pytest.mark.asyncio
async def test_raw_attempt_uses_public_send_without_callbacks_and_bounds_read(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    response = FakeResponse(401, b"{}")
    transport = FakeClientSession([response])
    client = AsyncClient(
        "https://example.org",
        ACCOUNT,
        DEVICE,
        config=AsyncClientConfig(custom_headers={"X-Diagnostic": "yes"}),
    )
    client.restore_login(ACCOUNT, DEVICE, "secret-token")
    client.client_session = transport  # type: ignore[assignment]

    async def callback(*args: object) -> None:
        raise AssertionError("ingestion must not invoke application callbacks")

    client.add_response_callback(callback)
    client.add_event_callback(callback, None)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()

    assert transport.calls == [
        (
            (
                "GET",
                "https://example.org/_matrix/client/v3/sync?"
                "full_state=true&timeout=30000&filter=%7B%7D",
            ),
            {
                "data": None,
                "ssl": client.ssl,
                "headers": {
                    "Authorization": "Bearer secret-token",
                    "X-Diagnostic": "yes",
                },
                "trace_request_ctx": None,
                "timeout": 30.0,
            },
        )
    ]
    assert response.content.read_sizes == [16 * 1024 * 1024 + 1, 16 * 1024 * 1024 - 1]
    assert response.released
    client.client_session = None
    await session.close()


@pytest.mark.asyncio
async def test_http_attempt_holds_no_sqlite_writer_transaction(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    client.responses.append(FakeResponse(401, b"{}"))

    def take_external_writer() -> None:
        connection = sqlite3.connect(bootstrap.database_path)
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.commit()
        finally:
            connection.close()

    client.on_send = take_external_writer
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert client.send_count == 1
    await session.close()


@pytest.mark.asyncio
async def test_retry_sleep_replans_exact_unchanged_request(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    before = bootstrap._journal.load_source()
    client = RecordingClient()
    client.responses.extend(
        [
            FakeResponse(
                429,
                b'{"errcode":"M_LIMIT_EXCEEDED","retry_after_ms":7}',
            ),
            FakeResponse(200, canonical_json(_blocked_classic_success())),
            FakeResponse(401, b"{}"),
        ]
    )
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", record_sleep)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await session.run()

    assert sleeps == [0.007]
    assert len(client.send_calls) == 3
    assert client.send_calls[0] == client.send_calls[1]
    after = bootstrap._journal.load_source()
    assert after.next_request_id == before.next_request_id + 1
    assert bootstrap._journal.list_frames(2) == ()
    await session.close()


@pytest.mark.asyncio
async def test_retry_after_header_is_bounded_and_owned_by_session(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    client.responses.extend(
        [
            FakeResponse(429, b"{}", {"Retry-After": "999"}),
            FakeResponse(200, canonical_json(_blocked_classic_success())),
            FakeResponse(401, b"{}"),
        ]
    )
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", record_sleep)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await session.run()
    assert sleeps == [60.0]
    await session.close()


@pytest.mark.asyncio
async def test_terminal_error_is_sticky_without_second_http(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    client.responses.append(FakeResponse(403, b"{}"))
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError) as first:
        await session.run()
    with pytest.raises(nio.IngestionSourceError) as second:
        await session.run()
    assert first.value is second.value
    assert client.send_count == 1
    assert bootstrap._journal.load_source().next_request_id == 0
    assert bootstrap._journal.list_frames(1) == ()
    await session.close()


@pytest.mark.asyncio
async def test_delivery_remains_sync_after_sticky_failure_and_stops_at_close(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    client.responses.append(FakeResponse(403, b"{}"))
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()

    assert session.next_batch(max_records=1, max_canonical_bytes=1) is None
    with pytest.raises(LocalProtocolError):
        session.acknowledge_batch(object())  # type: ignore[arg-type]
    assert client.send_count == 1

    await session.close()
    with pytest.raises(LocalProtocolError):
        session.next_batch()
    with pytest.raises(LocalProtocolError):
        session.acknowledge_batch(object())  # type: ignore[arg-type]


class HoldingClient(RecordingClient):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()

    async def send(self, *args: object, **kwargs: object) -> FakeResponse:
        self.send_count += 1
        self.entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")


class SlowCancellationClient(RecordingClient):
    def __init__(self) -> None:
        super().__init__()
        self.entered = asyncio.Event()
        self.cancelling = asyncio.Event()
        self.release = asyncio.Event()

    async def send(self, *args: object, **kwargs: object) -> FakeResponse:
        self.entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            self.cancelling.set()
            await self.release.wait()
            raise


@pytest.mark.asyncio
async def test_close_cancels_runner_and_releases_bootstrap_lock(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = HoldingClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    task = asyncio.create_task(session.run())
    await client.entered.wait()
    with pytest.raises(LocalProtocolError):
        await session.run()
    await session.close()
    await session.close()
    assert task.cancelled()

    reopened = open_bootstrap(tmp_path, generation)
    reopened.close()


@pytest.mark.asyncio
async def test_concurrent_close_survives_first_caller_cancellation(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = SlowCancellationClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    runner = asyncio.create_task(session.run())
    await client.entered.wait()
    first = asyncio.create_task(session.close())
    await client.cancelling.wait()
    second = asyncio.create_task(session.close())
    first.cancel()
    with pytest.raises(asyncio.CancelledError):
        await first
    assert not second.done()
    client.release.set()
    await second
    await session.close()
    assert runner.cancelled()

    reopened = open_bootstrap(tmp_path, generation)
    reopened.close()


@pytest.mark.parametrize(
    "first",
    [
        TimeoutError(),
        ClientConnectionError(),
        FakeResponse(408, b"{}"),
        FakeResponse(503, b"{}"),
    ],
    ids=("timeout", "connection", "408", "5xx"),
)
@pytest.mark.asyncio
async def test_only_retryable_fates_retry_without_durable_mutation(
    tmp_path, monkeypatch: pytest.MonkeyPatch, first: FakeResponse | BaseException
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    before = bootstrap._journal.load_source()
    client = RecordingClient()
    client.responses.extend(
        [
            first,
            FakeResponse(200, canonical_json(_blocked_classic_success())),
            FakeResponse(401, b"{}"),
        ]
    )
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        assert bootstrap._journal.load_source() == before
        assert bootstrap._journal.list_frames(1) == ()
        sleeps.append(delay)

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", record_sleep)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await session.run()
    assert sleeps == [0.0]
    assert client.send_calls[0] == client.send_calls[1]
    await session.close()


@pytest.mark.parametrize(
    ("status", "body"),
    [
        (401, b"{}"),
        (403, b"{}"),
        (200, b"{"),
        (200, b"x" * (16 * 1024 * 1024 + 1)),
    ],
    ids=("401", "403", "malformed-200", "oversized-200"),
)
@pytest.mark.asyncio
async def test_terminal_fates_leave_source_and_frames_unchanged(
    tmp_path, status: int, body: bytes
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    before = bootstrap._journal.load_source()
    response = FakeResponse(status, body)
    client = RecordingClient()
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert client.send_count == 1
    assert bootstrap._journal.load_source() == before
    assert bootstrap._journal.list_frames(1) == ()
    assert response.released
    await session.close()


@pytest.mark.asyncio
async def test_cancellation_during_http_leaves_durable_request_unchanged(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    before = bootstrap._journal.load_source()
    client = HoldingClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    task = asyncio.create_task(session.run())
    await client.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bootstrap._journal.load_source() == before
    assert bootstrap._journal.list_frames(1) == ()
    await session.close()


@pytest.mark.asyncio
async def test_cancellation_during_retry_sleep_leaves_request_unchanged(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    before = bootstrap._journal.load_source()
    client = RecordingClient()
    client.responses.append(TimeoutError())
    sleeping = asyncio.Event()

    async def held_sleep(delay: float) -> None:
        sleeping.set()
        await asyncio.Event().wait()

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", held_sleep)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    task = asyncio.create_task(session.run())
    await sleeping.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert bootstrap._journal.load_source() == before
    assert bootstrap._journal.list_frames(1) == ()
    await session.close()


@pytest.mark.asyncio
async def test_idle_with_retained_crypto_frame_blocks_without_http(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    frame = stage_classic(
        bootstrap,
        {
            "next_batch": "s1",
            "to_device": {
                "events": [
                    {
                        "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                        "type": "m.room_key",
                    }
                ]
            },
        },
    )
    result = bootstrap._journal.materialize_oldest_frame(limits=MaterializerLimits())
    assert result.status is MaterializeStatus.MATERIALIZED
    assert bootstrap._journal.load_frame(frame.frame_id) is not None
    client = RecordingClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionBlockedError):
        await session.run()
    assert client.send_count == 0
    await session.close()


@pytest.mark.asyncio
async def test_malformed_transport_state_is_a_sticky_source_error(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    response = FakeResponse(200, b"{}")
    response.status = "200"  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert client.send_count == 1
    await session.close()


@pytest.mark.asyncio
async def test_body_read_connection_failure_releases_and_retries(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    failed = FakeResponse(200, b"")
    failed.content = FailingContent()  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.extend(
        [
            failed,
            FakeResponse(200, canonical_json(_blocked_classic_success())),
            FakeResponse(401, b"{}"),
        ]
    )

    async def no_wait(delay: float) -> None:
        pass

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", no_wait)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await session.run()
    assert failed.released
    assert client.send_count == 3
    await session.close()


@pytest.mark.asyncio
async def test_reader_overrun_fails_closed_and_releases_response(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    response = FakeResponse(200, b"")
    response.content = OverreadContent()  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert response.released
    assert client.send_count == 1
    await session.close()


@pytest.mark.asyncio
async def test_payload_protocol_failure_is_terminal_and_releases_response(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    response = FakeResponse(200, b"")
    response.content = FailingContent(ClientPayloadError())  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert response.released
    assert client.send_count == 1
    await session.close()


@pytest.mark.asyncio
async def test_missing_response_content_is_a_terminal_source_error(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    response = FakeResponse(200, b"")
    del response.content
    client = RecordingClient()
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert response.released
    await session.close()


@pytest.mark.asyncio
async def test_cancellation_during_body_read_releases_response(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    before = bootstrap._journal.load_source()
    response = FakeResponse(200, b"")
    content = HoldingContent()
    response.content = content  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    task = asyncio.create_task(session.run())
    await content.entered.wait()
    task.cancel()
    with pytest.raises(asyncio.CancelledError):
        await task
    assert response.released
    assert bootstrap._journal.load_source() == before
    await session.close()


@pytest.mark.asyncio
async def test_positioned_unknown_pos_commits_reloads_drains_then_replans(
    tmp_path,
) -> None:
    generation = uuid4()
    config = sliding_config()
    transitions: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=config.source,
        transition_statement_hook=transitions.append,
    )
    journal = bootstrap._journal
    staged = stage_sliding_position(bootstrap)
    assert (
        journal.materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    assert journal.list_frames(1) == ()
    owner_before = journal.load_owner()
    source_before = journal.load_source()
    adapter = SlidingSource(
        owner_before.stream_id, config.source, owner_before.account_id
    )
    first_request = adapter.plan_request(source_before, source_before.next_request_id)
    assert first_request is not None
    _successor, old_frame = _sliding_frame(
        bootstrap,
        first_request,
        pos="p2",
        to_device_since="td2",
    )

    inserted = False

    def inject_old_frame(label: str) -> None:
        nonlocal inserted
        transitions.append(label)
        if label != "sliding_reset_meta_cas" or inserted:
            return
        inserted = True
        write_owner = journal._decode_owner_row(journal._meta())
        journal._write_frame(
            old_frame,
            write_owner.revision,
            write_owner,
            (ACCOUNT, write_owner.stream_id, write_owner.transport_kind),
        )

    journal.set_transition_statement_hook(inject_old_frame)
    client = RecordingClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=config,
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    requests = []
    second_request_entered = asyncio.Event()

    async def request(request, **_kwargs):
        requests.append(request)
        if len(requests) == 1:
            return session._network_result(
                request,
                400,
                b'{"errcode":"M_UNKNOWN_POS"}',
            )
        assert journal.list_frames(1) == ()
        committed_source = journal.load_source()
        expected = adapter.plan_request(
            committed_source,
            committed_source.next_request_id,
        )
        assert request == expected
        second_request_entered.set()
        await asyncio.Event().wait()
        raise AssertionError("unreachable")

    session._request = request  # type: ignore[method-assign]
    transitions.clear()
    run_task = asyncio.create_task(session.run())
    try:
        async with asyncio.timeout(5):
            await second_request_entered.wait()
        assert requests[0] == first_request
        reset_request = requests[1]
        committed_owner = journal.load_owner()
        committed_source = journal.load_source()
        reset_cursor = _sliding_cursor_from_json(committed_source.cursor_json)
        assert committed_owner.revision == owner_before.revision + 2
        assert committed_owner.next_source_epoch == owner_before.next_source_epoch + 1
        assert committed_source.source_epoch == owner_before.next_source_epoch
        assert committed_source.next_request_id == 0
        assert reset_cursor.pos is None
        assert reset_cursor.to_device_since == "td1"
        assert reset_request.source_epoch == committed_source.source_epoch
        assert reset_request.request_id == 0
        assert reset_request.request_cursor_json == committed_source.cursor_json
        assert reset_request != first_request
        assert journal.load_frame(staged.frame_id) is None
        assert journal.load_frame(old_frame.frame_id) is None
        assert transitions == [
            "sliding_reset_meta_cas",
            "sliding_reset_source_upsert",
            "before_commit",
            "commit",
            "meta_revision_epoch_cas",
            "frame_delete",
            "before_commit",
            "commit",
        ]
    finally:
        await session.close()
        await asyncio.gather(run_task, return_exceptions=True)


@pytest.mark.parametrize("state", ("positionless", "repeated_cold"))
@pytest.mark.asyncio
async def test_cold_unknown_pos_is_terminal_and_byte_identical(
    tmp_path,
    state: str,
) -> None:
    generation = uuid4()
    config = sliding_config()
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=config.source,
    )
    journal = bootstrap._journal
    if state == "repeated_cold":
        stage_sliding_position(bootstrap)
        assert (
            journal.materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        owner = journal.load_owner()
        positioned = journal.load_source()
        request = SlidingSource(
            owner.stream_id,
            config.source,
            owner.account_id,
        ).plan_request(positioned, positioned.next_request_id)
        assert request is not None
        assert journal._reset_sliding_source(request=request) is not None
    before = _raw_ingestion_rows(bootstrap.database_path)
    client = RecordingClient()
    client.responses.append(FakeResponse(400, b'{"errcode":"M_UNKNOWN_POS"}'))
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=config,
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert client.send_count == 1
    assert _raw_ingestion_rows(bootstrap.database_path) == before
    await session.close()


@pytest.mark.parametrize("mutation", ("source_epoch", "request_id", "cursor"))
@pytest.mark.asyncio
async def test_stale_unknown_pos_is_terminal_at_authenticated_state(
    tmp_path,
    mutation: str,
) -> None:
    generation = uuid4()
    config = sliding_config()
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=config.source,
    )
    journal = bootstrap._journal
    stage_sliding_position(bootstrap)
    assert (
        journal.materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    session = nio.open_ingestion(
        RecordingClient(),
        bootstrap,
        config=config,
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    authenticated: tuple[tuple[object, ...], ...] | None = None

    async def mutate_while_in_flight(request, **_kwargs):
        nonlocal authenticated
        with journal._transaction():
            owner, source = journal._load_stage_snapshot()
            if mutation == "source_epoch":
                changed = replace(source, source_epoch=source.source_epoch + 1)
            elif mutation == "request_id":
                changed = replace(
                    source,
                    next_request_id=source.next_request_id + 1,
                )
            else:
                cursor = _sliding_cursor_from_json(source.cursor_json)
                changed = replace(
                    source,
                    cursor_json=canonical_sliding_cursor(
                        replace(cursor, pos="foreign")
                    ),
                )
            journal._write_source(changed, owner)
        authenticated = _raw_ingestion_rows(bootstrap.database_path)
        return session._network_result(
            request,
            400,
            b'{"errcode":"M_UNKNOWN_POS"}',
        )

    session._request = mutate_while_in_flight  # type: ignore[method-assign]
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert authenticated is not None
    assert _raw_ingestion_rows(bootstrap.database_path) == authenticated
    await session.close()


@pytest.mark.parametrize(
    ("transport", "body"),
    (
        ("sliding", b"{"),
        ("sliding", b'{"errcode":"M_UNKNOWN_TOKEN"}'),
        ("classic", b'{"errcode":"M_UNKNOWN_POS"}'),
    ),
)
@pytest.mark.asyncio
async def test_non_reset_errors_are_terminal_and_byte_identical(
    tmp_path,
    transport: str,
    body: bytes,
) -> None:
    generation = uuid4()
    config = sliding_config() if transport == "sliding" else classic_config()
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=config.source,
    )
    if transport == "sliding":
        stage_sliding_position(bootstrap)
        assert (
            bootstrap._journal.materialize_oldest_frame(
                limits=MaterializerLimits()
            ).status
            is MaterializeStatus.MATERIALIZED
        )
        assert (
            _sliding_cursor_from_json(bootstrap._journal.load_source().cursor_json).pos
            is not None
        )
    before = _raw_ingestion_rows(bootstrap.database_path)
    client = RecordingClient()
    client.responses.append(FakeResponse(400, body))
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=config,
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert client.send_count == 1
    assert _raw_ingestion_rows(bootstrap.database_path) == before
    await session.close()


@pytest.mark.asyncio
async def test_429_public_send_does_not_invoke_callbacks(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    transport = FakeClientSession([FakeResponse(429, b"{}"), FakeResponse(401, b"{}")])
    client = AsyncClient("https://example.org", ACCOUNT, DEVICE)
    client.restore_login(ACCOUNT, DEVICE, "secret-token")
    client.client_session = transport  # type: ignore[assignment]

    async def callback(*args: object) -> None:
        raise AssertionError("callback invoked")

    async def no_wait(delay: float) -> None:
        pass

    client.add_response_callback(callback)
    client.add_event_callback(callback, None)
    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", no_wait)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert len(transport.calls) == 2
    client.client_session = None
    await session.close()


@pytest.mark.asyncio
async def test_materializer_at_capacity_is_sticky_without_http(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    frame_id = uuid4()

    def at_capacity(*args: object, **kwargs: object) -> MaterializeResult:
        return MaterializeResult(MaterializeStatus.AT_CAPACITY, frame_id, None)

    monkeypatch.setattr(
        type(bootstrap._journal), "materialize_oldest_frame", at_capacity
    )
    client = RecordingClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionBlockedError) as first:
        await session.run()
    with pytest.raises(nio.IngestionBlockedError) as second:
        await session.run()
    assert first.value is second.value
    assert first.value.frame_id == frame_id
    assert client.send_count == 0
    await session.close()


@pytest.mark.asyncio
async def test_retry_counter_resets_after_successful_frame(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    client.responses.extend(
        [
            TimeoutError(),
            TimeoutError(),
            FakeResponse(
                200,
                b'{"next_batch":"s1","rooms":{"join":{"!diagnostic:example.org":{}}}}',
            ),
            FakeResponse(
                200,
                b'[{"content":{"membership":"join"},"event_id":"$member","state_key":"@alice:example.org","type":"m.room.member"}]',
            ),
            TimeoutError(),
            FakeResponse(403, b"{}"),
        ]
    )
    sleeps: list[float] = []

    async def record_sleep(delay: float) -> None:
        sleeps.append(delay)

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", record_sleep)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert sleeps == [0.0, 0.2, 0.0]
    assert client.send_calls[0] == client.send_calls[1] == client.send_calls[2]
    assert client.send_calls[4] == client.send_calls[5]
    assert client.send_calls[2] != client.send_calls[3] != client.send_calls[4]
    await session.close()


@pytest.mark.asyncio
async def test_sliding_public_send_preserves_exact_planned_request(tmp_path) -> None:
    generation = uuid4()
    config = IngestionConfig(SlidingSourceConfig(30_000, "diag", b"{}", b"{}", b"{}"))
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=config.source,
    )
    owner = bootstrap._journal.load_owner()
    planned = SlidingSource(
        owner.stream_id, config.source, owner.account_id
    ).plan_request(bootstrap._journal.load_source(), 0)
    assert planned is not None
    transport = FakeClientSession([FakeResponse(400, b'{"errcode":"M_UNKNOWN_POS"}')])
    client = AsyncClient("https://example.org", ACCOUNT, DEVICE)
    client.restore_login(ACCOUNT, DEVICE, "secret-token")
    client.client_session = transport  # type: ignore[assignment]
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=config,
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    args, kwargs = transport.calls[0]
    assert args == (
        planned.method,
        "https://example.org" + planned.path + "?timeout=30000",
    )
    assert kwargs["data"] == planned.body
    assert kwargs["timeout"] == 30.0
    client.client_session = None
    await session.close()
