import asyncio
import sqlite3
from collections.abc import Callable
from uuid import UUID, uuid4

import pytest
from aiohttp import ClientConnectionError, ClientPayloadError

import nio
import nio.ingest
from nio import AsyncClient, AsyncClientConfig
from nio.exceptions import LocalProtocolError
from nio.ingest.config import (
    ClassicSourceConfig,
    IngestionConfig,
    SlidingSourceConfig,
)
from nio.ingest.classic import ClassicSource
from nio.ingest.ports import NetworkResult, StagedSourceResponse
from nio.ingest.source import canonical_json
from nio.ingest.state import SourceState, StagedFrame
from nio.store import SqliteStore, open_ingestion_store
from nio.ingest.sliding import SlidingSource
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


def open_bootstrap(tmp_path: object, generation: UUID):
    return open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=classic_config().source,
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
            room_id=ROOM,
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
            room_id=ROOM,
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
            room_id=ROOM,
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
        room_id=ROOM,
    )

    with pytest.raises(LocalProtocolError):
        nio.open_ingestion(
            client,
            bootstrap,
            config=classic_config(),
            consumer_generation=generation,
            stream_id=bootstrap.stream_id,
            room_id=ROOM,
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
            room_id=ROOM,
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
            room_id=ROOM,
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
        room_id=ROOM,
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
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert client.send_count == 1
    await session.close()


@pytest.mark.asyncio
async def test_run_stops_on_prestaged_blocked_frame_without_http(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    frame = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    client = RecordingClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionBlockedError):
        await session.run()
    assert bootstrap._journal.load_frame(frame.frame_id) is not None
    assert client.send_count == 0
    await session.close()

    reopened = open_bootstrap(tmp_path, generation)
    restarted_client = RecordingClient()
    restarted = nio.open_ingestion(
        restarted_client,
        reopened,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=reopened.stream_id,
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionBlockedError):
        await restarted.run()
    assert restarted_client.send_count == 0
    assert reopened._journal.load_frame(frame.frame_id) is not None
    await restarted.close()


@pytest.mark.asyncio
async def test_success_stages_blocked_frame_before_any_second_http(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    before = bootstrap._journal.load_source()
    client = RecordingClient()
    response = FakeResponse(200, b'{"next_batch":"s1","rooms":{}}')
    client.responses.append(response)
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionBlockedError):
        await session.run()

    after = bootstrap._journal.load_source()
    frames = bootstrap._journal.list_frames(2)
    assert after.next_request_id == before.next_request_id + 1
    assert after.cursor_json != before.cursor_json
    assert len(frames) == 1
    assert frames[0].response.response_body == response.content.body
    assert client.send_count == 1
    assert response.released
    await session.close()


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
        room_id=ROOM,
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
        room_id=ROOM,
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
            FakeResponse(200, b'{"next_batch":"s1","rooms":{}}'),
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
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionBlockedError):
        await session.run()

    assert sleeps == [0.007]
    assert len(client.send_calls) == 2
    assert client.send_calls[0] == client.send_calls[1]
    after = bootstrap._journal.load_source()
    assert after.next_request_id == before.next_request_id + 1
    assert len(bootstrap._journal.list_frames(2)) == 1
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
            FakeResponse(200, b'{"next_batch":"s1","rooms":{}}'),
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
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionBlockedError):
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
        room_id=ROOM,
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
        room_id=ROOM,
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
        room_id=ROOM,
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
        [first, FakeResponse(200, b'{"next_batch":"s1","rooms":{}}')]
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
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionBlockedError):
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
        room_id=ROOM,
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
        room_id=ROOM,
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
        room_id=ROOM,
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
        room_id=ROOM,
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
        room_id=ROOM,
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
        [failed, FakeResponse(200, b'{"next_batch":"s1","rooms":{}}')]
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
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionBlockedError):
        await session.run()
    assert failed.released
    assert client.send_count == 2
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
        room_id=ROOM,
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
        room_id=ROOM,
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
        room_id=ROOM,
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
        room_id=ROOM,
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
async def test_sliding_reset_required_is_terminal_without_state_change(
    tmp_path,
) -> None:
    generation = uuid4()
    config = IngestionConfig(SlidingSourceConfig(30_000, "diag", b"{}", b"{}", b"{}"))
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=config.source,
    )
    before = bootstrap._journal.load_source()
    client = RecordingClient()
    client.responses.append(FakeResponse(400, b'{"errcode":"M_UNKNOWN_POS"}'))
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=config,
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert client.send_count == 1
    assert bootstrap._journal.load_source() == before
    assert bootstrap._journal.list_frames(1) == ()
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
        room_id=ROOM,
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
        type(bootstrap._journal), "materialize_oldest_diagnostic_frame", at_capacity
    )
    client = RecordingClient()
    session = nio.open_ingestion(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
        room_id=ROOM,
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
        room_id=ROOM,
    )
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    assert sleeps == [0.0, 0.2, 0.0]
    assert client.send_calls[0] == client.send_calls[1] == client.send_calls[2]
    assert client.send_calls[3] == client.send_calls[4]
    assert client.send_calls[2] != client.send_calls[3]
    await session.close()


@pytest.mark.parametrize("room_id", ["", 1])
def test_open_ingestion_requires_exact_nonempty_room_id(
    tmp_path, room_id: object
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    with pytest.raises(LocalProtocolError):
        nio.open_ingestion(
            client,
            bootstrap,
            config=classic_config(),
            consumer_generation=generation,
            stream_id=bootstrap.stream_id,
            room_id=room_id,  # type: ignore[arg-type]
        )
    assert client.send_count == 0
    bootstrap.close()


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
        room_id=ROOM,
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
