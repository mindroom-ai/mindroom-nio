"""Behavioral ownership fences between ordinary sync and owned ingestion."""

import asyncio
import json
from pathlib import Path
from typing import Any, cast
from uuid import UUID, uuid4

import nio
import pytest
from ingestion_helpers import open_test_session
from nio import AsyncClient, AsyncClientConfig, Client, ClientConfig
from nio.exceptions import LocalProtocolError
from nio.ingest.config import ClassicSourceConfig, IngestionConfig
from nio.responses import RoomMessagesResponse, SyncResponse
from nio.store import SqliteStore
from nio.store.sync_journal import (
    StoreBootstrap,
    _open_configured_ingestion_store,
    _open_fresh_ingestion_store,
)

ACCOUNT = "@alice:example.org"
DEVICE = "ALICE"
ROOM = "!room:example.org"
PICKLE_KEY = "owned-secret"
DATABASE_NAME = "owned.db"
OWNED_SYNC_ERROR = "ordinary sync is unavailable during owned ingestion"


class _FakeContent:
    def __init__(self, body: bytes) -> None:
        self.body = body
        self.position = 0

    async def read(self, size: int = -1) -> bytes:
        end = len(self.body) if size < 0 else self.position + size
        chunk = self.body[self.position : end]
        self.position += len(chunk)
        return chunk


class _FakeResponse:
    def __init__(self, body: dict[str, object]) -> None:
        self.body = body
        self.status = 200
        self.content = _FakeContent(json.dumps(body).encode())
        self.content_type = "application/json"
        self.content_disposition = None
        self.headers: dict[str, str] = {}
        self.released = False

    async def json(self) -> dict[str, object]:
        return self.body

    async def text(self) -> str:
        return json.dumps(self.body)

    def release(self) -> None:
        self.released = True


class _SyncProbeClient(AsyncClient):
    def __init__(self) -> None:
        super().__init__("https://example.org", ACCOUNT, DEVICE)
        self.restore_login(ACCOUNT, DEVICE, "secret-token")
        self.send_calls: list[tuple[tuple[object, ...], dict[str, object]]] = []
        self.responses: list[_FakeResponse | BaseException] = []
        self.send_started = asyncio.Event()
        self.release_send: asyncio.Event | None = None

    async def send(self, *args: object, **kwargs: object) -> _FakeResponse:
        self.send_calls.append((args, kwargs))
        self.send_started.set()
        if self.release_send is not None:
            await self.release_send.wait()
        if not self.responses:
            raise AssertionError("unexpected HTTP request")
        response = self.responses.pop(0)
        if isinstance(response, BaseException):
            raise response
        return response


def _config() -> IngestionConfig:
    return IngestionConfig(ClassicSourceConfig(30_000, b"{}"))


def _fresh_bootstrap(tmp_path: Path, generation: UUID) -> StoreBootstrap:
    return _open_fresh_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=_config().source,
        pickle_key=PICKLE_KEY,
        database_name=DATABASE_NAME,
    )


def _reopen_bootstrap(tmp_path: Path, generation: UUID) -> StoreBootstrap:
    return _open_configured_ingestion_store(
        tmp_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=_config().source,
        pickle_key=PICKLE_KEY,
        database_name=DATABASE_NAME,
    )


def _prepare_for_owned(client: _SyncProbeClient, bootstrap: StoreBootstrap) -> None:
    client.store_path = str(bootstrap.database_path.parent)
    client.config = AsyncClientConfig(
        store=SqliteStore,
        pickle_key=PICKLE_KEY,
        store_name=DATABASE_NAME,
    )


def _open_session(
    client: _SyncProbeClient,
    bootstrap: StoreBootstrap,
    generation: UUID,
):
    return open_test_session(
        client,
        bootstrap,
        config=_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )


def _sync_response(token: str = "s1") -> SyncResponse:
    return SyncResponse.from_dict(
        {
            "account_data": {"events": []},
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {},
            "next_batch": token,
            "presence": {"events": []},
            "rooms": {
                "join": {
                    ROOM: {
                        "account_data": {"events": []},
                        "ephemeral": {"events": []},
                        "state": {"events": []},
                        "timeline": {
                            "events": [
                                {
                                    "content": {
                                        "body": "ownership fence",
                                        "msgtype": "m.text",
                                    },
                                    "event_id": "$owned-sync",
                                    "origin_server_ts": 1,
                                    "sender": "@bob:example.org",
                                    "type": "m.room.message",
                                }
                            ],
                            "limited": False,
                            "prev_batch": "p0",
                        },
                        "unread_notifications": {},
                    }
                }
            },
            "to_device": {"events": []},
        }
    )


async def _close_unexpected_session(session: object | None) -> None:
    if session is not None:
        await cast(Any, session).close()


@pytest.mark.asyncio
async def test_inflight_ordinary_sync_prevents_owned_attachment(tmp_path: Path) -> None:
    generation = uuid4()
    bootstrap = _fresh_bootstrap(tmp_path, generation)
    client = _SyncProbeClient()
    _prepare_for_owned(client, bootstrap)
    client.release_send = asyncio.Event()
    syncing = asyncio.create_task(client.sync())
    await client.send_started.wait()

    unexpected_session = None
    try:
        with pytest.raises(LocalProtocolError, match="pristine pre-sync client"):
            unexpected_session = _open_session(client, bootstrap, generation)
    finally:
        await _close_unexpected_session(unexpected_session)
        syncing.cancel()
        with pytest.raises(asyncio.CancelledError):
            await syncing

    with pytest.raises(LocalProtocolError, match="pristine pre-sync client"):
        _open_session(client, bootstrap, generation)

    fresh_client = _SyncProbeClient()
    fresh_session = _open_session(fresh_client, bootstrap, generation)
    await fresh_session.close()


@pytest.mark.asyncio
async def test_failed_ordinary_sync_requires_fresh_client(tmp_path: Path) -> None:
    generation = uuid4()
    bootstrap = _fresh_bootstrap(tmp_path, generation)
    client = _SyncProbeClient()
    _prepare_for_owned(client, bootstrap)
    failure = RuntimeError("transport failed")
    client.responses.append(failure)

    with pytest.raises(RuntimeError) as raised:
        await client.sync()
    assert raised.value is failure

    unexpected_session = None
    try:
        with pytest.raises(LocalProtocolError, match="pristine pre-sync client"):
            unexpected_session = _open_session(client, bootstrap, generation)
    finally:
        await _close_unexpected_session(unexpected_session)

    fresh_client = _SyncProbeClient()
    fresh_session = _open_session(fresh_client, bootstrap, generation)
    await fresh_session.close()


@pytest.mark.asyncio
async def test_owned_sync_entrypoints_reject_before_effects_and_do_not_stick(
    tmp_path: Path,
) -> None:
    generation = uuid4()
    bootstrap = _fresh_bootstrap(tmp_path, generation)
    client = _SyncProbeClient()
    session = _open_session(client, bootstrap, generation)
    before_token = client.next_batch
    before_source = session._journal.load_source()
    callback_events: list[str] = []
    crypto_calls: list[SyncResponse] = []

    async def callback(_room: nio.MatrixRoom, event: nio.RoomMessageText) -> None:
        callback_events.append(event.event_id)

    def observe_olm_events(response: SyncResponse) -> None:
        crypto_calls.append(response)

    client.add_event_callback(callback, nio.RoomMessageText)
    client._handle_olm_events = observe_olm_events  # type: ignore[method-assign]
    response = _sync_response()

    with pytest.raises(LocalProtocolError, match=OWNED_SYNC_ERROR):
        await client.sync()
    with pytest.raises(LocalProtocolError, match=OWNED_SYNC_ERROR):
        await client.receive_response(response)

    sync_forever_calls: list[str] = []

    async def bypassed_sync(*_args: object, **_kwargs: object) -> SyncResponse:
        sync_forever_calls.append("sync")
        client.stop_sync_forever()
        return response

    client.sync = bypassed_sync  # type: ignore[method-assign]
    with pytest.raises(LocalProtocolError, match=OWNED_SYNC_ERROR):
        await client.sync_forever()

    assert client.send_calls == []
    assert sync_forever_calls == []
    assert callback_events == []
    assert crypto_calls == []
    assert client.next_batch == before_token
    assert session._journal.load_source() == before_source

    client.responses.append(
        _FakeResponse({"chunk": [], "end": "history-end", "start": "history-start"})
    )
    history = await client.room_messages(ROOM, "history-start")
    assert type(history) is RoomMessagesResponse
    assert len(client.send_calls) == 1

    await session.close()
    reopened = _reopen_bootstrap(tmp_path, generation)
    reattached = _open_session(client, reopened, generation)
    await reattached.close()


def test_owned_base_client_sync_response_rejects_before_projection() -> None:
    client = Client(
        ACCOUNT,
        DEVICE,
        config=ClientConfig(encryption_enabled=False),
    )
    client._ingestion_store_snapshot = cast(Any, object())
    callback_events: list[str] = []

    def callback(_room: nio.MatrixRoom, event: nio.RoomMessageText) -> None:
        callback_events.append(event.event_id)

    client.add_event_callback(callback, nio.RoomMessageText)

    with pytest.raises(LocalProtocolError, match=OWNED_SYNC_ERROR):
        client.receive_response(_sync_response())

    assert callback_events == []
    assert client.next_batch == ""


@pytest.mark.asyncio
async def test_healthy_owned_detach_allows_ordinary_sync(tmp_path: Path) -> None:
    generation = uuid4()
    bootstrap = _fresh_bootstrap(tmp_path, generation)
    client = _SyncProbeClient()
    session = _open_session(client, bootstrap, generation)
    await session.close()
    client.responses.append(
        _FakeResponse(
            {
                "account_data": {"events": []},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {},
                "next_batch": "ordinary-token",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": []},
            }
        )
    )

    response = await client.sync()

    assert type(response) is SyncResponse
    assert response.next_batch == "ordinary-token"
    assert client.next_batch == "ordinary-token"
    assert len(client.send_calls) == 1
