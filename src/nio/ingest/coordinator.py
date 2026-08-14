from __future__ import annotations

import asyncio
import os
import sys
from typing import TYPE_CHECKING
from urllib.parse import quote, urlencode
from uuid import UUID

from aiohttp import ClientConnectionError, ClientError, ClientPayloadError

from ..exceptions import LocalProtocolError
from ..store._sync_journal_values import MaterializerLimits, MaterializeStatus
from . import ports
from .classic import ClassicSource
from .config import IngestionConfig, source_transport
from .hydration import normalize_hydration_response
from .model import BatchRef, SyncBatch, TransportKind
from .sliding import SlidingSource
from .source import SourceResultKind, SourceScheduleStatus, plan_source_poll
from .state import SourceState, StagedFrame

if TYPE_CHECKING:
    from collections.abc import Coroutine

    from ..client.async_client import AsyncClient
    from ..store._sync_journal import SqliteIngestionJournal as _SqliteIngestionJournal
    from ..store.database import MatrixStore
    from ..store.sync_journal import StoreBootstrap, _OwnedStoreLease

__all__ = (
    "IngestionBlockedError",
    "IngestionError",
    "IngestionHydrationError",
    "IngestionSession",
    "IngestionSourceError",
    "open_ingestion",
)


class IngestionError(LocalProtocolError):
    pass


class IngestionBlockedError(IngestionError):
    def __init__(self, frame_id: UUID | None) -> None:
        self.frame_id = frame_id
        super().__init__(f"durable ingestion is blocked by Frame {frame_id}")


class IngestionSourceError(IngestionError):
    pass


class IngestionHydrationError(IngestionError):
    def __init__(self, hydration_id: UUID) -> None:
        self.hydration_id = hydration_id
        super().__init__(f"room-state hydration failed for {hydration_id}")


class IngestionSession:
    def __init__(
        self,
        client: AsyncClient,
        bootstrap: StoreBootstrap,
        config: IngestionConfig,
    ) -> None:
        self._client = client
        self._bootstrap = bootstrap
        self._config = config
        self._journal = bootstrap._claim_session()
        owner = self._journal.load_owner()
        classic = owner.transport_kind is TransportKind.CLASSIC
        source_type = ClassicSource if classic else SlidingSource
        self._source = source_type(owner.stream_id, config.source, owner.account_id)  # type: ignore[arg-type]
        self._close_task: asyncio.Task[None] | None = None
        self._running: asyncio.Task[None] | None = None
        self._terminal: BaseException | None = None

    def next_batch(
        self,
        *,
        max_records: int = 256,
        max_canonical_bytes: int = 16 * 1024 * 1024,
    ) -> SyncBatch | None:
        if self._close_task is not None:
            raise LocalProtocolError("ingestion session is closed")
        limits = {"max_records": max_records}
        limits["max_canonical_bytes"] = max_canonical_bytes
        return self._journal.next_batch(**limits)

    def acknowledge_batch(self, ref: BatchRef) -> None:
        if self._close_task is not None:
            raise LocalProtocolError("ingestion session is closed")
        self._journal.acknowledge_batch(ref)

    async def run(self) -> None:
        if self._close_task is not None:
            raise LocalProtocolError("ingestion session is closed")
        if self._terminal is not None:
            raise self._terminal
        if self._running is not None:
            raise LocalProtocolError("ingestion session already has an active runner")
        running = asyncio.current_task()
        assert running is not None
        self._running = running
        try:
            await self._run_loop()
        except Exception as error:
            self._terminal = error
            raise
        finally:
            self._running = None

    async def close(self) -> None:
        if self._close_task is None:
            self._close_task = asyncio.create_task(self._cleanup())
        await asyncio.shield(self._close_task)

    async def _cleanup(self) -> None:
        if self._running is not None:
            self._running.cancel()
            await asyncio.gather(self._running, return_exceptions=True)
        self._bootstrap.close()

    async def __aenter__(self) -> IngestionSession:
        return self

    async def __aexit__(self, *args: object) -> None:
        await self.close()

    async def _run_loop(self) -> None:
        retries = 0
        hydration_retries = 0
        while self._close_task is None:
            while True:
                materialized = self._journal.materialize_oldest_frame(
                    limits=MaterializerLimits()
                )
                if materialized.status is MaterializeStatus.MATERIALIZED:
                    continue
                if materialized.status in (
                    MaterializeStatus.BLOCKED,
                    MaterializeStatus.AT_CAPACITY,
                ):
                    raise IngestionBlockedError(materialized.frame_id)
                frames = self._journal.list_frames(self._config.max_staged_frames + 1)
                if frames:
                    raise IngestionBlockedError(frames[0].frame_id)
                break

            # fmt: off
            pending = self._journal.load_pending_hydrations(limit=2)
            if pending:
                candidate = pending[0]
                if (
                    len(pending) == 2
                    and pending[1].intent.hydration_id == candidate.intent.hydration_id
                ):
                    raise IngestionHydrationError(candidate.intent.hydration_id)
                prior = self._journal.load_source()
                owner = self._journal.load_owner()
                hydration_request = ports.NetworkRequest(owner.stream_id, owner.transport_kind, prior.source_epoch, prior.next_request_id, "GET", f"/_matrix/client/v3/rooms/{quote(candidate.continuity.room_id, safe='')}/state", (), None, self._config.source.timeout_ms, prior.cursor_json)
                try:
                    network = await self._request(hydration_request, body_disconnect_retry=True)
                except (IngestionSourceError, AttributeError, TypeError, ValueError) as error:
                    raise IngestionHydrationError(candidate.intent.hydration_id) from error
                retryable = network.failure is not None or network.status_code in (408, 429) or network.status_code is not None and 500 <= network.status_code <= 599
                if retryable:
                    hydration_retries += 1
                    delay = network.retry_after_ms / 1000 if network.retry_after_ms else 0.0
                    if network.retry_after_ms is None and hydration_retries >= 2:
                        delay = min(self._client.config.backoff_factor * (2 ** (min(hydration_retries, 1000) - 1)), self._client.config.max_timeout_retry_wait_time)
                    await asyncio.sleep(delay)
                    continue
                if network.status_code != 200:
                    raise IngestionHydrationError(candidate.intent.hydration_id)
                try:
                    hydration_result = normalize_hydration_response(candidate, own_user_id=owner.account_id, response_body=network.body)
                except (TypeError, ValueError) as error:
                    raise IngestionHydrationError(candidate.intent.hydration_id) from error
                self._journal.apply_hydration_result(result=hydration_result)
                hydration_retries = 0
                continue
            hydration_retries = 0
            # fmt: on
            prior = self._journal.load_source()
            decision = plan_source_poll(
                self._source,
                prior,
                prior.next_request_id,
                frames,
                self._config.max_staged_frames,
            )
            if decision.status is SourceScheduleStatus.INACTIVE:
                return
            if decision.status is SourceScheduleStatus.AT_CAPACITY:
                raise IngestionBlockedError(frames[0].frame_id if frames else None)
            request = decision.request
            assert request is not None
            try:
                result = self._source.normalize(request, await self._request(request))
            except (AttributeError, TypeError, ValueError) as error:
                raise IngestionSourceError("malformed source result") from error
            if result.kind is SourceResultKind.RETRYABLE_ERROR:
                retries += 1
                delay = result.retry_after_ms / 1000 if result.retry_after_ms else 0.0
                if result.retry_after_ms is None and retries >= 2:
                    delay = min(
                        self._client.config.backoff_factor
                        * (2 ** (min(retries, 1000) - 1)),
                        self._client.config.max_timeout_retry_wait_time,
                    )
                await asyncio.sleep(delay)
                continue
            if result.kind is SourceResultKind.RESET_REQUIRED:
                if self._journal._reset_sliding_source(request=request) is not None:
                    continue
            if result.kind is not SourceResultKind.FRAME:
                raise IngestionSourceError(result.detail or result.kind.value)
            retries = 0
            assert result.frame is not None
            frame = StagedFrame(
                result.frame.frame_id,
                ports.StagedSourceResponse(
                    request,
                    result.response_body,
                    result.frame.source_sha256,
                ),
            )
            successor = SourceState(
                prior.source_epoch,
                prior.transport_kind,
                result.frame.candidate_cursor_json,
                request.request_id + 1,
                prior.active,
            )
            self._journal.stage_source_response(source=successor, frame=frame)

    @staticmethod
    def _network_result(
        request: ports.NetworkRequest,
        status: int | None,
        body: bytes,
        failure: ports.NetworkFailureKind | None = None,
        retry_after: int | None = None,
    ) -> ports.NetworkResult:
        return ports.NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            status,
            body,
            failure,
            retry_after,
        )

    # fmt: off
    async def _request(self, request: ports.NetworkRequest, *, body_disconnect_retry: bool = False) -> ports.NetworkResult:
        # fmt: on
        path = request.path
        if request.query:
            path += "?" + urlencode(request.query)
        try:
            response = await self._client.send(
                request.method,
                path,
                request.body,
                {"Authorization": f"Bearer {self._client.access_token}"},
                None,
                request.timeout_ms / 1000,
            )
            try:
                remaining = ports.MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES + 1
                chunks: list[bytes] = []
                while remaining:
                    chunk = await response.content.read(remaining)
                    if not chunk:
                        break
                    if len(chunk) > remaining:
                        raise ValueError("response reader exceeded requested bound")
                    chunks.append(chunk)
                    remaining -= len(chunk)
                body = b"".join(chunks)
                retry_after = None
                try:
                    seconds = int(response.headers.get("Retry-After", ""))
                    if seconds >= 0:
                        ceiling = int(
                            self._client.config.max_timeout_retry_wait_time * 1000
                        )
                        retry_after = min(seconds * 1000, ceiling)
                except ValueError:
                    pass
                return self._network_result(
                    request, response.status, body, retry_after=retry_after
                )
            finally:
                response.release()
        except (TimeoutError, ClientConnectionError) as error:
            failure = (
                ports.NetworkFailureKind.CONNECTION
                if isinstance(error, ClientConnectionError)
                else ports.NetworkFailureKind.TIMEOUT
            )
            return self._network_result(request, None, b"", failure)
        except ClientPayloadError as error:
            if body_disconnect_retry:
                return self._network_result(request, None, b"", ports.NetworkFailureKind.CONNECTION)
            raise IngestionSourceError("malformed response body") from error
        except ClientError as error:
            raise IngestionSourceError("malformed response body") from error


async def _drain_owned_cleanup(
    operation: Coroutine[object, object, None],
) -> None:
    """Finish owned cleanup before propagating caller cancellation."""
    task = asyncio.create_task(operation)
    cancellations = 0
    deferred_error: BaseException | None = None
    while not task.done():
        try:
            await asyncio.shield(task)
        except asyncio.CancelledError:  # noqa: PERF203
            cancellations += 1
        except BaseException as error:
            if deferred_error is None:
                deferred_error = error
    try:
        task.result()
    except BaseException:
        caller = asyncio.current_task()
        if caller is not None and sys.version_info >= (3, 11):
            for _ in range(cancellations):
                caller.uncancel()
        raise
    if deferred_error is not None:
        caller = asyncio.current_task()
        if caller is not None and sys.version_info >= (3, 11):
            for _ in range(cancellations):
                caller.uncancel()
        raise deferred_error
    if cancellations:
        raise asyncio.CancelledError


class _OwnedIngestionSession(IngestionSession):
    """Private ingestion session holding the exact journal/store close token."""

    def __init__(
        self,
        client: AsyncClient,
        config: IngestionConfig,
        lease: _OwnedStoreLease,
        store: MatrixStore,
        journal: _SqliteIngestionJournal,
        source: ClassicSource | SlidingSource,
    ) -> None:
        self._client = client
        self._config = config
        self._journal = journal
        self._source = source
        self._lease = lease
        self._owned_store = store
        self._close_task: asyncio.Task[None] | None = None
        self._running: asyncio.Task[None] | None = None
        self._terminal: BaseException | None = None
        self._detached = False
        self._revoked = False
        self._closed = False

    async def close(self) -> None:
        self._lease._prepare_close()
        if self._closed:
            raise LocalProtocolError("owned ingestion session is already closed")
        if self._running is asyncio.current_task():
            raise LocalProtocolError("owned ingestion runner cannot close itself")
        if self._close_task is None or (
            self._close_task.done()
            and not self._close_task.cancelled()
            and self._close_task.exception() is not None
        ):
            self._close_task = asyncio.create_task(self._cleanup())
        task = self._close_task
        assert task is not None

        async def wait_for_shared_cleanup() -> None:
            await asyncio.shield(task)

        await _drain_owned_cleanup(wait_for_shared_cleanup())

    async def _cleanup(self) -> None:
        if self._running is not None:
            self._running.cancel()
            await asyncio.gather(self._running, return_exceptions=True)
        if not self._detached:
            self._client._detach_ingestion_store(self._owned_store)
            self._detached = True
        if not self._revoked:
            self._lease._revoke_store(self._owned_store)
            self._revoked = True
        self._lease._close_owner()
        self._closed = True


def _validate_owned_client_is_pristine(client: AsyncClient) -> None:
    fence = client._sync_reset_fence
    recovery = client._recovery
    if recovery is None:
        raise LocalProtocolError("owned ingestion requires pristine recovery state")
    dirty = (
        client.store is not None
        or client.olm is not None
        or client.client_session is not None
        or client._closing
        or client._stop_sync_forever
        or client._sync_response_seen
        or client._sync_response_lock.locked()
        or client._active_sync_executor_token is not None
        or client._active_classic_sync_state_operation is not None
        or client._sync_generation.classic_request_lock.locked()
        or client._sync_generation.to_device_request_lock.locked()
        or client._sync_generation.to_device_transport is not None
        or bool(client._sync_generation.sliding_request_locks)
        or fence.request_id != 0
        or bool(fence.active_request_ids)
        or bool(fence.room_cutoffs)
        or fence.to_device_floor != 0
        or bool(fence.one_time_key_count_floors)
        or bool(client.next_batch)
        or bool(client.loaded_sync_token)
        or bool(client.rooms)
        or bool(client.invited_rooms)
        or bool(client.encrypted_rooms)
        or client._classic_sync_rebuild_pending
        or client._classic_sync_state_staged
        or client._classic_sync_acknowledgeable_token is not None
        or bool(client._classic_sync_abandonment_before_staged_response)
        or bool(client._pending_sliding_room_account_data)
        or bool(client._sliding_room_prev_batch)
        or client._sliding_sync_to_device_since is not None
        or bool(client._recovery_room_gates)
        or bool(client.sharing_session)
        or client.synced.is_set()
        or bool(recovery.gaps)
        or bool(recovery.events)
        or bool(recovery.completed)
        or bool(recovery.outcomes)
        or bool(recovery.abandoned)
        or recovery.room_offset != 0
        or bool(recovery._active_dispatches)
        or bool(recovery._dispatch_waiters)
        or bool(recovery._deferred_dispatch_errors)
        or client._sync_request_generation.get() is not None
        or client._sync_request_id.get() is not None
        or client._classic_sync_state_operation_context.get() is not None
        or client._current_response_room_ids.get() is not None
        or client._sliding_request_scope.get() is not None
        or client._event_callback_scope.get() is not None
        or client._response_callback_scope.get() is not None
    )
    if dirty:
        raise LocalProtocolError("owned ingestion requires a pristine pre-sync client")


def _open_owned_ingestion(
    client: AsyncClient,
    bootstrap: StoreBootstrap,
    *,
    config: IngestionConfig,
    consumer_generation: UUID,
    stream_id: UUID | None,
) -> _OwnedIngestionSession:
    from ..client.async_client import AsyncClient
    from ..store.database import SqliteStore
    from ..store.sync_journal import StoreBootstrap

    if not isinstance(client, AsyncClient):
        raise LocalProtocolError("ingestion requires an AsyncClient")
    authenticated = all(
        type(value) is str and bool(value)
        for value in (client.access_token, client.user_id, client.device_id)
    )
    if not authenticated or any(
        key.lower() == "authorization" for key in client.config.custom_headers or {}
    ):
        raise LocalProtocolError("ingestion requires an authenticated client")
    if client.store is not None or client.olm is not None:
        raise LocalProtocolError("owned ingestion requires a storeless client")
    if not client.config.encryption_enabled:
        raise LocalProtocolError("owned ingestion requires encryption")
    if type(bootstrap) is not StoreBootstrap:
        raise LocalProtocolError("ingestion requires a StoreBootstrap")
    if (
        bootstrap._owned_store_class is not SqliteStore
        or bootstrap._authenticated_pickle_key is None
    ):
        raise LocalProtocolError("owned ingestion requires a bound bootstrap")
    if type(config) is not IngestionConfig:
        raise LocalProtocolError("config must be IngestionConfig")
    if type(consumer_generation) is not UUID:
        raise LocalProtocolError("consumer_generation must be UUID")
    if type(stream_id) is not UUID:
        raise LocalProtocolError("stream_id must be a bound UUID")
    owner = bootstrap._journal.load_owner()
    if (client.user_id, client.device_id) != (owner.account_id, owner.device_id):
        raise LocalProtocolError("client identity does not match ingestion owner")
    if (consumer_generation, stream_id) != (
        owner.consumer_generation,
        owner.stream_id,
    ):
        raise LocalProtocolError("consumer generation/stream binding does not match")
    if source_transport(config.source) is not owner.transport_kind:
        raise LocalProtocolError("source transport does not match ingestion owner")
    if config.source != bootstrap._bound_source:
        raise LocalProtocolError("source config does not match ingestion bootstrap")
    if config.sqlite_busy_timeout_ms != bootstrap._bound_sqlite_busy_timeout_ms:
        raise LocalProtocolError("SQLite timeout does not match ingestion bootstrap")
    if client.config.store is not SqliteStore:
        raise LocalProtocolError("owned ingestion config requires exact SqliteStore")
    if client.config.pickle_key != bootstrap._authenticated_pickle_key:
        raise LocalProtocolError("owned ingestion config pickle key does not match")
    if not client.store_path:
        raise LocalProtocolError("owned ingestion config requires a store path")
    database_name = (
        client.config.store_name or f"{client.user_id}_{client.device_id}.db"
    )
    configured_path = os.path.realpath(os.path.join(client.store_path, database_name))
    if configured_path != os.path.realpath(bootstrap.database_path):
        raise LocalProtocolError("owned ingestion config path does not match")
    _validate_owned_client_is_pristine(client)

    journal = bootstrap._journal
    source_type = (
        ClassicSource
        if owner.transport_kind is TransportKind.CLASSIC
        else SlidingSource
    )
    source = source_type(owner.stream_id, config.source, owner.account_id)  # type: ignore[arg-type]

    candidate = bootstrap._open_owned_store_candidate()
    store = candidate._store_for_attachment()
    try:
        _validate_owned_client_is_pristine(client)
        client._attach_ingestion_store(store)
        lease = candidate._prepare_transfer(store)
        session = _OwnedIngestionSession(
            client,
            config,
            lease,
            store,
            journal,
            source,
        )
        candidate._commit_transfer(store, lease)
        return session
    except BaseException:
        try:
            if client.store is store:
                client._detach_ingestion_store(store)
        finally:
            candidate._tombstone()
        raise


def open_ingestion(
    client: AsyncClient,
    bootstrap: StoreBootstrap,
    *,
    config: IngestionConfig,
    consumer_generation: UUID,
    stream_id: UUID | None,
) -> IngestionSession:
    from ..client.async_client import AsyncClient
    from ..store.sync_journal import StoreBootstrap

    if not isinstance(client, AsyncClient):
        raise LocalProtocolError("ingestion requires an AsyncClient")
    authenticated = client.access_token and client.user_id and client.device_id
    if not authenticated or any(
        key.lower() == "authorization" for key in client.config.custom_headers or {}
    ):
        raise LocalProtocolError("ingestion requires an authenticated client")
    if type(bootstrap) is not StoreBootstrap:
        raise LocalProtocolError("ingestion requires a StoreBootstrap")
    if type(config) is not IngestionConfig:
        raise LocalProtocolError("config must be IngestionConfig")
    if type(consumer_generation) is not UUID:
        raise LocalProtocolError("consumer_generation must be UUID")
    if type(stream_id) is not UUID:
        raise LocalProtocolError("stream_id must be a bound UUID")
    owner = bootstrap._journal.load_owner()
    if (client.user_id, client.device_id) != (owner.account_id, owner.device_id):
        raise LocalProtocolError("client identity does not match ingestion owner")
    if (consumer_generation, stream_id) != (
        owner.consumer_generation,
        owner.stream_id,
    ):
        raise LocalProtocolError("consumer generation/stream binding does not match")
    if source_transport(config.source) is not owner.transport_kind:
        raise LocalProtocolError("source transport does not match ingestion owner")
    return IngestionSession(client, bootstrap, config)
