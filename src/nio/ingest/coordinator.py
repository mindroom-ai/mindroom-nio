from __future__ import annotations

import asyncio
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
    from ..client.async_client import AsyncClient
    from ..store.sync_journal import StoreBootstrap

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
