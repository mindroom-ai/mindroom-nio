from __future__ import annotations

import asyncio
import os
import sys
from asyncio import sleep as _cooperative_sleep
from hashlib import sha256
from typing import TYPE_CHECKING, NamedTuple
from urllib.parse import quote, urlencode
from uuid import UUID, uuid5

from aiohttp import ClientConnectionError, ClientError, ClientPayloadError

from ..event_builders import DummyMessage, RoomKeyRequestMessage, ToDeviceMessage
from ..events.account_data import AccountDataEvent
from ..events.ephemeral import EphemeralEvent
from ..events.invite_events import InviteEvent
from ..events.misc import BadEvent, UnknownBadEvent
from ..events.presence import PresenceEvent
from ..events.room_events import Event, MegolmEvent
from ..events.to_device import (
    DummyEvent,
    ForwardedRoomKeyEvent,
    KeyVerificationCancel,
    RoomKeyEvent,
    RoomKeyRequest,
    RoomKeyRequestCancellation,
    ToDeviceEvent,
    UnknownToDeviceEvent,
)
from ..exceptions import LocalProtocolError
from ..responses import (
    JoinResponse,
    KeysClaimResponse,
    KeysQueryResponse,
    KeysUploadResponse,
    RoomLeaveResponse,
    ToDeviceResponse,
)
from ..store._sync_journal_rows import (
    _decoded_bytes,
    _encoded_bytes,
    _OutboundMaintenance,
    _OutboundOperation,
    _validate_outbound_maintenance,
)
from ..store._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
    RoomAggregateValue,
    _FrameCompletion,
    _LocalMembershipIntent,
)
from . import ports
from ._json import canonical_json, load_json
from .classic import ClassicSource
from .config import IngestionConfig, source_transport
from .errors import JournalConflictError, JournalIntegrityError
from .hydration import normalize_hydration_response
from .model import (
    BatchRef,
    EventRecord,
    LossRecord,
    RecordKind,
    SyncBatch,
    TransportKind,
    _CallbackRoute,
    _DecryptedToDeviceKind,
    _DecryptionDisposition,
    _PreparationPhase,
    _PreparedCryptoDelta,
    _PreparedIngestionFrame,
    _PreparedMegolmRerequest,
    _PreparedQueuedToDeviceMessage,
    _PreparedWaitingKeyRequest,
    _QueuedToDeviceSubtype,
)
from .reducer import RoomContinuity, _validate_prepared_crypto_delta
from .sliding import SlidingSource
from .source import SourceResultKind, SourceScheduleStatus, SyncFrame, plan_source_poll
from .state import CommitResult, SourceState, StagedFrame

if TYPE_CHECKING:
    from collections.abc import Awaitable, Callable, Coroutine

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


class _LocalMembershipCommand(NamedTuple):
    room_id: str
    intent: _LocalMembershipIntent
    completion: asyncio.Future[bool]


def _encoded_prepared_source(source_json: bytes) -> str:
    encoded = _encoded_bytes(source_json)
    assert encoded is not None
    return encoded


def _rerequest_context(
    event: _PreparedMegolmRerequest,
) -> dict[str, object]:
    return {
        "source_json": _encoded_prepared_source(event.source_json),
        "room_id": event.room_id,
        "event_id": event.event_id,
        "sender_user_id": event.sender_user_id,
        "sender_device_id": event.sender_device_id,
        "sender_key": event.sender_key,
        "session_id": event.session_id,
        "algorithm": event.algorithm,
    }


_KEY_CLAIM_REREQUEST_FIELDS = (
    "source_json",
    "room_id",
    "event_id",
    "sender_user_id",
    "sender_device_id",
    "sender_key",
    "session_id",
    "algorithm",
)

_KEY_CLAIM_WAITING_FIELDS = (
    "source_json",
    "sender_user_id",
    "requesting_device_id",
    "request_id",
    "room_id",
    "sender_key",
    "session_id",
    "algorithm",
)


def _key_claim_waiting_request(
    value: object,
    *,
    user_id: str,
    device_id: str,
) -> RoomKeyRequest:
    try:
        if type(value) is not dict or tuple(value) != _KEY_CLAIM_WAITING_FIELDS:
            raise ValueError
        fields = value
        if any(
            type(fields[name]) is not str or not fields[name]
            for name in _KEY_CLAIM_WAITING_FIELDS[1:]
        ):
            raise ValueError
        source_json = _decoded_bytes(fields["source_json"], "waiting key request")
        if source_json is None or fields["source_json"] != _encoded_bytes(source_json):
            raise ValueError
        source = load_json(source_json, "waiting key request")
        if type(source) is not dict or canonical_json(source) != source_json:
            raise ValueError
        event = ToDeviceEvent.parse_event(source)
        if type(event) is not RoomKeyRequest or (
            event.sender,
            event.requesting_device_id,
            event.request_id,
            event.room_id,
            event.sender_key,
            event.session_id,
            event.algorithm,
        ) != (
            user_id,
            device_id,
            fields["request_id"],
            fields["room_id"],
            fields["sender_key"],
            fields["session_id"],
            fields["algorithm"],
        ):
            raise ValueError
        return event
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise JournalIntegrityError("waiting key request is invalid") from error


def _same_key_claim_waiting(first: RoomKeyRequest, second: RoomKeyRequest) -> bool:
    return (
        type(first) is RoomKeyRequest
        and type(second) is RoomKeyRequest
        and canonical_json(first.source) == canonical_json(second.source)
        and (
            first.sender,
            first.requesting_device_id,
            first.request_id,
            first.room_id,
            first.sender_key,
            first.session_id,
            first.algorithm,
        )
        == (
            second.sender,
            second.requesting_device_id,
            second.request_id,
            second.room_id,
            second.sender_key,
            second.session_id,
            second.algorithm,
        )
    )


def _key_claim_rerequest_event(
    value: object,
    *,
    user_id: str,
    device_id: str,
) -> MegolmEvent:
    try:
        if type(value) is not dict or tuple(value) != _KEY_CLAIM_REREQUEST_FIELDS:
            raise ValueError
        fields = value
        if any(
            type(fields[name]) is not str or not fields[name]
            for name in _KEY_CLAIM_REREQUEST_FIELDS[1:]
        ):
            raise ValueError
        source_json = _decoded_bytes(fields["source_json"], "key-claim rerequest")
        if source_json is None or fields["source_json"] != _encoded_bytes(source_json):
            raise ValueError
        source = load_json(source_json, "key-claim rerequest")
        if (
            type(source) is not dict
            or canonical_json(source) != source_json
            or source.get("sender") != user_id
        ):
            raise ValueError
        event = Event.parse_event(source)
        if type(event) is not MegolmEvent:
            raise ValueError
        event.room_id = fields["room_id"]
        if (
            event.event_id,
            event.sender,
            event.device_id,
            event.sender_key,
            event.session_id,
            event.algorithm,
        ) != (
            fields["event_id"],
            user_id,
            device_id,
            fields["sender_key"],
            fields["session_id"],
            fields["algorithm"],
        ):
            raise ValueError
        return event
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise JournalIntegrityError("key-claim rerequest is invalid") from error


def _same_key_claim_rerequest(first: MegolmEvent, second: MegolmEvent) -> bool:
    return (
        type(first) is MegolmEvent
        and type(second) is MegolmEvent
        and canonical_json(first.source) == canonical_json(second.source)
        and (
            first.room_id,
            first.event_id,
            first.sender,
            first.device_id,
            first.sender_key,
            first.session_id,
            first.algorithm,
        )
        == (
            second.room_id,
            second.event_id,
            second.sender,
            second.device_id,
            second.sender_key,
            second.session_id,
            second.algorithm,
        )
    )


def _to_device_message(
    operation: _OutboundOperation,
) -> tuple[ToDeviceMessage, tuple[MegolmEvent, ...]]:
    try:
        if (
            operation.kind != "to_device"
            or type(operation.transaction_id) is not str
            or not operation.transaction_id
            or type(operation.event_type) is not str
            or not operation.event_type
            or type(operation.context) is not dict
        ):
            raise ValueError
        body = load_json(operation.body_json, "to-device operation body")
        if (
            type(body) is not dict
            or canonical_json(body) != operation.body_json
            or tuple(body) != ("messages",)
        ):
            raise ValueError
        messages = body["messages"]
        if type(messages) is not dict or len(messages) != 1:
            raise ValueError
        recipient, devices = next(iter(messages.items()))
        if type(recipient) is not str or not recipient or type(devices) is not dict:
            raise ValueError
        if len(devices) != 1:
            raise ValueError
        device_id, content = next(iter(devices.items()))
        if type(device_id) is not str or not device_id or type(content) is not dict:
            raise ValueError
        context = operation.context
        if context == {"subtype": "generic"}:
            message: ToDeviceMessage = ToDeviceMessage(
                operation.event_type,
                recipient,
                device_id,
                content,
            )
            rerequests: tuple[MegolmEvent, ...] = ()
        elif (
            tuple(context) == ("subtype", "rerequest_events")
            and context["subtype"] == "dummy"
            and type(context["rerequest_events"]) is list
            and len(context["rerequest_events"]) <= 1
            and operation.event_type == "m.room.encrypted"
        ):
            message = DummyMessage(
                operation.event_type,
                recipient,
                device_id,
                content,
            )
            rerequests = tuple(
                _key_claim_rerequest_event(
                    value,
                    user_id=recipient,
                    device_id=device_id,
                )
                for value in context["rerequest_events"]
            )
        elif (
            tuple(context)
            == ("subtype", "request_id", "session_id", "room_id", "algorithm")
            and context["subtype"] == "room_key_request"
            and operation.event_type == "m.room_key_request"
            and all(
                type(context[name]) is str and context[name]
                for name in ("request_id", "session_id", "room_id", "algorithm")
            )
        ):
            message = RoomKeyRequestMessage(
                operation.event_type,
                recipient,
                device_id,
                content,
                context["request_id"],
                context["session_id"],
                context["room_id"],
                context["algorithm"],
            )
            rerequests = ()
        else:
            raise ValueError
        if canonical_json(message.as_dict()) != operation.body_json:
            raise ValueError
        return message, rerequests
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise JournalIntegrityError("to-device operation is invalid") from error


def _waiting_request_context(
    request: _PreparedWaitingKeyRequest,
) -> dict[str, object]:
    return {
        "source_json": _encoded_prepared_source(request.source_json),
        "sender_user_id": request.sender_user_id,
        "requesting_device_id": request.requesting_device_id,
        "request_id": request.request_id,
        "room_id": request.room_id,
        "sender_key": request.sender_key,
        "session_id": request.session_id,
        "algorithm": request.algorithm,
    }


def _key_claim_context(delta: _PreparedCryptoDelta) -> dict[str, object]:
    return {
        "claims": [
            {
                "user_id": claim.user_id,
                "device_id": claim.device_id,
                "was_wedged": claim.was_wedged,
                "was_waiting": claim.was_waiting,
                "waiting_key_requests": [
                    _waiting_request_context(request)
                    for request in claim.waiting_key_requests
                ],
                "rerequest_events": [
                    _rerequest_context(event) for event in claim.rerequest_events
                ],
            }
            for claim in delta.key_claims
        ]
    }


def _queued_to_device_context(
    message: _PreparedQueuedToDeviceMessage,
) -> dict[str, object]:
    if message.subtype is _QueuedToDeviceSubtype.GENERIC:
        return {"subtype": "generic"}
    if message.subtype is _QueuedToDeviceSubtype.DUMMY:
        return {
            "subtype": "dummy",
            "rerequest_events": [
                _rerequest_context(event) for event in message.rerequest_events
            ],
        }
    if message.subtype is _QueuedToDeviceSubtype.ROOM_KEY_REQUEST:
        return {
            "subtype": "room_key_request",
            "request_id": message.request_id,
            "session_id": message.session_id,
            "room_id": message.room_id,
            "algorithm": message.algorithm,
        }
    raise ValueError("queued to-device subtype is invalid")


def _initial_outbound_maintenance(
    *,
    frame_id: UUID,
    delta: _PreparedCryptoDelta,
    upload_body: bytes | None,
) -> _OutboundMaintenance:
    operations: list[_OutboundOperation] = []
    if upload_body is not None:
        operations.append(
            _OutboundOperation(
                "key_upload",
                "pending",
                upload_body,
                None,
                None,
                None,
            )
        )
    if delta.users_for_key_query:
        operations.append(
            _OutboundOperation(
                "key_query",
                "pending",
                canonical_json(
                    {
                        "device_keys": {
                            user_id: [] for user_id in delta.users_for_key_query
                        }
                    }
                ),
                None,
                None,
                None,
            )
        )
    if delta.key_claims:
        claim_targets: dict[str, dict[str, str]] = {}
        for claim in delta.key_claims:
            claim_targets.setdefault(claim.user_id, {})[
                claim.device_id
            ] = "signed_curve25519"
        operations.append(
            _OutboundOperation(
                "key_claim",
                "pending",
                canonical_json({"one_time_keys": claim_targets}),
                None,
                None,
                _key_claim_context(delta),
            )
        )
    for message in delta.queued_to_device_messages:
        content = load_json(message.content_json, "queued to-device content")
        if type(content) is not dict or canonical_json(content) != message.content_json:
            raise ValueError("queued to-device content is not canonical")
        body_json = canonical_json(
            {
                "messages": {
                    message.recipient_user_id: {message.recipient_device_id: content}
                }
            }
        )
        operation_index = len(operations)
        operations.append(
            _OutboundOperation(
                "to_device",
                "pending",
                body_json,
                str(
                    uuid5(
                        frame_id,
                        "nio.ingest.outbound-maintenance.v1:"
                        f"to-device:{operation_index}:"
                        f"{sha256(body_json).hexdigest()}",
                    )
                ),
                message.event_type,
                _queued_to_device_context(message),
            )
        )
    maintenance = _OutboundMaintenance(tuple(operations))
    _validate_outbound_maintenance(maintenance, frame_id=frame_id)
    return maintenance


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
        self._source_commit_generation = 0
        self._quiesce_source_commit_target: int | None = None

    def next_batch(
        self,
        *,
        max_records: int = 256,
        max_canonical_bytes: int = 16 * 1024 * 1024,
    ) -> SyncBatch | None:
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None:
            raise LocalProtocolError("ingestion session is closed")
        limits = {"max_records": max_records}
        limits["max_canonical_bytes"] = max_canonical_bytes
        return self._journal.next_batch(**limits)

    def acknowledge_batch(self, ref: BatchRef) -> None:
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None:
            raise LocalProtocolError("ingestion session is closed")
        self._journal.acknowledge_batch(ref)

    def _materialize_oldest_frame(
        self, *, limits: MaterializerLimits
    ) -> MaterializeResult:
        return self._journal.materialize_oldest_frame(limits=limits)

    async def _advance_blocked_frame(self, frame_id: UUID) -> bool:
        return False

    async def _advance_local_membership_intent(self) -> bool:
        return False

    def _signal_progress(self) -> None:
        """Public sessions have no progress waiters."""

    def _signal_durable_progress(self) -> None:
        """Public sessions have no private durable-progress observer."""
        self._signal_progress()

    async def run(self) -> None:
        self._client._assert_ingestion_not_poisoned()
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

    async def _quiesce(self) -> None:
        """Commit one final source response before clean shutdown."""
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None:
            raise LocalProtocolError("ingestion session is closed")
        if self._terminal is not None:
            raise self._terminal
        if (
            self._quiesce_source_commit_target is not None
            and self._source_commit_generation >= self._quiesce_source_commit_target
        ):
            return
        if not self._journal.load_source().active:
            return
        running = self._running
        if running is None:
            raise LocalProtocolError("ingestion source is not running")
        if running is asyncio.current_task():
            raise LocalProtocolError("ingestion runner cannot quiesce itself")
        if self._quiesce_source_commit_target is None:
            self._quiesce_source_commit_target = self._source_commit_generation + 1
            self._signal_progress()
        await asyncio.shield(running)
        if self._source_commit_generation < self._quiesce_source_commit_target:
            raise LocalProtocolError("ingestion source stopped before quiescing")

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
                await _cooperative_sleep(0)
                materialized = self._materialize_oldest_frame(
                    limits=MaterializerLimits()
                )
                if materialized.status is MaterializeStatus.MATERIALIZED:
                    continue
                if materialized.status is MaterializeStatus.BLOCKED:
                    assert materialized.frame_id is not None
                    if await self._advance_blocked_frame(materialized.frame_id):
                        continue
                    if self._journal.load_pending_hydrations(limit=1):
                        break
                    raise IngestionBlockedError(materialized.frame_id)
                if materialized.status is MaterializeStatus.AT_CAPACITY:
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
                committed = self._journal.apply_hydration_result(
                    result=hydration_result
                )
                hydration_retries = 0
                if committed is not None:
                    self._signal_durable_progress()
                continue
            hydration_retries = 0
            # fmt: on
            if await self._advance_local_membership_intent():
                continue
            if (
                self._quiesce_source_commit_target is not None
                and self._source_commit_generation >= self._quiesce_source_commit_target
            ):
                return
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
            self._source_commit_generation += 1
            if (
                self._quiesce_source_commit_target is not None
                and self._source_commit_generation >= self._quiesce_source_commit_target
            ):
                return

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


def _apply_room_lifecycle_snapshot(
    client: AsyncClient,
    record: EventRecord,
    aggregate: RoomAggregateValue,
) -> None:
    from ..client.base_client import _overlay_room_snapshot
    from ..rooms import MatrixInvitedRoom, MatrixRoom

    continuity = aggregate.continuity
    room_id = record.room_id
    membership = continuity.membership
    snapshot = aggregate.room_snapshot
    if (
        type(room_id) is not str
        or not room_id
        or continuity.room_id != room_id
        or membership not in {None, "invite", "join", "knock", "leave", "ban"}
        or type(client.user_id) is not str
        or not client.user_id
        or type(client.rooms) is not dict
        or type(client.invited_rooms) is not dict
    ):
        raise JournalIntegrityError("room lifecycle Aggregate identity is invalid")
    if snapshot is not None and (
        snapshot.room_id != room_id or snapshot.own_user_id != client.user_id
    ):
        raise JournalIntegrityError("room lifecycle snapshot identity is invalid")

    invited = membership in {"invite", "knock"}
    desired = client.invited_rooms if invited else client.rooms
    opposite = client.rooms if invited else client.invited_rooms
    desired_room = desired.get(room_id)
    opposite_room = opposite.get(room_id)
    if desired_room is not None and opposite_room is not None:
        raise LocalProtocolError("owned room projection has duplicate routing")
    desired_type = MatrixInvitedRoom if invited else MatrixRoom
    opposite_type = MatrixRoom if invited else MatrixInvitedRoom

    def validate_live_room(room: MatrixRoom, expected_type: type[MatrixRoom]) -> None:
        if type(room) is not expected_type or (
            room.room_id,
            room.own_user_id,
        ) != (room_id, client.user_id):
            raise LocalProtocolError("owned room projection identity is invalid")

    if desired_room is not None:
        validate_live_room(desired_room, desired_type)
    if opposite_room is not None:
        validate_live_room(opposite_room, opposite_type)
    if snapshot is None:
        if opposite_room is not None:
            opposite.pop(room_id)
        return

    try:
        projected = _overlay_room_snapshot(
            desired_room if desired_room is not None else opposite_room,
            snapshot,
            invited=invited,
        )
    except (AttributeError, TypeError, ValueError) as error:
        raise JournalIntegrityError("room lifecycle snapshot is invalid") from error
    if type(projected) is not desired_type or (
        projected.room_id,
        projected.own_user_id,
    ) != (room_id, client.user_id):
        raise JournalIntegrityError("room lifecycle projection identity is invalid")
    if desired_room is not None and projected is not desired_room:
        raise JournalIntegrityError("room lifecycle overlay replaced a routed room")
    if opposite_room is not None:
        opposite.pop(room_id)
    if invited:
        if type(projected) is not MatrixInvitedRoom:
            raise JournalIntegrityError(
                "room lifecycle invited projection has the wrong type"
            )
        client.invited_rooms[room_id] = projected
    else:
        if type(projected) is not MatrixRoom:
            raise JournalIntegrityError(
                "room lifecycle current projection has the wrong type"
            )
        client.rooms[room_id] = projected


def _load_settlement_event(
    payload: bytes,
    field_name: str,
    invalid_message: str,
) -> dict[str, object]:
    try:
        value = load_json(payload, field_name)
        if type(value) is not dict or canonical_json(value) != payload:
            raise ValueError
    except (TypeError, ValueError) as error:
        raise JournalIntegrityError(invalid_message) from error
    return value


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
        completion_sink: Callable[[_FrameCompletion], Awaitable[None]] | None = None,
    ) -> None:
        self._client = client
        self._config = config
        self._journal = journal
        self._source = source
        if completion_sink is not None and not callable(completion_sink):
            raise TypeError("completion sink must be callable")
        self._completion_sink = completion_sink
        self._lease = lease
        self._owned_store = store
        self._close_task: asyncio.Task[None] | None = None
        self._running: asyncio.Task[None] | None = None
        self._terminal: BaseException | None = None
        self._source_commit_generation = 0
        self._quiesce_source_commit_target: int | None = None
        self._settlement_lock = asyncio.Lock()
        self._settlement_task: asyncio.Task[object] | None = None
        self._local_membership_command: _LocalMembershipCommand | None = None
        self._progress_generation = 0
        self._durable_progress_generation = 0
        self._progress_event = asyncio.Event()
        self._detached = False
        self._revoked = False
        self._closed = False

    def _materialize_oldest_frame(
        self, *, limits: MaterializerLimits
    ) -> MaterializeResult:
        self._client._assert_ingestion_not_poisoned()
        store = self._owned_store
        preparation_started = False

        def prepare(
            frame: SyncFrame,
            staged_revision: int,
            prior_continuities: tuple[RoomContinuity, ...],
        ) -> _PreparedIngestionFrame:
            nonlocal preparation_started
            preparation_started = True
            prepared = self._client._prepare_ingestion_frame(
                frame,
                staged_revision,
                prior_continuities,
            )
            if self._client.next_batch:
                store.save_sync_token(self._client.next_batch)
            store.save_encrypted_rooms(self._client.encrypted_rooms)
            return prepared

        def freeze_outbound(
            frame: SyncFrame,
            prepared: _PreparedIngestionFrame,
        ) -> _OutboundMaintenance:
            # Import at the runtime boundary: crypto imports store, which imports
            # ingestion while the client modules are still initializing.
            from ..client.base_client import (
                _prepared_crypto_delta_snapshot,
                _prepared_frame_encrypted_room_ids,
            )

            olm = self._client.olm
            if olm is None:
                raise LocalProtocolError("owned materialization requires Olm")
            delta = prepared.crypto_delta
            encrypted_room_ids = delta.encrypted_room_ids
            source_encrypted_room_ids = _prepared_frame_encrypted_room_ids(
                frame,
                set(self._client.rooms) | set(self._client.invited_rooms),
            )
            if encrypted_room_ids != source_encrypted_room_ids:
                raise ValueError(
                    "prepared crypto delta disagrees with source encryption"
                )
            segment_room_ids = {segment.room_id for segment in frame.room_segments}
            if (
                type(encrypted_room_ids) is not tuple
                or tuple(sorted(encrypted_room_ids)) != encrypted_room_ids
                or len(encrypted_room_ids) != len(set(encrypted_room_ids))
                or any(
                    type(room_id) is not str or not room_id
                    for room_id in encrypted_room_ids
                )
                or not set(encrypted_room_ids) <= segment_room_ids
                or not set(encrypted_room_ids) <= self._client.encrypted_rooms
            ):
                raise ValueError("prepared encrypted-room IDs are invalid")
            key_counts = load_json(
                frame.one_time_key_counts_json,
                "one-time-key counts",
            )
            if (
                type(key_counts) is not dict
                or canonical_json(key_counts) != frame.one_time_key_counts_json
            ):
                raise ValueError("one-time-key counts are invalid")
            live_uploaded_count = olm.uploaded_key_count
            prepared_uploaded_count = delta.uploaded_key_count
            if (
                live_uploaded_count is not None
                and (type(live_uploaded_count) is not int or live_uploaded_count < 0)
                or prepared_uploaded_count is not None
                and (
                    type(prepared_uploaded_count) is not int
                    or prepared_uploaded_count < 0
                )
                or live_uploaded_count != prepared_uploaded_count
            ):
                raise ValueError("prepared uploaded key count is invalid")
            signed_curve_count = key_counts.get("signed_curve25519")
            if signed_curve_count is not None and (
                type(signed_curve_count) is not int
                or signed_curve_count < 0
                or live_uploaded_count != signed_curve_count
                or prepared_uploaded_count != signed_curve_count
            ):
                raise ValueError("prepared uploaded key count disagrees with source")
            recaptured = _prepared_crypto_delta_snapshot(
                frame,
                olm,
                encrypted_room_ids,
            )
            _validate_prepared_crypto_delta(recaptured)
            if recaptured != delta:
                raise ValueError("prepared crypto delta disagrees with live state")

            upload_body: bytes | None = None
            if olm.should_upload_keys:
                upload_body = canonical_json(olm.share_keys())
                olm.save_account()
            return _initial_outbound_maintenance(
                frame_id=prepared.frame_id,
                delta=delta,
                upload_body=upload_body,
            )

        try:
            result = self._journal._prepare_and_materialize_oldest_frame(
                prepare=prepare,
                freeze_outbound=freeze_outbound,
                limits=limits,
            )
            if result.status is MaterializeStatus.MATERIALIZED:
                self._signal_durable_progress()
            return result
        except BaseException:
            if preparation_started:
                self._client._poison_ingestion()
            raise

    async def _advance_blocked_frame(self, frame_id: UUID) -> bool:
        progress_generation = self._progress_generation
        pending = self._journal._load_ready_outbound_maintenance()
        if pending is None:
            if self._journal.load_pending_hydrations(limit=1):
                return False
            if self._journal._oldest_prepared_frame_has_work(frame_id):
                await self._wait_for_progress(progress_generation)
                return True
            completion = self._journal._claim_frame_completion()
            if completion is None:
                claimed = self._journal._load_claimed_frame_completion()
                if claimed is None:
                    return False
                if claimed.frame_id != frame_id:
                    raise JournalIntegrityError(
                        "claimed completion does not own the blocked Frame"
                    )
                self._journal._retire_claimed_frame(claimed)
                return True
            if completion.frame_id != frame_id:
                raise JournalIntegrityError(
                    "completion claim does not own the blocked Frame"
                )
            if self._completion_sink is not None:
                await self._completion_sink(completion)
            self._journal._retire_claimed_frame(completion)
            return True
        if pending.frame_id != frame_id:
            raise JournalIntegrityError(
                "outbound maintenance does not own the blocked Frame"
            )
        operation = pending.operation
        to_device_message: ToDeviceMessage | None = None
        to_device_rerequests: tuple[MegolmEvent, ...] = ()
        if operation.kind == "key_upload":
            method = "POST"
            path = "/_matrix/client/v3/keys/upload"
        elif operation.kind == "key_query":
            method = "POST"
            path = "/_matrix/client/v3/keys/query"
        elif operation.kind == "key_claim":
            method = "POST"
            path = "/_matrix/client/v3/keys/claim"
        elif operation.kind == "to_device":
            method = "PUT"
            to_device_message, to_device_rerequests = _to_device_message(operation)
            assert operation.event_type is not None
            assert operation.transaction_id is not None
            path = (
                "/_matrix/client/v3/sendToDevice/"
                f"{quote(operation.event_type, safe='')}/"
                f"{quote(operation.transaction_id, safe='')}"
            )
        else:
            return False
        request = ports.NetworkRequest(
            pending.stream_id,
            pending.transport,
            pending.source_epoch,
            pending.request_id,
            method,
            path,
            (),
            operation.body_json,
            self._config.source.timeout_ms,
            pending.request_cursor_json,
        )
        retries = 0
        while True:
            network = await self._request(
                request,
                body_disconnect_retry=True,
            )
            if network.status_code == 200:
                break
            error_code: str | None = None
            try:
                error_value = load_json(
                    network.body,
                    "outbound maintenance error",
                )
                if (
                    type(error_value) is dict
                    and canonical_json(error_value) == network.body
                    and type(error_value.get("errcode")) is str
                ):
                    error_code = error_value["errcode"]
            except (TypeError, ValueError) as _error:
                pass
            if error_code is None and network.status_code == 401:
                error_code = "M_UNKNOWN_TOKEN"
            elif error_code is None and network.status_code == 403:
                error_code = "M_FORBIDDEN"
            if error_code in {"M_UNKNOWN_TOKEN", "M_FORBIDDEN"}:
                raise IngestionSourceError(
                    "outbound maintenance authentication failed: " f"{error_code}"
                )
            retryable = (
                network.failure is not None
                or network.status_code in {408, 429}
                or network.status_code is not None
                and 500 <= network.status_code <= 599
                or error_code == "M_LIMIT_EXCEEDED"
            )
            if not retryable:
                detail = error_code or str(network.status_code)
                raise IngestionSourceError(
                    f"outbound maintenance request failed: {detail}"
                )
            retries += 1
            delay = (
                network.retry_after_ms / 1000
                if network.retry_after_ms is not None
                else 0.0
            )
            if network.retry_after_ms is None and retries >= 2:
                delay = min(
                    self._client.config.backoff_factor
                    * (2 ** (min(retries, 1000) - 1)),
                    self._client.config.max_timeout_retry_wait_time,
                )
            await asyncio.sleep(delay)
            self._client._assert_ingestion_not_poisoned()
            if self._close_task is not None or self._closed:
                raise LocalProtocolError("ingestion session is closed")
            current = self._journal._load_ready_outbound_maintenance()
            if current != pending:
                raise JournalConflictError(
                    "outbound maintenance operation changed during retry"
                )
        try:
            value = load_json(network.body, "outbound maintenance response")
            if type(value) is not dict:
                raise ValueError
            if operation.kind == "key_upload":
                response = KeysUploadResponse.from_dict(value)
                expected_type = KeysUploadResponse
            elif operation.kind == "key_query":
                response = KeysQueryResponse.from_dict(value)
                expected_type = KeysQueryResponse
            elif operation.kind == "key_claim":
                response = KeysClaimResponse.from_dict(value)
                expected_type = KeysClaimResponse
            else:
                assert to_device_message is not None
                response = ToDeviceResponse.from_dict(value, to_device_message)
                expected_type = ToDeviceResponse
            if type(response) is not expected_type:
                raise ValueError
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise IngestionSourceError(
                "malformed outbound maintenance result"
            ) from error
        olm = self._client.olm
        if olm is None:
            raise LocalProtocolError("owned maintenance requires Olm")
        apply_started = False

        def apply() -> tuple[_OutboundOperation, ...]:
            nonlocal apply_started
            apply_started = True
            claim_targets: list[tuple[str, str]] = []
            claim_was_waiting: set[tuple[str, str]] = set()
            claim_rerequest_contexts: dict[tuple[str, str], list[dict[str, object]]] = (
                {}
            )
            if operation.kind == "key_claim":
                context = operation.context
                if type(context) is not dict or set(context) != {"claims"}:
                    raise JournalIntegrityError("key-claim context is invalid")
                claims = context["claims"]
                if type(claims) is not list or not claims:
                    raise JournalIntegrityError("key-claim context is unsupported")
                for claim in claims:
                    if (
                        type(claim) is not dict
                        or set(claim)
                        != {
                            "user_id",
                            "device_id",
                            "was_wedged",
                            "was_waiting",
                            "waiting_key_requests",
                            "rerequest_events",
                        }
                        or type(claim["was_wedged"]) is not bool
                        or type(claim["was_waiting"]) is not bool
                        or claim["was_wedged"] == claim["was_waiting"]
                        or type(claim["waiting_key_requests"]) is not list
                        or len(claim["waiting_key_requests"]) > 1
                        or bool(claim["waiting_key_requests"]) != claim["was_waiting"]
                        or type(claim["rerequest_events"]) is not list
                        or len(claim["rerequest_events"]) > 1
                        or (
                            bool(claim["rerequest_events"])
                            and claim["was_wedged"] is not True
                        )
                        or type(claim["user_id"]) is not str
                        or type(claim["device_id"]) is not str
                    ):
                        raise JournalIntegrityError("key-claim context is unsupported")
                    target = (claim["user_id"], claim["device_id"])
                    if target in claim_targets:
                        raise JournalIntegrityError("key-claim context is unsupported")
                    claim_targets.append(target)
                    if claim["was_waiting"]:
                        claim_was_waiting.add(target)
                    try:
                        device = olm.device_store[target[0]][target[1]]
                    except KeyError as error:
                        raise JournalIntegrityError(
                            "key-claim device is unavailable"
                        ) from error
                    reconstructed_waiting = tuple(
                        _key_claim_waiting_request(
                            value,
                            user_id=target[0],
                            device_id=target[1],
                        )
                        for value in claim["waiting_key_requests"]
                    )
                    reconstructed_rerequests = tuple(
                        _key_claim_rerequest_event(
                            value,
                            user_id=target[0],
                            device_id=target[1],
                        )
                        for value in claim["rerequest_events"]
                    )
                    claim_rerequest_contexts[target] = [
                        dict(value) for value in claim["rerequest_events"]
                    ]
                    if reconstructed_rerequests:
                        live_rerequests = olm.key_re_requests_events[target]
                        if live_rerequests:
                            if len(live_rerequests) != len(
                                reconstructed_rerequests
                            ) or any(
                                not _same_key_claim_rerequest(live, expected)
                                for live, expected in zip(
                                    live_rerequests,
                                    reconstructed_rerequests,
                                    strict=True,
                                )
                            ):
                                raise JournalIntegrityError(
                                    "key-claim rerequest state changed"
                                )
                        else:
                            live_rerequests.extend(reconstructed_rerequests)
                    if target in claim_was_waiting:
                        if device not in olm.key_request_devices_no_session:
                            olm.key_request_devices_no_session.append(device)
                        live_waiting = olm.key_requests_waiting_for_session[target]
                        if live_waiting:
                            if tuple(live_waiting) != tuple(
                                event.request_id for event in reconstructed_waiting
                            ) or any(
                                not _same_key_claim_waiting(
                                    live_waiting[event.request_id],
                                    event,
                                )
                                for event in reconstructed_waiting
                            ):
                                raise JournalIntegrityError(
                                    "waiting key-request state changed"
                                )
                        else:
                            live_waiting.update(
                                (event.request_id, event)
                                for event in reconstructed_waiting
                            )
                    elif device not in olm.wedged_devices:
                        olm.wedged_devices.append(device)
                queued_before = tuple(olm.outgoing_to_device_messages)
            elif operation.kind == "to_device":
                assert to_device_message is not None
                live_messages = olm.outgoing_to_device_messages
                if live_messages:
                    if live_messages != [to_device_message]:
                        raise JournalIntegrityError("to-device live queue changed")
                elif type(to_device_message) in {DummyMessage, RoomKeyRequestMessage}:
                    live_messages.append(to_device_message)
                if type(to_device_message) is DummyMessage:
                    target = (
                        to_device_message.recipient,
                        to_device_message.recipient_device,
                    )
                    live_rerequests = olm.key_re_requests_events[target]
                    if live_rerequests:
                        if len(live_rerequests) != len(to_device_rerequests) or any(
                            not _same_key_claim_rerequest(live, expected)
                            for live, expected in zip(
                                live_rerequests,
                                to_device_rerequests,
                                strict=True,
                            )
                        ):
                            raise JournalIntegrityError("dummy rerequest state changed")
                    else:
                        live_rerequests.extend(to_device_rerequests)
                queued_before = tuple(live_messages)
            else:
                queued_before = ()
            olm.handle_response(response)
            if operation.kind == "to_device":
                assert to_device_message is not None
                if type(to_device_message) is not DummyMessage:
                    if olm.outgoing_to_device_messages:
                        raise JournalIntegrityError(
                            "generic to-device queue was not consumed"
                        )
                    return ()
                generated = tuple(olm.outgoing_to_device_messages)
                if len(generated) != len(to_device_rerequests) or any(
                    type(message) is not RoomKeyRequestMessage for message in generated
                ):
                    raise JournalIntegrityError(
                        "dummy to-device follow-ups are invalid"
                    )
                dummy_follow_ups: list[_OutboundOperation] = []
                for offset, message in enumerate(generated):
                    assert type(message) is RoomKeyRequestMessage
                    body_json = canonical_json(message.as_dict())
                    operation_index = pending.operation_count + offset
                    dummy_follow_ups.append(
                        _OutboundOperation(
                            "to_device",
                            "pending",
                            body_json,
                            str(
                                uuid5(
                                    pending.frame_id,
                                    "nio.ingest.outbound-maintenance.v1:"
                                    f"to-device:{operation_index}:"
                                    f"{sha256(body_json).hexdigest()}",
                                )
                            ),
                            message.type,
                            {
                                "subtype": "room_key_request",
                                "request_id": message.request_id,
                                "session_id": message.session_id,
                                "room_id": message.room_id,
                                "algorithm": message.algorithm,
                            },
                        )
                    )
                return tuple(dummy_follow_ups)
            if operation.kind != "key_claim":
                return ()
            if claim_was_waiting and olm.collect_key_requests() != []:
                raise JournalIntegrityError(
                    "waiting key claim generated unsupported callback Work"
                )
            queued_after = tuple(olm.outgoing_to_device_messages)
            if queued_after[: len(queued_before)] != queued_before:
                raise JournalIntegrityError("key-claim changed the queued prefix")
            follow_ups: list[_OutboundOperation] = []
            generated = queued_after[len(queued_before) :]
            generated_targets: set[tuple[str, str]] = set()
            for offset, message in enumerate(generated):
                target = (message.recipient, message.recipient_device)
                if target not in claim_targets or target in generated_targets:
                    raise JournalIntegrityError(
                        "key-claim generated a follow-up for another target"
                    )
                generated_targets.add(target)
                if type(message) is DummyMessage:
                    if target in claim_was_waiting:
                        raise JournalIntegrityError(
                            "waiting key claim generated a dummy"
                        )
                    follow_up_context: dict[str, object] = {
                        "subtype": "dummy",
                        "rerequest_events": claim_rerequest_contexts[target],
                    }
                elif type(message) is ToDeviceMessage:
                    if target not in claim_was_waiting:
                        raise JournalIntegrityError(
                            "wedged key claim generated a share"
                        )
                    follow_up_context = {"subtype": "generic"}
                else:
                    raise JournalIntegrityError(
                        "key-claim generated an unsupported follow-up"
                    )
                body_json = canonical_json(message.as_dict())
                operation_index = pending.operation_count + offset
                follow_ups.append(
                    _OutboundOperation(
                        "to_device",
                        "pending",
                        body_json,
                        str(
                            uuid5(
                                pending.frame_id,
                                "nio.ingest.outbound-maintenance.v1:"
                                f"to-device:{operation_index}:"
                                f"{sha256(body_json).hexdigest()}",
                            )
                        ),
                        message.type,
                        follow_up_context,
                    )
                )
            if not claim_was_waiting.issubset(generated_targets):
                raise JournalIntegrityError(
                    "waiting key claim did not generate one share"
                )
            return tuple(follow_ups)

        try:
            self._journal._settle_outbound_maintenance(
                pending=pending,
                apply=apply,
            )
        except BaseException:
            if apply_started:
                self._client._poison_ingestion()
            raise
        self._signal_durable_progress()
        return True

    async def _advance_local_membership_intent(self) -> bool:
        command = self._local_membership_command
        progress_generation = self._progress_generation
        pending = self._journal._load_pending_local_membership_intents(limit=2)
        if not pending:
            if command is None:
                has_local_work = self._journal._has_pending_local_membership_work()
            else:
                try:
                    has_local_work, matching_revision = (
                        self._journal._local_membership_work_state(
                            room_id=command.room_id,
                            intent=command.intent,
                        )
                    )
                except BaseException as error:
                    if not command.completion.done():
                        command.completion.set_exception(error)
                    self._local_membership_command = None
                    raise
                if matching_revision is not None:
                    if not command.completion.done():
                        command.completion.set_result(True)
                    self._local_membership_command = None
                    command = None
            if has_local_work:
                await self._wait_for_progress(progress_generation)
                return True
            if command is None:
                return False
            intent = command.intent
            try:
                self._journal._record_local_membership_intent(
                    operation_id=intent.operation_id,
                    room_id=command.room_id,
                    previous_membership=intent.previous_membership,
                    previous_epoch=intent.previous_epoch,
                    current_membership=intent.current_membership,
                )
            except BaseException as error:
                if not command.completion.done():
                    command.completion.set_exception(error)
                self._local_membership_command = None
                raise
            pending = self._journal._load_pending_local_membership_intents(limit=2)
            if not pending:
                if not command.completion.done():
                    command.completion.set_result(True)
                self._local_membership_command = None
                self._signal_durable_progress()
                return True
        if len(pending) != 1:
            raise JournalIntegrityError("multiple local membership intents are pending")
        candidate = pending[0]
        if command is not None and (command.room_id, command.intent) != (
            candidate.room_id,
            candidate.intent,
        ):
            raise JournalConflictError("queued local membership command changed")
        access_token = self._client.access_token
        if type(access_token) is not str or not access_token:
            raise LocalProtocolError("local membership intent requires an access token")
        if candidate.intent.current_membership == "join":
            method = "POST"
            path = f"/_matrix/client/v3/join/{quote(candidate.room_id, safe='')}"
        else:
            assert candidate.intent.current_membership == "leave"
            method = "POST"
            path = (
                "/_matrix/client/v3/rooms/" f"{quote(candidate.room_id, safe='')}/leave"
            )
        owner = self._journal.load_owner()
        source = self._journal.load_source()
        request = ports.NetworkRequest(
            owner.stream_id,
            owner.transport_kind,
            source.source_epoch,
            source.next_request_id,
            method,
            path,
            (),
            None,
            self._config.source.timeout_ms,
            source.cursor_json,
        )

        def fail_command(error: BaseException) -> None:
            nonlocal command
            if command is not None:
                if not command.completion.done():
                    command.completion.set_exception(error)
                self._local_membership_command = None
                command = None

        retries = 0
        while True:
            network = await self._request(request, body_disconnect_retry=True)
            if network.status_code == 200:
                break
            error_code: str | None = None
            try:
                error_value = load_json(
                    network.body,
                    "local membership error",
                )
                if (
                    type(error_value) is dict
                    and canonical_json(error_value) == network.body
                    and type(error_value.get("errcode")) is str
                ):
                    error_code = error_value["errcode"]
            except (TypeError, ValueError) as _error:
                pass
            if error_code is None and network.status_code == 401:
                error_code = "M_UNKNOWN_TOKEN"
            elif error_code is None and network.status_code == 403:
                error_code = "M_FORBIDDEN"
            if error_code in {"M_UNKNOWN_TOKEN", "M_FORBIDDEN"}:
                failure = IngestionSourceError(
                    "local membership authentication failed: " f"{error_code}"
                )
                fail_command(failure)
                raise failure
            retryable = (
                network.failure is not None
                or network.status_code in {408, 429}
                or network.status_code is not None
                and 500 <= network.status_code <= 599
                or error_code == "M_LIMIT_EXCEEDED"
            )
            if not retryable:
                try:
                    self._journal._clear_local_membership_intent(
                        room_id=candidate.room_id,
                        intent=candidate.intent,
                    )
                except BaseException as error:
                    fail_command(error)
                    raise
                if command is not None:
                    if not command.completion.done():
                        command.completion.set_result(False)
                    self._local_membership_command = None
                    command = None
                self._signal_durable_progress()
                return True
            retries += 1
            delay = (
                network.retry_after_ms / 1000
                if network.retry_after_ms is not None
                else 0.0
            )
            if network.retry_after_ms is None and retries >= 2:
                delay = min(
                    self._client.config.backoff_factor
                    * (2 ** (min(retries, 1000) - 1)),
                    self._client.config.max_timeout_retry_wait_time,
                )
            await asyncio.sleep(delay)
            self._client._assert_ingestion_not_poisoned()
            if self._close_task is not None or self._closed:
                raise LocalProtocolError("ingestion session is closed")
            current = self._journal._load_pending_local_membership_intents(limit=2)
            if current != (candidate,):
                conflict = JournalConflictError(
                    "local membership intent changed during retry"
                )
                fail_command(conflict)
                raise conflict
        try:
            value = load_json(network.body, "local membership response")
            if type(value) is not dict or canonical_json(value) != network.body:
                raise ValueError
            if candidate.intent.current_membership == "join":
                response = JoinResponse.from_dict(value)
                if type(response) is not JoinResponse or response.room_id != (
                    candidate.room_id
                ):
                    raise ValueError
            else:
                response = RoomLeaveResponse.from_dict(value)
                if type(response) is not RoomLeaveResponse:
                    raise ValueError
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            failure = IngestionSourceError("malformed local membership response")
            fail_command(failure)
            raise failure from error
        intent = candidate.intent
        try:
            self._journal._publish_local_membership_transition(
                operation_id=intent.operation_id,
                room_id=candidate.room_id,
                previous_membership=intent.previous_membership,
                previous_epoch=intent.previous_epoch,
                current_membership=intent.current_membership,
            )
        except BaseException as error:
            fail_command(error)
            raise
        if command is not None:
            if not command.completion.done():
                command.completion.set_result(True)
            self._local_membership_command = None
        self._signal_durable_progress()
        return True

    def _signal_durable_progress(self) -> None:
        self._durable_progress_generation += 1
        self._signal_progress()

    def _signal_progress(self) -> None:
        self._progress_generation += 1
        self._progress_event.set()

    async def _wait_for_work(self) -> None:
        """Wait without polling until one delivery may be available."""
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None or self._closed:
            raise LocalProtocolError("ingestion session is closed")
        observed_generation = self._progress_generation
        if self.next_batch(max_records=1) is not None:
            return
        await self._wait_for_progress(observed_generation)
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None or self._closed:
            raise LocalProtocolError("ingestion session is closed")

    async def _wait_for_local_membership_idle(self) -> None:
        """Wait without polling until no local intent or lifecycle Work remains."""
        while True:
            self._client._assert_ingestion_not_poisoned()
            if self._close_task is not None or self._closed:
                raise LocalProtocolError("ingestion session is closed")
            observed_generation = self._progress_generation
            if (
                not self._journal._load_pending_local_membership_intents(limit=1)
                and not self._journal._has_pending_local_membership_work()
            ):
                return
            await self._wait_for_progress(observed_generation)

    async def _wait_for_progress(self, observed_generation: int) -> None:
        if self._progress_generation != observed_generation:
            return
        self._progress_event.clear()
        if self._progress_generation != observed_generation:
            return
        await self._progress_event.wait()

    async def _settle_batch(
        self,
        batch: SyncBatch,
        *,
        receipt_new: bool,
        semantic_event_new: bool,
    ) -> None:
        if type(receipt_new) is not bool or type(semantic_event_new) is not bool:
            raise LocalProtocolError("settlement facts must be booleans")
        if semantic_event_new and not receipt_new:
            raise LocalProtocolError("a new semantic event requires a new receipt")
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None or self._closed:
            raise LocalProtocolError("ingestion session is closed")
        current = asyncio.current_task()
        assert current is not None
        if self._settlement_task is current:
            raise LocalProtocolError("settlement cannot recursively settle")

        async with self._settlement_lock:
            self._client._assert_ingestion_not_poisoned()
            if self._close_task is not None or self._closed:
                raise LocalProtocolError("ingestion session is closed")
            assert self._settlement_task is None
            self._settlement_task = current
            try:
                settlement = self._journal._load_batch_settlement(batch)
                if settlement is None:
                    self._journal.acknowledge_batch(batch.ref)
                    self._signal_durable_progress()
                    return

                work, room = settlement
                record = work.value
                metadata = work.metadata
                terminal = False
                if type(record) is LossRecord:
                    if metadata is not None or room is not None:
                        raise LocalProtocolError(
                            "unsupported durable settlement record"
                        )
                    terminal = True
                elif (
                    type(record) is EventRecord
                    and record.kind is RecordKind.ROOM_LIFECYCLE
                ):
                    if metadata is not None or type(room) is not RoomAggregateValue:
                        raise LocalProtocolError(
                            "unsupported durable settlement record"
                        )
                    _apply_room_lifecycle_snapshot(self._client, record, room)
                    terminal = True
                elif (
                    type(record) is EventRecord
                    and record.kind is RecordKind.STATE
                    and type(room) is RoomAggregateValue
                    and metadata is not None
                    and metadata.preparation_phase is _PreparationPhase.SOURCE
                    and metadata.decryption is _DecryptionDisposition.NONE
                    and metadata.decryption_verified is None
                    and metadata.decrypted_to_device_kind is None
                    and metadata.callback_route in (None, _CallbackRoute.EVENT)
                ):
                    invite_callback = metadata.callback_route is _CallbackRoute.EVENT
                    if invite_callback and room.continuity.membership != "invite":
                        raise LocalProtocolError(
                            "unsupported durable settlement record"
                        )
                    _apply_room_lifecycle_snapshot(self._client, record, room)
                    if invite_callback:
                        from ..rooms import MatrixInvitedRoom

                        room_id = record.room_id
                        if type(room_id) is not str:
                            raise JournalIntegrityError("invite STATE event is invalid")
                        projected_room = self._client.invited_rooms.get(room_id)
                        if type(projected_room) is not MatrixInvitedRoom:
                            raise JournalIntegrityError(
                                "invite STATE Aggregate has no invited projection"
                            )
                        source = _load_settlement_event(
                            record.source_json,
                            "invite STATE event",
                            "invite STATE event source is invalid",
                        )
                        if source.get("type") != metadata.effective_event_type:
                            raise JournalIntegrityError(
                                "invite STATE event source is invalid"
                            )
                        event = InviteEvent.parse_event(source)
                        if event is None:
                            raise JournalIntegrityError(
                                "invite STATE event source is invalid"
                            )
                        if isinstance(event, InviteEvent):
                            projected_room.handle_event(event)  # type: ignore[arg-type]
                        _apply_room_lifecycle_snapshot(self._client, record, room)
                        if (
                            self._client.invited_rooms.get(room_id)
                            is not projected_room
                        ):
                            raise JournalIntegrityError(
                                "invite STATE overlay replaced the invited projection"
                            )
                        if receipt_new:
                            await self._client._on_invited_rooms(  # type: ignore[arg-type]
                                event,
                                projected_room,
                            )
                    terminal = True
                elif (
                    type(record) is EventRecord
                    and record.kind is RecordKind.EPHEMERAL
                    and type(room) is RoomAggregateValue
                    and metadata is not None
                    and metadata.preparation_phase is _PreparationPhase.SOURCE
                    and metadata.decryption is _DecryptionDisposition.NONE
                    and metadata.decryption_verified is None
                    and metadata.decrypted_to_device_kind is None
                    and metadata.callback_route is _CallbackRoute.EPHEMERAL
                ):
                    _apply_room_lifecycle_snapshot(self._client, record, room)
                    room_id = record.room_id
                    if type(room_id) is not str:
                        raise JournalIntegrityError("ephemeral event is invalid")
                    projected_rooms = (
                        self._client.invited_rooms
                        if room.continuity.membership in {"invite", "knock"}
                        else self._client.rooms
                    )
                    projected_room = projected_rooms.get(room_id)
                    if projected_room is None:
                        raise JournalIntegrityError(
                            "ephemeral Aggregate has no room projection"
                        )
                    source = _load_settlement_event(
                        record.source_json,
                        "ephemeral event",
                        "ephemeral event source is invalid",
                    )
                    if source.get("type") != metadata.effective_event_type:
                        raise JournalIntegrityError("ephemeral event source is invalid")
                    event = EphemeralEvent.parse_event(source)
                    if event is None:
                        raise JournalIntegrityError("ephemeral event source is invalid")
                    projected_room.handle_ephemeral_event(event)
                    if receipt_new:
                        await self._client._on_ephemeral(event, projected_room)
                    terminal = True
                elif (
                    type(record) is EventRecord
                    and record.kind is RecordKind.ROOM_ACCOUNT_DATA
                    and type(room) is RoomAggregateValue
                    and metadata is not None
                    and metadata.preparation_phase is _PreparationPhase.SOURCE
                    and metadata.decryption is _DecryptionDisposition.NONE
                    and metadata.decryption_verified is None
                    and metadata.decrypted_to_device_kind is None
                    and metadata.callback_route is _CallbackRoute.ROOM_ACCOUNT_DATA
                ):
                    _apply_room_lifecycle_snapshot(self._client, record, room)
                    room_id = record.room_id
                    if type(room_id) is not str:
                        raise JournalIntegrityError(
                            "room account-data event is invalid"
                        )
                    projected_rooms = (
                        self._client.invited_rooms
                        if room.continuity.membership in {"invite", "knock"}
                        else self._client.rooms
                    )
                    projected_room = projected_rooms.get(room_id)
                    if projected_room is None:
                        raise JournalIntegrityError(
                            "room account-data Aggregate has no room projection"
                        )
                    source = _load_settlement_event(
                        record.source_json,
                        "room account-data event",
                        "room account-data source is invalid",
                    )
                    if source.get("type") != metadata.effective_event_type:
                        raise JournalIntegrityError(
                            "room account-data source is invalid"
                        )
                    event = AccountDataEvent.parse_event(source)
                    projected_room.handle_account_data(event)
                    if receipt_new:
                        await self._client._on_room_account_data(
                            event,
                            projected_room,
                        )
                    terminal = True
                elif (
                    type(record) is EventRecord
                    and record.kind is RecordKind.PRESENCE
                    and room is None
                    and metadata is not None
                    and metadata.preparation_phase is _PreparationPhase.SOURCE
                    and metadata.decryption is _DecryptionDisposition.NONE
                    and metadata.decryption_verified is None
                    and metadata.decrypted_to_device_kind is None
                    and metadata.callback_route is _CallbackRoute.PRESENCE
                ):
                    source = _load_settlement_event(
                        record.source_json,
                        "presence event",
                        "presence event source is invalid",
                    )
                    if source.get("type") != metadata.effective_event_type:
                        raise JournalIntegrityError("presence event source is invalid")
                    event = PresenceEvent.from_dict(source)
                    if type(event) is not PresenceEvent:
                        raise JournalIntegrityError("presence event source is invalid")
                    for projected_room in self._client.rooms.values():
                        user = projected_room.users.get(event.user_id)
                        if user is None:
                            continue
                        user.presence = event.presence
                        user.last_active_ago = event.last_active_ago
                        user.currently_active = event.currently_active
                        user.status_msg = event.status_msg
                    if receipt_new:
                        await self._client._on_presence(event)
                    terminal = True
                elif (
                    type(record) is EventRecord
                    and record.kind is RecordKind.TIMELINE
                    and type(room) is RoomAggregateValue
                    and metadata is not None
                    and metadata.preparation_phase is _PreparationPhase.SOURCE
                    and metadata.decryption is _DecryptionDisposition.NONE
                    and metadata.decryption_verified is None
                    and metadata.decrypted_to_device_kind is None
                    and metadata.callback_route is _CallbackRoute.EVENT
                ):
                    _apply_room_lifecycle_snapshot(self._client, record, room)
                    if semantic_event_new:
                        room_id = record.room_id
                        assert room_id is not None
                        projected_rooms = (
                            self._client.invited_rooms
                            if room.continuity.membership in {"invite", "knock"}
                            else self._client.rooms
                        )
                        projected_room = projected_rooms.get(room_id)
                        if projected_room is None:
                            raise JournalIntegrityError(
                                "timeline Aggregate has no room projection"
                            )
                        source = load_json(record.source_json, "timeline event")
                        if (
                            type(source) is not dict
                            or canonical_json(source) != record.source_json
                        ):
                            raise JournalIntegrityError(
                                "timeline event source is invalid"
                            )
                        event = Event.parse_event(source)
                        await self._client._on_event(event, projected_room)
                    terminal = True
                elif (
                    type(record) is EventRecord
                    and record.kind is RecordKind.TIMELINE
                    and type(room) is RoomAggregateValue
                    and metadata is not None
                    and metadata.preparation_phase is _PreparationPhase.SOURCE
                    and metadata.decryption is _DecryptionDisposition.DECRYPTED
                    and type(metadata.decryption_verified) is bool
                    and metadata.decrypted_to_device_kind is None
                    and metadata.callback_route is _CallbackRoute.EVENT
                ):
                    _apply_room_lifecycle_snapshot(self._client, record, room)
                    if semantic_event_new:
                        room_id = record.room_id
                        if type(room_id) is not str or record.clear_json is None:
                            raise JournalIntegrityError(
                                "decrypted timeline event is invalid"
                            )
                        projected_rooms = (
                            self._client.invited_rooms
                            if room.continuity.membership in {"invite", "knock"}
                            else self._client.rooms
                        )
                        projected_room = projected_rooms.get(room_id)
                        if projected_room is None:
                            raise JournalIntegrityError(
                                "timeline Aggregate has no room projection"
                            )
                        source = _load_settlement_event(
                            record.source_json,
                            "encrypted timeline event",
                            "encrypted timeline event source is invalid",
                        )
                        clear = _load_settlement_event(
                            record.clear_json,
                            "decrypted timeline event",
                            "decrypted timeline event is invalid",
                        )
                        source_content = source.get("content")
                        if (
                            source.get("type") != "m.room.encrypted"
                            or type(source_content) is not dict
                            or source_content.get("algorithm") != "m.megolm.v1.aes-sha2"
                            or type(source_content.get("sender_key")) is not str
                            or not source_content["sender_key"]
                            or type(source_content.get("session_id")) is not str
                            or not source_content["session_id"]
                            or clear.get("type") != metadata.effective_event_type
                            or clear.get("event_id") != record.event_id
                            or clear.get("room_id") != room_id
                            or clear.get("sender") != source.get("sender")
                        ):
                            raise JournalIntegrityError(
                                "decrypted timeline event is invalid"
                            )
                        event = Event.parse_decrypted_event(clear)
                        if not isinstance(event, UnknownBadEvent):
                            event.decrypted = True
                            event.verified = metadata.decryption_verified
                            event.sender_key = source_content["sender_key"]
                            event.session_id = source_content["session_id"]
                            setattr(event, "room_id", room_id)
                        await self._client._on_event(event, projected_room)
                    terminal = True
                elif (
                    type(record) is EventRecord
                    and record.kind is RecordKind.TIMELINE
                    and type(room) is RoomAggregateValue
                    and metadata is not None
                    and metadata.preparation_phase is _PreparationPhase.SOURCE
                    and metadata.decryption is _DecryptionDisposition.MEGOLM_FAILED
                    and metadata.decryption_verified is None
                    and metadata.decrypted_to_device_kind is None
                    and metadata.callback_route is _CallbackRoute.EVENT
                ):
                    _apply_room_lifecycle_snapshot(self._client, record, room)
                    if semantic_event_new:
                        room_id = record.room_id
                        if type(room_id) is not str:
                            raise JournalIntegrityError(
                                "failed Megolm event is invalid"
                            )
                        projected_rooms = (
                            self._client.invited_rooms
                            if room.continuity.membership in {"invite", "knock"}
                            else self._client.rooms
                        )
                        projected_room = projected_rooms.get(room_id)
                        if projected_room is None:
                            raise JournalIntegrityError(
                                "timeline Aggregate has no room projection"
                            )
                        source = _load_settlement_event(
                            record.source_json,
                            "failed Megolm event",
                            "failed Megolm event source is invalid",
                        )
                        source_content = source.get("content")
                        if (
                            source.get("type") != "m.room.encrypted"
                            or type(source_content) is not dict
                            or source_content.get("algorithm") != "m.megolm.v1.aes-sha2"
                            or source.get("event_id") != record.event_id
                        ):
                            raise JournalIntegrityError(
                                "failed Megolm event source is invalid"
                            )
                        event = Event.parse_event(source)
                        if type(event) is not MegolmEvent:
                            raise JournalIntegrityError(
                                "failed Megolm event source is invalid"
                            )
                        event.room_id = room_id
                        await self._client._on_event(event, projected_room)
                    terminal = True

                if not terminal:
                    if (
                        type(record) is not EventRecord
                        or metadata is None
                        or room is not None
                    ):
                        raise LocalProtocolError(
                            "unsupported durable settlement record"
                        )
                    global_account_data = (
                        record.kind is RecordKind.GLOBAL_ACCOUNT_DATA
                        and metadata.callback_route
                        is _CallbackRoute.GLOBAL_ACCOUNT_DATA
                    )
                    source_to_device = (
                        record.kind is RecordKind.TO_DEVICE
                        and metadata.preparation_phase is _PreparationPhase.SOURCE
                        and metadata.decryption is _DecryptionDisposition.NONE
                        and metadata.decryption_verified is None
                        and metadata.decrypted_to_device_kind is None
                        and metadata.callback_route is _CallbackRoute.TO_DEVICE
                    )
                    source_to_device_ack_only = (
                        record.kind is RecordKind.TO_DEVICE
                        and metadata.preparation_phase is _PreparationPhase.SOURCE
                        and metadata.decryption is _DecryptionDisposition.NONE
                        and metadata.decryption_verified is None
                        and metadata.decrypted_to_device_kind is None
                        and metadata.callback_route is None
                    )
                    decrypted_room_key = (
                        record.kind is RecordKind.TO_DEVICE
                        and metadata.preparation_phase is _PreparationPhase.SOURCE
                        and metadata.decryption is _DecryptionDisposition.DECRYPTED
                        and metadata.decryption_verified is None
                        and metadata.decrypted_to_device_kind
                        is _DecryptedToDeviceKind.ROOM_KEY
                        and metadata.callback_route is _CallbackRoute.TO_DEVICE
                    )
                    expired_verification = (
                        record.kind is RecordKind.TO_DEVICE
                        and metadata.preparation_phase
                        is _PreparationPhase.EXPIRED_VERIFICATION
                        and metadata.effective_event_type == "m.key.verification.cancel"
                        and metadata.decryption is _DecryptionDisposition.NONE
                        and metadata.decryption_verified is None
                        and metadata.decrypted_to_device_kind is None
                        and metadata.callback_route is _CallbackRoute.TO_DEVICE
                    )
                    collected_key_request = (
                        record.kind is RecordKind.TO_DEVICE
                        and metadata.preparation_phase
                        is _PreparationPhase.COLLECTED_KEY_REQUEST
                        and metadata.effective_event_type == "m.room_key_request"
                        and metadata.decryption is _DecryptionDisposition.NONE
                        and metadata.decryption_verified is None
                        and metadata.decrypted_to_device_kind is None
                        and metadata.callback_route is _CallbackRoute.TO_DEVICE
                    )
                    remaining_decrypted_to_device = (
                        record.kind is RecordKind.TO_DEVICE
                        and metadata.preparation_phase is _PreparationPhase.SOURCE
                        and metadata.decryption is _DecryptionDisposition.DECRYPTED
                        and metadata.decryption_verified is None
                        and metadata.decrypted_to_device_kind
                        in {
                            _DecryptedToDeviceKind.FORWARDED_ROOM_KEY,
                            _DecryptedToDeviceKind.DUMMY,
                            _DecryptedToDeviceKind.UNKNOWN,
                            _DecryptedToDeviceKind.BAD,
                            _DecryptedToDeviceKind.UNKNOWN_BAD,
                        }
                        and metadata.callback_route is _CallbackRoute.TO_DEVICE
                    )
                    if (
                        not global_account_data
                        and not source_to_device
                        and not source_to_device_ack_only
                        and not decrypted_room_key
                        and not expired_verification
                        and not collected_key_request
                        and not remaining_decrypted_to_device
                    ):
                        raise LocalProtocolError(
                            "unsupported durable settlement record"
                        )

                    if decrypted_room_key:
                        if record.clear_json is None:
                            raise JournalIntegrityError(
                                "decrypted room-key event is invalid"
                            )
                        source = _load_settlement_event(
                            record.source_json,
                            "encrypted to-device event",
                            "encrypted to-device source is invalid",
                        )
                        clear = _load_settlement_event(
                            record.clear_json,
                            "decrypted room-key event",
                            "decrypted room-key event is invalid",
                        )
                        source_content = source.get("content")
                        clear_content = clear.get("content")
                        if (
                            source.get("type") != "m.room.encrypted"
                            or type(source.get("sender")) is not str
                            or not source["sender"]
                            or type(source_content) is not dict
                            or source_content.get("algorithm")
                            != "m.olm.v1.curve25519-aes-sha2"
                            or type(source_content.get("sender_key")) is not str
                            or not source_content["sender_key"]
                            or clear.get("type") != metadata.effective_event_type
                            or clear.get("sender") != source["sender"]
                            or "keys" in clear
                            or type(clear_content) is not dict
                            or "session_key" in clear_content
                            or type(clear_content.get("room_id")) is not str
                            or not clear_content["room_id"]
                            or type(clear_content.get("session_id")) is not str
                            or not clear_content["session_id"]
                            or type(clear_content.get("algorithm")) is not str
                            or not clear_content["algorithm"]
                        ):
                            raise JournalIntegrityError(
                                "decrypted room-key event is invalid"
                            )
                        event = RoomKeyEvent(
                            clear,
                            source["sender"],
                            source_content["sender_key"],
                            clear_content["room_id"],
                            clear_content["session_id"],
                            clear_content["algorithm"],
                        )
                        if receipt_new:
                            await self._client._on_to_device(event)
                    elif remaining_decrypted_to_device:
                        if record.clear_json is None:
                            raise JournalIntegrityError(
                                "decrypted to-device event is invalid"
                            )
                        source = _load_settlement_event(
                            record.source_json,
                            "encrypted to-device event",
                            "encrypted to-device source is invalid",
                        )
                        clear = _load_settlement_event(
                            record.clear_json,
                            "decrypted to-device event",
                            "decrypted to-device event is invalid",
                        )
                        source_content = source.get("content")
                        if (
                            source.get("type") != "m.room.encrypted"
                            or type(source.get("sender")) is not str
                            or not source["sender"]
                            or type(source_content) is not dict
                            or source_content.get("algorithm")
                            != "m.olm.v1.curve25519-aes-sha2"
                            or type(source_content.get("sender_key")) is not str
                            or not source_content["sender_key"]
                            or clear.get("type") != metadata.effective_event_type
                        ):
                            raise JournalIntegrityError(
                                "decrypted to-device event is invalid"
                            )
                        sender = source["sender"]
                        sender_key = source_content["sender_key"]
                        if (
                            metadata.decrypted_to_device_kind
                            is _DecryptedToDeviceKind.FORWARDED_ROOM_KEY
                        ):
                            clear_content = clear.get("content")
                            if (
                                clear.get("type") != "m.forwarded_room_key"
                                or clear.get("sender") != sender
                                or type(clear_content) is not dict
                                or "session_key" in clear_content
                                or type(clear_content.get("room_id")) is not str
                                or not clear_content["room_id"]
                                or type(clear_content.get("session_id")) is not str
                                or not clear_content["session_id"]
                                or type(clear_content.get("algorithm")) is not str
                                or not clear_content["algorithm"]
                            ):
                                raise JournalIntegrityError(
                                    "decrypted forwarded room-key event is invalid"
                                )
                            event = ForwardedRoomKeyEvent(
                                clear,
                                sender,
                                sender_key,
                                clear_content["room_id"],
                                clear_content["session_id"],
                                clear_content["algorithm"],
                            )
                        elif (
                            metadata.decrypted_to_device_kind
                            is _DecryptedToDeviceKind.DUMMY
                        ):
                            if clear.get("type") != "m.dummy":
                                raise JournalIntegrityError(
                                    "decrypted dummy event is invalid"
                                )
                            event = DummyEvent.from_dict(clear, sender, sender_key)
                            if type(event) is not DummyEvent:
                                raise JournalIntegrityError(
                                    "decrypted dummy event is invalid"
                                )
                        elif (
                            metadata.decrypted_to_device_kind
                            is _DecryptedToDeviceKind.UNKNOWN
                        ):
                            if type(clear.get("sender")) is not str:
                                raise JournalIntegrityError(
                                    "decrypted unknown event is invalid"
                                )
                            event = UnknownToDeviceEvent.from_dict(clear)
                            if type(event) is not UnknownToDeviceEvent:
                                raise JournalIntegrityError(
                                    "decrypted unknown event is invalid"
                                )
                        elif (
                            metadata.decrypted_to_device_kind
                            is _DecryptedToDeviceKind.BAD
                        ):
                            if (
                                type(clear.get("event_id")) is not str
                                or type(clear.get("sender")) is not str
                                or type(clear.get("origin_server_ts")) is not int
                            ):
                                raise JournalIntegrityError(
                                    "decrypted bad event is invalid"
                                )
                            event = BadEvent.from_dict(clear)
                            if type(event) is not BadEvent:
                                raise JournalIntegrityError(
                                    "decrypted bad event is invalid"
                                )
                        else:
                            assert (
                                metadata.decrypted_to_device_kind
                                is _DecryptedToDeviceKind.UNKNOWN_BAD
                            )
                            event = UnknownBadEvent(clear)
                        if receipt_new:
                            await self._client._on_to_device(event)  # type: ignore[arg-type]
                    elif expired_verification:
                        source = _load_settlement_event(
                            record.source_json,
                            "expired verification",
                            "expired verification source is invalid",
                        )
                        if source.get("type") not in (
                            None,
                            metadata.effective_event_type,
                        ):
                            raise JournalIntegrityError(
                                "expired verification source is invalid"
                            )
                        event = KeyVerificationCancel.from_dict(source)
                        if type(event) is not KeyVerificationCancel:
                            raise JournalIntegrityError(
                                "expired verification source is invalid"
                            )
                        if receipt_new:
                            await self._client._on_expired_verifications(event)
                    elif collected_key_request:
                        source = _load_settlement_event(
                            record.source_json,
                            "collected key request",
                            "collected key-request source is invalid",
                        )
                        if source.get("type") != metadata.effective_event_type:
                            raise JournalIntegrityError(
                                "collected key-request source is invalid"
                            )
                        event = ToDeviceEvent.parse_event(source)
                        if type(event) not in (
                            RoomKeyRequest,
                            RoomKeyRequestCancellation,
                        ):
                            raise JournalIntegrityError(
                                "collected key-request source is invalid"
                            )
                        if receipt_new:
                            await self._client._on_to_device(event)
                    elif not source_to_device_ack_only:
                        source_name = (
                            "global account-data event"
                            if global_account_data
                            else "to-device event"
                        )
                        source = load_json(record.source_json, source_name)
                        if (
                            type(source) is not dict
                            or canonical_json(source) != record.source_json
                        ):
                            invalid_source = (
                                "global account-data source is invalid"
                                if global_account_data
                                else "to-device source is invalid"
                            )
                            raise JournalIntegrityError(invalid_source)
                        if global_account_data:
                            event = AccountDataEvent.parse_event(source)
                            if receipt_new:
                                await self._client._on_global_account_data(event)
                        else:
                            event = ToDeviceEvent.parse_event(source)
                            if event is None:
                                raise JournalIntegrityError(
                                    "to-device event is invalid"
                                )
                            if receipt_new:
                                await self._client._on_to_device(event)
                self._client._assert_ingestion_not_poisoned()
                if self._close_task is not None or self._closed:
                    raise LocalProtocolError("ingestion session is closed")
                self._journal.acknowledge_batch(batch.ref)
                self._signal_durable_progress()
            finally:
                self._settlement_task = None

    def acknowledge_batch(self, ref: BatchRef) -> None:
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None or self._closed:
            raise LocalProtocolError("ingestion session is closed")
        if self._settlement_task is not None:
            raise LocalProtocolError("cannot acknowledge during settlement")
        self._journal.acknowledge_batch(ref)
        self._signal_durable_progress()

    def _publish_local_membership_transition(
        self,
        *,
        operation_id: UUID,
        room_id: str,
        previous_membership: str,
        previous_epoch: int,
        current_membership: str,
    ) -> CommitResult:
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None or self._closed:
            raise LocalProtocolError("ingestion session is closed")
        return self._journal._publish_local_membership_transition(
            operation_id=operation_id,
            room_id=room_id,
            previous_membership=previous_membership,
            previous_epoch=previous_epoch,
            current_membership=current_membership,
        )

    async def _run_local_membership_transition(
        self,
        *,
        operation_id: UUID,
        room_id: str,
        previous_membership: str,
        previous_epoch: int,
        current_membership: str,
    ) -> bool:
        self._client._assert_ingestion_not_poisoned()
        if self._close_task is not None or self._closed:
            raise LocalProtocolError("ingestion session is closed")
        if type(room_id) is not str or not room_id:
            raise TypeError("room_id must be a nonempty str")
        intent = _LocalMembershipIntent(
            operation_id,
            previous_membership,
            previous_epoch,
            current_membership,
        )
        command = self._local_membership_command
        if command is None:
            completion: asyncio.Future[bool] = (
                asyncio.get_running_loop().create_future()
            )
            command = _LocalMembershipCommand(room_id, intent, completion)
            self._local_membership_command = command
            self._signal_progress()
        elif (command.room_id, command.intent) != (room_id, intent):
            raise LocalProtocolError(
                "another local membership command is already queued"
            )
        return await asyncio.shield(command.completion)

    async def close(self) -> None:
        if self._settlement_task is asyncio.current_task():
            raise LocalProtocolError("owned ingestion settlement cannot close itself")
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
        active: list[asyncio.Task[object]] = []
        for task in (self._running, self._settlement_task):
            if task is not None and all(task is not candidate for candidate in active):
                active.append(task)
        for task in active:
            task.cancel()
        if active:
            await asyncio.gather(*active, return_exceptions=True)
        command = self._local_membership_command
        if command is not None and not command.completion.done():
            command.completion.cancel()
        self._local_membership_command = None
        if not self._detached:
            self._client._detach_ingestion_store(self._owned_store)
            self._detached = True
        if not self._revoked:
            self._lease._revoke_store(self._owned_store)
            self._revoked = True
        self._lease._close_owner()
        self._closed = True


def _validate_owned_client_is_pristine(client: AsyncClient) -> None:
    client._assert_ingestion_not_poisoned()
    event_scope = client._event_callback_scope.get()
    response_scope = client._response_callback_scope.get()
    dirty = (
        client.store is not None
        or client.olm is not None
        or client.client_session is not None
        or client._stop_sync_forever
        or bool(client.next_batch)
        or bool(client.loaded_sync_token)
        or bool(client.rooms)
        or bool(client.invited_rooms)
        or bool(client.encrypted_rooms)
        or bool(client.sharing_session)
        or client.synced.is_set()
        or client._ingestion_store_snapshot is not None
        or (event_scope is not None and event_scope.active)
        or (response_scope is not None and response_scope.active)
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
    _completion_sink: Callable[[_FrameCompletion], Awaitable[None]] | None = None,
) -> _OwnedIngestionSession:
    from ..client.async_client import AsyncClient
    from ..client.base_client import _room_from_snapshot
    from ..rooms import MatrixInvitedRoom, MatrixRoom
    from ..store.database import SqliteStore
    from ..store.sync_journal import StoreBootstrap

    if not isinstance(client, AsyncClient):
        raise LocalProtocolError("ingestion requires an AsyncClient")
    client._assert_ingestion_not_poisoned()
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
    if _completion_sink is not None and not callable(_completion_sink):
        raise LocalProtocolError("completion sink must be callable")
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
    rooms: dict[str, MatrixRoom] = {}
    invited_rooms: dict[str, MatrixInvitedRoom] = {}
    restore_view = journal._load_room_restore_view()
    try:
        for continuity, snapshot in restore_view:
            membership = continuity.membership
            if membership not in {None, "invite", "join", "knock", "leave", "ban"}:
                raise ValueError("persisted room membership is invalid")
            if snapshot is None:
                continue
            invited = membership in {"invite", "knock"}
            room = _room_from_snapshot(snapshot, invited=invited)
            if invited:
                if type(room) is not MatrixInvitedRoom:
                    raise TypeError("invited room restore has the wrong type")
                invited_rooms[continuity.room_id] = room
            else:
                rooms[continuity.room_id] = room
    except (TypeError, ValueError) as error:
        raise JournalIntegrityError("persisted room restore view is invalid") from error
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
        client._attach_ingestion_store(
            store,
            rooms=rooms,
            invited_rooms=invited_rooms,
        )
        lease = candidate._prepare_transfer(store)
        session = _OwnedIngestionSession(
            client,
            config,
            lease,
            store,
            journal,
            source,
            _completion_sink,
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
