from ingestion_helpers import (
    open_test_session,
    materialize_journal,
    retire_completed_frame,
    run_with_acknowledgements,
)
from nio.store.sync_journal import (
    StoreBootstrap,
    _open_fresh_ingestion_store as open_ingestion_store,
)
import asyncio
import base64
import hashlib
import inspect
import json
import sqlite3
import threading
from collections.abc import Callable
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid4, uuid5

import pytest
from aiohttp import ClientConnectionError, ClientPayloadError

import nio
from nio import AsyncClient, AsyncClientConfig
from nio.crypto import Olm, OlmAccount, OlmDevice
from nio.event_builders import DummyMessage, RoomKeyRequestMessage, ToDeviceMessage
from nio.events import (
    AccountDataEvent,
    EphemeralEvent,
    Event,
    InviteEvent,
    KeyVerificationCancel,
    MegolmEvent,
    PresenceEvent,
    RoomKeyEvent,
    RoomKeyRequest,
    RoomKeyRequestCancellation,
    ToDeviceEvent,
)
from nio.events.misc import BadEvent, UnknownBadEvent
from nio.events.to_device import (
    DummyEvent,
    ForwardedRoomKeyEvent,
    UnknownToDeviceEvent,
)
from nio.exceptions import LocalProtocolError
from nio.ingest.errors import JournalConflictError, JournalIntegrityError
from nio.ingest.hydration import normalize_hydration_response
from nio.ingest.classic import ClassicSource
from nio.ingest.config import (
    ClassicSourceConfig,
    IngestionConfig,
    SlidingSourceConfig,
)
from nio.ingest.model import (
    BatchRef,
    EventRecord,
    LossRecord,
    RecordKind,
    RecordOrigin,
    SystemOrigin,
    SystemOriginKind,
    TransportKind,
    _CallbackRoute,
    _DecryptionDisposition,
    _DecryptedToDeviceKind,
    _PreparationPhase,
)
from nio.ingest.ports import NetworkFailureKind, NetworkResult, StagedSourceResponse
from nio.ingest.sliding import (
    RESERVED_ALL_ROOMS_LIST,
    SlidingSource,
    _sliding_cursor_from_json,
    canonical_sliding_cursor,
)
from nio.ingest.reducer import HydrationIntent, MembershipBaseline
from nio.ingest.source import (
    ClassicCursor,
    SyncFrame,
    canonical_classic_cursor,
    canonical_json,
)
from nio.ingest.state import CommitResult, SourceState, StagedFrame
from nio.responses import (
    KeysClaimResponse,
    KeysQueryResponse,
    KeysUploadResponse,
    ToDeviceResponse,
)
from nio.store import (
    DefaultStore,
    SqliteMemoryStore,
    SqliteStore,
)
from nio.store._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
)

ACCOUNT = "@alice:example.org"
DEVICE = "ALICE"
BOB = "@bob:example.org"
BOB_DEVICE = "BOB"
ROOM = "!diagnostic:example.org"


def _bearer_only_path(request: tuple[str, str]) -> str:
    return request[1].partition("?")[0]


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


class QuiesceBarrierClient(RecordingClient):
    def __init__(self, response: FakeResponse) -> None:
        super().__init__()
        self.response = response
        self.first_request_entered = asyncio.Event()
        self.release_first_request = asyncio.Event()

    async def send(self, *args: object, **kwargs: object) -> FakeResponse:
        self.send_count += 1
        self.send_calls.append((args, kwargs))
        if self.send_count != 1:
            raise AssertionError("quiesced ingestion issued another source request")
        self.first_request_entered.set()
        await self.release_first_request.wait()
        return self.response


class TimedIdleQuiesceClient(RecordingClient):
    def __init__(self, response: FakeResponse, *, server_wait_seconds: float) -> None:
        super().__init__()
        self.response = response
        self.server_wait_seconds = server_wait_seconds
        self.first_request_entered = asyncio.Event()
        self.deadline_expired = asyncio.Event()

    async def send(self, *args: object, **kwargs: object) -> FakeResponse:
        self.send_count += 1
        self.send_calls.append((args, kwargs))
        self.first_request_entered.set()
        timeout_seconds = args[5]
        assert type(timeout_seconds) is float
        try:
            async with asyncio.timeout(timeout_seconds):
                await asyncio.sleep(self.server_wait_seconds)
                await asyncio.sleep(self.server_wait_seconds)
        except TimeoutError:
            self.deadline_expired.set()
            raise
        return self.response


class QuiesceResetBarrierClient(RecordingClient):
    def __init__(self, event_source: dict[str, object]) -> None:
        super().__init__()
        self.event_source = event_source
        self.first_request_entered = asyncio.Event()
        self.release_first_request = asyncio.Event()
        self.second_request_entered = asyncio.Event()

    async def send(self, *args: object, **kwargs: object) -> FakeResponse:
        self.send_count += 1
        self.send_calls.append((args, kwargs))
        if self.send_count == 1:
            self.first_request_entered.set()
            await self.release_first_request.wait()
            return FakeResponse(400, b'{"errcode":"M_UNKNOWN_POS"}')
        if self.send_count != 2:
            raise AssertionError("quiesced ingestion issued another source request")
        request_body = args[2]
        assert type(request_body) is bytes
        self.second_request_entered.set()
        return FakeResponse(
            200,
            canonical_json(
                {
                    "extensions": {
                        "account_data": {"global": [self.event_source], "rooms": {}},
                        "to_device": {"events": [], "next_batch": "td-after-reset"},
                    },
                    "lists": {RESERVED_ALL_ROOMS_LIST: {"count": 0}},
                    "pos": "after-reset",
                    "rooms": {},
                    "txn_id": json.loads(request_body)["txn_id"],
                }
            ),
        )


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


def open_owned_bootstrap(
    tmp_path,
    generation: UUID,
    *,
    source=None,
    pickle_key: str = "owned-secret",
    database_name: str = "owned.db",
):
    source = source or classic_config().source
    seed = SqliteStore(
        ACCOUNT,
        DEVICE,
        str(tmp_path),
        pickle_key=pickle_key,
        database_name=database_name,
    )
    account = OlmAccount()
    seed.save_account(account)
    seed.database.close()
    import nio.store.sync_journal as bootstrap_api

    bootstrap = bootstrap_api._open_configured_ingestion_store(
        tmp_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=source,
        pickle_key=pickle_key,
        database_name=database_name,
    )
    return bootstrap, account


def owned_client(
    tmp_path,
    *,
    pickle_key: str = "owned-secret",
    database_name: str = "owned.db",
    client_type=RecordingClient,
):
    client = client_type()
    client.store_path = str(tmp_path)
    client.config = AsyncClientConfig(
        store=SqliteStore,
        pickle_key=pickle_key,
        store_name=database_name,
    )
    return client


def open_owned_session(client, bootstrap, generation: UUID, config=None):
    from nio.ingest import coordinator

    return coordinator._open_owned_ingestion(
        client,
        bootstrap,
        config=config or classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )


def reopen_owned_bootstrap(tmp_path, generation: UUID, *, source=None):
    import nio.store.sync_journal as bootstrap_api

    return bootstrap_api._open_configured_ingestion_store(
        tmp_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=source or classic_config().source,
        pickle_key="owned-secret",
        database_name="owned.db",
    )


def stage_classic(
    bootstrap,
    body: dict[str, object],
    *,
    quiesce_reserved: bool = False,
) -> StagedFrame:
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
        quiesce_reserved=quiesce_reserved,
    )
    return frame


def _owned_crypto_classic_body(
    client: AsyncClient,
) -> tuple[dict[str, object], SqliteMemoryStore]:
    assert client.olm is not None
    assert type(client.store) is SqliteStore
    sender_store = SqliteMemoryStore(BOB, BOB_DEVICE, "sender-secret")
    sender = Olm(BOB, BOB_DEVICE, sender_store)
    recipient_device = OlmDevice(
        ACCOUNT,
        DEVICE,
        client.olm.account.identity_keys,
    )
    sender_device = OlmDevice(BOB, BOB_DEVICE, sender.account.identity_keys)
    sender.store.save_device_keys({ACCOUNT: {DEVICE: recipient_device}})
    sender.device_store.add(recipient_device)
    sender.verify_device(recipient_device)
    client.store.save_device_keys({BOB: {BOB_DEVICE: sender_device}})
    client.olm.device_store.add(sender_device)
    client.olm.verify_device(sender_device)

    client.olm.account.generate_one_time_keys(1)
    generated = dict(client.olm.account.one_time_keys["curve25519"])
    assert len(generated) == 1
    client.olm.save_account()
    persisted = client.store._load_persisted_account()
    assert persisted is not None
    assert persisted.one_time_keys["curve25519"] == generated
    one_time_key = next(iter(generated.values()))
    client.olm.account.mark_keys_as_published()

    sender.create_session(one_time_key, recipient_device.curve25519)
    _sharing_with, to_device = sender.share_group_session(ROOM, [ACCOUNT])
    sender.outbound_group_sessions[ROOM].shared = True
    encrypted_content = sender.group_encrypt(
        ROOM,
        {
            "content": {"body": "same-frame secret", "msgtype": "m.text"},
            "type": "m.room.message",
        },
    )

    def event(event_type: str, content: object, **fields: object) -> object:
        return {"content": content, "type": event_type, **fields}

    state = [
        event(
            "m.room.encryption",
            {"algorithm": "m.megolm.v1.aes-sha2"},
            event_id="$encryption",
            origin_server_ts=1,
            sender=ACCOUNT,
            state_key="",
        ),
        *(
            event(
                "m.room.member",
                {"membership": "join"},
                event_id=f"$member-{user_id}",
                origin_server_ts=2,
                sender=user_id,
                state_key=user_id,
            )
            for user_id in (ACCOUNT, BOB)
        ),
    ]
    body: dict[str, object] = {
        "device_lists": {"changed": [], "left": []},
        "device_one_time_keys_count": {"signed_curve25519": 0},
        "device_unused_fallback_key_types": [],
        "next_batch": "s1",
        "rooms": {
            "join": {
                ROOM: {
                    "state": {"events": state},
                    "timeline": {
                        "events": [
                            event(
                                "m.room.encrypted",
                                encrypted_content,
                                event_id="$secret",
                                origin_server_ts=3,
                                sender=BOB,
                            )
                        ]
                    },
                }
            }
        },
        "to_device": {
            "events": [
                event(
                    "m.room.encrypted",
                    to_device["messages"][ACCOUNT][DEVICE],
                    sender=BOB,
                )
            ]
        },
    }
    return body, sender_store


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


def _parity_response(
    transport: str,
    request_body: bytes | None,
    *,
    nonempty: bool,
    one_time_key_count: int,
) -> dict[str, object]:
    to_device = {
        "content": {"value": "to-device"},
        "sender": BOB,
        "type": "org.example.parity.to_device",
    }
    member = {
        "content": {"displayname": "Alice", "membership": "join"},
        "event_id": "$parity-member",
        "origin_server_ts": 1,
        "sender": ACCOUNT,
        "state_key": ACCOUNT,
        "type": "m.room.member",
    }
    message = {
        "content": {"body": "history", "msgtype": "m.text"},
        "event_id": "$parity-message",
        "origin_server_ts": 2,
        "sender": BOB,
        "type": "m.room.message",
    }
    receipt = {
        "content": {"$parity-message": {"m.read": {BOB: {"ts": 3}}}},
        "type": "m.receipt",
    }
    room_account_data = {
        "content": {"tags": {"u.work": {"order": 1}}},
        "type": "m.tag",
    }
    global_account_data = {
        "content": {"theme": "dark"},
        "type": "org.example.parity.theme",
    }
    presence = {
        "content": {
            "currently_active": True,
            "last_active_ago": 0,
            "presence": "online",
            "status_msg": "ready",
        },
        "sender": BOB,
        "type": "m.presence",
    }
    key_counts = {"signed_curve25519": one_time_key_count}
    if transport == "classic":
        return {
            "account_data": {"events": [global_account_data] if nonempty else []},
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": key_counts,
            "device_unused_fallback_key_types": [],
            "next_batch": "s1",
            "presence": {"events": [presence] if nonempty else []},
            "rooms": (
                {
                    "join": {
                        ROOM: {
                            "account_data": {"events": [room_account_data]},
                            "ephemeral": {"events": [receipt]},
                            "state": {"events": [member]},
                            "timeline": {"events": [message], "limited": False},
                        }
                    }
                }
                if nonempty
                else {}
            ),
            "to_device": {"events": [to_device] if nonempty else []},
        }

    assert transport == "sliding"
    assert request_body is not None
    request_value = json.loads(request_body)
    return {
        "extensions": {
            "account_data": {
                "global": [global_account_data] if nonempty else [],
                "rooms": {ROOM: [room_account_data]} if nonempty else {},
            },
            "e2ee": {
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": key_counts,
                "device_unused_fallback_key_types": [],
            },
            "presence": {"events": [presence] if nonempty else []},
            "to_device": {
                "events": [to_device] if nonempty else [],
                "next_batch": "td1",
            },
            "receipts": {"rooms": {ROOM: receipt} if nonempty else {}},
        },
        "lists": {RESERVED_ALL_ROOMS_LIST: {"count": int(nonempty)}},
        "pos": "p1",
        "rooms": (
            {
                ROOM: {
                    "limited": False,
                    "membership": "join",
                    "num_live": 0,
                    "required_state": [member],
                    "timeline": [message],
                }
            }
            if nonempty
            else {}
        ),
        "txn_id": request_value["txn_id"],
    }


def _stage_parity_frame(
    bootstrap,
    config: IngestionConfig,
    *,
    nonempty: bool,
    one_time_key_count: int,
) -> tuple[StagedFrame, SyncFrame]:
    journal = bootstrap._journal
    owner = journal.load_owner()
    prior = journal.load_source()
    if type(config.source) is ClassicSourceConfig:
        source = ClassicSource(owner.stream_id, config.source, owner.account_id)
        transport = "classic"
    else:
        assert type(config.source) is SlidingSourceConfig
        source = SlidingSource(owner.stream_id, config.source, owner.account_id)
        transport = "sliding"
    request = source.plan_request(prior, prior.next_request_id)
    assert request is not None
    body = _parity_response(
        transport,
        request.body,
        nonempty=nonempty,
        one_time_key_count=one_time_key_count,
    )
    normalized = source.normalize(
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
    staged = StagedFrame(
        normalized.frame.frame_id,
        StagedSourceResponse(
            request,
            normalized.response_body,
            normalized.frame.source_sha256,
        ),
    )
    committed = journal.stage_source_response(
        source=SourceState(
            prior.source_epoch,
            prior.transport_kind,
            normalized.frame.candidate_cursor_json,
            request.request_id + 1,
            prior.active,
        ),
        frame=staged,
    )
    return replace(staged, staged_revision=committed.revision), normalized.frame


def _parity_origin(origin: RecordOrigin) -> tuple[int, int, int]:
    return origin.source_epoch, origin.request_id, origin.frame_index


def _sync_frame_parity(frame: SyncFrame) -> object:
    segments = tuple(
        (
            segment.room_id,
            segment.section,
            segment.state_json,
            segment.timeline_json,
            segment.room_account_data_json,
            segment.timeline_limited,
            segment.timeline_prev_batch,
            segment.initial,
            segment.expanded_timeline,
            segment.live_event_count,
            (
                segment.membership_observation.room_membership,
                segment.membership_observation.event_membership,
                segment.membership_observation.event_id,
                segment.membership_observation.previous_membership,
                segment.membership_observation.replaces_state,
                segment.membership_observation.is_live,
                segment.membership_observation.is_initial,
                segment.membership_observation.is_expanded_timeline,
                segment.membership_observation.is_unparsed,
            ),
        )
        for segment in frame.room_segments
    )
    return (
        _parity_origin(frame.origin),
        frame.to_device_json,
        frame.device_list_delta_json,
        frame.one_time_key_counts_json,
        frame.unused_fallback_key_types_json,
        segments,
        frame.ephemeral_json,
        frame.global_account_data_json,
    )


def _prepared_frame_parity(prepared) -> object:
    record_index = {
        record.record_id: index for index, record in enumerate(prepared.records)
    }
    records = tuple(
        (
            record.kind,
            _parity_origin(record.origin),
            record.preparation_phase,
            record.effective_event_type,
            record.room_id,
            record.event_id,
            record.provenance,
            record.source_json,
            record.clear_json,
            record.decryption,
            record.decryption_verified,
            record.decrypted_to_device_kind,
            record.callback_route,
        )
        for record in prepared.records
    )
    transitions = tuple(
        (
            (
                None
                if transition.source_record_id is None
                else record_index[transition.source_record_id]
            ),
            transition.room_id,
            transition.event_id,
            transition.previous_membership,
            transition.current_membership,
            transition.previous_epoch,
            transition.current_epoch,
            transition.source_kind,
            transition.timeline_provenance,
            transition.membership_provenance,
            _parity_origin(transition.origin),
            transition.source_json,
        )
        for transition in prepared.membership_transitions
    )
    return (
        prepared.source_epoch,
        prepared.request_id,
        prepared.staged_revision,
        records,
        transitions,
        prepared.room_snapshots,
        prepared.crypto_delta,
    )


def _aggregate_parity(value) -> object:
    continuity = value.continuity
    gap = continuity.gap
    pending = value.pending_hydration
    return (
        continuity.room_id,
        continuity.membership_epoch,
        continuity.membership,
        continuity.baseline,
        (
            None
            if gap is None
            else (
                gap.room_id,
                gap.membership_epoch,
                _parity_origin(gap.origin),
                gap.start_token,
                gap.target_token,
            )
        ),
        continuity.hydration_id is not None,
        value.next_room_sequence,
        value.updated_revision,
        None if pending is None else _parity_origin(pending.origin),
        value.room_snapshot,
    )


def _work_value_parity(
    value: EventRecord | LossRecord,
    event_by_id: dict[str, EventRecord],
) -> object:
    if type(value) is EventRecord:
        source_json = value.source_json
        if value.kind.value == "room_lifecycle" and type(value.origin) is RecordOrigin:
            evidence = json.loads(source_json)
            assert type(evidence) is dict
            source_record_id = evidence["source_record_id"]
            assert type(source_record_id) is str
            source = event_by_id[source_record_id]
            assert source.room_id == value.room_id
            assert source.event_id == evidence["event_id"]
            assert canonical_json(evidence) == source_json
            evidence["source_record_id"] = "<bound-source-record>"
            source_json = canonical_json(evidence)
        return (
            "event",
            value.kind,
            _parity_origin(value.origin),
            value.room_id,
            value.membership_epoch,
            value.room_sequence,
            value.event_id,
            value.provenance,
            source_json,
            value.clear_json,
        )
    assert type(value) is LossRecord
    return (
        "loss",
        _parity_origin(value.origin),
        value.room_id,
        value.membership_epoch,
        value.reason,
        value.boundary,
        value.detail_json,
    )


def _work_inventory_parity(inventory) -> object:
    event_by_id = {
        work.value.record_id: work.value
        for work in inventory.work
        if type(work.value) is EventRecord
    }
    projected = []
    for row, work in zip(inventory.storage_rows, inventory.work, strict=True):
        metadata = work.metadata
        projected.append(
            (
                row[2],
                row[3],
                row[5],
                row[6],
                row[7],
                row[8],
                row[9],
                row[10],
                _work_value_parity(work.value, event_by_id),
                (
                    None
                    if metadata is None
                    else (
                        metadata.preparation_phase,
                        metadata.effective_event_type,
                        metadata.decryption,
                        metadata.decryption_verified,
                        metadata.decrypted_to_device_kind,
                        metadata.callback_route,
                    )
                ),
            )
        )
    return tuple(sorted(projected, key=repr))


def _raw_materialized_rows(connection) -> tuple[tuple[object, ...], ...]:
    return tuple(
        (table, *row)
        for table in (
            "NioIngestFrame",
            "NioIngestRoomAggregate",
            "NioIngestWork",
        )
        for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
    )


def _raw_e2ee_rows(connection) -> tuple[tuple[object, ...], ...]:
    tables = tuple(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT LIKE 'sqlite_%' AND name NOT LIKE 'NioIngest%' "
            "ORDER BY name"
        )
    )
    return tuple(
        (table, *row)
        for table in tables
        for row in connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')
    )


async def _owned_transport_parity_case(
    tmp_path,
    *,
    transport: str,
    nonempty: bool,
) -> tuple[object, object, object]:
    Path(tmp_path).mkdir(parents=True)
    config = classic_config() if transport == "classic" else sliding_config()
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(
        tmp_path,
        generation,
        source=config.source,
    )
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation, config)
    connection = session._owned_store.database.connection()
    captured_prepared: list[object] = []
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        assert not olm.should_upload_keys
        assert not olm.should_query_keys

        staged, normalized = _stage_parity_frame(
            bootstrap,
            config,
            nonempty=nonempty,
            one_time_key_count=olm.account.max_one_time_keys,
        )
        real_prepare = client._prepare_ingestion_frame

        def capture_prepare(*args: object, **kwargs: object):
            prepared = real_prepare(*args, **kwargs)
            captured_prepared.append(prepared)
            return prepared

        client._prepare_ingestion_frame = capture_prepare
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            staged.staged_revision + 1,
        )
        assert len(captured_prepared) == 1
        prepared = captured_prepared[0]
        assert prepared.frame_id == staged.frame_id
        assert prepared.transport.value == transport
        assert prepared.request_cursor_json == normalized.request_cursor_json
        assert prepared.candidate_cursor_json == normalized.candidate_cursor_json
        assert prepared.source_sha256 == normalized.source_sha256
        if nonempty:
            assert len(normalized.to_device_json) == 1
            assert len(normalized.room_segments) == 1
            segment = normalized.room_segments[0]
            assert segment.room_id == ROOM
            assert (
                len(segment.state_json),
                len(segment.timeline_json),
                len(segment.room_account_data_json),
                segment.live_event_count,
            ) == (1, 1, 1, 0)
            assert len(normalized.ephemeral_json) == 1
            assert len(normalized.global_account_data_json) == 1
            assert len(prepared.room_snapshots) == 1
            assert prepared.records
        else:
            assert normalized.room_segments == ()
            assert prepared.records == ()
            assert prepared.membership_transitions == ()
            assert prepared.room_snapshots == ()

        owner = session._journal.load_owner()
        with session._journal._owner.read():
            retained = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner,
            )
            inventory = session._journal._load_task3_work_inventory(owner)
            loaded_aggregate = (
                session._journal._load_room_aggregate(owner, ROOM) if nonempty else None
            )
        assert retained is not None
        assert retained.request_cursor_json == normalized.request_cursor_json
        assert retained.candidate_cursor_json == normalized.candidate_cursor_json
        assert retained.source_sha256 == normalized.source_sha256
        assert retained.compatibility_token == (
            "s1" if transport == "classic" else None
        )
        assert retained.outbound_maintenance.operations == ()
        if nonempty:
            assert loaded_aggregate is not None
            aggregate_parity = (_aggregate_parity(loaded_aggregate[1]),)
        else:
            assert loaded_aggregate is None
            aggregate_parity = ()
        persisted_parity = (aggregate_parity, _work_inventory_parity(inventory))

        raw_ingestion = _raw_ingestion_rows(bootstrap.database_path)
        raw_materialized = _raw_materialized_rows(connection)
        raw_e2ee = _raw_e2ee_rows(connection)
        assert session._materialize_oldest_frame(
            limits=MaterializerLimits()
        ) == MaterializeResult(MaterializeStatus.BLOCKED, staged.frame_id, None)
        assert _raw_ingestion_rows(bootstrap.database_path) == raw_ingestion
        assert _raw_materialized_rows(connection) == raw_materialized
        assert _raw_e2ee_rows(connection) == raw_e2ee
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(
        tmp_path,
        generation,
        source=config.source,
    )
    try:
        reopened_connection = reopened._journal._owner.database.connection()
        reopened_owner = reopened._journal.load_owner()
        with reopened._journal._owner.read():
            reopened_retained = reopened._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                reopened_owner,
            )
            reopened_inventory = reopened._journal._load_task3_work_inventory(
                reopened_owner
            )
            reopened_aggregate = (
                reopened._journal._load_room_aggregate(reopened_owner, ROOM)
                if nonempty
                else None
            )
        assert reopened_retained == retained
        assert _raw_materialized_rows(reopened_connection) == raw_materialized
        assert _raw_e2ee_rows(reopened_connection) == raw_e2ee
        if nonempty:
            assert reopened_aggregate is not None
            reopened_aggregate_parity = (_aggregate_parity(reopened_aggregate[1]),)
        else:
            assert reopened_aggregate is None
            reopened_aggregate_parity = ()
        assert (
            reopened_aggregate_parity,
            _work_inventory_parity(reopened_inventory),
        ) == persisted_parity
    finally:
        reopened.close()

    return (
        _sync_frame_parity(normalized),
        _prepared_frame_parity(prepared),
        persisted_parity,
    )


@pytest.mark.parametrize("nonempty", (False, True), ids=("empty", "nonempty"))
@pytest.mark.asyncio
async def test_owned_materializer_classic_and_sliding_are_semantically_equivalent(
    tmp_path,
    nonempty: bool,
) -> None:
    classic = await _owned_transport_parity_case(
        tmp_path / "classic",
        transport="classic",
        nonempty=nonempty,
    )
    sliding = await _owned_transport_parity_case(
        tmp_path / "sliding",
        transport="sliding",
        nonempty=nonempty,
    )

    assert classic == sliding


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


def _local_room_body() -> dict[str, object]:
    return {
        "next_batch": "s1",
        "rooms": {
            "join": {
                ROOM: {
                    "state": {
                        "events": [
                            {
                                "content": {"membership": "join"},
                                "event_id": "$local-member",
                                "origin_server_ts": 1,
                                "sender": ACCOUNT,
                                "state_key": ACCOUNT,
                                "type": "m.room.member",
                            },
                            {
                                "content": {"name": "Local transition room"},
                                "event_id": "$local-name",
                                "origin_server_ts": 2,
                                "sender": ACCOUNT,
                                "state_key": "",
                                "type": "m.room.name",
                            },
                        ]
                    },
                    "timeline": {"events": []},
                }
            }
        },
    }


def _rich_joined_room_body() -> dict[str, object]:
    def state_event(
        event_type: str,
        content: dict[str, object],
        timestamp: int,
        state_key: str,
    ) -> dict[str, object]:
        return {
            "content": content,
            "event_id": f"$rich-{timestamp}",
            "origin_server_ts": timestamp,
            "sender": ACCOUNT,
            "state_key": state_key,
            "type": event_type,
        }

    values = (
        (
            "m.room.create",
            {"creator": ACCOUNT, "m.federate": False, "room_version": "10"},
            "",
        ),
        ("m.room.encryption", {"algorithm": "m.megolm.v1.aes-sha2"}, ""),
        ("m.room.name", {"name": "Durable Room"}, ""),
        ("m.room.canonical_alias", {"alias": "#durable:example.org"}, ""),
        ("m.room.topic", {"topic": "Persist every projected field"}, ""),
        ("m.room.avatar", {"url": "mxc://example.org/durable-room"}, ""),
        ("m.room.join_rules", {"join_rule": "knock"}, ""),
        ("m.room.guest_access", {"guest_access": "can_join"}, ""),
        (
            "m.room.power_levels",
            {
                "ban": 75,
                "events": {"m.room.message": 5, "m.room.name": 60},
                "events_default": 5,
                "invite": 10,
                "kick": 65,
                "notifications": {"room": 70},
                "redact": 55,
                "state_default": 50,
                "users": {ACCOUNT: 100, BOB: 25},
                "users_default": 3,
            },
            "",
        ),
        (
            "m.room.member",
            {
                "avatar_url": "mxc://example.org/alice",
                "displayname": "Alice Durable",
                "membership": "join",
            },
            ACCOUNT,
        ),
        (
            "m.room.member",
            {
                "avatar_url": "mxc://example.org/bob",
                "displayname": "Bob Invited",
                "membership": "invite",
            },
            BOB,
        ),
    )
    state = tuple(
        state_event(event_type, content, timestamp, state_key)
        for timestamp, (event_type, content, state_key) in enumerate(values, start=1)
    )
    return {
        "next_batch": "rich-snapshot-token",
        "rooms": {
            "join": {
                ROOM: {
                    "state": {"events": state},
                    "timeline": {"events": [], "limited": False},
                }
            }
        },
    }


def _ready_local_room(session, bootstrap):
    staged = stage_classic(bootstrap, _local_room_body())
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    pending = session._journal.load_pending_hydrations(limit=2)
    assert len(pending) == 1
    hydrated = normalize_hydration_response(
        pending[0],
        own_user_id=ACCOUNT,
        response_body=hydration_state(),
    )
    assert session._journal.apply_hydration_result(result=hydrated) is not None
    while batch := session.next_batch():
        session.acknowledge_batch(batch.ref)
    owner = session._journal.load_owner()
    with session._journal._owner.read():
        loaded = session._journal._load_room_aggregate(owner, ROOM)
    assert loaded is not None
    assert loaded[1].continuity.membership == "join"
    return staged, loaded[1]


def _local_source(
    previous_membership: str,
    previous_epoch: int,
    current_membership: str,
    current_epoch: int,
) -> bytes:
    return canonical_json(
        {
            "event_id": None,
            "membership": current_membership,
            "membership_epoch": current_epoch,
            "membership_provenance": "local",
            "previous_membership": previous_membership,
            "previous_membership_epoch": previous_epoch,
            "source_kind": "local",
            "source_record_id": None,
            "timeline_provenance": None,
        }
    )


def _publish_fresh_local_join(session):
    operation_id = uuid4()
    committed = session._publish_local_membership_transition(
        operation_id=operation_id,
        room_id=ROOM,
        previous_membership="leave",
        previous_epoch=0,
        current_membership="join",
    )
    owner = session._journal.load_owner()
    with session._journal._owner.read():
        loaded = session._journal._load_room_aggregate(owner, ROOM)
        inventory = session._journal._load_task3_work_inventory(owner)
    assert loaded is not None
    assert len(inventory.work) == 1
    return operation_id, committed, loaded[1], inventory.work[0]


def _local_publication_target(
    session,
    path: str,
    *,
    drain_seed: bool = False,
) -> tuple[UUID, str, int, str]:
    if path == "insert":
        return uuid4(), "leave", 0, "join"
    assert path == "update"
    _publish_fresh_local_join(session)
    if drain_seed:
        while batch := session.next_batch():
            session.acknowledge_batch(batch.ref)
    return uuid4(), "join", 0, "leave"


def _reseal_local_work(
    session,
    work_id: str,
    *,
    value: object | None = None,
    header_updates: tuple[tuple[int, object], ...] = (),
) -> None:
    from nio.store._sync_journal_plan import _canonical_work_plaintext
    from nio.store._sync_journal_preflight import _canonical_internal

    owner = session._journal.load_owner()
    with session._journal._owner.read():
        inventory = session._journal._load_task3_work_inventory(owner)
    item = next(item for item in inventory.work if item.value.record_id == work_id)
    row = (
        session._owned_store.database.connection()
        .execute(
            "SELECT account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload, payload_sha256 FROM NioIngestWork "
            "WHERE account_id = ? AND work_id = ?",
            (ACCOUNT, work_id),
        )
        .fetchone()
    )
    assert row is not None
    clear = list(row[1:11])
    for index, replacement in header_updates:
        clear[index] = replacement
    if value is None:
        plaintext = item.plaintext
    else:
        clear[4:7] = [
            value.room_id,  # type: ignore[attr-defined]
            value.membership_epoch,  # type: ignore[attr-defined]
            value.room_sequence,  # type: ignore[attr-defined]
        ]
        plaintext = _canonical_work_plaintext("event", value)  # type: ignore[arg-type]
    assert plaintext is not None
    payload, digest = session._journal._payload(
        owner,
        "NioIngestWork",
        plaintext,
        header=_canonical_internal(clear),
    )
    session._owned_store.database.connection().execute(
        "UPDATE NioIngestWork SET room_id = ?, membership_epoch = ?, "
        "room_sequence = ?, ready_revision = ?, ready_ordinal = ?, "
        "created_revision = ?, payload = ?, payload_sha256 = ? "
        "WHERE account_id = ? AND work_id = ?",
        (
            clear[4],
            clear[5],
            clear[6],
            clear[7],
            clear[8],
            clear[9],
            payload,
            digest,
            ACCOUNT,
            work_id,
        ),
    )


def _reseal_local_aggregate(
    session,
    value: object,
    *,
    intent_kind: str | None = None,
) -> None:
    from nio.store._sync_journal_preflight import _canonical_internal
    from nio.store._sync_journal_rows import _canonical_room_aggregate_plaintext

    owner = session._journal.load_owner()
    row = (
        session._owned_store.database.connection()
        .execute(
            "SELECT updated_revision, intent_kind FROM NioIngestRoomAggregate "
            "WHERE account_id = ? AND room_id = ?",
            (ACCOUNT, ROOM),
        )
        .fetchone()
    )
    assert row is not None
    plaintext = _canonical_room_aggregate_plaintext(value)  # type: ignore[arg-type]
    payload, digest = session._journal._payload(
        owner,
        "NioIngestRoomAggregate",
        plaintext,
        header=_canonical_internal([ROOM, row[0], intent_kind]),
    )
    session._owned_store.database.connection().execute(
        "UPDATE NioIngestRoomAggregate SET intent_kind = ?, payload = ?, "
        "payload_sha256 = ? "
        "WHERE account_id = ? AND room_id = ?",
        (intent_kind, payload, digest, ACCOUNT, ROOM),
    )


def stage_pending(bootstrap) -> StagedFrame:
    return stage_classic(bootstrap, {"next_batch": "s1", "rooms": {"join": {ROOM: {}}}})


@pytest.mark.asyncio
async def test_owned_factory_attaches_exact_store_before_olm_initialization(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    from nio.client import base_client as base_client_module

    generation = uuid4()
    bootstrap, account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    real_from_persisted_account = (
        base_client_module.Olm._from_persisted_account.__func__
    )
    real_load_account = SqliteStore._load_persisted_account
    initialized_with: list[object] = []
    load_count = 0

    def observing_from_persisted_account(cls, user_id, device_id, store, **kwargs):
        assert client.store is store
        assert client.olm is None
        initialized_with.append(store)
        return real_from_persisted_account(cls, user_id, device_id, store, **kwargs)

    def counting_load(store):
        nonlocal load_count
        load_count += 1
        return real_load_account(store)

    def forbidden_save(*args, **kwargs):
        raise AssertionError("owned Olm initialization saved an account")

    monkeypatch.setattr(
        base_client_module.Olm,
        "_from_persisted_account",
        classmethod(observing_from_persisted_account),
    )
    monkeypatch.setattr(SqliteStore, "_load_persisted_account", counting_load)
    monkeypatch.setattr(
        SqliteStore,
        "load_account",
        lambda *_args, **_kwargs: (_ for _ in ()).throw(
            AssertionError("owned Olm initialization used public account loading")
        ),
    )
    monkeypatch.setattr(SqliteStore, "save_account", forbidden_save)
    session = open_owned_session(client, bootstrap, generation)

    assert initialized_with == [client.store]
    assert load_count == 1
    assert client.store is not None
    assert client.olm is not None
    assert client.olm.account.identity_keys == account.identity_keys
    assert client.store.database is bootstrap._journal._owner.database
    await session.close()


@pytest.mark.asyncio
async def test_owned_factory_restores_rich_joined_snapshot_before_delivery(
    tmp_path,
) -> None:
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    client.config = replace(client.config, store_sync_tokens=True)
    session = open_owned_session(client, bootstrap, generation)
    try:
        stage_classic(bootstrap, _rich_joined_room_body())
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        pending = session._journal.load_pending_hydrations(limit=2)
        assert len(pending) == 1
        hydrated = normalize_hydration_response(
            pending[0],
            own_user_id=ACCOUNT,
            response_body=hydration_state(),
        )
        assert session._journal.apply_hydration_result(result=hydrated) is not None
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        snapshot = loaded[1].room_snapshot
        assert snapshot is not None
        assert (
            snapshot.own_membership,
            snapshot.membership_epoch,
            snapshot.name,
            snapshot.canonical_alias,
            snapshot.topic,
            snapshot.avatar_url,
            snapshot.join_rule,
            snapshot.room_version,
            snapshot.guest_access,
        ) == (
            "join",
            0,
            "Durable Room",
            "#durable:example.org",
            "Persist every projected field",
            "mxc://example.org/durable-room",
            "knock",
            "10",
            "can_join",
        )
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_client.config = replace(restored_client.config, store_sync_tokens=True)

    def callback_tripwire(*_args: object) -> None:
        raise AssertionError("owned snapshot restore invoked an application callback")

    for callback_name in (
        "_on_to_device",
        "_on_invited_rooms",
        "_on_event",
        "_on_ephemeral",
        "_on_room_account_data",
        "_on_presence",
        "_on_global_account_data",
        "_on_expired_verifications",
    ):
        setattr(restored_client, callback_name, callback_tripwire)
    restored_client.add_event_callback(callback_tripwire, None)
    restored_client.add_ephemeral_callback(callback_tripwire, None)
    restored_client.add_global_account_data_callback(callback_tripwire, None)
    restored_client.add_room_account_data_callback(callback_tripwire, None)
    restored_client.add_to_device_callback(callback_tripwire, None)
    restored_client.add_presence_callback(callback_tripwire, None)
    restored_client.add_response_callback(callback_tripwire, None)

    connection = reopened._journal._owner.database.connection()
    database_before = tuple(connection.iterdump())
    restore_statements: list[str] = []
    connection.set_trace_callback(restore_statements.append)
    restored_session = open_owned_session(restored_client, reopened, generation)
    connection.set_trace_callback(None)
    try:
        assert restored_client.send_count == 0
        assert tuple(connection.iterdump()) == database_before
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(
                (
                    "INSERT ",
                    "UPDATE ",
                    "DELETE ",
                    "REPLACE ",
                    "CREATE ",
                    "DROP ",
                    "ALTER ",
                )
            )
            for statement in restore_statements
        )
        assert restored_client.loaded_sync_token == "rich-snapshot-token"

        # Restoration is an owned-open prerequisite, not incremental Work.
        assert tuple(restored_client.rooms) == (ROOM,)
        assert restored_client.invited_rooms == {}
        room = restored_client.rooms[ROOM]
        assert type(room) is nio.MatrixRoom
        assert (
            _room_snapshot(
                room,
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            == snapshot
        )
        assert tuple(room.invited_users) == (BOB,)
        assert dict(room.names) == {
            "Alice Durable": [ACCOUNT],
            "Bob Invited": [BOB],
        }
        assert restored_client.encrypted_rooms == {ROOM}

        batch = restored_session.next_batch()
        assert batch is not None
    finally:
        connection.set_trace_callback(None)
        await restored_session.close()

    assert (
        restored_client.store,
        restored_client.olm,
        restored_client.rooms,
        restored_client.invited_rooms,
        restored_client.encrypted_rooms,
        restored_client.next_batch,
        restored_client.loaded_sync_token,
    ) == (None, None, {}, {}, set(), "", "")


@pytest.mark.asyncio
async def test_owned_factory_does_not_fabricate_snapshotless_encrypted_room(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        _operation_id, _committed, aggregate, _work = _publish_fresh_local_join(session)
        assert aggregate.continuity.membership == "join"
        assert aggregate.room_snapshot is None
        session._owned_store.save_encrypted_rooms({ROOM})
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_client.config = replace(
        restored_client.config,
    )
    restored_session = open_owned_session(restored_client, reopened, generation)
    try:
        assert restored_client.encrypted_rooms == {ROOM}
        assert restored_client.rooms == {}
        assert restored_client.invited_rooms == {}
    finally:
        await restored_session.close()


@pytest.mark.asyncio
async def test_owned_close_restores_first_materializing_client_room_maps_and_token(
    tmp_path,
) -> None:
    invite_room = "!first-client-invite:example.org"
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    pristine_rooms = client.rooms
    pristine_invited_rooms = client.invited_rooms
    pristine_encrypted_rooms = client.encrypted_rooms
    session = open_owned_session(client, bootstrap, generation)
    try:
        body = _rich_joined_room_body()
        rooms = body["rooms"]
        assert type(rooms) is dict
        rooms["invite"] = {
            invite_room: {
                "invite_state": {
                    "events": [
                        {
                            "content": {
                                "displayname": "Invited Alice",
                                "membership": "invite",
                            },
                            "event_id": "$first-client-invite",
                            "sender": BOB,
                            "state_key": ACCOUNT,
                            "type": "m.room.member",
                        }
                    ]
                }
            }
        }
        stage_classic(bootstrap, body)
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        assert tuple(client.rooms) == (ROOM,)
        assert tuple(client.invited_rooms) == (invite_room,)
        assert client.next_batch == "rich-snapshot-token"
        assert client.rooms is not pristine_rooms
        assert client.invited_rooms is not pristine_invited_rooms
    finally:
        await session.close()

    assert client.rooms is pristine_rooms
    assert client.invited_rooms is pristine_invited_rooms
    assert client.encrypted_rooms is pristine_encrypted_rooms
    assert client.next_batch == ""


@pytest.mark.parametrize("membership", ["invite", "knock"])
@pytest.mark.asyncio
async def test_owned_factory_restores_invited_room_from_current_membership(
    tmp_path,
    membership: str,
) -> None:
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    state_key = "invite_state" if membership == "invite" else "knock_state"
    try:
        stage_classic(
            bootstrap,
            {
                "next_batch": f"{membership}-snapshot-token",
                "rooms": {
                    membership: {
                        ROOM: {
                            state_key: {
                                "events": [
                                    {
                                        "content": {"membership": membership},
                                        "sender": BOB,
                                        "state_key": ACCOUNT,
                                        "type": "m.room.member",
                                    }
                                ]
                            }
                        }
                    }
                },
            },
        )
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        snapshot = loaded[1].room_snapshot
        assert snapshot is not None
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_session = open_owned_session(restored_client, reopened, generation)
    try:
        assert restored_client.rooms == {}
        assert tuple(restored_client.invited_rooms) == (ROOM,)
        room = restored_client.invited_rooms[ROOM]
        assert type(room) is nio.MatrixInvitedRoom
        assert (
            _room_snapshot(
                room,
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            == snapshot
        )
    finally:
        await restored_session.close()


@pytest.mark.parametrize("membership", ["invite", "knock"])
@pytest.mark.asyncio
async def test_owned_cold_invited_state_is_ready_without_join_hydration(
    tmp_path,
    membership: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        section = "invite_state" if membership == "invite" else "knock_state"
        stage_classic(
            bootstrap,
            {
                "next_batch": f"cold-{membership}-s1",
                "rooms": {
                    membership: {
                        ROOM: {
                            section: {
                                "events": [
                                    {
                                        "content": {
                                            "name": "Cold invited room",
                                        },
                                        "sender": BOB,
                                        "state_key": "",
                                        "type": "m.room.name",
                                    },
                                    {
                                        "content": {
                                            "displayname": "Cold invited Alice",
                                            "membership": membership,
                                        },
                                        "sender": BOB,
                                        "state_key": ACCOUNT,
                                        "type": "m.room.member",
                                    },
                                ]
                            }
                        }
                    }
                },
            },
        )
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        aggregate = loaded[1]
        assert aggregate.continuity.membership == membership
        assert aggregate.room_snapshot is not None
        assert aggregate.pending_hydration is None
        assert aggregate.continuity.hydration_id is None
        assert session._journal.load_pending_hydrations(limit=2) == ()

        leading_state = session.next_batch(max_records=1)
        assert leading_state is not None
        assert len(leading_state.records) == 1
        leading_record = leading_state.records[0]
        assert type(leading_record) is EventRecord
        assert (
            leading_record.kind,
            leading_record.room_id,
            leading_record.membership_epoch,
            leading_record.room_sequence,
            json.loads(leading_record.source_json)["type"],
        ) == (RecordKind.STATE, ROOM, 0, 0, "m.room.name")
        session.acknowledge_batch(leading_state.ref)

        lifecycle = session.next_batch(max_records=1)
        assert lifecycle is not None
        assert len(lifecycle.records) == 1
        assert lifecycle.records[0].kind is RecordKind.ROOM_LIFECYCLE
        session.acknowledge_batch(lifecycle.ref)
        state = session.next_batch(max_records=1)
        assert state is not None
        assert len(state.records) == 1
        state_record = state.records[0]
        assert type(state_record) is EventRecord
        assert (
            state_record.kind,
            state_record.room_id,
            state_record.membership_epoch,
            state_record.room_sequence,
            json.loads(state_record.source_json)["type"],
        ) == (RecordKind.STATE, ROOM, 0, 2, "m.room.member")
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_session = open_owned_session(restored_client, reopened, generation)
    try:
        assert restored_session.next_batch(max_records=1) == state
        assert restored_client.rooms == {}
        assert tuple(restored_client.invited_rooms) == (ROOM,)
    finally:
        await restored_session.close()


@pytest.mark.asyncio
async def test_owned_factory_routes_preserved_snapshot_by_current_continuity(
    tmp_path,
) -> None:
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        stage_classic(bootstrap, _rich_joined_room_body())
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        pending = session._journal.load_pending_hydrations(limit=2)
        assert len(pending) == 1
        hydrated = normalize_hydration_response(
            pending[0],
            own_user_id=ACCOUNT,
            response_body=hydration_state(),
        )
        assert session._journal.apply_hydration_result(result=hydrated) is not None
        while batch := session.next_batch():
            session.acknowledge_batch(batch.ref)
        owner_before = session._journal.load_owner()
        with session._journal._owner.read():
            loaded_before = session._journal._load_room_aggregate(owner_before, ROOM)
        assert loaded_before is not None
        previous = loaded_before[1].continuity
        assert (previous.membership, previous.membership_epoch) == ("join", 0)
        session._publish_local_membership_transition(
            operation_id=uuid4(),
            room_id=ROOM,
            previous_membership="join",
            previous_epoch=previous.membership_epoch,
            current_membership="leave",
        )
        while batch := session.next_batch():
            session.acknowledge_batch(batch.ref)
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        current = loaded[1]
        current_snapshot = current.room_snapshot
        assert current_snapshot is not None
        assert (
            current.continuity.membership,
            current.continuity.membership_epoch,
            current_snapshot.own_membership,
            current_snapshot.membership_epoch,
        ) == ("leave", 1, "leave", 1)
        stale_snapshot = replace(
            current_snapshot,
            own_membership="join",
            membership_epoch=0,
        )
        stale_aggregate = replace(current, room_snapshot=stale_snapshot)
        _reseal_local_aggregate(session, stale_aggregate)
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_session = open_owned_session(restored_client, reopened, generation)
    try:
        assert tuple(restored_client.rooms) == (ROOM,)
        assert restored_client.invited_rooms == {}
        room = restored_client.rooms[ROOM]
        assert type(room) is nio.MatrixRoom
        assert (
            _room_snapshot(
                room,
                stale_snapshot.membership_epoch,
                stale_snapshot.own_membership,
            )
            == stale_snapshot
        )
    finally:
        await restored_session.close()


@pytest.mark.asyncio
async def test_owned_materializer_commits_task4c_crypto_work_snapshot_and_compact_frame_in_one_immediate(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    sender_store: SqliteMemoryStore | None = None
    connection = session._owned_store.database.connection()
    try:
        body, sender_store = _owned_crypto_classic_body(client)
        staged = stage_classic(bootstrap, body)
        owner_before = session._journal.load_owner()
        account_before = connection.execute("SELECT account FROM accounts").fetchone()[
            0
        ]

        def callback_tripwire(*_args: object) -> None:
            raise AssertionError(
                "owned materialization invoked an application callback"
            )

        for callback_name in (
            "_on_to_device",
            "_on_invited_rooms",
            "_on_event",
            "_on_ephemeral",
            "_on_room_account_data",
            "_on_presence",
            "_on_global_account_data",
            "_on_expired_verifications",
        ):
            setattr(client, callback_name, callback_tripwire)
        client.add_event_callback(callback_tripwire, None)
        client.add_ephemeral_callback(callback_tripwire, None)
        client.add_global_account_data_callback(callback_tripwire, None)
        client.add_room_account_data_callback(callback_tripwire, None)
        client.add_to_device_callback(callback_tripwire, None)
        client.add_presence_callback(callback_tripwire, None)
        client.add_response_callback(callback_tripwire, None)

        prepared_frames: list[object] = []
        real_prepare = client._prepare_ingestion_frame

        def prepare_inside_owner_transaction(*args: object, **kwargs: object):
            assert connection.in_transaction
            assert session._journal._owner._outer_scope == "journal_write"
            prepared = real_prepare(*args, **kwargs)
            prepared_frames.append(prepared)
            return prepared

        client._prepare_ingestion_frame = prepare_inside_owner_transaction
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        connection.set_trace_callback(None)

        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            owner_before.revision + 1,
        )
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1
        assert transaction_statements.count("COMMIT") == 1
        assert not any(
            statement.startswith("BEGIN") and statement != "BEGIN IMMEDIATE"
            for statement in transaction_statements
        )
        assert client.send_count == 0
        assert len(prepared_frames) == 1

        frame_row = connection.execute(
            "SELECT source_epoch, request_id, staged_revision, payload, "
            "payload_sha256, room_materialized_revision "
            "FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()
        assert frame_row is not None
        assert frame_row[5] == result.revision
        prepared_payload = json.loads(frame_row[3])["value"]
        assert tuple(prepared_payload) == (
            "prepared_version",
            "request_cursor_json",
            "candidate_cursor_json",
            "source_sha256",
            "compatibility_token",
            "outbound_maintenance",
        )
        assert prepared_payload["prepared_version"] == 1
        assert prepared_payload["source_sha256"] == base64.b64encode(
            staged.response.source_sha256
        ).decode("ascii")
        assert "request" not in prepared_payload
        assert "response_body" not in prepared_payload
        outbound = prepared_payload["outbound_maintenance"]
        assert tuple(outbound) == ("version", "operations")
        assert outbound["version"] == 1
        operations = outbound["operations"]
        assert [operation["kind"] for operation in operations[:2]] == [
            "key_upload",
            "key_query",
        ]
        for operation in operations:
            assert tuple(operation) == (
                "kind",
                "state",
                "body_json",
                "transaction_id",
                "event_type",
                "context",
            )
            assert operation["state"] == "pending"
            body_json = base64.b64decode(operation["body_json"], validate=True)
            assert body_json == canonical_json(json.loads(body_json))

        aggregate_payload = connection.execute(
            "SELECT payload FROM NioIngestRoomAggregate "
            "WHERE account_id = ? AND room_id = ?",
            (ACCOUNT, ROOM),
        ).fetchone()
        assert aggregate_payload is not None
        aggregate = json.loads(aggregate_payload[0])["value"]
        snapshot = aggregate["room_snapshot"]
        assert (
            snapshot["room_id"],
            snapshot["own_user_id"],
            snapshot["own_membership"],
            snapshot["encrypted"],
        ) == (ROOM, ACCOUNT, "join", True)

        work_payloads = tuple(
            json.loads(row[0])["value"]
            for row in connection.execute(
                "SELECT payload FROM NioIngestWork WHERE account_id = ?",
                (ACCOUNT,),
            ).fetchall()
        )
        prepared_work = tuple(
            payload for payload in work_payloads if "preparation" in payload
        )
        assert prepared_work
        assert all(
            payload["preparation"]["record_id"] == payload["value"]["record_id"]
            for payload in prepared_work
        )
        timeline = next(
            payload
            for payload in prepared_work
            if payload["value"]["kind"] == "timeline"
        )
        clear = json.loads(base64.b64decode(timeline["value"]["clear_json"]))
        assert clear["content"]["body"] == "same-frame secret"

        assert client.store is not None
        assert client.store.load_sync_token() == "s1"
        assert ROOM in client.store.load_encrypted_rooms()
        assert (
            connection.execute("SELECT COUNT(*) FROM megolminboundsessions").fetchone()[
                0
            ]
            == 1
        )
        assert (
            connection.execute("SELECT account FROM accounts").fetchone()[0]
            != account_before
        )
    finally:
        connection.set_trace_callback(None)
        if sender_store is not None:
            sender_store.database.close()
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    try:
        reopened_owner = reopened._journal.load_owner()
        assert reopened_owner.revision == result.revision
        assert reopened._journal.load_frame(staged.frame_id) is None
        assert reopened._journal.list_frames(1) == ()
        with reopened._journal._owner.read():
            retained = reopened._journal._load_authenticated_frame_headers(
                reopened_owner
            )
            prepared_state = reopened._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                reopened_owner,
            )
        assert len(retained) == 1
        assert retained[0].frame_id == staged.frame_id
        assert retained[0].room_materialized_revision == result.revision
        assert prepared_state is not None
        assert prepared_state.source_sha256 == staged.response.source_sha256
        assert prepared_state.compatibility_token == "s1"
        replay_source = reopened._journal.load_source()
        replay = reopened._journal.stage_source_response(
            source=replay_source,
            frame=staged,
        )
        assert replay.revision == retained[0].staged_revision
        with pytest.raises(JournalIntegrityError, match="cursor"):
            reopened._journal.stage_source_response(
                source=replace(
                    replay_source,
                    cursor_json=canonical_classic_cursor(ClassicCursor("different")),
                ),
                frame=staged,
            )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_owned_materializer_replaces_exact_16mib_raw_frame_with_compact_prepared_owner(
    tmp_path,
) -> None:
    response_limit = 16 * 1024 * 1024
    event = {
        "content": {"payload": "w" * (640 * 1024)},
        "type": "org.example.large-account-data",
    }
    event_json = canonical_json(event)
    body: dict[str, object] = {
        "_padding": "",
        "account_data": {"events": [event]},
        "device_unused_fallback_key_types": [],
        "next_batch": "s1",
        "rooms": {},
    }

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        assert not olm.should_upload_keys
        assert not olm.should_query_keys
        body["device_one_time_keys_count"] = {
            "signed_curve25519": olm.account.max_one_time_keys
        }
        unpadded = canonical_json(body)
        body["_padding"] = "x" * (response_limit - len(unpadded))
        response_body = canonical_json(body)
        assert len(response_body) == response_limit

        staged = stage_classic(bootstrap, body)
        assert staged.response.response_body == response_body
        staged_row = connection.execute(
            "SELECT payload, payload_sha256 FROM NioIngestFrame "
            "WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()
        assert staged_row is not None
        staged_payload, staged_digest = staged_row
        staged_value = json.loads(staged_payload)["value"]
        assert tuple(staged_value) == (
            "normalization_version",
            "request",
            "response_body",
            "source_sha256",
        )
        assert (
            base64.b64decode(staged_value["response_body"], validate=True)
            == response_body
        )
        assert hashlib.sha256(staged_payload).digest() == staged_digest

        prepared_calls = 0
        real_prepare = client._prepare_ingestion_frame

        def capture_prepare(*args: object, **kwargs: object):
            nonlocal prepared_calls
            prepared = real_prepare(*args, **kwargs)
            prepared_calls += 1
            return prepared

        client._prepare_ingestion_frame = capture_prepare
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        connection.set_trace_callback(None)
        assert result.status is MaterializeStatus.MATERIALIZED
        assert result.frame_id == staged.frame_id
        assert prepared_calls == 1
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1
        assert transaction_statements.count("COMMIT") == 1
        assert transaction_statements.count("ROLLBACK") == 0

        prepared_row = connection.execute(
            "SELECT payload, payload_sha256, room_materialized_revision "
            "FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()
        assert prepared_row is not None
        prepared_payload, prepared_digest, materialized_revision = prepared_row
        assert materialized_revision == result.revision
        assert len(prepared_payload) <= 24 * 1024 * 1024
        assert len(prepared_payload) < len(staged_payload) // 100
        assert hashlib.sha256(prepared_payload).digest() == prepared_digest
        prepared_value = json.loads(prepared_payload)["value"]
        assert tuple(prepared_value) == (
            "prepared_version",
            "request_cursor_json",
            "candidate_cursor_json",
            "source_sha256",
            "compatibility_token",
            "outbound_maintenance",
        )
        assert "response_body" not in prepared_value
        assert "request" not in prepared_value
        assert prepared_value["outbound_maintenance"] == {
            "version": 1,
            "operations": [],
        }

        owner = session._journal.load_owner()
        with session._journal._owner.read():
            prepared_state = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner,
            )
            inventory = session._journal._load_task3_work_inventory(owner)
        assert prepared_state is not None
        assert prepared_state.outbound_maintenance.operations == ()
        assert len(inventory.work) == 1
        work = inventory.work[0]
        assert work.status == "ready"
        assert work.frame_id == staged.frame_id
        assert work.value.kind.value == "global_account_data"
        assert work.value.source_json == event_json
        assert work.value.clear_json is None
        assert work.metadata is not None
        assert work.metadata.record_id == work.value.record_id
        assert work.metadata.preparation_phase.value == "source"
        assert work.metadata.effective_event_type == event["type"]
        assert work.metadata.decryption.value == "none"
        assert work.metadata.decryption_verified is None
        assert work.metadata.decrypted_to_device_kind is None
        assert work.metadata.callback_route is not None
        assert work.metadata.callback_route.value == "global_account_data"
        assert work.canonical_size <= MaterializerLimits().max_record_canonical_bytes
        raw_work = connection.execute(
            "SELECT * FROM NioIngestWork WHERE account_id = ?",
            (ACCOUNT,),
        ).fetchone()
        assert raw_work is not None
    finally:
        connection.set_trace_callback(None)
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    try:
        reopened_connection = reopened._journal._owner.database.connection()
        reopened_owner = reopened._journal.load_owner()
        with reopened._journal._owner.read():
            reopened_prepared = reopened._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                reopened_owner,
            )
            reopened_inventory = reopened._journal._load_task3_work_inventory(
                reopened_owner
            )
        assert reopened_prepared == prepared_state
        assert reopened_inventory.work == (work,)
        assert (
            reopened_connection.execute(
                "SELECT * FROM NioIngestWork WHERE account_id = ?",
                (ACCOUNT,),
            ).fetchone()
            == raw_work
        )
        reopened_frame = reopened_connection.execute(
            "SELECT payload, payload_sha256 FROM NioIngestFrame "
            "WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()
        assert reopened_frame is not None
        assert tuple(reopened_frame) == (prepared_payload, prepared_digest)
    finally:
        reopened.close()


@pytest.mark.parametrize("extra_byte", (0, 1))
@pytest.mark.asyncio
async def test_owned_materializer_enforces_exact_authenticated_prepared_frame_capacity(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    extra_byte: int,
) -> None:
    import nio.store._sync_journal as journal_module
    import nio.store._sync_journal_rows as journal_rows_module

    frame_limit = 24 * 1024 * 1024
    recipient = "@capacity:example.org"
    recipient_device = "CAPACITY"
    event_type = "org.example.capacity"
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    session_closed = False
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        assert not olm.should_upload_keys
        assert not olm.should_query_keys
        assert not olm.wedged_devices
        assert not olm.outgoing_to_device_messages
        staged = stage_classic(
            bootstrap,
            {
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "s1",
                "rooms": {},
            },
        )
        stored_staged = session._journal.load_frame(staged.frame_id)
        assert stored_staged is not None
        owner_before = session._journal.load_owner()
        source_before = session._journal.load_source()
        raw_frame_before = connection.execute(
            "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()
        database_before = tuple(connection.iterdump())
        owner_tuple = (
            owner_before.account_id,
            owner_before.stream_id,
            owner_before.transport_kind,
        )
        source_epoch = staged.response.request.source_epoch
        request_id = staged.response.request.request_id
        request_cursor_json = staged.response.request.request_cursor_json
        candidate_cursor_json = canonical_classic_cursor(ClassicCursor("s1"))
        header = journal_rows_module._canonical_internal(
            [
                str(staged.frame_id),
                source_epoch,
                request_id,
                stored_staged.staged_revision,
            ]
        )

        def maintenance(padding_length: int, operation_type: str):
            body_json = canonical_json(
                {
                    "messages": {
                        recipient: {recipient_device: {"padding": "x" * padding_length}}
                    }
                }
            )
            transaction_id = str(
                uuid5(
                    staged.frame_id,
                    "nio.ingest.outbound-maintenance.v1:"
                    f"to-device:0:{hashlib.sha256(body_json).hexdigest()}",
                )
            )
            return journal_rows_module._OutboundMaintenance(
                (
                    journal_rows_module._OutboundOperation(
                        "to_device",
                        "pending",
                        body_json,
                        transaction_id,
                        operation_type,
                        {"subtype": "generic"},
                    ),
                )
            )

        def stored_payload(plan: object) -> tuple[bytes, bytes, bytes]:
            state = journal_rows_module._PreparedFrameState(
                request_cursor_json,
                candidate_cursor_json,
                staged.response.source_sha256,
                "s1",
                plan,
            )
            plaintext = journal_rows_module._canonical_internal(
                journal_rows_module._prepared_frame_envelope(
                    state,
                    frame_id=staged.frame_id,
                )
            )
            payload, digest = journal_rows_module._row(
                owner_tuple,
                "NioIngestFrame",
                plaintext,
                header=header,
            )
            return plaintext, payload, digest

        _, baseline_payload, _ = stored_payload(maintenance(0, event_type))
        padding_length = 3 * ((frame_limit - len(baseline_payload)) // 4)
        near_plan = maintenance(padding_length, event_type)
        _, near_payload, _ = stored_payload(near_plan)
        exact_event_type = event_type + "e" * (frame_limit - len(near_payload))
        selected_event_type = exact_event_type + "e" * extra_byte
        selected_plan = journal_rows_module._OutboundMaintenance(
            (near_plan.operations[0]._replace(event_type=selected_event_type),)
        )
        expected_plaintext, expected_payload, expected_digest = stored_payload(
            selected_plan
        )
        assert len(expected_plaintext) < frame_limit
        assert len(expected_payload) == frame_limit + extra_byte

        queued_content = {"padding": "x" * padding_length}
        queued = ToDeviceMessage(
            selected_event_type,
            recipient,
            recipient_device,
            queued_content,
        )
        olm.outgoing_to_device_messages.append(queued)
        assert canonical_json(queued.as_dict()) == selected_plan.operations[0].body_json
        expected_transaction_id = str(
            uuid5(
                staged.frame_id,
                "nio.ingest.outbound-maintenance.v1:"
                "to-device:0:"
                f"{hashlib.sha256(canonical_json(queued.as_dict())).hexdigest()}",
            )
        )
        assert selected_plan.operations[0].transaction_id == expected_transaction_id

        payload_calls = 0
        real_prepared_frame_payload = journal_module._prepared_frame_payload
        expected_payload_arguments = {
            "owner": owner_tuple,
            "frame_id": staged.frame_id,
            "source_epoch": source_epoch,
            "request_id": request_id,
            "staged_revision": stored_staged.staged_revision,
            "request_cursor_json": request_cursor_json,
            "candidate_cursor_json": candidate_cursor_json,
            "source_sha256": staged.response.source_sha256,
            "compatibility_token": "s1",
            "outbound_maintenance": selected_plan,
        }

        def capture_prepared_frame_payload(**kwargs: object):
            nonlocal payload_calls
            payload_calls += 1
            assert kwargs == expected_payload_arguments
            return real_prepared_frame_payload(**kwargs)  # type: ignore[arg-type]

        monkeypatch.setattr(
            journal_module,
            "_prepared_frame_payload",
            capture_prepared_frame_payload,
        )

        prepared_calls = 0
        saved_prepare_args: tuple[object, ...] | None = None
        real_prepare = client._prepare_ingestion_frame

        def capture_prepare(*args: object, **kwargs: object):
            nonlocal prepared_calls, saved_prepare_args
            prepared = real_prepare(*args, **kwargs)
            prepared_calls += 1
            saved_prepare_args = args
            return prepared

        client._prepare_ingestion_frame = capture_prepare
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        if extra_byte == 0:
            result = session._materialize_oldest_frame(limits=MaterializerLimits())
        else:
            with pytest.raises(
                JournalIntegrityError,
                match="^prepared frame envelope exceeds 24 MiB$",
            ):
                session._materialize_oldest_frame(limits=MaterializerLimits())
            result = None
        connection.set_trace_callback(None)
        assert prepared_calls == 1
        assert payload_calls == 1
        assert saved_prepare_args is not None
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1

        if extra_byte == 0:
            assert result is not None
            assert result.status is MaterializeStatus.MATERIALIZED
            assert result.frame_id == staged.frame_id
            assert transaction_statements.count("COMMIT") == 1
            assert transaction_statements.count("ROLLBACK") == 0
            frame_row = connection.execute(
                "SELECT payload, payload_sha256, room_materialized_revision "
                "FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
                (ACCOUNT, str(staged.frame_id)),
            ).fetchone()
            assert frame_row is not None
            assert tuple(frame_row[:2]) == (expected_payload, expected_digest)
            assert frame_row[2] == result.revision
            assert len(frame_row[0]) == frame_limit
            assert hashlib.sha256(frame_row[0]).digest() == frame_row[1]
            owner = session._journal.load_owner()
            with session._journal._owner.read():
                prepared = session._journal._load_prepared_frame_with_owner(
                    staged.frame_id,
                    owner,
                )
            assert prepared is not None
            assert prepared.outbound_maintenance == selected_plan
            operation = prepared.outbound_maintenance.operations[0]
            assert operation.kind == "to_device"
            assert operation.state == "pending"
            assert operation.body_json == canonical_json(queued.as_dict())
            assert operation.transaction_id == expected_transaction_id
            assert operation.event_type == selected_event_type
            assert operation.context == {"subtype": "generic"}
            assert olm.outgoing_to_device_messages == [queued]
            await session.close()
            session_closed = True

            reopened = reopen_owned_bootstrap(tmp_path, generation)
            try:
                reopened_owner = reopened._journal.load_owner()
                with reopened._journal._owner.read():
                    reopened_prepared = (
                        reopened._journal._load_prepared_frame_with_owner(
                            staged.frame_id,
                            reopened_owner,
                        )
                    )
                assert reopened_prepared == prepared
            finally:
                reopened.close()
        else:
            assert result is None
            assert transaction_statements.count("COMMIT") == 0
            assert transaction_statements.count("ROLLBACK") == 1
            assert session._journal.load_owner() == owner_before
            assert session._journal.load_source() == source_before
            assert session._journal.load_frame(staged.frame_id) == stored_staged
            assert (
                connection.execute(
                    "SELECT * FROM NioIngestFrame "
                    "WHERE account_id = ? AND frame_id = ?",
                    (ACCOUNT, str(staged.frame_id)),
                ).fetchone()
                == raw_frame_before
            )
            assert tuple(connection.iterdump()) == database_before
            assert olm.outgoing_to_device_messages == [queued]

            poison_message = (
                "owned ingestion session is poisoned; reopen with a fresh client"
            )

            def assert_poisoned(operation: Callable[[], object]) -> None:
                with pytest.raises(LocalProtocolError) as failure:
                    operation()
                assert type(failure.value) is LocalProtocolError
                assert str(failure.value) == poison_message
                assert failure.value.__cause__ is None

            poisoned_trace: list[str] = []
            connection.set_trace_callback(poisoned_trace.append)
            assert_poisoned(
                lambda: session._materialize_oldest_frame(limits=MaterializerLimits())
            )
            assert_poisoned(session.next_batch)
            assert_poisoned(
                lambda: client._prepare_ingestion_frame(
                    *saved_prepare_args  # type: ignore[arg-type]
                )
            )
            connection.set_trace_callback(None)
            assert poisoned_trace == []
            assert tuple(connection.iterdump()) == database_before
            await session.close()
            session_closed = True

            reopened = reopen_owned_bootstrap(tmp_path, generation)
            try:
                assert reopened._journal.load_owner().revision == owner_before.revision
                assert reopened._journal.load_source() == source_before
                assert reopened._journal.load_frame(staged.frame_id) == stored_staged
                assert (
                    reopened._journal._owner.database.connection()
                    .execute(
                        "SELECT * FROM NioIngestFrame "
                        "WHERE account_id = ? AND frame_id = ?",
                        (ACCOUNT, str(staged.frame_id)),
                    )
                    .fetchone()
                    == raw_frame_before
                )
            finally:
                reopened.close()
    finally:
        if not session_closed:
            connection.set_trace_callback(None)
            await session.close()


@pytest.mark.asyncio
async def test_owned_progress_generation_excludes_enqueue_only_membership_command(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    transition = asyncio.create_task(
        session._run_local_membership_transition(
            operation_id=uuid4(),
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        )
    )
    try:
        durable_before = session._durable_progress_generation
        waiter_before = session._progress_generation
        await asyncio.sleep(0)

        assert session._progress_generation == waiter_before + 1
        assert session._durable_progress_generation == durable_before
        assert session._journal._load_pending_local_membership_intents(limit=1) == ()
    finally:
        transition.cancel()
        await asyncio.gather(transition, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_owned_durable_progress_generation_advances_after_commits(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    event_source = {
        "content": {"value": "durable-progress"},
        "type": "org.example.task9.progress",
    }
    try:
        stage_classic(
            bootstrap,
            {
                "account_data": {"events": [event_source]},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": client.olm.account.max_one_time_keys,
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "s1",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": []},
            },
        )
        before = session._durable_progress_generation
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        assert session._durable_progress_generation == before + 1
        batch = session.next_batch(max_records=1)
        assert batch is not None
        session.acknowledge_batch(batch.ref)
        assert session._durable_progress_generation == before + 2
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_local_membership_intent_is_durable_before_http(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    operation_id = uuid4()
    owner_before = session._journal.load_owner()
    source_before = session._journal.load_source()
    ordinary_before = tuple(
        statement for statement in connection.iterdump() if "NioIngest" not in statement
    )
    assert (
        connection.execute("SELECT COUNT(*) FROM NioIngestRoomAggregate").fetchone()[0]
        == 0
    )
    assert connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 0
    assert connection.execute("SELECT COUNT(*) FROM NioIngestFrame").fetchone()[0] == 0
    try:
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        committed = session._journal._record_local_membership_intent(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        )
        connection.set_trace_callback(None)
        assert committed == CommitResult(owner_before.revision + 1)
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1
        assert transaction_statements.count("COMMIT") == 1
        assert transaction_statements.count("ROLLBACK") == 0
        assert (
            sum(
                statement.startswith("UPDATE NIOINGESTMETA")
                for statement in transaction_statements
            )
            == 1
        )
        assert (
            sum(
                statement.startswith("INSERT INTO NIOINGESTROOMAGGREGATE")
                for statement in transaction_statements
            )
            == 1
        )
        assert not any("NIOINGESTWORK" in statement for statement in statements)
        assert not any("NIOINGESTFRAME" in statement for statement in statements)

        owner_after = session._journal.load_owner()
        assert owner_after.revision == committed.revision
        assert session._journal.load_source() == source_before
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner_after, ROOM)
            pending = session._journal._load_pending_local_membership_intents(limit=2)
            inventory = session._journal._load_task3_work_inventory(owner_after)
        assert loaded is not None
        aggregate = loaded[1]
        assert aggregate.continuity.room_id == ROOM
        assert aggregate.continuity.membership == "leave"
        assert aggregate.continuity.membership_epoch == 0
        assert aggregate.continuity.baseline is None
        assert aggregate.continuity.gap is None
        assert aggregate.continuity.hydration_id is None
        assert aggregate.next_room_sequence == 0
        assert aggregate.updated_revision == committed.revision
        assert aggregate.pending_hydration is None
        assert aggregate.room_snapshot is None
        intent = aggregate.pending_local_membership
        assert (
            intent.operation_id,
            intent.previous_membership,
            intent.previous_epoch,
            intent.current_membership,
        ) == (operation_id, "leave", 0, "join")
        assert len(pending) == 1
        assert (
            pending[0].room_id,
            pending[0].continuity,
            pending[0].intent,
        ) == (ROOM, aggregate.continuity, intent)
        assert inventory.work == ()
        raw_aggregate = connection.execute(
            "SELECT * FROM NioIngestRoomAggregate WHERE room_id = ?",
            (ROOM,),
        ).fetchone()
        assert raw_aggregate is not None
        assert raw_aggregate[3] == "local_membership"
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 0
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestFrame").fetchone()[0] == 0
        )
        assert (
            tuple(
                statement
                for statement in connection.iterdump()
                if "NioIngest" not in statement
            )
            == ordinary_before
        )
    finally:
        connection.set_trace_callback(None)
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    fresh_connection = fresh_session._owned_store.database.connection()
    try:
        reopened_statements: list[str] = []
        fresh_connection.set_trace_callback(reopened_statements.append)
        pending_after_reopen = (
            fresh_session._journal._load_pending_local_membership_intents(limit=2)
        )
        fresh_connection.set_trace_callback(None)
        assert len(pending_after_reopen) == 1
        assert (
            pending_after_reopen[0].room_id,
            pending_after_reopen[0].continuity,
            pending_after_reopen[0].intent,
        ) == (ROOM, aggregate.continuity, intent)
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in reopened_statements
        )
        assert (
            fresh_connection.execute(
                "SELECT * FROM NioIngestRoomAggregate WHERE room_id = ?",
                (ROOM,),
            ).fetchone()
            == raw_aggregate
        )
    finally:
        fresh_connection.set_trace_callback(None)
        await fresh_session.close()


def test_owned_reopen_rejects_multiple_authenticated_local_membership_intents(
    tmp_path,
) -> None:
    from nio.store._sync_journal_preflight import _canonical_internal
    from nio.store._sync_journal_rows import _canonical_room_aggregate_plaintext

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    journal = bootstrap._journal
    operation_id = uuid4()
    journal._record_local_membership_intent(
        operation_id=operation_id,
        room_id=ROOM,
        previous_membership="leave",
        previous_epoch=0,
        current_membership="join",
    )
    owner = journal.load_owner()
    with journal._owner.read():
        loaded = journal._load_room_aggregate(owner, ROOM)
    assert loaded is not None
    aggregate = loaded[1]
    intent = aggregate.pending_local_membership
    assert intent is not None
    other_room = "!second-local-intent:example.org"
    second = replace(
        aggregate,
        continuity=replace(aggregate.continuity, room_id=other_room),
        pending_local_membership=type(intent)(
            uuid4(),
            "leave",
            0,
            "join",
        ),
    )
    plaintext = _canonical_room_aggregate_plaintext(second)
    payload, digest = journal._payload(
        owner,
        "NioIngestRoomAggregate",
        plaintext,
        header=_canonical_internal(
            [other_room, second.updated_revision, "local_membership"]
        ),
    )
    connection = journal._owner.database.connection()
    connection.execute(
        "INSERT INTO NioIngestRoomAggregate("
        "account_id, room_id, updated_revision, intent_kind, payload, "
        "payload_sha256) VALUES (?, ?, ?, ?, ?, ?)",
        (
            ACCOUNT,
            other_room,
            second.updated_revision,
            "local_membership",
            payload,
            digest,
        ),
    )
    connection.commit()
    bootstrap.close()

    with pytest.raises(
        JournalIntegrityError,
        match="multiple local membership intents are pending",
    ):
        reopen_owned_bootstrap(tmp_path, generation)


@pytest.mark.parametrize("target_membership", ("join", "leave"))
@pytest.mark.asyncio
async def test_owned_local_membership_http_uses_only_bearer_authentication(
    tmp_path,
    target_membership: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    previous_membership = "leave"
    response_body = canonical_json({"room_id": ROOM})
    if target_membership == "leave":
        operation_id, _commit, _aggregate, _work = _publish_fresh_local_join(session)
        batch = session.next_batch()
        assert batch is not None
        assert batch.records[0].origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            operation_id,
        )
        session.acknowledge_batch(batch.ref)
        previous_membership = "join"
        response_body = canonical_json({})

    client.responses.append(FakeResponse(200, response_body))
    transition = asyncio.create_task(
        session._run_local_membership_transition(
            operation_id=uuid4(),
            room_id=ROOM,
            previous_membership=previous_membership,
            previous_epoch=0,
            current_membership=target_membership,
        )
    )
    try:
        await asyncio.sleep(0)
        assert await session._advance_local_membership_intent() is True
        assert await asyncio.wait_for(transition, timeout=1) is True
        assert len(client.send_calls) == 1
        (method, path, body, headers, trace_context, timeout), kwargs = (
            client.send_calls[0]
        )
        token_path = (
            _bearer_only_path(nio.Api.join("secret-token", ROOM))
            if target_membership == "join"
            else _bearer_only_path(nio.Api.room_leave("secret-token", ROOM))
        )
        assert (method, path, body, headers, trace_context, timeout, kwargs) == (
            "POST",
            token_path.partition("?")[0],
            None,
            {"Authorization": "Bearer secret-token"},
            None,
            45.0,
            {},
        )
        assert "access_token" not in path
    finally:
        if not transition.done():
            transition.cancel()
        await asyncio.gather(transition, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_owned_runner_restarts_local_membership_intent_before_source_http(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    operation_id = uuid4()
    source_before = session._journal.load_source()
    requests: list[object] = []
    request_entered = asyncio.Event()
    request_cancelled = asyncio.Event()
    runner: asyncio.Task[None] | None = None
    transition: asyncio.Task[bool] | None = None

    async def hold_local_request(request, **kwargs: object) -> NetworkResult:
        assert kwargs == {"body_disconnect_retry": True}
        assert (
            request.method,
            request.path,
            request.query,
            request.body,
            request.timeout_ms,
            request.request_cursor_json,
        ) == (
            "POST",
            _bearer_only_path(nio.Api.join("secret-token", ROOM)),
            (),
            None,
            30_000,
            source_before.cursor_json,
        )
        assert connection.in_transaction is False
        pending = session._journal._load_pending_local_membership_intents(limit=2)
        assert len(pending) == 1
        assert pending[0].room_id == ROOM
        assert pending[0].intent.operation_id == operation_id
        assert session._journal.load_source() == source_before
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 0
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestFrame").fetchone()[0] == 0
        )
        requests.append(request)
        request_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            request_cancelled.set()
            raise
        raise AssertionError("unreachable")

    session._request = hold_local_request  # type: ignore[method-assign]
    try:
        transition = asyncio.create_task(
            session._run_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        )
        await asyncio.sleep(0)
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM NioIngestRoomAggregate"
            ).fetchone()[0]
            == 0
        )
        runner = asyncio.create_task(session.run())
        await asyncio.wait_for(request_entered.wait(), timeout=1)
        assert len(requests) == 1
        assert transition.done() is False
        await session.close()
        await asyncio.wait_for(request_cancelled.wait(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await runner
        with pytest.raises(asyncio.CancelledError):
            await transition
    finally:
        if runner is not None and not runner.done():
            runner.cancel()
        if transition is not None and not transition.done():
            transition.cancel()
        await asyncio.gather(
            *(task for task in (runner, transition) if task is not None),
            return_exceptions=True,
        )
        if not session._closed:
            await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    fresh_connection = fresh_session._owned_store.database.connection()
    fresh_entered = asyncio.Event()
    fresh_cancelled = asyncio.Event()
    fresh_requests: list[object] = []
    fresh_runner: asyncio.Task[None] | None = None

    async def hold_reconstructed_request(request, **kwargs: object) -> NetworkResult:
        assert kwargs == {"body_disconnect_retry": True}
        assert requests == [request]
        assert fresh_connection.in_transaction is False
        pending = fresh_session._journal._load_pending_local_membership_intents(limit=2)
        assert len(pending) == 1
        assert pending[0].room_id == ROOM
        assert pending[0].intent.operation_id == operation_id
        assert fresh_session._journal.load_source() == source_before
        assert (
            fresh_connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
            == 0
        )
        assert (
            fresh_connection.execute("SELECT COUNT(*) FROM NioIngestFrame").fetchone()[
                0
            ]
            == 0
        )
        fresh_requests.append(request)
        fresh_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            fresh_cancelled.set()
            raise
        raise AssertionError("unreachable")

    fresh_session._request = hold_reconstructed_request  # type: ignore[method-assign]
    try:
        fresh_runner = asyncio.create_task(fresh_session.run())
        await asyncio.wait_for(fresh_entered.wait(), timeout=1)
        assert len(fresh_requests) == 1
        await fresh_session.close()
        await asyncio.wait_for(fresh_cancelled.wait(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await fresh_runner
    finally:
        if fresh_runner is not None and not fresh_runner.done():
            fresh_runner.cancel()
        await asyncio.gather(
            *(task for task in (fresh_runner,) if task is not None),
            return_exceptions=True,
        )
        if not fresh_session._closed:
            await fresh_session.close()


@pytest.mark.asyncio
async def test_owned_local_membership_success_waits_for_work_ack_before_source(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    operation_id = uuid4()
    source_before = session._journal.load_source()
    local_requests: list[object] = []
    source_requests: list[object] = []
    source_entered = asyncio.Event()
    source_cancelled = asyncio.Event()
    runner: asyncio.Task[None] | None = None
    transition: asyncio.Task[bool] | None = None

    async def respond(request, **kwargs: object) -> NetworkResult:
        if request.path == _bearer_only_path(nio.Api.join("secret-token", ROOM)):
            assert kwargs == {"body_disconnect_retry": True}
            assert session._journal.load_source() == source_before
            pending = session._journal._load_pending_local_membership_intents(limit=2)
            assert len(pending) == 1
            assert pending[0].intent.operation_id == operation_id
            assert connection.in_transaction is False
            local_requests.append(request)
            return session._network_result(
                request,
                200,
                canonical_json({"room_id": ROOM}),
            )
        assert kwargs == {}
        assert local_requests
        assert session._journal._load_pending_local_membership_intents(limit=2) == ()
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner)
        assert inventory.work == ()
        assert connection.in_transaction is False
        source_requests.append(request)
        source_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            source_cancelled.set()
            raise
        raise AssertionError("unreachable")

    session._request = respond  # type: ignore[method-assign]
    try:
        transition = asyncio.create_task(
            session._run_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        )
        runner = asyncio.create_task(session.run())
        assert await asyncio.wait_for(transition, timeout=1) is True
        assert len(local_requests) == 1
        assert source_requests == []
        assert session._journal.load_source() == source_before

        owner_after = session._journal.load_owner()
        assert owner_after.revision == 2
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner_after, ROOM)
            inventory = session._journal._load_task3_work_inventory(owner_after)
        assert loaded is not None
        aggregate = loaded[1]
        assert aggregate.continuity.membership == "join"
        assert aggregate.continuity.membership_epoch == 0
        assert aggregate.pending_hydration is None
        assert aggregate.pending_local_membership is None
        assert aggregate.next_room_sequence == 1
        assert aggregate.updated_revision == 2
        assert aggregate.room_snapshot is None
        assert len(inventory.work) == 1
        work = inventory.work[0]
        assert work.status == "ready"
        assert work.frame_id == operation_id
        assert work.metadata is None
        assert work.value.kind is RecordKind.ROOM_LIFECYCLE
        assert work.value.origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            operation_id,
        )
        assert work.value.room_id == ROOM
        assert work.value.membership_epoch == 0
        assert work.value.room_sequence == 0

        waiting_statements: list[str] = []
        connection.set_trace_callback(waiting_statements.append)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        connection.set_trace_callback(None)
        assert source_requests == []
        assert not any(
            statement.strip().upper().startswith("BEGIN")
            for statement in waiting_statements
        )

        batch = session.next_batch()
        assert batch is not None
        assert batch.records == (work.value,)
        await session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        await asyncio.wait_for(source_entered.wait(), timeout=1)
        assert len(source_requests) == 1
        await session.close()
        await asyncio.wait_for(source_cancelled.wait(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await runner
    finally:
        if not session._closed:
            connection.set_trace_callback(None)
        if runner is not None and not runner.done():
            runner.cancel()
        if transition is not None and not transition.done():
            transition.cancel()
        await asyncio.gather(
            *(task for task in (runner, transition) if task is not None),
            return_exceptions=True,
        )
        if not session._closed:
            await session.close()


@pytest.mark.asyncio
async def test_owned_local_command_reconciles_when_lifecycle_reaches_target(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    stale_operation = uuid4()
    transition: asyncio.Task[bool] | None = None
    waiting: asyncio.Task[bool] | None = None
    leave: asyncio.Task[bool] | None = None
    try:
        transition = asyncio.create_task(
            session._run_local_membership_transition(
                operation_id=stale_operation,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        )
        await asyncio.sleep(0)
        assert transition.done() is False

        winning_operation, _commit, _aggregate, winning_work = (
            _publish_fresh_local_join(session)
        )
        assert winning_operation != stale_operation
        waiting = asyncio.create_task(session._advance_local_membership_intent())
        await asyncio.sleep(0)
        assert waiting.done() is False

        winning_batch = session.next_batch()
        assert winning_batch is not None
        assert winning_batch.records == (winning_work.value,)
        await session._settle_batch(
            winning_batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        assert await asyncio.wait_for(waiting, timeout=1) is True
        assert transition.done() is False

        graph_at_target = tuple(connection.iterdump())
        statements: list[str] = []
        requests: list[object] = []

        async def forbid_request(request, **kwargs: object) -> NetworkResult:
            requests.append((request, kwargs))
            raise AssertionError("fulfilled local membership command reached HTTP")

        session._request = forbid_request  # type: ignore[method-assign]
        connection.set_trace_callback(statements.append)
        assert await session._advance_local_membership_intent() is True
        connection.set_trace_callback(None)
        assert await asyncio.wait_for(transition, timeout=1) is True
        assert requests == []
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in statements
        )
        assert tuple(connection.iterdump()) == graph_at_target
        assert session._journal._load_pending_local_membership_intents(limit=2) == ()
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
            inventory = session._journal._load_task3_work_inventory(owner)
        assert loaded is not None
        assert loaded[1].continuity.membership == "join"
        assert loaded[1].continuity.membership_epoch == 0
        assert loaded[1].pending_local_membership is None
        assert inventory.work == ()
        assert session._terminal is None

        leave_requests: list[object] = []

        async def leave_success(request, **kwargs: object) -> NetworkResult:
            assert request.path == _bearer_only_path(
                nio.Api.room_leave("secret-token", ROOM)
            )
            assert kwargs == {"body_disconnect_retry": True}
            leave_requests.append(request)
            return session._network_result(request, 200, canonical_json({}))

        session._request = leave_success  # type: ignore[method-assign]
        leave = asyncio.create_task(
            session._run_local_membership_transition(
                operation_id=uuid4(),
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=0,
                current_membership="leave",
            )
        )
        await asyncio.sleep(0)
        assert await session._advance_local_membership_intent() is True
        assert await asyncio.wait_for(leave, timeout=1) is True
        assert len(leave_requests) == 1
    finally:
        connection.set_trace_callback(None)
        await asyncio.sleep(0)
        for task in (transition, waiting, leave):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (transition, waiting, leave) if task is not None),
            return_exceptions=True,
        )
        if not session._closed:
            await session.close()


@pytest.mark.parametrize("membership", ("invite", "knock"))
@pytest.mark.asyncio
async def test_owned_local_join_accepts_prejoin_transport_predecessor(
    tmp_path,
    membership: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    operation_id = uuid4()
    transition: asyncio.Task[bool] | None = None
    try:
        state_key = "invite_state" if membership == "invite" else "knock_state"
        staged = stage_classic(
            bootstrap,
            {
                "device_one_time_keys_count": {
                    "signed_curve25519": client.olm.account.max_one_time_keys,
                },
                "next_batch": f"{membership}-before-local-join",
                "rooms": {
                    membership: {
                        ROOM: {
                            state_key: {
                                "events": [
                                    {
                                        "content": {
                                            "displayname": "Prejoin Alice",
                                            "membership": membership,
                                        },
                                        "sender": BOB,
                                        "state_key": ACCOUNT,
                                        "type": "m.room.member",
                                    }
                                ]
                            }
                        }
                    }
                },
            },
        )
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        while batch := session.next_batch():
            session.acknowledge_batch(batch.ref)
        assert await session._advance_blocked_frame(staged.frame_id)

        owner_before = session._journal.load_owner()
        with session._journal._owner.read():
            loaded_before = session._journal._load_room_aggregate(owner_before, ROOM)
            inventory_before = session._journal._load_task3_work_inventory(owner_before)
            headers_before = session._journal._load_authenticated_frame_headers(
                owner_before
            )
        assert loaded_before is not None
        assert loaded_before[1].continuity.membership == membership
        assert loaded_before[1].continuity.membership_epoch == 0
        assert loaded_before[1].pending_hydration is None
        assert loaded_before[1].pending_local_membership is None
        assert inventory_before.work == ()
        assert headers_before == ()
        assert tuple(client.invited_rooms) == (ROOM,)

        connection = session._owned_store.database.connection()
        graph_before_direct_publish = tuple(connection.iterdump())
        with pytest.raises(
            JournalConflictError,
            match="local membership transition does not match current state",
        ):
            session._publish_local_membership_transition(
                operation_id=uuid4(),
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        assert tuple(connection.iterdump()) == graph_before_direct_publish

        requests: list[object] = []

        async def join_success(request, **kwargs: object) -> NetworkResult:
            assert request.path == _bearer_only_path(nio.Api.join("secret-token", ROOM))
            assert kwargs == {"body_disconnect_retry": True}
            requests.append(request)
            return session._network_result(
                request,
                200,
                canonical_json({"room_id": ROOM}),
            )

        session._request = join_success  # type: ignore[method-assign]
        transition = asyncio.create_task(
            session._run_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        )
        await asyncio.sleep(0)
        assert await session._advance_local_membership_intent() is True
        assert await asyncio.wait_for(transition, timeout=1) is True
        assert len(requests) == 1

        owner_after = session._journal.load_owner()
        with session._journal._owner.read():
            loaded_after = session._journal._load_room_aggregate(owner_after, ROOM)
            inventory_after = session._journal._load_task3_work_inventory(owner_after)
        assert loaded_after is not None
        assert loaded_after[1].continuity.membership == "join"
        assert loaded_after[1].continuity.membership_epoch == 0
        assert loaded_after[1].pending_local_membership is None
        assert loaded_after[1].room_snapshot is not None
        assert loaded_after[1].room_snapshot.own_membership == "join"
        assert len(inventory_after.work) == 1
        work = inventory_after.work[0]
        assert work.value.origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            operation_id,
        )
        assert work.value.source_json == _local_source("leave", 0, "join", 0)

        batch = session.next_batch()
        assert batch is not None
        assert batch.records == (work.value,)
        await session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        assert tuple(client.rooms) == (ROOM,)
        assert client.invited_rooms == {}
    finally:
        await asyncio.sleep(0)
        if transition is not None and not transition.done():
            transition.cancel()
        await asyncio.gather(
            *(task for task in (transition,) if task is not None),
            return_exceptions=True,
        )
        if not session._closed:
            await session.close()


@pytest.mark.asyncio
async def test_owned_prejoin_local_join_intent_restarts_without_duplicate_work(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    operation_id = uuid4()
    try:
        staged = stage_classic(
            bootstrap,
            {
                "device_one_time_keys_count": {
                    "signed_curve25519": client.olm.account.max_one_time_keys,
                },
                "next_batch": "invited-intent-before-restart",
                "rooms": {
                    "invite": {
                        ROOM: {
                            "invite_state": {
                                "events": [
                                    {
                                        "content": {
                                            "displayname": "Restarted Alice",
                                            "membership": "invite",
                                        },
                                        "sender": BOB,
                                        "state_key": ACCOUNT,
                                        "type": "m.room.member",
                                    }
                                ]
                            }
                        }
                    }
                },
            },
        )
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        while batch := session.next_batch():
            session.acknowledge_batch(batch.ref)
        assert await session._advance_blocked_frame(staged.frame_id)

        committed = session._journal._record_local_membership_intent(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        )
        pending_before = session._journal._load_pending_local_membership_intents(
            limit=2
        )
        assert committed == CommitResult(session._journal.load_owner().revision)
        assert len(pending_before) == 1
        assert pending_before[0].continuity.membership == "invite"
        assert pending_before[0].continuity.membership_epoch == 0
        assert pending_before[0].intent.operation_id == operation_id
        raw_aggregate_before = (
            session._owned_store.database.connection()
            .execute(
                "SELECT * FROM NioIngestRoomAggregate WHERE room_id = ?",
                (ROOM,),
            )
            .fetchone()
        )
        assert raw_aggregate_before is not None
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    requests: list[object] = []

    async def join_success(request, **kwargs: object) -> NetworkResult:
        assert request.path == _bearer_only_path(nio.Api.join("secret-token", ROOM))
        assert kwargs == {"body_disconnect_retry": True}
        requests.append(request)
        return fresh_session._network_result(
            request,
            200,
            canonical_json({"room_id": ROOM}),
        )

    fresh_session._request = join_success  # type: ignore[method-assign]
    try:
        pending_after = fresh_session._journal._load_pending_local_membership_intents(
            limit=2
        )
        assert pending_after == pending_before
        assert (
            fresh_session._owned_store.database.connection()
            .execute(
                "SELECT * FROM NioIngestRoomAggregate WHERE room_id = ?",
                (ROOM,),
            )
            .fetchone()
            == raw_aggregate_before
        )
        assert tuple(fresh_client.invited_rooms) == (ROOM,)

        assert await fresh_session._advance_local_membership_intent() is True
        assert len(requests) == 1
        owner_after = fresh_session._journal.load_owner()
        with fresh_session._journal._owner.read():
            loaded_after = fresh_session._journal._load_room_aggregate(
                owner_after,
                ROOM,
            )
            inventory_after = fresh_session._journal._load_task3_work_inventory(
                owner_after
            )
        assert loaded_after is not None
        assert loaded_after[1].continuity.membership == "join"
        assert loaded_after[1].continuity.membership_epoch == 0
        assert loaded_after[1].pending_local_membership is None
        assert len(inventory_after.work) == 1
        local_work = inventory_after.work[0]
        assert local_work.value.origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            operation_id,
        )
        assert local_work.value.source_json == _local_source("leave", 0, "join", 0)

        graph_after = tuple(fresh_session._owned_store.database.connection().iterdump())
        assert fresh_session._publish_local_membership_transition(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        ) == CommitResult(owner_after.revision)
        assert (
            tuple(fresh_session._owned_store.database.connection().iterdump())
            == graph_after
        )

        batch = fresh_session.next_batch()
        assert batch is not None
        assert batch.records == (local_work.value,)
        await fresh_session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        assert fresh_session.next_batch() is None
        assert tuple(fresh_client.rooms) == (ROOM,)
        assert fresh_client.invited_rooms == {}
    finally:
        await fresh_session.close()


@pytest.mark.asyncio
async def test_owned_runner_orders_duplicate_leave_and_rejoin_behind_local_work(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    seed_operation, _seed_commit, _seed_aggregate, _seed_work = (
        _publish_fresh_local_join(session)
    )
    seed_batch = session.next_batch()
    assert seed_batch is not None
    assert seed_batch.records[0].origin == SystemOrigin(
        SystemOriginKind.MEMBERSHIP_CHANGE,
        seed_operation,
    )
    session.acknowledge_batch(seed_batch.ref)

    leave_operation = uuid4()
    rejoin_operation = uuid4()
    local_paths: list[str] = []
    source_paths: list[str] = []
    source_entered = asyncio.Event()
    source_cancelled = asyncio.Event()
    runner: asyncio.Task[None] | None = None
    leave_first: asyncio.Task[bool] | None = None
    leave_second: asyncio.Task[bool] | None = None
    leave_replay: asyncio.Task[bool] | None = None
    rejoin: asyncio.Task[bool] | None = None

    async def respond(request, **kwargs: object) -> NetworkResult:
        assert kwargs == {"body_disconnect_retry": True} or kwargs == {}
        leave_path = _bearer_only_path(nio.Api.room_leave("secret-token", ROOM))
        join_path = _bearer_only_path(nio.Api.join("secret-token", ROOM))
        if request.path == leave_path:
            assert kwargs == {"body_disconnect_retry": True}
            assert local_paths == []
            local_paths.append(request.path)
            return session._network_result(request, 200, canonical_json({}))
        if request.path == join_path:
            assert kwargs == {"body_disconnect_retry": True}
            assert local_paths == [leave_path]
            local_paths.append(request.path)
            return session._network_result(
                request,
                200,
                canonical_json({"room_id": ROOM}),
            )
        assert kwargs == {}
        assert local_paths == [leave_path, join_path]
        source_paths.append(request.path)
        source_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            source_cancelled.set()
            raise
        raise AssertionError("unreachable")

    session._request = respond  # type: ignore[method-assign]
    try:
        leave_arguments = {
            "operation_id": leave_operation,
            "room_id": ROOM,
            "previous_membership": "join",
            "previous_epoch": 0,
            "current_membership": "leave",
        }
        leave_first = asyncio.create_task(
            session._run_local_membership_transition(**leave_arguments)
        )
        leave_second = asyncio.create_task(
            session._run_local_membership_transition(**leave_arguments)
        )
        await asyncio.sleep(0)
        with pytest.raises(
            LocalProtocolError,
            match="another local membership command is already queued",
        ):
            await session._run_local_membership_transition(
                operation_id=uuid4(),
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=0,
                current_membership="leave",
            )

        runner = asyncio.create_task(session.run())
        assert await asyncio.wait_for(leave_first, timeout=1) is True
        assert await asyncio.wait_for(leave_second, timeout=1) is True
        assert local_paths == [
            _bearer_only_path(nio.Api.room_leave("secret-token", ROOM))
        ]

        owner_after_leave = session._journal.load_owner()
        with session._journal._owner.read():
            loaded_leave = session._journal._load_room_aggregate(
                owner_after_leave,
                ROOM,
            )
            leave_inventory = session._journal._load_task3_work_inventory(
                owner_after_leave
            )
        assert loaded_leave is not None
        assert loaded_leave[1].continuity.membership == "leave"
        assert loaded_leave[1].continuity.membership_epoch == 1
        assert loaded_leave[1].pending_local_membership is None
        assert len(leave_inventory.work) == 1
        leave_work = leave_inventory.work[0]
        assert leave_work.value.origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            leave_operation,
        )

        leave_replay = asyncio.create_task(
            session._run_local_membership_transition(**leave_arguments)
        )
        assert await asyncio.wait_for(leave_replay, timeout=1) is True
        assert local_paths == [
            _bearer_only_path(nio.Api.room_leave("secret-token", ROOM))
        ]

        rejoin = asyncio.create_task(
            session._run_local_membership_transition(
                operation_id=rejoin_operation,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=1,
                current_membership="join",
            )
        )
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert rejoin.done() is False
        assert local_paths == [
            _bearer_only_path(nio.Api.room_leave("secret-token", ROOM))
        ]
        assert source_paths == []

        leave_batch = session.next_batch()
        assert leave_batch is not None
        assert leave_batch.records == (leave_work.value,)
        await session._settle_batch(
            leave_batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        assert await asyncio.wait_for(rejoin, timeout=1) is True
        assert local_paths == [
            _bearer_only_path(nio.Api.room_leave("secret-token", ROOM)),
            _bearer_only_path(nio.Api.join("secret-token", ROOM)),
        ]
        assert source_paths == []

        owner_after_rejoin = session._journal.load_owner()
        with session._journal._owner.read():
            loaded_rejoin = session._journal._load_room_aggregate(
                owner_after_rejoin,
                ROOM,
            )
            rejoin_inventory = session._journal._load_task3_work_inventory(
                owner_after_rejoin
            )
        assert loaded_rejoin is not None
        assert loaded_rejoin[1].continuity.membership == "join"
        assert loaded_rejoin[1].continuity.membership_epoch == 1
        assert loaded_rejoin[1].pending_local_membership is None
        assert len(rejoin_inventory.work) == 1
        rejoin_work = rejoin_inventory.work[0]
        assert rejoin_work.value.origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            rejoin_operation,
        )

        rejoin_batch = session.next_batch()
        assert rejoin_batch is not None
        assert rejoin_batch.records == (rejoin_work.value,)
        await session._settle_batch(
            rejoin_batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        await asyncio.wait_for(source_entered.wait(), timeout=1)
        assert len(source_paths) == 1
        await session.close()
        await asyncio.wait_for(source_cancelled.wait(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await runner
    finally:
        for task in (runner, leave_first, leave_second, leave_replay, rejoin):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(
                task
                for task in (
                    runner,
                    leave_first,
                    leave_second,
                    leave_replay,
                    rejoin,
                )
                if task is not None
            ),
            return_exceptions=True,
        )
        if not session._closed:
            await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "fate",
    ("retry", "transport", "timeout", "terminal", "auth", "malformed"),
)
async def test_owned_local_membership_http_fate_preserves_exact_restart_state(
    tmp_path,
    fate: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operation_id = uuid4()
    session._journal._record_local_membership_intent(
        operation_id=operation_id,
        room_id=ROOM,
        previous_membership="leave",
        previous_epoch=0,
        current_membership="join",
    )
    before = session._journal._load_pending_local_membership_intents(limit=2)
    assert len(before) == 1
    requests: list[object] = []

    async def respond(request, **kwargs: object) -> NetworkResult:
        assert kwargs == {"body_disconnect_retry": True}
        requests.append(request)
        if fate == "retry" and len(requests) == 1:
            return session._network_result(
                request,
                429,
                canonical_json({"errcode": "M_LIMIT_EXCEEDED"}),
                retry_after=0,
            )
        if fate == "retry" and len(requests) == 2:
            return session._network_result(
                request,
                503,
                canonical_json({"errcode": "M_UNAVAILABLE"}),
                retry_after=0,
            )
        if fate == "transport" and len(requests) == 1:
            return session._network_result(
                request,
                None,
                b"",
                failure=NetworkFailureKind.CONNECTION,
            )
        if fate == "timeout" and len(requests) == 1:
            return session._network_result(
                request,
                408,
                canonical_json({"errcode": "M_TIMEOUT"}),
                retry_after=0,
            )
        if fate == "terminal":
            return session._network_result(
                request,
                400,
                canonical_json({"errcode": "M_NOT_FOUND"}),
            )
        if fate == "auth":
            return session._network_result(
                request,
                401,
                canonical_json({"errcode": "M_UNKNOWN_TOKEN"}),
            )
        if fate == "malformed":
            return session._network_result(
                request,
                200,
                canonical_json({"room_id": 5}),
            )
        return session._network_result(
            request,
            200,
            canonical_json({"room_id": ROOM}),
        )

    session._request = respond  # type: ignore[method-assign]
    try:
        if fate == "auth":
            with pytest.raises(
                nio.IngestionSourceError,
                match="local membership authentication failed: M_UNKNOWN_TOKEN",
            ):
                await session._advance_local_membership_intent()
        elif fate == "malformed":
            with pytest.raises(
                nio.IngestionSourceError,
                match="malformed local membership response",
            ):
                await session._advance_local_membership_intent()
        else:
            assert await session._advance_local_membership_intent() is True

        assert requests
        assert all(request == requests[0] for request in requests)
        if fate == "retry":
            assert len(requests) == 3
        elif fate in {"transport", "timeout"}:
            assert len(requests) == 2
        else:
            assert len(requests) == 1

        pending = session._journal._load_pending_local_membership_intents(limit=2)
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
            inventory = session._journal._load_task3_work_inventory(owner)
        assert loaded is not None
        aggregate = loaded[1]
        if fate in {"retry", "transport", "timeout"}:
            assert pending == ()
            assert aggregate.continuity.membership == "join"
            assert aggregate.continuity.membership_epoch == 0
            assert aggregate.pending_local_membership is None
            assert len(inventory.work) == 1
            assert inventory.work[0].value.origin == SystemOrigin(
                SystemOriginKind.MEMBERSHIP_CHANGE,
                operation_id,
            )
        elif fate == "terminal":
            assert pending == ()
            assert aggregate.continuity.membership == "leave"
            assert aggregate.continuity.membership_epoch == 0
            assert aggregate.pending_local_membership is None
            assert inventory.work == ()
        else:
            assert pending == before
            assert aggregate.pending_local_membership == before[0].intent
            assert inventory.work == ()
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("fate", ("terminal", "auth"))
async def test_owned_runner_reports_local_membership_terminal_and_auth_fates(
    tmp_path,
    fate: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operation_id = uuid4()
    requests: list[object] = []
    source_entered = asyncio.Event()
    source_cancelled = asyncio.Event()
    runner: asyncio.Task[None] | None = None
    transition: asyncio.Task[bool] | None = None

    async def respond(request, **kwargs: object) -> NetworkResult:
        requests.append(request)
        if request.path == _bearer_only_path(nio.Api.join("secret-token", ROOM)):
            assert kwargs == {"body_disconnect_retry": True}
            if fate == "terminal":
                return session._network_result(
                    request,
                    400,
                    canonical_json({"errcode": "M_NOT_FOUND"}),
                )
            return session._network_result(
                request,
                403,
                canonical_json({"errcode": "M_FORBIDDEN"}),
            )
        assert fate == "terminal"
        assert kwargs == {}
        source_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            source_cancelled.set()
            raise
        raise AssertionError("unreachable")

    session._request = respond  # type: ignore[method-assign]
    try:
        transition = asyncio.create_task(
            session._run_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        )
        runner = asyncio.create_task(session.run())
        if fate == "terminal":
            assert await asyncio.wait_for(transition, timeout=1) is False
            await asyncio.wait_for(source_entered.wait(), timeout=1)
            assert len(requests) == 2
            assert (
                session._journal._load_pending_local_membership_intents(limit=2) == ()
            )
            owner = session._journal.load_owner()
            with session._journal._owner.read():
                inventory = session._journal._load_task3_work_inventory(owner)
            assert inventory.work == ()
            await session.close()
            await asyncio.wait_for(source_cancelled.wait(), timeout=1)
            with pytest.raises(asyncio.CancelledError):
                await runner
        else:
            with pytest.raises(
                nio.IngestionSourceError,
                match="local membership authentication failed: M_FORBIDDEN",
            ) as transition_error:
                await asyncio.wait_for(transition, timeout=1)
            with pytest.raises(nio.IngestionSourceError) as runner_error:
                await runner
            assert runner_error.value is transition_error.value
            assert len(requests) == 1
            pending = session._journal._load_pending_local_membership_intents(limit=2)
            assert len(pending) == 1
            assert pending[0].intent.operation_id == operation_id
            with pytest.raises(nio.IngestionSourceError) as sticky:
                await session.run()
            assert sticky.value is runner_error.value
    finally:
        for task in (runner, transition):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (runner, transition) if task is not None),
            return_exceptions=True,
        )
        if not session._closed:
            await session.close()


@pytest.mark.asyncio
async def test_owned_close_cancels_local_membership_retry_backoff(tmp_path) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operation_id = uuid4()
    request_entered = asyncio.Event()
    runner: asyncio.Task[None] | None = None
    transition: asyncio.Task[bool] | None = None
    requests: list[object] = []

    async def respond(request, **kwargs: object) -> NetworkResult:
        assert kwargs == {"body_disconnect_retry": True}
        requests.append(request)
        request_entered.set()
        return session._network_result(
            request,
            429,
            canonical_json({"errcode": "M_LIMIT_EXCEEDED"}),
            retry_after=60_000,
        )

    session._request = respond  # type: ignore[method-assign]
    try:
        transition = asyncio.create_task(
            session._run_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        )
        runner = asyncio.create_task(session.run())
        await asyncio.wait_for(request_entered.wait(), timeout=1)
        await session.close()
        with pytest.raises(asyncio.CancelledError):
            await runner
        with pytest.raises(asyncio.CancelledError):
            await transition
        assert len(requests) == 1
    finally:
        for task in (runner, transition):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (runner, transition) if task is not None),
            return_exceptions=True,
        )
        if not session._closed:
            await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    try:
        pending = reopened._journal._load_pending_local_membership_intents(limit=2)
        assert len(pending) == 1
        assert pending[0].intent.operation_id == operation_id
        owner = reopened._journal.load_owner()
        with reopened._journal._owner.read():
            inventory = reopened._journal._load_task3_work_inventory(owner)
        assert inventory.work == ()
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("response_fate", ("success", "terminal"))
@pytest.mark.parametrize("transaction_fate", ("rollback", "commit"))
async def test_owned_local_membership_response_uncertainty_reopens_exact_fate(
    tmp_path,
    response_fate: str,
    transaction_fate: str,
) -> None:
    class ResponseAbort(BaseException):
        pass

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operation_id = uuid4()
    session._journal._record_local_membership_intent(
        operation_id=operation_id,
        room_id=ROOM,
        previous_membership="leave",
        previous_epoch=0,
        current_membership="join",
    )
    connection = session._owned_store.database.connection()
    graph_before = tuple(connection.iterdump())
    selected_label = (
        "work_insert"
        if response_fate == "success"
        else "local_intent_clear_aggregate_update"
    )
    if transaction_fate == "commit":
        selected_label = "commit"
    sentinel = ResponseAbort(
        f"{response_fate} {transaction_fate} acknowledgement failed"
    )
    labels: list[str] = []
    statements: list[str] = []

    def abort(label: str) -> None:
        labels.append(label)
        if label == selected_label:
            raise sentinel

    async def respond(request, **kwargs: object) -> NetworkResult:
        assert kwargs == {"body_disconnect_retry": True}
        if response_fate == "success":
            return session._network_result(
                request,
                200,
                canonical_json({"room_id": ROOM}),
            )
        return session._network_result(
            request,
            400,
            canonical_json({"errcode": "M_NOT_FOUND"}),
        )

    session._request = respond  # type: ignore[method-assign]
    session._journal.set_transition_statement_hook(abort)
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(ResponseAbort) as failure:
            await session._advance_local_membership_intent()
        connection.set_trace_callback(None)
        assert failure.value is sentinel
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        if transaction_fate == "rollback":
            assert normalized.count("ROLLBACK") == 1
            assert normalized.count("COMMIT") == 0
            assert tuple(connection.iterdump()) == graph_before
        else:
            assert normalized.count("COMMIT") == 1
            assert normalized.count("ROLLBACK") == 0
            assert tuple(connection.iterdump()) != graph_before
        assert labels[-1] == selected_label
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    replay_requests: list[object] = []

    async def replay(request, **kwargs: object) -> NetworkResult:
        assert kwargs == {"body_disconnect_retry": True}
        replay_requests.append(request)
        if response_fate == "success":
            return fresh_session._network_result(
                request,
                200,
                canonical_json({"room_id": ROOM}),
            )
        return fresh_session._network_result(
            request,
            400,
            canonical_json({"errcode": "M_NOT_FOUND"}),
        )

    fresh_session._request = replay  # type: ignore[method-assign]
    try:
        pending = fresh_session._journal._load_pending_local_membership_intents(limit=2)
        owner = fresh_session._journal.load_owner()
        with fresh_session._journal._owner.read():
            inventory = fresh_session._journal._load_task3_work_inventory(owner)
        if transaction_fate == "rollback":
            assert len(pending) == 1
            assert pending[0].intent.operation_id == operation_id
            assert inventory.work == ()
            assert await fresh_session._advance_local_membership_intent() is True
            assert len(replay_requests) == 1
            pending = fresh_session._journal._load_pending_local_membership_intents(
                limit=2
            )
            owner = fresh_session._journal.load_owner()
            with fresh_session._journal._owner.read():
                inventory = fresh_session._journal._load_task3_work_inventory(owner)
        else:
            assert replay_requests == []
        assert pending == ()
        if response_fate == "success":
            assert len(inventory.work) == 1
            assert inventory.work[0].value.origin == SystemOrigin(
                SystemOriginKind.MEMBERSHIP_CHANGE,
                operation_id,
            )
        else:
            assert inventory.work == ()
    finally:
        await fresh_session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("path", ("insert", "update"))
@pytest.mark.parametrize("transaction_fate", ("rollback", "commit"))
async def test_owned_local_membership_intent_uncertainty_reopens_exact_fate(
    tmp_path,
    path: str,
    transaction_fate: str,
) -> None:
    class IntentAbort(BaseException):
        pass

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    if path == "insert":
        previous_membership, previous_epoch, current_membership = (
            "leave",
            0,
            "join",
        )
    else:
        _seed_operation, _commit, _aggregate, _work = _publish_fresh_local_join(session)
        seed_batch = session.next_batch()
        assert seed_batch is not None
        session.acknowledge_batch(seed_batch.ref)
        previous_membership, previous_epoch, current_membership = (
            "join",
            0,
            "leave",
        )
    operation_id = uuid4()
    connection = session._owned_store.database.connection()
    owner_before = session._journal.load_owner()
    graph_before = tuple(connection.iterdump())
    aggregate_label = f"local_intent_aggregate_{path}"
    selected_label = "commit" if transaction_fate == "commit" else aggregate_label
    sentinel = IntentAbort(f"intent {path} {transaction_fate} failed")
    labels: list[str] = []
    statements: list[str] = []

    def abort(label: str) -> None:
        labels.append(label)
        if label == selected_label:
            raise sentinel

    session._journal.set_transition_statement_hook(abort)
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(IntentAbort) as failure:
            session._journal._record_local_membership_intent(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous_membership,
                previous_epoch=previous_epoch,
                current_membership=current_membership,
            )
        connection.set_trace_callback(None)
        assert failure.value is sentinel
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        if transaction_fate == "rollback":
            assert normalized.count("ROLLBACK") == 1
            assert normalized.count("COMMIT") == 0
            assert tuple(connection.iterdump()) == graph_before
        else:
            assert normalized.count("COMMIT") == 1
            assert normalized.count("ROLLBACK") == 0
            assert tuple(connection.iterdump()) != graph_before
        assert labels[-1] == selected_label
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    try:
        pending = reopened._journal._load_pending_local_membership_intents(limit=2)
        if transaction_fate == "rollback":
            assert pending == ()
            committed = reopened._journal._record_local_membership_intent(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous_membership,
                previous_epoch=previous_epoch,
                current_membership=current_membership,
            )
            assert committed == CommitResult(owner_before.revision + 1)
            pending = reopened._journal._load_pending_local_membership_intents(limit=2)
        assert len(pending) == 1
        assert pending[0].room_id == ROOM
        assert pending[0].intent.operation_id == operation_id
        reopened_connection = reopened._journal._owner.database.connection()
        graph_after = tuple(reopened_connection.iterdump())
        retry_statements: list[str] = []
        reopened_connection.set_trace_callback(retry_statements.append)
        retried = reopened._journal._record_local_membership_intent(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership=previous_membership,
            previous_epoch=previous_epoch,
            current_membership=current_membership,
        )
        reopened_connection.set_trace_callback(None)
        assert retried == CommitResult(owner_before.revision + 1)
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in retry_statements
        )
        assert tuple(reopened_connection.iterdump()) == graph_after
    finally:
        reopened._journal._owner.database.connection().set_trace_callback(None)
        reopened.close()


@pytest.mark.asyncio
async def test_owned_local_membership_work_reopen_fences_source_until_ack(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operation_id = uuid4()
    session._journal._record_local_membership_intent(
        operation_id=operation_id,
        room_id=ROOM,
        previous_membership="leave",
        previous_epoch=0,
        current_membership="join",
    )

    async def membership_success(request, **kwargs: object) -> NetworkResult:
        assert request.path == _bearer_only_path(nio.Api.join("secret-token", ROOM))
        assert kwargs == {"body_disconnect_retry": True}
        return session._network_result(
            request,
            200,
            canonical_json({"room_id": ROOM}),
        )

    session._request = membership_success  # type: ignore[method-assign]
    assert await session._advance_local_membership_intent() is True
    assert session.next_batch() is not None
    await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    source_entered = asyncio.Event()
    source_cancelled = asyncio.Event()
    requests: list[object] = []
    runner: asyncio.Task[None] | None = None

    async def source_only(request, **kwargs: object) -> NetworkResult:
        assert request.path != _bearer_only_path(nio.Api.join("secret-token", ROOM))
        assert request.path != _bearer_only_path(
            nio.Api.room_leave("secret-token", ROOM)
        )
        assert kwargs == {}
        requests.append(request)
        source_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            source_cancelled.set()
            raise
        raise AssertionError("unreachable")

    fresh_session._request = source_only  # type: ignore[method-assign]
    try:
        runner = asyncio.create_task(fresh_session.run())
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert requests == []
        batch = fresh_session.next_batch()
        assert batch is not None
        assert len(batch.records) == 1
        assert batch.records[0].origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            operation_id,
        )
        await fresh_session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        await asyncio.wait_for(source_entered.wait(), timeout=1)
        assert len(requests) == 1
        await fresh_session.close()
        await asyncio.wait_for(source_cancelled.wait(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await runner
    finally:
        if runner is not None and not runner.done():
            runner.cancel()
        await asyncio.gather(
            *(task for task in (runner,) if task is not None),
            return_exceptions=True,
        )
        if not fresh_session._closed:
            await fresh_session.close()

    final_bootstrap = reopen_owned_bootstrap(tmp_path, generation)
    final_client = owned_client(tmp_path)
    final_session = open_owned_session(final_client, final_bootstrap, generation)
    final_entered = asyncio.Event()
    final_cancelled = asyncio.Event()
    final_requests: list[object] = []
    final_runner: asyncio.Task[None] | None = None

    async def final_source(request, **kwargs: object) -> NetworkResult:
        assert request.path != _bearer_only_path(nio.Api.join("secret-token", ROOM))
        assert request.path != _bearer_only_path(
            nio.Api.room_leave("secret-token", ROOM)
        )
        assert kwargs == {}
        final_requests.append(request)
        final_entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            final_cancelled.set()
            raise
        raise AssertionError("unreachable")

    final_session._request = final_source  # type: ignore[method-assign]
    try:
        assert final_session.next_batch() is None
        assert (
            final_session._journal._load_pending_local_membership_intents(limit=2) == ()
        )
        final_runner = asyncio.create_task(final_session.run())
        await asyncio.wait_for(final_entered.wait(), timeout=1)
        assert len(final_requests) == 1
        await final_session.close()
        await asyncio.wait_for(final_cancelled.wait(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await final_runner
    finally:
        if final_runner is not None and not final_runner.done():
            final_runner.cancel()
        await asyncio.gather(
            *(task for task in (final_runner,) if task is not None),
            return_exceptions=True,
        )
        if not final_session._closed:
            await final_session.close()


@pytest.mark.asyncio
async def test_owned_local_membership_transition_is_durable_and_idempotent(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        stage_classic(
            bootstrap,
            {
                "next_batch": "s1",
                "rooms": {
                    "join": {
                        ROOM: {
                            "state": {
                                "events": [
                                    {
                                        "content": {"membership": "join"},
                                        "event_id": "$local-member",
                                        "origin_server_ts": 1,
                                        "sender": ACCOUNT,
                                        "state_key": ACCOUNT,
                                        "type": "m.room.member",
                                    },
                                    {
                                        "content": {"name": "Local transition room"},
                                        "event_id": "$local-name",
                                        "origin_server_ts": 2,
                                        "sender": ACCOUNT,
                                        "state_key": "",
                                        "type": "m.room.name",
                                    },
                                ]
                            },
                            "timeline": {"events": []},
                        }
                    }
                },
            },
        )
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        pending = session._journal.load_pending_hydrations(limit=2)
        assert len(pending) == 1
        hydrated = normalize_hydration_response(
            pending[0],
            own_user_id=ACCOUNT,
            response_body=hydration_state(),
        )
        assert session._journal.apply_hydration_result(result=hydrated) is not None
        while batch := session.next_batch():
            session.acknowledge_batch(batch.ref)

        owner_before = session._journal.load_owner()
        source_before = session._journal.load_source()
        frame_rows_before = tuple(
            tuple(row)
            for row in connection.execute(
                "SELECT * FROM NioIngestFrame ORDER BY staged_revision"
            )
        )
        e2ee_before = tuple(
            statement
            for statement in connection.iterdump()
            if "NioIngest" not in statement
        )
        with session._journal._owner.read():
            loaded_before = session._journal._load_room_aggregate(owner_before, ROOM)
            inventory_before = session._journal._load_task3_work_inventory(owner_before)
        assert loaded_before is not None
        aggregate_before = loaded_before[1]
        snapshot_before = aggregate_before.room_snapshot
        assert aggregate_before.continuity.membership == "join"
        assert aggregate_before.continuity.baseline is not None
        assert aggregate_before.continuity.gap is None
        assert aggregate_before.continuity.hydration_id is None
        assert aggregate_before.pending_hydration is None
        assert snapshot_before is not None
        assert snapshot_before.own_membership == "join"
        assert snapshot_before.membership_epoch == (
            aggregate_before.continuity.membership_epoch
        )
        assert snapshot_before.name == "Local transition room"
        assert inventory_before.work == ()

        publish = session._publish_local_membership_transition
        signature = inspect.signature(publish)
        assert tuple(signature.parameters) == (
            "operation_id",
            "room_id",
            "previous_membership",
            "previous_epoch",
            "current_membership",
        )
        assert all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            for parameter in signature.parameters.values()
        )
        assert tuple(
            parameter.annotation for parameter in signature.parameters.values()
        ) == ("UUID", "str", "str", "int", "str")
        assert signature.return_annotation == "CommitResult"

        operation_id = uuid4()
        previous_epoch = aggregate_before.continuity.membership_epoch
        expected_record_id = str(uuid5(operation_id, "nio:room-lifecycle:v1"))
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        committed = publish(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership="join",
            previous_epoch=previous_epoch,
            current_membership="leave",
        )
        connection.set_trace_callback(None)
        assert committed == CommitResult(owner_before.revision + 1)
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1
        assert transaction_statements.count("COMMIT") == 1
        assert transaction_statements.count("ROLLBACK") == 0

        owner_after = session._journal.load_owner()
        with session._journal._owner.read():
            loaded_after = session._journal._load_room_aggregate(owner_after, ROOM)
            inventory_after = session._journal._load_task3_work_inventory(owner_after)
        assert loaded_after is not None
        aggregate_after = loaded_after[1]
        assert aggregate_after.continuity.room_id == ROOM
        assert aggregate_after.continuity.membership_epoch == previous_epoch + 1
        assert aggregate_after.continuity.membership == "leave"
        assert aggregate_after.continuity.baseline is None
        assert aggregate_after.continuity.gap is None
        assert aggregate_after.continuity.hydration_id is None
        assert aggregate_after.pending_hydration is None
        assert aggregate_after.next_room_sequence == (
            aggregate_before.next_room_sequence + 1
        )
        assert aggregate_after.updated_revision == committed.revision
        assert aggregate_after.room_snapshot == replace(
            snapshot_before,
            membership_epoch=previous_epoch + 1,
            own_membership="leave",
        )

        assert len(inventory_after.work) == 1
        local_work = inventory_after.work[0]
        assert local_work.status == "ready"
        assert local_work.frame_id == operation_id
        assert local_work.created_revision == committed.revision
        assert local_work.metadata is None
        assert local_work.value.record_id == expected_record_id
        assert local_work.value.kind.value == "room_lifecycle"
        assert local_work.value.origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            operation_id,
        )
        assert local_work.value.room_id == ROOM
        assert local_work.value.membership_epoch == previous_epoch + 1
        assert local_work.value.room_sequence == aggregate_before.next_room_sequence
        assert local_work.value.event_id is None
        assert local_work.value.provenance is None
        assert local_work.value.clear_json is None
        expected_source = canonical_json(
            {
                "event_id": None,
                "membership": "leave",
                "membership_epoch": previous_epoch + 1,
                "membership_provenance": "local",
                "previous_membership": "join",
                "previous_membership_epoch": previous_epoch,
                "source_kind": "local",
                "source_record_id": None,
                "timeline_provenance": None,
            }
        )
        assert local_work.value.source_json == expected_source
        assert local_work.plaintext is not None
        assert tuple(json.loads(local_work.plaintext)) == ("kind", "value")
        raw_work = connection.execute(
            "SELECT work_id, kind, status, frame_id, room_id, membership_epoch, "
            "room_sequence, ready_revision, ready_ordinal, created_revision "
            "FROM NioIngestWork WHERE account_id = ?",
            (ACCOUNT,),
        ).fetchone()
        assert raw_work is not None
        assert tuple(raw_work) == (
            expected_record_id,
            "event",
            "ready",
            str(operation_id),
            ROOM,
            previous_epoch + 1,
            aggregate_before.next_room_sequence,
            committed.revision,
            0,
            committed.revision,
        )
        assert (
            connection.execute(
                "SELECT COUNT(*) FROM NioIngestFrame WHERE frame_id = ?",
                (str(operation_id),),
            ).fetchone()[0]
            == 0
        )
        assert session._journal.load_source() == source_before
        assert (
            tuple(
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM NioIngestFrame ORDER BY staged_revision"
                )
            )
            == frame_rows_before
        )
        assert (
            tuple(
                statement
                for statement in connection.iterdump()
                if "NioIngest" not in statement
            )
            == e2ee_before
        )

        def assert_retry_has_no_dml() -> None:
            retry_statements: list[str] = []
            connection.set_trace_callback(retry_statements.append)
            assert (
                publish(
                    operation_id=operation_id,
                    room_id=ROOM,
                    previous_membership="join",
                    previous_epoch=previous_epoch,
                    current_membership="leave",
                )
                == committed
            )
            connection.set_trace_callback(None)
            assert not any(
                statement.lstrip()
                .upper()
                .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
                for statement in retry_statements
            )

        assert_retry_has_no_dml()
        claimed = session.next_batch()
        assert claimed is not None
        assert claimed.records == (local_work.value,)
        assert_retry_has_no_dml()
        assert (
            connection.execute(
                "SELECT work_id, kind, status, frame_id, room_id, membership_epoch, "
                "room_sequence, ready_revision, ready_ordinal, created_revision "
                "FROM NioIngestWork WHERE account_id = ?",
                (ACCOUNT,),
            ).fetchone()
            == raw_work
        )
        assert session._journal.load_source() == source_before
        assert (
            tuple(
                tuple(row)
                for row in connection.execute(
                    "SELECT * FROM NioIngestFrame ORDER BY staged_revision"
                )
            )
            == frame_rows_before
        )
        assert (
            tuple(
                statement
                for statement in connection.iterdump()
                if "NioIngest" not in statement
            )
            == e2ee_before
        )
    finally:
        connection.set_trace_callback(None)
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    try:
        reopened_owner = reopened._journal.load_owner()
        with reopened._journal._owner.read():
            reopened_aggregate = reopened._journal._load_room_aggregate(
                reopened_owner,
                ROOM,
            )
            reopened_inventory = reopened._journal._load_task3_work_inventory(
                reopened_owner
            )
        assert reopened_aggregate is not None
        assert reopened_aggregate[1] == aggregate_after
        assert reopened_inventory.work == (local_work,)
        assert reopened_inventory.work[0].value.origin == SystemOrigin(
            SystemOriginKind.MEMBERSHIP_CHANGE,
            operation_id,
        )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_owned_local_membership_transition_creates_missing_aggregate_and_orders_chain(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operations = (
        (uuid4(), "leave", 0, "join", 0),
        (uuid4(), "join", 0, "leave", 1),
        (uuid4(), "leave", 1, "join", 1),
    )
    committed: list[CommitResult] = []
    expected_values: list[object] = []
    try:
        owner_before = session._journal.load_owner()
        assert session._journal.load_source().next_request_id == 0
        assert session._journal.list_frames(1) == ()
        with session._journal._owner.read():
            assert session._journal._load_room_aggregate(owner_before, ROOM) is None
        for previous_membership, previous_epoch, current_membership in (
            ("join", 0, "leave"),
            ("leave", 1, "join"),
            ("invite", 0, "join"),
        ):
            before = tuple(session._owned_store.database.connection().iterdump())
            statements: list[str] = []
            session._owned_store.database.connection().set_trace_callback(
                statements.append
            )
            with pytest.raises(JournalConflictError):
                session._publish_local_membership_transition(
                    operation_id=uuid4(),
                    room_id=ROOM,
                    previous_membership=previous_membership,
                    previous_epoch=previous_epoch,
                    current_membership=current_membership,
                )
            session._owned_store.database.connection().set_trace_callback(None)
            assert not any(
                statement.lstrip()
                .upper()
                .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
                for statement in statements
            )
            assert (
                tuple(session._owned_store.database.connection().iterdump()) == before
            )
        for sequence, (
            operation_id,
            previous_membership,
            previous_epoch,
            current_membership,
            current_epoch,
        ) in enumerate(operations):
            publish_statements: list[str] = []
            if sequence == 0:
                session._owned_store.database.connection().set_trace_callback(
                    publish_statements.append
                )
            result = session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous_membership,
                previous_epoch=previous_epoch,
                current_membership=current_membership,
            )
            if sequence == 0:
                session._owned_store.database.connection().set_trace_callback(None)
                normalized = tuple(
                    statement.strip().rstrip(";").upper()
                    for statement in publish_statements
                )
                assert normalized.count("BEGIN IMMEDIATE") == 1
                assert normalized.count("COMMIT") == 1
                assert (
                    sum(
                        statement.startswith("UPDATE NIOINGESTMETA SET REVISION")
                        for statement in normalized
                    )
                    == 1
                )
                assert (
                    sum(
                        statement.startswith("INSERT INTO NIOINGESTROOMAGGREGATE")
                        for statement in normalized
                    )
                    == 1
                )
                assert (
                    sum(
                        statement.startswith("INSERT INTO NIOINGESTWORK")
                        for statement in normalized
                    )
                    == 1
                )
            assert result == CommitResult(owner_before.revision + sequence + 1)
            committed.append(result)

            owner = session._journal.load_owner()
            with session._journal._owner.read():
                loaded = session._journal._load_room_aggregate(owner, ROOM)
                inventory = session._journal._load_task3_work_inventory(owner)
            assert loaded is not None
            aggregate = loaded[1]
            assert aggregate.continuity.membership == current_membership
            assert aggregate.continuity.membership_epoch == current_epoch
            assert aggregate.continuity.baseline is None
            assert aggregate.continuity.gap is None
            assert aggregate.continuity.hydration_id is None
            assert aggregate.pending_hydration is None
            assert aggregate.next_room_sequence == sequence + 1
            assert aggregate.updated_revision == result.revision
            assert aggregate.room_snapshot is None

            ordered = sorted(
                inventory.work,
                key=lambda item: item.created_revision or 0,
            )
            assert len(ordered) == sequence + 1
            current = ordered[-1]
            assert current.status == "ready"
            assert current.frame_id == operation_id
            assert current.created_revision == result.revision
            assert current.metadata is None
            assert current.value.record_id == str(
                uuid5(operation_id, "nio:room-lifecycle:v1")
            )
            assert current.value.origin == SystemOrigin(
                SystemOriginKind.MEMBERSHIP_CHANGE,
                operation_id,
            )
            assert current.value.room_id == ROOM
            assert current.value.membership_epoch == current_epoch
            assert current.value.room_sequence == sequence
            assert current.value.source_json == _local_source(
                previous_membership,
                previous_epoch,
                current_membership,
                current_epoch,
            )
            assert current.value.event_id is None
            assert current.value.provenance is None
            assert current.value.clear_json is None
            expected_values.append(current.value)
        assert session._journal.list_frames(1) == ()
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    reopened_client = owned_client(tmp_path)
    reopened_session = open_owned_session(reopened_client, reopened, generation)
    try:
        owner = reopened_session._journal.load_owner()
        with reopened_session._journal._owner.read():
            loaded = reopened_session._journal._load_room_aggregate(owner, ROOM)
            inventory = reopened_session._journal._load_task3_work_inventory(owner)
        assert loaded is not None
        assert loaded[1].continuity.membership == "join"
        assert loaded[1].continuity.membership_epoch == 1
        assert loaded[1].next_room_sequence == 3
        assert loaded[1].room_snapshot is None
        assert sorted(
            inventory.work,
            key=lambda item: item.created_revision or 0,
        ) == [
            next(item for item in inventory.work if item.value == expected_value)
            for expected_value in expected_values
        ]

        delivered: list[object] = []
        delivered_revisions: list[int] = []
        while batch := reopened_session.next_batch():
            delivered.extend(batch.records)
            delivered_revisions.append(batch.created_revision)
            reopened_session.acknowledge_batch(batch.ref)
        assert delivered == expected_values
        assert delivered_revisions == [result.revision for result in committed]
    finally:
        await reopened_session.close()


@pytest.mark.parametrize(
    ("current_membership", "error"),
    (
        pytest.param("invite", ValueError, id="invite"),
        pytest.param("knock", ValueError, id="knock"),
        pytest.param("ban", ValueError, id="ban"),
        pytest.param("join", ValueError, id="no-op"),
        pytest.param(object(), TypeError, id="foreign-type"),
    ),
)
@pytest.mark.asyncio
async def test_owned_local_membership_transition_rejects_invalid_current_before_transaction(
    tmp_path,
    current_membership: object,
    error: type[Exception],
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        _staged, aggregate = _ready_local_room(session, bootstrap)
        before = tuple(connection.iterdump())
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with pytest.raises(error):
            session._publish_local_membership_transition(
                operation_id=uuid4(),
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=aggregate.continuity.membership_epoch,
                current_membership=current_membership,  # type: ignore[arg-type]
            )
        connection.set_trace_callback(None)
        assert not any(
            statement.strip()
            .upper()
            .startswith(("BEGIN", "INSERT", "UPDATE", "DELETE", "REPLACE"))
            for statement in statements
        )
        assert tuple(connection.iterdump()) == before
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_local_membership_transition_rejects_pending_room_barrier_without_dml(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        stage_classic(bootstrap, _local_room_body())
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
            inventory = session._journal._load_task3_work_inventory(owner)
        assert loaded is not None
        aggregate = loaded[1]
        assert aggregate.continuity.membership == "join"
        assert aggregate.continuity.hydration_id is not None
        assert aggregate.pending_hydration is not None
        held = tuple(item for item in inventory.work if item.status == "held")
        assert held

        before = tuple(connection.iterdump())
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with pytest.raises(JournalConflictError):
            session._publish_local_membership_transition(
                operation_id=uuid4(),
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=aggregate.continuity.membership_epoch,
                current_membership="leave",
            )
        connection.set_trace_callback(None)
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert normalized.count("ROLLBACK") == 1
        assert normalized.count("COMMIT") == 0
        assert not any(
            statement.startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in normalized
        )
        assert tuple(connection.iterdump()) == before
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_local_membership_transition_repairs_snapshot_and_rejects_conflicts_without_dml(
    tmp_path,
) -> None:
    from nio.store._sync_journal_preflight import _canonical_internal
    from nio.store._sync_journal_rows import _canonical_room_aggregate_plaintext

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        retained, aggregate = _ready_local_room(session, bootstrap)
        owner = session._journal.load_owner()
        snapshot = aggregate.room_snapshot
        assert snapshot is not None
        stale_snapshot = replace(
            snapshot,
            membership_epoch=aggregate.continuity.membership_epoch + 17,
            own_membership="ban",
        )
        stale_aggregate = replace(aggregate, room_snapshot=stale_snapshot)
        plaintext = _canonical_room_aggregate_plaintext(stale_aggregate)
        payload, digest = session._journal._payload(
            owner,
            "NioIngestRoomAggregate",
            plaintext,
            header=_canonical_internal([ROOM, aggregate.updated_revision, None]),
        )
        connection.execute(
            "UPDATE NioIngestRoomAggregate SET payload = ?, payload_sha256 = ? "
            "WHERE account_id = ? AND room_id = ?",
            (payload, digest, ACCOUNT, ROOM),
        )

        operation_id = uuid4()
        previous_epoch = aggregate.continuity.membership_epoch
        committed = session._publish_local_membership_transition(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership="join",
            previous_epoch=previous_epoch,
            current_membership="leave",
        )
        updated_owner = session._journal.load_owner()
        with session._journal._owner.read():
            updated = session._journal._load_room_aggregate(updated_owner, ROOM)
        assert updated is not None
        assert updated[1].room_snapshot == replace(
            stale_snapshot,
            membership_epoch=previous_epoch + 1,
            own_membership="leave",
        )
        assert updated[1].updated_revision == committed.revision

        stale_before = tuple(connection.iterdump())
        stale_statements: list[str] = []
        connection.set_trace_callback(stale_statements.append)
        with pytest.raises(JournalConflictError):
            session._publish_local_membership_transition(
                operation_id=uuid4(),
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=previous_epoch,
                current_membership="leave",
            )
        connection.set_trace_callback(None)
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in stale_statements
        )
        assert tuple(connection.iterdump()) == stale_before

        hydration_id = uuid4()
        barrier_aggregate = replace(
            updated[1],
            continuity=replace(
                updated[1].continuity,
                baseline=None,
                hydration_id=hydration_id,
            ),
            pending_hydration=HydrationIntent(
                hydration_id,
                RecordOrigin(
                    updated_owner.transport_kind,
                    0,
                    0,
                    0,
                ),
            ),
        )
        barrier_plaintext = _canonical_room_aggregate_plaintext(barrier_aggregate)
        barrier_payload, barrier_digest = session._journal._payload(
            updated_owner,
            "NioIngestRoomAggregate",
            barrier_plaintext,
            header=_canonical_internal(
                [ROOM, barrier_aggregate.updated_revision, "hydration"]
            ),
        )
        connection.execute(
            "UPDATE NioIngestRoomAggregate SET intent_kind = 'hydration', "
            "payload = ?, payload_sha256 = ? WHERE account_id = ? AND room_id = ?",
            (barrier_payload, barrier_digest, ACCOUNT, ROOM),
        )
        retry_before = tuple(connection.iterdump())
        retry_statements: list[str] = []
        connection.set_trace_callback(retry_statements.append)
        assert (
            session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=previous_epoch,
                current_membership="leave",
            )
            == committed
        )
        connection.set_trace_callback(None)
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in retry_statements
        )
        assert tuple(connection.iterdump()) == retry_before

        def assert_conflict(**arguments: object) -> None:
            before = tuple(connection.iterdump())
            statements: list[str] = []
            connection.set_trace_callback(statements.append)
            with pytest.raises(JournalConflictError):
                session._publish_local_membership_transition(
                    **arguments  # type: ignore[arg-type]
                )
            connection.set_trace_callback(None)
            assert not any(
                statement.lstrip()
                .upper()
                .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
                for statement in statements
            )
            assert tuple(connection.iterdump()) == before

        assert_conflict(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=previous_epoch + 1,
            current_membership="join",
        )
        assert_conflict(
            operation_id=operation_id,
            room_id="!different:example.org",
            previous_membership="join",
            previous_epoch=previous_epoch,
            current_membership="leave",
        )
        assert_conflict(
            operation_id=retained.frame_id,
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=previous_epoch + 1,
            current_membership="join",
        )
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_frame_staging_rejects_existing_local_operation_uuid_without_dml(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        owner = session._journal.load_owner()
        prior = session._journal.load_source()
        source = ClassicSource(
            owner.stream_id, classic_config().source, owner.account_id
        )
        request = source.plan_request(prior, prior.next_request_id)
        assert request is not None
        normalized = source.normalize(
            request,
            NetworkResult(
                request.stream_id,
                request.transport,
                request.source_epoch,
                request.request_id,
                200,
                canonical_json({"next_batch": "s1", "rooms": {}}),
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
        successor = SourceState(
            prior.source_epoch,
            prior.transport_kind,
            normalized.frame.candidate_cursor_json,
            request.request_id + 1,
            prior.active,
        )
        committed = session._publish_local_membership_transition(
            operation_id=frame.frame_id,
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        )
        assert committed.revision == owner.revision + 1

        before = tuple(connection.iterdump())
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with pytest.raises(JournalConflictError, match="collides"):
            session._journal.stage_source_response(source=successor, frame=frame)
        connection.set_trace_callback(None)
        normalized_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized_statements.count("BEGIN IMMEDIATE") == 1
        assert normalized_statements.count("ROLLBACK") == 1
        assert normalized_statements.count("COMMIT") == 0
        assert not any(
            statement.startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in normalized_statements
        )
        assert tuple(connection.iterdump()) == before
        assert session._journal.load_source() == prior
        assert session._journal.list_frames(1) == ()
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.parametrize("mutation", ("ready_ordinal", "ready_revision"))
@pytest.mark.asyncio
async def test_owned_local_work_loader_rejects_noncanonical_ready_header(
    tmp_path,
    mutation: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        _operation_id, committed, _aggregate, work = _publish_fresh_local_join(session)
        if mutation == "ready_revision":
            stage_classic(
                bootstrap,
                {"next_batch": "s1", "rooms": {}},
            )
            owner = session._journal.load_owner()
            assert owner.revision == committed.revision + 1
            updates = ((7, owner.revision),)
        else:
            updates = ((8, 1),)
        _reseal_local_work(
            session,
            work.value.record_id,
            header_updates=updates,
        )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            with pytest.raises(
                JournalIntegrityError,
                match="local READY Work header",
            ):
                session._journal._load_task3_work_inventory(owner)
    finally:
        await session.close()


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        ("missing_aggregate", "local Work requires Aggregate"),
        ("room_sequence", "local Work exceeds Aggregate sequence frontier"),
        ("membership_epoch", "local Work exceeds Aggregate epoch frontier"),
        ("created_revision", "local Work exceeds Aggregate revision frontier"),
        ("aggregate_membership", "local Work does not match Aggregate state"),
        ("aggregate_epoch", "local Work does not match Aggregate state"),
        ("aggregate_next_sequence", "local Work does not match Aggregate state"),
    ),
)
@pytest.mark.asyncio
async def test_owned_reopen_rejects_local_work_outside_aggregate_frontier(
    tmp_path,
    mutation: str,
    expected_error: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        _operation_id, committed, aggregate, work = _publish_fresh_local_join(session)
        if mutation == "missing_aggregate":
            session._owned_store.database.connection().execute(
                "DELETE FROM NioIngestRoomAggregate "
                "WHERE account_id = ? AND room_id = ?",
                (ACCOUNT, ROOM),
            )
        elif mutation == "room_sequence":
            _reseal_local_work(
                session,
                work.value.record_id,
                value=replace(
                    work.value,
                    room_sequence=aggregate.next_room_sequence,
                ),
            )
        elif mutation == "membership_epoch":
            _reseal_local_work(
                session,
                work.value.record_id,
                value=replace(
                    work.value,
                    membership_epoch=1,
                    source_json=_local_source("join", 0, "leave", 1),
                ),
            )
        elif mutation == "created_revision":
            stage_classic(
                bootstrap,
                {"next_batch": "s1", "rooms": {}},
            )
            owner = session._journal.load_owner()
            assert owner.revision == committed.revision + 1
            _reseal_local_work(
                session,
                work.value.record_id,
                header_updates=(
                    (7, owner.revision),
                    (9, owner.revision),
                ),
            )
        elif mutation == "aggregate_membership":
            _reseal_local_aggregate(
                session,
                replace(
                    aggregate,
                    continuity=replace(
                        aggregate.continuity,
                        membership="leave",
                    ),
                ),
            )
        elif mutation == "aggregate_epoch":
            _reseal_local_aggregate(
                session,
                replace(
                    aggregate,
                    continuity=replace(
                        aggregate.continuity,
                        membership_epoch=1,
                    ),
                ),
            )
        else:
            assert mutation == "aggregate_next_sequence"
            _reseal_local_aggregate(
                session,
                replace(
                    aggregate,
                    next_room_sequence=work.value.room_sequence + 2,
                ),
            )
    finally:
        await session.close()

    def reopen() -> None:
        reopened = reopen_owned_bootstrap(tmp_path, generation)
        reopened.close()

    with pytest.raises(JournalIntegrityError, match=expected_error):
        reopen()


@pytest.mark.parametrize(
    "mutation",
    ("baseline", "hydration", "snapshot"),
)
@pytest.mark.asyncio
async def test_owned_reopen_rejects_local_work_with_noncanonical_successor_aggregate(
    tmp_path,
    mutation: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        _retained, aggregate = _ready_local_room(session, bootstrap)
        operation_id = uuid4()
        committed = session._publish_local_membership_transition(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership="join",
            previous_epoch=aggregate.continuity.membership_epoch,
            current_membership="leave",
        )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        successor = loaded[1]
        assert successor.updated_revision == committed.revision
        intent_kind = None
        if mutation == "baseline":
            successor = replace(
                successor,
                continuity=replace(
                    successor.continuity,
                    baseline=MembershipBaseline("$stale", None),
                ),
            )
        elif mutation == "hydration":
            hydration_id = uuid4()
            successor = replace(
                successor,
                continuity=replace(
                    successor.continuity,
                    hydration_id=hydration_id,
                ),
                pending_hydration=HydrationIntent(
                    hydration_id,
                    RecordOrigin(owner.transport_kind, 0, 0, 0),
                ),
            )
            intent_kind = "hydration"
        else:
            assert mutation == "snapshot"
            snapshot = successor.room_snapshot
            assert snapshot is not None
            successor = replace(
                successor,
                room_snapshot=replace(
                    snapshot,
                    membership_epoch=aggregate.continuity.membership_epoch,
                    own_membership="join",
                ),
            )
        _reseal_local_aggregate(
            session,
            successor,
            intent_kind=intent_kind,
        )
    finally:
        await session.close()

    with pytest.raises(
        JournalIntegrityError,
        match="local Work does not match Aggregate state",
    ):
        reopen_owned_bootstrap(tmp_path, generation)


@pytest.mark.parametrize(
    ("mutation", "room_sequence"),
    (("duplicate", 1), ("reversed", 0)),
)
@pytest.mark.asyncio
async def test_owned_reopen_rejects_cross_local_room_sequence_order(
    tmp_path,
    mutation: str,
    room_sequence: int,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operations = (
        (uuid4(), "leave", 0, "join"),
        (uuid4(), "join", 0, "leave"),
        (uuid4(), "leave", 1, "join"),
        (uuid4(), "join", 1, "leave"),
    )
    try:
        for operation_id, previous, epoch, current in operations:
            session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous,
                previous_epoch=epoch,
                current_membership=current,
            )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
            inventory = session._journal._load_task3_work_inventory(owner)
        assert loaded is not None
        ordered = sorted(
            inventory.work,
            key=lambda item: item.created_revision or 0,
        )
        assert [item.value.room_sequence for item in ordered] == [0, 1, 2, 3]
        assert loaded[1].next_room_sequence == 4
        target = ordered[2]
        _reseal_local_work(
            session,
            target.value.record_id,
            value=replace(target.value, room_sequence=room_sequence),
        )
        assert mutation in {"duplicate", "reversed"}
    finally:
        await session.close()

    with pytest.raises(
        JournalIntegrityError,
        match="local Work exceeds Aggregate sequence frontier",
    ):
        reopen_owned_bootstrap(tmp_path, generation)


@pytest.mark.asyncio
async def test_owned_reopen_rejects_adjacent_local_membership_chain_mismatch(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operations = (
        (uuid4(), "leave", 0, "join"),
        (uuid4(), "join", 0, "leave"),
        (uuid4(), "leave", 1, "join"),
    )
    try:
        for operation_id, previous, epoch, current in operations:
            session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous,
                previous_epoch=epoch,
                current_membership=current,
            )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner)
        ordered = sorted(
            inventory.work,
            key=lambda item: item.created_revision or 0,
        )
        assert [item.value.room_sequence for item in ordered] == [0, 1, 2]
        target = ordered[-1]
        _reseal_local_work(
            session,
            target.value.record_id,
            value=replace(
                target.value,
                source_json=_local_source("invite", 1, "join", 1),
            ),
        )
    finally:
        await session.close()

    with pytest.raises(
        JournalIntegrityError,
        match="local Work membership chain",
    ):
        reopen_owned_bootstrap(tmp_path, generation)


@pytest.mark.asyncio
async def test_owned_local_membership_rejects_pending_hydration_without_held_work(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        stage_classic(
            bootstrap,
            {
                "next_batch": "s1",
                "rooms": {
                    "join": {
                        ROOM: {
                            "state": {"events": []},
                            "timeline": {"events": []},
                        }
                    }
                },
            },
        )
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        while batch := session.next_batch():
            session.acknowledge_batch(batch.ref)
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
            inventory = session._journal._load_task3_work_inventory(owner)
        assert loaded is not None
        aggregate = loaded[1]
        assert aggregate.continuity.membership == "join"
        assert aggregate.continuity.hydration_id is not None
        assert aggregate.pending_hydration is not None
        assert inventory.work == ()

        before = tuple(connection.iterdump())
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with pytest.raises(JournalConflictError, match="barrier"):
            session._publish_local_membership_transition(
                operation_id=uuid4(),
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=aggregate.continuity.membership_epoch,
                current_membership="leave",
            )
        connection.set_trace_callback(None)
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in statements
        )
        assert tuple(connection.iterdump()) == before
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_local_membership_oversized_work_rolls_back_without_poison(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nio.store import _sync_journal as journal_module

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    labels: list[str] = []
    session._journal.set_transition_statement_hook(labels.append)
    real_payload = session._journal._payload
    work_sizes: list[int] = []

    def capture_payload(owner: object, *args: object, **kwargs: object):
        result = real_payload(owner, *args, **kwargs)
        if args and args[0] == "NioIngestWork":
            work_sizes.append(len(result[0]))
        return result

    monkeypatch.setattr(session._journal, "_payload", capture_payload)
    before = tuple(connection.iterdump())
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    oversized_room = f"!{'r' * 1_100_000}:example.org"
    try:
        with pytest.raises(
            LocalProtocolError,
            match="local membership Work exceeds immutable capacity",
        ):
            session._publish_local_membership_transition(
                operation_id=uuid4(),
                room_id=oversized_room,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        connection.set_trace_callback(None)
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert normalized.count("ROLLBACK") == 1
        assert normalized.count("COMMIT") == 0
        assert len(work_sizes) == 1
        assert work_sizes[0] > journal_module._MAX_WORK_PAYLOAD_BYTES
        assert labels == []
        assert not any(
            statement.startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in normalized
        )
        assert tuple(connection.iterdump()) == before

        owner = session._journal.load_owner()
        result = session._publish_local_membership_transition(
            operation_id=uuid4(),
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        )
        assert result == CommitResult(owner.revision + 1)
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_local_membership_work_accepts_exact_last_size_before_limit(
    tmp_path,
) -> None:
    from nio.ingest.model import (
        EventRecord,
        RecordKind,
        _local_membership_record_id,
        _local_membership_source_json,
    )
    from nio.store._sync_journal_plan import _canonical_work_plaintext
    from nio.store._sync_journal_preflight import _canonical_internal
    from nio.store._sync_journal_rows import _MAX_WORK_PAYLOAD_BYTES

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    operation_id = uuid4()
    owner = session._journal.load_owner()

    def candidate(padding: int, operation: UUID, revision: int) -> tuple[str, int]:
        room_id = f"!{'r' * padding}:example.org"
        record_id = _local_membership_record_id(operation)
        event = EventRecord(
            record_id,
            RecordKind.ROOM_LIFECYCLE,
            SystemOrigin(SystemOriginKind.MEMBERSHIP_CHANGE, operation),
            room_id,
            0,
            0,
            None,
            None,
            _local_membership_source_json("leave", 0, "join"),
            None,
        )
        plaintext = _canonical_work_plaintext("event", event)
        payload, _digest = session._journal._payload(
            owner,
            "NioIngestWork",
            plaintext,
            header=_canonical_internal(
                (
                    record_id,
                    "event",
                    "ready",
                    str(operation),
                    room_id,
                    0,
                    0,
                    revision,
                    0,
                    revision,
                )
            ),
        )
        return room_id, len(payload)

    low = 0
    high = 1
    while candidate(high, operation_id, owner.revision + 1)[1] <= (
        _MAX_WORK_PAYLOAD_BYTES
    ):
        low = high
        high *= 2
    while low + 1 < high:
        middle = (low + high) // 2
        if candidate(middle, operation_id, owner.revision + 1)[1] <= (
            _MAX_WORK_PAYLOAD_BYTES
        ):
            low = middle
        else:
            high = middle
    accepted_room, accepted_size = candidate(
        low,
        operation_id,
        owner.revision + 1,
    )
    first_over_room, first_over_size = candidate(
        high,
        operation_id,
        owner.revision + 1,
    )
    assert high == low + 1
    assert accepted_size <= _MAX_WORK_PAYLOAD_BYTES
    assert first_over_size > _MAX_WORK_PAYLOAD_BYTES

    try:
        committed = session._publish_local_membership_transition(
            operation_id=operation_id,
            room_id=accepted_room,
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        )
        assert committed == CommitResult(owner.revision + 1)
        stored = connection.execute(
            "SELECT payload FROM NioIngestWork WHERE account_id = ?",
            (ACCOUNT,),
        ).fetchone()
        assert stored is not None
        assert len(stored[0]) == accepted_size

        rejected_operation = uuid4()
        updated_owner = session._journal.load_owner()
        rejected_room, rejected_size = candidate(
            high,
            rejected_operation,
            updated_owner.revision + 1,
        )
        assert rejected_room == first_over_room
        assert rejected_size == first_over_size
        before_rejection = tuple(connection.iterdump())
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with pytest.raises(
            LocalProtocolError,
            match="local membership Work exceeds immutable capacity",
        ):
            session._publish_local_membership_transition(
                operation_id=rejected_operation,
                room_id=rejected_room,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
        connection.set_trace_callback(None)
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert normalized.count("ROLLBACK") == 1
        assert normalized.count("COMMIT") == 0
        assert not any(
            statement.startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in normalized
        )
        assert tuple(connection.iterdump()) == before_rejection
    finally:
        connection.set_trace_callback(None)
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    try:
        reopened_owner = reopened._journal.load_owner()
        with reopened._journal._owner.read():
            inventory = reopened._journal._load_task3_work_inventory(reopened_owner)
            aggregate = reopened._journal._load_room_aggregate(
                reopened_owner,
                accepted_room,
            )
        assert len(inventory.work) == 1
        assert inventory.work[0].frame_id == operation_id
        assert inventory.work[0].value.room_id == accepted_room
        assert len(inventory.storage_rows[0][11]) == accepted_size
        assert aggregate is not None
        assert aggregate[1].continuity.membership == "join"
    finally:
        reopened.close()


@pytest.mark.parametrize("limit_kind", ("count", "canonical_bytes"))
@pytest.mark.asyncio
async def test_owned_local_membership_total_capacity_retry_precedes_limit(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    limit_kind: str,
) -> None:
    from nio.store import _sync_journal as journal_module

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    labels: list[str] = []
    session._journal.set_transition_statement_hook(labels.append)
    try:
        operation_id, committed, aggregate, _work = _publish_fresh_local_join(session)
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner)
        authenticated_count = len(inventory.storage_rows)
        authenticated_bytes = sum(len(row[11]) for row in inventory.storage_rows)
        assert authenticated_count == 1
        assert authenticated_bytes > 0
        limit_name, authenticated_limit = (
            ("_MAX_TOTAL_WORK_COUNT", authenticated_count)
            if limit_kind == "count"
            else ("_MAX_TOTAL_WORK_CANONICAL_BYTES", authenticated_bytes)
        )
        original_limit = getattr(journal_module, limit_name)
        monkeypatch.setattr(
            journal_module,
            limit_name,
            authenticated_limit,
        )
        labels.clear()

        retry_before = tuple(connection.iterdump())
        retry_statements: list[str] = []
        connection.set_trace_callback(retry_statements.append)
        assert (
            session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
            == committed
        )
        connection.set_trace_callback(None)
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in retry_statements
        )
        assert tuple(connection.iterdump()) == retry_before

        next_operation = uuid4()
        capacity_before = tuple(connection.iterdump())
        capacity_statements: list[str] = []
        connection.set_trace_callback(capacity_statements.append)
        with pytest.raises(
            LocalProtocolError,
            match="local membership Work exceeds immutable capacity",
        ):
            session._publish_local_membership_transition(
                operation_id=next_operation,
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=aggregate.continuity.membership_epoch,
                current_membership="leave",
            )
        connection.set_trace_callback(None)
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in capacity_statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert normalized.count("ROLLBACK") == 1
        assert normalized.count("COMMIT") == 0
        assert labels == []
        assert not any(
            statement.startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in normalized
        )
        assert tuple(connection.iterdump()) == capacity_before

        session._journal.set_transition_statement_hook(None)
        monkeypatch.setattr(
            journal_module,
            limit_name,
            original_limit,
        )
        committed_next = session._publish_local_membership_transition(
            operation_id=next_operation,
            room_id=ROOM,
            previous_membership="join",
            previous_epoch=aggregate.continuity.membership_epoch,
            current_membership="leave",
        )
        assert committed_next == CommitResult(owner.revision + 1)
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()


@pytest.mark.parametrize("path", ("insert", "update"))
@pytest.mark.parametrize(
    "abort_at",
    (
        "meta_revision_epoch_cas",
        "aggregate_insert_or_update",
        "work_insert",
        "before_commit",
    ),
)
@pytest.mark.asyncio
async def test_owned_local_membership_statement_abort_rolls_back_and_retries(
    tmp_path,
    path: str,
    abort_at: str,
) -> None:
    class PublicationAbort(BaseException):
        pass

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    operation_id, previous_membership, previous_epoch, current_membership = (
        _local_publication_target(session, path)
    )
    aggregate_label = f"aggregate_{path}"
    selected_abort = (
        aggregate_label if abort_at == "aggregate_insert_or_update" else abort_at
    )
    sentinel = PublicationAbort(f"{selected_abort} uncertainty")
    labels: list[str] = []

    def abort(label: str) -> None:
        labels.append(label)
        if label == selected_abort:
            raise sentinel

    session._journal.set_transition_statement_hook(abort)
    owner_before = session._journal.load_owner()
    graph_before = tuple(connection.iterdump())
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(PublicationAbort) as failure:
            session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous_membership,
                previous_epoch=previous_epoch,
                current_membership=current_membership,
            )
        connection.set_trace_callback(None)
        assert failure.value is sentinel
        expected_labels = (
            "meta_revision_epoch_cas",
            aggregate_label,
            "work_insert",
            "before_commit",
        )
        assert labels == list(
            expected_labels[: expected_labels.index(selected_abort) + 1]
        )
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert normalized.count("ROLLBACK") == 1
        assert normalized.count("COMMIT") == 0
        assert any(
            statement.startswith("UPDATE NIOINGESTMETA SET REVISION")
            for statement in normalized
        )
        if selected_abort != "meta_revision_epoch_cas":
            assert any(
                "NIOINGESTROOMAGGREGATE" in statement
                and statement.startswith(("INSERT ", "UPDATE "))
                for statement in normalized
            )
        if selected_abort in {"work_insert", "before_commit"}:
            assert any(
                statement.startswith("INSERT INTO NIOINGESTWORK")
                for statement in normalized
            )
        assert tuple(connection.iterdump()) == graph_before
        assert session._journal.load_owner() == owner_before
        client._assert_ingestion_not_poisoned()

        session._journal.set_transition_statement_hook(None)
        committed = session._publish_local_membership_transition(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership=previous_membership,
            previous_epoch=previous_epoch,
            current_membership=current_membership,
        )
        assert committed == CommitResult(owner_before.revision + 1)
        owner_after = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner_after)
        operation_work = tuple(
            item for item in inventory.work if item.frame_id == operation_id
        )
        assert len(operation_work) == 1

        committed_graph = tuple(connection.iterdump())
        retry_statements: list[str] = []
        connection.set_trace_callback(retry_statements.append)
        assert (
            session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous_membership,
                previous_epoch=previous_epoch,
                current_membership=current_membership,
            )
            == committed
        )
        connection.set_trace_callback(None)
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in retry_statements
        )
        assert tuple(connection.iterdump()) == committed_graph
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()


@pytest.mark.parametrize("path", ("insert", "update"))
@pytest.mark.asyncio
async def test_owned_local_membership_commit_uncertainty_retries_committed_operation(
    tmp_path,
    path: str,
) -> None:
    class CommitAbort(BaseException):
        pass

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    operation_id, previous_membership, previous_epoch, current_membership = (
        _local_publication_target(session, path, drain_seed=True)
    )
    owner_before = session._journal.load_owner()
    source_before = session._journal.load_source()
    frames_before = tuple(
        connection.execute("SELECT * FROM NioIngestFrame ORDER BY frame_id")
    )
    meta_before_row = connection.execute("SELECT * FROM NioIngestMeta").fetchone()
    assert meta_before_row is not None
    meta_before = dict(meta_before_row)
    graph_before = tuple(connection.iterdump())
    sentinel = CommitAbort("local membership commit acknowledgement failed")
    labels: list[str] = []

    def abort_commit(label: str) -> None:
        labels.append(label)
        if label == "commit":
            raise sentinel

    session._journal.set_transition_statement_hook(abort_commit)
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(CommitAbort) as failure:
            session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous_membership,
                previous_epoch=previous_epoch,
                current_membership=current_membership,
            )
        connection.set_trace_callback(None)
        assert failure.value is sentinel
        aggregate_label = f"aggregate_{path}"
        assert labels == [
            "meta_revision_epoch_cas",
            aggregate_label,
            "work_insert",
            "before_commit",
            "commit",
        ]
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert normalized.count("COMMIT") == 1
        assert normalized.count("ROLLBACK") == 0
        graph_after = tuple(connection.iterdump())
        assert graph_after != graph_before
        owner_after = session._journal.load_owner()
        committed = CommitResult(owner_before.revision + 1)
        assert owner_after == replace(owner_before, revision=committed.revision)
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner_after)
            loaded = session._journal._load_room_aggregate(owner_after, ROOM)
        operation_work = tuple(
            item for item in inventory.work if item.frame_id == operation_id
        )
        assert len(inventory.work) == 1
        assert len(inventory.storage_rows) == 1
        assert len(operation_work) == 1
        work = operation_work[0]
        current_epoch = 0 if path == "insert" else 1
        room_sequence = 0 if path == "insert" else 1
        storage = next(
            row for row in inventory.storage_rows if row[1] == work.value.record_id
        )
        assert storage[1:11] == (
            work.value.record_id,
            "event",
            "ready",
            str(operation_id),
            ROOM,
            current_epoch,
            room_sequence,
            committed.revision,
            0,
            committed.revision,
        )
        assert work.created_revision == committed.revision
        assert loaded is not None
        assert loaded[1].updated_revision == committed.revision
        assert loaded[1].continuity.membership == current_membership
        assert loaded[1].continuity.membership_epoch == current_epoch
        assert loaded[1].continuity.baseline is None
        assert loaded[1].continuity.gap is None
        assert loaded[1].continuity.hydration_id is None
        assert loaded[1].pending_hydration is None
        assert loaded[1].room_snapshot is None
        assert loaded[1].next_room_sequence == room_sequence + 1
        assert tuple(
            row[0]
            for row in connection.execute(
                "SELECT room_id FROM NioIngestRoomAggregate ORDER BY room_id"
            )
        ) == (ROOM,)
        meta_after_row = connection.execute("SELECT * FROM NioIngestMeta").fetchone()
        assert meta_after_row is not None
        assert dict(meta_after_row) == {
            **meta_before,
            "revision": committed.revision,
        }
        assert session._journal.load_source() == source_before
        assert (
            tuple(connection.execute("SELECT * FROM NioIngestFrame ORDER BY frame_id"))
            == frames_before
        )
        changed_tables = (
            'INSERT INTO "NioIngestMeta"',
            'INSERT INTO "NioIngestRoomAggregate"',
            'INSERT INTO "NioIngestWork"',
        )
        assert tuple(
            line for line in graph_after if not line.startswith(changed_tables)
        ) == tuple(line for line in graph_before if not line.startswith(changed_tables))
        client._assert_ingestion_not_poisoned()

        session._journal.set_transition_statement_hook(None)
        retry_statements: list[str] = []
        connection.set_trace_callback(retry_statements.append)
        assert (
            session._publish_local_membership_transition(
                operation_id=operation_id,
                room_id=ROOM,
                previous_membership=previous_membership,
                previous_epoch=previous_epoch,
                current_membership=current_membership,
            )
            == committed
        )
        connection.set_trace_callback(None)
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
            for statement in retry_statements
        )
        assert tuple(connection.iterdump()) == graph_after

        batch = session.next_batch()
        assert batch is not None
        assert batch.records == (work.value,)
        delivered = work.value
        assert type(delivered.origin) is SystemOrigin
        assert delivered.origin.operation_id == operation_id
        session.acknowledge_batch(batch.ref)
        assert session.next_batch() is None
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_materializer_freezes_full_task4c_outbound_plan_without_applying_it(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    sender_store: SqliteMemoryStore | None = None
    connection = session._owned_store.database.connection()
    staged: StagedFrame | None = None
    retained_plan: object | None = None
    try:
        body, sender_store = _owned_crypto_classic_body(client)
        device_lists = body["device_lists"]
        assert type(device_lists) is dict
        device_lists["changed"] = [BOB]
        assert client.olm is not None
        olm = client.olm
        olm.tracked_users.add(ACCOUNT)
        olm.users_for_key_query.clear()

        claim_user = "@claim:example.org"
        claim_device_id = "CLAIM"
        claim_room = "!claim:example.org"
        claim_algorithm = "m.megolm.v1.aes-sha2"
        claim_device = OlmDevice(
            claim_user,
            claim_device_id,
            {"curve25519": "claim-curve", "ed25519": "claim-ed"},
        )
        olm.wedged_devices.append(claim_device)
        olm.key_request_devices_no_session.append(claim_device)
        waiting_source = {
            "content": {
                "action": "request",
                "body": {
                    "algorithm": claim_algorithm,
                    "room_id": claim_room,
                    "sender_key": "claim-sender-key",
                    "session_id": "claim-session",
                },
                "request_id": "waiting-request",
                "requesting_device_id": claim_device_id,
            },
            "sender": claim_user,
            "type": "m.room_key_request",
        }
        waiting_request = ToDeviceEvent.parse_event(waiting_source)
        assert isinstance(waiting_request, RoomKeyRequest)
        olm.key_requests_waiting_for_session[(claim_user, claim_device_id)][
            waiting_request.request_id
        ] = waiting_request
        claim_rerequest_source = {
            "content": {
                "algorithm": claim_algorithm,
                "ciphertext": "claim-ciphertext",
                "device_id": claim_device_id,
                "sender_key": "claim-sender-key",
                "session_id": "claim-session",
            },
            "event_id": "$claim-rerequest",
            "origin_server_ts": 1,
            "sender": claim_user,
            "type": "m.room.encrypted",
        }
        claim_rerequest = Event.parse_event(claim_rerequest_source)
        assert isinstance(claim_rerequest, MegolmEvent)
        claim_rerequest.room_id = claim_room
        olm.key_re_requests_events[(claim_user, claim_device_id)].append(
            claim_rerequest
        )

        dummy_user = "@dummy:example.org"
        dummy_device_id = "DUMMY"
        dummy_room = "!dummy:example.org"
        dummy_rerequest_source = {
            "content": {
                "algorithm": claim_algorithm,
                "ciphertext": "dummy-rerequest-ciphertext",
                "device_id": dummy_device_id,
                "sender_key": "dummy-rerequest-sender-key",
                "session_id": "dummy-rerequest-session",
            },
            "event_id": "$dummy-rerequest",
            "origin_server_ts": 2,
            "sender": dummy_user,
            "type": "m.room.encrypted",
        }
        dummy_rerequest = Event.parse_event(dummy_rerequest_source)
        assert isinstance(dummy_rerequest, MegolmEvent)
        dummy_rerequest.room_id = dummy_room
        olm.key_re_requests_events[(dummy_user, dummy_device_id)].append(
            dummy_rerequest
        )

        room_key_user = "@room-key:example.org"
        room_key_device = "ROOMKEY"
        room_key_content = {
            "action": "request",
            "body": {
                "algorithm": claim_algorithm,
                "room_id": "!room-key:example.org",
                "sender_key": "room-key-sender-key",
                "session_id": "room-key-session",
            },
            "request_id": "room-key-request",
            "requesting_device_id": DEVICE,
        }
        olm.outgoing_to_device_messages.extend(
            [
                ToDeviceMessage(
                    "org.example.generic",
                    "@generic:example.org",
                    "GENERIC",
                    {"value": "generic"},
                ),
                DummyMessage(
                    "m.room.encrypted",
                    dummy_user,
                    dummy_device_id,
                    {
                        "algorithm": "m.olm.v1.curve25519-aes-sha2",
                        "ciphertext": {
                            "dummy-curve-key": {
                                "body": "dummy-ciphertext",
                                "type": 0,
                            }
                        },
                        "sender_key": "owner-curve-key",
                    },
                ),
                RoomKeyRequestMessage(
                    "m.room_key_request",
                    room_key_user,
                    room_key_device,
                    room_key_content,
                    "room-key-request",
                    "room-key-session",
                    "!room-key:example.org",
                    claim_algorithm,
                ),
            ]
        )
        queued_before = tuple(olm.outgoing_to_device_messages)
        wedged_before = tuple(olm.wedged_devices)
        waiting_devices_before = tuple(olm.key_request_devices_no_session)
        waiting_map_before = {
            target: tuple(requests.items())
            for target, requests in olm.key_requests_waiting_for_session.items()
        }
        rerequest_buckets_before = {
            target: tuple(events)
            for target, events in olm.key_re_requests_events.items()
        }
        staged = stage_classic(bootstrap, body)
        database_before = tuple(connection.iterdump())
        account_before = connection.execute("SELECT account FROM accounts").fetchone()[
            0
        ]

        def callback_tripwire(*_args: object) -> None:
            raise AssertionError("owned outbound freezing invoked a callback")

        for callback_name in (
            "_on_to_device",
            "_on_invited_rooms",
            "_on_event",
            "_on_ephemeral",
            "_on_room_account_data",
            "_on_presence",
            "_on_global_account_data",
            "_on_expired_verifications",
        ):
            setattr(client, callback_name, callback_tripwire)
        monkeypatch.setattr(olm, "handle_response", callback_tripwire)
        monkeypatch.setattr(
            olm,
            "_mark_to_device_message_as_sent",
            callback_tripwire,
        )

        prepared_frames: list[object] = []
        real_prepare = client._prepare_ingestion_frame

        def capture_prepare(*args: object, **kwargs: object):
            assert connection.in_transaction
            assert session._journal._owner._outer_scope == "journal_write"
            prepared = real_prepare(*args, **kwargs)
            prepared_frames.append(prepared)
            return prepared

        client._prepare_ingestion_frame = capture_prepare
        share_bodies: list[bytes] = []
        call_order: list[str] = []
        real_share_keys = olm.share_keys
        real_save_account = olm.save_account

        def capture_share_keys():
            assert connection.in_transaction
            assert session._journal._owner._outer_scope == "journal_write"
            assert len(prepared_frames) == 1
            result = real_share_keys()
            share_bodies.append(canonical_json(result))
            call_order.append("share")
            return result

        def capture_save_account() -> None:
            assert connection.in_transaction
            call_order.append("save")
            real_save_account()

        monkeypatch.setattr(olm, "share_keys", capture_share_keys)
        monkeypatch.setattr(olm, "save_account", capture_save_account)
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        try:
            result = session._materialize_oldest_frame(limits=MaterializerLimits())
        except BaseException:
            connection.set_trace_callback(None)
            transaction_statements = tuple(
                statement.strip().rstrip(";").upper() for statement in statements
            )
            assert len(prepared_frames) == 1
            assert share_bodies == []
            assert transaction_statements.count("BEGIN IMMEDIATE") == 1
            assert transaction_statements.count("ROLLBACK") == 1
            assert transaction_statements.count("COMMIT") == 0
            assert tuple(connection.iterdump()) == database_before
            raise
        connection.set_trace_callback(None)

        assert result.status is MaterializeStatus.MATERIALIZED
        assert result.frame_id == staged.frame_id
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1
        assert transaction_statements.count("COMMIT") == 1
        assert transaction_statements.count("ROLLBACK") == 0
        assert len(prepared_frames) == 1
        prepared = prepared_frames[0]
        delta = prepared.crypto_delta
        assert delta.one_time_key_counts_json == canonical_json(
            body["device_one_time_keys_count"]
        )
        assert delta.unused_fallback_key_types_json == canonical_json(
            body["device_unused_fallback_key_types"]
        )
        assert (
            delta.uploaded_key_count
            == body["device_one_time_keys_count"]["signed_curve25519"]
        )
        assert delta.users_for_key_query == (BOB,)
        assert delta.encrypted_room_ids == (ROOM,)
        assert olm.users_for_key_query == {BOB}
        assert {
            user_id: set(device_ids)
            for user_id, device_ids in olm.get_users_for_key_claiming().items()
        } == {claim_user: {claim_device_id}}
        assert tuple(olm.outgoing_to_device_messages) == queued_before
        assert tuple(olm.wedged_devices) == wedged_before
        assert tuple(olm.key_request_devices_no_session) == waiting_devices_before
        assert {
            target: tuple(requests.items())
            for target, requests in olm.key_requests_waiting_for_session.items()
        } == waiting_map_before
        assert {
            target: tuple(events)
            for target, events in olm.key_re_requests_events.items()
        } == rerequest_buckets_before
        assert client.send_count == 0
        assert len(share_bodies) == 1
        share_index = call_order.index("share")
        assert call_order[share_index + 1] == "save"
        assert (
            connection.execute("SELECT account FROM accounts").fetchone()[0]
            != account_before
        )
        persisted_account = client.store._load_persisted_account()
        assert persisted_account is not None
        assert persisted_account.one_time_keys == olm.account.one_time_keys

        owner = session._journal.load_owner()
        with session._journal._owner.read():
            retained_plan = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner,
            )
        assert retained_plan is not None
        operations = retained_plan.outbound_maintenance.operations
        assert tuple(operation.kind for operation in operations) == (
            "key_upload",
            "key_query",
            "key_claim",
            "to_device",
            "to_device",
            "to_device",
        )
        assert all(operation.state == "pending" for operation in operations)
        assert operations[0].body_json == share_bodies[0]
        assert operations[1].body_json == canonical_json(
            {"device_keys": {user_id: [] for user_id in delta.users_for_key_query}}
        )

        claims = delta.key_claims
        assert len(claims) == 1
        claim = claims[0]
        assert (claim.user_id, claim.device_id) == (claim_user, claim_device_id)
        assert (claim.was_wedged, claim.was_waiting) == (True, True)
        assert len(claim.waiting_key_requests) == 1
        assert len(claim.rerequest_events) == 1
        assert operations[2].body_json == canonical_json(
            {"one_time_keys": {claim.user_id: {claim.device_id: "signed_curve25519"}}}
        )

        def encoded_rerequest(value: object) -> dict[str, object]:
            return {
                "source_json": base64.b64encode(value.source_json).decode("ascii"),
                "room_id": value.room_id,
                "event_id": value.event_id,
                "sender_user_id": value.sender_user_id,
                "sender_device_id": value.sender_device_id,
                "sender_key": value.sender_key,
                "session_id": value.session_id,
                "algorithm": value.algorithm,
            }

        assert operations[2].context == {
            "claims": [
                {
                    "user_id": claim.user_id,
                    "device_id": claim.device_id,
                    "was_wedged": True,
                    "was_waiting": True,
                    "waiting_key_requests": [
                        {
                            "source_json": base64.b64encode(
                                claim.waiting_key_requests[0].source_json
                            ).decode("ascii"),
                            "sender_user_id": claim_user,
                            "requesting_device_id": claim_device_id,
                            "request_id": "waiting-request",
                            "room_id": claim_room,
                            "sender_key": "claim-sender-key",
                            "session_id": "claim-session",
                            "algorithm": claim_algorithm,
                        }
                    ],
                    "rerequest_events": [encoded_rerequest(claim.rerequest_events[0])],
                }
            ]
        }

        messages = delta.queued_to_device_messages
        assert tuple(message.subtype.value for message in messages) == (
            "generic",
            "dummy",
            "room_key_request",
        )
        assert len(messages[1].rerequest_events) == 1
        expected_contexts = (
            {"subtype": "generic"},
            {
                "subtype": "dummy",
                "rerequest_events": [
                    encoded_rerequest(messages[1].rerequest_events[0])
                ],
            },
            {
                "subtype": "room_key_request",
                "request_id": "room-key-request",
                "session_id": "room-key-session",
                "room_id": "!room-key:example.org",
                "algorithm": claim_algorithm,
            },
        )
        for index, (operation, message, expected_context) in enumerate(
            zip(operations[3:], messages, expected_contexts, strict=True),
            start=3,
        ):
            body_json = canonical_json(
                {
                    "messages": {
                        message.recipient_user_id: {
                            message.recipient_device_id: json.loads(
                                message.content_json
                            )
                        }
                    }
                }
            )
            assert operation.body_json == body_json
            assert operation.event_type == message.event_type
            assert operation.context == expected_context
            assert operation.transaction_id == str(
                uuid5(
                    staged.frame_id,
                    "nio.ingest.outbound-maintenance.v1:"
                    f"to-device:{index}:{hashlib.sha256(body_json).hexdigest()}",
                )
            )
    finally:
        connection.set_trace_callback(None)
        if sender_store is not None:
            sender_store.database.close()
        await session.close()

    assert staged is not None
    assert retained_plan is not None
    reopened = reopen_owned_bootstrap(tmp_path, generation)
    try:
        reopened_owner = reopened._journal.load_owner()
        with reopened._journal._owner.read():
            reopened_plan = reopened._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                reopened_owner,
            )
        assert reopened_plan == retained_plan
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("abort_at", ("freeze", "frame_prepared_retain"))
async def test_owned_materializer_poison_requires_fresh_client_after_rollback(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    abort_at: str,
) -> None:
    class FreezeAbort(BaseException):
        pass

    poison_message = "owned ingestion session is poisoned; reopen with a fresh client"
    sentinel = FreezeAbort(f"{abort_at} aborted after account mutation")
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    sender_store: SqliteMemoryStore | None = None
    connection = session._owned_store.database.connection()
    session_closed = False
    staged: StagedFrame | None = None
    saved_prepare_args: tuple[object, ...] | None = None
    try:
        body, sender_store = _owned_crypto_classic_body(client)
        staged = stage_classic(bootstrap, body)
        transition_labels: list[str] = []

        def abort_transition(label: str) -> None:
            transition_labels.append(label)
            if abort_at == "frame_prepared_retain" and label == abort_at:
                raise sentinel

        session._journal.set_transition_statement_hook(abort_transition)
        owner_before = session._journal.load_owner()
        source_before = session._journal.load_source()
        stored_frame_before = session._journal.load_frame(staged.frame_id)
        assert stored_frame_before is not None
        raw_frame_before = connection.execute(
            "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()
        account_before = connection.execute("SELECT * FROM accounts").fetchone()
        aggregates_before = tuple(
            connection.execute("SELECT * FROM NioIngestRoomAggregate ORDER BY room_id")
        )
        work_before = tuple(
            connection.execute("SELECT * FROM NioIngestWork ORDER BY work_id")
        )
        assert aggregates_before == ()
        assert work_before == ()
        database_before = tuple(connection.iterdump())
        assert client.olm is not None
        olm = client.olm
        one_time_keys_before = dict(olm.account.one_time_keys["curve25519"])
        assert one_time_keys_before == {}

        prepared_returns = 0
        real_prepare = client._prepare_ingestion_frame

        def capture_prepare(*args: object, **kwargs: object):
            nonlocal prepared_returns, saved_prepare_args
            if prepared_returns == 0:
                assert connection.in_transaction
                assert session._journal._owner._outer_scope == "journal_write"
            prepared = real_prepare(*args, **kwargs)
            prepared_returns += 1
            saved_prepare_args = args
            return prepared

        client._prepare_ingestion_frame = capture_prepare
        share_returns = 0
        preparation_save_returns = 0
        outbound_save_returns = 0
        real_share_keys = olm.share_keys
        real_save_account = olm.save_account

        def abort_after_share_keys():
            nonlocal share_returns
            assert prepared_returns == 1
            result = real_share_keys()
            share_returns += 1
            generated = dict(olm.account.one_time_keys["curve25519"])
            assert generated
            assert generated != one_time_keys_before
            returned = result["one_time_keys"]
            assert type(returned) is dict
            assert returned
            assert all(
                type(key_id) is str and key_id.startswith("signed_curve25519:")
                for key_id in returned
            )
            if abort_at == "freeze":
                raise sentinel
            return result

        def capture_save_account() -> None:
            nonlocal outbound_save_returns, preparation_save_returns
            real_save_account()
            if prepared_returns == 0:
                preparation_save_returns += 1
            else:
                outbound_save_returns += 1

        monkeypatch.setattr(olm, "share_keys", abort_after_share_keys)
        monkeypatch.setattr(olm, "save_account", capture_save_account)
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with pytest.raises(FreezeAbort) as first_failure:
            session._materialize_oldest_frame(limits=MaterializerLimits())
        connection.set_trace_callback(None)

        assert first_failure.value is sentinel
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1
        assert transaction_statements.count("ROLLBACK") == 1
        assert transaction_statements.count("COMMIT") == 0
        assert prepared_returns == 1
        assert share_returns == 1
        assert preparation_save_returns > 0
        assert outbound_save_returns == (0 if abort_at == "freeze" else 1)
        assert saved_prepare_args is not None
        if abort_at == "freeze":
            assert transition_labels == []
        else:
            assert "meta_revision_epoch_cas" in transition_labels
            assert any(
                label in {"aggregate_insert", "aggregate_update"}
                for label in transition_labels
            )
            assert "work_insert" in transition_labels
            assert transition_labels[-1] == "frame_prepared_retain"
            assert "before_commit" not in transition_labels
            assert any(
                "UPDATE NIOINGESTMETA SET REVISION" in statement
                for statement in transaction_statements
            )
            assert any(
                "NIOINGESTROOMAGGREGATE" in statement
                and ("INSERT INTO" in statement or "UPDATE" in statement)
                for statement in transaction_statements
            )
            assert any(
                "INSERT INTO NIOINGESTWORK" in statement
                for statement in transaction_statements
            )
            assert any(
                "UPDATE NIOINGESTFRAME SET PAYLOAD" in statement
                for statement in transaction_statements
            )
        assert session._journal.load_owner() == owner_before
        assert session._journal.load_source() == source_before
        assert session._journal.load_frame(staged.frame_id) == stored_frame_before
        assert (
            connection.execute(
                "SELECT * FROM NioIngestFrame " "WHERE account_id = ? AND frame_id = ?",
                (ACCOUNT, str(staged.frame_id)),
            ).fetchone()
            == raw_frame_before
        )
        assert connection.execute("SELECT * FROM accounts").fetchone() == account_before
        assert (
            tuple(
                connection.execute(
                    "SELECT * FROM NioIngestRoomAggregate ORDER BY room_id"
                )
            )
            == aggregates_before
        )
        assert (
            tuple(connection.execute("SELECT * FROM NioIngestWork ORDER BY work_id"))
            == work_before
        )
        assert tuple(connection.iterdump()) == database_before

        def assert_poisoned(operation: Callable[[], object]) -> None:
            with pytest.raises(LocalProtocolError) as failure:
                operation()
            assert type(failure.value) is LocalProtocolError
            assert str(failure.value) == poison_message
            assert failure.value.__cause__ is None

        poisoned_trace: list[str] = []
        connection.set_trace_callback(poisoned_trace.append)
        assert_poisoned(
            lambda: session._materialize_oldest_frame(limits=MaterializerLimits())
        )
        assert_poisoned(session.next_batch)
        assert_poisoned(
            lambda: session.acknowledge_batch(
                BatchRef(owner_before.stream_id, 0, uuid4(), bytes(32))
            )
        )
        with pytest.raises(LocalProtocolError) as run_failure:
            await session.run()
        assert type(run_failure.value) is LocalProtocolError
        assert str(run_failure.value) == poison_message
        assert run_failure.value.__cause__ is None
        assert_poisoned(
            lambda: client._prepare_ingestion_frame(
                *saved_prepare_args  # type: ignore[arg-type]
            )
        )
        connection.set_trace_callback(None)
        assert poisoned_trace == []
        assert prepared_returns == 1
        assert share_returns == 1
        assert preparation_save_returns > 0
        assert outbound_save_returns == (0 if abort_at == "freeze" else 1)
        assert tuple(connection.iterdump()) == database_before

        await session.close()
        session_closed = True

        rejected_bootstrap = reopen_owned_bootstrap(tmp_path, generation)
        rejected_connection = rejected_bootstrap._journal._owner.database.connection()
        rejected_trace: list[str] = []
        rejected_connection.set_trace_callback(rejected_trace.append)
        from nio.ingest import coordinator as coordinator_module

        with pytest.raises(LocalProtocolError) as reattach_failure:
            coordinator_module._open_owned_ingestion(
                client,
                rejected_bootstrap,
                config=classic_config(),
                consumer_generation=generation,
                stream_id=owner_before.stream_id,
            )
        rejected_connection.set_trace_callback(None)
        assert type(reattach_failure.value) is LocalProtocolError
        assert str(reattach_failure.value) == poison_message
        assert reattach_failure.value.__cause__ is None
        assert rejected_trace == []
        rejected_bootstrap.close()
    finally:
        if not session_closed:
            connection.set_trace_callback(None)
            await session.close()
        if sender_store is not None:
            sender_store.database.close()

    assert staged is not None
    assert saved_prepare_args is not None
    fresh_bootstrap = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, fresh_bootstrap, generation)
    try:
        fresh_connection = fresh_session._owned_store.database.connection()
        fresh_owner = fresh_session._journal.load_owner()
        assert (
            replace(
                fresh_owner,
                writer_epoch=owner_before.writer_epoch,
            )
            == owner_before
        )
        assert fresh_session._journal.load_source() == source_before
        fresh_staged = fresh_session._journal.load_frame(staged.frame_id)
        assert fresh_staged == stored_frame_before
        assert fresh_staged is not None
        assert fresh_staged.staged_revision == stored_frame_before.staged_revision
        assert (
            fresh_connection.execute(
                "SELECT * FROM NioIngestFrame " "WHERE account_id = ? AND frame_id = ?",
                (ACCOUNT, str(staged.frame_id)),
            ).fetchone()
            == raw_frame_before
        )
        assert fresh_connection.execute("SELECT * FROM accounts").fetchone() == (
            account_before
        )

        fresh_prepare_args: list[tuple[object, ...]] = []
        real_fresh_prepare = fresh_client._prepare_ingestion_frame

        def capture_fresh_prepare(*args: object, **kwargs: object):
            fresh_prepare_args.append(args)
            return real_fresh_prepare(*args, **kwargs)

        fresh_client._prepare_ingestion_frame = capture_fresh_prepare
        result = fresh_session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        assert result.frame_id == staged.frame_id
        assert fresh_prepare_args == [saved_prepare_args]
        assert fresh_session._materialize_oldest_frame(
            limits=MaterializerLimits()
        ) == MaterializeResult(MaterializeStatus.BLOCKED, staged.frame_id, None)
    finally:
        await fresh_session.close()


@pytest.mark.asyncio
async def test_owned_materializer_commit_hook_poison_reopens_committed_graph(
    tmp_path,
) -> None:
    class CommitAbort(BaseException):
        pass

    poison_message = "owned ingestion session is poisoned; reopen with a fresh client"
    sentinel = CommitAbort("commit acknowledgement failed")
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    sender_store: SqliteMemoryStore | None = None
    session_closed = False
    try:
        body, sender_store = _owned_crypto_classic_body(client)
        staged = stage_classic(bootstrap, body)
        owner_before = session._journal.load_owner()
        source_before = session._journal.load_source()
        account_before = connection.execute("SELECT * FROM accounts").fetchone()
        assert account_before is not None

        prepared_returns = 0
        saved_prepare_args: tuple[object, ...] | None = None
        real_prepare = client._prepare_ingestion_frame

        def capture_prepare(*args: object, **kwargs: object):
            nonlocal prepared_returns, saved_prepare_args
            prepared = real_prepare(*args, **kwargs)
            prepared_returns += 1
            saved_prepare_args = args
            return prepared

        client._prepare_ingestion_frame = capture_prepare
        transition_labels: list[str] = []

        def abort_commit(label: str) -> None:
            transition_labels.append(label)
            if label == "commit":
                raise sentinel

        session._journal.set_transition_statement_hook(abort_commit)
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with pytest.raises(CommitAbort) as first_failure:
            session._materialize_oldest_frame(limits=MaterializerLimits())
        connection.set_trace_callback(None)

        assert first_failure.value is sentinel
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1
        assert transaction_statements.count("COMMIT") == 1
        assert transaction_statements.count("ROLLBACK") == 0
        assert transition_labels[-2:] == ["before_commit", "commit"]
        assert "meta_revision_epoch_cas" in transition_labels
        assert any(
            label in {"aggregate_insert", "aggregate_update"}
            for label in transition_labels
        )
        assert "work_insert" in transition_labels
        assert "frame_prepared_retain" in transition_labels
        assert prepared_returns == 1
        assert saved_prepare_args is not None

        owner_after = session._journal.load_owner()
        source_after = session._journal.load_source()
        committed_revision = owner_before.revision + 1
        assert owner_after.revision == committed_revision
        assert source_after == source_before
        account_after = connection.execute("SELECT * FROM accounts").fetchone()
        assert account_after is not None
        assert account_after != account_before
        aggregate_rows_after = tuple(
            connection.execute("SELECT * FROM NioIngestRoomAggregate ORDER BY room_id")
        )
        aggregate_revisions = tuple(
            row[0]
            for row in connection.execute(
                "SELECT updated_revision FROM NioIngestRoomAggregate "
                "ORDER BY room_id"
            )
        )
        work_rows_after = tuple(
            connection.execute("SELECT * FROM NioIngestWork ORDER BY work_id")
        )
        work_revisions = tuple(
            row[0]
            for row in connection.execute(
                "SELECT created_revision FROM NioIngestWork ORDER BY work_id"
            )
        )
        assert aggregate_rows_after
        assert aggregate_revisions == (committed_revision,)
        assert work_rows_after
        assert set(work_revisions) == {committed_revision}
        raw_frame_after = connection.execute(
            "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()
        assert raw_frame_after is not None
        assert session._journal.load_frame(staged.frame_id) is None
        assert session._journal.list_frames(1) == ()
        with session._journal._owner.read():
            headers_after = session._journal._load_authenticated_frame_headers(
                owner_after
            )
            prepared_after = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_after,
            )
        assert len(headers_after) == 1
        assert headers_after[0].frame_id == staged.frame_id
        assert headers_after[0].room_materialized_revision == committed_revision
        assert prepared_after is not None
        assert prepared_after.source_sha256 == staged.response.source_sha256
        assert prepared_after.compatibility_token == "s1"
        assert tuple(
            operation.kind
            for operation in prepared_after.outbound_maintenance.operations
        ) == ("key_upload", "key_query")
        graph_after = tuple(connection.iterdump())

        def assert_poisoned(operation: Callable[[], object]) -> None:
            with pytest.raises(LocalProtocolError) as failure:
                operation()
            assert type(failure.value) is LocalProtocolError
            assert str(failure.value) == poison_message
            assert failure.value.__cause__ is None

        poisoned_trace: list[str] = []
        connection.set_trace_callback(poisoned_trace.append)
        assert_poisoned(
            lambda: session._materialize_oldest_frame(limits=MaterializerLimits())
        )
        assert_poisoned(session.next_batch)
        assert_poisoned(
            lambda: client._prepare_ingestion_frame(
                *saved_prepare_args  # type: ignore[arg-type]
            )
        )
        connection.set_trace_callback(None)
        assert poisoned_trace == []
        assert prepared_returns == 1
        assert tuple(connection.iterdump()) == graph_after

        await session.close()
        session_closed = True
    finally:
        if not session_closed:
            connection.set_trace_callback(None)
            await session.close()
        if sender_store is not None:
            sender_store.database.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    reopened_connection = reopened._journal._owner.database.connection()
    reopened_owner = reopened._journal.load_owner()
    assert (
        replace(
            reopened_owner,
            writer_epoch=owner_after.writer_epoch,
        )
        == owner_after
    )
    assert reopened._journal.load_source() == source_after
    assert reopened._journal.load_frame(staged.frame_id) is None
    assert reopened._journal.list_frames(1) == ()
    assert reopened_connection.execute("SELECT * FROM accounts").fetchone() == (
        account_after
    )
    assert (
        tuple(
            reopened_connection.execute(
                "SELECT * FROM NioIngestRoomAggregate ORDER BY room_id"
            )
        )
        == aggregate_rows_after
    )
    assert (
        tuple(
            reopened_connection.execute("SELECT * FROM NioIngestWork ORDER BY work_id")
        )
        == work_rows_after
    )
    assert (
        reopened_connection.execute(
            "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()
        == raw_frame_after
    )
    with reopened._journal._owner.read():
        reopened_prepared = reopened._journal._load_prepared_frame_with_owner(
            staged.frame_id,
            reopened_owner,
        )
    assert reopened_prepared == prepared_after

    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    try:
        fresh_connection = fresh_session._owned_store.database.connection()
        fresh_prepare_calls = 0
        real_fresh_prepare = fresh_client._prepare_ingestion_frame

        def capture_fresh_prepare(*args: object, **kwargs: object):
            nonlocal fresh_prepare_calls
            fresh_prepare_calls += 1
            return real_fresh_prepare(*args, **kwargs)

        fresh_client._prepare_ingestion_frame = capture_fresh_prepare
        before_idle = tuple(fresh_connection.iterdump())
        assert fresh_session._materialize_oldest_frame(
            limits=MaterializerLimits()
        ) == MaterializeResult(MaterializeStatus.BLOCKED, staged.frame_id, None)
        assert fresh_prepare_calls == 0
        assert tuple(fresh_connection.iterdump()) == before_idle
        fresh_owner = fresh_session._journal.load_owner()
        with fresh_session._journal._owner.read():
            fresh_prepared = fresh_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                fresh_owner,
            )
        assert fresh_prepared == prepared_after
    finally:
        await fresh_session.close()


@pytest.mark.asyncio
async def test_owned_runner_enters_frame_owned_key_upload_before_source_poll(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    staged = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    owner = session._journal.load_owner()
    source = session._journal.load_source()
    with session._journal._owner.read():
        headers = session._journal._load_authenticated_frame_headers(owner)
        prepared = session._journal._load_prepared_frame_with_owner(
            staged.frame_id,
            owner,
        )
        inventory = session._journal._load_task3_work_inventory(owner)
    assert len(headers) == 1
    header = headers[0]
    assert header.frame_id == staged.frame_id
    assert header.room_materialized_revision is not None
    assert header.callbacks_claimed_revision is None
    assert prepared is not None
    assert inventory.work == ()
    assert len(prepared.outbound_maintenance.operations) == 1
    operation = prepared.outbound_maintenance.operations[0]
    assert (operation.kind, operation.state) == ("key_upload", "pending")
    assert operation.transaction_id is None
    assert operation.event_type is None
    assert operation.context is None
    graph_before = tuple(connection.iterdump())

    requests: list[object] = []
    entered = asyncio.Event()
    never_release = asyncio.Event()

    async def observe_request(request, **kwargs: object):
        requests.append(request)
        assert kwargs == {"body_disconnect_retry": True}
        assert (
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            request.method,
            request.path,
            request.query,
            request.body,
            request.timeout_ms,
            request.request_cursor_json,
        ) == (
            owner.stream_id,
            owner.transport_kind,
            header.source_epoch,
            header.request_id,
            "POST",
            "/_matrix/client/v3/keys/upload",
            (),
            operation.body_json,
            classic_config().source.timeout_ms,
            prepared.request_cursor_json,
        )
        entered.set()
        await never_release.wait()
        raise AssertionError("maintenance request unexpectedly resumed")

    session._request = observe_request  # type: ignore[method-assign]
    runner = asyncio.create_task(session.run())
    entered_wait = asyncio.create_task(entered.wait())
    try:
        done, _pending = await asyncio.wait(
            (runner, entered_wait),
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        runner_failure = runner.exception() if runner.done() else None
        assert entered_wait in done, (
            "owned runner stopped before pending maintenance: " f"{runner_failure!r}"
        )
        assert not runner.done()
        assert len(requests) == 1
        assert session._journal.load_source() == source
        assert tuple(connection.iterdump()) == graph_before
        current_owner = session._journal.load_owner()
        with session._journal._owner.read():
            current = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                current_owner,
            )
        assert current == prepared
        assert client.send_count == 0
    finally:
        never_release.set()
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        if not entered_wait.done():
            entered_wait.cancel()
        await asyncio.gather(entered_wait, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_owned_key_upload_response_commits_with_settled_plan(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    staged = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    owner_before = session._journal.load_owner()
    source_before = session._journal.load_source()
    with session._journal._owner.read():
        prepared_before = session._journal._load_prepared_frame_with_owner(
            staged.frame_id,
            owner_before,
        )
        inventory = session._journal._load_task3_work_inventory(owner_before)
    assert prepared_before is not None
    assert inventory.work == ()
    assert len(prepared_before.outbound_maintenance.operations) == 1
    pending = prepared_before.outbound_maintenance.operations[0]
    assert (pending.kind, pending.state, pending.context) == (
        "key_upload",
        "pending",
        None,
    )
    assert client.olm is not None
    olm = client.olm
    assert not olm.account.shared
    assert olm.uploaded_key_count is None
    account_before = connection.execute("SELECT account FROM accounts").fetchone()
    assert account_before is not None

    response_body = canonical_json(
        {
            "one_time_key_counts": {
                "curve25519": 0,
                "signed_curve25519": 7,
            }
        }
    )
    requests: list[object] = []

    async def respond(request, **kwargs: object) -> NetworkResult:
        requests.append(request)
        assert kwargs == {"body_disconnect_retry": True}
        assert (request.path, request.body) == (
            "/_matrix/client/v3/keys/upload",
            pending.body_json,
        )
        return session._network_result(request, 200, response_body)

    session._request = respond  # type: ignore[method-assign]
    parsed_responses: list[KeysUploadResponse] = []
    real_parse = KeysUploadResponse.from_dict

    def observe_parse(_cls, value: object):
        assert not connection.in_transaction
        assert session._journal._owner._outer_scope is None
        response = real_parse(value)  # type: ignore[arg-type]
        assert type(response) is KeysUploadResponse
        parsed_responses.append(response)
        return response

    monkeypatch.setattr(KeysUploadResponse, "from_dict", classmethod(observe_parse))
    applied_responses: list[KeysUploadResponse] = []
    real_handle = olm.handle_response

    def observe_apply(response: object) -> None:
        assert connection.in_transaction
        assert session._journal._owner._outer_scope == "journal_write"
        assert response is parsed_responses[0]
        applied_responses.append(response)
        real_handle(response)

    monkeypatch.setattr(olm, "handle_response", observe_apply)

    def forbid_callback(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("key-upload maintenance invoked a callback")

    for callback_name in (
        "_on_to_device",
        "_on_invited_rooms",
        "_on_event",
        "_on_ephemeral",
        "_on_room_account_data",
        "_on_presence",
        "_on_global_account_data",
        "_on_expired_verifications",
    ):
        monkeypatch.setattr(client, callback_name, forbid_callback)
    monkeypatch.setattr(client, "receive_response", forbid_callback)

    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        assert await session._advance_blocked_frame(staged.frame_id)
        connection.set_trace_callback(None)
        assert len(requests) == 1
        assert len(parsed_responses) == 1
        assert applied_responses == parsed_responses
        assert olm.account.shared
        assert olm.uploaded_key_count == 7
        persisted = client.store._load_persisted_account()
        assert persisted is not None
        assert persisted.shared
        assert connection.execute("SELECT account FROM accounts").fetchone() != (
            account_before
        )

        owner_after = session._journal.load_owner()
        assert owner_after.revision == owner_before.revision + 1
        assert session._journal.load_source() == source_before
        with session._journal._owner.read():
            prepared_after = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_after,
            )
        assert prepared_after is not None
        assert (
            prepared_after._replace(
                outbound_maintenance=prepared_before.outbound_maintenance
            )
            == prepared_before
        )
        assert prepared_after.outbound_maintenance.operations == (
            pending._replace(state="settled", context=None),
        )
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transaction_statements.count("BEGIN IMMEDIATE") == 1
        assert transaction_statements.count("COMMIT") == 1
        assert transaction_statements.count("ROLLBACK") == 0
        assert any(
            statement.lstrip().upper().startswith("UPDATE")
            and "ACCOUNTS" in statement.upper()
            for statement in statements
        )
        assert any(
            statement.lstrip().upper().startswith("UPDATE")
            and "NIOINGESTMETA" in statement.upper()
            for statement in statements
        )
        assert any(
            statement.lstrip().upper().startswith("UPDATE")
            and "NIOINGESTFRAME" in statement.upper()
            for statement in statements
        )
        assert not any(
            "/_matrix/client/v3/sync" in getattr(request, "path", "")
            for request in requests
        )
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_key_query_response_commits_with_settled_plan(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    olm = client.olm
    olm.account.shared = True
    olm.uploaded_key_count = olm.account.max_one_time_keys
    olm.users_for_key_query.add(BOB)
    olm.save_account()
    staged = stage_classic(
        bootstrap,
        {
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {
                "signed_curve25519": olm.account.max_one_time_keys
            },
            "device_unused_fallback_key_types": [],
            "next_batch": "s1",
            "rooms": {},
        },
    )
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    owner_before = session._journal.load_owner()
    with session._journal._owner.read():
        prepared_before = session._journal._load_prepared_frame_with_owner(
            staged.frame_id,
            owner_before,
        )
        inventory = session._journal._load_task3_work_inventory(owner_before)
    assert prepared_before is not None
    assert inventory.work == ()
    assert prepared_before.outbound_maintenance.operations == (
        prepared_before.outbound_maintenance.operations[0],
    )
    pending = prepared_before.outbound_maintenance.operations[0]
    assert (pending.kind, pending.state, pending.context) == (
        "key_query",
        "pending",
        None,
    )
    assert pending.body_json == canonical_json({"device_keys": {BOB: []}})
    assert olm.users_for_key_query == {BOB}
    assert BOB not in olm.tracked_users

    response_body = canonical_json(
        {
            "device_keys": {BOB: {}},
            "failures": {},
        }
    )
    requests: list[object] = []

    async def respond(request, **kwargs: object) -> NetworkResult:
        requests.append(request)
        assert kwargs == {"body_disconnect_retry": True}
        assert (request.method, request.path, request.body) == (
            "POST",
            "/_matrix/client/v3/keys/query",
            pending.body_json,
        )
        return session._network_result(request, 200, response_body)

    session._request = respond  # type: ignore[method-assign]
    connection = session._owned_store.database.connection()
    parsed_responses: list[KeysQueryResponse] = []
    real_parse = KeysQueryResponse.from_dict

    def observe_parse(_cls, value: object):
        assert not connection.in_transaction
        assert session._journal._owner._outer_scope is None
        response = real_parse(value)  # type: ignore[arg-type]
        assert type(response) is KeysQueryResponse
        parsed_responses.append(response)
        return response

    monkeypatch.setattr(KeysQueryResponse, "from_dict", classmethod(observe_parse))
    applied_responses: list[KeysQueryResponse] = []
    real_handle = olm.handle_response

    def observe_apply(response: object) -> None:
        assert connection.in_transaction
        assert session._journal._owner._outer_scope == "journal_write"
        assert response is parsed_responses[0]
        applied_responses.append(response)
        real_handle(response)

    monkeypatch.setattr(olm, "handle_response", observe_apply)

    def forbid_callback(*_args: object, **_kwargs: object) -> None:
        raise AssertionError("key-query maintenance invoked a callback")

    for callback_name in (
        "_on_to_device",
        "_on_invited_rooms",
        "_on_event",
        "_on_ephemeral",
        "_on_room_account_data",
        "_on_presence",
        "_on_global_account_data",
        "_on_expired_verifications",
    ):
        monkeypatch.setattr(client, callback_name, forbid_callback)
    monkeypatch.setattr(client, "receive_response", forbid_callback)

    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        assert await session._advance_blocked_frame(staged.frame_id)
        connection.set_trace_callback(None)
        assert len(requests) == 1
        assert len(parsed_responses) == 1
        assert applied_responses == parsed_responses
        assert BOB not in olm.users_for_key_query
        assert BOB in olm.tracked_users
        owner_after = session._journal.load_owner()
        assert owner_after.revision == owner_before.revision + 1
        with session._journal._owner.read():
            prepared_after = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_after,
            )
        assert prepared_after is not None
        assert (
            prepared_after._replace(
                outbound_maintenance=prepared_before.outbound_maintenance
            )
            == prepared_before
        )
        assert prepared_after.outbound_maintenance.operations == (
            pending._replace(state="settled", context=None),
        )
        transactions = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transactions.count("BEGIN IMMEDIATE") == 1
        assert transactions.count("COMMIT") == 1
        assert transactions.count("ROLLBACK") == 0
        assert any(
            statement.lstrip().upper().startswith("UPDATE")
            and "NIOINGESTMETA" in statement.upper()
            for statement in statements
        )
        assert any(
            statement.lstrip().upper().startswith("UPDATE")
            and "NIOINGESTFRAME" in statement.upper()
            for statement in statements
        )
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.parametrize("with_rerequest", (False, True), ids=("plain", "rerequest"))
@pytest.mark.asyncio
async def test_owned_key_claim_reconstructs_context_and_appends_dummy(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    with_rerequest: bool,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    remote_store = SqliteMemoryStore(BOB, BOB_DEVICE, "remote-secret")
    remote = Olm(BOB, BOB_DEVICE, remote_store)
    remote.account.generate_one_time_keys(1)
    shared = remote.share_keys()
    one_time_key = next(iter(shared["one_time_keys"].items()))
    response_body = canonical_json(
        {
            "failures": {},
            "one_time_keys": {
                BOB: {
                    BOB_DEVICE: {
                        one_time_key[0]: one_time_key[1],
                    }
                }
            },
        }
    )
    staged: StagedFrame | None = None
    prepared_before: object | None = None
    pending: object | None = None
    rerequest_contexts: list[dict[str, object]] = []
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        remote_device = OlmDevice(
            BOB,
            BOB_DEVICE,
            remote.account.identity_keys,
        )
        client.store.save_device_keys({BOB: {BOB_DEVICE: remote_device}})
        olm.device_store.add(remote_device)
        olm.wedged_devices.append(remote_device)
        if with_rerequest:
            rerequest_source = {
                "content": {
                    "algorithm": "m.megolm.v1.aes-sha2",
                    "ciphertext": "rerequest-ciphertext",
                    "device_id": BOB_DEVICE,
                    "sender_key": remote_device.curve25519,
                    "session_id": "missing-session",
                },
                "event_id": "$missing-key",
                "origin_server_ts": 1,
                "sender": BOB,
                "type": "m.room.encrypted",
            }
            rerequest = Event.parse_event(rerequest_source)
            assert type(rerequest) is MegolmEvent
            rerequest.room_id = ROOM
            olm.key_re_requests_events[(BOB, BOB_DEVICE)].append(rerequest)
            rerequest_contexts.append(
                {
                    "source_json": base64.b64encode(
                        canonical_json(rerequest_source)
                    ).decode("ascii"),
                    "room_id": ROOM,
                    "event_id": "$missing-key",
                    "sender_user_id": BOB,
                    "sender_device_id": BOB_DEVICE,
                    "sender_key": remote_device.curve25519,
                    "session_id": "missing-session",
                    "algorithm": "m.megolm.v1.aes-sha2",
                }
            )
        staged = stage_classic(
            bootstrap,
            {
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "s1",
                "rooms": {},
            },
        )
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            prepared_before = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner,
            )
            inventory = session._journal._load_task3_work_inventory(owner)
        assert prepared_before is not None
        assert inventory.work == ()
        assert len(prepared_before.outbound_maintenance.operations) == 1
        pending = prepared_before.outbound_maintenance.operations[0]
        assert (pending.kind, pending.state, pending.context) == (
            "key_claim",
            "pending",
            {
                "claims": [
                    {
                        "device_id": BOB_DEVICE,
                        "rerequest_events": rerequest_contexts,
                        "user_id": BOB,
                        "waiting_key_requests": [],
                        "was_waiting": False,
                        "was_wedged": True,
                    }
                ]
            },
        )
        assert pending.body_json == canonical_json(
            {"one_time_keys": {BOB: {BOB_DEVICE: "signed_curve25519"}}}
        )
    finally:
        await session.close()

    assert staged is not None
    assert prepared_before is not None
    assert pending is not None
    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    connection = fresh_session._owned_store.database.connection()
    expected_operations: object | None = None
    try:
        assert fresh_client.olm is not None
        fresh_olm = fresh_client.olm
        assert fresh_olm.wedged_devices == []
        assert fresh_olm.key_request_devices_no_session == []
        assert dict(fresh_olm.key_re_requests_events) == {}
        assert fresh_olm.outgoing_to_device_messages == []
        stored_device = fresh_olm.device_store[BOB][BOB_DEVICE]
        assert stored_device.curve25519 == remote_device.curve25519
        assert fresh_olm.session_store.get(stored_device.curve25519) is None
        owner_before = fresh_session._journal.load_owner()
        session_rows_before = tuple(
            connection.execute("SELECT * FROM olmsessions ORDER BY session_id")
        )
        requests: list[object] = []

        async def respond(request, **kwargs: object) -> NetworkResult:
            requests.append(request)
            assert kwargs == {"body_disconnect_retry": True}
            assert (request.method, request.path, request.body) == (
                "POST",
                "/_matrix/client/v3/keys/claim",
                pending.body_json,
            )
            return fresh_session._network_result(request, 200, response_body)

        fresh_session._request = respond  # type: ignore[method-assign]
        parsed_responses: list[KeysClaimResponse] = []
        real_parse = KeysClaimResponse.from_dict

        def observe_parse(_cls, value: object):
            assert not connection.in_transaction
            assert fresh_session._journal._owner._outer_scope is None
            response = real_parse(value)  # type: ignore[arg-type]
            assert type(response) is KeysClaimResponse
            parsed_responses.append(response)
            return response

        monkeypatch.setattr(KeysClaimResponse, "from_dict", classmethod(observe_parse))
        applied_responses: list[KeysClaimResponse] = []
        real_handle = fresh_olm.handle_response

        def observe_apply(response: object) -> None:
            assert connection.in_transaction
            assert fresh_session._journal._owner._outer_scope == "journal_write"
            assert response is parsed_responses[0]
            assert fresh_olm.wedged_devices == [stored_device]
            queued_rerequests = fresh_olm.key_re_requests_events[(BOB, BOB_DEVICE)]
            assert len(queued_rerequests) == len(rerequest_contexts)
            if queued_rerequests:
                reconstructed = queued_rerequests[0]
                assert type(reconstructed) is MegolmEvent
                assert (
                    reconstructed.room_id,
                    reconstructed.event_id,
                    reconstructed.sender,
                    reconstructed.device_id,
                    reconstructed.sender_key,
                    reconstructed.session_id,
                    reconstructed.algorithm,
                ) == (
                    ROOM,
                    "$missing-key",
                    BOB,
                    BOB_DEVICE,
                    remote_device.curve25519,
                    "missing-session",
                    "m.megolm.v1.aes-sha2",
                )
            applied_responses.append(response)
            real_handle(response)

        monkeypatch.setattr(fresh_olm, "handle_response", observe_apply)

        def forbid_callback(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("key-claim maintenance invoked a callback")

        for callback_name in (
            "_on_to_device",
            "_on_invited_rooms",
            "_on_event",
            "_on_ephemeral",
            "_on_room_account_data",
            "_on_presence",
            "_on_global_account_data",
            "_on_expired_verifications",
        ):
            monkeypatch.setattr(fresh_client, callback_name, forbid_callback)
        monkeypatch.setattr(fresh_client, "receive_response", forbid_callback)

        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        assert await fresh_session._advance_blocked_frame(staged.frame_id)
        connection.set_trace_callback(None)
        assert len(requests) == 1
        assert len(parsed_responses) == 1
        assert applied_responses == parsed_responses
        assert fresh_olm.wedged_devices == []
        assert len(fresh_olm.key_re_requests_events[(BOB, BOB_DEVICE)]) == len(
            rerequest_contexts
        )
        assert len(fresh_olm.outgoing_to_device_messages) == 1
        dummy = fresh_olm.outgoing_to_device_messages[0]
        assert type(dummy) is DummyMessage
        assert (dummy.recipient, dummy.recipient_device, dummy.type) == (
            BOB,
            BOB_DEVICE,
            "m.room.encrypted",
        )
        assert fresh_olm.session_store.get(stored_device.curve25519) is not None
        session_rows_after = tuple(
            connection.execute("SELECT * FROM olmsessions ORDER BY session_id")
        )
        assert len(session_rows_after) == len(session_rows_before) + 1
        owner_after = fresh_session._journal.load_owner()
        assert owner_after.revision == owner_before.revision + 1
        with fresh_session._journal._owner.read():
            prepared_after = fresh_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_after,
            )
        assert prepared_after is not None
        operations = prepared_after.outbound_maintenance.operations
        assert len(operations) == 2
        assert operations[0] == pending._replace(state="settled", context=None)
        expected_body = canonical_json({"messages": {BOB: {BOB_DEVICE: dummy.content}}})
        assert operations[1].body_json == expected_body
        assert operations[1].event_type == "m.room.encrypted"
        assert operations[1].context == {
            "rerequest_events": rerequest_contexts,
            "subtype": "dummy",
        }
        assert operations[1].transaction_id == str(
            uuid5(
                staged.frame_id,
                "nio.ingest.outbound-maintenance.v1:"
                f"to-device:1:{hashlib.sha256(expected_body).hexdigest()}",
            )
        )
        transactions = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert transactions.count("BEGIN IMMEDIATE") == 1
        assert transactions.count("COMMIT") == 1
        assert transactions.count("ROLLBACK") == 0
    finally:
        connection.set_trace_callback(None)
        await fresh_session.close()

    reopened_again = reopen_owned_bootstrap(tmp_path, generation)
    final_client = owned_client(tmp_path)
    final_session = open_owned_session(final_client, reopened_again, generation)
    try:
        assert final_client.olm is not None
        final_olm = final_client.olm
        assert final_olm.outgoing_to_device_messages == []
        assert dict(final_olm.key_re_requests_events) == {}
        final_owner = final_session._journal.load_owner()
        with final_session._journal._owner.read():
            final_prepared = final_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                final_owner,
            )
        assert final_prepared is not None
        pending_dummy = final_prepared.outbound_maintenance.operations[1]
        requests: list[object] = []

        async def respond(request, **kwargs: object) -> NetworkResult:
            requests.append(request)
            assert kwargs == {"body_disconnect_retry": True}
            assert (request.method, request.path, request.body) == (
                "PUT",
                "/_matrix/client/v3/sendToDevice/"
                f"m.room.encrypted/{pending_dummy.transaction_id}",
                pending_dummy.body_json,
            )
            return final_session._network_result(request, 200, canonical_json({}))

        final_session._request = respond  # type: ignore[method-assign]
        real_handle = final_olm.handle_response
        handled: list[ToDeviceResponse] = []

        def observe_handle(response: object) -> None:
            assert final_session._owned_store.database.connection().in_transaction
            assert final_session._journal._owner._outer_scope == "journal_write"
            assert type(response) is ToDeviceResponse
            assert type(response.to_device_message) is DummyMessage
            assert final_olm.outgoing_to_device_messages == [response.to_device_message]
            assert len(final_olm.key_re_requests_events[(BOB, BOB_DEVICE)]) == len(
                rerequest_contexts
            )
            handled.append(response)
            real_handle(response)

        monkeypatch.setattr(final_olm, "handle_response", observe_handle)
        assert await final_session._advance_blocked_frame(staged.frame_id)
        assert len(requests) == 1
        assert len(handled) == 1
        generated = tuple(final_olm.outgoing_to_device_messages)
        assert len(generated) == len(rerequest_contexts)
        owner_after_dummy = final_session._journal.load_owner()
        assert owner_after_dummy.revision == final_owner.revision + 1
        with final_session._journal._owner.read():
            after_dummy = final_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_after_dummy,
            )
        assert after_dummy is not None
        expected_prefix = (
            final_prepared.outbound_maintenance.operations[0],
            pending_dummy._replace(state="settled", context=None),
        )
        if not rerequest_contexts:
            assert after_dummy.outbound_maintenance.operations == expected_prefix
        else:
            assert len(generated) == 1
            request_message = generated[0]
            assert type(request_message) is RoomKeyRequestMessage
            assert (
                request_message.recipient,
                request_message.recipient_device,
                request_message.request_id,
                request_message.session_id,
                request_message.room_id,
                request_message.algorithm,
            ) == (
                BOB,
                BOB_DEVICE,
                "missing-session",
                "missing-session",
                ROOM,
                "m.megolm.v1.aes-sha2",
            )
            appended = after_dummy.outbound_maintenance.operations[2]
            expected_body = canonical_json(request_message.as_dict())
            assert after_dummy.outbound_maintenance.operations[:2] == expected_prefix
            assert (
                appended.kind,
                appended.state,
                appended.body_json,
                appended.event_type,
                appended.context,
            ) == (
                "to_device",
                "pending",
                expected_body,
                "m.room_key_request",
                {
                    "subtype": "room_key_request",
                    "request_id": "missing-session",
                    "session_id": "missing-session",
                    "room_id": ROOM,
                    "algorithm": "m.megolm.v1.aes-sha2",
                },
            )
            assert appended.transaction_id == str(
                uuid5(
                    staged.frame_id,
                    "nio.ingest.outbound-maintenance.v1:"
                    f"to-device:2:{hashlib.sha256(expected_body).hexdigest()}",
                )
            )
    finally:
        await final_session.close()

    if with_rerequest:
        request_reopen = reopen_owned_bootstrap(tmp_path, generation)
        request_client = owned_client(tmp_path)
        request_session = open_owned_session(request_client, request_reopen, generation)
        try:
            assert request_client.olm is not None
            request_olm = request_client.olm
            assert request_olm.outgoing_to_device_messages == []
            assert request_olm.outgoing_key_requests == {}
            request_owner = request_session._journal.load_owner()
            with request_session._journal._owner.read():
                request_prepared = (
                    request_session._journal._load_prepared_frame_with_owner(
                        staged.frame_id,
                        request_owner,
                    )
                )
            assert request_prepared is not None
            pending_request = request_prepared.outbound_maintenance.operations[2]
            requests: list[object] = []

            async def respond(request, **kwargs: object) -> NetworkResult:
                requests.append(request)
                assert kwargs == {"body_disconnect_retry": True}
                assert (request.method, request.path, request.body) == (
                    "PUT",
                    "/_matrix/client/v3/sendToDevice/"
                    f"m.room_key_request/{pending_request.transaction_id}",
                    pending_request.body_json,
                )
                return request_session._network_result(
                    request,
                    200,
                    canonical_json({}),
                )

            request_session._request = respond  # type: ignore[method-assign]
            assert await request_session._advance_blocked_frame(staged.frame_id)
            assert len(requests) == 1
            assert request_olm.outgoing_to_device_messages == []
            assert tuple(request_olm.outgoing_key_requests) == ("missing-session",)
            persisted_requests = request_client.store.load_outgoing_key_requests()
            assert tuple(persisted_requests) == ("missing-session",)
            persisted_request = persisted_requests["missing-session"]
            assert (
                persisted_request.request_id,
                persisted_request.session_id,
                persisted_request.room_id,
                persisted_request.algorithm,
            ) == (
                "missing-session",
                "missing-session",
                ROOM,
                "m.megolm.v1.aes-sha2",
            )
            settled_owner = request_session._journal.load_owner()
            assert settled_owner.revision == request_owner.revision + 1
            with request_session._journal._owner.read():
                settled_request_frame = (
                    request_session._journal._load_prepared_frame_with_owner(
                        staged.frame_id,
                        settled_owner,
                    )
                )
            assert settled_request_frame is not None
            assert settled_request_frame.outbound_maintenance.operations == (
                *request_prepared.outbound_maintenance.operations[:2],
                pending_request._replace(state="settled", context=None),
            )
        finally:
            await request_session.close()
        remote_store.database.close()


@pytest.mark.asyncio
async def test_owned_key_claim_reconstructs_waiting_request_and_appends_share(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    other_device_id = "OTHER"
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    sender_store = SqliteMemoryStore(BOB, BOB_DEVICE, "sender-secret")
    sender = Olm(BOB, BOB_DEVICE, sender_store)
    remote_store = SqliteMemoryStore(ACCOUNT, other_device_id, "remote-secret")
    remote = Olm(ACCOUNT, other_device_id, remote_store)
    assert client.olm is not None
    olm = client.olm
    recipient_device = OlmDevice(ACCOUNT, DEVICE, olm.account.identity_keys)
    sender_device = OlmDevice(BOB, BOB_DEVICE, sender.account.identity_keys)
    sender.store.save_device_keys({ACCOUNT: {DEVICE: recipient_device}})
    sender.device_store.add(recipient_device)
    sender.verify_device(recipient_device)
    client.store.save_device_keys({BOB: {BOB_DEVICE: sender_device}})
    olm.device_store.add(sender_device)
    olm.verify_device(sender_device)
    olm.account.generate_one_time_keys(1)
    recipient_one_time_key = next(
        iter(olm.account.one_time_keys["curve25519"].values())
    )
    sender.create_session(recipient_one_time_key, recipient_device.curve25519)
    _sharing_with, room_key_message = sender.share_group_session(ROOM, [ACCOUNT])
    sender_group = sender.outbound_group_sessions[ROOM]
    olm.account.mark_keys_as_published()
    olm.account.shared = True
    olm.uploaded_key_count = olm.account.max_one_time_keys
    olm.save_account()

    remote.account.generate_one_time_keys(1)
    remote_shared = remote.share_keys()
    claim_one_time_key = next(iter(remote_shared["one_time_keys"].items()))
    remote_device = OlmDevice(
        ACCOUNT,
        other_device_id,
        remote.account.identity_keys,
    )
    client.store.save_device_keys({ACCOUNT: {other_device_id: remote_device}})
    olm.device_store.add(remote_device)
    olm.verify_device(remote_device)
    request_source = {
        "content": {
            "action": "request",
            "body": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "room_id": ROOM,
                "sender_key": sender.account.identity_keys["curve25519"],
                "session_id": sender_group.id,
            },
            "request_id": "waiting-request",
            "requesting_device_id": other_device_id,
        },
        "sender": ACCOUNT,
        "type": "m.room_key_request",
    }
    waiting_request = ToDeviceEvent.parse_event(request_source)
    assert type(waiting_request) is RoomKeyRequest
    olm.key_request_devices_no_session.append(remote_device)
    olm.key_requests_waiting_for_session[(ACCOUNT, other_device_id)][
        waiting_request.request_id
    ] = waiting_request
    staged = stage_classic(
        bootstrap,
        {
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {
                "signed_curve25519": olm.account.max_one_time_keys
            },
            "device_unused_fallback_key_types": [],
            "next_batch": "s1",
            "rooms": {},
            "to_device": {
                "events": [
                    {
                        "content": room_key_message["messages"][ACCOUNT][DEVICE],
                        "sender": BOB,
                        "type": "m.room.encrypted",
                    }
                ]
            },
        },
    )
    response_body = canonical_json(
        {
            "failures": {},
            "one_time_keys": {
                ACCOUNT: {
                    other_device_id: {
                        claim_one_time_key[0]: claim_one_time_key[1],
                    }
                }
            },
        }
    )
    prepared_before: object | None = None
    pending: object | None = None
    try:
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        batch = session.next_batch()
        assert batch is not None
        assert len(batch.records) == 1
        room_key_record = batch.records[0]
        assert type(room_key_record) is EventRecord
        assert room_key_record.kind is RecordKind.TO_DEVICE
        session.acknowledge_batch(batch.ref)
        assert session.next_batch() is None
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            prepared_before = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner,
            )
        assert prepared_before is not None
        assert len(prepared_before.outbound_maintenance.operations) == 1
        pending = prepared_before.outbound_maintenance.operations[0]
        assert (pending.kind, pending.state) == ("key_claim", "pending")
        assert pending.context == {
            "claims": [
                {
                    "device_id": other_device_id,
                    "rerequest_events": [],
                    "user_id": ACCOUNT,
                    "waiting_key_requests": [
                        {
                            "algorithm": "m.megolm.v1.aes-sha2",
                            "request_id": "waiting-request",
                            "requesting_device_id": other_device_id,
                            "room_id": ROOM,
                            "sender_key": sender.account.identity_keys["curve25519"],
                            "sender_user_id": ACCOUNT,
                            "session_id": sender_group.id,
                            "source_json": base64.b64encode(
                                canonical_json(request_source)
                            ).decode("ascii"),
                        }
                    ],
                    "was_waiting": True,
                    "was_wedged": False,
                }
            ]
        }
    finally:
        await session.close()

    assert prepared_before is not None
    assert pending is not None
    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    connection = fresh_session._owned_store.database.connection()
    try:
        assert fresh_client.olm is not None
        fresh_olm = fresh_client.olm
        stored_device = fresh_olm.device_store[ACCOUNT][other_device_id]
        assert fresh_olm.key_request_devices_no_session == []
        assert dict(fresh_olm.key_requests_waiting_for_session) == {}
        assert fresh_olm.received_key_requests == {}
        assert fresh_olm.outgoing_to_device_messages == []
        assert (
            fresh_olm.inbound_group_store.get(
                ROOM,
                sender.account.identity_keys["curve25519"],
                sender_group.id,
            )
            is not None
        )
        requests: list[object] = []

        async def respond(request, **kwargs: object) -> NetworkResult:
            requests.append(request)
            assert kwargs == {"body_disconnect_retry": True}
            assert (request.path, request.body) == (
                "/_matrix/client/v3/keys/claim",
                pending.body_json,
            )
            return fresh_session._network_result(request, 200, response_body)

        fresh_session._request = respond  # type: ignore[method-assign]
        real_collect = fresh_olm.collect_key_requests
        collected: list[tuple[object, ...]] = []

        def observe_collect():
            assert connection.in_transaction
            assert fresh_session._journal._owner._outer_scope == "journal_write"
            assert fresh_olm.key_request_devices_no_session == []
            assert (
                fresh_olm.key_requests_waiting_for_session[(ACCOUNT, other_device_id)]
                == {}
            )
            assert tuple(fresh_olm.received_key_requests) == ("waiting-request",)
            result = tuple(real_collect())
            collected.append(result)
            return list(result)

        monkeypatch.setattr(fresh_olm, "collect_key_requests", observe_collect)
        owner_before = fresh_session._journal.load_owner()
        assert await fresh_session._advance_blocked_frame(staged.frame_id)
        assert len(requests) == 1
        assert collected == [()]
        assert fresh_olm.received_key_requests == {}
        assert len(fresh_olm.outgoing_to_device_messages) == 1
        share = fresh_olm.outgoing_to_device_messages[0]
        assert type(share) is ToDeviceMessage
        assert (share.recipient, share.recipient_device, share.type) == (
            ACCOUNT,
            other_device_id,
            "m.room.encrypted",
        )
        assert fresh_olm.session_store.get(stored_device.curve25519) is not None
        owner_after = fresh_session._journal.load_owner()
        assert owner_after.revision == owner_before.revision + 1
        with fresh_session._journal._owner.read():
            prepared_after = fresh_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_after,
            )
        assert prepared_after is not None
        operations = prepared_after.outbound_maintenance.operations
        assert len(operations) == 2
        assert operations[0] == pending._replace(state="settled", context=None)
        expected_body = canonical_json(share.as_dict())
        assert (
            operations[1].kind,
            operations[1].state,
            operations[1].body_json,
            operations[1].event_type,
            operations[1].context,
        ) == (
            "to_device",
            "pending",
            expected_body,
            "m.room.encrypted",
            {"subtype": "generic"},
        )
        assert operations[1].transaction_id == str(
            uuid5(
                staged.frame_id,
                "nio.ingest.outbound-maintenance.v1:"
                f"to-device:1:{hashlib.sha256(expected_body).hexdigest()}",
            )
        )
        expected_operations = operations
    finally:
        await fresh_session.close()

    assert expected_operations is not None
    reopened_again = reopen_owned_bootstrap(tmp_path, generation)
    final_client = owned_client(tmp_path)
    final_session = open_owned_session(final_client, reopened_again, generation)
    try:
        assert final_client.olm is not None
        assert final_client.olm.outgoing_to_device_messages == []
        final_device = final_client.olm.device_store[ACCOUNT][other_device_id]
        assert final_client.olm.session_store.get(final_device.curve25519) is not None
        final_owner = final_session._journal.load_owner()
        with final_session._journal._owner.read():
            final_prepared = final_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                final_owner,
            )
        assert final_prepared is not None
        assert final_prepared.outbound_maintenance.operations == expected_operations
        pending_share = final_prepared.outbound_maintenance.operations[1]
        requests: list[object] = []

        async def respond(request, **kwargs: object) -> NetworkResult:
            requests.append(request)
            assert kwargs == {"body_disconnect_retry": True}
            assert (
                request.method,
                request.path,
                request.body,
            ) == (
                "PUT",
                "/_matrix/client/v3/sendToDevice/"
                f"m.room.encrypted/{pending_share.transaction_id}",
                pending_share.body_json,
            )
            return final_session._network_result(request, 200, canonical_json({}))

        final_session._request = respond  # type: ignore[method-assign]
        parsed: list[ToDeviceResponse] = []
        real_parse = ToDeviceResponse.from_dict

        def observe_parse(_cls, value: object, message: object):
            assert not final_session._owned_store.database.connection().in_transaction
            assert final_session._journal._owner._outer_scope is None
            assert type(message) is ToDeviceMessage
            assert message.as_dict() == json.loads(pending_share.body_json)
            response = real_parse(value, message)
            assert type(response) is ToDeviceResponse
            parsed.append(response)
            return response

        monkeypatch.setattr(ToDeviceResponse, "from_dict", classmethod(observe_parse))
        applied: list[ToDeviceResponse] = []
        real_handle = final_client.olm.handle_response

        def observe_handle(response: object) -> None:
            assert final_session._owned_store.database.connection().in_transaction
            assert final_session._journal._owner._outer_scope == "journal_write"
            assert response is parsed[0]
            applied.append(response)
            real_handle(response)

        monkeypatch.setattr(final_client.olm, "handle_response", observe_handle)
        assert await final_session._advance_blocked_frame(staged.frame_id)
        assert len(requests) == 1
        assert applied == parsed
        assert final_client.olm.outgoing_to_device_messages == []
        settled_owner = final_session._journal.load_owner()
        assert settled_owner.revision == final_owner.revision + 1
        with final_session._journal._owner.read():
            settled_prepared = final_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                settled_owner,
            )
        assert settled_prepared is not None
        assert settled_prepared.outbound_maintenance.operations == (
            expected_operations[0],
            pending_share._replace(state="settled", context=None),
        )
    finally:
        await final_session.close()
        sender_store.database.close()
        remote_store.database.close()


@pytest.mark.asyncio
async def test_owned_key_claim_reconstructs_multiple_targets_in_canonical_order(
    tmp_path,
) -> None:
    device_ids = ("A", "B")
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    olm = client.olm
    olm.account.shared = True
    olm.uploaded_key_count = olm.account.max_one_time_keys
    olm.save_account()
    remote_stores: list[SqliteMemoryStore] = []
    devices: dict[str, OlmDevice] = {}
    response_keys: dict[str, object] = {}
    rerequest_context: dict[str, object] | None = None
    for device_id in reversed(device_ids):
        remote_store = SqliteMemoryStore(BOB, device_id, f"remote-{device_id}")
        remote_stores.append(remote_store)
        remote = Olm(BOB, device_id, remote_store)
        remote.account.generate_one_time_keys(1)
        shared = remote.share_keys()
        one_time_key = next(iter(shared["one_time_keys"].items()))
        response_keys[device_id] = {one_time_key[0]: one_time_key[1]}
        device = OlmDevice(BOB, device_id, remote.account.identity_keys)
        devices[device_id] = device
        client.store.save_device_keys({BOB: {device_id: device}})
        olm.device_store.add(device)
        olm.wedged_devices.append(device)
        if device_id == "A":
            source = {
                "content": {
                    "algorithm": "m.megolm.v1.aes-sha2",
                    "ciphertext": "multi-rerequest-ciphertext",
                    "device_id": device_id,
                    "sender_key": device.curve25519,
                    "session_id": "multi-missing-session",
                },
                "event_id": "$multi-rerequest",
                "origin_server_ts": 1,
                "sender": BOB,
                "type": "m.room.encrypted",
            }
            event = Event.parse_event(source)
            assert type(event) is MegolmEvent
            event.room_id = ROOM
            olm.key_re_requests_events[(BOB, device_id)].append(event)
            rerequest_context = {
                "source_json": base64.b64encode(canonical_json(source)).decode("ascii"),
                "room_id": ROOM,
                "event_id": "$multi-rerequest",
                "sender_user_id": BOB,
                "sender_device_id": device_id,
                "sender_key": device.curve25519,
                "session_id": "multi-missing-session",
                "algorithm": "m.megolm.v1.aes-sha2",
            }
    assert rerequest_context is not None
    staged = stage_classic(
        bootstrap,
        {
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {
                "signed_curve25519": olm.account.max_one_time_keys
            },
            "device_unused_fallback_key_types": [],
            "next_batch": "s1",
            "rooms": {},
        },
    )
    pending: object | None = None
    try:
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            prepared = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner,
            )
        assert prepared is not None
        assert len(prepared.outbound_maintenance.operations) == 1
        pending = prepared.outbound_maintenance.operations[0]
        assert pending.kind == "key_claim"
        assert pending.body_json == canonical_json(
            {
                "one_time_keys": {
                    BOB: {device_id: "signed_curve25519" for device_id in device_ids}
                }
            }
        )
        assert [
            (claim["user_id"], claim["device_id"])
            for claim in pending.context["claims"]
        ] == [(BOB, device_id) for device_id in device_ids]
        assert pending.context["claims"][0]["rerequest_events"] == [rerequest_context]
        assert pending.context["claims"][1]["rerequest_events"] == []
    finally:
        await session.close()

    assert pending is not None
    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    try:
        assert fresh_client.olm is not None
        fresh_olm = fresh_client.olm
        assert fresh_olm.wedged_devices == []
        assert dict(fresh_olm.key_re_requests_events) == {}
        assert fresh_olm.outgoing_to_device_messages == []
        response_body = canonical_json(
            {"failures": {}, "one_time_keys": {BOB: response_keys}}
        )

        async def respond(request, **kwargs: object) -> NetworkResult:
            assert kwargs == {"body_disconnect_retry": True}
            assert (request.path, request.body) == (
                "/_matrix/client/v3/keys/claim",
                pending.body_json,
            )
            return fresh_session._network_result(request, 200, response_body)

        fresh_session._request = respond  # type: ignore[method-assign]
        owner_before = fresh_session._journal.load_owner()
        assert await fresh_session._advance_blocked_frame(staged.frame_id)
        assert fresh_olm.wedged_devices == []
        assert tuple(
            (message.recipient, message.recipient_device)
            for message in fresh_olm.outgoing_to_device_messages
        ) == tuple((BOB, device_id) for device_id in device_ids)
        for device_id in device_ids:
            assert (
                fresh_olm.session_store.get(devices[device_id].curve25519) is not None
            )
        owner_after = fresh_session._journal.load_owner()
        assert owner_after.revision == owner_before.revision + 1
        with fresh_session._journal._owner.read():
            prepared_after = fresh_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_after,
            )
        assert prepared_after is not None
        operations = prepared_after.outbound_maintenance.operations
        assert len(operations) == 3
        assert operations[0] == pending._replace(state="settled", context=None)
        for index, (operation, message, device_id) in enumerate(
            zip(
                operations[1:],
                fresh_olm.outgoing_to_device_messages,
                device_ids,
                strict=True,
            ),
            start=1,
        ):
            expected_body = canonical_json(message.as_dict())
            assert operation.body_json == expected_body
            assert operation.transaction_id == str(
                uuid5(
                    staged.frame_id,
                    "nio.ingest.outbound-maintenance.v1:"
                    f"to-device:{index}:{hashlib.sha256(expected_body).hexdigest()}",
                )
            )
            assert operation.context == {
                "subtype": "dummy",
                "rerequest_events": ([rerequest_context] if device_id == "A" else []),
            }
    finally:
        await fresh_session.close()
        for remote_store in remote_stores:
            remote_store.database.close()


@pytest.mark.parametrize(
    "abort_at",
    ("outbound_frame_settle", "commit"),
    ids=("rollback-after-frame-write", "postcommit-uncertainty"),
)
@pytest.mark.parametrize(
    "maintenance_kind",
    (
        "key_upload",
        "key_query",
        "key_claim",
        "multi_key_claim",
        "to_device",
        "dummy",
        "room_key_request",
    ),
)
@pytest.mark.asyncio
async def test_owned_maintenance_response_crash_boundary_requires_fresh_client(
    tmp_path,
    abort_at: str,
    maintenance_kind: str,
) -> None:
    class MaintenanceAbort(BaseException):
        pass

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    assert client.olm is not None
    olm = client.olm
    remote_stores: list[SqliteMemoryStore] = []
    remote_devices: list[OlmDevice] = []
    claim_response_keys: dict[str, object] = {}
    if maintenance_kind in {
        "key_query",
        "key_claim",
        "multi_key_claim",
        "to_device",
        "dummy",
        "room_key_request",
    }:
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        if maintenance_kind == "key_query":
            olm.users_for_key_query.add(BOB)
        elif maintenance_kind in {"key_claim", "multi_key_claim"}:
            device_ids = (
                (BOB_DEVICE,)
                if maintenance_kind == "key_claim"
                else ("multi-a", "multi-b")
            )
            for device_id in device_ids:
                remote_store = SqliteMemoryStore(
                    BOB,
                    device_id,
                    f"remote-{device_id}",
                )
                remote_stores.append(remote_store)
                remote = Olm(BOB, device_id, remote_store)
                remote.account.generate_one_time_keys(1)
                shared = remote.share_keys()
                one_time_key = next(iter(shared["one_time_keys"].items()))
                claim_response_keys[device_id] = {one_time_key[0]: one_time_key[1]}
                remote_device = OlmDevice(
                    BOB,
                    device_id,
                    remote.account.identity_keys,
                )
                remote_devices.append(remote_device)
                client.store.save_device_keys({BOB: {device_id: remote_device}})
                olm.device_store.add(remote_device)
                olm.wedged_devices.append(remote_device)
        elif maintenance_kind == "to_device":
            olm.outgoing_to_device_messages.append(
                ToDeviceMessage(
                    "org.example.maintenance",
                    BOB,
                    BOB_DEVICE,
                    {"value": "durable"},
                )
            )
        elif maintenance_kind == "dummy":
            dummy_rerequest_source = {
                "content": {
                    "algorithm": "m.megolm.v1.aes-sha2",
                    "ciphertext": "dummy-rerequest-ciphertext",
                    "device_id": BOB_DEVICE,
                    "sender_key": "dummy-rerequest-sender-key",
                    "session_id": "dummy-rerequest-session",
                },
                "event_id": "$dummy-rerequest",
                "origin_server_ts": 1,
                "sender": BOB,
                "type": "m.room.encrypted",
            }
            dummy_rerequest = Event.parse_event(dummy_rerequest_source)
            assert type(dummy_rerequest) is MegolmEvent
            dummy_rerequest.room_id = ROOM
            olm.key_re_requests_events[(BOB, BOB_DEVICE)].append(dummy_rerequest)
            olm.outgoing_to_device_messages.append(
                DummyMessage(
                    "m.room.encrypted",
                    BOB,
                    BOB_DEVICE,
                    {
                        "algorithm": "m.olm.v1.curve25519-aes-sha2",
                        "ciphertext": {
                            "dummy-curve-key": {
                                "body": "dummy-ciphertext",
                                "type": 0,
                            }
                        },
                        "sender_key": "owner-curve-key",
                    },
                )
            )
        else:
            olm.outgoing_to_device_messages.append(
                RoomKeyRequestMessage(
                    "m.room_key_request",
                    BOB,
                    BOB_DEVICE,
                    {
                        "action": "request",
                        "body": {
                            "algorithm": "m.megolm.v1.aes-sha2",
                            "room_id": ROOM,
                            "sender_key": "request-sender-key",
                            "session_id": "request-session",
                        },
                        "request_id": "request-session",
                        "requesting_device_id": DEVICE,
                    },
                    "request-session",
                    "request-session",
                    ROOM,
                    "m.megolm.v1.aes-sha2",
                )
            )
        olm.save_account()
        body = {
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {
                "signed_curve25519": olm.account.max_one_time_keys
            },
            "device_unused_fallback_key_types": [],
            "next_batch": "s1",
            "rooms": {},
        }
    else:
        body = {"next_batch": "s1", "rooms": {}}
    staged = stage_classic(bootstrap, body)
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    owner_before = session._journal.load_owner()
    source_before = session._journal.load_source()
    with session._journal._owner.read():
        prepared_before = session._journal._load_prepared_frame_with_owner(
            staged.frame_id,
            owner_before,
        )
    assert prepared_before is not None
    assert len(prepared_before.outbound_maintenance.operations) == 1
    pending = prepared_before.outbound_maintenance.operations[0]
    expected_kind = (
        "to_device"
        if maintenance_kind in {"to_device", "dummy", "room_key_request"}
        else "key_claim" if maintenance_kind == "multi_key_claim" else maintenance_kind
    )
    assert (pending.kind, pending.state) == (expected_kind, "pending")
    account_before = connection.execute("SELECT account FROM accounts").fetchone()
    assert account_before is not None
    sessions_before = tuple(
        connection.execute("SELECT * FROM olmsessions ORDER BY session_id")
    )
    graph_before = tuple(connection.iterdump())
    if maintenance_kind == "key_query":
        response_body = canonical_json({"device_keys": {BOB: {}}, "failures": {}})
    elif maintenance_kind in {"key_claim", "multi_key_claim"}:
        response_body = canonical_json(
            {
                "failures": {},
                "one_time_keys": {BOB: claim_response_keys},
            }
        )
    elif maintenance_kind in {"to_device", "dummy", "room_key_request"}:
        response_body = canonical_json({})
    else:
        response_body = canonical_json(
            {
                "one_time_key_counts": {
                    "curve25519": 0,
                    "signed_curve25519": 7,
                }
            }
        )
    requests: list[object] = []

    async def respond(request, **kwargs: object) -> NetworkResult:
        requests.append(request)
        assert kwargs == {"body_disconnect_retry": True}
        assert (
            request.method
            == {
                "key_upload": "POST",
                "key_query": "POST",
                "key_claim": "POST",
                "multi_key_claim": "POST",
                "to_device": "PUT",
                "dummy": "PUT",
                "room_key_request": "PUT",
            }[maintenance_kind]
        )
        assert (
            request.path
            == {
                "key_upload": "/_matrix/client/v3/keys/upload",
                "key_query": "/_matrix/client/v3/keys/query",
                "key_claim": "/_matrix/client/v3/keys/claim",
                "multi_key_claim": "/_matrix/client/v3/keys/claim",
                "to_device": (
                    "/_matrix/client/v3/sendToDevice/"
                    f"{pending.event_type}/{pending.transaction_id}"
                ),
                "dummy": (
                    "/_matrix/client/v3/sendToDevice/"
                    f"{pending.event_type}/{pending.transaction_id}"
                ),
                "room_key_request": (
                    "/_matrix/client/v3/sendToDevice/"
                    f"{pending.event_type}/{pending.transaction_id}"
                ),
            }[maintenance_kind]
        )
        assert request.body == pending.body_json
        return session._network_result(request, 200, response_body)

    session._request = respond  # type: ignore[method-assign]
    labels: list[str] = []
    sentinel = MaintenanceAbort(abort_at)

    def abort(label: str) -> None:
        labels.append(label)
        if label == abort_at:
            raise sentinel

    session._journal.set_transition_statement_hook(abort)
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(MaintenanceAbort) as failure:
            await session.run()
        connection.set_trace_callback(None)
        assert failure.value is sentinel
        assert len(requests) == 1
        assert "outbound_meta_revision_epoch_cas" in labels
        assert "outbound_frame_settle" in labels
        if maintenance_kind == "key_query":
            assert BOB not in olm.users_for_key_query
            assert BOB in olm.tracked_users
        elif maintenance_kind in {"key_claim", "multi_key_claim"}:
            assert olm.wedged_devices == []
            assert all(
                olm.session_store.get(device.curve25519) is not None
                for device in remote_devices
            )
            assert len(olm.outgoing_to_device_messages) == len(remote_devices)
            assert all(
                type(message) is DummyMessage
                for message in olm.outgoing_to_device_messages
            )
        elif maintenance_kind == "to_device":
            assert olm.outgoing_to_device_messages == []
        elif maintenance_kind == "dummy":
            assert len(olm.outgoing_to_device_messages) == 1
            assert type(olm.outgoing_to_device_messages[0]) is RoomKeyRequestMessage
        elif maintenance_kind == "room_key_request":
            assert olm.outgoing_to_device_messages == []
            assert tuple(olm.outgoing_key_requests) == ("request-session",)
        transaction_statements = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        if abort_at == "outbound_frame_settle":
            assert labels[-1] == "outbound_frame_settle"
            assert "before_commit" not in labels
            assert transaction_statements.count("BEGIN IMMEDIATE") == 1
            assert transaction_statements.count("ROLLBACK") == 1
            assert transaction_statements.count("COMMIT") == 0
            assert tuple(connection.iterdump()) == graph_before
            assert session._journal.load_owner() == owner_before
            assert session._journal.load_source() == source_before
            assert (
                connection.execute("SELECT account FROM accounts").fetchone()
                == account_before
            )
            assert (
                tuple(
                    connection.execute("SELECT * FROM olmsessions ORDER BY session_id")
                )
                == sessions_before
            )
            current_owner = owner_before
            expected_prepared = prepared_before
        else:
            assert labels[-2:] == ["before_commit", "commit"]
            assert transaction_statements.count("BEGIN IMMEDIATE") == 1
            assert transaction_statements.count("COMMIT") == 1
            assert transaction_statements.count("ROLLBACK") == 0
            current_owner = session._journal.load_owner()
            assert current_owner.revision == owner_before.revision + 1
            assert session._journal.load_source() == source_before
            account_after = connection.execute(
                "SELECT account FROM accounts"
            ).fetchone()
            if maintenance_kind == "key_upload":
                assert account_after != account_before
            else:
                assert account_after == account_before
            sessions_after = tuple(
                connection.execute("SELECT * FROM olmsessions ORDER BY session_id")
            )
            assert len(sessions_after) == len(sessions_before) + (
                len(remote_devices)
                if maintenance_kind in {"key_claim", "multi_key_claim"}
                else 0
            )
            with session._journal._owner.read():
                expected_prepared = session._journal._load_prepared_frame_with_owner(
                    staged.frame_id,
                    current_owner,
                )
            assert expected_prepared is not None
            if maintenance_kind in {"key_claim", "multi_key_claim"}:
                expected_claim_operations = [
                    pending._replace(state="settled", context=None)
                ]
                for operation_index, dummy in enumerate(
                    olm.outgoing_to_device_messages,
                    start=1,
                ):
                    expected_body = canonical_json(dummy.as_dict())
                    expected_claim_operations.append(
                        pending.__class__(
                            "to_device",
                            "pending",
                            expected_body,
                            str(
                                uuid5(
                                    staged.frame_id,
                                    "nio.ingest.outbound-maintenance.v1:"
                                    f"to-device:{operation_index}:"
                                    f"{hashlib.sha256(expected_body).hexdigest()}",
                                )
                            ),
                            dummy.type,
                            {"subtype": "dummy", "rerequest_events": []},
                        )
                    )
                expected_operations = tuple(expected_claim_operations)
            elif maintenance_kind == "dummy":
                request_message = olm.outgoing_to_device_messages[0]
                expected_body = canonical_json(request_message.as_dict())
                expected_operations = (
                    pending._replace(state="settled", context=None),
                    pending.__class__(
                        "to_device",
                        "pending",
                        expected_body,
                        str(
                            uuid5(
                                staged.frame_id,
                                "nio.ingest.outbound-maintenance.v1:"
                                "to-device:1:"
                                f"{hashlib.sha256(expected_body).hexdigest()}",
                            )
                        ),
                        request_message.type,
                        {
                            "subtype": "room_key_request",
                            "request_id": request_message.request_id,
                            "session_id": request_message.session_id,
                            "room_id": request_message.room_id,
                            "algorithm": request_message.algorithm,
                        },
                    ),
                )
            else:
                expected_operations = (pending._replace(state="settled", context=None),)
            assert (
                expected_prepared.outbound_maintenance.operations == expected_operations
            )

        poison_message = (
            "owned ingestion session is poisoned; reopen with a fresh client"
        )
        poisoned_sql: list[str] = []
        connection.set_trace_callback(poisoned_sql.append)
        with pytest.raises(LocalProtocolError, match=poison_message):
            session.next_batch()
        connection.set_trace_callback(None)
        assert poisoned_sql == []
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    try:
        reopened_owner = fresh_session._journal.load_owner()
        assert reopened_owner.revision == current_owner.revision
        assert fresh_session._journal.load_source() == source_before
        with fresh_session._journal._owner.read():
            reopened_prepared = fresh_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                reopened_owner,
            )
        assert reopened_prepared == expected_prepared
        assert fresh_client.olm is not None
        assert fresh_client.olm.account.shared == (
            maintenance_kind
            in {
                "key_query",
                "key_claim",
                "multi_key_claim",
                "to_device",
                "dummy",
            }
            or maintenance_kind == "room_key_request"
            or abort_at == "commit"
        )
        assert fresh_client.olm.uploaded_key_count is None
        if maintenance_kind in {"key_claim", "multi_key_claim"}:
            assert all(
                (fresh_client.olm.session_store.get(device.curve25519) is not None)
                == (abort_at == "commit")
                for device in remote_devices
            )
            assert fresh_client.olm.outgoing_to_device_messages == []
        elif maintenance_kind in {"to_device", "dummy"}:
            assert fresh_client.olm.outgoing_to_device_messages == []
        elif maintenance_kind == "room_key_request":
            assert fresh_client.olm.outgoing_to_device_messages == []
            assert tuple(fresh_client.olm.outgoing_key_requests) == (
                ("request-session",) if abort_at == "commit" else ()
            )
    finally:
        await fresh_session.close()
        for remote_store in remote_stores:
            remote_store.database.close()


def _single_owned_maintenance(
    bootstrap,
    client: AsyncClient,
    session,
    kind: str,
) -> tuple[StagedFrame, object]:
    assert client.olm is not None
    if kind == "to_device":
        client.olm.account.shared = True
        client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
        client.olm.outgoing_to_device_messages.append(
            ToDeviceMessage(
                "org.example.retry",
                BOB,
                BOB_DEVICE,
                {"value": "byte-identical"},
            )
        )
        client.olm.save_account()
        body = {
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {
                "signed_curve25519": client.olm.account.max_one_time_keys
            },
            "device_unused_fallback_key_types": [],
            "next_batch": "s1",
            "rooms": {},
        }
    else:
        assert kind == "key_upload"
        body = {"next_batch": "s1", "rooms": {}}
    staged = stage_classic(bootstrap, body)
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    owner = session._journal.load_owner()
    with session._journal._owner.read():
        prepared = session._journal._load_prepared_frame_with_owner(
            staged.frame_id,
            owner,
        )
    assert prepared is not None
    assert len(prepared.outbound_maintenance.operations) == 1
    pending = prepared.outbound_maintenance.operations[0]
    assert pending.state == "pending"
    return staged, pending


@pytest.mark.parametrize("kind", ("key_upload", "to_device"))
@pytest.mark.asyncio
async def test_owned_maintenance_retry_reuses_exact_authenticated_request(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    staged, pending = _single_owned_maintenance(bootstrap, client, session, kind)
    owner_before = session._journal.load_owner()
    graph_before = tuple(connection.iterdump())
    requests: list[object] = []
    attempts = [
        (503, canonical_json({"errcode": "M_UNAVAILABLE"}), None),
        (429, canonical_json({"errcode": "M_LIMIT_EXCEEDED"}), 7),
        (
            200,
            (
                canonical_json({})
                if kind == "to_device"
                else canonical_json(
                    {
                        "one_time_key_counts": {
                            "curve25519": 0,
                            "signed_curve25519": 7,
                        }
                    }
                )
            ),
            None,
        ),
    ]

    async def respond(request, **kwargs: object) -> NetworkResult:
        requests.append(request)
        assert kwargs == {"body_disconnect_retry": True}
        status, body, retry_after = attempts.pop(0)
        return session._network_result(
            request,
            status,
            body,
            retry_after=retry_after,
        )

    session._request = respond  # type: ignore[method-assign]
    sleeps: list[float] = []

    async def observe_sleep(delay: float) -> None:
        assert tuple(connection.iterdump()) == graph_before
        assert session._journal.load_owner() == owner_before
        with session._journal._owner.read():
            current = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_before,
            )
        assert current is not None
        assert current.outbound_maintenance.operations == (pending,)
        sleeps.append(delay)

    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", observe_sleep)
    try:
        assert await session._advance_blocked_frame(staged.frame_id)
        assert attempts == []
        assert requests == [requests[0], requests[0], requests[0]]
        assert sleeps == [0.0, 0.007]
        owner_after = session._journal.load_owner()
        assert owner_after.revision == owner_before.revision + 1
        with session._journal._owner.read():
            prepared_after = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner_after,
            )
        assert prepared_after is not None
        assert prepared_after.outbound_maintenance.operations == (
            pending._replace(state="settled", context=None),
        )
    finally:
        await session.close()


@pytest.mark.parametrize(
    ("status", "body", "errcode"),
    (
        (401, canonical_json({}), "M_UNKNOWN_TOKEN"),
        (403, canonical_json({"errcode": "M_FORBIDDEN"}), "M_FORBIDDEN"),
        (400, canonical_json({"errcode": "M_UNKNOWN_TOKEN"}), "M_UNKNOWN_TOKEN"),
    ),
)
@pytest.mark.asyncio
async def test_owned_maintenance_auth_error_is_sticky_and_retains_frame(
    tmp_path,
    status: int,
    body: bytes,
    errcode: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    staged, pending = _single_owned_maintenance(
        bootstrap,
        client,
        session,
        "key_upload",
    )
    graph_before = tuple(connection.iterdump())
    requests: list[object] = []

    async def respond(request, **kwargs: object) -> NetworkResult:
        requests.append(request)
        assert kwargs == {"body_disconnect_retry": True}
        return session._network_result(request, status, body)

    session._request = respond  # type: ignore[method-assign]
    try:
        with pytest.raises(
            nio.IngestionSourceError,
            match=f"outbound maintenance authentication failed: {errcode}",
        ) as first:
            await session.run()
        with pytest.raises(nio.IngestionSourceError) as second:
            await session.run()
        assert second.value is first.value
        assert len(requests) == 1
        assert tuple(connection.iterdump()) == graph_before
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            retained = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner,
            )
        assert retained is not None
        assert retained.outbound_maintenance.operations == (pending,)
        client._assert_ingestion_not_poisoned()
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_close_cancels_maintenance_backoff_with_pending_frame(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    staged, pending = _single_owned_maintenance(
        bootstrap,
        client,
        session,
        "key_upload",
    )
    sleeping = asyncio.Event()

    async def respond(request, **kwargs: object) -> NetworkResult:
        assert kwargs == {"body_disconnect_retry": True}
        return session._network_result(
            request,
            503,
            canonical_json({"errcode": "M_UNAVAILABLE"}),
        )

    async def held_sleep(_delay: float) -> None:
        sleeping.set()
        await asyncio.Event().wait()

    session._request = respond  # type: ignore[method-assign]
    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", held_sleep)
    runner = asyncio.create_task(session.run())
    try:
        await asyncio.wait_for(sleeping.wait(), 1)
        await session.close()
        with pytest.raises(asyncio.CancelledError):
            await runner
    finally:
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        if session._close_task is None:
            await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    try:
        owner = fresh_session._journal.load_owner()
        with fresh_session._journal._owner.read():
            retained = fresh_session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                owner,
            )
        assert retained is not None
        assert retained.outbound_maintenance.operations == (pending,)
    finally:
        await fresh_session.close()


@pytest.mark.asyncio
async def test_owned_runner_claims_fans_out_retires_then_polls_source(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    staged = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    stored = session._journal.load_frame(staged.frame_id)
    assert stored is not None
    completion_calls: list[object] = []

    async def completion_sink(completion: object) -> None:
        assert not connection.in_transaction
        assert session._journal._owner._outer_scope is None
        claimed_owner = session._journal.load_owner()
        with session._journal._owner.read():
            headers = session._journal._load_authenticated_frame_headers(claimed_owner)
        assert len(headers) == 1
        assert headers[0].frame_id == staged.frame_id
        assert headers[0].callbacks_claimed_revision == claimed_owner.revision
        assert (
            completion.frame_id,
            completion.transport,
            completion.source_epoch,
            completion.request_id,
            completion.staged_revision,
            completion.claimed_revision,
        ) == (
            staged.frame_id,
            TransportKind.CLASSIC,
            stored.response.request.source_epoch,
            stored.response.request.request_id,
            stored.staged_revision,
            claimed_owner.revision,
        )
        completion_calls.append(completion)

    session._completion_sink = completion_sink  # type: ignore[attr-defined]
    response_body = canonical_json(
        {
            "one_time_key_counts": {
                "curve25519": 0,
                "signed_curve25519": 7,
            }
        }
    )
    requests: list[object] = []
    source_entered = asyncio.Event()
    never_release = asyncio.Event()

    async def respond(request, **kwargs: object) -> NetworkResult:
        requests.append(request)
        if len(requests) == 1:
            assert kwargs == {"body_disconnect_retry": True}
            assert request.path == "/_matrix/client/v3/keys/upload"
            return session._network_result(request, 200, response_body)
        assert kwargs == {}
        assert completion_calls
        assert session._journal.load_frame(staged.frame_id) is None
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            assert session._journal._load_authenticated_frame_headers(owner) == ()
        assert request.path == "/_matrix/client/v3/sync"
        source_entered.set()
        await never_release.wait()
        raise AssertionError("source request unexpectedly resumed")

    session._request = respond  # type: ignore[method-assign]
    runner = asyncio.create_task(session.run())
    source_wait = asyncio.create_task(source_entered.wait())
    try:
        done, _pending = await asyncio.wait(
            (runner, source_wait),
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        runner_failure = runner.exception() if runner.done() else None
        assert source_wait in done, (
            "owned runner stopped before completion/next source: " f"{runner_failure!r}"
        )
        assert not runner.done()
        assert len(completion_calls) == 1
        completion = completion_calls[0]
        assert type(completion).__name__ == "_FrameCompletion"
        assert len(requests) == 2
        assert session._journal.load_source().next_request_id == 1
        assert session._journal.load_owner().revision == 5
    finally:
        never_release.set()
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        if not source_wait.done():
            source_wait.cancel()
        await asyncio.gather(source_wait, return_exceptions=True)
        await session.close()


@pytest.mark.parametrize(
    "failure_type",
    (RuntimeError, asyncio.CancelledError),
    ids=("raise", "cancel"),
)
@pytest.mark.asyncio
async def test_owned_claimed_completion_reopens_without_second_sink_call(
    tmp_path,
    failure_type: type[BaseException],
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    staged = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    connection = session._owned_store.database.connection()
    sink_calls: list[object] = []
    sentinel = failure_type("completion sink stopped")

    async def stop_after_claim(completion: object) -> None:
        assert not connection.in_transaction
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            headers = session._journal._load_authenticated_frame_headers(owner)
        assert len(headers) == 1
        assert headers[0].frame_id == staged.frame_id
        assert headers[0].callbacks_claimed_revision == owner.revision
        assert completion.claimed_revision == owner.revision
        sink_calls.append(completion)
        raise sentinel

    session._completion_sink = stop_after_claim  # type: ignore[attr-defined]
    try:
        with pytest.raises(failure_type) as failure:
            await session.run()
        assert failure.value is sentinel
        assert len(sink_calls) == 1
        client._assert_ingestion_not_poisoned()
        claimed_owner = session._journal.load_owner()
        with session._journal._owner.read():
            headers = session._journal._load_authenticated_frame_headers(claimed_owner)
            prepared = session._journal._load_prepared_frame_with_owner(
                staged.frame_id,
                claimed_owner,
            )
            inventory = session._journal._load_task3_work_inventory(claimed_owner)
        assert len(headers) == 1
        assert headers[0].callbacks_claimed_revision == claimed_owner.revision
        assert prepared is not None
        assert prepared.outbound_maintenance.operations == ()
        assert inventory.work == ()
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    second_sink_calls = 0

    async def forbid_second_sink(_completion: object) -> None:
        nonlocal second_sink_calls
        second_sink_calls += 1
        raise AssertionError("claimed completion sink ran twice")

    fresh_session._completion_sink = forbid_second_sink  # type: ignore[attr-defined]
    try:
        result = fresh_session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result == MaterializeResult(
            MaterializeStatus.BLOCKED,
            staged.frame_id,
            None,
        )
        assert await fresh_session._advance_blocked_frame(staged.frame_id)
        assert second_sink_calls == 0
        assert fresh_session._journal.load_frame(staged.frame_id) is None
        owner = fresh_session._journal.load_owner()
        with fresh_session._journal._owner.read():
            assert fresh_session._journal._load_authenticated_frame_headers(owner) == ()
    finally:
        await fresh_session.close()


@pytest.mark.parametrize(
    "abort_at",
    (
        "frame_completion_claim",
        "claim_commit",
        "frame_completion_retire",
        "retire_commit",
    ),
    ids=("claim-rollback", "claim-postcommit", "retire-rollback", "retire-postcommit"),
)
@pytest.mark.asyncio
async def test_owned_completion_statement_boundary_reopens_exact_fate(
    tmp_path,
    abort_at: str,
) -> None:
    class CompletionAbort(BaseException):
        pass

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    staged = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    revision_before_materialization = session._journal.load_owner().revision
    assert session._materialize_oldest_frame(limits=MaterializerLimits()) == (
        MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            revision_before_materialization + 1,
        )
    )
    connection = session._owned_store.database.connection()
    owner_before = session._journal.load_owner()
    graph_before = tuple(connection.iterdump())
    sink_calls: list[object] = []

    async def completion_sink(completion: object) -> None:
        sink_calls.append(completion)

    session._completion_sink = completion_sink  # type: ignore[attr-defined]
    labels: list[str] = []
    sentinel = CompletionAbort(abort_at)
    commit_count = 0

    def abort(label: str) -> None:
        nonlocal commit_count
        labels.append(label)
        if label == "commit":
            commit_count += 1
        if label == abort_at:
            raise sentinel
        if abort_at == "claim_commit" and label == "commit" and commit_count == 1:
            raise sentinel
        if abort_at == "retire_commit" and label == "commit" and commit_count == 2:
            raise sentinel

    session._journal.set_transition_statement_hook(abort)
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(CompletionAbort) as failure:
            await session._advance_blocked_frame(staged.frame_id)
        connection.set_trace_callback(None)
        assert failure.value is sentinel
        transactions = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        if abort_at == "frame_completion_claim":
            assert labels == [
                "completion_meta_revision_epoch_cas",
                "frame_completion_claim",
            ]
            assert transactions.count("BEGIN IMMEDIATE") == 1
            assert transactions.count("ROLLBACK") == 1
            assert transactions.count("COMMIT") == 0
            assert tuple(connection.iterdump()) == graph_before
            assert session._journal.load_owner() == owner_before
            assert sink_calls == []
            claimed = False
        elif abort_at == "claim_commit":
            assert labels[-2:] == ["before_commit", "commit"]
            assert transactions.count("BEGIN IMMEDIATE") == 1
            assert transactions.count("ROLLBACK") == 0
            assert transactions.count("COMMIT") == 1
            assert sink_calls == []
            claimed = True
        elif abort_at == "frame_completion_retire":
            assert labels[-2:] == [
                "completion_retire_meta_revision_epoch_cas",
                "frame_completion_retire",
            ]
            assert transactions.count("BEGIN IMMEDIATE") == 2
            assert transactions.count("ROLLBACK") == 1
            assert transactions.count("COMMIT") == 1
            assert len(sink_calls) == 1
            claimed = True
        else:
            assert labels[-2:] == ["before_commit", "commit"]
            assert transactions.count("BEGIN IMMEDIATE") == 2
            assert transactions.count("ROLLBACK") == 0
            assert transactions.count("COMMIT") == 2
            assert len(sink_calls) == 1
            claimed = False

        owner_after = session._journal.load_owner()
        if abort_at == "frame_completion_claim":
            assert owner_after == owner_before
        elif abort_at in {"claim_commit", "frame_completion_retire"}:
            assert owner_after.revision == owner_before.revision + 1
        else:
            assert owner_after.revision == owner_before.revision + 2
        with session._journal._owner.read():
            headers = session._journal._load_authenticated_frame_headers(owner_after)
        if abort_at == "retire_commit":
            assert headers == ()
        else:
            assert len(headers) == 1
            assert headers[0].frame_id == staged.frame_id
            assert (headers[0].callbacks_claimed_revision is not None) is claimed
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    fresh_sink_calls: list[object] = []

    async def fresh_sink(completion: object) -> None:
        fresh_sink_calls.append(completion)

    fresh_session._completion_sink = fresh_sink  # type: ignore[attr-defined]
    try:
        result = fresh_session._materialize_oldest_frame(limits=MaterializerLimits())
        if abort_at == "retire_commit":
            assert result == MaterializeResult(MaterializeStatus.IDLE, None, None)
        else:
            assert result == MaterializeResult(
                MaterializeStatus.BLOCKED,
                staged.frame_id,
                None,
            )
            assert await fresh_session._advance_blocked_frame(staged.frame_id)
        assert len(fresh_sink_calls) == (abort_at == "frame_completion_claim")
        assert fresh_session._journal.load_frame(staged.frame_id) is None
        fresh_owner = fresh_session._journal.load_owner()
        with fresh_session._journal._owner.read():
            assert (
                fresh_session._journal._load_authenticated_frame_headers(fresh_owner)
                == ()
            )
    finally:
        await fresh_session.close()


@pytest.mark.parametrize("settle_privately", (False, True), ids=("ack", "settle"))
@pytest.mark.asyncio
async def test_owned_runner_waits_read_only_for_record_ack_then_resumes(
    tmp_path,
    settle_privately: bool,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    event_source = {
        "content": {"value": "wait-for-ack"},
        "type": "org.example.task4f.wait",
    }
    staged = stage_classic(
        bootstrap,
        {
            "account_data": {"events": [event_source]},
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {
                "signed_curve25519": client.olm.account.max_one_time_keys
            },
            "device_unused_fallback_key_types": [],
            "next_batch": "s1",
            "presence": {"events": []},
            "rooms": {},
            "to_device": {"events": []},
        },
    )
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    batch = session.next_batch(max_records=1)
    assert batch is not None and len(batch.records) == 1
    record = batch.records[0]
    assert type(record) is EventRecord
    assert record.kind is RecordKind.GLOBAL_ACCOUNT_DATA
    assert record.source_json == canonical_json(event_source)
    connection = session._owned_store.database.connection()
    completion_calls: list[object] = []

    async def completion_sink(completion: object) -> None:
        completion_calls.append(completion)

    session._completion_sink = completion_sink  # type: ignore[attr-defined]
    maintenance_checked = asyncio.Event()
    real_load_maintenance = session._journal._load_ready_outbound_maintenance

    def observe_maintenance_check():
        pending = real_load_maintenance()
        assert pending is None
        maintenance_checked.set()
        return pending

    session._journal._load_ready_outbound_maintenance = observe_maintenance_check
    source_entered = asyncio.Event()
    never_release = asyncio.Event()
    requests: list[object] = []

    async def respond(request, **kwargs: object) -> NetworkResult:
        requests.append(request)
        assert kwargs == {}
        assert request.path == "/_matrix/client/v3/sync"
        assert completion_calls
        assert session._journal.load_frame(staged.frame_id) is None
        source_entered.set()
        await never_release.wait()
        raise AssertionError("source request unexpectedly resumed")

    session._request = respond  # type: ignore[method-assign]
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    runner = asyncio.create_task(session.run())
    maintenance_wait = asyncio.create_task(maintenance_checked.wait())
    source_wait = asyncio.create_task(source_entered.wait())
    try:
        done, _pending = await asyncio.wait(
            (runner, maintenance_wait),
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        runner_failure = runner.exception() if runner.done() else None
        assert maintenance_wait in done, (
            "runner stopped before the Work wait boundary: " f"{runner_failure!r}"
        )
        await asyncio.sleep(0)
        assert not runner.done(), f"record wait became terminal: {runner_failure!r}"
        assert not any(
            statement.strip().rstrip(";").upper() == "BEGIN IMMEDIATE"
            for statement in statements
        )
        assert requests == []
        assert completion_calls == []
        assert session.next_batch(max_records=1) == batch

        if settle_privately:
            await session._settle_batch(
                batch,
                receipt_new=False,
                semantic_event_new=False,
            )
        else:
            session.acknowledge_batch(batch.ref)
        done, _pending = await asyncio.wait(
            (runner, source_wait),
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        runner_failure = runner.exception() if runner.done() else None
        assert source_wait in done, (
            "record ack did not resume completion: " f"{runner_failure!r}"
        )
        assert not runner.done()
        assert len(completion_calls) == 1
        assert len(requests) == 1
        assert session.next_batch(max_records=1) is None
    finally:
        connection.set_trace_callback(None)
        never_release.set()
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        for waiter in (maintenance_wait, source_wait):
            if not waiter.done():
                waiter.cancel()
        await asyncio.gather(maintenance_wait, source_wait, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_blocked_frame_probes_do_not_scan_unrelated_work_inventory(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    staged = stage_classic(
        bootstrap,
        {
            "account_data": {
                "events": [
                    {
                        "content": {"sequence": index},
                        "type": "org.example.bounded-frame-probe",
                    }
                    for index in range(500)
                ]
            },
            "next_batch": "s1",
            "rooms": {},
        },
    )
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )

    monkeypatch.setattr(
        session._journal,
        "_load_task3_work_inventory",
        lambda *_args, **_kwargs: pytest.fail("blocked-frame probe scanned all Work"),
    )
    try:
        assert session._journal._load_ready_outbound_maintenance() is None
        assert session._journal._oldest_prepared_frame_has_work(staged.frame_id)
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_runner_hydrates_held_work_before_waiting_for_pump(
    tmp_path,
) -> None:
    class HydratedWorkWaitReached(Exception):
        pass

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    staged = stage_classic(bootstrap, _local_room_body())
    hydration = FakeResponse(200, hydration_state())
    client.responses.append(hydration)
    waits: list[int] = []

    async def stop_at_hydrated_work(observed_generation: int) -> None:
        waits.append(observed_generation)
        assert client.send_count == 1
        assert client.send_calls[0][0][:2] == (
            "GET",
            "/_matrix/client/v3/rooms/%21diagnostic%3Aexample.org/state",
        )
        assert hydration.released
        assert session._journal.load_pending_hydrations(limit=2) == ()
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            headers = session._journal._load_authenticated_frame_headers(owner)
            inventory = session._journal._load_task3_work_inventory(owner)
        assert len(headers) == 1 and headers[0].frame_id == staged.frame_id
        assert [work.status for work in inventory.work] == [
            "ready",
            "ready",
            "ready",
        ]
        raise HydratedWorkWaitReached

    session._wait_for_progress = stop_at_hydrated_work  # type: ignore[method-assign]
    try:
        with pytest.raises(HydratedWorkWaitReached):
            await session.run()
        assert len(waits) == 1
        assert client.send_count == 1
        client._assert_ingestion_not_poisoned()
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_hydration_commit_wakes_rearmed_work_pump(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    class CoordinatedHydrationContent(FakeContent):
        def __init__(self, body: bytes) -> None:
            super().__init__(body)
            self.entered = asyncio.Event()
            self.release_read = asyncio.Event()

        async def read(self, size: int = -1) -> bytes:
            if not self.entered.is_set():
                self.entered.set()
                await self.release_read.wait()
            return await super().read(size)

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    stage_classic(bootstrap, _local_room_body())
    content = CoordinatedHydrationContent(hydration_state())
    hydration = FakeResponse(200, b"")
    hydration.content = content
    client.responses.append(hydration)
    pump_rearmed = asyncio.Event()
    hydration_committed = asyncio.Event()
    real_apply_hydration = session._journal.apply_hydration_result

    def observe_hydration_commit(*, result):
        committed = real_apply_hydration(result=result)
        assert committed is not None
        hydration_committed.set()
        return committed

    monkeypatch.setattr(
        session._journal,
        "apply_hydration_result",
        observe_hydration_commit,
    )

    async def pump_once():
        await session._wait_for_work()
        lifecycle = session.next_batch(max_records=1)
        assert lifecycle is not None and len(lifecycle.records) == 1
        lifecycle_record = lifecycle.records[0]
        assert type(lifecycle_record) is EventRecord
        assert lifecycle_record.kind is RecordKind.ROOM_LIFECYCLE
        session.acknowledge_batch(lifecycle.ref)
        assert session.next_batch(max_records=1) is None
        pump_rearmed.set()
        await session._wait_for_work()
        batch = session.next_batch(max_records=1)
        assert batch is not None and len(batch.records) == 1
        return batch

    pump = asyncio.create_task(pump_once())
    await asyncio.sleep(0)
    runner = asyncio.create_task(session.run())
    try:
        await asyncio.wait_for(content.entered.wait(), timeout=2)
        await asyncio.wait_for(pump_rearmed.wait(), timeout=2)
        assert not session._progress_event.is_set()
        assert session.next_batch(max_records=1) is None
        content.release_read.set()
        await asyncio.wait_for(hydration_committed.wait(), timeout=2)
        assert session._journal.load_pending_hydrations(limit=2) == ()
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner)
        assert [work.status for work in inventory.work] == ["ready", "ready"]
        done, _pending = await asyncio.wait(
            (runner, pump),
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        runner_failure = runner.exception() if runner.done() else None
        assert pump in done, (
            "hydration commit did not wake the rearmed Work pump: "
            f"{runner_failure!r}"
        )
        batch = pump.result()
        assert not runner.done()
        assert client.send_count == 1
        assert hydration.released
        assert session.next_batch(max_records=1) == batch
        client._assert_ingestion_not_poisoned()
    finally:
        content.release_read.set()
        for task in (runner, pump):
            if not task.done():
                task.cancel()
        await asyncio.gather(runner, pump, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_owned_work_wait_wakes_when_materialization_publishes_delivery(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    connection = session._owned_store.database.connection()
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    waiting = asyncio.create_task(session._wait_for_work())
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))
            for statement in statements
        )

        event_source = {
            "content": {"value": "wake-pump"},
            "type": "org.example.task5d.wake",
        }
        stage_classic(
            bootstrap,
            {
                "account_data": {"events": [event_source]},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": client.olm.account.max_one_time_keys,
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "s1",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": []},
            },
        )
        assert not waiting.done()
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        await asyncio.wait_for(waiting, timeout=2)
        batch = session.next_batch(max_records=1)
        assert batch is not None and len(batch.records) == 1
        record = batch.records[0]
        assert type(record) is EventRecord
        assert record.kind is RecordKind.GLOBAL_ACCOUNT_DATA
        assert record.source_json == canonical_json(event_source)
    finally:
        connection.set_trace_callback(None)
        if not waiting.done():
            waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("pending_kind", ("intent", "work"))
async def test_owned_local_membership_idle_waits_for_durable_progress(
    tmp_path,
    pending_kind: str,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    operation_id = uuid4()
    if pending_kind == "intent":
        session._journal._record_local_membership_intent(
            operation_id=operation_id,
            room_id=ROOM,
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        )
    else:
        operation_id, _committed, _aggregate, _work = _publish_fresh_local_join(
            session,
        )

    waiting = asyncio.create_task(session._wait_for_local_membership_idle())
    try:
        await asyncio.sleep(0)
        assert not waiting.done()
        if pending_kind == "intent":
            pending = session._journal._load_pending_local_membership_intents(
                limit=2,
            )
            assert len(pending) == 1
            assert pending[0].intent.operation_id == operation_id
            session._journal._clear_local_membership_intent(
                room_id=ROOM,
                intent=pending[0].intent,
            )
            session._signal_progress()
        else:
            batch = session.next_batch(max_records=1)
            assert batch is not None
            session.acknowledge_batch(batch.ref)
        await asyncio.wait_for(waiting, timeout=2)
    finally:
        if not waiting.done():
            waiting.cancel()
        await asyncio.gather(waiting, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_runner_yields_before_synchronous_materialization_catchup(
    tmp_path,
) -> None:
    backlog_size = 500
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    client = RecordingClient()
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    frame_id = uuid4()
    remaining = [
        MaterializeResult(MaterializeStatus.MATERIALIZED, frame_id, revision)
        for revision in range(1, backlog_size + 1)
    ]

    def materialize_synchronously(*, limits: MaterializerLimits) -> MaterializeResult:
        assert limits == MaterializerLimits()
        if remaining:
            return remaining.pop()
        return MaterializeResult(MaterializeStatus.AT_CAPACITY, frame_id, None)

    session._materialize_oldest_frame = materialize_synchronously  # type: ignore[method-assign]
    runner = asyncio.create_task(session.run())

    async def competing_ready_task() -> int:
        return len(remaining)

    competing = asyncio.create_task(competing_ready_task())
    try:
        assert await asyncio.wait_for(competing, timeout=2) == backlog_size
    finally:
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_owned_runner_uses_private_materializer_in_fifo_order(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    first = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    second = stage_classic(bootstrap, {"next_batch": "s2", "rooms": {}})
    stored_first = session._journal.load_frame(first.frame_id)
    stored_second = session._journal.load_frame(second.frame_id)
    assert stored_first is not None
    assert stored_second is not None
    assert stored_first.staged_revision < stored_second.staged_revision
    revision_before_run = session._journal.load_owner().revision

    http_calls = 0

    async def forbid_http(*_args: object, **_kwargs: object) -> None:
        nonlocal http_calls
        http_calls += 1
        raise AssertionError("owned runner attempted HTTP")

    monkeypatch.setattr(client, "send", forbid_http)
    private_results: list[MaterializeResult] = []
    real_private_materializer = session._materialize_oldest_frame

    def capture_private_materializer(*args: object, **kwargs: object):
        result = real_private_materializer(*args, **kwargs)
        private_results.append(result)
        return result

    session._materialize_oldest_frame = capture_private_materializer
    prepared_args: list[tuple[object, ...]] = []
    real_prepare = client._prepare_ingestion_frame

    def capture_prepare(*args: object, **kwargs: object):
        prepared = real_prepare(*args, **kwargs)
        prepared_args.append(args)
        return prepared

    client._prepare_ingestion_frame = capture_prepare
    completion_calls: list[object] = []
    completion_entered = asyncio.Event()
    never_release = asyncio.Event()

    async def hold_first_completion(completion: object) -> None:
        completion_calls.append(completion)
        completion_entered.set()
        await never_release.wait()
        raise AssertionError("first completion unexpectedly resumed")

    session._completion_sink = hold_first_completion  # type: ignore[attr-defined]
    runner = asyncio.create_task(session.run())
    completion_wait = asyncio.create_task(completion_entered.wait())
    try:
        done, _pending = await asyncio.wait(
            (runner, completion_wait),
            timeout=2,
            return_when=asyncio.FIRST_COMPLETED,
        )
        runner_failure = runner.exception() if runner.done() else None
        assert completion_wait in done, (
            "owned runner stopped before first completion: " f"{runner_failure!r}"
        )
        assert not runner.done()
        assert http_calls == 0
        assert len(completion_calls) == 1
        assert completion_calls[0].frame_id == first.frame_id
        assert len(prepared_args) == 1
        assert getattr(prepared_args[0][0], "frame_id") == first.frame_id
        assert prepared_args[0][1] == stored_first.staged_revision
        assert tuple(
            (result.status, result.frame_id, result.revision)
            for result in private_results
        ) == (
            (
                MaterializeStatus.MATERIALIZED,
                first.frame_id,
                revision_before_run + 1,
            ),
            (MaterializeStatus.BLOCKED, first.frame_id, None),
        )
        assert session._journal.load_frame(first.frame_id) is None
        assert session._journal.load_frame(second.frame_id) == stored_second
        assert session._journal.list_frames(2) == (stored_second,)
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            headers = session._journal._load_authenticated_frame_headers(owner)
            prepared = session._journal._load_prepared_frame_with_owner(
                first.frame_id,
                owner,
            )
        assert tuple(header.frame_id for header in headers) == (
            first.frame_id,
            second.frame_id,
        )
        assert headers[0].room_materialized_revision is not None
        assert headers[0].callbacks_claimed_revision == owner.revision
        assert headers[1].room_materialized_revision is None
        assert prepared is not None
    finally:
        never_release.set()
        if not runner.done():
            runner.cancel()
        await asyncio.gather(runner, return_exceptions=True)
        if not completion_wait.done():
            completion_wait.cancel()
        await asyncio.gather(completion_wait, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_owned_private_blocked_preflight_authenticates_retained_frame_payload(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        staged = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        payload = connection.execute(
            "SELECT payload FROM NioIngestFrame "
            "WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT, str(staged.frame_id)),
        ).fetchone()[0]
        assert type(payload) is bytes
        tampered = bytes([payload[0] ^ 1]) + payload[1:]
        assert tampered != payload
        assert len(tampered) == len(payload)
        updated = connection.execute(
            "UPDATE NioIngestFrame SET payload = ? "
            "WHERE account_id = ? AND frame_id = ?",
            (tampered, ACCOUNT, str(staged.frame_id)),
        )
        assert updated.rowcount == 1

        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError, match="Frame"):
            session._materialize_oldest_frame(limits=MaterializerLimits())
        connection.set_trace_callback(None)
        assert not any(
            statement.strip().rstrip(";").upper() == "BEGIN IMMEDIATE"
            for statement in statements
        )
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_materializer_binds_prepared_staged_revision_to_selected_frame(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        selected = stage_classic(bootstrap, _blocked_classic_success())
        stored_selected = bootstrap._journal.load_frame(selected.frame_id)
        assert stored_selected is not None
        selected_revision = stored_selected.staged_revision
        stage_classic(
            bootstrap,
            {
                "next_batch": "s2",
                "rooms": {"join": {"!other:example.org": {}}},
            },
        )
        real_prepare = client._prepare_ingestion_frame

        def forge_revision(*args: object, **kwargs: object):
            prepared = real_prepare(*args, **kwargs)
            return prepared._replace(staged_revision=selected_revision + 1)

        client._prepare_ingestion_frame = forge_revision
        with pytest.raises(JournalIntegrityError, match="staged revision"):
            session._materialize_oldest_frame(limits=MaterializerLimits())
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_reopen_rejects_aggregate_snapshot_for_foreign_owner(
    tmp_path,
) -> None:
    from nio.store._sync_journal_preflight import _canonical_internal, _row

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        stage_classic(
            bootstrap,
            {"next_batch": "s1", "rooms": {"join": {ROOM: {}}}},
        )
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        owner = session._journal.load_owner()
        aggregate_row = connection.execute(
            "SELECT updated_revision, intent_kind, payload "
            "FROM NioIngestRoomAggregate WHERE account_id = ? AND room_id = ?",
            (ACCOUNT, ROOM),
        ).fetchone()
        assert aggregate_row is not None
        value = json.loads(aggregate_row[2])["value"]
        value["room_snapshot"]["own_user_id"] = BOB
        plaintext = canonical_json(value)
        payload, digest = _row(
            (ACCOUNT, owner.stream_id, owner.transport_kind),
            "NioIngestRoomAggregate",
            plaintext,
            header=_canonical_internal([ROOM, aggregate_row[0], aggregate_row[1]]),
        )
        connection.execute(
            "UPDATE NioIngestRoomAggregate SET payload = ?, payload_sha256 = ? "
            "WHERE account_id = ? AND room_id = ?",
            (payload, digest, ACCOUNT, ROOM),
        )
    finally:
        await session.close()

    with pytest.raises(JournalIntegrityError, match="snapshot"):
        reopen_owned_bootstrap(tmp_path, generation)


@pytest.mark.parametrize(
    ("fixture_name", "fixture_account", "pickle_query"),
    (
        (
            "example_DEVICEID.libolm_account_pickle_v4.db",
            "example",
            "SELECT account FROM accounts",
        ),
        (
            "example_DEVICEID.libolm_session_pickle_v1.db",
            "example",
            "SELECT session FROM olmsessions",
        ),
        (
            "@example:localhost_DEVICEID.libolm_inbound_group_session_pickle_v1.db",
            "@example:localhost",
            "SELECT session FROM megolminboundsessions",
        ),
    ),
)
def test_owned_attach_late_failure_preserves_legacy_pickle_bytes(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    fixture_name: str,
    fixture_account: str,
    pickle_query: str,
) -> None:
    import nio.store.sync_journal as bootstrap_api

    generation = uuid4()
    pickle_key = "DEFAULT_KEY"
    database_name = "legacy-owned.db"
    fixture = Path(__file__).parents[1] / "data/encryption" / fixture_name
    with sqlite3.connect(fixture) as connection:
        account_pickle = connection.execute("SELECT account FROM accounts").fetchone()[
            0
        ]
        legacy_pickle = connection.execute(pickle_query).fetchone()[0]

    seed = SqliteStore(
        fixture_account,
        "DEVICEID",
        str(tmp_path),
        pickle_key=pickle_key,
        database_name=database_name,
    )
    seed.save_account(OlmAccount())
    seed.database.execute_sql("UPDATE accounts SET account = ?", (account_pickle,))
    with sqlite3.connect(fixture) as source:
        if "olmsessions" in pickle_query:
            row = source.execute(
                "SELECT session_id, creation_time, last_usage_date, sender_key, session "
                "FROM olmsessions"
            ).fetchone()
            seed.database.execute_sql(
                "INSERT INTO olmsessions(session_id, creation_time, last_usage_date, "
                "sender_key, account_id, session) VALUES (?, ?, ?, ?, 1, ?)",
                tuple(row),
            )
        elif "megolminboundsessions" in pickle_query:
            columns = "session_id, fp_key, sender_key, room_id, session, account_id"
            row = source.execute(
                f"SELECT {columns} FROM megolminboundsessions"
            ).fetchone()
            seed.database.execute_sql(
                f"INSERT INTO megolminboundsessions({columns}) VALUES (?, ?, ?, ?, ?, 1)",
                tuple(row[:5]),
            )
    seed.database.close()
    bootstrap = bootstrap_api._open_configured_ingestion_store(
        tmp_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        account_id=fixture_account,
        device_id="DEVICEID",
        consumer_generation=generation,
        source=classic_config().source,
        pickle_key=pickle_key,
        database_name=database_name,
    )
    client = RecordingClient(
        fixture_account,
        "DEVICEID",
        config=AsyncClientConfig(
            store=SqliteStore,
            pickle_key=pickle_key,
            store_name=database_name,
        ),
    )
    client.store_path = str(tmp_path)

    def late_failure(_store):
        raise RuntimeError("late owned attach failure")

    monkeypatch.setattr(SqliteStore, "load_encrypted_rooms", late_failure)
    with pytest.raises(RuntimeError, match="late owned attach failure"):
        open_owned_session(client, bootstrap, generation)

    with sqlite3.connect(tmp_path / database_name) as connection:
        persisted = connection.execute(pickle_query).fetchone()[0]
    assert persisted == legacy_pickle
    assert client.store is None and client.olm is None

    reopened = bootstrap_api._open_configured_ingestion_store(
        tmp_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        account_id=fixture_account,
        device_id="DEVICEID",
        consumer_generation=generation,
        source=classic_config().source,
        pickle_key=pickle_key,
        database_name=database_name,
    )
    reopened.close()


@pytest.mark.parametrize("config", [classic_config(), sliding_config()])
@pytest.mark.asyncio
async def test_fresh_and_marked_owned_paths_load_one_account_without_store_or_io_fallback(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    config: IngestionConfig,
) -> None:
    import nio.store.sync_journal as bootstrap_api

    generation = uuid4()
    database_name = "fresh-owned.db"
    pickle_key = "fresh-owned-secret"
    real_init = OlmAccount.__init__
    real_pickle = OlmAccount.pickle
    native_generations = 0
    pickle_calls = 0

    def counting_init(self, account=None):
        nonlocal native_generations
        if account is None:
            native_generations += 1
        real_init(self, account)

    def counting_pickle(self, key: str = "") -> bytes:
        nonlocal pickle_calls
        pickle_calls += 1
        return real_pickle(self, key)

    def forbidden(*args, **kwargs):
        raise AssertionError("owned path used a legacy store/network fallback")

    monkeypatch.setattr(OlmAccount, "__init__", counting_init)
    monkeypatch.setattr(OlmAccount, "pickle", counting_pickle)
    monkeypatch.setattr(SqliteStore, "save_account", forbidden)
    monkeypatch.setattr(SqliteStore, "_create_database", forbidden)
    bootstrap = bootstrap_api._open_fresh_ingestion_store(
        tmp_path,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=config.source,
        pickle_key=pickle_key,
        database_name=database_name,
    )

    identities: list[dict[str, str]] = []
    for candidate_bootstrap in (bootstrap, None):
        if candidate_bootstrap is None:
            candidate_bootstrap = bootstrap_api._open_configured_ingestion_store(
                tmp_path,
                source_store_class=SqliteStore,
                owned_store_class=SqliteStore,
                account_id=ACCOUNT,
                device_id=DEVICE,
                consumer_generation=generation,
                source=config.source,
                pickle_key=pickle_key,
                database_name=database_name,
            )
        client = owned_client(
            tmp_path,
            pickle_key=pickle_key,
            database_name=database_name,
        )
        monkeypatch.setattr(client, "load_store", forbidden)
        monkeypatch.setattr(client, "sync", forbidden)
        monkeypatch.setattr(client, "keys_upload", forbidden)
        session = open_owned_session(client, candidate_bootstrap, generation, config)
        assert client.store is not None and client.olm is not None
        identities.append(dict(client.olm.account.identity_keys))
        assert client.send_count == 0
        await session.close()

    assert identities == [identities[0], identities[0]]
    assert native_generations == 1
    assert pickle_calls == 1


@pytest.mark.parametrize("require_persisted", [False, True])
def test_olm_public_and_owned_missing_account_contracts(
    monkeypatch: pytest.MonkeyPatch,
    require_persisted: bool,
) -> None:
    from nio.crypto import Olm
    from nio.crypto import olm_machine
    from nio.store import SqliteMemoryStore

    assert tuple(inspect.signature(Olm).parameters) == (
        "user_id",
        "device_id",
        "store",
        "replace_rotated_device_keys",
    )
    store = SqliteMemoryStore(ACCOUNT, DEVICE)
    real_save = store.save_account
    loads = 0
    saves = 0

    def missing_account():
        nonlocal loads
        loads += 1
        return None

    def counting_save(account):
        nonlocal saves
        saves += 1
        return real_save(account)

    monkeypatch.setattr(store, "load_account", missing_account)
    monkeypatch.setattr(store, "save_account", counting_save)
    if require_persisted:
        monkeypatch.setattr(store, "_load_persisted_account", missing_account)
        monkeypatch.setattr(
            olm_machine,
            "OlmAccount",
            lambda: pytest.fail("owned constructor generated an account"),
        )
        with pytest.raises(LocalProtocolError, match="committed Olm account"):
            Olm._from_persisted_account(ACCOUNT, DEVICE, store)
        assert saves == 0
    else:
        assert Olm(ACCOUNT, DEVICE, store).account.identity_keys
        assert saves == 1
    assert loads == 1


@pytest.mark.parametrize(
    "invalid",
    (
        "pre_store",
        "pre_olm",
        "wrong_store_class",
        "wrong_pickle_key",
        "wrong_path",
        "encryption_disabled",
        "dirty_sync",
        "client_session",
        "wrong_identity",
        "missing_auth",
        "custom_authorization",
        "wrong_source",
        "wrong_timeout",
        "wrong_generation",
        "wrong_stream",
    ),
)
@pytest.mark.asyncio
async def test_owned_factory_rejects_invalid_client_without_consuming_bootstrap(
    tmp_path, invalid: str
) -> None:
    from nio.ingest import coordinator

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    supplied_config = classic_config()
    supplied_generation = generation
    supplied_stream = bootstrap.stream_id
    if invalid == "pre_store":
        client.store = object()  # type: ignore[assignment]
    elif invalid == "pre_olm":
        client.olm = object()  # type: ignore[assignment]
    elif invalid == "wrong_store_class":
        client.config = replace(client.config, store=DefaultStore)
    elif invalid == "wrong_pickle_key":
        client.config = replace(client.config, pickle_key="wrong")
    elif invalid == "wrong_path":
        client.store_path = str(tmp_path / "foreign")
    elif invalid == "encryption_disabled":
        client.config = replace(client.config, encryption_enabled=False)
    elif invalid == "dirty_sync":
        client.next_batch = "already-synced"
    elif invalid == "client_session":
        client.client_session = object()  # type: ignore[assignment]
    elif invalid == "wrong_identity":
        client.user_id = "@mallory:example.org"
    elif invalid == "missing_auth":
        client.access_token = ""
    elif invalid == "custom_authorization":
        client.config = replace(
            client.config,
            custom_headers={"aUtHoRiZaTiOn": "Bearer attacker"},
        )
    elif invalid == "wrong_source":
        supplied_config = IngestionConfig(ClassicSourceConfig(30_000, b'{"x":1}'))
    elif invalid == "wrong_timeout":
        supplied_config = IngestionConfig(
            classic_config().source,
            sqlite_busy_timeout_ms=1_999,
        )
    elif invalid == "wrong_generation":
        supplied_generation = uuid4()
    else:
        supplied_stream = uuid4()

    with pytest.raises(LocalProtocolError):
        coordinator._open_owned_ingestion(
            client,
            bootstrap,
            config=supplied_config,
            consumer_generation=supplied_generation,
            stream_id=supplied_stream,
        )

    replacement = owned_client(tmp_path)
    session = open_owned_session(replacement, bootstrap, generation)
    await session.close()


@pytest.mark.parametrize(
    "fault",
    (
        "account_missing",
        "load_account",
        "load_sessions",
        "load_inbound_group_sessions",
        "load_device_keys",
        "load_outgoing_key_requests",
        "load_encrypted_rooms",
        "load_sync_token",
        "session_construction",
    ),
)
@pytest.mark.asyncio
async def test_owned_factory_post_creation_failure_restores_tombstones_and_reopens(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    fault: str,
) -> None:
    from nio.ingest import coordinator

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    client.config = replace(
        client.config,
        store_sync_tokens=True,
    )
    previous = (
        client.store,
        client.olm,
        client.rooms,
        client.invited_rooms,
        client.encrypted_rooms,
        client.next_batch,
        client.loaded_sync_token,
        client._ingestion_store_snapshot,
    )
    revoked: list[SqliteStore] = []
    real_revoke = SqliteStore._revoke_ingestion_lease

    def record_revoke(store: SqliteStore) -> None:
        revoked.append(store)
        real_revoke(store)

    def fail(*args, **kwargs):
        raise RuntimeError(f"injected {fault}")

    def forbid_save(*args, **kwargs):
        raise AssertionError("owned attach saved an Olm account")

    with monkeypatch.context() as patch:
        patch.setattr(SqliteStore, "_revoke_ingestion_lease", record_revoke)
        patch.setattr(SqliteStore, "save_account", forbid_save)
        if fault == "account_missing":
            patch.setattr(
                SqliteStore,
                "_load_persisted_account",
                lambda _store: None,
            )
        elif fault == "load_account":
            patch.setattr(SqliteStore, "_load_persisted_account", fail)
        elif fault == "load_sessions":
            patch.setattr(SqliteStore, "_load_persisted_sessions", fail)
        elif fault == "load_inbound_group_sessions":
            patch.setattr(
                SqliteStore,
                "_load_persisted_inbound_group_sessions",
                fail,
            )
        elif fault == "session_construction":
            patch.setattr(coordinator, "_OwnedIngestionSession", fail)
        else:
            patch.setattr(SqliteStore, fault, fail)
        expected = LocalProtocolError if fault == "account_missing" else RuntimeError
        with pytest.raises(expected):
            open_owned_session(client, bootstrap, generation)

    restored = (
        client.store,
        client.olm,
        client.rooms,
        client.invited_rooms,
        client.encrypted_rooms,
        client.next_batch,
        client.loaded_sync_token,
        client._ingestion_store_snapshot,
    )
    assert all(current is original for current, original in zip(restored, previous))
    assert len(revoked) == 1
    with pytest.raises(LocalProtocolError, match="revoked"):
        revoked[0].load_account()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    reopened.close()


@pytest.mark.asyncio
async def test_unbound_bootstrap_rejects_owned_factory_without_consumption(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    bootstrap = StoreBootstrap(bootstrap._journal)
    client = owned_client(
        tmp_path,
        pickle_key="",
        database_name=f"{ACCOUNT}_{DEVICE}.db",
    )
    with pytest.raises(LocalProtocolError, match="bound bootstrap"):
        open_owned_session(client, bootstrap, generation)
    bootstrap.close()


def test_owned_candidate_rejects_foreign_store_and_tombstones(tmp_path) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    candidate = bootstrap._open_owned_store_candidate()
    store = candidate._store_for_attachment()

    with pytest.raises(LocalProtocolError, match="foreign"):
        candidate._prepare_transfer(object())  # type: ignore[arg-type]

    assert candidate._store_for_attachment() is store
    candidate._tombstone()
    with pytest.raises(LocalProtocolError, match="revoked"):
        store.load_account()


@pytest.mark.asyncio
async def test_owned_close_is_sole_token_detaches_revokes_and_leaves_http(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    http_owner = object()
    session = open_owned_session(client, bootstrap, generation)
    client.client_session = http_owner  # type: ignore[assignment]
    saved_store = client.store
    saved_olm = client.olm
    assert saved_store is not None and saved_olm is not None

    with pytest.raises(LocalProtocolError, match="close token"):
        bootstrap.close()
    first, second = asyncio.create_task(session.close()), asyncio.create_task(
        session.close()
    )
    await asyncio.gather(first, second)

    assert client.store is None
    assert client.olm is None
    assert client.client_session is http_owner
    assert saved_olm.store is saved_store
    with pytest.raises(LocalProtocolError, match="revoked"):
        saved_store.load_account()
    with pytest.raises(LocalProtocolError, match="already closed"):
        await session.close()


@pytest.mark.asyncio
async def test_owned_close_physical_failure_is_retryable_with_exclusion(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    import nio.store.sync_journal as bootstrap_api

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    owner = bootstrap._journal._owner
    real_close = owner.database.close
    failures = 0

    def fail_once() -> bool:
        nonlocal failures
        failures += 1
        if failures == 1:
            raise RuntimeError("physical close failed")
        return real_close()

    monkeypatch.setattr(owner.database, "close", fail_once)
    with pytest.raises(RuntimeError, match="physical close failed"):
        await session.close()
    assert client.store is None and client.olm is None
    with pytest.raises(LocalProtocolError, match="already held"):
        bootstrap_api._open_configured_ingestion_store(
            tmp_path,
            source_store_class=SqliteStore,
            owned_store_class=SqliteStore,
            account_id=ACCOUNT,
            device_id=DEVICE,
            consumer_generation=generation,
            source=classic_config().source,
            pickle_key="owned-secret",
            database_name="owned.db",
        )

    await session.close()
    reopened = reopen_owned_bootstrap(tmp_path, generation)
    reopened.close()


@pytest.mark.asyncio
async def test_owned_close_lock_release_failure_is_retryable_with_exclusion(
    tmp_path, monkeypatch: pytest.MonkeyPatch
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    owner = bootstrap._journal._owner
    real_close = owner._lock.close
    failures = 0

    def fail_once() -> None:
        nonlocal failures
        failures += 1
        if failures == 1:
            raise RuntimeError("lock release failed")
        real_close()

    monkeypatch.setattr(owner._lock, "close", fail_once)
    with pytest.raises(RuntimeError, match="lock release failed"):
        await session.close()
    with pytest.raises(LocalProtocolError, match="already held"):
        reopen_owned_bootstrap(tmp_path, generation)

    await session.close()
    reopened = reopen_owned_bootstrap(tmp_path, generation)
    reopened.close()


@pytest.mark.asyncio
async def test_owned_close_wrong_thread_is_preflight_only_and_main_thread_recovers(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    saved_store = client.store
    saved_olm = client.olm
    errors: list[BaseException] = []

    def close_from_foreign_thread() -> None:
        try:
            asyncio.run(session.close())
        except BaseException as error:  # noqa: BLE001 - exact error asserted below
            errors.append(error)

    thread = threading.Thread(target=close_from_foreign_thread)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(errors) == 1 and isinstance(errors[0], LocalProtocolError)
    assert client.store is saved_store and client.olm is saved_olm
    assert saved_store is not None and saved_store.load_account() is not None

    await session.close()
    assert client.store is None and client.olm is None


@pytest.mark.asyncio
async def test_owned_close_caller_cancellation_drains_shared_runner_cleanup(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path, client_type=SlowCancellationClient)
    session = open_owned_session(client, bootstrap, generation)
    runner = asyncio.create_task(session.run())
    await client.entered.wait()
    first = asyncio.create_task(session.close())
    await client.cancelling.wait()
    second = asyncio.create_task(session.close())
    first.cancel()
    await asyncio.sleep(0)
    assert not first.done() and not second.done()
    client.release.set()
    with pytest.raises(asyncio.CancelledError):
        await first
    await second

    assert runner.cancelled()
    assert client.store is None and client.olm is None
    reopened = reopen_owned_bootstrap(tmp_path, generation)
    reopened.close()


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
        open_test_session(
            client,
            bootstrap,
            config=classic_config(),
            consumer_generation=generation,
            stream_id=bootstrap.stream_id,
        )

    assert client.send_count == 0
    safe_client = RecordingClient()
    session = open_test_session(
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
    session = open_test_session(
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
        assert any(row[0] == "held" for row in rows)

    client.on_send = assert_owned_before_get
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    admitted = []
    with pytest.raises(nio.IngestionSourceError):
        await asyncio.wait_for(run_with_acknowledgements(session, admitted), timeout=3)

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
    assert any(getattr(record, "event_id", None) == "$message" for record in admitted)
    assert session.next_batch() is None
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
    session = open_test_session(
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
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await asyncio.wait_for(run_with_acknowledgements(session), timeout=3)
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

    async def callback(*args: object) -> None:
        raise AssertionError("hydration must not invoke callbacks")

    client.add_response_callback(callback)
    client.add_event_callback(callback, None)
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    client.client_session = transport  # type: ignore[assignment]
    with pytest.raises(nio.IngestionSourceError):
        await asyncio.wait_for(run_with_acknowledgements(session), timeout=3)
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
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await asyncio.wait_for(run_with_acknowledgements(session), timeout=3)
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionHydrationError):
        await asyncio.wait_for(run_with_acknowledgements(session), timeout=3)
    await session.close()


@pytest.mark.asyncio
async def test_pending_hydrations_for_multiple_rooms_are_processed_in_sequence(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    stage_pending(bootstrap)
    other = "!other:example.org"
    stage_classic(
        bootstrap,
        {"next_batch": "s2", "rooms": {"join": {other: {}}}},
    )
    client = RecordingClient()
    client.responses.extend(
        [
            FakeResponse(200, hydration_state()),
            FakeResponse(200, hydration_state()),
            FakeResponse(401, b"{}"),
        ]
    )
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await asyncio.wait_for(run_with_acknowledgements(session), timeout=3)
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
    session = open_test_session(
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
    session = open_test_session(
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
            await run_task
            pytest.fail("source runner stopped before its second request")
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
        assert transitions.index("frame_insert") < transitions.index(
            "frame_prepared_retain"
        )
        assert transitions.count("commit") == 4
        assert client.send_count == 2
        assert client.send_calls[0] == (
            (
                "GET",
                "/_matrix/client/v3/sync?full_state=true&timeout=30000&filter=%7B%7D",
                None,
                {"Authorization": "Bearer secret-token"},
                None,
                45.0,
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
                45.0,
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
async def test_owned_quiesce_commits_one_final_source_response_for_restart_recovery(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    event_source = {
        "content": {"value": "shutdown-barrier"},
        "type": "org.example.shutdown-barrier",
    }
    response = FakeResponse(
        200,
        canonical_json(
            {
                "account_data": {"events": [event_source]},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {},
                "device_unused_fallback_key_types": [],
                "next_batch": "shutdown-barrier",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": []},
            }
        ),
    )
    client = owned_client(tmp_path, client_type=lambda: QuiesceBarrierClient(response))
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    source_before = session._journal.load_source()
    runner = asyncio.create_task(session.run())
    quiesce: asyncio.Task[None] | None = None

    try:
        await asyncio.wait_for(client.first_request_entered.wait(), timeout=2)
        quiesce = asyncio.create_task(session._quiesce())
        client.release_first_request.set()
        await asyncio.wait_for(quiesce, timeout=2)
        await asyncio.wait_for(runner, timeout=2)
        await session._quiesce()

        source_after = session._journal.load_source()
        assert source_after.next_request_id == source_before.next_request_id + 1
        assert len(session._journal.list_frames(1)) == 1
        assert session.next_batch(max_records=1) is None
        assert client.send_count == 1
    finally:
        client.release_first_request.set()
        for task in (quiesce, runner):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quiesce, runner) if task is not None),
            return_exceptions=True,
        )
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_session = open_owned_session(restored_client, reopened, generation)
    assert restored_client.olm is not None
    restored_client.olm.account.shared = True
    restored_client.olm.uploaded_key_count = (
        restored_client.olm.account.max_one_time_keys
    )
    restored_client.olm.save_account()
    try:
        assert (
            restored_session._materialize_oldest_frame(
                limits=MaterializerLimits()
            ).status
            is MaterializeStatus.MATERIALIZED
        )
        batch = restored_session.next_batch(max_records=1)
        assert batch is not None and len(batch.records) == 1
        record = batch.records[0]
        assert type(record) is EventRecord
        assert record.kind is RecordKind.GLOBAL_ACCOUNT_DATA
        assert record.source_json == canonical_json(event_source)
        await restored_session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        assert restored_session.next_batch(max_records=1) is None
    finally:
        await restored_session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_prioritizes_final_source_commit_over_materialization_backlog(
    tmp_path,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    client.responses.append(FakeResponse(200, b'{"next_batch":"quiesced","rooms":{}}'))
    session = open_owned_session(client, bootstrap, generation)
    remaining = 500
    materialized_before_source: list[int] = []

    def materialize_synchronously(*, limits: MaterializerLimits) -> MaterializeResult:
        nonlocal remaining
        assert limits == MaterializerLimits()
        if remaining:
            remaining -= 1
            return MaterializeResult(
                MaterializeStatus.MATERIALIZED,
                uuid4(),
                500 - remaining,
            )
        return MaterializeResult(MaterializeStatus.IDLE, None, None)

    session._materialize_oldest_frame = materialize_synchronously  # type: ignore[method-assign]
    client.on_send = lambda: materialized_before_source.append(500 - remaining)
    runner = asyncio.create_task(session.run())
    while session._running is None:
        await asyncio.sleep(0)
    quiesce = asyncio.create_task(session._quiesce())

    try:
        await asyncio.wait_for(quiesce, timeout=2)
        await asyncio.wait_for(runner, timeout=2)
        assert len(materialized_before_source) == 1
        assert materialized_before_source[0] <= 1
        assert remaining >= 499
        assert len(session._journal.list_frames(1)) == 1
    finally:
        for task in (quiesce, runner):
            if not task.done():
                task.cancel()
        await asyncio.gather(quiesce, runner, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_reserves_one_final_frame_at_full_capacity(
    tmp_path,
) -> None:
    generation = uuid4()
    config = IngestionConfig(
        classic_config().source,
        max_staged_frames=2,
    )
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    client.responses.append(FakeResponse(200, b'{"next_batch":"quiesced","rooms":{}}'))
    session = open_owned_session(
        client,
        bootstrap,
        generation,
        config=config,
    )
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    first = stage_classic(
        bootstrap,
        {
            "account_data": {
                "events": [
                    {
                        "content": {"value": "blocks-oldest-frame"},
                        "type": "org.example.quiesce-capacity",
                    }
                ]
            },
            "next_batch": "s1",
            "rooms": {},
        },
    )
    materialize_calls = 0
    materialize = session._materialize_oldest_frame

    def count_materialization(*, limits: MaterializerLimits) -> MaterializeResult:
        nonlocal materialize_calls
        materialize_calls += 1
        return materialize(limits=limits)

    session._materialize_oldest_frame = count_materialization  # type: ignore[method-assign]
    runner = asyncio.create_task(session.run())
    quiesce: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(2):
            while materialize_calls < 2:
                await asyncio.sleep(0)
        assert session._journal._oldest_prepared_frame_has_work(first.frame_id)
        stage_classic(bootstrap, {"next_batch": "s2", "rooms": {}})
        stage_classic(bootstrap, {"next_batch": "s3", "rooms": {}})
        calls_before_quiesce = materialize_calls
        assert len(session._journal.list_frames(3)) == 2

        quiesce = asyncio.create_task(session._quiesce())
        await asyncio.wait_for(quiesce, timeout=2)
        await asyncio.wait_for(runner, timeout=2)
        await session._quiesce()

        assert materialize_calls == calls_before_quiesce
        assert len(session._journal.list_frames(4)) == 3
        assert client.send_count == 1
        await asyncio.wait_for(session.run(), timeout=2)
        assert materialize_calls == calls_before_quiesce
        assert client.send_count == 1
    finally:
        for task in (quiesce, runner):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quiesce, runner) if task is not None),
            return_exceptions=True,
        )
        await session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_allows_prepared_plus_maximum_staged_frames(
    tmp_path,
) -> None:
    generation = uuid4()
    config = IngestionConfig(
        classic_config().source,
        max_staged_frames=256,
    )
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    client.responses.append(FakeResponse(200, b'{"next_batch":"quiesced","rooms":{}}'))
    session = open_owned_session(
        client,
        bootstrap,
        generation,
        config=config,
    )
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    first = stage_classic(
        bootstrap,
        {
            "account_data": {
                "events": [
                    {
                        "content": {"value": "blocks-oldest-frame"},
                        "type": "org.example.quiesce-capacity",
                    }
                ]
            },
            "next_batch": "s1",
            "rooms": {},
        },
    )
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    assert session._journal._oldest_prepared_frame_has_work(first.frame_id)
    for sequence in range(2, 258):
        stage_classic(
            bootstrap,
            {"next_batch": f"s{sequence}", "rooms": {}},
        )
    assert len(session._journal.list_frames(257)) == 256

    runner = asyncio.create_task(session.run())
    quiesce: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(2):
            while session._running is None:
                await asyncio.sleep(0)
        quiesce = asyncio.create_task(session._quiesce())
        await asyncio.wait_for(quiesce, timeout=5)
        await asyncio.wait_for(runner, timeout=5)

        headers = session._journal._load_authenticated_frame_headers(
            session._journal.load_owner()
        )
        assert len(headers) == 258
        assert sum(row.room_materialized_revision is None for row in headers) == 257
        assert sum(row.room_materialized_revision is not None for row in headers) == 1
        assert client.send_count == 1
    finally:
        for task in (quiesce, runner):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quiesce, runner) if task is not None),
            return_exceptions=True,
        )
        await session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_recovers_persisted_reserved_frame_after_reopen(
    tmp_path,
) -> None:
    generation = uuid4()
    config = IngestionConfig(
        classic_config().source,
        max_staged_frames=2,
    )
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    seed_client = owned_client(tmp_path)
    seed_session = open_owned_session(
        seed_client,
        bootstrap,
        generation,
        config=config,
    )
    assert seed_client.olm is not None
    seed_client.olm.account.shared = True
    seed_client.olm.uploaded_key_count = seed_client.olm.account.max_one_time_keys
    seed_client.olm.save_account()
    first = stage_classic(
        bootstrap,
        {
            "account_data": {
                "events": [
                    {
                        "content": {"value": "blocks-oldest-frame"},
                        "type": "org.example.quiesce-capacity",
                    }
                ]
            },
            "next_batch": "s1",
            "rooms": {},
        },
    )
    assert (
        seed_session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    assert seed_session._journal._oldest_prepared_frame_has_work(first.frame_id)
    for sequence in range(2, 4):
        stage_classic(
            bootstrap,
            {"next_batch": f"s{sequence}", "rooms": {}},
        )
    stage_classic(
        bootstrap,
        {"next_batch": "s4", "rooms": {}},
        quiesce_reserved=True,
    )
    assert seed_session._journal.has_reserved_quiesce_response()
    await seed_session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(
        client,
        reopened,
        generation,
        config=config,
    )
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    runner = asyncio.create_task(session.run())
    quiesce: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(2):
            while session._running is None:
                await asyncio.sleep(0)
        quiesce = asyncio.create_task(session._quiesce())
        await asyncio.wait_for(quiesce, timeout=2)
        await asyncio.wait_for(runner, timeout=2)

        assert len(session._journal.list_frames(3)) == 3
        assert not session._journal.has_reserved_quiesce_response()
        assert client.send_count == 0
    finally:
        for task in (quiesce, runner):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quiesce, runner) if task is not None),
            return_exceptions=True,
        )
        await session.close()


@pytest.mark.asyncio
async def test_completed_quiesce_consumes_durable_reservation(tmp_path) -> None:
    generation = uuid4()
    config = IngestionConfig(
        classic_config().source,
        max_staged_frames=2,
    )
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    client.responses.append(FakeResponse(200, b'{"next_batch":"quiesced","rooms":{}}'))
    session = open_owned_session(
        client,
        bootstrap,
        generation,
        config=config,
    )
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    first = stage_classic(
        bootstrap,
        {
            "account_data": {
                "events": [
                    {
                        "content": {"value": "blocks-oldest-frame"},
                        "type": "org.example.quiesce-capacity",
                    }
                ]
            },
            "next_batch": "s1",
            "rooms": {},
        },
    )
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    assert session._journal._oldest_prepared_frame_has_work(first.frame_id)
    for sequence in range(2, 4):
        stage_classic(
            bootstrap,
            {"next_batch": f"s{sequence}", "rooms": {}},
        )

    runner = asyncio.create_task(session.run())
    quiesce: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(2):
            while session._running is None:
                await asyncio.sleep(0)
        quiesce = asyncio.create_task(session._quiesce())
        await asyncio.wait_for(quiesce, timeout=2)
        await asyncio.wait_for(runner, timeout=2)

        assert len(session._journal.list_frames(3)) == 3
        assert not session._journal.has_reserved_quiesce_response()
        assert client.send_count == 1
    finally:
        for task in (quiesce, runner):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quiesce, runner) if task is not None),
            return_exceptions=True,
        )
        await session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_does_not_infer_reservation_after_limit_reduction(
    tmp_path,
) -> None:
    generation = uuid4()
    seed_config = IngestionConfig(
        classic_config().source,
        max_staged_frames=4,
    )
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    seed_client = owned_client(tmp_path)
    seed_session = open_owned_session(
        seed_client,
        bootstrap,
        generation,
        config=seed_config,
    )
    assert seed_client.olm is not None
    seed_client.olm.account.shared = True
    seed_client.olm.uploaded_key_count = seed_client.olm.account.max_one_time_keys
    seed_client.olm.save_account()
    first = stage_classic(
        bootstrap,
        {
            "account_data": {
                "events": [
                    {
                        "content": {"value": "blocks-oldest-frame"},
                        "type": "org.example.quiesce-capacity",
                    }
                ]
            },
            "next_batch": "s1",
            "rooms": {},
        },
    )
    assert (
        seed_session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    assert seed_session._journal._oldest_prepared_frame_has_work(first.frame_id)
    for sequence in range(2, 5):
        stage_classic(
            bootstrap,
            {"next_batch": f"s{sequence}", "rooms": {}},
        )
    await seed_session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(
        client,
        reopened,
        generation,
        config=IngestionConfig(
            classic_config().source,
            max_staged_frames=2,
        ),
    )
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    runner = asyncio.create_task(session.run())
    quiesce: asyncio.Task[None] | None = None
    try:
        async with asyncio.timeout(2):
            while session._running is None:
                await asyncio.sleep(0)
        quiesce = asyncio.create_task(session._quiesce())
        with pytest.raises(nio.IngestionBlockedError):
            await asyncio.wait_for(quiesce, timeout=2)

        assert len(session._journal.list_frames(3)) == 3
        assert client.send_count == 0
        assert not runner.done()
    finally:
        for task in (quiesce, runner):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quiesce, runner) if task is not None),
            return_exceptions=True,
        )
        await session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_allows_idle_poll_response_delivery_margin(
    tmp_path,
) -> None:
    generation = uuid4()
    source = ClassicSourceConfig(10, b"{}")
    config = IngestionConfig(source)
    bootstrap, _account = open_owned_bootstrap(
        tmp_path,
        generation,
        source=source,
    )
    response = FakeResponse(200, b'{"next_batch":"idle","rooms":{}}')
    client = owned_client(
        tmp_path,
        client_type=lambda: TimedIdleQuiesceClient(
            response,
            server_wait_seconds=0.01,
        ),
    )
    session = open_owned_session(
        client,
        bootstrap,
        generation,
        config=config,
    )
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    runner = asyncio.create_task(session.run())
    quiesce: asyncio.Task[None] | None = None

    try:
        await asyncio.wait_for(client.first_request_entered.wait(), timeout=1)
        quiesce = asyncio.create_task(session._quiesce())
        await asyncio.wait_for(quiesce, timeout=1)
        await asyncio.wait_for(runner, timeout=1)

        assert not client.deadline_expired.is_set()
        assert client.send_count == 1
        assert client.send_calls[0][0][5] == 15.01
        assert len(session._journal.list_frames(1)) == 1
    finally:
        for task in (quiesce, runner):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quiesce, runner) if task is not None),
            return_exceptions=True,
        )
        await session.close()


@pytest.mark.asyncio
async def test_zero_source_timeout_preserves_unbounded_transport_deadline(
    tmp_path,
) -> None:
    generation = uuid4()
    source = ClassicSourceConfig(0, b"{}")
    bootstrap, _account = open_owned_bootstrap(
        tmp_path,
        generation,
        source=source,
    )
    client = owned_client(tmp_path)
    client.responses.append(FakeResponse(401, b"{}"))
    session = open_owned_session(
        client,
        bootstrap,
        generation,
        config=IngestionConfig(source),
    )

    try:
        with pytest.raises(nio.IngestionSourceError):
            await session.run()
        assert client.send_calls[0][0][5] == 0
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_rejects_active_source_without_a_runner(tmp_path) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)

    try:
        with pytest.raises(LocalProtocolError, match="source is not running"):
            await session._quiesce()
        assert session._quiesce_source_commit_target is None
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_surfaces_the_runner_terminal_error(tmp_path) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    client.responses.append(FakeResponse(200, b"{}"))
    session = open_owned_session(client, bootstrap, generation)

    try:
        with pytest.raises(nio.IngestionSourceError) as runner_failure:
            await session.run()
        with pytest.raises(nio.IngestionSourceError) as quiesce_failure:
            await session._quiesce()
        assert quiesce_failure.value is runner_failure.value
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_quiesce_requires_a_frame_commit_after_sliding_reset(
    tmp_path,
) -> None:
    generation = uuid4()
    config = sliding_config()
    bootstrap, _account = open_owned_bootstrap(
        tmp_path,
        generation,
        source=config.source,
    )
    stage_sliding_position(bootstrap)
    assert (
        materialize_journal(bootstrap._journal, limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    retire_completed_frame(bootstrap._journal)
    event_source = {
        "content": {"value": "after-reset"},
        "type": "org.example.after-reset",
    }
    client = owned_client(
        tmp_path,
        client_type=lambda: QuiesceResetBarrierClient(event_source),
    )
    session = open_owned_session(client, bootstrap, generation, config=config)
    assert client.olm is not None
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    source_before = session._journal.load_source()
    runner = asyncio.create_task(session.run())
    quiesce: asyncio.Task[None] | None = None

    try:
        await asyncio.wait_for(client.first_request_entered.wait(), timeout=2)
        quiesce = asyncio.create_task(session._quiesce())
        client.release_first_request.set()
        await asyncio.wait_for(client.second_request_entered.wait(), timeout=2)
        await asyncio.wait_for(quiesce, timeout=2)
        await asyncio.wait_for(runner, timeout=2)

        source_after = session._journal.load_source()
        assert source_after.source_epoch == source_before.source_epoch + 1
        assert source_after.next_request_id == 1
        assert len(session._journal.list_frames(1)) == 1
        assert session.next_batch(max_records=1) is None
        assert client.send_count == 2
    finally:
        client.release_first_request.set()
        for task in (quiesce, runner):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (quiesce, runner) if task is not None),
            return_exceptions=True,
        )
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation, source=config.source)
    restored_client = owned_client(tmp_path)
    restored_session = open_owned_session(
        restored_client,
        reopened,
        generation,
        config=config,
    )
    assert restored_client.olm is not None
    restored_client.olm.account.shared = True
    restored_client.olm.uploaded_key_count = (
        restored_client.olm.account.max_one_time_keys
    )
    restored_client.olm.save_account()
    try:
        assert (
            restored_session._materialize_oldest_frame(
                limits=MaterializerLimits()
            ).status
            is MaterializeStatus.MATERIALIZED
        )
        batch = restored_session.next_batch(max_records=1)
        assert batch is not None and len(batch.records) == 1
        record = batch.records[0]
        assert type(record) is EventRecord
        assert record.kind is RecordKind.GLOBAL_ACCOUNT_DATA
        assert record.source_json == canonical_json(event_source)
        await restored_session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
        assert restored_session.next_batch(max_records=1) is None
    finally:
        await restored_session.close()


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

    async def callback(*args: object) -> None:
        raise AssertionError("ingestion must not invoke application callbacks")

    client.add_response_callback(callback)
    client.add_event_callback(callback, None)
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    client.client_session = transport  # type: ignore[assignment]
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
                "timeout": 45.0,
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
    with pytest.raises(LocalProtocolError, match="already closed"):
        await session.close()
    assert task.cancelled()

    from nio.store.sync_journal import _open_configured_ingestion_store

    reopened = _open_configured_ingestion_store(
        tmp_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=generation,
        source=classic_config().source,
    )
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
async def test_malformed_transport_state_is_a_sticky_source_error(tmp_path) -> None:
    generation = uuid4()
    bootstrap = open_bootstrap(tmp_path, generation)
    response = FakeResponse(200, b"{}")
    response.status = "200"  # type: ignore[assignment]
    client = RecordingClient()
    client.responses.append(response)
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
    session = open_test_session(
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
        materialize_journal(journal, limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    retire_completed_frame(journal)
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
    session = open_test_session(
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
        assert committed_owner.revision == owner_before.revision + 4
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
        assert transitions[:4] == [
            "sliding_reset_meta_cas",
            "sliding_reset_source_upsert",
            "before_commit",
            "commit",
        ]
        assert "frame_prepared_retain" in transitions
        assert transitions.count("commit") == 4

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
            materialize_journal(journal, limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        retire_completed_frame(journal)
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
    session = open_test_session(
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
        materialize_journal(journal, limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    retire_completed_frame(journal)
    session = open_test_session(
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
            materialize_journal(bootstrap._journal, limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        retire_completed_frame(bootstrap._journal)
        assert (
            _sliding_cursor_from_json(bootstrap._journal.load_source().cursor_json).pos
            is not None
        )
    before = _raw_ingestion_rows(bootstrap.database_path)
    client = RecordingClient()
    client.responses.append(FakeResponse(400, body))
    session = open_test_session(
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

    async def callback(*args: object) -> None:
        raise AssertionError("callback invoked")

    async def no_wait(delay: float) -> None:
        pass

    client.add_response_callback(callback)
    client.add_event_callback(callback, None)
    monkeypatch.setattr("nio.ingest.coordinator.asyncio.sleep", no_wait)
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    client.client_session = transport  # type: ignore[assignment]
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

    client = RecordingClient()
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    monkeypatch.setattr(session, "_materialize_oldest_frame", at_capacity)
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
    session = open_test_session(
        client,
        bootstrap,
        config=classic_config(),
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    with pytest.raises(nio.IngestionSourceError):
        await asyncio.wait_for(run_with_acknowledgements(session), timeout=3)
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
    session = open_test_session(
        client,
        bootstrap,
        config=config,
        consumer_generation=generation,
        stream_id=bootstrap.stream_id,
    )
    client.client_session = transport  # type: ignore[assignment]
    with pytest.raises(nio.IngestionSourceError):
        await session.run()
    args, kwargs = transport.calls[0]
    assert args == (
        planned.method,
        "https://example.org" + planned.path + "?timeout=30000",
    )
    assert kwargs["data"] == planned.body
    assert kwargs["timeout"] == 45.0
    client.client_session = None
    await session.close()


@pytest.mark.asyncio
async def test_owned_settle_global_account_data_callbacks_before_ack_outside_store(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    event_source = {
        "content": {"theme": "durable"},
        "type": "org.example.settlement.theme",
    }
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        staged = stage_classic(
            bootstrap,
            {
                "account_data": {"events": [event_source]},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "settled-token",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": []},
            },
        )
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        assert materialized.frame_id == staged.frame_id
        assert materialized.revision is not None
        batch = session.next_batch(max_records=1)
        assert batch is not None
        assert len(batch.records) == 1
        record = batch.records[0]
        assert type(record) is EventRecord
        assert record.kind.value == "global_account_data"

        authenticated_views: list[object] = []
        parsed_events: list[object] = []
        real_load_settlement = session._journal._load_batch_settlement
        real_parse_account_data = AccountDataEvent.parse_event

        def observe_authenticated_settlement(candidate):
            assert candidate == batch
            view = real_load_settlement(candidate)
            assert view is not None
            work, room = view
            assert work.value == record
            assert work.metadata is not None
            assert work.metadata.record_id == record.record_id
            assert work.metadata.preparation_phase.value == "source"
            assert work.metadata.effective_event_type == "org.example.settlement.theme"
            assert work.metadata.decryption.value == "none"
            assert work.metadata.decryption_verified is None
            assert work.metadata.decrypted_to_device_kind is None
            assert work.metadata.callback_route is not None
            assert work.metadata.callback_route.value == "global_account_data"
            assert room is None
            authenticated_views.append(view)
            return view

        def observe_account_data_parse(
            _event_type: type[AccountDataEvent],
            source: dict[object, object],
        ) -> object:
            assert authenticated_views
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            assert canonical_json(source) == record.source_json
            parsed = real_parse_account_data(source)
            assert parsed.source == event_source
            parsed_events.append(parsed)
            return parsed

        monkeypatch.setattr(
            session._journal,
            "_load_batch_settlement",
            observe_authenticated_settlement,
        )
        monkeypatch.setattr(
            AccountDataEvent,
            "parse_event",
            classmethod(observe_account_data_parse),
        )

        def delivery_state() -> tuple[object, ...]:
            row = connection.execute(
                "SELECT delivery_next_sequence, delivery_acknowledged_sha256, "
                "delivery_outstanding_work_id, "
                "delivery_outstanding_ready_revision, "
                "delivery_outstanding_ready_ordinal, "
                "delivery_outstanding_batch_sha256 FROM NioIngestMeta"
            ).fetchone()
            assert row is not None
            return tuple(row)

        outstanding = delivery_state()
        assert outstanding == (
            1,
            None,
            record.record_id,
            batch.created_revision,
            0,
            batch.ref.sha256,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )

        callback_events: list[object] = []
        forbidden_callbacks: list[str] = []

        async def on_global_account_data(event: object) -> None:
            assert authenticated_views
            assert len(parsed_events) == 1
            assert event is parsed_events[0]
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            assert delivery_state() == outstanding
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == 1
            )
            assert type(event) is nio.UnknownAccountDataEvent
            assert event.source == event_source
            callback_events.append(event)

        async def forbidden_response(_response: object) -> None:
            forbidden_callbacks.append("response")
            raise AssertionError("settlement invoked a response callback")

        async def forbidden_event(_room: object, _event: object) -> None:
            forbidden_callbacks.append("event")
            raise AssertionError("settlement invoked an event callback")

        client.add_global_account_data_callback(on_global_account_data, None)
        client.add_response_callback(forbidden_response, None)
        client.add_event_callback(forbidden_event, None)
        real_on_global_account_data = client._on_global_account_data
        routed_events: list[object] = []

        async def observe_global_route(event: object) -> None:
            assert len(parsed_events) == 1
            assert event is parsed_events[0]
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            routed_events.append(event)
            await real_on_global_account_data(event)

        monkeypatch.setattr(
            client,
            "_on_global_account_data",
            observe_global_route,
        )
        outstanding_graph = tuple(connection.iterdump())
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        for receipt_new, semantic_event_new in (
            (1, False),
            (True, 0),
            (False, True),
        ):
            statements.clear()
            with pytest.raises(LocalProtocolError):
                await session._settle_batch(  # type: ignore[attr-defined]
                    batch,
                    receipt_new=receipt_new,  # type: ignore[arg-type]
                    semantic_event_new=semantic_event_new,  # type: ignore[arg-type]
                )
            assert statements == []
            assert authenticated_views == []
            assert parsed_events == []
            assert routed_events == []
            assert callback_events == []
            assert forbidden_callbacks == []
            assert tuple(connection.iterdump()) == outstanding_graph
            assert delivery_state() == outstanding
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == 1
            )

        statements.clear()
        await session._settle_batch(  # type: ignore[attr-defined]
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )

        connection.set_trace_callback(None)
        assert len(authenticated_views) == 1
        assert len(parsed_events) == 1
        assert routed_events == callback_events
        assert routed_events[0] is parsed_events[0]
        assert len(callback_events) == 1
        assert forbidden_callbacks == []
        assert client.send_count == 0
        assert delivery_state() == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 0
        )
        assert any(
            statement.lstrip().upper().startswith("DELETE FROM NIOINGESTWORK")
            for statement in statements
        )
        client._assert_ingestion_not_poisoned()
        assert session.next_batch() is None
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_settle_source_to_device_reconstructs_without_crypto_replay(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    event_source = {
        "content": {"value": "durable"},
        "sender": BOB,
        "type": "org.example.settlement.to_device",
    }
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        staged = stage_classic(
            bootstrap,
            {
                "account_data": {"events": []},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "to-device-settled-token",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": [event_source]},
            },
        )
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        assert materialized.frame_id == staged.frame_id
        assert materialized.revision is not None
        batch = session.next_batch(max_records=1)
        assert batch is not None
        assert len(batch.records) == 1
        record = batch.records[0]
        assert type(record) is EventRecord
        assert record.kind is RecordKind.TO_DEVICE
        assert record.source_json == canonical_json(event_source)
        assert record.clear_json is None
        assert (
            record.room_id,
            record.membership_epoch,
            record.room_sequence,
            record.event_id,
            record.provenance,
        ) == (None, None, None, None, None)

        authenticated_views: list[object] = []
        parsed_events: list[object] = []
        real_load_settlement = session._journal._load_batch_settlement
        real_parse_to_device = ToDeviceEvent.parse_event

        def observe_authenticated_settlement(candidate):
            assert candidate == batch
            view = real_load_settlement(candidate)
            assert view is not None
            work, room = view
            assert work.value == record
            assert work.metadata is not None
            assert work.metadata.record_id == record.record_id
            assert work.metadata.preparation_phase is _PreparationPhase.SOURCE
            assert work.metadata.effective_event_type == event_source["type"]
            assert work.metadata.decryption is _DecryptionDisposition.NONE
            assert work.metadata.decryption_verified is None
            assert work.metadata.decrypted_to_device_kind is None
            assert work.metadata.callback_route is _CallbackRoute.TO_DEVICE
            assert room is None
            authenticated_views.append(view)
            return view

        def observe_to_device_parse(
            _event_type: type[ToDeviceEvent],
            source: dict[object, object],
        ) -> object:
            assert authenticated_views
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            assert source == event_source
            assert canonical_json(source) == record.source_json
            parsed = real_parse_to_device(source)
            assert type(parsed) is nio.UnknownToDeviceEvent
            assert parsed.source == event_source
            assert parsed.sender == BOB
            assert parsed.type == event_source["type"]
            parsed_events.append(parsed)
            return parsed

        outstanding = _owned_delivery_frontier(connection)
        assert outstanding == (
            1,
            None,
            record.record_id,
            batch.created_revision,
            0,
            batch.ref.sha256,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )
        e2ee_rows = _raw_e2ee_rows(connection)

        callback_events: list[object] = []
        routed_events: list[object] = []
        forbidden_callbacks: list[str] = []
        forbidden_paths: list[str] = []

        async def on_to_device(event: object) -> None:
            assert authenticated_views
            assert len(parsed_events) == 1
            assert event is parsed_events[0]
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            assert _owned_delivery_frontier(connection) == outstanding
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == 1
            )
            assert type(event) is nio.UnknownToDeviceEvent
            assert event.source == event_source
            callback_events.append(event)

        def wrong_callback(name: str):
            async def forbidden(*_args: object) -> None:
                forbidden_callbacks.append(name)
                raise AssertionError(f"settlement invoked {name} callback")

            return forbidden

        client.add_to_device_callback(on_to_device, None)
        client.add_response_callback(wrong_callback("response"), None)
        client.add_event_callback(wrong_callback("event"), None)
        client.add_ephemeral_callback(wrong_callback("ephemeral"), None)
        client.add_room_account_data_callback(
            wrong_callback("room-account-data"),
            None,
        )
        client.add_global_account_data_callback(
            wrong_callback("global-account-data"),
            None,
        )
        client.add_presence_callback(wrong_callback("presence"), None)
        real_on_to_device = client._on_to_device

        async def observe_to_device_route(event: object) -> None:
            assert len(parsed_events) == 1
            assert event is parsed_events[0]
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            routed_events.append(event)
            await real_on_to_device(event)

        def forbid_sync(name: str):
            def forbidden(*_args: object, **_kwargs: object) -> None:
                forbidden_paths.append(name)
                raise AssertionError(f"settlement invoked {name}")

            return forbidden

        def forbid_async(name: str):
            async def forbidden(*_args: object, **_kwargs: object) -> None:
                forbidden_paths.append(name)
                raise AssertionError(f"settlement invoked {name}")

            return forbidden

        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with monkeypatch.context() as patch:
            patch.setattr(
                session._journal,
                "_load_batch_settlement",
                observe_authenticated_settlement,
            )
            patch.setattr(
                ToDeviceEvent,
                "parse_event",
                classmethod(observe_to_device_parse),
            )
            patch.setattr(client, "_on_to_device", observe_to_device_route)
            for name in (
                "_handle_decrypt_to_device",
                "_handle_olm_events",
                "_handle_timeline_event",
                "_invalidate_session_for_member_event",
                "_collect_key_requests",
            ):
                patch.setattr(client, name, forbid_sync(f"client.{name}"))
            for name in (
                "_handle_expired_verifications",
                "_handle_sync",
                "_handle_to_device",
                "_on_response",
            ):
                patch.setattr(client, name, forbid_async(f"client.{name}"))
            for name in (
                "handle_to_device_event",
                "decrypt_event",
                "_decrypt_megolm_no_error",
                "decrypt_megolm_event",
                "clear_verifications",
                "collect_key_requests",
                "update_tracked_users",
                "add_changed_users",
                "save_account",
            ):
                patch.setattr(olm, name, forbid_sync(f"olm.{name}"))

            await session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )

        connection.set_trace_callback(None)
        assert len(authenticated_views) == 1
        assert len(parsed_events) == 1
        assert routed_events == callback_events
        assert routed_events[0] is parsed_events[0]
        assert len(callback_events) == 1
        assert forbidden_callbacks == []
        assert forbidden_paths == []
        assert client.send_count == 0
        assert client.send_calls == []
        assert _raw_e2ee_rows(connection) == e2ee_rows
        assert _owned_delivery_frontier(connection) == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 0
        )
        assert any(
            statement.lstrip().upper().startswith("DELETE FROM NIOINGESTWORK")
            for statement in statements
        )
        client._assert_ingestion_not_poisoned()
        assert session.next_batch() is None
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_settle_source_to_device_without_callback_is_ack_only(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    request_id = "settlement-untrusted-request"
    request_source = {
        "content": {
            "action": "request",
            "body": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "room_id": ROOM,
                "sender_key": "settlement-sender-key",
                "session_id": "settlement-session",
            },
            "request_id": request_id,
            "requesting_device_id": BOB_DEVICE,
        },
        "sender": BOB,
        "type": "m.room_key_request",
    }
    cancellation_source = {
        "content": {
            "action": "request_cancellation",
            "request_id": request_id,
            "requesting_device_id": BOB_DEVICE,
        },
        "sender": BOB,
        "type": "m.room_key_request",
    }
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        pending_request = ToDeviceEvent.parse_event(request_source)
        assert type(pending_request) is RoomKeyRequest
        olm.key_request_from_untrusted[request_id] = pending_request

        staged = stage_classic(
            bootstrap,
            {
                "account_data": {"events": []},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "to-device-ack-only-token",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": [cancellation_source]},
            },
        )
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        assert materialized.frame_id == staged.frame_id
        assert materialized.revision is not None
        assert olm.key_request_from_untrusted == {}
        assert olm.received_key_requests == {}

        owner = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner)
        assert len(inventory.work) == 2
        source_work = next(
            work
            for work in inventory.work
            if work.metadata is not None
            and work.metadata.preparation_phase is _PreparationPhase.SOURCE
        )
        collected_work = next(
            work
            for work in inventory.work
            if work.metadata is not None
            and work.metadata.preparation_phase
            is _PreparationPhase.COLLECTED_KEY_REQUEST
        )
        assert type(source_work.value) is EventRecord
        assert type(collected_work.value) is EventRecord
        assert source_work.value.kind is RecordKind.TO_DEVICE
        assert collected_work.value.kind is RecordKind.TO_DEVICE
        assert source_work.value.source_json == canonical_json(cancellation_source)
        assert collected_work.value.source_json == source_work.value.source_json
        assert source_work.value.clear_json is None
        assert collected_work.value.clear_json is None
        assert source_work.value.origin.frame_index == 0
        assert collected_work.value.origin.frame_index == 1
        assert source_work.value.record_id != collected_work.value.record_id
        assert source_work.metadata is not None
        assert source_work.metadata.effective_event_type == "m.room_key_request"
        assert source_work.metadata.decryption is _DecryptionDisposition.NONE
        assert source_work.metadata.decryption_verified is None
        assert source_work.metadata.decrypted_to_device_kind is None
        assert source_work.metadata.callback_route is None
        assert collected_work.metadata is not None
        assert collected_work.metadata.effective_event_type == "m.room_key_request"
        assert collected_work.metadata.decryption is _DecryptionDisposition.NONE
        assert collected_work.metadata.decryption_verified is None
        assert collected_work.metadata.decrypted_to_device_kind is None
        assert collected_work.metadata.callback_route is _CallbackRoute.TO_DEVICE

        batch = session.next_batch(max_records=1)
        assert batch is not None
        assert batch.records == (source_work.value,)
        outstanding = _owned_delivery_frontier(connection)
        assert outstanding == (
            1,
            None,
            source_work.value.record_id,
            batch.created_revision,
            0,
            batch.ref.sha256,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 2
        )
        e2ee_rows = _raw_e2ee_rows(connection)

        def key_request_state() -> tuple[object, ...]:
            return (
                tuple(olm.received_key_requests.items()),
                tuple(olm.key_request_from_untrusted.items()),
                tuple(
                    (target, tuple(requests.items()))
                    for target, requests in sorted(
                        olm.key_requests_waiting_for_session.items()
                    )
                ),
                tuple(olm.key_request_devices_no_session),
                tuple(olm.outgoing_to_device_messages),
                tuple(
                    (target, tuple(events))
                    for target, events in sorted(olm.key_re_requests_events.items())
                ),
            )

        prepared_key_request_state = key_request_state()
        authenticated_views: list[object] = []
        acked_refs: list[BatchRef] = []
        forbidden_paths: list[str] = []
        real_load_settlement = session._journal._load_batch_settlement
        real_acknowledge = session._journal.acknowledge_batch

        def observe_authenticated_settlement(candidate):
            assert candidate == batch
            view = real_load_settlement(candidate)
            assert view == (source_work, None)
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            authenticated_views.append(view)
            return view

        def observe_acknowledge(ref: BatchRef) -> None:
            assert authenticated_views == [(source_work, None)]
            assert forbidden_paths == []
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            assert _owned_delivery_frontier(connection) == outstanding
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == 2
            )
            assert _raw_e2ee_rows(connection) == e2ee_rows
            assert key_request_state() == prepared_key_request_state
            acked_refs.append(ref)
            real_acknowledge(ref)

        def forbid_sync(name: str):
            def forbidden(*_args: object, **_kwargs: object) -> None:
                forbidden_paths.append(name)
                raise AssertionError(f"settlement invoked {name}")

            return forbidden

        def forbid_async(name: str):
            async def forbidden(*_args: object, **_kwargs: object) -> None:
                forbidden_paths.append(name)
                raise AssertionError(f"settlement invoked {name}")

            return forbidden

        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with monkeypatch.context() as patch:
            patch.setattr(
                session._journal,
                "_load_batch_settlement",
                observe_authenticated_settlement,
            )
            patch.setattr(
                session._journal,
                "acknowledge_batch",
                observe_acknowledge,
            )
            patch.setattr(
                "nio.ingest.coordinator.load_json",
                forbid_sync("coordinator.load_json"),
            )
            patch.setattr(
                "nio.ingest.coordinator.canonical_json",
                forbid_sync("coordinator.canonical_json"),
            )
            patch.setattr(
                ToDeviceEvent,
                "parse_event",
                classmethod(forbid_sync("ToDeviceEvent.parse_event")),
            )
            patch.setattr(
                RoomKeyRequest,
                "from_dict",
                classmethod(forbid_sync("RoomKeyRequest.from_dict")),
            )
            patch.setattr(
                RoomKeyRequestCancellation,
                "from_dict",
                classmethod(forbid_sync("RoomKeyRequestCancellation.from_dict")),
            )
            for name in (
                "_handle_decrypt_to_device",
                "_handle_olm_events",
                "_collect_key_requests",
            ):
                patch.setattr(client, name, forbid_sync(f"client.{name}"))
            for name in (
                "_handle_expired_verifications",
                "_handle_sync",
                "_handle_to_device",
                "_on_response",
                "_on_to_device",
            ):
                patch.setattr(client, name, forbid_async(f"client.{name}"))
            for name in (
                "handle_to_device_event",
                "_handle_key_requests",
                "decrypt_event",
                "_decrypt_megolm_no_error",
                "decrypt_megolm_event",
                "clear_verifications",
                "collect_key_requests",
                "update_tracked_users",
                "add_changed_users",
                "save_account",
            ):
                patch.setattr(olm, name, forbid_sync(f"olm.{name}"))

            await session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )

        connection.set_trace_callback(None)
        assert authenticated_views == [(source_work, None)]
        assert acked_refs == [batch.ref]
        assert forbidden_paths == []
        assert client.send_count == 0
        assert client.send_calls == []
        assert _raw_e2ee_rows(connection) == e2ee_rows
        assert key_request_state() == prepared_key_request_state
        assert _owned_delivery_frontier(connection) == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )
        assert any(
            statement.lstrip().upper().startswith("DELETE FROM NIOINGESTWORK")
            for statement in statements
        )

        synthetic = session.next_batch(max_records=1)
        assert synthetic is not None
        assert synthetic.ref.sequence == 1
        assert synthetic.records == (collected_work.value,)
        synthetic_view = session._journal._load_batch_settlement(synthetic)
        assert synthetic_view == (collected_work, None)
        assert _raw_e2ee_rows(connection) == e2ee_rows
        assert key_request_state() == prepared_key_request_state
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.parametrize(
    "phase",
    (
        _PreparationPhase.EXPIRED_VERIFICATION,
        _PreparationPhase.COLLECTED_KEY_REQUEST,
    ),
)
@pytest.mark.asyncio
async def test_owned_settle_synthetic_to_device_phase_callbacks_without_replay(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    phase: _PreparationPhase,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    request_id = "settlement-synthetic-request"
    request_source = {
        "content": {
            "action": "request",
            "body": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "room_id": ROOM,
                "sender_key": "settlement-synthetic-sender-key",
                "session_id": "settlement-synthetic-session",
            },
            "request_id": request_id,
            "requesting_device_id": BOB_DEVICE,
        },
        "sender": BOB,
        "type": "m.room_key_request",
    }
    cancellation_source = {
        "content": {
            "action": "request_cancellation",
            "request_id": request_id,
            "requesting_device_id": BOB_DEVICE,
        },
        "sender": BOB,
        "type": "m.room_key_request",
    }
    expired_source = {
        "content": {
            "code": "m.timeout",
            "reason": "Expired",
            "transaction_id": "settlement-expired-verification",
        },
        "sender": ACCOUNT,
    }
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        pending_request = ToDeviceEvent.parse_event(request_source)
        expired_event = KeyVerificationCancel.from_dict(expired_source)
        assert type(pending_request) is RoomKeyRequest
        assert type(expired_event) is KeyVerificationCancel
        olm.key_request_from_untrusted[request_id] = pending_request
        clear_calls: list[None] = []

        def clear_verifications() -> list[KeyVerificationCancel]:
            clear_calls.append(None)
            return [expired_event]

        monkeypatch.setattr(olm, "clear_verifications", clear_verifications)
        stage_classic(
            bootstrap,
            {
                "account_data": {"events": []},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "synthetic-to-device-token",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": [cancellation_source]},
            },
        )
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        assert clear_calls == [None]
        assert olm.key_request_from_untrusted == {}
        assert olm.received_key_requests == {}

        owner = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner)
        assert len(inventory.work) == 3
        phase_work = {
            work.metadata.preparation_phase: work
            for work in inventory.work
            if work.metadata is not None
        }
        assert set(phase_work) == {
            _PreparationPhase.SOURCE,
            _PreparationPhase.EXPIRED_VERIFICATION,
            _PreparationPhase.COLLECTED_KEY_REQUEST,
        }
        target = phase_work[phase]
        assert type(target.value) is EventRecord
        record = target.value
        metadata = target.metadata
        assert metadata is not None
        expected_source = (
            expired_source
            if phase is _PreparationPhase.EXPIRED_VERIFICATION
            else cancellation_source
        )
        expected_type = (
            "m.key.verification.cancel"
            if phase is _PreparationPhase.EXPIRED_VERIFICATION
            else "m.room_key_request"
        )
        assert record.kind is RecordKind.TO_DEVICE
        assert type(record.origin) is RecordOrigin
        assert record.origin.frame_index == (
            1 if phase is _PreparationPhase.EXPIRED_VERIFICATION else 2
        )
        assert record.room_id is None
        assert record.membership_epoch is None
        assert record.room_sequence is None
        assert record.event_id is None
        assert record.provenance is None
        assert record.source_json == canonical_json(expected_source)
        assert record.clear_json is None
        assert metadata.record_id == record.record_id
        assert metadata.preparation_phase is phase
        assert metadata.effective_event_type == expected_type
        assert metadata.decryption is _DecryptionDisposition.NONE
        assert metadata.decryption_verified is None
        assert metadata.decrypted_to_device_kind is None
        assert metadata.callback_route is _CallbackRoute.TO_DEVICE

        predecessors = {
            _PreparationPhase.EXPIRED_VERIFICATION: (
                phase_work[_PreparationPhase.SOURCE].value,
            ),
            _PreparationPhase.COLLECTED_KEY_REQUEST: (
                phase_work[_PreparationPhase.SOURCE].value,
                phase_work[_PreparationPhase.EXPIRED_VERIFICATION].value,
            ),
        }[phase]
        for predecessor in predecessors:
            prior = session.next_batch(max_records=1)
            assert prior is not None
            assert prior.records == (predecessor,)
            session.acknowledge_batch(prior.ref)
        batch = session.next_batch(max_records=1)
        assert batch is not None
        assert batch.records == (record,)
        outstanding = _owned_delivery_frontier(connection)
        e2ee_rows = _raw_e2ee_rows(connection)
        assert connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[
            0
        ] == 3 - len(predecessors)
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_client.config = replace(
        restored_client.config,
    )
    restored_session = open_owned_session(restored_client, reopened, generation)
    restored_connection = restored_session._owned_store.database.connection()
    try:
        assert restored_client.olm is not None
        restored_olm = restored_client.olm
        assert restored_session.next_batch(max_records=1) == batch
        assert _raw_e2ee_rows(restored_connection) == e2ee_rows
        assert _owned_delivery_frontier(restored_connection) == outstanding
        authenticated: list[object] = []
        parsed: list[ToDeviceEvent] = []
        routed: list[ToDeviceEvent] = []
        callbacks: list[ToDeviceEvent] = []
        acknowledged: list[BatchRef] = []
        forbidden: list[str] = []
        real_load = restored_session._journal._load_batch_settlement
        real_ack = restored_session._journal.acknowledge_batch
        real_cancel_parse = KeyVerificationCancel.from_dict
        real_to_device_parse = ToDeviceEvent.parse_event
        real_route = (
            restored_client._on_expired_verifications
            if phase is _PreparationPhase.EXPIRED_VERIFICATION
            else restored_client._on_to_device
        )

        def observe_load(candidate):
            assert candidate == batch
            view = real_load(candidate)
            assert view == (target, None)
            assert restored_connection.in_transaction is False
            assert restored_session._journal._owner._depth == 0
            assert restored_session._journal._owner.database.transaction_depth() == 0
            authenticated.append(view)
            return view

        def assert_outside_store() -> None:
            assert authenticated == [(target, None)]
            assert restored_connection.in_transaction is False
            assert restored_session._journal._owner._depth == 0
            assert restored_session._journal._owner.database.transaction_depth() == 0
            assert _owned_delivery_frontier(restored_connection) == outstanding
            assert _raw_e2ee_rows(restored_connection) == e2ee_rows

        def observe_cancel_parse(_cls, source):
            assert phase is _PreparationPhase.EXPIRED_VERIFICATION
            assert source == expected_source
            assert "type" not in source
            assert_outside_store()
            event = real_cancel_parse(source)
            assert type(event) is KeyVerificationCancel
            parsed.append(event)
            return event

        def observe_to_device_parse(_cls, source):
            assert phase is _PreparationPhase.COLLECTED_KEY_REQUEST
            assert source == expected_source
            assert_outside_store()
            event = real_to_device_parse(source)
            assert type(event) is RoomKeyRequestCancellation
            parsed.append(event)
            return event

        async def observe_route(event: ToDeviceEvent) -> None:
            assert parsed == [event]
            assert_outside_store()
            routed.append(event)
            await real_route(event)

        async def callback(event: ToDeviceEvent) -> None:
            assert routed == [event]
            assert_outside_store()
            callbacks.append(event)

        def observe_ack(ref: BatchRef) -> None:
            assert callbacks == parsed
            assert forbidden == []
            assert_outside_store()
            acknowledged.append(ref)
            real_ack(ref)

        def forbid(name: str):
            def forbidden_path(*_args: object, **_kwargs: object) -> None:
                forbidden.append(name)
                raise AssertionError(f"settlement invoked {name}")

            return forbidden_path

        restored_client.add_to_device_callback(callback, None)
        statements: list[str] = []
        restored_connection.set_trace_callback(statements.append)
        with monkeypatch.context() as patch:
            patch.setattr(
                restored_session._journal,
                "_load_batch_settlement",
                observe_load,
            )
            patch.setattr(
                restored_session._journal,
                "acknowledge_batch",
                observe_ack,
            )
            if phase is _PreparationPhase.EXPIRED_VERIFICATION:
                patch.setattr(
                    KeyVerificationCancel,
                    "from_dict",
                    classmethod(observe_cancel_parse),
                )
                patch.setattr(
                    ToDeviceEvent,
                    "parse_event",
                    classmethod(forbid("ToDeviceEvent.parse_event")),
                )
                patch.setattr(
                    restored_client,
                    "_on_expired_verifications",
                    observe_route,
                )
                patch.setattr(
                    restored_client,
                    "_on_to_device",
                    forbid("client._on_to_device"),
                )
            else:
                patch.setattr(
                    ToDeviceEvent,
                    "parse_event",
                    classmethod(observe_to_device_parse),
                )
                patch.setattr(
                    KeyVerificationCancel,
                    "from_dict",
                    classmethod(forbid("KeyVerificationCancel.from_dict")),
                )
                patch.setattr(restored_client, "_on_to_device", observe_route)
                patch.setattr(
                    restored_client,
                    "_on_expired_verifications",
                    forbid("client._on_expired_verifications"),
                )
            for name in ("clear_verifications", "collect_key_requests"):
                patch.setattr(restored_olm, name, forbid(f"olm.{name}"))
            patch.setattr(
                restored_client,
                "_handle_expired_verifications",
                forbid("client._handle_expired_verifications"),
            )
            patch.setattr(
                restored_client,
                "_collect_key_requests",
                forbid("client._collect_key_requests"),
            )
            await restored_session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )

        restored_connection.set_trace_callback(None)
        assert len(authenticated) == 1
        assert len(parsed) == 1
        assert routed == parsed
        assert callbacks == parsed
        assert acknowledged == [batch.ref]
        assert forbidden == []
        assert restored_client.send_count == 0
        assert restored_client.send_calls == []
        assert _raw_e2ee_rows(restored_connection) == e2ee_rows
        assert _owned_delivery_frontier(restored_connection) == (
            batch.ref.sequence + 1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert any(
            statement.lstrip().upper().startswith("DELETE FROM NIOINGESTWORK")
            for statement in statements
        )
    finally:
        restored_connection.set_trace_callback(None)
        await restored_session.close()


@pytest.mark.parametrize(
    ("kind", "decrypted_kind"),
    (
        ("forwarded", _DecryptedToDeviceKind.FORWARDED_ROOM_KEY),
        ("dummy", _DecryptedToDeviceKind.DUMMY),
        ("unknown", _DecryptedToDeviceKind.UNKNOWN),
        ("bad", _DecryptedToDeviceKind.BAD),
        ("unknown-bad", _DecryptedToDeviceKind.UNKNOWN_BAD),
    ),
)
@pytest.mark.asyncio
async def test_owned_settle_remaining_decrypted_to_device_exact_classes(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    kind: str,
    decrypted_kind: _DecryptedToDeviceKind,
) -> None:
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    outer_sender_key = "settlement-outer-sender-key"
    outer_source = {
        "content": {
            "algorithm": "m.olm.v1.curve25519-aes-sha2",
            "ciphertext": {
                "YWJjZA": {
                    "body": "settlement-ciphertext",
                    "type": 0,
                }
            },
            "sender_key": outer_sender_key,
        },
        "sender": BOB,
        "type": "m.room.encrypted",
    }
    if kind == "forwarded":
        clear_input = {
            "content": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "forwarding_curve25519_key_chain": ["forwarded-curve-key"],
                "room_id": ROOM,
                "sender_claimed_ed25519_key": "forwarded-ed25519-key",
                "sender_key": "forwarded-sender-key",
                "session_id": "forwarded-session",
                "session_key": "forwarded-session-secret",
            },
            "sender": BOB,
            "type": "m.forwarded_room_key",
        }
        decrypted_event = ForwardedRoomKeyEvent.from_dict(
            clear_input,
            BOB,
            outer_sender_key,
        )
    elif kind == "dummy":
        clear_input = {
            "content": {},
            "keys": {"ed25519": "dummy-ed25519-key"},
            "sender": BOB,
            "sender_device": BOB_DEVICE,
            "type": "m.dummy",
        }
        decrypted_event = DummyEvent.from_dict(
            clear_input,
            BOB,
            outer_sender_key,
        )
    elif kind == "unknown":
        clear_input = {
            "content": {"value": "unknown"},
            "sender": BOB,
            "type": "org.example.settlement.unknown",
        }
        decrypted_event = UnknownToDeviceEvent.from_dict(clear_input)
    elif kind == "bad":
        clear_input = {
            "content": {"value": "bad"},
            "event_id": "$settlement-bad",
            "origin_server_ts": 7,
            "sender": BOB,
            "type": "org.example.settlement.bad",
        }
        decrypted_event = BadEvent.from_dict(clear_input)
    else:
        assert kind == "unknown-bad"
        clear_input = {
            "content": {"value": "unknown-bad"},
            "type": "org.example.settlement.unknown-bad",
        }
        decrypted_event = UnknownBadEvent(clear_input)
    assert type(decrypted_event.source) is dict
    clear = decrypted_event.source
    assert canonical_json(clear) == canonical_json(decrypted_event.source)
    if kind == "forwarded":
        assert "session_key" not in clear["content"]

    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        decrypt_calls: list[object] = []

        def decrypt_to_device(event: object) -> object:
            assert getattr(event, "source", None) == outer_source
            decrypt_calls.append(event)
            return decrypted_event

        monkeypatch.setattr(client, "_handle_decrypt_to_device", decrypt_to_device)
        stage_classic(
            bootstrap,
            {
                "account_data": {"events": []},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": f"decrypted-{kind}-token",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": [outer_source]},
            },
        )
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        assert len(decrypt_calls) == 1
        batch = session.next_batch(max_records=1)
        assert batch is not None
        assert len(batch.records) == 1
        record = batch.records[0]
        assert type(record) is EventRecord
        assert record.kind is RecordKind.TO_DEVICE
        assert type(record.origin) is RecordOrigin
        assert record.origin.frame_index == 0
        assert record.room_id is None
        assert record.membership_epoch is None
        assert record.room_sequence is None
        assert record.event_id is None
        assert record.provenance is None
        assert record.source_json == canonical_json(outer_source)
        assert record.clear_json == canonical_json(clear)
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner)
        assert len(inventory.work) == 1
        work = inventory.work[0]
        assert work.value == record
        metadata = work.metadata
        assert metadata is not None
        assert metadata.record_id == record.record_id
        assert metadata.preparation_phase is _PreparationPhase.SOURCE
        assert metadata.effective_event_type == clear["type"]
        assert metadata.decryption is _DecryptionDisposition.DECRYPTED
        assert metadata.decryption_verified is None
        assert metadata.decrypted_to_device_kind is decrypted_kind
        assert metadata.callback_route is _CallbackRoute.TO_DEVICE
        outstanding = _owned_delivery_frontier(connection)
        e2ee_rows = _raw_e2ee_rows(connection)
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_client.config = replace(
        restored_client.config,
    )
    restored_session = open_owned_session(restored_client, reopened, generation)
    restored_connection = restored_session._owned_store.database.connection()
    try:
        assert restored_client.olm is not None
        assert restored_client.store is not None
        assert restored_session.next_batch(max_records=1) == batch
        assert _owned_delivery_frontier(restored_connection) == outstanding
        assert _raw_e2ee_rows(restored_connection) == e2ee_rows
        authenticated: list[object] = []
        reconstructed: list[object] = []
        routed: list[object] = []
        callbacks: list[object] = []
        acknowledgements: list[BatchRef] = []
        forbidden: list[str] = []
        real_load = restored_session._journal._load_batch_settlement
        real_ack = restored_session._journal.acknowledge_batch
        real_route = restored_client._on_to_device

        def assert_outside_store() -> None:
            assert authenticated == [(work, None)]
            assert restored_connection.in_transaction is False
            assert restored_session._journal._owner._depth == 0
            assert restored_session._journal._owner.database.transaction_depth() == 0
            assert _owned_delivery_frontier(restored_connection) == outstanding
            assert _raw_e2ee_rows(restored_connection) == e2ee_rows

        def observe_load(candidate):
            assert candidate == batch
            view = real_load(candidate)
            assert view == (work, None)
            assert restored_connection.in_transaction is False
            assert restored_session._journal._owner._depth == 0
            assert restored_session._journal._owner.database.transaction_depth() == 0
            authenticated.append(view)
            return view

        def assert_event(event: object) -> None:
            assert type(event) is type(decrypted_event)
            assert event.source == clear
            if kind == "forwarded":
                assert (
                    event.sender,
                    event.sender_key,
                    event.room_id,
                    event.session_id,
                    event.algorithm,
                ) == (
                    BOB,
                    outer_sender_key,
                    ROOM,
                    "forwarded-session",
                    "m.megolm.v1.aes-sha2",
                )
            elif kind == "dummy":
                assert (event.sender, event.sender_key, event.sender_device) == (
                    BOB,
                    outer_sender_key,
                    BOB_DEVICE,
                )
            elif kind == "unknown":
                assert (event.sender, event.type) == (BOB, clear["type"])
            elif kind == "bad":
                assert (
                    event.event_id,
                    event.sender,
                    event.server_timestamp,
                    event.type,
                ) == ("$settlement-bad", BOB, 7, clear["type"])

        def capture(event: object) -> object:
            assert_outside_store()
            reconstructed.append(event)
            return event

        real_forwarded_init = ForwardedRoomKeyEvent.__init__
        real_dummy_parse = DummyEvent.from_dict
        real_unknown_parse = UnknownToDeviceEvent.from_dict
        real_bad_parse = BadEvent.from_dict
        real_unknown_bad_init = UnknownBadEvent.__init__

        def observe_forwarded_init(self, *args: object, **kwargs: object) -> None:
            real_forwarded_init(self, *args, **kwargs)
            capture(self)

        def observe_dummy_parse(_cls, source, sender, sender_key):
            assert (source, sender, sender_key) == (clear, BOB, outer_sender_key)
            return capture(real_dummy_parse(source, sender, sender_key))

        def observe_unknown_parse(_cls, source):
            assert source == clear
            return capture(real_unknown_parse(source))

        def observe_bad_parse(_cls, source):
            assert source == clear
            return capture(real_bad_parse(source))

        def observe_unknown_bad_init(
            self,
            source,
            transaction_id=None,
        ) -> None:
            assert source == clear
            real_unknown_bad_init(self, source, transaction_id)
            capture(self)

        async def observe_route(event: object) -> None:
            assert reconstructed == [event]
            assert_event(event)
            assert_outside_store()
            routed.append(event)
            await real_route(event)  # type: ignore[arg-type]

        async def callback(event: object) -> None:
            assert routed == [event]
            assert_event(event)
            assert_outside_store()
            callbacks.append(event)

        def observe_ack(ref: BatchRef) -> None:
            assert callbacks == reconstructed
            assert forbidden == []
            assert_outside_store()
            acknowledgements.append(ref)
            real_ack(ref)

        def forbid(name: str):
            def forbidden_path(*_args: object, **_kwargs: object) -> None:
                forbidden.append(name)
                raise AssertionError(f"settlement invoked {name}")

            return forbidden_path

        restored_client.add_to_device_callback(callback, None)
        statements: list[str] = []
        restored_connection.set_trace_callback(statements.append)
        with monkeypatch.context() as patch:
            patch.setattr(
                restored_session._journal,
                "_load_batch_settlement",
                observe_load,
            )
            patch.setattr(restored_session._journal, "acknowledge_batch", observe_ack)
            patch.setattr(restored_client, "_on_to_device", observe_route)
            patch.setattr(
                ToDeviceEvent,
                "parse_event",
                classmethod(forbid("ToDeviceEvent.parse_event")),
            )
            if kind == "forwarded":
                patch.setattr(ForwardedRoomKeyEvent, "__init__", observe_forwarded_init)
                patch.setattr(
                    ForwardedRoomKeyEvent,
                    "from_dict",
                    classmethod(forbid("ForwardedRoomKeyEvent.from_dict")),
                )
            elif kind == "dummy":
                patch.setattr(
                    DummyEvent,
                    "from_dict",
                    classmethod(observe_dummy_parse),
                )
            elif kind == "unknown":
                patch.setattr(
                    UnknownToDeviceEvent,
                    "from_dict",
                    classmethod(observe_unknown_parse),
                )
            elif kind == "bad":
                patch.setattr(
                    BadEvent,
                    "from_dict",
                    classmethod(observe_bad_parse),
                )
            else:
                patch.setattr(UnknownBadEvent, "__init__", observe_unknown_bad_init)
            for name in (
                "receive_response",
                "_handle_sync",
                "_handle_to_device",
                "_handle_decrypt_to_device",
                "_handle_olm_events",
                "_prepare_ingestion_frame",
            ):
                patch.setattr(restored_client, name, forbid(f"client.{name}"))
            for name in (
                "handle_to_device_event",
                "decrypt_event",
                "_try_decrypt",
                "_handle_olm_event",
                "decrypt",
                "save_session",
                "save_inbound_group_session",
                "save_account",
                "collect_key_requests",
                "clear_verifications",
            ):
                patch.setattr(restored_client.olm, name, forbid(f"olm.{name}"))
            patch.setattr(
                restored_client.olm.session_store,
                "get",
                forbid("session_store.get"),
            )
            patch.setattr(
                restored_client.olm.inbound_group_store,
                "get",
                forbid("inbound_group_store.get"),
            )
            for name in (
                "load_account",
                "load_sessions",
                "load_inbound_group_sessions",
                "save_account",
                "save_session",
                "save_inbound_group_session",
            ):
                patch.setattr(restored_client.store, name, forbid(f"store.{name}"))
            patch.setattr(
                restored_session,
                "_materialize_oldest_frame",
                forbid("session._materialize_oldest_frame"),
            )
            await restored_session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )

        restored_connection.set_trace_callback(None)
        assert len(authenticated) == 1
        assert len(reconstructed) == 1
        assert reconstructed == routed == callbacks
        assert acknowledgements == [batch.ref]
        assert forbidden == []
        assert restored_client.send_count == 0
        assert restored_client.send_calls == []
        assert _raw_e2ee_rows(restored_connection) == e2ee_rows
        assert _owned_delivery_frontier(restored_connection) == (
            batch.ref.sequence + 1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert any(
            statement.lstrip().upper().startswith("DELETE FROM NIOINGESTWORK")
            for statement in statements
        )
    finally:
        restored_connection.set_trace_callback(None)
        await restored_session.close()


def _claimed_owned_global_settlement(tmp_path):
    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    assert client.olm is not None
    olm = client.olm
    olm.account.shared = True
    olm.uploaded_key_count = olm.account.max_one_time_keys
    olm.save_account()
    sources = (
        {
            "content": {"index": 0},
            "type": "org.example.settlement.retry",
        },
        {
            "content": {"index": 1},
            "type": "org.example.settlement.retry",
        },
    )
    stage_classic(
        bootstrap,
        {
            "account_data": {"events": list(sources)},
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {
                "signed_curve25519": olm.account.max_one_time_keys
            },
            "device_unused_fallback_key_types": [],
            "next_batch": "settlement-retry-token",
            "presence": {"events": []},
            "rooms": {},
            "to_device": {"events": []},
        },
    )
    materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
    assert materialized.status is MaterializeStatus.MATERIALIZED
    batch = session.next_batch(max_records=1)
    assert batch is not None
    assert len(batch.records) == 1
    assert type(batch.records[0]) is EventRecord
    assert batch.records[0].source_json == canonical_json(sources[0])
    connection = session._owned_store.database.connection()
    assert connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 2
    return client, session, connection, batch, sources


def _owned_delivery_frontier(connection) -> tuple[object, ...]:
    row = connection.execute(
        "SELECT delivery_next_sequence, delivery_acknowledged_sha256, "
        "delivery_outstanding_work_id, delivery_outstanding_ready_revision, "
        "delivery_outstanding_ready_ordinal, "
        "delivery_outstanding_batch_sha256 FROM NioIngestMeta"
    ).fetchone()
    assert row is not None
    return tuple(row)


@pytest.mark.asyncio
async def test_owned_settle_blocks_callback_reentrant_ack_before_later_failure(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, session, connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    outstanding = _owned_delivery_frontier(connection)
    graph = tuple(connection.iterdump())
    authenticated: list[object] = []
    real_load_settlement = session._journal._load_batch_settlement

    def observe_settlement(candidate):
        view = real_load_settlement(candidate)
        authenticated.append(view)
        return view

    monkeypatch.setattr(
        session._journal,
        "_load_batch_settlement",
        observe_settlement,
    )
    callback_order: list[str] = []
    sentinel = RuntimeError("later callback failed")

    async def reentrant_ack(_event: object) -> None:
        callback_order.append("ack")
        with pytest.raises(LocalProtocolError):
            session.acknowledge_batch(batch.ref)
        assert _owned_delivery_frontier(connection) == outstanding
        assert tuple(connection.iterdump()) == graph

    async def fail_later(_event: object) -> None:
        callback_order.append("fail")
        raise sentinel

    client.add_global_account_data_callback(reentrant_ack, None)
    client.add_global_account_data_callback(fail_later, None)
    try:
        with pytest.raises(RuntimeError) as failure:
            await session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )
        assert failure.value is sentinel
        assert callback_order == ["ack", "fail"]
        assert len(authenticated) == 1
        assert _owned_delivery_frontier(connection) == outstanding
        assert tuple(connection.iterdump()) == graph
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 2
        )
        client._assert_ingestion_not_poisoned()

        await session._settle_batch(
            batch,
            receipt_new=False,
            semantic_event_new=False,
        )
        assert callback_order == ["ack", "fail"]
        assert _owned_delivery_frontier(connection) == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )
        client._assert_ingestion_not_poisoned()
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_settle_rejects_recursive_same_task_settlement(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, session, connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    authenticated: list[object] = []
    real_load_settlement = session._journal._load_batch_settlement

    def observe_settlement(candidate):
        view = real_load_settlement(candidate)
        authenticated.append(view)
        return view

    monkeypatch.setattr(
        session._journal,
        "_load_batch_settlement",
        observe_settlement,
    )
    callback_depth = 0
    callback_calls = 0

    async def callback(_event: object) -> None:
        nonlocal callback_calls, callback_depth
        callback_calls += 1
        callback_depth += 1
        try:
            if callback_depth > 1:
                raise AssertionError("recursive settlement reentered callbacks")
            with pytest.raises(LocalProtocolError):
                await session._settle_batch(
                    batch,
                    receipt_new=True,
                    semantic_event_new=False,
                )
            assert len(authenticated) == 1
        finally:
            callback_depth -= 1

    client.add_global_account_data_callback(callback, None)
    try:
        await asyncio.wait_for(
            session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            ),
            timeout=1,
        )
        assert callback_calls == 1
        assert len(authenticated) == 1
        assert _owned_delivery_frontier(connection) == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )
        client._assert_ingestion_not_poisoned()
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_owned_settle_serializes_concurrent_calls_for_same_batch(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, session, connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    authenticated: list[object] = []
    real_load_settlement = session._journal._load_batch_settlement

    def observe_settlement(candidate):
        view = real_load_settlement(candidate)
        authenticated.append(view)
        return view

    monkeypatch.setattr(
        session._journal,
        "_load_batch_settlement",
        observe_settlement,
    )
    callback_events: list[object] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def callback(event: object) -> None:
        callback_events.append(event)
        entered.set()
        await release.wait()

    client.add_global_account_data_callback(callback, None)
    first = asyncio.create_task(
        session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
    )
    second_started = asyncio.Event()

    async def settle_second() -> None:
        second_started.set()
        await session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )

    second: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        second = asyncio.create_task(settle_second())
        await asyncio.wait_for(second_started.wait(), timeout=1)
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        pre_release_callbacks = len(callback_events)
        pre_release_views = len(authenticated)
        second_pending = not second.done()
        release.set()
        await asyncio.wait_for(asyncio.gather(first, second), timeout=1)

        assert pre_release_callbacks == 1
        assert pre_release_views == 1
        assert second_pending
        assert len(callback_events) == 1
        assert len(authenticated) == 2
        assert sum(view is not None for view in authenticated) == 1
        assert _owned_delivery_frontier(connection) == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )
        client._assert_ingestion_not_poisoned()
    finally:
        release.set()
        pending = [task for task in (first, second) if task is not None]
        if pending:
            await asyncio.gather(*pending, return_exceptions=True)
        await session.close()


@pytest.mark.asyncio
async def test_owned_settle_duplicate_receipts_ack_without_callbacks_or_rewrites(
    tmp_path,
) -> None:
    client, session, connection, first, sources = _claimed_owned_global_settlement(
        tmp_path
    )
    callbacks: list[object] = []

    async def callback(event: object) -> None:
        callbacks.append(event)

    client.add_global_account_data_callback(callback, None)
    labels: list[str] = []
    session._journal.set_transition_statement_hook(labels.append)
    try:
        await session._settle_batch(
            first,
            receipt_new=False,
            semantic_event_new=False,
        )
        assert callbacks == []
        assert labels == [
            "delivery_work_delete",
            "delivery_ack_meta_cas",
            "before_commit",
            "commit",
        ]
        assert _owned_delivery_frontier(connection) == (
            1,
            first.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )

        labels.clear()
        no_outstanding = tuple(connection.iterdump())
        await session._settle_batch(
            first,
            receipt_new=False,
            semantic_event_new=False,
        )
        assert callbacks == []
        assert labels == []
        assert tuple(connection.iterdump()) == no_outstanding

        second = session.next_batch(max_records=1)
        assert second is not None
        assert second.ref.sequence == 1
        assert second.records[0].source_json == canonical_json(sources[1])
        labels.clear()
        next_outstanding = tuple(connection.iterdump())
        next_frontier = _owned_delivery_frontier(connection)
        await session._settle_batch(
            first,
            receipt_new=False,
            semantic_event_new=False,
        )
        assert callbacks == []
        assert labels == []
        assert tuple(connection.iterdump()) == next_outstanding
        assert _owned_delivery_frontier(connection) == next_frontier

        await session._settle_batch(
            second,
            receipt_new=False,
            semantic_event_new=False,
        )
        assert callbacks == []
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 0
        )
        client._assert_ingestion_not_poisoned()
    finally:
        session._journal.set_transition_statement_hook(None)
        await session.close()


@pytest.mark.parametrize(
    "error_type",
    (RuntimeError, asyncio.CancelledError),
    ids=("error", "cancel"),
)
@pytest.mark.asyncio
async def test_owned_settle_callback_failure_keeps_work_for_receipt_replay(
    tmp_path,
    error_type,
) -> None:
    client, session, connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    sentinel = error_type("callback interrupted")
    callback_events: list[object] = []

    async def callback(event: object) -> None:
        callback_events.append(event)
        raise sentinel

    client.add_global_account_data_callback(callback, None)
    outstanding = _owned_delivery_frontier(connection)
    graph = tuple(connection.iterdump())
    try:
        with pytest.raises(error_type) as failure:
            await session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )
        assert failure.value is sentinel
        assert len(callback_events) == 1
        assert _owned_delivery_frontier(connection) == outstanding
        assert tuple(connection.iterdump()) == graph
        assert session.next_batch(max_records=1) == batch
        client._assert_ingestion_not_poisoned()

        await session._settle_batch(
            batch,
            receipt_new=False,
            semantic_event_new=False,
        )
        assert len(callback_events) == 1
        assert _owned_delivery_frontier(connection) == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )
        client._assert_ingestion_not_poisoned()
    finally:
        await session.close()


@pytest.mark.parametrize("boundary", ("before_commit", "commit"))
@pytest.mark.asyncio
async def test_owned_settle_ack_uncertainty_retries_without_second_callback(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    client, session, connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    callback_events: list[object] = []

    async def callback(event: object) -> None:
        callback_events.append(event)

    client.add_global_account_data_callback(callback, None)
    parsed_events: list[object] = []
    real_parse_account_data = AccountDataEvent.parse_event

    def observe_parse(
        _event_type: type[AccountDataEvent],
        source: dict[object, object],
    ) -> object:
        event = real_parse_account_data(source)
        parsed_events.append(event)
        return event

    monkeypatch.setattr(
        AccountDataEvent,
        "parse_event",
        classmethod(observe_parse),
    )
    sentinel = RuntimeError(f"ack {boundary} uncertainty")
    labels: list[str] = []

    def abort(label: str) -> None:
        labels.append(label)
        if label == boundary:
            raise sentinel

    session._journal.set_transition_statement_hook(abort)
    outstanding = _owned_delivery_frontier(connection)
    graph = tuple(connection.iterdump())
    expected_labels = (
        "delivery_work_delete",
        "delivery_ack_meta_cas",
        "before_commit",
        "commit",
    )
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(RuntimeError) as failure:
            await session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )
        connection.set_trace_callback(None)
        assert failure.value is sentinel
        assert len(callback_events) == 1
        assert len(parsed_events) == 1
        assert labels == list(expected_labels[: expected_labels.index(boundary) + 1])
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        assert normalized.count("BEGIN IMMEDIATE") == 1
        assert any(
            statement.startswith("DELETE FROM NIOINGESTWORK")
            for statement in normalized
        )
        assert any(
            statement.startswith("UPDATE NIOINGESTMETA") for statement in normalized
        )
        client._assert_ingestion_not_poisoned()

        if boundary == "before_commit":
            assert normalized.count("ROLLBACK") == 1
            assert normalized.count("COMMIT") == 0
            assert tuple(connection.iterdump()) == graph
            assert _owned_delivery_frontier(connection) == outstanding
            assert session.next_batch(max_records=1) == batch
            retry_receipt_new = False
        else:
            assert normalized.count("COMMIT") == 1
            assert normalized.count("ROLLBACK") == 0
            assert tuple(connection.iterdump()) != graph
            assert _owned_delivery_frontier(connection) == (
                1,
                batch.ref.sha256,
                None,
                None,
                None,
                None,
            )
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == 1
            )
            retry_receipt_new = True

        session._journal.set_transition_statement_hook(None)
        labels.clear()
        parsed_events.clear()
        before_retry = tuple(connection.iterdump())
        await session._settle_batch(
            batch,
            receipt_new=retry_receipt_new,
            semantic_event_new=False,
        )
        assert len(callback_events) == 1
        if boundary == "before_commit":
            assert len(parsed_events) == 1
            assert tuple(connection.iterdump()) != before_retry
        else:
            assert parsed_events == []
            assert tuple(connection.iterdump()) == before_retry
        assert labels == []
        assert _owned_delivery_frontier(connection) == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 1
        )
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        session._journal.set_transition_statement_hook(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_settle_rechecks_poison_after_callback_before_ack(tmp_path) -> None:
    client, session, connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    outstanding = _owned_delivery_frontier(connection)
    graph = tuple(connection.iterdump())
    callback_events: list[object] = []
    entered = asyncio.Event()
    release = asyncio.Event()

    async def callback(event: object) -> None:
        callback_events.append(event)
        entered.set()
        await release.wait()

    client.add_global_account_data_callback(callback, None)
    statements: list[str] = []
    connection.set_trace_callback(statements.append)
    settlement = asyncio.create_task(
        session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
    )
    poison_message = "owned ingestion session is poisoned; reopen with a fresh client"
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        client._poison_ingestion()
        release.set()
        with pytest.raises(LocalProtocolError, match=poison_message):
            await settlement

        assert len(callback_events) == 1
        assert _owned_delivery_frontier(connection) == outstanding
        assert tuple(connection.iterdump()) == graph
        assert session._settlement_task is None
        assert session._settlement_lock.locked() is False

        statements.clear()
        with pytest.raises(LocalProtocolError, match=poison_message):
            await session._settle_batch(
                batch,
                receipt_new=False,
                semantic_event_new=False,
            )
        assert statements == []
        assert _owned_delivery_frontier(connection) == outstanding
        assert tuple(connection.iterdump()) == graph
    finally:
        release.set()
        await asyncio.gather(settlement, return_exceptions=True)
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_close_cancels_blocked_settlement_before_detach_and_replays(
    tmp_path,
) -> None:
    client, session, _connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    generation = session._journal.load_owner().consumer_generation
    saved_store = session._owned_store
    callback_events: list[object] = []
    entered = asyncio.Event()
    callback_cancelled = asyncio.Event()

    async def callback(event: object) -> None:
        callback_events.append(event)
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            assert client.store is saved_store
            assert session._detached is False
            callback_cancelled.set()
            raise

    client.add_global_account_data_callback(callback, None)
    settlement = asyncio.create_task(
        session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
    )
    close_task: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        close_task = asyncio.create_task(session.close())
        await asyncio.wait_for(callback_cancelled.wait(), timeout=1)
        with pytest.raises(asyncio.CancelledError):
            await settlement
        await asyncio.wait_for(close_task, timeout=1)
        assert callback_events
        assert len(callback_events) == 1
        assert session._closed
        assert client.store is None and client.olm is None
    finally:
        if not settlement.done():
            settlement.cancel()
        await asyncio.gather(settlement, return_exceptions=True)
        if close_task is not None:
            if not close_task.done():
                close_task.cancel()
            await asyncio.gather(close_task, return_exceptions=True)
        if not session._closed:
            await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    try:
        assert fresh_session.next_batch(max_records=1) == batch
    finally:
        await fresh_session.close()


@pytest.mark.asyncio
async def test_owned_callback_close_fails_before_lease_and_outer_settles(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client, session, connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    saved_store = session._owned_store
    prepare_close_calls: list[None] = []
    real_prepare_close = session._lease._prepare_close

    def observe_prepare_close() -> None:
        prepare_close_calls.append(None)
        raise AssertionError("callback close reached lease preparation")

    monkeypatch.setattr(session._lease, "_prepare_close", observe_prepare_close)
    callback_events: list[object] = []

    async def callback(event: object) -> None:
        callback_events.append(event)
        with pytest.raises(LocalProtocolError):
            await session.close()
        assert prepare_close_calls == []
        assert client.store is saved_store
        assert session._close_task is None
        assert session._detached is False

    client.add_global_account_data_callback(callback, None)
    try:
        await asyncio.wait_for(
            session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            ),
            timeout=1,
        )
        assert len(callback_events) == 1
        assert prepare_close_calls == []
        assert _owned_delivery_frontier(connection) == (
            1,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        second = session.next_batch(max_records=1)
        assert second is not None
        session.acknowledge_batch(second.ref)
        assert session.next_batch(max_records=1) is None
        client._assert_ingestion_not_poisoned()
    finally:
        monkeypatch.setattr(session._lease, "_prepare_close", real_prepare_close)
        if not session._closed:
            await session.close()


@pytest.mark.asyncio
async def test_owned_close_waits_for_cancel_suppressing_callback_and_prevents_ack(
    tmp_path,
) -> None:
    client, session, _connection, batch, _sources = _claimed_owned_global_settlement(
        tmp_path
    )
    generation = session._journal.load_owner().consumer_generation
    saved_store = session._owned_store
    callback_events: list[object] = []
    entered = asyncio.Event()
    cancellation_suppressed = asyncio.Event()
    release = asyncio.Event()

    async def callback(event: object) -> None:
        callback_events.append(event)
        entered.set()
        try:
            await asyncio.Event().wait()
        except asyncio.CancelledError:
            cancellation_suppressed.set()
            await release.wait()

    client.add_global_account_data_callback(callback, None)
    settlement = asyncio.create_task(
        session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )
    )
    first_close: asyncio.Task[None] | None = None
    second_close: asyncio.Task[None] | None = None
    try:
        await asyncio.wait_for(entered.wait(), timeout=1)
        first_close = asyncio.create_task(session.close())
        await asyncio.wait_for(cancellation_suppressed.wait(), timeout=1)
        assert first_close.done() is False
        assert client.store is saved_store
        assert session._detached is False

        second_close = asyncio.create_task(session.close())
        first_close.cancel()
        await asyncio.sleep(0)
        await asyncio.sleep(0)
        assert first_close.done() is False
        assert second_close.done() is False
        assert client.store is saved_store

        release.set()
        with pytest.raises(asyncio.CancelledError):
            await first_close
        await asyncio.wait_for(second_close, timeout=1)
        with pytest.raises(LocalProtocolError, match="ingestion session is closed"):
            await settlement
        assert len(callback_events) == 1
        assert session._closed
        assert client.store is None and client.olm is None
    finally:
        release.set()
        if not settlement.done():
            settlement.cancel()
        await asyncio.gather(settlement, return_exceptions=True)
        for task in (first_close, second_close):
            if task is not None and not task.done():
                task.cancel()
        await asyncio.gather(
            *(task for task in (first_close, second_close) if task is not None),
            return_exceptions=True,
        )
        if not session._closed:
            await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    fresh_client = owned_client(tmp_path)
    fresh_session = open_owned_session(fresh_client, reopened, generation)
    try:
        assert fresh_session.next_batch(max_records=1) == batch
    finally:
        await fresh_session.close()


def _install_owned_settlement_callback_tripwires(
    client: RecordingClient,
    forbidden: list[str],
) -> None:
    async def one_arg(_event: object) -> None:
        forbidden.append("one-arg")
        raise AssertionError("settlement invoked an auxiliary callback")

    async def room_event(_room: object, _event: object) -> None:
        forbidden.append("room-event")
        raise AssertionError("settlement invoked a room callback")

    async def response(_response: object) -> None:
        forbidden.append("response")
        raise AssertionError("settlement invoked a response callback")

    client.add_global_account_data_callback(one_arg, None)
    client.add_presence_callback(one_arg, None)
    client.add_to_device_callback(one_arg, None)
    client.add_event_callback(room_event, None)
    client.add_ephemeral_callback(room_event, None)
    client.add_room_account_data_callback(room_event, None)
    client.add_response_callback(response, None)


def _seed_owned_room_auxiliary_sentinels(room: nio.MatrixRoom, label: str):
    expected_tags: dict[str, dict[str, float] | None] = {
        f"u.settlement.{label}": {"order": 0.375}
    }
    room.tags = {f"u.settlement.{label}": {"order": 0.375}}
    expected_typing_users = (BOB,)
    room.typing_users = [BOB]
    expected_fully_read_marker = f"$settlement-{label}-fully-read"
    room.fully_read_marker = expected_fully_read_marker
    expected_presence = (
        "online",
        4242,
        True,
        f"{label} presence sentinel",
    )
    own_user = room.users[ACCOUNT]
    (
        own_user.presence,
        own_user.last_active_ago,
        own_user.currently_active,
        own_user.status_msg,
    ) = expected_presence
    return (
        expected_tags,
        expected_typing_users,
        expected_fully_read_marker,
        expected_presence,
    )


def _assert_owned_room_auxiliary_sentinels(
    client: RecordingClient,
    room: nio.MatrixRoom,
    expected,
) -> None:
    (
        expected_tags,
        expected_typing_users,
        expected_fully_read_marker,
        expected_presence,
    ) = expected
    current_room = client.rooms[ROOM]
    assert current_room is room
    assert current_room.tags == expected_tags
    assert tuple(current_room.typing_users) == expected_typing_users
    assert current_room.fully_read_marker == expected_fully_read_marker
    own_user = current_room.users[ACCOUNT]
    assert (
        own_user.presence,
        own_user.last_active_ago,
        own_user.currently_active,
        own_user.status_msg,
    ) == expected_presence


@pytest.mark.asyncio
async def test_owned_settle_room_lifecycle_overlays_snapshot_then_acks_without_callbacks(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        stage_classic(bootstrap, _local_room_body())
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        pending = session._journal.load_pending_hydrations(limit=2)
        assert len(pending) == 1
        hydrated = normalize_hydration_response(
            pending[0],
            own_user_id=ACCOUNT,
            response_body=hydration_state(),
        )
        assert session._journal.apply_hydration_result(result=hydrated) is not None
        batch = session.next_batch(max_records=1)
        assert batch is not None and len(batch.records) == 1
        record = batch.records[0]
        assert type(record) is EventRecord
        assert record.kind.value == "room_lifecycle"
        assert record.room_id == ROOM

        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        aggregate = loaded[1]
        snapshot = aggregate.room_snapshot
        assert snapshot is not None
        assert snapshot.name == "Local transition room"
        continuity = aggregate.continuity
        snapshot = replace(
            snapshot,
            membership_epoch=snapshot.membership_epoch + 7,
            own_membership="leave",
        )
        aggregate = replace(aggregate, room_snapshot=snapshot)
        _reseal_local_aggregate(session, aggregate)
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            reloaded = session._journal._load_room_aggregate(owner, ROOM)
        assert reloaded is not None and reloaded[1] == aggregate
        assert aggregate.continuity == continuity
        room = client.rooms[ROOM]
        assert (
            _room_snapshot(
                room,
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            == snapshot
        )
        auxiliary_sentinels = _seed_owned_room_auxiliary_sentinels(
            room,
            "lifecycle",
        )
        room.name = "corrupted lifecycle projection"
        room.users[ACCOUNT].display_name = "Corrupted Alice"
        room.users[ACCOUNT].power_level = 88
        room.power_levels.users[ACCOUNT] = 88
        assert (
            _room_snapshot(
                room,
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            != snapshot
        )

        forbidden: list[str] = []
        _install_owned_settlement_callback_tripwires(client, forbidden)

        def forbid_event_parse(
            _event_type: type[Event],
            _source: dict[object, object],
        ) -> object:
            raise AssertionError("room lifecycle settlement parsed an event")

        monkeypatch.setattr(Event, "parse_event", classmethod(forbid_event_parse))
        authenticated: list[object] = []
        real_load_settlement = session._journal._load_batch_settlement

        def observe_settlement(candidate):
            view = real_load_settlement(candidate)
            assert view is not None
            work, room_value = view
            assert work.value == record
            assert work.metadata is None
            assert room_value == aggregate
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            authenticated.append(view)
            return view

        monkeypatch.setattr(
            session._journal,
            "_load_batch_settlement",
            observe_settlement,
        )
        acknowledgements: list[BatchRef] = []
        real_acknowledge = session._journal.acknowledge_batch

        def observe_acknowledgement(ref: BatchRef) -> None:
            assert authenticated
            assert ref == batch.ref
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            _assert_owned_room_auxiliary_sentinels(
                client,
                room,
                auxiliary_sentinels,
            )
            assert (
                _room_snapshot(
                    client.rooms[ROOM],
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == snapshot
            )
            acknowledgements.append(ref)
            real_acknowledge(ref)

        monkeypatch.setattr(
            session._journal,
            "acknowledge_batch",
            observe_acknowledgement,
        )
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        await session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )

        connection.set_trace_callback(None)
        assert len(authenticated) == 1
        assert acknowledgements == [batch.ref]
        assert forbidden == []
        assert client.send_count == 0
        _assert_owned_room_auxiliary_sentinels(
            client,
            room,
            auxiliary_sentinels,
        )
        assert (
            _room_snapshot(
                client.rooms[ROOM],
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            == snapshot
        )
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        begin = normalized.index("BEGIN IMMEDIATE")
        delete = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("DELETE FROM NIOINGESTWORK")
        )
        meta_update = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("UPDATE NIOINGESTMETA")
        )
        commit = normalized.index("COMMIT")
        assert begin < delete < meta_update < commit
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 2
        )
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_settle_terminal_loss_acks_without_state_parse_or_callbacks(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    try:
        stage_classic(bootstrap, _local_room_body())
        result = session._materialize_oldest_frame(
            limits=MaterializerLimits(max_record_canonical_bytes=1)
        )
        assert result.status is MaterializeStatus.MATERIALIZED
        assert session._journal.load_pending_hydrations(limit=2) == ()
        lifecycle = session.next_batch(max_records=1)
        assert lifecycle is not None and len(lifecycle.records) == 1
        lifecycle_record = lifecycle.records[0]
        assert type(lifecycle_record) is EventRecord
        assert lifecycle_record.kind.value == "room_lifecycle"
        session.acknowledge_batch(lifecycle.ref)

        batch = session.next_batch(max_records=1)
        assert batch is not None and len(batch.records) == 1
        loss = batch.records[0]
        assert type(loss) is LossRecord
        assert loss.reason.value == "oversized_event"
        assert loss.room_id == ROOM
        assert loss.membership_epoch == 0
        assert (
            loss.boundary.prior_event_id,
            loss.boundary.prior_origin_server_ts,
            loss.boundary.start_token,
            loss.boundary.target_token,
        ) == (None, None, None, None)

        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        aggregate = loaded[1]
        snapshot = aggregate.room_snapshot
        assert snapshot is not None
        room = client.rooms[ROOM]
        auxiliary_sentinels = _seed_owned_room_auxiliary_sentinels(
            room,
            "loss",
        )
        room.name = "loss must not restore this mutation"
        room.users[ACCOUNT].power_level = 77
        room.power_levels.users[ACCOUNT] = 77
        mutated_snapshot = _room_snapshot(
            room,
            aggregate.continuity.membership_epoch,
            aggregate.continuity.membership,
        )
        assert mutated_snapshot != snapshot

        forbidden: list[str] = []
        _install_owned_settlement_callback_tripwires(client, forbidden)

        def forbid_event_parse(
            _event_type: type[Event],
            _source: dict[object, object],
        ) -> object:
            raise AssertionError("Loss settlement parsed an event")

        monkeypatch.setattr(Event, "parse_event", classmethod(forbid_event_parse))
        authenticated: list[object] = []
        real_load_settlement = session._journal._load_batch_settlement

        def observe_settlement(candidate):
            view = real_load_settlement(candidate)
            assert view is not None
            work, room_value = view
            assert work.value == loss
            assert work.metadata is None
            assert room_value is None
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            authenticated.append(view)
            return view

        monkeypatch.setattr(
            session._journal,
            "_load_batch_settlement",
            observe_settlement,
        )
        acknowledgements: list[BatchRef] = []
        real_acknowledge = session._journal.acknowledge_batch

        def observe_acknowledgement(ref: BatchRef) -> None:
            assert authenticated
            assert ref == batch.ref
            assert connection.in_transaction is False
            assert session._journal._owner._depth == 0
            assert session._journal._owner.database.transaction_depth() == 0
            _assert_owned_room_auxiliary_sentinels(
                client,
                room,
                auxiliary_sentinels,
            )
            assert (
                _room_snapshot(
                    client.rooms[ROOM],
                    aggregate.continuity.membership_epoch,
                    aggregate.continuity.membership,
                )
                == mutated_snapshot
            )
            acknowledgements.append(ref)
            real_acknowledge(ref)

        monkeypatch.setattr(
            session._journal,
            "acknowledge_batch",
            observe_acknowledgement,
        )
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        await session._settle_batch(
            batch,
            receipt_new=True,
            semantic_event_new=False,
        )

        connection.set_trace_callback(None)
        assert len(authenticated) == 1
        assert acknowledgements == [batch.ref]
        assert forbidden == []
        assert client.send_count == 0
        _assert_owned_room_auxiliary_sentinels(
            client,
            room,
            auxiliary_sentinels,
        )
        assert (
            _room_snapshot(
                client.rooms[ROOM],
                aggregate.continuity.membership_epoch,
                aggregate.continuity.membership,
            )
            == mutated_snapshot
        )
        owner_after = session._journal.load_owner()
        with session._journal._owner.read():
            loaded_after = session._journal._load_room_aggregate(owner_after, ROOM)
        assert loaded_after is not None and loaded_after[1] == aggregate
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        begin = normalized.index("BEGIN IMMEDIATE")
        delete = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("DELETE FROM NIOINGESTWORK")
        )
        meta_update = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("UPDATE NIOINGESTMETA")
        )
        commit = normalized.index("COMMIT")
        assert begin < delete < meta_update < commit
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 0
        )
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        await session.close()


async def _open_claimed_owned_live_timeline(tmp_path):
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    warm_frame = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    warm = materialize_journal(bootstrap._journal, limits=MaterializerLimits())
    assert (warm.status, warm.frame_id) == (
        MaterializeStatus.MATERIALIZED,
        warm_frame.frame_id,
    )
    retire_completed_frame(bootstrap._journal)
    assert bootstrap._journal.list_frames(1) == ()
    assert bootstrap._journal.load_pending_hydrations(limit=1) == ()
    assert bootstrap._journal.load_source().cursor_json == canonical_classic_cursor(
        ClassicCursor("s1")
    )
    assert (
        bootstrap._journal._owner.database.connection()
        .execute("SELECT COUNT(*) FROM NioIngestWork")
        .fetchone()[0]
        == 0
    )

    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        event_source = {
            "content": {"name": "Timeline Final"},
            "event_id": "$timeline-final-name",
            "origin_server_ts": 3,
            "sender": ACCOUNT,
            "state_key": "",
            "type": "m.room.name",
        }
        body = _local_room_body()
        body["next_batch"] = "s2"
        rooms = body["rooms"]
        assert type(rooms) is dict
        joined = rooms["join"]
        assert type(joined) is dict
        room_body = joined[ROOM]
        assert type(room_body) is dict
        room_body["timeline"] = {"events": [event_source], "limited": False}
        stage_classic(bootstrap, body)
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        pending = session._journal.load_pending_hydrations(limit=2)
        assert len(pending) == 1
        hydrated = normalize_hydration_response(
            pending[0],
            own_user_id=ACCOUNT,
            response_body=hydration_state(),
        )
        assert session._journal.apply_hydration_result(result=hydrated) is not None

        for expected_kind, expected_event_id in (
            (RecordKind.ROOM_LIFECYCLE, None),
            (RecordKind.STATE, "$local-member"),
            (RecordKind.STATE, "$local-name"),
        ):
            prior = session.next_batch(max_records=1)
            assert prior is not None
            assert len(prior.records) == 1
            prior_record = prior.records[0]
            assert type(prior_record) is EventRecord
            assert (prior_record.kind, prior_record.event_id) == (
                expected_kind,
                expected_event_id,
            )
            session.acknowledge_batch(prior.ref)

        batch = session.next_batch(max_records=1)
        assert batch is not None
        assert len(batch.records) == 1
        record = batch.records[0]
        assert type(record) is EventRecord
        assert (
            record.kind,
            record.event_id,
            record.provenance,
            record.source_json,
            record.clear_json,
        ) == (
            RecordKind.TIMELINE,
            "$timeline-final-name",
            nio.TimelineEventProvenance.LIVE,
            canonical_json(event_source),
            None,
        )
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        aggregate = loaded[1]
        snapshot = aggregate.room_snapshot
        assert snapshot is not None
        assert snapshot.name == "Timeline Final"
        assert (
            _room_snapshot(
                client.rooms[ROOM],
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            == snapshot
        )
        assert (
            session._owned_store.database.connection()
            .execute("SELECT COUNT(*) FROM NioIngestWork")
            .fetchone()[0]
            == 1
        )
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_client.config = replace(
        restored_client.config,
    )
    restored_session = open_owned_session(restored_client, reopened, generation)
    try:
        assert restored_session.next_batch(max_records=1) == batch
        owner = restored_session._journal.load_owner()
        with restored_session._journal._owner.read():
            restored = restored_session._journal._load_room_aggregate(owner, ROOM)
        assert restored is not None
        assert restored[1] == aggregate
        room = restored_client.rooms[ROOM]
        assert type(room) is nio.MatrixRoom
        assert restored_client.invited_rooms == {}
        assert (
            _room_snapshot(
                room,
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            == snapshot
        )
    except BaseException:
        await restored_session.close()
        raise
    return restored_client, restored_session, batch, record, aggregate, snapshot, room


@pytest.mark.parametrize(
    ("receipt_new", "semantic_event_new", "expected_callbacks"),
    (
        (False, False, 0),
        (True, False, 0),
        (True, True, 1),
        (False, True, None),
    ),
    ids=("duplicate", "receipt-only", "semantic-event", "invalid-facts"),
)
@pytest.mark.asyncio
async def test_owned_settle_live_timeline_none_truth_table_restores_snapshot_before_callback(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    receipt_new: bool,
    semantic_event_new: bool,
    expected_callbacks: int | None,
) -> None:
    from nio.client.base_client import _room_snapshot

    (
        client,
        session,
        batch,
        record,
        aggregate,
        snapshot,
        room,
    ) = await _open_claimed_owned_live_timeline(tmp_path)
    connection = session._owned_store.database.connection()
    try:
        room.name = "corrupted timeline projection"
        room.users[ACCOUNT].display_name = "Corrupted Alice"
        room.users[ACCOUNT].power_level = 77
        room.power_levels.users[ACCOUNT] = 77
        mutated_snapshot = _room_snapshot(
            room,
            snapshot.membership_epoch,
            snapshot.own_membership,
        )
        assert mutated_snapshot != snapshot

        def assert_outside_owner_transaction() -> None:
            assert (
                connection.in_transaction,
                session._journal._owner._depth,
                session._journal._owner.database.transaction_depth(),
            ) == (False, 0, 0)

        def assert_snapshot_overlay() -> None:
            assert client.rooms[ROOM] is room
            assert (
                _room_snapshot(
                    room,
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == snapshot
            )

        forbidden: list[str] = []
        authenticated: list[object] = []
        real_load_settlement = session._journal._load_batch_settlement

        def observe_settlement(candidate):
            view = real_load_settlement(candidate)
            assert view is not None
            work, room_value = view
            metadata = work.metadata
            assert work.value == record
            assert metadata is not None
            assert (
                metadata.record_id,
                metadata.preparation_phase,
                metadata.effective_event_type,
                metadata.decryption,
                metadata.decryption_verified,
                metadata.decrypted_to_device_kind,
                metadata.callback_route,
            ) == (
                record.record_id,
                _PreparationPhase.SOURCE,
                "m.room.name",
                _DecryptionDisposition.NONE,
                None,
                None,
                _CallbackRoute.EVENT,
            )
            assert room_value == aggregate
            assert (
                _room_snapshot(
                    room,
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == mutated_snapshot
            )
            assert_outside_owner_transaction()
            authenticated.append(view)
            return view

        monkeypatch.setattr(
            session._journal,
            "_load_batch_settlement",
            observe_settlement,
        )
        parsed: list[object] = []
        real_parse_event = Event.parse_event

        def observe_event_parse(
            _event_type: type[Event],
            source: dict[object, object],
        ) -> object:
            assert len(authenticated) == 1
            assert canonical_json(source) == record.source_json
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            event = real_parse_event(source)
            assert type(event) is nio.RoomNameEvent
            assert event.source["content"] == {"name": "Timeline Final"}
            parsed.append(event)
            return event

        monkeypatch.setattr(Event, "parse_event", classmethod(observe_event_parse))

        def forbid_state_replay(_event: object) -> None:
            raise AssertionError("timeline settlement reapplied durable room state")

        monkeypatch.setattr(room, "handle_event", forbid_state_replay)

        def forbid_live_replay(*_args: object, **_kwargs: object) -> None:
            forbidden.append("live-replay")
            raise AssertionError("timeline settlement entered a live replay path")

        for method_name in (
            "receive_response",
            "_handle_sync",
            "_handle_timeline_event",
            "_on_to_device",
            "_on_global_account_data",
            "_on_ephemeral",
            "_on_room_account_data",
            "_on_presence",
            "_on_invited_rooms",
        ):
            monkeypatch.setattr(client, method_name, forbid_live_replay)
        assert client.olm is not None
        monkeypatch.setattr(client.olm, "_decrypt_megolm_no_error", forbid_live_replay)
        routed: list[object] = []
        callbacks: list[object] = []
        real_on_event = client._on_event

        async def observe_event_route(event: object, candidate_room: object) -> None:
            assert len(parsed) == 1
            assert event is parsed[0]
            assert candidate_room is room
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            routed.append(event)
            await real_on_event(event, candidate_room)  # type: ignore[arg-type]

        monkeypatch.setattr(client, "_on_event", observe_event_route)

        async def observe_event_callback(
            candidate_room: object,
            event: object,
        ) -> None:
            assert len(parsed) == 1
            assert event is parsed[0]
            assert candidate_room is room
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            callbacks.append(event)

        async def forbid_response(*_args: object) -> None:
            forbidden.append("response")
            raise AssertionError("timeline settlement invoked a response callback")

        client.add_event_callback(observe_event_callback, None)
        client.add_response_callback(forbid_response, None)
        acknowledgements: list[BatchRef] = []
        real_acknowledge = session._journal.acknowledge_batch

        def observe_acknowledgement(ref: BatchRef) -> None:
            expected = int(semantic_event_new)
            assert len(parsed) == expected
            assert len(routed) == expected
            assert len(callbacks) == expected
            assert ref == batch.ref
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            acknowledgements.append(ref)
            real_acknowledge(ref)

        monkeypatch.setattr(
            session._journal,
            "acknowledge_batch",
            observe_acknowledgement,
        )
        outstanding = _owned_delivery_frontier(connection)
        graph = tuple(connection.iterdump())
        statements: list[str] = []
        connection.set_trace_callback(statements.append)

        if expected_callbacks is None:
            try:
                with pytest.raises(LocalProtocolError):
                    await session._settle_batch(
                        batch,
                        receipt_new=receipt_new,
                        semantic_event_new=semantic_event_new,
                    )
            finally:
                connection.set_trace_callback(None)
            assert statements == []
            assert (authenticated, parsed, routed, callbacks) == ([], [], [], [])
            assert acknowledgements == []
            assert forbidden == []
            assert client.send_count == 0
            assert _owned_delivery_frontier(connection) == outstanding
            assert tuple(connection.iterdump()) == graph
            assert client.rooms[ROOM] is room
            assert (
                _room_snapshot(
                    room,
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == mutated_snapshot
            )
            client._assert_ingestion_not_poisoned()
            return

        try:
            await session._settle_batch(
                batch,
                receipt_new=receipt_new,
                semantic_event_new=semantic_event_new,
            )
        finally:
            connection.set_trace_callback(None)
        assert len(authenticated) == 1
        assert (len(parsed), len(routed), len(callbacks)) == (
            expected_callbacks,
            expected_callbacks,
            expected_callbacks,
        )
        if expected_callbacks:
            assert routed[0] is parsed[0]
            assert callbacks[0] is parsed[0]
        assert acknowledgements == [batch.ref]
        assert forbidden == []
        assert client.send_count == 0
        assert_snapshot_overlay()
        assert _owned_delivery_frontier(connection) == (
            4,
            batch.ref.sha256,
            None,
            None,
            None,
            None,
        )
        owner_after = session._journal.load_owner()
        with session._journal._owner.read():
            aggregate_after = session._journal._load_room_aggregate(owner_after, ROOM)
        assert aggregate_after is not None
        assert aggregate_after[1] == aggregate
        normalized = tuple(
            statement.strip().rstrip(";").upper() for statement in statements
        )
        begin = normalized.index("BEGIN IMMEDIATE")
        delete = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("DELETE FROM NIOINGESTWORK")
        )
        update = next(
            index
            for index, statement in enumerate(normalized)
            if statement.startswith("UPDATE NIOINGESTMETA")
        )
        commit = normalized.index("COMMIT")
        assert begin < delete < update < commit
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        await session.close()


async def _open_claimed_owned_decrypted_settlement(tmp_path, target: str):
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    warm_frame = stage_classic(bootstrap, {"next_batch": "s1", "rooms": {}})
    warm = materialize_journal(bootstrap._journal, limits=MaterializerLimits())
    assert (warm.status, warm.frame_id) == (
        MaterializeStatus.MATERIALIZED,
        warm_frame.frame_id,
    )
    retire_completed_frame(bootstrap._journal)
    assert bootstrap._journal.list_frames(1) == ()
    assert bootstrap._journal.load_source().cursor_json == canonical_classic_cursor(
        ClassicCursor("s1")
    )

    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    sender_store: SqliteMemoryStore | None = None
    try:
        body, sender_store = _owned_crypto_classic_body(client)
        body["next_batch"] = "s2"
        if target == "megolm-failed":
            rooms = body["rooms"]
            assert type(rooms) is dict
            joined = rooms["join"]
            assert type(joined) is dict
            room_body = joined[ROOM]
            assert type(room_body) is dict
            timeline = room_body["timeline"]
            assert type(timeline) is dict
            events = timeline["events"]
            assert type(events) is list and len(events) == 1
            encrypted = events[0]
            assert type(encrypted) is dict
            encrypted_content = encrypted["content"]
            assert type(encrypted_content) is dict
            missing_content = {
                **encrypted_content,
                "session_id": "missing-session",
            }
            events.append(
                {
                    **encrypted,
                    "content": missing_content,
                    "event_id": "$missing",
                    "origin_server_ts": 4,
                }
            )
        staged = stage_classic(bootstrap, body)
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        assert materialized.frame_id == staged.frame_id
        assert materialized.revision is not None
        pending = session._journal.load_pending_hydrations(limit=2)
        assert len(pending) == 1
        if target in {"timeline", "megolm-failed"}:
            hydrated = normalize_hydration_response(
                pending[0],
                own_user_id=ACCOUNT,
                response_body=hydration_state(),
            )
            assert session._journal.apply_hydration_result(result=hydrated) is not None

        expected_fifo = (
            (RecordKind.TO_DEVICE, None),
            (RecordKind.ROOM_LIFECYCLE, None),
            (RecordKind.STATE, "$encryption"),
            (RecordKind.STATE, f"$member-{ACCOUNT}"),
            (RecordKind.STATE, f"$member-{BOB}"),
            (RecordKind.TIMELINE, "$secret"),
        )
        if target == "megolm-failed":
            expected_fifo += ((RecordKind.TIMELINE, "$missing"),)
        target_index = 0 if target == "room-key" else len(expected_fifo) - 1
        for index, expected in enumerate(expected_fifo[: target_index + 1]):
            claimed = session.next_batch(max_records=1)
            assert claimed is not None
            assert len(claimed.records) == 1
            claimed_record = claimed.records[0]
            assert type(claimed_record) is EventRecord
            assert (claimed_record.kind, claimed_record.event_id) == expected
            if index == target_index:
                batch = claimed
                record = claimed_record
            else:
                session.acknowledge_batch(claimed.ref)

        aggregate = None
        snapshot = None
        if target in {"timeline", "megolm-failed"}:
            assert record.provenance is nio.TimelineEventProvenance.LIVE
            owner = session._journal.load_owner()
            with session._journal._owner.read():
                loaded = session._journal._load_room_aggregate(owner, ROOM)
            assert loaded is not None
            aggregate = loaded[1]
            snapshot = aggregate.room_snapshot
            assert snapshot is not None
            assert (
                _room_snapshot(
                    client.rooms[ROOM],
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == snapshot
            )
        connection = session._owned_store.database.connection()
        e2ee_rows = _raw_e2ee_rows(connection)
        work_count = connection.execute(
            "SELECT COUNT(*) FROM NioIngestWork"
        ).fetchone()[0]
        assert work_count == len(expected_fifo) - target_index
    finally:
        if sender_store is not None:
            sender_store.database.close()
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_client.config = replace(
        restored_client.config,
    )
    restored_session = open_owned_session(restored_client, reopened, generation)
    restored_connection = restored_session._owned_store.database.connection()
    try:
        assert _raw_e2ee_rows(restored_connection) == e2ee_rows
        if target in {"timeline", "megolm-failed"}:
            assert snapshot is not None
            assert (
                _room_snapshot(
                    restored_client.rooms[ROOM],
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == snapshot
            )
        assert restored_session.next_batch(max_records=1) == batch
    except BaseException:
        await restored_session.close()
        raise
    return (
        restored_client,
        restored_session,
        batch,
        record,
        aggregate,
        snapshot,
        e2ee_rows,
        work_count,
    )


@pytest.mark.parametrize("target", ("room-key", "timeline"))
@pytest.mark.asyncio
async def test_owned_settle_reconstructs_decrypted_events_without_crypto_replay(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    from nio.client.base_client import _room_snapshot

    (
        client,
        session,
        batch,
        record,
        aggregate,
        snapshot,
        e2ee_rows,
        work_count,
    ) = await _open_claimed_owned_decrypted_settlement(tmp_path, target)
    connection = session._owned_store.database.connection()
    try:
        assert record.clear_json is not None
        outer = json.loads(record.source_json)
        clear = json.loads(record.clear_json)
        assert canonical_json(outer) == record.source_json
        assert canonical_json(clear) == record.clear_json
        assert outer["type"] == "m.room.encrypted"
        outer_content = outer["content"]
        clear_content = clear["content"]
        assert type(outer_content) is dict
        assert type(clear_content) is dict
        assert type(outer["sender"]) is str
        assert type(outer_content["sender_key"]) is str
        assert type(record.origin) is RecordOrigin
        expected_record_fields = (
            (
                RecordKind.TO_DEVICE,
                0,
                None,
                None,
                None,
                None,
                None,
            )
            if target == "room-key"
            else (
                RecordKind.TIMELINE,
                4,
                ROOM,
                0,
                4,
                "$secret",
                nio.TimelineEventProvenance.LIVE,
            )
        )
        assert (
            record.kind,
            record.origin.frame_index,
            record.room_id,
            record.membership_epoch,
            record.room_sequence,
            record.event_id,
            record.provenance,
        ) == expected_record_fields
        assert (
            record.origin.transport,
            record.origin.source_epoch,
            record.origin.request_id,
        ) == (TransportKind.CLASSIC, 0, 1)
        assert batch.records == (record,)
        outstanding = _owned_delivery_frontier(connection)

        room = None
        mutated_snapshot = None
        if target == "timeline":
            assert aggregate is not None
            assert snapshot is not None
            room = client.rooms[ROOM]
            room.name = "corrupted decrypted timeline projection"
            room.users[ACCOUNT].display_name = "Corrupted Alice"
            room.users[ACCOUNT].power_level = 77
            room.power_levels.users[ACCOUNT] = 77
            mutated_snapshot = _room_snapshot(
                room,
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            assert mutated_snapshot != snapshot

        def assert_outside_owner_transaction() -> None:
            assert (
                connection.in_transaction,
                session._journal._owner._depth,
                session._journal._owner.database.transaction_depth(),
            ) == (False, 0, 0)

        def assert_timeline_overlay() -> None:
            if target != "timeline":
                return
            assert room is not None
            assert snapshot is not None
            assert client.rooms[ROOM] is room
            assert (
                _room_snapshot(
                    room,
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == snapshot
            )

        authenticated: list[object] = []
        real_load_settlement = session._journal._load_batch_settlement

        def observe_settlement(candidate):
            assert candidate == batch
            view = real_load_settlement(candidate)
            assert view is not None
            work, room_value = view
            metadata = work.metadata
            assert work.value == record
            assert metadata is not None
            assert metadata.record_id == record.record_id
            expected_metadata = (
                (
                    "m.room_key",
                    None,
                    _DecryptedToDeviceKind.ROOM_KEY,
                    _CallbackRoute.TO_DEVICE,
                )
                if target == "room-key"
                else ("m.room.message", True, None, _CallbackRoute.EVENT)
            )
            assert (
                metadata.preparation_phase,
                metadata.effective_event_type,
                metadata.decryption,
                metadata.decryption_verified,
                metadata.decrypted_to_device_kind,
                metadata.callback_route,
            ) == (
                _PreparationPhase.SOURCE,
                expected_metadata[0],
                _DecryptionDisposition.DECRYPTED,
                expected_metadata[1],
                expected_metadata[2],
                expected_metadata[3],
            )
            assert room_value == (None if target == "room-key" else aggregate)
            if target == "timeline":
                assert room is not None
                assert snapshot is not None
                assert mutated_snapshot is not None
                assert (
                    _room_snapshot(
                        room,
                        snapshot.membership_epoch,
                        snapshot.own_membership,
                    )
                    == mutated_snapshot
                )
            assert_outside_owner_transaction()
            authenticated.append(view)
            return view

        reconstructed: list[object] = []
        real_room_key_init = RoomKeyEvent.__init__
        real_parse_decrypted = Event.parse_decrypted_event

        def observe_room_key_init(
            event: RoomKeyEvent,
            source: dict[object, object],
            sender: str,
            sender_key: str,
            room_id: str,
            session_id: str,
            algorithm: str,
        ) -> None:
            assert len(authenticated) == 1
            assert source == clear
            assert (sender, sender_key) == (
                outer["sender"],
                outer_content["sender_key"],
            )
            assert (room_id, session_id, algorithm) == (
                clear_content["room_id"],
                clear_content["session_id"],
                clear_content["algorithm"],
            )
            assert_outside_owner_transaction()
            real_room_key_init(
                event,
                source,
                sender,
                sender_key,
                room_id,
                session_id,
                algorithm,
            )
            reconstructed.append(event)

        def observe_decrypted_parse(
            _event_type: type[Event],
            source: dict[object, object],
        ) -> object:
            assert len(authenticated) == 1
            assert source == clear
            assert_timeline_overlay()
            assert_outside_owner_transaction()
            event = real_parse_decrypted(source)
            assert type(event) is nio.RoomMessageText
            assert event.decrypted is False
            assert event.verified is False
            assert event.sender_key is None
            assert event.session_id is None
            reconstructed.append(event)
            return event

        def forbidden_path(*_args: object, **_kwargs: object) -> None:
            forbidden_paths.append("crypto-or-replay")
            raise AssertionError(
                "decrypted settlement replayed live client or crypto state"
            )

        def assert_reconstructed(event: object) -> None:
            assert len(reconstructed) == 1
            assert event is reconstructed[0]
            if target == "room-key":
                assert type(event) is RoomKeyEvent
                assert event.source == clear
                assert (
                    event.sender,
                    event.sender_key,
                    event.room_id,
                    event.session_id,
                    event.algorithm,
                ) == (
                    outer["sender"],
                    outer_content["sender_key"],
                    clear_content["room_id"],
                    clear_content["session_id"],
                    clear_content["algorithm"],
                )
            else:
                assert type(event) is nio.RoomMessageText
                assert event.source == clear
                assert (
                    event.decrypted,
                    event.verified,
                    event.sender_key,
                    event.session_id,
                    event.room_id,
                ) == (
                    True,
                    True,
                    outer_content["sender_key"],
                    outer_content["session_id"],
                    ROOM,
                )

        def assert_work_is_outstanding() -> None:
            assert _owned_delivery_frontier(connection) == outstanding
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == work_count
            )

        forbidden_callbacks: list[str] = []
        forbidden_paths: list[str] = []
        _install_owned_settlement_callback_tripwires(client, forbidden_callbacks)
        callbacks: list[object] = []
        routed: list[object] = []
        if target == "room-key":
            client.to_device_callbacks.clear()
            real_route = client._on_to_device

            async def callback(event: object) -> None:
                assert_reconstructed(event)
                assert_outside_owner_transaction()
                assert_work_is_outstanding()
                assert _raw_e2ee_rows(connection) == e2ee_rows
                callbacks.append(event)

            async def observe_route(event: object) -> None:
                assert_reconstructed(event)
                assert_outside_owner_transaction()
                assert_work_is_outstanding()
                routed.append(event)
                await real_route(event)  # type: ignore[arg-type]

            client.add_to_device_callback(callback, None)
        else:
            client.event_callbacks.clear()
            real_route = client._on_event

            async def callback(candidate_room: object, event: object) -> None:
                assert candidate_room is room
                assert_reconstructed(event)
                assert_timeline_overlay()
                assert_outside_owner_transaction()
                assert_work_is_outstanding()
                assert _raw_e2ee_rows(connection) == e2ee_rows
                callbacks.append(event)

            async def observe_route(event: object, candidate_room: object) -> None:
                assert candidate_room is room
                assert_reconstructed(event)
                assert_timeline_overlay()
                assert_outside_owner_transaction()
                assert_work_is_outstanding()
                routed.append(event)
                await real_route(event, candidate_room)  # type: ignore[arg-type]

            client.add_event_callback(callback, None)

        acknowledgements: list[BatchRef] = []
        real_acknowledge = session._journal.acknowledge_batch

        def observe_acknowledgement(ref: BatchRef) -> None:
            assert ref == batch.ref
            assert len(authenticated) == 1
            assert len(callbacks) == 1
            assert routed == callbacks
            assert_reconstructed(callbacks[0])
            assert_timeline_overlay()
            assert_outside_owner_transaction()
            assert _raw_e2ee_rows(connection) == e2ee_rows
            assert_work_is_outstanding()
            acknowledgements.append(ref)
            real_acknowledge(ref)

        client_forbidden = (
            "receive_response",
            "_handle_sync",
            "_handle_to_device",
            "_handle_decrypt_to_device",
            "_handle_olm_events",
            "_handle_timeline_event",
            "decrypt_event",
            "_on_response",
        )
        olm_forbidden = (
            "handle_to_device_event",
            "decrypt_event",
            "_try_decrypt",
            "_handle_olm_event",
            "_handle_room_key_event",
            "create_group_session",
            "_decrypt_megolm_no_error",
            "decrypt_megolm_event",
            "decrypt",
            "message_index_ok",
            "check_if_wedged",
            "save_session",
            "save_inbound_group_session",
            "save_account",
            "update_tracked_users",
            "add_changed_users",
            "collect_key_requests",
            "clear_verifications",
        )
        store_forbidden = (
            "load_account",
            "load_sessions",
            "load_inbound_group_sessions",
            "load_device_keys",
            "load_encrypted_rooms",
            "load_outgoing_key_requests",
            "save_account",
            "save_session",
            "save_inbound_group_session",
            "save_device_keys",
            "add_outgoing_key_request",
            "remove_outgoing_key_request",
            "save_encrypted_rooms",
            "save_sync_token",
            "delete_encrypted_room",
            "blacklist_device",
            "verify_device",
            "unverify_device",
        )
        assert client.olm is not None
        assert client.store is not None
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with monkeypatch.context() as patch:
            patch.setattr(
                session._journal,
                "_load_batch_settlement",
                observe_settlement,
            )
            patch.setattr(
                session._journal,
                "acknowledge_batch",
                observe_acknowledgement,
            )
            if target == "room-key":
                patch.setattr(RoomKeyEvent, "__init__", observe_room_key_init)
                patch.setattr(RoomKeyEvent, "from_dict", classmethod(forbidden_path))
                patch.setattr(
                    ToDeviceEvent,
                    "parse_event",
                    classmethod(forbidden_path),
                )
                patch.setattr(client, "_on_to_device", observe_route)
            else:
                patch.setattr(
                    Event,
                    "parse_decrypted_event",
                    classmethod(observe_decrypted_parse),
                )
                patch.setattr(Event, "parse_event", classmethod(forbidden_path))
                patch.setattr(client, "_on_event", observe_route)
                assert room is not None
                patch.setattr(room, "handle_event", forbidden_path)
            for name in client_forbidden:
                patch.setattr(client, name, forbidden_path)
            for name in olm_forbidden:
                patch.setattr(client.olm, name, forbidden_path)
            patch.setattr(client.olm.session_store, "get", forbidden_path)
            patch.setattr(client.olm.session_store, "add", forbidden_path)
            patch.setattr(client.olm.inbound_group_store, "get", forbidden_path)
            patch.setattr(client.olm.inbound_group_store, "add", forbidden_path)
            for name in store_forbidden:
                patch.setattr(client.store, name, forbidden_path)
            patch.setattr(client, "_prepare_ingestion_frame", forbidden_path)
            patch.setattr(session, "_materialize_oldest_frame", forbidden_path)
            patch.setattr(
                session._journal,
                "_prepare_and_materialize_oldest_frame",
                forbidden_path,
            )

            await session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=target == "timeline",
            )

        connection.set_trace_callback(None)
        assert len(authenticated) == 1
        assert len(reconstructed) == 1
        assert len(routed) == 1
        assert routed == callbacks
        assert acknowledgements == [batch.ref]
        assert forbidden_callbacks == []
        assert forbidden_paths == []
        assert client.send_count == 0
        assert client.send_calls == []
        assert_timeline_overlay()
        assert _raw_e2ee_rows(connection) == e2ee_rows
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
            == work_count - 1
        )
        assert any(
            statement.lstrip().upper().startswith("DELETE FROM NIOINGESTWORK")
            for statement in statements
        )
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.asyncio
async def test_owned_settle_failed_megolm_without_crypto_or_rerequest_replay(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    from nio.client.base_client import _room_snapshot

    (
        client,
        session,
        batch,
        record,
        aggregate,
        snapshot,
        e2ee_rows,
        work_count,
    ) = await _open_claimed_owned_decrypted_settlement(tmp_path, "megolm-failed")
    connection = session._owned_store.database.connection()
    try:
        assert type(record.origin) is RecordOrigin
        assert (
            record.kind,
            record.origin.transport,
            record.origin.source_epoch,
            record.origin.request_id,
            record.origin.frame_index,
            record.room_id,
            record.membership_epoch,
            record.room_sequence,
            record.event_id,
            record.provenance,
            record.clear_json,
        ) == (
            RecordKind.TIMELINE,
            TransportKind.CLASSIC,
            0,
            1,
            5,
            ROOM,
            0,
            5,
            "$missing",
            nio.TimelineEventProvenance.LIVE,
            None,
        )
        source = json.loads(record.source_json)
        assert canonical_json(source) == record.source_json
        assert source["type"] == "m.room.encrypted"
        source_content = source["content"]
        assert type(source_content) is dict
        assert (
            source_content["algorithm"],
            source_content["session_id"],
        ) == ("m.megolm.v1.aes-sha2", "missing-session")
        assert aggregate is not None
        assert snapshot is not None
        room = client.rooms[ROOM]
        room.name = "corrupted failed-Megolm projection"
        mutated_snapshot = _room_snapshot(
            room,
            snapshot.membership_epoch,
            snapshot.own_membership,
        )
        assert mutated_snapshot != snapshot

        owner = session._journal.load_owner()
        with session._journal._owner.read():
            headers = session._journal._load_authenticated_frame_headers(owner)
            assert len(headers) == 1
            prepared = session._journal._load_prepared_frame_with_owner(
                headers[0].frame_id,
                owner,
            )
        assert prepared is not None
        assert tuple(
            (operation.kind, operation.state, operation.context)
            for operation in prepared.outbound_maintenance.operations
        ) == (
            ("key_upload", "pending", None),
            ("key_query", "pending", None),
            (
                "to_device",
                "pending",
                {
                    "algorithm": "m.megolm.v1.aes-sha2",
                    "request_id": "missing-session",
                    "room_id": ROOM,
                    "session_id": "missing-session",
                    "subtype": "room_key_request",
                },
            ),
        )
        frame_rows = tuple(
            connection.execute("SELECT * FROM NioIngestFrame ORDER BY rowid")
        )
        outstanding = _owned_delivery_frontier(connection)
        assert work_count == 1
        assert client.olm is not None
        assert client.store is not None

        def crypto_queue_state() -> tuple[object, ...]:
            olm = client.olm
            assert olm is not None
            return (
                tuple(sorted(olm.outgoing_key_requests)),
                tuple(olm.outgoing_to_device_messages),
                tuple(olm.wedged_devices),
                tuple(
                    (key, tuple(events))
                    for key, events in sorted(olm.key_re_requests_events.items())
                ),
                tuple(sorted(olm.received_key_requests)),
                tuple(sorted(olm.key_request_from_untrusted)),
                tuple(olm.key_request_devices_no_session),
                tuple(
                    (key, tuple(sorted(events)))
                    for key, events in sorted(
                        olm.key_requests_waiting_for_session.items()
                    )
                ),
                tuple(sorted(olm.users_for_key_query)),
            )

        queues = crypto_queue_state()
        assert queues == ((), (), (), (), (), (), (), (), ())

        def assert_outside_owner_transaction() -> None:
            assert (
                connection.in_transaction,
                session._journal._owner._depth,
                session._journal._owner.database.transaction_depth(),
            ) == (False, 0, 0)

        def assert_snapshot_overlay() -> None:
            assert client.rooms[ROOM] is room
            assert (
                _room_snapshot(
                    room,
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == snapshot
            )

        def assert_outstanding_and_stable() -> None:
            assert _owned_delivery_frontier(connection) == outstanding
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == work_count
            )
            assert _raw_e2ee_rows(connection) == e2ee_rows
            assert crypto_queue_state() == queues
            assert (
                tuple(connection.execute("SELECT * FROM NioIngestFrame ORDER BY rowid"))
                == frame_rows
            )

        authenticated: list[object] = []
        real_load_settlement = session._journal._load_batch_settlement

        def observe_settlement(candidate):
            assert candidate == batch
            view = real_load_settlement(candidate)
            assert view is not None
            work, room_value = view
            metadata = work.metadata
            assert work.value == record
            assert metadata is not None
            assert (
                metadata.record_id,
                metadata.preparation_phase,
                metadata.effective_event_type,
                metadata.decryption,
                metadata.decryption_verified,
                metadata.decrypted_to_device_kind,
                metadata.callback_route,
            ) == (
                record.record_id,
                _PreparationPhase.SOURCE,
                "m.room.encrypted",
                _DecryptionDisposition.MEGOLM_FAILED,
                None,
                None,
                _CallbackRoute.EVENT,
            )
            assert room_value == aggregate
            assert (
                _room_snapshot(
                    room,
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == mutated_snapshot
            )
            assert_outside_owner_transaction()
            assert_outstanding_and_stable()
            authenticated.append(view)
            return view

        parsed: list[MegolmEvent] = []
        real_parse_event = Event.parse_event

        def observe_parse_event(
            _event_type: type[Event],
            event_source: dict[object, object],
        ) -> object:
            assert len(authenticated) == 1
            assert event_source == source
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            assert_outstanding_and_stable()
            event = real_parse_event(event_source)
            assert type(event) is MegolmEvent
            assert event.room_id is None
            parsed.append(event)
            return event

        forbidden_callbacks: list[str] = []
        forbidden_paths: list[str] = []
        _install_owned_settlement_callback_tripwires(client, forbidden_callbacks)

        def forbidden_path(*_args: object, **_kwargs: object) -> None:
            forbidden_paths.append("crypto-or-replay")
            raise AssertionError("failed Megolm settlement replayed live crypto")

        routed: list[object] = []
        callbacks: list[object] = []
        real_route = client._on_event

        def assert_reconstructed(event: object) -> None:
            assert len(parsed) == 1
            assert event is parsed[0]
            assert type(event) is MegolmEvent
            assert event.source == source
            assert event.room_id == ROOM
            assert event.sender_key == source_content["sender_key"]
            assert event.session_id == "missing-session"

        async def observe_route(event: object, candidate_room: object) -> None:
            assert candidate_room is room
            assert_reconstructed(event)
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            assert_outstanding_and_stable()
            routed.append(event)
            await real_route(event, candidate_room)  # type: ignore[arg-type]

        async def callback(candidate_room: object, event: object) -> None:
            assert candidate_room is room
            assert_reconstructed(event)
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            assert_outstanding_and_stable()
            callbacks.append(event)

        client.event_callbacks.clear()
        client.add_event_callback(callback, None)

        acknowledgements: list[BatchRef] = []
        real_acknowledge = session._journal.acknowledge_batch

        def observe_acknowledgement(ref: BatchRef) -> None:
            assert ref == batch.ref
            assert len(authenticated) == 1
            assert routed == callbacks == parsed
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            assert_outstanding_and_stable()
            acknowledgements.append(ref)
            real_acknowledge(ref)

        client_forbidden = (
            "receive_response",
            "_handle_sync",
            "_handle_timeline_event",
            "_handle_decrypt_to_device",
            "decrypt_event",
            "_prepare_ingestion_frame",
            "_on_response",
        )
        olm_forbidden = (
            "_decrypt_megolm_no_error",
            "decrypt_megolm_event",
            "decrypt_event",
            "check_if_wedged",
            "message_index_ok",
            "collect_key_requests",
            "save_session",
            "save_inbound_group_session",
            "save_account",
        )
        store_forbidden = (
            "load_account",
            "load_sessions",
            "load_inbound_group_sessions",
            "load_device_keys",
            "load_outgoing_key_requests",
            "add_outgoing_key_request",
            "remove_outgoing_key_request",
            "save_session",
            "save_inbound_group_session",
            "save_account",
        )
        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with monkeypatch.context() as patch:
            patch.setattr(
                session._journal,
                "_load_batch_settlement",
                observe_settlement,
            )
            patch.setattr(
                session._journal,
                "acknowledge_batch",
                observe_acknowledgement,
            )
            patch.setattr(Event, "parse_event", classmethod(observe_parse_event))
            patch.setattr(Event, "parse_decrypted_event", classmethod(forbidden_path))
            patch.setattr(client, "_on_event", observe_route)
            patch.setattr(room, "handle_event", forbidden_path)
            for name in client_forbidden:
                patch.setattr(client, name, forbidden_path)
            for name in olm_forbidden:
                patch.setattr(client.olm, name, forbidden_path)
            patch.setattr(client.olm.session_store, "get", forbidden_path)
            patch.setattr(client.olm.inbound_group_store, "get", forbidden_path)
            for name in store_forbidden:
                patch.setattr(client.store, name, forbidden_path)
            patch.setattr(session, "_materialize_oldest_frame", forbidden_path)
            patch.setattr(
                session._journal,
                "_prepare_and_materialize_oldest_frame",
                forbidden_path,
            )

            await session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=True,
            )

        connection.set_trace_callback(None)
        assert len(authenticated) == 1
        assert len(parsed) == 1
        assert routed == callbacks == parsed
        assert acknowledgements == [batch.ref]
        assert forbidden_callbacks == []
        assert forbidden_paths == []
        assert client.send_count == 0
        assert client.send_calls == []
        assert_snapshot_overlay()
        assert _raw_e2ee_rows(connection) == e2ee_rows
        assert crypto_queue_state() == queues
        assert (
            tuple(connection.execute("SELECT * FROM NioIngestFrame ORDER BY rowid"))
            == frame_rows
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 0
        )
        owner_after = session._journal.load_owner()
        with session._journal._owner.read():
            prepared_after = session._journal._load_prepared_frame_with_owner(
                headers[0].frame_id,
                owner_after,
            )
        assert prepared_after == prepared
        journal_dml = tuple(
            statement.lstrip().upper()
            for statement in statements
            if statement.lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))
        )
        assert len(journal_dml) == 2
        assert journal_dml[0].startswith("DELETE FROM NIOINGESTWORK")
        assert journal_dml[1].startswith("UPDATE NIOINGESTMETA")
        client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        await session.close()


@pytest.mark.parametrize("target", ["invite", "joined-no-route"])
@pytest.mark.asyncio
async def test_owned_settle_state_uses_invite_parser_or_snapshot_only(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
    target: str,
) -> None:
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        if target == "invite":
            body = {
                "next_batch": "state-invite-s1",
                "rooms": {
                    "invite": {
                        ROOM: {
                            "invite_state": {
                                "events": [
                                    {
                                        "content": {"name": "Final Invite Name"},
                                        "sender": BOB,
                                        "state_key": "",
                                        "type": "m.room.name",
                                    },
                                    {
                                        "content": {
                                            "displayname": "Older Invite Alice",
                                            "membership": "invite",
                                        },
                                        "sender": BOB,
                                        "state_key": ACCOUNT,
                                        "type": "m.room.member",
                                    },
                                    {
                                        "content": {
                                            "displayname": "Final Invite Alice",
                                            "membership": "invite",
                                        },
                                        "sender": BOB,
                                        "state_key": ACCOUNT,
                                        "type": "m.room.member",
                                    },
                                ]
                            }
                        }
                    }
                },
            }
        else:
            body = _local_room_body()
        stage_classic(bootstrap, body)
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        if target == "joined-no-route":
            pending = session._journal.load_pending_hydrations(limit=2)
            assert len(pending) == 1
            hydrated = normalize_hydration_response(
                pending[0],
                own_user_id=ACCOUNT,
                response_body=hydration_state(),
            )
            assert session._journal.apply_hydration_result(result=hydrated) is not None

        expected_sources = (
            ("m.room.name", "room_lifecycle", "m.room.member")
            if target == "invite"
            else ("room_lifecycle", "m.room.member", "m.room.name")
        )
        for index, expected_type in enumerate(expected_sources):
            claimed = session.next_batch(max_records=1)
            assert claimed is not None
            assert len(claimed.records) == 1
            claimed_record = claimed.records[0]
            if expected_type == "room_lifecycle":
                assert claimed_record.kind is RecordKind.ROOM_LIFECYCLE
            else:
                assert type(claimed_record) is EventRecord
                assert claimed_record.kind is RecordKind.STATE
                assert json.loads(claimed_record.source_json)["type"] == expected_type
            if index == len(expected_sources) - 1:
                batch = claimed
                record = claimed_record
            else:
                session.acknowledge_batch(claimed.ref)

        assert type(record) is EventRecord
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        aggregate = loaded[1]
        snapshot = aggregate.room_snapshot
        assert snapshot is not None
        expected_membership = "invite" if target == "invite" else "join"
        assert aggregate.continuity.membership == expected_membership
        connection = session._owned_store.database.connection()
        work_count = connection.execute(
            "SELECT COUNT(*) FROM NioIngestWork"
        ).fetchone()[0]
        assert work_count == (2 if target == "invite" else 1)
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_client.config = replace(
        restored_client.config,
    )
    restored_session = open_owned_session(restored_client, reopened, generation)
    connection = restored_session._owned_store.database.connection()
    try:
        assert restored_session.next_batch(max_records=1) == batch
        room = (
            restored_client.invited_rooms[ROOM]
            if target == "invite"
            else restored_client.rooms[ROOM]
        )
        assert (
            _room_snapshot(
                room,
                snapshot.membership_epoch,
                snapshot.own_membership,
            )
            == snapshot
        )
        if target == "invite":
            assert type(room) is nio.MatrixInvitedRoom
            assert room.inviter is None
        else:
            assert type(room) is nio.MatrixRoom
        room.name = "corrupted STATE projection"
        room.users[ACCOUNT].display_name = "Corrupted STATE Alice"
        room.users[ACCOUNT].power_level = 88
        room.power_levels.users[ACCOUNT] = 88
        mutated_snapshot = _room_snapshot(
            room,
            snapshot.membership_epoch,
            snapshot.own_membership,
        )
        assert mutated_snapshot != snapshot

        expected_record = (
            (
                "m.room.member",
                None,
                1,
                2,
                _CallbackRoute.EVENT,
            )
            if target == "invite"
            else (
                "m.room.name",
                "$local-name",
                1,
                2,
                None,
            )
        )
        assert type(record.origin) is RecordOrigin
        assert (
            json.loads(record.source_json)["type"],
            record.event_id,
            record.origin.frame_index,
            record.room_sequence,
        ) == expected_record[:4]
        assert (
            record.origin.transport,
            record.origin.source_epoch,
            record.origin.request_id,
            record.room_id,
            record.membership_epoch,
            record.provenance,
            record.clear_json,
        ) == (TransportKind.CLASSIC, 0, 0, ROOM, 0, None, None)
        source = json.loads(record.source_json)
        assert canonical_json(source) == record.source_json
        outstanding = _owned_delivery_frontier(connection)

        def assert_outside_owner_transaction() -> None:
            assert (
                connection.in_transaction,
                restored_session._journal._owner._depth,
                restored_session._journal._owner.database.transaction_depth(),
            ) == (False, 0, 0)

        def assert_snapshot_overlay() -> None:
            selected = (
                restored_client.invited_rooms[ROOM]
                if target == "invite"
                else restored_client.rooms[ROOM]
            )
            assert selected is room
            assert (
                _room_snapshot(
                    room,
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == snapshot
            )

        def assert_outstanding() -> None:
            assert _owned_delivery_frontier(connection) == outstanding
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == work_count
            )

        authenticated: list[object] = []
        real_load_settlement = restored_session._journal._load_batch_settlement

        def observe_settlement(candidate):
            assert candidate == batch
            view = real_load_settlement(candidate)
            assert view is not None
            work, room_value = view
            metadata = work.metadata
            assert work.value == record
            assert metadata is not None
            assert (
                metadata.record_id,
                metadata.preparation_phase,
                metadata.effective_event_type,
                metadata.decryption,
                metadata.decryption_verified,
                metadata.decrypted_to_device_kind,
                metadata.callback_route,
            ) == (
                record.record_id,
                _PreparationPhase.SOURCE,
                expected_record[0],
                _DecryptionDisposition.NONE,
                None,
                None,
                expected_record[4],
            )
            assert room_value == aggregate
            assert (
                _room_snapshot(
                    room,
                    snapshot.membership_epoch,
                    snapshot.own_membership,
                )
                == mutated_snapshot
            )
            assert_outside_owner_transaction()
            assert_outstanding()
            authenticated.append(view)
            return view

        parsed: list[object] = []
        handled: list[object] = []
        real_invite_parse = InviteEvent.parse_event
        real_handle_event = room.handle_event

        def forbidden_path(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("STATE settlement guessed or replayed a parser path")

        def observe_invite_parse(
            _event_type: type[InviteEvent],
            event_source: dict[object, object],
        ) -> object:
            assert target == "invite"
            assert len(authenticated) == 1
            assert event_source == source
            assert_snapshot_overlay()
            assert room.inviter is None
            assert_outside_owner_transaction()
            assert_outstanding()
            event = real_invite_parse(event_source)
            assert type(event) is nio.InviteMemberEvent
            parsed.append(event)
            return event

        def observe_handle_event(event: object) -> None:
            assert target == "invite"
            assert parsed == [event]
            assert_snapshot_overlay()
            assert room.inviter is None
            assert_outside_owner_transaction()
            assert_outstanding()
            real_handle_event(event)  # type: ignore[arg-type]
            assert room.inviter == BOB
            assert room.users[ACCOUNT].display_name == "Older Invite Alice"
            handled.append(event)

        forbidden_callbacks: list[str] = []
        _install_owned_settlement_callback_tripwires(
            restored_client,
            forbidden_callbacks,
        )
        routed: list[object] = []
        callbacks: list[object] = []
        if target == "invite":
            restored_client.event_callbacks.clear()
            real_route = restored_client._on_invited_rooms

            async def callback(candidate_room: object, event: object) -> None:
                assert candidate_room is room
                assert parsed == handled == [event]
                assert room.inviter == BOB
                assert_snapshot_overlay()
                assert room.users[ACCOUNT].display_name == "Final Invite Alice"
                assert_outside_owner_transaction()
                assert_outstanding()
                callbacks.append(event)

            async def observe_route(event: object, candidate_room: object) -> None:
                assert candidate_room is room
                assert parsed == handled == [event]
                assert room.inviter == BOB
                assert_snapshot_overlay()
                assert_outside_owner_transaction()
                assert_outstanding()
                routed.append(event)
                await real_route(event, candidate_room)  # type: ignore[arg-type]

            restored_client.add_event_callback(callback, None)

        acknowledgements: list[BatchRef] = []
        real_acknowledge = restored_session._journal.acknowledge_batch

        def observe_acknowledgement(ref: BatchRef) -> None:
            assert ref == batch.ref
            assert len(authenticated) == 1
            expected_count = int(target == "invite")
            assert len(parsed) == len(handled) == expected_count
            assert len(routed) == len(callbacks) == expected_count
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            assert_outstanding()
            acknowledgements.append(ref)
            real_acknowledge(ref)

        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with monkeypatch.context() as patch:
            patch.setattr(
                restored_session._journal,
                "_load_batch_settlement",
                observe_settlement,
            )
            patch.setattr(
                restored_session._journal,
                "acknowledge_batch",
                observe_acknowledgement,
            )
            patch.setattr(Event, "parse_event", classmethod(forbidden_path))
            if target == "invite":
                patch.setattr(
                    InviteEvent,
                    "parse_event",
                    classmethod(observe_invite_parse),
                )
                patch.setattr(room, "handle_event", observe_handle_event)
                patch.setattr(restored_client, "_on_invited_rooms", observe_route)
            else:
                patch.setattr(
                    InviteEvent,
                    "parse_event",
                    classmethod(forbidden_path),
                )
                patch.setattr(room, "handle_event", forbidden_path)

            live_paths = (
                "receive_response",
                "_handle_sync",
                "_handle_invited_rooms",
                "_handle_joined_rooms",
                "_handle_joined_state",
                "_handle_timeline_event",
                "_prepare_ingestion_frame",
                "_on_event",
                "_on_ephemeral",
                "_on_room_account_data",
                "_on_presence",
                "_on_global_account_data",
                "_on_to_device",
                "_on_response",
            )
            for name in live_paths:
                patch.setattr(restored_client, name, forbidden_path)
            if target != "invite":
                patch.setattr(restored_client, "_on_invited_rooms", forbidden_path)
            patch.setattr(restored_session, "_materialize_oldest_frame", forbidden_path)
            patch.setattr(
                restored_session._journal,
                "_prepare_and_materialize_oldest_frame",
                forbidden_path,
            )

            await restored_session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )

        connection.set_trace_callback(None)
        expected_count = int(target == "invite")
        assert len(authenticated) == 1
        assert len(parsed) == len(handled) == expected_count
        assert len(routed) == len(callbacks) == expected_count
        assert acknowledgements == [batch.ref]
        assert forbidden_callbacks == []
        assert_snapshot_overlay()
        if target == "invite":
            assert room.inviter == BOB
        assert restored_client.send_count == 0
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
            == work_count - 1
        )
        restored_client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        await restored_session.close()


@pytest.mark.parametrize("target", ["ephemeral", "room-account-data"])
@pytest.mark.asyncio
async def test_owned_settle_auxiliary_state_before_receipt_callback(
    tmp_path, monkeypatch: pytest.MonkeyPatch, target: str
) -> None:
    from nio.client.base_client import _room_snapshot

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    try:
        assert client.olm is not None
        body = _parity_response(
            "classic",
            None,
            nonempty=True,
            one_time_key_count=client.olm.account.max_one_time_keys,
        )
        to_device = body["to_device"]
        account_data = body["account_data"]
        rooms = body["rooms"]
        assert type(to_device) is dict
        assert type(account_data) is dict
        assert type(rooms) is dict
        to_device["events"] = []
        account_data["events"] = []
        joined = rooms["join"]
        assert type(joined) is dict
        room_body = joined[ROOM]
        assert type(room_body) is dict
        timeline = room_body["timeline"]
        state = room_body["state"]
        assert type(timeline) is dict
        assert type(state) is dict
        timeline["events"] = []
        state_events = state["events"]
        assert type(state_events) is list
        state_events.append(
            {
                "content": {"displayname": "Bob", "membership": "join"},
                "event_id": "$aux-bob",
                "origin_server_ts": 2,
                "sender": BOB,
                "state_key": BOB,
                "type": "m.room.member",
            }
        )
        stage_classic(bootstrap, body)
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        pending = session._journal.load_pending_hydrations(limit=2)
        assert len(pending) == 1
        hydrated = normalize_hydration_response(
            pending[0], own_user_id=ACCOUNT, response_body=hydration_state()
        )
        assert session._journal.apply_hydration_result(result=hydrated) is not None
        fifo = (
            RecordKind.ROOM_LIFECYCLE,
            RecordKind.STATE,
            RecordKind.STATE,
            RecordKind.EPHEMERAL,
            RecordKind.ROOM_ACCOUNT_DATA,
        )
        target_kind = {
            "ephemeral": RecordKind.EPHEMERAL,
            "room-account-data": RecordKind.ROOM_ACCOUNT_DATA,
        }[target]
        target_index = fifo.index(target_kind)
        for index, expected_kind in enumerate(fifo[: target_index + 1]):
            claimed = session.next_batch(max_records=1)
            assert claimed is not None
            assert len(claimed.records) == 1
            assert claimed.records[0].kind is expected_kind
            if index == target_index:
                batch = claimed
                record = claimed.records[0]
            else:
                session.acknowledge_batch(claimed.ref)
        assert type(record) is EventRecord
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            loaded = session._journal._load_room_aggregate(owner, ROOM)
        assert loaded is not None
        aggregate = loaded[1]
        snapshot = aggregate.room_snapshot
        assert snapshot is not None
        connection = session._owned_store.database.connection()
        work_count = connection.execute(
            "SELECT COUNT(*) FROM NioIngestWork"
        ).fetchone()[0]
    finally:
        await session.close()
    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_client.config = replace(restored_client.config)
    restored_session = open_owned_session(restored_client, reopened, generation)
    connection = restored_session._owned_store.database.connection()
    try:
        assert restored_session.next_batch(max_records=1) == batch
        room = restored_client.rooms[ROOM]
        assert type(room) is nio.MatrixRoom
        assert (
            _room_snapshot(room, snapshot.membership_epoch, snapshot.own_membership)
            == snapshot
        )
        assert BOB in room.users
        room.name = "corrupted auxiliary snapshot"
        room.users[ACCOUNT].display_name = "Corrupted auxiliary Alice"
        room.fully_read_marker = "$aux-keep"
        room.tags = {"u.keep": {"order": 0.25}}
        room.typing_users = [ACCOUNT]
        alice_presence = ("unavailable", 91, False, "Alice preserved")
        (
            room.users[ACCOUNT].presence,
            room.users[ACCOUNT].last_active_ago,
            room.users[ACCOUNT].currently_active,
            room.users[ACCOUNT].status_msg,
        ) = alice_presence
        mutated_snapshot = _room_snapshot(
            room, snapshot.membership_epoch, snapshot.own_membership
        )
        assert mutated_snapshot != snapshot
        expected_metadata = {
            "ephemeral": ("m.receipt", _CallbackRoute.EPHEMERAL, ROOM, 0, 3),
            "room-account-data": (
                "m.tag",
                _CallbackRoute.ROOM_ACCOUNT_DATA,
                ROOM,
                0,
                4,
            ),
        }[target]
        assert (
            record.kind,
            record.room_id,
            record.membership_epoch,
            record.room_sequence,
            record.event_id,
            record.provenance,
            record.clear_json,
        ) == (
            target_kind,
            expected_metadata[2],
            expected_metadata[3],
            expected_metadata[4],
            None,
            None,
            None,
        )
        source = json.loads(record.source_json)
        assert canonical_json(source) == record.source_json
        outstanding = _owned_delivery_frontier(connection)

        def assert_outside_owner_transaction() -> None:
            assert (
                connection.in_transaction,
                restored_session._journal._owner._depth,
                restored_session._journal._owner.database.transaction_depth(),
            ) == (False, 0, 0)

        def assert_snapshot_overlay() -> None:
            assert restored_client.rooms[ROOM] is room
            assert (
                _room_snapshot(room, snapshot.membership_epoch, snapshot.own_membership)
                == snapshot
            )

        def assert_outstanding() -> None:
            assert _owned_delivery_frontier(connection) == outstanding
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == work_count
            )

        def assert_preserved_auxiliary() -> None:
            assert room.fully_read_marker == "$aux-keep"
            assert (
                room.users[ACCOUNT].presence,
                room.users[ACCOUNT].last_active_ago,
                room.users[ACCOUNT].currently_active,
                room.users[ACCOUNT].status_msg,
            ) == alice_presence
            assert room.typing_users == [ACCOUNT]
            if target != "room-account-data":
                assert room.tags == {"u.keep": {"order": 0.25}}

        def assert_applied() -> None:
            if target == "ephemeral":
                assert room.read_receipts[BOB].event_id == "$parity-message"
            else:
                assert room.tags == {"u.work": {"order": 1}}
            assert_preserved_auxiliary()

        authenticated: list[object] = []
        real_load_settlement = restored_session._journal._load_batch_settlement

        def observe_settlement(candidate):
            assert candidate == batch
            view = real_load_settlement(candidate)
            assert view is not None
            work, room_value = view
            metadata = work.metadata
            assert work.value == record
            assert metadata is not None
            assert (
                metadata.record_id,
                metadata.preparation_phase,
                metadata.effective_event_type,
                metadata.decryption,
                metadata.decryption_verified,
                metadata.decrypted_to_device_kind,
                metadata.callback_route,
            ) == (
                record.record_id,
                _PreparationPhase.SOURCE,
                expected_metadata[0],
                _DecryptionDisposition.NONE,
                None,
                None,
                expected_metadata[1],
            )
            assert room_value == aggregate
            assert (
                _room_snapshot(room, snapshot.membership_epoch, snapshot.own_membership)
                == mutated_snapshot
            )
            assert_outside_owner_transaction()
            assert_outstanding()
            authenticated.append(view)
            return view

        parsed: list[object] = []
        applied: list[object] = []
        real_ephemeral_parse = EphemeralEvent.parse_event
        real_account_parse = AccountDataEvent.parse_event
        real_handle_ephemeral = room.handle_ephemeral_event
        real_handle_account = room.handle_account_data

        def forbidden_path(*_args: object, **_kwargs: object) -> None:
            raise AssertionError("auxiliary settlement replayed a wrong path")

        def observe_parse(
            _event_type: type[object], event_source: dict[object, object]
        ) -> object:
            assert len(authenticated) == 1
            assert event_source == source
            assert_snapshot_overlay()
            assert_outside_owner_transaction()
            assert_outstanding()
            if target == "ephemeral":
                event = real_ephemeral_parse(event_source)
                assert type(event) is nio.ReceiptEvent
            else:
                event = real_account_parse(event_source)
                assert type(event) is nio.TagEvent
            parsed.append(event)
            return event

        def observe_handle(event: object) -> None:
            assert parsed == [event]
            assert_snapshot_overlay()
            assert_preserved_auxiliary()
            assert_outside_owner_transaction()
            assert_outstanding()
            if target == "ephemeral":
                real_handle_ephemeral(event)
            else:
                assert target == "room-account-data"
                real_handle_account(event)
            applied.append(event)
            assert_applied()

        forbidden_callbacks: list[str] = []
        _install_owned_settlement_callback_tripwires(
            restored_client, forbidden_callbacks
        )
        routed: list[object] = []
        callbacks: list[object] = []
        if target == "ephemeral":
            restored_client.ephemeral_callbacks.clear()
            real_route = restored_client._on_ephemeral

            async def callback(candidate_room: object, event: object) -> None:
                assert candidate_room is room
                assert parsed == applied == [event]
                assert_applied()
                assert_outside_owner_transaction()
                assert_outstanding()
                callbacks.append(event)

            async def observe_route(event: object, candidate_room: object) -> None:
                assert candidate_room is room
                assert parsed == applied == [event]
                assert_applied()
                assert_outside_owner_transaction()
                assert_outstanding()
                routed.append(event)
                await real_route(event, candidate_room)

            restored_client.add_ephemeral_callback(callback, None)
        else:
            restored_client.room_account_data_callbacks.clear()
            real_route = restored_client._on_room_account_data

            async def callback(candidate_room: object, event: object) -> None:
                assert candidate_room is room
                assert parsed == applied == [event]
                assert_applied()
                assert_outside_owner_transaction()
                assert_outstanding()
                callbacks.append(event)

            async def observe_route(event: object, candidate_room: object) -> None:
                assert candidate_room is room
                assert parsed == applied == [event]
                assert_applied()
                assert_outside_owner_transaction()
                assert_outstanding()
                routed.append(event)
                await real_route(event, candidate_room)

            restored_client.add_room_account_data_callback(callback, None)
        acknowledgements: list[BatchRef] = []
        real_acknowledge = restored_session._journal.acknowledge_batch

        def observe_acknowledgement(ref: BatchRef) -> None:
            assert ref == batch.ref
            assert len(authenticated) == 1
            assert routed == callbacks == parsed
            assert applied == parsed
            assert_snapshot_overlay()
            assert_applied()
            assert_outside_owner_transaction()
            assert_outstanding()
            acknowledgements.append(ref)
            real_acknowledge(ref)

        statements: list[str] = []
        connection.set_trace_callback(statements.append)
        with monkeypatch.context() as patch:
            patch.setattr(
                restored_session._journal, "_load_batch_settlement", observe_settlement
            )
            patch.setattr(
                restored_session._journal, "acknowledge_batch", observe_acknowledgement
            )
            patch.setattr(Event, "parse_event", classmethod(forbidden_path))
            patch.setattr(InviteEvent, "parse_event", classmethod(forbidden_path))
            patch.setattr(room, "handle_event", forbidden_path)
            if target == "ephemeral":
                patch.setattr(EphemeralEvent, "parse_event", classmethod(observe_parse))
                patch.setattr(PresenceEvent, "from_dict", classmethod(forbidden_path))
                patch.setattr(
                    AccountDataEvent, "parse_event", classmethod(forbidden_path)
                )
                patch.setattr(room, "handle_ephemeral_event", observe_handle)
                patch.setattr(restored_client, "_on_ephemeral", observe_route)
            else:
                patch.setattr(
                    AccountDataEvent, "parse_event", classmethod(observe_parse)
                )
                patch.setattr(PresenceEvent, "from_dict", classmethod(forbidden_path))
                patch.setattr(
                    EphemeralEvent, "parse_event", classmethod(forbidden_path)
                )
                patch.setattr(room, "handle_account_data", observe_handle)
                patch.setattr(restored_client, "_on_room_account_data", observe_route)
            live_paths = (
                "receive_response",
                "_handle_sync",
                "_handle_invited_rooms",
                "_handle_joined_rooms",
                "_handle_joined_state",
                "_handle_timeline_event",
                "_prepare_ingestion_frame",
                "_on_event",
                "_on_invited_rooms",
                "_on_to_device",
                "_on_global_account_data",
                "_on_response",
            )
            for name in live_paths:
                patch.setattr(restored_client, name, forbidden_path)
            expected_route = {
                "ephemeral": "_on_ephemeral",
                "room-account-data": "_on_room_account_data",
            }[target]
            for name in ("_on_presence", "_on_ephemeral", "_on_room_account_data"):
                if name != expected_route:
                    patch.setattr(restored_client, name, forbidden_path)
            if target != "ephemeral":
                patch.setattr(room, "handle_ephemeral_event", forbidden_path)
            if target != "room-account-data":
                patch.setattr(room, "handle_account_data", forbidden_path)
            patch.setattr(restored_session, "_materialize_oldest_frame", forbidden_path)
            patch.setattr(
                restored_session._journal,
                "_prepare_and_materialize_oldest_frame",
                forbidden_path,
            )
            await restored_session._settle_batch(
                batch, receipt_new=True, semantic_event_new=False
            )
        connection.set_trace_callback(None)
        assert len(authenticated) == 1
        assert len(parsed) == len(routed) == len(callbacks) == 1
        assert acknowledgements == [batch.ref]
        assert forbidden_callbacks == []
        assert_snapshot_overlay()
        assert_applied()
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
            == work_count - 1
        )
        journal_dml = tuple(
            (
                statement.lstrip().upper()
                for statement in statements
                if statement.lstrip()
                .upper()
                .startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))
            )
        )
        assert len(journal_dml) == 2
        assert journal_dml[0].startswith("DELETE FROM NIOINGESTWORK")
        assert journal_dml[1].startswith("UPDATE NIOINGESTMETA")
        assert restored_client.send_count == 0
        restored_client._assert_ingestion_not_poisoned()
    finally:
        connection.set_trace_callback(None)
        await restored_session.close()


@pytest.mark.asyncio
async def test_owned_settle_rejects_authenticated_malformed_collected_request(
    tmp_path,
) -> None:
    from nio.store._sync_journal_plan import _canonical_work_plaintext
    from nio.store._sync_journal_preflight import _canonical_internal

    generation = uuid4()
    bootstrap, _account = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    connection = session._owned_store.database.connection()
    request_id = "malformed-collected-request"
    request_source = {
        "content": {
            "action": "request",
            "body": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "room_id": ROOM,
                "sender_key": "malformed-collected-sender-key",
                "session_id": "malformed-collected-session",
            },
            "request_id": request_id,
            "requesting_device_id": BOB_DEVICE,
        },
        "sender": BOB,
        "type": "m.room_key_request",
    }
    cancellation_source = {
        "content": {
            "action": "request_cancellation",
            "request_id": request_id,
            "requesting_device_id": BOB_DEVICE,
        },
        "sender": BOB,
        "type": "m.room_key_request",
    }
    malformed_source = {
        "content": {
            "action": "request_cancellation",
            "request_id": request_id,
        },
        "sender": BOB,
        "type": "m.room_key_request",
    }
    try:
        assert client.olm is not None
        olm = client.olm
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        pending = ToDeviceEvent.parse_event(request_source)
        assert type(pending) is RoomKeyRequest
        olm.key_request_from_untrusted[request_id] = pending
        stage_classic(
            bootstrap,
            {
                "account_data": {"events": []},
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "malformed-collected-token",
                "presence": {"events": []},
                "rooms": {},
                "to_device": {"events": [cancellation_source]},
            },
        )
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        owner = session._journal.load_owner()
        with session._journal._owner.read():
            inventory = session._journal._load_task3_work_inventory(owner)
        assert len(inventory.work) == 2
        source_work = next(
            work
            for work in inventory.work
            if work.metadata is not None
            and work.metadata.preparation_phase is _PreparationPhase.SOURCE
        )
        collected_work = next(
            work
            for work in inventory.work
            if work.metadata is not None
            and work.metadata.preparation_phase
            is _PreparationPhase.COLLECTED_KEY_REQUEST
        )
        assert type(collected_work.value) is EventRecord
        malformed = replace(
            collected_work.value,
            source_json=canonical_json(malformed_source),
        )
        row = connection.execute(
            "SELECT account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload, payload_sha256 FROM NioIngestWork "
            "WHERE account_id = ? AND work_id = ?",
            (ACCOUNT, collected_work.value.record_id),
        ).fetchone()
        assert row is not None
        plaintext = _canonical_work_plaintext(
            "event",
            malformed,
            collected_work.metadata,
        )
        payload, digest = session._journal._payload(
            owner,
            "NioIngestWork",
            plaintext,
            header=_canonical_internal(row[1:11]),
        )
        connection.execute(
            "UPDATE NioIngestWork SET payload = ?, payload_sha256 = ? "
            "WHERE account_id = ? AND work_id = ?",
            (payload, digest, ACCOUNT, collected_work.value.record_id),
        )
        first = session.next_batch(max_records=1)
        assert first is not None
        assert first.records == (source_work.value,)
        session.acknowledge_batch(first.ref)
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored_client = owned_client(tmp_path)
    restored_session = open_owned_session(restored_client, reopened, generation)
    restored_connection = restored_session._owned_store.database.connection()
    try:
        batch = restored_session.next_batch(max_records=1)
        assert batch is not None
        assert batch.records == (malformed,)
        settlement = restored_session._journal._load_batch_settlement(batch)
        assert settlement is not None
        authenticated_work, aggregate = settlement
        assert authenticated_work.value == malformed
        assert authenticated_work.metadata == collected_work.metadata
        assert aggregate is None
        frontier = _owned_delivery_frontier(restored_connection)
        graph = tuple(restored_connection.iterdump())
        callbacks: list[object] = []

        async def callback(event: object) -> None:
            callbacks.append(event)
            raise AssertionError("malformed collected request reached a callback")

        restored_client.add_to_device_callback(callback, None)
        statements: list[str] = []
        restored_connection.set_trace_callback(statements.append)
        with pytest.raises(
            JournalIntegrityError,
            match="collected key-request source is invalid",
        ):
            await restored_session._settle_batch(
                batch,
                receipt_new=True,
                semantic_event_new=False,
            )

        restored_connection.set_trace_callback(None)
        assert callbacks == []
        assert tuple(restored_connection.iterdump()) == graph
        assert _owned_delivery_frontier(restored_connection) == frontier
        assert restored_session.next_batch(max_records=1) == batch
        assert not any(
            statement.lstrip()
            .upper()
            .startswith(("INSERT", "UPDATE", "DELETE", "REPLACE"))
            for statement in statements
        )
        restored_client._assert_ingestion_not_poisoned()
    finally:
        restored_connection.set_trace_callback(None)
        await restored_session.close()


@pytest.mark.parametrize("transport", ["classic", "sliding"])
@pytest.mark.parametrize(
    "transient",
    [
        "valid",
        "bad-content",
        "bad-container",
        "bad-presence-type",
        "callback-error",
        "callback-timeout",
        "cancel",
    ],
)
@pytest.mark.asyncio
async def test_fresh_transients_are_best_effort(tmp_path, caplog, transport, transient):
    config = classic_config() if transport == "classic" else sliding_config()
    generation = uuid4()
    bootstrap, _ = open_owned_bootstrap(tmp_path, generation, source=config.source)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation, config)
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    room = nio.MatrixRoom(ROOM, ACCOUNT)
    room.add_member(BOB, "Bob", None)
    room.users[BOB].presence = "unavailable"
    client.rooms[ROOM] = room
    seen = []

    async def typing_callback(candidate, event):
        assert not session._owned_store.database.connection().in_transaction
        assert session._journal.load_source().next_request_id == 1
        seen.append(("typing", candidate.room_id, event.users))
        if transient == "callback-error":
            raise RuntimeError("payload-secret")
        if transient == "callback-timeout":
            await asyncio.Event().wait()
        if transient == "cancel":
            asyncio.current_task().cancel()
            await asyncio.sleep(0)

    async def presence_callback(event):
        assert not session._owned_store.database.connection().in_transaction
        seen.append(("presence", event.user_id, event.presence))

    client.add_ephemeral_callback(typing_callback, nio.TypingNoticeEvent)
    client.add_presence_callback(presence_callback, None)
    requests = []

    async def respond(request, **kwargs):
        requests.append(request.path)
        if request.path.endswith("/state"):
            return session._network_result(request, 200, hydration_state())
        if len([path for path in requests if not path.endswith("/state")]) > 1:
            raise asyncio.CancelledError
        body = _parity_response(
            transport,
            request.body,
            nonempty=True,
            one_time_key_count=client.olm.account.max_one_time_keys,
        )
        typing = {"type": "m.typing", "content": {"user_ids": [BOB]}}
        if transport == "classic":
            body["rooms"]["join"][ROOM]["ephemeral"]["events"].append(typing)
        else:
            body["extensions"]["typing"] = {"rooms": {ROOM: typing}}
        if transient == "bad-content":
            bad_typing = {"type": "m.typing", "content": "payload-secret"}
            bad_presence = {"type": "m.presence", "content": "payload-secret"}
        else:
            bad_typing = None
            bad_presence = None
        if transport == "classic":
            if transient == "bad-content":
                body["rooms"]["join"][ROOM]["ephemeral"]["events"] = [
                    body["rooms"]["join"][ROOM]["ephemeral"]["events"][0],
                    *([bad_typing] * 5),
                ]
                body["presence"]["events"] = [bad_presence] * 5
            elif transient == "bad-container":
                body["presence"] = "payload-secret"
        elif transient == "bad-content":
            body["extensions"]["typing"]["rooms"][ROOM] = bad_typing
            body["extensions"]["presence"]["events"] = [bad_presence] * 5
        elif transient == "bad-container":
            body["extensions"]["typing"] = "payload-secret"
            body["extensions"]["presence"] = "payload-secret"
        if transient == "bad-presence-type":
            presence_root = body if transport == "classic" else body["extensions"]
            presence_root["presence"]["events"][0]["type"] = "m.invalid"
        return session._network_result(request, 200, canonical_json(body))

    session._request = respond
    records = []
    closed = False
    started = asyncio.get_running_loop().time()
    try:
        with pytest.raises(asyncio.CancelledError):
            await run_with_acknowledgements(session, records)
        if transient == "cancel":
            assert seen == [("typing", ROOM, [BOB])]
            assert records == []
            assert len(session._journal.list_frames(2)) == 1
            await session.close()
            closed = True
            restored_client = owned_client(tmp_path)
            reopened = reopen_owned_bootstrap(
                tmp_path, generation, source=config.source
            )
            restored = open_owned_session(restored_client, reopened, generation, config)
            replayed = []
            restored_client.add_ephemeral_callback(
                lambda *args: replayed.append(args), nio.TypingNoticeEvent
            )
            restored_client.add_presence_callback(
                lambda *args: replayed.append(args), None
            )

            async def restart_response(request, **kwargs):
                if request.path.endswith("/state"):
                    return restored._network_result(request, 200, hydration_state())
                raise asyncio.CancelledError

            restored._request = restart_response
            try:
                with pytest.raises(asyncio.CancelledError):
                    await run_with_acknowledgements(restored, records)
                assert any(record.event_id == "$parity-message" for record in records)
                assert replayed == []
                assert restored._journal.list_frames(2) == ()
            finally:
                await restored.close()
            return
        assert any(record.event_id == "$parity-message" for record in records)
        assert any(record.kind is RecordKind.EPHEMERAL for record in records)
        if transient in {"callback-error", "callback-timeout"}:
            assert seen == [("typing", ROOM, [BOB])]
        if transient == "bad-presence-type":
            assert client.rooms[ROOM].users[BOB].presence == "unavailable"
            assert seen == [("typing", ROOM, [BOB])]
        if transient == "callback-timeout":
            assert 0.9 <= asyncio.get_running_loop().time() - started < 5
        assert all(
            not isinstance(record, EventRecord)
            or json.loads(record.source_json).get("type") != "m.typing"
            for record in records
        )
        assert session._journal.load_pending_hydrations(limit=1) == ()
        assert session._journal.list_frames(2) == ()
        if transient == "valid":
            assert seen == [("typing", ROOM, [BOB]), ("presence", BOB, "online")]
            assert client.rooms[ROOM].typing_users == [BOB]
            assert client.rooms[ROOM].users[BOB].presence == "online"
        assert "payload-secret" not in caplog.text
        assert len(caplog.records) == (0 if transient == "valid" else 1)
        client._assert_ingestion_not_poisoned()
    finally:
        if not closed:
            await session.close()


@pytest.mark.parametrize("transport", ["classic", "sliding"])
@pytest.mark.asyncio
async def test_transient_only_response_creates_no_durable_obligations(
    tmp_path, transport
):
    config = classic_config() if transport == "classic" else sliding_config()
    generation = uuid4()
    bootstrap, _ = open_owned_bootstrap(tmp_path, generation, source=config.source)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation, config)
    client.olm.account.shared = True
    client.olm.uploaded_key_count = client.olm.account.max_one_time_keys
    client.olm.save_account()
    polls = 0

    async def respond(request, **kwargs):
        nonlocal polls
        polls += 1
        if polls > 1:
            raise asyncio.CancelledError
        body = _parity_response(
            transport,
            request.body,
            nonempty=False,
            one_time_key_count=client.olm.account.max_one_time_keys,
        )
        presence = {
            "events": [
                {"type": "m.presence", "sender": BOB, "content": {"presence": "online"}}
            ]
        }
        if transport == "sliding":
            body["extensions"]["presence"] = presence
            body["extensions"]["typing"] = {
                "rooms": {ROOM: {"type": "m.typing", "content": {"user_ids": [BOB]}}}
            }
        else:
            body["presence"] = presence
        return session._network_result(request, 200, canonical_json(body))

    session._request = respond
    records = []
    try:
        with pytest.raises(asyncio.CancelledError):
            await run_with_acknowledgements(session, records)
        assert polls == 2
        assert records == []
        assert client.rooms == {}
        assert session._journal.load_source().next_request_id == 1
        assert session._journal.load_pending_hydrations(limit=1) == ()
        connection = session._owned_store.database.connection()
        for table in ("NioIngestFrame", "NioIngestWork", "NioIngestRoomAggregate"):
            assert (
                connection.execute(f"SELECT COUNT(*) FROM {table}").fetchone()[0] == 0
            )
    finally:
        await session.close()


@pytest.mark.parametrize(
    "ephemeral",
    [
        {"events": [{"content": "payload-secret"}]},
        {"events": ["payload-secret"]},
        "payload-secret",
    ],
)
@pytest.mark.asyncio
async def test_unidentified_classic_ephemeral_remains_strict(tmp_path, ephemeral):
    generation = uuid4()
    bootstrap, _ = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    before = session._journal.load_source()
    try:
        body = {
            "next_batch": "s1",
            "rooms": {
                "join": {
                    ROOM: {
                        "ephemeral": ephemeral,
                        "timeline": {"events": []},
                    }
                }
            },
        }
        from nio.ingest.source import SourceResultKind

        request = session._source.plan_request(before, before.next_request_id)
        result = session._source.normalize(
            request, session._network_result(request, 200, canonical_json(body))
        )
        if isinstance(ephemeral, dict) and isinstance(ephemeral["events"][0], dict):
            assert result.kind is SourceResultKind.FRAME
            stage_classic(bootstrap, body)
            with pytest.raises(JournalIntegrityError):
                session._materialize_oldest_frame(limits=MaterializerLimits())
            connection = session._owned_store.database.connection()
            assert (
                connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0]
                == 0
            )
        else:
            assert result.kind is SourceResultKind.TERMINAL_ERROR
            assert session._journal.load_source() == before
    finally:
        await session.close()
