"""Classic sync transactions around nio's shared synchronous processor."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID

from ..api import Api
from ..client.base_client import _SyncItem
from ..crypto import Olm
from ..event_provenance import TimelineEventProvenance
from ..events import InviteMemberEvent, RoomMemberEvent
from ..exceptions import LocalProtocolError
from ..responses import ErrorResponse, SyncResponse
from ..rooms import MatrixInvitedRoom
from ..store import MatrixStore, SqliteStore
from .codec import freeze_event
from .crypto import CryptoMaintenance
from .model import (
    OwnMembership,
    RecordKind,
    SyncBatch,
    SyncRecord,
    encode_json,
    encode_records,
)
from .projection import encode_member, encode_room, restore_room
from .store import DurableStore
from .transport import Transport

if TYPE_CHECKING:
    from ..client.async_client import AsyncClient
    from ..rooms import MatrixRoom


@dataclass(frozen=True, slots=True)
class DurableSyncConfig:
    max_response_bytes: int = 16 * 1024 * 1024
    max_batch_records: int = 256
    max_batch_bytes: int = 2 * 1024 * 1024
    max_pending_bytes: int = 64 * 1024 * 1024
    sync_timeout_ms: int = 30_000
    recovery_page_size: int = 100
    max_recovery_pages: int = 1_000

    def __post_init__(self) -> None:
        if (
            min(
                self.max_response_bytes,
                self.max_batch_records,
                self.max_batch_bytes,
                self.max_pending_bytes,
                self.recovery_page_size,
                self.max_recovery_pages,
            )
            <= 0
            or self.sync_timeout_ms < 0
        ):
            raise ValueError("durable sync bounds must be positive")


class DurableSync:
    """One durable Matrix input stream and one application batch consumer."""

    def __init__(
        self, client: AsyncClient, store: DurableStore, config: DurableSyncConfig
    ):
        self.client = client
        self._store = store
        self.config = config
        self.stream_id = store.stream_id
        self._changed = asyncio.Event()
        self._closed = False
        self._running: asyncio.Task[None] | None = None
        self._poll: asyncio.Task[bytes] | None = None
        self._quiescing = False
        self._crypto_lock = asyncio.Lock()
        self._transport = Transport(client, config.max_response_bytes)
        self._crypto = CryptoMaintenance(client, store)
        self._crypto.restore()
        self._metadata: dict[str, dict[str, Any]] = {}
        self._restore_rooms()

    def _restore_rooms(self) -> None:
        for room_id, encoded in self._store.database.execute_sql(
            "SELECT room_id,metadata FROM NioDurableRoom"
        ):
            metadata = json.loads(encoded)
            members = {
                user_id: json.loads(member)
                for user_id, member in self._store.database.execute_sql(
                    "SELECT user_id,member FROM NioDurableMember WHERE room_id=?",
                    (room_id,),
                )
            }
            room = restore_room(room_id, metadata, members)
            self._metadata[room_id] = metadata
            if metadata.get("membership") in ("leave", "ban"):
                continue
            if isinstance(room, MatrixInvitedRoom):
                self.client.invited_rooms[room_id] = room
            else:
                self.client.rooms[room_id] = room

    def _assert_active(self) -> None:
        self.client._assert_ingestion_not_poisoned()
        if self._closed:
            raise LocalProtocolError("durable sync session is closed")

    def _decode_response(self, body: bytes) -> SyncResponse:
        if len(body) > self.config.max_response_bytes:
            raise LocalProtocolError("sync response exceeds the durable input bound")
        try:
            root = json.loads(body)
            if not isinstance(root, dict):
                raise ValueError("response is not an object")
            # Transient observations never decide whether durable data is accepted.
            root.pop("presence", None)
            rooms = root.get("rooms", {})
            if isinstance(rooms, dict):
                for section in ("join", "leave"):
                    infos = rooms.get(section, {})
                    if isinstance(infos, dict):
                        for info in infos.values():
                            if isinstance(info, dict):
                                info.pop("ephemeral", None)
            response = SyncResponse.from_dict(root)
            if not isinstance(response, SyncResponse) or not response.next_batch:
                raise ValueError("invalid sync response")
            return response
        except (ValueError, TypeError, KeyError) as error:
            raise LocalProtocolError("malformed durable sync response") from error

    def _capture_response(self, body: bytes) -> SyncResponse:
        self._assert_active()
        response = self._decode_response(body)
        with self._store.transaction():
            self._store.capture(body)
        return response

    async def _accept_response(self, body: bytes) -> None:
        response = self._capture_response(body)
        self._prepare_pending(response)

    def _change_membership(self, room_id: str, membership: str) -> OwnMembership | None:
        metadata = self._metadata.setdefault(room_id, {})
        previous = metadata.get("membership")
        epoch = metadata.get("membership_epoch", 0)
        if previous == membership:
            return None
        next_epoch = epoch + (previous == "join" and membership != "join")
        change = OwnMembership(previous, membership, epoch, next_epoch)
        metadata.update(membership=membership, membership_epoch=next_epoch)
        return change

    def _prepare_pending(self, response: SyncResponse | None = None) -> None:
        self._assert_active()
        pending = self._store.input
        if pending is None or pending[1].get("phase") == "prepared":
            return
        response = response or self._decode_response(pending[0])
        known_rooms = {
            room_id
            for room_id, metadata in self._metadata.items()
            if metadata.get("membership") == "join"
        }
        fresh_rooms = set(response.rooms.join) - set(self.client.rooms)
        changed_rooms: dict[str, MatrixRoom] = {}
        changed_members: set[tuple[str, str]] = set()
        explicit_memberships = {
            room_id
            for section in (response.rooms.join, response.rooms.leave)
            for room_id, info in section.items()
            if any(
                isinstance(event, RoomMemberEvent)
                and event.state_key == self.client.user_id
                for event in (*info.state, *info.timeline.events)
            )
        }
        records: list[SyncRecord] = []
        pending_bytes = self._store.database.execute_sql(
            "SELECT COALESCE(SUM(length(CAST(records AS BLOB))),0) FROM NioDurableBatch"
        ).fetchone()[0]

        def publish(chunk: tuple[SyncRecord, ...]) -> None:
            nonlocal pending_bytes
            encoded = encode_records(chunk)
            size = len(encoded.encode())
            if size > self.config.max_batch_bytes:
                if len(chunk) == 1:
                    raise LocalProtocolError(
                        "prepared event exceeds the durable batch bound"
                    )
                middle = len(chunk) // 2
                publish(chunk[:middle])
                publish(chunk[middle:])
                return
            pending_bytes += size
            if pending_bytes > self.config.max_pending_bytes:
                raise LocalProtocolError(
                    "prepared output exceeds the durable pending bound"
                )
            self._store.publish(chunk, encoded_records=encoded)

        def flush() -> None:
            if records:
                publish(tuple(records))
                records.clear()

        try:
            with self._store.transaction():
                for fresh_room_id in fresh_rooms:
                    self._store.database.execute_sql(
                        "DELETE FROM NioDurableMember WHERE room_id=?", (fresh_room_id,)
                    )
                for item in self.client._iter_sync(response, include_left=True):
                    if item.route in ("presence", "ephemeral"):
                        continue
                    room = item.room
                    room_id = room.room_id if room else None
                    change = None
                    if room is not None and room_id is not None:
                        if room_id not in known_rooms or item.route in (
                            None,
                            "invite",
                            "room_account_data",
                        ):
                            changed_rooms[room_id] = room
                        if item.event is None:
                            if room_id in explicit_memberships:
                                continue
                            assert item.section is not None
                            change = self._change_membership(room_id, item.section)
                            if change is None:
                                continue
                        if isinstance(item.event, (RoomMemberEvent, InviteMemberEvent)):
                            changed_members.add((room_id, item.event.state_key))
                            changed_rooms[room_id] = room
                            if item.event.state_key == self.client.user_id:
                                change = self._change_membership(
                                    room_id, item.event.membership
                                )
                        elif item.event is not None and "state_key" in getattr(
                            item.event, "source", {}
                        ):
                            changed_rooms[room_id] = room
                    record = freeze_event(item)
                    if room_id is not None:
                        record = replace(
                            record,
                            membership=change,
                            membership_epoch=self._metadata.get(room_id, {}).get(
                                "membership_epoch", 0
                            ),
                            provenance=(
                                (
                                    TimelineEventProvenance.LIVE
                                    if room_id in known_rooms
                                    else TimelineEventProvenance.HISTORY
                                )
                                if record.kind is RecordKind.TIMELINE
                                else record.provenance
                            ),
                        )
                    barrier = change is not None or isinstance(
                        item.event, (RoomMemberEvent, InviteMemberEvent)
                    )
                    if barrier:
                        flush()
                    records.append(record)
                    if barrier or len(records) >= self.config.max_batch_records:
                        flush()
                flush()
                for room_id, info in response.rooms.join.items():
                    if info.summary or info.unread_notifications:
                        changed_rooms[room_id] = self.client.rooms[room_id]
                self._save_rooms(changed_rooms, changed_members)
                self._crypto.capture()
                self._store.set_cursor(response.next_batch)
                self._store.save_continuation({"phase": "prepared"})
            self.client.next_batch = response.next_batch
            self._changed.set()
        except BaseException:
            self.client._poison_ingestion()
            self._store.close()
            self._changed.set()
            raise

    def _save_rooms(
        self, rooms: dict[str, MatrixRoom], members: set[tuple[str, str]]
    ) -> None:
        database = self._store.database
        for room_id, room in rooms.items():
            prior = self._metadata.get(room_id, {})
            metadata = encode_room(
                room,
                membership=prior.get("membership"),
                membership_epoch=prior.get("membership_epoch", 0),
                members_complete=room.members_synced,
            )
            self._metadata[room_id] = metadata
            database.execute_sql(
                "INSERT INTO NioDurableRoom(room_id,metadata) VALUES(?,?) "
                "ON CONFLICT(room_id) DO UPDATE SET metadata=excluded.metadata",
                (room_id, encode_json(metadata)),
            )
        for room_id, user_id in members:
            member = encode_member(rooms[room_id], user_id)
            if member is None:
                database.execute_sql(
                    "DELETE FROM NioDurableMember WHERE room_id=? AND user_id=?",
                    (room_id, user_id),
                )
            else:
                database.execute_sql(
                    "INSERT INTO NioDurableMember(room_id,user_id,member) VALUES(?,?,?) "
                    "ON CONFLICT(room_id,user_id) DO UPDATE SET member=excluded.member",
                    (room_id, user_id, encode_json(member)),
                )

    async def next_batch(self) -> SyncBatch | None:
        self._assert_active()
        return self._store.next_batch()

    @property
    def cursor(self) -> str | None:
        """Last atomically prepared Matrix position."""
        self._assert_active()
        return self._store.cursor

    async def _maintain_crypto(self) -> None:
        async with self._crypto_lock:
            while True:
                try:
                    with self._store.transaction():
                        request = self._crypto.next_request()
                except BaseException:
                    self.client._poison_ingestion()
                    self._store.close()
                    raise
                if request is None:
                    return
                body = await self._transport.request(
                    request.method, request.path, request.body
                )
                decoded = json.loads(body)
                try:
                    with self._store.transaction():
                        response, observations = self._crypto.apply(request, decoded)
                        if isinstance(response, ErrorResponse):
                            raise LocalProtocolError("invalid durable crypto response")
                        if observations:
                            self._store.publish(
                                tuple(
                                    freeze_event(_SyncItem("to_device", event))
                                    for event in observations
                                )
                            )
                except BaseException:
                    self.client._poison_ingestion()
                    self._store.close()
                    raise
                self._changed.set()

    async def run(self) -> None:
        """Run one Classic source, draining committed batches between responses."""
        self._assert_active()
        if self._running is not None:
            raise LocalProtocolError("durable sync runner is already active")
        running = asyncio.current_task()
        assert running is not None
        self._running = running
        try:
            while not self._closed:
                self._assert_active()
                if self._quiescing and self._store.input is None:
                    return
                if self._store.has_batches():
                    self._changed.clear()
                    await self._changed.wait()
                    continue
                if self._store.input is not None:
                    self._prepare_pending()
                    if self._store.has_batches():
                        continue
                    await self._maintain_crypto()
                    with self._store.transaction():
                        self._store.finish_input()
                        self._store.publish((), completes_sync=True)
                    self._changed.set()
                    continue
                method, path = Api.sync("", self.cursor, self.config.sync_timeout_ms)
                self._poll = asyncio.create_task(
                    self._transport.request(
                        method,
                        path,
                        request_timeout=self.config.sync_timeout_ms / 1000 + 30,
                    )
                )
                try:
                    body = await self._poll
                except asyncio.CancelledError:
                    if self._quiescing and not running.cancelling():
                        return
                    raise
                finally:
                    self._poll = None
                await self._accept_response(body)
        finally:
            self._running = None
            self._changed.set()

    async def quiesce(self) -> None:
        """Stop polling; finish captured input while the consumer keeps draining."""
        self._quiescing = True
        self._changed.set()
        if self._poll is not None:
            self._poll.cancel()
        if self._running is not None and self._running is not asyncio.current_task():
            await asyncio.shield(self._running)

    async def ack(self, batch: SyncBatch) -> None:
        self._assert_active()
        self._store.ack(batch)
        self._changed.set()

    async def wait_for_work(self) -> None:
        self._assert_active()
        while not self._store.has_batches():
            self._changed.clear()
            await self._changed.wait()
            self._assert_active()

    async def close(self) -> None:
        if self._closed:
            return
        self._closed = True
        running = self._running
        try:
            if running is not None and running is not asyncio.current_task():
                running.cancel()
                try:
                    await running
                except asyncio.CancelledError:
                    pass
        finally:
            self._store.close()
            self.client._poison_ingestion()
            self._changed.set()


def open_durable_sync(
    client: AsyncClient,
    *,
    consumer_id: UUID,
    store_path: Path,
    database_name: str | None = None,
    config: DurableSyncConfig | None = None,
    source_store_class: type[MatrixStore] = SqliteStore,
) -> DurableSync:
    """Attach storage to an authenticated client before any sync or crypto use."""
    if not client.logged_in or not client.device_id:
        raise LocalProtocolError(
            "durable sync requires authenticated account and device"
        )
    if not client.config.encryption_enabled:
        raise LocalProtocolError("durable sync requires the SQLite crypto store")
    if (
        client.store is not None
        or client.olm is not None
        or client._ordinary_sync_started
    ):
        raise LocalProtocolError(
            "durable sync requires a fresh client without a loaded store"
        )
    client._assert_ingestion_not_poisoned()
    store = DurableStore(
        store_path,
        user_id=client.user_id,
        device_id=client.device_id,
        consumer_id=consumer_id,
        database_name=database_name,
        pickle_key=client.config.pickle_key,
        source_store_class=source_store_class,
    )
    try:
        with store.transaction():
            client.store = store.matrix
            client.store_path = str(store_path)
            if client.config.encryption_enabled:
                client.olm = Olm(
                    client.user_id,
                    client.device_id,
                    store.matrix,
                    replace_rotated_device_keys=client.config.replace_rotated_device_keys,
                )
            client.encrypted_rooms = store.matrix.load_encrypted_rooms()
            client.next_batch = store.cursor or ""
            client.loaded_sync_token = client.next_batch
            session = DurableSync(client, store, config or DurableSyncConfig())
            client._durable_session = session
        return session
    except BaseException:
        store.close()
        client._poison_ingestion()
        raise
