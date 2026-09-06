"""Classic sync transactions around nio's shared synchronous processor."""

from __future__ import annotations

import asyncio
import json
from dataclasses import dataclass, replace
from inspect import isawaitable
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
from ..rooms import MatrixInvitedRoom, MatrixRoom
from ..store import MatrixStore, SqliteStore
from .codec import freeze_event, restore_event
from .crypto import CryptoMaintenance
from .model import (
    OwnMembership,
    RecordKind,
    SyncBatch,
    SyncRecord,
    encode_json,
    encode_records,
)
from .observations import TransientSections, deliver_transients
from .projection import encode_member, encode_room, restore_room
from .store import DurableStore
from .transport import HttpError, Transport

if TYPE_CHECKING:
    from ..client.async_client import AsyncClient


@dataclass(frozen=True, slots=True)
class DurableSyncConfig:
    max_response_bytes: int = 16 * 1024 * 1024
    max_batch_records: int = 256
    max_batch_bytes: int = 2 * 1024 * 1024
    max_pending_bytes: int = 64 * 1024 * 1024
    sync_timeout_ms: int = 30_000
    sync_filter: dict[str, Any] | str | None = None
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
        self._local_lock = asyncio.Lock()
        self._source_ready = asyncio.Event()
        self._source_ready.set()
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

    def _decode_response(self, body: bytes) -> tuple[SyncResponse, TransientSections]:
        if len(body) > self.config.max_response_bytes:
            raise LocalProtocolError("sync response exceeds the durable input bound")
        try:
            root = json.loads(body)
            if not isinstance(root, dict):
                raise ValueError("response is not an object")
            # Transient observations never decide whether durable data is accepted.
            transients: TransientSections = [(None, root.pop("presence", {}))]
            rooms = root.get("rooms", {})
            if isinstance(rooms, dict):
                for section in ("join", "leave"):
                    infos = rooms.get(section, {})
                    if isinstance(infos, dict):
                        for room_id, info in infos.items():
                            if isinstance(info, dict):
                                transients.append((room_id, info.pop("ephemeral", {})))
            response = SyncResponse.from_dict(root)
            if not isinstance(response, SyncResponse) or not response.next_batch:
                raise ValueError("invalid sync response")
            return response, transients
        except (ValueError, TypeError, KeyError) as error:
            raise LocalProtocolError("malformed durable sync response") from error

    def _capture_response(self, body: bytes) -> tuple[SyncResponse, TransientSections]:
        self._assert_active()
        response = self._decode_response(body)
        with self._store.transaction():
            self._store.capture(body)
        return response

    async def _accept_response(self, body: bytes) -> None:
        response, transients = self._capture_response(body)
        self._prepare_pending(response)
        await deliver_transients(self.client, transients)

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
        response = response or self._decode_response(pending[0])[0]
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

    @property
    def progress_generation(self) -> int:
        self._assert_active()
        return self._store.database.execute_sql(
            "SELECT MAX(acked_sequence, COALESCE((SELECT MAX(sequence) FROM NioDurableBatch),0)) "
            "FROM NioDurableMeta WHERE id=1"
        ).fetchone()[0]

    async def dispatch(
        self, record: SyncRecord, *, event: object | None = None
    ) -> None:
        """Invoke one committed observation's callback without acknowledging it."""
        self._assert_active()
        if record.route is None:
            return
        room = None
        if record.room_id is not None:
            room = self.client.rooms.get(
                record.room_id
            ) or self.client.invited_rooms.get(record.room_id)
            if room is None:
                members = {
                    user_id: json.loads(member)
                    for user_id, member in self._store.database.execute_sql(
                        "SELECT user_id,member FROM NioDurableMember WHERE room_id=?",
                        (record.room_id,),
                    )
                }
                room = restore_room(
                    record.room_id, self._metadata[record.room_id], members
                )
        value = self.client._dispatch_sync_item(
            _SyncItem(
                record.route,
                event if event is not None else restore_event(record),
                room,
            )
        )
        if isawaitable(value):
            await value

    async def _maintain_crypto(self) -> None:
        async with self._crypto_lock:
            completed: set[str] = set()
            while True:
                try:
                    with self._store.transaction():
                        request = self._crypto.next_request(completed=completed)
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
                completed.add(request.kind)

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
                local = self._read_local_intent()
                if (
                    local is not None
                    and "sequence" not in local
                    and not self._local_lock.locked()
                ):
                    async with self._local_lock:
                        await self._apply_local_intent(local)
                    continue
                await self._source_ready.wait()
                if self._store.has_batches():
                    continue
                if self._quiescing or self._closed:
                    return
                method, path = Api.sync(
                    "",
                    self.cursor,
                    self.config.sync_timeout_ms,
                    self.config.sync_filter,
                )
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
                        continue
                    if self._local_lock.locked() and not running.cancelling():
                        continue
                    raise
                finally:
                    self._poll = None
                await self._accept_response(body)
        finally:
            self._running = None
            self._changed.set()

    def _read_local_intent(self) -> dict[str, Any] | None:
        row = self._store.database.execute_sql(
            "SELECT body FROM NioDurableCrypto WHERE kind='membership' AND key='current'"
        ).fetchone()
        return json.loads(row[0]) if row else None

    def _local_position_matches(self, intent: dict[str, Any]) -> bool:
        metadata = self._metadata.get(intent["room_id"], {})
        return (
            "join" if metadata.get("membership") == "join" else "leave",
            metadata.get("membership_epoch", 0),
        ) == (intent["previous_membership"], intent["previous_epoch"])

    async def wait_for_membership_idle(self) -> None:
        """Wait for the preceding local outcome to reach application acknowledgement."""
        self._assert_active()
        while intent := self._read_local_intent():
            if "sequence" in intent:
                acked = self._store.database.execute_sql(
                    "SELECT acked_sequence FROM NioDurableMeta WHERE id=1"
                ).fetchone()[0]
                if intent["sequence"] <= acked:
                    with self._store.transaction():
                        self._store.database.execute_sql(
                            "DELETE FROM NioDurableCrypto WHERE kind='membership'"
                        )
                    return
            self._changed.clear()
            await self._changed.wait()
            self._assert_active()

    async def change_membership(
        self,
        *,
        operation_id: UUID,
        room_id: str,
        previous_membership: str | None,
        previous_epoch: int,
        current_membership: str,
    ) -> bool:
        """Retain a local join/leave before HTTP; consumer draining must continue."""
        self._assert_active()
        if (
            not room_id
            or current_membership not in ("join", "leave")
            or previous_membership not in ("join", "leave", None)
            or previous_epoch < 0
        ):
            raise ValueError("invalid local membership transition")
        intent = {
            "operation_id": str(operation_id),
            "room_id": room_id,
            "previous_membership": previous_membership or "leave",
            "previous_epoch": previous_epoch,
            "current_membership": current_membership,
        }
        await self.wait_for_membership_idle()
        async with self._local_lock:
            if not self._local_position_matches(intent):
                return False
            if self._read_local_intent() is not None:
                raise LocalProtocolError("another local membership command is pending")
            with self._store.transaction():
                self._store.database.execute_sql(
                    "INSERT INTO NioDurableCrypto(kind,key,body) VALUES('membership','current',?)",
                    (encode_json(intent),),
                )
            self._source_ready.clear()
            if self._poll is not None:
                self._poll.cancel()
            try:
                while self._store.input is not None:
                    self._changed.clear()
                    await self._changed.wait()
                    self._assert_active()
                return await self._apply_local_intent(intent)
            finally:
                self._source_ready.set()
                self._changed.set()

    async def _apply_local_intent(self, intent: dict[str, Any]) -> bool:
        if not self._local_position_matches(intent):
            with self._store.transaction():
                self._store.database.execute_sql(
                    "DELETE FROM NioDurableCrypto WHERE kind='membership'"
                )
            self._changed.set()
            return False
        room_id = intent["room_id"]
        target = intent["current_membership"]
        method, path = (
            Api.join("", room_id) if target == "join" else Api.room_leave("", room_id)
        )
        try:
            await self._transport.request(method, path, "{}")
        except HttpError:
            with self._store.transaction():
                self._store.database.execute_sql(
                    "DELETE FROM NioDurableCrypto WHERE kind='membership'"
                )
            self._changed.set()
            return False
        self._assert_active()
        try:
            with self._store.transaction():
                previous, epoch = (
                    intent["previous_membership"],
                    intent["previous_epoch"],
                )
                change = OwnMembership(
                    previous,
                    target,
                    epoch,
                    epoch + (previous == "join" and target == "leave"),
                    "local",
                )
                room = self.client.rooms.get(room_id) or MatrixRoom(
                    room_id, self.client.user_id
                )
                self._metadata.setdefault(room_id, {}).update(
                    membership=target, membership_epoch=change.current_epoch
                )
                if target == "join":
                    room = MatrixRoom(room_id, self.client.user_id)
                    self.client.rooms[room_id] = room
                    self._store.database.execute_sql(
                        "DELETE FROM NioDurableMember WHERE room_id=?", (room_id,)
                    )
                else:
                    self.client.rooms.pop(room_id, None)
                    if self.client.olm:
                        self.client.olm.outbound_group_sessions.pop(room_id, None)
                self.client.invited_rooms.pop(room_id, None)
                self._save_rooms({room_id: room}, set())
                batch = self._store.publish(
                    (
                        SyncRecord(
                            RecordKind.ROOM_LIFECYCLE,
                            room_id,
                            {"membership": target},
                            membership=change,
                            membership_epoch=change.current_epoch,
                        ),
                    )
                )
                intent["sequence"] = batch.sequence
                self._store.database.execute_sql(
                    "UPDATE NioDurableCrypto SET body=? WHERE kind='membership' AND key='current'",
                    (encode_json(intent),),
                )
            self._changed.set()
            return True
        except BaseException:
            self.client._poison_ingestion()
            self._store.close()
            self._changed.set()
            raise

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
