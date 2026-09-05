from __future__ import annotations

import hmac
import os
import sqlite3
from dataclasses import dataclass, replace
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import UUID, uuid5

from peewee import IntegrityError, SqliteDatabase

from ..exceptions import LocalProtocolError
from ..ingest import errors as ingest_errors
from ..ingest import reducer as ingest_reducer
from ..ingest._json import load_json
from ..ingest.classic import ClassicSource
from ..ingest.config import (
    ClassicSourceConfig,
    SlidingSourceConfig,
    SourceConfig,
    source_transport,
)
from ..ingest.errors import JournalConflictError, JournalIntegrityError
from ..ingest.hydration import (
    HydrationResult,
    revalidated_hydration_result,
)
from ..ingest.model import (
    BatchRef,
    EventRecord,
    RecordKind,
    SyncBatch,
    SystemOrigin,
    SystemOriginKind,
    TransportKind,
    _local_membership_predecessor_matches,
    _local_membership_record_id,
    _local_membership_source_json,
    _local_membership_transition_epoch,
    _PreparedIngestionFrame,
)
from ..ingest.ports import NetworkRequest, StagedSourceResponse, _frame_id_for_response
from ..ingest.recovery import (
    apply_classic_recovery,
    recovery_progress,
    start_classic_recovery,
)
from ..ingest.serialization import (
    _validate_batch,
    batch_from_records,
    canonical_batch_payload,
)
from ..ingest.sliding import SlidingSource, _sliding_cursor_from_json
from ..ingest.source import (
    SyncFrame,
    _classic_cursor_from_json,
    _frame_room_ids,
    renormalize_staged_frame,
)
from ..ingest.state import CommitResult, OwnerView, SourceState, StagedFrame
from ._sync_journal_plan import (
    AuthenticatedWork,
    MaterializationPlan,
    _canonical_work_plaintext,
    _stored_work_insert_row,
    _stored_work_release_row,
    _work_id,
    plan_prepared_frame_materialization,
)
from ._sync_journal_preflight import (
    IngestionStoreOwner,
    _decode_delivery_state,
    _different_uuid,
    _rotate_sliding_source,
    open_journal_database,
)
from ._sync_journal_rows import (
    _MAX_DURABLE_FRAME_ROWS,
    _MAX_DURABLE_STAGED_FRAMES,
    _MAX_TOTAL_WORK_CANONICAL_BYTES,
    _MAX_TOTAL_WORK_COUNT,
    _MAX_WORK_PAYLOAD_BYTES,
    JournalRows,
    _canonical_internal,
    _canonical_room_aggregate_plaintext,
    _frame_drain_sha256,
    _frame_envelope,
    _frame_payload,
    _frame_response_from_envelope,
    _FrameDrainRow,
    _OutboundMaintenance,
    _OutboundOperation,
    _PendingOutboundMaintenance,
    _prepared_frame_payload,
    _PreparedFrameState,
    _Task3WorkInventory,
)
from ._sync_journal_values import (
    SQLITE_INT_MAX,
    DeliveryState,
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
    RoomAggregateValue,
    _FrameCompletion,
    _LocalMembershipIntent,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from ..ingest.model import LossRecord, RoomSnapshot
    from ..ingest.reducer import RoomContinuity
    from .database import MatrixStore


def _hydration_final_total(storage, releases) -> int:  # type: ignore[no-untyped-def]
    sizes = {row[1]: len(row[11]) for row in storage}
    return sum(sizes.values()) + sum(len(row[2]) - sizes[row[0]] for row in releases)


type _DeliveryMember = tuple[
    tuple[int, int, str], tuple[object, ...], EventRecord | LossRecord
]
type _DeliveryLoadedMember = tuple[
    tuple[int, int, str],
    tuple[object, ...],
    EventRecord | LossRecord,
    AuthenticatedWork,
]
type _DeliverySnapshot = tuple[
    tuple[object, ...],
    OwnerView,
    DeliveryState,
    _DeliveryLoadedMember | None,
    int,
]
_DELIVERY_COLUMNS = tuple(f"delivery_{name}" for name in DeliveryState._fields)
_DELIVERY_UPDATE = (
    "UPDATE NioIngestMeta SET revision = ?, "
    + ", ".join(f"{name} = ?" for name in _DELIVERY_COLUMNS)
    + " WHERE account_id = ? AND revision = ? AND writer_epoch = ? AND "
    + " AND ".join(f"{name} IS ?" for name in _DELIVERY_COLUMNS)
)


@dataclass(frozen=True, slots=True)
class _MaterializationWriteSet:
    aggregate_rows: tuple[tuple[object, ...], ...]
    existing_aggregate_rooms: frozenset[str]
    work_insert_rows: tuple[tuple[object, ...], ...]
    work_release_args: tuple[tuple[object, ...], ...]


def _ready_member(
    inventory: _Task3WorkInventory, key: tuple[int, int, str] | None = None
) -> _DeliveryMember | None:
    members = (
        cast(
            "_DeliveryMember",
            ((row[8], row[9], row[1]), row, work.value),
        )
        for row, work in zip(inventory.storage_rows, inventory.work, strict=True)
        if work.status == "ready"
    )
    if key is not None:
        return next((member for member in members if member[0] == key), None)
    return min(members, default=None)


def _renormalized_frame(owner: OwnerView, frame: StagedFrame) -> SyncFrame:
    request = frame.response.request
    adapter: ClassicSource | SlidingSource
    if request.transport is TransportKind.CLASSIC:
        adapter = ClassicSource(
            owner.stream_id,
            ClassicSourceConfig(request.timeout_ms, b"{}"),
            owner.account_id,
        )
    else:
        adapter = SlidingSource(
            owner.stream_id,
            SlidingSourceConfig(
                request.timeout_ms,
                "journal-validation",
                b"{}",
                b"{}",
                b"{}",
            ),
            owner.account_id,
        )
    try:
        normalized = renormalize_staged_frame(adapter, frame)
        return apply_classic_recovery(
            normalized, frame.response.recovery_json, owner.account_id
        )
    except (TypeError, ValueError) as error:
        raise JournalIntegrityError("staged response does not re-normalize") from error


class SqliteIngestionJournal(JournalRows):
    """Direct-SQLite source-state journal for the version-1 checkpoint."""

    def __init__(
        self,
        *,
        database_path: Path,
        account_id: str,
        device_id: str,
        pickle_key: str,
        owner: IngestionStoreOwner,
        writer_epoch: UUID,
        stream_id: UUID,
        transition_statement_hook: Callable[[str], None] | None,
    ) -> None:
        super().__init__()
        self.database_path = database_path
        self.account_id = account_id
        self.device_id = device_id
        self.pickle_key = pickle_key
        self.writer_epoch = writer_epoch
        self._owner = owner
        self._transition_statement_hook = transition_statement_hook

    @classmethod
    def open(
        cls,
        database: str | os.PathLike[str] | SqliteDatabase,
        *,
        account_id: str,
        device_id: str,
        consumer_generation: UUID,
        source: SourceConfig,
        pickle_key: str = "",
        sqlite_busy_timeout_ms: int = 2_000,
        statement_observer: Callable[[str], None] | None = None,
        transition_statement_hook: Callable[[str], None] | None = None,
        schema_statement_hook: Callable[[str], None] | None = None,
        configured_source_store_class: type[MatrixStore] | None = None,
        configured_store_path: Path | None = None,
        adoption_statement_hook: Callable[[str], None] | None = None,
        fresh_store: bool = False,
        fresh_store_statement_hook: Callable[[str], None] | None = None,
    ) -> SqliteIngestionJournal:
        if type(account_id) is not str or not account_id:
            raise TypeError("account_id must be a nonempty str")
        if type(device_id) is not str or not device_id:
            raise TypeError("device_id must be a nonempty str")
        if type(consumer_generation) is not UUID:
            raise TypeError("consumer_generation must be UUID")
        if type(pickle_key) is not str:
            raise TypeError("pickle_key must be str")
        source_transport(source)
        if type(sqlite_busy_timeout_ms) is not int or sqlite_busy_timeout_ms <= 0:
            raise ValueError("sqlite_busy_timeout_ms must be positive")

        opened = open_journal_database(
            database,
            account_id=account_id,
            device_id=device_id,
            consumer_generation=consumer_generation,
            source=source,
            pickle_key=pickle_key,
            sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
            statement_observer=statement_observer,
            transition_statement_hook=transition_statement_hook,
            schema_statement_hook=schema_statement_hook,
            configured_source_store_class=configured_source_store_class,
            configured_store_path=configured_store_path,
            adoption_statement_hook=adoption_statement_hook,
            fresh_store=fresh_store,
            fresh_store_statement_hook=fresh_store_statement_hook,
        )
        return cls(
            database_path=opened.path,
            account_id=account_id,
            device_id=device_id,
            pickle_key=pickle_key,
            owner=opened.owner,
            writer_epoch=opened.writer_epoch,
            stream_id=opened.stream_id,
            transition_statement_hook=transition_statement_hook,
        )

    def _execute(
        self,
        statement: str,
        parameters: tuple[object, ...] = (),
    ) -> sqlite3.Cursor:
        return self._owner.database.execute_sql(statement, parameters)

    def _load_frame_work(
        self,
        owner: OwnerView,
        frame_id: UUID,
    ) -> tuple[tuple[object, ...], AuthenticatedWork] | None:
        if type(frame_id) is not UUID:
            raise TypeError("Frame ID must be UUID")
        foreign = self._execute(
            "SELECT account_id FROM NioIngestWork WHERE account_id < ? "
            "UNION ALL SELECT account_id FROM NioIngestWork WHERE account_id > ? "
            "LIMIT 1",
            (self.account_id, self.account_id),
        ).fetchone()
        if foreign is not None:
            raise JournalIntegrityError("invalid Work row")
        row = self._execute(
            "SELECT account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload, payload_sha256 FROM NioIngestWork "
            "WHERE account_id = ? AND frame_id = ? LIMIT 1",
            (self.account_id, str(frame_id)),
        ).fetchone()
        if row is None:
            return None
        stored = tuple(row)
        return stored, self._decode_task3_work_row(owner, stored)

    def _read(self):
        return self._owner.read()

    def _transaction(self):
        return self._owner.journal_write()

    @property
    def schema_version(self) -> int:
        return self.load_owner().schema_version

    @property
    def stream_id(self) -> UUID:
        return self.load_owner().stream_id

    def set_transition_statement_hook(
        self,
        hook: Callable[[str], None] | None,
    ) -> None:
        with self._read():
            pass
        self._transition_statement_hook = hook

    def _transition_hook(self, label: str) -> None:
        if self._transition_statement_hook is not None:
            self._transition_statement_hook(label)

    def _transition_execute(
        self,
        label: str,
        statement: str,
        parameters: tuple[object, ...] = (),
    ) -> sqlite3.Cursor:
        cursor = self._execute(statement, parameters)
        self._transition_hook(label)
        return cursor

    def _materialization_write_set(
        self,
        *,
        owner: OwnerView,
        plan: MaterializationPlan,
        frame_id: UUID,
        revision: int,
        inventory: _Task3WorkInventory | None,
        existing_aggregate_rooms: set[str],
        release_error: type[Exception],
    ) -> _MaterializationWriteSet:
        aggregate_rows: list[tuple[object, ...]] = []
        for aggregate_value in plan.room_values:
            room_id = aggregate_value.continuity.room_id
            intent_kind = "hydration" if aggregate_value.pending_hydration else None
            plaintext = _canonical_room_aggregate_plaintext(aggregate_value)
            payload, digest = self._payload(
                owner,
                "NioIngestRoomAggregate",
                plaintext,
                header=_canonical_internal([room_id, revision, intent_kind]),
            )
            aggregate_rows.append(
                (self.account_id, room_id, revision, intent_kind, payload, digest)
            )

        work_insert_rows: list[tuple[object, ...]] = []
        for item in plan.work_inserts:
            stored = _stored_work_insert_row(item, frame_id, revision)
            payload, digest = self._payload(
                owner,
                "NioIngestWork",
                stored.plaintext,
                header=_canonical_internal(stored.clear_values),
            )
            work_insert_rows.append(
                (self.account_id, *stored.clear_values, payload, digest)
            )

        authenticated_by_id = (
            {_work_id(item.value): item for item in inventory.work}
            if inventory is not None
            else {}
        )
        work_release_args: list[tuple[object, ...]] = []
        for item in plan.work_releases:
            if type(item.value) is not EventRecord:
                raise release_error("released Work must be an event")
            stored = _stored_work_release_row(
                item,
                authenticated_by_id[item.value.record_id],
                revision,
            )
            payload, digest = self._payload(
                owner,
                "NioIngestWork",
                stored.plaintext,
                header=_canonical_internal(stored.clear_values),
            )
            work_release_args.append(
                (
                    revision,
                    stored.ready_ordinal,
                    payload,
                    digest,
                    self.account_id,
                    stored.work_id,
                )
            )

        return _MaterializationWriteSet(
            tuple(aggregate_rows),
            frozenset(existing_aggregate_rooms),
            tuple(work_insert_rows),
            tuple(work_release_args),
        )

    def _apply_materialization_write_set(
        self,
        write_set: _MaterializationWriteSet,
    ) -> None:
        try:
            for row in write_set.aggregate_rows:
                if row[1] in write_set.existing_aggregate_rooms:
                    aggregate_cursor = self._transition_execute(
                        "aggregate_update",
                        "UPDATE NioIngestRoomAggregate SET updated_revision = ?, "
                        "intent_kind = ?, payload = ?, "
                        "payload_sha256 = ? WHERE account_id = ? AND room_id = ?",
                        (*row[2:], row[0], row[1]),
                    )
                else:
                    aggregate_cursor = self._transition_execute(
                        "aggregate_insert",
                        "INSERT INTO NioIngestRoomAggregate("
                        "account_id, room_id, updated_revision, intent_kind, "
                        "payload, payload_sha256) VALUES (?, ?, ?, ?, ?, ?)",
                        row,
                    )
                if aggregate_cursor.rowcount != 1:
                    raise JournalIntegrityError(
                        "Aggregate write did not affect one row"
                    )
            for row in write_set.work_insert_rows:
                work_cursor = self._transition_execute(
                    "work_insert",
                    "INSERT INTO NioIngestWork("
                    "account_id, work_id, kind, status, frame_id, room_id, "
                    "membership_epoch, room_sequence, ready_revision, "
                    "ready_ordinal, created_revision, payload, "
                    "payload_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?, ?)",
                    row,
                )
                if work_cursor.rowcount != 1:
                    raise JournalIntegrityError("Work insert did not write a row")
            for args in write_set.work_release_args:
                work_cursor = self._transition_execute(
                    "work_release",
                    "UPDATE NioIngestWork SET status = 'ready', "
                    "ready_revision = ?, ready_ordinal = ?, payload = ?, "
                    "payload_sha256 = ? "
                    "WHERE account_id = ? AND work_id = ? AND status = 'held'",
                    args,
                )
                if work_cursor.rowcount != 1:
                    raise JournalIntegrityError("Work release did not update a row")
        except (sqlite3.IntegrityError, IntegrityError) as error:
            raise JournalIntegrityError("planned materialization collided") from error

    def _reset_sliding_source(
        self,
        *,
        request: NetworkRequest,
    ) -> CommitResult | None:
        if type(request) is not NetworkRequest:
            raise TypeError("request must be NetworkRequest")
        request = NetworkRequest(
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
        )

        with self._transaction():
            owner, source = self._load_stage_snapshot()
            _decode_delivery_state(
                cast("Mapping[str, object]", self._meta()),
                owner,
            )
            if (
                owner.transport_kind is not TransportKind.SLIDING
                or source.transport_kind is not TransportKind.SLIDING
                or request.transport is not TransportKind.SLIDING
                or request.stream_id != owner.stream_id
                or request.source_epoch != source.source_epoch
                or request.request_id != source.next_request_id
                or request.request_cursor_json != source.cursor_json
            ):
                return None
            if _sliding_cursor_from_json(source.cursor_json).pos is None:
                return None

            old_writer_epoch = owner.writer_epoch
            new_writer_epoch = _different_uuid(old_writer_epoch)
            _rotate_sliding_source(
                self._owner.database,
                owner=owner,
                source=source,
                writer_epoch=new_writer_epoch,
                transition_hook=self._transition_statement_hook,
            )

        self._owner._handoff_writer_epoch(old_writer_epoch, new_writer_epoch)
        self.writer_epoch = new_writer_epoch
        self._transition_hook("commit")
        return CommitResult(owner.revision + 1)

    def _delivery_snapshot(self) -> _DeliverySnapshot:
        row = self._meta()
        owner = self._decode_owner_row(cast("Mapping[str, object]", row))
        state = _decode_delivery_state(cast("Mapping[str, object]", row), owner)
        work_count = self._load_delivery_work_count()
        loaded = self._load_delivery_work(owner, state.outstanding_work_id)
        member: _DeliveryLoadedMember | None = None
        if loaded is not None and loaded[1].status == "ready":
            storage, work = loaded
            member = cast(
                "_DeliveryLoadedMember",
                (
                    (storage[8], storage[9], storage[1]),
                    storage,
                    work.value,
                    work,
                ),
            )
        return (
            tuple(row),
            owner,
            state,
            member,
            work_count,
        )

    def _delivery_cas(
        self,
        label: str,
        owner: OwnerView,
        old: DeliveryState,
        new: DeliveryState,
    ) -> None:
        cursor = self._transition_execute(
            label,
            _DELIVERY_UPDATE,
            (
                owner.revision + 1,
                *new,
                self.account_id,
                owner.revision,
                str(owner.writer_epoch),
                *old,
            ),
        )
        if cursor.rowcount != 1:
            raise JournalConflictError(f"{label} failed")

    def _delivery_writer_snapshot(
        self,
        meta: tuple[object, ...],
        member: _DeliveryLoadedMember | None,
        work_count: int,
    ) -> _DeliverySnapshot:
        try:
            current = self._delivery_snapshot()
        except JournalIntegrityError as error:
            raise JournalConflictError("delivery snapshot changed") from error
        if current[0] != meta or current[3] != member or current[4] != work_count:
            raise JournalConflictError("delivery snapshot changed")
        return current

    def _replay_batch(
        self,
        owner: OwnerView,
        state: DeliveryState,
        member: _DeliveryLoadedMember | None,
    ) -> tuple[SyncBatch, tuple[object, ...], AuthenticatedWork]:
        work_id, ready_revision, ordinal, digest = cast(
            "tuple[str, int, int, bytes]", state[2:]
        )
        if member is None:
            raise JournalIntegrityError("claimed Work is missing or moved")
        member_key, stored, record, authenticated = member
        if member_key != (ready_revision, ordinal, work_id):
            raise JournalIntegrityError("claimed Work is missing or moved")
        batch = batch_from_records(
            account_id=owner.account_id,
            device_id=owner.device_id,
            consumer_generation=owner.consumer_generation,
            stream_id=owner.stream_id,
            sequence=state.next_sequence - 1,
            created_revision=ready_revision,
            records=(record,),
        )
        if not hmac.compare_digest(batch.ref.sha256, digest):
            raise JournalIntegrityError("claimed Work does not match batch digest")
        return batch, stored, authenticated

    def _load_batch_settlement(
        self,
        batch: SyncBatch,
    ) -> tuple[AuthenticatedWork, RoomAggregateValue | None] | None:
        if type(batch) is not SyncBatch:
            raise LocalProtocolError("settlement batch must be a SyncBatch")
        with self._read():
            _meta, owner, state, member, _work_count = self._delivery_snapshot()
            if batch.ref.stream_id != owner.stream_id:
                raise LocalProtocolError("settlement batch stream is invalid")
            outstanding = state.outstanding_work_id is not None
            acknowledged_sequence = state.next_sequence - (2 if outstanding else 1)
            acknowledged = state.acknowledged_sha256
            if acknowledged is not None and batch.ref.sequence == acknowledged_sequence:
                _validate_batch(batch)
                name = f"{acknowledged_sequence}:{acknowledged.hex()}"
                expected = acknowledged, uuid5(owner.stream_id, name)
                if (batch.ref.sha256, batch.ref.batch_id) != expected:
                    raise ingest_errors.BatchIntegrityError(
                        "acknowledged batch changed"
                    )
                return None
            if not outstanding or batch.ref.sequence != state.next_sequence - 1:
                raise LocalProtocolError("settlement batch is not FIFO")
            expected_batch, _storage, work = self._replay_batch(owner, state, member)
            if batch != expected_batch:
                raise ingest_errors.BatchIntegrityError("outstanding batch changed")
            room: RoomAggregateValue | None = None
            if type(work.value) is EventRecord and work.value.room_id is not None:
                loaded = self._load_room_aggregate(owner, work.value.room_id)
                if loaded is None:
                    raise JournalIntegrityError("room settlement Aggregate is missing")
                room = loaded[1]
            return work, room

    def _load_room_restore_view(
        self,
    ) -> tuple[tuple[RoomContinuity, RoomSnapshot | None], ...]:
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            identities = self._execute(
                "SELECT account_id, room_id FROM NioIngestRoomAggregate "
                "ORDER BY room_id"
            ).fetchall()
            states: list[tuple[RoomContinuity, RoomSnapshot | None]] = []
            for account_id, room_id in identities:
                if (
                    account_id != self.account_id
                    or type(room_id) is not str
                    or not room_id
                ):
                    raise JournalIntegrityError(
                        "persisted Aggregate room identity is invalid"
                    )
                loaded = self._load_room_aggregate(owner, room_id)
                if loaded is None:
                    raise JournalIntegrityError("persisted Aggregate disappeared")
                value = loaded[1]
                states.append((value.continuity, value.room_snapshot))
            return tuple(states)

    def next_batch(
        self,
        *,
        max_records: int = 256,
        max_canonical_bytes: int = 16 * 1024 * 1024,
    ) -> SyncBatch | None:
        if (
            type(max_records) is not int
            or not 1 <= max_records <= 256
            or type(max_canonical_bytes) is not int
            or not 1 <= max_canonical_bytes <= 16 * 1024 * 1024
        ):
            raise LocalProtocolError("delivery limits are invalid")
        with self._read():
            meta, owner, state, member, work_count = self._delivery_snapshot()
            if state.outstanding_work_id is not None:
                return self._replay_batch(owner, state, member)[0]
            if member is None:
                return None
            if SQLITE_INT_MAX in (owner.revision, state.next_sequence):
                raise LocalProtocolError("delivery sequence or revision is exhausted")
            batch = batch_from_records(
                account_id=owner.account_id,
                device_id=owner.device_id,
                consumer_generation=owner.consumer_generation,
                stream_id=owner.stream_id,
                sequence=state.next_sequence,
                created_revision=member[0][0],
                records=(member[2],),
            )
            if len(canonical_batch_payload(batch)) > max_canonical_bytes:
                raise LocalProtocolError("READY Work exceeds delivery byte limit")
            successor = DeliveryState(
                state.next_sequence + 1,
                state.acknowledged_sha256,
                member[0][2],
                member[0][0],
                member[0][1],
                batch.ref.sha256,
            )

        with self._transaction():
            self._delivery_writer_snapshot(meta, member, work_count)
            self._delivery_cas("delivery_claim_meta_cas", owner, state, successor)
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return batch

    def acknowledge_batch(self, ref: BatchRef) -> None:
        if type(ref) is not BatchRef:
            raise LocalProtocolError("acknowledgement must be a BatchRef")
        with self._read():
            meta, owner, state, member, work_count = self._delivery_snapshot()
            outstanding = state.outstanding_work_id is not None
            if outstanding:
                batch, storage, _work = self._replay_batch(owner, state, member)
            if ref.stream_id != owner.stream_id:
                raise LocalProtocolError("acknowledgement stream is invalid")
            acknowledged = state.acknowledged_sha256
            acknowledged_sequence = state.next_sequence - (2 if outstanding else 1)
            if acknowledged is not None and ref.sequence == acknowledged_sequence:
                name = f"{acknowledged_sequence}:{acknowledged.hex()}"
                expected = acknowledged, uuid5(owner.stream_id, name)
                if (ref.sha256, ref.batch_id) != expected:
                    raise ingest_errors.BatchIntegrityError(
                        "acknowledged batch changed"
                    )
                return
            if not outstanding or ref.sequence != state.next_sequence - 1:
                raise LocalProtocolError("acknowledgement is not FIFO")
            if ref != batch.ref:
                raise ingest_errors.BatchIntegrityError("outstanding batch changed")
            if owner.revision == SQLITE_INT_MAX:
                raise LocalProtocolError("delivery revision is exhausted")
            successor = DeliveryState(
                state.next_sequence, ref.sha256, None, None, None, None
            )

        with self._transaction():
            self._delivery_writer_snapshot(meta, member, work_count)
            cursor = self._transition_execute(
                "delivery_work_delete",
                "DELETE FROM NioIngestWork WHERE account_id = ? AND work_id = ?",
                (self.account_id, storage[1]),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("delivery ack Work delete failed")
            self._delivery_cas("delivery_ack_meta_cas", owner, state, successor)
            self._transition_hook("before_commit")
        self._transition_hook("commit")

    def _reconstruct_stage(
        self,
        source: SourceState,
        frame: StagedFrame,
    ) -> tuple[SourceState, StagedFrame]:
        try:
            if type(source) is not SourceState:
                raise TypeError("source must be SourceState")
            if type(frame) is not StagedFrame:
                raise TypeError("frame must be StagedFrame")
            source = SourceState(
                source.source_epoch,
                source.transport_kind,
                source.cursor_json,
                source.next_request_id,
                source.active,
            )
            frame = StagedFrame(
                frame.frame_id,
                frame.response,
                frame.staged_revision,
            )
            return source, frame
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError("source staging proposal is invalid") from error

    def _renormalized_frame(
        self,
        owner: OwnerView,
        frame: StagedFrame,
    ) -> SyncFrame:
        return _renormalized_frame(owner, frame)

    def _classic_recovery_rooms(self, frame: SyncFrame) -> tuple[str, ...]:
        if frame.origin.transport is not TransportKind.CLASSIC:
            return ()
        with self._read():
            owner = self.load_owner()
            rooms = []
            for segment in frame.room_segments:
                if segment.initial or not segment.timeline_limited:
                    continue
                loaded = self._load_room_aggregate(owner, segment.room_id)
                if loaded is not None:
                    prior = loaded[1].continuity
                    if (
                        prior.membership == "join"
                        and prior.baseline is not None
                        and prior.hydration_id is None
                    ):
                        rooms.append(segment.room_id)
            return tuple(rooms)

    def _load_classic_recovery(self) -> StagedSourceResponse | None:
        """Authenticate the frozen response paired with the current continuation."""
        with self._read():
            owner, source = self._load_stage_snapshot()
            rows = self._execute("SELECT * FROM NioIngestRecovery LIMIT 2").fetchall()
            progress = (
                recovery_progress(source.cursor_json)
                if source.transport_kind is TransportKind.CLASSIC
                else None
            )
            if not rows and progress is None:
                return None
            if len(rows) != 1 or progress is None:
                raise JournalIntegrityError("recovery input and continuation disagree")
            row = rows[0]
            try:
                if (
                    row["account_id"] != owner.account_id
                    or row["source_epoch"] != source.source_epoch
                    or type(row["request_id"]) is not int
                    or not 0 <= row["request_id"] <= source.next_request_id
                    or type(row["payload"]) is not bytes
                    or len(row["payload"]) > 24 * 1024 * 1024
                ):
                    raise ValueError("recovery source identity changed")
                payload = self._payload(
                    owner,
                    "NioIngestRecovery",
                    row["payload"],
                    row["payload_sha256"],
                    header=_canonical_internal(
                        [row["source_epoch"], row["request_id"]]
                    ),
                )
                response, reserved = _frame_response_from_envelope(
                    load_json(payload, "retained recovery input")
                )
                request = response.request
                if (
                    reserved
                    or response.recovery_json is not None
                    or request.stream_id != owner.stream_id
                    or request.transport is not TransportKind.CLASSIC
                    or request.source_epoch != row["source_epoch"]
                    or request.request_id != row["request_id"]
                    or recovery_progress(request.request_cursor_json) is not None
                    or _classic_cursor_from_json(request.request_cursor_json).next_batch
                    != _classic_cursor_from_json(source.cursor_json).next_batch
                    or progress["source_sha256"] != response.source_sha256.hex()
                ):
                    raise ValueError("retained recovery response changed")
                return response
            except (KeyError, TypeError, ValueError) as error:
                raise JournalIntegrityError(
                    "persisted recovery input is invalid"
                ) from error

    def _begin_classic_recovery(
        self, *, response: StagedSourceResponse, cursor_json: bytes
    ) -> None:
        """Freeze input and publish its initial continuation in one transaction."""
        with self._transaction():
            owner, source = self._load_stage_snapshot()
            request = response.request
            try:
                if (
                    source.transport_kind is not TransportKind.CLASSIC
                    or not source.active
                    or request.transport is not source.transport_kind
                    or request.stream_id != owner.stream_id
                    or request.source_epoch != source.source_epoch
                    or request.request_id != source.next_request_id
                    or request.request_cursor_json != source.cursor_json
                    or response.recovery_json is not None
                    or recovery_progress(source.cursor_json) is not None
                ):
                    raise ValueError("recovery input does not match current source")
                progress = recovery_progress(cursor_json)
                if progress is None:
                    raise ValueError("recovery continuation is missing")
                frame = StagedFrame(
                    _frame_id_for_response(request, response.source_sha256), response
                )
                normalized = self._renormalized_frame(owner, frame)
                if (
                    start_classic_recovery(normalized, tuple(progress["rooms"]))
                    != cursor_json
                ):
                    raise ValueError("recovery continuation does not match response")
                if self._load_classic_recovery() is not None:
                    raise ValueError("a recovery input is already retained")
                payload, digest = self._payload(
                    owner,
                    "NioIngestRecovery",
                    _canonical_internal(_frame_envelope(frame)),
                    header=_canonical_internal(
                        [request.source_epoch, request.request_id]
                    ),
                )
                if len(payload) > 24 * 1024 * 1024:
                    raise ValueError("retained recovery envelope exceeds 24 MiB")
            except (AttributeError, KeyError, TypeError, ValueError) as error:
                raise JournalIntegrityError(
                    "recovery staging proposal is invalid"
                ) from error

            updated = self._transition_execute(
                "meta_revision_epoch_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    owner.revision + 1,
                    self.account_id,
                    owner.revision,
                    str(owner.writer_epoch),
                ),
            )
            if updated.rowcount != 1:
                raise JournalConflictError("journal recovery compare-and-swap failed")
            self._execute(
                "INSERT INTO NioIngestRecovery "
                "(account_id, source_epoch, request_id, payload, payload_sha256) "
                "VALUES (?, ?, ?, ?, ?)",
                (
                    self.account_id,
                    request.source_epoch,
                    request.request_id,
                    payload,
                    digest,
                ),
            )
            self._transition_hook("recovery_insert")
            self._write_source(replace(source, cursor_json=cursor_json), owner)
            self._transition_hook("source_state_upsert")
        self._transition_hook("commit")

    def _validate_stage_relationship(
        self,
        owner: OwnerView,
        current: SourceState,
        proposed: SourceState,
        frame: StagedFrame,
    ) -> bool:
        request = frame.response.request
        if (
            current.transport_kind is not owner.transport_kind
            or proposed.transport_kind is not owner.transport_kind
            or request.transport is not owner.transport_kind
            or request.stream_id != owner.stream_id
        ):
            raise JournalIntegrityError(
                "source staging proposal does not match journal owner"
            )
        if (
            proposed.source_epoch != current.source_epoch
            or request.source_epoch != current.source_epoch
            or proposed.next_request_id != request.request_id + 1
            or proposed.active is not current.active
        ):
            raise JournalIntegrityError(
                "source staging proposal has an invalid successor relation"
            )
        if (
            proposed.cursor_json
            != self._renormalized_frame(owner, frame).candidate_cursor_json
        ):
            raise JournalIntegrityError(
                "source staging cursor does not match normalized response"
            )

        replay = current == proposed
        if replay:
            return True
        if (
            request.request_id != current.next_request_id
            or request.request_cursor_json != current.cursor_json
        ):
            raise JournalIntegrityError(
                "source staging request does not match current source"
            )
        return False

    def stage_source_response(
        self,
        *,
        source: SourceState,
        frame: StagedFrame,
        quiesce_reserved: bool = False,
    ) -> CommitResult:
        if type(quiesce_reserved) is not bool:
            raise TypeError("quiesce_reserved must be bool")
        proposed, frame = self._reconstruct_stage(source, frame)
        if (
            len(
                _canonical_internal(
                    _frame_envelope(
                        frame,
                        quiesce_reserved=quiesce_reserved,
                    )
                )
            )
            > 24 * 1024 * 1024
        ):
            raise JournalIntegrityError("staged frame envelope exceeds 24 MiB")
        request = frame.response.request
        payload_owner = self.account_id, request.stream_id, request.transport
        try:
            _frame_payload(
                frame,
                2**63 - 1,
                payload_owner,
                quiesce_reserved=quiesce_reserved,
            )
        except JournalIntegrityError:
            with self._read():
                preflight_owner = self.load_owner()
                preflight_staged = self._load_frame_with_owner(
                    frame.frame_id, preflight_owner
                )
                if (
                    preflight_staged is None
                    or preflight_staged.response != frame.response
                ):
                    _frame_payload(
                        frame,
                        preflight_owner.revision + 1,
                        payload_owner,
                        quiesce_reserved=quiesce_reserved,
                    )

        with self._transaction():
            owner, current = self._load_stage_snapshot()
            read_revision = owner.revision
            read_writer_epoch = owner.writer_epoch

            replay = self._validate_stage_relationship(
                owner,
                current,
                proposed,
                frame,
            )

            finishing_recovery = False
            if current.transport_kind is TransportKind.CLASSIC:
                progress = recovery_progress(current.cursor_json)
                if progress is not None:
                    retained = self._load_classic_recovery()
                    if (
                        retained is None
                        or frame.response.source_sha256 != retained.source_sha256
                        or frame.response.response_body != retained.response_body
                    ):
                        raise JournalIntegrityError(
                            "recovery child changed retained sync input"
                        )
                    finishing_recovery = recovery_progress(proposed.cursor_json) is None
                    if finishing_recovery and progress["phase"] != "tail":
                        raise JournalIntegrityError(
                            "recovery completed before its final phase"
                        )

            frame_ids = self._classify_frame_ids(owner)
            self._transition_hook("frame_collision_probe")
            if frame.frame_id in frame_ids:
                stored_row = self._frame_row(frame.frame_id)
                stored_state = self._decode_frame_state(
                    frame.frame_id,
                    cast("Mapping[str, object]", stored_row),
                    owner,
                )
                if type(stored_state) is StagedFrame:
                    staged_stored = stored_state
                    stored_revision = staged_stored.staged_revision
                    same_contents = staged_stored.response == frame.response
                else:
                    prepared_stored = cast("_PreparedFrameState", stored_state)
                    normalized = self._renormalized_frame(owner, frame)
                    candidate = load_json(
                        normalized.candidate_cursor_json,
                        "replayed candidate cursor",
                    )
                    compatibility_token = (
                        candidate.get("next_batch")
                        if owner.transport_kind is TransportKind.CLASSIC
                        and type(candidate) is dict
                        else None
                    )
                    stored_revision = stored_row["staged_revision"]
                    same_contents = (
                        prepared_stored.request_cursor_json
                        == frame.response.request.request_cursor_json
                        and prepared_stored.candidate_cursor_json
                        == normalized.candidate_cursor_json
                        and prepared_stored.source_sha256
                        == frame.response.source_sha256
                        and prepared_stored.compatibility_token == compatibility_token
                    )
                if not same_contents or frame.staged_revision not in (
                    0,
                    stored_revision,
                ):
                    raise JournalIntegrityError(
                        "frame_id collides with different authenticated contents"
                    )
                if not replay:
                    raise JournalIntegrityError(
                        "existing frame does not match the current source successor"
                    )
                return CommitResult(stored_revision)

            headers = self._load_authenticated_frame_headers(owner)
            if frame_ids != {header.frame_id for header in headers}:
                raise JournalIntegrityError("authenticated Frame inventory changed")
            if (
                sum(header.room_materialized_revision is None for header in headers)
                >= _MAX_DURABLE_STAGED_FRAMES
            ):
                raise JournalIntegrityError(
                    "staged frame count exceeds the 257 frame cap"
                )
            if len(headers) >= _MAX_DURABLE_FRAME_ROWS:
                raise JournalIntegrityError("Frame row count exceeds the 258 row cap")

            inventory = self._load_task3_work_inventory(owner)
            if any(
                item.frame_id == frame.frame_id
                and type(item.value) is EventRecord
                and type(item.value.origin) is SystemOrigin
                and item.value.origin.kind is SystemOriginKind.MEMBERSHIP_CHANGE
                and item.value.origin.operation_id == frame.frame_id
                for item in inventory.work
            ):
                raise JournalConflictError(
                    "staged Frame collides with a local membership operation"
                )

            if replay or frame.staged_revision != 0:
                raise JournalIntegrityError(
                    "new staged frame requires the current source predecessor"
                )

            new_revision = read_revision + 1
            cursor = self._transition_execute(
                "meta_revision_epoch_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    self.account_id,
                    read_revision,
                    str(read_writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("journal stage compare-and-swap failed")

            self._write_source(proposed, owner)
            self._transition_hook("source_state_upsert")
            try:
                self._write_frame(
                    frame,
                    new_revision,
                    owner,
                    payload_owner,
                    quiesce_reserved=quiesce_reserved,
                )
            except (sqlite3.IntegrityError, IntegrityError) as error:
                raise JournalIntegrityError("staged frame insert collided") from error
            self._transition_hook("frame_insert")
            if finishing_recovery:
                self._execute(
                    "DELETE FROM NioIngestRecovery WHERE account_id = ?",
                    (self.account_id,),
                )
                self._transition_hook("recovery_delete")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def consume_reserved_quiesce_response(self) -> CommitResult | None:
        with self._transaction():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            staged = tuple(
                header
                for header in headers
                if header.room_materialized_revision is None
            )
            if not staged:
                return None
            selected = staged[-1]
            selected_row = cast(
                "Mapping[str, object]",
                self._frame_row(selected.frame_id),
            )
            if (
                self._frame_drain_row_from_full(
                    selected_row,
                    owner,
                    authenticate=False,
                )
                != selected
            ):
                raise JournalIntegrityError("reserved Frame header snapshot changed")
            value, quiesce_reserved = self._decode_frame_state_with_reservation(
                selected.frame_id,
                selected_row,
                owner,
                drain_header_authenticated=True,
            )
            if type(value) is not StagedFrame:
                raise JournalIntegrityError("reserved Frame is not staged")
            if not quiesce_reserved:
                return None
            if owner.revision == SQLITE_INT_MAX:
                raise LocalProtocolError("quiesce reservation revision is exhausted")
            payload_owner = (
                self.account_id,
                owner.stream_id,
                owner.transport_kind,
            )
            payload, digest = _frame_payload(
                value,
                selected.staged_revision,
                payload_owner,
            )
            new_revision = owner.revision + 1
            cursor = self._transition_execute(
                "quiesce_reservation_meta_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    self.account_id,
                    owner.revision,
                    str(owner.writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError(
                    "quiesce reservation compare-and-swap failed"
                )
            proof = _frame_drain_sha256(
                owner,
                frame_id=selected.frame_id,
                source_epoch=selected.source_epoch,
                request_id=selected.request_id,
                staged_revision=selected.staged_revision,
                payload_sha256=digest,
                payload_length=len(payload),
                room_materialized_revision=None,
                callbacks_claimed_revision=None,
            )
            cursor = self._transition_execute(
                "quiesce_reservation_frame_update",
                "UPDATE NioIngestFrame SET payload = ?, payload_sha256 = ?, "
                "drain_header_sha256 = ? WHERE account_id = ? AND frame_id = ? "
                "AND source_epoch = ? AND request_id = ? AND staged_revision = ? "
                "AND payload = ? AND payload_sha256 = ? "
                "AND room_materialized_revision IS NULL "
                "AND callbacks_claimed_revision IS NULL "
                "AND drain_header_sha256 = ?",
                (
                    payload,
                    digest,
                    proof,
                    selected_row["account_id"],
                    selected_row["frame_id"],
                    selected_row["source_epoch"],
                    selected_row["request_id"],
                    selected_row["staged_revision"],
                    selected_row["payload"],
                    selected_row["payload_sha256"],
                    selected_row["drain_header_sha256"],
                ),
            )
            if cursor.rowcount != 1:
                raise JournalIntegrityError("reserved Frame update failed")
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def _record_local_membership_intent(
        self,
        *,
        operation_id: UUID,
        room_id: str,
        previous_membership: str,
        previous_epoch: int,
        current_membership: str,
    ) -> CommitResult:
        if type(room_id) is not str or not room_id:
            raise TypeError("room_id must be a nonempty str")
        intent = _LocalMembershipIntent(
            operation_id,
            previous_membership,
            previous_epoch,
            current_membership,
        )

        with self._transaction():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            if self._load_authenticated_frame_headers(owner):
                raise JournalConflictError(
                    "local membership intent requires the Frame queue to drain"
                )
            inventory = self._load_task3_work_inventory(owner)
            if inventory.work:
                raise JournalConflictError(
                    "local membership intent requires the Work queue to drain"
                )
            pending_rows = self._execute(
                "SELECT room_id FROM NioIngestRoomAggregate "
                "WHERE account_id = ? AND intent_kind = 'local_membership' "
                "ORDER BY room_id LIMIT 2",
                (self.account_id,),
            ).fetchall()
            if pending_rows:
                if len(pending_rows) != 1 or pending_rows[0][0] != room_id:
                    raise JournalConflictError(
                        "another local membership intent is pending"
                    )
                loaded_pending = self._load_room_aggregate(owner, room_id)
                if loaded_pending is None:
                    raise JournalIntegrityError(
                        "local membership intent Aggregate disappeared"
                    )
                pending_value = loaded_pending[1]
                if pending_value.pending_local_membership != intent:
                    raise JournalConflictError(
                        "local membership operation identity collides"
                    )
                return CommitResult(pending_value.updated_revision)

            loaded = self._load_room_aggregate(owner, room_id)
            if loaded is None:
                if (previous_membership, previous_epoch, current_membership) != (
                    "leave",
                    0,
                    "join",
                ):
                    raise JournalConflictError(
                        "local membership intent requires an Aggregate"
                    )
                stored_aggregate = None
                continuity = ingest_reducer.RoomContinuity(
                    room_id,
                    0,
                    "leave",
                    None,
                    None,
                    None,
                )
                next_room_sequence = 0
                snapshot = None
            else:
                stored_aggregate, aggregate = loaded
                continuity = aggregate.continuity
                next_room_sequence = aggregate.next_room_sequence
                snapshot = aggregate.room_snapshot
                if (
                    continuity.gap is not None
                    or continuity.hydration_id is not None
                    or aggregate.pending_hydration is not None
                    or aggregate.pending_local_membership is not None
                ):
                    raise JournalConflictError(
                        "local membership intent is blocked by a room barrier"
                    )
                if continuity.membership == current_membership:
                    return CommitResult(aggregate.updated_revision)
            if not _local_membership_predecessor_matches(
                continuity.membership,
                continuity.membership_epoch,
                previous_membership=previous_membership,
                previous_epoch=previous_epoch,
                current_membership=current_membership,
            ):
                raise JournalConflictError(
                    "local membership intent does not match current state"
                )
            if owner.revision == SQLITE_INT_MAX:
                raise LocalProtocolError("local membership revision is exhausted")

            new_revision = owner.revision + 1
            successor = RoomAggregateValue(
                continuity,
                next_room_sequence,
                new_revision,
                None,
                snapshot,
                intent,
            )
            aggregate_plaintext = _canonical_room_aggregate_plaintext(successor)
            aggregate_payload, aggregate_digest = self._payload(
                owner,
                "NioIngestRoomAggregate",
                aggregate_plaintext,
                header=_canonical_internal([room_id, new_revision, "local_membership"]),
            )
            cursor = self._transition_execute(
                "local_intent_meta_revision_epoch_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    self.account_id,
                    owner.revision,
                    str(owner.writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError(
                    "local membership intent compare-and-swap failed"
                )
            if stored_aggregate is None:
                try:
                    cursor = self._transition_execute(
                        "local_intent_aggregate_insert",
                        "INSERT INTO NioIngestRoomAggregate("
                        "account_id, room_id, updated_revision, intent_kind, "
                        "payload, payload_sha256) VALUES (?, ?, ?, ?, ?, ?)",
                        (
                            self.account_id,
                            room_id,
                            new_revision,
                            "local_membership",
                            aggregate_payload,
                            aggregate_digest,
                        ),
                    )
                except (sqlite3.IntegrityError, IntegrityError) as error:
                    raise JournalConflictError(
                        "local membership intent Aggregate insert collided"
                    ) from error
            else:
                cursor = self._transition_execute(
                    "local_intent_aggregate_update",
                    "UPDATE NioIngestRoomAggregate SET updated_revision = ?, "
                    "intent_kind = 'local_membership', payload = ?, "
                    "payload_sha256 = ? WHERE account_id = ? AND room_id = ? "
                    "AND updated_revision = ? AND intent_kind IS NULL",
                    (
                        new_revision,
                        aggregate_payload,
                        aggregate_digest,
                        self.account_id,
                        room_id,
                        stored_aggregate[2],
                    ),
                )
            if cursor.rowcount != 1:
                raise JournalConflictError(
                    "local membership intent Aggregate snapshot changed"
                )
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def _publish_local_membership_transition(
        self,
        *,
        operation_id: UUID,
        room_id: str,
        previous_membership: str,
        previous_epoch: int,
        current_membership: str,
    ) -> CommitResult:
        if type(operation_id) is not UUID:
            raise TypeError("operation_id must be UUID")
        if type(room_id) is not str or not room_id:
            raise TypeError("room_id must be a nonempty str")
        current_epoch = _local_membership_transition_epoch(
            previous_membership,
            previous_epoch,
            current_membership,
        )
        record_id = _local_membership_record_id(operation_id)
        source_json = _local_membership_source_json(
            previous_membership,
            previous_epoch,
            current_membership,
        )
        expected_intent = _LocalMembershipIntent(
            operation_id,
            previous_membership,
            previous_epoch,
            current_membership,
        )

        with self._transaction():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if operation_id in {header.frame_id for header in headers}:
                raise JournalConflictError(
                    "local membership operation collides with a Frame"
                )
            inventory = self._load_task3_work_inventory(owner)
            operation_work = tuple(
                item
                for item in inventory.work
                if item.frame_id == operation_id
                or type(item.value) is EventRecord
                and item.value.record_id == record_id
            )
            if operation_work:
                if len(operation_work) != 1:
                    raise JournalConflictError(
                        "local membership operation identity collides"
                    )
                item = operation_work[0]
                value = item.value
                if (
                    type(value) is not EventRecord
                    or value
                    != EventRecord(
                        record_id,
                        RecordKind.ROOM_LIFECYCLE,
                        SystemOrigin(
                            SystemOriginKind.MEMBERSHIP_CHANGE,
                            operation_id,
                        ),
                        room_id,
                        current_epoch,
                        value.room_sequence,
                        None,
                        None,
                        source_json,
                        None,
                    )
                    or item.status != "ready"
                    or item.frame_id != operation_id
                    or item.created_revision is None
                    or item.metadata is not None
                ):
                    raise JournalConflictError(
                        "local membership operation identity collides"
                    )
                return CommitResult(item.created_revision)

            loaded = self._load_room_aggregate(owner, room_id)
            if loaded is None:
                if (previous_membership, previous_epoch, current_membership) != (
                    "leave",
                    0,
                    "join",
                ):
                    raise JournalConflictError(
                        "local membership transition requires an Aggregate"
                    )
                stored_aggregate = None
                continuity = ingest_reducer.RoomContinuity(
                    room_id,
                    0,
                    "leave",
                    None,
                    None,
                    None,
                )
                next_room_sequence = 0
                pending_hydration = None
                pending_local_membership = None
                snapshot = None
            else:
                stored_aggregate, aggregate = loaded
                continuity = aggregate.continuity
                next_room_sequence = aggregate.next_room_sequence
                pending_hydration = aggregate.pending_hydration
                pending_local_membership = aggregate.pending_local_membership
                snapshot = aggregate.room_snapshot
            if pending_local_membership not in (None, expected_intent):
                raise JournalConflictError(
                    "local membership transition does not own the pending intent"
                )
            if (
                continuity.gap is not None
                or continuity.hydration_id is not None
                or pending_hydration is not None
                or any(
                    item.status == "held"
                    and type(item.value) is EventRecord
                    and item.value.room_id == room_id
                    for item in inventory.work
                )
            ):
                raise JournalConflictError(
                    "local membership transition is blocked by a room barrier"
                )
            predecessor_matches = (
                continuity.membership == previous_membership
                and continuity.membership_epoch == previous_epoch
            )
            if pending_local_membership == expected_intent:
                predecessor_matches = _local_membership_predecessor_matches(
                    continuity.membership,
                    continuity.membership_epoch,
                    previous_membership=previous_membership,
                    previous_epoch=previous_epoch,
                    current_membership=current_membership,
                )
            if not predecessor_matches:
                raise JournalConflictError(
                    "local membership transition does not match current state"
                )
            if (
                owner.revision == SQLITE_INT_MAX
                or next_room_sequence == SQLITE_INT_MAX
                or current_epoch > SQLITE_INT_MAX
            ):
                raise LocalProtocolError(
                    "local membership revision or sequence is exhausted"
                )

            new_revision = owner.revision + 1
            room_sequence = next_room_sequence
            if snapshot is not None:
                snapshot = replace(
                    snapshot,
                    membership_epoch=current_epoch,
                    own_membership=current_membership,
                )
            successor = RoomAggregateValue(
                replace(
                    continuity,
                    membership_epoch=current_epoch,
                    membership=current_membership,
                    baseline=None,
                    gap=None,
                    hydration_id=None,
                ),
                room_sequence + 1,
                new_revision,
                None,
                snapshot,
            )
            aggregate_plaintext = _canonical_room_aggregate_plaintext(successor)
            aggregate_payload, aggregate_digest = self._payload(
                owner,
                "NioIngestRoomAggregate",
                aggregate_plaintext,
                header=_canonical_internal([room_id, new_revision, None]),
            )

            event = EventRecord(
                record_id,
                RecordKind.ROOM_LIFECYCLE,
                SystemOrigin(
                    SystemOriginKind.MEMBERSHIP_CHANGE,
                    operation_id,
                ),
                room_id,
                current_epoch,
                room_sequence,
                None,
                None,
                source_json,
                None,
            )
            plaintext = _canonical_work_plaintext("event", event)
            clear = (
                record_id,
                "event",
                "ready",
                str(operation_id),
                room_id,
                current_epoch,
                room_sequence,
                new_revision,
                0,
                new_revision,
            )
            work_payload, work_digest = self._payload(
                owner,
                "NioIngestWork",
                plaintext,
                header=_canonical_internal(clear),
            )
            if (
                len(work_payload) > _MAX_WORK_PAYLOAD_BYTES
                or len(inventory.storage_rows) >= _MAX_TOTAL_WORK_COUNT
                or sum(len(cast("bytes", row[11])) for row in inventory.storage_rows)
                + len(work_payload)
                > _MAX_TOTAL_WORK_CANONICAL_BYTES
            ):
                raise LocalProtocolError(
                    "local membership Work exceeds immutable capacity"
                )

            cursor = self._transition_execute(
                "meta_revision_epoch_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    self.account_id,
                    owner.revision,
                    str(owner.writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("local membership compare-and-swap failed")
            if stored_aggregate is None:
                try:
                    cursor = self._transition_execute(
                        "aggregate_insert",
                        "INSERT INTO NioIngestRoomAggregate("
                        "account_id, room_id, updated_revision, intent_kind, "
                        "payload, payload_sha256) VALUES (?, ?, ?, NULL, ?, ?)",
                        (
                            self.account_id,
                            room_id,
                            new_revision,
                            aggregate_payload,
                            aggregate_digest,
                        ),
                    )
                except (sqlite3.IntegrityError, IntegrityError) as error:
                    raise JournalConflictError(
                        "local membership Aggregate insert collided"
                    ) from error
            else:
                cursor = self._transition_execute(
                    "aggregate_update",
                    "UPDATE NioIngestRoomAggregate SET updated_revision = ?, "
                    "intent_kind = NULL, payload = ?, payload_sha256 = ? "
                    "WHERE account_id = ? AND room_id = ? AND updated_revision = ? "
                    "AND intent_kind IS ?",
                    (
                        new_revision,
                        aggregate_payload,
                        aggregate_digest,
                        self.account_id,
                        room_id,
                        stored_aggregate[2],
                        stored_aggregate[3],
                    ),
                )
            if cursor.rowcount != 1:
                raise JournalConflictError(
                    "local membership Aggregate snapshot changed"
                )
            try:
                cursor = self._transition_execute(
                    "work_insert",
                    "INSERT INTO NioIngestWork("
                    "account_id, work_id, kind, status, frame_id, room_id, "
                    "membership_epoch, room_sequence, ready_revision, "
                    "ready_ordinal, created_revision, payload, payload_sha256) "
                    "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                    (
                        self.account_id,
                        *clear,
                        work_payload,
                        work_digest,
                    ),
                )
            except (sqlite3.IntegrityError, IntegrityError) as error:
                raise JournalIntegrityError(
                    "local membership Work insert collided"
                ) from error
            if cursor.rowcount != 1:
                raise JournalIntegrityError(
                    "local membership Work insert did not write a row"
                )
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def _clear_local_membership_intent(
        self,
        *,
        room_id: str,
        intent: _LocalMembershipIntent,
    ) -> CommitResult:
        if type(room_id) is not str or not room_id:
            raise TypeError("room_id must be a nonempty str")
        if type(intent) is not _LocalMembershipIntent:
            raise TypeError("intent must be _LocalMembershipIntent")
        with self._transaction():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            loaded = self._load_room_aggregate(owner, room_id)
            if loaded is None:
                raise JournalConflictError(
                    "local membership intent Aggregate disappeared"
                )
            stored_aggregate, aggregate = loaded
            if aggregate.pending_local_membership != intent:
                raise JournalConflictError("local membership intent changed")
            if owner.revision == SQLITE_INT_MAX:
                raise LocalProtocolError("local membership revision is exhausted")
            new_revision = owner.revision + 1
            successor = RoomAggregateValue(
                aggregate.continuity,
                aggregate.next_room_sequence,
                new_revision,
                None,
                aggregate.room_snapshot,
            )
            aggregate_plaintext = _canonical_room_aggregate_plaintext(successor)
            aggregate_payload, aggregate_digest = self._payload(
                owner,
                "NioIngestRoomAggregate",
                aggregate_plaintext,
                header=_canonical_internal([room_id, new_revision, None]),
            )
            cursor = self._transition_execute(
                "local_intent_clear_meta_revision_epoch_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    self.account_id,
                    owner.revision,
                    str(owner.writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError(
                    "local membership intent clear compare-and-swap failed"
                )
            cursor = self._transition_execute(
                "local_intent_clear_aggregate_update",
                "UPDATE NioIngestRoomAggregate SET updated_revision = ?, "
                "intent_kind = NULL, payload = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND room_id = ? AND updated_revision = ? "
                "AND intent_kind = 'local_membership'",
                (
                    new_revision,
                    aggregate_payload,
                    aggregate_digest,
                    self.account_id,
                    room_id,
                    stored_aggregate[2],
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError(
                    "local membership intent Aggregate snapshot changed"
                )
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def _has_pending_local_membership_work(self) -> bool:
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            inventory = self._load_task3_work_inventory(owner)
            return any(
                type(item.value) is EventRecord
                and type(item.value.origin) is SystemOrigin
                and item.value.origin.kind is SystemOriginKind.MEMBERSHIP_CHANGE
                for item in inventory.work
            )

    def _local_membership_work_state(
        self,
        *,
        room_id: str,
        intent: _LocalMembershipIntent,
    ) -> tuple[bool, int | None]:
        if type(room_id) is not str or not room_id:
            raise TypeError("room_id must be a nonempty str")
        if type(intent) is not _LocalMembershipIntent:
            raise TypeError("intent must be _LocalMembershipIntent")
        current_epoch = _local_membership_transition_epoch(
            intent.previous_membership,
            intent.previous_epoch,
            intent.current_membership,
        )
        record_id = _local_membership_record_id(intent.operation_id)
        source_json = _local_membership_source_json(
            intent.previous_membership,
            intent.previous_epoch,
            intent.current_membership,
        )
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            inventory = self._load_task3_work_inventory(owner)
            local_work = tuple(
                item
                for item in inventory.work
                if type(item.value) is EventRecord
                and type(item.value.origin) is SystemOrigin
                and item.value.origin.kind is SystemOriginKind.MEMBERSHIP_CHANGE
            )
            operation_work = tuple(
                item
                for item in inventory.work
                if item.frame_id == intent.operation_id
                or type(item.value) is EventRecord
                and item.value.record_id == record_id
            )
        if not operation_work:
            return bool(local_work), None
        if len(operation_work) != 1:
            raise JournalConflictError("local membership operation identity collides")
        item = operation_work[0]
        value = item.value
        if (
            type(value) is not EventRecord
            or value
            != EventRecord(
                record_id,
                RecordKind.ROOM_LIFECYCLE,
                SystemOrigin(
                    SystemOriginKind.MEMBERSHIP_CHANGE,
                    intent.operation_id,
                ),
                room_id,
                current_epoch,
                value.room_sequence,
                None,
                None,
                source_json,
                None,
            )
            or item.status != "ready"
            or item.frame_id != intent.operation_id
            or item.created_revision is None
            or item.metadata is not None
        ):
            raise JournalConflictError("local membership operation identity collides")
        return True, item.created_revision

    def _authenticate_blocking_frame(
        self,
        owner: OwnerView,
        selected: _FrameDrainRow,
    ) -> None:
        row = cast("Mapping[str, object]", self._frame_row(selected.frame_id))
        if (
            self._frame_drain_row_from_full(
                row,
                owner,
                authenticate=False,
            )
            != selected
        ):
            raise JournalIntegrityError("blocked frame header snapshot changed")
        self._decode_frame_state(
            selected.frame_id,
            row,
            owner,
            drain_header_authenticated=True,
        )

    def _ready_outbound_maintenance_with_owner(
        self,
        owner: OwnerView,
        selected: _FrameDrainRow,
    ) -> tuple[_PreparedFrameState, _PendingOutboundMaintenance] | None:
        if (
            selected.room_materialized_revision is None
            or selected.callbacks_claimed_revision is not None
        ):
            return None
        self._authenticate_blocking_frame(owner, selected)
        prepared = self._load_prepared_frame_with_owner(
            selected.frame_id,
            owner,
        )
        if prepared is None:
            return None
        if self._load_frame_work(owner, selected.frame_id) is not None:
            return None
        for index, operation in enumerate(prepared.outbound_maintenance.operations):
            if operation.state == "pending":
                return prepared, _PendingOutboundMaintenance(
                    selected.frame_id,
                    owner.stream_id,
                    owner.transport_kind,
                    selected.source_epoch,
                    selected.request_id,
                    prepared.request_cursor_json,
                    index,
                    len(prepared.outbound_maintenance.operations),
                    operation,
                )
        return None

    def _load_ready_outbound_maintenance(
        self,
    ) -> _PendingOutboundMaintenance | None:
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if not headers:
                return None
            ready = self._ready_outbound_maintenance_with_owner(owner, headers[0])
            return None if ready is None else ready[1]

    def _settle_outbound_maintenance(
        self,
        *,
        pending: _PendingOutboundMaintenance,
        apply: Callable[[], tuple[_OutboundOperation, ...]],
    ) -> CommitResult:
        if type(pending) is not _PendingOutboundMaintenance:
            raise TypeError("pending maintenance carrier is invalid")
        if not callable(apply):
            raise TypeError("maintenance apply callback must be callable")
        with self._transaction():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if not headers:
                raise JournalConflictError("outbound maintenance Frame is absent")
            selected = headers[0]
            ready = self._ready_outbound_maintenance_with_owner(owner, selected)
            if ready is None or ready[1] != pending:
                raise JournalConflictError("outbound maintenance operation changed")
            prepared, _current = ready
            selected_row = cast(
                "Mapping[str, object]",
                self._frame_row(selected.frame_id),
            )
            if (
                self._frame_drain_row_from_full(
                    selected_row,
                    owner,
                    authenticate=False,
                )
                != selected
            ):
                raise JournalIntegrityError(
                    "outbound maintenance Frame snapshot changed"
                )
            operations = list(prepared.outbound_maintenance.operations)
            operation = operations[pending.operation_index]
            operations[pending.operation_index] = _OutboundOperation(
                operation.kind,
                "settled",
                operation.body_json,
                operation.transaction_id,
                operation.event_type,
                None,
            )
            new_revision = owner.revision + 1
            follow_ups = apply()
            if type(follow_ups) is not tuple or any(
                type(operation) is not _OutboundOperation for operation in follow_ups
            ):
                raise TypeError("maintenance follow-up operations are invalid")
            operations.extend(follow_ups)
            payload, digest = _prepared_frame_payload(
                owner=(
                    self.account_id,
                    owner.stream_id,
                    owner.transport_kind,
                ),
                frame_id=selected.frame_id,
                source_epoch=selected.source_epoch,
                request_id=selected.request_id,
                staged_revision=selected.staged_revision,
                request_cursor_json=prepared.request_cursor_json,
                candidate_cursor_json=prepared.candidate_cursor_json,
                source_sha256=prepared.source_sha256,
                compatibility_token=prepared.compatibility_token,
                outbound_maintenance=_OutboundMaintenance(tuple(operations)),
            )
            cursor = self._transition_execute(
                "outbound_meta_revision_epoch_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    self.account_id,
                    owner.revision,
                    str(owner.writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError(
                    "outbound maintenance compare-and-swap failed"
                )
            proof = _frame_drain_sha256(
                owner,
                frame_id=selected.frame_id,
                source_epoch=selected.source_epoch,
                request_id=selected.request_id,
                staged_revision=selected.staged_revision,
                payload_sha256=digest,
                payload_length=len(payload),
                room_materialized_revision=selected.room_materialized_revision,
                callbacks_claimed_revision=None,
            )
            cursor = self._transition_execute(
                "outbound_frame_settle",
                "UPDATE NioIngestFrame SET payload = ?, payload_sha256 = ?, "
                "drain_header_sha256 = ? WHERE account_id = ? AND frame_id = ? "
                "AND source_epoch = ? AND request_id = ? AND staged_revision = ? "
                "AND payload = ? AND payload_sha256 = ? "
                "AND room_materialized_revision = ? "
                "AND callbacks_claimed_revision IS NULL "
                "AND drain_header_sha256 = ?",
                (
                    payload,
                    digest,
                    proof,
                    selected_row["account_id"],
                    selected_row["frame_id"],
                    selected_row["source_epoch"],
                    selected_row["request_id"],
                    selected_row["staged_revision"],
                    selected_row["payload"],
                    selected_row["payload_sha256"],
                    selected_row["room_materialized_revision"],
                    selected_row["drain_header_sha256"],
                ),
            )
            if cursor.rowcount != 1:
                raise JournalIntegrityError("outbound maintenance Frame update failed")
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def _completed_prepared_frame_with_owner(
        self,
        owner: OwnerView,
        selected: _FrameDrainRow,
    ) -> _PreparedFrameState | None:
        if selected.room_materialized_revision is None:
            return None
        self._authenticate_blocking_frame(owner, selected)
        prepared = self._load_prepared_frame_with_owner(
            selected.frame_id,
            owner,
        )
        if prepared is None:
            return None
        if self._load_frame_work(owner, selected.frame_id) is not None:
            return None
        if any(
            operation.state != "settled"
            for operation in prepared.outbound_maintenance.operations
        ):
            return None
        return prepared

    def _oldest_prepared_frame_has_work(self, frame_id: UUID) -> bool:
        if type(frame_id) is not UUID:
            raise TypeError("Frame ID must be UUID")
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if not headers:
                return False
            selected = headers[0]
            if selected.frame_id != frame_id:
                raise JournalConflictError("blocked Frame ownership changed")
            if (
                selected.room_materialized_revision is None
                or selected.callbacks_claimed_revision is not None
            ):
                return False
            self._authenticate_blocking_frame(owner, selected)
            if self._load_prepared_frame_with_owner(frame_id, owner) is None:
                return False
            return self._load_frame_work(owner, frame_id) is not None

    def _completion_claim_is_ready(self) -> bool:
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if not headers:
                return False
            selected = headers[0]
            return (
                selected.callbacks_claimed_revision is None
                and self._completed_prepared_frame_with_owner(owner, selected)
                is not None
            )

    @staticmethod
    def _frame_completion(
        owner: OwnerView,
        selected: _FrameDrainRow,
    ) -> _FrameCompletion:
        claimed_revision = selected.callbacks_claimed_revision
        if claimed_revision is None:
            raise JournalIntegrityError("Frame completion is not claimed")
        return _FrameCompletion(
            selected.frame_id,
            owner.transport_kind,
            selected.source_epoch,
            selected.request_id,
            selected.staged_revision,
            claimed_revision,
        )

    def _completion_is_partial_recovery(self, completion: _FrameCompletion) -> bool:
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if not headers or self._frame_completion(owner, headers[0]) != completion:
                raise JournalConflictError("claimed Frame completion changed")
            prepared = self._load_prepared_frame_with_owner(completion.frame_id, owner)
            if prepared is None:
                raise JournalIntegrityError("completed Frame is not prepared")
            return (
                owner.transport_kind is TransportKind.CLASSIC
                and recovery_progress(prepared.candidate_cursor_json) is not None
            )

    def _claim_frame_completion(self) -> _FrameCompletion | None:
        if not self._completion_claim_is_ready():
            return None
        completion: _FrameCompletion | None = None
        with self._transaction():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if headers:
                selected = headers[0]
                if (
                    selected.callbacks_claimed_revision is None
                    and self._completed_prepared_frame_with_owner(owner, selected)
                    is not None
                ):
                    selected_row = cast(
                        "Mapping[str, object]",
                        self._frame_row(selected.frame_id),
                    )
                    if (
                        self._frame_drain_row_from_full(
                            selected_row,
                            owner,
                            authenticate=False,
                        )
                        != selected
                    ):
                        raise JournalIntegrityError("completion Frame snapshot changed")
                    new_revision = owner.revision + 1
                    cursor = self._transition_execute(
                        "completion_meta_revision_epoch_cas",
                        "UPDATE NioIngestMeta SET revision = ? "
                        "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                        (
                            new_revision,
                            self.account_id,
                            owner.revision,
                            str(owner.writer_epoch),
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise JournalConflictError(
                            "Frame completion compare-and-swap failed"
                        )
                    proof = _frame_drain_sha256(
                        owner,
                        frame_id=selected.frame_id,
                        source_epoch=selected.source_epoch,
                        request_id=selected.request_id,
                        staged_revision=selected.staged_revision,
                        payload_sha256=selected.payload_sha256,
                        payload_length=selected.payload_length,
                        room_materialized_revision=(
                            selected.room_materialized_revision
                        ),
                        callbacks_claimed_revision=new_revision,
                    )
                    cursor = self._transition_execute(
                        "frame_completion_claim",
                        "UPDATE NioIngestFrame SET callbacks_claimed_revision = ?, "
                        "drain_header_sha256 = ? WHERE account_id = ? "
                        "AND frame_id = ? AND source_epoch = ? AND request_id = ? "
                        "AND staged_revision = ? AND payload = ? "
                        "AND payload_sha256 = ? AND room_materialized_revision = ? "
                        "AND callbacks_claimed_revision IS NULL "
                        "AND drain_header_sha256 = ?",
                        (
                            new_revision,
                            proof,
                            selected_row["account_id"],
                            selected_row["frame_id"],
                            selected_row["source_epoch"],
                            selected_row["request_id"],
                            selected_row["staged_revision"],
                            selected_row["payload"],
                            selected_row["payload_sha256"],
                            selected_row["room_materialized_revision"],
                            selected_row["drain_header_sha256"],
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise JournalIntegrityError("Frame completion claim failed")
                    self._transition_hook("before_commit")
                    completion = _FrameCompletion(
                        selected.frame_id,
                        owner.transport_kind,
                        selected.source_epoch,
                        selected.request_id,
                        selected.staged_revision,
                        new_revision,
                    )
        if completion is not None:
            self._transition_hook("commit")
        return completion

    def _load_claimed_frame_completion(self) -> _FrameCompletion | None:
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if not headers:
                return None
            selected = headers[0]
            if (
                selected.callbacks_claimed_revision is None
                or self._completed_prepared_frame_with_owner(owner, selected) is None
            ):
                return None
            return self._frame_completion(owner, selected)

    def _retire_claimed_frame(
        self,
        completion: _FrameCompletion,
    ) -> CommitResult:
        if type(completion) is not _FrameCompletion:
            raise TypeError("Frame completion carrier is invalid")
        with self._transaction():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            if not headers:
                raise JournalConflictError("claimed completion Frame is absent")
            selected = headers[0]
            if (
                self._completed_prepared_frame_with_owner(owner, selected) is None
                or self._frame_completion(owner, selected) != completion
            ):
                raise JournalConflictError("claimed Frame completion changed")
            selected_row = cast(
                "Mapping[str, object]",
                self._frame_row(selected.frame_id),
            )
            if (
                self._frame_drain_row_from_full(
                    selected_row,
                    owner,
                    authenticate=False,
                )
                != selected
            ):
                raise JournalIntegrityError("claimed Frame snapshot changed")
            new_revision = owner.revision + 1
            cursor = self._transition_execute(
                "completion_retire_meta_revision_epoch_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    self.account_id,
                    owner.revision,
                    str(owner.writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("Frame retirement compare-and-swap failed")
            cursor = self._transition_execute(
                "frame_completion_retire",
                "DELETE FROM NioIngestFrame WHERE account_id = ? "
                "AND frame_id = ? AND source_epoch = ? AND request_id = ? "
                "AND staged_revision = ? AND payload = ? AND payload_sha256 = ? "
                "AND room_materialized_revision = ? "
                "AND callbacks_claimed_revision = ? "
                "AND drain_header_sha256 = ?",
                (
                    selected_row["account_id"],
                    selected_row["frame_id"],
                    selected_row["source_epoch"],
                    selected_row["request_id"],
                    selected_row["staged_revision"],
                    selected_row["payload"],
                    selected_row["payload_sha256"],
                    selected_row["room_materialized_revision"],
                    selected_row["callbacks_claimed_revision"],
                    selected_row["drain_header_sha256"],
                ),
            )
            if cursor.rowcount != 1:
                raise JournalIntegrityError("claimed Frame retirement failed")
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def _prepare_and_materialize_oldest_frame(
        self,
        *,
        prepare: Callable[
            [SyncFrame, int, tuple[ingest_reducer.RoomContinuity, ...]],
            _PreparedIngestionFrame,
        ],
        freeze_outbound: Callable[
            [SyncFrame, _PreparedIngestionFrame], _OutboundMaintenance
        ],
        limits: MaterializerLimits,
    ) -> MaterializeResult:
        if not callable(prepare) or not callable(freeze_outbound):
            raise TypeError("owned materialization callbacks must be callable")
        if type(limits) is not MaterializerLimits:
            raise TypeError("limits must be MaterializerLimits")
        MaterializerLimits(
            limits.max_record_canonical_bytes,
            limits.max_held_work_count,
            limits.max_held_work_canonical_bytes,
            limits.max_total_work_count,
            limits.max_total_work_canonical_bytes,
        )

        with self._read():
            read_owner = self._decode_owner_row(
                cast("Mapping[str, object]", self._meta())
            )
            read_headers = self._load_authenticated_frame_headers(read_owner)
            if not read_headers:
                return MaterializeResult(MaterializeStatus.IDLE, None, None)
            read_oldest = read_headers[0]
            if (
                read_oldest.room_materialized_revision is not None
                or read_oldest.callbacks_claimed_revision is not None
            ):
                self._authenticate_blocking_frame(read_owner, read_oldest)
                return MaterializeResult(
                    MaterializeStatus.BLOCKED,
                    read_oldest.frame_id,
                    None,
                )

        result: MaterializeResult
        with self._transaction():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            headers = self._load_authenticated_frame_headers(owner)
            selected = headers[0] if headers else None
            if selected is None:
                result = MaterializeResult(MaterializeStatus.IDLE, None, None)
            elif (
                selected.room_materialized_revision is not None
                or selected.callbacks_claimed_revision is not None
            ):
                self._authenticate_blocking_frame(owner, selected)
                result = MaterializeResult(
                    MaterializeStatus.BLOCKED,
                    selected.frame_id,
                    None,
                )
            else:
                selected_row = self._frame_row(selected.frame_id)
                selected_mapping = cast("Mapping[str, object]", selected_row)
                if (
                    self._frame_drain_row_from_full(
                        selected_mapping,
                        owner,
                        authenticate=False,
                    )
                    != selected
                ):
                    raise JournalIntegrityError(
                        "selected frame header snapshot changed"
                    )
                staged = self._decode_frame_row(
                    selected.frame_id,
                    selected_mapping,
                    owner,
                    drain_header_authenticated=True,
                )
                normalized = self._renormalized_frame(owner, staged)
                aggregate_rooms = _frame_room_ids(normalized)
                aggregate_snapshot = tuple(
                    (room_id, self._load_room_aggregate(owner, room_id))
                    for room_id in aggregate_rooms
                )
                existing_aggregate_rooms = {
                    room_id
                    for room_id, loaded in aggregate_snapshot
                    if loaded is not None
                }
                aggregates = tuple(
                    loaded[1]
                    for _room_id, loaded in aggregate_snapshot
                    if loaded is not None
                )
                inventory = self._load_task3_work_inventory(owner)
                new_revision = owner.revision + 1
                continuities = tuple(aggregate.continuity for aggregate in aggregates)
                try:
                    prepared = prepare(
                        normalized,
                        selected.staged_revision,
                        continuities,
                    )
                    if type(prepared) is not _PreparedIngestionFrame:
                        raise TypeError("owned preparation returned an invalid carrier")
                    if prepared.staged_revision != selected.staged_revision:
                        raise ValueError(
                            "prepared staged revision does not match selected Frame"
                        )
                    plan = plan_prepared_frame_materialization(
                        account_id=self.account_id,
                        stream_id=owner.stream_id,
                        frame=normalized,
                        prepared=prepared,
                        aggregates=aggregates,
                        work=inventory.work,
                        revision=new_revision,
                        limits=limits,
                    )
                    outbound = freeze_outbound(normalized, prepared)
                    if type(outbound) is not _OutboundMaintenance:
                        raise TypeError("outbound freezer returned an invalid plan")
                except JournalIntegrityError:
                    raise
                except (TypeError, ValueError) as error:
                    raise JournalIntegrityError(str(error)) from error

                write_set = self._materialization_write_set(
                    owner=owner,
                    plan=plan,
                    frame_id=staged.frame_id,
                    revision=new_revision,
                    inventory=inventory,
                    existing_aggregate_rooms=existing_aggregate_rooms,
                    release_error=JournalIntegrityError,
                )

                prepared_payload, prepared_digest = _prepared_frame_payload(
                    owner=(
                        self.account_id,
                        owner.stream_id,
                        owner.transport_kind,
                    ),
                    frame_id=selected.frame_id,
                    source_epoch=selected.source_epoch,
                    request_id=selected.request_id,
                    staged_revision=selected.staged_revision,
                    request_cursor_json=prepared.request_cursor_json,
                    candidate_cursor_json=prepared.candidate_cursor_json,
                    source_sha256=prepared.source_sha256,
                    compatibility_token=prepared.compatibility_token,
                    outbound_maintenance=outbound,
                )

                cursor = self._transition_execute(
                    "meta_revision_epoch_cas",
                    "UPDATE NioIngestMeta SET revision = ? "
                    "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                    (
                        new_revision,
                        self.account_id,
                        owner.revision,
                        str(owner.writer_epoch),
                    ),
                )
                if cursor.rowcount != 1:
                    raise JournalConflictError(
                        "owned materializer compare-and-swap failed"
                    )

                self._apply_materialization_write_set(write_set)

                proof = _frame_drain_sha256(
                    owner,
                    frame_id=selected.frame_id,
                    source_epoch=selected.source_epoch,
                    request_id=selected.request_id,
                    staged_revision=selected.staged_revision,
                    payload_sha256=prepared_digest,
                    payload_length=len(prepared_payload),
                    room_materialized_revision=new_revision,
                    callbacks_claimed_revision=None,
                )
                frame_cursor = self._transition_execute(
                    "frame_prepared_retain",
                    "UPDATE NioIngestFrame SET payload = ?, payload_sha256 = ?, "
                    "room_materialized_revision = ?, drain_header_sha256 = ? "
                    "WHERE account_id = ? AND frame_id = ? AND source_epoch = ? "
                    "AND request_id = ? AND staged_revision = ? AND payload = ? "
                    "AND payload_sha256 = ? AND room_materialized_revision IS NULL "
                    "AND callbacks_claimed_revision IS NULL "
                    "AND drain_header_sha256 = ?",
                    (
                        prepared_payload,
                        prepared_digest,
                        new_revision,
                        proof,
                        selected_row["account_id"],
                        selected_row["frame_id"],
                        selected_row["source_epoch"],
                        selected_row["request_id"],
                        selected_row["staged_revision"],
                        selected_row["payload"],
                        selected_row["payload_sha256"],
                        selected_row["drain_header_sha256"],
                    ),
                )
                if frame_cursor.rowcount != 1:
                    raise JournalIntegrityError("selected frame prepared update failed")
                self._transition_hook("before_commit")
                result = MaterializeResult(
                    MaterializeStatus.MATERIALIZED,
                    selected.frame_id,
                    new_revision,
                )
        self._transition_hook("commit")
        return result

    # fmt: off
    def apply_hydration_result(self, *, result: HydrationResult) -> CommitResult | None:
        with self._read():
            owner = self._decode_owner_row(
                cast("Mapping[str, object]", self._meta())
            )
            rebuilt, event_id = revalidated_hydration_result(result, own_user_id=owner.account_id)
            room_id = rebuilt.pending.continuity.room_id
            loaded = self._load_room_aggregate(owner, room_id)
            if loaded is None:
                return None
            value = loaded[1]
            pending = value.pending_hydration
            if (value.continuity, pending) != (rebuilt.pending.continuity, rebuilt.pending.intent):
                return None
            inventory = self._load_task3_work_inventory(owner)
            selected = sorted(
                (item for item in inventory.work if item.status == "held" and type(item.value) is EventRecord and item.value.room_id == room_id and item.value.membership_epoch == value.continuity.membership_epoch),
                key=lambda item: (cast("int", cast("EventRecord", item.value).room_sequence), cast("EventRecord", item.value).record_id),
            )
            new_revision = owner.revision + 1
            successor = RoomAggregateValue(replace(value.continuity, baseline=ingest_reducer.MembershipBaseline(event_id, None), hydration_id=None), value.next_room_sequence, new_revision, None, value.room_snapshot)
            aggregate_plaintext = _canonical_room_aggregate_plaintext(successor)
            aggregate_payload, aggregate_digest = self._payload(owner, "NioIngestRoomAggregate", aggregate_plaintext, header=_canonical_internal([room_id, new_revision, None]))
            storage = {row[1]: row for row in inventory.storage_rows}
            releases: list[tuple[str, int, bytes, bytes]] = []
            for ordinal, item in enumerate(selected):
                record = cast("EventRecord", item.value)
                row = storage[record.record_id]
                clear = (*row[1:3], "ready", *row[4:8], new_revision, ordinal, row[10])
                plaintext = item.plaintext or _canonical_work_plaintext("event", record)
                payload, digest = self._payload(owner, "NioIngestWork", plaintext, header=_canonical_internal(clear))
                if len(payload) > 1024 * 1024:
                    raise ValueError("promoted Work exceeds immutable record capacity")
                releases.append((record.record_id, ordinal, payload, digest))
            total_bytes = _hydration_final_total(inventory.storage_rows, releases)
            if total_bytes > 64 * 1024 * 1024:
                raise ValueError("promoted Work exceeds immutable total capacity")

        with self._transaction():
            write_owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            if (write_owner.revision, write_owner.writer_epoch) != (owner.revision, owner.writer_epoch):
                raise JournalConflictError("hydration owner snapshot changed")
            if self._load_room_aggregate(write_owner, room_id) != loaded or self._load_task3_work_inventory(write_owner).storage_rows != inventory.storage_rows:
                raise JournalIntegrityError("hydration ownership snapshot changed")
            cursor = self._transition_execute("meta_revision_epoch_cas", "UPDATE NioIngestMeta SET revision = ? WHERE account_id = ? AND revision = ? AND writer_epoch = ?", (new_revision, self.account_id, owner.revision, str(owner.writer_epoch)))
            if cursor.rowcount != 1:
                raise JournalConflictError("hydration compare-and-swap failed")
            cursor = self._transition_execute("aggregate_update", "UPDATE NioIngestRoomAggregate SET updated_revision = ?, intent_kind = NULL, payload = ?, payload_sha256 = ? WHERE account_id = ? AND room_id = ?", (new_revision, aggregate_payload, aggregate_digest, self.account_id, room_id))
            if cursor.rowcount != 1:
                raise JournalIntegrityError("hydration Aggregate update failed")
            for work_id, ordinal, payload, digest in releases:
                cursor = self._transition_execute("work_release", "UPDATE NioIngestWork SET status = 'ready', ready_revision = ?, ready_ordinal = ?, payload = ?, payload_sha256 = ? WHERE account_id = ? AND work_id = ? AND status = 'held'", (new_revision, ordinal, payload, digest, self.account_id, work_id))
                if cursor.rowcount != 1:
                    raise JournalIntegrityError("hydration Work release failed")
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return CommitResult(new_revision)
    # fmt: on

    def close(self) -> None:
        self._frame_cache = None
        self._work_cache = None
        self._room_aggregate_cache = None
        self._owner.close()
