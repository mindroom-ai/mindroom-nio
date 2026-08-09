from __future__ import annotations

import hashlib
import hmac
import json
import os
import sqlite3
import threading
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from peewee import SqliteDatabase

from ..exceptions import LocalProtocolError
from ..ingest.config import SourceConfig, source_transport
from ..ingest.errors import (
    JournalConflictError,
    JournalIntegrityError,
)
from ..ingest.model import (
    BatchRef,
    ConsumerBinding,
    ConsumerBootstrap,
    LossBoundary,
    LossReason,
    LossRecord,
    RoomHydrationStatus,
    SyncBatch,
    SystemOrigin,
    SystemOriginKind,
    TransportKind,
)
from ..ingest.serialization import (
    _canonical_json,
    _loss_id,
    _record_to_dict,
)
from ..ingest.state import (
    AckOutcome,
    CommitResult,
    JournalTransition,
    LaneRecord,
    LaneRecordSection,
    LaneStatus,
    OwnerView,
    ReadyRecord,
    RoomLane,
    RoomState,
)
from ._sync_journal_codec import EncryptedRowCodec
from ._sync_journal_preflight import (
    FileIdentity,
    StableFileLock,
    _validate_source_cursor,
    immediate_transaction,
    open_journal_database,
)
from ._sync_journal_rows import JournalRows

if TYPE_CHECKING:
    from collections.abc import Callable


class SqliteIngestionJournal(JournalRows):
    """Direct-SQLite implementation of the version-1 ingestion journal."""

    def __init__(
        self,
        *,
        database_path: Path,
        account_id: str,
        device_id: str,
        pickle_key: str,
        connection: sqlite3.Connection,
        writer_lock: StableFileLock,
        writer_epoch: UUID,
        file_identity: FileIdentity,
        transition_statement_hook: Callable[[str], None] | None,
    ) -> None:
        self.database_path = database_path
        self.account_id = account_id
        self.device_id = device_id
        self.pickle_key = pickle_key
        self.connection = connection
        self.writer_epoch = writer_epoch
        self._writer_lock = writer_lock
        self._transition_statement_hook = transition_statement_hook
        self._closed = False
        self._file_identity = file_identity
        self._consumer_validated = False
        self._codec = EncryptedRowCodec(pickle_key, account_id, self.stream_id)
        self._ack_lock = threading.Lock()

    @classmethod
    def open(
        cls,
        database: str | os.PathLike[str] | SqliteDatabase,
        *,
        account_id: str,
        device_id: str,
        source: SourceConfig,
        pickle_key: str = "",
        sqlite_busy_timeout_ms: int = 2_000,
        statement_observer: Callable[[str], None] | None = None,
        transition_statement_hook: Callable[[str], None] | None = None,
        schema_statement_hook: Callable[[str], None] | None = None,
    ) -> SqliteIngestionJournal:
        if type(account_id) is not str or not account_id:
            raise TypeError("account_id must be a nonempty str")
        if type(device_id) is not str or not device_id:
            raise TypeError("device_id must be a nonempty str")
        if type(pickle_key) is not str:
            raise TypeError("pickle_key must be str")
        source_transport(source)
        if type(sqlite_busy_timeout_ms) is not int or sqlite_busy_timeout_ms <= 0:
            raise ValueError("sqlite_busy_timeout_ms must be positive")

        opened = open_journal_database(
            database,
            account_id=account_id,
            device_id=device_id,
            pickle_key=pickle_key,
            source=source,
            sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
            statement_observer=statement_observer,
            schema_statement_hook=schema_statement_hook,
        )
        return cls(
            database_path=opened.path,
            account_id=account_id,
            device_id=device_id,
            pickle_key=pickle_key,
            connection=opened.connection,
            writer_lock=opened.writer_lock,
            writer_epoch=opened.writer_epoch,
            file_identity=opened.file_identity,
            transition_statement_hook=transition_statement_hook,
        )

    def _assert_file_owner(self) -> None:
        self._writer_lock.assert_process_owner()
        if self._closed:
            raise LocalProtocolError("ingestion journal is closed")
        self._writer_lock.assert_identity()
        try:
            stat = os.stat(self.database_path)
        except FileNotFoundError as error:
            raise LocalProtocolError(
                "ingestion database file identity is no longer present"
            ) from error
        if (stat.st_dev, stat.st_ino) != self._file_identity:
            raise LocalProtocolError(
                "ingestion database file identity changed after lock acquisition"
            )

    def _assert_open(self) -> None:
        self._assert_file_owner()

    def set_transition_statement_hook(
        self,
        hook: Callable[[str], None] | None,
    ) -> None:
        self._assert_open()
        self._transition_statement_hook = hook

    def _transition_execute(
        self,
        label: str,
        statement: str,
        parameters: tuple[object, ...] = (),
    ) -> sqlite3.Cursor:
        cursor = self.connection.execute(statement, parameters)
        if self._transition_statement_hook is not None:
            self._transition_statement_hook(label)
        return cursor

    def _meta(self) -> sqlite3.Row:
        self._assert_open()
        row = self.connection.execute(
            "SELECT * FROM NioIngestMeta WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if row is None:
            raise LocalProtocolError("ingestion-v1 marker row disappeared")
        return row

    def load_owner(self) -> OwnerView:
        row = self._meta()
        journal_generation = row["journal_generation"]
        consumer_generation = row["consumer_generation"]
        binding_values = (
            journal_generation,
            consumer_generation,
            row["consumer_first_sequence"],
            row["baseline_rooms_sha256"],
            row["consumer_attached_revision"],
        )
        if any(value is None for value in binding_values) and any(
            value is not None for value in binding_values
        ):
            raise JournalIntegrityError("partial consumer binding in ingestion meta")
        binding = (
            ConsumerBinding(UUID(journal_generation), UUID(consumer_generation))
            if journal_generation is not None
            else None
        )
        return OwnerView(
            account_id=row["account_id"],
            device_id=row["device_id"],
            schema_version=row["schema_version"],
            stream_id=UUID(row["stream_id"]),
            transport_kind=TransportKind(row["transport_kind"]),
            binding_operation_id=UUID(row["binding_operation_id"]),
            binding=binding,
            consumer_first_sequence=row["consumer_first_sequence"],
            baseline_rooms_sha256=(
                bytes(row["baseline_rooms_sha256"])
                if row["baseline_rooms_sha256"] is not None
                else None
            ),
            consumer_attached_revision=row["consumer_attached_revision"],
            revision=row["revision"],
            writer_epoch=UUID(row["writer_epoch"]),
            next_ready_order=row["next_ready_order"],
            next_batch_sequence=row["next_batch_sequence"],
            last_acked_sequence=row["last_acked_sequence"],
        )

    def _require_attached(self) -> OwnerView:
        owner = self.load_owner()
        if owner.binding is None:
            raise LocalProtocolError("ingestion consumer is not attached")
        if not self._consumer_validated:
            raise LocalProtocolError(
                "ingestion consumer is not validated for this owner lifetime"
            )
        return owner

    @property
    def schema_version(self) -> int:
        return int(self._meta()["schema_version"])

    @property
    def stream_id(self) -> UUID:
        return UUID(self._meta()["stream_id"])

    @property
    def binding_operation_id(self) -> UUID:
        return UUID(self._meta()["binding_operation_id"])

    @property
    def next_batch_sequence(self) -> int:
        return int(self._meta()["next_batch_sequence"])

    @staticmethod
    def _canonical_baseline(room_ids: tuple[str, ...]) -> bytes:
        if type(room_ids) is not tuple or any(
            type(room_id) is not str for room_id in room_ids
        ):
            raise TypeError("baseline_room_ids must be a tuple of str")
        if room_ids != tuple(sorted(set(room_ids))):
            raise LocalProtocolError(
                "baseline_room_ids must be sorted and contain no duplicates"
            )
        return json.dumps(
            list(room_ids),
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()

    def _baseline_plan(
        self,
        consumer: ConsumerBootstrap,
        first_ready_order: int,
    ) -> tuple[tuple[RoomState, RoomLane, LossRecord, ReadyRecord], ...]:
        origin = SystemOrigin(
            SystemOriginKind.FRESH_START,
            consumer.binding_operation_id,
        )
        boundary = LossBoundary(None, None, None, None)
        detail = b'{"cause":"fresh_start","scope":"consumer_baseline"}'
        planned = []
        for offset, room_id in enumerate(consumer.baseline_room_ids):
            loss = LossRecord(
                "",
                origin,
                room_id,
                0,
                LossReason.BASELINE_LOST,
                boundary,
                detail,
            )
            loss = replace(loss, loss_id=_loss_id(self.stream_id, loss))
            planned.append(
                (
                    RoomState(room_id, 0, 0, RoomHydrationStatus.PENDING, None),
                    RoomLane(room_id, 0, LaneStatus.ACTIVE),
                    loss,
                    ReadyRecord(
                        first_ready_order + offset,
                        loss,
                        canonical_bytes=len(_canonical_json(_record_to_dict(loss))),
                    ),
                )
            )
        return tuple(planned)

    def attach_consumer(self, consumer: ConsumerBootstrap) -> None:
        if type(consumer) is not ConsumerBootstrap:
            raise TypeError("consumer must be ConsumerBootstrap")
        baseline_payload = self._canonical_baseline(consumer.baseline_room_ids)
        baseline_digest = hashlib.sha256(baseline_payload).digest()
        if not hmac.compare_digest(baseline_digest, consumer.baseline_sha256):
            raise LocalProtocolError("baseline_sha256 does not match canonical rooms")

        owner = self.load_owner()
        if consumer.binding_operation_id != owner.binding_operation_id:
            raise LocalProtocolError("binding_operation_id does not match journal")
        if owner.binding is not None:
            if owner.binding != consumer.binding:
                raise LocalProtocolError(
                    "consumer binding does not match attached owner"
                )
            if owner.consumer_first_sequence != consumer.first_sequence:
                raise LocalProtocolError("first_sequence does not match attached owner")
            assert owner.baseline_rooms_sha256 is not None
            if not hmac.compare_digest(
                owner.baseline_rooms_sha256,
                consumer.baseline_sha256,
            ):
                raise LocalProtocolError(
                    "consumer baseline does not match attached owner"
                )
            self._consumer_validated = True
            try:
                self._validate_attached_baseline(consumer)
            except BaseException:
                self._consumer_validated = False
                raise
            return
        if consumer.first_sequence != owner.next_batch_sequence:
            raise LocalProtocolError("first_sequence does not match journal")

        new_revision = owner.revision + 1
        planned = self._baseline_plan(consumer, owner.next_ready_order)

        with immediate_transaction(self.connection):
            cursor = self._transition_execute(
                "meta_attach",
                """UPDATE NioIngestMeta
                SET journal_generation = ?, consumer_generation = ?,
                    consumer_first_sequence = ?, baseline_rooms_sha256 = ?,
                    consumer_attached_revision = ?,
                    revision = ?, next_ready_order = ?
                WHERE account_id = ? AND revision = ? AND writer_epoch = ?
                  AND journal_generation IS NULL AND consumer_generation IS NULL""",
                (
                    str(consumer.binding.journal_generation),
                    str(consumer.binding.consumer_generation),
                    consumer.first_sequence,
                    consumer.baseline_sha256,
                    new_revision,
                    new_revision,
                    owner.next_ready_order + len(planned),
                    self.account_id,
                    owner.revision,
                    str(self.writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("consumer attach compare-and-swap failed")
            for state, lane, loss, ready in planned:
                self._write_room_state(state, new_revision)
                self._write_room_lane(lane, new_revision, owner.transport_kind)
                self._write_loss(loss, new_revision)
                self._write_ready(ready, new_revision)
        self._consumer_validated = True

    def _validate_attached_baseline(self, consumer: ConsumerBootstrap) -> None:
        room_ids = consumer.baseline_room_ids
        if not room_ids:
            return
        aggregates = self.load_rooms(frozenset(room_ids))
        if set(aggregates) != set(room_ids):
            raise JournalIntegrityError("attached baseline room plan is incomplete")
        for _, _, expected, _ in self._baseline_plan(consumer, 0):
            if self.load_loss(expected.loss_id) != expected:
                raise JournalIntegrityError("attached baseline loss plan is incomplete")

    @staticmethod
    def _validate_held_lane_reconciliation(
        before_lanes: dict[tuple[str, int], RoomLane],
        lane_updates: dict[tuple[str, int], RoomLane],
        inserted_records: tuple[LaneRecord, ...],
        deleted_records: tuple[LaneRecord, ...],
    ) -> None:
        inserted_by_lane: dict[tuple[str, int], list[LaneRecord]] = {}
        deleted_by_lane: dict[tuple[str, int], list[LaneRecord]] = {}
        for record in inserted_records:
            if record.key.section is LaneRecordSection.HELD:
                identity = (record.key.room_id, record.key.membership_epoch)
                inserted_by_lane.setdefault(identity, []).append(record)
        for record in deleted_records:
            if record.key.section is LaneRecordSection.HELD:
                identity = (record.key.room_id, record.key.membership_epoch)
                deleted_by_lane.setdefault(identity, []).append(record)

        changed_lanes = set(inserted_by_lane) | set(deleted_by_lane)
        if changed_lanes - set(lane_updates):
            raise JournalIntegrityError(
                "HELD lane record delta requires a same-transition RoomLane update"
            )

        for identity, lane in lane_updates.items():
            before = before_lanes.get(identity)
            old_count = before.held_record_count if before is not None else 0
            old_bytes = before.held_canonical_bytes if before is not None else 0
            old_next = before.next_held_ordinal if before is not None else 0
            inserted = tuple(inserted_by_lane.get(identity, ()))
            deleted = tuple(deleted_by_lane.get(identity, ()))
            expected_count = old_count + len(inserted) - len(deleted)
            expected_bytes = (
                old_bytes
                + sum(record.canonical_bytes for record in inserted)
                - sum(record.canonical_bytes for record in deleted)
            )
            if (
                lane.held_record_count != expected_count
                or lane.held_canonical_bytes != expected_bytes
            ):
                raise JournalIntegrityError(
                    "HELD lane count/bytes do not match lane-record deltas"
                )

            inserted_ordinals = tuple(
                sorted(record.key.record_ordinal for record in inserted)
            )
            if lane.next_held_ordinal < old_next:
                raise JournalIntegrityError("HELD lane next ordinal cannot rewind")
            expected_ordinals = tuple(range(old_next, lane.next_held_ordinal))
            if inserted_ordinals != expected_ordinals:
                raise JournalIntegrityError(
                    "HELD lane ordinals are not an exact monotonic append"
                )

    def commit(
        self,
        *,
        expected_revision: int,
        writer_epoch: UUID,
        transition: JournalTransition,
    ) -> CommitResult:
        owner = self._require_attached()
        if type(expected_revision) is not int:
            raise TypeError("expected_revision must be int")
        if type(writer_epoch) is not UUID:
            raise TypeError("writer_epoch must be UUID")
        if type(transition) is not JournalTransition:
            raise TypeError("transition must be JournalTransition")
        if owner.revision != expected_revision or writer_epoch != self.writer_epoch:
            raise JournalConflictError("journal revision or writer_epoch is stale")
        if transition.source_state is not None:
            if transition.source_state.transport_kind is not owner.transport_kind:
                raise JournalIntegrityError(
                    "source transport does not match immutable journal transport"
                )
            try:
                _validate_source_cursor(
                    owner.transport_kind,
                    transition.source_state.cursor_json,
                )
            except LocalProtocolError as error:
                raise JournalIntegrityError(str(error)) from error
        for lane in transition.room_lanes:
            self._validate_room_lane_transport(lane, owner.transport_kind)
        for lane_record in transition.lane_record_inserts:
            self._validate_lane_record_transport(
                lane_record,
                owner.transport_kind,
            )
        for ready in transition.ready_records:
            self._validate_ready_record(ready)
        for batch in transition.batches:
            self._validate_batch_integrity(batch)

        ready_orders = tuple(
            sorted(ready.ready_order for ready in transition.ready_records)
        )
        if ready_orders and ready_orders != tuple(
            range(owner.next_ready_order, owner.next_ready_order + len(ready_orders))
        ):
            raise JournalConflictError("ready_order allocation is not contiguous")
        batch_sequences = tuple(
            sorted(batch.ref.sequence for batch in transition.batches)
        )
        if batch_sequences and batch_sequences != tuple(
            range(
                owner.next_batch_sequence,
                owner.next_batch_sequence + len(batch_sequences),
            )
        ):
            raise JournalConflictError("batch sequence allocation is not contiguous")

        touched_ids = frozenset(
            [state.room_id for state in transition.room_states]
            + [lane.room_id for lane in transition.room_lanes]
            + [record.key.room_id for record in transition.lane_record_inserts]
            + [key.room_id for key in transition.lane_record_deletes]
        )
        insert_keys = tuple(record.key for record in transition.lane_record_inserts)
        insert_item_ids = tuple(
            self._validated_record_id(record.record)
            for record in transition.lane_record_inserts
        )
        for record in transition.lane_record_inserts:
            canonical_record = _canonical_json(_record_to_dict(record.record))
            if record.canonical_bytes != len(canonical_record):
                raise JournalIntegrityError(
                    "lane record canonical_bytes does not match canonical payload"
                )
        delete_keys = transition.lane_record_deletes
        if len(set(insert_keys)) != len(insert_keys):
            raise JournalIntegrityError("lane record insert keys contain duplicates")
        if len(set(insert_item_ids)) != len(insert_item_ids):
            raise JournalIntegrityError("lane record insert items contain duplicates")
        if len(set(delete_keys)) != len(delete_keys):
            raise JournalIntegrityError("lane record delete keys contain duplicates")
        if set(insert_keys) & set(delete_keys):
            raise JournalIntegrityError("lane record insert/delete keys overlap")
        lane_update_ids = tuple(
            (lane.room_id, lane.membership_epoch) for lane in transition.room_lanes
        )
        if len(set(lane_update_ids)) != len(lane_update_ids):
            raise JournalIntegrityError("room lane transition keys contain duplicates")
        lane_updates = dict(zip(lane_update_ids, transition.room_lanes, strict=True))

        new_revision = expected_revision + 1
        with immediate_transaction(self.connection):
            genuinely_new: list[LaneRecord] = []
            for lane_record, item_id in zip(
                transition.lane_record_inserts,
                insert_item_ids,
                strict=True,
            ):
                by_key = self.load_lane_record(lane_record.key)
                by_item = self._load_lane_record_by_item_id(item_id)
                if by_key is None and by_item is None:
                    genuinely_new.append(lane_record)
                elif by_key != lane_record or by_item != lane_record:
                    raise JournalIntegrityError(
                        "lane record key or item identity collides with different contents"
                    )

            deleted_records: list[LaneRecord] = []
            for key in delete_keys:
                target = self.load_lane_record(key)
                if target is None:
                    raise JournalIntegrityError("lane record delete target is missing")
                deleted_records.append(target)

            proposed: dict[str, tuple[RoomState, dict[int, RoomLane]]] = {}
            before_lanes: dict[tuple[str, int], RoomLane] = {}
            if touched_ids:
                for room_id, aggregate in self.load_rooms(touched_ids).items():
                    existing_lanes = (
                        *aggregate.retiring_lanes,
                        aggregate.active_lane,
                    )
                    proposed[room_id] = (
                        aggregate.state,
                        {lane.membership_epoch: lane for lane in existing_lanes},
                    )
                    before_lanes.update(
                        {
                            (room_id, lane.membership_epoch): lane
                            for lane in existing_lanes
                        }
                    )
                for state in transition.room_states:
                    existing = proposed.get(state.room_id)
                    proposed[state.room_id] = (
                        state,
                        existing[1] if existing else {},
                    )
                for lane in transition.room_lanes:
                    existing = proposed.get(lane.room_id)
                    if existing is None:
                        raise JournalIntegrityError(
                            "room lane transition requires room state"
                        )
                    existing[1][lane.membership_epoch] = lane
                for state, lanes in proposed.values():
                    self._validate_room_aggregate(
                        state,
                        tuple(lanes[epoch] for epoch in sorted(lanes)),
                    )
                for key in (*insert_keys, *delete_keys):
                    room = proposed.get(key.room_id)
                    if room is None or key.membership_epoch not in room[1]:
                        raise JournalIntegrityError(
                            "lane record transition requires its exact membership lane"
                        )
            self._validate_held_lane_reconciliation(
                before_lanes,
                lane_updates,
                tuple(genuinely_new),
                tuple(deleted_records),
            )

            cursor = self._transition_execute(
                "meta_revision",
                "UPDATE NioIngestMeta SET revision = ?, next_ready_order = ?, "
                "next_batch_sequence = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    owner.next_ready_order + len(ready_orders),
                    owner.next_batch_sequence + len(batch_sequences),
                    self.account_id,
                    expected_revision,
                    str(writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("journal commit compare-and-swap failed")
            if transition.source_state is not None:
                self._write_source(transition.source_state)
            for state in transition.room_states:
                self._write_room_state(state, new_revision)
            for lane in transition.room_lanes:
                self._write_room_lane(lane, new_revision, owner.transport_kind)
            for lane_record in transition.lane_record_inserts:
                self._write_lane_record(lane_record, new_revision)
            for key in transition.lane_record_deletes:
                self._delete_lane_record(key)
            for ready in transition.ready_records:
                self._write_ready(ready, new_revision)
            for frame in transition.frames:
                self._write_frame(frame, new_revision)
            for batch in transition.batches:
                self._write_batch(batch, new_revision, owner)
            for loss in transition.losses:
                self._write_loss(loss, new_revision)
            for frame_id in transition.delete_frame_ids:
                self._transition_execute(
                    "delete_frame",
                    "DELETE FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
                    (self.account_id, str(frame_id)),
                )
        return CommitResult(new_revision)

    def oldest_unacknowledged(self) -> SyncBatch | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestBatch "
            "WHERE account_id = ? AND acknowledged_revision IS NULL "
            "ORDER BY sequence LIMIT 1",
            (self.account_id,),
        ).fetchone()
        return self._decode_batch(row) if row is not None else None

    @staticmethod
    def _reference_matches(batch: SyncBatch, ref: BatchRef) -> bool:
        return (
            batch.ref.stream_id == ref.stream_id
            and batch.ref.sequence == ref.sequence
            and batch.ref.batch_id == ref.batch_id
            and hmac.compare_digest(batch.ref.sha256, ref.sha256)
        )

    def acknowledge(self, ref: BatchRef) -> AckOutcome:
        if type(ref) is not BatchRef:
            raise TypeError("ref must be BatchRef")
        if not self._ack_lock.acquire(blocking=False):
            raise LocalProtocolError("concurrent acknowledgement is not allowed")
        try:
            owner = self._require_attached()
            if ref.stream_id != owner.stream_id:
                raise JournalConflictError("batch reference stream does not match")

            if ref.sequence < owner.last_acked_sequence:
                raise JournalConflictError("stale acknowledgement")
            if ref.sequence == owner.last_acked_sequence:
                row = self.connection.execute(
                    "SELECT * FROM NioIngestBatch "
                    "WHERE account_id = ? AND sequence = ? "
                    "AND acknowledged_revision IS NOT NULL",
                    (self.account_id, ref.sequence),
                ).fetchone()
                if row is None:
                    raise JournalIntegrityError(
                        "latest acknowledged batch row is not retained"
                    )
                batch = self._decode_batch(row)
                frontier = self._meta()
                if (
                    frontier["last_acked_batch_id"] != str(batch.ref.batch_id)
                    or frontier["last_acked_sha256"] is None
                    or not hmac.compare_digest(
                        bytes(frontier["last_acked_sha256"]),
                        batch.ref.sha256,
                    )
                ):
                    raise JournalIntegrityError(
                        "acknowledgement frontier does not match retained payload"
                    )
                if not self._reference_matches(batch, ref):
                    raise JournalConflictError(
                        "acknowledgement reference does not match payload"
                    )
                return AckOutcome.ALREADY_ACKNOWLEDGED

            if ref.sequence != owner.last_acked_sequence + 1:
                raise JournalConflictError("acknowledgement is out of order")
            row = self.connection.execute(
                "SELECT * FROM NioIngestBatch "
                "WHERE account_id = ? AND sequence = ? "
                "AND acknowledged_revision IS NULL",
                (self.account_id, ref.sequence),
            ).fetchone()
            if row is None:
                raise JournalConflictError("acknowledgement is out of order")
            batch = self._decode_batch(row)
            if not self._reference_matches(batch, ref):
                raise JournalConflictError(
                    "acknowledgement reference does not match payload"
                )

            new_revision = owner.revision + 1
            with immediate_transaction(self.connection):
                cursor = self._transition_execute(
                    "ack_meta",
                    """UPDATE NioIngestMeta
                    SET revision = ?, last_acked_sequence = ?,
                        last_acked_batch_id = ?, last_acked_sha256 = ?
                    WHERE account_id = ? AND revision = ? AND writer_epoch = ?
                      AND last_acked_sequence = ?""",
                    (
                        new_revision,
                        ref.sequence,
                        str(ref.batch_id),
                        ref.sha256,
                        self.account_id,
                        owner.revision,
                        str(self.writer_epoch),
                        owner.last_acked_sequence,
                    ),
                )
                if cursor.rowcount != 1:
                    raise JournalConflictError(
                        "acknowledgement compare-and-swap failed"
                    )
                cursor = self._transition_execute(
                    "ack_batch",
                    "UPDATE NioIngestBatch SET acknowledged_revision = ? "
                    "WHERE account_id = ? AND sequence = ? "
                    "AND acknowledged_revision IS NULL",
                    (new_revision, self.account_id, ref.sequence),
                )
                if cursor.rowcount != 1:
                    raise JournalConflictError("batch acknowledgement row changed")
                if owner.last_acked_sequence:
                    cursor = self._transition_execute(
                        "ack_delete_previous",
                        "DELETE FROM NioIngestBatch "
                        "WHERE account_id = ? AND sequence = ? "
                        "AND acknowledged_revision IS NOT NULL",
                        (self.account_id, owner.last_acked_sequence),
                    )
                    if cursor.rowcount != 1:
                        raise JournalIntegrityError(
                            "previous acknowledged batch row is missing"
                        )
            return AckOutcome.ACKNOWLEDGED
        finally:
            self._ack_lock.release()

    def close(self) -> None:
        self._writer_lock.assert_process_owner()
        if self._closed:
            return
        self.connection.close()
        self._writer_lock.close()
        self._closed = True
