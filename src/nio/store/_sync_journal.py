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
from ..ingest.config import (
    MAX_BYTES_PER_BATCH,
    MAX_RECORD_BYTES,
    SourceConfig,
    source_transport,
)
from ..ingest.effects import (
    MembershipDeliveryState,
    MembershipRequest,
    RoomHydrationRequest,
)
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
from ..ingest.ports import _revalidated_staged_source_response
from ..ingest.serialization import (
    _canonical_json,
    _loss_id,
    _record_to_dict,
    canonical_batch_payload,
)
from ..ingest.state import (
    AckOutcome,
    CommitResult,
    ConsumerAttachStatus,
    JournalTransition,
    LaneRecord,
    LaneRecordKey,
    LaneRecordSection,
    LaneStatus,
    OwnerView,
    ReadyRecord,
    ReadyRecordKey,
    RoomAggregate,
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


_ATTACH_ROOM_CHUNK_SIZE = 256


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
        consumer_first_sequence = row["consumer_first_sequence"]
        baseline_value = row["baseline_rooms_sha256"]
        attached_revision = row["consumer_attached_revision"]
        last_acked_sequence = row["last_acked_sequence"]
        last_acked_batch_id = row["last_acked_batch_id"]
        last_acked_sha256 = row["last_acked_sha256"]
        required_binding_values = (
            journal_generation,
            consumer_generation,
            consumer_first_sequence,
            baseline_value,
        )
        try:
            attach_status = ConsumerAttachStatus(row["consumer_attach_status"])
            attach_ordinal = row["consumer_attach_next_room_ordinal"]
            revision = row["revision"]
            next_ready_order = row["next_ready_order"]
            next_batch_sequence = row["next_batch_sequence"]
            if any(
                type(value) is not int or value < 0
                for value in (
                    attach_ordinal,
                    revision,
                    next_ready_order,
                    next_batch_sequence,
                    last_acked_sequence,
                )
            ):
                raise ValueError("attach counters must be nonnegative integers")

            if attach_status is ConsumerAttachStatus.UNBOUND:
                if (
                    attach_ordinal != 0
                    or revision != 0
                    or next_ready_order != 0
                    or next_batch_sequence != 1
                    or last_acked_sequence != 0
                    or any(value is not None for value in required_binding_values)
                    or attached_revision is not None
                    or last_acked_batch_id is not None
                    or last_acked_sha256 is not None
                ):
                    raise ValueError("UNBOUND attach metadata is inconsistent")
                binding = None
                baseline_digest = None
            else:
                if any(value is None for value in required_binding_values):
                    raise ValueError("bound attach metadata is incomplete")
                if (
                    type(consumer_first_sequence) is not int
                    or consumer_first_sequence <= 0
                ):
                    raise ValueError("consumer first sequence is invalid")
                if type(baseline_value) is not bytes or len(baseline_value) != 32:
                    raise ValueError("baseline digest is invalid")
                binding = ConsumerBinding(
                    UUID(journal_generation),
                    UUID(consumer_generation),
                )
                baseline_digest = baseline_value
                if attach_status is ConsumerAttachStatus.ATTACHING:
                    if (
                        attached_revision is not None
                        or revision < 1
                        or attach_ordinal <= 0
                        or attach_ordinal != next_ready_order
                        or next_batch_sequence != consumer_first_sequence
                        or last_acked_sequence != consumer_first_sequence - 1
                        or last_acked_batch_id is not None
                        or last_acked_sha256 is not None
                    ):
                        raise ValueError("ATTACHING metadata is inconsistent")
                else:
                    if (
                        type(attached_revision) is not int
                        or attached_revision <= 0
                        or attached_revision > revision
                        or attach_ordinal > next_ready_order
                        or next_batch_sequence < consumer_first_sequence
                        or last_acked_sequence < consumer_first_sequence - 1
                        or last_acked_sequence >= next_batch_sequence
                    ):
                        raise ValueError("ATTACHED metadata is inconsistent")
                    if last_acked_sequence == consumer_first_sequence - 1:
                        if (
                            last_acked_batch_id is not None
                            or last_acked_sha256 is not None
                        ):
                            raise ValueError("unacknowledged frontier has an identity")
                    else:
                        if (
                            type(last_acked_batch_id) is not str
                            or type(last_acked_sha256) is not bytes
                            or len(last_acked_sha256) != 32
                        ):
                            raise ValueError("acknowledgement identity is invalid")
                        UUID(last_acked_batch_id)
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "consumer attach metadata is invalid"
            ) from error

        return OwnerView(
            account_id=row["account_id"],
            device_id=row["device_id"],
            schema_version=row["schema_version"],
            stream_id=UUID(row["stream_id"]),
            transport_kind=TransportKind(row["transport_kind"]),
            binding_operation_id=UUID(row["binding_operation_id"]),
            consumer_attach_status=attach_status,
            consumer_attach_next_room_ordinal=attach_ordinal,
            binding=binding,
            consumer_first_sequence=consumer_first_sequence,
            baseline_rooms_sha256=baseline_digest,
            consumer_attached_revision=attached_revision,
            revision=revision,
            writer_epoch=UUID(row["writer_epoch"]),
            next_ready_order=next_ready_order,
            next_batch_sequence=next_batch_sequence,
            last_acked_sequence=last_acked_sequence,
        )

    def _require_attached(self) -> OwnerView:
        owner = self.load_owner()
        if owner.consumer_attach_status is not ConsumerAttachStatus.ATTACHED:
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
        if any(not room_id for room_id in room_ids):
            raise LocalProtocolError("baseline_room_ids must contain nonempty IDs")
        if room_ids != tuple(sorted(room_ids)):
            raise LocalProtocolError("baseline_room_ids must be sorted")
        if len(set(room_ids)) != len(room_ids):
            raise LocalProtocolError("baseline_room_ids must contain no duplicates")
        return json.dumps(
            list(room_ids),
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()

    def _baseline_plan(
        self,
        consumer: ConsumerBootstrap,
        room_ids: tuple[str, ...],
        first_ready_order: int,
    ) -> tuple[tuple[RoomState, RoomLane, ReadyRecord], ...]:
        origin = SystemOrigin(
            SystemOriginKind.FRESH_START,
            consumer.binding_operation_id,
        )
        boundary = LossBoundary(None, None, None, None)
        detail = b'{"cause":"fresh_start","scope":"consumer_baseline"}'
        stream_id = self.stream_id
        planned = []
        for offset, room_id in enumerate(room_ids):
            loss = LossRecord(
                "",
                origin,
                room_id,
                0,
                LossReason.BASELINE_LOST,
                boundary,
                detail,
            )
            loss = replace(loss, loss_id=_loss_id(stream_id, loss))
            planned.append(
                (
                    RoomState(room_id, 0, 0, RoomHydrationStatus.PENDING, None),
                    RoomLane(room_id, 0, LaneStatus.ACTIVE),
                    ReadyRecord(
                        first_ready_order + offset,
                        loss,
                        canonical_bytes=len(_canonical_json(_record_to_dict(loss))),
                    ),
                )
            )
        return tuple(planned)

    def attach_consumer_step(
        self,
        consumer: ConsumerBootstrap,
    ) -> ConsumerAttachStatus:
        if type(consumer) is not ConsumerBootstrap:
            raise TypeError("consumer must be ConsumerBootstrap")
        baseline_payload = self._canonical_baseline(consumer.baseline_room_ids)
        baseline_digest = hashlib.sha256(baseline_payload).digest()
        if not hmac.compare_digest(baseline_digest, consumer.baseline_sha256):
            raise LocalProtocolError("baseline_sha256 does not match canonical rooms")

        owner = self.load_owner()
        if consumer.binding_operation_id != owner.binding_operation_id:
            raise LocalProtocolError("binding_operation_id does not match journal")
        if owner.consumer_attach_status is ConsumerAttachStatus.UNBOUND:
            if consumer.first_sequence != owner.next_batch_sequence:
                raise LocalProtocolError("first_sequence does not match journal")
            start_ordinal = 0
        else:
            assert owner.binding is not None
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
            start_ordinal = owner.consumer_attach_next_room_ordinal
            room_count = len(consumer.baseline_room_ids)
            if owner.consumer_attach_status is ConsumerAttachStatus.ATTACHING:
                if start_ordinal >= room_count:
                    raise LocalProtocolError(
                        "ATTACHING consumer ordinal must precede baseline length"
                    )
            elif start_ordinal != room_count:
                raise LocalProtocolError(
                    "ATTACHED consumer ordinal must equal baseline length"
                )

        if owner.consumer_attach_status is ConsumerAttachStatus.ATTACHED:
            self._consumer_validated = True
            return ConsumerAttachStatus.ATTACHED

        new_revision = owner.revision + 1
        end_ordinal = min(
            start_ordinal + _ATTACH_ROOM_CHUNK_SIZE,
            len(consumer.baseline_room_ids),
        )
        final = end_ordinal == len(consumer.baseline_room_ids)
        new_status = (
            ConsumerAttachStatus.ATTACHED if final else ConsumerAttachStatus.ATTACHING
        )
        planned = self._baseline_plan(
            consumer,
            consumer.baseline_room_ids[start_ordinal:end_ordinal],
            owner.next_ready_order,
        )
        new_ready_order = owner.next_ready_order + len(planned)
        attached_revision = new_revision if final else None

        if owner.consumer_attach_status is ConsumerAttachStatus.UNBOUND:
            comparison = """consumer_attach_status = 'unbound'
                  AND consumer_attach_next_room_ordinal = 0
                  AND journal_generation IS NULL
                  AND consumer_generation IS NULL
                  AND consumer_first_sequence IS NULL
                  AND baseline_rooms_sha256 IS NULL
                  AND consumer_attached_revision IS NULL"""
            comparison_parameters: tuple[object, ...] = ()
        else:
            comparison = """consumer_attach_status = 'attaching'
                  AND consumer_attach_next_room_ordinal = ?
                  AND journal_generation = ? AND consumer_generation = ?
                  AND consumer_first_sequence = ?
                  AND baseline_rooms_sha256 = ?
                  AND consumer_attached_revision IS NULL
                  AND next_ready_order = ?
                  AND next_batch_sequence = ?"""
            comparison_parameters = (
                start_ordinal,
                str(consumer.binding.journal_generation),
                str(consumer.binding.consumer_generation),
                consumer.first_sequence,
                consumer.baseline_sha256,
                owner.next_ready_order,
                consumer.first_sequence,
            )

        with immediate_transaction(self.connection):
            cursor = self._transition_execute(
                "meta_attach",
                f"""UPDATE NioIngestMeta
                SET consumer_attach_status = ?,
                    consumer_attach_next_room_ordinal = ?,
                    journal_generation = ?, consumer_generation = ?,
                    consumer_first_sequence = ?, baseline_rooms_sha256 = ?,
                    consumer_attached_revision = ?,
                    revision = ?, next_ready_order = ?
                WHERE account_id = ? AND revision = ? AND writer_epoch = ?
                  AND binding_operation_id = ? AND {comparison}""",
                (
                    new_status.value,
                    end_ordinal,
                    str(consumer.binding.journal_generation),
                    str(consumer.binding.consumer_generation),
                    consumer.first_sequence,
                    consumer.baseline_sha256,
                    attached_revision,
                    new_revision,
                    new_ready_order,
                    self.account_id,
                    owner.revision,
                    str(self.writer_epoch),
                    str(consumer.binding_operation_id),
                    *comparison_parameters,
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("consumer attach compare-and-swap failed")
            for state, lane, ready in planned:
                self._write_room_state(state, new_revision)
                self._write_room_lane(lane, new_revision, owner.transport_kind)
                self._write_ready(ready, new_revision)
        if final:
            self._consumer_validated = True
        return new_status

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

    @staticmethod
    def _sql_placeholders(count: int) -> str:
        return ",".join("?" for _ in range(count))

    def _load_ready_items(
        self,
        item_ids: tuple[str, ...],
    ) -> dict[str, ReadyRecord]:
        if not item_ids:
            return {}
        rows = self.connection.execute(
            "SELECT * FROM NioIngestReadyRecord WHERE account_id = ? "
            f"AND item_id IN ({self._sql_placeholders(len(item_ids))})",
            (self.account_id, *item_ids),
        ).fetchall()
        decoded = tuple(self._decode_ready_row(row) for row in rows)
        return {self._validated_record_id(ready.record): ready for ready in decoded}

    def _load_lane_items(
        self,
        item_ids: tuple[str, ...],
        transport_kind: TransportKind,
    ) -> dict[str, LaneRecord]:
        if not item_ids:
            return {}
        rows = self.connection.execute(
            "SELECT * FROM NioIngestLaneRecord WHERE account_id = ? "
            f"AND item_id IN ({self._sql_placeholders(len(item_ids))})",
            (self.account_id, *item_ids),
        ).fetchall()
        decoded = tuple(
            self._decode_lane_record_row(row, transport_kind) for row in rows
        )
        return {self._validated_record_id(record.record): record for record in decoded}

    def _load_batch_item_ids(self, item_ids: tuple[str, ...]) -> frozenset[str]:
        if not item_ids:
            return frozenset()
        rows = self.connection.execute(
            "SELECT item_id FROM NioIngestBatchItem WHERE account_id = ? "
            f"AND item_id IN ({self._sql_placeholders(len(item_ids))})",
            (self.account_id, *item_ids),
        ).fetchall()
        return frozenset(row["item_id"] for row in rows)

    def _validate_materialization_limits(self, batch: SyncBatch) -> None:
        for record in batch.records:
            if len(_canonical_json(_record_to_dict(record))) > MAX_RECORD_BYTES:
                raise JournalIntegrityError(
                    "materialized record exceeds immutable byte ceiling"
                )
        if len(canonical_batch_payload(batch)) > MAX_BYTES_PER_BATCH:
            raise JournalIntegrityError(
                "materialized batch exceeds immutable byte ceiling"
            )

    def _delete_materialization_sources(
        self,
        ready_item_ids: tuple[str, ...],
        lane_item_ids: tuple[str, ...],
    ) -> None:
        if ready_item_ids:
            cursor = self._transition_execute(
                "delete_ready_sources",
                "DELETE FROM NioIngestReadyRecord WHERE account_id = ? "
                f"AND item_id IN ({self._sql_placeholders(len(ready_item_ids))})",
                (self.account_id, *ready_item_ids),
            )
            if cursor.rowcount != len(ready_item_ids):
                raise JournalIntegrityError(
                    "ready materialization delete is incomplete"
                )
        if lane_item_ids:
            cursor = self._transition_execute(
                "delete_lane_sources",
                "DELETE FROM NioIngestLaneRecord WHERE account_id = ? "
                f"AND item_id IN ({self._sql_placeholders(len(lane_item_ids))})",
                (self.account_id, *lane_item_ids),
            )
            if cursor.rowcount != len(lane_item_ids):
                raise JournalIntegrityError("lane materialization delete is incomplete")

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
        if (
            owner.revision != expected_revision
            or owner.writer_epoch != writer_epoch
            or writer_epoch != self.writer_epoch
        ):
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
        for frame in transition.frames:
            try:
                replace(
                    frame,
                    response=_revalidated_staged_source_response(frame.response),
                )
            except (TypeError, ValueError) as error:
                raise JournalIntegrityError("staged frame is invalid") from error
            request = frame.response.request
            if (
                request.stream_id != owner.stream_id
                or request.transport is not owner.transport_kind
            ):
                raise JournalIntegrityError(
                    "staged frame request does not match journal owner"
                )
        for effect in (
            *transition.network_effect_inserts,
            *transition.network_effect_updates,
        ):
            self._validate_network_effect(effect, owner)
        for effect in transition.network_effect_inserts:
            if type(effect.request) is MembershipRequest and (
                effect.attempt_ordinal != 0
                or effect.membership_delivery_state is not MembershipDeliveryState.READY
                or effect.prior_delivery_uncertain is not False
            ):
                raise JournalIntegrityError(
                    "new membership effect must start READY at attempt zero"
                )
        new_revision = expected_revision + 1
        materialization = transition.batch_materialization
        if materialization is not None:
            try:
                materialization = replace(materialization)
            except (TypeError, ValueError) as error:
                raise JournalIntegrityError(
                    "batch materialization sources are invalid"
                ) from error
        batch = materialization.batch if materialization is not None else None
        batch_item_ids: tuple[str, ...] = ()
        if batch is not None:
            self._validate_batch_integrity(batch)
            self._validate_materialization_limits(batch)
            if batch.ref.sequence != owner.next_batch_sequence:
                raise JournalConflictError(
                    "batch sequence allocation is not contiguous"
                )
            if batch.created_revision != new_revision:
                raise JournalIntegrityError(
                    "batch created_revision does not match commit revision"
                )
            if (
                batch.account_id != self.account_id
                or batch.device_id != self.device_id
                or batch.consumer != owner.binding
                or batch.ref.stream_id != owner.stream_id
            ):
                raise JournalIntegrityError(
                    "batch owner identity does not match journal"
                )
            batch_item_ids = tuple(
                self._validated_record_id(record) for record in batch.records
            )
            for source, item_id in zip(
                materialization.sources,
                batch_item_ids,
                strict=True,
            ):
                if type(source) is ReadyRecordKey and source.item_id != item_id:
                    raise JournalIntegrityError(
                        "ready materialization source does not match batch position"
                    )

        ready_orders = tuple(
            sorted(ready.ready_order for ready in transition.ready_records)
        )
        if ready_orders and ready_orders != tuple(
            range(owner.next_ready_order, owner.next_ready_order + len(ready_orders))
        ):
            raise JournalConflictError("ready_order allocation is not contiguous")

        materialized_lane_keys = (
            tuple(
                source
                for source in materialization.sources
                if type(source) is LaneRecordKey
            )
            if materialization is not None
            else ()
        )
        touched_ids = set(
            [state.room_id for state in transition.room_states]
            + [lane.room_id for lane in transition.room_lanes]
            + [record.key.room_id for record in transition.lane_record_inserts]
            + [key.room_id for key in materialized_lane_keys]
            + [
                effect.request.room_id
                for effect in (
                    *transition.network_effect_inserts,
                    *transition.network_effect_updates,
                )
            ]
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
        ready_insert_ids = tuple(
            self._validated_record_id(ready.record)
            for ready in transition.ready_records
        )
        if len(set(insert_keys)) != len(insert_keys):
            raise JournalIntegrityError("lane record insert keys contain duplicates")
        if len(set(insert_item_ids)) != len(insert_item_ids):
            raise JournalIntegrityError("lane record insert items contain duplicates")
        if len(set(ready_insert_ids)) != len(ready_insert_ids):
            raise JournalIntegrityError("ready record insert items contain duplicates")
        if set(insert_item_ids) & set(ready_insert_ids):
            raise JournalIntegrityError(
                "ready and lane inserts cannot claim the same item"
            )
        inserted_item_ids = (*insert_item_ids, *ready_insert_ids)
        if set(inserted_item_ids) & set(batch_item_ids):
            raise JournalIntegrityError(
                "materialized items cannot also be inserted as ready or lane records"
            )
        lane_update_ids = tuple(
            (lane.room_id, lane.membership_epoch) for lane in transition.room_lanes
        )
        if len(set(lane_update_ids)) != len(lane_update_ids):
            raise JournalIntegrityError("room lane transition keys contain duplicates")
        lane_updates = dict(zip(lane_update_ids, transition.room_lanes, strict=True))
        room_state_ids = tuple(state.room_id for state in transition.room_states)
        if len(set(room_state_ids)) != len(room_state_ids):
            raise JournalIntegrityError("duplicate room state transition keys")

        genuinely_new_frames = transition.frames
        with immediate_transaction(self.connection):
            if transition.frames:
                frame_ids = tuple(frame.frame_id for frame in transition.frames)
                placeholders = self._sql_placeholders(len(frame_ids))
                rows = self.connection.execute(
                    "SELECT * FROM NioIngestFrame WHERE account_id = ? "
                    f"AND frame_id IN ({placeholders})",
                    (self.account_id, *(str(frame_id) for frame_id in frame_ids)),
                ).fetchall()
                existing_by_id = {UUID(row["frame_id"]): row for row in rows}
                for frame in transition.frames:
                    row = existing_by_id.get(frame.frame_id)
                    if row is None:
                        if frame.staged_revision != 0:
                            raise JournalIntegrityError(
                                "new staged frame staged_revision must be zero"
                            )
                        continue
                    try:
                        stored = self._decode_frame_row(frame.frame_id, row, owner)
                        expected = replace(
                            frame,
                            response=_revalidated_staged_source_response(
                                frame.response
                            ),
                            staged_revision=stored.staged_revision,
                        )
                    except (TypeError, ValueError) as error:
                        raise JournalIntegrityError(
                            "staged frame is invalid"
                        ) from error
                    if (
                        frame.staged_revision not in (0, stored.staged_revision)
                        or stored != expected
                    ):
                        raise JournalIntegrityError(
                            "frame_id collides with different authenticated contents"
                        )
                genuinely_new_frames = tuple(
                    frame
                    for frame in transition.frames
                    if frame.frame_id not in existing_by_id
                )

            effect_mutation_ids = (
                tuple(
                    effect.request.effect_id
                    for effect in (
                        *transition.network_effect_inserts,
                        *transition.network_effect_updates,
                    )
                )
                + transition.network_effect_deletes
            )
            existing_effects_by_id = self._load_network_effect_rows_by_ids(
                effect_mutation_ids,
                owner,
            )
            genuinely_new_effects = []
            for effect in transition.network_effect_inserts:
                existing = existing_effects_by_id.get(effect.request.effect_id)
                if existing is None:
                    genuinely_new_effects.append(effect)
                elif existing.effect != effect:
                    raise JournalIntegrityError(
                        "network effect identity collides with different contents"
                    )
            real_effect_updates = []
            for effect in transition.network_effect_updates:
                stored = existing_effects_by_id.get(effect.request.effect_id)
                if stored is None:
                    raise JournalIntegrityError(
                        "network effect update target is absent"
                    )
                if self._validate_network_effect_update_edge(stored.effect, effect):
                    real_effect_updates.append((stored, effect))
            deleted_effects = []
            for effect_id in transition.network_effect_deletes:
                stored = existing_effects_by_id.get(effect_id)
                if stored is None:
                    raise JournalIntegrityError(
                        "network effect delete target is absent"
                    )
                deleted_effects.append(stored)
            touched_ids.update(
                stored.effect.request.room_id
                for stored in existing_effects_by_id.values()
            )

            materialized_ready_ids: list[str] = []
            materialized_lane_ids: list[str] = []
            deleted_records: list[LaneRecord] = []
            if materialization is not None:
                ready_sources = self._load_ready_items(batch_item_ids)
                lane_sources = self._load_lane_items(
                    batch_item_ids,
                    owner.transport_kind,
                )
                if self._load_batch_item_ids(batch_item_ids):
                    raise JournalIntegrityError(
                        "materialized item is already owned by an unacknowledged batch"
                    )
                for source, expected_record, item_id in zip(
                    materialization.sources,
                    batch.records,
                    batch_item_ids,
                    strict=True,
                ):
                    ready_source = ready_sources.get(item_id)
                    lane_source = lane_sources.get(item_id)
                    if (ready_source is None) == (lane_source is None):
                        raise JournalIntegrityError(
                            "materialization source must have exactly one durable owner"
                        )
                    if type(source) is ReadyRecordKey:
                        if ready_source is None or lane_source is not None:
                            raise JournalIntegrityError(
                                "ready materialization source has the wrong owner"
                            )
                        if ready_source.record != expected_record:
                            raise JournalIntegrityError(
                                "ready source record does not match batch record"
                            )
                        materialized_ready_ids.append(item_id)
                    else:
                        if lane_source is None or ready_source is not None:
                            raise JournalIntegrityError(
                                "lane materialization source has the wrong owner"
                            )
                        if lane_source.key != source:
                            raise JournalIntegrityError(
                                "lane source key does not match batch position"
                            )
                        if lane_source.record != expected_record:
                            raise JournalIntegrityError(
                                "lane source record does not match batch record"
                            )
                        materialized_lane_ids.append(item_id)
                        deleted_records.append(lane_source)

            inserted_ready_rows = self._load_ready_items(inserted_item_ids)
            inserted_lane_rows = self._load_lane_items(
                inserted_item_ids,
                owner.transport_kind,
            )
            batch_owned_insert_ids = self._load_batch_item_ids(inserted_item_ids)
            if batch_owned_insert_ids:
                raise JournalIntegrityError(
                    "ready or lane insert item is already owned by a batch"
                )
            for ready, item_id in zip(
                transition.ready_records,
                ready_insert_ids,
                strict=True,
            ):
                if item_id in inserted_lane_rows:
                    raise JournalIntegrityError(
                        "ready insert item is already owned by a lane"
                    )
                existing = inserted_ready_rows.get(item_id)
                if existing is not None:
                    canonical_bytes = len(
                        _canonical_json(_record_to_dict(ready.record))
                    )
                    if existing != replace(ready, canonical_bytes=canonical_bytes):
                        raise JournalIntegrityError(
                            "ready item identity collides with different contents"
                        )

            genuinely_new: list[LaneRecord] = []
            for lane_record, item_id in zip(
                transition.lane_record_inserts,
                insert_item_ids,
                strict=True,
            ):
                if item_id in inserted_ready_rows:
                    raise JournalIntegrityError(
                        "lane insert item is already owned by ready"
                    )
                by_key = self.load_lane_record(lane_record.key)
                by_item = self._load_lane_record_by_item_id(item_id)
                if by_key is None and by_item is None:
                    genuinely_new.append(lane_record)
                elif by_key != lane_record or by_item != lane_record:
                    raise JournalIntegrityError(
                        "lane record key or item identity collides with different contents"
                    )

            proposed: dict[str, tuple[RoomState, dict[int, RoomLane]]] = {}
            before_lanes: dict[tuple[str, int], RoomLane] = {}
            if touched_ids:
                frozen_touched_ids = frozenset(touched_ids)
                current_aggregates = self._load_room_aggregates(frozen_touched_ids)
                persisted_effect_rows = self._load_network_effect_rows_for_rooms(
                    frozen_touched_ids,
                    owner,
                )
                persisted_effects = {
                    stored.effect.request.effect_id: stored.effect
                    for stored in persisted_effect_rows
                }
                self._validate_network_effect_graph(
                    current_aggregates,
                    persisted_effects,
                )
                for room_id, aggregate in current_aggregates.items():
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
                for key in (*insert_keys, *materialized_lane_keys):
                    room = proposed.get(key.room_id)
                    if room is None or key.membership_epoch not in room[1]:
                        raise JournalIntegrityError(
                            "lane record transition requires its exact membership lane"
                        )
                proposed_aggregates = {
                    state.room_id: RoomAggregate(
                        state,
                        lanes[max(lanes)],
                        tuple(lanes[epoch] for epoch in sorted(lanes)[:-1]),
                    )
                    for state, lanes in proposed.values()
                }
                proposed_effects = {
                    stored.effect.request.effect_id: stored.effect
                    for stored in persisted_effect_rows
                }
                proposed_effects.update(
                    {
                        effect.request.effect_id: effect
                        for effect in genuinely_new_effects
                    }
                )
                proposed_effects.update(
                    {
                        effect.request.effect_id: effect
                        for _, effect in real_effect_updates
                    }
                )
                for stored in deleted_effects:
                    proposed_effects.pop(stored.effect.request.effect_id)
                    if type(stored.effect.request) is RoomHydrationRequest:
                        changed_state = proposed_aggregates[
                            stored.effect.request.room_id
                        ].state
                        if stored.effect.request.room_id not in room_state_ids or (
                            changed_state.current_membership_epoch
                            == stored.effect.request.membership_epoch
                            and changed_state.hydration_status
                            is RoomHydrationStatus.PENDING
                        ):
                            raise JournalIntegrityError(
                                "hydration deletion requires a room state transition"
                            )
                new_effect_ids = {
                    effect.request.effect_id for effect in genuinely_new_effects
                }
                self._validate_network_effect_graph(
                    proposed_aggregates,
                    proposed_effects,
                    insertion_ids=frozenset(new_effect_ids),
                )
            self._validate_held_lane_reconciliation(
                before_lanes,
                lane_updates,
                tuple(genuinely_new),
                tuple(deleted_records),
            )

            deleted_frame_ids = transition.delete_frame_ids
            if deleted_frame_ids:
                placeholders = self._sql_placeholders(len(deleted_frame_ids))
                frame_rows = self.connection.execute(
                    "SELECT * FROM NioIngestFrame WHERE account_id = ? "
                    f"AND frame_id IN ({placeholders})",
                    (
                        self.account_id,
                        *(str(frame_id) for frame_id in deleted_frame_ids),
                    ),
                ).fetchall()
                if len(frame_rows) != len(deleted_frame_ids):
                    raise JournalIntegrityError("staged frame delete target is missing")
                try:
                    for row in frame_rows:
                        self._decode_frame_row(UUID(row["frame_id"]), row, owner)
                except (TypeError, ValueError) as error:
                    raise JournalIntegrityError(
                        "staged frame delete target is invalid"
                    ) from error

            has_other_mutation = (
                transition.source_state is not None
                or bool(transition.room_states)
                or bool(transition.room_lanes)
                or bool(transition.lane_record_inserts)
                or bool(transition.ready_records)
                or bool(genuinely_new_frames)
                or transition.batch_materialization is not None
                or bool(transition.delete_frame_ids)
            )
            effect_mutation_requested = bool(effect_mutation_ids)
            if not has_other_mutation and not effect_mutation_requested:
                return CommitResult(owner.revision)
            if (
                effect_mutation_requested
                and not genuinely_new_effects
                and not real_effect_updates
                and not deleted_effects
                and not has_other_mutation
            ):
                return CommitResult(owner.revision)

            prepared_effect_inserts = self._prepare_network_effect_inserts(
                tuple(genuinely_new_effects),
                new_revision,
                owner,
            )
            prepared_effect_updates = self._prepare_network_effect_state_updates(
                tuple(real_effect_updates),
                new_revision,
            )

            cursor = self._transition_execute(
                "meta_revision",
                "UPDATE NioIngestMeta SET revision = ?, next_ready_order = ?, "
                "next_batch_sequence = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    owner.next_ready_order + len(ready_orders),
                    owner.next_batch_sequence + (batch is not None),
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
            for ready in transition.ready_records:
                self._write_ready(ready, new_revision)
            for frame in genuinely_new_frames:
                self._write_frame(frame, new_revision, owner)
            self._insert_network_effects(prepared_effect_inserts)
            self._update_network_effects(
                prepared_effect_updates,
                new_revision,
            )
            self._delete_network_effects(tuple(deleted_effects))
            if batch is not None:
                self._write_batch(batch, new_revision, owner)
                self._write_batch_items(batch)
                self._delete_materialization_sources(
                    tuple(materialized_ready_ids),
                    tuple(materialized_lane_ids),
                )
            if deleted_frame_ids:
                placeholders = self._sql_placeholders(len(deleted_frame_ids))
                cursor = self._transition_execute(
                    "delete_frame",
                    "DELETE FROM NioIngestFrame WHERE account_id = ? "
                    f"AND frame_id IN ({placeholders})",
                    (
                        self.account_id,
                        *(str(frame_id) for frame_id in deleted_frame_ids),
                    ),
                )
                if cursor.rowcount != len(deleted_frame_ids):
                    raise JournalIntegrityError("staged frame delete is incomplete")
        return CommitResult(new_revision)

    def oldest_unacknowledged(self) -> SyncBatch | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestBatch "
            "WHERE account_id = ? ORDER BY sequence LIMIT 1",
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
                frontier = self._meta()
                if (
                    frontier["last_acked_batch_id"] != str(ref.batch_id)
                    or frontier["last_acked_sha256"] is None
                    or not hmac.compare_digest(
                        bytes(frontier["last_acked_sha256"]),
                        ref.sha256,
                    )
                ):
                    raise JournalConflictError(
                        "acknowledgement reference does not match frontier"
                    )
                return AckOutcome.ALREADY_ACKNOWLEDGED

            if ref.sequence != owner.last_acked_sequence + 1:
                raise JournalConflictError("acknowledgement is out of order")
            new_revision = owner.revision + 1
            with immediate_transaction(self.connection):
                row = self.connection.execute(
                    "SELECT * FROM NioIngestBatch WHERE account_id = ? "
                    "ORDER BY sequence LIMIT 1",
                    (self.account_id,),
                ).fetchone()
                if row is None:
                    raise JournalConflictError("acknowledgement is out of order")
                batch = self._decode_batch(row)
                if not self._reference_matches(batch, ref):
                    raise JournalConflictError(
                        "acknowledgement reference does not match payload"
                    )
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
                    "ack_delete_batch",
                    "DELETE FROM NioIngestBatch WHERE account_id = ? "
                    "AND sequence = ? AND batch_id = ? AND payload_sha256 = ?",
                    (
                        self.account_id,
                        ref.sequence,
                        str(ref.batch_id),
                        ref.sha256,
                    ),
                )
                if cursor.rowcount != 1:
                    raise JournalConflictError("batch acknowledgement row changed")
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
