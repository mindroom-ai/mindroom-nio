from __future__ import annotations

import hmac
import os
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import UUID, uuid5

from peewee import IntegrityError, SqliteDatabase

from ..exceptions import LocalProtocolError
from ..ingest import errors as ingest_errors
from ..ingest import reducer as ingest_reducer
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
from ..ingest.model import BatchRef, EventRecord, TransportKind
from ..ingest.ports import _revalidated_staged_source_response
from ..ingest.serialization import batch_from_records, canonical_batch_payload
from ..ingest.sliding import SlidingSource
from ..ingest.source import SyncFrame, _frame_room_ids, renormalize_staged_frame
from ..ingest.state import CommitResult, OwnerView, SourceState, StagedFrame
from ._sync_journal_plan import (
    _canonical_work_plaintext,
    _work_id,
    plan_frame_materialization,
)
from ._sync_journal_preflight import (
    IngestionStoreOwner,
    _decode_delivery_state,
    open_journal_database,
)
from ._sync_journal_rows import (
    JournalRows,
    _canonical_internal,
    _canonical_room_aggregate_plaintext,
    _frame_drain_sha256,
    _frame_envelope,
    _frame_payload,
    _Task3WorkInventory,
)
from ._sync_journal_values import (
    SQLITE_INT_MAX,
    DeliveryState,
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
    RoomAggregateValue,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from ..ingest.model import LossRecord, SyncBatch


def _hydration_final_total(storage, releases) -> int:  # type: ignore[no-untyped-def]
    sizes = {row[1]: len(row[11]) for row in storage}
    return sum(sizes.values()) + sum(len(row[2]) - sizes[row[0]] for row in releases)


type _DeliveryMember = tuple[
    tuple[int, int, str], tuple[object, ...], EventRecord | LossRecord
]
_DELIVERY_COLUMNS = tuple(f"delivery_{name}" for name in DeliveryState._fields)
_DELIVERY_UPDATE = (
    "UPDATE NioIngestMeta SET revision = ?, "
    + ", ".join(f"{name} = ?" for name in _DELIVERY_COLUMNS)
    + " WHERE account_id = ? AND revision = ? AND writer_epoch = ? AND "
    + " AND ".join(f"{name} IS ?" for name in _DELIVERY_COLUMNS)
)


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
            sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
            statement_observer=statement_observer,
            schema_statement_hook=schema_statement_hook,
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

    def _delivery_snapshot(self):  # type: ignore[no-untyped-def]
        row = self._meta()
        owner = self._decode_owner_row(cast("Mapping[str, object]", row))
        return (
            tuple(row),
            owner,
            _decode_delivery_state(row, owner),
            self._load_task3_work_inventory(owner),
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

    def _delivery_writer_snapshot(self, meta, inventory):  # type: ignore[no-untyped-def]
        try:
            current = self._delivery_snapshot()
        except JournalIntegrityError as error:
            raise JournalConflictError("delivery snapshot changed") from error
        if current[0] != meta or current[3].storage_rows != inventory.storage_rows:
            raise JournalConflictError("delivery snapshot changed")
        return current

    def _replay_batch(
        self,
        owner: OwnerView,
        state: DeliveryState,
        inventory: _Task3WorkInventory,
    ) -> tuple[SyncBatch, tuple[object, ...]]:
        work_id, ready_revision, ordinal, digest = cast(
            "tuple[str, int, int, bytes]", state[2:]
        )
        member = _ready_member(inventory, (ready_revision, ordinal, work_id))
        if member is None:
            raise JournalIntegrityError("claimed Work is missing or moved")
        batch = batch_from_records(
            account_id=owner.account_id,
            device_id=owner.device_id,
            consumer_generation=owner.consumer_generation,
            stream_id=owner.stream_id,
            sequence=state.next_sequence - 1,
            created_revision=ready_revision,
            records=(member[2],),
        )
        if not hmac.compare_digest(batch.ref.sha256, digest):
            raise JournalIntegrityError("claimed Work does not match batch digest")
        return batch, member[1]

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
            meta, owner, state, inventory = self._delivery_snapshot()
            if state.outstanding_work_id is not None:
                return self._replay_batch(owner, state, inventory)[0]
            member = _ready_member(inventory)
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
            current = self._delivery_writer_snapshot(meta, inventory)
            if _ready_member(current[3]) != member:
                raise JournalConflictError("delivery claim snapshot changed")
            self._delivery_cas("delivery_claim_meta_cas", owner, state, successor)
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return batch

    def acknowledge_batch(self, ref: BatchRef) -> None:
        if type(ref) is not BatchRef:
            raise LocalProtocolError("acknowledgement must be a BatchRef")
        with self._read():
            meta, owner, state, inventory = self._delivery_snapshot()
            outstanding = state.outstanding_work_id is not None
            if outstanding:
                batch, storage = self._replay_batch(owner, state, inventory)
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
            self._delivery_writer_snapshot(meta, inventory)
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
                _revalidated_staged_source_response(frame.response),
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
            return renormalize_staged_frame(adapter, frame)
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "staged response does not re-normalize"
            ) from error

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
    ) -> CommitResult:
        proposed, frame = self._reconstruct_stage(source, frame)
        if len(_canonical_internal(_frame_envelope(frame))) > 24 * 1024 * 1024:
            raise JournalIntegrityError("staged frame envelope exceeds 24 MiB")
        request = frame.response.request
        payload_owner = self.account_id, request.stream_id, request.transport
        try:
            _frame_payload(frame, 2**63 - 1, payload_owner)
        except JournalIntegrityError:
            with self._read():
                preflight_owner = self.load_owner()
                stored = self._load_frame_with_owner(frame.frame_id, preflight_owner)
                if stored is None or stored.response != frame.response:
                    _frame_payload(frame, preflight_owner.revision + 1, payload_owner)

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

            frame_ids = self._classify_frame_ids()
            self._transition_hook("frame_collision_probe")
            if frame.frame_id in frame_ids:
                stored = self._decode_frame_row(
                    frame.frame_id,
                    self._frame_row(frame.frame_id),
                    owner,
                )
                if stored.response != frame.response or frame.staged_revision not in (
                    0,
                    stored.staged_revision,
                ):
                    raise JournalIntegrityError(
                        "frame_id collides with different authenticated contents"
                    )
                if not replay:
                    raise JournalIntegrityError(
                        "existing frame does not match the current source successor"
                    )
                return CommitResult(stored.staged_revision)

            if replay or frame.staged_revision != 0:
                raise JournalIntegrityError(
                    "new staged frame requires the current source predecessor"
                )

            new_revision = read_revision + 1
            _frame_payload(frame, new_revision, payload_owner)
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
                self._write_frame(frame, new_revision, owner, payload_owner)
            except (sqlite3.IntegrityError, IntegrityError) as error:
                raise JournalIntegrityError("staged frame insert collided") from error
            self._transition_hook("frame_insert")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def materialize_oldest_frame(
        self,
        *,
        limits: MaterializerLimits,
    ) -> MaterializeResult:
        return self._materialize_oldest_frame(limits)

    # fmt: off
    def apply_hydration_result(self, *, result: HydrationResult) -> CommitResult | None:
        with self._read():
            owner = self._decode_owner_row(self._meta())
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
                (cast("EventRecord", item.value) for item in inventory.work if item.status == "held" and type(item.value) is EventRecord and item.value.room_id == room_id and item.value.membership_epoch == value.continuity.membership_epoch),
                key=lambda record: (cast("int", record.room_sequence), record.record_id),
            )
            new_revision = owner.revision + 1
            successor = RoomAggregateValue(replace(value.continuity, baseline=ingest_reducer.MembershipBaseline(event_id, None), hydration_id=None), value.next_room_sequence, new_revision, None)
            aggregate_plaintext = _canonical_room_aggregate_plaintext(successor)
            aggregate_payload, aggregate_digest = self._payload(owner, "NioIngestRoomAggregate", aggregate_plaintext, header=_canonical_internal([room_id, new_revision, None]))
            storage = {row[1]: row for row in inventory.storage_rows}
            releases: list[tuple[str, int, bytes, bytes]] = []
            for ordinal, record in enumerate(selected):
                row = storage[record.record_id]
                clear = (*row[1:3], "ready", *row[4:8], new_revision, ordinal, row[10])
                payload, digest = self._payload(owner, "NioIngestWork", _canonical_work_plaintext("event", record), header=_canonical_internal(clear))
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

    def _materialize_oldest_frame(
        self, limits: MaterializerLimits
    ) -> MaterializeResult:
        if type(limits) is not MaterializerLimits:
            raise TypeError("limits must be MaterializerLimits")
        MaterializerLimits(
            limits.max_record_canonical_bytes,
            limits.max_held_work_count,
            limits.max_held_work_canonical_bytes,
            limits.max_ready_work_count,
            limits.max_ready_work_canonical_bytes,
            limits.max_total_work_count,
            limits.max_total_work_canonical_bytes,
        )

        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            read_revision = owner.revision
            read_writer_epoch = owner.writer_epoch
            headers = self._load_authenticated_frame_headers(owner)
            selected = next(
                (
                    header
                    for header in headers
                    if header.room_materialized_revision is None
                ),
                None,
            )
            if selected is None:
                return MaterializeResult(MaterializeStatus.IDLE, None, None)

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
                raise JournalIntegrityError("selected frame header snapshot changed")
            selected_snapshot = tuple(selected_row)
            staged = self._decode_frame_row(
                selected.frame_id,
                selected_mapping,
                owner,
                drain_header_authenticated=True,
            )
            try:
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
                needs_inventory = bool(
                    aggregate_rooms
                    or normalized.global_account_data_json
                    or normalized.presence_json
                )
                inventory = (
                    self._load_task3_work_inventory(owner) if needs_inventory else None
                )
                new_revision = read_revision + 1
                continuities = tuple(aggregate.continuity for aggregate in aggregates)
                proposal = ingest_reducer.reduce_staged_frame(
                    owner.stream_id, normalized.frame_id, normalized, continuities
                )
                plan = plan_frame_materialization(
                    account_id=self.account_id,
                    stream_id=owner.stream_id,
                    frame=normalized,
                    aggregates=aggregates,
                    work=inventory.work if inventory is not None else (),
                    revision=new_revision,
                    limits=limits,
                    proposal=proposal,
                )
            except (TypeError, ValueError) as error:
                raise JournalIntegrityError(str(error)) from error
            if plan is None:
                return MaterializeResult(
                    MaterializeStatus.AT_CAPACITY,
                    selected.frame_id,
                    None,
                )

            planned_aggregates: list[tuple[object, ...]] = []
            for aggregate_value in plan.room_values:
                room_id = aggregate_value.continuity.room_id
                intent_kind = "hydration" if aggregate_value.pending_hydration else None
                plaintext = _canonical_room_aggregate_plaintext(aggregate_value)
                payload, digest = self._payload(
                    owner,
                    "NioIngestRoomAggregate",
                    plaintext,
                    header=_canonical_internal([room_id, new_revision, intent_kind]),
                )
                planned_aggregates.append(
                    (
                        self.account_id,
                        room_id,
                        new_revision,
                        intent_kind,
                        payload,
                        digest,
                    )
                )

            planned_rows: list[tuple[object, ...]] = []
            for value, plaintext, ordinal in plan.work_inserts:
                work_id = _work_id(value)
                is_event = isinstance(value, EventRecord)
                kind = "event" if is_event else "loss"
                room_sequence = (
                    value.room_sequence if isinstance(value, EventRecord) else None
                )
                status = "held" if ordinal is None else "ready"
                clear_values = (
                    work_id,
                    kind,
                    status,
                    str(staged.frame_id),
                    value.room_id,
                    value.membership_epoch,
                    room_sequence,
                    None if ordinal is None else new_revision,
                    ordinal,
                    new_revision,
                )
                payload, digest = self._payload(
                    owner,
                    "NioIngestWork",
                    plaintext,
                    header=_canonical_internal(clear_values),
                )
                planned_rows.append((self.account_id, *clear_values, payload, digest))

            stored_rows = inventory.storage_rows if inventory is not None else ()
            storage_by_id = {row[1]: row for row in stored_rows}
            planned_releases: list[tuple[object, ...]] = []
            for value, plaintext, ordinal in plan.work_releases:
                old = storage_by_id[value.record_id]
                clear = (*old[1:3], "ready", *old[4:8], new_revision, ordinal, old[10])
                payload, digest = self._payload(
                    owner,
                    "NioIngestWork",
                    plaintext,
                    header=_canonical_internal(clear),
                )
                planned_releases.append((ordinal, payload, digest, value.record_id))

        with self._transaction():
            write_owner = self._decode_owner_row(
                cast("Mapping[str, object]", self._meta())
            )
            if write_owner.revision != read_revision:
                raise JournalConflictError("journal revision is stale")
            if write_owner.writer_epoch != read_writer_epoch:
                raise JournalConflictError("journal writer_epoch is stale")
            if self._load_authenticated_frame_headers(write_owner) != headers:
                raise JournalIntegrityError("frame drain snapshot changed")
            write_selected_row = self._frame_row(selected.frame_id)
            write_selected_mapping = cast(
                "Mapping[str, object]",
                write_selected_row,
            )
            if (
                tuple(write_selected_row) != selected_snapshot
                or self._frame_drain_row_from_full(
                    write_selected_mapping,
                    write_owner,
                    authenticate=False,
                )
                != selected
            ):
                raise JournalIntegrityError("selected frame snapshot changed")
            if inventory is not None and (
                self._load_task3_work_inventory(write_owner).storage_rows
                != inventory.storage_rows
            ):
                raise JournalIntegrityError("Work inventory snapshot changed")
            if (
                tuple(
                    (room_id, self._load_room_aggregate(write_owner, room_id))
                    for room_id in aggregate_rooms
                )
                != aggregate_snapshot
            ):
                raise JournalIntegrityError("room Aggregate snapshot changed")

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
                raise JournalConflictError(
                    "journal materializer compare-and-swap failed"
                )

            try:
                for row in planned_aggregates:
                    if row[1] in existing_aggregate_rooms:
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
                            "payload, payload_sha256) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            row,
                        )
                    if aggregate_cursor.rowcount != 1:
                        raise JournalIntegrityError(
                            "Aggregate write did not affect one row"
                        )
                for row in planned_rows:
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
                for row in planned_releases:
                    work_cursor = self._transition_execute(
                        "work_release",
                        "UPDATE NioIngestWork SET status = 'ready', "
                        "ready_revision = ?, ready_ordinal = ?, payload = ?, "
                        "payload_sha256 = ? "
                        "WHERE account_id = ? AND work_id = ? AND status = 'held'",
                        (new_revision, row[0], row[1], row[2], self.account_id, row[3]),
                    )
                    if work_cursor.rowcount != 1:
                        raise JournalIntegrityError("Work release did not update a row")
            except (sqlite3.IntegrityError, IntegrityError) as error:
                raise JournalIntegrityError(
                    "planned materialization collided"
                ) from error

            row_predicate = (
                write_selected_row["account_id"],
                write_selected_row["frame_id"],
                write_selected_row["source_epoch"],
                write_selected_row["request_id"],
                write_selected_row["staged_revision"],
                write_selected_row["payload"],
                write_selected_row["payload_sha256"],
                write_selected_row["drain_header_sha256"],
            )
            snapshot_where = (
                "account_id = ? AND frame_id = ? AND source_epoch = ? "
                "AND request_id = ? AND staged_revision = ? "
                "AND payload = ? AND payload_sha256 = ? "
                "AND room_materialized_revision IS NULL "
                "AND drain_header_sha256 = ?"
            )
            if plan.crypto_deferred:
                proof = _frame_drain_sha256(write_owner, (*selected[2:8], new_revision))
                frame_cursor = self._transition_execute(
                    "frame_crypto_retain",
                    "UPDATE NioIngestFrame SET room_materialized_revision = ?, "
                    "drain_header_sha256 = ? WHERE " + snapshot_where,
                    (new_revision, proof, *row_predicate),
                )
            else:
                frame_cursor = self._transition_execute(
                    "frame_delete",
                    "DELETE FROM NioIngestFrame WHERE " + snapshot_where,
                    row_predicate,
                )
            if frame_cursor.rowcount != 1:
                raise JournalIntegrityError("selected frame snapshot update failed")
            self._transition_hook("before_commit")
        self._transition_hook("commit")
        return MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            new_revision,
        )

    def close(self) -> None:
        self._owner.close()
