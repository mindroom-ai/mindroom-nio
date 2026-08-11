from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import UUID

from peewee import IntegrityError, SqliteDatabase

from ..event_provenance import TimelineEventProvenance
from ..ingest import reducer as ingest_reducer
from ..ingest.classic import ClassicSource
from ..ingest.config import (
    ClassicSourceConfig,
    SlidingSourceConfig,
    SourceConfig,
    source_transport,
)
from ..ingest.errors import JournalConflictError, JournalIntegrityError
from ..ingest.model import EventRecord, RecordKind, TransportKind
from ..ingest.ports import _revalidated_staged_source_response
from ..ingest.sliding import SlidingSource
from ..ingest.source import (
    RoomSection,
    SyncFrame,
    _frame_room_ids,
    renormalize_staged_frame,
)
from ..ingest.state import CommitResult, OwnerView, SourceState, StagedFrame
from ._sync_journal_plan import _work_id, plan_frame_materialization
from ._sync_journal_preflight import (
    IngestionStoreOwner,
    open_journal_database,
)
from ._sync_journal_rows import (
    JournalRows,
    _canonical_internal,
    _canonical_room_aggregate_plaintext,
    _frame_drain_sha256,
    _frame_envelope,
    _frame_payload,
)
from ._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


# fmt: off
def _diagnostic_admits(room_id: str, frame: SyncFrame, proposal: ingest_reducer.FrameProposal) -> bool:
    segments, rooms, descriptors = frame.room_segments, proposal.room_proposals, proposal.descriptors
    if len(segments) != 1 or len(rooms) != 1:
        return False
    segment, room = segments[0], rooms[0]
    before, after = room.before, room.after
    if frame.origin.transport is not TransportKind.CLASSIC or proposal.crypto_deferred or segment.room_id != room_id or segment.section is not RoomSection.JOIN or after.room_id != room_id or after.membership != "join" or after.gap is not None or room.recovery is not None or room.retirement_epoch is not None or room.losses or room.release is not ingest_reducer.RecoveryRelease.NONE or before is not None and (before.membership_epoch != after.membership_epoch or before.membership != after.membership):
        return False
    if not descriptors:
        return room.hydration is not None and after.hydration_id is not None
    descriptor = descriptors[0]
    return len(descriptors) == 1 and descriptor.kind is RecordKind.TIMELINE and descriptor.room_id == room_id and descriptor.provenance is TimelineEventProvenance.LIVE and descriptor.route in (ingest_reducer.DescriptorRoute.HOLD_FOR_HYDRATION, ingest_reducer.DescriptorRoute.READY)
# fmt: on


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
        return self._materialize_oldest_frame(limits, None)

    # fmt: off
    def materialize_oldest_diagnostic_frame(self, *, room_id: str, limits: MaterializerLimits = MaterializerLimits()) -> MaterializeResult:
        if type(room_id) is not str or not room_id:
            raise TypeError("room_id must be a nonempty str")
        return self._materialize_oldest_frame(limits, room_id)

    def _materialize_oldest_frame(self, limits: MaterializerLimits, diagnostic_room_id: str | None) -> MaterializeResult:
        # fmt: on
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
                if diagnostic_room_id is not None:
                    for row in self._execute("SELECT room_id FROM NioIngestRoomAggregate WHERE account_id = ?", (self.account_id,)).fetchall():  # fmt: skip
                        self._load_room_aggregate(owner, cast("str", row[0]))
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
                    diagnostic_room_id is not None
                    or aggregate_rooms
                    or normalized.global_account_data_json
                    or normalized.presence_json
                )
                inventory = (
                    self._load_task3_work_inventory(owner) if needs_inventory else None
                )
                new_revision = read_revision + 1
# fmt: off
                proposal = ingest_reducer.reduce_staged_frame(owner.stream_id, normalized.frame_id, normalized, tuple(a.continuity for a in aggregates))
                if diagnostic_room_id is not None and not _diagnostic_admits(diagnostic_room_id, normalized, proposal):
                    return MaterializeResult(MaterializeStatus.BLOCKED, selected.frame_id, None)
# fmt: on
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
