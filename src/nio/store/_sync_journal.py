from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING, cast
from uuid import UUID

from peewee import IntegrityError, SqliteDatabase

from ..ingest.classic import ClassicSource
from ..ingest.config import (
    ClassicSourceConfig,
    SlidingSourceConfig,
    SourceConfig,
    source_transport,
)
from ..ingest.errors import JournalConflictError, JournalIntegrityError
from ..ingest.model import TransportKind
from ..ingest.ports import _revalidated_staged_source_response
from ..ingest.sliding import SlidingSource
from ..ingest.source import (
    MAX_ENCRYPTED_STAGED_FRAME_ENVELOPE_BYTES,
    SyncFrame,
    _frame_room_ids,
    renormalize_staged_frame,
)
from ..ingest.state import CommitResult, OwnerView, SourceState, StagedFrame
from ._sync_journal_codec import EncryptedRowCodec
from ._sync_journal_plan import plan_frame_materialization
from ._sync_journal_preflight import (
    IngestionStoreOwner,
    open_journal_database,
)
from ._sync_journal_rows import (
    JournalRows,
    _canonical_internal,
    _canonical_room_aggregate_plaintext,
    _frame_drain_header,
    _frame_envelope,
)
from ._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
)

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping


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
        self._codec = EncryptedRowCodec(pickle_key, account_id, stream_id)

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
        expected_revision: int,
        writer_epoch: UUID,
        source: SourceState,
        frame: StagedFrame,
    ) -> CommitResult:
        if type(expected_revision) is not int:
            raise TypeError("expected_revision must be int")
        if expected_revision < 0:
            raise ValueError("expected_revision must be nonnegative")
        if type(writer_epoch) is not UUID:
            raise TypeError("writer_epoch must be UUID")

        proposed, frame = self._reconstruct_stage(source, frame)
        if (
            len(_canonical_internal(_frame_envelope(frame))) + 29
            > MAX_ENCRYPTED_STAGED_FRAME_ENVELOPE_BYTES
        ):
            raise JournalIntegrityError("staged frame envelope exceeds 24 MiB")

        with self._transaction():
            owner, current = self._load_stage_snapshot()
            if owner.revision != expected_revision:
                raise JournalConflictError("journal revision is stale")
            if owner.writer_epoch != writer_epoch:
                raise JournalConflictError("journal writer_epoch is stale")

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

            new_revision = owner.revision + 1
            cursor = self._transition_execute(
                "meta_revision_epoch_cas",
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    self.account_id,
                    expected_revision,
                    str(writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("journal stage compare-and-swap failed")

            self._write_source(proposed)
            self._transition_hook("source_state_upsert")
            try:
                self._write_frame(frame, new_revision)
            except (sqlite3.IntegrityError, IntegrityError) as error:
                raise JournalIntegrityError("staged frame insert collided") from error
            self._transition_hook("frame_insert")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def materialize_oldest_frame(
        self,
        *,
        expected_revision: int,
        writer_epoch: UUID,
        limits: MaterializerLimits,
    ) -> MaterializeResult:
        if type(expected_revision) is not int:
            raise TypeError("expected_revision must be int")
        if expected_revision < 0:
            raise ValueError("expected_revision must be nonnegative")
        if type(writer_epoch) is not UUID:
            raise TypeError("writer_epoch must be UUID")
        if type(limits) is not MaterializerLimits:
            raise TypeError("limits must be MaterializerLimits")
        MaterializerLimits(
            limits.max_record_canonical_bytes,
            limits.max_held_records_per_room,
            limits.max_held_canonical_bytes_per_room,
            limits.max_ready_work_count,
            limits.max_ready_work_canonical_bytes,
            limits.max_total_work_count,
            limits.max_total_work_canonical_bytes,
        )

        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            if owner.revision != expected_revision:
                raise JournalConflictError("journal revision is stale")
            if owner.writer_epoch != writer_epoch:
                raise JournalConflictError("journal writer_epoch is stale")
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
                new_revision = expected_revision + 1
                plan = plan_frame_materialization(
                    stream_id=owner.stream_id,
                    frame=normalized,
                    aggregates=aggregates,
                    work=inventory.work if inventory is not None else (),
                    revision=new_revision,
                    limits=limits,
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
                plaintext = _canonical_room_aggregate_plaintext(aggregate_value)
                ciphertext, digest = self._codec.seal(
                    "NioIngestRoomAggregate",
                    (room_id,),
                    plaintext,
                    header=_canonical_internal([room_id, new_revision, "hydration"]),
                )
                planned_aggregates.append(
                    (
                        self.account_id,
                        room_id,
                        new_revision,
                        "hydration",
                        ciphertext,
                        digest,
                    )
                )

            planned_rows: list[tuple[object, ...]] = []
            for value, plaintext, ordinal in plan.work_inserts:
                status = "held" if ordinal is None else "ready"
                clear_values = (
                    value.record_id,
                    "event",
                    status,
                    str(staged.frame_id),
                    value.room_id,
                    value.membership_epoch,
                    value.room_sequence,
                    None if ordinal is None else new_revision,
                    ordinal,
                    new_revision,
                )
                ciphertext, digest = self._codec.seal(
                    "NioIngestWork",
                    (value.record_id,),
                    plaintext,
                    header=_canonical_internal([self.account_id, *clear_values]),
                )
                planned_rows.append(
                    (self.account_id, *clear_values, ciphertext, digest)
                )

        with self._transaction():
            write_owner = self._decode_owner_row(
                cast("Mapping[str, object]", self._meta())
            )
            if write_owner.revision != expected_revision:
                raise JournalConflictError("journal revision is stale")
            if write_owner.writer_epoch != writer_epoch:
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
                    expected_revision,
                    str(writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError(
                    "journal materializer compare-and-swap failed"
                )

            try:
                for row in planned_aggregates:
                    if row[1] in existing_aggregate_rooms:
                        aggregate_cursor = self._execute(
                            "UPDATE NioIngestRoomAggregate SET updated_revision = ?, "
                            "intent_kind = ?, payload_ciphertext = ?, "
                            "payload_sha256 = ? WHERE account_id = ? AND room_id = ?",
                            (*row[2:], row[0], row[1]),
                        )
                    else:
                        aggregate_cursor = self._execute(
                            "INSERT INTO NioIngestRoomAggregate("
                            "account_id, room_id, updated_revision, intent_kind, "
                            "payload_ciphertext, payload_sha256) "
                            "VALUES (?, ?, ?, ?, ?, ?)",
                            row,
                        )
                    if aggregate_cursor.rowcount != 1:
                        raise JournalIntegrityError(
                            "Aggregate write did not affect one row"
                        )
                for row in planned_rows:
                    work_cursor = self._execute(
                        "INSERT INTO NioIngestWork("
                        "account_id, work_id, kind, status, frame_id, room_id, "
                        "membership_epoch, room_sequence, ready_revision, "
                        "ready_ordinal, created_revision, payload_ciphertext, "
                        "payload_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, "
                        "?, ?, ?, ?)",
                        row,
                    )
                    if work_cursor.rowcount != 1:
                        raise JournalIntegrityError("Work insert did not write a row")
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
                write_selected_row["payload_ciphertext"],
                write_selected_row["payload_sha256"],
                write_selected_row["drain_header_ciphertext"],
            )
            snapshot_where = (
                "account_id = ? AND frame_id = ? AND source_epoch = ? "
                "AND request_id = ? AND staged_revision = ? "
                "AND payload_ciphertext = ? AND payload_sha256 = ? "
                "AND room_materialized_revision IS NULL "
                "AND drain_header_ciphertext = ?"
            )
            if plan.crypto_deferred:
                proof = self._codec.encrypt(
                    "NioIngestFrameDrainHeader",
                    (selected.frame_id,),
                    b"",
                    header=_frame_drain_header(
                        selected.source_epoch,
                        selected.request_id,
                        selected.staged_revision,
                        selected.payload_sha256,
                        selected.payload_ciphertext_length,
                        new_revision,
                    ),
                )
                frame_cursor = self._transition_execute(
                    "frame_crypto_retain",
                    "UPDATE NioIngestFrame SET room_materialized_revision = ?, "
                    "drain_header_ciphertext = ? WHERE " + snapshot_where,
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
        self._transition_hook("commit")
        return MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            new_revision,
        )

    def close(self) -> None:
        self._owner.close()
