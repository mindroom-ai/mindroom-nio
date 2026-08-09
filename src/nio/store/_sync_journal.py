from __future__ import annotations

import os
import sqlite3
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from peewee import SqliteDatabase

from ..exceptions import LocalProtocolError
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
from ..ingest.source import renormalize_staged_frame
from ..ingest.state import CommitResult, OwnerView, SourceState, StagedFrame
from ._sync_journal_codec import EncryptedRowCodec
from ._sync_journal_preflight import (
    FileIdentity,
    StableFileLock,
    immediate_transaction,
    open_journal_database,
)
from ._sync_journal_rows import JournalRows

if TYPE_CHECKING:
    from collections.abc import Callable


class SqliteIngestionJournal(JournalRows):
    """Direct-SQLite source-state journal for the version-1 checkpoint."""

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
        stream_id: UUID,
        file_identity: FileIdentity,
        transition_statement_hook: Callable[[str], None] | None,
    ) -> None:
        self.database_path = database_path
        self.account_id = account_id
        self.device_id = device_id
        self.pickle_key = pickle_key
        self.writer_epoch = writer_epoch
        self._connection = connection
        self._writer_lock = writer_lock
        self._transition_statement_hook = transition_statement_hook
        self._closed = False
        self._file_identity = file_identity
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
            connection=opened._connection,
            writer_lock=opened.writer_lock,
            writer_epoch=opened.writer_epoch,
            stream_id=opened.stream_id,
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
        self._assert_open()
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
        cursor = self._connection.execute(statement, parameters)
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

    def _normalized_candidate(
        self,
        owner: OwnerView,
        frame: StagedFrame,
    ) -> bytes:
        request = frame.response.request
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
            return renormalize_staged_frame(adapter, frame).candidate_cursor_json
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
        if proposed.cursor_json != self._normalized_candidate(owner, frame):
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

        self._assert_open()
        with immediate_transaction(self._connection):
            owner = self.load_owner()
            current = self.load_source()
            if owner.revision != expected_revision:
                raise JournalConflictError("journal revision is stale")
            if owner.writer_epoch != writer_epoch:
                raise JournalConflictError("journal writer_epoch is stale")

            proposed, frame = self._reconstruct_stage(source, frame)
            replay = self._validate_stage_relationship(
                owner,
                current,
                proposed,
                frame,
            )

            rows = dict(self._classify_frame_rows())
            self._transition_hook("frame_collision_probe")
            row = rows.get(frame.frame_id)
            if row is not None:
                stored = self._decode_frame_row(frame.frame_id, row, owner)
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
            except sqlite3.IntegrityError as error:
                raise JournalIntegrityError("staged frame insert collided") from error
            self._transition_hook("frame_insert")
        self._transition_hook("commit")
        return CommitResult(new_revision)

    def close(self) -> None:
        self._writer_lock.assert_process_owner()
        if self._closed:
            return
        self._connection.close()
        self._writer_lock.close()
        self._closed = True
