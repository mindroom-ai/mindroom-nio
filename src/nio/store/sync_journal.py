from __future__ import annotations

import os
import threading
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from ..exceptions import LocalProtocolError
from ..ingest.model import ConsumerBootstrap
from ._sync_journal import SqliteIngestionJournal as _SqliteIngestionJournal

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from peewee import SqliteDatabase

    from .database import MatrixStore


class _StoreOwnershipLease:
    def __init__(self, journal: _SqliteIngestionJournal) -> None:
        self._owner_pid = os.getpid()
        self._active = True
        self._lock = threading.RLock()
        self._journal = journal
        self._operation_depth = 0

    def _assert_process_owner(self) -> None:
        if os.getpid() != self._owner_pid:
            raise LocalProtocolError("ownership lease belongs to acquiring process")

    @contextmanager
    def operation(self, database: SqliteDatabase) -> Iterator[None]:
        self._assert_process_owner()
        with self._lock:
            if not self._active:
                raise LocalProtocolError("store ownership lease is revoked")
            self._journal._assert_file_owner()
            if self._operation_depth:
                self._operation_depth += 1
                try:
                    yield
                finally:
                    self._operation_depth -= 1
                return
            if database.transaction_depth():
                raise LocalProtocolError(
                    "ingestion store operation cannot use an ambient transaction"
                )

            self._operation_depth = 1
            try:
                with database.atomic("IMMEDIATE"):
                    self._journal._assert_file_owner()
                    cursor = database.execute_sql(
                        "UPDATE NioIngestMeta SET writer_epoch = writer_epoch "
                        "WHERE account_id = ? AND writer_epoch = ?",
                        (
                            self._journal.account_id,
                            str(self._journal.writer_epoch),
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise LocalProtocolError(
                            "ingestion store writer_epoch is stale"
                        )
                    yield
            finally:
                self._operation_depth = 0

    def revoke(self) -> None:
        self._assert_process_owner()
        with self._lock:
            self._active = False


class StoreBootstrap:
    """Single-owner preflight handle retaining the ingestion writer lock."""

    def __init__(self, journal: _SqliteIngestionJournal) -> None:
        self._journal = journal
        self._store: MatrixStore | None = None
        self._store_lock = threading.RLock()

    @contextmanager
    def _claim_store(self, store: MatrixStore) -> Iterator[_StoreOwnershipLease]:
        self._journal._assert_file_owner()
        with self._store_lock:
            if self._store is not None:
                raise LocalProtocolError(
                    "StoreBootstrap can open MatrixStore only once"
                )
            lease = _StoreOwnershipLease(self._journal)
            self._store = store
            try:
                yield lease
            except BaseException:
                lease.revoke()
                self._store = None
                raise

    @property
    def database_path(self) -> Path:
        self._journal._assert_file_owner()
        return self._journal.database_path

    @property
    def schema_version(self) -> int:
        return self._journal.schema_version

    @property
    def stream_id(self) -> UUID:
        return self._journal.stream_id

    @property
    def binding_operation_id(self) -> UUID:
        return self._journal.binding_operation_id

    @property
    def next_batch_sequence(self) -> int:
        return self._journal.next_batch_sequence

    def open_matrix_store(
        self,
        store_class: type[MatrixStore],
        *,
        pickle_key: str | None = None,
    ) -> MatrixStore:
        from .database import _open_matrix_store_from_ingestion

        return _open_matrix_store_from_ingestion(
            self,
            store_class,
            self._journal.pickle_key if pickle_key is None else pickle_key,
        )

    async def attach_consumer(self, consumer: ConsumerBootstrap) -> None:
        self._journal.attach_consumer(consumer)

    def assert_http_enabled(self) -> None:
        self._journal._require_attached()

    def close(self) -> None:
        self._journal._writer_lock.assert_process_owner()
        with self._store_lock:
            if self._store is not None:
                self._store._revoke_ingestion_lease()
                if not self._store.database.is_closed():
                    self._store.database.close()
            self._journal.close()


def open_ingestion_store(
    store_path: str | os.PathLike[str],
    *,
    account_id: str,
    device_id: str,
    pickle_key: str = "",
    database_name: str = "",
    sqlite_busy_timeout_ms: int = 2_000,
    statement_observer: Callable[[str], None] | None = None,
    transition_statement_hook: Callable[[str], None] | None = None,
    schema_statement_hook: Callable[[str], None] | None = None,
) -> StoreBootstrap:
    database_name = database_name or f"{account_id}_{device_id}.db"
    journal = _SqliteIngestionJournal.open(
        Path(store_path) / database_name,
        account_id=account_id,
        device_id=device_id,
        pickle_key=pickle_key,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        statement_observer=statement_observer,
        transition_statement_hook=transition_statement_hook,
        schema_statement_hook=schema_statement_hook,
    )
    return StoreBootstrap(journal)
