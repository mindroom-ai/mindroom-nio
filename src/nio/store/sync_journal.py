from __future__ import annotations

import os
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from ..exceptions import LocalProtocolError
from ..ingest.model import ConsumerBootstrap
from ._sync_journal import SqliteIngestionJournal as _SqliteIngestionJournal

if TYPE_CHECKING:
    from collections.abc import Callable

    from .database import MatrixStore


class StoreBootstrap:
    """Single-owner preflight handle retaining the ingestion writer lock."""

    def __init__(self, journal: _SqliteIngestionJournal) -> None:
        self._journal = journal
        self._store: MatrixStore | None = None

    @property
    def database_path(self) -> Path:
        self._journal._assert_open()
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
        if self._store is not None:
            raise LocalProtocolError("StoreBootstrap can open MatrixStore only once")
        from .database import _open_matrix_store_from_ingestion

        store = _open_matrix_store_from_ingestion(
            self,
            store_class,
            self._journal.pickle_key if pickle_key is None else pickle_key,
        )
        self._store = store
        return store

    async def attach_consumer(self, consumer: ConsumerBootstrap) -> None:
        self._journal.attach_consumer(consumer)

    def assert_http_enabled(self) -> None:
        self._journal._require_attached()

    def close(self) -> None:
        self._journal._assert_process_owner()
        try:
            if self._store is not None and not self._store.database.is_closed():
                self._store.database.close()
        finally:
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
