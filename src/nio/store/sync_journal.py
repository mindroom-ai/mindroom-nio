from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from ..exceptions import LocalProtocolError
from ..ingest.config import SourceConfig, source_transport
from ..ingest.diagnostic import DiagnosticIngestionScope
from ._sync_journal import SqliteIngestionJournal as _SqliteIngestionJournal

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from .database import MatrixStore


class StoreBootstrap:
    """Single-owner preflight handle retaining the ingestion writer lock."""

    def __init__(self, journal: _SqliteIngestionJournal) -> None:
        self._journal = journal
        self._store: MatrixStore | None = None
        self._store_revoked = False
        self._session_claimed = False

    @contextmanager
    def _claim_store(self, store: MatrixStore) -> Iterator[None]:
        with self._journal._owner.read():
            pass
        if self._session_claimed:
            raise LocalProtocolError("StoreBootstrap belongs to an ingestion session")
        if self._store is not None:
            raise LocalProtocolError("StoreBootstrap can open MatrixStore only once")
        self._store = store
        try:
            yield
        except BaseException:
            self._store = None
            raise

    def _claim_session(self) -> _SqliteIngestionJournal:
        with self._journal._owner.read():
            pass
        if self._session_claimed or self._store is not None:
            raise LocalProtocolError("StoreBootstrap already has a store or session")
        self._session_claimed = True
        return self._journal

    @property
    def database_path(self) -> Path:
        return self._journal.database_path

    @property
    def schema_version(self) -> int:
        return self._journal.schema_version

    @property
    def stream_id(self) -> UUID:
        return self._journal.stream_id

    def open_matrix_store(
        self,
        store_class: type[MatrixStore],
        *,
        pickle_key: str | None = None,
    ) -> MatrixStore:
        from .database import SqliteStore, _open_matrix_store_from_ingestion

        if store_class is not SqliteStore:
            raise LocalProtocolError("ingestion v2 requires exact SqliteStore")

        return _open_matrix_store_from_ingestion(
            self,
            store_class,
            self._journal.pickle_key if pickle_key is None else pickle_key,
        )

    def close(self) -> None:
        self._journal._owner.prepare_close()
        if self._store is not None and not self._store_revoked:
            self._store._revoke_ingestion_lease()
            self._store_revoked = True
        self._journal.close()


def open_ingestion_store(
    store_path: str | os.PathLike[str],
    *,
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    source: SourceConfig,
    diagnostic_scope: DiagnosticIngestionScope | None = None,
    pickle_key: str = "",
    database_name: str = "",
    sqlite_busy_timeout_ms: int = 2_000,
    statement_observer: Callable[[str], None] | None = None,
    transition_statement_hook: Callable[[str], None] | None = None,
    schema_statement_hook: Callable[[str], None] | None = None,
) -> StoreBootstrap:
    if type(consumer_generation) is not UUID:
        raise TypeError("consumer_generation must be UUID")
    source_transport(source)
    database_name = database_name or f"{account_id}_{device_id}.db"
    journal = _SqliteIngestionJournal.open(
        Path(store_path) / database_name,
        account_id=account_id,
        device_id=device_id,
        consumer_generation=consumer_generation,
        source=source,
        diagnostic_scope=diagnostic_scope,
        pickle_key=pickle_key,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        statement_observer=statement_observer,
        transition_statement_hook=transition_statement_hook,
        schema_statement_hook=schema_statement_hook,
    )
    return StoreBootstrap(journal)
