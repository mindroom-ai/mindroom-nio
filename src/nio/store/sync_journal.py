from __future__ import annotations

import os
from contextlib import contextmanager
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID

from ..exceptions import LocalProtocolError
from ..ingest.config import SourceConfig, source_transport
from ._sync_journal import SqliteIngestionJournal as _SqliteIngestionJournal

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator

    from .database import MatrixStore


class StoreBootstrap:
    """Single-owner preflight handle retaining the ingestion writer lock."""

    def __init__(
        self,
        journal: _SqliteIngestionJournal,
        *,
        owned_store_class: type[MatrixStore] | None = None,
        authenticated_pickle_key: str | None = None,
    ) -> None:
        self._journal = journal
        self.__owned_store_class = owned_store_class
        self.__authenticated_pickle_key = authenticated_pickle_key
        self._store: MatrixStore | None = None
        self._store_revoked = False
        self._session_claimed = False

    @property
    def _owned_store_class(self) -> type[MatrixStore] | None:
        return self.__owned_store_class

    @property
    def _authenticated_pickle_key(self) -> str | None:
        return self.__authenticated_pickle_key

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
            raise LocalProtocolError("ingestion v1 requires exact SqliteStore")
        if (
            self._owned_store_class is not None
            and store_class is not self._owned_store_class
        ):
            raise LocalProtocolError("configured ingestion store class does not match")
        if (
            self._authenticated_pickle_key is not None
            and pickle_key is not None
            and pickle_key != self._authenticated_pickle_key
        ):
            raise LocalProtocolError("configured ingestion pickle key does not match")

        return _open_matrix_store_from_ingestion(
            self,
            store_class,
            (
                self._authenticated_pickle_key
                if pickle_key is None and self._authenticated_pickle_key is not None
                else self._journal.pickle_key if pickle_key is None else pickle_key
            ),
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
        pickle_key=pickle_key,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        statement_observer=statement_observer,
        transition_statement_hook=transition_statement_hook,
        schema_statement_hook=schema_statement_hook,
    )
    return StoreBootstrap(journal)


def _open_configured_ingestion_store(
    store_path: str | os.PathLike[str],
    *,
    source_store_class: type[MatrixStore],
    owned_store_class: type[MatrixStore],
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    source: SourceConfig,
    pickle_key: str = "",
    database_name: str = "",
    sqlite_busy_timeout_ms: int = 2_000,
    statement_observer: Callable[[str], None] | None = None,
    transition_statement_hook: Callable[[str], None] | None = None,
    adoption_statement_hook: Callable[[str], None] | None = None,
) -> StoreBootstrap:
    from .database import DefaultStore, SqliteStore

    if not (
        source_store_class is DefaultStore
        and owned_store_class is SqliteStore
        or source_store_class is SqliteStore
        and owned_store_class is SqliteStore
    ):
        raise LocalProtocolError("configured ingestion store class pairing is invalid")
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
        pickle_key=pickle_key,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        statement_observer=statement_observer,
        transition_statement_hook=transition_statement_hook,
        configured_source_store_class=source_store_class,
        configured_store_path=Path(store_path),
        adoption_statement_hook=adoption_statement_hook,
    )
    return StoreBootstrap(
        journal,
        owned_store_class=SqliteStore,
        authenticated_pickle_key=pickle_key,
    )
