from __future__ import annotations

import os
import stat
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


class _OwnedStoreLease:
    """One-shot ownership token for an exact borrowed ingestion store."""

    def __init__(
        self,
        bootstrap: StoreBootstrap,
        store: MatrixStore,
        journal: _SqliteIngestionJournal,
    ) -> None:
        self.__bootstrap = bootstrap
        self.__store = store
        self.__journal = journal
        self.__token = object()

    def _token_for_transfer(self) -> object:
        return self.__token

    def _prepare_close(self) -> None:
        self.__journal._owner.prepare_close()

    def _revoke_store(self, store: MatrixStore) -> None:
        if store is not self.__store:
            raise LocalProtocolError("owned ingestion store identity changed")
        self.__bootstrap._revoke_owned_store(self.__token, store)

    def _close_owner(self) -> None:
        self.__journal.close()


class _OwnedStoreCandidate:
    """Exact borrowed store awaiting a single session-ownership transfer."""

    def __init__(self, bootstrap: StoreBootstrap) -> None:
        self.__bootstrap = bootstrap
        self.__store: MatrixStore | None = None
        self.__prepared_lease: _OwnedStoreLease | None = None
        self.__active = True

    @property
    def _journal(self) -> _SqliteIngestionJournal:
        return self.__bootstrap._journal

    @property
    def database_path(self) -> Path:
        return self.__bootstrap.database_path

    @contextmanager
    def _claim_store(self, store: MatrixStore) -> Iterator[None]:
        if not self.__active or self.__store is not None:
            raise LocalProtocolError("owned ingestion store candidate is unavailable")
        self.__bootstrap._validate_unconstructed_candidate(self)
        self.__store = store
        try:
            yield
        except BaseException:
            self.__store = None
            raise

    def _store_for_attachment(self) -> MatrixStore:
        if not self.__active:
            raise LocalProtocolError("owned ingestion store candidate was consumed")
        if self.__store is None:
            raise LocalProtocolError("owned ingestion store candidate has no store")
        self.__bootstrap._validate_owned_candidate(self, self.__store)
        return self.__store

    def _prepare_transfer(self, store: MatrixStore) -> _OwnedStoreLease:
        if not self.__active or self.__store is None:
            raise LocalProtocolError("owned ingestion store candidate was consumed")
        if store is not self.__store:
            raise LocalProtocolError("owned ingestion store candidate is foreign")
        if self.__prepared_lease is not None:
            raise LocalProtocolError(
                "owned ingestion store transfer was already prepared"
            )
        self.__bootstrap._validate_owned_candidate(self, store)
        lease = _OwnedStoreLease(self.__bootstrap, store, self._journal)
        self.__prepared_lease = lease
        return lease

    def _commit_transfer(
        self,
        store: MatrixStore,
        lease: _OwnedStoreLease,
    ) -> None:
        if (
            not self.__active
            or store is not self.__store
            or lease is not self.__prepared_lease
        ):
            raise LocalProtocolError("owned ingestion store transfer is foreign")
        self.__bootstrap._transfer_owned_candidate(
            self,
            store,
            lease._token_for_transfer(),
        )
        self.__active = False

    def _tombstone(self) -> None:
        if not self.__active:
            return
        self.__bootstrap._tombstone_owned_candidate(self, self.__store)
        self.__active = False


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
        self.__bound_source: SourceConfig | None = None
        self.__bound_sqlite_busy_timeout_ms: int | None = None
        self._store_revoked = False
        self._session_claimed = False
        self._owned_candidate: _OwnedStoreCandidate | None = None
        self._owned_lease_token: object | None = None

    @property
    def _owned_store_class(self) -> type[MatrixStore] | None:
        return self.__owned_store_class

    @property
    def _authenticated_pickle_key(self) -> str | None:
        return self.__authenticated_pickle_key

    @property
    def _bound_source(self) -> SourceConfig | None:
        return self.__bound_source

    @property
    def _bound_sqlite_busy_timeout_ms(self) -> int | None:
        return self.__bound_sqlite_busy_timeout_ms

    def _bind_owned_config(
        self,
        source: SourceConfig,
        sqlite_busy_timeout_ms: int,
    ) -> StoreBootstrap:
        if self._owned_store_class is None or self.__bound_source is not None:
            raise LocalProtocolError("owned ingestion config binding is unavailable")
        self.__bound_source = source
        self.__bound_sqlite_busy_timeout_ms = sqlite_busy_timeout_ms
        return self

    def _open_owned_store_candidate(self) -> _OwnedStoreCandidate:
        from .database import SqliteStore, _open_matrix_store_from_owned_candidate

        with self._journal._owner.read():
            pass
        if self._owned_store_class is not SqliteStore:
            raise LocalProtocolError("owned ingestion requires bound exact SqliteStore")
        if self._authenticated_pickle_key is None:
            raise LocalProtocolError(
                "owned ingestion requires an authenticated pickle key"
            )
        if (
            self._session_claimed
            or self._owned_candidate is not None
            or self._owned_lease_token is not None
        ):
            raise LocalProtocolError("StoreBootstrap already has a store or session")
        candidate = _OwnedStoreCandidate(self)
        self._owned_candidate = candidate
        try:
            _open_matrix_store_from_owned_candidate(
                candidate,
                SqliteStore,
                self._authenticated_pickle_key,
            )
            return candidate
        except BaseException:
            candidate._tombstone()
            raise

    def _validate_unconstructed_candidate(
        self,
        candidate: _OwnedStoreCandidate,
    ) -> None:
        with self._journal._owner.read():
            pass
        if (
            self._owned_candidate is not candidate
            or self._session_claimed
            or self._owned_lease_token is not None
        ):
            raise LocalProtocolError("owned ingestion store candidate is foreign")

    def _validate_owned_candidate(
        self,
        candidate: _OwnedStoreCandidate,
        store: MatrixStore,
    ) -> None:
        with self._journal._owner.read():
            pass
        if self._owned_candidate is not candidate:
            raise LocalProtocolError("owned ingestion store candidate is foreign")
        if self._session_claimed or self._owned_lease_token is not None:
            raise LocalProtocolError("owned ingestion store candidate was consumed")

    def _transfer_owned_candidate(
        self,
        candidate: _OwnedStoreCandidate,
        store: MatrixStore,
        lease_token: object,
    ) -> None:
        self._validate_owned_candidate(candidate, store)
        self._owned_candidate = None
        self._owned_lease_token = lease_token
        self._session_claimed = True

    def _tombstone_owned_candidate(
        self,
        candidate: _OwnedStoreCandidate,
        store: MatrixStore | None,
    ) -> None:
        if self._owned_candidate is not candidate:
            raise LocalProtocolError("owned ingestion store candidate is foreign")
        if store is not None and not self._store_revoked:
            store._revoke_ingestion_lease()
            self._store_revoked = True
        self._journal.close()
        self._owned_candidate = None

    def _revoke_owned_store(
        self,
        lease_token: object,
        store: MatrixStore,
    ) -> None:
        if self._owned_lease_token is not lease_token:
            raise LocalProtocolError("owned ingestion store lease is foreign")
        if not self._store_revoked:
            store._revoke_ingestion_lease()
            self._store_revoked = True

    @property
    def database_path(self) -> Path:
        return self._journal.database_path

    @property
    def schema_version(self) -> int:
        return self._journal.schema_version

    @property
    def stream_id(self) -> UUID:
        return self._journal.stream_id

    def close(self) -> None:
        if self._owned_lease_token is not None:
            raise LocalProtocolError("owned ingestion session holds the close token")
        self._journal._owner.prepare_close()
        if self._owned_candidate is not None:
            self._owned_candidate._tombstone()
            return
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
    )._bind_owned_config(source, sqlite_busy_timeout_ms)


def _open_fresh_ingestion_store(
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
    fresh_statement_hook: Callable[[str], None] | None = None,
) -> StoreBootstrap:
    from .database import SqliteStore

    if type(consumer_generation) is not UUID:
        raise TypeError("consumer_generation must be UUID")
    source_transport(source)
    database_name = database_name or f"{account_id}_{device_id}.db"
    database_path = Path(store_path) / database_name
    try:
        existing = database_path.lstat()
    except FileNotFoundError:
        existing = None
    if existing is not None and (
        not stat.S_ISREG(existing.st_mode) or existing.st_nlink != 1
    ):
        raise LocalProtocolError(
            "fresh ingestion requires a singly linked regular database path"
        )
    journal = _SqliteIngestionJournal.open(
        database_path,
        account_id=account_id,
        device_id=device_id,
        consumer_generation=consumer_generation,
        source=source,
        pickle_key=pickle_key,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        statement_observer=statement_observer,
        transition_statement_hook=transition_statement_hook,
        fresh_store=True,
        fresh_store_statement_hook=fresh_statement_hook,
    )
    return StoreBootstrap(
        journal,
        owned_store_class=SqliteStore,
        authenticated_pickle_key=pickle_key,
    )._bind_owned_config(source, sqlite_busy_timeout_ms)
