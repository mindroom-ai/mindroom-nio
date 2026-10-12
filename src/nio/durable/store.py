"""One SQLite transaction boundary for input, crypto and output batches."""

from __future__ import annotations

import asyncio
import json
import os
from collections.abc import AsyncIterator, Iterator
from concurrent.futures import Future, ThreadPoolExecutor, wait
from contextlib import asynccontextmanager, contextmanager
from copy import deepcopy
from pathlib import Path
from typing import Any, NamedTuple
from uuid import UUID, uuid4

from peewee import SqliteDatabase

from ..crypto import TrustState
from ..exceptions import LocalProtocolError
from ..store import (
    DefaultStore,
    DeviceTrustState,
    Key,
    KeyStore,
    MatrixStore,
    SqliteStore,
)
from ..store._sqlite_lease import FileLease
from .model import SyncBatch, SyncRecord, decode_records, encode_json, encode_records

_COMMITTED = (
    "SELECT cursor, acked_sequence, "
    "(SELECT continuation FROM NioDurableInput WHERE id=1), "
    "(SELECT MAX(sequence) FROM NioDurableBatch), "
    "(SELECT body FROM NioDurableCrypto WHERE kind='membership' AND key='current'), "
    "(SELECT body FROM NioDurableCrypto WHERE kind='request' AND key='current') "
    "FROM NioDurableMeta WHERE id=1"
)


class _Committed(NamedTuple):
    """Status that other tasks read while a transaction is in flight."""

    cursor: str | None
    acked_sequence: int
    continuation: str | None
    newest_batch: int | None
    local_intent: str | None
    crypto_request: str | None


async def _finish[T](future: asyncio.Future[T]) -> T:
    """Await to completion, delivering cancellation at the next suspension point.

    A transaction must not stop before COMMIT or ROLLBACK. Callers dispose the
    client on any exception raised from a transaction, so cancellation must not
    be raised at the lock wait or at COMMIT.
    """
    task = asyncio.current_task()
    assert task is not None
    deferred: list[Any] = []
    try:
        while True:
            try:
                return await asyncio.shield(future)
            except asyncio.CancelledError as error:
                if future.cancelled():
                    raise
                deferred.append(error.args[0] if error.args else None)
    finally:
        for message in deferred:
            task.uncancel()
            task.cancel(message)


class _Database(SqliteDatabase):
    revoked = False
    # COMMIT running on the store thread, if any.
    commit_job: Future[Any] | None = None

    def connect(self, reuse_if_open=False):
        if self.revoked:
            raise LocalProtocolError("durable store is closed")
        return super().connect(reuse_if_open)

    def atomic(self, *args, **kwargs):
        # A MatrixStore write outside the store lock, such as a key import,
        # must not join or interleave with another task's COMMIT.
        if self.commit_job is not None:
            wait((self.commit_job,))
        return super().atomic(*args, **kwargs)


class _MatrixStore(SqliteStore):
    """Normal crypto methods bound to the already owned connection."""

    def __init__(
        self,
        database: SqliteDatabase,
        user_id: str,
        device_id: str,
        path: Path,
        pickle_key: str,
    ) -> None:
        self._bound_database = database
        super().__init__(user_id, device_id, str(path.parent), pickle_key, path.name)

    def __post_init__(self) -> None:
        self.database_path = str(Path(self.store_path) / self.database_name)
        self.database = self._bound_database
        self._initialize_schema()


class DurableStore:
    """Persistence used by one durable sync session.

    Mutations other than acknowledgement require an enclosing transaction so
    they can commit with the ordinary MatrixStore's crypto changes.

    One asyncio lock serializes transactions. COMMIT runs on the store's own
    thread, so its fsync does not stall the event loop; BEGIN, the body and any
    ROLLBACK run on the loop, and the body never awaits. Outside a body, status
    reads return the last committed values without using SQLite; other reads
    belong inside a transaction or under locked().
    """

    def __init__(
        self,
        store_path: Path,
        *,
        user_id: str,
        device_id: str,
        consumer_id: UUID,
        database_name: str | None = None,
        pickle_key: str = "",
        source_store_class: type[MatrixStore] = SqliteStore,
    ) -> None:
        if source_store_class not in (SqliteStore, DefaultStore):
            raise LocalProtocolError(
                "durable adoption requires a built-in SQLite store"
            )
        store_path = Path(store_path)
        store_path.mkdir(parents=True, exist_ok=True)
        self.path = store_path / (database_name or f"{user_id}_{device_id}.db")
        self._closed = False
        self._pid = os.getpid()
        self._room_metadata: dict[str, str] | None = None
        self._intent_cached = False
        self._local_intent: dict[str, Any] | None = None
        self._lock = asyncio.Lock()
        self._thread = ThreadPoolExecutor(1, thread_name_prefix="nio-durable-store")
        self._lease = FileLease(self.path)
        self.database = _Database(
            str(self.path),
            pragmas={"foreign_keys": 1, "secure_delete": "fast"},
            timeout=5,
            # COMMIT runs on the store thread while the loop may use this
            # connection; serialized SQLite makes loop statements, such as nio
            # store reads while decrypting, wait for that COMMIT to finish.
            check_same_thread=False,
            thread_safe=False,
        )
        try:
            self.database.connect()
            tables = set(self.database.get_tables())
            if "nioingestmeta" in {table.lower() for table in tables}:
                raise LocalProtocolError(
                    "unmerged ingestion format cannot open as durable sync"
                )
            if "accounts" in tables:
                identities = self.database.execute_sql(
                    "SELECT user_id, device_id FROM accounts"
                ).fetchall()
                if any(
                    tuple(identity) != (user_id, device_id) for identity in identities
                ):
                    raise LocalProtocolError(
                        "durable store account/device identity mismatch"
                    )
            with self.database.atomic("IMMEDIATE"):
                if "NioDurableMeta" not in tables:
                    self._check_legacy_recovery(tables)
                self.matrix = _MatrixStore(
                    self.database, user_id, device_id, self.path, pickle_key
                )
                # Authenticate an existing pickle before writing the adoption marker.
                self.matrix.load_account()
                if "NioDurableMeta" in tables:
                    row = self.database.execute_sql(
                        "SELECT version, stream_id, consumer_id, user_id, device_id "
                        "FROM NioDurableMeta WHERE id = 1"
                    ).fetchone()
                    if row is None or row[0] != 1:
                        raise LocalProtocolError("unsupported durable store version")
                    if row[2] != str(consumer_id):
                        raise LocalProtocolError("durable consumer identity mismatch")
                    if tuple(row[3:]) != (user_id, device_id):
                        raise LocalProtocolError(
                            "durable store account/device identity mismatch"
                        )
                    self.stream_id = UUID(row[1])
                else:
                    self.stream_id = uuid4()
                    self._create_schema(consumer_id, user_id, device_id)
                    if source_store_class is DefaultStore:
                        self._adopt_trust(user_id, device_id)
            self.database.execute_sql("PRAGMA journal_mode=WAL")
            self.database.execute_sql("PRAGMA synchronous=FULL")
            self._committed = self._read_committed()
        except BaseException:
            self.close()
            raise

    def _check_legacy_recovery(self, tables: set[str]) -> None:
        """Refuse outstanding released-store work before any adoption writes."""
        tables = {table.lower() for table in tables}
        for table, predicate in (
            (
                "pendingtimelineevents",
                "generation > 0 AND admission_accepted = 0",
            ),
            ("syncrecoverygaps", "target_token != ''"),
            ("syncrecoveryabandonedrooms", "1"),
        ):
            if (
                table in tables
                and self.database.execute_sql(
                    f"SELECT 1 FROM {table} WHERE {predicate} LIMIT 1"
                ).fetchone()
            ):
                raise LocalProtocolError(
                    "durable adoption requires recovery to be drained or explicitly "
                    "settled using the prior release; outstanding legacy recovery "
                    "work or unacknowledged loss remains"
                )

    def _create_schema(self, consumer_id: UUID, user_id: str, device_id: str) -> None:
        statements = (
            "CREATE TABLE NioDurableMeta (id INTEGER PRIMARY KEY CHECK(id=1), "
            "version INTEGER NOT NULL, stream_id TEXT NOT NULL, consumer_id TEXT NOT NULL, "
            "user_id TEXT NOT NULL, device_id TEXT NOT NULL, cursor TEXT, acked_sequence INTEGER NOT NULL DEFAULT 0)",
            "CREATE TABLE NioDurableInput (id INTEGER PRIMARY KEY CHECK(id=1), "
            "body BLOB NOT NULL, continuation TEXT NOT NULL DEFAULT '{}')",
            "CREATE TABLE NioDurableBatch (sequence INTEGER PRIMARY KEY AUTOINCREMENT, "
            "records TEXT NOT NULL, completes_sync INTEGER NOT NULL CHECK(completes_sync IN (0,1)))",
            "CREATE TABLE NioDurableRoom (room_id TEXT PRIMARY KEY, metadata TEXT NOT NULL)",
            "CREATE TABLE NioDurableMember (room_id TEXT NOT NULL REFERENCES NioDurableRoom(room_id) "
            "ON DELETE CASCADE, user_id TEXT NOT NULL, member TEXT NOT NULL, PRIMARY KEY(room_id,user_id))",
            "CREATE TABLE NioDurableCrypto (kind TEXT NOT NULL, key TEXT NOT NULL, "
            "body TEXT NOT NULL, PRIMARY KEY(kind,key))",
        )
        for statement in statements:
            self.database.execute_sql(statement)
        self.database.execute_sql(
            "INSERT INTO NioDurableMeta (id,version,stream_id,consumer_id,user_id,device_id,cursor) "
            "VALUES (1,1,?,?,?,?,?)",
            (
                str(self.stream_id),
                str(consumer_id),
                user_id,
                device_id,
                self.matrix.load_sync_token(),
            ),
        )

    def _adopt_trust(self, user_id: str, device_id: str) -> None:
        sidecars = [
            (
                state,
                KeyStore(
                    str(self.path.parent / f"{user_id}_{device_id}.{suffix}_devices")
                ),
            )
            for state, suffix in (
                (TrustState.verified, "trusted"),
                (TrustState.blacklisted, "blacklisted"),
                (TrustState.ignored, "ignored"),
            )
        ]
        with self.database.bind_ctx(self.matrix.models):
            for device in self.matrix.load_device_keys():
                key = Key.from_olmdevice(device)
                state = next(
                    (state for state, keys in sidecars if key in keys), TrustState.unset
                )
                row = self.matrix._get_device(device)
                DeviceTrustState.replace(device=row, state=state).execute()

    def _assert_open(self) -> None:
        if self._closed or self._pid != os.getpid():
            raise LocalProtocolError(
                "durable store is closed or belongs to another process"
            )

    def _in_transaction(self) -> bool:
        # While COMMIT runs on the store thread, other tasks read committed state.
        job = self.database.commit_job
        return (job is None or job.done()) and self.database.in_transaction()

    def _require_transaction(self) -> None:
        self._assert_open()
        if not self._in_transaction():
            raise LocalProtocolError("durable mutation requires a transaction")

    def _read_committed(self) -> _Committed:
        return _Committed(*self.database.execute_sql(_COMMITTED).fetchone())

    @contextmanager
    def _body(self) -> Iterator[None]:
        self._invalidate_local_intent()
        try:
            yield
        except BaseException:
            # A failed commit or nested rollback invalidates speculative writes.
            # Reload from this connection before comparing another projection.
            self._room_metadata = None
            raise
        finally:
            self._invalidate_local_intent()

    async def _commit(self, transaction: Any) -> None:
        job = self._thread.submit(transaction.__exit__, None, None, None)
        self.database.commit_job = job
        try:
            await _finish(asyncio.wrap_future(job))
        finally:
            self.database.commit_job = None

    @asynccontextmanager
    async def transaction(self) -> AsyncIterator[None]:
        """Hold the store for one IMMEDIATE transaction; the body must not await.

        BEGIN, the body and any ROLLBACK run on the loop and COMMIT runs on the
        store thread. Once entered, the transaction commits or rolls back even
        if the task is cancelled, and the store stays usable. The cancellation
        is then delivered at the task's next suspension point.
        """
        self._assert_open()
        if self._in_transaction():
            # Nested in this task's own transaction: a savepoint.
            with self._body(), self.database.atomic():
                yield
            return
        # A separate acquiring task keeps this place in the queue if the
        # caller is cancelled while waiting.
        await _finish(asyncio.ensure_future(self._lock.acquire()))
        try:
            self._assert_open()
            transaction = self.database.transaction("IMMEDIATE")
            transaction.__enter__()
            with self._body():
                try:
                    yield
                except BaseException as error:
                    transaction.__exit__(type(error), error, error.__traceback__)
                    raise
                await self._commit(transaction)
            if not self._closed:
                self._committed = self._read_committed()
        finally:
            self._lock.release()

    @contextmanager
    def blocking_transaction(self) -> Iterator[None]:
        """Run one transaction on the calling thread, blocking it until COMMIT.

        This serves opening and synchronous public APIs. It first waits for
        another task's COMMIT running on the store thread.
        """
        self._assert_open()
        if self._in_transaction():
            with self._body(), self.database.atomic():
                yield
            return
        with self._body(), self.database.atomic("IMMEDIATE"):
            yield
        self._committed = self._read_committed()

    @asynccontextmanager
    async def locked(self) -> AsyncIterator[None]:
        """Hold the store for reads outside a transaction; do not await inside."""
        async with self._lock:
            self._assert_open()
            yield

    def load_room_metadata(self) -> dict[str, str]:
        self._assert_open()
        if self._room_metadata is None:
            self._room_metadata = dict(
                self.database.execute_sql("SELECT room_id,metadata FROM NioDurableRoom")
            )
        return self._room_metadata.copy()

    def save_room_metadata(self, room_id: str, metadata: str) -> None:
        self._require_transaction()
        if self._room_metadata is None:
            self.load_room_metadata()
        assert self._room_metadata is not None
        if self._room_metadata.get(room_id) == metadata:
            return
        self.database.execute_sql(
            "INSERT INTO NioDurableRoom(room_id,metadata) VALUES(?,?) "
            "ON CONFLICT(room_id) DO UPDATE SET metadata=excluded.metadata",
            (room_id, metadata),
        )
        self._room_metadata[room_id] = metadata

    def _invalidate_local_intent(self) -> None:
        self._intent_cached = False
        self._local_intent = None

    def read_local_intent(self) -> dict[str, Any] | None:
        self._assert_open()
        if not self._in_transaction():
            # Decoding returns a fresh copy for callers that edit it.
            body = self._committed.local_intent
            return json.loads(body) if body else None
        if not self._intent_cached:
            row = self.database.execute_sql(
                "SELECT body FROM NioDurableCrypto WHERE kind='membership' AND key='current'"
            ).fetchone()
            self._local_intent = json.loads(row[0]) if row else None
            self._intent_cached = True
        # Callers edit their intent before explicitly saving it.
        return deepcopy(self._local_intent)

    def save_local_intent(
        self, intent: dict[str, Any], *, create: bool = False
    ) -> None:
        self._require_transaction()
        self.database.execute_sql(
            (
                "INSERT INTO NioDurableCrypto(kind,key,body) VALUES('membership','current',?)"
                if create
                else "UPDATE NioDurableCrypto SET body=? WHERE kind='membership' AND key='current'"
            ),
            (encode_json(intent),),
        )
        self._invalidate_local_intent()

    def delete_local_intent(self) -> None:
        self._require_transaction()
        self.database.execute_sql(
            "DELETE FROM NioDurableCrypto WHERE kind='membership'"
        )
        self._invalidate_local_intent()

    @property
    def cursor(self) -> str | None:
        self._assert_open()
        if not self._in_transaction():
            return self._committed.cursor
        return self.database.execute_sql(
            "SELECT cursor FROM NioDurableMeta WHERE id=1"
        ).fetchone()[0]

    def set_cursor(self, cursor: str) -> None:
        self._require_transaction()
        self.database.execute_sql(
            "UPDATE NioDurableMeta SET cursor=? WHERE id=1", (cursor,)
        )

    @property
    def input(self) -> tuple[bytes, dict[str, Any]] | None:
        self._assert_open()
        row = self.database.execute_sql(
            "SELECT body,continuation FROM NioDurableInput WHERE id=1"
        ).fetchone()
        if row is None:
            return None
        return bytes(row[0]), self._decode_continuation(row[1])

    def has_input(self) -> bool:
        """Whether committed input awaits preparation or completion."""
        self._assert_open()
        return self._committed.continuation is not None

    @property
    def continuation(self) -> dict[str, Any] | None:
        """Committed continuation of the retained input."""
        self._assert_open()
        encoded = self._committed.continuation
        return None if encoded is None else self._decode_continuation(encoded)

    @staticmethod
    def _decode_continuation(encoded: str) -> dict[str, Any]:
        continuation = json.loads(encoded)
        if not isinstance(continuation, dict):
            raise LocalProtocolError("invalid stored continuation")
        return continuation

    def capture(self, body: bytes) -> None:
        self._require_transaction()
        if self.database.execute_sql("SELECT 1 FROM NioDurableInput").fetchone():
            raise LocalProtocolError("cannot replace unfinished durable response")
        self.database.execute_sql(
            "INSERT INTO NioDurableInput(id,body) VALUES(1,?)", (body,)
        )

    def save_continuation(self, continuation: dict[str, Any]) -> None:
        self._require_transaction()
        self.database.execute_sql(
            "UPDATE NioDurableInput SET continuation=? WHERE id=1",
            (encode_json(continuation),),
        )

    def finish_input(self) -> None:
        self._require_transaction()
        self.database.execute_sql("DELETE FROM NioDurableInput WHERE id=1")

    def publish(
        self,
        records: tuple[SyncRecord, ...],
        *,
        completes_sync: bool = False,
        encoded_records: str | None = None,
    ) -> SyncBatch:
        self._require_transaction()
        cursor = self.database.execute_sql(
            "INSERT INTO NioDurableBatch(records,completes_sync) VALUES(?,?)",
            (
                (
                    encoded_records
                    if encoded_records is not None
                    else encode_records(records)
                ),
                completes_sync,
            ),
        )
        return SyncBatch(self.stream_id, cursor.lastrowid, records, completes_sync)

    def has_batches(self) -> bool:
        """Whether committed batches await acknowledgement."""
        self._assert_open()
        return self._committed.newest_batch is not None

    @property
    def acked_sequence(self) -> int:
        """Committed acknowledgement position."""
        self._assert_open()
        return self._committed.acked_sequence

    @property
    def progress_generation(self) -> int:
        """Monotonic progress from committed publication and acknowledgement."""
        self._assert_open()
        committed = self._committed
        acked = committed.acked_sequence
        return acked + max(acked, committed.newest_batch or 0)

    def read_crypto_request(self) -> str | None:
        """Return the retained crypto request body, as committed outside a body."""
        self._assert_open()
        if not self._in_transaction():
            return self._committed.crypto_request
        row = self.database.execute_sql(
            "SELECT body FROM NioDurableCrypto WHERE kind='request' AND key='current'"
        ).fetchone()
        return row[0] if row else None

    @property
    def pending_bytes(self) -> int:
        """Count queued payload bytes without loading them on SQLite 3.43+."""
        self._assert_open()
        byte_length = (
            "octet_length(records)"
            if self.database.server_version >= (3, 43, 0)
            else "length(CAST(records AS BLOB))"
        )
        return self.database.execute_sql(
            f"SELECT COALESCE(SUM({byte_length}),0) FROM NioDurableBatch"
        ).fetchone()[0]

    def next_batch(self) -> SyncBatch | None:
        self._assert_open()
        row = self.database.execute_sql(
            "SELECT sequence,records,completes_sync FROM NioDurableBatch ORDER BY sequence LIMIT 1"
        ).fetchone()
        if row is None:
            return None
        try:
            return SyncBatch(
                self.stream_id, row[0], decode_records(row[1]), bool(row[2])
            )
        except (ValueError, TypeError, KeyError) as error:
            raise LocalProtocolError("invalid stored durable batch") from error

    async def ack(self, batch: SyncBatch) -> None:
        self._assert_open()
        if batch.stream_id != self.stream_id:
            raise LocalProtocolError("batch belongs to another durable stream")
        async with self.transaction():
            acked = self.database.execute_sql(
                "SELECT acked_sequence FROM NioDurableMeta WHERE id=1"
            ).fetchone()[0]
            if batch.sequence <= acked:
                return
            oldest = self.database.execute_sql(
                "SELECT sequence FROM NioDurableBatch ORDER BY sequence LIMIT 1"
            ).fetchone()
            if oldest is None or batch.sequence != oldest[0]:
                raise LocalProtocolError("acknowledgement must name the oldest batch")
            self.database.execute_sql(
                "DELETE FROM NioDurableBatch WHERE sequence=?", (batch.sequence,)
            )
            self.database.execute_sql(
                "UPDATE NioDurableMeta SET acked_sequence=? WHERE id=1",
                (batch.sequence,),
            )

    def close(self) -> None:
        if self._closed:
            return
        # Let an in-flight COMMIT finish, then stop the store thread.
        self._thread.shutdown(wait=True)
        if not self.database.is_closed():
            self.database.close()
        self.database.revoked = True
        self._closed = True
        self._lease.close()
