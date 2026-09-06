"""One SQLite transaction boundary for input, crypto and output batches."""

from __future__ import annotations

import json
import os
from collections.abc import Iterator
from contextlib import contextmanager
from pathlib import Path
from typing import Any
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
from .model import SyncBatch, SyncRecord, decode_records, encode_json, encode_records


class _Lease:
    """Opening-time coordination; live filesystem replacement is unsupported."""

    def __init__(self, database_path: Path) -> None:
        try:
            import fcntl
        except ImportError as error:
            raise LocalProtocolError(
                "store filesystem ownership requires fcntl support"
            ) from error
        path = Path(f"{database_path.resolve()}.ingest.lock")
        self.fd = os.open(path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self.fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BaseException as error:
            self.close()
            if isinstance(error, BlockingIOError):
                raise LocalProtocolError(
                    "store lifetime lease is already held"
                ) from error
            raise

    def close(self) -> None:
        if self.fd >= 0:
            os.close(self.fd)
            self.fd = -1


class _Database(SqliteDatabase):
    revoked = False

    def connect(self, reuse_if_open=False):
        if self.revoked:
            raise LocalProtocolError("durable store is closed")
        return super().connect(reuse_if_open)


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
    """Synchronous persistence used by one durable sync session.

    Mutations other than acknowledgement require an enclosing transaction so
    they can commit with the ordinary MatrixStore's crypto changes.
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
        self._lease = _Lease(self.path)
        self.database = _Database(
            str(self.path),
            pragmas={"foreign_keys": 1, "secure_delete": "fast"},
            timeout=5,
        )
        try:
            self.database.connect()
            tables = set(self.database.get_tables())
            if "NioIngestMeta" in tables:
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
            self.database.execute_sql("PRAGMA synchronous=NORMAL")
        except BaseException:
            self.close()
            raise

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

    def _require_transaction(self) -> None:
        self._assert_open()
        if not self.database.in_transaction():
            raise LocalProtocolError("durable mutation requires a transaction")

    @contextmanager
    def transaction(self) -> Iterator[None]:
        self._assert_open()
        with self.database.atomic("IMMEDIATE"):
            yield

    @property
    def cursor(self) -> str | None:
        self._assert_open()
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
        continuation = json.loads(row[1])
        if not isinstance(continuation, dict):
            raise LocalProtocolError("invalid stored continuation")
        return bytes(row[0]), continuation

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
        self._assert_open()
        return (
            self.database.execute_sql(
                "SELECT 1 FROM NioDurableBatch LIMIT 1"
            ).fetchone()
            is not None
        )

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

    def ack(self, batch: SyncBatch) -> None:
        self._assert_open()
        if batch.stream_id != self.stream_id:
            raise LocalProtocolError("batch belongs to another durable stream")
        with self.transaction():
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
        if not self.database.is_closed():
            self.database.close()
        self.database.revoked = True
        self._closed = True
        self._lease.close()
