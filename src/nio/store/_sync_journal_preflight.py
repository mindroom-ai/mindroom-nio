from __future__ import annotations

import fcntl
import os
import sqlite3
import time
from contextlib import contextmanager
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING
from uuid import UUID, uuid4

from peewee import SqliteDatabase
from playhouse.sqliteq import SqliteQueueDatabase

from ..exceptions import LocalProtocolError
from ..ingest.errors import FreshIngestionRequired
from .sync_journal_schema import META_TABLE_SQL, SCHEMA_SQL, SCHEMA_VERSION

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator


FileIdentity = tuple[int, int]


@contextmanager
def immediate_transaction(connection: sqlite3.Connection) -> Iterator[None]:
    connection.execute("BEGIN IMMEDIATE")
    try:
        yield
    except BaseException:
        if connection.in_transaction:
            connection.execute("ROLLBACK")
        raise
    else:
        connection.execute("COMMIT")


class StableFileLock:
    def __init__(self, database_path: Path) -> None:
        self.path = Path(f"{database_path}.ingest.lock")
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(self._fd)
            self._fd = -1
            raise LocalProtocolError(
                f"ingestion writer lock is already held for {database_path}"
            ) from error
        stat = os.fstat(self._fd)
        self.identity = (stat.st_dev, stat.st_ino)

    @property
    def active(self) -> bool:
        return self._fd >= 0

    def assert_identity(self, expected: FileIdentity | None = None) -> None:
        if not self.active:
            raise LocalProtocolError("ingestion writer lock is closed")
        try:
            stat = os.stat(self.path)
        except FileNotFoundError as error:
            raise LocalProtocolError(
                "ingestion lock file identity is no longer present"
            ) from error
        actual = (stat.st_dev, stat.st_ino)
        if actual != self.identity or (expected is not None and actual != expected):
            raise LocalProtocolError(
                "ingestion lock file identity changed after lock acquisition"
            )

    def close(self) -> None:
        if self._fd < 0:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = -1

    def __enter__(self) -> StableFileLock:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


def database_path(database: str | os.PathLike[str] | SqliteDatabase) -> Path:
    if isinstance(database, SqliteQueueDatabase):
        raise LocalProtocolError(
            "SqliteQueueDatabase cannot provide atomic ingestion transactions"
        )
    if isinstance(database, SqliteDatabase):
        database = database.database
    path = os.fspath(database)
    if path == ":memory:":
        raise LocalProtocolError("the ingestion journal requires an on-disk database")
    return Path(path).resolve()


def _connect_read_only(path: Path) -> sqlite3.Connection:
    return sqlite3.connect(f"file:{path}?mode=ro", uri=True, isolation_level=None)


def database_shape(path: str | os.PathLike[str]) -> tuple[bool, bool]:
    database = Path(path)
    if not database.exists() or database.stat().st_size == 0:
        return False, False
    with _connect_read_only(database) as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    names = {row[0] for row in rows}
    return "NioIngestMeta" in names, bool(names)


def database_has_ingestion_marker(path: str | os.PathLike[str]) -> bool:
    return database_shape(path)[0]


def _normalized_sql(sql: str | None) -> str | None:
    return " ".join(sql.split()) if sql is not None else None


@dataclass(frozen=True)
class _SchemaContract:
    master: tuple[tuple[object, ...], ...]
    tables: tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]
    foreign_keys: tuple[tuple[str, tuple[tuple[object, ...], ...]], ...]
    indexes: tuple[
        tuple[
            str, tuple[tuple[str, int, str, int, tuple[tuple[object, ...], ...]], ...]
        ],
        ...,
    ]


def _capture_contract(connection: sqlite3.Connection) -> _SchemaContract:
    table_names = tuple(
        row[0]
        for row in connection.execute(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name GLOB 'NioIngest*' ORDER BY name"
        )
    )
    master = tuple(
        (row[0], row[1], row[2], _normalized_sql(row[3]))
        for row in connection.execute(
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE (type = 'table' AND name GLOB 'NioIngest*') "
            "OR (type = 'index' AND tbl_name GLOB 'NioIngest*') "
            "ORDER BY type, name"
        )
    )
    tables = tuple(
        (
            name,
            tuple(
                tuple(row) for row in connection.execute(f'PRAGMA table_info("{name}")')
            ),
        )
        for name in table_names
    )
    foreign_keys = tuple(
        (
            name,
            tuple(
                tuple(row)
                for row in connection.execute(f'PRAGMA foreign_key_list("{name}")')
            ),
        )
        for name in table_names
    )
    indexes = []
    for table in table_names:
        values = []
        for row in connection.execute(f'PRAGMA index_list("{table}")'):
            name = row[1]
            values.append(
                (
                    name,
                    row[2],
                    row[3],
                    row[4],
                    tuple(
                        tuple(item)
                        for item in connection.execute(f'PRAGMA index_xinfo("{name}")')
                    ),
                )
            )
        indexes.append((table, tuple(sorted(values))))
    return _SchemaContract(master, tables, foreign_keys, tuple(indexes))


@cache
def _expected_contract() -> _SchemaContract:
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(META_TABLE_SQL)
        for statement in SCHEMA_SQL:
            connection.execute(statement.strip())
        return _capture_contract(connection)
    finally:
        connection.close()


def validate_schema_topology(connection: sqlite3.Connection) -> None:
    try:
        unexpected = connection.execute(
            "SELECT type, name FROM sqlite_master "
            "WHERE type IN ('trigger', 'view') ORDER BY type, name"
        ).fetchall()
        if unexpected or _capture_contract(connection) != _expected_contract():
            raise LocalProtocolError("ingestion-v1 schema topology does not match v1")
    except sqlite3.DatabaseError as error:
        raise LocalProtocolError("ingestion-v1 schema topology is invalid") from error


def _create_fresh(
    connection: sqlite3.Connection,
    *,
    account_id: str,
    device_id: str,
    writer_epoch: UUID,
    lock_identity: FileIdentity,
    statement_hook: Callable[[str], None] | None,
) -> None:
    with immediate_transaction(connection):
        connection.execute(META_TABLE_SQL)
        if statement_hook is not None:
            statement_hook("create_meta")
        connection.execute(
            """
            INSERT INTO NioIngestMeta (
                account_id, device_id, schema_version, stream_id,
                binding_operation_id, journal_generation,
                consumer_generation, consumer_first_sequence,
                baseline_rooms_sha256, consumer_attached_revision, revision,
                writer_epoch, lock_device, lock_inode, next_source_epoch,
                next_ready_order, next_batch_sequence, last_acked_sequence,
                last_acked_batch_id, last_acked_sha256, created_at_ns
            ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, 0, ?, ?, ?,
                      1, 0, 1, 0, NULL, NULL, ?)
            """,
            (
                account_id,
                device_id,
                SCHEMA_VERSION,
                str(uuid4()),
                str(uuid4()),
                str(writer_epoch),
                *lock_identity,
                time.time_ns(),
            ),
        )
        if statement_hook is not None:
            statement_hook("insert_meta")
        for index, statement in enumerate(SCHEMA_SQL):
            connection.execute(statement.strip())
            if statement_hook is not None:
                statement_hook(f"schema_{index}")


def _open_existing(
    connection: sqlite3.Connection,
    *,
    account_id: str,
    device_id: str,
    lock_identity: FileIdentity,
) -> UUID:
    validate_schema_topology(connection)
    rows = connection.execute(
        "SELECT account_id, device_id, schema_version, writer_epoch, "
        "lock_device, lock_inode FROM NioIngestMeta"
    ).fetchall()
    if len(rows) != 1:
        raise LocalProtocolError("ingestion-v1 marker row cardinality is not one")
    row = rows[0]
    if row["account_id"] != account_id:
        raise LocalProtocolError("ingestion account_id does not match")
    if row["device_id"] != device_id:
        raise LocalProtocolError("ingestion device_id does not match")
    if row["schema_version"] != SCHEMA_VERSION:
        raise LocalProtocolError(
            f"unsupported ingestion schema_version {row['schema_version']}"
        )
    if (row["lock_device"], row["lock_inode"]) != lock_identity:
        raise LocalProtocolError("persisted ingestion lock file identity changed")

    old_epoch = row["writer_epoch"]
    writer_epoch = uuid4()
    with immediate_transaction(connection):
        cursor = connection.execute(
            "UPDATE NioIngestMeta SET writer_epoch = ? "
            "WHERE account_id = ? AND writer_epoch = ? "
            "AND lock_device = ? AND lock_inode = ?",
            (str(writer_epoch), account_id, old_epoch, *lock_identity),
        )
        if cursor.rowcount != 1:
            raise LocalProtocolError("persisted writer_epoch changed during open")
    return writer_epoch


@dataclass(frozen=True)
class OpenedJournalDatabase:
    path: Path
    connection: sqlite3.Connection
    writer_lock: StableFileLock
    writer_epoch: UUID
    file_identity: FileIdentity


def open_journal_database(
    database: str | os.PathLike[str] | SqliteDatabase,
    *,
    account_id: str,
    device_id: str,
    sqlite_busy_timeout_ms: int,
    statement_observer: Callable[[str], None] | None,
    schema_statement_hook: Callable[[str], None] | None,
) -> OpenedJournalDatabase:
    path = database_path(database)
    path.parent.mkdir(parents=True, exist_ok=True)
    writer_lock = StableFileLock(path)
    connection: sqlite3.Connection | None = None
    try:
        marker_exists, has_tables = database_shape(path)
        if not marker_exists and has_tables:
            raise FreshIngestionRequired(
                "nonempty store has no ingestion-v1 marker; explicit fresh "
                "initialization is required"
            )
        connection = sqlite3.connect(
            path,
            isolation_level=None,
            timeout=sqlite_busy_timeout_ms / 1000,
        )
        connection.row_factory = sqlite3.Row
        if statement_observer is not None:
            connection.set_trace_callback(statement_observer)
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms}")
        if marker_exists:
            writer_epoch = _open_existing(
                connection,
                account_id=account_id,
                device_id=device_id,
                lock_identity=writer_lock.identity,
            )
        else:
            writer_epoch = uuid4()
            _create_fresh(
                connection,
                account_id=account_id,
                device_id=device_id,
                writer_epoch=writer_epoch,
                lock_identity=writer_lock.identity,
                statement_hook=schema_statement_hook,
            )
        connection.execute("PRAGMA journal_mode = WAL")
        connection.execute("PRAGMA synchronous = NORMAL")
        stat = os.stat(path)
        return OpenedJournalDatabase(
            path,
            connection,
            writer_lock,
            writer_epoch,
            (stat.st_dev, stat.st_ino),
        )
    except BaseException:
        if connection is not None:
            connection.close()
        writer_lock.close()
        raise
