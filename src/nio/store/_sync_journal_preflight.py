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
from ..ingest.config import (
    ClassicSourceConfig,
    SlidingSourceConfig,
    SourceConfig,
    source_transport,
)
from ..ingest.errors import FreshIngestionRequired
from ..ingest.model import TransportKind
from ..ingest.sliding import (
    SlidingCursor,
    SlidingRangeAckMode,
    _sliding_cursor_from_json,
    canonical_sliding_cursor,
)
from ..ingest.source import (
    ClassicCursor,
    _classic_cursor_from_json,
    canonical_classic_cursor,
)
from ._sync_journal_codec import EncryptedRowCodec
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
        self.owner_pid = os.getpid()
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

    def assert_process_owner(self) -> None:
        if os.getpid() != self.owner_pid:
            raise LocalProtocolError("ownership belongs to the acquiring process")

    def assert_identity(self) -> None:
        self.assert_process_owner()
        if self._fd < 0:
            raise LocalProtocolError("ingestion writer lock is closed")
        try:
            stat = os.stat(self.path)
        except FileNotFoundError as error:
            raise LocalProtocolError(
                "ingestion lock file identity is no longer present"
            ) from error
        actual = (stat.st_dev, stat.st_ino)
        if actual != self.identity:
            raise LocalProtocolError(
                "ingestion lock file identity changed after lock acquisition"
            )

    def close(self) -> None:
        self.assert_process_owner()
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


def database_shape(path: str | os.PathLike[str]) -> tuple[bool, bool]:
    database = Path(path)
    if not database.exists() or database.stat().st_size == 0:
        return False, False
    with sqlite3.connect(f"file:{database}?mode=ro", uri=True) as connection:
        rows = connection.execute(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    names = {row[0] for row in rows}
    return "NioIngestMeta" in names, bool(names)


def database_has_ingestion_marker(path: str | os.PathLike[str]) -> bool:
    return database_shape(path)[0]


def _normalized_sql(sql: str | None) -> str | None:
    return " ".join(sql.split()) if sql is not None else None


def _pragma_rows(
    connection: sqlite3.Connection, pragma: str, name: str
) -> tuple[tuple[object, ...], ...]:
    return tuple(tuple(row) for row in connection.execute(f'{pragma}("{name}")'))


def _capture_contract(connection: sqlite3.Connection) -> tuple[object, ...]:
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
    details = tuple(
        (
            table,
            _pragma_rows(connection, "PRAGMA table_info", table),
            _pragma_rows(connection, "PRAGMA foreign_key_list", table),
            tuple(
                sorted(
                    (
                        tuple(row),
                        _pragma_rows(connection, "PRAGMA index_xinfo", row[1]),
                    )
                    for row in connection.execute(f'PRAGMA index_list("{table}")')
                )
            ),
        )
        for table in table_names
    )
    return master, details


@cache
def _expected_contract() -> tuple[object, ...]:
    with sqlite3.connect(":memory:") as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(META_TABLE_SQL)
        for statement in SCHEMA_SQL:
            connection.execute(statement.strip())
        return _capture_contract(connection)


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


def _validate_source_cursor(
    transport_kind: TransportKind,
    cursor_json: bytes,
) -> None:
    try:
        if transport_kind is TransportKind.CLASSIC:
            classic_cursor = _classic_cursor_from_json(cursor_json)
            canonical = canonical_classic_cursor(classic_cursor)
        else:
            sliding_cursor = _sliding_cursor_from_json(cursor_json)
            canonical = canonical_sliding_cursor(sliding_cursor)
    except (TypeError, ValueError) as error:
        raise LocalProtocolError(
            f"persisted {transport_kind.value} source cursor is invalid"
        ) from error
    if canonical != cursor_json:
        raise LocalProtocolError(
            f"persisted {transport_kind.value} source cursor is not canonical"
        )


def _cold_source_cursor(source: SourceConfig) -> bytes:
    if type(source) is ClassicSourceConfig:
        return canonical_classic_cursor(ClassicCursor(None))
    if type(source) is not SlidingSourceConfig:
        source_transport(source)
        raise AssertionError("unreachable")
    return canonical_sliding_cursor(
        SlidingCursor(
            pos=None,
            to_device_since=None,
            connection_instance=uuid4(),
            connection_name=source.connection_name,
            all_rooms_range_end=source.all_rooms_page_size - 1,
            all_rooms_page_size=source.all_rooms_page_size,
            all_rooms_range_ack_mode=SlidingRangeAckMode.UNKNOWN,
            all_rooms_coverage_complete=False,
        )
    )


def _create_fresh(
    connection: sqlite3.Connection,
    account_id: str,
    device_id: str,
    pickle_key: str,
    source: SourceConfig,
    writer_epoch: UUID,
    statement_hook: Callable[[str], None] | None,
) -> None:
    transport_kind = source_transport(source)
    stream_id = uuid4()
    cursor_json = _cold_source_cursor(source)
    cursor_ciphertext, cursor_sha256 = EncryptedRowCodec(
        pickle_key,
        account_id,
        stream_id,
    ).seal("NioIngestSourceState", (account_id,), cursor_json)
    with immediate_transaction(connection):
        connection.execute(META_TABLE_SQL)
        if statement_hook is not None:
            statement_hook("create_meta")
        connection.execute(
            """INSERT INTO NioIngestMeta (
                account_id, device_id, schema_version, stream_id,
                transport_kind, binding_operation_id, consumer_attach_status,
                consumer_attach_next_room_ordinal, journal_generation,
                consumer_generation, consumer_first_sequence,
                baseline_rooms_sha256, consumer_attached_revision, revision,
                writer_epoch, next_source_epoch, next_ready_order,
                next_batch_sequence, last_acked_sequence, last_acked_batch_id,
                last_acked_sha256, created_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, 'unbound', 0, NULL, NULL, NULL, NULL, NULL, 0, ?,
                      1, 0, 1, 0, NULL, NULL, ?)""",
            (
                account_id,
                device_id,
                SCHEMA_VERSION,
                str(stream_id),
                transport_kind.value,
                str(uuid4()),
                str(writer_epoch),
                time.time_ns(),
            ),
        )
        if statement_hook is not None:
            statement_hook("insert_meta")
        for index, statement in enumerate(SCHEMA_SQL):
            connection.execute(statement.strip())
            if statement_hook is not None:
                statement_hook(f"schema_{index}")
        connection.execute(
            """INSERT INTO NioIngestSourceState (
                account_id, source_epoch, cursor_ciphertext, cursor_sha256,
                next_request_id, active
            ) VALUES (?, 0, ?, ?, 1, 1)""",
            (account_id, cursor_ciphertext, cursor_sha256),
        )
        if statement_hook is not None:
            statement_hook("insert_source")


def _open_existing(
    connection: sqlite3.Connection,
    account_id: str,
    device_id: str,
    pickle_key: str,
    source: SourceConfig,
) -> UUID:
    validate_schema_topology(connection)
    rows = connection.execute(
        "SELECT account_id, device_id, schema_version, stream_id, "
        "transport_kind, writer_epoch "
        "FROM NioIngestMeta"
    ).fetchall()
    if len(rows) != 1:
        raise LocalProtocolError("ingestion-v1 marker row cardinality is not one")
    row = rows[0]
    if row["account_id"] != account_id:
        raise LocalProtocolError("ingestion account_id does not match")
    if row["device_id"] != device_id:
        raise LocalProtocolError("ingestion device_id does not match")
    if row["schema_version"] != SCHEMA_VERSION:
        raise LocalProtocolError(f"unsupported schema_version {row['schema_version']}")
    try:
        transport_kind = TransportKind(row["transport_kind"])
    except ValueError as error:
        raise LocalProtocolError("ingestion transport_kind is invalid") from error
    if source_transport(source) is not transport_kind:
        raise LocalProtocolError("ingestion transport kind does not match source")

    source_rows = connection.execute("SELECT * FROM NioIngestSourceState").fetchall()
    if len(source_rows) != 1:
        raise LocalProtocolError("ingestion source row cardinality is not one")
    source_row = source_rows[0]
    if source_row["account_id"] != account_id:
        raise LocalProtocolError("ingestion source account_id does not match")
    try:
        stream_id = UUID(row["stream_id"])
    except (TypeError, ValueError) as error:
        raise LocalProtocolError("ingestion stream_id is invalid") from error
    cursor_json = EncryptedRowCodec(
        pickle_key,
        account_id,
        stream_id,
    ).decrypt(
        "NioIngestSourceState",
        (account_id,),
        bytes(source_row["cursor_ciphertext"]),
        bytes(source_row["cursor_sha256"]),
    )
    _validate_source_cursor(transport_kind, cursor_json)
    old_epoch = row["writer_epoch"]
    writer_epoch = uuid4()
    with immediate_transaction(connection):
        cursor = connection.execute(
            "UPDATE NioIngestMeta SET writer_epoch = ? "
            "WHERE account_id = ? AND writer_epoch = ?",
            (str(writer_epoch), account_id, old_epoch),
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
    pickle_key: str,
    source: SourceConfig,
    sqlite_busy_timeout_ms: int,
    statement_observer: Callable[[str], None] | None,
    schema_statement_hook: Callable[[str], None] | None,
) -> OpenedJournalDatabase:
    source_transport(source)
    path = database_path(database)
    path.parent.mkdir(parents=True, exist_ok=True)
    writer_lock = StableFileLock(path)
    connection: sqlite3.Connection | None = None
    try:
        marker_exists, has_tables = database_shape(path)
        if not marker_exists and has_tables:
            raise FreshIngestionRequired(
                "nonempty store without ingestion-v1 marker requires fresh initialization"
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
                account_id,
                device_id,
                pickle_key,
                source,
            )
        else:
            writer_epoch = uuid4()
            _create_fresh(
                connection,
                account_id,
                device_id,
                pickle_key,
                source,
                writer_epoch,
                schema_statement_hook,
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
