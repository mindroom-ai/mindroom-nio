from __future__ import annotations

import hashlib
import json
import os
import sqlite3
import time
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any
from uuid import UUID, uuid4

from peewee import DatabaseError as PeeweeDatabaseError
from peewee import SqliteDatabase
from playhouse.sqliteq import SqliteQueueDatabase

from ..exceptions import LocalProtocolError
from ..ingest._json import load_internal_json
from ..ingest.config import (
    ClassicSourceConfig,
    SlidingSourceConfig,
    SourceConfig,
    source_transport,
)
from ..ingest.errors import FreshIngestionRequired, JournalIntegrityError
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
from ..ingest.state import SourceState
from ._ingestion_store_owner import IngestionStoreOwner
from ._ingestion_store_owner import StableFileLock as StableFileLock
from .sync_journal_schema import META_TABLE_SQL, SCHEMA_SQL, SCHEMA_VERSION

if TYPE_CHECKING:
    from collections.abc import Callable


_E2EE_TABLES = frozenset(
    {
        "accounts",
        "devicekeys",
        "devicetruststate",
        "encryptedrooms",
        "forwardedchains",
        "keys",
        "megolminboundsessions",
        "olmsessions",
        "outgoingkeyrequests",
        "storeversion",
    }
)


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
    return Path(os.path.abspath(path))


def database_shape(path: str | os.PathLike[str]) -> tuple[bool, bool]:
    database = Path(path)
    if not database.exists() or database.stat().st_size == 0:
        return False, False
    connection = SqliteDatabase(
        f"file:{database}?mode=ro",
        autoconnect=False,
        uri=True,
    )
    connection.connect()
    try:
        rows = connection.execute_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table'"
        ).fetchall()
    finally:
        connection.close()
    names = {row[0] for row in rows}
    return "NioIngestMeta" in names, bool(names)


def database_has_ingestion_marker(path: str | os.PathLike[str]) -> bool:
    return database_shape(path)[0]


def _normalized_sql(sql: str | None) -> str | None:
    return " ".join(sql.split()) if sql is not None else None


def _pragma_rows(
    connection: sqlite3.Connection | SqliteDatabase,
    pragma: str,
    name: str,
) -> tuple[tuple[object, ...], ...]:
    return tuple(tuple(row) for row in _execute(connection, f'{pragma}("{name}")'))


def _execute(
    connection: sqlite3.Connection | SqliteDatabase,
    sql: str,
    parameters: tuple[object, ...] = (),
):
    if isinstance(connection, SqliteDatabase):
        return connection.execute_sql(sql, parameters)
    return connection.execute(sql, parameters)


def _capture_contract(
    connection: sqlite3.Connection | SqliteDatabase,
) -> tuple[object, ...]:
    table_names = tuple(
        row[0]
        for row in _execute(
            connection,
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name GLOB 'NioIngest*' ORDER BY name",
        )
    )
    master = tuple(
        (row[0], row[1], row[2], _normalized_sql(row[3]))
        for row in _execute(
            connection,
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE name GLOB 'NioIngest*' OR tbl_name GLOB 'NioIngest*' "
            "ORDER BY type, name",
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
                    for row in _execute(connection, f'PRAGMA index_list("{table}")')
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
            connection.execute(statement)
        return _capture_contract(connection)


def validate_schema_topology(connection: SqliteDatabase) -> None:
    try:
        if _capture_contract(connection) != _expected_contract():
            raise FreshIngestionRequired(
                "ingestion-v1 schema topology does not match v1"
            )
    except (sqlite3.DatabaseError, PeeweeDatabaseError) as error:
        raise FreshIngestionRequired(
            "ingestion-v1 schema topology is invalid"
        ) from error


def _validate_source_cursor(
    transport_kind: TransportKind,
    cursor_json: bytes,
) -> None:
    try:
        if transport_kind is TransportKind.CLASSIC:
            canonical = canonical_classic_cursor(_classic_cursor_from_json(cursor_json))
        else:
            canonical = canonical_sliding_cursor(_sliding_cursor_from_json(cursor_json))
    except (TypeError, ValueError) as error:
        raise JournalIntegrityError(
            f"persisted {transport_kind.value} source cursor is invalid"
        ) from error
    if canonical != cursor_json:
        raise JournalIntegrityError(
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


def _canonical_internal(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


_STORED_ROWS = {
    "NioIngestSourceState": "source source_epoch next_request_id active",
    "NioIngestFrame": "frame frame_id source_epoch request_id staged_revision",
    "NioIngestFrameDrainHeader": "frame frame_id source_epoch request_id staged_revision payload_sha256 payload_length room_materialized_revision",
    "NioIngestRoomAggregate": "aggregate room_id updated_revision intent_kind",
    "NioIngestWork": "work work_id kind status frame_id room_id membership_epoch room_sequence ready_revision ready_ordinal created_revision",
}


def _row(
    owner: tuple[str, UUID, TransportKind],
    table: str,
    value: Any,
    digest: object = None,
    header: bytes | tuple[object, ...] = b"",
) -> Any:
    if digest is not None:
        envelope = load_internal_json(value, "stored payload")
        payload, value = value, _canonical_internal(dict.get(envelope, "value"))
    kind, *fields = _STORED_ROWS[table].split()
    clear = [*header] if isinstance(header, tuple) else load_internal_json(header, kind)
    prefix = _canonical_internal(
        {
            "schema_version": 1,
            "row_kind": kind,
            "account_id": owner[0],
            "stream_id": str(owner[1]),
            "transport_kind": owner[2].value,
            **dict(zip(fields, clear, strict=True)),
        }
    )
    if value is None:
        return hashlib.sha256(prefix).digest()
    expected = prefix[:-1] + b',"value":' + value + b"}"
    if digest is None:
        return expected, hashlib.sha256(expected).digest()
    if hashlib.sha256(payload).digest() != digest or payload != expected:
        raise ValueError("stored payload is not canonical")
    return value


def _source_header(source: SourceState) -> bytes:
    return _canonical_internal(
        [
            source.source_epoch,
            source.next_request_id,
            source.active,
        ]
    )


def _create_fresh(
    connection: SqliteDatabase,
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    source: SourceConfig,
    writer_epoch: UUID,
    statement_hook: Callable[[str], None] | None,
) -> tuple[UUID, SourceState]:
    transport_kind = source_transport(source)
    stream_id = uuid4()
    source_state = SourceState(
        0,
        transport_kind,
        _cold_source_cursor(source),
        0,
        True,
    )
    owner = account_id, stream_id, transport_kind
    payload, payload_sha256 = _row(
        owner,
        "NioIngestSourceState",
        source_state.cursor_json,
        header=_source_header(source_state),
    )
    connection.execute_sql(META_TABLE_SQL)
    if statement_hook is not None:
        statement_hook("create_meta")
    connection.execute_sql(
        "INSERT INTO NioIngestMeta ("
        "account_id, device_id, schema_version, stream_id, consumer_generation, transport_kind, "
        "revision, writer_epoch, next_source_epoch, created_at_ns"
        ") VALUES (?, ?, ?, ?, ?, ?, 0, ?, 1, ?)",
        (
            account_id,
            device_id,
            SCHEMA_VERSION,
            str(stream_id),
            str(consumer_generation),
            transport_kind.value,
            str(writer_epoch),
            time.time_ns(),
        ),
    )
    if statement_hook is not None:
        statement_hook("insert_meta")
    for index, statement in enumerate(SCHEMA_SQL):
        connection.execute_sql(statement)
        if statement_hook is not None:
            statement_hook(f"schema_{index}")
    connection.execute_sql(
        "INSERT INTO NioIngestSourceState ("
        "account_id, source_epoch, payload, payload_sha256, "
        "next_request_id, active) VALUES (?, 0, ?, ?, 0, 1)",
        (account_id, payload, payload_sha256),
    )
    if statement_hook is not None:
        statement_hook("insert_source")
    return stream_id, source_state


def _inspect_existing(
    connection: SqliteDatabase,
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    source: SourceConfig,
) -> tuple[UUID, str]:
    validate_schema_topology(connection)
    all_tables = {
        row[0]
        for row in connection.execute_sql(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name NOT GLOB 'sqlite_*'"
        )
    }
    borrowed_tables = {name for name in all_tables if not name.startswith("NioIngest")}
    if borrowed_tables not in (set(), set(_E2EE_TABLES)):
        raise FreshIngestionRequired(
            "ingestion-v1 store has unexpected or incomplete borrowed tables"
        )
    if borrowed_tables:
        versions = connection.execute_sql("SELECT version FROM storeversion").fetchall()
        if len(versions) != 1 or versions[0][0] != 10:
            raise FreshIngestionRequired(
                "ingestion-v1 store has an unsupported borrowed schema"
            )
    rows = connection.execute_sql("SELECT * FROM NioIngestMeta").fetchall()
    if len(rows) != 1:
        raise FreshIngestionRequired("ingestion-v1 marker row cardinality is not one")
    row = rows[0]
    if row["account_id"] != account_id:
        raise FreshIngestionRequired("ingestion account_id does not match")
    if row["device_id"] != device_id:
        raise FreshIngestionRequired("ingestion device_id does not match")
    if row["schema_version"] != SCHEMA_VERSION:
        raise FreshIngestionRequired(
            f"unsupported schema_version {row['schema_version']}"
        )
    try:
        transport_kind = TransportKind(row["transport_kind"])
    except (TypeError, ValueError) as error:
        raise JournalIntegrityError("ingestion transport_kind is invalid") from error
    if source_transport(source) is not transport_kind:
        raise FreshIngestionRequired("ingestion transport kind does not match source")
    try:
        stream_id = UUID(row["stream_id"])
        stored_consumer_generation = UUID(row["consumer_generation"])
        UUID(row["writer_epoch"])
    except (AttributeError, TypeError, ValueError) as error:
        raise JournalIntegrityError("ingestion owner UUID is invalid") from error
    if row["consumer_generation"] != str(stored_consumer_generation):
        raise JournalIntegrityError("ingestion consumer_generation is not canonical")
    if stored_consumer_generation != consumer_generation:
        raise LocalProtocolError("ingestion consumer_generation does not match")

    source_rows = connection.execute_sql(
        "SELECT * FROM NioIngestSourceState"
    ).fetchall()
    if len(source_rows) != 1:
        raise JournalIntegrityError("ingestion source row cardinality is not one")
    source_row = source_rows[0]
    if source_row["account_id"] != account_id:
        raise JournalIntegrityError("ingestion source account_id does not match")
    try:
        state = SourceState(
            source_row["source_epoch"],
            transport_kind,
            b"",
            source_row["next_request_id"],
            bool(source_row["active"]),
        )
        if type(source_row["active"]) is not int or source_row["active"] not in (0, 1):
            raise ValueError("source active column is invalid")
        cursor_json = _row(
            (account_id, stream_id, transport_kind),
            "NioIngestSourceState",
            source_row["payload"],
            source_row["payload_sha256"],
            header=_source_header(state),
        )
        state = SourceState(
            state.source_epoch,
            state.transport_kind,
            cursor_json,
            state.next_request_id,
            state.active,
        )
    except JournalIntegrityError:
        raise
    except (TypeError, ValueError) as error:
        raise JournalIntegrityError("persisted source state is invalid") from error
    _validate_source_cursor(transport_kind, state.cursor_json)
    return stream_id, row["writer_epoch"]


@dataclass(frozen=True)
class OpenedJournalDatabase:
    path: Path
    owner: IngestionStoreOwner
    writer_epoch: UUID
    stream_id: UUID


def open_journal_database(
    database: str | os.PathLike[str] | SqliteDatabase,
    *,
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    source: SourceConfig,
    sqlite_busy_timeout_ms: int,
    statement_observer: Callable[[str], None] | None,
    schema_statement_hook: Callable[[str], None] | None,
) -> OpenedJournalDatabase:
    if type(consumer_generation) is not UUID:
        raise TypeError("consumer_generation must be UUID")
    source_transport(source)
    path = database_path(database)
    path.parent.mkdir(parents=True, exist_ok=True)
    owner = IngestionStoreOwner(path, sqlite_busy_timeout_ms, statement_observer)
    connection = owner.database
    try:
        if owner.is_fresh:
            connection.execute_sql("PRAGMA foreign_keys = ON")
            connection.execute_sql(f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms}")
            writer_epoch = uuid4()
            with owner.bootstrap_write():
                stream_id, _source_state = _create_fresh(
                    connection,
                    account_id,
                    device_id,
                    consumer_generation,
                    source,
                    writer_epoch,
                    schema_statement_hook,
                )
        else:
            try:
                stream_id, old_epoch = _inspect_existing(
                    connection,
                    account_id,
                    device_id,
                    consumer_generation,
                    source,
                )
            except (sqlite3.DatabaseError, PeeweeDatabaseError) as error:
                raise FreshIngestionRequired(
                    "nonempty store without a valid ingestion-v1 marker requires "
                    "fresh initialization"
                ) from error
            connection.execute_sql("PRAGMA foreign_keys = ON")
            connection.execute_sql(f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms}")
            writer_epoch = uuid4()
            with owner.bootstrap_write():
                cursor = connection.execute_sql(
                    "UPDATE NioIngestMeta SET writer_epoch = ? "
                    "WHERE account_id = ? AND writer_epoch = ?",
                    (str(writer_epoch), account_id, old_epoch),
                )
                if cursor.rowcount != 1:
                    raise LocalProtocolError(
                        "persisted writer_epoch changed during open"
                    )

        connection.execute_sql("PRAGMA journal_mode = WAL")
        connection.execute_sql("PRAGMA synchronous = NORMAL")
        owner.activate(account_id, writer_epoch)
        return OpenedJournalDatabase(
            path,
            owner,
            writer_epoch,
            stream_id,
        )
    except BaseException:
        owner.close()
        raise
