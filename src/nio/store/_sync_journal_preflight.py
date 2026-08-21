from __future__ import annotations

import os
import re
import sqlite3
import stat
import time
from dataclasses import dataclass
from functools import cache
from pathlib import Path
from typing import TYPE_CHECKING, Any, cast
from uuid import UUID, uuid4

from peewee import DatabaseError as PeeweeDatabaseError
from peewee import SqliteDatabase, sort_models
from playhouse.sqliteq import SqliteQueueDatabase

from ..crypto import OlmAccount, TrustState
from ..exceptions import LocalProtocolError
from ..ingest.config import (
    ClassicSourceConfig,
    SlidingSourceConfig,
    SourceConfig,
    source_transport,
)
from ..ingest.errors import (
    FreshIngestionRequired,
    JournalConflictError,
    JournalIntegrityError,
    _MarkedStoreRequiresSqlite,
)
from ..ingest.model import (
    EventRecord,
    RecordKind,
    RecordOrigin,
    SystemOrigin,
    SystemOriginKind,
    TransportKind,
    _local_membership_evidence,
    _LocalMembershipEvidence,
)
from ..ingest.sliding import (
    SlidingCursor,
    SlidingRangeAckMode,
    _sliding_cursor_from_json,
    canonical_sliding_cursor,
    reset_sliding_connection,
)
from ..ingest.source import (
    ClassicCursor,
    canonical_classic_cursor,
)
from ..ingest.state import OwnerView, SourceState, StagedFrame
from ._ingestion_store_owner import IngestionStoreOwner
from ._ingestion_store_owner import StableFileLock as StableFileLock
from ._sync_journal_format import (
    _canonical_internal as _canonical_internal,
)
from ._sync_journal_format import _row as _row
from ._sync_journal_format import _source_header as _source_header
from ._sync_journal_format import _validate_source_cursor as _validate_source_cursor
from ._sync_journal_values import SQLITE_INT_MAX, DeliveryState, RoomAggregateValue
from .file_trustdb import Ed25519Key, KeyStore
from .models import (
    Accounts,
    DeviceKeys,
    DeviceTrustState,
    EncryptedRooms,
    ForwardedChains,
    Keys,
    MegolmInboundSessions,
    OlmSessions,
    OutgoingKeyRequests,
    StoreVersion,
    SyncTokens,
)
from .sync_journal_schema import META_TABLE_SQL, SCHEMA_SQL, SCHEMA_VERSION

if TYPE_CHECKING:
    from collections.abc import Callable, Mapping

    from .database import MatrixStore


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

_ACTIVE_ORDINARY_MODELS = (
    Accounts,
    OlmSessions,
    MegolmInboundSessions,
    ForwardedChains,
    DeviceKeys,
    EncryptedRooms,
    OutgoingKeyRequests,
    StoreVersion,
    Keys,
    SyncTokens,
)
_RETIRED_RECOVERY_SCHEMA_SQL = (
    'CREATE TABLE "pendingtimelineevents" ("id" INTEGER NOT NULL PRIMARY KEY, '
    '"room_id" TEXT NOT NULL, "generation" INTEGER NOT NULL, '
    '"sequence" INTEGER NOT NULL, "event_id" TEXT NOT NULL, '
    '"event_payload" BLOB NOT NULL, "is_live" INTEGER NOT NULL, '
    '"was_encrypted" INTEGER NOT NULL, "was_completed" INTEGER NOT NULL, '
    '"admission_accepted" INTEGER NOT NULL, "provenance" TEXT NOT NULL, '
    '"apply_room_state" INTEGER NOT NULL, "account_id" INTEGER NOT NULL, '
    'FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") '
    "ON DELETE CASCADE, UNIQUE(account_id,room_id,event_id))",
    'CREATE TABLE "slidingwindowtokens" ("id" INTEGER NOT NULL PRIMARY KEY, '
    '"room_id" TEXT NOT NULL, "token" TEXT NOT NULL, '
    '"membership_event_id" TEXT NOT NULL, "account_id" INTEGER NOT NULL, '
    'FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") '
    "ON DELETE CASCADE, UNIQUE(account_id,room_id))",
    'CREATE TABLE "syncrecoveryabandonedrooms" ('
    '"id" INTEGER NOT NULL PRIMARY KEY, "room_id" TEXT NOT NULL, '
    '"reason" TEXT NOT NULL, "account_id" INTEGER NOT NULL, '
    'FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") '
    "ON DELETE CASCADE, UNIQUE(account_id,room_id,reason))",
    'CREATE TABLE "syncrecoverygaps" ("id" INTEGER NOT NULL PRIMARY KEY, '
    '"room_id" TEXT NOT NULL, "generation" INTEGER NOT NULL, '
    '"target_token" TEXT NOT NULL, "cursor_token" TEXT, '
    '"membership_bound" INTEGER NOT NULL, "account_id" INTEGER NOT NULL, '
    'FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") '
    "ON DELETE CASCADE, UNIQUE(account_id,room_id,generation))",
    'CREATE INDEX "pendingtimelineevents_account_id" '
    'ON "pendingtimelineevents" ("account_id")',
    'CREATE INDEX "slidingwindowtokens_account_id" '
    'ON "slidingwindowtokens" ("account_id")',
    'CREATE INDEX "syncrecoveryabandonedrooms_account_id" '
    'ON "syncrecoveryabandonedrooms" ("account_id")',
    'CREATE INDEX "syncrecoverygaps_account_id" '
    'ON "syncrecoverygaps" ("account_id")',
)
_RETIRED_RECOVERY_TABLES = frozenset(
    {
        "pendingtimelineevents",
        "slidingwindowtokens",
        "syncrecoveryabandonedrooms",
        "syncrecoverygaps",
    }
)
_TRUST_SIDECAR_SUFFIXES = (
    "trusted_devices",
    "blacklisted_devices",
    "ignored_devices",
)
_DEVICE_TRUST_SQL = (
    'CREATE TABLE "devicetruststate" ('
    '"device_id" INTEGER NOT NULL PRIMARY KEY, '
    '"state" INTEGER NOT NULL, '
    'FOREIGN KEY ("device_id") REFERENCES "devicekeys" ("id"))'
)
_MIGRATED_COLUMN_DEFAULTS = {
    ("pendingtimelineevents", "admission_accepted"): frozenset({None, "0"}),
    ("pendingtimelineevents", "provenance"): frozenset({None, "'live'"}),
    ("pendingtimelineevents", "apply_room_state"): frozenset({None, "1"}),
    ("syncrecoverygaps", "membership_bound"): frozenset({None, "0"}),
}


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


def _check_constraints(sql: str | None) -> tuple[str, ...]:
    """Return normalized CHECK expressions without depending on DDL history."""

    if sql is None:
        return ()
    upper = sql.upper()
    expressions: list[str] = []
    offset = 0
    while (start := upper.find("CHECK", offset)) >= 0:
        cursor = start + len("CHECK")
        while cursor < len(sql) and sql[cursor].isspace():
            cursor += 1
        if cursor >= len(sql) or sql[cursor] != "(":
            offset = cursor
            continue
        depth = 1
        quote: str | None = None
        end = cursor + 1
        while end < len(sql) and depth:
            character = sql[end]
            if quote is not None:
                if character == quote:
                    if end + 1 < len(sql) and sql[end + 1] == quote:
                        end += 1
                    else:
                        quote = None
            elif character in {"'", '"'}:
                quote = character
            elif character == "(":
                depth += 1
            elif character == ")":
                depth -= 1
            end += 1
        if depth:
            raise FreshIngestionRequired("configured ordinary CHECK is malformed")
        expressions.append(" ".join(sql[cursor + 1 : end - 1].split()).lower())
        offset = end
    return tuple(expressions)


def _ddl_attributes(sql: str | None) -> tuple[object, ...]:
    normalized = _normalized_sql(sql)
    if normalized is None:
        return ()
    upper = normalized.upper()
    return (
        "AUTOINCREMENT" in upper,
        "WITHOUT ROWID" in upper,
        bool(re.search(r"\bSTRICT\s*$", upper)),
        tuple(re.findall(r"\bCOLLATE\s+([A-Z0-9_]+)", upper)),
        tuple(re.findall(r"\bON\s+CONFLICT\s+([A-Z]+)", upper)),
        "DEFERRABLE" in upper,
        "INITIALLY DEFERRED" in upper,
    )


def _semantic_columns(
    connection: sqlite3.Connection | SqliteDatabase,
    table: str,
) -> tuple[tuple[object, ...], ...]:
    rows = _pragma_rows(connection, "PRAGMA table_xinfo", table)
    names = [row[1] for row in rows]
    if len(names) != len(set(names)):
        raise FreshIngestionRequired("configured ordinary columns are duplicated")
    stable: list[tuple[object, ...]] = []
    migrated: list[tuple[object, ...]] = []
    for cid, name, kind, not_null, default, primary_key, hidden in rows:
        name = cast("str", name)
        allowed = _MIGRATED_COLUMN_DEFAULTS.get((table, name))
        if allowed is not None:
            if default not in allowed:
                raise FreshIngestionRequired(
                    "configured migrated column default is unsupported"
                )
            default = "<ordinary-v10-migrated-default>"
        semantic = (
            name,
            kind,
            not_null,
            default,
            primary_key,
            hidden,
        )
        if allowed is not None:
            migrated.append((None, *semantic))
        else:
            # ALTER-added v10 fields may legally appear after the original
            # columns, but the original declaration order remains semantic.
            stable.append((len(stable), *semantic))
    return (*stable, *sorted(migrated))


def _semantic_indexes(
    connection: sqlite3.Connection | SqliteDatabase,
    table: str,
) -> tuple[tuple[object, ...], ...]:
    indexes: list[tuple[object, ...]] = []
    for row in _execute(connection, f'PRAGMA index_list("{table}")'):
        _sequence, name, unique, origin, partial = tuple(row)
        columns = tuple(
            (item[2], item[3], item[4], item[5])
            for item in _pragma_rows(connection, "PRAGMA index_xinfo", name)
        )
        indexes.append((name, unique, origin, partial, columns))
    return tuple(sorted(indexes))


def _table_flags(
    connection: sqlite3.Connection | SqliteDatabase,
    table: str,
) -> tuple[object, ...]:
    rows = tuple(
        tuple(row)
        for row in _execute(
            connection,
            "SELECT type, ncol, wr, strict FROM pragma_table_list "
            "WHERE schema = 'main' AND name = ?",
            (table,),
        )
    )
    if len(rows) != 1:
        raise FreshIngestionRequired("configured ordinary table flags are invalid")
    return rows[0]


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


def _model_contract(
    models: tuple[type, ...],
    *,
    extra_schema_sql: tuple[str, ...] = (),
) -> tuple[object, ...]:
    database = SqliteDatabase(":memory:")
    database.connect()
    try:
        database.execute_sql("PRAGMA foreign_keys = ON")
        with database.bind_ctx(models):
            database.create_tables(models)
            for statement in extra_schema_sql:
                database.execute_sql(statement)
            return _capture_named_contract(
                database,
                frozenset(cast("Any", model)._meta.table_name for model in models)
                | (_RETIRED_RECOVERY_TABLES if extra_schema_sql else frozenset()),
            )
    finally:
        database.close()


def _capture_named_contract(
    connection: sqlite3.Connection | SqliteDatabase,
    table_names: frozenset[str],
    *,
    allow_ingestion_objects: bool = False,
) -> tuple[object, ...]:
    master_rows = tuple(
        _execute(
            connection,
            "SELECT type, name, tbl_name, sql FROM sqlite_master "
            "WHERE type IN ('table', 'index', 'view', 'trigger') "
            "AND name NOT GLOB 'sqlite_*' ORDER BY type, name",
        )
    )
    ingestion_rows = tuple(
        row
        for row in master_rows
        if str(row[1]).startswith("NioIngest") or str(row[2]).startswith("NioIngest")
    )
    unexpected = tuple(
        row
        for row in master_rows
        if row[2] not in table_names and row not in ingestion_rows
    )
    if ingestion_rows and not allow_ingestion_objects:
        unexpected += ingestion_rows
    if unexpected:
        raise FreshIngestionRequired("configured ordinary topology has extra objects")
    master = tuple(
        (
            row[0],
            row[1],
            row[2],
            _check_constraints(row[3]),
            _ddl_attributes(row[3]),
        )
        for row in master_rows
        if row not in ingestion_rows
    )
    details = tuple(
        (
            table,
            _table_flags(connection, table),
            _semantic_columns(connection, table),
            tuple(
                sorted(
                    tuple(row)[2:]
                    for row in _execute(
                        connection, f'PRAGMA foreign_key_list("{table}")'
                    )
                )
            ),
            _semantic_indexes(connection, table),
        )
        for table in sorted(table_names)
    )
    return master, details


@cache
def _ordinary_contract(
    include_trust: bool,
    include_retired_recovery: bool,
) -> tuple[object, ...]:
    models = (
        *_ACTIVE_ORDINARY_MODELS,
        *((DeviceTrustState,) if include_trust else ()),
    )
    return _model_contract(
        models,
        extra_schema_sql=(
            _RETIRED_RECOVERY_SCHEMA_SQL if include_retired_recovery else ()
        ),
    )


def _ordinary_table_names(connection: SqliteDatabase) -> frozenset[str]:
    return frozenset(
        row[0]
        for row in connection.execute_sql(
            "SELECT name FROM sqlite_master WHERE type = 'table' "
            "AND name NOT GLOB 'sqlite_*' AND name NOT GLOB 'NioIngest*'"
        )
    )


def _validate_ordinary_topology(
    connection: SqliteDatabase,
    *,
    source_store_class: type[MatrixStore],
) -> bool:
    from .database import DefaultStore, SqliteStore

    active_tables = frozenset(
        model._meta.table_name for model in _ACTIVE_ORDINARY_MODELS
    )
    historical_tables = active_tables | _RETIRED_RECOVERY_TABLES
    trust_table = DeviceTrustState._meta.table_name
    actual_tables = _ordinary_table_names(connection)
    accepted_tables: tuple[frozenset[str], ...]
    if source_store_class is SqliteStore:
        accepted_tables = (
            active_tables | {trust_table},
            historical_tables | {trust_table},
        )
        include_trust = True
    elif source_store_class is DefaultStore:
        accepted_tables = (
            active_tables,
            active_tables | {trust_table},
            historical_tables,
            historical_tables | {trust_table},
        )
        include_trust = trust_table in actual_tables
    else:  # Pair validation should have rejected this before filesystem access.
        raise LocalProtocolError("configured source store class is unsupported")
    if actual_tables not in accepted_tables:
        raise FreshIngestionRequired("configured ordinary store topology is incomplete")
    include_retired_recovery = actual_tables in (
        historical_tables,
        historical_tables | {trust_table},
    )
    try:
        actual = _capture_named_contract(
            connection,
            actual_tables,
            allow_ingestion_objects=connection.table_exists("NioIngestMeta"),
        )
        expected = _ordinary_contract(include_trust, include_retired_recovery)
    except (sqlite3.DatabaseError, PeeweeDatabaseError) as error:
        raise FreshIngestionRequired(
            "configured ordinary store topology is invalid"
        ) from error
    if actual != expected:
        raise FreshIngestionRequired("configured ordinary store topology is malformed")
    return include_trust


def _authenticate_ordinary_store(
    connection: SqliteDatabase,
    *,
    source_store_class: type[MatrixStore],
    account_id: str,
    device_id: str,
    pickle_key: str,
) -> bool:
    include_trust = _validate_ordinary_topology(
        connection,
        source_store_class=source_store_class,
    )
    versions = connection.execute_sql("SELECT version FROM storeversion").fetchall()
    if len(versions) != 1 or type(versions[0][0]) is not int or versions[0][0] != 10:
        raise FreshIngestionRequired(
            "configured ordinary store version is not exact v10"
        )
    accounts = connection.execute_sql(
        "SELECT user_id, device_id, shared, account FROM accounts LIMIT 2"
    ).fetchall()
    if len(accounts) != 1:
        raise FreshIngestionRequired(
            "configured ordinary account cardinality is not one"
        )
    user, device, shared, pickle = tuple(accounts[0])
    if (user, device) != (account_id, device_id):
        raise FreshIngestionRequired(
            "configured ordinary account identity does not match"
        )
    if type(shared) is not int or shared not in (0, 1):
        raise FreshIngestionRequired(
            "configured ordinary account shared flag is invalid"
        )
    if type(pickle) is not bytes or not pickle:
        raise FreshIngestionRequired("configured ordinary account pickle is invalid")
    try:
        OlmAccount.from_pickle(pickle, pickle_key, bool(shared))
    except Exception as error:
        raise FreshIngestionRequired(
            "configured ordinary account pickle is invalid"
        ) from error
    from .database import DefaultStore, SqliteStore

    if include_trust and source_store_class is SqliteStore:
        trust_rows = connection.execute_sql(
            "SELECT state FROM devicetruststate"
        ).fetchall()
        if any(
            type(row[0]) is not int
            or row[0] not in {state.value for state in TrustState}
            for row in trust_rows
        ):
            raise FreshIngestionRequired("configured device trust state is invalid")
    foreign_key_failures = connection.execute_sql("PRAGMA foreign_key_check").fetchall()
    if source_store_class is DefaultStore:
        foreign_key_failures = [
            row for row in foreign_key_failures if row[0] != "devicetruststate"
        ]
    if foreign_key_failures:
        raise FreshIngestionRequired(
            "configured ordinary store has foreign key violations"
        )
    return include_trust


def _sidecar_paths(
    store_path: Path, account_id: str, device_id: str
) -> tuple[Path, ...]:
    prefix = store_path / f"{account_id}_{device_id}"
    return tuple(Path(f"{prefix}.{suffix}") for suffix in _TRUST_SIDECAR_SUFFIXES)


def _read_default_trust(
    connection: SqliteDatabase,
    paths: tuple[Path, ...],
) -> tuple[tuple[int, int], ...]:
    stores = tuple(KeyStore(str(path)) for path in paths)
    rows = connection.execute_sql(
        "SELECT d.id, d.user_id, d.device_id, k.key FROM devicekeys AS d "
        "JOIN keys AS k ON k.device_id = d.id WHERE k.key_type = 'ed25519' "
        "ORDER BY d.id"
    ).fetchall()
    effective: list[tuple[int, int]] = []
    for raw in rows:
        row_id, user_id, device_id, fingerprint = tuple(raw)
        key = Ed25519Key(user_id, device_id, fingerprint)
        for store, state in zip(
            stores,
            (TrustState.verified, TrustState.blacklisted, TrustState.ignored),
            strict=True,
        ):
            if store.check(key):
                effective.append((row_id, state.value))
                break
    return tuple(effective)


def _sidecar_snapshot(paths: tuple[Path, ...]) -> tuple[bytes | None, ...]:
    return tuple(path.read_bytes() if path.exists() else None for path in paths)


def _adopt_populated_store(
    connection: SqliteDatabase,
    *,
    path: Path,
    store_path: Path,
    source_store_class: type[MatrixStore],
    account_id: str,
    device_id: str,
    pickle_key: str,
    consumer_generation: UUID,
    source: SourceConfig,
    writer_epoch: UUID,
    statement_hook: Callable[[str], None] | None,
) -> tuple[UUID, SourceState]:
    from .database import DefaultStore, SqliteStore

    sidecar_paths = (
        _sidecar_paths(store_path, account_id, device_id)
        if source_store_class is DefaultStore
        else ()
    )
    sidecar_before = (
        _sidecar_snapshot(sidecar_paths) if source_store_class is DefaultStore else ()
    )
    include_trust = _authenticate_ordinary_store(
        connection,
        source_store_class=source_store_class,
        account_id=account_id,
        device_id=device_id,
        pickle_key=pickle_key,
    )
    trust_rows = (
        _read_default_trust(connection, sidecar_paths)
        if source_store_class is DefaultStore
        else ()
    )

    # The caller holds the exclusive lifetime lease. Recheck the complete
    # ordinary snapshot after BEGIN IMMEDIATE, immediately before conversion.
    if (
        _authenticate_ordinary_store(
            connection,
            source_store_class=source_store_class,
            account_id=account_id,
            device_id=device_id,
            pickle_key=pickle_key,
        )
        != include_trust
        or source_store_class is DefaultStore
        and _sidecar_snapshot(sidecar_paths) != sidecar_before
    ):
        raise FreshIngestionRequired(
            "configured ordinary store changed during adoption"
        )
    if source_store_class is DefaultStore:
        if _read_default_trust(connection, sidecar_paths) != trust_rows:
            raise FreshIngestionRequired(
                "configured trust sidecars changed during adoption"
            )
        if include_trust:
            connection.execute_sql("DELETE FROM devicetruststate")
            if statement_hook is not None:
                statement_hook("delete_legacy_trust")
        else:
            connection.execute_sql(_DEVICE_TRUST_SQL)
            if statement_hook is not None:
                statement_hook("create_device_trust_state")
        if statement_hook is not None:
            statement_hook("before_first_trust_insert")
        for index, row in enumerate(trust_rows):
            connection.execute_sql(
                "INSERT INTO devicetruststate (device_id, state) VALUES (?, ?)",
                row,
            )
            if statement_hook is not None:
                statement_hook(f"insert_trust_{index}")

    _authenticate_ordinary_store(
        connection,
        source_store_class=SqliteStore,
        account_id=account_id,
        device_id=device_id,
        pickle_key=pickle_key,
    )

    stream_id, source_state = _create_fresh(
        connection,
        account_id,
        device_id,
        consumer_generation,
        source,
        writer_epoch,
        statement_hook,
    )
    if connection.execute_sql("PRAGMA foreign_key_check").fetchall():
        raise FreshIngestionRequired("adopted store has foreign key violations")
    if statement_hook is not None:
        statement_hook("foreign_key_check")
    if statement_hook is not None:
        statement_hook("before_commit")
    return stream_id, source_state


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


def _decode_delivery_state(
    row: Mapping[str, object], owner: OwnerView
) -> DeliveryState:
    try:
        state = DeliveryState(
            *(row[f"delivery_{name}"] for name in DeliveryState._fields)
        )
        sequence, acknowledged, work_id, ready_revision, ordinal, batch_sha256 = state
        present = work_id is not None
        digests = acknowledged, batch_sha256
        if (
            type(sequence) is not int
            or not 0 <= sequence <= SQLITE_INT_MAX
            or any(
                value is not None and (type(value) is not bytes or len(value) != 32)
                for value in digests
            )
            or any((value is None) == present for value in state[2:])
            or (sequence == 0 and (acknowledged is not None or present))
            or (
                sequence > 0 and acknowledged is None and (sequence != 1 or not present)
            )
            or (acknowledged is not None and present and sequence < 2)
        ):
            raise ValueError("delivery frontier is invalid")
        if present and (
            type(work_id) is not str
            or work_id != str(UUID(work_id))
            or type(ready_revision) is not int
            or not 1 <= ready_revision <= owner.revision
            or type(ordinal) is not int
            or not 0 <= ordinal <= SQLITE_INT_MAX
        ):
            raise ValueError("delivery outstanding value is invalid")
        return state
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise JournalIntegrityError("persisted delivery state is invalid") from error


def _different_uuid(previous: UUID) -> UUID:
    while (successor := uuid4()) == previous:
        pass
    return successor


def _rotate_sliding_source(
    connection: SqliteDatabase,
    *,
    owner: OwnerView,
    source: SourceState,
    writer_epoch: UUID,
    transition_hook: Callable[[str], None] | None,
) -> SourceState:
    if (
        owner.transport_kind is not TransportKind.SLIDING
        or source.transport_kind is not TransportKind.SLIDING
    ):
        raise LocalProtocolError("sliding reset requires a Sliding source")
    if owner.revision == SQLITE_INT_MAX or owner.next_source_epoch == SQLITE_INT_MAX:
        raise LocalProtocolError("sliding reset revision or source epoch is exhausted")
    if type(writer_epoch) is not UUID or writer_epoch == owner.writer_epoch:
        raise LocalProtocolError("sliding reset requires a new writer epoch")

    old_payload, old_digest = _row(
        (owner.account_id, owner.stream_id, owner.transport_kind),
        "NioIngestSourceState",
        source.cursor_json,
        header=_source_header(source),
    )
    old_cursor = _sliding_cursor_from_json(source.cursor_json)
    reset = reset_sliding_connection(
        old_cursor,
        _different_uuid(old_cursor.connection_instance),
    )
    successor = SourceState(
        owner.next_source_epoch,
        TransportKind.SLIDING,
        canonical_sliding_cursor(reset.cursor),
        0,
        source.active,
    )
    new_payload, new_digest = _row(
        (owner.account_id, owner.stream_id, owner.transport_kind),
        "NioIngestSourceState",
        successor.cursor_json,
        header=_source_header(successor),
    )

    meta_cursor = connection.execute_sql(
        "UPDATE NioIngestMeta SET revision = ?, writer_epoch = ?, "
        "next_source_epoch = ? WHERE account_id = ? AND revision = ? "
        "AND writer_epoch = ? AND next_source_epoch = ?",
        (
            owner.revision + 1,
            str(writer_epoch),
            owner.next_source_epoch + 1,
            owner.account_id,
            owner.revision,
            str(owner.writer_epoch),
            owner.next_source_epoch,
        ),
    )
    if transition_hook is not None:
        transition_hook("sliding_reset_meta_cas")
    if meta_cursor.rowcount != 1:
        raise JournalConflictError("sliding reset Meta compare-and-swap failed")

    source_cursor = connection.execute_sql(
        "UPDATE NioIngestSourceState SET source_epoch = ?, payload = ?, "
        "payload_sha256 = ?, next_request_id = ?, active = ? "
        "WHERE account_id = ? AND source_epoch = ? AND payload = ? "
        "AND payload_sha256 = ? AND next_request_id = ? AND active = ?",
        (
            successor.source_epoch,
            new_payload,
            new_digest,
            successor.next_request_id,
            int(successor.active),
            owner.account_id,
            source.source_epoch,
            old_payload,
            old_digest,
            source.next_request_id,
            int(source.active),
        ),
    )
    if transition_hook is not None:
        transition_hook("sliding_reset_source_upsert")
    if source_cursor.rowcount != 1:
        raise JournalConflictError("sliding reset Source compare-and-swap failed")
    if transition_hook is not None:
        transition_hook("before_commit")
    return successor


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
        "revision, writer_epoch, next_source_epoch, created_at_ns, "
        "delivery_next_sequence, delivery_acknowledged_sha256, "
        "delivery_outstanding_work_id, delivery_outstanding_ready_revision, "
        "delivery_outstanding_ready_ordinal, delivery_outstanding_batch_sha256) "
        "VALUES (?, ?, ?, ?, ?, ?, 0, ?, 1, ?, 0, NULL, NULL, NULL, NULL, NULL)",
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


def _fresh_user_objects(connection: SqliteDatabase) -> tuple[tuple[object, ...], ...]:
    return tuple(
        connection.execute_sql(
            "SELECT type, name, tbl_name FROM sqlite_master " "ORDER BY type, name"
        ).fetchall()
    )


def _create_fresh_owned_store(
    connection: SqliteDatabase,
    *,
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    source: SourceConfig,
    pickle_key: str,
    writer_epoch: UUID,
    statement_hook: Callable[[str], None] | None,
) -> tuple[UUID, SourceState]:
    if _fresh_user_objects(connection):
        raise FreshIngestionRequired(
            "fresh owned ingestion requires an empty SQLite user graph"
        )

    account = OlmAccount()
    if account.shared:
        raise LocalProtocolError("fresh Olm account must be unshared")
    if statement_hook is not None:
        statement_hook("account_generated")
    pickled = account.pickle(pickle_key)
    if statement_hook is not None:
        statement_hook("account_pickled")

    models = tuple(sort_models((*_ACTIVE_ORDINARY_MODELS, DeviceTrustState)))
    with connection.bind_ctx(models):
        for model in models:
            model.create_table(safe=False)
            if statement_hook is not None:
                statement_hook(f"ordinary_schema_{model._meta.table_name}")

    connection.execute_sql(
        "INSERT INTO storeversion(version) VALUES (?)",
        (10,),
    )
    if statement_hook is not None:
        statement_hook("insert_store_version")
    connection.execute_sql(
        "INSERT INTO accounts(account, user_id, device_id, shared) "
        "VALUES (?, ?, ?, 0)",
        (pickled, account_id, device_id),
    )
    if statement_hook is not None:
        statement_hook("insert_account")

    stream_id, source_state = _create_fresh(
        connection,
        account_id,
        device_id,
        consumer_generation,
        source,
        writer_epoch,
        statement_hook,
    )
    _authenticate_ordinary_store(
        connection,
        source_store_class=_exact_sqlite_store_class(),
        account_id=account_id,
        device_id=device_id,
        pickle_key=pickle_key,
    )
    _inspect_existing(
        connection,
        account_id,
        device_id,
        consumer_generation,
        source,
    )
    _authenticate_full_ingestion_graph(
        connection,
        account_id=account_id,
        device_id=device_id,
    )
    if connection.execute_sql("PRAGMA foreign_key_check").fetchall():
        raise FreshIngestionRequired("fresh owned store has foreign key violations")
    if statement_hook is not None:
        statement_hook("foreign_key_check")
        statement_hook("before_commit")
    return stream_id, source_state


def _exact_sqlite_store_class() -> type[MatrixStore]:
    from .database import SqliteStore

    return SqliteStore


def _inspect_existing(
    connection: SqliteDatabase,
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    source: SourceConfig,
) -> tuple[OwnerView, SourceState]:
    validate_schema_topology(connection)
    all_tables = {
        row[0]
        for row in connection.execute_sql(
            "SELECT name FROM sqlite_master "
            "WHERE type = 'table' AND name NOT GLOB 'sqlite_*'"
        )
    }
    borrowed_tables = {name for name in all_tables if not name.startswith("NioIngest")}
    active_ordinary_tables = {
        model._meta.table_name for model in _ACTIVE_ORDINARY_MODELS
    }
    historical_ordinary_tables = active_ordinary_tables | set(_RETIRED_RECOVERY_TABLES)
    allowed_borrowed: tuple[set[str], ...] = (
        set(),
        set(_E2EE_TABLES),
        active_ordinary_tables,
        active_ordinary_tables | {DeviceTrustState._meta.table_name},
        historical_ordinary_tables,
        historical_ordinary_tables | {DeviceTrustState._meta.table_name},
    )
    if borrowed_tables not in allowed_borrowed:
        raise FreshIngestionRequired(
            "ingestion-v1 store has unexpected or incomplete borrowed tables"
        )
    if borrowed_tables:
        versions = connection.execute_sql("SELECT version FROM storeversion").fetchall()
        if len(versions) != 1 or versions[0][0] != 10:
            raise FreshIngestionRequired(
                "ingestion-v1 store has an unsupported borrowed schema"
            )
    rows = connection.execute_sql("SELECT * FROM NioIngestMeta LIMIT 2").fetchall()
    if len(rows) != 1:
        raise FreshIngestionRequired("ingestion-v1 marker row cardinality is not one")
    row = rows[0]
    if row["account_id"] != account_id:
        raise FreshIngestionRequired("ingestion account_id does not match")
    if row["device_id"] != device_id:
        raise FreshIngestionRequired("ingestion device_id does not match")
    if (
        type(row["schema_version"]) is not int
        or row["schema_version"] != SCHEMA_VERSION
    ):
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
        stored_writer_epoch = UUID(row["writer_epoch"])
    except (AttributeError, TypeError, ValueError) as error:
        raise JournalIntegrityError("ingestion owner UUID is invalid") from error
    if (row["stream_id"], row["consumer_generation"], row["writer_epoch"]) != (
        str(stream_id),
        str(stored_consumer_generation),
        str(stored_writer_epoch),
    ):
        raise JournalIntegrityError("ingestion owner UUID is not canonical")
    if stored_consumer_generation != consumer_generation:
        raise LocalProtocolError("ingestion consumer_generation does not match")
    owner = OwnerView(
        account_id,
        device_id,
        row["schema_version"],
        stream_id,
        stored_consumer_generation,
        transport_kind,
        row["revision"],
        stored_writer_epoch,
        row["next_source_epoch"],
    )
    _decode_delivery_state(row, owner)
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
    return owner, state


def _authenticate_full_ingestion_graph(
    connection: SqliteDatabase,
    *,
    account_id: str,
    device_id: str,
) -> None:
    """Authenticate every persisted ingestion row without normal owner scopes."""

    # Imported lazily because the row codec imports the pure preflight helpers.
    from ..ingest.serialization import batch_from_records
    from ._sync_journal import _ready_member, _renormalized_frame
    from ._sync_journal_rows import JournalRows

    class BootstrapRows(JournalRows):
        def __init__(self) -> None:
            self.account_id = account_id
            self.device_id = device_id

        def _execute(self, statement: str, parameters: tuple[object, ...] = ()):
            return connection.execute_sql(statement, parameters)

    rows = BootstrapRows()
    meta = rows._meta()
    owner = rows._decode_owner_row(cast("Mapping[str, object]", meta))
    source_rows = connection.execute_sql(
        "SELECT * FROM NioIngestSourceState LIMIT 2"
    ).fetchall()
    if len(source_rows) != 1:
        raise JournalIntegrityError("ingestion source row cardinality is not one")
    source_state = rows._decode_source_row(
        cast("Mapping[str, object]", source_rows[0]), owner
    )
    if (
        owner.next_source_epoch > owner.revision + 1
        or source_state.source_epoch != owner.next_source_epoch - 1
        or source_state.next_request_id > owner.revision
    ):
        raise JournalIntegrityError("persisted Source epoch is outside owner frontier")

    headers = rows._load_authenticated_frame_headers(owner)
    if len({(header.source_epoch, header.request_id) for header in headers}) != len(
        headers
    ) or len({header.staged_revision for header in headers}) != len(headers):
        raise JournalIntegrityError("persisted Frame ordering identity is duplicated")
    frame_ids = rows._classify_frame_ids(owner)
    if frame_ids != {header.frame_id for header in headers}:
        raise JournalIntegrityError("authenticated Frame inventory changed")
    authenticated_frames = []
    for header in headers:
        if (
            header.staged_revision > owner.revision
            or header.staged_revision < header.source_epoch + header.request_id + 1
            or not 0 <= header.source_epoch < owner.next_source_epoch
            or header.source_epoch == source_state.source_epoch
            and header.request_id >= source_state.next_request_id
        ):
            raise JournalIntegrityError("persisted Frame is outside owner frontier")
        stored = rows._frame_row(header.frame_id)
        if (
            rows._frame_drain_row_from_full(
                cast("Mapping[str, object]", stored),
                owner,
                authenticate=True,
            )
            != header
        ):
            raise JournalIntegrityError("authenticated Frame header changed")
        state = rows._decode_frame_state(
            header.frame_id,
            cast("Mapping[str, object]", stored),
            owner,
            drain_header_authenticated=True,
        )
        if type(state) is StagedFrame:
            staged_state = cast("StagedFrame", state)
            normalized = _renormalized_frame(owner, staged_state)
            request_cursor = staged_state.response.request.request_cursor_json
            candidate_cursor = normalized.candidate_cursor_json
        else:
            request_cursor = state.request_cursor_json
            candidate_cursor = state.candidate_cursor_json
        authenticated_frames.append((header, request_cursor, candidate_cursor))
    if any(
        (successor[0].source_epoch, successor[0].request_id)
        <= (previous[0].source_epoch, previous[0].request_id)
        for previous, successor in zip(authenticated_frames, authenticated_frames[1:])
    ):
        raise JournalIntegrityError("persisted Frame revision order is invalid")
    by_source = authenticated_frames
    for previous, successor in zip(by_source, by_source[1:]):
        previous_header, _previous_request_cursor, previous_candidate_cursor = previous
        successor_header, successor_request_cursor, _successor_candidate_cursor = (
            successor
        )
        if (
            successor_header.source_epoch == previous_header.source_epoch
            and successor_header.request_id == previous_header.request_id + 1
            and successor_request_cursor != previous_candidate_cursor
        ):
            raise JournalIntegrityError("persisted Frame cursor chain is broken")
    current = [
        item for item in by_source if item[0].source_epoch == source_state.source_epoch
    ]
    if current and current[-1][0].request_id == source_state.next_request_id - 1:
        if current[-1][2] != source_state.cursor_json:
            raise JournalIntegrityError("persisted Frame tail does not match Source")

    inventory = rows._load_task3_work_inventory(owner)
    headers_by_id = {str(header.frame_id): header for header in headers}
    aggregate_ids = connection.execute_sql(
        "SELECT room_id FROM NioIngestRoomAggregate ORDER BY room_id"
    ).fetchall()
    if any(type(row[0]) is not str or not row[0] for row in aggregate_ids):
        raise JournalIntegrityError("persisted Aggregate room identity is invalid")
    if len({row[0] for row in aggregate_ids}) != len(aggregate_ids):
        raise JournalIntegrityError("persisted Aggregate identity is duplicated")
    aggregates: dict[str, RoomAggregateValue] = {}
    for (room_id,) in aggregate_ids:
        loaded = rows._load_room_aggregate(owner, room_id)
        if loaded is None:
            raise JournalIntegrityError("persisted Aggregate disappeared")
        aggregates[room_id] = loaded[1]
        hydration = loaded[1].pending_hydration
        if (
            hydration is not None
            and hydration.origin.source_epoch >= owner.next_source_epoch
            or hydration is not None
            and hydration.origin.source_epoch == source_state.source_epoch
            and hydration.origin.request_id >= source_state.next_request_id
        ):
            raise JournalIntegrityError("Aggregate origin exceeds source frontier")
    pending_local_rooms = tuple(
        room_id
        for room_id, aggregate in aggregates.items()
        if aggregate.pending_local_membership is not None
    )
    if len(pending_local_rooms) > 1:
        raise JournalIntegrityError("multiple local membership intents are pending")
    if pending_local_rooms and (headers or inventory.work):
        raise JournalIntegrityError(
            "local membership intent requires empty Frame and Work queues"
        )

    held_identities: set[tuple[str, int, int]] = set()
    local_work_by_room: dict[
        str,
        list[tuple[int, int, int, _LocalMembershipEvidence]],
    ] = {}
    for storage, authenticated in zip(
        inventory.storage_rows, inventory.work, strict=True
    ):
        origin = authenticated.value.origin
        if type(origin) is SystemOrigin:
            if (
                type(authenticated.value) is not EventRecord
                or origin.kind is not SystemOriginKind.MEMBERSHIP_CHANGE
                or authenticated.value.kind is not RecordKind.ROOM_LIFECYCLE
                or authenticated.status != "ready"
                or authenticated.metadata is not None
                or storage[4] != str(origin.operation_id)
                or storage[4] in headers_by_id
            ):
                raise JournalIntegrityError("local Work origin is invalid")
            value = authenticated.value
            room_id = cast("str", value.room_id)
            aggregate = aggregates.get(room_id)
            if aggregate is None:
                raise JournalIntegrityError("local Work requires Aggregate")
            room_sequence = cast("int", value.room_sequence)
            membership_epoch = cast("int", value.membership_epoch)
            created_revision = cast("int", storage[10])
            if room_sequence >= aggregate.next_room_sequence:
                raise JournalIntegrityError(
                    "local Work exceeds Aggregate sequence frontier"
                )
            if membership_epoch > aggregate.continuity.membership_epoch:
                raise JournalIntegrityError(
                    "local Work exceeds Aggregate epoch frontier"
                )
            if created_revision > aggregate.updated_revision:
                raise JournalIntegrityError(
                    "local Work exceeds Aggregate revision frontier"
                )
            try:
                evidence = _local_membership_evidence(value.source_json)
            except (TypeError, ValueError) as error:
                raise JournalIntegrityError(
                    "local Work membership evidence is invalid"
                ) from error
            if aggregate.updated_revision == created_revision:
                continuity = aggregate.continuity
                snapshot = aggregate.room_snapshot
                if (
                    continuity.membership != evidence.current_membership
                    or continuity.membership_epoch != membership_epoch
                    or aggregate.next_room_sequence != room_sequence + 1
                    or continuity.baseline is not None
                    or continuity.gap is not None
                    or continuity.hydration_id is not None
                    or aggregate.pending_hydration is not None
                    or aggregate.pending_local_membership is not None
                    or snapshot is not None
                    and (
                        snapshot.membership_epoch != membership_epoch
                        or snapshot.own_membership != evidence.current_membership
                    )
                ):
                    raise JournalIntegrityError(
                        "local Work does not match Aggregate state"
                    )
            local_work_by_room.setdefault(room_id, []).append(
                (created_revision, room_sequence, membership_epoch, evidence)
            )
            continue
        if type(origin) is not RecordOrigin:
            raise JournalIntegrityError("Work origin is not transport-bound")
        if origin.source_epoch >= owner.next_source_epoch:
            raise JournalIntegrityError("Work origin exceeds source frontier")
        if (
            origin.source_epoch == source_state.source_epoch
            and origin.request_id >= source_state.next_request_id
        ):
            raise JournalIntegrityError("Work request exceeds source frontier")
        retained = headers_by_id.get(cast("str", storage[4]))
        if retained is not None and (
            retained.room_materialized_revision is None
            or retained.room_materialized_revision != storage[10]
            or (retained.source_epoch, retained.request_id)
            != (origin.source_epoch, origin.request_id)
        ):
            raise JournalIntegrityError("Work does not match retained Frame")
        if authenticated.status != "held":
            continue
        room_id, membership_epoch, room_sequence = cast(
            "tuple[str, int, int]", storage[5:8]
        )
        identity = (room_id, membership_epoch, room_sequence)
        if identity in held_identities:
            raise JournalIntegrityError("HELD Work identity is duplicated")
        held_identities.add(identity)
        aggregate = aggregates.get(room_id)
        if (
            aggregate is None
            or aggregate.pending_hydration is None
            or aggregate.continuity.membership_epoch != membership_epoch
            or room_sequence >= aggregate.next_room_sequence
        ):
            raise JournalIntegrityError("HELD Work does not match Aggregate frontier")

    for local_work in local_work_by_room.values():
        ordered = sorted(local_work)
        for previous_local, current_local in zip(
            ordered,
            ordered[1:],
            strict=False,
        ):
            if current_local[0] <= previous_local[0]:
                raise JournalIntegrityError(
                    "local Work exceeds Aggregate revision frontier"
                )
            if current_local[1] <= previous_local[1]:
                raise JournalIntegrityError(
                    "local Work exceeds Aggregate sequence frontier"
                )
            if current_local[2] < previous_local[2]:
                raise JournalIntegrityError(
                    "local Work exceeds Aggregate epoch frontier"
                )
            previous_evidence = previous_local[3]
            current_evidence = current_local[3]
            if current_local[1] == previous_local[1] + 1 and (
                current_evidence.previous_membership
                != previous_evidence.current_membership
                or current_evidence.previous_epoch != previous_evidence.current_epoch
            ):
                raise JournalIntegrityError("local Work membership chain is invalid")

    delivery = _decode_delivery_state(cast("Mapping[str, object]", meta), owner)
    if delivery.next_sequence > owner.revision or (
        delivery.outstanding_ready_revision is not None
        and delivery.outstanding_ready_revision >= owner.revision
    ):
        raise JournalIntegrityError("persisted delivery state is invalid")
    if delivery.outstanding_work_id is not None:
        work_id, ready_revision, ordinal, digest = cast(
            "tuple[str, int, int, bytes]", delivery[2:]
        )
        member = _ready_member(
            inventory,
            (ready_revision, ordinal, work_id),
        )
        if member is None:
            raise JournalIntegrityError("claimed Work is missing or moved")
        if _ready_member(inventory) != member:
            raise JournalIntegrityError("claimed Work is not the FIFO READY member")
        batch = batch_from_records(
            account_id=owner.account_id,
            device_id=owner.device_id,
            consumer_generation=owner.consumer_generation,
            stream_id=owner.stream_id,
            sequence=delivery.next_sequence - 1,
            created_revision=ready_revision,
            records=(member[2],),
        )
        if batch.ref.sha256 != digest:
            raise JournalIntegrityError("claimed Work does not match batch digest")
    if connection.execute_sql("PRAGMA foreign_key_check").fetchall():
        raise FreshIngestionRequired("marked ingestion store has FK violations")


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
    pickle_key: str,
    sqlite_busy_timeout_ms: int,
    statement_observer: Callable[[str], None] | None,
    transition_statement_hook: Callable[[str], None] | None,
    schema_statement_hook: Callable[[str], None] | None,
    configured_source_store_class: type[MatrixStore] | None = None,
    configured_store_path: Path | None = None,
    adoption_statement_hook: Callable[[str], None] | None = None,
    fresh_store: bool = False,
    fresh_store_statement_hook: Callable[[str], None] | None = None,
) -> OpenedJournalDatabase:
    if type(consumer_generation) is not UUID:
        raise TypeError("consumer_generation must be UUID")
    source_transport(source)
    if type(fresh_store) is not bool:
        raise TypeError("fresh_store must be bool")
    if fresh_store and configured_source_store_class is not None:
        raise LocalProtocolError("fresh and configured ingestion modes conflict")
    path = database_path(database)
    if configured_source_store_class is not None:
        try:
            configured_stat = path.lstat()
        except FileNotFoundError:
            configured_stat = None
        if (
            configured_stat is None
            or not stat.S_ISREG(configured_stat.st_mode)
            or configured_stat.st_size == 0
            or configured_stat.st_nlink != 1
        ):
            raise FreshIngestionRequired(
                "configured adoption requires an existing singly linked store"
            )
    if configured_source_store_class is None:
        path.parent.mkdir(parents=True, exist_ok=True)
    owner = IngestionStoreOwner(
        path,
        sqlite_busy_timeout_ms,
        statement_observer,
        require_nonempty=configured_source_store_class is not None,
        require_regular_path=(configured_source_store_class is not None or fresh_store),
    )
    connection = owner.database
    try:
        connection.execute_sql("PRAGMA secure_delete = FAST")
        if fresh_store:
            if _fresh_user_objects(connection):
                raise FreshIngestionRequired(
                    "fresh owned ingestion requires an empty SQLite user graph"
                )
            connection.execute_sql("PRAGMA foreign_keys = ON")
            connection.execute_sql(f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms}")
            writer_epoch = uuid4()
            with owner.bootstrap_write():
                stream_id, _source_state = _create_fresh_owned_store(
                    connection,
                    account_id=account_id,
                    device_id=device_id,
                    consumer_generation=consumer_generation,
                    source=source,
                    pickle_key=pickle_key,
                    writer_epoch=writer_epoch,
                    statement_hook=fresh_store_statement_hook,
                )
            if fresh_store_statement_hook is not None:
                fresh_store_statement_hook("commit")
            connection.execute_sql("PRAGMA journal_mode = WAL")
            connection.execute_sql("PRAGMA synchronous = NORMAL")
            connection.execute_sql("PRAGMA secure_delete = FAST")
            owner.activate(account_id, writer_epoch)
            return OpenedJournalDatabase(path, owner, writer_epoch, stream_id)
        if owner.is_fresh:
            if configured_source_store_class is not None:
                raise FreshIngestionRequired(
                    "configured adoption requires a populated v10 store"
                )
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
            from .database import DefaultStore, SqliteStore

            marked = connection.table_exists("NioIngestMeta")
            if configured_source_store_class is not None and not marked:
                if not (
                    configured_source_store_class is DefaultStore
                    or configured_source_store_class is SqliteStore
                ):
                    raise LocalProtocolError(
                        "configured source store class is unsupported"
                    )
                connection.execute_sql("PRAGMA foreign_keys = ON")
                connection.execute_sql(
                    f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms}"
                )
                connection.execute_sql("PRAGMA secure_delete = FAST")
                _authenticate_ordinary_store(
                    connection,
                    source_store_class=configured_source_store_class,
                    account_id=account_id,
                    device_id=device_id,
                    pickle_key=pickle_key,
                )
                writer_epoch = uuid4()
                with owner.bootstrap_write():
                    stream_id, _source_state = _adopt_populated_store(
                        connection,
                        path=path,
                        store_path=(
                            configured_store_path
                            if configured_store_path is not None
                            else path.parent
                        ),
                        source_store_class=configured_source_store_class,
                        account_id=account_id,
                        device_id=device_id,
                        pickle_key=pickle_key,
                        consumer_generation=consumer_generation,
                        source=source,
                        writer_epoch=writer_epoch,
                        statement_hook=(
                            adoption_statement_hook or schema_statement_hook
                        ),
                    )
                hook = adoption_statement_hook or schema_statement_hook
                if hook is not None:
                    hook("commit")
                connection.execute_sql("PRAGMA journal_mode = WAL")
                connection.execute_sql("PRAGMA synchronous = NORMAL")
                connection.execute_sql("PRAGMA secure_delete = FAST")
                owner.activate(account_id, writer_epoch)
                return OpenedJournalDatabase(path, owner, writer_epoch, stream_id)

            if configured_source_store_class is not None:
                if configured_source_store_class is not SqliteStore:
                    raise _MarkedStoreRequiresSqlite(
                        "marked configured reopen requires exact SqliteStore"
                    )
                _authenticate_ordinary_store(
                    connection,
                    source_store_class=SqliteStore,
                    account_id=account_id,
                    device_id=device_id,
                    pickle_key=pickle_key,
                )
            elif _ordinary_table_names(connection) in {
                frozenset(model._meta.table_name for model in _ACTIVE_ORDINARY_MODELS),
                frozenset(model._meta.table_name for model in _ACTIVE_ORDINARY_MODELS)
                | {DeviceTrustState._meta.table_name},
                frozenset(model._meta.table_name for model in _ACTIVE_ORDINARY_MODELS)
                | _RETIRED_RECOVERY_TABLES,
                frozenset(model._meta.table_name for model in _ACTIVE_ORDINARY_MODELS)
                | _RETIRED_RECOVERY_TABLES
                | {DeviceTrustState._meta.table_name},
            }:
                raise FreshIngestionRequired(
                    "populated marked stores require the private configured opener"
                )
            try:
                _inspect_existing(
                    connection,
                    account_id,
                    device_id,
                    consumer_generation,
                    source,
                )
                if configured_source_store_class is not None:
                    _authenticate_full_ingestion_graph(
                        connection,
                        account_id=account_id,
                        device_id=device_id,
                    )
            except (sqlite3.DatabaseError, PeeweeDatabaseError) as error:
                raise FreshIngestionRequired(
                    "nonempty store without a valid ingestion-v1 marker requires "
                    "fresh initialization"
                ) from error
            connection.execute_sql("PRAGMA foreign_keys = ON")
            connection.execute_sql(f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms}")
            sliding_reopened = False
            with owner.bootstrap_write():
                if configured_source_store_class is not None:
                    _authenticate_ordinary_store(
                        connection,
                        source_store_class=SqliteStore,
                        account_id=account_id,
                        device_id=device_id,
                        pickle_key=pickle_key,
                    )
                    _authenticate_full_ingestion_graph(
                        connection,
                        account_id=account_id,
                        device_id=device_id,
                    )
                existing_owner, existing_source = _inspect_existing(
                    connection,
                    account_id,
                    device_id,
                    consumer_generation,
                    source,
                )
                stream_id = existing_owner.stream_id
                writer_epoch = _different_uuid(existing_owner.writer_epoch)
                if existing_owner.transport_kind is TransportKind.SLIDING:
                    _rotate_sliding_source(
                        connection,
                        owner=existing_owner,
                        source=existing_source,
                        writer_epoch=writer_epoch,
                        transition_hook=transition_statement_hook,
                    )
                    sliding_reopened = True
                else:
                    cursor = connection.execute_sql(
                        "UPDATE NioIngestMeta SET writer_epoch = ? "
                        "WHERE account_id = ? AND writer_epoch = ?",
                        (
                            str(writer_epoch),
                            account_id,
                            str(existing_owner.writer_epoch),
                        ),
                    )
                    if cursor.rowcount != 1:
                        raise LocalProtocolError(
                            "persisted writer_epoch changed during open"
                        )
            if sliding_reopened and transition_statement_hook is not None:
                transition_statement_hook("commit")

        connection.execute_sql("PRAGMA journal_mode = WAL")
        connection.execute_sql("PRAGMA synchronous = NORMAL")
        connection.execute_sql("PRAGMA secure_delete = FAST")
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
