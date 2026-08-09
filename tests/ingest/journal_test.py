import asyncio
import hashlib
import os
import sqlite3
import threading
from pathlib import Path
from uuid import UUID

import pytest
from peewee import SqliteDatabase

from nio.event_provenance import TimelineEventProvenance
from nio.ingest import (
    ConsumerBinding,
    ConsumerBootstrap,
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RecordOrigin,
    RoomHydrationStatus,
    RoomSnapshot,
    SystemOrigin,
    SystemOriginKind,
    TransportKind,
)
from nio.ingest.config import (
    ClassicSourceConfig,
    IngestionConfig,
    SlidingSourceConfig,
)
from nio.ingest.errors import (
    FreshIngestionRequired,
    JournalConflictError,
    JournalIntegrityError,
)
from nio.ingest.state import (
    AckOutcome,
    JournalTransition,
    LaneStatus,
    ReleasePhase,
    ReadyRecord,
    RoomLane,
    RoomState,
    SourceState,
    StagedFrame,
)
from nio.ingest.serialization import (
    _canonical_json,
    _loss_id,
    _record_to_dict,
    batch_from_records,
    canonical_batch_payload,
)
from nio.exceptions import LocalProtocolError
from nio.store import SqliteStore
from nio.store._sync_journal import SqliteIngestionJournal
from nio.store._sync_journal_codec import EncryptedRowCodec
from nio.store.sync_journal import (
    open_ingestion_store,
)

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
STREAM_ID = UUID("33333333-3333-3333-3333-333333333333")


JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")


def test_store_public_surface_exposes_only_ingestion_bootstrap() -> None:
    import nio.store as store_api
    import nio.store.sync_journal as bootstrap_api

    assert store_api.StoreBootstrap is not None
    assert store_api.open_ingestion_store is not None
    assert not hasattr(store_api, "EncryptedRowCodec")
    assert not hasattr(store_api, "IngestionJournal")
    assert not hasattr(store_api, "SqliteIngestionJournal")
    assert not hasattr(bootstrap_api, "SqliteIngestionJournal")


def test_internal_journal_protocol_accepts_a_dependency_free_recording_fake() -> None:
    from nio.store._sync_journal_port import IngestionJournal

    class RecordingJournal:
        def load_owner(self):
            return None

        def load_source(self):
            return None

        def load_rooms(self, room_ids):
            return {}

        def load_ready_heads(self, limit):
            return ()

        def load_frame(self, frame_id):
            return None

        def load_loss(self, loss_id):
            return None

        def commit(self, *, expected_revision, writer_epoch, transition):
            return None

        def oldest_unacknowledged(self):
            return None

        def acknowledge(self, ref):
            return AckOutcome.ACKNOWLEDGED

    assert isinstance(RecordingJournal(), IngestionJournal)


def test_ingestion_config_enforces_frozen_resource_ceilings() -> None:
    source = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
    consumer = ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION)

    config = IngestionConfig(source=source, consumer=consumer)

    assert config.max_records_per_batch == 256
    assert config.max_bytes_per_batch == 2 * 1024 * 1024
    assert config.max_record_bytes == 1024 * 1024
    with pytest.raises(ValueError, match="max_records_per_batch"):
        IngestionConfig(
            source=source,
            consumer=consumer,
            max_records_per_batch=257,
        )


def test_source_configs_are_frozen_and_validate_exact_wire_values() -> None:
    classic = ClassicSourceConfig(timeout_ms=0, filter_json=b"{}")
    sliding = SlidingSourceConfig(
        timeout_ms=1,
        connection_name="main",
        lists_json=b"{}",
        room_subscriptions_json=b"{}",
        extensions_json=b"{}",
    )

    assert classic.full_state_on_cold_start is True
    assert sliding.connection_name == "main"
    with pytest.raises(TypeError, match="timeout_ms"):
        ClassicSourceConfig(timeout_ms=True, filter_json=b"{}")
    with pytest.raises(TypeError, match="filter_json"):
        ClassicSourceConfig(timeout_ms=1, filter_json="{}")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="consumer"):
        IngestionConfig(source=classic, consumer=object())  # type: ignore[arg-type]


def test_ingestion_config_accepts_every_resource_bound_at_one() -> None:
    source = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
    consumer = ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION)

    config = IngestionConfig(
        source=source,
        consumer=consumer,
        max_staged_frames=1,
        max_unacknowledged_batches=1,
        max_records_per_batch=1,
        max_bytes_per_batch=1,
        max_record_bytes=1,
        max_crypto_inputs_per_commit=1,
        max_crypto_input_bytes_per_commit=1,
        sqlite_busy_timeout_ms=1,
        sqlite_write_retry_limit=1,
        max_concurrent_recovery_rooms=1,
        max_concurrent_room_hydrations=1,
        sliding_bootstrap_range_size=1,
        max_recovery_events_per_room=1,
        max_held_events_per_room=1,
        max_held_bytes_per_room=1,
    )

    assert config.max_held_bytes_per_room == 1
    with pytest.raises(ValueError, match="max_bytes_per_batch"):
        IngestionConfig(
            source=source,
            consumer=consumer,
            max_bytes_per_batch=2 * 1024 * 1024 + 1,
        )
    with pytest.raises(ValueError, match="max_record_bytes"):
        IngestionConfig(
            source=source,
            consumer=consumer,
            max_record_bytes=1024 * 1024 + 1,
        )


@pytest.mark.parametrize(
    "field",
    (
        "max_staged_frames",
        "max_unacknowledged_batches",
        "max_records_per_batch",
        "max_bytes_per_batch",
        "max_record_bytes",
        "max_crypto_inputs_per_commit",
        "max_crypto_input_bytes_per_commit",
        "sqlite_busy_timeout_ms",
        "sqlite_write_retry_limit",
        "max_concurrent_recovery_rooms",
        "max_concurrent_room_hydrations",
        "sliding_bootstrap_range_size",
        "max_recovery_events_per_room",
        "max_held_events_per_room",
        "max_held_bytes_per_room",
    ),
)
def test_ingestion_config_rejects_nonpositive_bounds(field: str) -> None:
    values = {
        "source": ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}"),
        "consumer": ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        field: 0,
    }

    with pytest.raises(ValueError, match=field):
        IngestionConfig(**values)


def _table_names(database_path: Path) -> set[str]:
    with sqlite3.connect(database_path) as connection:
        return {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }


def _fork_outcomes(operation) -> tuple[str, ...]:
    read_fd, write_fd = os.pipe()
    child_pid = os.fork()
    if child_pid == 0:
        os.close(read_fd)
        try:
            outcomes = operation()
            os.write(write_fd, "\0".join(outcomes).encode())
        finally:
            os.close(write_fd)
            os._exit(0)
    os.close(write_fd)
    payload = os.read(read_fd, 64 * 1024)
    os.close(read_fd)
    waited_pid, status = os.waitpid(child_pid, 0)
    assert waited_pid == child_pid
    assert os.waitstatus_to_exitcode(status) == 0
    return tuple(payload.decode().split("\0"))


def _operation_outcome(operation) -> str:
    try:
        operation()
    except LocalProtocolError as error:
        return str(error)
    return "accepted"


def test_fresh_open_creates_independent_v1_schema_with_marker_first(
    tmp_path: Path,
) -> None:
    statements: list[str] = []

    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    database_path = tmp_path / "journal.db"
    try:
        with sqlite3.connect(database_path) as connection:
            row = connection.execute(
                "SELECT account_id, device_id, schema_version " "FROM NioIngestMeta"
            ).fetchone()
        assert row == (ACCOUNT_ID, DEVICE_ID, 1)
        assert bootstrap.schema_version == 1
        schema_mutations = [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith(("CREATE ", "ALTER ", "DROP "))
        ]
        assert schema_mutations[0].startswith("CREATE TABLE NioIngestMeta")
    finally:
        bootstrap.close()


def test_opening_v1_journal_never_reads_or_alters_legacy_sync_tables(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    bootstrap.close()
    legacy_tables = (
        "SyncTokens",
        "SyncRecoveryGaps",
        "SyncRecoveryAbandonedRooms",
        "PendingTimelineEvents",
        "SlidingWindowTokens",
    )
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        for table in legacy_tables:
            connection.execute(f'CREATE TABLE "{table}" (evidence TEXT)')
            connection.execute(f'INSERT INTO "{table}" VALUES (?)', (table,))

    statements: list[str] = []
    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
        statement_observer=statements.append,
    )
    reopened.close()

    assert all(table not in "\n".join(statements) for table in legacy_tables)
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        for table in legacy_tables:
            assert connection.execute(f'SELECT evidence FROM "{table}"').fetchone() == (
                table,
            )


def test_second_writer_and_wrong_store_identity_are_refused(tmp_path: Path) -> None:
    first = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        with pytest.raises(LocalProtocolError, match="writer lock"):
            open_ingestion_store(
                tmp_path,
                account_id=ACCOUNT_ID,
                device_id=DEVICE_ID,
                database_name="journal.db",
            )
    finally:
        first.close()

    with pytest.raises(LocalProtocolError, match="account_id"):
        open_ingestion_store(
            tmp_path,
            account_id="@mallory:example.org",
            device_id=DEVICE_ID,
            database_name="journal.db",
        )
    with pytest.raises(LocalProtocolError, match="device_id"):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id="OTHER",
            database_name="journal.db",
        )


def test_schema_version_is_validated_independently(tmp_path: Path) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    bootstrap.close()
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute("UPDATE NioIngestMeta SET schema_version = 10")

    with pytest.raises(LocalProtocolError, match="schema_version"):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name="journal.db",
        )


def test_existing_v1_schema_is_validated_without_repair(tmp_path: Path) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )
    bootstrap.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP TABLE NioIngestBatch")

    with pytest.raises(LocalProtocolError, match="topology"):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name=database_path.name,
        )

    assert "NioIngestBatch" not in _table_names(database_path)


def _rewrite_table_sql(
    connection: sqlite3.Connection,
    table: str,
    old: str,
    new: str,
) -> None:
    original = connection.execute(
        "SELECT sql FROM sqlite_master WHERE type = 'table' AND name = ?",
        (table,),
    ).fetchone()[0]
    changed = original.replace(old, new)
    assert changed != original
    schema_version = connection.execute("PRAGMA schema_version").fetchone()[0]
    connection.execute("PRAGMA writable_schema = ON")
    connection.execute(
        "UPDATE sqlite_master SET sql = ? WHERE type = 'table' AND name = ?",
        (changed, table),
    )
    connection.execute("PRAGMA writable_schema = OFF")
    connection.execute(f"PRAGMA schema_version = {schema_version + 1}")


@pytest.mark.parametrize(
    "mutation",
    (
        "column",
        "missing_index",
        "unexpected_index",
        "foreign_key",
        "primary_key",
        "check_constraint",
        "view",
    ),
)
def test_existing_v1_open_rejects_every_topology_drift_before_epoch_write(
    tmp_path: Path,
    mutation: str,
) -> None:
    database_path = tmp_path / f"{mutation}.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )
    writer_epoch = str(bootstrap._journal.writer_epoch)
    bootstrap.close()

    with sqlite3.connect(database_path) as connection:
        if mutation == "column":
            connection.execute("ALTER TABLE NioIngestFrame ADD COLUMN unexpected TEXT")
        elif mutation == "missing_index":
            connection.execute("DROP INDEX NioIngestRoomLane_ready")
        elif mutation == "unexpected_index":
            connection.execute(
                "CREATE INDEX NioIngestFrame_unexpected "
                "ON NioIngestFrame(account_id)"
            )
        elif mutation == "foreign_key":
            _rewrite_table_sql(
                connection,
                "NioIngestFrame",
                "account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id)",
                "account_id TEXT NOT NULL",
            )
        elif mutation == "primary_key":
            _rewrite_table_sql(
                connection,
                "NioIngestFrame",
                "PRIMARY KEY (account_id, frame_id)",
                "UNIQUE (account_id, frame_id)",
            )
        elif mutation == "check_constraint":
            _rewrite_table_sql(
                connection,
                "NioIngestSourceState",
                "active INTEGER NOT NULL CHECK (active IN (0, 1))",
                "active INTEGER NOT NULL",
            )
        else:
            connection.execute(
                "CREATE VIEW NioIngestUnexpectedView AS "
                "SELECT account_id FROM NioIngestMeta"
            )

    with pytest.raises(LocalProtocolError, match="topology"):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name=database_path.name,
        )

    with sqlite3.connect(database_path) as connection:
        assert (
            connection.execute(
                "SELECT writer_epoch FROM NioIngestMeta WHERE account_id = ?",
                (ACCOUNT_ID,),
            ).fetchone()[0]
            == writer_epoch
        )


def test_existing_v1_open_rejects_multiple_marker_rows_before_epoch_write(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )
    writer_epoch = str(bootstrap._journal.writer_epoch)
    bootstrap.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO NioIngestMeta SELECT ?, device_id, schema_version, "
            "stream_id, binding_operation_id, journal_generation, "
            "consumer_generation, consumer_first_sequence, baseline_rooms_sha256, "
            "consumer_attached_revision, revision, writer_epoch, lock_device, "
            "lock_inode, next_source_epoch, next_ready_order, "
            "next_batch_sequence, last_acked_sequence, last_acked_batch_id, "
            "last_acked_sha256, created_at_ns "
            "FROM NioIngestMeta WHERE account_id = ?",
            ("@mallory:example.org", ACCOUNT_ID),
        )

    with pytest.raises(LocalProtocolError, match="marker row"):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name=database_path.name,
        )

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT DISTINCT writer_epoch FROM NioIngestMeta"
        ).fetchall() == [(writer_epoch,)]


def test_unexpected_trigger_is_rejected_before_it_can_mutate_legacy_state(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )
    writer_epoch = str(bootstrap._journal.writer_epoch)
    bootstrap.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE SyncTokens (token TEXT NOT NULL)")
        connection.execute(
            "CREATE TRIGGER unexpected_ingest_meta_update "
            "AFTER UPDATE OF writer_epoch ON NioIngestMeta "
            "BEGIN INSERT INTO SyncTokens VALUES ('triggered'); END"
        )

    error: BaseException | None = None
    try:
        reopened = open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name=database_path.name,
        )
    except BaseException as caught:
        error = caught
    else:
        reopened.close()

    assert isinstance(error, LocalProtocolError)
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT * FROM SyncTokens").fetchall() == []
        assert (
            connection.execute(
                "SELECT writer_epoch FROM NioIngestMeta WHERE account_id = ?",
                (ACCOUNT_ID,),
            ).fetchone()[0]
            == writer_epoch
        )


def test_nonempty_unmarked_database_is_refused_without_sql_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "legacy.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("CREATE TABLE Accounts (id INTEGER PRIMARY KEY)")
        connection.execute("INSERT INTO Accounts DEFAULT VALUES")

    import nio.store._sync_journal_preflight as journal_preflight

    real_connect = sqlite3.connect
    statements: list[str] = []

    def observed_connect(*args: object, **kwargs: object) -> sqlite3.Connection:
        connection = real_connect(*args, **kwargs)
        connection.set_trace_callback(statements.append)
        return connection

    monkeypatch.setattr(journal_preflight.sqlite3, "connect", observed_connect)

    with pytest.raises(FreshIngestionRequired):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name="legacy.db",
        )

    assert _table_names(database_path) == {"Accounts"}
    assert all(
        statement.lstrip().upper().startswith("SELECT ") for statement in statements
    )


def test_v1_marker_blocks_legacy_store_but_bootstrap_opens_only_e2ee_tables(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        with pytest.raises(LocalProtocolError, match="writer lock|ingestion v1"):
            SqliteStore(
                ACCOUNT_ID,
                DEVICE_ID,
                str(tmp_path),
                database_name="journal.db",
            )

        store = bootstrap.open_matrix_store(SqliteStore)
        try:
            tables = _table_names(tmp_path / "journal.db")
            assert "accounts" in tables
            assert "NioIngestMeta" in tables
            assert (
                not {
                    "synctokens",
                    "syncrecoverygaps",
                    "syncrecoveryabandonedrooms",
                    "pendingtimelineevents",
                    "slidingwindowtokens",
                }
                & tables
            )
        finally:
            store.database.close()
    finally:
        bootstrap.close()


def test_concurrent_v1_and_legacy_initialization_cannot_interleave(
    tmp_path: Path,
) -> None:
    marker_is_uncommitted = threading.Event()
    allow_v1_commit = threading.Event()
    legacy_passed_marker_check = threading.Event()
    legacy_finished = threading.Event()
    v1_result: list[object] = []
    legacy_result: list[object] = []

    class ObservedLegacyStore(SqliteStore):
        def _create_database(self):
            legacy_passed_marker_check.set()
            return super()._create_database()

    def pause_after_marker(label: str) -> None:
        if label == "create_meta":
            marker_is_uncommitted.set()
            assert allow_v1_commit.wait(timeout=5)

    def initialize_v1() -> None:
        try:
            bootstrap = open_ingestion_store(
                tmp_path,
                account_id=ACCOUNT_ID,
                device_id=DEVICE_ID,
                database_name="journal.db",
                schema_statement_hook=pause_after_marker,
            )
            v1_result.append("opened")
            assert legacy_finished.wait(timeout=5)
            bootstrap.close()
        except BaseException as error:
            v1_result.append(error)

    def initialize_legacy() -> None:
        try:
            store = ObservedLegacyStore(
                ACCOUNT_ID,
                DEVICE_ID,
                str(tmp_path),
                database_name="journal.db",
            )
            legacy_result.append("opened")
            store.database.close()
        except BaseException as error:
            legacy_result.append(error)
        finally:
            legacy_finished.set()

    v1_thread = threading.Thread(target=initialize_v1)
    legacy_thread = threading.Thread(target=initialize_legacy)
    v1_thread.start()
    assert marker_is_uncommitted.wait(timeout=5)
    legacy_thread.start()
    legacy_passed_marker_check.wait(timeout=1)
    allow_v1_commit.set()
    v1_thread.join(timeout=5)
    legacy_thread.join(timeout=5)
    assert not v1_thread.is_alive()
    assert not legacy_thread.is_alive()

    assert v1_result == ["opened"]
    assert len(legacy_result) == 1
    assert isinstance(legacy_result[0], LocalProtocolError)
    assert not {
        "synctokens",
        "pendingtimelineevents",
        "slidingwindowtokens",
    } & _table_names(tmp_path / "journal.db")


def test_e2ee_bootstrap_creation_is_writer_epoch_fenced(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    statements: list[str] = []
    execute_sql = SqliteDatabase.execute_sql

    def observe_sql(
        database: SqliteDatabase,
        sql: str,
        params: tuple[object, ...] | None = None,
        commit: bool | None = None,
    ):
        statements.append(sql)
        return execute_sql(database, sql, params, commit)

    monkeypatch.setattr(SqliteDatabase, "execute_sql", observe_sql)
    try:
        store = bootstrap.open_matrix_store(SqliteStore)
        store.database.close()
    finally:
        bootstrap.close()

    normalized = tuple(" ".join(statement.split()).upper() for statement in statements)
    fence_index = next(
        index
        for index, statement in enumerate(normalized)
        if statement.startswith("UPDATE NIOINGESTMETA SET WRITER_EPOCH = WRITER_EPOCH")
    )
    first_create = next(
        index
        for index, statement in enumerate(normalized)
        if statement.startswith("CREATE TABLE")
    )
    assert fence_index < first_create


def test_bootstrap_close_closes_its_e2ee_store_before_releasing_lock(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)

    bootstrap.close()

    assert store.database.is_closed()
    replacement = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    replacement.close()


def test_direct_bootstrap_authority_is_consumed_and_registered_once(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    stores = []
    try:
        stores.append(
            SqliteStore(
                ACCOUNT_ID,
                DEVICE_ID,
                str(tmp_path),
                database_name="journal.db",
                _ingestion_bootstrap=bootstrap,
            )
        )
        with pytest.raises(LocalProtocolError, match="only once"):
            stores.append(
                SqliteStore(
                    ACCOUNT_ID,
                    DEVICE_ID,
                    str(tmp_path),
                    database_name="journal.db",
                    _ingestion_bootstrap=bootstrap,
                )
            )

        bootstrap.close()
        assert stores[0].database.is_closed()
    finally:
        for store in stores:
            if not store.database.is_closed():
                store.database.close()
        bootstrap.close()


def test_concurrent_direct_bootstrap_claims_admit_exactly_one_store(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    first_entered = threading.Event()
    allow_first = threading.Event()
    second_started = threading.Event()
    second_finished = threading.Event()
    results: list[object] = []

    class PausingStore(SqliteStore):
        def _post_init_ingestion_store(self, authority) -> None:
            first_entered.set()
            assert allow_first.wait(timeout=5)
            super()._post_init_ingestion_store(authority)

    def open_first() -> None:
        try:
            results.append(
                PausingStore(
                    ACCOUNT_ID,
                    DEVICE_ID,
                    str(tmp_path),
                    database_name="journal.db",
                    _ingestion_bootstrap=bootstrap,
                )
            )
        except BaseException as error:
            results.append(error)

    def open_second() -> None:
        second_started.set()
        try:
            results.append(
                SqliteStore(
                    ACCOUNT_ID,
                    DEVICE_ID,
                    str(tmp_path),
                    database_name="journal.db",
                    _ingestion_bootstrap=bootstrap,
                )
            )
        except BaseException as error:
            results.append(error)
        finally:
            second_finished.set()

    first = threading.Thread(target=open_first)
    second = threading.Thread(target=open_second)
    first.start()
    assert first_entered.wait(timeout=5)
    second.start()
    assert second_started.wait(timeout=5)
    try:
        assert not second_finished.wait(timeout=0.1)
    finally:
        allow_first.set()
        first.join(timeout=5)
        second.join(timeout=5)

    try:
        stores = [result for result in results if isinstance(result, SqliteStore)]
        errors = [result for result in results if isinstance(result, BaseException)]
        assert len(stores) == 1
        assert len(errors) == 1
        assert isinstance(errors[0], LocalProtocolError)
        assert "only once" in str(errors[0])
    finally:
        for result in results:
            if isinstance(result, SqliteStore) and not result.database.is_closed():
                result.database.close()
        bootstrap.close()


def test_failed_direct_bootstrap_claim_rolls_back_for_retry(tmp_path: Path) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )

    class FailingStore(SqliteStore):
        def _post_init_ingestion_store(self, authority) -> None:
            raise RuntimeError("injected constructor failure")

    try:
        with pytest.raises(RuntimeError, match="injected constructor failure"):
            FailingStore(
                ACCOUNT_ID,
                DEVICE_ID,
                str(tmp_path),
                database_name="journal.db",
                _ingestion_bootstrap=bootstrap,
            )

        store = SqliteStore(
            ACCOUNT_ID,
            DEVICE_ID,
            str(tmp_path),
            database_name="journal.db",
            _ingestion_bootstrap=bootstrap,
        )
        bootstrap.close()
        assert store.database.is_closed()
    finally:
        bootstrap.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_inherited_child_cannot_use_or_release_parent_ownership(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)

    def inherited_operations() -> tuple[str, ...]:
        return (
            _operation_outcome(bootstrap._journal._assert_open),
            _operation_outcome(bootstrap.close),
            _operation_outcome(bootstrap._journal._writer_lock.close),
        )

    try:
        outcomes = _fork_outcomes(inherited_operations)
        assert len(outcomes) == 3
        assert all("acquiring process" in outcome for outcome in outcomes)
        assert not store.database.is_closed()
        assert bootstrap._journal.load_owner().revision == 0
        second = None
        try:
            second = open_ingestion_store(
                tmp_path,
                account_id=ACCOUNT_ID,
                device_id=DEVICE_ID,
                database_name="journal.db",
            )
        except LocalProtocolError as error:
            assert "writer lock" in str(error)
        else:
            second.close()
            pytest.fail("child cleanup released the parent's writer lock")
    finally:
        bootstrap.close()


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_inherited_child_cannot_attach_or_open_e2ee_store(tmp_path: Path) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)

    def inherited_operations() -> tuple[str, ...]:
        return (
            _operation_outcome(
                lambda: asyncio.run(bootstrap.attach_consumer(consumer))
            ),
            _operation_outcome(
                lambda: SqliteStore(
                    ACCOUNT_ID,
                    DEVICE_ID,
                    str(tmp_path),
                    database_name="journal.db",
                    _ingestion_bootstrap=bootstrap,
                )
            ),
        )

    try:
        outcomes = _fork_outcomes(inherited_operations)
        assert len(outcomes) == 2
        assert all("acquiring process" in outcome for outcome in outcomes)
        assert bootstrap._journal.load_owner().revision == 0
        assert "accounts" not in _table_names(tmp_path / "journal.db")
    finally:
        bootstrap.close()


def test_queue_database_is_refused_before_ingestion_schema_creation(
    tmp_path: Path,
) -> None:
    from playhouse.sqliteq import SqliteQueueDatabase

    database_path = tmp_path / "queued.db"
    database = SqliteQueueDatabase(database_path)

    with pytest.raises(LocalProtocolError, match="SqliteQueueDatabase"):
        SqliteIngestionJournal.open(
            database,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
        )

    assert "NioIngestMeta" not in _table_names(database_path)


def _consumer_bootstrap(
    bootstrap,
    room_ids: tuple[str, ...] = (),
    *,
    consumer_generation: UUID = CONSUMER_GENERATION,
    baseline_sha256: bytes | None = None,
) -> ConsumerBootstrap:
    canonical = (
        b"[" + b",".join(b'"' + room.encode() + b'"' for room in room_ids) + b"]"
    )
    return ConsumerBootstrap(
        bootstrap.binding_operation_id,
        ConsumerBinding(JOURNAL_GENERATION, consumer_generation),
        bootstrap.next_batch_sequence,
        room_ids,
        baseline_sha256 or hashlib.sha256(canonical).digest(),
    )


@pytest.mark.asyncio
async def test_attach_consumer_installs_priority_baseline_loss_plan_atomically(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    room_ids = ("!alpha:example.org", "!beta:example.org")
    consumer = _consumer_bootstrap(bootstrap, room_ids)
    try:
        with pytest.raises(LocalProtocolError, match="consumer is not attached"):
            bootstrap._journal.oldest_unacknowledged()
        with pytest.raises(LocalProtocolError, match="consumer is not attached"):
            bootstrap.assert_http_enabled()

        await bootstrap.attach_consumer(consumer)
        bootstrap.assert_http_enabled()

        owner = bootstrap._journal.load_owner()
        assert owner.binding == consumer.binding
        assert owner.baseline_rooms_sha256 == consumer.baseline_sha256
        assert owner.consumer_attached_revision == 1
        rooms = bootstrap._journal.load_rooms(frozenset(room_ids))
        assert set(rooms) == set(room_ids)
        assert all(room.state.current_membership_epoch == 0 for room in rooms.values())
        assert all(room.active_lane.membership_epoch == 0 for room in rooms.values())
        assert all(
            room.state.hydration_status is RoomHydrationStatus.PENDING
            for room in rooms.values()
        )

        ready = bootstrap._journal.load_ready_heads(limit=10)
        assert [row.ready_order for row in ready] == [0, 1]
        assert [row.record.room_id for row in ready] == list(room_ids)
        assert all(isinstance(row.record, LossRecord) for row in ready)
        for row in ready:
            loss = row.record
            assert isinstance(loss, LossRecord)
            assert loss.reason is LossReason.BASELINE_LOST
            assert loss.membership_epoch == 0
            assert loss.origin == SystemOrigin(
                SystemOriginKind.FRESH_START,
                bootstrap.binding_operation_id,
            )
            assert loss.boundary == LossBoundary(None, None, None, None)
            assert loss.detail_json == (
                b'{"cause":"fresh_start","scope":"consumer_baseline"}'
            )

        revision = owner.revision
        await bootstrap.attach_consumer(consumer)
        assert bootstrap._journal.load_owner().revision == revision
        assert bootstrap._journal.load_ready_heads(limit=10) == ready
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_attach_consumer_rejects_digest_operation_sequence_and_retry_drift(
    tmp_path: Path,
) -> None:
    room_ids = ("!alpha:example.org",)
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        valid = _consumer_bootstrap(bootstrap, room_ids)
        with pytest.raises(LocalProtocolError, match="baseline_sha256"):
            await bootstrap.attach_consumer(
                _consumer_bootstrap(bootstrap, room_ids, baseline_sha256=b"x" * 32)
            )
        assert bootstrap._journal.load_owner().binding is None

        wrong_operation = ConsumerBootstrap(
            UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            valid.binding,
            valid.first_sequence,
            valid.baseline_room_ids,
            valid.baseline_sha256,
        )
        with pytest.raises(LocalProtocolError, match="binding_operation_id"):
            await bootstrap.attach_consumer(wrong_operation)

        wrong_sequence = ConsumerBootstrap(
            valid.binding_operation_id,
            valid.binding,
            valid.first_sequence + 1,
            valid.baseline_room_ids,
            valid.baseline_sha256,
        )
        with pytest.raises(LocalProtocolError, match="first_sequence"):
            await bootstrap.attach_consumer(wrong_sequence)

        await bootstrap.attach_consumer(valid)
        with pytest.raises(LocalProtocolError, match="consumer binding"):
            await bootstrap.attach_consumer(
                _consumer_bootstrap(
                    bootstrap,
                    room_ids,
                    consumer_generation=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                )
            )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_exact_attach_retry_authenticates_durable_baseline_losses(
    tmp_path: Path,
) -> None:
    room_ids = ("!alpha:example.org",)
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, room_ids)
    await bootstrap.attach_consumer(consumer)
    row = bootstrap._journal.connection.execute(
        "SELECT loss_id, detail_ciphertext FROM NioIngestLoss WHERE account_id = ?",
        (ACCOUNT_ID,),
    ).fetchone()
    tampered = bytearray(row["detail_ciphertext"])
    tampered[-1] ^= 1
    bootstrap._journal.connection.execute(
        "UPDATE NioIngestLoss SET detail_ciphertext = ? "
        "WHERE account_id = ? AND loss_id = ?",
        (bytes(tampered), ACCOUNT_ID, row["loss_id"]),
    )
    try:
        with pytest.raises(JournalIntegrityError, match="authentication"):
            await bootstrap.attach_consumer(consumer)
        with pytest.raises(LocalProtocolError, match="not validated"):
            bootstrap.assert_http_enabled()
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_exact_attach_retry_survives_batch_frontier_advance_and_restart(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=consumer.first_sequence,
        created_revision=2,
        records=(_batch_event(1),),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(batches=(batch,)),
    )
    assert bootstrap.next_batch_sequence == consumer.first_sequence + 1
    bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        with pytest.raises(LocalProtocolError, match="not validated"):
            reopened.assert_http_enabled()
        with pytest.raises(LocalProtocolError, match="not validated"):
            reopened._journal.oldest_unacknowledged()
        await reopened.attach_consumer(consumer)
        assert reopened._journal.load_owner().revision == 2

        changed_sequence = ConsumerBootstrap(
            consumer.binding_operation_id,
            consumer.binding,
            consumer.first_sequence + 1,
            consumer.baseline_room_ids,
            consumer.baseline_sha256,
        )
        with pytest.raises(LocalProtocolError, match="first_sequence"):
            await reopened.attach_consumer(changed_sequence)
    finally:
        reopened.close()


def test_bootstrap_rejects_database_path_replaced_after_lock_acquisition(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    database_path = tmp_path / "journal.db"
    moved_path = tmp_path / "moved.db"
    try:
        connection = bootstrap._journal.connection
        connection.execute("PRAGMA wal_checkpoint(FULL)")
        os.replace(database_path, moved_path)
        with sqlite3.connect(database_path) as replacement:
            connection.backup(replacement)

        with pytest.raises(LocalProtocolError, match="file identity"):
            bootstrap.open_matrix_store(SqliteStore)
    finally:
        bootstrap.close()


def _replace_lock_path(database_path: Path) -> None:
    lock_path = Path(f"{database_path}.ingest.lock")
    lock_path.unlink()
    lock_path.write_bytes(b"replacement")


def test_current_owner_fails_closed_when_retained_lock_path_is_replaced(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        _replace_lock_path(tmp_path / "journal.db")

        with pytest.raises(LocalProtocolError, match="lock file identity"):
            bootstrap._journal.load_owner()
    finally:
        bootstrap.close()


def test_replaced_lock_path_cannot_admit_second_owner_or_mutate_state(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    writer_epoch = str(bootstrap._journal.writer_epoch)
    with sqlite3.connect(database_path) as connection:
        e2ee_tables = _table_names(database_path) - {
            table
            for table in _table_names(database_path)
            if table.startswith("NioIngest")
        }
        store_versions = connection.execute(
            "SELECT version FROM storeversion"
        ).fetchall()
    _replace_lock_path(database_path)

    second = None
    error: BaseException | None = None
    try:
        second = open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name=database_path.name,
        )
    except BaseException as caught:
        error = caught
    finally:
        if second is not None:
            second.close()
        bootstrap.close()

    assert isinstance(error, LocalProtocolError)
    with sqlite3.connect(database_path) as connection:
        assert (
            connection.execute(
                "SELECT writer_epoch FROM NioIngestMeta WHERE account_id = ?",
                (ACCOUNT_ID,),
            ).fetchone()[0]
            == writer_epoch
        )
        assert (
            _table_names(database_path)
            - {
                table
                for table in _table_names(database_path)
                if table.startswith("NioIngest")
            }
            == e2ee_tables
        )
        assert (
            connection.execute("SELECT version FROM storeversion").fetchall()
            == store_versions
        )


def test_encrypted_row_codec_authenticates_every_aad_dimension() -> None:
    digest = hashlib.sha256(b"payload").digest()
    codec = EncryptedRowCodec("secret", ACCOUNT_ID, STREAM_ID)
    ciphertext = codec.encrypt("NioIngestFrame", ("frame-1",), b"payload", digest)

    assert (
        codec.decrypt(
            "NioIngestFrame",
            ("frame-1",),
            ciphertext,
            digest,
        )
        == b"payload"
    )
    swaps = (
        (
            EncryptedRowCodec("secret", "@other:example.org", STREAM_ID),
            "NioIngestFrame",
            ("frame-1",),
            digest,
        ),
        (codec, "NioIngestBatch", ("frame-1",), digest),
        (codec, "NioIngestFrame", ("frame-2",), digest),
        (
            EncryptedRowCodec(
                "secret", ACCOUNT_ID, UUID("44444444-4444-4444-4444-444444444444")
            ),
            "NioIngestFrame",
            ("frame-1",),
            digest,
        ),
        (codec, "NioIngestFrame", ("frame-1",), b"z" * 32),
    )
    for swapped_codec, table, primary_key, swapped_digest in swaps:
        with pytest.raises(JournalIntegrityError):
            swapped_codec.decrypt(
                table,
                primary_key,
                ciphertext,
                swapped_digest,
            )


def _lifecycle(room_id: str, membership_epoch: int, frame_index: int) -> EventRecord:
    return EventRecord(
        f"lifecycle-{membership_epoch}",
        RecordKind.ROOM_LIFECYCLE,
        RecordOrigin(TransportKind.CLASSIC, 1, 1, frame_index),
        room_id,
        membership_epoch,
        membership_epoch,
        None,
        TimelineEventProvenance.LIVE,
        f'{{"epoch":{membership_epoch}}}'.encode(),
        None,
    )


def _snapshot(room_id: str, membership_epoch: int) -> RoomSnapshot:
    return RoomSnapshot(
        room_id,
        membership_epoch,
        ACCOUNT_ID,
        "join",
        True,
        "Room",
        None,
        None,
        None,
        "invite",
        "11",
        "forbidden",
        None,
        (),
    )


@pytest.mark.asyncio
async def test_room_state_round_trips_active_lane_and_retiring_epoch_chain(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    room_id = "!room:example.org"
    state = RoomState(
        room_id=room_id,
        current_membership_epoch=3,
        next_room_sequence=8,
        hydration_status=RoomHydrationStatus.READY,
        snapshot=_snapshot(room_id, 3),
    )
    lanes = (
        RoomLane(
            room_id=room_id,
            membership_epoch=1,
            lane_status=LaneStatus.RETIRING,
            successor_membership_epoch=2,
            pending_lifecycle=_lifecycle(room_id, 2, 0),
        ),
        RoomLane(
            room_id=room_id,
            membership_epoch=2,
            lane_status=LaneStatus.RETIRING,
            successor_membership_epoch=3,
            pending_lifecycle=_lifecycle(room_id, 3, 1),
        ),
        RoomLane(
            room_id=room_id,
            membership_epoch=3,
            lane_status=LaneStatus.ACTIVE,
            release_phase=ReleasePhase.IDLE,
        ),
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(room_states=(state,), room_lanes=lanes),
        )
    finally:
        bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        aggregate = reopened._journal.load_rooms(frozenset({room_id}))[room_id]
        assert aggregate.state == state
        assert aggregate.active_lane == lanes[2]
        assert aggregate.retiring_lanes == lanes[:2]
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_one_room_transition_never_scans_or_rewrites_other_rooms(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_a = "!a:example.org"
    room_b = "!b:example.org"
    initial = JournalTransition(
        room_states=(
            RoomState(room_a, 1, 1, RoomHydrationStatus.READY, _snapshot(room_a, 1)),
            RoomState(room_b, 1, 1, RoomHydrationStatus.READY, _snapshot(room_b, 1)),
        ),
        room_lanes=(
            RoomLane(room_a, 1, LaneStatus.ACTIVE),
            RoomLane(room_b, 1, LaneStatus.ACTIVE),
        ),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=initial,
    )
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        updated_a = RoomState(
            room_a,
            1,
            2,
            RoomHydrationStatus.READY,
            _snapshot(room_a, 1),
        )
        bootstrap._journal.commit(
            expected_revision=2,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(room_states=(updated_a,)),
        )

        aggregate_b = bootstrap._journal.load_rooms(frozenset({room_b}))[room_b]
        assert aggregate_b.state.next_room_sequence == 1
        room_queries = [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith("SELECT")
            and ("NioIngestRoomState" in statement or "NioIngestRoomLane" in statement)
        ]
        assert room_queries
        assert all("room_id IN" in statement for statement in room_queries)
        writes = [
            statement
            for statement in statements
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE"))
            and ("NioIngestRoomState" in statement or "NioIngestRoomLane" in statement)
        ]
        assert all(room_b not in statement for statement in writes)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_wrong_revision_or_writer_epoch_refuses_transition_without_write(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    transition = JournalTransition(
        source_state=SourceState(1, TransportKind.CLASSIC, b'"cursor"', 2, True)
    )
    try:
        with pytest.raises(JournalConflictError):
            bootstrap._journal.commit(
                expected_revision=0,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=transition,
            )
        with pytest.raises(JournalConflictError):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=UUID("ffffffff-ffff-ffff-ffff-ffffffffffff"),
                transition=transition,
            )
        assert bootstrap._journal.load_owner().revision == 1
        assert bootstrap._journal.load_source() is None
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_loss_round_trips_after_its_source_frame_is_compacted(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    frame = StagedFrame(
        UUID("55555555-5555-5555-5555-555555555555"),
        4,
        9,
        b'{"next_batch":"s10"}',
    )
    origin = RecordOrigin(TransportKind.CLASSIC, 4, 9, 3)
    boundary = LossBoundary("$prior", 1_700_000_000_000, "s1", "s9")
    incomplete = LossRecord(
        "",
        origin,
        "!loss:example.org",
        7,
        LossReason.FETCH_FAILED,
        boundary,
        b'{"errcode":"M_FORBIDDEN"}',
    )
    loss = LossRecord(
        _loss_id(bootstrap.stream_id, incomplete),
        origin,
        incomplete.room_id,
        incomplete.membership_epoch,
        incomplete.reason,
        boundary,
        incomplete.detail_json,
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(frames=(frame,), losses=(loss,)),
        )
        bootstrap._journal.commit(
            expected_revision=2,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(delete_frame_ids=(frame.frame_id,)),
        )
        assert bootstrap._journal.load_frame(frame.frame_id) is None

        loaded = bootstrap._journal.load_loss(loss.loss_id)
        assert loaded == loss
        row = bootstrap._journal.connection.execute(
            "SELECT origin_sha256, boundary_sha256, detail_sha256, loss_sha256 "
            "FROM NioIngestLoss WHERE account_id = ? AND loss_id = ?",
            (ACCOUNT_ID, loss.loss_id),
        ).fetchone()
        origin_payload = (
            b'{"origin_type":"transport","transport":"classic",'
            b'"source_epoch":4,"request_id":9,"frame_index":3}'
        )
        boundary_payload = (
            b'{"prior_event_id":"$prior",'
            b'"prior_origin_server_ts":1700000000000,'
            b'"start_token":"s1","target_token":"s9"}'
        )
        whole_loss_payload = (
            b'{"record_type":"loss","loss_id":"'
            + loss.loss_id.encode()
            + b'","origin":'
            + origin_payload
            + b',"room_id":"!loss:example.org","membership_epoch":7,'
            b'"reason":"fetch_failed","boundary":'
            + boundary_payload
            + b',"detail_json":"eyJlcnJjb2RlIjoiTV9GT1JCSURERU4ifQ=="}'
        )
        assert tuple(bytes(value) for value in row) == (
            hashlib.sha256(origin_payload).digest(),
            hashlib.sha256(boundary_payload).digest(),
            hashlib.sha256(loss.detail_json).digest(),
            hashlib.sha256(whole_loss_payload).digest(),
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_deterministic_loss_and_ready_ids_fail_closed_on_payload_drift(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    origin = RecordOrigin(TransportKind.CLASSIC, 1, 1, 0)
    boundary = LossBoundary(None, None, "s1", "s2")
    incomplete = LossRecord(
        "",
        origin,
        "!room:example.org",
        1,
        LossReason.FETCH_FAILED,
        boundary,
        b'{"attempt":1}',
    )
    loss = LossRecord(
        _loss_id(bootstrap.stream_id, incomplete),
        origin,
        incomplete.room_id,
        incomplete.membership_epoch,
        incomplete.reason,
        boundary,
        incomplete.detail_json,
    )
    event = _batch_event(1)
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                ready_records=(ReadyRecord(0, event),),
                losses=(loss,),
            ),
        )
        changed_loss = LossRecord(
            loss.loss_id,
            loss.origin,
            loss.room_id,
            loss.membership_epoch,
            loss.reason,
            loss.boundary,
            b'{"attempt":2}',
        )
        changed_event = EventRecord(
            event.record_id,
            event.kind,
            event.origin,
            event.room_id,
            event.membership_epoch,
            event.room_sequence,
            event.event_id,
            event.provenance,
            b'{"sequence":"changed"}',
            None,
        )
        with pytest.raises(JournalIntegrityError, match="ready record_id"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    ready_records=(ReadyRecord(1, changed_event),),
                ),
            )
        with pytest.raises(JournalIntegrityError, match="loss_id"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(losses=(changed_loss,)),
            )
        owner = bootstrap._journal.load_owner()
        assert owner.revision == 2
        assert owner.next_ready_order == 1
        assert bootstrap._journal.load_loss(loss.loss_id) == loss
        assert bootstrap._journal.load_ready_heads(limit=10)[0].record == event

        forged_loss = LossRecord(
            loss.loss_id,
            loss.origin,
            "!forged:example.org",
            loss.membership_epoch,
            loss.reason,
            loss.boundary,
            loss.detail_json,
        )
        forged_digest = hashlib.sha256(
            _canonical_json(_record_to_dict(forged_loss))
        ).digest()
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestLoss SET room_id = ?, loss_sha256 = ? "
            "WHERE account_id = ? AND loss_id = ?",
            (forged_loss.room_id, forged_digest, ACCOUNT_ID, loss.loss_id),
        )
        with pytest.raises(JournalIntegrityError, match="loss_id"):
            bootstrap._journal.load_loss(loss.loss_id)

        bootstrap._journal.connection.execute(
            "UPDATE NioIngestReadyRecord SET room_id = ?, "
            "membership_epoch = ?, room_sequence = ? "
            "WHERE account_id = ? AND record_id = ?",
            ("!forged:example.org", 9, 9, ACCOUNT_ID, event.record_id),
        )
        with pytest.raises(JournalIntegrityError, match="ready record columns"):
            bootstrap._journal.load_ready_heads(limit=10)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_ready_canonical_byte_accounting_is_derived_and_revalidated(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    event = _batch_event(1)
    try:
        with pytest.raises(JournalIntegrityError, match="canonical_bytes"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    ready_records=(ReadyRecord(0, event, canonical_bytes=1),),
                ),
            )
        assert bootstrap._journal.load_owner().revision == 1

        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(ready_records=(ReadyRecord(0, event),)),
        )
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestReadyRecord SET canonical_bytes = canonical_bytes + 1 "
            "WHERE account_id = ? AND record_id = ?",
            (ACCOUNT_ID, event.record_id),
        )
        with pytest.raises(JournalIntegrityError, match="canonical_bytes"):
            bootstrap._journal.load_ready_heads(limit=1)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_frame_id_is_insert_or_exact_revalidate_never_overwritten(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    frame = StagedFrame(
        UUID("55555555-5555-5555-5555-555555555555"),
        1,
        1,
        b'{"raw":1}',
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(frames=(frame,)),
        )
        changed = StagedFrame(frame.frame_id, 2, 9, b'{"raw":2}')
        with pytest.raises(JournalIntegrityError, match="frame_id"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(frames=(changed,)),
            )
        assert bootstrap._journal.load_owner().revision == 2
        assert bootstrap._journal.load_frame(frame.frame_id) == frame
    finally:
        bootstrap.close()


def _batch_event(sequence: int) -> EventRecord:
    return EventRecord(
        f"batch-record-{sequence}",
        RecordKind.GLOBAL_ACCOUNT_DATA,
        RecordOrigin(TransportKind.CLASSIC, 1, sequence, 0),
        None,
        None,
        None,
        None,
        None,
        f'{{"sequence":{sequence}}}'.encode(),
        None,
    )


@pytest.mark.asyncio
async def test_frame_read_authenticates_every_returned_metadata_field(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    frame = StagedFrame(
        UUID("55555555-5555-5555-5555-555555555555"),
        4,
        9,
        b'{"raw":true}',
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(frames=(frame,)),
        )
        for column, original, forged in (
            ("source_epoch", 4, 99),
            ("request_id", 9, 88),
            ("staged_revision", 2, 77),
        ):
            bootstrap._journal.connection.execute(
                f"UPDATE NioIngestFrame SET {column} = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (forged, ACCOUNT_ID, str(frame.frame_id)),
            )
            with pytest.raises(JournalIntegrityError, match="frame"):
                bootstrap._journal.load_frame(frame.frame_id)
            bootstrap._journal.connection.execute(
                f"UPDATE NioIngestFrame SET {column} = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (original, ACCOUNT_ID, str(frame.frame_id)),
            )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_ready_read_authenticates_every_returned_metadata_field(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    source_frame_id = UUID("55555555-5555-5555-5555-555555555555")
    event = _batch_event(1)
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                ready_records=(ReadyRecord(0, event, source_frame_id),),
            ),
        )
        for column, original, forged in (
            ("ready_order", 0, 99),
            (
                "source_frame_id",
                str(source_frame_id),
                "66666666-6666-6666-6666-666666666666",
            ),
            ("created_revision", 2, 77),
        ):
            bootstrap._journal.connection.execute(
                f"UPDATE NioIngestReadyRecord SET {column} = ? "
                "WHERE account_id = ? AND record_id = ?",
                (forged, ACCOUNT_ID, event.record_id),
            )
            with pytest.raises(JournalIntegrityError, match="ready"):
                bootstrap._journal.load_ready_heads(limit=1)
            bootstrap._journal.connection.execute(
                f"UPDATE NioIngestReadyRecord SET {column} = ? "
                "WHERE account_id = ? AND record_id = ?",
                (original, ACCOUNT_ID, event.record_id),
            )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_batch_commit_and_read_validate_full_owner_and_row_identity(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    wrong_revision = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=99,
        records=(_batch_event(1),),
    )
    try:
        with pytest.raises(JournalIntegrityError, match="created_revision"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(batches=(wrong_revision,)),
            )
        assert bootstrap._journal.load_owner().revision == 1

        batch = batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=1,
            created_revision=2,
            records=(_batch_event(1),),
        )
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(batches=(batch,)),
        )
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatch SET created_revision = 98 "
            "WHERE account_id = ? AND sequence = 1",
            (ACCOUNT_ID,),
        )
        with pytest.raises(JournalIntegrityError, match="created_revision"):
            bootstrap._journal.oldest_unacknowledged()
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatch SET created_revision = 2 "
            "WHERE account_id = ? AND sequence = 1",
            (ACCOUNT_ID,),
        )
        forged = batch_from_records(
            account_id="@forged:example.org",
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=1,
            created_revision=2,
            records=(_batch_event(1),),
        )
        payload = canonical_batch_payload(forged)
        digest = hashlib.sha256(payload).digest()
        ciphertext = EncryptedRowCodec(
            "secret",
            ACCOUNT_ID,
            bootstrap.stream_id,
        ).encrypt("NioIngestBatch", (1,), payload, digest)
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatch SET batch_id = ?, payload_ciphertext = ?, "
            "payload_sha256 = ? WHERE account_id = ? AND sequence = 1",
            (str(forged.ref.batch_id), ciphertext, digest, ACCOUNT_ID),
        )
        with pytest.raises(JournalIntegrityError, match="owner"):
            bootstrap._journal.oldest_unacknowledged()
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_acknowledgement_is_fifo_idempotent_and_keeps_only_latest_ack(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    batches = tuple(
        batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=sequence,
            created_revision=2,
            records=(_batch_event(sequence),),
        )
        for sequence in range(1, 4)
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(batches=batches),
        )
        assert bootstrap._journal.oldest_unacknowledged() == batches[0]

        with pytest.raises(JournalConflictError, match="out of order"):
            bootstrap._journal.acknowledge(batches[1].ref)
        wrong_digest = type(batches[0].ref)(
            batches[0].ref.stream_id,
            batches[0].ref.sequence,
            batches[0].ref.batch_id,
            b"x" * 32,
        )
        with pytest.raises(JournalConflictError, match="reference"):
            bootstrap._journal.acknowledge(wrong_digest)

        assert bootstrap._journal.acknowledge(batches[0].ref) is AckOutcome.ACKNOWLEDGED
        assert (
            bootstrap._journal.acknowledge(batches[0].ref)
            is AckOutcome.ALREADY_ACKNOWLEDGED
        )
        with pytest.raises(JournalConflictError, match="reference"):
            bootstrap._journal.acknowledge(wrong_digest)

        assert bootstrap._journal.acknowledge(batches[1].ref) is AckOutcome.ACKNOWLEDGED
        rows = bootstrap._journal.connection.execute(
            "SELECT sequence, acknowledged_revision FROM NioIngestBatch "
            "WHERE account_id = ? ORDER BY sequence",
            (ACCOUNT_ID,),
        ).fetchall()
        assert [(row[0], row[1] is not None) for row in rows] == [
            (2, True),
            (3, False),
        ]
        with pytest.raises(JournalConflictError, match="stale"):
            bootstrap._journal.acknowledge(batches[0].ref)
        assert bootstrap._journal.oldest_unacknowledged() == batches[2]
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_latest_ack_retry_revalidates_persisted_frontier_and_payload(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=2,
        records=(_batch_event(1),),
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(batches=(batch,)),
        )
        bootstrap._journal.acknowledge(batch.ref)
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestMeta SET last_acked_sha256 = ? WHERE account_id = ?",
            (b"z" * 32, ACCOUNT_ID),
        )

        with pytest.raises(JournalIntegrityError, match="frontier"):
            bootstrap._journal.acknowledge(batch.ref)
    finally:
        bootstrap.close()
