import asyncio
import hashlib
import json
import os
import sqlite3
import threading
import time
import warnings
from dataclasses import replace
from pathlib import Path
from uuid import UUID, uuid5

import pytest
from peewee import SqliteDatabase

from nio.event_provenance import TimelineEventProvenance
from nio.ingest import (
    BatchRef,
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
from nio.ingest.classic import ClassicSource
from nio.ingest.errors import (
    FreshIngestionRequired,
    JournalConflictError,
    JournalIntegrityError,
)
from nio.ingest.effects import (
    MembershipOperationRef,
    MembershipOperationResolution,
)
from nio.ingest.membership import MembershipBaseline
from nio.ingest.recovery import RecoveryGap
from nio.ingest.state import (
    AckOutcome,
    BatchMaterialization,
    ConsumerAttachStatus,
    JournalTransition,
    LaneRecord,
    LaneRecordKey,
    LaneRecordSection,
    LaneStatus,
    ReleasePhase,
    ReadyRecord,
    ReadyRecordKey,
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
from nio.crypto import OlmAccount
from nio.store import DefaultStore, MatrixStore, SqliteStore
from nio.store._sync_journal import SqliteIngestionJournal
from nio.store._sync_journal_codec import EncryptedRowCodec
from nio.store.database import use_database
from nio.store.sync_journal import (
    open_ingestion_store,
)

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
STREAM_ID = UUID("33333333-3333-3333-3333-333333333333")
SYSTEM_OPERATION_ID = UUID("44444444-4444-4444-4444-444444444444")
STALE_SYSTEM_RECORD_ID = "99999999-9999-5999-8999-999999999999"


JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")

CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")


def _sliding_source_config(*, all_rooms_page_size: int = 100) -> SlidingSourceConfig:
    return SlidingSourceConfig(
        timeout_ms=30_000,
        connection_name="worker",
        lists_json=b"{}",
        room_subscriptions_json=b"{}",
        extensions_json=b"{}",
        all_rooms_page_size=all_rooms_page_size,
    )


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

        def load_lane_record(self, key):
            return None

        def list_lane_records(self, room_id, membership_epoch, section=None):
            return ()

        def load_frame(self, frame_id):
            return None

        def load_network_effect(self, effect_id):
            return None

        def list_network_effects(self, limit):
            return ()

        def list_schedulable_network_effects(self, limit):
            return ()

        def claim_membership_operation(self, effect_id):
            return None

        def uncertain_membership_operations(self, limit, *, after_effect_id=None):
            return ()

        def resolve_membership_operation(self, ref, resolution):
            return None

        def commit(self, *, expected_revision, writer_epoch, transition):
            return None

        def oldest_unacknowledged(self):
            return None

        def acknowledge(self, ref):
            return AckOutcome.ACKNOWLEDGED

    assert isinstance(RecordingJournal(), IngestionJournal)


def test_internal_journal_protocol_has_no_parallel_loss_ledger_lookup() -> None:
    from nio.store._sync_journal_port import IngestionJournal

    assert "load_loss" not in IngestionJournal.__dict__


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

    assert not hasattr(classic, "full_state_on_cold_start")
    assert sliding.connection_name == "main"
    assert sliding.all_rooms_page_size == 100
    with pytest.raises(TypeError, match="timeout_ms"):
        ClassicSourceConfig(timeout_ms=True, filter_json=b"{}")
    with pytest.raises(TypeError, match="filter_json"):
        ClassicSourceConfig(timeout_ms=1, filter_json="{}")  # type: ignore[arg-type]
    with pytest.raises(TypeError, match="all_rooms_page_size"):
        replace(sliding, all_rooms_page_size=True)
    with pytest.raises(ValueError, match="all_rooms_page_size"):
        replace(sliding, all_rooms_page_size=0)
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
    with pytest.raises(TypeError, match="sliding_bootstrap_range_size"):
        IngestionConfig(
            source=source,
            consumer=consumer,
            sliding_bootstrap_range_size=1,  # type: ignore[call-arg]
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
    with warnings.catch_warnings():
        warnings.filterwarnings("ignore", message="This process .* multi-threaded")
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


def test_open_requires_an_exact_source_config_before_touching_the_store(
    tmp_path: Path,
) -> None:
    class DerivedClassicSourceConfig(ClassicSourceConfig):
        pass

    for source in (
        object(),
        DerivedClassicSourceConfig(timeout_ms=0, filter_json=b"{}"),
    ):
        with pytest.raises(
            TypeError,
            match="source must be ClassicSourceConfig or SlidingSourceConfig",
        ):
            open_ingestion_store(
                tmp_path,
                account_id=ACCOUNT_ID,
                device_id=DEVICE_ID,
                source=source,  # type: ignore[arg-type]
                database_name="journal.db",
            )

    assert not (tmp_path / "journal.db").exists()
    assert not (tmp_path / "journal.db.ingest.lock").exists()


@pytest.mark.parametrize(
    ("source_factory", "transport", "page_size"),
    (
        (lambda: CLASSIC_SOURCE, TransportKind.CLASSIC, None),
        (
            lambda: _sliding_source_config(all_rooms_page_size=1),
            TransportKind.SLIDING,
            1,
        ),
        (lambda: _sliding_source_config(), TransportKind.SLIDING, 100),
    ),
)
def test_fresh_open_freezes_transport_and_inserts_one_cold_source_before_attach(
    tmp_path: Path,
    source_factory,
    transport: TransportKind,
    page_size: int | None,
) -> None:
    source_config = source_factory()
    database_name = f"{transport.value}-{page_size}.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=source_config,
        database_name=database_name,
    )
    try:
        owner = bootstrap._journal.load_owner()
        source_state = bootstrap._journal.load_source()

        assert owner.transport_kind is transport
        assert source_state is not None
        assert source_state.source_epoch == 0
        assert source_state.transport_kind is transport
        assert source_state.next_request_id == 1
        assert source_state.active is True
        with sqlite3.connect(tmp_path / database_name) as connection:
            assert connection.execute(
                "SELECT transport_kind FROM NioIngestMeta"
            ).fetchall() == [(transport.value,)]
            assert connection.execute(
                "SELECT COUNT(*) FROM NioIngestSourceState"
            ).fetchone() == (1,)
            source_columns = {
                row[1]
                for row in connection.execute(
                    'PRAGMA table_info("NioIngestSourceState")'
                )
            }
        assert "transport_kind" not in source_columns

        if transport is TransportKind.CLASSIC:
            assert source_state.cursor_json == b'{"next_batch":null}'
            request = ClassicSource(bootstrap.stream_id, source_config).plan_request(
                source_state,
                request_id=1,
            )
            assert request is not None
            assert request.query[0] == ("full_state", "true")
        else:
            cursor = json.loads(source_state.cursor_json)
            assert cursor == {
                "all_rooms_coverage_complete": False,
                "all_rooms_page_size": page_size,
                "all_rooms_range_ack_mode": "unknown",
                "all_rooms_range_end": page_size - 1,
                "connection_instance": cursor["connection_instance"],
                "connection_name": source_config.connection_name,
                "pos": None,
                "to_device_since": None,
            }
            UUID(cursor["connection_instance"])
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("source_pair_factory",),
    (
        (
            lambda: (
                ClassicSourceConfig(timeout_ms=1, filter_json=b'{"a":1}'),
                ClassicSourceConfig(timeout_ms=90_000, filter_json=b'{"b":2}'),
            ),
        ),
        (
            lambda: (
                SlidingSourceConfig(1, "first", b"{}", b"{}", b"{}", 1),
                SlidingSourceConfig(
                    90_000,
                    "changed",
                    b'{"caller":{}}',
                    b'{"!room:example.org":{}}',
                    b'{"custom":{"enabled":true}}',
                    99,
                ),
            ),
        ),
    ),
)
def test_same_transport_reopen_keeps_the_exact_durable_source_across_config_drift(
    tmp_path: Path,
    source_pair_factory,
) -> None:
    created_source, reopened_source = source_pair_factory()
    database_path = tmp_path / "journal.db"
    first = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=created_source,
        pickle_key="secret",
        database_name=database_path.name,
    )
    durable_source = first._journal.load_source()
    first.close()
    with sqlite3.connect(database_path) as connection:
        encrypted_before = connection.execute(
            "SELECT * FROM NioIngestSourceState"
        ).fetchone()

    reopened = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=reopened_source,
        pickle_key="secret",
        database_name=database_path.name,
    )
    try:
        assert reopened._journal.load_source() == durable_source
        with sqlite3.connect(database_path) as connection:
            encrypted_after = connection.execute(
                "SELECT * FROM NioIngestSourceState"
            ).fetchone()
        assert encrypted_after == encrypted_before
    finally:
        reopened.close()


@pytest.mark.parametrize(
    ("source_pair_factory",),
    (
        (lambda: (CLASSIC_SOURCE, _sliding_source_config()),),
        (lambda: (_sliding_source_config(), CLASSIC_SOURCE),),
    ),
)
def test_cross_transport_reopen_fails_before_any_database_write(
    tmp_path: Path,
    source_pair_factory,
) -> None:
    created_source, wrong_source = source_pair_factory()
    database_path = tmp_path / "journal.db"
    first = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=created_source,
        database_name=database_path.name,
    )
    first.close()
    with sqlite3.connect(database_path) as connection:
        before = connection.execute(
            "SELECT writer_epoch, transport_kind FROM NioIngestMeta"
        ).fetchone()

    statements: list[str] = []
    with pytest.raises(LocalProtocolError, match="transport"):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            source=wrong_source,
            database_name=database_path.name,
            statement_observer=statements.append,
        )

    with sqlite3.connect(database_path) as connection:
        after = connection.execute(
            "SELECT writer_epoch, transport_kind FROM NioIngestMeta"
        ).fetchone()
    assert after == before
    assert not any(
        statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in statements
    )
    exact = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=created_source,
        database_name=database_path.name,
    )
    exact.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("source_factory", "wrong_transport"),
    (
        (lambda: CLASSIC_SOURCE, TransportKind.SLIDING),
        (lambda: _sliding_source_config(), TransportKind.CLASSIC),
    ),
)
async def test_source_transition_cannot_replace_frozen_transport_before_write(
    tmp_path: Path,
    source_factory,
    wrong_transport: TransportKind,
) -> None:
    source_config = source_factory()
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        source=source_config,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    original_source = bootstrap._journal.load_source()
    assert original_source is not None
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="transport"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    source_state=replace(
                        original_source,
                        transport_kind=wrong_transport,
                    )
                ),
            )

        assert bootstrap._journal.load_owner().revision == 1
        assert bootstrap._journal.load_source() == original_source
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


def test_fresh_open_creates_independent_v1_schema_with_marker_first(
    tmp_path: Path,
) -> None:
    statements: list[str] = []

    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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


def test_v1_schema_has_only_ready_lane_and_unacknowledged_batch_record_owners(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        connection = bootstrap._journal.connection
        tables = {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }
        assert "NioIngestLoss" not in tables
        assert "NioIngestConsumerResetRoom" not in tables
        assert "NioIngestBatchItem" in tables

        ready_columns = tuple(
            row[1]
            for row in connection.execute("PRAGMA table_info(NioIngestReadyRecord)")
        )
        assert ready_columns == (
            "account_id",
            "ready_order",
            "item_id",
            "item_kind",
            "source_frame_id",
            "room_id",
            "membership_epoch",
            "room_sequence",
            "payload_ciphertext",
            "payload_sha256",
            "canonical_bytes",
            "created_revision",
        )
        batch_columns = tuple(
            row[1] for row in connection.execute("PRAGMA table_info(NioIngestBatch)")
        )
        assert batch_columns == (
            "account_id",
            "sequence",
            "batch_id",
            "payload_ciphertext",
            "payload_sha256",
            "created_revision",
        )
        batch_item_info = connection.execute(
            "PRAGMA table_info(NioIngestBatchItem)"
        ).fetchall()
        batch_item_columns = tuple(row[1] for row in batch_item_info)
        assert batch_item_columns == (
            "account_id",
            "item_id",
            "item_kind",
            "sequence",
            "record_ordinal",
        )
        assert tuple(
            row[1] for row in sorted(batch_item_info, key=lambda row: row[5]) if row[5]
        ) == ("account_id", "item_id")
        unique_indexes = {
            tuple(
                column[2]
                for column in connection.execute(
                    f'PRAGMA index_info("{index[1]}")'
                ).fetchall()
            )
            for index in connection.execute(
                "PRAGMA index_list(NioIngestBatchItem)"
            ).fetchall()
            if index[2]
        }
        assert unique_indexes == {
            ("account_id", "item_id"),
            ("account_id", "sequence", "record_ordinal"),
        }
        foreign_keys = connection.execute(
            "PRAGMA foreign_key_list(NioIngestBatchItem)"
        ).fetchall()
        assert {(row[2], row[3], row[4], row[6]) for row in foreign_keys} == {
            ("NioIngestBatch", "account_id", "account_id", "CASCADE"),
            ("NioIngestBatch", "sequence", "sequence", "CASCADE"),
        }
    finally:
        bootstrap.close()


def test_batch_item_schema_rejects_invalid_or_ambiguous_derived_ownership(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    connection = bootstrap._journal.connection
    try:
        connection.execute(
            "INSERT INTO NioIngestBatch VALUES (?, ?, ?, ?, ?, ?)",
            (
                ACCOUNT_ID,
                1,
                "11111111-1111-1111-1111-111111111111",
                b"ciphertext",
                b"d" * 32,
                1,
            ),
        )
        for values in (
            (ACCOUNT_ID, "orphan", "event", 2, 0),
            (ACCOUNT_ID, "", "event", 1, 0),
            (ACCOUNT_ID, "bad-kind", "unknown", 1, 0),
            (ACCOUNT_ID, "negative", "loss", 1, -1),
        ):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO NioIngestBatchItem VALUES (?, ?, ?, ?, ?)",
                    values,
                )

        connection.execute(
            "INSERT INTO NioIngestBatchItem VALUES (?, ?, ?, ?, ?)",
            (ACCOUNT_ID, "owned", "loss", 1, 0),
        )
        for values in (
            (ACCOUNT_ID, "owned", "loss", 1, 1),
            (ACCOUNT_ID, "other", "event", 1, 0),
        ):
            with pytest.raises(sqlite3.IntegrityError):
                connection.execute(
                    "INSERT INTO NioIngestBatchItem VALUES (?, ?, ?, ?, ?)",
                    values,
                )

        connection.execute(
            "DELETE FROM NioIngestBatch WHERE account_id = ? AND sequence = 1",
            (ACCOUNT_ID,),
        )
        assert (
            connection.execute("SELECT COUNT(*) FROM NioIngestBatchItem").fetchone()[0]
            == 0
        )
    finally:
        bootstrap.close()


def test_opening_v1_journal_never_reads_or_alters_legacy_sync_tables(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        with pytest.raises(LocalProtocolError, match="writer lock"):
            open_ingestion_store(
                tmp_path,
                source=CLASSIC_SOURCE,
                account_id=ACCOUNT_ID,
                device_id=DEVICE_ID,
                database_name="journal.db",
            )
    finally:
        first.close()

    with pytest.raises(LocalProtocolError, match="account_id"):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id="@mallory:example.org",
            device_id=DEVICE_ID,
            database_name="journal.db",
        )
    with pytest.raises(LocalProtocolError, match="device_id"):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id="OTHER",
            database_name="journal.db",
        )


def test_sqlite_backup_opens_at_a_distinct_path_with_a_new_sidecar(
    tmp_path: Path,
) -> None:
    source_path = tmp_path / "source.db"
    restored_path = tmp_path / "restored.db"
    source = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=source_path.name,
    )
    stream_id = source.stream_id
    source.close()

    with (
        sqlite3.connect(source_path) as source_connection,
        sqlite3.connect(restored_path) as restored_connection,
    ):
        source_connection.backup(restored_connection)

    restored_lock_path = Path(f"{restored_path}.ingest.lock")
    assert not restored_lock_path.exists()

    restored = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=restored_path.name,
    )
    try:
        assert restored.stream_id == stream_id
        store = restored.open_matrix_store(SqliteStore)
        assert not store.database.is_closed()
        assert restored_lock_path.exists()
    finally:
        restored.close()


def test_schema_version_is_validated_independently(tmp_path: Path) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name="journal.db",
        )


def test_existing_v1_schema_is_validated_without_repair(tmp_path: Path) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
            source=CLASSIC_SOURCE,
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
        "transport_check",
        "attach_status_check",
        "attach_ordinal_check",
        "attach_digest_type_check",
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
        source=CLASSIC_SOURCE,
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
        elif mutation == "transport_check":
            _rewrite_table_sql(
                connection,
                "NioIngestMeta",
                "transport_kind TEXT NOT NULL CHECK (transport_kind IN ('classic', 'sliding'))",
                "transport_kind TEXT NOT NULL",
            )
        elif mutation == "attach_status_check":
            _rewrite_table_sql(
                connection,
                "NioIngestMeta",
                "consumer_attach_status TEXT NOT NULL CHECK "
                "(consumer_attach_status IN ('unbound', 'attaching', 'attached'))",
                "consumer_attach_status TEXT NOT NULL",
            )
        elif mutation == "attach_ordinal_check":
            _rewrite_table_sql(
                connection,
                "NioIngestMeta",
                "consumer_attach_next_room_ordinal INTEGER NOT NULL CHECK "
                "(consumer_attach_next_room_ordinal >= 0)",
                "consumer_attach_next_room_ordinal INTEGER NOT NULL",
            )
        elif mutation == "attach_digest_type_check":
            _rewrite_table_sql(
                connection,
                "NioIngestMeta",
                "AND typeof(baseline_rooms_sha256) = 'blob'\n"
                "            AND length(baseline_rooms_sha256) = 32",
                "AND length(baseline_rooms_sha256) = 32",
            )
        else:
            connection.execute(
                "CREATE VIEW NioIngestUnexpectedView AS "
                "SELECT account_id FROM NioIngestMeta"
            )

    with pytest.raises(LocalProtocolError, match="topology"):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )
    writer_epoch = str(bootstrap._journal.writer_epoch)
    bootstrap.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "INSERT INTO NioIngestMeta SELECT ?, device_id, schema_version, "
            "stream_id, transport_kind, binding_operation_id, "
            "consumer_attach_status, consumer_attach_next_room_ordinal, "
            "journal_generation, "
            "consumer_generation, consumer_first_sequence, baseline_rooms_sha256, "
            "consumer_attached_revision, revision, writer_epoch, "
            "next_source_epoch, next_ready_order, "
            "next_batch_sequence, last_acked_sequence, last_acked_batch_id, "
            "last_acked_sha256, created_at_ns "
            "FROM NioIngestMeta WHERE account_id = ?",
            ("@mallory:example.org", ACCOUNT_ID),
        )

    with pytest.raises(LocalProtocolError, match="marker row"):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name=database_path.name,
        )

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT DISTINCT writer_epoch FROM NioIngestMeta"
        ).fetchall() == [(writer_epoch,)]


def test_existing_v1_open_rejects_a_missing_source_before_epoch_write(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )
    writer_epoch = str(bootstrap._journal.writer_epoch)
    bootstrap.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute("DELETE FROM NioIngestSourceState")

    with pytest.raises(LocalProtocolError, match="source row cardinality"):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name=database_path.name,
        )

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT writer_epoch FROM NioIngestMeta"
        ).fetchone() == (writer_epoch,)


def test_existing_v1_open_authenticates_source_before_epoch_write(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name=database_path.name,
    )
    writer_epoch = str(bootstrap._journal.writer_epoch)
    bootstrap.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE NioIngestSourceState SET cursor_sha256 = ?",
            (bytes(32),),
        )

    with pytest.raises(JournalIntegrityError, match="authentication"):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            pickle_key="secret",
            database_name=database_path.name,
        )

    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT writer_epoch FROM NioIngestMeta"
        ).fetchone() == (writer_epoch,)


def test_unexpected_trigger_is_rejected_before_it_can_mutate_legacy_state(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
            source=CLASSIC_SOURCE,
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
            source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
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
                source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
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
    begin_index = normalized.index("BEGIN IMMEDIATE")
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
    assert begin_index < fence_index < first_create


def test_nested_e2ee_calls_share_one_outer_writer_epoch_fence(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    store.save_account(OlmAccount())
    statements: list[str] = []
    store.database.connection().set_trace_callback(statements.append)

    try:
        store.load_sessions()
    finally:
        store.database.connection().set_trace_callback(None)
        bootstrap.close()

    assert (
        sum(
            "UPDATE NioIngestMeta SET writer_epoch = writer_epoch" in statement
            for statement in statements
        )
        == 1
    )


def test_e2ee_operation_rejects_an_ambient_transaction_before_sql(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)

    try:
        with store.database.atomic():
            statements: list[str] = []
            store.database.connection().set_trace_callback(statements.append)
            with pytest.raises(LocalProtocolError, match="ambient transaction"):
                store.load_account()
            assert statements == []
            store.database.connection().set_trace_callback(None)
    finally:
        store.database.connection().set_trace_callback(None)
        bootstrap.close()


def test_bootstrap_close_closes_its_e2ee_store_before_releasing_lock(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)

    bootstrap.close()

    assert store.database.is_closed()
    replacement = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    replacement.close()


def test_every_decorated_database_entrypoint_checks_ownership_first() -> None:
    class RevokedStoreProbe:
        models = ()

        def _database_operation(self):
            raise LocalProtocolError("ownership lease is revoked")

        @property
        def database(self):
            raise AssertionError("database was accessed before the ownership lease")

    probe = RevokedStoreProbe()
    decorated = []
    for store_class in (MatrixStore, DefaultStore, SqliteStore):
        for method in vars(store_class).values():
            if (
                callable(method)
                and hasattr(method, "__wrapped__")
                and not isinstance(method, (staticmethod, classmethod))
                and method.__name__ != "__repr__"
            ):
                decorated.append(method)
                with pytest.raises(LocalProtocolError, match="ownership lease"):
                    method(probe)

    assert decorated


@pytest.mark.parametrize("operation", ("load", "save", "upgrade"))
def test_retained_store_is_revoked_before_a_replacement_owner_opens(
    tmp_path: Path,
    operation: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    stale_store = bootstrap.open_matrix_store(SqliteStore)
    stale_store.save_account(OlmAccount())
    bootstrap.close()

    replacement = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    current_store = replacement.open_matrix_store(SqliteStore)
    try:
        operations = {
            "load": stale_store.load_account,
            "save": lambda: stale_store.save_account(OlmAccount()),
            "upgrade": stale_store.upgrade_to_v2,
        }
        with pytest.raises(LocalProtocolError, match="ownership lease"):
            operations[operation]()
        assert stale_store.database.is_closed()

        assert current_store.load_account() is not None
        current_store.save_account(OlmAccount())
    finally:
        replacement.close()


def test_store_lease_remains_revoked_when_database_close_fails(
    tmp_path: Path,
    monkeypatch,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    real_close = store.database.close

    def fail_close() -> None:
        raise RuntimeError("injected database close failure")

    monkeypatch.setattr(store.database, "close", fail_close)
    with pytest.raises(RuntimeError, match="injected database close failure"):
        bootstrap.close()

    with pytest.raises(LocalProtocolError, match="ownership lease"):
        store.load_account()
    with pytest.raises(LocalProtocolError, match="writer lock"):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name="journal.db",
        )

    monkeypatch.setattr(store.database, "close", real_close)
    bootstrap.close()


def test_bootstrap_close_waits_for_an_inflight_store_operation(
    tmp_path: Path,
) -> None:
    operation_entered = threading.Event()
    allow_operation = threading.Event()

    class PausingStore(SqliteStore):
        @use_database
        def pausing_load_account(self):
            operation_entered.set()
            assert allow_operation.wait(timeout=5)
            return self._get_account()

    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(PausingStore)
    store.save_account(OlmAccount())
    outcomes: list[object] = []

    def load_account() -> None:
        try:
            outcomes.append(store.pausing_load_account())
        except BaseException as error:
            outcomes.append(error)

    operation = threading.Thread(target=load_account)
    operation.start()
    assert operation_entered.wait(timeout=5)
    release = threading.Timer(0.1, allow_operation.set)
    release.start()
    started = time.monotonic()
    try:
        bootstrap.close()
    finally:
        allow_operation.set()
        operation.join(timeout=5)
        release.join(timeout=5)

    assert len(outcomes) == 1
    assert not isinstance(outcomes[0], BaseException)
    assert time.monotonic() - started >= 0.09
    with pytest.raises(LocalProtocolError, match="ownership lease"):
        store.load_account()


def test_direct_bootstrap_authority_is_consumed_and_registered_once(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    ref = MembershipOperationRef(
        UUID("aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"),
        "!room:example.org",
        0,
        1,
        b"x" * 32,
    )

    def inherited_operations() -> tuple[str, ...]:
        statements: list[str] = []
        bootstrap._journal.connection.set_trace_callback(statements.append)
        outcomes = (
            _operation_outcome(bootstrap._journal._assert_open),
            _operation_outcome(
                lambda: bootstrap._journal.claim_membership_operation(ref.effect_id)
            ),
            _operation_outcome(
                lambda: bootstrap._journal.resolve_membership_operation(
                    ref,
                    MembershipOperationResolution.SUPERSEDE,
                )
            ),
            _operation_outcome(bootstrap.close),
            _operation_outcome(bootstrap._journal._writer_lock.close),
        )
        return (*outcomes, json.dumps(statements))

    try:
        outcomes = _fork_outcomes(inherited_operations)
        assert len(outcomes) == 6
        assert all("acquiring process" in outcome for outcome in outcomes[:-1])
        assert outcomes[-1] == "[]"
        assert not store.database.is_closed()
        assert bootstrap._journal.load_owner().revision == 0
        second = None
        try:
            second = open_ingestion_store(
                tmp_path,
                source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
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


@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires POSIX fork")
def test_inherited_child_cannot_read_or_write_an_open_e2ee_store(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    store.save_account(OlmAccount())

    def inherited_operations() -> tuple[str, ...]:
        outcomes = (
            _operation_outcome(store.load_account),
            _operation_outcome(lambda: store.save_account(OlmAccount())),
            _operation_outcome(bootstrap.close),
        )
        store.database.close()
        return outcomes

    try:
        outcomes = _fork_outcomes(inherited_operations)
        assert len(outcomes) == 3
        assert all("acquiring process" in outcome for outcome in outcomes)

        assert store.load_account() is not None
        store.save_account(OlmAccount())
        with pytest.raises(LocalProtocolError, match="writer lock"):
            open_ingestion_store(
                tmp_path,
                source=CLASSIC_SOURCE,
                account_id=ACCOUNT_ID,
                device_id=DEVICE_ID,
                database_name="journal.db",
            )
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
            source=CLASSIC_SOURCE,
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


def _baseline_room_ids(count: int) -> tuple[str, ...]:
    return tuple(f"!baseline-{ordinal:05d}:example.org" for ordinal in range(count))


def _attach_room_chunks(labels: list[str]) -> list[int]:
    chunks: list[int] = []
    for label in labels:
        if label == "meta_attach":
            chunks.append(0)
        elif label == "room_state":
            chunks[-1] += 1
    return chunks


def test_fresh_owner_is_durably_unbound_at_ordinal_zero(tmp_path: Path) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        owner = bootstrap._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.UNBOUND
        assert owner.consumer_attach_next_room_ordinal == 0
        assert owner.binding is None
        assert owner.consumer_first_sequence is None
        assert owner.baseline_rooms_sha256 is None
        assert owner.consumer_attached_revision is None
        assert owner.revision == 0
        assert owner.next_ready_order == 0
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("assignments", "parameters"),
    (
        ("consumer_attach_status = 'unknown'", ()),
        ("consumer_attach_next_room_ordinal = -1", ()),
        ("consumer_attach_next_room_ordinal = 1", ()),
        ("revision = 1", ()),
        ("next_batch_sequence = 2", ()),
        ("last_acked_sequence = 1", ()),
        ("last_acked_batch_id = 'not-null'", ()),
        ("last_acked_sha256 = X'00'", ()),
        (
            "journal_generation = ?",
            (str(JOURNAL_GENERATION),),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "last_acked_sequence = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "last_acked_batch_id = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
            ),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "last_acked_sha256 = ?",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32, b"y" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, last_acked_sequence = -1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, last_acked_sequence = 1, "
            "last_acked_batch_id = ?, last_acked_sha256 = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
                b"y" * 32,
            ),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, last_acked_batch_id = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
            ),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, next_batch_sequence = 2, last_acked_sequence = 1, "
            "last_acked_batch_id = ?, last_acked_sha256 = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
                "y" * 32,
            ),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), "x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 0, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 0, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "next_batch_sequence = 0",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "next_batch_sequence = 2",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 0, "
            "revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, next_batch_sequence = 0",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 2, "
            "revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
    ),
)
def test_meta_attach_checks_reject_invalid_state_matrix(
    tmp_path: Path,
    assignments: str,
    parameters: tuple[object, ...],
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        with pytest.raises(sqlite3.IntegrityError):
            bootstrap._journal.connection.execute(
                f"UPDATE NioIngestMeta SET {assignments} WHERE account_id = ?",
                (*parameters, ACCOUNT_ID),
            )
        assert (
            bootstrap._journal.load_owner().consumer_attach_status
            is ConsumerAttachStatus.UNBOUND
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "missing_column",
    (
        "journal_generation",
        "consumer_generation",
        "consumer_first_sequence",
        "baseline_rooms_sha256",
    ),
)
def test_attaching_schema_requires_every_frozen_binding_field(
    tmp_path: Path,
    missing_column: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        with pytest.raises(sqlite3.IntegrityError):
            bootstrap._journal.connection.execute(
                "UPDATE NioIngestMeta SET consumer_attach_status = 'attaching', "
                "journal_generation = ?, consumer_generation = ?, "
                "consumer_first_sequence = 1, baseline_rooms_sha256 = ?, "
                "revision = 1, consumer_attach_next_room_ordinal = 1, "
                f"next_ready_order = 1, {missing_column} = NULL WHERE account_id = ?",
                (
                    str(JOURNAL_GENERATION),
                    str(CONSUMER_GENERATION),
                    b"x" * 32,
                    ACCOUNT_ID,
                ),
            )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("assignments", "parameters"),
    (
        ("consumer_attach_status = 'unknown'", ()),
        ("consumer_attach_next_room_ordinal = -1", ()),
        ("consumer_attach_next_room_ordinal = 1", ()),
        ("revision = 1", ()),
        ("next_batch_sequence = 2", ()),
        ("last_acked_sequence = 1", ()),
        ("last_acked_batch_id = 'not-null'", ()),
        ("last_acked_sha256 = X'00'", ()),
        (
            "journal_generation = ?",
            (str(JOURNAL_GENERATION),),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), "x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 31),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 0, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 0, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "next_batch_sequence = 0",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "next_batch_sequence = 2",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "last_acked_sequence = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "last_acked_batch_id = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
            ),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 1, "
            "last_acked_sha256 = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                b"y" * 32,
            ),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 0, "
            "revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, next_batch_sequence = 0",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, last_acked_sequence = -1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, last_acked_sequence = 1, "
            "last_acked_batch_id = ?, last_acked_sha256 = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
                b"y" * 32,
            ),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, last_acked_batch_id = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
            ),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 1, "
            "revision = 1, next_batch_sequence = 2, last_acked_sequence = 1, "
            "last_acked_batch_id = ?, last_acked_sha256 = ?",
            (
                str(JOURNAL_GENERATION),
                str(CONSUMER_GENERATION),
                b"x" * 32,
                str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")),
                "y" * 32,
            ),
        ),
        (
            "consumer_attach_status = 'attached', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, consumer_attached_revision = 2, "
            "revision = 1",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
        (
            "consumer_attach_status = 'attaching', journal_generation = ?, "
            "consumer_generation = ?, consumer_first_sequence = 1, "
            "baseline_rooms_sha256 = ?, revision = 1, "
            "consumer_attach_next_room_ordinal = 1, next_ready_order = 0",
            (str(JOURNAL_GENERATION), str(CONSUMER_GENERATION), b"x" * 32),
        ),
    ),
)
def test_load_owner_rejects_forged_attach_state_even_without_sql_checks(
    tmp_path: Path,
    assignments: str,
    parameters: tuple[object, ...],
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        bootstrap._journal.connection.execute("PRAGMA ignore_check_constraints = ON")
        bootstrap._journal.connection.execute(
            f"UPDATE NioIngestMeta SET {assignments} WHERE account_id = ?",
            (*parameters, ACCOUNT_ID),
        )
        with pytest.raises(JournalIntegrityError, match="consumer attach"):
            bootstrap._journal.load_owner()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("last_sequence", "last_batch_id", "last_digest", "sql_rejects"),
    (
        (1, None, b"y" * 32, True),
        (1, str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")), None, True),
        (1, str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")), b"y" * 31, True),
        (1, str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")), "y" * 32, True),
        (1, "not-a-uuid", b"y" * 32, False),
        (1.5, str(UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa")), b"y" * 32, True),
    ),
)
def test_attached_ack_identity_schema_and_decoder_are_exact(
    tmp_path: Path,
    last_sequence: object,
    last_batch_id: object,
    last_digest: object,
    sql_rejects: bool,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    statement = (
        "UPDATE NioIngestMeta SET consumer_attach_status = 'attached', "
        "journal_generation = ?, consumer_generation = ?, "
        "consumer_first_sequence = 1, baseline_rooms_sha256 = ?, "
        "consumer_attached_revision = 1, revision = 1, next_batch_sequence = 2, "
        "last_acked_sequence = ?, last_acked_batch_id = ?, last_acked_sha256 = ? "
        "WHERE account_id = ?"
    )
    parameters = (
        str(JOURNAL_GENERATION),
        str(CONSUMER_GENERATION),
        b"x" * 32,
        last_sequence,
        last_batch_id,
        last_digest,
        ACCOUNT_ID,
    )
    try:
        if sql_rejects:
            with pytest.raises(sqlite3.IntegrityError):
                bootstrap._journal.connection.execute(statement, parameters)
            bootstrap._journal.connection.execute(
                "PRAGMA ignore_check_constraints = ON"
            )
        bootstrap._journal.connection.execute(statement, parameters)
        with pytest.raises(JournalIntegrityError, match="consumer attach"):
            bootstrap._journal.load_owner()
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("room_count", "expected_chunks", "expected_yields"),
    (
        (0, [0], []),
        (1, [1], []),
        (256, [256], []),
        (257, [256, 1], [(ConsumerAttachStatus.ATTACHING, 256, 1)]),
    ),
)
async def test_attach_consumer_uses_bounded_transactions_and_yields_only_between_chunks(
    tmp_path: Path,
    room_count: int,
    expected_chunks: list[int],
    expected_yields: list[tuple[ConsumerAttachStatus, int, int]],
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    room_ids = _baseline_room_ids(room_count)
    consumer = _consumer_bootstrap(bootstrap, room_ids)
    labels: list[str] = []
    yielded_states: list[tuple[ConsumerAttachStatus, int, int]] = []
    bootstrap._journal.set_transition_statement_hook(labels.append)

    def observe_yield() -> None:
        owner = bootstrap._journal.load_owner()
        yielded_states.append(
            (
                owner.consumer_attach_status,
                owner.consumer_attach_next_room_ordinal,
                owner.revision,
            )
        )

    try:
        observer = asyncio.get_running_loop().call_soon(observe_yield)
        await bootstrap.attach_consumer(consumer)
        observer.cancel()

        owner = bootstrap._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.ATTACHED
        assert owner.consumer_attach_next_room_ordinal == room_count
        assert owner.next_ready_order == room_count
        assert owner.revision == len(expected_chunks)
        assert owner.consumer_attached_revision == len(expected_chunks)
        assert _attach_room_chunks(labels) == expected_chunks
        assert yielded_states == expected_yields
        assert (
            bootstrap._journal.connection.execute(
                "SELECT COUNT(*) FROM NioIngestReadyRecord"
            ).fetchone()[0]
            == room_count
        )
    finally:
        observer.cancel()
        bootstrap.close()


@pytest.mark.asyncio
async def test_attach_consumer_10000_rooms_has_exact_transaction_and_order_bound(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    room_ids = _baseline_room_ids(10_000)
    consumer = _consumer_bootstrap(bootstrap, room_ids)
    labels: list[str] = []
    bootstrap._journal.set_transition_statement_hook(labels.append)
    try:
        await bootstrap.attach_consumer(consumer)

        expected_chunks = [256] * 39 + [16]
        owner = bootstrap._journal.load_owner()
        assert _attach_room_chunks(labels) == expected_chunks
        assert owner.consumer_attach_status is ConsumerAttachStatus.ATTACHED
        assert owner.consumer_attach_next_room_ordinal == 10_000
        assert owner.next_ready_order == 10_000
        assert owner.revision == 40
        assert owner.consumer_attached_revision == 40
        for table in (
            "NioIngestRoomState",
            "NioIngestRoomLane",
            "NioIngestReadyRecord",
        ):
            assert (
                bootstrap._journal.connection.execute(
                    f'SELECT COUNT(*) FROM "{table}"'
                ).fetchone()[0]
                == 10_000
            )
        ready_orders = bootstrap._journal.connection.execute(
            "SELECT ready_order FROM NioIngestReadyRecord ORDER BY ready_order"
        ).fetchall()
        assert [row[0] for row in ready_orders] == list(range(10_000))
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_cancelled_attach_resumes_from_committed_chunk_boundary(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, _baseline_room_ids(257))
    task = asyncio.create_task(bootstrap.attach_consumer(consumer))
    try:
        await asyncio.sleep(0)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task

        owner = bootstrap._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.ATTACHING
        assert owner.consumer_attach_next_room_ordinal == 256
        assert owner.next_ready_order == 256
        assert owner.revision == 1
        assert (
            bootstrap._journal.connection.execute(
                "SELECT COUNT(*) FROM NioIngestRoomState"
            ).fetchone()[0]
            == 256
        )

        await bootstrap.attach_consumer(consumer)
        owner = bootstrap._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.ATTACHED
        assert owner.consumer_attach_next_room_ordinal == 257
        assert owner.next_ready_order == 257
        assert owner.revision == 2
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_reopened_attach_resumes_exact_committed_prefix(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, _baseline_room_ids(257))
    assert (
        bootstrap._journal.attach_consumer_step(consumer)
        is ConsumerAttachStatus.ATTACHING
    )
    bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    labels: list[str] = []
    reopened._journal.set_transition_statement_hook(labels.append)
    try:
        await reopened.attach_consumer(consumer)
        owner = reopened._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.ATTACHED
        assert owner.consumer_attach_next_room_ordinal == 257
        assert owner.next_ready_order == 257
        assert owner.revision == 2
        assert _attach_room_chunks(labels) == [1]
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_attaching_prefix_keeps_all_attached_operations_closed(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, _baseline_room_ids(257))
    source = bootstrap._journal.load_source()
    try:
        status = bootstrap._journal.attach_consumer_step(consumer)
        assert status is ConsumerAttachStatus.ATTACHING
        assert bootstrap._journal.load_source() == source
        statements: list[str] = []
        bootstrap._journal.connection.set_trace_callback(statements.append)

        ref = BatchRef(
            bootstrap.stream_id,
            1,
            UUID("aaaaaaaa-aaaa-5aaa-8aaa-aaaaaaaaaaaa"),
            b"x" * 32,
        )
        operations = (
            bootstrap.assert_http_enabled,
            lambda: bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(),
            ),
            bootstrap._journal.oldest_unacknowledged,
            lambda: bootstrap._journal.acknowledge(ref),
            lambda: bootstrap._journal.load_rooms(frozenset()),
            lambda: bootstrap._journal.load_ready_heads(limit=1),
            lambda: bootstrap._journal.load_frame(
                UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
            ),
            lambda: bootstrap._journal.load_lane_record(
                LaneRecordKey("!missing:example.org", 0, LaneRecordSection.HELD, 0, 0)
            ),
            lambda: bootstrap._journal.list_lane_records("!missing:example.org", 0),
            lambda: bootstrap._journal.load_network_effect(
                UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
            ),
            lambda: bootstrap._journal.list_network_effects(1),
            lambda: bootstrap._journal.list_schedulable_network_effects(1),
            lambda: bootstrap._journal.claim_membership_operation(
                UUID("dddddddd-dddd-4ddd-8ddd-dddddddddddd")
            ),
            lambda: bootstrap._journal.uncertain_membership_operations(1),
            lambda: bootstrap._journal.resolve_membership_operation(
                MembershipOperationRef(
                    UUID("eeeeeeee-eeee-4eee-8eee-eeeeeeeeeeee"),
                    "!missing:example.org",
                    0,
                    1,
                    b"x" * 32,
                ),
                MembershipOperationResolution.SUPERSEDE,
            ),
        )
        for operation in operations:
            with pytest.raises(LocalProtocolError, match="consumer is not attached"):
                operation()
        bootstrap._journal.connection.set_trace_callback(None)
        assert "BEGIN IMMEDIATE" not in statements
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("room_ids", "message"),
    (
        (("!b:example.org", "!a:example.org"), "sorted"),
        (("!a:example.org", "!a:example.org"), "duplicates"),
        (("",), "nonempty"),
    ),
)
async def test_attach_consumer_rejects_noncanonical_room_tuple_before_dml(
    tmp_path: Path,
    room_ids: tuple[str, ...],
    message: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, room_ids)
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(LocalProtocolError, match=message):
            await bootstrap.attach_consumer(consumer)
        owner = bootstrap._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.UNBOUND
        assert owner.revision == 0
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "drift",
    (
        "operation",
        "journal_generation",
        "consumer_generation",
        "first_sequence",
        "rooms",
        "digest",
    ),
)
async def test_attaching_retry_rejects_every_frozen_tuple_drift_before_dml(
    tmp_path: Path,
    drift: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, _baseline_room_ids(257))
    try:
        assert (
            bootstrap._journal.attach_consumer_step(consumer)
            is ConsumerAttachStatus.ATTACHING
        )
        before = bootstrap._journal.load_owner()
        if drift == "operation":
            candidate = replace(
                consumer,
                binding_operation_id=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
            )
        elif drift == "journal_generation":
            candidate = replace(
                consumer,
                binding=replace(
                    consumer.binding,
                    journal_generation=UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
                ),
            )
        elif drift == "consumer_generation":
            candidate = replace(
                consumer,
                binding=replace(
                    consumer.binding,
                    consumer_generation=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
                ),
            )
        elif drift == "first_sequence":
            candidate = replace(consumer, first_sequence=2)
        elif drift == "rooms":
            changed_rooms = (*consumer.baseline_room_ids[:-1], "!zzzzz:example.org")
            candidate = _consumer_bootstrap(bootstrap, changed_rooms)
        else:
            candidate = replace(consumer, baseline_sha256=b"x" * 32)

        statements: list[str] = []
        bootstrap._journal.connection.set_trace_callback(statements.append)
        with pytest.raises(LocalProtocolError):
            await bootstrap.attach_consumer(candidate)
        assert bootstrap._journal.load_owner() == before
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("status", ("attaching", "attached"))
async def test_attach_retry_requires_exact_ordinal_relation_to_supplied_tuple(
    tmp_path: Path,
    status: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    if status == "attaching":
        original = _consumer_bootstrap(bootstrap, _baseline_room_ids(257))
        assert (
            bootstrap._journal.attach_consumer_step(original)
            is ConsumerAttachStatus.ATTACHING
        )
        candidate = _consumer_bootstrap(bootstrap, _baseline_room_ids(256))
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestMeta SET baseline_rooms_sha256 = ? "
            "WHERE account_id = ?",
            (candidate.baseline_sha256, ACCOUNT_ID),
        )
    else:
        candidate = _consumer_bootstrap(bootstrap, _baseline_room_ids(1))
        await bootstrap.attach_consumer(candidate)
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestMeta SET consumer_attach_next_room_ordinal = 0 "
            "WHERE account_id = ?",
            (ACCOUNT_ID,),
        )
    bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    statements: list[str] = []
    reopened._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(LocalProtocolError, match="ordinal"):
            await reopened.attach_consumer(candidate)
        with pytest.raises(LocalProtocolError, match="not attached|not validated"):
            reopened.assert_http_enabled()
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_attach_consumer_installs_priority_baseline_loss_plan_atomically(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
            bootstrap._journal.load_network_effect(
                UUID("cccccccc-cccc-4ccc-8ccc-cccccccccccc")
            )
        with pytest.raises(LocalProtocolError, match="consumer is not attached"):
            bootstrap._journal.list_network_effects(1)
        with pytest.raises(LocalProtocolError, match="consumer is not attached"):
            bootstrap._journal.list_schedulable_network_effects(1)
        with pytest.raises(LocalProtocolError, match="consumer is not attached"):
            bootstrap.assert_http_enabled()

        await bootstrap.attach_consumer(consumer)
        bootstrap.assert_http_enabled()

        owner = bootstrap._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.ATTACHED
        assert owner.consumer_attach_next_room_ordinal == len(room_ids)
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
        ready_owner_rows = bootstrap._journal.connection.execute(
            "SELECT item_id, item_kind FROM NioIngestReadyRecord "
            "ORDER BY ready_order"
        ).fetchall()
        assert [(row[0], row[1]) for row in ready_owner_rows] == [
            (ready_row.record.loss_id, "loss") for ready_row in ready
        ]

        revision = owner.revision
        statements: list[str] = []
        bootstrap._journal.connection.set_trace_callback(statements.append)
        await bootstrap.attach_consumer(consumer)
        bootstrap._journal.connection.set_trace_callback(None)
        assert bootstrap._journal.load_owner().revision == revision
        assert bootstrap._journal.load_ready_heads(limit=10) == ready
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
        assert not any(
            table in statement
            for table in (
                "NioIngestRoomState",
                "NioIngestRoomLane",
                "NioIngestReadyRecord",
            )
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_attach_consumer_rejects_digest_operation_sequence_and_retry_drift(
    tmp_path: Path,
) -> None:
    room_ids = ("!alpha:example.org",)
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
async def test_exact_attach_retry_is_metadata_only_even_if_baseline_loss_is_corrupt(
    tmp_path: Path,
) -> None:
    room_ids = ("!alpha:example.org",)
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, room_ids)
    await bootstrap.attach_consumer(consumer)
    row = bootstrap._journal.connection.execute(
        "SELECT item_id, payload_ciphertext FROM NioIngestReadyRecord "
        "WHERE account_id = ?",
        (ACCOUNT_ID,),
    ).fetchone()
    tampered = bytearray(row["payload_ciphertext"])
    tampered[-1] ^= 1
    bootstrap._journal.connection.execute(
        "UPDATE NioIngestReadyRecord SET payload_ciphertext = ? "
        "WHERE account_id = ? AND item_id = ?",
        (bytes(tampered), ACCOUNT_ID, row["item_id"]),
    )
    bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    statements: list[str] = []
    reopened._journal.connection.set_trace_callback(statements.append)
    try:
        await reopened.attach_consumer(consumer)
        reopened._journal.connection.set_trace_callback(None)
        reopened.assert_http_enabled()
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
        assert not any("NioIngestReadyRecord" in statement for statement in statements)
        with pytest.raises(JournalIntegrityError, match="authentication"):
            reopened._journal.load_ready_heads(limit=1)
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_exact_attach_retry_survives_batch_frontier_advance_and_restart(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, ("!baseline:example.org",))
    await bootstrap.attach_consumer(consumer)
    ready = bootstrap._journal.load_ready_heads(limit=1)[0]
    assert type(ready.record) is LossRecord
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=consumer.first_sequence,
        created_revision=2,
        records=(ready.record,),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            batch_materialization=BatchMaterialization(
                batch,
                (ReadyRecordKey(ready.record.loss_id),),
            )
        ),
    )
    assert bootstrap.next_batch_sequence == consumer.first_sequence + 1
    assert bootstrap._journal.acknowledge(batch.ref) is AckOutcome.ACKNOWLEDGED
    assert bootstrap._journal.load_owner().consumer_first_sequence == 1
    bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
        owner = reopened._journal.load_owner()
        assert owner.revision == 3
        assert owner.consumer_attached_revision == 1
        assert owner.consumer_first_sequence == consumer.first_sequence
        assert owner.next_batch_sequence == consumer.first_sequence + 1

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
        source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
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


def test_lock_path_replacement_takeover_fences_every_stale_handle_before_sql(
    tmp_path: Path,
) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name=database_path.name,
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    store.save_account(OlmAccount())
    _replace_lock_path(database_path)

    second = None
    try:
        second = open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name=database_path.name,
        )
        statements: list[str] = []
        store.database.connection().set_trace_callback(statements.append)

        with pytest.raises(LocalProtocolError, match="lock file identity"):
            bootstrap._journal.load_owner()
        for operation in (
            store.load_account,
            lambda: store.save_account(OlmAccount()),
            store.upgrade_to_v2,
        ):
            with pytest.raises(LocalProtocolError, match="lock file identity"):
                operation()
        assert statements == []
    finally:
        bootstrap.close()
        if second is not None:
            second.close()


@pytest.mark.parametrize("operation", ("load", "save", "upgrade"))
def test_stale_e2ee_writer_epoch_is_cas_fenced_before_store_sql(
    tmp_path: Path,
    operation: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    store.save_account(OlmAccount())
    bootstrap._journal.connection.execute(
        "UPDATE NioIngestMeta SET writer_epoch = ? WHERE account_id = ?",
        ("ffffffff-ffff-ffff-ffff-ffffffffffff", ACCOUNT_ID),
    )
    statements: list[str] = []
    store.database.connection().set_trace_callback(statements.append)
    operations = {
        "load": store.load_account,
        "save": lambda: store.save_account(OlmAccount()),
        "upgrade": store.upgrade_to_v2,
    }

    try:
        with pytest.raises(LocalProtocolError, match="writer_epoch"):
            operations[operation]()
    finally:
        store.database.connection().set_trace_callback(None)
        bootstrap.close()

    normalized = tuple(" ".join(statement.split()).upper() for statement in statements)
    assert any(
        statement.startswith("UPDATE NIOINGESTMETA SET WRITER_EPOCH = WRITER_EPOCH")
        for statement in normalized
    )
    assert all(
        statement in {"BEGIN IMMEDIATE", "ROLLBACK"} or "NIOINGESTMETA" in statement
        for statement in normalized
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


def _baseline(
    room_id: str,
    membership_epoch: int,
    *,
    membership_event_id: str = "$member",
    prev_batch: str = "old-prev",
) -> MembershipBaseline:
    return MembershipBaseline(
        room_id,
        1,
        membership_epoch,
        prev_batch,
        membership_event_id,
    )


def _gap(
    room_id: str,
    membership_epoch: int,
    *,
    membership_event_id: str = "$member",
    start_token: str = "old-prev",
) -> RecoveryGap:
    return RecoveryGap(
        UUID("44444444-4444-4444-4444-444444444444"),
        room_id,
        4,
        membership_epoch,
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 2),
        membership_event_id,
        start_token,
        "new-prev",
        "cursor-1",
        (start_token, "cursor-1"),
        1,
        8,
        None,
    )


@pytest.mark.asyncio
async def test_room_state_round_trips_active_lane_and_retiring_epoch_chain(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
        membership_baseline=_baseline(room_id, 3),
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
            release_phase=ReleasePhase.RECOVERING,
            recovery_gap=_gap(room_id, 3),
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
        source=CLASSIC_SOURCE,
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


def _lane_event(
    record_id: str,
    room_id: str,
    membership_epoch: int,
    room_sequence: int,
    provenance: TimelineEventProvenance,
) -> EventRecord:
    return EventRecord(
        record_id,
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 4, 9, room_sequence),
        room_id,
        membership_epoch,
        room_sequence,
        f"${record_id}",
        provenance,
        f'{{"record":"{record_id}"}}'.encode(),
        None,
    )


def _lane_record(
    key: LaneRecordKey,
    record: EventRecord | LossRecord,
    *,
    source_frame_id: UUID | None = None,
    source_effect_id: UUID | None = None,
    canonical_bytes: int | None = None,
) -> LaneRecord:
    return LaneRecord(
        key,
        record,
        source_frame_id,
        source_effect_id,
        (
            len(_canonical_json(_record_to_dict(record)))
            if canonical_bytes is None
            else canonical_bytes
        ),
    )


def _terminal_loss(stream_id: UUID, room_id: str, membership_epoch: int) -> LossRecord:
    incomplete = LossRecord(
        "",
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 4),
        room_id,
        membership_epoch,
        LossReason.FETCH_FAILED,
        LossBoundary("$prior", 1_700_000_000_000, "old-prev", "new-prev"),
        b'{"errcode":"M_FORBIDDEN"}',
    )
    return replace(incomplete, loss_id=_loss_id(stream_id, incomplete))


def _room_carrier_transition(
    room_id: str,
    membership_epoch: int,
    *,
    lane_records: tuple[LaneRecord, ...] = (),
) -> JournalTransition:
    held_records = tuple(
        record
        for record in lane_records
        if record.key.section is LaneRecordSection.HELD
    )
    return JournalTransition(
        room_states=(
            RoomState(
                room_id,
                membership_epoch,
                8,
                RoomHydrationStatus.READY,
                _snapshot(room_id, membership_epoch),
                _baseline(room_id, membership_epoch),
            ),
        ),
        room_lanes=(
            RoomLane(
                room_id,
                membership_epoch,
                LaneStatus.ACTIVE,
                held_record_count=len(held_records),
                held_canonical_bytes=sum(
                    record.canonical_bytes for record in held_records
                ),
                release_phase=ReleasePhase.RECOVERING,
                next_held_ordinal=len(held_records),
                recovery_gap=_gap(room_id, membership_epoch),
            ),
        ),
        lane_record_inserts=lane_records,
    )


@pytest.mark.asyncio
async def test_empty_room_carriers_are_always_authenticated_after_attach(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    room_id = "!baseline:example.org"
    try:
        await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap, (room_id,)))
        state_row = bootstrap._journal.connection.execute(
            "SELECT state_ciphertext, state_sha256 FROM NioIngestRoomState "
            "WHERE account_id = ? AND room_id = ?",
            (ACCOUNT_ID, room_id),
        ).fetchone()
        lane_row = bootstrap._journal.connection.execute(
            "SELECT lane_state_ciphertext, lane_state_sha256 "
            "FROM NioIngestRoomLane WHERE account_id = ? AND room_id = ?",
            (ACCOUNT_ID, room_id),
        ).fetchone()

        assert all(value is not None for value in state_row)
        assert all(value is not None for value in lane_row)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_lane_records_round_trip_and_materialize_after_restart(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    room_id = "!room:example.org"
    epoch = 3
    effect_id = UUID("55555555-5555-5555-5555-555555555555")
    frame_id = UUID("66666666-6666-6666-6666-666666666666")
    loss = _terminal_loss(bootstrap.stream_id, room_id, epoch)
    recovered = _lane_event(
        "recovered", room_id, epoch, 6, TimelineEventProvenance.RECOVERED
    )
    held = _lane_event("held", room_id, epoch, 7, TimelineEventProvenance.LIVE)
    records = (
        _lane_record(
            LaneRecordKey(room_id, epoch, LaneRecordSection.HELD, 0, 0),
            held,
            source_frame_id=frame_id,
        ),
        _lane_record(
            LaneRecordKey(room_id, epoch, LaneRecordSection.RECOVERED, 1, 0),
            recovered,
            source_effect_id=effect_id,
        ),
        _lane_record(
            LaneRecordKey(room_id, epoch, LaneRecordSection.LOSS, 0, 0),
            loss,
            source_effect_id=effect_id,
        ),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=_room_carrier_transition(
            room_id,
            epoch,
            lane_records=records,
        ),
    )
    bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        expected = (records[2], records[1], records[0])
        assert reopened._journal.list_lane_records(room_id, epoch) == expected
        assert (
            tuple(reopened._journal.load_lane_record(record.key) for record in expected)
            == expected
        )
        assert reopened._journal.list_lane_records(
            room_id,
            epoch,
            LaneRecordSection.RECOVERED,
        ) == (records[1],)

        batch = batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=reopened.stream_id,
            sequence=1,
            created_revision=3,
            records=(records[1].record,),
        )
        reopened._journal.commit(
            expected_revision=2,
            writer_epoch=reopened._journal.writer_epoch,
            transition=JournalTransition(
                batch_materialization=BatchMaterialization(
                    batch,
                    (records[1].key,),
                ),
            ),
        )
        assert reopened._journal.load_lane_record(records[1].key) is None
        assert reopened._journal.list_lane_records(room_id, epoch) == (
            records[2],
            records[0],
        )
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_active_gap_accepts_linked_membership_event_rotation(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_id = "!room:example.org"
    transition = _room_carrier_transition(room_id, 3)
    rotated_gap = _gap(room_id, 3, membership_event_id="$linked-rotation")
    transition = replace(
        transition,
        room_lanes=(replace(transition.room_lanes[0], recovery_gap=rotated_gap),),
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=transition,
        )
        aggregate = bootstrap._journal.load_rooms(frozenset({room_id}))[room_id]
        assert aggregate.state.membership_baseline == _baseline(room_id, 3)
        assert aggregate.active_lane.recovery_gap == rotated_gap
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_held_lane_record_deltas_reconcile_exact_counters_and_never_rewind(
    tmp_path: Path,
) -> None:
    bootstrap, room_id, epoch, existing = await _journal_with_carriers(tmp_path)
    inserted = _lane_record(
        LaneRecordKey(room_id, epoch, LaneRecordSection.HELD, 0, 1),
        _lane_event("next-held", room_id, epoch, 8, TimelineEventProvenance.LIVE),
        source_frame_id=UUID("77777777-7777-7777-7777-777777777777"),
    )
    lane = bootstrap._journal.load_rooms(frozenset({room_id}))[room_id].active_lane
    try:
        with_insert = replace(
            lane,
            held_record_count=2,
            held_canonical_bytes=(existing.canonical_bytes + inserted.canonical_bytes),
            next_held_ordinal=2,
        )
        bootstrap._journal.commit(
            expected_revision=2,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                room_lanes=(with_insert,),
                lane_record_inserts=(inserted,),
            ),
        )
        after_delete = replace(
            with_insert,
            held_record_count=1,
            held_canonical_bytes=inserted.canonical_bytes,
        )
        batch = batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=bootstrap._journal.load_owner().binding,
            stream_id=bootstrap.stream_id,
            sequence=1,
            created_revision=4,
            records=(existing.record,),
        )
        bootstrap._journal.commit(
            expected_revision=3,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                room_lanes=(after_delete,),
                batch_materialization=BatchMaterialization(
                    batch,
                    (existing.key,),
                ),
            ),
        )

        aggregate = bootstrap._journal.load_rooms(frozenset({room_id}))[room_id]
        assert aggregate.active_lane == after_delete
        assert aggregate.active_lane.next_held_ordinal == 2
        assert bootstrap._journal.list_lane_records(room_id, epoch) == (inserted,)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    (
        "missing_lane_update",
        "wrong_count",
        "wrong_bytes",
        "ordinal_gap",
        "counter_without_delta",
        "delete_wrong_count",
    ),
)
async def test_held_lane_record_deltas_reject_counter_or_ordinal_drift_before_cas(
    tmp_path: Path,
    case: str,
) -> None:
    bootstrap, room_id, epoch, existing = await _journal_with_carriers(tmp_path)
    lane = bootstrap._journal.load_rooms(frozenset({room_id}))[room_id].active_lane
    ordinal = 2 if case == "ordinal_gap" else 1
    inserted = _lane_record(
        LaneRecordKey(room_id, epoch, LaneRecordSection.HELD, 0, ordinal),
        _lane_event("candidate", room_id, epoch, 8, TimelineEventProvenance.LIVE),
        source_frame_id=UUID("77777777-7777-7777-7777-777777777777"),
    )
    expected_lane = replace(
        lane,
        held_record_count=2,
        held_canonical_bytes=existing.canonical_bytes + inserted.canonical_bytes,
        next_held_ordinal=2,
    )
    inserts = (inserted,)
    materialization = None
    lanes = (expected_lane,)
    if case == "missing_lane_update":
        lanes = ()
    elif case == "wrong_count":
        lanes = (replace(expected_lane, held_record_count=1),)
    elif case == "wrong_bytes":
        lanes = (replace(expected_lane, held_canonical_bytes=existing.canonical_bytes),)
    elif case == "ordinal_gap":
        lanes = (replace(expected_lane, next_held_ordinal=3),)
    elif case == "counter_without_delta":
        inserts = ()
        lanes = (replace(lane, next_held_ordinal=2),)
    elif case == "delete_wrong_count":
        inserts = ()
        lanes = (lane,)
        owner = bootstrap._journal.load_owner()
        assert owner.binding is not None
        batch = batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=owner.binding,
            stream_id=bootstrap.stream_id,
            sequence=1,
            created_revision=3,
            records=(existing.record,),
        )
        materialization = BatchMaterialization(batch, (existing.key,))

    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="HELD lane"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    room_lanes=lanes,
                    lane_record_inserts=inserts,
                    batch_materialization=materialization,
                ),
            )
        assert bootstrap._journal.load_owner().revision == 2
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("part", ("ciphertext", "digest", "metadata"))
async def test_lane_record_delete_authenticates_target_before_meta_cas(
    tmp_path: Path,
    part: str,
) -> None:
    bootstrap, room_id, _, existing = await _journal_with_carriers(tmp_path)
    lane = bootstrap._journal.load_rooms(frozenset({room_id}))[room_id].active_lane
    after_delete = replace(
        lane,
        held_record_count=0,
        held_canonical_bytes=0,
    )
    owner = bootstrap._journal.load_owner()
    assert owner.binding is not None
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=owner.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=3,
        records=(existing.record,),
    )
    if part == "metadata":
        statement = (
            "UPDATE NioIngestLaneRecord SET canonical_bytes = 999 "
            "WHERE account_id = ? AND item_id = ?"
        )
        parameters: tuple[object, ...] = (ACCOUNT_ID, existing.record.record_id)
    else:
        column = "payload_ciphertext" if part == "ciphertext" else "payload_sha256"
        value = bootstrap._journal.connection.execute(
            f"SELECT {column} FROM NioIngestLaneRecord "
            "WHERE account_id = ? AND item_id = ?",
            (ACCOUNT_ID, existing.record.record_id),
        ).fetchone()[0]
        changed = bytes(value[:-1]) + bytes((value[-1] ^ 1,))
        statement = (
            f"UPDATE NioIngestLaneRecord SET {column} = ? "
            "WHERE account_id = ? AND item_id = ?"
        )
        parameters = (changed, ACCOUNT_ID, existing.record.record_id)
    bootstrap._journal.connection.execute(statement, parameters)
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    room_lanes=(after_delete,),
                    batch_materialization=BatchMaterialization(
                        batch,
                        (existing.key,),
                    ),
                ),
            )
        assert bootstrap._journal.load_owner().revision == 2
        assert (
            bootstrap._journal.connection.execute(
                "SELECT count(*) FROM NioIngestLaneRecord "
                "WHERE account_id = ? AND item_id = ?",
                (ACCOUNT_ID, existing.record.record_id),
            ).fetchone()[0]
            == 1
        )
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_commit_revalidates_room_lane_ready_head_before_meta_cas(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_id = "!room:example.org"
    lane = RoomLane(room_id, 1, LaneStatus.ACTIVE)
    object.__setattr__(lane, "ready_order", 0)
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="ready_order"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    room_states=(
                        RoomState(
                            room_id,
                            1,
                            1,
                            RoomHydrationStatus.READY,
                            _snapshot(room_id, 1),
                        ),
                    ),
                    room_lanes=(lane,),
                ),
            )
        assert bootstrap._journal.load_owner().revision == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("case", "message"),
    (
        ("active_gap_without_baseline", "baseline"),
        ("gap_token_mismatch", "start token"),
    ),
)
async def test_room_aggregate_rejects_cross_carrier_counterexamples_before_write(
    tmp_path: Path,
    case: str,
    message: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_id = "!room:example.org"
    state = RoomState(
        room_id,
        1,
        1,
        RoomHydrationStatus.READY,
        _snapshot(room_id, 1),
        _baseline(room_id, 1),
    )
    lanes = (
        RoomLane(
            room_id,
            1,
            LaneStatus.ACTIVE,
            release_phase=ReleasePhase.RECOVERING,
            recovery_gap=_gap(room_id, 1),
        ),
    )
    if case == "active_gap_without_baseline":
        state = replace(state, membership_baseline=None)
    else:
        lanes = (replace(lanes[0], recovery_gap=_gap(room_id, 1, start_token="other")),)

    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match=message):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(room_states=(state,), room_lanes=lanes),
            )
        assert bootstrap._journal.load_owner().revision == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("owner_transport", tuple(TransportKind))
@pytest.mark.parametrize("carrier", ("gap", "lifecycle", "lane_record"))
async def test_room_carriers_reject_foreign_transport_before_meta_cas(
    tmp_path: Path,
    owner_transport: TransportKind,
    carrier: str,
) -> None:
    source = (
        CLASSIC_SOURCE
        if owner_transport is TransportKind.CLASSIC
        else _sliding_source_config()
    )
    foreign = (
        TransportKind.SLIDING
        if owner_transport is TransportKind.CLASSIC
        else TransportKind.CLASSIC
    )
    bootstrap = open_ingestion_store(
        tmp_path,
        source=source,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_id = "!room:example.org"
    if carrier == "gap":
        gap = replace(
            _gap(room_id, 1),
            origin=RecordOrigin(foreign, 4, 9, 2),
        )
        transition = JournalTransition(
            room_states=(
                RoomState(
                    room_id,
                    1,
                    1,
                    RoomHydrationStatus.READY,
                    _snapshot(room_id, 1),
                    _baseline(room_id, 1),
                ),
            ),
            room_lanes=(
                RoomLane(
                    room_id,
                    1,
                    LaneStatus.ACTIVE,
                    release_phase=ReleasePhase.RECOVERING,
                    recovery_gap=gap,
                ),
            ),
        )
    elif carrier == "lifecycle":
        lifecycle = replace(
            _lifecycle(room_id, 2, 0),
            origin=RecordOrigin(foreign, 1, 1, 0),
        )
        transition = JournalTransition(
            room_states=(
                RoomState(
                    room_id,
                    2,
                    1,
                    RoomHydrationStatus.READY,
                    _snapshot(room_id, 2),
                    _baseline(room_id, 2),
                ),
            ),
            room_lanes=(
                RoomLane(
                    room_id,
                    1,
                    LaneStatus.RETIRING,
                    successor_membership_epoch=2,
                    pending_lifecycle=lifecycle,
                ),
                RoomLane(room_id, 2, LaneStatus.ACTIVE),
            ),
        )
    else:
        event = replace(
            _lane_event("foreign", room_id, 1, 1, TimelineEventProvenance.LIVE),
            origin=RecordOrigin(foreign, 4, 9, 1),
        )
        lane_record = _lane_record(
            LaneRecordKey(room_id, 1, LaneRecordSection.HELD, 0, 0),
            event,
            source_frame_id=UUID("66666666-6666-6666-6666-666666666666"),
        )
        transition = JournalTransition(
            room_states=(
                RoomState(
                    room_id,
                    1,
                    1,
                    RoomHydrationStatus.READY,
                    _snapshot(room_id, 1),
                ),
            ),
            room_lanes=(RoomLane(room_id, 1, LaneStatus.ACTIVE),),
            lane_record_inserts=(lane_record,),
        )

    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="transport"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=transition,
            )
        assert bootstrap._journal.load_owner().revision == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("carrier", ("gap", "lifecycle", "lane_record"))
async def test_authenticated_room_carriers_reject_foreign_transport_after_restart(
    tmp_path: Path,
    carrier: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    room_id = "!room:example.org"
    foreign_origin = RecordOrigin(TransportKind.SLIDING, 4, 9, 2)
    lane_record = None
    if carrier == "gap":
        transition = _room_carrier_transition(room_id, 1)
        lane = replace(
            transition.room_lanes[0],
            recovery_gap=replace(
                transition.room_lanes[0].recovery_gap,
                origin=foreign_origin,
            ),
        )
    elif carrier == "lifecycle":
        lifecycle = _lifecycle(room_id, 2, 0)
        transition = JournalTransition(
            room_states=(
                RoomState(
                    room_id,
                    2,
                    1,
                    RoomHydrationStatus.READY,
                    _snapshot(room_id, 2),
                    _baseline(room_id, 2),
                ),
            ),
            room_lanes=(
                RoomLane(
                    room_id,
                    1,
                    LaneStatus.RETIRING,
                    successor_membership_epoch=2,
                    pending_lifecycle=lifecycle,
                ),
                RoomLane(room_id, 2, LaneStatus.ACTIVE),
            ),
        )
        lane = replace(
            transition.room_lanes[0],
            pending_lifecycle=replace(lifecycle, origin=foreign_origin),
        )
    else:
        event = _lane_event(
            "transport-forge",
            room_id,
            1,
            1,
            TimelineEventProvenance.LIVE,
        )
        lane_record = _lane_record(
            LaneRecordKey(room_id, 1, LaneRecordSection.HELD, 0, 0),
            event,
            source_frame_id=UUID("66666666-6666-6666-6666-666666666666"),
        )
        transition = JournalTransition(
            room_states=(
                RoomState(
                    room_id,
                    1,
                    1,
                    RoomHydrationStatus.READY,
                    _snapshot(room_id, 1),
                ),
            ),
            room_lanes=(
                RoomLane(
                    room_id,
                    1,
                    LaneStatus.ACTIVE,
                    held_record_count=1,
                    held_canonical_bytes=lane_record.canonical_bytes,
                    next_held_ordinal=1,
                ),
            ),
            lane_record_inserts=(lane_record,),
        )
        lane = transition.room_lanes[0]

    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=transition,
    )
    if carrier == "lane_record":
        assert lane_record is not None
        forged_record = replace(
            lane_record,
            record=replace(lane_record.record, origin=foreign_origin),
        )
        payload = bootstrap._journal._lane_record_payload(
            forged_record,
            forged_record.record.record_id,
            "event",
            2,
        )
        ciphertext, digest = bootstrap._journal._codec.seal(
            "NioIngestLaneRecord",
            bootstrap._journal._lane_record_primary_key(forged_record.key),
            payload,
        )
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestLaneRecord SET payload_ciphertext = ?, "
            "payload_sha256 = ? WHERE account_id = ? AND item_id = ?",
            (ciphertext, digest, ACCOUNT_ID, forged_record.record.record_id),
        )
    else:
        payload = bootstrap._journal._room_lane_payload(lane, 2)
        ciphertext, digest = bootstrap._journal._codec.seal(
            "NioIngestRoomLane",
            (lane.room_id, lane.membership_epoch),
            payload,
        )
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestRoomLane SET lane_state_ciphertext = ?, "
            "lane_state_sha256 = ? WHERE account_id = ? AND room_id = ? "
            "AND membership_epoch = ?",
            (
                ciphertext,
                digest,
                ACCOUNT_ID,
                lane.room_id,
                lane.membership_epoch,
            ),
        )
    bootstrap.close()

    reopened = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        with pytest.raises(JournalIntegrityError, match="transport"):
            if lane_record is None:
                reopened._journal.load_rooms(frozenset({room_id}))
            else:
                reopened._journal.load_lane_record(lane_record.key)
    finally:
        reopened.close()


@pytest.mark.asyncio
async def test_lane_record_decode_wraps_authenticated_uuid_shape_errors(
    tmp_path: Path,
) -> None:
    bootstrap, _, _, lane_record = await _journal_with_carriers(tmp_path)
    payload = bootstrap._journal._lane_record_payload(
        lane_record,
        lane_record.record.record_id,
        "event",
        2,
    )
    envelope = json.loads(payload)
    envelope["source_frame_id"] = 7
    forged_payload = _canonical_json(envelope)
    ciphertext, digest = bootstrap._journal._codec.seal(
        "NioIngestLaneRecord",
        bootstrap._journal._lane_record_primary_key(lane_record.key),
        forged_payload,
    )
    bootstrap._journal.connection.execute(
        "UPDATE NioIngestLaneRecord SET payload_ciphertext = ?, "
        "payload_sha256 = ? WHERE account_id = ? AND item_id = ?",
        (ciphertext, digest, ACCOUNT_ID, lane_record.record.record_id),
    )
    try:
        with pytest.raises(JournalIntegrityError, match="authenticated envelope"):
            bootstrap._journal.load_lane_record(lane_record.key)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_lane_record_decode_rejects_transport_loss_without_source_owner(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_id = "!room:example.org"
    loss = _terminal_loss(bootstrap.stream_id, room_id, 1)
    lane_record = _lane_record(
        LaneRecordKey(room_id, 1, LaneRecordSection.LOSS, 0, 0),
        loss,
        source_effect_id=UUID("55555555-5555-5555-5555-555555555555"),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=_room_carrier_transition(
            room_id,
            1,
            lane_records=(lane_record,),
        ),
    )
    payload = bootstrap._journal._lane_record_payload(
        lane_record,
        loss.loss_id,
        "loss",
        2,
    )
    envelope = json.loads(payload)
    envelope["source_effect_id"] = None
    forged_payload = _canonical_json(envelope)
    ciphertext, digest = bootstrap._journal._codec.seal(
        "NioIngestLaneRecord",
        bootstrap._journal._lane_record_primary_key(lane_record.key),
        forged_payload,
    )
    bootstrap._journal.connection.execute(
        "UPDATE NioIngestLaneRecord SET source_effect_id = NULL, "
        "payload_ciphertext = ?, payload_sha256 = ? "
        "WHERE account_id = ? AND item_id = ?",
        (ciphertext, digest, ACCOUNT_ID, loss.loss_id),
    )
    try:
        with pytest.raises(JournalIntegrityError, match="authenticated envelope"):
            bootstrap._journal.load_lane_record(lane_record.key)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_lane_record_insert_revalidates_key_item_identity_and_exact_bytes(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_id = "!room:example.org"
    epoch = 3
    frame_id = UUID("66666666-6666-6666-6666-666666666666")
    event = _lane_event("held", room_id, epoch, 7, TimelineEventProvenance.LIVE)
    existing = _lane_record(
        LaneRecordKey(room_id, epoch, LaneRecordSection.HELD, 0, 0),
        event,
        source_frame_id=frame_id,
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=_room_carrier_transition(
            room_id,
            epoch,
            lane_records=(existing,),
        ),
    )
    bootstrap._journal.commit(
        expected_revision=2,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(lane_record_inserts=(existing,)),
    )

    changed_event = _lane_event(
        "changed", room_id, epoch, 8, TimelineEventProvenance.LIVE
    )
    collisions = (
        _lane_record(existing.key, changed_event, source_frame_id=frame_id),
        _lane_record(
            replace(existing.key, record_ordinal=1),
            event,
            source_frame_id=frame_id,
        ),
        _lane_record(
            replace(existing.key, record_ordinal=2),
            _lane_event("wrong-size", room_id, epoch, 9, TimelineEventProvenance.LIVE),
            source_frame_id=frame_id,
            canonical_bytes=existing.canonical_bytes,
        ),
    )
    try:
        for collision in collisions:
            with pytest.raises(JournalIntegrityError, match="lane record"):
                bootstrap._journal.commit(
                    expected_revision=3,
                    writer_epoch=bootstrap._journal.writer_epoch,
                    transition=JournalTransition(
                        lane_record_inserts=(collision,),
                    ),
                )
            assert bootstrap._journal.load_owner().revision == 3
            assert bootstrap._journal.load_lane_record(existing.key) == existing

    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "case",
    (
        "missing_room",
        "missing_epoch",
        "duplicate_insert",
        "duplicate_item",
    ),
)
async def test_lane_record_transition_rejects_invalid_key_sets_before_write(
    tmp_path: Path,
    case: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_id = "!room:example.org"
    epoch = 3
    frame_id = UUID("66666666-6666-6666-6666-666666666666")
    existing = _lane_record(
        LaneRecordKey(room_id, epoch, LaneRecordSection.HELD, 0, 0),
        _lane_event("existing", room_id, epoch, 7, TimelineEventProvenance.LIVE),
        source_frame_id=frame_id,
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=_room_carrier_transition(
            room_id,
            epoch,
            lane_records=(existing,),
        ),
    )
    candidate_room = "!missing:example.org" if case == "missing_room" else room_id
    candidate_epoch = 4 if case == "missing_epoch" else epoch
    candidate = _lane_record(
        LaneRecordKey(
            candidate_room,
            candidate_epoch,
            LaneRecordSection.HELD,
            0,
            1,
        ),
        _lane_event(
            "candidate",
            candidate_room,
            candidate_epoch,
            8,
            TimelineEventProvenance.LIVE,
        ),
        source_frame_id=frame_id,
    )
    if case in {"missing_room", "missing_epoch"}:
        transition = JournalTransition(lane_record_inserts=(candidate,))
    elif case == "duplicate_insert":
        transition = JournalTransition(lane_record_inserts=(candidate, candidate))
    elif case == "duplicate_item":
        transition = JournalTransition(
            lane_record_inserts=(
                candidate,
                replace(candidate, key=replace(candidate.key, record_ordinal=2)),
            ),
        )
    else:
        transition = JournalTransition(
            lane_record_inserts=(
                candidate,
                replace(candidate, key=replace(candidate.key, record_ordinal=2)),
            ),
        )

    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="lane record"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=transition,
            )
        assert bootstrap._journal.load_owner().revision == 2
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


async def _journal_with_carriers(tmp_path: Path):
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    room_id = "!room:example.org"
    epoch = 3
    event = _lane_event("held", room_id, epoch, 7, TimelineEventProvenance.LIVE)
    lane_record = _lane_record(
        LaneRecordKey(room_id, epoch, LaneRecordSection.HELD, 0, 0),
        event,
        source_frame_id=UUID("66666666-6666-6666-6666-666666666666"),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=_room_carrier_transition(
            room_id,
            epoch,
            lane_records=(lane_record,),
        ),
    )
    return bootstrap, room_id, epoch, lane_record


@pytest.mark.asyncio
@pytest.mark.parametrize("target", ("room_state", "room_lane", "lane_record"))
@pytest.mark.parametrize("part", ("ciphertext", "sha256"))
async def test_carrier_payload_or_digest_tamper_fails_before_any_revision_mutation(
    tmp_path: Path,
    target: str,
    part: str,
) -> None:
    bootstrap, room_id, epoch, lane_record = await _journal_with_carriers(tmp_path)
    locations = {
        "room_state": (
            "NioIngestRoomState",
            "state",
            "room_id = ?",
            (room_id,),
        ),
        "room_lane": (
            "NioIngestRoomLane",
            "lane_state",
            "room_id = ? AND membership_epoch = ?",
            (room_id, epoch),
        ),
        "lane_record": (
            "NioIngestLaneRecord",
            "payload",
            "room_id = ? AND membership_epoch = ? AND section = ? "
            "AND page_ordinal = ? AND record_ordinal = ?",
            (
                lane_record.key.room_id,
                lane_record.key.membership_epoch,
                lane_record.key.section.value,
                lane_record.key.page_ordinal,
                lane_record.key.record_ordinal,
            ),
        ),
    }
    table, prefix, where, parameters = locations[target]
    column = f"{prefix}_{part}"
    row = bootstrap._journal.connection.execute(
        f'SELECT "{column}" FROM "{table}" WHERE account_id = ? AND {where}',
        (ACCOUNT_ID, *parameters),
    ).fetchone()
    value = bytes(row[0])
    changed = value[:-1] + bytes((value[-1] ^ 1,))
    bootstrap._journal.connection.execute(
        f'UPDATE "{table}" SET "{column}" = ? WHERE account_id = ? AND {where}',
        (changed, ACCOUNT_ID, *parameters),
    )
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError):
            if target == "lane_record":
                bootstrap._journal.load_lane_record(lane_record.key)
            else:
                bootstrap._journal.load_rooms(frozenset({room_id}))
        assert bootstrap._journal.load_owner().revision == 2
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("target", "column", "forged"),
    (
        ("room_state", "room_id", "!forged:example.org"),
        ("room_state", "current_membership_epoch", 4),
        ("room_state", "next_room_sequence", 9),
        ("room_state", "hydration_status", "unavailable"),
        ("room_state", "updated_revision", 99),
        ("room_lane", "room_id", "!forged:example.org"),
        ("room_lane", "membership_epoch", 4),
        ("room_lane", "lane_status", "retiring"),
        ("room_lane", "held_record_count", 2),
        ("room_lane", "held_canonical_bytes", 1),
        ("room_lane", "release_phase", "releasing_recovered"),
        ("room_lane", "ready_order", 42),
        ("room_lane", "next_held_ordinal", 2),
        ("room_lane", "successor_membership_epoch", 4),
        ("room_lane", "updated_revision", 99),
        ("lane_record", "room_id", "!forged:example.org"),
        ("lane_record", "membership_epoch", 4),
        ("lane_record", "section", "recovered"),
        ("lane_record", "page_ordinal", 2),
        ("lane_record", "record_ordinal", 1),
        ("lane_record", "item_id", "$forged"),
        ("lane_record", "item_kind", "loss"),
        (
            "lane_record",
            "source_frame_id",
            "77777777-7777-7777-7777-777777777777",
        ),
        (
            "lane_record",
            "source_effect_id",
            "88888888-8888-8888-8888-888888888888",
        ),
        ("lane_record", "canonical_bytes", 999),
        ("lane_record", "created_revision", 99),
    ),
)
async def test_carrier_plaintext_metadata_must_match_authenticated_envelope(
    tmp_path: Path,
    target: str,
    column: str,
    forged: object,
) -> None:
    bootstrap, room_id, epoch, lane_record = await _journal_with_carriers(tmp_path)
    locations = {
        "room_state": ("NioIngestRoomState", "room_id = ?", (room_id,)),
        "room_lane": (
            "NioIngestRoomLane",
            "room_id = ? AND membership_epoch = ?",
            (room_id, epoch),
        ),
        "lane_record": (
            "NioIngestLaneRecord",
            "room_id = ? AND membership_epoch = ? AND section = ? "
            "AND page_ordinal = ? AND record_ordinal = ?",
            (
                lane_record.key.room_id,
                lane_record.key.membership_epoch,
                lane_record.key.section.value,
                lane_record.key.page_ordinal,
                lane_record.key.record_ordinal,
            ),
        ),
    }
    table, where, parameters = locations[target]
    bootstrap._journal.connection.execute("PRAGMA ignore_check_constraints = ON")
    bootstrap._journal.connection.execute(
        f'UPDATE "{table}" SET "{column}" = ? WHERE account_id = ? AND {where}',
        (forged, ACCOUNT_ID, *parameters),
    )
    bootstrap._journal.connection.execute("PRAGMA ignore_check_constraints = OFF")
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="authenticat"):
            if target == "lane_record":
                lookup_room = forged if column == "room_id" else lane_record.key.room_id
                lookup_epoch = (
                    forged
                    if column == "membership_epoch"
                    else lane_record.key.membership_epoch
                )
                assert type(lookup_room) is str
                assert type(lookup_epoch) is int
                bootstrap._journal.list_lane_records(lookup_room, lookup_epoch)
            else:
                lookup_room = forged if column == "room_id" else room_id
                assert type(lookup_room) is str
                bootstrap._journal.load_rooms(frozenset({room_id, lookup_room}))
        assert bootstrap._journal.load_owner().revision == 2
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_one_room_transition_never_scans_or_rewrites_other_rooms(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    cold_source = bootstrap._journal.load_source()
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
        assert bootstrap._journal.load_source() == cold_source
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_ready_owned_loss_round_trips_after_its_source_frame_is_compacted(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
            transition=JournalTransition(
                frames=(frame,),
                ready_records=(ReadyRecord(0, loss, frame.frame_id),),
            ),
        )
        bootstrap._journal.commit(
            expected_revision=2,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(delete_frame_ids=(frame.frame_id,)),
        )
        assert bootstrap._journal.load_frame(frame.frame_id) is None

        loaded = bootstrap._journal.load_ready_heads(limit=1)[0]
        assert loaded.record == loss
        row = bootstrap._journal.connection.execute(
            "SELECT item_id, item_kind, canonical_bytes "
            "FROM NioIngestReadyRecord WHERE account_id = ? AND item_id = ?",
            (ACCOUNT_ID, loss.loss_id),
        ).fetchone()
        assert tuple(row) == (
            loss.loss_id,
            "loss",
            len(_canonical_json(_record_to_dict(loss))),
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_deterministic_loss_and_ready_ids_fail_closed_on_payload_drift(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
                ready_records=(ReadyRecord(0, event), ReadyRecord(1, loss)),
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
        with pytest.raises(JournalIntegrityError, match="ready item"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    ready_records=(ReadyRecord(2, changed_event),),
                ),
            )
        with pytest.raises(JournalIntegrityError, match="ready item"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    ready_records=(ReadyRecord(2, changed_loss),)
                ),
            )
        owner = bootstrap._journal.load_owner()
        assert owner.revision == 2
        assert owner.next_ready_order == 2
        assert [
            ready.record for ready in bootstrap._journal.load_ready_heads(limit=10)
        ] == [
            event,
            loss,
        ]

        forged_loss = LossRecord(
            loss.loss_id,
            loss.origin,
            "!forged:example.org",
            loss.membership_epoch,
            loss.reason,
            loss.boundary,
            loss.detail_json,
        )
        row = bootstrap._journal.connection.execute(
            "SELECT * FROM NioIngestReadyRecord WHERE account_id = ? AND item_id = ?",
            (ACCOUNT_ID, loss.loss_id),
        ).fetchone()
        payload = bootstrap._journal._open_payload(
            "NioIngestReadyRecord", (loss.loss_id,), row, "payload"
        )
        envelope = json.loads(payload)
        envelope["record"] = _record_to_dict(forged_loss)
        forged_canonical_bytes = len(_canonical_json(_record_to_dict(forged_loss)))
        envelope["canonical_bytes"] = forged_canonical_bytes
        forged_payload = _canonical_json(envelope)
        ciphertext, digest = bootstrap._journal._codec.seal(
            "NioIngestReadyRecord",
            (loss.loss_id,),
            forged_payload,
        )
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestReadyRecord SET room_id = ?, canonical_bytes = ?, "
            "payload_ciphertext = ?, payload_sha256 = ? "
            "WHERE account_id = ? AND item_id = ?",
            (
                forged_loss.room_id,
                forged_canonical_bytes,
                ciphertext,
                digest,
                ACCOUNT_ID,
                loss.loss_id,
            ),
        )
        with pytest.raises(JournalIntegrityError, match="loss_id"):
            bootstrap._journal.load_ready_heads(limit=10)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_ready_canonical_byte_accounting_is_derived_and_revalidated(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    event = _batch_event(1)
    try:
        with pytest.raises(JournalIntegrityError, match="ready"):
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
            "WHERE account_id = ? AND item_id = ?",
            (ACCOUNT_ID, event.record_id),
        )
        with pytest.raises(JournalIntegrityError, match="ready"):
            bootstrap._journal.load_ready_heads(limit=1)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_frame_id_is_insert_or_exact_revalidate_never_overwritten(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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


def _journal_system_event(
    stream_id: UUID,
    kind: RecordKind,
    *,
    room_id: str = "!room:example.org",
    membership_epoch: int = 1,
    room_sequence: int = 1,
) -> EventRecord:
    if kind is RecordKind.STATE:
        origin_kind = SystemOriginKind.ROOM_HYDRATION
        event_id = "$hydrated"
        source_json = b'{"type":"m.room.name"}'
        identity = f"{kind.value}:{room_id}:{event_id}"
    elif kind is RecordKind.ROOM_LIFECYCLE:
        origin_kind = SystemOriginKind.MEMBERSHIP_CHANGE
        event_id = None
        source_json = b'{"membership":"leave"}'
        identity = (
            f"{kind.value}:{room_id}:{membership_epoch}:{origin_kind.value}:"
            f"{SYSTEM_OPERATION_ID}:{hashlib.sha256(source_json).hexdigest()}"
        )
    else:
        assert kind is RecordKind.ROOM_READINESS
        origin_kind = SystemOriginKind.ROOM_HYDRATION
        event_id = None
        source_json = b'{"status":"ready"}'
        identity = (
            f"{kind.value}:{room_id}:{membership_epoch}:{origin_kind.value}:"
            f"{SYSTEM_OPERATION_ID}:{hashlib.sha256(source_json).hexdigest()}"
        )
    return EventRecord(
        str(uuid5(stream_id, identity)),
        kind,
        SystemOrigin(origin_kind, SYSTEM_OPERATION_ID),
        room_id,
        membership_epoch,
        room_sequence,
        event_id,
        None,
        source_json,
        None,
    )


def _journal_system_loss(stream_id: UUID) -> LossRecord:
    incomplete = LossRecord(
        "",
        SystemOrigin(SystemOriginKind.STORE_VALIDATION, SYSTEM_OPERATION_ID),
        "!room:example.org",
        1,
        LossReason.CORRUPT_STORED_RECORD,
        LossBoundary(None, None, None, None),
        b'{"table":"ready"}',
    )
    return replace(incomplete, loss_id=_loss_id(stream_id, incomplete))


def _system_lifecycle_transition(lifecycle: EventRecord) -> JournalTransition:
    room_id = lifecycle.room_id
    assert room_id is not None
    return JournalTransition(
        room_states=(
            RoomState(
                room_id,
                2,
                2,
                RoomHydrationStatus.READY,
                _snapshot(room_id, 2),
            ),
        ),
        room_lanes=(
            RoomLane(
                room_id,
                1,
                LaneStatus.RETIRING,
                successor_membership_epoch=2,
                pending_lifecycle=lifecycle,
            ),
            RoomLane(room_id, 2, LaneStatus.ACTIVE),
        ),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("carrier", ("ready", "lifecycle", "batch"))
async def test_system_event_identity_rejects_before_journal_dml(
    tmp_path: Path,
    carrier: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    if carrier == "ready":
        valid = _journal_system_event(
            bootstrap.stream_id,
            RecordKind.ROOM_READINESS,
        )
        transition = JournalTransition(
            ready_records=(
                ReadyRecord(0, replace(valid, record_id=STALE_SYSTEM_RECORD_ID)),
            ),
        )
    elif carrier == "lifecycle":
        valid = _journal_system_event(
            bootstrap.stream_id,
            RecordKind.ROOM_LIFECYCLE,
            membership_epoch=2,
        )
        transition = _system_lifecycle_transition(
            replace(valid, record_id=STALE_SYSTEM_RECORD_ID)
        )
    else:
        valid = _journal_system_event(bootstrap.stream_id, RecordKind.STATE)
        batch = batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=1,
            created_revision=2,
            records=(valid,),
        )
        object.__setattr__(valid, "record_id", STALE_SYSTEM_RECORD_ID)
        payload = canonical_batch_payload(batch)
        digest = hashlib.sha256(payload).digest()
        object.__setattr__(batch.ref, "sha256", digest)
        object.__setattr__(
            batch.ref,
            "batch_id",
            uuid5(bootstrap.stream_id, f"1:{digest.hex()}"),
        )
        transition = JournalTransition(
            batch_materialization=BatchMaterialization(
                batch,
                (ReadyRecordKey(STALE_SYSTEM_RECORD_ID),),
            )
        )

    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="record_id"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=transition,
            )
        assert bootstrap._journal.load_owner().revision == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("carrier", ("ready", "lifecycle", "batch"))
async def test_authenticated_system_event_identity_is_revalidated_on_read(
    tmp_path: Path,
    carrier: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    codec = EncryptedRowCodec("secret", ACCOUNT_ID, bootstrap.stream_id)
    try:
        if carrier == "ready":
            valid = _journal_system_event(
                bootstrap.stream_id,
                RecordKind.ROOM_READINESS,
            )
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    ready_records=(ReadyRecord(0, valid),),
                ),
            )
            forged = replace(valid, record_id=STALE_SYSTEM_RECORD_ID)
            row = bootstrap._journal.connection.execute(
                "SELECT * FROM NioIngestReadyRecord "
                "WHERE account_id = ? AND item_id = ?",
                (ACCOUNT_ID, valid.record_id),
            ).fetchone()
            envelope = json.loads(
                bootstrap._journal._open_payload(
                    "NioIngestReadyRecord",
                    (valid.record_id,),
                    row,
                    "payload",
                )
            )
            envelope["item_id"] = forged.record_id
            envelope["record"] = _record_to_dict(forged)
            payload = _canonical_json(envelope)
            ciphertext, digest = codec.seal(
                "NioIngestReadyRecord",
                (forged.record_id,),
                payload,
            )
            bootstrap._journal.connection.execute(
                "UPDATE NioIngestReadyRecord SET item_id = ?, "
                "payload_ciphertext = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND item_id = ?",
                (
                    forged.record_id,
                    ciphertext,
                    digest,
                    ACCOUNT_ID,
                    valid.record_id,
                ),
            )
            load = lambda: bootstrap._journal.load_ready_heads(limit=1)
        elif carrier == "lifecycle":
            valid = _journal_system_event(
                bootstrap.stream_id,
                RecordKind.ROOM_LIFECYCLE,
                membership_epoch=2,
            )
            transition = _system_lifecycle_transition(valid)
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=transition,
            )
            forged = replace(valid, record_id=STALE_SYSTEM_RECORD_ID)
            lane = replace(
                transition.room_lanes[0],
                pending_lifecycle=forged,
                updated_revision=2,
            )
            payload = bootstrap._journal._room_lane_payload(lane, 2)
            ciphertext, digest = codec.seal(
                "NioIngestRoomLane",
                (lane.room_id, lane.membership_epoch),
                payload,
            )
            bootstrap._journal.connection.execute(
                "UPDATE NioIngestRoomLane SET lane_state_ciphertext = ?, "
                "lane_state_sha256 = ? WHERE account_id = ? AND room_id = ? "
                "AND membership_epoch = ?",
                (
                    ciphertext,
                    digest,
                    ACCOUNT_ID,
                    lane.room_id,
                    lane.membership_epoch,
                ),
            )
            load = lambda: bootstrap._journal.load_rooms(frozenset({lane.room_id}))
        else:
            valid = _journal_system_event(bootstrap.stream_id, RecordKind.STATE)
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    ready_records=(ReadyRecord(0, valid),),
                ),
            )
            batch = batch_from_records(
                account_id=ACCOUNT_ID,
                device_id=DEVICE_ID,
                consumer=consumer.binding,
                stream_id=bootstrap.stream_id,
                sequence=1,
                created_revision=3,
                records=(valid,),
            )
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    batch_materialization=BatchMaterialization(
                        batch,
                        (ReadyRecordKey(valid.record_id),),
                    )
                ),
            )
            payload = canonical_batch_payload(batch).replace(
                valid.record_id.encode(),
                STALE_SYSTEM_RECORD_ID.encode(),
            )
            assert len(payload) == len(canonical_batch_payload(batch))
            digest = hashlib.sha256(payload).digest()
            batch_id = uuid5(
                bootstrap.stream_id,
                f"1:{digest.hex()}",
            )
            ciphertext = codec.encrypt(
                "NioIngestBatch",
                (1,),
                payload,
                digest,
            )
            bootstrap._journal.connection.execute(
                "UPDATE NioIngestBatch SET batch_id = ?, payload_ciphertext = ?, "
                "payload_sha256 = ? WHERE account_id = ? AND sequence = 1",
                (str(batch_id), ciphertext, digest, ACCOUNT_ID),
            )
            load = bootstrap._journal.oldest_unacknowledged

        with pytest.raises(JournalIntegrityError, match="record_id"):
            load()
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_authenticated_ready_wraps_strict_system_origin_decode_errors(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    record = _journal_system_event(
        bootstrap.stream_id,
        RecordKind.ROOM_READINESS,
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                ready_records=(ReadyRecord(0, record),),
            ),
        )
        record_payload = _record_to_dict(record)
        origin_payload = record_payload["origin"]
        assert isinstance(origin_payload, dict)
        origin_payload["extra"] = True
        row = bootstrap._journal.connection.execute(
            "SELECT * FROM NioIngestReadyRecord "
            "WHERE account_id = ? AND item_id = ?",
            (ACCOUNT_ID, record.record_id),
        ).fetchone()
        envelope = json.loads(
            bootstrap._journal._open_payload(
                "NioIngestReadyRecord",
                (record.record_id,),
                row,
                "payload",
            )
        )
        envelope["record"] = record_payload
        payload = _canonical_json(envelope)
        ciphertext, digest = EncryptedRowCodec(
            "secret",
            ACCOUNT_ID,
            bootstrap.stream_id,
        ).seal("NioIngestReadyRecord", (record.record_id,), payload)
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestReadyRecord SET payload_ciphertext = ?, "
            "payload_sha256 = ? WHERE account_id = ? AND item_id = ?",
            (ciphertext, digest, ACCOUNT_ID, record.record_id),
        )

        with pytest.raises(JournalIntegrityError, match="ready"):
            bootstrap._journal.load_ready_heads(limit=1)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("record_family", ("event", "loss"))
async def test_system_ready_lineage_rejects_source_frame_on_write_and_read(
    tmp_path: Path,
    record_family: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    record: EventRecord | LossRecord
    if record_family == "event":
        record = _journal_system_event(
            bootstrap.stream_id,
            RecordKind.ROOM_READINESS,
        )
    else:
        record = _journal_system_loss(bootstrap.stream_id)
    item_id = record.record_id if type(record) is EventRecord else record.loss_id
    ready = ReadyRecord(0, record)
    source_frame_id = UUID("55555555-5555-5555-5555-555555555555")
    object.__setattr__(ready, "source_frame_id", source_frame_id)
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="source_frame_id"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(ready_records=(ready,)),
            )
        assert bootstrap._journal.load_owner().revision == 1
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )

        object.__setattr__(ready, "source_frame_id", None)
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(ready_records=(ready,)),
        )
        row = bootstrap._journal.connection.execute(
            "SELECT * FROM NioIngestReadyRecord "
            "WHERE account_id = ? AND item_id = ?",
            (ACCOUNT_ID, item_id),
        ).fetchone()
        envelope = json.loads(
            bootstrap._journal._open_payload(
                "NioIngestReadyRecord",
                (item_id,),
                row,
                "payload",
            )
        )
        envelope["source_frame_id"] = str(source_frame_id)
        payload = _canonical_json(envelope)
        ciphertext, digest = EncryptedRowCodec(
            "secret",
            ACCOUNT_ID,
            bootstrap.stream_id,
        ).seal("NioIngestReadyRecord", (item_id,), payload)
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestReadyRecord SET source_frame_id = ?, "
            "payload_ciphertext = ?, payload_sha256 = ? "
            "WHERE account_id = ? AND item_id = ?",
            (
                str(source_frame_id),
                ciphertext,
                digest,
                ACCOUNT_ID,
                item_id,
            ),
        )
        with pytest.raises(JournalIntegrityError, match="source_frame_id"):
            bootstrap._journal.load_ready_heads(limit=1)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_frame_read_authenticates_every_returned_metadata_field(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
        source=CLASSIC_SOURCE,
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
            ("item_id", event.record_id, "forged-item"),
            ("item_kind", "event", "loss"),
            ("ready_order", 0, 99),
            (
                "source_frame_id",
                str(source_frame_id),
                "66666666-6666-6666-6666-666666666666",
            ),
            ("room_id", None, "!forged:example.org"),
            ("membership_epoch", None, 9),
            ("room_sequence", None, 9),
            (
                "canonical_bytes",
                len(_canonical_json(_record_to_dict(event))),
                999,
            ),
            ("created_revision", 2, 77),
        ):
            bootstrap._journal.connection.execute(
                f"UPDATE NioIngestReadyRecord SET {column} = ? "
                "WHERE account_id = ? AND item_id = ?",
                (forged, ACCOUNT_ID, event.record_id),
            )
            with pytest.raises(JournalIntegrityError, match="ready|authentication"):
                bootstrap._journal.load_ready_heads(limit=1)
            current_item_id = forged if column == "item_id" else event.record_id
            bootstrap._journal.connection.execute(
                f"UPDATE NioIngestReadyRecord SET {column} = ? "
                "WHERE account_id = ? AND item_id = ?",
                (original, ACCOUNT_ID, current_item_id),
            )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_ready_decode_rejects_equal_but_wrong_typed_canonical_bytes(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer_bootstrap(bootstrap))
    event = _batch_event(1)
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(ready_records=(ReadyRecord(0, event),)),
    )
    row = bootstrap._journal.connection.execute(
        "SELECT * FROM NioIngestReadyRecord WHERE account_id = ? AND item_id = ?",
        (ACCOUNT_ID, event.record_id),
    ).fetchone()
    envelope = json.loads(
        bootstrap._journal._open_payload(
            "NioIngestReadyRecord",
            (event.record_id,),
            row,
            "payload",
        )
    )
    envelope["canonical_bytes"] = float(envelope["canonical_bytes"])
    payload = _canonical_json(envelope)
    ciphertext, digest = bootstrap._journal._codec.seal(
        "NioIngestReadyRecord", (event.record_id,), payload
    )
    bootstrap._journal.connection.execute(
        "UPDATE NioIngestReadyRecord SET payload_ciphertext = ?, "
        "payload_sha256 = ? WHERE account_id = ? AND item_id = ?",
        (ciphertext, digest, ACCOUNT_ID, event.record_id),
    )
    try:
        with pytest.raises(JournalIntegrityError, match="canonical_bytes"):
            bootstrap._journal.load_ready_heads(limit=1)
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_batch_commit_and_read_validate_full_owner_and_row_identity(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
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
                transition=JournalTransition(
                    batch_materialization=BatchMaterialization(
                        wrong_revision,
                        (ReadyRecordKey(_batch_event(1).record_id),),
                    )
                ),
            )
        assert bootstrap._journal.load_owner().revision == 1

        event = _batch_event(1)
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(ready_records=(ReadyRecord(0, event),)),
        )
        batch = batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=1,
            created_revision=3,
            records=(event,),
        )
        bootstrap._journal.commit(
            expected_revision=2,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                batch_materialization=BatchMaterialization(
                    batch,
                    (ReadyRecordKey(event.record_id),),
                )
            ),
        )
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatch SET created_revision = 98 "
            "WHERE account_id = ? AND sequence = 1",
            (ACCOUNT_ID,),
        )
        with pytest.raises(JournalIntegrityError, match="created_revision"):
            bootstrap._journal.oldest_unacknowledged()
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatch SET created_revision = 3 "
            "WHERE account_id = ? AND sequence = 1",
            (ACCOUNT_ID,),
        )
        forged = batch_from_records(
            account_id="@forged:example.org",
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=1,
            created_revision=3,
            records=(event,),
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
async def test_acknowledgement_is_fifo_idempotent_and_deletes_payloads(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, _baseline_room_ids(3))
    await bootstrap.attach_consumer(consumer)
    ready = bootstrap._journal.load_ready_heads(limit=3)
    batches = []
    for sequence, source in enumerate(ready, start=1):
        assert type(source.record) is LossRecord
        batch = batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=sequence,
            created_revision=sequence + 1,
            records=(source.record,),
        )
        bootstrap._journal.commit(
            expected_revision=sequence,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                batch_materialization=BatchMaterialization(
                    batch,
                    (ReadyRecordKey(source.record.loss_id),),
                )
            ),
        )
        batches.append(batch)
    batches = tuple(batches)
    try:
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
            "SELECT sequence FROM NioIngestBatch "
            "WHERE account_id = ? ORDER BY sequence",
            (ACCOUNT_ID,),
        ).fetchall()
        assert [row[0] for row in rows] == [3]
        with pytest.raises(JournalConflictError, match="stale"):
            bootstrap._journal.acknowledge(batches[0].ref)
        assert bootstrap._journal.oldest_unacknowledged() == batches[2]
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_latest_ack_retry_rejects_corrupt_frontier_without_payload(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, ("!baseline:example.org",))
    await bootstrap.attach_consumer(consumer)
    ready = bootstrap._journal.load_ready_heads(limit=1)[0]
    assert type(ready.record) is LossRecord
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=2,
        records=(ready.record,),
    )
    try:
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                batch_materialization=BatchMaterialization(
                    batch,
                    (ReadyRecordKey(ready.record.loss_id),),
                )
            ),
        )
        bootstrap._journal.acknowledge(batch.ref)
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestMeta SET last_acked_sha256 = ? WHERE account_id = ?",
            (b"z" * 32, ACCOUNT_ID),
        )

        with pytest.raises(JournalConflictError, match="frontier"):
            bootstrap._journal.acknowledge(batch.ref)
    finally:
        bootstrap.close()


def _owned_loss(
    stream_id: UUID,
    *,
    room_id: str = "!lane:example.org",
    request_id: int = 10,
) -> LossRecord:
    loss = LossRecord(
        "",
        RecordOrigin(TransportKind.CLASSIC, 1, request_id, 0),
        room_id,
        0,
        LossReason.FETCH_FAILED,
        LossBoundary(None, None, "old", "new"),
        f'{{"request":{request_id}}}'.encode(),
    )
    return replace(loss, loss_id=_loss_id(stream_id, loss))


def _loss_lane_record(loss: LossRecord, *, ordinal: int = 0) -> LaneRecord:
    return LaneRecord(
        LaneRecordKey(
            loss.room_id,
            loss.membership_epoch,
            LaneRecordSection.LOSS,
            0,
            ordinal,
        ),
        loss,
        UUID("55555555-5555-5555-5555-555555555555"),
        None,
        len(_canonical_json(_record_to_dict(loss))),
    )


@pytest.mark.asyncio
async def test_materialization_moves_exact_ready_and_lane_sources_with_grouped_sql(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    room_id = "!lane:example.org"
    consumer = _consumer_bootstrap(bootstrap, (room_id,))
    await bootstrap.attach_consumer(consumer)
    ready_loss = bootstrap._journal.load_ready_heads(limit=1)[0].record
    assert type(ready_loss) is LossRecord
    lane_record = _loss_lane_record(_owned_loss(bootstrap.stream_id, room_id=room_id))
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(lane_record_inserts=(lane_record,)),
    )
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=3,
        records=(ready_loss, lane_record.record),
    )
    transition = JournalTransition(
        batch_materialization=BatchMaterialization(
            batch,
            (ReadyRecordKey(ready_loss.loss_id), lane_record.key),
        )
    )
    statements: list[str] = []
    labels: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    bootstrap._journal.set_transition_statement_hook(labels.append)
    try:
        result = bootstrap._journal.commit(
            expected_revision=2,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=transition,
        )
        bootstrap._journal.connection.set_trace_callback(None)

        assert result.revision == 3
        assert bootstrap._journal.load_ready_heads(limit=10) == ()
        assert bootstrap._journal.load_lane_record(lane_record.key) is None
        assert bootstrap._journal.oldest_unacknowledged() == batch
        index_rows = bootstrap._journal.connection.execute(
            "SELECT item_id, item_kind, sequence, record_ordinal "
            "FROM NioIngestBatchItem ORDER BY record_ordinal"
        ).fetchall()
        assert [tuple(row) for row in index_rows] == [
            (ready_loss.loss_id, "loss", 1, 0),
            (lane_record.record.loss_id, "loss", 1, 1),
        ]
        assert (
            sum(
                statement.lstrip().upper().startswith("SELECT")
                and "NioIngestReadyRecord" in statement
                for statement in statements
            )
            == 1
        )
        assert (
            sum(
                statement.lstrip().upper().startswith("SELECT")
                and "NioIngestLaneRecord" in statement
                for statement in statements
            )
            == 1
        )
        assert (
            sum(
                statement.lstrip().upper().startswith("DELETE")
                and "NioIngestReadyRecord" in statement
                for statement in statements
            )
            == 1
        )
        assert (
            sum(
                statement.lstrip().upper().startswith("DELETE")
                and "NioIngestLaneRecord" in statement
                for statement in statements
            )
            == 1
        )
        assert labels.count("batch_items") == 1
        assert sum(statement == "BEGIN IMMEDIATE" for statement in statements) == 1
        assert sum(statement == "COMMIT" for statement in statements) == 1
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_materialization_rejects_changed_source_contents_before_dml(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    source = _batch_event(1)
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(ready_records=(ReadyRecord(0, source),)),
    )
    changed = replace(source, source_json=b'{"sequence":999}')
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=3,
        records=(changed,),
    )
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="source.*batch|batch.*source"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    batch_materialization=BatchMaterialization(
                        batch,
                        (ReadyRecordKey(source.record_id),),
                    )
                ),
            )
        assert bootstrap._journal.load_owner().revision == 2
        assert bootstrap._journal.load_ready_heads(limit=1)[0].record == source
        assert bootstrap._journal.oldest_unacknowledged() is None
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_ready_lane_and_batch_item_ownership_collisions_fail_before_dml(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    room_id = "!lane:example.org"
    consumer = _consumer_bootstrap(bootstrap, (room_id,))
    await bootstrap.attach_consumer(consumer)
    ready = bootstrap._journal.load_ready_heads(limit=1)[0]
    assert type(ready.record) is LossRecord
    duplicate_lane = LaneRecord(
        LaneRecordKey(room_id, 0, LaneRecordSection.LOSS, 0, 0),
        ready.record,
        None,
        None,
        ready.canonical_bytes,
    )
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    with pytest.raises(JournalIntegrityError, match="owner|owned"):
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(lane_record_inserts=(duplicate_lane,)),
        )
    assert not any(
        statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in statements
    )

    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=2,
        records=(ready.record,),
    )
    bootstrap._journal.connection.set_trace_callback(None)
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            batch_materialization=BatchMaterialization(
                batch,
                (ReadyRecordKey(ready.record.loss_id),),
            )
        ),
    )
    for transition in (
        JournalTransition(ready_records=(replace(ready, ready_order=1),)),
        JournalTransition(lane_record_inserts=(duplicate_lane,)),
    ):
        statements = []
        bootstrap._journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError, match="batch|owner|owned"):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=transition,
            )
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("mutation", ("missing", "extra", "item_id", "kind", "ordinal"))
async def test_batch_reads_reject_mutated_derived_item_index(
    tmp_path: Path,
    mutation: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, ("!room:example.org",))
    await bootstrap.attach_consumer(consumer)
    ready = bootstrap._journal.load_ready_heads(limit=1)[0]
    assert type(ready.record) is LossRecord
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=2,
        records=(ready.record,),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            batch_materialization=BatchMaterialization(
                batch,
                (ReadyRecordKey(ready.record.loss_id),),
            )
        ),
    )
    if mutation == "missing":
        bootstrap._journal.connection.execute(
            "DELETE FROM NioIngestBatchItem WHERE account_id = ?",
            (ACCOUNT_ID,),
        )
    elif mutation == "extra":
        bootstrap._journal.connection.execute(
            "INSERT INTO NioIngestBatchItem VALUES (?, ?, ?, ?, ?)",
            (ACCOUNT_ID, "extra", "event", 1, 1),
        )
    elif mutation == "item_id":
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatchItem SET item_id = 'forged' " "WHERE account_id = ?",
            (ACCOUNT_ID,),
        )
    elif mutation == "kind":
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatchItem SET item_kind = 'event' " "WHERE account_id = ?",
            (ACCOUNT_ID,),
        )
    else:
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatchItem SET record_ordinal = 7 " "WHERE account_id = ?",
            (ACCOUNT_ID,),
        )
    try:
        with pytest.raises(JournalIntegrityError, match="batch item"):
            bootstrap._journal.oldest_unacknowledged()
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("duplicate_owner", ("ready", "lane"))
async def test_batch_read_and_ack_reject_resurrected_authenticated_source_owner(
    tmp_path: Path,
    duplicate_owner: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(
        bootstrap,
        ("!ready:example.org",) if duplicate_owner == "ready" else (),
    )
    await bootstrap.attach_consumer(consumer)
    if duplicate_owner == "ready":
        ready = bootstrap._journal.load_ready_heads(limit=1)[0]
        record = ready.record
        source: ReadyRecordKey | LaneRecordKey = ReadyRecordKey(record.loss_id)
        revision = 1
    else:
        record = _owned_loss(bootstrap.stream_id)
        lane_record = _loss_lane_record(record)
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=_room_carrier_transition(
                record.room_id,
                record.membership_epoch,
                lane_records=(lane_record,),
            ),
        )
        source = lane_record.key
        revision = 2
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=revision + 1,
        records=(record,),
    )
    bootstrap._journal.commit(
        expected_revision=revision,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            batch_materialization=BatchMaterialization(batch, (source,))
        ),
    )
    if duplicate_owner == "ready":
        bootstrap._journal._write_ready(ready, revision + 1)
    else:
        bootstrap._journal._write_lane_record(lane_record, revision + 1)

    owner_before = bootstrap._journal.load_owner()
    batch_rows_before = bootstrap._journal.connection.execute(
        "SELECT * FROM NioIngestBatch WHERE account_id = ?",
        (ACCOUNT_ID,),
    ).fetchall()
    index_rows_before = bootstrap._journal.connection.execute(
        "SELECT * FROM NioIngestBatchItem WHERE account_id = ?",
        (ACCOUNT_ID,),
    ).fetchall()
    try:
        with pytest.raises(JournalIntegrityError, match="duplicate.*owner"):
            bootstrap._journal.oldest_unacknowledged()

        statements: list[str] = []
        bootstrap._journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError, match="duplicate.*owner"):
            bootstrap._journal.acknowledge(batch.ref)
        bootstrap._journal.connection.set_trace_callback(None)
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
        assert bootstrap._journal.load_owner() == owner_before
        assert (
            bootstrap._journal.connection.execute(
                "SELECT * FROM NioIngestBatch WHERE account_id = ?",
                (ACCOUNT_ID,),
            ).fetchall()
            == batch_rows_before
        )
        assert (
            bootstrap._journal.connection.execute(
                "SELECT * FROM NioIngestBatchItem WHERE account_id = ?",
                (ACCOUNT_ID,),
            ).fetchall()
            == index_rows_before
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_ack_deletes_payload_and_exact_retry_uses_only_frontier_metadata(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, ("!room:example.org",))
    await bootstrap.attach_consumer(consumer)
    ready = bootstrap._journal.load_ready_heads(limit=1)[0]
    assert type(ready.record) is LossRecord
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=2,
        records=(ready.record,),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            batch_materialization=BatchMaterialization(
                batch,
                (ReadyRecordKey(ready.record.loss_id),),
            )
        ),
    )
    read_statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(read_statements.append)
    assert bootstrap._journal.oldest_unacknowledged() == batch
    bootstrap._journal.connection.set_trace_callback(None)
    assert (
        sum(
            statement.lstrip().upper().startswith("SELECT")
            and "NioIngestBatchItem" in statement
            for statement in read_statements
        )
        == 1
    )
    assert bootstrap._journal.acknowledge(batch.ref) is AckOutcome.ACKNOWLEDGED
    assert (
        bootstrap._journal.connection.execute(
            "SELECT COUNT(*) FROM NioIngestBatch"
        ).fetchone()[0]
        == 0
    )
    assert (
        bootstrap._journal.connection.execute(
            "SELECT COUNT(*) FROM NioIngestBatchItem"
        ).fetchone()[0]
        == 0
    )

    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    assert bootstrap._journal.acknowledge(batch.ref) is AckOutcome.ALREADY_ACKNOWLEDGED
    bootstrap._journal.connection.set_trace_callback(None)
    assert not any("NioIngestBatch" in statement for statement in statements)
    assert not any(
        statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in statements
    )
    bootstrap.close()


@pytest.mark.asyncio
async def test_materialization_of_256_records_uses_bounded_grouped_sql(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    records = tuple(_batch_event(sequence) for sequence in range(1, 257))
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            ready_records=tuple(
                ReadyRecord(ordinal, record) for ordinal, record in enumerate(records)
            )
        ),
    )
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=3,
        records=records,
    )
    statements: list[str] = []
    labels: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    bootstrap._journal.set_transition_statement_hook(labels.append)
    try:
        bootstrap._journal.commit(
            expected_revision=2,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                batch_materialization=BatchMaterialization(
                    batch,
                    tuple(ReadyRecordKey(record.record_id) for record in records),
                )
            ),
        )
        bootstrap._journal.connection.set_trace_callback(None)
        assert (
            bootstrap._journal.connection.execute(
                "SELECT COUNT(*) FROM NioIngestBatchItem"
            ).fetchone()[0]
            == 256
        )
        assert (
            sum(
                statement.lstrip().upper().startswith("SELECT")
                and "NioIngestReadyRecord" in statement
                for statement in statements
            )
            == 1
        )
        assert (
            sum(
                statement.lstrip().upper().startswith("DELETE")
                and "NioIngestReadyRecord" in statement
                for statement in statements
            )
            == 1
        )
        assert labels.count("batch_items") == 1
        assert sum(statement == "BEGIN IMMEDIATE" for statement in statements) == 1
        assert sum(statement == "COMMIT" for statement in statements) == 1
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("payload_sizes", "message"),
    (
        ((1024 * 1024 + 1,), "record"),
        ((700_000, 700_000, 700_000), "batch"),
    ),
)
async def test_materialization_enforces_immutable_record_and_batch_byte_ceilings(
    tmp_path: Path,
    payload_sizes: tuple[int, ...],
    message: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    records = tuple(
        replace(
            _batch_event(ordinal + 1),
            source_json=b"x" * payload_size,
        )
        for ordinal, payload_size in enumerate(payload_sizes)
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            ready_records=tuple(
                ReadyRecord(ordinal, record) for ordinal, record in enumerate(records)
            )
        ),
    )
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=3,
        records=records,
    )
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match=message):
            bootstrap._journal.commit(
                expected_revision=2,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    batch_materialization=BatchMaterialization(
                        batch,
                        tuple(ReadyRecordKey(record.record_id) for record in records),
                    )
                ),
            )
        assert bootstrap._journal.load_owner().revision == 2
        assert bootstrap._journal.oldest_unacknowledged() is None
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("case", ("missing", "wrong_owner", "changed_lane"))
async def test_materialization_requires_the_exact_positional_source_before_dml(
    tmp_path: Path,
    case: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    source_record = _lane_event(
        "held-source",
        "!lane:example.org",
        1,
        1,
        TimelineEventProvenance.LIVE,
    )
    lane_record = _lane_record(
        LaneRecordKey(
            "!lane:example.org",
            1,
            LaneRecordSection.HELD,
            0,
            0,
        ),
        source_record,
        source_frame_id=UUID("55555555-5555-5555-5555-555555555555"),
    )
    if case != "missing":
        bootstrap._journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=_room_carrier_transition(
                lane_record.key.room_id,
                lane_record.key.membership_epoch,
                lane_records=(lane_record,),
            ),
        )
        revision = 2
    else:
        revision = 1
    batch_record = (
        replace(source_record, source_json=b'{"changed":true}')
        if case == "changed_lane"
        else source_record
    )
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=revision + 1,
        records=(batch_record,),
    )
    source = (
        ReadyRecordKey(source_record.record_id)
        if case == "wrong_owner"
        else lane_record.key
    )
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="source|owner"):
            bootstrap._journal.commit(
                expected_revision=revision,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    batch_materialization=BatchMaterialization(batch, (source,))
                ),
            )
        assert bootstrap._journal.load_owner().revision == revision
        assert bootstrap._journal.oldest_unacknowledged() is None
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
async def test_lane_owned_item_cannot_be_inserted_into_ready_or_both_in_one_transition(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    loss = _owned_loss(bootstrap.stream_id)
    lane_record = _loss_lane_record(loss)
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=_room_carrier_transition(
            loss.room_id,
            loss.membership_epoch,
            lane_records=(lane_record,),
        ),
    )
    same_id_event = replace(_batch_event(1), record_id=loss.loss_id)
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    with pytest.raises(JournalIntegrityError, match="owned"):
        bootstrap._journal.commit(
            expected_revision=2,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                ready_records=(ReadyRecord(0, same_id_event),)
            ),
        )
    assert not any(
        statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in statements
    )

    other = open_ingestion_store(
        tmp_path / "same-transition",
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    await other.attach_consumer(_consumer_bootstrap(other))
    other_loss = _owned_loss(other.stream_id)
    other_lane = _loss_lane_record(other_loss)
    other_event = replace(_batch_event(1), record_id=other_loss.loss_id)
    transition = replace(
        _room_carrier_transition(
            other_loss.room_id,
            other_loss.membership_epoch,
            lane_records=(other_lane,),
        ),
        ready_records=(ReadyRecord(0, other_event),),
    )
    try:
        statements: list[str] = []
        other._journal.connection.set_trace_callback(statements.append)
        with pytest.raises(JournalIntegrityError, match="same item"):
            other._journal.commit(
                expected_revision=1,
                writer_epoch=other._journal.writer_epoch,
                transition=transition,
            )
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        other.close()
        bootstrap.close()


@pytest.mark.asyncio
async def test_commit_revalidates_a_mutated_materialization_carrier_before_dml(
    tmp_path: Path,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap)
    await bootstrap.attach_consumer(consumer)
    record = _batch_event(1)
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=2,
        records=(record,),
    )
    materialization = BatchMaterialization(
        batch,
        (ReadyRecordKey(record.record_id),),
    )
    object.__setattr__(
        materialization,
        "sources",
        (
            ReadyRecordKey(record.record_id),
            *tuple(ReadyRecordKey(f"record-{ordinal}") for ordinal in range(256)),
        ),
    )
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError, match="source|256"):
            bootstrap._journal.commit(
                expected_revision=1,
                writer_epoch=bootstrap._journal.writer_epoch,
                transition=JournalTransition(
                    batch_materialization=materialization,
                ),
            )
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("corruption", ("batch", "batch_item"))
async def test_ack_authenticates_payload_and_item_index_before_any_dml(
    tmp_path: Path,
    corruption: str,
) -> None:
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer_bootstrap(bootstrap, ("!room:example.org",))
    await bootstrap.attach_consumer(consumer)
    ready = bootstrap._journal.load_ready_heads(limit=1)[0]
    assert type(ready.record) is LossRecord
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=2,
        records=(ready.record,),
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            batch_materialization=BatchMaterialization(
                batch,
                (ReadyRecordKey(ready.record.loss_id),),
            )
        ),
    )
    if corruption == "batch":
        row = bootstrap._journal.connection.execute(
            "SELECT payload_ciphertext FROM NioIngestBatch WHERE account_id = ?",
            (ACCOUNT_ID,),
        ).fetchone()
        ciphertext = bytearray(row[0])
        ciphertext[-1] ^= 1
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatch SET payload_ciphertext = ? WHERE account_id = ?",
            (bytes(ciphertext), ACCOUNT_ID),
        )
    else:
        bootstrap._journal.connection.execute(
            "UPDATE NioIngestBatchItem SET item_kind = 'event' " "WHERE account_id = ?",
            (ACCOUNT_ID,),
        )
    before = bootstrap._journal.load_owner()
    statements: list[str] = []
    bootstrap._journal.connection.set_trace_callback(statements.append)
    try:
        with pytest.raises(JournalIntegrityError):
            bootstrap._journal.acknowledge(batch.ref)
        assert bootstrap._journal.load_owner() == before
        assert (
            bootstrap._journal.connection.execute(
                "SELECT COUNT(*) FROM NioIngestBatch"
            ).fetchone()[0]
            == 1
        )
        assert not any(
            statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
            for statement in statements
        )
    finally:
        bootstrap.close()
