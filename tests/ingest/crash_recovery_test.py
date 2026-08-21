"""On-disk restart and schema kill-point coverage for ingestion v1."""

import hashlib
import json
import multiprocessing
import os
import sqlite3
from dataclasses import replace
from pathlib import Path
from typing import Callable
from uuid import UUID

import pytest

from nio.crypto import OlmAccount, OlmDevice, TrustState
from nio.event_provenance import TimelineEventProvenance
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.errors import FreshIngestionRequired
from nio.ingest.model import EventRecord, RecordKind, RecordOrigin, TransportKind
from nio.ingest.ports import NetworkResult, StagedSourceResponse
from nio.ingest.sliding import RESERVED_ALL_ROOMS_LIST, SlidingSource
from nio.ingest.source import canonical_json
from nio.ingest.state import CommitResult, SourceState, StagedFrame
from nio.store import DefaultStore, SqliteStore
from nio.store._sync_journal_plan import _canonical_work_plaintext
from nio.store._sync_journal_rows import _canonical_internal
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus
import nio.store.sync_journal as bootstrap_api
from nio.store.sync_journal import open_ingestion_store
from nio.store.sync_journal_schema import SCHEMA_SQL

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
SLIDING_SOURCE = SlidingSourceConfig(
    timeout_ms=30_000,
    connection_name="worker",
    lists_json=b"{}",
    room_subscriptions_json=b"{}",
    extensions_json=b"{}",
    all_rooms_page_size=2,
)
CONSUMER_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
CRASH_EXIT_CODE = 86
PICKLE_KEY = "secret"
_ADOPTION_COMMON_BOUNDARIES = (
    "before_first_trust_insert",
    "insert_trust_0",
    "insert_trust_1",
    "insert_trust_2",
    "create_meta",
    "insert_meta",
    *(f"schema_{index}" for index in range(len(SCHEMA_SQL))),
    "insert_source",
    "foreign_key_check",
    "before_commit",
    "commit",
)
ADOPTION_BOUNDARIES = (
    *((False, "create_device_trust_state"),),
    *((False, boundary) for boundary in _ADOPTION_COMMON_BOUNDARIES),
    *((True, "delete_legacy_trust"),),
    *((True, boundary) for boundary in _ADOPTION_COMMON_BOUNDARIES),
)
SQLITE_ADOPTION_BOUNDARIES = (
    "create_meta",
    "insert_meta",
    *(f"schema_{index}" for index in range(len(SCHEMA_SQL))),
    "insert_source",
    "foreign_key_check",
    "before_commit",
    "commit",
)
FRESH_STORE_BOUNDARIES = (
    "account_generated",
    "account_pickled",
    "ordinary_schema_accounts",
    "ordinary_schema_devicekeys",
    "ordinary_schema_devicetruststate",
    "ordinary_schema_encryptedrooms",
    "ordinary_schema_megolminboundsessions",
    "ordinary_schema_forwardedchains",
    "ordinary_schema_keys",
    "ordinary_schema_olmsessions",
    "ordinary_schema_outgoingkeyrequests",
    "ordinary_schema_storeversion",
    "ordinary_schema_synctokens",
    "insert_store_version",
    "insert_account",
    "create_meta",
    "insert_meta",
    "schema_0",
    "schema_1",
    "schema_2",
    "schema_3",
    "schema_4",
    "schema_5",
    "schema_6",
    "schema_7",
    "schema_8",
    "insert_source",
    "foreign_key_check",
    "before_commit",
    "commit",
)


def _exit_at_statement(kill_after: int) -> Callable[[str], None]:
    observed = 0

    def exit_process(_label: str) -> None:
        nonlocal observed
        observed += 1
        if observed == kill_after:
            os._exit(CRASH_EXIT_CODE)

    return exit_process


def _assert_process_crashed(
    target: Callable[..., None],
    *args: object,
) -> None:
    process = multiprocessing.get_context("spawn").Process(target=target, args=args)
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("crash-injection child did not exit")
    assert process.exitcode == CRASH_EXIT_CODE


def _kill_during_schema(store_path: Path, kill_after: int) -> None:
    open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
        schema_statement_hook=_exit_at_statement(kill_after),
    )


def _kill_during_configured_adoption(
    store_path: Path,
    boundary: str,
) -> None:
    def kill(label: str) -> None:
        if label == boundary:
            os._exit(CRASH_EXIT_CODE)

    bootstrap_api._open_configured_ingestion_store(
        store_path,
        source_store_class=DefaultStore,
        owned_store_class=SqliteStore,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
        adoption_statement_hook=kill,
    )


def _kill_during_configured_sqlite_adoption(
    store_path: Path,
    boundary: str,
) -> None:
    def kill(label: str) -> None:
        if label == boundary:
            os._exit(CRASH_EXIT_CODE)

    bootstrap_api._open_configured_ingestion_store(
        store_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
        adoption_statement_hook=kill,
    )


def _open_fresh_store(
    store_path: Path,
    hook: Callable[[str], None] | None = None,
):
    return bootstrap_api._open_fresh_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
        fresh_statement_hook=hook,
    )


def _kill_during_fresh_store_creation(store_path: Path, boundary: str) -> None:
    def kill(label: str) -> None:
        if label == boundary:
            os._exit(CRASH_EXIT_CODE)

    _open_fresh_store(store_path, kill)


def _logical_sqlite_graph(database_path: Path) -> tuple[object, ...]:
    with sqlite3.connect(database_path) as connection:
        master = tuple(
            connection.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name"
            )
        )
        tables = tuple(
            row[1]
            for row in connection.execute("PRAGMA table_list")
            if row[0] == "main" and not row[1].startswith("sqlite_")
        )
        rows = tuple(
            (
                table,
                tuple(connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')),
            )
            for table in sorted(tables)
        )
    return master, rows


def _seed_default_adoption_source(
    store_path: Path,
    *,
    legacy_trust: bool,
) -> tuple[bytes | None, ...]:
    store = DefaultStore(
        ACCOUNT_ID,
        DEVICE_ID,
        str(store_path),
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
    )
    store.save_account(OlmAccount())
    devices = {
        state: OlmDevice(
            "@bob:example.org",
            state.name.upper(),
            OlmAccount().identity_keys,
        )
        for state in (TrustState.verified, TrustState.blacklisted, TrustState.ignored)
    }
    store.save_device_keys(
        {"@bob:example.org": {device.id: device for device in devices.values()}}
    )
    store.verify_device(devices[TrustState.verified])
    store.blacklist_device(devices[TrustState.blacklisted])
    store.ignore_device(devices[TrustState.ignored])
    store.database.close()
    if legacy_trust:
        with sqlite3.connect(store_path / "journal.db") as connection:
            connection.execute(
                'CREATE TABLE "devicetruststate" ('
                '"device_id" INTEGER NOT NULL PRIMARY KEY, '
                '"state" INTEGER NOT NULL, FOREIGN KEY ("device_id") '
                'REFERENCES "devicekeys" ("id"))'
            )
            device_key_id = connection.execute(
                "SELECT id FROM devicekeys WHERE device_id = 'VERIFIED'"
            ).fetchone()[0]
            connection.execute(
                "INSERT INTO devicetruststate VALUES (?, ?)",
                (device_key_id, 99),
            )
    return tuple(
        path.read_bytes() if path.exists() else None
        for path in (
            store_path / f"{ACCOUNT_ID}_{DEVICE_ID}.trusted_devices",
            store_path / f"{ACCOUNT_ID}_{DEVICE_ID}.blacklisted_devices",
            store_path / f"{ACCOUNT_ID}_{DEVICE_ID}.ignored_devices",
        )
    )


def _seed_sqlite_adoption_source(store_path: Path) -> None:
    store = SqliteStore(
        ACCOUNT_ID,
        DEVICE_ID,
        str(store_path),
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
    )
    store.save_account(OlmAccount())
    devices = {
        state: OlmDevice(
            "@bob:example.org",
            state.name.upper(),
            OlmAccount().identity_keys,
        )
        for state in (TrustState.verified, TrustState.blacklisted, TrustState.ignored)
    }
    store.save_device_keys(
        {"@bob:example.org": {device.id: device for device in devices.values()}}
    )
    store.verify_device(devices[TrustState.verified])
    store.blacklist_device(devices[TrustState.blacklisted])
    store.ignore_device(devices[TrustState.ignored])
    store.database.close()


def _ordinary_graph_without_trust_or_ingestion(
    graph: tuple[object, ...],
) -> tuple[object, ...]:
    master, rows = graph
    return (
        tuple(
            entry
            for entry in master
            if entry[2] != "devicetruststate" and not entry[2].startswith("NioIngest")
        ),
        tuple(
            entry
            for entry in rows
            if entry[0] != "devicetruststate" and not entry[0].startswith("NioIngest")
        ),
    )


def _graph_without_ingestion(graph: tuple[object, ...]) -> tuple[object, ...]:
    master, rows = graph
    return (
        tuple(entry for entry in master if not entry[2].startswith("NioIngest")),
        tuple(entry for entry in rows if not entry[0].startswith("NioIngest")),
    )


def _open_delivery(store_path: Path, hook: Callable[[str], None] | None = None):
    return open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
        transition_statement_hook=hook,
    )


def _open_sliding(store_path: Path, hook: Callable[[str], None] | None = None):
    return open_ingestion_store(
        store_path,
        source=SLIDING_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
        transition_statement_hook=hook,
    )


def _open_configured_local(store_path: Path):
    return bootstrap_api._open_configured_ingestion_store(
        store_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
    )


def _graph_without_writer_epoch(graph: tuple[object, ...]) -> tuple[object, ...]:
    master, rows = graph
    return (
        master,
        tuple(
            (
                (
                    table,
                    tuple(
                        row[:7] + ("<writer-epoch>",) + row[8:] for row in table_rows
                    ),
                )
                if table == "NioIngestMeta"
                else (table, table_rows)
            )
            for table, table_rows in rows
        ),
    )


def _kill_during_local_membership_publication(
    store_path: Path,
    path: str,
    boundary: str,
    operation_id: UUID,
    sequence_path: Path,
) -> None:
    bootstrap = _open_configured_local(store_path)
    journal = bootstrap._journal

    def kill(label: str) -> None:
        with sequence_path.open("a", encoding="utf-8") as sequence:
            sequence.write(f"{label}\n")
            sequence.flush()
            os.fsync(sequence.fileno())
        if label == boundary:
            os._exit(CRASH_EXIT_CODE)

    journal.set_transition_statement_hook(kill)
    journal._publish_local_membership_transition(
        operation_id=operation_id,
        room_id="!local:example.org",
        previous_membership="leave" if path == "insert" else "join",
        previous_epoch=0,
        current_membership="join" if path == "insert" else "leave",
    )
    bootstrap.close()


def _seed_positioned_sliding(bootstrap) -> None:
    journal = bootstrap._journal
    owner = journal.load_owner()
    prior = journal.load_source()
    adapter = SlidingSource(owner.stream_id, SLIDING_SOURCE, owner.account_id)
    request = adapter.plan_request(prior, prior.next_request_id)
    assert request is not None and request.body is not None
    body = canonical_json(
        {
            "pos": "p1",
            "txn_id": json.loads(request.body)["txn_id"],
            "lists": {RESERVED_ALL_ROOMS_LIST: {"count": 0}},
            "rooms": {},
            "extensions": {"to_device": {"events": [], "next_batch": "td1"}},
        }
    )
    normalized = adapter.normalize(
        request,
        NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            body,
            None,
            None,
        ),
    )
    assert normalized.frame is not None
    journal.stage_source_response(
        source=SourceState(
            prior.source_epoch,
            prior.transport_kind,
            normalized.frame.candidate_cursor_json,
            request.request_id + 1,
            prior.active,
        ),
        frame=StagedFrame(
            normalized.frame.frame_id,
            StagedSourceResponse(
                request,
                normalized.response_body,
                normalized.frame.source_sha256,
            ),
        ),
    )
    assert (
        journal.materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )


def _kill_during_sliding_reopen(store_path: Path, boundary: str) -> None:
    def kill(label: str) -> None:
        if label == boundary:
            os._exit(CRASH_EXIT_CODE)

    _open_sliding(store_path, kill)


def _stored_sliding_reset_graph(
    database_path: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        meta_row = connection.execute("SELECT * FROM NioIngestMeta").fetchone()
        source_row = connection.execute("SELECT * FROM NioIngestSourceState").fetchone()
    assert meta_row is not None and source_row is not None
    meta = dict(meta_row)
    source = dict(source_row)
    payload = source["payload"]
    digest = source["payload_sha256"]
    assert isinstance(payload, bytes) and isinstance(digest, bytes)
    assert hashlib.sha256(payload).digest() == digest
    envelope = json.loads(payload)
    assert isinstance(envelope, dict)
    cursor = envelope["value"]
    assert isinstance(cursor, dict)
    return meta, source, cursor


def _seed_ready(bootstrap) -> EventRecord:
    journal = bootstrap._journal
    owner = journal.load_owner()
    record = EventRecord(
        "00000000-0000-4000-8000-000000000001",
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 0, 0, 0),
        "!room:example.org",
        0,
        0,
        None,
        TimelineEventProvenance.LIVE,
        b"{}",
        None,
    )
    clear = (
        record.record_id,
        "event",
        "ready",
        "00000000-0000-4000-8000-000000000002",
        record.room_id,
        record.membership_epoch,
        record.room_sequence,
        1,
        0,
        1,
    )
    payload, digest = journal._payload(
        owner,
        "NioIngestWork",
        _canonical_work_plaintext("event", record),
        header=_canonical_internal(clear),
    )
    with journal._owner.journal_write():
        journal._execute("UPDATE NioIngestMeta SET revision = 1")
        journal._execute(
            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ACCOUNT_ID, *clear, payload, digest),
        )
    return record


def _kill_during_delivery(store_path: Path, operation: str, boundary: str) -> None:
    bootstrap = _open_delivery(store_path)
    journal = bootstrap._journal
    batch = journal.next_batch() if operation == "ack" else None

    def kill(label: str) -> None:
        if label == boundary:
            os._exit(CRASH_EXIT_CODE)

    journal.set_transition_statement_hook(kill)
    if operation == "claim":
        journal.next_batch()
    else:
        assert batch is not None
        journal.acknowledge_batch(batch.ref)


@pytest.mark.parametrize("kill_after", range(1, len(SCHEMA_SQL) + 4))
def test_fresh_schema_creation_is_atomic_at_every_statement(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"schema-kill-{kill_after}"
    _assert_process_crashed(_kill_during_schema, store_path, kill_after)

    database_path = store_path / "journal.db"
    if database_path.exists():
        import sqlite3

        with sqlite3.connect(database_path) as connection:
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        assert tables == []

    reopened = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
    )
    try:
        assert reopened.schema_version == 1
        assert reopened._journal.load_owner().consumer_generation == CONSUMER_GENERATION
        assert reopened._journal.load_source() == SourceState(
            0,
            TransportKind.CLASSIC,
            b'{"next_batch":null}',
            0,
            True,
        )
    finally:
        reopened.close()


@pytest.mark.parametrize("boundary", FRESH_STORE_BOUNDARIES)
def test_fresh_owned_store_process_death_is_empty_or_complete_and_reopenable(
    tmp_path: Path,
    boundary: str,
) -> None:
    store_path = tmp_path / boundary
    database_path = store_path / "journal.db"
    _assert_process_crashed(_kill_during_fresh_store_creation, store_path, boundary)

    graph = _logical_sqlite_graph(database_path)
    if boundary != "commit":
        assert graph == ((), ())
        retried = _open_fresh_store(store_path)
        try:
            assert retried._owned_store_class is SqliteStore
        finally:
            retried.close()
        assert retried._journal._owner.database.is_closed()
        return

    master, rows = graph
    table_names = {name for kind, name, _table, _sql in master if kind == "table"}
    assert len(table_names) == 16
    assert {
        "NioIngestMeta",
        "NioIngestSourceState",
        "NioIngestFrame",
        "NioIngestRoomAggregate",
        "NioIngestWork",
    } <= table_names
    assert {table: len(table_rows) for table, table_rows in rows if table_rows} == {
        "accounts": 1,
        "storeversion": 1,
        "NioIngestMeta": 1,
        "NioIngestSourceState": 1,
    }
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT version FROM storeversion").fetchall() == [
            (10,)
        ]
        account_rows = connection.execute(
            "SELECT account, user_id, device_id, shared FROM accounts"
        ).fetchall()
        assert len(account_rows) == 1
        raw_pickle = account_rows[0][0]
        assert isinstance(raw_pickle, bytes)
        assert account_rows[0][1:] == (ACCOUNT_ID, DEVICE_ID, 0)
        assert connection.execute("PRAGMA foreign_key_check").fetchall() == []

    generated: list[str] = []
    with pytest.raises(FreshIngestionRequired, match="empty SQLite user graph"):
        _open_fresh_store(store_path, generated.append)
    assert generated == []

    reopened = bootstrap_api._open_configured_ingestion_store(
        store_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
    )
    try:
        assert reopened._owned_store_class is SqliteStore
    finally:
        reopened.close()
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT account FROM accounts").fetchall() == [
            (raw_pickle,)
        ]


@pytest.mark.parametrize(("legacy_trust", "boundary"), ADOPTION_BOUNDARIES)
def test_configured_adoption_process_death_is_exact_old_or_complete_new_graph(
    tmp_path: Path,
    legacy_trust: bool,
    boundary: str,
) -> None:
    topology = "legacy" if legacy_trust else "absent"
    store_path = tmp_path / f"{topology}-{boundary}"
    store_path.mkdir()
    sidecars_before = _seed_default_adoption_source(
        store_path,
        legacy_trust=legacy_trust,
    )
    database_path = store_path / "journal.db"
    graph_before = _logical_sqlite_graph(database_path)

    _assert_process_crashed(_kill_during_configured_adoption, store_path, boundary)
    graph_after = _logical_sqlite_graph(database_path)
    committed = boundary == "commit"
    if not committed:
        assert graph_after == graph_before
        assert not any(
            name.startswith("NioIngest") for _kind, name, _table, _sql in graph_after[0]
        )
    else:
        assert _ordinary_graph_without_trust_or_ingestion(
            graph_after
        ) == _ordinary_graph_without_trust_or_ingestion(graph_before)
        names = {
            name
            for kind, name, _table, _sql in graph_after[0]
            if kind == "table" and name.startswith("NioIngest")
        }
        assert names == {
            "NioIngestMeta",
            "NioIngestSourceState",
            "NioIngestFrame",
            "NioIngestRoomAggregate",
            "NioIngestWork",
        }
        assert "devicetruststate" in {
            name for kind, name, _table, _sql in graph_after[0] if kind == "table"
        }
        with sqlite3.connect(database_path) as connection:
            trust = connection.execute(
                "SELECT d.device_id, t.state FROM devicetruststate AS t "
                "JOIN devicekeys AS d ON d.id = t.device_id ORDER BY d.device_id"
            ).fetchall()
            assert trust == [
                ("BLACKLISTED", TrustState.blacklisted.value),
                ("IGNORED", TrustState.ignored.value),
                ("VERIFIED", TrustState.verified.value),
            ]
            assert connection.execute(
                "SELECT COUNT(*) FROM NioIngestMeta"
            ).fetchone() == (1,)
            assert connection.execute(
                "SELECT COUNT(*) FROM NioIngestSourceState"
            ).fetchone() == (1,)
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
    assert (
        tuple(
            path.read_bytes() if path.exists() else None
            for path in (
                store_path / f"{ACCOUNT_ID}_{DEVICE_ID}.trusted_devices",
                store_path / f"{ACCOUNT_ID}_{DEVICE_ID}.blacklisted_devices",
                store_path / f"{ACCOUNT_ID}_{DEVICE_ID}.ignored_devices",
            )
        )
        == sidecars_before
    )

    if committed:
        reopened = bootstrap_api._open_configured_ingestion_store(
            store_path,
            source_store_class=SqliteStore,
            owned_store_class=SqliteStore,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            pickle_key=PICKLE_KEY,
            database_name="journal.db",
        )
    else:
        reopened = bootstrap_api._open_configured_ingestion_store(
            store_path,
            source_store_class=DefaultStore,
            owned_store_class=SqliteStore,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            pickle_key=PICKLE_KEY,
            database_name="journal.db",
        )
    reopened.close()


@pytest.mark.parametrize("boundary", SQLITE_ADOPTION_BOUNDARIES)
def test_configured_sqlite_adoption_crash_preserves_exact_populated_graph(
    tmp_path: Path,
    boundary: str,
) -> None:
    store_path = tmp_path / boundary
    store_path.mkdir()
    _seed_sqlite_adoption_source(store_path)
    database_path = store_path / "journal.db"
    graph_before = _logical_sqlite_graph(database_path)

    _assert_process_crashed(
        _kill_during_configured_sqlite_adoption,
        store_path,
        boundary,
    )
    graph_after = _logical_sqlite_graph(database_path)
    if boundary == "commit":
        assert _graph_without_ingestion(graph_after) == graph_before
        assert {
            name
            for kind, name, _table, _sql in graph_after[0]
            if kind == "table" and name.startswith("NioIngest")
        } == {
            "NioIngestMeta",
            "NioIngestSourceState",
            "NioIngestFrame",
            "NioIngestRoomAggregate",
            "NioIngestWork",
        }
    else:
        assert graph_after == graph_before

    source_class = SqliteStore
    reopened = bootstrap_api._open_configured_ingestion_store(
        store_path,
        source_store_class=source_class,
        owned_store_class=SqliteStore,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
    )
    reopened.close()


@pytest.mark.parametrize(
    ("operation", "boundary", "committed"),
    (
        ("claim", "delivery_claim_meta_cas", False),
        ("claim", "before_commit", False),
        ("claim", "commit", True),
        ("ack", "delivery_work_delete", False),
        ("ack", "delivery_ack_meta_cas", False),
        ("ack", "before_commit", False),
        ("ack", "commit", True),
    ),
)
def test_delivery_process_death_reopens_only_old_or_complete_new_graph(
    tmp_path: Path,
    operation: str,
    boundary: str,
    committed: bool,
) -> None:
    store_path = tmp_path / f"{operation}-{boundary}"
    bootstrap = _open_delivery(store_path)
    record = _seed_ready(bootstrap)
    claimed = bootstrap._journal.next_batch() if operation == "ack" else None
    bootstrap.close()

    _assert_process_crashed(
        _kill_during_delivery,
        store_path,
        operation,
        boundary,
    )
    with sqlite3.connect(store_path / "journal.db") as connection:
        frontier = connection.execute(
            "SELECT delivery_outstanding_work_id FROM NioIngestMeta"
        ).fetchone()
        work_count = connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()
    assert frontier == (
        (record.record_id,) if committed is (operation == "claim") else (None,)
    )
    assert work_count == ((0,) if committed and operation == "ack" else (1,))

    reopened = _open_delivery(store_path)
    if operation == "claim":
        batch = reopened._journal.next_batch()
        assert batch is not None and batch.records == (record,)
        reopened._journal.acknowledge_batch(batch.ref)
    else:
        assert claimed is not None
        if committed:
            assert reopened._journal.next_batch() is None
        else:
            assert reopened._journal.next_batch() == claimed
        reopened._journal.acknowledge_batch(claimed.ref)
    assert reopened._journal.next_batch() is None
    reopened.close()


@pytest.mark.parametrize(
    ("path", "boundary"),
    (
        ("insert", "aggregate_insert"),
        ("insert", "commit"),
        ("update", "aggregate_update"),
        ("update", "commit"),
    ),
)
def test_local_membership_process_death_retries_only_old_or_complete_new_graph(
    tmp_path: Path,
    path: str,
    boundary: str,
) -> None:
    store_path = tmp_path / f"local-{path}-{boundary}"
    bootstrap = _open_fresh_store(store_path)
    journal = bootstrap._journal
    if path == "update":
        journal._publish_local_membership_transition(
            operation_id=UUID("00000000-0000-4000-8000-000000000001"),
            room_id="!local:example.org",
            previous_membership="leave",
            previous_epoch=0,
            current_membership="join",
        )
        while batch := journal.next_batch():
            journal.acknowledge_batch(batch.ref)
    else:
        assert path == "insert"
    owner_before = journal.load_owner()
    source_before = journal.load_source()
    assert journal.list_frames(1) == ()
    bootstrap.close()

    database_path = store_path / "journal.db"
    graph_before = _logical_sqlite_graph(database_path)
    operation_id = UUID(
        "00000000-0000-4000-8000-000000000010"
        if path == "insert"
        else "00000000-0000-4000-8000-000000000020"
    )
    sequence_path = store_path / "local-publication-hook-sequence.txt"
    _assert_process_crashed(
        _kill_during_local_membership_publication,
        store_path,
        path,
        boundary,
        operation_id,
        sequence_path,
    )
    expected_labels = (
        "meta_revision_epoch_cas",
        f"aggregate_{path}",
        "work_insert",
        "before_commit",
        "commit",
    )
    assert tuple(sequence_path.read_text(encoding="utf-8").splitlines()) == (
        expected_labels[: expected_labels.index(boundary) + 1]
    )

    graph_after = _logical_sqlite_graph(database_path)
    if boundary == "commit":
        assert _graph_without_writer_epoch(graph_after) != (
            _graph_without_writer_epoch(graph_before)
        )
    else:
        assert _graph_without_writer_epoch(graph_after) == (
            _graph_without_writer_epoch(graph_before)
        )

    reopened = _open_configured_local(store_path)
    try:
        reopened_journal = reopened._journal
        reopened_owner = reopened_journal.load_owner()
        expected_revision = owner_before.revision + int(boundary == "commit")
        assert replace(
            reopened_owner,
            writer_epoch=owner_before.writer_epoch,
        ) == replace(owner_before, revision=expected_revision)
        assert reopened_journal.load_source() == source_before
        assert reopened_journal.list_frames(1) == ()
        with reopened_journal._owner.read():
            loaded_before_retry = reopened_journal._load_room_aggregate(
                reopened_owner,
                "!local:example.org",
            )
            inventory_before_retry = reopened_journal._load_task3_work_inventory(
                reopened_owner
            )
        operation_before_retry = tuple(
            item
            for item in inventory_before_retry.work
            if item.frame_id == operation_id
        )
        if boundary == "commit":
            assert loaded_before_retry is not None
            assert len(operation_before_retry) == 1
            committed = CommitResult(owner_before.revision + 1)
            retry_graph = _logical_sqlite_graph(database_path)
            statements: list[str] = []
            reopened_journal._owner.database.connection().set_trace_callback(
                statements.append
            )
            assert (
                reopened_journal._publish_local_membership_transition(
                    operation_id=operation_id,
                    room_id="!local:example.org",
                    previous_membership="leave" if path == "insert" else "join",
                    previous_epoch=0,
                    current_membership="join" if path == "insert" else "leave",
                )
                == committed
            )
            reopened_journal._owner.database.connection().set_trace_callback(None)
            assert not any(
                statement.lstrip()
                .upper()
                .startswith(("INSERT ", "UPDATE ", "DELETE ", "REPLACE "))
                for statement in statements
            )
            assert _logical_sqlite_graph(database_path) == retry_graph
        else:
            assert operation_before_retry == ()
            if path == "insert":
                assert loaded_before_retry is None
            else:
                assert loaded_before_retry is not None
                assert loaded_before_retry[1].continuity.membership == "join"
                assert loaded_before_retry[1].continuity.membership_epoch == 0
                assert loaded_before_retry[1].next_room_sequence == 1
            committed = reopened_journal._publish_local_membership_transition(
                operation_id=operation_id,
                room_id="!local:example.org",
                previous_membership="leave" if path == "insert" else "join",
                previous_epoch=0,
                current_membership="join" if path == "insert" else "leave",
            )
            assert committed == CommitResult(owner_before.revision + 1)

        final_owner = reopened_journal.load_owner()
        assert replace(
            final_owner,
            writer_epoch=owner_before.writer_epoch,
        ) == replace(owner_before, revision=committed.revision)
        with reopened_journal._owner.read():
            aggregate = reopened_journal._load_room_aggregate(
                final_owner,
                "!local:example.org",
            )
            inventory = reopened_journal._load_task3_work_inventory(final_owner)
        assert aggregate is not None
        current_membership = "join" if path == "insert" else "leave"
        current_epoch = 0 if path == "insert" else 1
        room_sequence = 0 if path == "insert" else 1
        assert aggregate[1].continuity.membership == current_membership
        assert aggregate[1].continuity.membership_epoch == current_epoch
        assert aggregate[1].continuity.baseline is None
        assert aggregate[1].continuity.gap is None
        assert aggregate[1].continuity.hydration_id is None
        assert aggregate[1].pending_hydration is None
        assert aggregate[1].room_snapshot is None
        assert aggregate[1].next_room_sequence == room_sequence + 1
        assert aggregate[1].updated_revision == committed.revision
        assert len(inventory.work) == 1
        assert len(inventory.storage_rows) == 1
        operation_work = tuple(
            item for item in inventory.work if item.frame_id == operation_id
        )
        assert len(operation_work) == 1
        work = operation_work[0]
        storage = next(
            row for row in inventory.storage_rows if row[1] == work.value.record_id
        )
        assert storage[3:11] == (
            "ready",
            str(operation_id),
            "!local:example.org",
            current_epoch,
            room_sequence,
            committed.revision,
            0,
            committed.revision,
        )
        assert tuple(
            row[0]
            for row in reopened_journal._owner.database.connection().execute(
                "SELECT room_id FROM NioIngestRoomAggregate ORDER BY room_id"
            )
        ) == ("!local:example.org",)
        assert reopened_journal.load_source() == source_before
        assert reopened_journal.list_frames(1) == ()
    finally:
        reopened._journal._owner.database.connection().set_trace_callback(None)
        reopened.close()


@pytest.mark.parametrize(
    ("boundary", "committed"),
    (
        ("sliding_reset_meta_cas", False),
        ("sliding_reset_source_upsert", False),
        ("before_commit", False),
        ("commit", True),
    ),
)
def test_sliding_reopen_process_death_leaves_only_old_or_complete_new_graph(
    tmp_path: Path,
    boundary: str,
    committed: bool,
) -> None:
    store_path = tmp_path / f"sliding-reset-{boundary}"
    bootstrap = _open_sliding(store_path)
    _seed_positioned_sliding(bootstrap)
    bootstrap.close()
    database_path = store_path / "journal.db"
    meta_before, source_before, cursor_before = _stored_sliding_reset_graph(
        database_path
    )

    _assert_process_crashed(
        _kill_during_sliding_reopen,
        store_path,
        boundary,
    )

    meta_after, source_after, cursor_after = _stored_sliding_reset_graph(database_path)
    if not committed:
        assert meta_after == meta_before
        assert source_after == source_before
        assert cursor_after == cursor_before
        return

    writer_after = meta_after["writer_epoch"]
    assert writer_after != meta_before["writer_epoch"]
    assert meta_after == {
        **meta_before,
        "revision": meta_before["revision"] + 1,  # type: ignore[operator]
        "writer_epoch": writer_after,
        "next_source_epoch": meta_before["next_source_epoch"] + 1,  # type: ignore[operator]
    }
    assert source_after["account_id"] == source_before["account_id"]
    assert source_after["source_epoch"] == meta_before["next_source_epoch"]
    assert source_after["next_request_id"] == 0
    assert source_after["active"] == source_before["active"]
    assert cursor_after == {
        **cursor_before,
        "pos": None,
        "connection_instance": cursor_after["connection_instance"],
        "all_rooms_range_ack_mode": "unknown",
        "all_rooms_coverage_complete": False,
    }
    assert cursor_after["connection_instance"] != cursor_before["connection_instance"]
