from dataclasses import replace
import sqlite3
import inspect
import os
from pathlib import Path
from uuid import UUID

import pytest

from nio.crypto import OlmAccount, OlmDevice, TrustState
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig
from nio.ingest.errors import FreshIngestionRequired, JournalIntegrityError
from nio.ingest.ports import NetworkResult, StagedSourceResponse
from nio.ingest.serialization import batch_from_records
from nio.ingest.source import canonical_json
from nio.ingest.state import SourceState, StagedFrame
from nio.exceptions import LocalProtocolError
from nio.store import DefaultStore, SqliteStore
from nio.store._sync_journal_rows import (
    _canonical_internal,
    _canonical_room_aggregate_plaintext,
    _frame_drain_sha256,
    _frame_payload,
    _source_header,
)
from nio.store._sync_journal_plan import _canonical_work_plaintext
from nio.store._sync_journal_preflight import _row
from nio.store._sync_journal_values import MaterializerLimits
import nio.store.sync_journal as bootstrap_api
from nio.store.sync_journal import open_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
CONSUMER_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
PICKLE_KEY = "secret"
BOB_ID = "@bob:example.org"
FRESH_BOUNDARIES = (
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
    "ordinary_schema_pendingtimelineevents",
    "ordinary_schema_slidingwindowtokens",
    "ordinary_schema_storeversion",
    "ordinary_schema_syncrecoveryabandonedrooms",
    "ordinary_schema_syncrecoverygaps",
    "ordinary_schema_synctokens",
    "insert_store_version",
    "insert_account",
    "create_meta",
    "insert_meta",
    *(f"schema_{index}" for index in range(9)),
    "insert_source",
    "foreign_key_check",
    "before_commit",
    "commit",
)


def _seed_populated_store(
    tmp_path: Path,
    store_class: type[DefaultStore] | type[SqliteStore],
) -> tuple[dict[str, OlmDevice], dict[str, bytes]]:
    store = store_class(
        ACCOUNT_ID,
        DEVICE_ID,
        str(tmp_path),
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
    )
    store.save_account(OlmAccount())
    devices = {
        name: OlmDevice(BOB_ID, name, OlmAccount().identity_keys)
        for name in ("VERIFIED", "BLACKLISTED", "IGNORED", "UNSET")
    }
    store.save_device_keys({BOB_ID: {device.id: device for device in devices.values()}})
    store.verify_device(devices["VERIFIED"])
    store.blacklist_device(devices["BLACKLISTED"])
    store.ignore_device(devices["IGNORED"])
    store.database.close()
    sidecars = {
        suffix: (tmp_path / f"{ACCOUNT_ID}_{DEVICE_ID}.{suffix}").read_bytes()
        for suffix in ("trusted_devices", "blacklisted_devices", "ignored_devices")
        if (tmp_path / f"{ACCOUNT_ID}_{DEVICE_ID}.{suffix}").exists()
    }
    return devices, sidecars


def _configured_open(
    tmp_path: Path,
    source_store_class: type[DefaultStore] | type[SqliteStore],
    **kwargs: object,
):
    return bootstrap_api._open_configured_ingestion_store(
        tmp_path,
        source_store_class=source_store_class,
        owned_store_class=SqliteStore,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
        **kwargs,
    )


def _table_names(path: Path) -> set[str]:
    with sqlite3.connect(path) as connection:
        return {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }


def _fresh_open(tmp_path: Path, **kwargs: object):
    return bootstrap_api._open_fresh_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        source=CLASSIC_SOURCE,
        pickle_key=PICKLE_KEY,
        database_name="journal.db",
        **kwargs,
    )


def test_fresh_owned_store_bootstrap_is_one_atomic_full_graph(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    database_path = tmp_path / "journal.db"
    pickle_calls: list[tuple[dict[str, str], bool, str, bytes]] = []
    real_pickle = OlmAccount.pickle
    boundaries: list[str] = []

    def recording_pickle(account: OlmAccount, pickle_key: str = "") -> bytes:
        pickled = real_pickle(account, pickle_key)
        pickle_calls.append(
            (dict(account.identity_keys), account.shared, pickle_key, pickled)
        )
        return pickled

    monkeypatch.setattr(OlmAccount, "pickle", recording_pickle)
    bootstrap = _fresh_open(tmp_path, fresh_statement_hook=boundaries.append)

    assert _table_names(database_path) == {
        "accounts",
        "devicekeys",
        "devicetruststate",
        "encryptedrooms",
        "forwardedchains",
        "keys",
        "megolminboundsessions",
        "olmsessions",
        "outgoingkeyrequests",
        "pendingtimelineevents",
        "slidingwindowtokens",
        "storeversion",
        "syncrecoveryabandonedrooms",
        "syncrecoverygaps",
        "synctokens",
        "NioIngestMeta",
        "NioIngestSourceState",
        "NioIngestFrame",
        "NioIngestRoomAggregate",
        "NioIngestWork",
    }
    assert len(pickle_calls) == 1
    identity_keys, shared, used_key, pickled = pickle_calls[0]
    assert shared is False
    assert used_key == PICKLE_KEY
    with bootstrap._journal._owner.read():
        connection = bootstrap._journal._owner.database
        assert [
            tuple(row)
            for row in connection.execute_sql(
                "SELECT version FROM storeversion"
            ).fetchall()
        ] == [(10,)]
        assert [
            tuple(row)
            for row in connection.execute_sql(
                "SELECT user_id, device_id, shared, account FROM accounts"
            ).fetchall()
        ] == [(ACCOUNT_ID, DEVICE_ID, 0, pickled)]
        assert connection.execute_sql("PRAGMA foreign_key_check").fetchall() == []
    restored = OlmAccount.from_pickle(pickled, PICKLE_KEY, False)
    assert restored.identity_keys == identity_keys
    assert not tuple(tmp_path.glob(f"{ACCOUNT_ID}_{DEVICE_ID}.*_devices"))
    assert tuple(boundaries) == FRESH_BOUNDARIES
    bootstrap.close()


def test_fresh_private_signature_and_exports_are_exact() -> None:
    import nio
    from nio import ingest
    import nio.store as store_package

    parameters = inspect.signature(bootstrap_api._open_fresh_ingestion_store).parameters
    assert tuple(parameters) == (
        "store_path",
        "account_id",
        "device_id",
        "consumer_generation",
        "source",
        "pickle_key",
        "database_name",
        "sqlite_busy_timeout_ms",
        "statement_observer",
        "transition_statement_hook",
        "fresh_statement_hook",
    )
    assert parameters["fresh_statement_hook"].default is None
    assert not hasattr(nio, "_open_fresh_ingestion_store")
    assert not hasattr(ingest, "_open_fresh_ingestion_store")
    assert not hasattr(store_package, "_open_fresh_ingestion_store")


@pytest.mark.parametrize("residue", ["zero_length", "sqlite_header"])
def test_fresh_owned_store_retries_empty_database_residue(
    tmp_path: Path,
    residue: str,
) -> None:
    database_path = tmp_path / "journal.db"
    if residue == "zero_length":
        database_path.touch()
    else:
        with sqlite3.connect(database_path) as connection:
            connection.execute("VACUUM")
        assert database_path.stat().st_size > 0
        with sqlite3.connect(database_path) as connection:
            assert connection.execute("SELECT * FROM sqlite_master").fetchall() == []

    bootstrap = _fresh_open(tmp_path)
    assert len(_table_names(database_path)) == 20
    bootstrap.close()


@pytest.mark.parametrize(
    "ddl",
    (
        ("CREATE TABLE stray(value INTEGER)",),
        ("CREATE VIEW stray AS SELECT 1 AS value",),
        (
            "CREATE TABLE stray(value INTEGER)",
            "CREATE INDEX stray_index ON stray(value)",
        ),
        (
            "CREATE TABLE stray(value INTEGER)",
            "CREATE TRIGGER stray_trigger AFTER INSERT ON stray BEGIN "
            "UPDATE stray SET value = value; END",
        ),
        (
            "CREATE TABLE transient(id INTEGER PRIMARY KEY AUTOINCREMENT)",
            "DROP TABLE transient",
        ),
    ),
)
def test_fresh_owned_store_rejects_any_committed_object_before_generation_or_dml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    ddl: tuple[str, ...],
) -> None:
    database_path = tmp_path / "journal.db"
    with sqlite3.connect(database_path) as connection:
        for statement in ddl:
            connection.execute(statement)
    with sqlite3.connect(database_path) as connection:
        before = tuple(connection.execute("SELECT * FROM sqlite_master ORDER BY name"))
    generations = 0
    real_init = OlmAccount.__init__

    def counting_init(self, account=None):
        nonlocal generations
        if account is None:
            generations += 1
        real_init(self, account)

    monkeypatch.setattr(OlmAccount, "__init__", counting_init)
    statements: list[str] = []
    with pytest.raises(FreshIngestionRequired, match="empty SQLite"):
        _fresh_open(tmp_path, statement_observer=statements.append)
    with sqlite3.connect(database_path) as connection:
        after = tuple(connection.execute("SELECT * FROM sqlite_master ORDER BY name"))
    assert after == before
    assert generations == 0
    assert not any(
        statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in statements
    )


@pytest.mark.parametrize("boundary", FRESH_BOUNDARIES)
def test_fresh_owned_store_hook_failure_is_all_absent_or_complete(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    database_path = tmp_path / "journal.db"
    generations = 0
    real_init = OlmAccount.__init__

    def counting_init(self, account=None):
        nonlocal generations
        if account is None:
            generations += 1
        real_init(self, account)

    def fail_at_boundary(label: str) -> None:
        if label == boundary:
            raise RuntimeError(f"injected at {label}")

    monkeypatch.setattr(OlmAccount, "__init__", counting_init)
    with pytest.raises(RuntimeError, match=f"injected at {boundary}"):
        _fresh_open(tmp_path, fresh_statement_hook=fail_at_boundary)

    with sqlite3.connect(database_path) as connection:
        master = tuple(connection.execute("SELECT * FROM sqlite_master"))
    if boundary != "commit":
        assert master == ()
        retried = _fresh_open(tmp_path)
        assert len(_table_names(database_path)) == 20
        retried.close()
        return

    assert len(_table_names(database_path)) == 20
    with sqlite3.connect(database_path) as connection:
        account_before = connection.execute(
            "SELECT account, shared FROM accounts"
        ).fetchone()
    assert account_before is not None and account_before[1] == 0
    with pytest.raises(FreshIngestionRequired, match="empty SQLite"):
        _fresh_open(tmp_path)
    assert generations == 1
    reopened = _configured_open(tmp_path, SqliteStore)
    with sqlite3.connect(database_path) as connection:
        assert (
            connection.execute("SELECT account, shared FROM accounts").fetchone()
            == account_before
        )
    reopened.close()


def _logical_graph(path: Path) -> tuple[object, ...]:
    with sqlite3.connect(path) as connection:
        master = tuple(
            connection.execute(
                "SELECT type, name, tbl_name, sql FROM sqlite_master "
                "WHERE name NOT GLOB 'sqlite_*' ORDER BY type, name"
            )
        )
        tables = tuple(
            sorted(
                row[1]
                for row in connection.execute("PRAGMA table_list")
                if row[0] == "main" and not row[1].startswith("sqlite_")
            )
        )
        rows = tuple(
            (
                table,
                tuple(connection.execute(f'SELECT * FROM "{table}" ORDER BY rowid')),
            )
            for table in tables
        )
    return master, rows


def _stage_classic(
    journal,
    sequence: int,
    *,
    crypto: bool = False,
    global_count: int = 0,
    room: bool = False,
) -> StagedFrame:
    owner = journal.load_owner()
    prior = journal.load_source()
    adapter = ClassicSource(owner.stream_id, CLASSIC_SOURCE, ACCOUNT_ID)
    request = adapter.plan_request(prior, prior.next_request_id)
    assert request is not None
    body: dict[str, object] = {"next_batch": f"s{sequence}"}
    if crypto:
        body["to_device"] = {
            "events": [
                {
                    "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                    "type": "m.room_key",
                }
            ]
        }
    if global_count:
        body["account_data"] = {
            "events": [
                {
                    "content": {"index": index, "sequence": sequence},
                    "type": "m.push_rules",
                }
                for index in range(global_count)
            ]
        }
    if room:
        body["rooms"] = {
            "join": {
                "!room:example.org": {
                    "timeline": {
                        "events": [
                            {
                                "content": {"body": "hello", "msgtype": "m.text"},
                                "event_id": f"$event-{sequence}",
                                "sender": BOB_ID,
                                "type": "m.room.message",
                            }
                        ]
                    }
                }
            }
        }
    normalized = adapter.normalize(
        request,
        NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            canonical_json(body),
            None,
            None,
        ),
    )
    assert normalized.frame is not None
    frame = StagedFrame(
        normalized.frame.frame_id,
        StagedSourceResponse(
            request,
            normalized.response_body,
            normalized.frame.source_sha256,
        ),
    )
    committed = journal.stage_source_response(
        source=SourceState(
            prior.source_epoch,
            prior.transport_kind,
            normalized.frame.candidate_cursor_json,
            request.request_id + 1,
            prior.active,
        ),
        frame=frame,
    )
    return replace(frame, staged_revision=committed.revision)


def _assert_marked_reopen_rejects_before_rotation(
    tmp_path: Path,
    expected: str,
) -> None:
    database_path = tmp_path / "journal.db"
    with sqlite3.connect(database_path) as connection:
        writer_before = connection.execute(
            "SELECT writer_epoch FROM NioIngestMeta"
        ).fetchone()[0]
    statements: list[str] = []
    with pytest.raises(JournalIntegrityError, match=expected):
        _configured_open(
            tmp_path,
            SqliteStore,
            statement_observer=statements.append,
        )
    with sqlite3.connect(database_path) as connection:
        assert connection.execute(
            "SELECT writer_epoch FROM NioIngestMeta"
        ).fetchone() == (writer_before,)
    assert not any(
        statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in statements
    )


def test_store_public_surface_exposes_only_ingestion_bootstrap() -> None:
    import nio.store as store_api
    import nio.store.sync_journal as bootstrap_api

    assert store_api.StoreBootstrap is not None
    assert store_api.open_ingestion_store is not None
    assert not hasattr(store_api, "EncryptedRowCodec")
    assert not hasattr(store_api, "IngestionJournal")
    assert not hasattr(store_api, "SqliteIngestionJournal")
    assert not hasattr(store_api, "_open_configured_ingestion_store")
    assert not hasattr(bootstrap_api, "SqliteIngestionJournal")


def test_public_ingestion_opener_signature_remains_literal() -> None:
    parameters = inspect.signature(open_ingestion_store).parameters
    assert tuple(parameters) == (
        "store_path",
        "account_id",
        "device_id",
        "consumer_generation",
        "source",
        "pickle_key",
        "database_name",
        "sqlite_busy_timeout_ms",
        "statement_observer",
        "transition_statement_hook",
        "schema_statement_hook",
    )
    assert parameters["store_path"].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(
        parameters[name].kind is inspect.Parameter.KEYWORD_ONLY
        for name in tuple(parameters)[1:]
    )
    assert {
        name: parameters[name].default
        for name in (
            "pickle_key",
            "database_name",
            "sqlite_busy_timeout_ms",
            "statement_observer",
            "transition_statement_hook",
            "schema_statement_hook",
        )
    } == {
        "pickle_key": "",
        "database_name": "",
        "sqlite_busy_timeout_ms": 2_000,
        "statement_observer": None,
        "transition_statement_hook": None,
        "schema_statement_hook": None,
    }


def test_bootstrap_opens_only_the_matrix_store_e2ee_subset(tmp_path: Path) -> None:
    database_path = tmp_path / "journal.db"
    bootstrap = open_ingestion_store(
        tmp_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        database_name=database_path.name,
    )

    store = bootstrap.open_matrix_store(SqliteStore)
    try:
        tables = _table_names(database_path)
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
        bootstrap.close()

    assert store.database.is_closed()


def test_configured_default_store_adoption_copies_effective_trust_in_place(
    tmp_path: Path,
) -> None:
    devices, sidecars = _seed_populated_store(tmp_path, DefaultStore)
    database_path = tmp_path / "journal.db"
    with sqlite3.connect(database_path) as connection:
        before_account = connection.execute("SELECT * FROM accounts").fetchall()
        before_keys = connection.execute("SELECT * FROM devicekeys").fetchall()
        assert "devicetruststate" not in {
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            )
        }

    bootstrap = _configured_open(tmp_path, DefaultStore)
    try:
        assert bootstrap._owned_store_class is SqliteStore
        assert bootstrap.schema_version == 1
        with sqlite3.connect(database_path) as connection:
            rows = connection.execute(
                "SELECT d.device_id, t.state FROM devicetruststate AS t "
                "JOIN devicekeys AS d ON d.id = t.device_id ORDER BY d.device_id"
            ).fetchall()
            assert rows == [
                ("BLACKLISTED", TrustState.blacklisted.value),
                ("IGNORED", TrustState.ignored.value),
                ("VERIFIED", TrustState.verified.value),
            ]
            assert (
                connection.execute("SELECT * FROM accounts").fetchall()
                == before_account
            )
            assert (
                connection.execute("SELECT * FROM devicekeys").fetchall() == before_keys
            )
            assert connection.execute("PRAGMA foreign_key_check").fetchall() == []
        assert {
            suffix: (tmp_path / f"{ACCOUNT_ID}_{DEVICE_ID}.{suffix}").read_bytes()
            for suffix in sidecars
        } == sidecars
        assert set(devices) == {"VERIFIED", "BLACKLISTED", "IGNORED", "UNSET"}
    finally:
        bootstrap.close()


def test_configured_sqlite_store_adoption_and_marked_reopen_preserve_trust(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    database_path = tmp_path / "journal.db"
    with sqlite3.connect(database_path) as connection:
        trust_before = connection.execute(
            "SELECT * FROM devicetruststate ORDER BY device_id"
        ).fetchall()

    adopted = _configured_open(tmp_path, SqliteStore)
    adopted.close()
    reopened = _configured_open(tmp_path, SqliteStore)
    try:
        assert reopened._owned_store_class is SqliteStore
        with sqlite3.connect(database_path) as connection:
            assert (
                connection.execute(
                    "SELECT * FROM devicetruststate ORDER BY device_id"
                ).fetchall()
                == trust_before
            )
    finally:
        reopened.close()


def test_configured_marked_reopen_authenticates_frame_revision_order_before_dml(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    bootstrap = _configured_open(tmp_path, SqliteStore)
    journal = bootstrap._journal
    owner = journal.load_owner()
    frames = (_stage_classic(journal, 1), _stage_classic(journal, 2))
    bootstrap.close()

    bound = owner.account_id, owner.stream_id, owner.transport_kind
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute("UPDATE NioIngestMeta SET revision = 3")
        for frame, new_revision in zip(
            frames,
            (3, 2),
            strict=True,
        ):
            payload, digest = _frame_payload(frame, new_revision, bound)
            request = frame.response.request
            proof = _frame_drain_sha256(
                owner,
                frame_id=frame.frame_id,
                source_epoch=request.source_epoch,
                request_id=request.request_id,
                staged_revision=new_revision,
                payload_sha256=digest,
                payload_length=len(payload),
                room_materialized_revision=None,
                callbacks_claimed_revision=None,
            )
            connection.execute(
                "UPDATE NioIngestFrame SET staged_revision = ?, payload = ?, "
                "payload_sha256 = ?, drain_header_sha256 = ? WHERE frame_id = ?",
                (new_revision, payload, digest, proof, str(frame.frame_id)),
            )

    _assert_marked_reopen_rejects_before_rotation(
        tmp_path,
        "persisted Frame revision order is invalid",
    )


def test_configured_marked_reopen_authenticates_frame_cursor_tail_before_dml(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    bootstrap = _configured_open(tmp_path, SqliteStore)
    journal = bootstrap._journal
    owner = journal.load_owner()
    _stage_classic(journal, 1)
    source = replace(journal.load_source(), cursor_json=b'{"next_batch":"other"}')
    bootstrap.close()

    bound = owner.account_id, owner.stream_id, owner.transport_kind
    payload, digest = _row(
        bound,
        "NioIngestSourceState",
        source.cursor_json,
        header=_source_header(source),
    )
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute(
            "UPDATE NioIngestSourceState SET payload = ?, payload_sha256 = ?",
            (payload, digest),
        )

    _assert_marked_reopen_rejects_before_rotation(
        tmp_path,
        "persisted Frame tail does not match Source",
    )


def test_configured_marked_reopen_binds_work_to_retained_frame_before_dml(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    bootstrap = _configured_open(tmp_path, SqliteStore)
    journal = bootstrap._journal
    _stage_classic(journal, 1, crypto=True, global_count=1)
    journal.materialize_oldest_frame(limits=MaterializerLimits())
    owner = journal.load_owner()
    inventory = journal._load_task3_work_inventory(owner)
    assert len(inventory.storage_rows) >= 1
    index = next(
        index
        for index, row in enumerate(inventory.storage_rows)
        if row[3] == "ready" and row[10] > 1
    )
    row = list(inventory.storage_rows[index])
    value = inventory.work[index].value
    row[10] -= 1
    plaintext = _canonical_work_plaintext(row[2], value)
    payload, digest = _row(
        (owner.account_id, owner.stream_id, owner.transport_kind),
        "NioIngestWork",
        plaintext,
        header=_canonical_internal(row[1:11]),
    )
    bootstrap.close()

    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute(
            "UPDATE NioIngestWork SET created_revision = ?, payload = ?, "
            "payload_sha256 = ? WHERE work_id = ?",
            (row[10], payload, digest, row[1]),
        )

    _assert_marked_reopen_rejects_before_rotation(
        tmp_path,
        "Work does not match retained Frame",
    )


def test_configured_marked_reopen_binds_held_work_to_aggregate_before_dml(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    bootstrap = _configured_open(tmp_path, SqliteStore)
    journal = bootstrap._journal
    _stage_classic(journal, 1, room=True)
    journal.materialize_oldest_frame(limits=MaterializerLimits())
    owner = journal.load_owner()
    inventory = journal._load_task3_work_inventory(owner)
    assert [row[3] for row in inventory.storage_rows] == ["held"]
    loaded = journal._load_room_aggregate(owner, "!room:example.org")
    assert loaded is not None
    aggregate = loaded[1]
    assert aggregate.pending_hydration is not None
    clean = replace(
        aggregate,
        continuity=replace(aggregate.continuity, hydration_id=None),
        pending_hydration=None,
    )
    plaintext = _canonical_room_aggregate_plaintext(clean)
    payload, digest = _row(
        (owner.account_id, owner.stream_id, owner.transport_kind),
        "NioIngestRoomAggregate",
        plaintext,
        header=_canonical_internal(
            [clean.continuity.room_id, clean.updated_revision, None]
        ),
    )
    bootstrap.close()

    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute(
            "UPDATE NioIngestRoomAggregate SET intent_kind = NULL, payload = ?, "
            "payload_sha256 = ? WHERE room_id = ?",
            (payload, digest, clean.continuity.room_id),
        )

    _assert_marked_reopen_rejects_before_rotation(
        tmp_path,
        "HELD Work does not match Aggregate frontier",
    )


@pytest.mark.parametrize("fault", ("fifo", "digest"))
def test_configured_marked_reopen_authenticates_outstanding_batch_before_dml(
    tmp_path: Path,
    fault: str,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    bootstrap = _configured_open(tmp_path, SqliteStore)
    journal = bootstrap._journal
    _stage_classic(journal, 1, global_count=2)
    journal.materialize_oldest_frame(limits=MaterializerLimits())
    claimed = journal.next_batch()
    assert claimed is not None
    owner = journal.load_owner()
    inventory = journal._load_task3_work_inventory(owner)
    ready = sorted(
        (
            (row[8], row[9], row[1]),
            row,
            authenticated.value,
        )
        for row, authenticated in zip(
            inventory.storage_rows, inventory.work, strict=True
        )
        if row[3] == "ready"
    )
    assert len(ready) == 2
    if fault == "fifo":
        key, _row_value, value = ready[1]
        work_id = key[2]
        ready_revision = key[0]
        ready_ordinal = key[1]
        digest = batch_from_records(
            account_id=owner.account_id,
            device_id=owner.device_id,
            consumer_generation=owner.consumer_generation,
            stream_id=owner.stream_id,
            sequence=claimed.ref.sequence,
            created_revision=ready_revision,
            records=(value,),
        ).ref.sha256
        expected = "claimed Work is not the FIFO READY member"
    else:
        work_id = ready[0][0][2]
        ready_revision = ready[0][0][0]
        ready_ordinal = ready[0][0][1]
        digest = bytes(byte ^ 0xFF for byte in claimed.ref.sha256)
        expected = "claimed Work does not match batch digest"
    bootstrap.close()

    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute(
            "UPDATE NioIngestMeta SET delivery_outstanding_work_id = ?, "
            "delivery_outstanding_ready_revision = ?, "
            "delivery_outstanding_ready_ordinal = ?, "
            "delivery_outstanding_batch_sha256 = ?",
            (work_id, ready_revision, ready_ordinal, digest),
        )

    _assert_marked_reopen_rejects_before_rotation(tmp_path, expected)


@pytest.mark.parametrize("fault", ("sequence", "ready_revision"))
def test_configured_marked_reopen_authenticates_delivery_frontier_before_dml(
    tmp_path: Path,
    fault: str,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    bootstrap = _configured_open(tmp_path, SqliteStore)
    journal = bootstrap._journal
    _stage_classic(journal, 1, global_count=1)
    journal.materialize_oldest_frame(limits=MaterializerLimits())
    batch = journal.next_batch()
    assert batch is not None
    if fault == "sequence":
        journal.acknowledge_batch(batch.ref)
    owner = journal.load_owner()
    bootstrap.close()

    column = (
        "delivery_next_sequence"
        if fault == "sequence"
        else "delivery_outstanding_ready_revision"
    )
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute(
            f"UPDATE NioIngestMeta SET {column} = ?",
            (owner.revision + 1 if fault == "sequence" else owner.revision,),
        )

    _assert_marked_reopen_rejects_before_rotation(
        tmp_path,
        "persisted delivery state is invalid",
    )


def test_configured_store_class_pairings_reject_before_target_sql(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, DefaultStore)
    public_signature = inspect.signature(open_ingestion_store)

    class DefaultSubclass(DefaultStore):
        pass

    for source_class, owned_class in (
        (DefaultStore, DefaultStore),
        (SqliteStore, DefaultStore),
        (DefaultSubclass, SqliteStore),
        (object, SqliteStore),
    ):
        statements: list[str] = []
        with pytest.raises(LocalProtocolError):
            bootstrap_api._open_configured_ingestion_store(
                tmp_path,
                source_store_class=source_class,
                owned_store_class=owned_class,
                source=CLASSIC_SOURCE,
                account_id=ACCOUNT_ID,
                device_id=DEVICE_ID,
                consumer_generation=CONSUMER_GENERATION,
                pickle_key=PICKLE_KEY,
                database_name="journal.db",
                statement_observer=statements.append,
            )
        assert statements == []

    assert inspect.signature(open_ingestion_store) == public_signature
    with pytest.raises(FreshIngestionRequired):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            pickle_key=PICKLE_KEY,
            database_name="journal.db",
        )


def test_configured_subclass_rejects_before_any_filesystem_mutation(
    tmp_path: Path,
) -> None:
    class SqliteSubclass(SqliteStore):
        pass

    target = tmp_path / "missing" / "nested"
    with pytest.raises(LocalProtocolError):
        bootstrap_api._open_configured_ingestion_store(
            target,
            source_store_class=SqliteSubclass,
            owned_store_class=SqliteStore,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            pickle_key=PICKLE_KEY,
            database_name="journal.db",
        )
    assert not (tmp_path / "missing").exists()


def test_configured_class_equality_spoof_rejects_before_filesystem_mutation(
    tmp_path: Path,
) -> None:
    class SpoofingMeta(type):
        def __eq__(cls, _other: object) -> bool:
            return True

    class SpoofedStore(metaclass=SpoofingMeta):
        pass

    target = tmp_path / "missing" / "nested"
    statements: list[str] = []
    with pytest.raises(LocalProtocolError, match="class pairing"):
        bootstrap_api._open_configured_ingestion_store(
            target,
            source_store_class=SpoofedStore,
            owned_store_class=SqliteStore,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            pickle_key=PICKLE_KEY,
            database_name="journal.db",
            statement_observer=statements.append,
        )
    assert statements == []
    assert not (tmp_path / "missing").exists()


@pytest.mark.parametrize(
    "fault",
    (
        "wrong_account",
        "wrong_device",
        "wrong_pickle_key",
        "corrupt_pickle",
        "non_v10",
        "ordinary_fk",
        "partial_ingestion",
        "sqlite_missing_trust",
        "marked_wrong_pickle_key",
        "marked_fk",
        "marked_default_repeat",
    ),
)
def test_configured_preflight_negatives_emit_no_dml_and_preserve_graph(
    tmp_path: Path,
    fault: str,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    marked = fault.startswith("marked_")
    if marked:
        adopted = _configured_open(tmp_path, SqliteStore)
        adopted.close()

    database_path = tmp_path / "journal.db"
    account_id = ACCOUNT_ID
    device_id = DEVICE_ID
    pickle_key = PICKLE_KEY
    source_store_class = SqliteStore
    with sqlite3.connect(database_path) as connection:
        if fault == "wrong_account":
            account_id = "@mallory:example.org"
        elif fault == "wrong_device":
            device_id = "OTHER"
        elif fault in {"wrong_pickle_key", "marked_wrong_pickle_key"}:
            pickle_key = "wrong-key"
        elif fault == "corrupt_pickle":
            connection.execute("UPDATE accounts SET account = ?", (b"invalid",))
        elif fault == "non_v10":
            connection.execute("UPDATE storeversion SET version = 9")
        elif fault in {"ordinary_fk", "marked_fk"}:
            connection.execute("PRAGMA foreign_keys = OFF")
            connection.execute(
                "INSERT INTO keys (key_type, key, device_id) VALUES (?, ?, ?)",
                ("orphan", "orphan", 999_999),
            )
        elif fault == "partial_ingestion":
            connection.execute("CREATE TABLE NioIngestFrame (partial INTEGER)")
        elif fault == "sqlite_missing_trust":
            connection.execute("DROP TABLE devicetruststate")
        elif fault == "marked_default_repeat":
            source_store_class = DefaultStore

    before = _logical_graph(database_path)
    statements: list[str] = []
    with pytest.raises((FreshIngestionRequired, LocalProtocolError)):
        bootstrap_api._open_configured_ingestion_store(
            tmp_path,
            source_store_class=source_store_class,
            owned_store_class=SqliteStore,
            source=CLASSIC_SOURCE,
            account_id=account_id,
            device_id=device_id,
            consumer_generation=CONSUMER_GENERATION,
            pickle_key=pickle_key,
            database_name=database_path.name,
            statement_observer=statements.append,
        )
    assert _logical_graph(database_path) == before
    assert not any(
        statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for statement in statements
    )


def test_configured_missing_store_creates_no_directory_or_lock(tmp_path: Path) -> None:
    missing = tmp_path / "missing"
    with pytest.raises(FreshIngestionRequired):
        _configured_open(missing, SqliteStore)
    assert not missing.exists()


def test_public_marked_populated_store_reopen_remains_fail_closed(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    adopted = _configured_open(tmp_path, SqliteStore)
    adopted.close()
    with pytest.raises(FreshIngestionRequired):
        open_ingestion_store(
            tmp_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            pickle_key=PICKLE_KEY,
            database_name="journal.db",
        )


@pytest.mark.parametrize("extra_kind", ("trigger", "view"))
def test_configured_adoption_rejects_unexpected_ordinary_schema_objects(
    tmp_path: Path,
    extra_kind: str,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        if extra_kind == "trigger":
            connection.execute(
                "CREATE TRIGGER unexpected AFTER INSERT ON devicetruststate BEGIN "
                "SELECT 1; END"
            )
        else:
            connection.execute("CREATE VIEW unexpected AS SELECT * FROM accounts")
    with pytest.raises(FreshIngestionRequired, match="topology"):
        _configured_open(tmp_path, SqliteStore)


def test_configured_adoption_accepts_exact_historical_v10_alter_layouts(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute("PRAGMA foreign_keys = OFF")
        connection.execute("DROP TABLE pendingtimelineevents")
        connection.execute(
            'CREATE TABLE "pendingtimelineevents" ('
            '"id" INTEGER NOT NULL PRIMARY KEY, "room_id" TEXT NOT NULL, '
            '"generation" INTEGER NOT NULL, "sequence" INTEGER NOT NULL, '
            '"event_id" TEXT NOT NULL, "event_payload" BLOB NOT NULL, '
            '"is_live" INTEGER NOT NULL, "was_encrypted" INTEGER NOT NULL, '
            '"was_completed" INTEGER NOT NULL, "account_id" INTEGER NOT NULL, '
            'FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") '
            "ON DELETE CASCADE, UNIQUE(account_id,room_id,event_id))"
        )
        connection.execute(
            "ALTER TABLE pendingtimelineevents ADD COLUMN "
            '"admission_accepted" INTEGER NOT NULL DEFAULT 0'
        )
        connection.execute(
            "ALTER TABLE pendingtimelineevents ADD COLUMN "
            "\"provenance\" TEXT NOT NULL DEFAULT 'live'"
        )
        connection.execute(
            "ALTER TABLE pendingtimelineevents ADD COLUMN "
            '"apply_room_state" INTEGER NOT NULL DEFAULT 1'
        )
        connection.execute(
            "CREATE INDEX pendingtimelineevents_account_id "
            "ON pendingtimelineevents (account_id)"
        )
        connection.execute("DROP TABLE syncrecoverygaps")
        connection.execute(
            'CREATE TABLE "syncrecoverygaps" ('
            '"id" INTEGER NOT NULL PRIMARY KEY, "room_id" TEXT NOT NULL, '
            '"generation" INTEGER NOT NULL, "target_token" TEXT NOT NULL, '
            '"cursor_token" TEXT, "account_id" INTEGER NOT NULL, '
            'FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") '
            "ON DELETE CASCADE, UNIQUE(account_id,room_id,generation))"
        )
        connection.execute(
            "ALTER TABLE syncrecoverygaps ADD COLUMN "
            '"membership_bound" INTEGER NOT NULL DEFAULT 0'
        )
        connection.execute(
            "CREATE INDEX syncrecoverygaps_account_id "
            "ON syncrecoverygaps (account_id)"
        )

    adopted = _configured_open(tmp_path, SqliteStore)
    adopted.close()


def test_configured_adoption_rejects_reordered_nonmigrated_columns(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute("DROP TABLE storeversion")
        connection.execute(
            'CREATE TABLE "storeversion" ('
            '"version" INTEGER NOT NULL, "id" INTEGER NOT NULL PRIMARY KEY)'
        )
        connection.execute("INSERT INTO storeversion VALUES (10, 1)")

    with pytest.raises(FreshIngestionRequired, match="topology"):
        _configured_open(tmp_path, SqliteStore)


def test_configured_adoption_rejects_reordered_base_columns_in_migrated_table(
    tmp_path: Path,
) -> None:
    _seed_populated_store(tmp_path, SqliteStore)
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute("DROP TABLE pendingtimelineevents")
        connection.execute(
            'CREATE TABLE "pendingtimelineevents" ('
            '"id" INTEGER NOT NULL PRIMARY KEY, '
            '"generation" INTEGER NOT NULL, "room_id" TEXT NOT NULL, '
            '"sequence" INTEGER NOT NULL, "event_id" TEXT NOT NULL, '
            '"event_payload" BLOB NOT NULL, "is_live" INTEGER NOT NULL, '
            '"was_encrypted" INTEGER NOT NULL, '
            '"was_completed" INTEGER NOT NULL, '
            '"admission_accepted" INTEGER NOT NULL, '
            '"provenance" TEXT NOT NULL, '
            '"apply_room_state" INTEGER NOT NULL, '
            '"account_id" INTEGER NOT NULL, FOREIGN KEY ("account_id") '
            'REFERENCES "accounts" ("id") ON DELETE CASCADE, '
            "UNIQUE(account_id,room_id,event_id))"
        )
        connection.execute(
            "CREATE INDEX pendingtimelineevents_account_id "
            "ON pendingtimelineevents (account_id)"
        )

    with pytest.raises(FreshIngestionRequired, match="topology"):
        _configured_open(tmp_path, SqliteStore)


@pytest.mark.parametrize("legacy_trust", ("absent", "empty", "populated", "orphan"))
def test_configured_default_store_raw_sidecar_authority_rebuilds_legacy_trust(
    tmp_path: Path,
    legacy_trust: str,
) -> None:
    devices, _sidecars = _seed_populated_store(tmp_path, DefaultStore)
    trusted = tmp_path / f"{ACCOUNT_ID}_{DEVICE_ID}.trusted_devices"
    blacklisted = tmp_path / f"{ACCOUNT_ID}_{DEVICE_ID}.blacklisted_devices"
    ignored = tmp_path / f"{ACCOUNT_ID}_{DEVICE_ID}.ignored_devices"
    verified = devices["VERIFIED"]
    blocked = devices["BLACKLISTED"]
    inert = devices["IGNORED"]
    trusted.write_bytes(
        b"# preserved comment\n\nshort\n"
        + f"{verified.user_id} {verified.id} matrix-ed25519 {verified.ed25519} extra\n".encode()
        + f"{verified.user_id} {verified.id} matrix-ed25519 wrong-fingerprint\n".encode()
    )
    blacklisted.write_bytes(
        f"{verified.user_id} {verified.id} matrix-ed25519 {verified.ed25519}\n".encode()
        + f"{blocked.user_id} {blocked.id} matrix-ed25519 {blocked.ed25519}\n".encode()
        + b"@stale:example.org STALE matrix-ed25519 stale\n"
    )
    ignored.write_bytes(
        f"{blocked.user_id} {blocked.id} matrix-ed25519 {blocked.ed25519}\n".encode()
        + f"{inert.user_id} {inert.id} matrix-ed25519 {inert.ed25519}\n".encode()
        + b"@stale:example.org STALE unsupported value\n"
    )
    sidecars_before = tuple(
        path.read_bytes() for path in (trusted, blacklisted, ignored)
    )

    if legacy_trust != "absent":
        with sqlite3.connect(tmp_path / "journal.db") as connection:
            connection.execute(
                'CREATE TABLE "devicetruststate" ('
                '"device_id" INTEGER NOT NULL PRIMARY KEY, '
                '"state" INTEGER NOT NULL, FOREIGN KEY ("device_id") '
                'REFERENCES "devicekeys" ("id"))'
            )
            if legacy_trust in {"populated", "orphan"}:
                row_id = connection.execute(
                    "SELECT id FROM devicekeys WHERE device_id = 'UNSET'"
                ).fetchone()[0]
                if legacy_trust == "orphan":
                    row_id += 10_000
                connection.execute(
                    "INSERT INTO devicetruststate VALUES (?, ?)", (row_id, 99)
                )

    bootstrap = _configured_open(tmp_path, DefaultStore)
    try:
        with sqlite3.connect(tmp_path / "journal.db") as connection:
            rows = connection.execute(
                "SELECT d.device_id, t.state FROM devicetruststate AS t "
                "JOIN devicekeys AS d ON d.id = t.device_id ORDER BY d.device_id"
            ).fetchall()
        assert rows == [
            ("BLACKLISTED", TrustState.blacklisted.value),
            ("IGNORED", TrustState.ignored.value),
            ("VERIFIED", TrustState.verified.value),
        ]
        assert tuple(path.read_bytes() for path in (trusted, blacklisted, ignored)) == (
            sidecars_before
        )
    finally:
        bootstrap.close()


def test_configured_sqlite_store_performs_zero_legacy_sidecar_operations(
    monkeypatch,
    tmp_path: Path,
) -> None:
    import nio.store._sync_journal_preflight as preflight

    _seed_populated_store(tmp_path, SqliteStore)

    def forbidden(*_args: object, **_kwargs: object):
        raise AssertionError("SqliteStore adoption touched a legacy sidecar")

    monkeypatch.setattr(preflight, "_sidecar_paths", forbidden)
    monkeypatch.setattr(preflight, "_sidecar_snapshot", forbidden)
    monkeypatch.setattr(preflight, "_read_default_trust", forbidden)
    bootstrap = _configured_open(tmp_path, SqliteStore)
    bootstrap.close()


def test_default_adoption_treats_missing_ed25519_as_unmatched_inert_identity(
    tmp_path: Path,
) -> None:
    devices, _sidecars = _seed_populated_store(tmp_path, DefaultStore)
    database_path = tmp_path / "journal.db"
    missing = devices["VERIFIED"]
    with sqlite3.connect(database_path) as connection:
        row_id = connection.execute(
            "SELECT id FROM devicekeys WHERE device_id = ?", (missing.id,)
        ).fetchone()[0]
        connection.execute(
            "DELETE FROM keys WHERE device_id = ? AND key_type = 'ed25519'", (row_id,)
        )
        devices_before = connection.execute(
            "SELECT * FROM devicekeys ORDER BY id"
        ).fetchall()
        keys_before = connection.execute("SELECT * FROM keys ORDER BY id").fetchall()

    bootstrap = _configured_open(tmp_path, DefaultStore)
    try:
        with sqlite3.connect(database_path) as connection:
            assert (
                connection.execute("SELECT * FROM devicekeys ORDER BY id").fetchall()
                == devices_before
            )
            assert connection.execute("SELECT * FROM keys ORDER BY id").fetchall() == (
                keys_before
            )
            trusted = connection.execute(
                "SELECT 1 FROM devicetruststate AS t JOIN devicekeys AS d "
                "ON d.id = t.device_id WHERE d.device_id = ?",
                (missing.id,),
            ).fetchone()
            assert trusted is None
    finally:
        bootstrap.close()


@pytest.mark.parametrize("alias_kind", ("symlink", "hardlink"))
def test_configured_alias_rejects_before_sql_or_lock_creation(
    alias_kind: str,
    tmp_path: Path,
) -> None:
    source = tmp_path / "source"
    source.mkdir()
    _seed_populated_store(source, SqliteStore)
    alias = tmp_path / "alias.db"
    if alias_kind == "symlink":
        alias.symlink_to(source / "journal.db")
    else:
        os.link(source / "journal.db", alias)
    statements: list[str] = []
    with pytest.raises((FreshIngestionRequired, LocalProtocolError)):
        bootstrap_api._open_configured_ingestion_store(
            tmp_path,
            source_store_class=SqliteStore,
            owned_store_class=SqliteStore,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            pickle_key=PICKLE_KEY,
            database_name=alias.name,
            statement_observer=statements.append,
        )
    assert statements == []
    assert not Path(f"{alias}.ingest.lock").exists()
