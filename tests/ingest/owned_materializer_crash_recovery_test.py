"""Global crash matrix for owned encrypted Task4C materialization."""

import asyncio
import json
import multiprocessing
import os
import re
import sqlite3
from collections.abc import Callable
from dataclasses import dataclass
from pathlib import Path
from uuid import UUID

import pytest

import nio.store.sync_journal as bootstrap_api
from nio import AsyncClient, AsyncClientConfig
from nio.crypto import Olm, OlmAccount, OlmDevice
from nio.event_builders import DummyMessage, RoomKeyRequestMessage, ToDeviceMessage
from nio.events import Event, MegolmEvent, RoomKeyRequest, ToDeviceEvent
from nio.exceptions import LocalProtocolError
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, IngestionConfig
from nio.ingest.errors import JournalCapacityError
from nio.ingest.ports import NetworkResult, StagedSourceResponse
from nio.ingest.source import canonical_json
from nio.ingest.state import SourceState, StagedFrame
from nio.store import SqliteMemoryStore, SqliteStore
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "ALICE"
BOB_ID = "@bob:example.org"
BOB_DEVICE_ID = "BOB"
ROOM_ID = "!diagnostic:example.org"
PICKLE_KEY = "owned-secret"
DATABASE_NAME = "owned.db"
CONSUMER_GENERATION = UUID("44444444-4444-4444-8444-444444444444")
CRASH_EXIT_CODE = 86
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")

DmlSignature = tuple[str, int]
OWNED_MATERIALIZER_DML_MANIFEST: tuple[DmlSignature, ...] = (
    (
        'INSERT OR REPLACE INTO "olmsessions" ("session_id", "creation_time", '
        '"last_usage_date", "sender_key", "account_id", "session") '
        "VALUES (?, ?, ?, ?, ?, ?)",
        6,
    ),
    (
        'INSERT OR IGNORE INTO "accounts" ("account", "user_id", "device_id", '
        '"shared") VALUES (?, ?, ?, ?)',
        4,
    ),
    (
        'UPDATE "accounts" SET "account" = ?, "shared" = ? WHERE '
        '(("accounts"."user_id" = ?) AND ("accounts"."device_id" = ?))',
        4,
    ),
    (
        'INSERT OR IGNORE INTO "megolminboundsessions" ("session_id", '
        '"sender_key", "account_id", "fp_key", "room_id", "session") '
        "VALUES (?, ?, ?, ?, ?, ?)",
        6,
    ),
    (
        'UPDATE "megolminboundsessions" SET "session" = ? WHERE '
        '("megolminboundsessions"."session_id" = ?)',
        2,
    ),
    (
        'INSERT OR REPLACE INTO "synctokens" ("token", "account_id") ' "VALUES (?, ?)",
        2,
    ),
    (
        'INSERT OR IGNORE INTO "encryptedrooms" ("room_id", "account_id") '
        "VALUES (?, ?)",
        2,
    ),
    (
        'INSERT OR IGNORE INTO "accounts" ("account", "user_id", "device_id", '
        '"shared") VALUES (?, ?, ?, ?)',
        4,
    ),
    (
        'UPDATE "accounts" SET "account" = ?, "shared" = ? WHERE '
        '(("accounts"."user_id" = ?) AND ("accounts"."device_id" = ?))',
        4,
    ),
    (
        "UPDATE NioIngestMeta SET revision = ? WHERE account_id = ? "
        "AND revision = ? AND writer_epoch = ?",
        4,
    ),
    (
        "INSERT INTO NioIngestRoomAggregate(account_id, room_id, "
        "updated_revision, intent_kind, payload, payload_sha256) "
        "VALUES (?, ?, ?, ?, ?, ?)",
        6,
    ),
    *(
        (
            "INSERT INTO NioIngestWork(account_id, work_id, kind, status, "
            "frame_id, room_id, membership_epoch, room_sequence, "
            "ready_revision, ready_ordinal, created_revision, payload, "
            "payload_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            13,
        )
        for _ in range(6)
    ),
    (
        "UPDATE NioIngestFrame SET payload = ?, payload_sha256 = ?, "
        "room_materialized_revision = ?, drain_header_sha256 = ? WHERE "
        "account_id = ? AND frame_id = ? AND source_epoch = ? AND "
        "request_id = ? AND staged_revision = ? AND payload = ? AND "
        "payload_sha256 = ? AND room_materialized_revision IS NULL AND "
        "callbacks_claimed_revision IS NULL AND drain_header_sha256 = ?",
        12,
    ),
)


@dataclass(frozen=True)
class _OwnedMaterializerSeed:
    database_path: Path
    frame_id: UUID
    staged_revision: int
    owner_revision: int


def _owned_client(store_path: Path) -> AsyncClient:
    client = AsyncClient("https://example.org", ACCOUNT_ID, DEVICE_ID)
    client.restore_login(ACCOUNT_ID, DEVICE_ID, "secret-token")
    client.store_path = str(store_path)
    client.config = AsyncClientConfig(
        store=SqliteStore,
        pickle_key=PICKLE_KEY,
        store_name=DATABASE_NAME,
    )
    return client


def _open_owned_bootstrap(store_path: Path):
    return bootstrap_api._open_configured_ingestion_store(
        store_path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        pickle_key=PICKLE_KEY,
        database_name=DATABASE_NAME,
    )


def _open_owned_session(store_path: Path):
    from nio.ingest import coordinator

    bootstrap = _open_owned_bootstrap(store_path)
    client = _owned_client(store_path)
    session = coordinator._open_owned_ingestion(
        client,
        bootstrap,
        config=IngestionConfig(CLASSIC_SOURCE),
        consumer_generation=CONSUMER_GENERATION,
        stream_id=bootstrap.stream_id,
    )
    return client, session


def _close_owned_session(session: object) -> None:
    asyncio.run(session.close())  # type: ignore[attr-defined]


def _stage_classic(bootstrap: object, body: dict[str, object]) -> StagedFrame:
    journal = bootstrap._journal  # type: ignore[attr-defined]
    owner = journal.load_owner()
    prior = journal.load_source()
    adapter = ClassicSource(owner.stream_id, CLASSIC_SOURCE, owner.account_id)
    request = adapter.plan_request(prior, prior.next_request_id)
    assert request is not None
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
    journal.stage_source_response(
        source=SourceState(
            prior.source_epoch,
            prior.transport_kind,
            normalized.frame.candidate_cursor_json,
            request.request_id + 1,
            prior.active,
        ),
        frame=frame,
    )
    return frame


def _owned_crypto_classic_body(
    client: AsyncClient,
) -> tuple[dict[str, object], SqliteMemoryStore]:
    assert client.olm is not None
    assert type(client.store) is SqliteStore
    sender_store = SqliteMemoryStore(BOB_ID, BOB_DEVICE_ID, "sender-secret")
    sender = Olm(BOB_ID, BOB_DEVICE_ID, sender_store)
    recipient_device = OlmDevice(
        ACCOUNT_ID,
        DEVICE_ID,
        client.olm.account.identity_keys,
    )
    sender_device = OlmDevice(
        BOB_ID,
        BOB_DEVICE_ID,
        sender.account.identity_keys,
    )
    sender.store.save_device_keys({ACCOUNT_ID: {DEVICE_ID: recipient_device}})
    sender.device_store.add(recipient_device)
    sender.verify_device(recipient_device)
    client.store.save_device_keys({BOB_ID: {BOB_DEVICE_ID: sender_device}})
    client.olm.device_store.add(sender_device)
    client.olm.verify_device(sender_device)

    client.olm.account.generate_one_time_keys(1)
    generated = dict(client.olm.account.one_time_keys["curve25519"])
    assert len(generated) == 1
    client.olm.save_account()
    one_time_key = next(iter(generated.values()))
    client.olm.account.mark_keys_as_published()

    sender.create_session(one_time_key, recipient_device.curve25519)
    _sharing_with, to_device = sender.share_group_session(ROOM_ID, [ACCOUNT_ID])
    sender.outbound_group_sessions[ROOM_ID].shared = True
    encrypted_content = sender.group_encrypt(
        ROOM_ID,
        {
            "content": {"body": "same-frame secret", "msgtype": "m.text"},
            "type": "m.room.message",
        },
    )

    def event(event_type: str, content: object, **fields: object) -> object:
        return {"content": content, "type": event_type, **fields}

    state = [
        event(
            "m.room.encryption",
            {"algorithm": "m.megolm.v1.aes-sha2"},
            event_id="$encryption",
            origin_server_ts=1,
            sender=ACCOUNT_ID,
            state_key="",
        ),
        *(
            event(
                "m.room.member",
                {"membership": "join"},
                event_id=f"$member-{user_id}",
                origin_server_ts=2,
                sender=user_id,
                state_key=user_id,
            )
            for user_id in (ACCOUNT_ID, BOB_ID)
        ),
    ]
    body: dict[str, object] = {
        "device_lists": {"changed": [BOB_ID], "left": []},
        "device_one_time_keys_count": {"signed_curve25519": 0},
        "device_unused_fallback_key_types": [],
        "next_batch": "s1",
        "rooms": {
            "join": {
                ROOM_ID: {
                    "state": {"events": state},
                    "timeline": {
                        "events": [
                            event(
                                "m.room.encrypted",
                                encrypted_content,
                                event_id="$secret",
                                origin_server_ts=3,
                                sender=BOB_ID,
                            )
                        ]
                    },
                }
            }
        },
        "to_device": {
            "events": [
                event(
                    "m.room.encrypted",
                    to_device["messages"][ACCOUNT_ID][DEVICE_ID],
                    sender=BOB_ID,
                )
            ]
        },
    }
    return body, sender_store


def _configure_maximal_outbound(client: AsyncClient) -> None:
    assert client.olm is not None
    olm = client.olm
    olm.tracked_users.add(ACCOUNT_ID)
    olm.users_for_key_query.clear()

    claim_user = "@claim:example.org"
    claim_device_id = "CLAIM"
    claim_room = "!claim:example.org"
    claim_algorithm = "m.megolm.v1.aes-sha2"
    claim_device = OlmDevice(
        claim_user,
        claim_device_id,
        {"curve25519": "claim-curve", "ed25519": "claim-ed"},
    )
    olm.wedged_devices.append(claim_device)
    olm.key_request_devices_no_session.append(claim_device)
    waiting_request = ToDeviceEvent.parse_event(
        {
            "content": {
                "action": "request",
                "body": {
                    "algorithm": claim_algorithm,
                    "room_id": claim_room,
                    "sender_key": "claim-sender-key",
                    "session_id": "claim-session",
                },
                "request_id": "waiting-request",
                "requesting_device_id": claim_device_id,
            },
            "sender": claim_user,
            "type": "m.room_key_request",
        }
    )
    assert isinstance(waiting_request, RoomKeyRequest)
    olm.key_requests_waiting_for_session[(claim_user, claim_device_id)][
        waiting_request.request_id
    ] = waiting_request
    claim_rerequest = Event.parse_event(
        {
            "content": {
                "algorithm": claim_algorithm,
                "ciphertext": "claim-ciphertext",
                "device_id": claim_device_id,
                "sender_key": "claim-sender-key",
                "session_id": "claim-session",
            },
            "event_id": "$claim-rerequest",
            "origin_server_ts": 1,
            "sender": claim_user,
            "type": "m.room.encrypted",
        }
    )
    assert isinstance(claim_rerequest, MegolmEvent)
    claim_rerequest.room_id = claim_room
    olm.key_re_requests_events[(claim_user, claim_device_id)].append(claim_rerequest)

    dummy_user = "@dummy:example.org"
    dummy_device_id = "DUMMY"
    dummy_rerequest = Event.parse_event(
        {
            "content": {
                "algorithm": claim_algorithm,
                "ciphertext": "dummy-rerequest-ciphertext",
                "device_id": dummy_device_id,
                "sender_key": "dummy-rerequest-sender-key",
                "session_id": "dummy-rerequest-session",
            },
            "event_id": "$dummy-rerequest",
            "origin_server_ts": 2,
            "sender": dummy_user,
            "type": "m.room.encrypted",
        }
    )
    assert isinstance(dummy_rerequest, MegolmEvent)
    dummy_rerequest.room_id = "!dummy:example.org"
    olm.key_re_requests_events[(dummy_user, dummy_device_id)].append(dummy_rerequest)

    room_key_content = {
        "action": "request",
        "body": {
            "algorithm": claim_algorithm,
            "room_id": "!room-key:example.org",
            "sender_key": "room-key-sender-key",
            "session_id": "room-key-session",
        },
        "request_id": "room-key-request",
        "requesting_device_id": DEVICE_ID,
    }
    olm.outgoing_to_device_messages.extend(
        [
            ToDeviceMessage(
                "org.example.generic",
                "@generic:example.org",
                "GENERIC",
                {"value": "generic"},
            ),
            DummyMessage(
                "m.room.encrypted",
                dummy_user,
                dummy_device_id,
                {
                    "algorithm": "m.olm.v1.curve25519-aes-sha2",
                    "ciphertext": {
                        "dummy-curve-key": {
                            "body": "dummy-ciphertext",
                            "type": 0,
                        }
                    },
                    "sender_key": "owner-curve-key",
                },
            ),
            RoomKeyRequestMessage(
                "m.room_key_request",
                "@room-key:example.org",
                "ROOMKEY",
                room_key_content,
                "room-key-request",
                "room-key-session",
                "!room-key:example.org",
                claim_algorithm,
            ),
        ]
    )


def _seed_owned_materializer_fixture(store_path: Path) -> _OwnedMaterializerSeed:
    store_path.mkdir(parents=True, exist_ok=True)
    seed_store = SqliteStore(
        ACCOUNT_ID,
        DEVICE_ID,
        str(store_path),
        pickle_key=PICKLE_KEY,
        database_name=DATABASE_NAME,
    )
    seed_store.save_account(OlmAccount())
    seed_store.database.close()
    client, session = _open_owned_session(store_path)
    sender_store: SqliteMemoryStore | None = None
    try:
        body, sender_store = _owned_crypto_classic_body(client)
        staged = _stage_classic(session, body)
        stored = session._journal.load_frame(staged.frame_id)
        assert stored is not None
        owner_revision = session._journal.load_owner().revision
    finally:
        if sender_store is not None:
            sender_store.database.close()
        _close_owned_session(session)
    return _OwnedMaterializerSeed(
        store_path / DATABASE_NAME,
        staged.frame_id,
        stored.staged_revision,
        owner_revision,
    )


def _clone_seed_database(seed: _OwnedMaterializerSeed, store_path: Path) -> None:
    store_path.mkdir(parents=True)
    with (
        sqlite3.connect(seed.database_path) as source,
        sqlite3.connect(store_path / DATABASE_NAME) as destination,
    ):
        source.backup(destination)


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


def _normalize_sql(statement: str) -> str:
    return " ".join(statement.split())


def _dml_signature(statement: str, parameters: object) -> DmlSignature | None:
    normalized = _normalize_sql(statement)
    if re.match(r"^(?:INSERT|UPDATE|DELETE|REPLACE)\b", normalized, re.IGNORECASE):
        return normalized, 0 if parameters is None else len(parameters)  # type: ignore[arg-type]
    return None


def _capture_owner_dml(
    session: object,
    observed: list[DmlSignature],
) -> tuple[object, Callable[..., object]]:
    database = session._journal._owner.database  # type: ignore[attr-defined]
    real_execute = database.execute_sql

    def capture(statement: str, parameters=None, commit=None):
        cursor = real_execute(statement, parameters, commit)
        signature = _dml_signature(statement, parameters)
        if signature is not None:
            observed.append(signature)
        return cursor

    database.execute_sql = capture
    return database, real_execute


def _append_crash_sequence(sequence_path: Path, value: tuple[object, ...]) -> None:
    with sequence_path.open("a", encoding="utf-8") as sequence:
        sequence.write(json.dumps(value, separators=(",", ":")) + "\n")
        sequence.flush()
        os.fsync(sequence.fileno())


def _read_crash_sequence(sequence_path: Path) -> tuple[tuple[object, ...], ...]:
    return tuple(
        tuple(json.loads(line))
        for line in sequence_path.read_text(encoding="utf-8").splitlines()
    )


def _kill_during_owned_materialization(
    store_path: Path,
    fault_kind: str,
    fault: int | str,
    sequence_path: Path,
) -> None:
    client, session = _open_owned_session(store_path)
    _configure_maximal_outbound(client)
    database = session._journal._owner.database
    real_execute = database.execute_sql
    observed_dml = 0

    def exit_after_real_dml(statement: str, parameters=None, commit=None):
        nonlocal observed_dml
        cursor = real_execute(statement, parameters, commit)
        signature = _dml_signature(statement, parameters)
        if signature is not None:
            observed_dml += 1
            _append_crash_sequence(sequence_path, ("dml", *signature))
            if fault_kind == "dml" and observed_dml == fault:
                os._exit(CRASH_EXIT_CODE)
        return cursor

    def exit_at_hook(label: str) -> None:
        if fault_kind == "hook" and label == fault:
            _append_crash_sequence(sequence_path, ("hook", label))
            os._exit(CRASH_EXIT_CODE)

    database.execute_sql = exit_after_real_dml
    session._journal.set_transition_statement_hook(exit_at_hook)
    session._materialize_oldest_frame(limits=MaterializerLimits())
    raise AssertionError("owned materializer crash boundary was not reached")


def _assert_process_crashed(
    target: Callable[..., None],
    *args: object,
) -> None:
    process = multiprocessing.get_context("spawn").Process(target=target, args=args)
    process.start()
    process.join(timeout=20)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("owned materializer crash-injection child did not exit")
    assert process.exitcode == CRASH_EXIT_CODE


def _assert_all_new(
    session: object,
    seed: _OwnedMaterializerSeed,
) -> None:
    journal = session._journal  # type: ignore[attr-defined]
    owner = journal.load_owner()
    assert owner.revision == seed.owner_revision + 1
    assert journal.load_frame(seed.frame_id) is None
    assert journal.list_frames(1) == ()
    with journal._owner.read():
        headers = journal._load_authenticated_frame_headers(owner)
        prepared = journal._load_prepared_frame_with_owner(seed.frame_id, owner)
    assert len(headers) == 1
    assert headers[0].frame_id == seed.frame_id
    assert headers[0].staged_revision == seed.staged_revision
    assert headers[0].room_materialized_revision == owner.revision
    assert prepared is not None
    assert prepared.compatibility_token == "s1"
    assert tuple(
        operation.kind for operation in prepared.outbound_maintenance.operations
    ) == (
        "key_upload",
        "key_query",
        "key_claim",
        "to_device",
        "to_device",
        "to_device",
    )
    connection = journal._owner.database.connection()
    assert (
        connection.execute("SELECT COUNT(*) FROM NioIngestRoomAggregate").fetchone()[0]
        == 1
    )
    assert connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[0] == 6
    assert (
        connection.execute("SELECT COUNT(*) FROM megolminboundsessions").fetchone()[0]
        == 1
    )
    assert connection.execute("SELECT token FROM synctokens").fetchone()[0] == "s1"
    assert connection.execute("SELECT room_id FROM encryptedrooms").fetchone()[0] == (
        ROOM_ID
    )


def _materialize_successful_clone(
    seed: _OwnedMaterializerSeed,
    store_path: Path,
) -> tuple[DmlSignature, ...]:
    _clone_seed_database(seed, store_path)
    client, session = _open_owned_session(store_path)
    _configure_maximal_outbound(client)
    observed: list[DmlSignature] = []
    database, real_execute = _capture_owner_dml(session, observed)
    try:
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        assert result.frame_id == seed.frame_id
        assert result.revision == seed.owner_revision + 1
    finally:
        database.execute_sql = real_execute
    try:
        _assert_all_new(session, seed)
    finally:
        _close_owned_session(session)
    return tuple(observed)


@pytest.fixture(scope="module")
def owned_materializer_seed(tmp_path_factory: pytest.TempPathFactory):
    return _seed_owned_materializer_fixture(
        tmp_path_factory.mktemp("owned-materializer-seed")
    )


def test_owned_materializer_success_matches_exact_dml_manifest(
    tmp_path: Path,
    owned_materializer_seed: _OwnedMaterializerSeed,
) -> None:
    observed = _materialize_successful_clone(
        owned_materializer_seed,
        tmp_path / "success",
    )

    assert observed == OWNED_MATERIALIZER_DML_MANIFEST


def test_owned_oversized_output_rolls_back_real_crypto_and_stays_rejected_on_restart(
    tmp_path: Path,
) -> None:
    seed_store = SqliteStore(
        ACCOUNT_ID,
        DEVICE_ID,
        str(tmp_path),
        pickle_key=PICKLE_KEY,
        database_name=DATABASE_NAME,
    )
    seed_store.save_account(OlmAccount())
    seed_store.database.close()
    client, session = _open_owned_session(tmp_path)
    sender_store = None
    try:
        body, sender_store = _owned_crypto_classic_body(client)
        body["account_data"] = {
            "events": [
                {
                    "type": "m.push_rules",
                    "content": {"global": {}, "padding": "x" * 800_000},
                }
            ]
        }
        staged = _stage_classic(session, body)
        source = session._journal.load_source()
    finally:
        if sender_store is not None:
            sender_store.database.close()
        _close_owned_session(session)

    for _attempt in range(2):
        client, session = _open_owned_session(tmp_path)
        observed: list[DmlSignature] = []
        database, real_execute = _capture_owner_dml(session, observed)
        before = _logical_sqlite_graph(tmp_path / DATABASE_NAME)
        try:
            with pytest.raises(JournalCapacityError, match="immutable byte limit"):
                session._materialize_oldest_frame(limits=MaterializerLimits())
            assert any('INTO "olmsessions"' in statement for statement, _ in observed)
            assert any(
                'INTO "megolminboundsessions"' in statement for statement, _ in observed
            )
            assert _logical_sqlite_graph(tmp_path / DATABASE_NAME) == before
            assert session._journal.list_frames(1)[0] == staged
            assert session._journal.load_source() == source
            with pytest.raises(LocalProtocolError, match="poisoned"):
                session._materialize_oldest_frame(limits=MaterializerLimits())
            with pytest.raises(LocalProtocolError, match="poisoned"):
                session.next_batch()
        finally:
            database.execute_sql = real_execute
            _close_owned_session(session)


@pytest.mark.parametrize(
    "dml_ordinal",
    range(1, len(OWNED_MATERIALIZER_DML_MANIFEST) + 1),
)
def test_owned_materializer_baseexception_after_real_dml_rolls_back_and_poisons(
    tmp_path: Path,
    owned_materializer_seed: _OwnedMaterializerSeed,
    dml_ordinal: int,
) -> None:
    class DmlAbort(BaseException):
        pass

    seed = owned_materializer_seed
    store_path = tmp_path / f"dml-{dml_ordinal}"
    _clone_seed_database(seed, store_path)
    client, session = _open_owned_session(store_path)
    _configure_maximal_outbound(client)
    database = session._journal._owner.database
    real_execute = database.execute_sql
    observed_dml: list[DmlSignature] = []
    all_sql: list[str] = []
    prepared_args: list[tuple[object, ...]] = []
    prepared_returns = 0
    real_prepare = client._prepare_ingestion_frame

    def capture_prepare(*args: object, **kwargs: object):
        nonlocal prepared_returns
        prepared_args.append(args)
        prepared = real_prepare(*args, **kwargs)
        prepared_returns += 1
        return prepared

    client._prepare_ingestion_frame = capture_prepare
    sentinel = DmlAbort(f"abort after real DML {dml_ordinal}")

    def abort_after_real_dml(statement: str, parameters=None, commit=None):
        cursor = real_execute(statement, parameters, commit)
        all_sql.append(_normalize_sql(statement))
        signature = _dml_signature(statement, parameters)
        if signature is not None:
            observed_dml.append(signature)
            if len(observed_dml) == dml_ordinal:
                raise sentinel
        return cursor

    database.execute_sql = abort_after_real_dml
    graph_before = _logical_sqlite_graph(store_path / DATABASE_NAME)
    try:
        with pytest.raises(DmlAbort) as failure:
            session._materialize_oldest_frame(limits=MaterializerLimits())
        assert failure.value is sentinel
        assert tuple(observed_dml) == OWNED_MATERIALIZER_DML_MANIFEST[:dml_ordinal]
        assert len(prepared_args) == 1
        assert prepared_returns == int(dml_ordinal >= 6)
        assert _logical_sqlite_graph(store_path / DATABASE_NAME) == graph_before

        sql_count_after_failure = len(all_sql)
        client._prepare_ingestion_frame = real_prepare

        def assert_poisoned(operation: Callable[[], object]) -> None:
            with pytest.raises(LocalProtocolError) as poisoned:
                operation()
            assert type(poisoned.value) is LocalProtocolError
            assert str(poisoned.value) == (
                "owned ingestion session is poisoned; reopen with a fresh client"
            )
            assert poisoned.value.__cause__ is None

        assert_poisoned(
            lambda: session._materialize_oldest_frame(limits=MaterializerLimits())
        )
        assert_poisoned(session.next_batch)
        assert_poisoned(lambda: client._prepare_ingestion_frame(*prepared_args[0]))
        assert len(all_sql) == sql_count_after_failure
        assert _logical_sqlite_graph(store_path / DATABASE_NAME) == graph_before
    finally:
        database.execute_sql = real_execute
        _close_owned_session(session)


@pytest.mark.parametrize(
    ("fault_kind", "fault"),
    [
        *(
            ("dml", ordinal)
            for ordinal in range(1, len(OWNED_MATERIALIZER_DML_MANIFEST) + 1)
        ),
        ("hook", "before_commit"),
    ],
)
def test_owned_materializer_process_death_before_commit_reopens_all_old(
    tmp_path: Path,
    owned_materializer_seed: _OwnedMaterializerSeed,
    fault_kind: str,
    fault: int | str,
) -> None:
    seed = owned_materializer_seed
    store_path = tmp_path / f"{fault_kind}-{fault}"
    _clone_seed_database(seed, store_path)
    graph_before = _logical_sqlite_graph(store_path / DATABASE_NAME)
    sequence_path = store_path / "materializer-sequence.jsonl"

    _assert_process_crashed(
        _kill_during_owned_materialization,
        store_path,
        fault_kind,
        fault,
        sequence_path,
    )

    sequence = _read_crash_sequence(sequence_path)
    dml_count = fault if fault_kind == "dml" else len(OWNED_MATERIALIZER_DML_MANIFEST)
    assert sequence[:dml_count] == tuple(
        ("dml", *signature) for signature in OWNED_MATERIALIZER_DML_MANIFEST[:dml_count]
    )
    if fault_kind == "hook":
        assert sequence[dml_count:] == (("hook", fault),)
    else:
        assert len(sequence) == dml_count
    graph_after = _logical_sqlite_graph(store_path / DATABASE_NAME)
    assert _graph_without_writer_epoch(graph_after) == _graph_without_writer_epoch(
        graph_before
    )

    reopened = _open_owned_bootstrap(store_path)
    try:
        assert reopened._journal.load_owner().revision == seed.owner_revision
        staged = reopened._journal.load_frame(seed.frame_id)
        assert staged is not None
        assert staged.staged_revision == seed.staged_revision
        assert reopened._journal.list_frames(1) == (staged,)
    finally:
        reopened.close()


def test_owned_materializer_process_death_after_commit_reopens_all_new_blocked(
    tmp_path: Path,
    owned_materializer_seed: _OwnedMaterializerSeed,
) -> None:
    seed = owned_materializer_seed
    store_path = tmp_path / "commit"
    _clone_seed_database(seed, store_path)
    graph_before = _logical_sqlite_graph(store_path / DATABASE_NAME)
    sequence_path = store_path / "materializer-sequence.jsonl"

    _assert_process_crashed(
        _kill_during_owned_materialization,
        store_path,
        "hook",
        "commit",
        sequence_path,
    )

    assert _read_crash_sequence(sequence_path) == (
        *(("dml", *signature) for signature in OWNED_MATERIALIZER_DML_MANIFEST),
        ("hook", "commit"),
    )
    graph_after = _logical_sqlite_graph(store_path / DATABASE_NAME)
    assert _graph_without_writer_epoch(graph_after) != _graph_without_writer_epoch(
        graph_before
    )

    _client, session = _open_owned_session(store_path)
    try:
        assert _graph_without_writer_epoch(
            _logical_sqlite_graph(store_path / DATABASE_NAME)
        ) == _graph_without_writer_epoch(graph_after)
        _assert_all_new(session, seed)
        observed: list[DmlSignature] = []
        database, real_execute = _capture_owner_dml(session, observed)
        try:
            result = session._materialize_oldest_frame(limits=MaterializerLimits())
        finally:
            database.execute_sql = real_execute
        assert result.status is MaterializeStatus.BLOCKED
        assert result.frame_id == seed.frame_id
        assert result.revision is None
        assert observed == []
    finally:
        _close_owned_session(session)
