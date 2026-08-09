import base64
import hashlib
import inspect
import json
import multiprocessing
import os
import sqlite3
from dataclasses import dataclass, fields, replace
from pathlib import Path
from typing import get_type_hints
from uuid import UUID, uuid4

import pytest

import nio.ingest.state as ingest_state
from nio.ingest.classic import ClassicSource
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
from nio.ingest.model import TransportKind
from nio.ingest.ports import (
    NetworkRequest,
    NetworkResult,
    StagedSourceResponse,
    _frame_id_for_response,
)
from nio.ingest.sliding import RESERVED_ALL_ROOMS_LIST, SlidingSource
from nio.ingest.source import canonical_json
from nio.ingest.source import renormalize_staged_frame
from nio.ingest.state import CommitResult, OwnerView, SourceState, StagedFrame
from nio.store import SqliteStore
from nio.store._sync_journal_codec import EncryptedRowCodec
from nio.store._sync_journal_port import IngestionJournal
from nio.store.sync_journal import open_ingestion_store

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
OWN_USER_ID = "@alice:example.org"
CRASH_EXIT_CODE = 86
LIST_FRAMES_SQL = (
    "SELECT * FROM NioIngestFrame WHERE account_id = ? "
    "ORDER BY staged_revision, source_epoch, request_id, frame_id LIMIT ?"
)

EXPECTED_INGESTION_OBJECTS = {
    ("table", "NioIngestMeta"),
    ("table", "NioIngestSourceState"),
    ("table", "NioIngestFrame"),
    ("index", "NioIngestFrame_drain"),
}


@dataclass(frozen=True, slots=True)
class _StageProposal:
    prior_source: SourceState
    successor_source: SourceState
    frame: StagedFrame


@dataclass(frozen=True, slots=True)
class _StoredStage:
    database_path: Path
    stream_id: UUID
    proposal: _StageProposal
    committed: CommitResult


def _open(
    store_path: Path,
    source: ClassicSourceConfig | SlidingSourceConfig = CLASSIC_SOURCE,
    *,
    account_id: str = ACCOUNT_ID,
    device_id: str = DEVICE_ID,
    database_name: str = "journal.db",
    statements: list[str] | None = None,
    **kwargs: object,
):
    return open_ingestion_store(
        store_path,
        account_id=account_id,
        device_id=device_id,
        source=source,
        pickle_key="secret",
        database_name=database_name,
        statement_observer=statements.append if statements is not None else None,
        **kwargs,
    )


def _source_adapter(
    stream_id: UUID,
    source_config: ClassicSourceConfig | SlidingSourceConfig,
) -> ClassicSource | SlidingSource:
    if type(source_config) is ClassicSourceConfig:
        return ClassicSource(stream_id, source_config, OWN_USER_ID)
    return SlidingSource(stream_id, source_config, OWN_USER_ID)


def _successful_body(
    request: NetworkRequest,
    sequence: int,
) -> bytes:
    own_member = {
        "type": "m.room.member",
        "state_key": OWN_USER_ID,
        "event_id": f"$own-{sequence}",
        "content": {"membership": "join"},
    }
    if request.transport is TransportKind.CLASSIC:
        return canonical_json(
            {
                "next_batch": f"s{sequence}",
                "rooms": {
                    "join": {
                        "!room:example.org": {
                            "state": {"events": [own_member]},
                            "timeline": {"events": []},
                        }
                    }
                },
            }
        )

    assert request.body is not None
    request_body = json.loads(request.body)
    return canonical_json(
        {
            "pos": f"p{sequence}",
            "txn_id": request_body["txn_id"],
            "lists": {RESERVED_ALL_ROOMS_LIST: {"count": 1}},
            "rooms": {
                "!room:example.org": {
                    "membership": "join",
                    "required_state": [own_member],
                }
            },
        }
    )


def _stage_proposal(
    journal: object,
    source_config: ClassicSourceConfig | SlidingSourceConfig,
    sequence: int,
) -> _StageProposal:
    owner = journal.load_owner()  # type: ignore[attr-defined]
    prior_source = journal.load_source()  # type: ignore[attr-defined]
    adapter = _source_adapter(owner.stream_id, source_config)
    request = adapter.plan_request(prior_source, prior_source.next_request_id)
    assert request is not None
    body = _successful_body(request, sequence)
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
    frame = StagedFrame(
        normalized.frame.frame_id,
        StagedSourceResponse(
            request,
            normalized.response_body,
            normalized.frame.source_sha256,
        ),
    )
    successor = SourceState(
        prior_source.source_epoch,
        prior_source.transport_kind,
        normalized.frame.candidate_cursor_json,
        request.request_id + 1,
        prior_source.active,
    )
    return _StageProposal(prior_source, successor, frame)


def _stage(
    journal: object,
    *,
    proposal: _StageProposal,
    expected_revision: int | None = None,
    writer_epoch: UUID | None = None,
) -> CommitResult:
    owner = journal.load_owner()  # type: ignore[attr-defined]
    return journal.stage_source_response(  # type: ignore[attr-defined,no-any-return]
        expected_revision=(
            owner.revision if expected_revision is None else expected_revision
        ),
        writer_epoch=owner.writer_epoch if writer_epoch is None else writer_epoch,
        source=proposal.successor_source,
        frame=proposal.frame,
    )


def _business_dml(statements: list[str]) -> list[str]:
    return [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        and any(
            table in statement
            for table in (
                "NioIngestMeta",
                "NioIngestSourceState",
                "NioIngestFrame",
            )
        )
    ]


def _frame_for_request(
    request: NetworkRequest,
    response_body: bytes,
) -> StagedFrame:
    digest = hashlib.sha256(response_body).digest()
    response = StagedSourceResponse(request, response_body, digest)
    return StagedFrame(_frame_id_for_response(request, digest), response)


def _canonical_internal(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _encoded_bytes(value: bytes | None) -> str | None:
    return base64.b64encode(value).decode("ascii") if value is not None else None


def _request_envelope(request: NetworkRequest) -> dict[str, object]:
    return {
        "stream_id": str(request.stream_id),
        "transport": request.transport.value,
        "source_epoch": request.source_epoch,
        "request_id": request.request_id,
        "method": request.method,
        "path": request.path,
        "query": [[key, value] for key, value in request.query],
        "body": _encoded_bytes(request.body),
        "timeout_ms": request.timeout_ms,
        "request_cursor_json": _encoded_bytes(request.request_cursor_json),
    }


def _frame_envelope(frame: StagedFrame) -> dict[str, object]:
    return {
        "normalization_version": 1,
        "request": _request_envelope(frame.response.request),
        "response_body": _encoded_bytes(frame.response.response_body),
        "source_sha256": _encoded_bytes(frame.response.source_sha256),
    }


def _frame_header(frame: StagedFrame, staged_revision: int) -> bytes:
    request = frame.response.request
    return _canonical_internal(
        [request.source_epoch, request.request_id, staged_revision]
    )


def _source_header(source: SourceState) -> bytes:
    return _canonical_internal(
        [
            source.transport_kind.value,
            source.source_epoch,
            source.next_request_id,
            source.active,
        ]
    )


def _stage_one(
    tmp_path: Path,
    source_config: ClassicSourceConfig | SlidingSourceConfig = CLASSIC_SOURCE,
) -> _StoredStage:
    bootstrap = _open(tmp_path, source_config)
    journal = bootstrap._journal
    owner = journal.load_owner()
    proposal = _stage_proposal(journal, source_config, 1)
    committed = _stage(journal, proposal=proposal)
    bootstrap.close()
    return _StoredStage(tmp_path / "journal.db", owner.stream_id, proposal, committed)


def _codec_for(stage: _StoredStage, account_id: str = ACCOUNT_ID) -> EncryptedRowCodec:
    return EncryptedRowCodec("secret", account_id, stage.stream_id)


def _stored_row(database_path: Path, frame_id: UUID | None = None) -> sqlite3.Row:
    table = "NioIngestSourceState" if frame_id is None else "NioIngestFrame"
    where = "account_id = ?" if frame_id is None else "account_id = ? AND frame_id = ?"
    parameters = (ACCOUNT_ID,) if frame_id is None else (ACCOUNT_ID, str(frame_id))
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            f"SELECT * FROM {table} WHERE {where}", parameters
        ).fetchone()
    assert row is not None
    return row


def _normalized_sql(sql: str) -> str:
    return " ".join(sql.split())


EXPECTED_DDL = {
    ("table", "NioIngestMeta"): _normalized_sql("""CREATE TABLE NioIngestMeta (
        account_id TEXT PRIMARY KEY CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        device_id TEXT NOT NULL CHECK (
            typeof(device_id) = 'text' AND length(device_id) > 0
        ),
        schema_version INTEGER NOT NULL CHECK (
            typeof(schema_version) = 'integer' AND schema_version = 1
        ),
        stream_id TEXT NOT NULL CHECK (
            typeof(stream_id) = 'text' AND length(stream_id) > 0
        ),
        transport_kind TEXT NOT NULL CHECK (
            typeof(transport_kind) = 'text'
            AND length(transport_kind) > 0
            AND transport_kind IN ('classic', 'sliding')
        ),
        revision INTEGER NOT NULL CHECK (
            typeof(revision) = 'integer' AND revision >= 0
        ),
        writer_epoch TEXT NOT NULL CHECK (
            typeof(writer_epoch) = 'text' AND length(writer_epoch) > 0
        ),
        next_source_epoch INTEGER NOT NULL CHECK (
            typeof(next_source_epoch) = 'integer' AND next_source_epoch >= 1
        ),
        created_at_ns INTEGER NOT NULL CHECK (
            typeof(created_at_ns) = 'integer' AND created_at_ns >= 0
        ))"""),
    ("table", "NioIngestSourceState"): _normalized_sql(
        """CREATE TABLE NioIngestSourceState (
        account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id) CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        source_epoch INTEGER NOT NULL CHECK (
            typeof(source_epoch) = 'integer' AND source_epoch >= 0
        ),
        cursor_ciphertext BLOB NOT NULL CHECK (
            typeof(cursor_ciphertext) = 'blob' AND length(cursor_ciphertext) >= 29
        ),
        cursor_sha256 BLOB NOT NULL CHECK (
            typeof(cursor_sha256) = 'blob' AND length(cursor_sha256) = 32
        ),
        next_request_id INTEGER NOT NULL CHECK (
            typeof(next_request_id) = 'integer' AND next_request_id >= 0
        ),
        active INTEGER NOT NULL CHECK (
            typeof(active) = 'integer' AND active IN (0, 1)
        ))"""
    ),
    ("table", "NioIngestFrame"): _normalized_sql("""CREATE TABLE NioIngestFrame (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        frame_id TEXT NOT NULL CHECK (
            typeof(frame_id) = 'text' AND length(frame_id) > 0
        ),
        source_epoch INTEGER NOT NULL CHECK (
            typeof(source_epoch) = 'integer' AND source_epoch >= 0
        ),
        request_id INTEGER NOT NULL CHECK (
            typeof(request_id) = 'integer' AND request_id >= 0
        ),
        payload_ciphertext BLOB NOT NULL CHECK (
            typeof(payload_ciphertext) = 'blob' AND length(payload_ciphertext) >= 29
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
        ),
        staged_revision INTEGER NOT NULL CHECK (
            typeof(staged_revision) = 'integer' AND staged_revision >= 1
        ),
        PRIMARY KEY (account_id, frame_id))"""),
    ("index", "NioIngestFrame_drain"): _normalized_sql(
        """CREATE INDEX NioIngestFrame_drain ON NioIngestFrame(
        account_id, staged_revision, source_epoch, request_id, frame_id)"""
    ),
}


def _ingestion_topology(
    database_path: Path,
) -> tuple[tuple[str, str, str], ...]:
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            "SELECT type, name, sql FROM sqlite_master "
            "WHERE type IN ('table', 'index') "
            "AND (name GLOB 'NioIngest*' OR name = 'NioIngestFrame_drain') "
            "ORDER BY type, name"
        ).fetchall()
    return tuple((kind, name, _normalized_sql(sql)) for kind, name, sql in rows)


def _all_schema_objects(database_path: Path) -> tuple[tuple[str, str, str], ...]:
    with sqlite3.connect(database_path) as connection:
        rows = connection.execute(
            "SELECT type, name, coalesce(sql, '') FROM sqlite_master "
            "ORDER BY type, name"
        ).fetchall()
    return tuple((kind, name, sql) for kind, name, sql in rows)


def _assert_exact_ingestion_topology(database_path: Path) -> None:
    topology = _ingestion_topology(database_path)
    assert {(kind, name) for kind, name, _sql in topology} == (
        EXPECTED_INGESTION_OBJECTS
    )
    assert len(topology) == 4

    assert {(kind, name): sql for kind, name, sql in topology} == EXPECTED_DDL
    with sqlite3.connect(database_path) as actual, sqlite3.connect(":memory:") as exact:
        for ddl in EXPECTED_DDL.values():
            exact.execute(ddl)
        for table in ("NioIngestMeta", "NioIngestSourceState", "NioIngestFrame"):
            for pragma in ("table_info", "foreign_key_list"):
                assert tuple(actual.execute(f"PRAGMA {pragma}({table})")) == tuple(
                    exact.execute(f"PRAGMA {pragma}({table})")
                )
        assert tuple(
            actual.execute("PRAGMA index_xinfo(NioIngestFrame_drain)")
        ) == tuple(exact.execute("PRAGMA index_xinfo(NioIngestFrame_drain)"))


_SCHEMA_TEXT_IDENTITIES = (
    ("NioIngestMeta", "account_id"),
    ("NioIngestMeta", "device_id"),
    ("NioIngestMeta", "stream_id"),
    ("NioIngestMeta", "transport_kind"),
    ("NioIngestMeta", "writer_epoch"),
    ("NioIngestSourceState", "account_id"),
    ("NioIngestFrame", "account_id"),
    ("NioIngestFrame", "frame_id"),
)


@pytest.mark.parametrize(
    ("table", "column", "invalid"),
    (
        *(
            (table, column, sqlite3.Binary(b"not-text"))
            for table, column in _SCHEMA_TEXT_IDENTITIES
        ),
        *((table, column, "") for table, column in _SCHEMA_TEXT_IDENTITIES),
        *(
            (table, column, 1.5)
            for table, columns in (
                (
                    "NioIngestMeta",
                    (
                        "schema_version",
                        "revision",
                        "next_source_epoch",
                        "created_at_ns",
                    ),
                ),
                (
                    "NioIngestSourceState",
                    ("source_epoch", "next_request_id", "active"),
                ),
                (
                    "NioIngestFrame",
                    ("source_epoch", "request_id", "staged_revision"),
                ),
            )
            for column in columns
        ),
        ("NioIngestMeta", "revision", -1),
        ("NioIngestMeta", "next_source_epoch", 0),
        ("NioIngestMeta", "created_at_ns", -1),
        ("NioIngestSourceState", "source_epoch", -1),
        ("NioIngestSourceState", "next_request_id", -1),
        ("NioIngestSourceState", "active", 2),
        ("NioIngestFrame", "source_epoch", -1),
        ("NioIngestFrame", "request_id", -1),
        ("NioIngestFrame", "staged_revision", 0),
        ("NioIngestSourceState", "cursor_ciphertext", bytes(28)),
        ("NioIngestSourceState", "cursor_ciphertext", "x" * 29),
        ("NioIngestFrame", "payload_ciphertext", bytes(28)),
        ("NioIngestFrame", "payload_ciphertext", "x" * 29),
        ("NioIngestSourceState", "cursor_sha256", "x" * 32),
        ("NioIngestSourceState", "cursor_sha256", bytes(31)),
        ("NioIngestSourceState", "cursor_sha256", bytes(33)),
        ("NioIngestFrame", "payload_sha256", "x" * 32),
        ("NioIngestFrame", "payload_sha256", bytes(31)),
        ("NioIngestFrame", "payload_sha256", bytes(33)),
    ),
)
def test_exact_schema_rejects_wrong_storage_classes_and_shapes(
    table: str, column: str, invalid: object
) -> None:
    rows = {
        "NioIngestMeta": (
            (
                "account_id",
                "device_id",
                "schema_version",
                "stream_id",
                "transport_kind",
                "revision",
                "writer_epoch",
                "next_source_epoch",
                "created_at_ns",
            ),
            (ACCOUNT_ID, DEVICE_ID, 1, str(uuid4()), "classic", 0, str(uuid4()), 1, 0),
        ),
        "NioIngestSourceState": (
            (
                "account_id",
                "source_epoch",
                "cursor_ciphertext",
                "cursor_sha256",
                "next_request_id",
                "active",
            ),
            (ACCOUNT_ID, 0, bytes(29), bytes(32), 1, 1),
        ),
        "NioIngestFrame": (
            (
                "account_id",
                "frame_id",
                "source_epoch",
                "request_id",
                "payload_ciphertext",
                "payload_sha256",
                "staged_revision",
            ),
            (ACCOUNT_ID, str(uuid4()), 0, 1, bytes(29), bytes(32), 1),
        ),
    }
    with sqlite3.connect(":memory:") as connection:
        for ddl in EXPECTED_DDL.values():
            connection.execute(ddl)
        if table != "NioIngestMeta":
            meta_columns, meta_values = rows["NioIngestMeta"]
            connection.execute(
                f"INSERT INTO NioIngestMeta ({', '.join(meta_columns)}) "
                f"VALUES ({', '.join('?' for _ in meta_columns)})",
                meta_values,
            )
        columns, valid_values = rows[table]
        values = list(valid_values)
        values[columns.index(column)] = invalid
        with pytest.raises(sqlite3.IntegrityError, match="CHECK constraint failed"):
            connection.execute(
                f"INSERT INTO {table} ({', '.join(columns)}) "
                f"VALUES ({', '.join('?' for _ in columns)})",
                values,
            )


def test_source_only_state_surface_is_exact() -> None:
    assert tuple(field.name for field in fields(OwnerView)) == (
        "account_id",
        "device_id",
        "schema_version",
        "stream_id",
        "transport_kind",
        "revision",
        "writer_epoch",
        "next_source_epoch",
    )
    assert tuple(field.name for field in fields(SourceState)) == (
        "source_epoch",
        "transport_kind",
        "cursor_json",
        "next_request_id",
        "active",
    )
    assert tuple(field.name for field in fields(StagedFrame)) == (
        "frame_id",
        "response",
        "staged_revision",
    )
    assert tuple(field.name for field in fields(CommitResult)) == ("revision",)

    for removed in (
        "RoomState",
        "RoomLane",
        "RoomAggregate",
        "LaneRecord",
        "ReadyRecord",
        "BatchMaterialization",
        "JournalTransition",
        "AckOutcome",
        "ConsumerAttachStatus",
    ):
        assert not hasattr(ingest_state, removed), removed


def test_source_only_journal_port_is_exact() -> None:
    methods = {
        name: value
        for name, value in vars(IngestionJournal).items()
        if not name.startswith("_") and inspect.isfunction(value)
    }
    assert set(methods) == {
        "load_owner",
        "load_source",
        "load_frame",
        "list_frames",
        "stage_source_response",
    }

    assert tuple(inspect.signature(methods["load_owner"]).parameters) == ("self",)
    assert tuple(inspect.signature(methods["load_source"]).parameters) == ("self",)
    assert tuple(inspect.signature(methods["load_frame"]).parameters) == (
        "self",
        "frame_id",
    )
    assert tuple(inspect.signature(methods["list_frames"]).parameters) == (
        "self",
        "limit",
    )
    stage_parameters = tuple(
        inspect.signature(methods["stage_source_response"]).parameters.values()
    )
    assert tuple(parameter.name for parameter in stage_parameters) == (
        "self",
        "expected_revision",
        "writer_epoch",
        "source",
        "frame",
    )
    assert stage_parameters[0].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
    assert all(
        parameter.kind is inspect.Parameter.KEYWORD_ONLY
        for parameter in stage_parameters[1:]
    )
    assert all(
        parameter.default is inspect.Parameter.empty for parameter in stage_parameters
    )

    hints = {name: get_type_hints(method) for name, method in methods.items()}
    assert hints == {
        "load_owner": {"return": OwnerView},
        "load_source": {"return": SourceState},
        "load_frame": {
            "frame_id": UUID,
            "return": StagedFrame | None,
        },
        "list_frames": {
            "limit": int,
            "return": tuple[StagedFrame, ...],
        },
        "stage_source_response": {
            "expected_revision": int,
            "writer_epoch": UUID,
            "source": SourceState,
            "frame": StagedFrame,
            "return": CommitResult,
        },
    }


def test_source_only_ingestion_config_is_exact() -> None:
    assert tuple(field.name for field in fields(IngestionConfig)) == (
        "source",
        "max_staged_frames",
        "sqlite_busy_timeout_ms",
    )

    config = IngestionConfig(CLASSIC_SOURCE)
    assert config.max_staged_frames == 2
    assert config.sqlite_busy_timeout_ms == 2_000

    for bound in (1, 256):
        assert (
            IngestionConfig(CLASSIC_SOURCE, max_staged_frames=bound).max_staged_frames
            == bound
        )

    invalid_values = (
        ("max_staged_frames", (0, 257, True, 1.0)),
        ("sqlite_busy_timeout_ms", (0, -1, True, 1.0)),
    )
    for field_name, values in invalid_values:
        for value in values:
            error = TypeError if type(value) is not int else ValueError
            with pytest.raises(error, match=field_name):
                IngestionConfig(CLASSIC_SOURCE, **{field_name: value})


@pytest.mark.parametrize("precreate_zero_length", (False, True))
def test_absent_or_zero_length_path_creates_only_the_exact_ingestion_topology(
    tmp_path: Path,
    precreate_zero_length: bool,
) -> None:
    database_path = tmp_path / "journal.db"
    if precreate_zero_length:
        database_path.write_bytes(b"")

    bootstrap = _open(tmp_path)
    bootstrap.close()

    _assert_exact_ingestion_topology(database_path)


def _create_rejected_path(tmp_path: Path, case: str) -> Path:
    database_path = tmp_path / "journal.db"
    if case == "legacy":
        with sqlite3.connect(database_path) as connection:
            connection.execute("CREATE TABLE LegacyState (value TEXT NOT NULL)")
            connection.execute("INSERT INTO LegacyState VALUES ('preserve me')")
        return database_path
    if case == "e2ee_only":
        store = SqliteStore(
            ACCOUNT_ID,
            DEVICE_ID,
            str(tmp_path),
            pickle_key="secret",
            database_name=database_path.name,
        )
        store.database.close()
        return database_path
    if case == "abandoned_current_v1":
        with sqlite3.connect(database_path) as connection:
            connection.execute(
                "CREATE TABLE NioIngestMeta (account_id TEXT PRIMARY KEY, "
                "device_id TEXT NOT NULL, schema_version INTEGER NOT NULL, "
                "binding_operation_id TEXT NOT NULL, writer_epoch TEXT NOT NULL)"
            )
            connection.execute(
                "CREATE TABLE NioIngestRoomState (account_id TEXT NOT NULL, "
                "room_id TEXT NOT NULL, PRIMARY KEY (account_id, room_id))"
            )
        return database_path

    foreign_cases = {"foreign_account", "foreign_device", "foreign_transport"}
    bootstrap = _open(
        tmp_path,
        SLIDING_SOURCE if case == "foreign_transport" else CLASSIC_SOURCE,
        account_id="@mallory:example.org" if case == "foreign_account" else ACCOUNT_ID,
        device_id="OTHER" if case == "foreign_device" else DEVICE_ID,
    )
    bootstrap.close()
    if case in foreign_cases:
        return database_path

    with sqlite3.connect(database_path) as connection:
        if case == "extra_object":
            connection.execute("CREATE TABLE NioIngestUnexpected (value TEXT)")
        elif case == "missing_column":
            connection.execute("ALTER TABLE NioIngestFrame DROP COLUMN payload_sha256")
        elif case == "missing_check":
            sql = connection.execute(
                "SELECT sql FROM sqlite_master WHERE name = 'NioIngestSourceState'"
            ).fetchone()[0]
            changed = sql.replace(
                "active INTEGER NOT NULL CHECK (\n"
                "        typeof(active) = 'integer' AND active IN (0, 1)\n"
                "    )",
                "active INTEGER NOT NULL",
            )
            assert changed != sql
            connection.execute("PRAGMA writable_schema = ON")
            connection.execute(
                "UPDATE sqlite_master SET sql = ? "
                "WHERE type = 'table' AND name = 'NioIngestSourceState'",
                (changed,),
            )
            connection.execute("PRAGMA writable_schema = OFF")
            connection.execute("PRAGMA schema_version = 2")
        elif case == "missing_index":
            connection.execute("DROP INDEX NioIngestFrame_drain")
        else:
            raise AssertionError(f"unknown rejected-path case: {case}")
    return database_path


def _writer_epoch(database_path: Path) -> str | None:
    with sqlite3.connect(database_path) as connection:
        try:
            row = connection.execute(
                "SELECT writer_epoch FROM NioIngestMeta LIMIT 1"
            ).fetchone()
        except sqlite3.OperationalError:
            return None
    return None if row is None else row[0]


def _assert_read_only_preflight(statements: list[str]) -> None:
    forbidden = ("CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE", "REPLACE")
    assert not [sql for sql in statements if sql.lstrip().upper().startswith(forbidden)]
    write_pragmas = (
        "JOURNAL_MODE",
        "WAL_CHECKPOINT",
        "WRITABLE_SCHEMA",
        "SCHEMA_VERSION",
        "USER_VERSION",
        "APPLICATION_ID",
        "LOCKING_MODE",
    )
    assert not [
        sql
        for sql in statements
        if sql.lstrip().upper().startswith("PRAGMA")
        and ("=" in sql or any(name in sql.upper() for name in write_pragmas))
    ]


@pytest.mark.parametrize(
    "case",
    (
        "legacy",
        "e2ee_only",
        "abandoned_current_v1",
        "extra_object",
        "missing_column",
        "missing_check",
        "missing_index",
        "foreign_account",
        "foreign_device",
        "foreign_transport",
    ),
)
def test_nonfresh_preflight_rejects_without_any_write_or_store_construction(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    database_path = _create_rejected_path(tmp_path, case)
    bytes_before = database_path.read_bytes()
    schema_before = _all_schema_objects(database_path)
    epoch_before = _writer_epoch(database_path)
    statements: list[str] = []
    store_constructions: list[tuple[object, ...]] = []

    def reject_store_construction(
        _self: object, *args: object, **_kwargs: object
    ) -> None:
        store_constructions.append(args)
        raise AssertionError("rejected preflight constructed SqliteStore")

    monkeypatch.setattr(SqliteStore, "__init__", reject_store_construction)
    with pytest.raises(FreshIngestionRequired):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            source=CLASSIC_SOURCE,
            pickle_key="secret",
            database_name=database_path.name,
            statement_observer=statements.append,
        )

    _assert_read_only_preflight(statements)
    assert store_constructions == []
    assert _writer_epoch(database_path) == epoch_before
    assert database_path.read_bytes() == bytes_before
    assert _all_schema_objects(database_path) == schema_before


def test_exact_same_owner_reopen_preserves_identity_and_topology(
    tmp_path: Path,
) -> None:
    first = _open(tmp_path)
    owner_before = first._journal.load_owner()
    source_before = first._journal.load_source()
    first.close()
    statements: list[str] = []
    reopened = _open(tmp_path, statements=statements)
    try:
        owner_after = reopened._journal.load_owner()
        assert owner_after == replace(
            owner_before, writer_epoch=owner_after.writer_epoch
        )
        assert owner_after.writer_epoch != owner_before.writer_epoch
        assert reopened._journal.load_source() == source_before
        _assert_exact_ingestion_topology(tmp_path / "journal.db")
        assert not [sql for sql in statements if "CREATE " in sql.upper()]
    finally:
        reopened.close()


@pytest.mark.parametrize("source_config", (CLASSIC_SOURCE, SLIDING_SOURCE))
def test_stage_is_atomic_and_exact_restage_is_write_free(
    tmp_path: Path,
    source_config: ClassicSourceConfig | SlidingSourceConfig,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, source_config, statements=statements)
    journal = bootstrap._journal
    try:
        owner_before = journal.load_owner()
        proposal = _stage_proposal(journal, source_config, 1)
        statements.clear()
        result = _stage(journal, proposal=proposal)

        assert result == CommitResult(owner_before.revision + 1)
        business_dml = _business_dml(statements)
        assert sum("UPDATE NioIngestMeta" in sql for sql in business_dml) == 1
        assert sum("NioIngestSourceState" in sql for sql in business_dml) == 1
        assert sum("NioIngestFrame" in sql for sql in business_dml) == 1
        assert journal.load_owner().revision == owner_before.revision + 1
        assert journal.load_source() == proposal.successor_source
        assert journal.load_frame(proposal.frame.frame_id) == replace(
            proposal.frame,
            staged_revision=owner_before.revision + 1,
        )
        assert journal.list_frames(256) == (
            replace(
                proposal.frame,
                staged_revision=owner_before.revision + 1,
            ),
        )
        statements.clear()
        repeated = _stage(journal, expected_revision=result.revision, proposal=proposal)

        assert repeated == result
        assert _business_dml(statements) == []
        assert journal.load_owner().revision == result.revision
        assert journal.load_source() == proposal.successor_source
        assert journal.load_frame(proposal.frame.frame_id) == replace(
            proposal.frame,
            staged_revision=result.revision,
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("mutation", ("request", "body", "digest", "revision"))
def test_same_frame_id_changed_content_or_revision_fails_before_dml(
    tmp_path: Path,
    mutation: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements=statements)
    journal = bootstrap._journal
    try:
        owner = journal.load_owner()
        proposal = _stage_proposal(journal, CLASSIC_SOURCE, 1)
        committed = _stage(journal, proposal=proposal)
        stored = journal.load_frame(proposal.frame.frame_id)
        assert stored is not None

        if mutation == "request":
            changed_request = replace(
                proposal.frame.response.request,
                timeout_ms=proposal.frame.response.request.timeout_ms + 1,
            )
            changed = _frame_for_request(
                changed_request,
                proposal.frame.response.response_body,
            )
        elif mutation == "body":
            changed = _frame_for_request(
                proposal.frame.response.request,
                canonical_json({"next_batch": "changed"}),
            )
            object.__setattr__(changed, "frame_id", proposal.frame.frame_id)
        elif mutation == "digest":
            changed = replace(proposal.frame)
            changed_response = replace(proposal.frame.response)
            object.__setattr__(changed_response, "source_sha256", bytes(32))
            object.__setattr__(changed, "response", changed_response)
        else:
            changed = replace(proposal.frame, staged_revision=999)

        changed_proposal = replace(proposal, frame=changed)
        statements.clear()
        with pytest.raises(JournalIntegrityError):
            _stage(
                journal,
                expected_revision=committed.revision,
                proposal=changed_proposal,
            )

        assert _business_dml(statements) == []
        assert journal.load_owner().revision == committed.revision
        assert journal.load_source() == proposal.successor_source
        assert journal.load_frame(proposal.frame.frame_id) == stored
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("mutation", "expected_error"),
    (
        ("expected_revision", JournalConflictError),
        ("writer_epoch", JournalConflictError),
        ("stream", JournalIntegrityError),
        ("transport", JournalIntegrityError),
        ("source_epoch", JournalIntegrityError),
        ("request_id", JournalIntegrityError),
        ("cursor_relation", JournalIntegrityError),
    ),
)
def test_stage_rejects_owner_source_and_request_relation_drift_before_dml(
    tmp_path: Path,
    mutation: str,
    expected_error: type[Exception],
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements=statements)
    journal = bootstrap._journal
    try:
        owner = journal.load_owner()
        proposal = _stage_proposal(journal, CLASSIC_SOURCE, 1)
        expected_revision = owner.revision
        writer_epoch = owner.writer_epoch

        if mutation == "expected_revision":
            expected_revision += 1
        elif mutation == "writer_epoch":
            writer_epoch = uuid4()
        elif mutation == "source_epoch":
            proposal = replace(
                proposal,
                successor_source=replace(
                    proposal.successor_source,
                    source_epoch=proposal.successor_source.source_epoch + 1,
                ),
            )
        else:
            request = proposal.frame.response.request
            if mutation == "stream":
                request = replace(request, stream_id=uuid4())
            elif mutation == "transport":
                request = replace(request, transport=TransportKind.SLIDING)
                proposal = replace(
                    proposal,
                    successor_source=replace(
                        proposal.successor_source,
                        transport_kind=TransportKind.SLIDING,
                    ),
                )
            elif mutation == "request_id":
                request = replace(request, request_id=request.request_id + 1)
                proposal = replace(
                    proposal,
                    successor_source=replace(
                        proposal.successor_source,
                        next_request_id=request.request_id + 1,
                    ),
                )
            else:
                assert mutation == "cursor_relation"
                request = replace(
                    request,
                    request_cursor_json=b'{"next_batch":"foreign"}',
                )
            proposal = replace(
                proposal,
                frame=_frame_for_request(
                    request,
                    proposal.frame.response.response_body,
                ),
            )

        statements.clear()
        with pytest.raises(expected_error):
            _stage(
                journal,
                expected_revision=expected_revision,
                writer_epoch=writer_epoch,
                proposal=proposal,
            )

        assert _business_dml(statements) == []
        assert journal.load_owner() == owner
        assert journal.load_source() == proposal.prior_source
        assert journal.list_frames(256) == ()
    finally:
        bootstrap.close()


def test_list_frames_accepts_exact_limits_and_uses_deterministic_drain_order(
    tmp_path: Path,
) -> None:
    source_config = CLASSIC_SOURCE
    statements: list[str] = []
    bootstrap = _open(tmp_path, source_config, statements=statements)
    journal = bootstrap._journal
    owner = journal.load_owner()
    committed = _stage(
        journal,
        proposal=_stage_proposal(journal, source_config, 1),
    )
    bootstrap.close()

    codec = EncryptedRowCodec("secret", ACCOUNT_ID, owner.stream_id)
    frames: list[StagedFrame] = []
    for epoch, request_id, marker in (
        (2, 0, "z"),
        (1, 2, "y"),
        (1, 1, "b"),
        (1, 1, "a"),
    ):
        request = NetworkRequest(
            owner.stream_id,
            owner.transport_kind,
            epoch,
            request_id,
            "GET",
            "/_matrix/client/v3/sync",
            (),
            None,
            30_000,
            b'{"next_batch":null}',
        )
        frame = _frame_for_request(
            request,
            canonical_json({"marker": marker, "next_batch": f"s-{marker}"}),
        )
        frames.append(replace(frame, staged_revision=committed.revision))
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.execute("DELETE FROM NioIngestFrame")
        for frame in frames:
            payload = _canonical_internal(_frame_envelope(frame))
            ciphertext, digest = codec.seal(
                "NioIngestFrame",
                (frame.frame_id,),
                payload,
                header=_frame_header(frame, committed.revision),
            )
            connection.execute(
                "INSERT INTO NioIngestFrame (account_id, frame_id, source_epoch, "
                "request_id, payload_ciphertext, payload_sha256, staged_revision) "
                "VALUES (?, ?, ?, ?, ?, ?, ?)",
                (
                    ACCOUNT_ID,
                    str(frame.frame_id),
                    frame.response.request.source_epoch,
                    frame.response.request.request_id,
                    ciphertext,
                    digest,
                    committed.revision,
                ),
            )

    reopened = _open(tmp_path, source_config, statements=statements)
    try:
        expected = tuple(
            sorted(
                frames,
                key=lambda frame: (
                    frame.staged_revision,
                    frame.response.request.source_epoch,
                    frame.response.request.request_id,
                    str(frame.frame_id),
                ),
            )
        )
        statements.clear()
        assert reopened._journal.list_frames(256) == expected
        selected = [sql for sql in statements if "FROM NioIngestFrame" in sql]
        assert [_normalized_sql(sql) for sql in selected] == [
            _normalized_sql(
                LIST_FRAMES_SQL.replace("?", f"'{ACCOUNT_ID}'", 1).replace(
                    "?", "256", 1
                )
            )
        ]
        for limit in range(1, 257):
            assert reopened._journal.list_frames(limit) == expected[:limit]
        for invalid_limit in (0, 257, True, 1.0):
            with pytest.raises(ValueError, match="frame limit"):
                reopened._journal.list_frames(invalid_limit)
        with sqlite3.connect(tmp_path / "journal.db") as connection:
            plan = connection.execute(
                f"EXPLAIN QUERY PLAN {LIST_FRAMES_SQL}",
                (ACCOUNT_ID, 256),
            ).fetchall()
        assert any("NioIngestFrame_drain" in row[3] for row in plan)
    finally:
        reopened.close()


def test_checkpoint_exposes_no_raw_connection_or_frame_consume_operation(
    tmp_path: Path,
) -> None:
    from nio.store._sync_journal import SqliteIngestionJournal

    bootstrap = _open(tmp_path)
    try:
        assert not hasattr(bootstrap._journal, "connection")
    finally:
        bootstrap.close()
    for deferred_method in (
        "commit",
        "delete_frame",
        "consume_frame",
        "acknowledge",
        "oldest_unacknowledged",
    ):
        assert not hasattr(IngestionJournal, deferred_method), deferred_method
        assert not hasattr(SqliteIngestionJournal, deferred_method), deferred_method


@pytest.mark.parametrize("source_config", (CLASSIC_SOURCE, SLIDING_SOURCE))
def test_authenticated_headers_and_frame_envelope_are_exact_and_canonical(
    tmp_path: Path,
    source_config: ClassicSourceConfig | SlidingSourceConfig,
) -> None:
    stage = _stage_one(tmp_path, source_config)
    stored = replace(
        stage.proposal.frame,
        staged_revision=stage.committed.revision,
    )
    codec = _codec_for(stage)
    frame_row = _stored_row(stage.database_path, stored.frame_id)
    frame_payload = codec.decrypt(
        "NioIngestFrame",
        (stored.frame_id,),
        bytes(frame_row["payload_ciphertext"]),
        bytes(frame_row["payload_sha256"]),
        header=_frame_header(stored, stage.committed.revision),
    )
    assert frame_payload == _canonical_internal(_frame_envelope(stored))
    assert tuple(json.loads(frame_payload)) == (
        "normalization_version",
        "request",
        "response_body",
        "source_sha256",
    )
    assert frame_payload.count(base64.b64encode(stored.response.response_body)) == 1
    for normalized_only_field in (
        b'"candidate_cursor_json"',
        b'"room_segments"',
        b'"membership_observation"',
        b'"to_device_json"',
        b'"ephemeral_json"',
    ):
        assert normalized_only_field not in frame_payload

    source_row = _stored_row(stage.database_path)
    assert (
        codec.decrypt(
            "NioIngestSourceState",
            (ACCOUNT_ID,),
            bytes(source_row["cursor_ciphertext"]),
            bytes(source_row["cursor_sha256"]),
            header=_source_header(stage.proposal.successor_source),
        )
        == stage.proposal.successor_source.cursor_json
    )


@pytest.mark.parametrize(
    "corruption",
    (
        "source_transport",
        "source_epoch",
        "source_next_request_id",
        "source_active",
        "frame_source_epoch",
        "frame_request_id",
        "frame_staged_revision",
        "frame_primary_key",
        "account",
        "owner_stream",
        "frame_ciphertext",
        "frame_digest",
        "frame_ciphertext_version",
        "source_ciphertext",
        "source_digest_ciphertext",
        "normalization_version",
        "missing_key",
        "extra_key",
        "noncanonical_bytes",
        "nested_request",
        "response_body",
        "source_digest",
    ),
)
def test_authenticated_corruption_fails_selected_row(
    tmp_path: Path,
    corruption: str,
) -> None:
    stage = _stage_one(tmp_path)
    frame = stage.proposal.frame
    codec = _codec_for(stage)
    if corruption.startswith("source_") and corruption not in {
        "source_ciphertext",
        "source_digest_ciphertext",
        "source_digest",
    }:
        source = stage.proposal.successor_source
        changes = {
            "source_transport": {"transport_kind": TransportKind.SLIDING},
            "source_epoch": {"source_epoch": source.source_epoch + 1},
            "source_next_request_id": {"next_request_id": source.next_request_id + 1},
            "source_active": {"active": not source.active},
        }[corruption]
        row = _stored_row(stage.database_path)
        with pytest.raises(JournalIntegrityError):
            codec.decrypt(
                "NioIngestSourceState",
                (ACCOUNT_ID,),
                bytes(row["cursor_ciphertext"]),
                bytes(row["cursor_sha256"]),
                header=_source_header(replace(source, **changes)),
            )
        return

    row = _stored_row(stage.database_path, frame.frame_id)
    header = _frame_header(frame, stage.committed.revision)
    if corruption in {
        "frame_source_epoch",
        "frame_request_id",
        "frame_staged_revision",
        "frame_primary_key",
        "account",
        "owner_stream",
    }:
        values = [
            frame.response.request.source_epoch,
            frame.response.request.request_id,
            stage.committed.revision,
        ]
        if corruption.startswith("frame_") and corruption != "frame_primary_key":
            values[
                {
                    "frame_source_epoch": 0,
                    "frame_request_id": 1,
                    "frame_staged_revision": 2,
                }[corruption]
            ] += 1
        key = (uuid4(),) if corruption == "frame_primary_key" else (frame.frame_id,)
        if corruption == "account":
            codec = _codec_for(stage, "@mallory:example.org")
        elif corruption == "owner_stream":
            codec = EncryptedRowCodec("secret", ACCOUNT_ID, uuid4())
        with pytest.raises(JournalIntegrityError):
            codec.decrypt(
                "NioIngestFrame",
                key,
                bytes(row["payload_ciphertext"]),
                bytes(row["payload_sha256"]),
                header=_canonical_internal(values),
            )
        return

    if corruption in {
        "frame_ciphertext",
        "frame_digest",
        "frame_ciphertext_version",
        "source_ciphertext",
        "source_digest_ciphertext",
    }:
        is_frame = corruption.startswith("frame_")
        row = row if is_frame else _stored_row(stage.database_path)
        digest = corruption in {"frame_digest", "source_digest_ciphertext"}
        prefix = "payload" if is_frame else "cursor"
        column = f"{prefix}_{'sha256' if digest else 'ciphertext'}"
        value = bytes(32) if digest else bytearray(row[column])
        if isinstance(value, bytearray):
            value[0 if corruption.endswith("version") else -1] ^= 1
        table = "NioIngestFrame" if is_frame else "NioIngestSourceState"
        where = "account_id = ? AND frame_id = ?" if is_frame else "account_id = ?"
        keys = (ACCOUNT_ID, str(frame.frame_id)) if is_frame else (ACCOUNT_ID,)
        with sqlite3.connect(stage.database_path) as connection:
            connection.execute(
                f"UPDATE {table} SET {column} = ? WHERE {where}", (bytes(value), *keys)
            )
        if not is_frame:
            with pytest.raises(JournalIntegrityError):
                _open(tmp_path)
            return
    else:
        payload = codec.decrypt(
            "NioIngestFrame",
            (frame.frame_id,),
            bytes(row["payload_ciphertext"]),
            bytes(row["payload_sha256"]),
            header=header,
        )
        envelope = json.loads(payload)
        if corruption == "normalization_version":
            envelope["normalization_version"] = 2
        elif corruption == "missing_key":
            del envelope["request"]
        elif corruption == "extra_key":
            envelope["extra"] = None
        elif corruption == "nested_request":
            envelope["request"]["request_id"] += 1
        elif corruption == "response_body":
            envelope["response_body"] = _encoded_bytes(b'{"changed":true}')
        elif corruption == "source_digest":
            envelope["source_sha256"] = _encoded_bytes(bytes(32))
        changed = (
            payload + b" "
            if corruption == "noncanonical_bytes"
            else _canonical_internal(envelope)
        )
        ciphertext, digest = codec.seal(
            "NioIngestFrame", (frame.frame_id,), changed, header=header
        )
        with sqlite3.connect(stage.database_path) as connection:
            connection.execute(
                "UPDATE NioIngestFrame SET payload_ciphertext = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (ciphertext, digest, ACCOUNT_ID, str(frame.frame_id)),
            )
    reopened = _open(tmp_path)
    try:
        with pytest.raises(JournalIntegrityError):
            reopened._journal.load_frame(frame.frame_id)
    finally:
        reopened.close()


def _kill_stage_at_hook(
    store_path: Path,
    proposal: _StageProposal,
    requested_label: str,
    sequence_path: Path,
) -> None:
    bootstrap = _open(store_path)
    journal = bootstrap._journal

    def kill_at_boundary(label: str) -> None:
        with sequence_path.open("a", encoding="utf-8") as sequence:
            sequence.write(f"{label}\n")
            sequence.flush()
            os.fsync(sequence.fileno())
        if label == requested_label:
            os._exit(CRASH_EXIT_CODE)

    journal.set_transition_statement_hook(kill_at_boundary)
    _stage(journal, proposal=proposal)
    bootstrap.close()


def _assert_stage_process_crashed(
    store_path: Path,
    proposal: _StageProposal,
    requested_label: str,
) -> tuple[str, ...]:
    sequence_path = store_path / "stage-hook-sequence.txt"
    process = multiprocessing.get_context("spawn").Process(
        target=_kill_stage_at_hook,
        args=(store_path, proposal, requested_label, sequence_path),
    )
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("stage crash-injection child did not exit")
    assert process.exitcode == CRASH_EXIT_CODE
    return tuple(sequence_path.read_text(encoding="utf-8").splitlines())


STAGE_HOOK_LABELS = (
    "frame_collision_probe",
    "meta_revision_epoch_cas",
    "source_state_upsert",
    "frame_insert",
    "commit",
)


@pytest.mark.parametrize("boundary", STAGE_HOOK_LABELS)
def test_stage_crash_boundary_reopens_to_exact_old_or_new_graph(
    tmp_path: Path,
    boundary: str,
) -> None:
    store_path = tmp_path / boundary
    bootstrap = _open(store_path)
    owner = bootstrap._journal.load_owner()
    proposal = _stage_proposal(bootstrap._journal, CLASSIC_SOURCE, 1)
    bootstrap.close()

    observed = _assert_stage_process_crashed(store_path, proposal, boundary)
    assert observed == STAGE_HOOK_LABELS[: STAGE_HOOK_LABELS.index(boundary) + 1]

    reopened = _open(store_path)
    try:
        actual_graph = (
            reopened._journal.load_owner().revision,
            reopened._journal.load_source(),
            reopened._journal.load_frame(proposal.frame.frame_id),
        )
        old_graph = (owner.revision, proposal.prior_source, None)
        new_graph = (
            owner.revision + 1,
            proposal.successor_source,
            replace(proposal.frame, staged_revision=owner.revision + 1),
        )
        assert actual_graph in (old_graph, new_graph)
        assert (actual_graph[1] == proposal.successor_source) is (
            actual_graph[2] is not None
        )
    finally:
        reopened.close()


@pytest.mark.parametrize(
    ("source_config", "drifted_config"),
    (
        pytest.param(
            CLASSIC_SOURCE,
            ClassicSourceConfig(
                timeout_ms=90_000,
                filter_json=b'{"room":{"timeline":{"limit":99}}}',
            ),
            id="classic",
        ),
        pytest.param(
            SLIDING_SOURCE,
            SlidingSourceConfig(
                timeout_ms=90_000,
                connection_name="drifted",
                lists_json=b'{"caller":{"ranges":[[0,9]]}}',
                room_subscriptions_json=b'{"!other:example.org":{}}',
                extensions_json=b'{"custom":{"enabled":true}}',
                all_rooms_page_size=99,
            ),
            id="sliding",
        ),
    ),
)
def test_reopened_deserialized_frame_renormalizes_exactly_across_config_drift(
    tmp_path: Path,
    source_config: ClassicSourceConfig | SlidingSourceConfig,
    drifted_config: ClassicSourceConfig | SlidingSourceConfig,
) -> None:
    bootstrap = _open(tmp_path, source_config)
    owner = bootstrap._journal.load_owner()
    proposal = _stage_proposal(bootstrap._journal, source_config, 1)
    expected = renormalize_staged_frame(
        _source_adapter(owner.stream_id, source_config),
        proposal.frame,
    )
    committed = _stage(
        bootstrap._journal,
        expected_revision=owner.revision,
        proposal=proposal,
    )
    bootstrap.close()

    reopened = _open(tmp_path, drifted_config)
    try:
        loaded = reopened._journal.load_frame(proposal.frame.frame_id)
        assert loaded == replace(
            proposal.frame,
            staged_revision=committed.revision,
        )
        assert loaded is not None
        actual = renormalize_staged_frame(
            _source_adapter(reopened._journal.load_owner().stream_id, drifted_config),
            loaded,
        )

        assert actual == expected
        assert actual.frame_id == proposal.frame.frame_id
        assert actual.source_sha256 == proposal.frame.response.source_sha256
        assert loaded.response.source_sha256 == proposal.frame.response.source_sha256
        observations = tuple(
            segment.membership_observation for segment in actual.room_segments
        )
        assert observations == tuple(
            segment.membership_observation for segment in expected.room_segments
        )
        assert tuple(
            (
                observation.room_membership,
                observation.event_membership,
                observation.event_id,
                observation.is_initial,
            )
            for observation in observations
        ) == (("join", "join", "$own-1", True),)
    finally:
        reopened.close()
