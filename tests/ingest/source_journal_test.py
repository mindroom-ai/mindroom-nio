from ingestion_helpers import materialize_journal, retire_completed_frame
import base64
import hashlib
import importlib.util
import inspect
import json
import multiprocessing
import os
import resource
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from peewee import OperationalError as PeeweeOperationalError

from nio.crypto import OlmAccount
from nio.exceptions import LocalProtocolError
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
from nio.ingest.sliding import (
    RESERVED_ALL_ROOMS_LIST,
    SlidingCursor,
    SlidingRangeAckMode,
    SlidingSource,
    _sliding_cursor_from_json,
)
from nio.ingest.source import SyncFrame, canonical_json, renormalize_staged_frame
from nio.ingest.state import CommitResult, SourceState, StagedFrame
from nio.store import SqliteStore
from nio.store._sync_journal_port import IngestionJournal
from nio.store._sync_journal_values import (
    SQLITE_INT_MAX,
    MaterializerLimits,
    MaterializeStatus,
)
from ingestion_helpers import open_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
CONSUMER_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
OTHER_CONSUMER_GENERATION = UUID("33333333-3333-4333-8333-333333333333")
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
FRAME_CLASSIFICATION_LIMIT = 259
LARGE_PAYLOAD_PLACEHOLDER_BYTES = 64 * 1024
CLASSIFY_FRAME_IDS_SQL = (
    "SELECT CASE account_id WHEN ? THEN frame_id END AS frame_id "
    "FROM NioIngestFrame LIMIT ?"
)
LOAD_FRAME_SQL = "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?"
LOAD_FRAME_HEADERS_SQL = (
    "SELECT account_id, frame_id, source_epoch, request_id, staged_revision, "
    "payload_sha256, LENGTH(payload) AS payload_length, "
    "room_materialized_revision, callbacks_claimed_revision, "
    "drain_header_sha256 FROM NioIngestFrame LIMIT ?"
)
LIST_FRAMES_SQL = (
    "SELECT * FROM NioIngestFrame WHERE account_id = ? "
    "ORDER BY staged_revision, source_epoch, request_id, frame_id LIMIT ?"
)

EXPECTED_INGESTION_OBJECTS = {
    ("table", "NioIngestRecovery"),
    ("table", "NioIngestMeta"),
    ("table", "NioIngestSourceState"),
    ("table", "NioIngestFrame"),
    ("index", "NioIngestFrame_drain"),
    ("table", "NioIngestRoomAggregate"),
    ("index", "NioIngestRoomAggregate_intent"),
    ("table", "NioIngestWork"),
    ("index", "NioIngestWork_ready"),
    ("index", "NioIngestWork_held_release"),
    ("index", "NioIngestWork_frame_kind"),
}


@dataclass(frozen=True, slots=True)
class _StageProposal:
    prior_source: SourceState
    successor_source: SourceState
    frame: StagedFrame
    normalized_frame: SyncFrame


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
        consumer_generation=CONSUMER_GENERATION,
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
    *,
    to_device_since: str | None = None,
    global_account_data_sequence: int | None = None,
    include_rooms: bool = True,
) -> _StageProposal:
    owner = journal.load_owner()  # type: ignore[attr-defined]
    prior_source = journal.load_source()  # type: ignore[attr-defined]
    adapter = _source_adapter(owner.stream_id, source_config)
    request = adapter.plan_request(prior_source, prior_source.next_request_id)
    assert request is not None
    body = _successful_body(request, sequence)
    if not include_rooms:
        decoded = json.loads(body)
        decoded["rooms"] = {}
        if "lists" in decoded:
            decoded["lists"][RESERVED_ALL_ROOMS_LIST]["count"] = 0
        body = canonical_json(decoded)
    if request.transport is TransportKind.SLIDING and (
        to_device_since is not None or global_account_data_sequence is not None
    ):
        decoded = json.loads(body)
        extensions: dict[str, object] = {}
        if to_device_since is not None:
            extensions["to_device"] = {
                "events": [],
                "next_batch": to_device_since,
            }
        if global_account_data_sequence is not None:
            extensions["account_data"] = {
                "global": [
                    {
                        "type": "io.mindroom.test",
                        "content": {"sequence": global_account_data_sequence},
                    }
                ],
                "rooms": {},
            }
        decoded["extensions"] = extensions
        body = canonical_json(decoded)
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
    return _StageProposal(prior_source, successor, frame, normalized.frame)


def _stage(
    journal: object,
    *,
    proposal: _StageProposal,
) -> CommitResult:
    return journal.stage_source_response(  # type: ignore[attr-defined,no-any-return]
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


@dataclass(frozen=True, slots=True)
class _LogicalInventory:
    frames: tuple[StagedFrame, ...]
    aggregates: tuple[tuple[str, object], ...]
    work: tuple[object, ...]
    delivery: object
    outstanding_batch: object


def _logical_inventory(journal: object) -> _LogicalInventory:
    frames = journal.list_frames(256)  # type: ignore[attr-defined]
    with journal._owner.read():  # type: ignore[attr-defined]
        owner = journal.load_owner()  # type: ignore[attr-defined]
        room_ids = tuple(
            row[0]
            for row in journal._execute(  # type: ignore[attr-defined]
                "SELECT room_id FROM NioIngestRoomAggregate "
                "WHERE account_id = ? ORDER BY room_id",
                (ACCOUNT_ID,),
            )
        )
        aggregates = tuple(
            (
                room_id,
                journal._load_room_aggregate(owner, room_id)[1],  # type: ignore[attr-defined,index]
            )
            for room_id in room_ids
        )
        work = journal._load_task3_work_inventory(owner).work  # type: ignore[attr-defined]
        delivery = journal._delivery_snapshot()[2]  # type: ignore[attr-defined]
    outstanding_batch = (
        journal.next_batch()  # type: ignore[attr-defined]
        if delivery.outstanding_work_id is not None  # type: ignore[attr-defined]
        else None
    )
    return _LogicalInventory(
        frames,
        aggregates,
        work,
        delivery,
        outstanding_batch,
    )


def _seed_sliding_reopen_inventory(journal: object) -> None:
    for sequence in (1, 2):
        proposal = _stage_proposal(
            journal,
            SLIDING_SOURCE,
            sequence,
            to_device_since=f"td{sequence}",
            global_account_data_sequence=sequence,
            include_rooms=sequence != 1,
        )
        _stage(journal, proposal=proposal)
        materialized = materialize_journal(
            journal, limits=MaterializerLimits()  # type: ignore[attr-defined]
        )
        assert materialized.revision is not None
        batch = journal.next_batch()  # type: ignore[attr-defined]
        assert batch is not None
        if sequence == 1:
            journal.acknowledge_batch(batch.ref)  # type: ignore[attr-defined]
            retire_completed_frame(journal)

    retained = _stage_proposal(
        journal,
        SLIDING_SOURCE,
        3,
        to_device_since="td3",
    )
    _stage(journal, proposal=retained)
    with journal._transaction():  # type: ignore[attr-defined]
        owner, source = journal._load_stage_snapshot()  # type: ignore[attr-defined]
        journal._write_source(replace(source, active=False), owner)  # type: ignore[attr-defined]


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


def _plaintext_frame_storage(
    stream_id: UUID,
    frame: StagedFrame,
    staged_revision: int,
    *,
    frame_id: UUID | None = None,
    value: object | None = None,
    room_materialized_revision: int | None = None,
) -> tuple[bytes, bytes, bytes]:
    stored_id = frame.frame_id if frame_id is None else frame_id
    request = frame.response.request
    payload = _expected_plaintext_row_envelope(
        row_kind="frame",
        account_id=ACCOUNT_ID,
        stream_id=stream_id,
        transport_kind=request.transport,
        clear_fields=(
            ("frame_id", str(stored_id)),
            ("source_epoch", request.source_epoch),
            ("request_id", request.request_id),
            ("staged_revision", staged_revision),
        ),
        value=_frame_envelope(frame) if value is None else value,
    )
    payload_sha256 = hashlib.sha256(payload).digest()
    drain_header = _expected_plaintext_frame_drain_header(
        stream_id=stream_id,
        frame_id=stored_id,
        source_epoch=request.source_epoch,
        request_id=request.request_id,
        staged_revision=staged_revision,
        payload_sha256=payload_sha256,
        payload_length=len(payload),
        room_materialized_revision=room_materialized_revision,
        transport_kind=request.transport,
    )
    return payload, payload_sha256, hashlib.sha256(drain_header).digest()


def _replace_plaintext_frame_value(
    stage: _StoredStage,
    value: dict[str, object],
    *,
    frame_id: UUID | None = None,
) -> UUID:
    original_id = stage.proposal.frame.frame_id
    stored_id = original_id if frame_id is None else frame_id
    payload, payload_sha256, drain_header_sha256 = _plaintext_frame_storage(
        stage.stream_id,
        stage.proposal.frame,
        stage.committed.revision,
        frame_id=stored_id,
        value=value,
    )
    with sqlite3.connect(stage.database_path) as connection:
        updated = connection.execute(
            "UPDATE NioIngestFrame SET frame_id = ?, payload = ?, "
            "payload_sha256 = ?, drain_header_sha256 = ? "
            "WHERE account_id = ? AND frame_id = ?",
            (
                str(stored_id),
                payload,
                payload_sha256,
                drain_header_sha256,
                ACCOUNT_ID,
                str(original_id),
            ),
        )
        assert updated.rowcount == 1
    return stored_id


def _stored_plaintext_frame_value(stage: _StoredStage) -> dict[str, object]:
    frame = stage.proposal.frame
    row = _stored_row(stage.database_path, frame.frame_id)
    assert tuple(row.keys()) == _PLAINTEXT_FRAME_COLUMNS
    payload = bytes(row["payload"])
    payload_sha256 = bytes(row["payload_sha256"])
    assert payload_sha256 == hashlib.sha256(payload).digest()
    assert row["drain_header_sha256"] == _plaintext_frame_header_sha256(
        stream_id=stage.stream_id,
        row=row,
        payload=payload,
        payload_sha256=payload_sha256,
        transport_kind=frame.response.request.transport,
    )
    envelope = json.loads(payload)
    assert type(envelope) is dict
    value = envelope["value"]
    assert type(value) is dict
    return value


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
    ("table", "NioIngestRecovery"): _normalized_sql("""CREATE TABLE NioIngestRecovery (
        account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id) CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        source_epoch INTEGER NOT NULL CHECK (
            typeof(source_epoch) = 'integer' AND source_epoch >= 0
        ),
        request_id INTEGER NOT NULL CHECK (
            typeof(request_id) = 'integer' AND request_id >= 0
        ),
        payload BLOB NOT NULL CHECK (
            typeof(payload) = 'blob' AND length(payload) > 0
            AND length(payload) <= 24 * 1024 * 1024
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
        ))"""),
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
        consumer_generation TEXT NOT NULL CHECK (
            typeof(consumer_generation) = 'text' AND length(consumer_generation) > 0
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
        ),
        delivery_next_sequence INTEGER NOT NULL CHECK (typeof(delivery_next_sequence) = 'integer' AND delivery_next_sequence BETWEEN 0 AND 9223372036854775807),
        delivery_acknowledged_sha256 BLOB NULL CHECK (delivery_acknowledged_sha256 IS NULL OR (typeof(delivery_acknowledged_sha256) = 'blob' AND length(delivery_acknowledged_sha256) = 32)),
        delivery_outstanding_work_id TEXT NULL CHECK (delivery_outstanding_work_id IS NULL OR (typeof(delivery_outstanding_work_id) = 'text' AND length(delivery_outstanding_work_id) > 0)),
        delivery_outstanding_ready_revision INTEGER NULL CHECK (delivery_outstanding_ready_revision IS NULL OR (typeof(delivery_outstanding_ready_revision) = 'integer' AND delivery_outstanding_ready_revision >= 1)),
        delivery_outstanding_ready_ordinal INTEGER NULL CHECK (delivery_outstanding_ready_ordinal IS NULL OR (typeof(delivery_outstanding_ready_ordinal) = 'integer' AND delivery_outstanding_ready_ordinal >= 0)),
        delivery_outstanding_batch_sha256 BLOB NULL CHECK (delivery_outstanding_batch_sha256 IS NULL OR (typeof(delivery_outstanding_batch_sha256) = 'blob' AND length(delivery_outstanding_batch_sha256) = 32)),
        CHECK (
            (delivery_outstanding_work_id IS NULL
             AND delivery_outstanding_ready_revision IS NULL
             AND delivery_outstanding_ready_ordinal IS NULL
             AND delivery_outstanding_batch_sha256 IS NULL)
         OR (delivery_outstanding_work_id IS NOT NULL
             AND delivery_outstanding_ready_revision IS NOT NULL
             AND delivery_outstanding_ready_revision <= revision
             AND delivery_outstanding_ready_ordinal IS NOT NULL
             AND delivery_outstanding_batch_sha256 IS NOT NULL)
        ),
        CHECK (
            (delivery_next_sequence = 0
             AND delivery_acknowledged_sha256 IS NULL
             AND delivery_outstanding_work_id IS NULL)
         OR (delivery_next_sequence = 1
             AND delivery_acknowledged_sha256 IS NULL
             AND delivery_outstanding_work_id IS NOT NULL)
         OR (delivery_next_sequence >= 1
             AND delivery_acknowledged_sha256 IS NOT NULL
             AND delivery_outstanding_work_id IS NULL)
         OR (delivery_next_sequence >= 2
             AND delivery_acknowledged_sha256 IS NOT NULL
             AND delivery_outstanding_work_id IS NOT NULL)
        ))"""),
    ("table", "NioIngestSourceState"): _normalized_sql(
        """CREATE TABLE NioIngestSourceState (
        account_id TEXT PRIMARY KEY REFERENCES NioIngestMeta(account_id) CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        source_epoch INTEGER NOT NULL CHECK (
            typeof(source_epoch) = 'integer' AND source_epoch >= 0
        ),
        payload BLOB NOT NULL CHECK (
            typeof(payload) = 'blob' AND length(payload) > 0
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
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
        staged_revision INTEGER NOT NULL CHECK (
            typeof(staged_revision) = 'integer' AND staged_revision >= 1
        ),
        payload BLOB NOT NULL CHECK (
            typeof(payload) = 'blob'
            AND length(payload) > 0
            AND length(payload) <= 24 * 1024 * 1024
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
        ),
        room_materialized_revision INTEGER NULL CHECK (
            room_materialized_revision IS NULL OR
            (typeof(room_materialized_revision) = 'integer'
             AND room_materialized_revision >= 1)
        ),
        callbacks_claimed_revision INTEGER NULL CHECK (
            callbacks_claimed_revision IS NULL OR
            (typeof(callbacks_claimed_revision) = 'integer'
             AND callbacks_claimed_revision >= 1)
        ),
        drain_header_sha256 BLOB NOT NULL CHECK (
            typeof(drain_header_sha256) = 'blob'
            AND length(drain_header_sha256) = 32
        ),
        PRIMARY KEY (account_id, frame_id))"""),
    ("index", "NioIngestFrame_drain"): _normalized_sql(
        """CREATE INDEX NioIngestFrame_drain ON NioIngestFrame(
        account_id, staged_revision, source_epoch, request_id, frame_id)"""
    ),
    ("table", "NioIngestRoomAggregate"): _normalized_sql(
        """CREATE TABLE NioIngestRoomAggregate (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        room_id TEXT NOT NULL CHECK (
            typeof(room_id) = 'text' AND length(room_id) > 0
        ),
        updated_revision INTEGER NOT NULL CHECK (
            typeof(updated_revision) = 'integer' AND updated_revision >= 1
        ),
        intent_kind TEXT NULL CHECK (
            intent_kind IS NULL OR
            (typeof(intent_kind) = 'text'
             AND intent_kind IN ('recovery','hydration','local_membership'))
        ),
        payload BLOB NOT NULL CHECK (
            typeof(payload) = 'blob' AND length(payload) > 0
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
        ),
        PRIMARY KEY (account_id, room_id))"""
    ),
    ("index", "NioIngestRoomAggregate_intent"): _normalized_sql(
        """CREATE INDEX NioIngestRoomAggregate_intent
        ON NioIngestRoomAggregate(account_id, intent_kind, room_id)"""
    ),
    ("table", "NioIngestWork"): _normalized_sql("""CREATE TABLE NioIngestWork (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        work_id TEXT NOT NULL CHECK (
            typeof(work_id) = 'text' AND length(work_id) > 0
        ),
        kind TEXT NOT NULL CHECK (
            typeof(kind) = 'text' AND kind IN ('event','loss')
        ),
        status TEXT NOT NULL CHECK (typeof(status) = 'text' AND (
            (kind = 'event' AND status IN ('ready','held')) OR
            (kind = 'loss' AND status = 'ready')
        )),
        frame_id TEXT NOT NULL CHECK (
            typeof(frame_id) = 'text' AND length(frame_id) > 0
        ),
        room_id TEXT NULL CHECK (room_id IS NULL OR
            (typeof(room_id) = 'text' AND length(room_id) > 0)
        ),
        membership_epoch INTEGER NULL CHECK (membership_epoch IS NULL OR
            (typeof(membership_epoch) = 'integer' AND membership_epoch >= 0)
        ),
        room_sequence INTEGER NULL CHECK (room_sequence IS NULL OR
            (typeof(room_sequence) = 'integer' AND room_sequence >= 0)
        ),
        ready_revision INTEGER NULL CHECK (ready_revision IS NULL OR
            (typeof(ready_revision) = 'integer' AND ready_revision >= 1)
        ),
        ready_ordinal INTEGER NULL CHECK (ready_ordinal IS NULL OR
            (typeof(ready_ordinal) = 'integer' AND ready_ordinal >= 0)
        ),
        created_revision INTEGER NOT NULL CHECK (
            typeof(created_revision) = 'integer' AND created_revision >= 1
        ),
        payload BLOB NOT NULL CHECK (
            typeof(payload) = 'blob'
            AND length(payload) > 0
            AND length(payload) <= 1024 * 1024
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
        ),
        PRIMARY KEY (account_id, work_id),
        UNIQUE (account_id, ready_revision, ready_ordinal),
        CHECK (
            (status = 'ready' AND ready_revision IS NOT NULL
                              AND ready_ordinal IS NOT NULL)
            OR
            (status <> 'ready' AND ready_revision IS NULL
                               AND ready_ordinal IS NULL)
        ),
        CHECK (
            (kind = 'event' AND (
                (status = 'held' AND room_id IS NOT NULL
                                  AND membership_epoch IS NOT NULL
                                  AND room_sequence IS NOT NULL)
                OR
                (status = 'ready' AND (
                    (room_id IS NULL AND membership_epoch IS NULL
                                     AND room_sequence IS NULL)
                    OR
                    (room_id IS NOT NULL AND membership_epoch IS NOT NULL
                                         AND room_sequence IS NOT NULL)
                ))
            ))
            OR
            (kind = 'loss' AND room_id IS NOT NULL
                                AND membership_epoch IS NOT NULL
                                AND room_sequence IS NULL)
        ))"""),
    ("index", "NioIngestWork_ready"): _normalized_sql(
        """CREATE INDEX NioIngestWork_ready ON NioIngestWork(
        account_id, status, ready_revision, ready_ordinal, work_id)"""
    ),
    ("index", "NioIngestWork_held_release"): _normalized_sql(
        """CREATE INDEX NioIngestWork_held_release ON NioIngestWork(
        account_id, room_id, membership_epoch, status, room_sequence, work_id)"""
    ),
    ("index", "NioIngestWork_frame_kind"): _normalized_sql(
        """CREATE INDEX NioIngestWork_frame_kind ON NioIngestWork(
        account_id, frame_id, kind)"""
    ),
}

# Frozen unreleased encrypted-v1 topology.  Keep this independent from
# EXPECTED_DDL: that active oracle changes to plaintext rows in the GREEN,
# while this rejected-input fixture must remain byte-for-byte old-v1 SQL.
_ENCRYPTED_V1_DDL = (
    """CREATE TABLE NioIngestMeta (
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
        ))""",
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
        ))""",
    """CREATE TABLE NioIngestFrame (
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
        staged_revision INTEGER NOT NULL CHECK (
            typeof(staged_revision) = 'integer' AND staged_revision >= 1
        ),
        payload_ciphertext BLOB NOT NULL CHECK (
            typeof(payload_ciphertext) = 'blob'
            AND length(payload_ciphertext) >= 29
            AND length(payload_ciphertext) <= 24 * 1024 * 1024
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
        ),
        room_materialized_revision INTEGER NULL CHECK (
            room_materialized_revision IS NULL OR
            (typeof(room_materialized_revision) = 'integer'
             AND room_materialized_revision >= 1)
        ),
        drain_header_ciphertext BLOB NOT NULL CHECK (
            typeof(drain_header_ciphertext) = 'blob'
            AND length(drain_header_ciphertext) = 29
        ),
        PRIMARY KEY (account_id, frame_id))""",
    """CREATE INDEX NioIngestFrame_drain ON NioIngestFrame(
        account_id, staged_revision, source_epoch, request_id, frame_id)""",
    """CREATE TABLE NioIngestRoomAggregate (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        room_id TEXT NOT NULL CHECK (
            typeof(room_id) = 'text' AND length(room_id) > 0
        ),
        updated_revision INTEGER NOT NULL CHECK (
            typeof(updated_revision) = 'integer' AND updated_revision >= 1
        ),
        intent_kind TEXT NULL CHECK (
            intent_kind IS NULL OR
            (typeof(intent_kind) = 'text'
             AND intent_kind IN ('recovery','hydration'))
        ),
        payload_ciphertext BLOB NOT NULL CHECK (
            typeof(payload_ciphertext) = 'blob'
            AND length(payload_ciphertext) >= 29
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
        ),
        PRIMARY KEY (account_id, room_id))""",
    """CREATE INDEX NioIngestRoomAggregate_intent
        ON NioIngestRoomAggregate(account_id, intent_kind, room_id)""",
    """CREATE TABLE NioIngestWork (
        account_id TEXT NOT NULL REFERENCES NioIngestMeta(account_id) CHECK (
            typeof(account_id) = 'text' AND length(account_id) > 0
        ),
        work_id TEXT NOT NULL CHECK (
            typeof(work_id) = 'text' AND length(work_id) > 0
        ),
        kind TEXT NOT NULL CHECK (
            typeof(kind) = 'text' AND kind IN ('event','loss')
        ),
        status TEXT NOT NULL CHECK (typeof(status) = 'text' AND (
            (kind = 'event' AND status IN ('ready','held')) OR
            (kind = 'loss' AND status = 'ready')
        )),
        frame_id TEXT NOT NULL CHECK (
            typeof(frame_id) = 'text' AND length(frame_id) > 0
        ),
        room_id TEXT NULL CHECK (room_id IS NULL OR
            (typeof(room_id) = 'text' AND length(room_id) > 0)
        ),
        membership_epoch INTEGER NULL CHECK (membership_epoch IS NULL OR
            (typeof(membership_epoch) = 'integer' AND membership_epoch >= 0)
        ),
        room_sequence INTEGER NULL CHECK (room_sequence IS NULL OR
            (typeof(room_sequence) = 'integer' AND room_sequence >= 0)
        ),
        ready_revision INTEGER NULL CHECK (ready_revision IS NULL OR
            (typeof(ready_revision) = 'integer' AND ready_revision >= 1)
        ),
        ready_ordinal INTEGER NULL CHECK (ready_ordinal IS NULL OR
            (typeof(ready_ordinal) = 'integer' AND ready_ordinal >= 0)
        ),
        created_revision INTEGER NOT NULL CHECK (
            typeof(created_revision) = 'integer' AND created_revision >= 1
        ),
        payload_ciphertext BLOB NOT NULL CHECK (
            typeof(payload_ciphertext) = 'blob'
            AND length(payload_ciphertext) >= 29
            AND length(payload_ciphertext) <= 1024 * 1024 + 29
        ),
        payload_sha256 BLOB NOT NULL CHECK (
            typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
        ),
        PRIMARY KEY (account_id, work_id),
        UNIQUE (account_id, ready_revision, ready_ordinal),
        CHECK (
            (status = 'ready' AND ready_revision IS NOT NULL
                              AND ready_ordinal IS NOT NULL)
            OR
            (status <> 'ready' AND ready_revision IS NULL
                               AND ready_ordinal IS NULL)
        ),
        CHECK (
            (kind = 'event' AND (
                (status = 'held' AND room_id IS NOT NULL
                                  AND membership_epoch IS NOT NULL
                                  AND room_sequence IS NOT NULL)
                OR
                (status = 'ready' AND (
                    (room_id IS NULL AND membership_epoch IS NULL
                                     AND room_sequence IS NULL)
                    OR
                    (room_id IS NOT NULL AND membership_epoch IS NOT NULL
                                         AND room_sequence IS NOT NULL)
                ))
            ))
            OR
            (kind = 'loss' AND room_id IS NOT NULL
                                AND membership_epoch IS NOT NULL
                                AND room_sequence IS NULL)
        ))""",
    """CREATE INDEX NioIngestWork_ready ON NioIngestWork(
        account_id, status, ready_revision, ready_ordinal, work_id)""",
    """CREATE INDEX NioIngestWork_held_release ON NioIngestWork(
        account_id, room_id, membership_epoch, status, room_sequence, work_id)""",
    """CREATE INDEX NioIngestWork_frame_kind ON NioIngestWork(
        account_id, frame_id, kind)""",
)


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
    assert len(topology) == 11

    assert {(kind, name): sql for kind, name, sql in topology} == EXPECTED_DDL
    with sqlite3.connect(database_path) as actual, sqlite3.connect(":memory:") as exact:
        for ddl in EXPECTED_DDL.values():
            exact.execute(ddl)
        for table in (
            "NioIngestMeta",
            "NioIngestSourceState",
            "NioIngestRecovery",
            "NioIngestFrame",
            "NioIngestRoomAggregate",
            "NioIngestWork",
        ):
            for pragma in ("table_info", "foreign_key_list"):
                assert tuple(actual.execute(f"PRAGMA {pragma}({table})")) == tuple(
                    exact.execute(f"PRAGMA {pragma}({table})")
                )
        for index in (
            "NioIngestFrame_drain",
            "NioIngestRoomAggregate_intent",
            "NioIngestWork_ready",
            "NioIngestWork_held_release",
            "NioIngestWork_frame_kind",
        ):
            assert tuple(actual.execute(f"PRAGMA index_xinfo({index})")) == tuple(
                exact.execute(f"PRAGMA index_xinfo({index})")
            )


def test_v1_frame_callbacks_claimed_revision_is_nullable_checked_and_authenticated(
    tmp_path: Path,
) -> None:
    """A claimed callback revision cannot be omitted from the Frame proof."""

    stage = _stage_one(tmp_path)
    frame_id = stage.proposal.frame.frame_id
    with sqlite3.connect(stage.database_path) as connection:
        connection.row_factory = sqlite3.Row
        columns = tuple(
            row["name"]
            for row in connection.execute("PRAGMA table_info(NioIngestFrame)")
        )
        assert "callbacks_claimed_revision" in columns
        row = connection.execute(
            "SELECT callbacks_claimed_revision FROM NioIngestFrame "
            "WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT_ID, str(frame_id)),
        ).fetchone()
        assert row is not None and row[0] is None

        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE NioIngestFrame SET callbacks_claimed_revision = 0 "
                "WHERE account_id = ? AND frame_id = ?",
                (ACCOUNT_ID, str(frame_id)),
            )

        updated = connection.execute(
            "UPDATE NioIngestFrame SET callbacks_claimed_revision = 1 "
            "WHERE account_id = ? AND frame_id = ?",
            (ACCOUNT_ID, str(frame_id)),
        )
        assert updated.rowcount == 1

    reopened = _open(tmp_path)
    try:
        with pytest.raises(JournalIntegrityError, match="frame drain"):
            reopened._journal.load_frame(frame_id)
    finally:
        reopened.close()


def test_v1_frame_valid_proof_rejects_semantically_impossible_callback_claim(
    tmp_path: Path,
) -> None:
    stage = _stage_one(tmp_path)
    with sqlite3.connect(stage.database_path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM NioIngestFrame").fetchone()
        assert row is not None
        claim = row["staged_revision"]
        changed = {**dict(row), "callbacks_claimed_revision": claim}
        proof = _plaintext_frame_header_sha256(
            stream_id=stage.stream_id,
            row=changed,  # type: ignore[arg-type]
            payload=row["payload"],
            payload_sha256=row["payload_sha256"],
        )
        connection.execute(
            "UPDATE NioIngestFrame SET callbacks_claimed_revision = ?, "
            "drain_header_sha256 = ?",
            (claim, proof),
        )

    reopened = _open(tmp_path)
    try:
        with pytest.raises(JournalIntegrityError, match="frame drain"):
            reopened._journal.load_frame(stage.proposal.frame.frame_id)
    finally:
        reopened.close()


def test_v1_authenticated_callback_claim_prevents_rematerialization_writes(
    tmp_path: Path,
) -> None:
    stage = _stage_one(tmp_path)
    with sqlite3.connect(stage.database_path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute("SELECT * FROM NioIngestFrame").fetchone()
        assert row is not None
        claim = row["staged_revision"] + 1
        changed = {**dict(row), "callbacks_claimed_revision": claim}
        proof = _plaintext_frame_header_sha256(
            stream_id=stage.stream_id,
            row=changed,  # type: ignore[arg-type]
            payload=row["payload"],
            payload_sha256=row["payload_sha256"],
        )
        connection.execute("UPDATE NioIngestMeta SET revision = ?", (claim,))
        connection.execute(
            "UPDATE NioIngestFrame SET callbacks_claimed_revision = ?, "
            "drain_header_sha256 = ?",
            (claim, proof),
        )

    reopened = _open(tmp_path)
    try:
        before = tuple(
            reopened._journal._execute(
                "SELECT revision, "
                "(SELECT COUNT(*) FROM NioIngestRoomAggregate), "
                "(SELECT COUNT(*) FROM NioIngestWork) FROM NioIngestMeta"
            ).fetchone()
        )
        result = materialize_journal(reopened._journal, limits=MaterializerLimits())
        after = tuple(
            reopened._journal._execute(
                "SELECT revision, "
                "(SELECT COUNT(*) FROM NioIngestRoomAggregate), "
                "(SELECT COUNT(*) FROM NioIngestWork) FROM NioIngestMeta"
            ).fetchone()
        )
        assert result.status is MaterializeStatus.BLOCKED
        assert after == before
    finally:
        reopened.close()


class _PlanningSource:
    def __init__(self, request: NetworkRequest | None) -> None:
        self.request = request
        self.plan_calls = 0

    def plan_request(
        self,
        _state: SourceState,
        _request_id: int,
    ) -> NetworkRequest | None:
        self.plan_calls += 1
        return self.request

    def normalize(
        self,
        _request: NetworkRequest,
        _result: NetworkResult,
    ) -> object:
        raise AssertionError("capacity planning must not normalize a response")


class _FakeSender:
    def __init__(self) -> None:
        self.calls = 0

    def __call__(self, _request: NetworkRequest) -> None:
        self.calls += 1


def test_plan_source_poll_returns_ready_or_inactive_from_adapter() -> None:
    from nio.ingest.source import (
        SourceScheduleDecision,
        SourceScheduleStatus,
        plan_source_poll,
    )

    state = SourceState(0, TransportKind.CLASSIC, b'{"next_batch":null}', 0, True)
    request = NetworkRequest(
        uuid4(),
        TransportKind.CLASSIC,
        0,
        0,
        "GET",
        "/_matrix/client/v3/sync",
        (),
        None,
        30_000,
        state.cursor_json,
    )
    ready_source = _PlanningSource(request)
    inactive_source = _PlanningSource(None)
    staged = _frame_for_request(request, canonical_json({"next_batch": "s1"}))

    assert plan_source_poll(ready_source, state, 0, (staged,), 2) == (
        SourceScheduleDecision(SourceScheduleStatus.READY, request)
    )
    assert plan_source_poll(inactive_source, state, 0, (), 2) == (
        SourceScheduleDecision(SourceScheduleStatus.INACTIVE, None)
    )
    assert (ready_source.plan_calls, inactive_source.plan_calls) == (1, 1)


def test_plan_source_poll_reserves_one_quiesce_frame_at_maximum_capacity() -> None:
    from nio.ingest.source import SourceScheduleStatus, plan_source_poll

    state = SourceState(0, TransportKind.CLASSIC, b'{"next_batch":null}', 0, True)
    request = NetworkRequest(
        uuid4(),
        TransportKind.CLASSIC,
        0,
        0,
        "GET",
        "/_matrix/client/v3/sync",
        (),
        None,
        30_000,
        state.cursor_json,
    )
    source = _PlanningSource(request)
    staged = _frame_for_request(request, canonical_json({"next_batch": "s1"}))
    full = (staged,) * 256

    ordinary = plan_source_poll(source, state, 0, full, 256)
    quiescing = plan_source_poll(
        source,
        state,
        0,
        full,
        256,
        reserved_staged_frames=1,
    )
    reserved_full = plan_source_poll(
        source,
        state,
        0,
        (*full, staged),
        256,
        reserved_staged_frames=1,
    )

    assert ordinary.status is SourceScheduleStatus.AT_CAPACITY
    assert quiescing.status is SourceScheduleStatus.READY
    assert reserved_full.status is SourceScheduleStatus.AT_CAPACITY
    assert source.plan_calls == 1


@pytest.mark.parametrize(
    ("field_name", "invalid"),
    (
        ("source", object()),
        ("state", object()),
        ("request_id", True),
        ("request_id", -1),
        ("staged_frames", []),
        ("staged_frames", (object(),)),
        ("max_staged_frames", True),
        ("max_staged_frames", 0),
        ("max_staged_frames", 257),
        ("reserved_staged_frames", True),
        ("reserved_staged_frames", -1),
        ("reserved_staged_frames", 2),
    ),
)
def test_plan_source_poll_exact_validates_every_input(
    field_name: str,
    invalid: object,
) -> None:
    from nio.ingest.source import plan_source_poll

    state = SourceState(0, TransportKind.CLASSIC, b'{"next_batch":null}', 0, True)
    values: dict[str, object] = {
        "source": _PlanningSource(None),
        "state": state,
        "request_id": 0,
        "staged_frames": (),
        "max_staged_frames": 2,
        "reserved_staged_frames": 0,
    }
    values[field_name] = invalid
    with pytest.raises((TypeError, ValueError), match=field_name):
        plan_source_poll(**values)  # type: ignore[arg-type]


def test_full_bounded_journal_tuple_stops_planning_sends_and_rss_growth(
    tmp_path: Path,
) -> None:
    from nio.ingest.source import SourceScheduleStatus, plan_source_poll

    config = IngestionConfig(CLASSIC_SOURCE, max_staged_frames=1)
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements=statements)
    journal = bootstrap._journal
    try:
        proposal = _stage_proposal(journal, CLASSIC_SOURCE, 1)
        _stage(journal, proposal=proposal)
        state = journal.load_source()
        staged_frames = journal.list_frames(config.max_staged_frames)
        source = _PlanningSource(proposal.frame.response.request)
        sender = _FakeSender()
        statements.clear()
        rss_before_kib = resource.getrusage(resource.RUSAGE_SELF).ru_maxrss
        at_capacity = 0

        for _ in range(10_000):
            decision = plan_source_poll(
                source,
                state,
                state.next_request_id,
                staged_frames,
                config.max_staged_frames,
            )
            at_capacity += decision.status is SourceScheduleStatus.AT_CAPACITY
            if decision.status is SourceScheduleStatus.READY:
                assert decision.request is not None
                sender(decision.request)

        rss_growth_bytes = (
            resource.getrusage(resource.RUSAGE_SELF).ru_maxrss - rss_before_kib
        ) * 1024
        assert at_capacity == 10_000
        assert source.plan_calls == 0
        assert sender.calls == 0
        assert statements == []
        assert rss_growth_bytes < 8 * 1024 * 1024
    finally:
        bootstrap.close()


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
    if case == "encrypted_v1":
        # Frozen unreleased-v1 topology from before canonical plaintext rows.
        # This cannot be built through the current schema constants: after the
        # GREEN those constants intentionally describe the replacement v1.
        stream_id = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")
        with sqlite3.connect(database_path) as connection:
            connection.execute("PRAGMA foreign_keys = ON")
            for ddl in _ENCRYPTED_V1_DDL:
                connection.execute(ddl)
            connection.execute(
                "INSERT INTO NioIngestMeta(account_id, device_id, schema_version, "
                "stream_id, transport_kind, revision, writer_epoch, "
                "next_source_epoch, created_at_ns) "
                "VALUES (?, ?, 1, ?, 'classic', 0, ?, 1, 0)",
                (ACCOUNT_ID, DEVICE_ID, str(stream_id), str(uuid4())),
            )
            connection.execute(
                "INSERT INTO NioIngestSourceState(account_id, source_epoch, "
                "cursor_ciphertext, cursor_sha256, next_request_id, active) "
                "VALUES (?, 0, ?, ?, 0, 1)",
                (
                    ACCOUNT_ID,
                    bytes.fromhex(
                        "015bcf2415d93e6f57a82a45fecd553e2eccdf687fbe320d"
                        "45406df445b9d60cbffdbaed969b2437b98eb17d65bf2cf9"
                    ),
                    bytes.fromhex(
                        "3a38f3335e554c03c79bff22ce15a843ab11010bc503ace5e"
                        "fa5ef01c84a5c31"
                    ),
                ),
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
        and not sql.lstrip().upper().startswith("PRAGMA SECURE_DELETE")
        and ("=" in sql or any(name in sql.upper() for name in write_pragmas))
    ]


@pytest.mark.parametrize(
    "case",
    (
        "legacy",
        "e2ee_only",
        "abandoned_current_v1",
        "encrypted_v1",
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
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
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
            consumer_generation=CONSUMER_GENERATION,
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


def test_sliding_reopen_rotates_source_and_preserves_logical_inventory(
    tmp_path: Path,
) -> None:
    first = _open(tmp_path, SLIDING_SOURCE)
    _seed_sliding_reopen_inventory(first._journal)
    owner_before = first._journal.load_owner()
    source_before = first._journal.load_source()
    cursor_before = _sliding_cursor_from_json(source_before.cursor_json)
    inventory_before = _logical_inventory(first._journal)
    first.close()

    transitions: list[str] = []
    reopened = _open(
        tmp_path,
        SLIDING_SOURCE,
        transition_statement_hook=transitions.append,
    )
    try:
        owner_after = reopened._journal.load_owner()
        source_after = reopened._journal.load_source()
        cursor_after = _sliding_cursor_from_json(source_after.cursor_json)

        assert owner_after == replace(
            owner_before,
            revision=owner_before.revision + 1,
            writer_epoch=owner_after.writer_epoch,
            next_source_epoch=owner_before.next_source_epoch + 1,
        )
        assert owner_after.writer_epoch != owner_before.writer_epoch
        assert source_after.source_epoch == owner_before.next_source_epoch
        assert source_after.next_request_id == 0
        assert source_after.active is source_before.active is False
        assert cursor_after == SlidingCursor(
            pos=None,
            to_device_since=cursor_before.to_device_since,
            connection_instance=cursor_after.connection_instance,
            connection_name=cursor_before.connection_name,
            all_rooms_range_end=cursor_before.all_rooms_range_end,
            all_rooms_page_size=cursor_before.all_rooms_page_size,
            all_rooms_range_ack_mode=SlidingRangeAckMode.UNKNOWN,
            all_rooms_coverage_complete=False,
        )
        assert cursor_after.connection_instance != cursor_before.connection_instance
        assert _logical_inventory(reopened._journal) == inventory_before
        assert transitions == [
            "sliding_reset_meta_cas",
            "sliding_reset_source_upsert",
            "before_commit",
            "commit",
        ]
        _assert_exact_ingestion_topology(tmp_path / "journal.db")
    finally:
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
def test_sliding_reopen_statement_boundaries_leave_only_old_or_new_state(
    tmp_path: Path,
    boundary: str,
    committed: bool,
) -> None:
    first = _open(tmp_path, SLIDING_SOURCE)
    proposal = _stage_proposal(
        first._journal,
        SLIDING_SOURCE,
        1,
        to_device_since="td1",
    )
    _stage(first._journal, proposal=proposal)
    owner_before = first._journal.load_owner()
    first.close()
    with sqlite3.connect(tmp_path / "journal.db") as connection:
        raw_meta_before = connection.execute("SELECT * FROM NioIngestMeta").fetchone()
        raw_source_before = connection.execute(
            "SELECT * FROM NioIngestSourceState"
        ).fetchone()
    assert raw_meta_before is not None and raw_source_before is not None

    class InjectedResetFailure(RuntimeError):
        pass

    observed: list[str] = []

    def fail_at_boundary(label: str) -> None:
        observed.append(label)
        if label == boundary:
            raise InjectedResetFailure(label)

    with pytest.raises(InjectedResetFailure, match=boundary):
        _open(
            tmp_path,
            SLIDING_SOURCE,
            transition_statement_hook=fail_at_boundary,
        )

    with sqlite3.connect(tmp_path / "journal.db") as connection:
        connection.row_factory = sqlite3.Row
        meta = connection.execute("SELECT * FROM NioIngestMeta").fetchone()
        source = connection.execute("SELECT * FROM NioIngestSourceState").fetchone()
    assert meta is not None and source is not None
    if not committed:
        assert tuple(meta) == raw_meta_before
        assert tuple(source) == raw_source_before
    else:
        assert meta["revision"] == owner_before.revision + 1
        assert meta["writer_epoch"] != str(owner_before.writer_epoch)
        assert meta["next_source_epoch"] == owner_before.next_source_epoch + 1
        assert source["source_epoch"] == owner_before.next_source_epoch
        assert source["next_request_id"] == 0
        stored_cursor = json.loads(source["payload"])["value"]
        assert stored_cursor["pos"] is None
        assert stored_cursor["to_device_since"] == "td1"
        assert stored_cursor["all_rooms_range_ack_mode"] == "unknown"
        assert stored_cursor["all_rooms_coverage_complete"] is False
    assert observed[-1] == boundary


def test_exact_positioned_request_resets_live_source_and_rotates_cached_writer(
    tmp_path: Path,
) -> None:
    transitions: list[str] = []
    bootstrap = _open(
        tmp_path,
        SLIDING_SOURCE,
        transition_statement_hook=transitions.append,
    )
    journal = bootstrap._journal
    try:
        proposal = _stage_proposal(
            journal,
            SLIDING_SOURCE,
            1,
            to_device_since="td1",
        )
        _stage(journal, proposal=proposal)
        owner_before = journal.load_owner()
        source_before = journal.load_source()
        inventory_before = _logical_inventory(journal)
        request = _source_adapter(
            owner_before.stream_id,
            SLIDING_SOURCE,
        ).plan_request(source_before, source_before.next_request_id)
        assert request is not None
        transitions.clear()

        committed = journal._reset_sliding_source(request=request)

        owner_after = journal.load_owner()
        source_after = journal.load_source()
        cursor_before = _sliding_cursor_from_json(source_before.cursor_json)
        cursor_after = _sliding_cursor_from_json(source_after.cursor_json)
        assert committed == CommitResult(owner_before.revision + 1)
        assert owner_after == replace(
            owner_before,
            revision=owner_before.revision + 1,
            writer_epoch=owner_after.writer_epoch,
            next_source_epoch=owner_before.next_source_epoch + 1,
        )
        assert owner_after.writer_epoch != owner_before.writer_epoch
        assert journal.writer_epoch == owner_after.writer_epoch
        assert source_after.source_epoch == owner_before.next_source_epoch
        assert source_after.next_request_id == 0
        assert source_after.active is source_before.active
        assert cursor_after.connection_instance != cursor_before.connection_instance
        assert cursor_after.pos is None
        assert cursor_after.to_device_since == cursor_before.to_device_since
        assert cursor_after.connection_name == cursor_before.connection_name
        assert cursor_after.all_rooms_range_end == cursor_before.all_rooms_range_end
        assert cursor_after.all_rooms_page_size == cursor_before.all_rooms_page_size
        assert cursor_after.all_rooms_range_ack_mode is SlidingRangeAckMode.UNKNOWN
        assert cursor_after.all_rooms_coverage_complete is False
        assert _logical_inventory(journal) == inventory_before
        assert transitions == [
            "sliding_reset_meta_cas",
            "sliding_reset_source_upsert",
            "before_commit",
            "commit",
        ]
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("boundary", "committed"),
    (
        ("sliding_reset_meta_cas", False),
        ("sliding_reset_source_upsert", False),
        ("before_commit", False),
        ("commit", True),
    ),
)
def test_live_reset_hands_cached_writer_over_only_after_commit(
    tmp_path: Path,
    boundary: str,
    committed: bool,
) -> None:
    bootstrap = _open(tmp_path, SLIDING_SOURCE)
    journal = bootstrap._journal
    proposal = _stage_proposal(journal, SLIDING_SOURCE, 1)
    _stage(journal, proposal=proposal)
    owner_before = journal.load_owner()
    source_before = journal.load_source()
    request = _source_adapter(
        owner_before.stream_id,
        SLIDING_SOURCE,
    ).plan_request(source_before, source_before.next_request_id)
    assert request is not None

    class InjectedResetFailure(RuntimeError):
        pass

    def fail_at_boundary(label: str) -> None:
        if label == boundary:
            raise InjectedResetFailure(label)

    journal.set_transition_statement_hook(fail_at_boundary)
    with pytest.raises(InjectedResetFailure, match=boundary):
        journal._reset_sliding_source(request=request)
    journal.set_transition_statement_hook(None)

    owner_after = journal.load_owner()
    source_after = journal.load_source()
    if committed:
        assert owner_after.revision == owner_before.revision + 1
        assert owner_after.writer_epoch != owner_before.writer_epoch
        assert source_after.source_epoch == owner_before.next_source_epoch
        assert source_after.next_request_id == 0
    else:
        assert owner_after == owner_before
        assert source_after == source_before
    bootstrap.close()


def test_live_reset_source_cas_rejects_changed_authenticated_snapshot(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path, SLIDING_SOURCE)
    journal = bootstrap._journal
    proposal = _stage_proposal(journal, SLIDING_SOURCE, 1)
    _stage(journal, proposal=proposal)
    owner_before = journal.load_owner()
    source_before = journal.load_source()
    request = _source_adapter(
        owner_before.stream_id,
        SLIDING_SOURCE,
    ).plan_request(source_before, source_before.next_request_id)
    assert request is not None

    def change_source_after_meta_cas(label: str) -> None:
        if label != "sliding_reset_meta_cas":
            return
        owner, source = journal._load_stage_snapshot()
        journal._write_source(
            replace(source, next_request_id=source.next_request_id + 1),
            owner,
        )

    journal.set_transition_statement_hook(change_source_after_meta_cas)
    with pytest.raises(JournalConflictError, match="Source"):
        journal._reset_sliding_source(request=request)
    journal.set_transition_statement_hook(None)

    assert journal.load_owner() == owner_before
    assert journal.load_source() == source_before
    bootstrap.close()


@pytest.mark.parametrize(
    "mutation",
    ("source_epoch", "request_id", "cursor", "positionless", "repeated_cold"),
)
def test_live_reset_rejects_stale_or_cold_request_without_dml(
    tmp_path: Path,
    mutation: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, SLIDING_SOURCE, statements=statements)
    journal = bootstrap._journal
    if mutation != "positionless":
        proposal = _stage_proposal(journal, SLIDING_SOURCE, 1)
        _stage(journal, proposal=proposal)
    owner_before = journal.load_owner()
    source_before = journal.load_source()
    request = _source_adapter(
        owner_before.stream_id,
        SLIDING_SOURCE,
    ).plan_request(source_before, source_before.next_request_id)
    assert request is not None
    if mutation == "source_epoch":
        request = replace(request, source_epoch=request.source_epoch + 1)
    elif mutation == "request_id":
        request = replace(request, request_id=request.request_id + 1)
    elif mutation == "cursor":
        request = replace(
            request,
            request_cursor_json=canonical_json(
                {
                    **json.loads(request.request_cursor_json),
                    "pos": "foreign",
                }
            ),
        )
    elif mutation == "repeated_cold":
        assert journal._reset_sliding_source(request=request) is not None
        owner_before = journal.load_owner()
        source_before = journal.load_source()
        request = _source_adapter(
            owner_before.stream_id,
            SLIDING_SOURCE,
        ).plan_request(source_before, source_before.next_request_id)
        assert request is not None

    statements.clear()
    assert journal._reset_sliding_source(request=request) is None
    assert _business_dml(statements) == []
    assert journal.load_owner() == owner_before
    assert journal.load_source() == source_before
    bootstrap.close()


@pytest.mark.parametrize("field", ("revision", "next_source_epoch"))
@pytest.mark.parametrize("operation", ("live", "reopen"))
def test_sliding_reset_integer_exhaustion_fails_before_dml(
    tmp_path: Path,
    field: str,
    operation: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, SLIDING_SOURCE, statements=statements)
    journal = bootstrap._journal
    proposal = _stage_proposal(journal, SLIDING_SOURCE, 1)
    _stage(journal, proposal=proposal)
    with journal._transaction():
        journal._execute(
            f"UPDATE NioIngestMeta SET {field} = ? WHERE account_id = ?",
            (SQLITE_INT_MAX, ACCOUNT_ID),
        )
    owner_before = journal.load_owner()
    source_before = journal.load_source()
    request = _source_adapter(
        owner_before.stream_id,
        SLIDING_SOURCE,
    ).plan_request(source_before, source_before.next_request_id)
    assert request is not None
    if operation == "reopen":
        bootstrap.close()

    statements.clear()
    with pytest.raises(LocalProtocolError, match="exhausted"):
        if operation == "live":
            journal._reset_sliding_source(request=request)
        else:
            _open(tmp_path, SLIDING_SOURCE, statements=statements)
    assert _business_dml(statements) == []
    if operation == "live":
        assert journal.load_owner() == owner_before
        assert journal.load_source() == source_before
        bootstrap.close()


def test_consumer_generation_mismatch_is_byte_identical_before_epoch_update(
    tmp_path: Path,
) -> None:
    first = _open(tmp_path)
    first.close()
    database_path = tmp_path / "journal.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    before = database_path.read_bytes()
    epoch_before = _writer_epoch(database_path)

    with pytest.raises(LocalProtocolError):
        open_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=OTHER_CONSUMER_GENERATION,
            source=CLASSIC_SOURCE,
            database_name=database_path.name,
        )

    assert database_path.read_bytes() == before
    assert _writer_epoch(database_path) == epoch_before


@pytest.mark.parametrize(
    "stored_generation",
    ("not-a-uuid", f"{{{CONSUMER_GENERATION}}}"),
    ids=("malformed", "noncanonical-alias"),
)
def test_malformed_or_noncanonical_stored_consumer_generation_is_byte_identical(
    tmp_path: Path,
    stored_generation: str,
) -> None:
    first = _open(tmp_path)
    first.close()
    database_path = tmp_path / "journal.db"
    with sqlite3.connect(database_path) as connection:
        connection.execute(
            "UPDATE NioIngestMeta SET consumer_generation = ?",
            (stored_generation,),
        )
        connection.commit()
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    before = database_path.read_bytes()
    epoch_before = _writer_epoch(database_path)

    with pytest.raises(JournalIntegrityError):
        _open(tmp_path)

    assert database_path.read_bytes() == before
    assert _writer_epoch(database_path) == epoch_before


def test_ordinary_owner_read_rejects_noncanonical_consumer_generation(
    tmp_path: Path,
) -> None:
    store = _open(tmp_path)
    with store._journal._transaction():
        store._journal._execute(
            "UPDATE NioIngestMeta SET consumer_generation = ?",
            (f"{{{CONSUMER_GENERATION}}}",),
        )

    with pytest.raises(JournalIntegrityError):
        store._journal.load_owner()


@pytest.mark.parametrize(
    "consumer_generation", (None, str(CONSUMER_GENERATION), object())
)
def test_non_uuid_consumer_generation_fails_before_filesystem_mutation(
    tmp_path: Path,
    consumer_generation: object,
) -> None:
    target = tmp_path / "never-created"

    with pytest.raises(TypeError, match="consumer_generation must be UUID"):
        open_ingestion_store(
            target,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=consumer_generation,  # type: ignore[arg-type]
            source=CLASSIC_SOURCE,
        )

    assert not target.exists()


def test_consumer_generation_is_required_by_the_public_api() -> None:
    parameter = inspect.signature(open_ingestion_store).parameters[
        "consumer_generation"
    ]

    assert parameter.default is inspect.Parameter.empty


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
        repeated = _stage(journal, proposal=proposal)

        assert repeated == result
        assert _business_dml(statements) == []
        frame_selects = [sql for sql in statements if "FROM NioIngestFrame" in sql]
        assert [_normalized_sql(sql) for sql in frame_selects] == [
            _normalized_sql(
                CLASSIFY_FRAME_IDS_SQL.replace("?", f"'{ACCOUNT_ID}'", 1).replace(
                    "?", str(FRAME_CLASSIFICATION_LIMIT), 1
                )
            ),
            _normalized_sql(
                LOAD_FRAME_SQL.replace("?", f"'{ACCOUNT_ID}'", 1).replace(
                    "?", f"'{proposal.frame.frame_id}'", 1
                )
            ),
        ]
        assert journal.load_owner().revision == result.revision
        assert journal.load_source() == proposal.successor_source
        assert journal.load_frame(proposal.frame.frame_id) == replace(
            proposal.frame,
            staged_revision=result.revision,
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("boundary", "committed"),
    (
        ("quiesce_reservation_meta_cas", False),
        ("quiesce_reservation_frame_update", False),
        ("before_commit", False),
        ("commit", True),
    ),
)
def test_quiesce_reservation_consumption_is_atomic_and_replayable(
    tmp_path: Path,
    boundary: str,
    committed: bool,
) -> None:
    class ReservationAbort(BaseException):
        pass

    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    proposal = _stage_proposal(journal, CLASSIC_SOURCE, 1)
    journal.stage_source_response(
        source=proposal.successor_source,
        frame=proposal.frame,
        quiesce_reserved=True,
    )
    owner_before = journal.load_owner()
    observed: list[str] = []
    sentinel = ReservationAbort(boundary)

    def abort_at_boundary(label: str) -> None:
        observed.append(label)
        if label == boundary:
            raise sentinel

    journal.set_transition_statement_hook(abort_at_boundary)
    try:
        with pytest.raises(ReservationAbort) as failure:
            journal.consume_reserved_quiesce_response()
        assert failure.value is sentinel
        assert observed[-1] == boundary
    finally:
        journal.set_transition_statement_hook(None)
        bootstrap.close()

    reopened = _open(tmp_path)
    try:
        assert reopened._journal.load_owner().revision == (
            owner_before.revision + int(committed)
        )
        assert reopened._journal.has_reserved_quiesce_response() is not committed
        if not committed:
            assert reopened._journal.consume_reserved_quiesce_response() == (
                CommitResult(owner_before.revision + 1)
            )
            assert not reopened._journal.has_reserved_quiesce_response()
    finally:
        reopened.close()


def test_stage_uses_source_predecessor_after_unrelated_revision_advance(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path, CLASSIC_SOURCE)
    journal = bootstrap._journal
    try:
        first = _stage_proposal(journal, CLASSIC_SOURCE, 1)
        _stage(journal, proposal=first)
        proposal = _stage_proposal(journal, CLASSIC_SOURCE, 2)
        source_predecessor = journal.load_source()
        owner_before_materialize = journal.load_owner()

        materialized = materialize_journal(
            journal,
            limits=MaterializerLimits(),
        )

        assert materialized.frame_id == first.frame.frame_id
        assert materialized.revision == owner_before_materialize.revision + 1
        assert journal.load_owner().revision == materialized.revision
        assert journal.load_source() == source_predecessor == proposal.prior_source

        committed = journal.stage_source_response(
            source=proposal.successor_source,
            frame=proposal.frame,
        )

        assert committed == CommitResult(materialized.revision + 1)
        assert journal.load_source() == proposal.successor_source
        assert journal.load_frame(proposal.frame.frame_id) == replace(
            proposal.frame,
            staged_revision=committed.revision,
        )
    finally:
        bootstrap.close()


def test_competing_results_for_one_source_predecessor_cannot_both_commit(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, CLASSIC_SOURCE, statements=statements)
    journal = bootstrap._journal
    try:
        accepted = _stage_proposal(journal, CLASSIC_SOURCE, 1)
        competing = _stage_proposal(journal, CLASSIC_SOURCE, 2)
        assert accepted.prior_source == competing.prior_source
        assert accepted.frame.response.request == competing.frame.response.request
        assert accepted.successor_source != competing.successor_source

        committed = _stage(journal, proposal=accepted)
        statements.clear()

        with pytest.raises(
            JournalIntegrityError,
            match="source staging request does not match current source",
        ):
            journal.stage_source_response(
                source=competing.successor_source,
                frame=competing.frame,
            )

        assert _business_dml(statements) == []
        assert journal.load_owner().revision == committed.revision
        assert journal.load_source() == accepted.successor_source
        assert journal.list_frames(256) == (
            replace(accepted.frame, staged_revision=committed.revision),
        )
    finally:
        bootstrap.close()


UUID_ALIAS_SPELLINGS = (
    "hex",
    "upper_canonical",
    "upper_hex",
    "braced_canonical",
    "braced_hex",
    "braced_upper_canonical",
    "braced_upper_hex",
    "urn_uuid_canonical",
    "urn_uuid_hex",
    "urn_uuid_upper_canonical",
    "urn_uuid_upper_hex",
    "urn_uuid_braced_canonical",
    "urn_uuid_braced_hex",
    "urn_uuid_braced_upper_canonical",
    "urn_uuid_braced_upper_hex",
    "uuid_canonical",
    "uuid_hex",
    "uuid_upper_canonical",
    "uuid_upper_hex",
    "urn_canonical",
    "urn_hex",
    "urn_upper_canonical",
    "urn_upper_hex",
    "partial_hyphens",
    "arbitrary_hyphens",
    "repeated_braces",
    "embedded_prefixes",
)


def _uuid_alias(value: UUID, spelling: str) -> str:
    canonical = str(value)
    hex_value = value.hex
    aliases = {
        "hex": hex_value,
        "upper_canonical": canonical.upper(),
        "upper_hex": hex_value.upper(),
        "braced_canonical": f"{{{canonical}}}",
        "braced_hex": f"{{{hex_value}}}",
        "braced_upper_canonical": f"{{{canonical.upper()}}}",
        "braced_upper_hex": f"{{{hex_value.upper()}}}",
        "urn_uuid_canonical": f"urn:uuid:{canonical}",
        "urn_uuid_hex": f"urn:uuid:{hex_value}",
        "urn_uuid_upper_canonical": f"urn:uuid:{canonical.upper()}",
        "urn_uuid_upper_hex": f"urn:uuid:{hex_value.upper()}",
        "urn_uuid_braced_canonical": f"urn:uuid:{{{canonical}}}",
        "urn_uuid_braced_hex": f"urn:uuid:{{{hex_value}}}",
        "urn_uuid_braced_upper_canonical": f"urn:uuid:{{{canonical.upper()}}}",
        "urn_uuid_braced_upper_hex": f"urn:uuid:{{{hex_value.upper()}}}",
        "uuid_canonical": f"uuid:{canonical}",
        "uuid_hex": f"uuid:{hex_value}",
        "uuid_upper_canonical": f"uuid:{canonical.upper()}",
        "uuid_upper_hex": f"uuid:{hex_value.upper()}",
        "urn_canonical": f"urn:{canonical}",
        "urn_hex": f"urn:{hex_value}",
        "urn_upper_canonical": f"urn:{canonical.upper()}",
        "urn_upper_hex": f"urn:{hex_value.upper()}",
        "partial_hyphens": canonical.replace("-", "", 1),
        "arbitrary_hyphens": "-".join(
            hex_value[index : index + 4] for index in range(0, 32, 4)
        ),
        "repeated_braces": f"{{{{{canonical}}}}}",
        "embedded_prefixes": (
            f"{hex_value[:8]}urn:{hex_value[8:16]}uuid:{hex_value[16:]}"
        ),
    }
    return aliases[spelling]


def _mutate_frame_id(
    database_path: Path,
    frame_id: UUID,
    alias: str,
    mutation: str,
) -> None:
    with sqlite3.connect(database_path) as connection:
        if mutation == "rename":
            connection.execute(
                "UPDATE NioIngestFrame SET frame_id = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (alias, ACCOUNT_ID, str(frame_id)),
            )
            return
        connection.execute(
            "INSERT INTO NioIngestFrame ("
            "account_id, frame_id, source_epoch, request_id, staged_revision, "
            "payload, payload_sha256, room_materialized_revision, "
            "drain_header_sha256) "
            "SELECT account_id, ?, source_epoch, request_id, staged_revision, "
            "payload, payload_sha256, room_materialized_revision, "
            "drain_header_sha256 "
            "FROM NioIngestFrame "
            "WHERE account_id = ? AND frame_id = ?",
            (alias, ACCOUNT_ID, str(frame_id)),
        )


@pytest.mark.parametrize("spelling", UUID_ALIAS_SPELLINGS)
@pytest.mark.parametrize("mutation", ("rename", "copy"))
@pytest.mark.parametrize("reader", ("load", "list"))
def test_every_python_uuid_alias_fails_stopped_for_every_frame_read(
    tmp_path: Path,
    spelling: str,
    mutation: str,
    reader: str,
) -> None:
    stage = _stage_one(tmp_path)
    frame_id = stage.proposal.frame.frame_id
    alias = _uuid_alias(frame_id, spelling)
    assert alias != str(frame_id)
    assert UUID(alias) == frame_id
    _mutate_frame_id(stage.database_path, frame_id, alias, mutation)

    reopened = _open(tmp_path)
    try:
        with pytest.raises(JournalIntegrityError, match="frame_id"):
            if reader == "load":
                reopened._journal.load_frame(frame_id)
            else:
                reopened._journal.list_frames(256)
    finally:
        reopened.close()


@pytest.mark.parametrize("reader", ("load", "list"))
def test_invalid_raw_frame_id_fails_stopped_for_every_frame_read(
    tmp_path: Path,
    reader: str,
) -> None:
    stage = _stage_one(tmp_path)
    frame_id = stage.proposal.frame.frame_id
    _mutate_frame_id(stage.database_path, frame_id, "not-a-uuid", "rename")

    reopened = _open(tmp_path)
    try:
        with pytest.raises(JournalIntegrityError, match="frame_id"):
            if reader == "load":
                reopened._journal.load_frame(frame_id)
            else:
                reopened._journal.list_frames(256)
    finally:
        reopened.close()


@pytest.mark.parametrize("mutation", ("rename", "copy"))
def test_collision_probe_uses_account_frame_identity_classification(
    tmp_path: Path,
    mutation: str,
) -> None:
    stage = _stage_one(tmp_path)
    frame_id = stage.proposal.frame.frame_id
    _mutate_frame_id(
        stage.database_path,
        frame_id,
        f"uuid:{frame_id.hex}",
        mutation,
    )
    statements: list[str] = []
    reopened = _open(tmp_path, statements=statements)
    try:
        statements.clear()
        with pytest.raises(JournalIntegrityError, match="frame_id"):
            _stage(
                reopened._journal,
                proposal=stage.proposal,
            )
        assert _business_dml(statements) == []
    finally:
        reopened.close()


def test_load_frame_classifies_ids_then_fetches_only_the_exact_payload(
    tmp_path: Path,
) -> None:
    stage = _stage_one(tmp_path)
    frame_id = stage.proposal.frame.frame_id
    statements: list[str] = []
    reopened = _open(tmp_path, statements=statements)
    try:
        statements.clear()
        assert reopened._journal.load_frame(frame_id) == replace(
            stage.proposal.frame,
            staged_revision=stage.committed.revision,
        )
        frame_selects = [sql for sql in statements if "FROM NioIngestFrame" in sql]
        assert [_normalized_sql(sql) for sql in frame_selects] == [
            _normalized_sql(
                CLASSIFY_FRAME_IDS_SQL.replace("?", f"'{ACCOUNT_ID}'", 1).replace(
                    "?", str(FRAME_CLASSIFICATION_LIMIT), 1
                )
            ),
            _normalized_sql(
                LOAD_FRAME_SQL.replace("?", f"'{ACCOUNT_ID}'", 1).replace(
                    "?", f"'{frame_id}'", 1
                )
            ),
        ]
    finally:
        reopened.close()


def test_missing_load_classifies_256_large_rows_without_fetching_payloads(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    owner = bootstrap._journal.load_owner()
    bootstrap.close()
    database_path = tmp_path / "journal.db"
    placeholder_sha256 = hashlib.sha256(bytes(LARGE_PAYLOAD_PLACEHOLDER_BYTES)).digest()

    def placeholder_row(index: int) -> tuple[object, ...]:
        frame_id = UUID(int=index + 1)
        header = _expected_plaintext_frame_drain_header(
            stream_id=owner.stream_id,
            frame_id=frame_id,
            source_epoch=0,
            request_id=index,
            staged_revision=1,
            payload_sha256=placeholder_sha256,
            payload_length=LARGE_PAYLOAD_PLACEHOLDER_BYTES,
            room_materialized_revision=None,
            transport_kind=owner.transport_kind,
        )
        return (
            ACCOUNT_ID,
            str(frame_id),
            index,
            LARGE_PAYLOAD_PLACEHOLDER_BYTES,
            placeholder_sha256,
            hashlib.sha256(header).digest(),
        )

    with sqlite3.connect(database_path) as connection:
        connection.executemany(
            "INSERT INTO NioIngestFrame ("
            "account_id, frame_id, source_epoch, request_id, staged_revision, "
            "payload, payload_sha256, drain_header_sha256) "
            "VALUES (?, ?, 0, ?, 1, zeroblob(?), ?, ?)",
            (placeholder_row(index) for index in range(256)),
        )

    statements: list[str] = []
    reopened = _open(tmp_path, statements=statements)
    try:
        statements.clear()
        assert reopened._journal.load_frame(UUID(int=0)) is None
        frame_selects = [sql for sql in statements if "FROM NioIngestFrame" in sql]
        assert [_normalized_sql(sql) for sql in frame_selects] == [
            _normalized_sql(
                CLASSIFY_FRAME_IDS_SQL.replace("?", f"'{ACCOUNT_ID}'", 1).replace(
                    "?", str(FRAME_CLASSIFICATION_LIMIT), 1
                )
            )
        ]
    finally:
        reopened.close()


@pytest.mark.parametrize("mutation", ("request", "body", "digest", "revision"))
def test_same_frame_id_changed_content_or_revision_fails_before_dml(
    tmp_path: Path,
    mutation: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements=statements)
    journal = bootstrap._journal
    try:
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
        if mutation == "source_epoch":
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
                proposal=proposal,
            )

        assert _business_dml(statements) == []
        assert journal.load_owner() == owner
        assert journal.load_source() == proposal.prior_source
        assert journal.list_frames(256) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "corruption", ("extra_meta", "extra_source", "mismatched_source")
)
def test_stage_snapshot_preserves_independent_global_cardinality_before_dml(
    tmp_path: Path, corruption: str
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements=statements)
    journal = bootstrap._journal
    owner = journal.load_owner()
    proposal = _stage_proposal(journal, CLASSIC_SOURCE, 1)
    foreign_account = "@mallory:example.org"
    with sqlite3.connect(tmp_path / "journal.db") as external:
        if corruption == "extra_meta":
            row = external.execute(
                "SELECT device_id, schema_version, stream_id, consumer_generation, transport_kind, "
                "revision, writer_epoch, next_source_epoch, created_at_ns, "
                "delivery_next_sequence, delivery_acknowledged_sha256, "
                "delivery_outstanding_work_id, delivery_outstanding_ready_revision, "
                "delivery_outstanding_ready_ordinal, delivery_outstanding_batch_sha256 "
                "FROM NioIngestMeta WHERE account_id = ?",
                (ACCOUNT_ID,),
            ).fetchone()
            external.execute(
                "INSERT INTO NioIngestMeta VALUES "
                "(?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
                (foreign_account, *row),
            )
        else:
            row = external.execute(
                "SELECT source_epoch, payload, payload_sha256, next_request_id, "
                "active FROM NioIngestSourceState "
                "WHERE account_id = ?",
                (ACCOUNT_ID,),
            ).fetchone()
            if corruption == "mismatched_source":
                external.execute(
                    "DELETE FROM NioIngestSourceState WHERE account_id = ?",
                    (ACCOUNT_ID,),
                )
            external.execute(
                "INSERT INTO NioIngestSourceState VALUES (?, ?, ?, ?, ?, ?)",
                (foreign_account, *row),
            )

    with pytest.raises(JournalIntegrityError):
        journal.load_owner() if corruption == "extra_meta" else journal.load_source()
    statements.clear()
    with pytest.raises(JournalIntegrityError):
        journal.stage_source_response(
            source=proposal.successor_source,
            frame=proposal.frame,
        )

    assert _business_dml(statements) == []
    with sqlite3.connect(tmp_path / "journal.db") as external:
        assert external.execute(
            "SELECT revision FROM NioIngestMeta WHERE account_id = ?", (ACCOUNT_ID,)
        ).fetchone() == (owner.revision,)
        assert external.execute("SELECT COUNT(*) FROM NioIngestFrame").fetchone() == (
            0,
        )
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
            payload, payload_sha256, drain_header_sha256 = _plaintext_frame_storage(
                owner.stream_id,
                frame,
                committed.revision,
            )
            connection.execute(
                "INSERT INTO NioIngestFrame (account_id, frame_id, source_epoch, "
                "request_id, staged_revision, payload, payload_sha256, "
                "drain_header_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    ACCOUNT_ID,
                    str(frame.frame_id),
                    frame.response.request.source_epoch,
                    frame.response.request.request_id,
                    committed.revision,
                    payload,
                    payload_sha256,
                    drain_header_sha256,
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
                CLASSIFY_FRAME_IDS_SQL.replace("?", f"'{ACCOUNT_ID}'", 1).replace(
                    "?", str(FRAME_CLASSIFICATION_LIMIT), 1
                )
            ),
            _normalized_sql(
                LOAD_FRAME_HEADERS_SQL.replace("?", str(FRAME_CLASSIFICATION_LIMIT), 1)
            ),
            _normalized_sql(
                LIST_FRAMES_SQL.replace("?", f"'{ACCOUNT_ID}'", 1).replace(
                    "?", "258", 1
                )
            ),
        ]
        for limit in range(1, 258):
            assert reopened._journal.list_frames(limit) == expected[:limit]
        for invalid_limit in (0, 258, True, 1.0):
            with pytest.raises(ValueError, match="frame limit"):
                reopened._journal.list_frames(invalid_limit)
        with sqlite3.connect(tmp_path / "journal.db") as connection:
            plan = connection.execute(
                f"EXPLAIN QUERY PLAN {CLASSIFY_FRAME_IDS_SQL}",
                (ACCOUNT_ID, FRAME_CLASSIFICATION_LIMIT),
            ).fetchall()
        assert any("USING COVERING INDEX" in row[3] for row in plan)
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


def test_plaintext_ingestion_rows_delete_the_dedicated_encryption_codec() -> None:
    assert importlib.util.find_spec("nio.store._sync_journal_codec") is None


def test_plaintext_ingestion_journal_has_no_row_codec(tmp_path: Path) -> None:
    bootstrap = _open(tmp_path)
    try:
        assert not hasattr(bootstrap._journal, "_codec")
    finally:
        bootstrap.close()


@pytest.mark.parametrize("source_config", (CLASSIC_SOURCE, SLIDING_SOURCE))
def test_plaintext_headers_and_frame_envelope_are_exact_and_canonical(
    tmp_path: Path,
    source_config: ClassicSourceConfig | SlidingSourceConfig,
) -> None:
    stage = _stage_one(tmp_path, source_config)
    stored = replace(
        stage.proposal.frame,
        staged_revision=stage.committed.revision,
    )
    frame_row = _stored_row(stage.database_path, stored.frame_id)
    source_row = _stored_row(stage.database_path)
    assert tuple(source_row.keys()) == _PLAINTEXT_SOURCE_COLUMNS
    assert tuple(frame_row.keys()) == _PLAINTEXT_FRAME_COLUMNS

    source = stage.proposal.successor_source
    source_payload = _expected_plaintext_row_envelope(
        row_kind="source",
        account_id=ACCOUNT_ID,
        stream_id=stage.stream_id,
        transport_kind=source.transport_kind,
        clear_fields=(
            ("source_epoch", source.source_epoch),
            ("next_request_id", source.next_request_id),
            ("active", source.active),
        ),
        value=json.loads(source.cursor_json),
    )
    assert source_row["payload"] == source_payload
    assert source_row["payload_sha256"] == hashlib.sha256(source_payload).digest()

    request = stored.response.request
    frame_payload = _expected_plaintext_row_envelope(
        row_kind="frame",
        account_id=ACCOUNT_ID,
        stream_id=stage.stream_id,
        transport_kind=request.transport,
        clear_fields=(
            ("frame_id", str(stored.frame_id)),
            ("source_epoch", request.source_epoch),
            ("request_id", request.request_id),
            ("staged_revision", stored.staged_revision),
        ),
        value=_frame_envelope(stored),
    )
    payload_sha256 = hashlib.sha256(frame_payload).digest()
    assert frame_row["payload"] == frame_payload
    assert frame_row["payload_sha256"] == payload_sha256
    assert frame_row["room_materialized_revision"] is None
    expected_header = _expected_plaintext_frame_drain_header(
        stream_id=stage.stream_id,
        frame_id=stored.frame_id,
        source_epoch=request.source_epoch,
        request_id=request.request_id,
        staged_revision=stored.staged_revision,
        payload_sha256=payload_sha256,
        payload_length=len(frame_payload),
        room_materialized_revision=None,
        transport_kind=request.transport,
    )
    assert frame_row["drain_header_sha256"] == hashlib.sha256(expected_header).digest()

    frame_envelope = json.loads(frame_payload)
    assert tuple(frame_envelope) == (
        "schema_version",
        "row_kind",
        "account_id",
        "stream_id",
        "transport_kind",
        "frame_id",
        "source_epoch",
        "request_id",
        "staged_revision",
        "value",
    )
    assert tuple(frame_envelope["value"]) == (
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


@pytest.mark.parametrize("mutation", ["source_epoch", "state", "header_sha256"])
def test_drain_header_sha_rejects_order_state_or_digest_before_payload_parse(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    mutation: str,
) -> None:
    stage = _stage_one(tmp_path)
    frame_id = stage.proposal.frame.frame_id
    row = _stored_row(stage.database_path, frame_id)
    assert tuple(row.keys()) == _PLAINTEXT_FRAME_COLUMNS
    with sqlite3.connect(stage.database_path) as connection:
        if mutation == "source_epoch":
            connection.execute(
                "UPDATE NioIngestFrame SET source_epoch = source_epoch + 1 "
                "WHERE account_id = ? AND frame_id = ?",
                (ACCOUNT_ID, str(frame_id)),
            )
        elif mutation == "state":
            connection.execute(
                "UPDATE NioIngestFrame SET room_materialized_revision = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (stage.committed.revision, ACCOUNT_ID, str(frame_id)),
            )
        else:
            digest = bytearray(row["drain_header_sha256"])
            assert len(digest) == 32
            digest[-1] ^= 1
            connection.execute(
                "UPDATE NioIngestFrame SET drain_header_sha256 = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (bytes(digest), ACCOUNT_ID, str(frame_id)),
            )

    reopened = _open(tmp_path)
    try:
        payload_parse_calls: list[None] = []

        def reject_payload_parse(*args: object, **kwargs: object) -> object:
            payload_parse_calls.append(None)
            raise AssertionError("payload parsing ran after a bad drain-header SHA")

        monkeypatch.setattr(json, "loads", reject_payload_parse)
        with pytest.raises(JournalIntegrityError):
            reopened._journal.load_frame(frame_id)
        assert payload_parse_calls == []
    finally:
        reopened.close()


@pytest.mark.parametrize(
    ("row_kind", "corruption"),
    (
        *(
            ("source", case)
            for case in (
                "payload-only",
                "digest-only",
                "recomputed-noncanonical",
                "semantic",
                "context-account_id",
                "context-stream_id",
                "context-transport_kind",
                "clear-account_id",
                "clear-source_epoch",
                "clear-next_request_id",
                "clear-active",
            )
        ),
        *(
            ("frame", case)
            for case in (
                "payload-only",
                "digest-only",
                "recomputed-noncanonical",
                "semantic",
                "context-account_id",
                "context-stream_id",
                "context-transport_kind",
                "clear-account_id",
                "clear-frame_id",
                "clear-source_epoch",
                "clear-request_id",
                "clear-staged_revision",
            )
        ),
    ),
)
def test_plaintext_corruption_fails_selected_row(
    tmp_path: Path,
    row_kind: str,
    corruption: str,
) -> None:
    """SHA catches accidents; every duplicated clear field requires equality."""

    stage = _stage_one(tmp_path, CLASSIC_SOURCE)
    frame_id = stage.proposal.frame.frame_id
    table = "NioIngestSourceState" if row_kind == "source" else "NioIngestFrame"
    where = (
        "account_id = ?"
        if row_kind == "source"
        else ("account_id = ? AND frame_id = ?")
    )
    keys: tuple[object, ...] = (
        (ACCOUNT_ID,)
        if row_kind == "source"
        else (
            ACCOUNT_ID,
            str(frame_id),
        )
    )
    with sqlite3.connect(stage.database_path) as connection:
        connection.row_factory = sqlite3.Row
        row = connection.execute(
            f"SELECT * FROM {table} WHERE {where}",
            keys,
        ).fetchone()
        assert row is not None
        assert tuple(row.keys()) == (
            _PLAINTEXT_SOURCE_COLUMNS
            if row_kind == "source"
            else _PLAINTEXT_FRAME_COLUMNS
        )
        payload = bytes(row["payload"])
        digest = bytes(row["payload_sha256"])
        selected_account_id = ACCOUNT_ID
        selected_frame_id = frame_id
        direct_frame_decode = False
        if corruption == "payload-only":
            payload = _flip_plaintext_test_byte(payload)
        elif corruption == "digest-only":
            digest = _flip_plaintext_test_byte(digest)
        elif corruption == "recomputed-noncanonical":
            payload += b" "
            digest = hashlib.sha256(payload).digest()
        elif corruption == "semantic" or corruption.startswith("context-"):
            envelope = json.loads(payload)
            if corruption == "semantic":
                envelope["schema_version"] = 2
            else:
                field = corruption.removeprefix("context-")
                value = envelope[field]
                if type(value) is bool:
                    envelope[field] = not value
                elif type(value) is int:
                    envelope[field] = value + 1
                elif field == "account_id":
                    envelope[field] = "@drift:example.org"
                elif field == "stream_id" or field.endswith("_id"):
                    envelope[field] = str(uuid4())
                else:
                    assert field == "transport_kind"
                    envelope[field] = TransportKind.SLIDING.value
            payload = _canonical_internal(envelope)
            digest = hashlib.sha256(payload).digest()
        else:
            # Keep the canonical envelope and its digest byte-for-byte intact.
            # These cases therefore isolate equality with the SQLite clear
            # column rather than nested value semantics or canonical parsing.
            field = corruption.removeprefix("clear-")
            if field == "account_id":
                clear_value: object = "@drift:example.org"
                selected_account_id = "@drift:example.org"
            elif field == "frame_id":
                selected_frame_id = uuid4()
                clear_value = str(selected_frame_id)
                direct_frame_decode = True
            elif field == "active":
                clear_value = 1 - row[field]
            else:
                clear_value = row[field] + 1
            updated = connection.execute(
                f"UPDATE {table} SET {field} = ? WHERE {where}",
                (clear_value, *keys),
            )
            assert updated.rowcount == 1
            if row_kind == "frame":
                mutated = connection.execute(
                    "SELECT * FROM NioIngestFrame WHERE account_id = ? "
                    "AND frame_id = ?",
                    (selected_account_id, str(selected_frame_id)),
                ).fetchone()
                assert mutated is not None
                updated = connection.execute(
                    "UPDATE NioIngestFrame SET drain_header_sha256 = ? "
                    "WHERE account_id = ? AND frame_id = ?",
                    (
                        _plaintext_frame_header_sha256(
                            account_id=selected_account_id,
                            stream_id=stage.stream_id,
                            transport_kind=TransportKind.CLASSIC,
                            row=mutated,
                            payload=bytes(mutated["payload"]),
                            payload_sha256=bytes(mutated["payload_sha256"]),
                        ),
                        selected_account_id,
                        str(selected_frame_id),
                    ),
                )
                assert updated.rowcount == 1
                row = connection.execute(
                    "SELECT * FROM NioIngestFrame WHERE account_id = ? "
                    "AND frame_id = ?",
                    (selected_account_id, str(selected_frame_id)),
                ).fetchone()
                assert row is not None

        if not corruption.startswith("clear-"):
            assignments = ["payload = ?", "payload_sha256 = ?"]
            values: list[object] = [payload, digest]
            if row_kind == "frame":
                assignments.append("drain_header_sha256 = ?")
                values.append(
                    _plaintext_frame_header_sha256(
                        stream_id=stage.stream_id,
                        row=row,
                        payload=payload,
                        payload_sha256=digest,
                    )
                )
            updated = connection.execute(
                f"UPDATE {table} SET {', '.join(assignments)} WHERE {where}",
                (*values, *keys),
            )
            assert updated.rowcount == 1

    if row_kind == "source":
        with pytest.raises(JournalIntegrityError):
            reopened = _open(tmp_path, CLASSIC_SOURCE)
            reopened.close()
        return
    reopened = _open(tmp_path, CLASSIC_SOURCE)
    try:
        if corruption == "clear-account_id":
            with pytest.raises(JournalIntegrityError):
                reopened._journal.load_frame(frame_id)
            with pytest.raises(JournalIntegrityError):
                reopened._journal.list_frames(256)
        else:
            with pytest.raises(JournalIntegrityError):
                if direct_frame_decode:
                    owner = reopened._journal.load_owner()
                    with reopened._journal._owner.read():
                        reopened._journal._decode_frame_row(
                            selected_frame_id,
                            row,
                            owner,
                        )
                else:
                    reopened._journal.load_frame(frame_id)
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


def _kill_nested_e2ee_stage(
    store_path: Path,
    proposal: _StageProposal,
    sequence_path: Path,
) -> None:
    bootstrap = _open(store_path)
    journal = bootstrap._journal
    store = bootstrap._open_owned_store_candidate()._store_for_attachment()

    def kill_after_frame_insert(label: str) -> None:
        with sequence_path.open("a", encoding="utf-8") as sequence:
            sequence.write(f"{label}\n")
            sequence.flush()
            os.fsync(sequence.fileno())
        if label == "frame_insert":
            os._exit(CRASH_EXIT_CODE)

    journal.set_transition_statement_hook(kill_after_frame_insert)
    with journal._owner.journal_write():
        store.save_account(OlmAccount())
        _stage(journal, proposal=proposal)
    bootstrap.close()


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


@pytest.mark.parametrize("boundary", STAGE_HOOK_LABELS)
def test_sliding_stage_failure_is_atomic_before_rotated_reopen(
    tmp_path: Path,
    boundary: str,
) -> None:
    class SlidingStageAbort(BaseException):
        pass

    bootstrap = _open(tmp_path, SLIDING_SOURCE)
    journal = bootstrap._journal
    owner_before = journal.load_owner()
    source_before = journal.load_source()
    proposal = _stage_proposal(journal, SLIDING_SOURCE, 1)
    observed: list[str] = []
    sentinel = SlidingStageAbort(boundary)

    def abort_at_boundary(label: str) -> None:
        observed.append(label)
        if label == boundary:
            raise sentinel

    committed = boundary == "commit"
    expected_frame = (
        replace(proposal.frame, staged_revision=owner_before.revision + 1)
        if committed
        else None
    )
    journal.set_transition_statement_hook(abort_at_boundary)
    try:
        with pytest.raises(SlidingStageAbort) as failure:
            _stage(journal, proposal=proposal)
        assert failure.value is sentinel
        assert observed == list(
            STAGE_HOOK_LABELS[: STAGE_HOOK_LABELS.index(boundary) + 1]
        )
        assert journal.load_owner().revision == owner_before.revision + int(committed)
        assert journal.load_source() == (
            proposal.successor_source if committed else source_before
        )
        assert journal.load_frame(proposal.frame.frame_id) == expected_frame
    finally:
        journal.set_transition_statement_hook(None)
        bootstrap.close()

    reopened = _open(tmp_path, SLIDING_SOURCE)
    try:
        reopened_source = reopened._journal.load_source()
        reopened_cursor = json.loads(reopened_source.cursor_json)
        assert reopened_cursor["pos"] is None
        assert reopened_source.next_request_id == 0
        assert reopened._journal.load_frame(proposal.frame.frame_id) == expected_frame
    finally:
        reopened.close()


def test_nested_real_e2ee_write_rolls_back_with_crashed_source_stage(
    tmp_path: Path,
) -> None:
    store_path = tmp_path / "nested-e2ee"
    bootstrap = _open(store_path)
    bootstrap._open_owned_store_candidate()._store_for_attachment()
    owner = bootstrap._journal.load_owner()
    proposal = _stage_proposal(bootstrap._journal, CLASSIC_SOURCE, 1)
    bootstrap.close()

    sequence_path = store_path / "nested-stage-hook-sequence.txt"
    process = multiprocessing.get_context("spawn").Process(
        target=_kill_nested_e2ee_stage,
        args=(store_path, proposal, sequence_path),
    )
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("nested E2EE crash-injection child did not exit")
    assert process.exitcode == CRASH_EXIT_CODE
    assert tuple(sequence_path.read_text(encoding="utf-8").splitlines()) == (
        "frame_collision_probe",
        "meta_revision_epoch_cas",
        "source_state_upsert",
        "frame_insert",
    )

    reopened = _open(store_path)
    try:
        store = reopened._open_owned_store_candidate()._store_for_attachment()
        assert (
            reopened._journal.load_owner().revision,
            reopened._journal.load_source(),
            reopened._journal.load_frame(proposal.frame.frame_id),
            store.load_account(),
        ) == (owner.revision, proposal.prior_source, None, None)
    finally:
        reopened.close()


def test_external_sqlite_write_lock_times_out_stage_without_partial_writes(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path, sqlite_busy_timeout_ms=100)
    journal = bootstrap._journal
    owner = journal.load_owner()
    proposal = _stage_proposal(journal, CLASSIC_SOURCE, 1)
    external = sqlite3.connect(
        bootstrap.database_path,
        isolation_level=None,
        timeout=0,
    )
    try:
        external.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        with pytest.raises(
            (sqlite3.OperationalError, PeeweeOperationalError),
            match="locked",
        ):
            _stage(journal, proposal=proposal)
        elapsed = time.monotonic() - started

        assert 0.100 <= elapsed <= 0.350
        assert (
            journal.load_owner(),
            journal.load_source(),
            journal.list_frames(256),
        ) == (owner, proposal.prior_source, ())

        external.rollback()
        committed = _stage(journal, proposal=proposal)
        assert (
            committed,
            journal.load_source(),
            journal.load_frame(proposal.frame.frame_id),
        ) == (
            CommitResult(owner.revision + 1),
            proposal.successor_source,
            replace(proposal.frame, staged_revision=owner.revision + 1),
        )
    finally:
        if external.in_transaction:
            external.rollback()
        external.close()
        bootstrap.close()


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
    proposal = _stage_proposal(bootstrap._journal, source_config, 1)
    expected = proposal.normalized_frame
    expected_observations = tuple(
        segment.membership_observation for segment in expected.room_segments
    )
    committed = _stage(
        bootstrap._journal,
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
        assert observations == expected_observations
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


@pytest.mark.parametrize("source_config", (CLASSIC_SOURCE, SLIDING_SOURCE))
@pytest.mark.parametrize(
    "mutation",
    (
        "stream",
        "transport",
        "source_epoch",
        "request_id",
        "response_body",
        "source_digest",
        "frame_id",
        "normalization_version",
    ),
)
def test_replay_rejects_each_stored_common_frame_mutation(
    tmp_path: Path,
    source_config: ClassicSourceConfig | SlidingSourceConfig,
    mutation: str,
) -> None:
    stage = _stage_one(tmp_path, source_config)
    envelope = _stored_plaintext_frame_value(stage)
    request = envelope["request"]
    assert type(request) is dict
    stored_id = stage.proposal.frame.frame_id
    if mutation == "stream":
        request["stream_id"] = str(uuid4())
    elif mutation == "transport":
        request["transport"] = (
            TransportKind.SLIDING.value
            if source_config is CLASSIC_SOURCE
            else TransportKind.CLASSIC.value
        )
    elif mutation == "source_epoch":
        request["source_epoch"] += 1
    elif mutation == "request_id":
        request["request_id"] += 1
    elif mutation == "response_body":
        envelope["response_body"] = _encoded_bytes(b'{"changed":true}')
    elif mutation == "source_digest":
        envelope["source_sha256"] = _encoded_bytes(bytes(32))
    elif mutation == "frame_id":
        stored_id = uuid4()
    else:
        assert mutation == "normalization_version"
        envelope["normalization_version"] = 2
    _replace_plaintext_frame_value(stage, envelope, frame_id=stored_id)

    reopened = _open(tmp_path, source_config)
    try:
        with pytest.raises(JournalIntegrityError):
            reopened._journal.load_frame(stored_id)
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "mutation",
    ("method", "path", "query", "full_state", "filter", "cursor", "timeout"),
)
def test_classic_replay_rejects_each_stored_frozen_request_mutation(
    tmp_path: Path,
    mutation: str,
) -> None:
    stage = _stage_one(tmp_path, CLASSIC_SOURCE)
    envelope = _stored_plaintext_frame_value(stage)
    request = envelope["request"]
    assert type(request) is dict
    query = request["query"]
    assert type(query) is list
    if mutation == "method":
        request["method"] = "POST"
    elif mutation == "path":
        request["path"] = "/_matrix/client/v3/rooms"
    elif mutation == "query":
        query.append(["extra", "unsafe"])
    elif mutation == "full_state":
        query[0][1] = "false"
    elif mutation == "filter":
        query[-1][1] = "{ }"
    elif mutation == "cursor":
        request["request_cursor_json"] = _encoded_bytes(b'{"next_batch":"other"}')
    else:
        assert mutation == "timeout"
        request["timeout_ms"] += 1
    _replace_plaintext_frame_value(stage, envelope)

    reopened = _open(tmp_path, CLASSIC_SOURCE)
    try:
        loaded = reopened._journal.load_frame(stage.proposal.frame.frame_id)
        assert loaded is not None
        with pytest.raises(ValueError):
            renormalize_staged_frame(
                _source_adapter(stage.stream_id, CLASSIC_SOURCE),
                loaded,
            )
    finally:
        reopened.close()


@pytest.mark.parametrize(
    "mutation",
    (
        "connection_name",
        "reserved_list",
        "reserved_range",
        "subscription",
        "extensions",
        "to_device_since",
        "e2ee_flags",
        "cursor",
        "body",
    ),
)
def test_sliding_replay_rejects_each_stored_frozen_request_mutation(
    tmp_path: Path,
    mutation: str,
) -> None:
    stage = _stage_one(tmp_path, SLIDING_SOURCE)
    envelope = _stored_plaintext_frame_value(stage)
    request = envelope["request"]
    assert type(request) is dict
    body = json.loads(base64.b64decode(request["body"], validate=True))
    cursor = json.loads(base64.b64decode(request["request_cursor_json"], validate=True))
    if mutation == "connection_name":
        cursor["connection_name"] = "other-worker"
    elif mutation == "reserved_list":
        del body["lists"][RESERVED_ALL_ROOMS_LIST]["required_state"]
    elif mutation == "reserved_range":
        body["lists"][RESERVED_ALL_ROOMS_LIST]["ranges"] = [[0, 99]]
    elif mutation == "subscription":
        body["room_subscriptions"] = {
            f"!room-{index}:example.org": {} for index in range(101)
        }
    elif mutation == "extensions":
        body["extensions"]["typing"]["lists"] = []
    elif mutation == "to_device_since":
        body["extensions"]["to_device"]["since"] = "td-unsafe"
    elif mutation == "e2ee_flags":
        body["extensions"]["e2ee"]["enabled"] = False
    elif mutation == "cursor":
        request["request_cursor_json"] = _encoded_bytes(
            base64.b64decode(request["request_cursor_json"], validate=True) + b" "
        )
    else:
        assert mutation == "body"
        request["body"] = _encoded_bytes(b"{}")
    if mutation == "connection_name":
        request["request_cursor_json"] = _encoded_bytes(canonical_json(cursor))
    elif mutation not in {"cursor", "body"}:
        request["body"] = _encoded_bytes(canonical_json(body))
    _replace_plaintext_frame_value(stage, envelope)

    reopened = _open(tmp_path, SLIDING_SOURCE)
    try:
        loaded = reopened._journal.load_frame(stage.proposal.frame.frame_id)
        assert loaded is not None
        with pytest.raises(ValueError):
            renormalize_staged_frame(
                _source_adapter(stage.stream_id, SLIDING_SOURCE),
                loaded,
            )
    finally:
        reopened.close()


_PLAINTEXT_SOURCE_COLUMNS = (
    "account_id",
    "source_epoch",
    "payload",
    "payload_sha256",
    "next_request_id",
    "active",
)
_PLAINTEXT_FRAME_COLUMNS = (
    "account_id",
    "frame_id",
    "source_epoch",
    "request_id",
    "staged_revision",
    "payload",
    "payload_sha256",
    "room_materialized_revision",
    "callbacks_claimed_revision",
    "drain_header_sha256",
)


def _expected_plaintext_row_envelope(
    *,
    row_kind: str,
    account_id: str,
    stream_id: UUID,
    transport_kind: TransportKind,
    clear_fields: tuple[tuple[str, object], ...],
    value: object,
) -> bytes:
    """Hand-derived v1 row envelope; production helpers are deliberately unused."""

    return _canonical_internal(
        {
            "schema_version": 1,
            "row_kind": row_kind,
            "account_id": account_id,
            "stream_id": str(stream_id),
            "transport_kind": transport_kind.value,
            **dict(clear_fields),
            "value": value,
        }
    )


def _expected_plaintext_frame_drain_header(
    *,
    stream_id: UUID,
    frame_id: UUID,
    source_epoch: int,
    request_id: int,
    staged_revision: int,
    payload_sha256: bytes,
    payload_length: int,
    room_materialized_revision: int | None,
    callbacks_claimed_revision: int | None = None,
    account_id: str = ACCOUNT_ID,
    transport_kind: TransportKind = TransportKind.CLASSIC,
) -> bytes:
    return _canonical_internal(
        {
            "schema_version": 1,
            "row_kind": "frame",
            "account_id": account_id,
            "stream_id": str(stream_id),
            "transport_kind": transport_kind.value,
            "frame_id": str(frame_id),
            "source_epoch": source_epoch,
            "request_id": request_id,
            "staged_revision": staged_revision,
            "payload_sha256": base64.b64encode(payload_sha256).decode("ascii"),
            "payload_length": payload_length,
            "room_materialized_revision": room_materialized_revision,
            "callbacks_claimed_revision": callbacks_claimed_revision,
        }
    )


def _flip_plaintext_test_byte(value: bytes) -> bytes:
    assert value
    return value[:-1] + bytes((value[-1] ^ 1,))


def _plaintext_frame_header_sha256(
    *,
    stream_id: UUID,
    row: sqlite3.Row,
    payload: bytes,
    payload_sha256: bytes,
    account_id: str = ACCOUNT_ID,
    transport_kind: TransportKind = TransportKind.CLASSIC,
) -> bytes:
    header = _expected_plaintext_frame_drain_header(
        stream_id=stream_id,
        frame_id=UUID(row["frame_id"]),
        source_epoch=row["source_epoch"],
        request_id=row["request_id"],
        staged_revision=row["staged_revision"],
        payload_sha256=payload_sha256,
        payload_length=len(payload),
        room_materialized_revision=row["room_materialized_revision"],
        callbacks_claimed_revision=row["callbacks_claimed_revision"],
        account_id=account_id,
        transport_kind=transport_kind,
    )
    return hashlib.sha256(header).digest()


@pytest.mark.parametrize("row_kind", ("source", "frame"))
def test_v1_plaintext_source_and_frame_payload_identity_moves_fail_equality(
    tmp_path: Path,
    row_kind: str,
) -> None:
    """RED: moving payload+SHA cannot silently change its stored identity."""

    if row_kind == "source":
        stage = _stage_one(tmp_path, CLASSIC_SOURCE)
        with sqlite3.connect(stage.database_path) as connection:
            connection.row_factory = sqlite3.Row
            row = connection.execute("SELECT * FROM NioIngestSourceState").fetchone()
            assert row is not None
            assert tuple(row.keys()) == _PLAINTEXT_SOURCE_COLUMNS
            envelope = json.loads(row["payload"])
            envelope["account_id"] = "@drift:example.org"
            payload = _canonical_internal(envelope)
            updated = connection.execute(
                "UPDATE NioIngestSourceState SET payload = ?, payload_sha256 = ? "
                "WHERE account_id = ?",
                (payload, hashlib.sha256(payload).digest(), ACCOUNT_ID),
            )
            assert updated.rowcount == 1
        with pytest.raises(JournalIntegrityError):
            reopened = _open(tmp_path, CLASSIC_SOURCE)
            reopened.close()
        return

    bootstrap = _open(tmp_path, CLASSIC_SOURCE)
    journal = bootstrap._journal
    stream_id = journal.load_owner().stream_id
    first = _stage_proposal(journal, CLASSIC_SOURCE, 1)
    _stage(journal, proposal=first)
    second = _stage_proposal(journal, CLASSIC_SOURCE, 2)
    _stage(journal, proposal=second)
    bootstrap.close()
    database_path = tmp_path / "journal.db"
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute(
            "SELECT * FROM NioIngestFrame ORDER BY request_id"
        ).fetchall()
        assert len(rows) == 2
        assert all(tuple(row.keys()) == _PLAINTEXT_FRAME_COLUMNS for row in rows)
        for target, moved in ((rows[0], rows[1]), (rows[1], rows[0])):
            payload = bytes(moved["payload"])
            digest = bytes(moved["payload_sha256"])
            header_digest = _plaintext_frame_header_sha256(
                stream_id=stream_id,
                row=target,
                payload=payload,
                payload_sha256=digest,
            )
            updated = connection.execute(
                "UPDATE NioIngestFrame SET payload = ?, payload_sha256 = ?, "
                "drain_header_sha256 = ? WHERE account_id = ? AND frame_id = ?",
                (payload, digest, header_digest, ACCOUNT_ID, target["frame_id"]),
            )
            assert updated.rowcount == 1

    reopened = _open(tmp_path, CLASSIC_SOURCE)
    try:
        with pytest.raises(JournalIntegrityError):
            reopened._journal.load_frame(first.frame.frame_id)
    finally:
        reopened.close()


def _plaintext_frame_capacity_proposal(
    journal: object,
    padding_bytes: int,
) -> tuple[SourceState, StagedFrame, bytes]:
    filter_json = b'{"padding":"' + (b"x" * padding_bytes) + b'"}'
    config = ClassicSourceConfig(30_000, filter_json)
    owner = journal.load_owner()  # type: ignore[attr-defined]
    prior = journal.load_source()  # type: ignore[attr-defined]
    adapter = ClassicSource(owner.stream_id, config, OWN_USER_ID)
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
            b'{"next_batch":"capacity","rooms":{}}',
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
        prior.source_epoch,
        prior.transport_kind,
        normalized.frame.candidate_cursor_json,
        prior.next_request_id + 1,
        prior.active,
    )
    stored = replace(frame, staged_revision=owner.revision + 1)
    payload = _expected_plaintext_row_envelope(
        row_kind="frame",
        account_id=owner.account_id,
        stream_id=owner.stream_id,
        transport_kind=request.transport,
        clear_fields=(
            ("frame_id", str(stored.frame_id)),
            ("source_epoch", request.source_epoch),
            ("request_id", request.request_id),
            ("staged_revision", stored.staged_revision),
        ),
        value=_frame_envelope(stored),
    )
    return successor, frame, payload


@pytest.mark.parametrize("extra_byte", (0, 1), ids=("exact", "over"))
def test_v1_frame_runtime_cap_counts_final_stored_canonical_envelope_before_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    extra_byte: int,
) -> None:
    """RED: exact 24 MiB is accepted and +1 never enters journal_write."""

    bootstrap = _open(tmp_path, CLASSIC_SOURCE)
    journal = bootstrap._journal
    try:
        _source, _frame, base_payload = _plaintext_frame_capacity_proposal(journal, 0)
        target_bytes = 24 * 1024 * 1024 + extra_byte
        padding_bytes = target_bytes - len(base_payload)
        assert padding_bytes > 0
        source, frame, payload = _plaintext_frame_capacity_proposal(
            journal,
            padding_bytes,
        )
        assert len(payload) == target_bytes

        writer_entries = 0
        real_journal_write = type(journal._owner).journal_write

        @contextmanager
        def trace_journal_write(owner: object) -> Iterator[None]:
            nonlocal writer_entries
            assert owner is journal._owner
            writer_entries += 1
            with real_journal_write(journal._owner):
                yield

        monkeypatch.setattr(
            type(journal._owner),
            "journal_write",
            trace_journal_write,
        )
        if extra_byte:
            with pytest.raises(JournalIntegrityError):
                journal.stage_source_response(source=source, frame=frame)
            assert writer_entries == 0
            return

        journal.stage_source_response(source=source, frame=frame)
        assert writer_entries == 1
        with sqlite3.connect(journal.database_path) as connection:
            connection.row_factory = sqlite3.Row
            stored = connection.execute(
                "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
                (journal.account_id, str(frame.frame_id)),
            ).fetchone()
        assert stored is not None
        assert tuple(stored.keys()) == _PLAINTEXT_FRAME_COLUMNS
        assert stored["payload"] == payload
        assert len(stored["payload"]) == 24 * 1024 * 1024
    finally:
        bootstrap.close()


def test_v1_exact_limit_restage_ignores_later_journal_revision(tmp_path: Path) -> None:
    """An exact replay keeps its stored revision and does not grow its payload."""

    statements: list[str] = []
    bootstrap = _open(tmp_path, CLASSIC_SOURCE, statements=statements)
    journal = bootstrap._journal
    try:
        for sequence in range(1, 5):
            proposal = _stage_proposal(
                journal, CLASSIC_SOURCE, sequence, include_rooms=False
            )
            assert _stage(journal, proposal=proposal).revision == sequence * 4 - 3
            result = materialize_journal(journal, limits=MaterializerLimits())
            assert result.revision == sequence * 4 - 2
            retire_completed_frame(journal)
        assert journal.load_owner().revision == 16

        _source, _frame, base_payload = _plaintext_frame_capacity_proposal(journal, 0)
        source, frame, payload = _plaintext_frame_capacity_proposal(
            journal,
            24 * 1024 * 1024 - len(base_payload),
        )
        assert len(payload) == 24 * 1024 * 1024
        committed = journal.stage_source_response(source=source, frame=frame)
        assert committed == CommitResult(17)
        assert journal.load_owner().revision == 17
        statements.clear()

        assert journal.stage_source_response(source=source, frame=frame) == committed
        assert _business_dml(statements) == []
        assert journal.load_frame(frame.frame_id) == replace(frame, staged_revision=17)
    finally:
        bootstrap.close()
