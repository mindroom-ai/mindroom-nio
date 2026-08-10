"""Task 6 private materializer contract RED tests."""

import base64
import hashlib
import importlib
import inspect
import json
import re
import sqlite3
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, fields, is_dataclass, replace
from enum import StrEnum
from operator import itemgetter
from pathlib import Path
from types import ModuleType
from typing import get_type_hints
from uuid import UUID, uuid4

import pytest

import nio.ingest as ingest
import nio.ingest.classic as classic_module
import nio.ingest.ports as ports_module
import nio.ingest.sliding as sliding_module
import nio.store as store
from nio.ingest import source
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.errors import (
    FreshIngestionRequired,
    JournalConflictError,
    JournalIntegrityError,
)
from nio.ingest.membership import MembershipObservation
from nio.ingest.model import RecordKind, RecordOrigin, TransportKind
from nio.ingest.ports import (
    NetworkRequest,
    NetworkResult,
    StagedSourceResponse,
    _frame_id_for_response,
)
from nio.ingest.reducer import (
    ReducerInputError,
    RoomContinuity,
    reduce_staged_frame,
)
from nio.ingest.sliding import (
    RESERVED_ALL_ROOMS_LIST,
    SlidingCursor,
    SlidingRangeAckMode,
    SlidingSource,
    canonical_sliding_cursor,
)
from nio.ingest.source import (
    ClassicCursor,
    RoomSection,
    RoomSegment,
    SourceResultKind,
    SyncFrame,
    canonical_classic_cursor,
    canonical_json,
)
from nio.ingest.state import SourceState, StagedFrame
from nio.store import SqliteStore
from nio.store._sync_journal import SqliteIngestionJournal
from nio.store._sync_journal_codec import EncryptedRowCodec
from nio.store._sync_journal_port import IngestionJournal
from nio.store._sync_journal_rows import _canonical_internal, _frame_envelope
from nio.store._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
)
from nio.store.sync_journal import open_ingestion_store
from nio.store.sync_journal_schema import META_TABLE_SQL, SCHEMA_SQL, SCHEMA_VERSION

_LIMIT_DEFAULTS = (
    1 * 1024 * 1024,
    10_000,
    32 * 1024 * 1024,
    2_048,
    16 * 1024 * 1024,
    20_000,
    64 * 1024 * 1024,
)
_LIMIT_FIELDS = (
    "max_record_canonical_bytes",
    "max_held_records_per_room",
    "max_held_canonical_bytes_per_room",
    "max_ready_work_count",
    "max_ready_work_canonical_bytes",
    "max_total_work_count",
    "max_total_work_canonical_bytes",
)
_FRAME_ID = UUID("12345678-1234-5678-1234-567812345678")
_STREAM_ID = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")
_SOURCE_BODY_LIMIT = 16 * 1024 * 1024
_FRAME_ENVELOPE_LIMIT = 24 * 1024 * 1024
_SCHEMA_ACCOUNT_ID = "@schema:example.org"
_SCHEMA_FRAME_ID = "12345678-1234-5678-1234-567812345678"
_SQLITE_CONSTRAINT_MATCH = (
    r"CHECK constraint failed|NOT NULL constraint failed|"
    r"FOREIGN KEY constraint failed|UNIQUE constraint failed"
)

_TASK6_SCHEMA_OBJECT_ORDER = (
    "NioIngestSourceState",
    "NioIngestFrame",
    "NioIngestFrame_drain",
)
_TASK6_TABLE_NAMES = (
    "NioIngestFrame",
    "NioIngestMeta",
    "NioIngestSourceState",
)
_FRAME_COLUMNS = (
    ("account_id", "TEXT", True, 1),
    ("frame_id", "TEXT", True, 2),
    ("source_epoch", "INTEGER", True, 0),
    ("request_id", "INTEGER", True, 0),
    ("staged_revision", "INTEGER", True, 0),
    ("payload_ciphertext", "BLOB", True, 0),
    ("payload_sha256", "BLOB", True, 0),
    ("room_materialized_revision", "INTEGER", False, 0),
    ("drain_header_ciphertext", "BLOB", True, 0),
)
_TASK6_SCHEMA_OBJECTS = frozenset(
    {
        ("table", "NioIngestMeta"),
        ("table", "NioIngestSourceState"),
        ("table", "NioIngestFrame"),
        ("index", "NioIngestFrame_drain"),
    }
)

# Frozen pre-Task6 v1 physical schema.  This fixture intentionally does not
# derive from SCHEMA_SQL: a source-only v1 store must be refused rather than
# silently treated as a Task6-capable owner.
_LEGACY_V1_DDL = (
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
        typeof(payload_ciphertext) = 'blob' AND length(payload_ciphertext) >= 29
    ),
    payload_sha256 BLOB NOT NULL CHECK (
        typeof(payload_sha256) = 'blob' AND length(payload_sha256) = 32
    ),
    PRIMARY KEY (account_id, frame_id))""",
    """CREATE INDEX NioIngestFrame_drain ON NioIngestFrame(
    account_id, staged_revision, source_epoch, request_id, frame_id)""",
)


def _values() -> ModuleType:
    return importlib.import_module("nio.store._sync_journal_values")


def _assert_frozen_slotted(value_type: type, field_names: tuple[str, ...]) -> None:
    assert is_dataclass(value_type)
    assert value_type.__dataclass_params__.frozen
    assert value_type.__slots__ == field_names
    assert tuple(field.name for field in fields(value_type)) == field_names


def _normalized_ephemeral_envelopes(
    ephemeral_json: tuple[bytes, ...],
) -> tuple[tuple[str, bytes], ...]:
    helper = getattr(source, "_normalized_ephemeral_envelopes")
    return helper(ephemeral_json)


def _frame_room_ids(frame: SyncFrame) -> tuple[str, ...]:
    helper = getattr(source, "_frame_room_ids")
    return helper(frame)


def _frame(ephemeral_json: tuple[bytes, ...] = ()) -> SyncFrame:
    observation = MembershipObservation(
        "join", None, None, None, None, False, False, False, False
    )
    segments = tuple(
        RoomSegment(
            room_id,
            RoomSection.JOIN,
            (),
            (),
            (),
            False,
            None,
            False,
            False,
            0,
            observation,
        )
        for room_id in ("!segment-a:example.org", "!segment-b:example.org")
    )
    return SyncFrame(
        _FRAME_ID,
        RecordOrigin(TransportKind.CLASSIC, 1, 2, 0),
        b'{"next_batch":"s0"}',
        b'{"next_batch":"s1"}',
        b"s" * 32,
        (),
        b'{"changed":[],"left":[]}',
        b"{}",
        b"null",
        segments,
        ephemeral_json,
        (),
        (),
    )


def _ephemeral_envelope(room_id: str, event: dict[str, object]) -> bytes:
    return canonical_json({"event": event, "room_id": room_id})


def test_contract_private_journal_values_expose_exact_symbols() -> None:
    values = _values()
    for name in (
        "MaterializeStatus",
        "MaterializerLimits",
        "MaterializeResult",
    ):
        assert hasattr(values, name), name


def test_contract_private_value_types_are_exact_frozen_and_slotted() -> None:
    values = _values()

    assert issubclass(values.MaterializeStatus, StrEnum)
    assert tuple(values.MaterializeStatus) == (
        values.MaterializeStatus.IDLE,
        values.MaterializeStatus.AT_CAPACITY,
        values.MaterializeStatus.MATERIALIZED,
    )
    assert tuple(member.value for member in values.MaterializeStatus) == (
        "idle",
        "at_capacity",
        "materialized",
    )
    _assert_frozen_slotted(values.MaterializerLimits, _LIMIT_FIELDS)
    _assert_frozen_slotted(
        values.MaterializeResult,
        ("status", "frame_id", "revision"),
    )


def test_contract_materializer_limits_have_exact_defaults_ceilings_and_strict_types() -> (
    None
):
    values = _values()

    limits = values.MaterializerLimits()
    assert tuple(getattr(limits, name) for name in _LIMIT_FIELDS) == _LIMIT_DEFAULTS
    for field_name in _LIMIT_FIELDS:
        narrowed = values.MaterializerLimits(**{field_name: 1})
        assert tuple(getattr(narrowed, name) for name in _LIMIT_FIELDS) == tuple(
            1 if name == field_name else default
            for name, default in zip(_LIMIT_FIELDS, _LIMIT_DEFAULTS, strict=True)
        )
    with pytest.raises(FrozenInstanceError):
        limits.max_total_work_count = 1  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        limits.unknown = 1  # type: ignore[attr-defined]

    for index in range(len(_LIMIT_DEFAULTS)):
        arguments = list(_LIMIT_DEFAULTS)
        arguments[index] = True
        with pytest.raises(TypeError):
            values.MaterializerLimits(*arguments)
        arguments[index] = float(_LIMIT_DEFAULTS[index])
        with pytest.raises(TypeError):
            values.MaterializerLimits(*arguments)
        arguments[index] = 0
        with pytest.raises(ValueError, match=r".+"):
            values.MaterializerLimits(*arguments)
        arguments[index] = _LIMIT_DEFAULTS[index] + 1
        with pytest.raises(ValueError, match=r".+"):
            values.MaterializerLimits(*arguments)


def test_contract_materialize_result_enforces_exact_status_fates_and_types() -> None:
    values = _values()

    assert values.MaterializeResult(values.MaterializeStatus.IDLE, None, None) == (
        values.MaterializeResult(values.MaterializeStatus.IDLE, None, None)
    )
    assert (
        values.MaterializeResult(
            values.MaterializeStatus.AT_CAPACITY,
            _FRAME_ID,
            None,
        ).frame_id
        == _FRAME_ID
    )
    assert (
        values.MaterializeResult(
            values.MaterializeStatus.MATERIALIZED,
            _FRAME_ID,
            1,
        ).revision
        == 1
    )

    for status, frame_id, revision in (
        (values.MaterializeStatus.IDLE, _FRAME_ID, None),
        (values.MaterializeStatus.IDLE, None, 1),
        (values.MaterializeStatus.AT_CAPACITY, None, None),
        (values.MaterializeStatus.AT_CAPACITY, _FRAME_ID, 1),
        (values.MaterializeStatus.MATERIALIZED, None, 1),
        (values.MaterializeStatus.MATERIALIZED, _FRAME_ID, None),
    ):
        with pytest.raises(ValueError, match=r".+"):
            values.MaterializeResult(status, frame_id, revision)
    with pytest.raises(TypeError):
        values.MaterializeResult("idle", None, None)
    with pytest.raises(TypeError):
        values.MaterializeResult(values.MaterializeStatus.MATERIALIZED, "id", 1)
    for revision in (True, 1.0, 0, -1):
        with pytest.raises((TypeError, ValueError)):
            values.MaterializeResult(
                values.MaterializeStatus.MATERIALIZED,
                _FRAME_ID,
                revision,
            )


def test_contract_private_materializer_port_signature_and_no_public_exports() -> None:
    for package in (ingest, store):
        for name in (
            "MaterializeStatus",
            "MaterializerLimits",
            "MaterializeResult",
            "materialize_oldest_frame",
            "peek_ready_work",
            "read_ready_work",
            "acknowledge",
        ):
            assert not hasattr(package, name), name

    protocol_public_methods = {
        name
        for name, value in vars(IngestionJournal).items()
        if not name.startswith("_") and callable(value)
    }
    assert protocol_public_methods == {
        "load_owner",
        "load_source",
        "load_frame",
        "list_frames",
        "stage_source_response",
        "materialize_oldest_frame",
    }
    forbidden_concrete_methods = (
        "peek_ready_work",
        "read_ready_work",
        "consume_ready_work",
        "consume_work",
        "create_batch",
        "list_batches",
        "load_batch",
        "acknowledge",
        "acknowledge_batch",
        "acknowledge_work",
    )
    assert not {
        name
        for name in forbidden_concrete_methods
        if hasattr(SqliteIngestionJournal, name)
    }

    values = _values()
    for journal_type in (IngestionJournal, SqliteIngestionJournal):
        method = getattr(journal_type, "materialize_oldest_frame")
        parameters = tuple(inspect.signature(method).parameters.values())
        assert tuple(parameter.name for parameter in parameters) == (
            "self",
            "expected_revision",
            "writer_epoch",
            "limits",
        )
        assert parameters[0].kind is inspect.Parameter.POSITIONAL_OR_KEYWORD
        assert all(
            parameter.kind is inspect.Parameter.KEYWORD_ONLY
            for parameter in parameters[1:]
        )
        assert all(
            parameter.default is inspect.Parameter.empty for parameter in parameters
        )
        assert get_type_hints(method) == {
            "expected_revision": int,
            "writer_epoch": UUID,
            "limits": values.MaterializerLimits,
            "return": values.MaterializeResult,
        }


def test_source_discovery_normalized_ephemeral_envelopes_are_canonical_ordered_pairs() -> (
    None
):
    first_event = {"content": {"user_ids": ["@a:example.org"]}, "type": "m.typing"}
    second_event = {
        "content": {"$event": {"m.read": "@b:example.org"}},
        "type": "m.receipt",
    }
    payloads = (
        _ephemeral_envelope("!segment-b:example.org", first_event),
        _ephemeral_envelope("!ephemeral:example.org", second_event),
        _ephemeral_envelope("!ephemeral:example.org", first_event),
    )

    assert _normalized_ephemeral_envelopes(payloads) == (
        ("!segment-b:example.org", canonical_json(first_event)),
        ("!ephemeral:example.org", canonical_json(second_event)),
        ("!ephemeral:example.org", canonical_json(first_event)),
    )


@pytest.mark.parametrize(
    "payload",
    [
        b'{"room_id":"!ephemeral:example.org"}',
        b'{"event":{"type":"m.typing"},"room_id":"!ephemeral:example.org","extra":0}',
        b'{"room_id":"!ephemeral:example.org","event":{"type":"m.typing"}}',
        b'{"event":[],"room_id":"!ephemeral:example.org"}',
        b'{"event":{"type":"m.typing"},"room_id":""}',
        b"{",
    ],
)
def test_source_discovery_normalized_ephemeral_envelopes_reject_invalid_canonical_envelopes(
    payload: bytes,
) -> None:
    with pytest.raises(ValueError, match=r".+"):
        _normalized_ephemeral_envelopes((payload,))


def test_source_discovery_frame_room_ids_keep_segment_then_ephemeral_order() -> None:
    first_event = {"type": "m.typing"}
    second_event = {"type": "m.receipt"}
    frame = _frame(
        (
            _ephemeral_envelope("!segment-b:example.org", first_event),
            _ephemeral_envelope("!ephemeral-c:example.org", second_event),
            _ephemeral_envelope("!segment-a:example.org", second_event),
            _ephemeral_envelope("!ephemeral-c:example.org", first_event),
            _ephemeral_envelope("!ephemeral-d:example.org", first_event),
        )
    )

    assert _frame_room_ids(frame) == (
        "!segment-a:example.org",
        "!segment-b:example.org",
        "!ephemeral-c:example.org",
        "!ephemeral-d:example.org",
    )


def test_source_discovery_reducer_ephemeral_descriptors_share_extractor_order() -> None:
    first_event = {"content": {"x": 1}, "type": "m.typing"}
    second_event = {"content": {"x": 2}, "type": "m.receipt"}
    payloads = (
        _ephemeral_envelope("!ephemeral-b:example.org", first_event),
        _ephemeral_envelope("!ephemeral-a:example.org", second_event),
        _ephemeral_envelope("!ephemeral-b:example.org", second_event),
    )
    frame = _frame(payloads)
    continuities = tuple(
        RoomContinuity(room_id, 0, None, None, None, None)
        for room_id in ("!ephemeral-b:example.org", "!ephemeral-a:example.org")
    )

    proposal = reduce_staged_frame(_STREAM_ID, frame.frame_id, frame, continuities)
    descriptors = tuple(
        (descriptor.room_id, descriptor.source_json)
        for descriptor in proposal.descriptors
        if descriptor.kind is RecordKind.EPHEMERAL
    )
    assert descriptors == (
        (
            "!ephemeral-b:example.org",
            b'{"content":{"x":1},"type":"m.typing"}',
        ),
        (
            "!ephemeral-a:example.org",
            b'{"content":{"x":2},"type":"m.receipt"}',
        ),
        (
            "!ephemeral-b:example.org",
            b'{"content":{"x":2},"type":"m.receipt"}',
        ),
    )


def test_discovery_malformed_ephemeral_envelope_fails_source_and_reducer() -> None:
    frame = _frame((b'{"event":[],"room_id":"!ephemeral:example.org"}',))

    with pytest.raises(ValueError, match=r".+"):
        _normalized_ephemeral_envelopes(frame.ephemeral_json)
    with pytest.raises(ValueError, match=r".+"):
        _frame_room_ids(frame)
    with pytest.raises(ReducerInputError):
        reduce_staged_frame(_STREAM_ID, frame.frame_id, frame, ())


def _oversized_classic_body() -> bytes:
    return b'{"next_batch":"' + (b"x" * (_SOURCE_BODY_LIMIT + 1)) + b'","rooms":{}}'


def _classic_oversized_response_fixture() -> (
    tuple[ClassicSource, NetworkRequest, bytes]
):
    adapter = ClassicSource(
        _STREAM_ID,
        ClassicSourceConfig(30_000, b"{}"),
        "@me:example.org",
    )
    request = adapter.plan_request(
        SourceState(
            1,
            TransportKind.CLASSIC,
            canonical_classic_cursor(ClassicCursor(None)),
            2,
            True,
        ),
        2,
    )
    assert request is not None
    return adapter, request, _oversized_classic_body()


def _sliding_oversized_response_fixture() -> (
    tuple[SlidingSource, NetworkRequest, bytes]
):
    cursor = SlidingCursor(
        None,
        None,
        UUID("236f12d0-c282-4594-8654-948a60a73ee9"),
        "worker",
        1,
        2,
        SlidingRangeAckMode.UNKNOWN,
        False,
    )
    adapter = SlidingSource(
        _STREAM_ID,
        SlidingSourceConfig(30_000, "worker", b"{}", b"{}", b"{}", 2),
        "@me:example.org",
    )
    request = adapter.plan_request(
        SourceState(
            1,
            TransportKind.SLIDING,
            canonical_sliding_cursor(cursor),
            2,
            True,
        ),
        2,
    )
    assert request is not None
    assert request.body is not None
    txn_id = json.loads(request.body)["txn_id"]
    assert type(txn_id) is str
    body = (
        b'{"lists":{"__nio_all_rooms_v1":{"count":0}},"pos":"'
        + (b"x" * (_SOURCE_BODY_LIMIT + 1))
        + b'","txn_id":"'
        + txn_id.encode("ascii")
        + b'"}'
    )
    return adapter, request, body


@pytest.mark.parametrize(
    ("fixture", "adapter_module"),
    [
        (_classic_oversized_response_fixture, classic_module),
        (_sliding_oversized_response_fixture, sliding_module),
    ],
    ids=("classic", "sliding"),
)
def test_contract_oversized_response_body_bound_is_terminal_before_response_parse(
    monkeypatch: pytest.MonkeyPatch,
    fixture,
    adapter_module,
) -> None:
    adapter, request, body = fixture()
    assert len(body) > _SOURCE_BODY_LIMIT

    def reject_any_json_parse(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized successful response parsed JSON")

    monkeypatch.setattr(adapter_module, "load_json", reject_any_json_parse)
    monkeypatch.setattr(source, "load_json", reject_any_json_parse)
    result = adapter.normalize(
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

    assert result.kind is SourceResultKind.TERMINAL_ERROR
    assert result.frame is None
    assert result.request is request
    assert result.status_code == 200
    assert result.response_body == body
    assert result.network_failure is None
    assert result.error_code is None
    assert result.retry_after_ms is None
    assert result.detail == "staged source response body exceeds 16 MiB"


@pytest.mark.parametrize(
    ("fixture", "adapter_module"),
    [
        (_classic_oversized_response_fixture, classic_module),
        (_sliding_oversized_response_fixture, sliding_module),
    ],
    ids=["classic", "sliding"],
)
def test_contract_normalize_frame_rejects_oversized_body_before_parse(
    monkeypatch: pytest.MonkeyPatch,
    fixture,
    adapter_module,
) -> None:
    adapter, request, body = fixture()
    original_load_json = adapter_module.load_json

    def reject_oversized_response_parse(data: bytes, field_name: str):
        if data == body and field_name in {"sync response", "sliding sync response"}:
            raise AssertionError("oversized response body was parsed")
        return original_load_json(data, field_name)

    monkeypatch.setattr(adapter_module, "load_json", reject_oversized_response_parse)
    with pytest.raises(
        ValueError,
        match="^staged source response body exceeds 16 MiB$",
    ):
        adapter._normalize_frame(request, body)


def test_contract_source_bound_constants_are_exact_and_not_public_exports() -> None:
    assert source.MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES == _SOURCE_BODY_LIMIT
    assert source.MAX_ENCRYPTED_STAGED_FRAME_ENVELOPE_BYTES == 24 * 1024 * 1024
    for package in (ingest, store):
        assert not hasattr(package, "MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES")
        assert not hasattr(package, "MAX_ENCRYPTED_STAGED_FRAME_ENVELOPE_BYTES")


def test_contract_source_bound_staged_response_rejects_before_hash_or_json_parse(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    _, request, body = _classic_oversized_response_fixture()

    def explode(*_args: object, **_kwargs: object) -> object:
        raise AssertionError("oversized staged response was inspected")

    monkeypatch.setattr(ports_module.hashlib, "sha256", explode)
    monkeypatch.setattr(ports_module, "load_json", explode)
    with pytest.raises(ValueError, match="16 MiB"):
        StagedSourceResponse(request, body, b"d" * 32)


def test_contract_envelope_bound_rejects_large_request_metadata_before_transaction(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    transitions: list[str] = []
    journal = SqliteIngestionJournal.open(
        tmp_path / "envelope-bound.db",
        account_id="@me:example.org",
        device_id="DEVICE",
        source=ClassicSourceConfig(30_000, b"{}"),
        statement_observer=statements.append,
        transition_statement_hook=transitions.append,
    )
    try:
        owner_before = journal.load_owner()
        source_before = journal.load_source()
        adapter = ClassicSource(
            owner_before.stream_id,
            ClassicSourceConfig(30_000, b"{}"),
            "@me:example.org",
        )
        request = adapter.plan_request(source_before, source_before.next_request_id)
        assert request is not None
        response_body = b'{"next_batch":"s1","rooms":{}}'
        normalized = adapter.normalize(
            request,
            NetworkResult(
                request.stream_id,
                request.transport,
                request.source_epoch,
                request.request_id,
                200,
                response_body,
                None,
                None,
            ),
        )
        assert normalized.frame is not None
        assert len(normalized.response_body) <= _SOURCE_BODY_LIMIT

        oversized_request = replace(
            request,
            request_cursor_json=b"x" * (18 * 1024 * 1024),
        )
        staged_response = StagedSourceResponse(
            oversized_request,
            normalized.response_body,
            hashlib.sha256(normalized.response_body).digest(),
        )
        frame = StagedFrame(
            _frame_id_for_response(oversized_request, staged_response.source_sha256),
            staged_response,
        )
        proposed_source = SourceState(
            source_before.source_epoch,
            TransportKind.CLASSIC,
            canonical_classic_cursor(ClassicCursor("s1")),
            source_before.next_request_id + 1,
            source_before.active,
        )
        assert len(_canonical_internal(_frame_envelope(frame))) + 29 > 24 * 1024 * 1024

        transaction_entries: list[None] = []

        class RejectTransaction:
            def __enter__(self) -> None:
                transaction_entries.append(None)
                raise AssertionError("envelope gate entered journal transaction")

            def __exit__(
                self,
                _error_type: object,
                _error: object,
                _traceback: object,
            ) -> bool:
                return False

        def reject_transaction() -> RejectTransaction:
            return RejectTransaction()

        def reject_sql(*_args: object, **_kwargs: object) -> object:
            raise AssertionError("envelope gate issued SQL")

        def record_transition(label: str) -> None:
            transitions.append(label)

        statements.clear()
        transitions.clear()
        with monkeypatch.context() as guard:
            guard.setattr(journal, "_transaction", reject_transaction)
            guard.setattr(journal, "_execute", reject_sql)
            guard.setattr(journal, "_transition_hook", record_transition)
            with pytest.raises(
                JournalIntegrityError,
                match="^staged frame envelope exceeds 24 MiB$",
            ):
                journal.stage_source_response(
                    expected_revision=owner_before.revision,
                    writer_epoch=owner_before.writer_epoch,
                    source=proposed_source,
                    frame=frame,
                )
            assert transaction_entries == []
            assert statements == []
            assert transitions == []
        assert journal.load_owner() == owner_before
        assert journal.load_source() == source_before
        assert journal.list_frames(1) == ()
    finally:
        journal.close()


def test_contract_staged_frame_drain_header_proof_is_authenticated_and_readable(
    tmp_path: Path,
) -> None:
    journal = SqliteIngestionJournal.open(
        tmp_path / "drain-header-proof.db",
        account_id="@proof:example.org",
        device_id="DEVICE",
        source=ClassicSourceConfig(30_000, b"{}"),
        pickle_key="proof-secret",
    )
    try:
        owner_before = journal.load_owner()
        source_before = journal.load_source()
        adapter = ClassicSource(
            owner_before.stream_id,
            ClassicSourceConfig(30_000, b"{}"),
            "@proof:example.org",
        )
        request = adapter.plan_request(source_before, source_before.next_request_id)
        assert request is not None
        response_body = b'{"next_batch":"s1","rooms":{}}'
        normalized = adapter.normalize(
            request,
            NetworkResult(
                request.stream_id,
                request.transport,
                request.source_epoch,
                request.request_id,
                200,
                response_body,
                None,
                None,
            ),
        )
        assert normalized.frame is not None
        staged_response = StagedSourceResponse(
            request,
            normalized.response_body,
            hashlib.sha256(normalized.response_body).digest(),
        )
        staged_frame = StagedFrame(normalized.frame.frame_id, staged_response)
        successor = SourceState(
            source_before.source_epoch,
            source_before.transport_kind,
            normalized.frame.candidate_cursor_json,
            source_before.next_request_id + 1,
            source_before.active,
        )
        committed = journal.stage_source_response(
            expected_revision=owner_before.revision,
            writer_epoch=owner_before.writer_epoch,
            source=successor,
            frame=staged_frame,
        )

        with journal._read():
            row = journal._execute(
                "SELECT frame_id, source_epoch, request_id, staged_revision, "
                "payload_ciphertext, payload_sha256, "
                "room_materialized_revision, drain_header_ciphertext "
                "FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
                (journal.account_id, str(staged_frame.frame_id)),
            ).fetchone()
        assert row is not None
        assert row["frame_id"] == str(staged_frame.frame_id)
        assert row["source_epoch"] == request.source_epoch
        assert row["request_id"] == request.request_id
        assert row["staged_revision"] == committed.revision
        assert row["room_materialized_revision"] is None
        assert type(row["source_epoch"]) is int
        assert type(row["request_id"]) is int
        assert type(row["staged_revision"]) is int

        payload_ciphertext = bytes(row["payload_ciphertext"])
        payload_sha256 = bytes(row["payload_sha256"])
        drain_header_ciphertext = bytes(row["drain_header_ciphertext"])
        header = _canonical_internal(
            [
                row["source_epoch"],
                row["request_id"],
                row["staged_revision"],
                base64.b64encode(payload_sha256).decode("ascii"),
                len(payload_ciphertext),
                None,
            ]
        )
        empty_sha256 = hashlib.sha256(b"").digest()
        assert len(drain_header_ciphertext) == 29
        assert (
            EncryptedRowCodec(
                "proof-secret",
                journal.account_id,
                owner_before.stream_id,
            ).decrypt(
                "NioIngestFrameDrainHeader",
                (staged_frame.frame_id,),
                drain_header_ciphertext,
                empty_sha256,
                header=header,
            )
            == b""
        )

        loaded = journal.load_frame(staged_frame.frame_id)
        assert loaded is not None
        assert loaded.response == staged_frame.response
        assert loaded.staged_revision == committed.revision

        with journal._transaction():
            updated = journal._execute(
                "UPDATE NioIngestFrame SET room_materialized_revision = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (committed.revision, journal.account_id, str(staged_frame.frame_id)),
            )
            assert updated.rowcount == 1
        with pytest.raises(JournalIntegrityError):
            journal.load_frame(staged_frame.frame_id)

        forged_proof = drain_header_ciphertext[:-1] + bytes(
            (drain_header_ciphertext[-1] ^ 1,)
        )
        with journal._transaction():
            updated = journal._execute(
                "UPDATE NioIngestFrame SET room_materialized_revision = NULL, "
                "drain_header_ciphertext = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (forged_proof, journal.account_id, str(staged_frame.frame_id)),
            )
            assert updated.rowcount == 1
        with pytest.raises(JournalIntegrityError):
            journal.load_frame(staged_frame.frame_id)
    finally:
        journal.close()


def _table_columns(
    connection: sqlite3.Connection,
    table_name: str,
) -> tuple[tuple[str, str, bool, int], ...]:
    return tuple(
        (str(row[1]), str(row[2]), bool(row[3]), int(row[5]))
        for row in connection.execute(f"PRAGMA table_info('{table_name}')")
    )


def _foreign_keys(
    connection: sqlite3.Connection,
    table_name: str,
) -> tuple[tuple[str, str, str], ...]:
    return tuple(
        (str(row[2]), str(row[3]), str(row[4]))
        for row in connection.execute(f"PRAGMA foreign_key_list('{table_name}')")
    )


def _index_columns(
    connection: sqlite3.Connection,
    index_name: str,
) -> tuple[str, ...]:
    return tuple(
        str(row[2]) for row in connection.execute(f"PRAGMA index_info('{index_name}')")
    )


def _open_task6_schema() -> sqlite3.Connection:
    connection = sqlite3.connect(":memory:")
    try:
        connection.execute("PRAGMA foreign_keys = ON")
        connection.execute(META_TABLE_SQL)
        for statement in SCHEMA_SQL:
            connection.execute(statement)

        table_names = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master " "WHERE type = 'table' ORDER BY name"
            )
        )
        assert table_names == _TASK6_TABLE_NAMES
        connection.execute(
            """INSERT INTO NioIngestMeta(
                account_id, device_id, schema_version, stream_id, transport_kind,
                revision, writer_epoch, next_source_epoch, created_at_ns
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?)""",
            (
                _SCHEMA_ACCOUNT_ID,
                "DEVICE",
                SCHEMA_VERSION,
                str(_STREAM_ID),
                "classic",
                0,
                str(_FRAME_ID),
                1,
                0,
            ),
        )
    except BaseException:
        connection.close()
        raise
    return connection


def _insert_frame(
    connection: sqlite3.Connection,
    **overrides: object,
) -> None:
    row: dict[str, object] = {
        "account_id": _SCHEMA_ACCOUNT_ID,
        "frame_id": _SCHEMA_FRAME_ID,
        "source_epoch": 0,
        "request_id": 0,
        "staged_revision": 1,
        "payload_ciphertext": b"p" * 29,
        "payload_sha256": b"d" * 32,
        "room_materialized_revision": None,
        "drain_header_ciphertext": b"h" * 29,
    }
    row.update(overrides)
    column_names = tuple(row)
    connection.execute(
        "INSERT INTO NioIngestFrame ("
        + ", ".join(column_names)
        + ") VALUES ("
        + ", ".join("?" for _ in column_names)
        + ")",
        tuple(row[column_name] for column_name in column_names),
    )


def _insert_frame_zeroblob(
    connection: sqlite3.Connection,
    *,
    frame_id: str,
    ciphertext_bytes: int,
) -> None:
    connection.execute(
        """INSERT INTO NioIngestFrame(
            account_id, frame_id, source_epoch, request_id, staged_revision,
            payload_ciphertext, payload_sha256, room_materialized_revision,
            drain_header_ciphertext
        ) VALUES (?, ?, ?, ?, ?, zeroblob(?), ?, NULL, ?)""",
        (
            _SCHEMA_ACCOUNT_ID,
            frame_id,
            0,
            0,
            1,
            ciphertext_bytes,
            b"d" * 32,
            b"h" * 29,
        ),
    )


def test_contract_task6_schema_topology_is_exact() -> None:
    connection = _open_task6_schema()
    try:
        assert (
            frozenset(
                (str(row[0]), str(row[1]))
                for row in connection.execute(
                    "SELECT type, name FROM sqlite_master "
                    "WHERE type IN ('table', 'index') AND name GLOB 'NioIngest*'"
                )
            )
            == _TASK6_SCHEMA_OBJECTS
        )
        assert _table_columns(connection, "NioIngestFrame") == _FRAME_COLUMNS
        assert _foreign_keys(connection, "NioIngestFrame") == (
            ("NioIngestMeta", "account_id", "account_id"),
        )
        assert _index_columns(connection, "NioIngestFrame_drain") == (
            "account_id",
            "staged_revision",
            "source_epoch",
            "request_id",
            "frame_id",
        )
    finally:
        connection.close()


def _assert_frame_rejected(overrides: dict[str, object]) -> None:
    connection = _open_task6_schema()
    try:
        with pytest.raises(sqlite3.IntegrityError, match=_SQLITE_CONSTRAINT_MATCH):
            _insert_frame(connection, **overrides)
    finally:
        connection.close()


@pytest.mark.parametrize(
    "overrides",
    [
        {"account_id": None},
        {"account_id": ""},
        {"account_id": b"account"},
        {"account_id": "@missing:example.org"},
        {"frame_id": None},
        {"frame_id": ""},
        {"frame_id": b"frame"},
        {"source_epoch": None},
        {"source_epoch": -1},
        {"source_epoch": 0.5},
        {"request_id": None},
        {"request_id": -1},
        {"request_id": 0.5},
        {"staged_revision": None},
        {"staged_revision": 0},
        {"staged_revision": 1.5},
        {"payload_ciphertext": None},
        {"payload_ciphertext": "not-a-blob"},
        {"payload_ciphertext": b"p" * 28},
        {"payload_sha256": None},
        {"payload_sha256": "not-a-blob"},
        {"payload_sha256": b"d" * 31},
        {"payload_sha256": b"d" * 33},
        {"room_materialized_revision": 0},
        {"room_materialized_revision": -1},
        {"room_materialized_revision": 1.5},
        {"room_materialized_revision": "not-an-integer"},
        {"drain_header_ciphertext": None},
        {"drain_header_ciphertext": "not-a-blob"},
        {"drain_header_ciphertext": b"h" * 28},
        {"drain_header_ciphertext": b"h" * 30},
    ],
    ids=[
        "account-null",
        "account-empty",
        "account-blob",
        "account-foreign",
        "frame-id-null",
        "frame-id-empty",
        "frame-id-blob",
        "source-epoch-null",
        "source-epoch-negative",
        "source-epoch-real",
        "request-id-null",
        "request-id-negative",
        "request-id-real",
        "staged-revision-null",
        "staged-revision-zero",
        "staged-revision-real",
        "payload-null",
        "payload-text",
        "payload-short",
        "payload-digest-null",
        "payload-digest-text",
        "payload-digest-short",
        "payload-digest-long",
        "room-materialized-revision-zero",
        "room-materialized-revision-negative",
        "room-materialized-revision-real",
        "room-materialized-revision-text",
        "drain-proof-null",
        "drain-proof-text",
        "drain-proof-short",
        "drain-proof-long",
    ],
)
def test_contract_task6_ddl_frame_column_constraints_are_isolated(
    overrides: dict[str, object],
) -> None:
    _assert_frame_rejected(overrides)


@pytest.mark.parametrize("revision", [None, 1, 2**63 - 1])
def test_contract_task6_ddl_frame_accepts_nullable_positive_materialized_revision(
    revision: int | None,
) -> None:
    connection = _open_task6_schema()
    try:
        _insert_frame(connection, room_materialized_revision=revision)
        stored = connection.execute(
            "SELECT room_materialized_revision FROM NioIngestFrame"
        ).fetchone()
        assert stored == (revision,)
    finally:
        connection.close()


def test_contract_task6_ddl_frame_ciphertext_ceiling_is_isolated() -> None:
    connection = _open_task6_schema()
    try:
        _insert_frame_zeroblob(
            connection,
            frame_id="frame-maximum",
            ciphertext_bytes=_FRAME_ENVELOPE_LIMIT,
        )
        with pytest.raises(sqlite3.IntegrityError, match=_SQLITE_CONSTRAINT_MATCH):
            _insert_frame_zeroblob(
                connection,
                frame_id="frame-over-maximum",
                ciphertext_bytes=_FRAME_ENVELOPE_LIMIT + 1,
            )
    finally:
        connection.close()


def _open_task6_bootstrap(
    store_path: Path,
    *,
    database_name: str = "task6-preflight.db",
    statements: list[str] | None = None,
    schema_statements: list[str] | None = None,
):
    return open_ingestion_store(
        store_path,
        account_id=_SCHEMA_ACCOUNT_ID,
        device_id="DEVICE",
        source=ClassicSourceConfig(30_000, b"{}"),
        pickle_key="secret",
        database_name=database_name,
        statement_observer=statements.append if statements is not None else None,
        schema_statement_hook=(
            schema_statements.append if schema_statements is not None else None
        ),
    )


def _ingestion_schema_objects(
    database_path: Path,
) -> frozenset[tuple[str, str]]:
    with sqlite3.connect(database_path) as connection:
        return frozenset(
            (str(row[0]), str(row[1]))
            for row in connection.execute(
                "SELECT type, name FROM sqlite_master "
                "WHERE type IN ('table', 'index') "
                "AND name GLOB 'NioIngest*'"
            )
        )


def _all_schema_objects(
    database_path: Path,
) -> tuple[tuple[str, str, str], ...]:
    with sqlite3.connect(database_path) as connection:
        return tuple(
            (str(row[0]), str(row[1]), str(row[2] or ""))
            for row in connection.execute(
                "SELECT type, name, sql FROM sqlite_master ORDER BY type, name"
            )
        )


def _sqlite_artifacts(database_path: Path) -> dict[str, bytes]:
    paths = (
        database_path,
        database_path.with_name(database_path.name + "-wal"),
        database_path.with_name(database_path.name + "-shm"),
    )
    return {path.name: path.read_bytes() for path in paths if path.exists()}


def _assert_persisted_task6_topology(database_path: Path) -> None:
    assert _ingestion_schema_objects(database_path) == _TASK6_SCHEMA_OBJECTS
    with sqlite3.connect(database_path) as connection:
        assert _table_columns(connection, "NioIngestFrame") == _FRAME_COLUMNS
        assert _index_columns(connection, "NioIngestFrame_drain") == (
            "account_id",
            "staged_revision",
            "source_epoch",
            "request_id",
            "frame_id",
        )


def _create_pre_task6_source_only_store(database_path: Path) -> tuple[int, str]:
    source = SourceState(
        0,
        TransportKind.CLASSIC,
        canonical_classic_cursor(ClassicCursor(None)),
        0,
        True,
    )
    cursor_ciphertext, cursor_sha256 = EncryptedRowCodec(
        "secret",
        _SCHEMA_ACCOUNT_ID,
        _STREAM_ID,
    ).seal(
        "NioIngestSourceState",
        (_SCHEMA_ACCOUNT_ID,),
        source.cursor_json,
        header=b'["classic",0,0,true]',
    )
    revision = 7
    writer_epoch = str(_FRAME_ID)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA foreign_keys = ON")
        for statement in _LEGACY_V1_DDL:
            connection.execute(statement)
        connection.execute(
            """INSERT INTO NioIngestMeta(
                account_id, device_id, schema_version, stream_id, transport_kind,
                revision, writer_epoch, next_source_epoch, created_at_ns
            ) VALUES (?, ?, 1, ?, 'classic', ?, ?, 1, 0)""",
            (_SCHEMA_ACCOUNT_ID, "DEVICE", str(_STREAM_ID), revision, writer_epoch),
        )
        connection.execute(
            """INSERT INTO NioIngestSourceState(
                account_id, source_epoch, cursor_ciphertext, cursor_sha256,
                next_request_id, active
            ) VALUES (?, 0, ?, ?, 0, 1)""",
            (_SCHEMA_ACCOUNT_ID, cursor_ciphertext, cursor_sha256),
        )
    return revision, writer_epoch


def _assert_read_only_preflight(statements: list[str]) -> None:
    forbidden_operations = ("CREATE", "ALTER", "DROP", "INSERT", "UPDATE", "DELETE")
    assert not [
        statement
        for statement in statements
        if statement.lstrip().upper().startswith(forbidden_operations)
    ]
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
        statement
        for statement in statements
        if statement.lstrip().upper().startswith("PRAGMA")
        and (
            "=" in statement or any(name in statement.upper() for name in write_pragmas)
        )
    ]


def test_preflight_fresh_open_creates_exact_task6_topology(tmp_path: Path) -> None:
    schema_statements: list[str] = []
    bootstrap = _open_task6_bootstrap(tmp_path, schema_statements=schema_statements)
    try:
        assert bootstrap.schema_version == SCHEMA_VERSION
        _assert_persisted_task6_topology(bootstrap.database_path)
        assert schema_statements == [
            "create_meta",
            "insert_meta",
            *(f"schema_{index}" for index in range(len(_TASK6_SCHEMA_OBJECT_ORDER))),
            "insert_source",
        ]
    finally:
        bootstrap.close()


def test_preflight_exact_same_owner_reopen_preserves_task6_topology(
    tmp_path: Path,
) -> None:
    first = _open_task6_bootstrap(tmp_path)
    owner_before = first._journal.load_owner()
    source_before = first._journal.load_source()
    database_path = first.database_path
    first.close()

    statements: list[str] = []
    reopened = _open_task6_bootstrap(tmp_path, statements=statements)
    try:
        owner_after = reopened._journal.load_owner()
        assert owner_after == replace(
            owner_before,
            writer_epoch=owner_after.writer_epoch,
        )
        assert owner_after.writer_epoch != owner_before.writer_epoch
        assert reopened._journal.load_source() == source_before
        _assert_persisted_task6_topology(database_path)
        assert not [
            statement for statement in statements if "CREATE " in statement.upper()
        ]
    finally:
        reopened.close()


def test_preflight_rejects_valid_pre_task6_source_only_store_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "pre-task6.db"
    revision_before, writer_epoch_before = _create_pre_task6_source_only_store(
        database_path
    )
    assert _ingestion_schema_objects(database_path) == frozenset(
        {
            ("table", "NioIngestMeta"),
            ("table", "NioIngestSourceState"),
            ("table", "NioIngestFrame"),
            ("index", "NioIngestFrame_drain"),
        }
    )
    bytes_before = _sqlite_artifacts(database_path)
    schema_before = _all_schema_objects(database_path)
    e2ee_constructions: list[tuple[tuple[object, ...], dict[str, object]]] = []
    statements: list[str] = []

    def reject_e2ee_construction(
        _self: object,
        *args: object,
        **kwargs: object,
    ) -> None:
        e2ee_constructions.append((args, kwargs))
        raise AssertionError("legacy preflight constructed an E2EE store")

    monkeypatch.setattr(SqliteStore, "__init__", reject_e2ee_construction)
    opened = None
    error: BaseException | None = None
    try:
        opened = _open_task6_bootstrap(
            tmp_path,
            database_name=database_path.name,
            statements=statements,
        )
    except BaseException as caught:  # noqa: BLE001 - exact type below is contractual
        error = caught
    finally:
        if opened is not None:
            opened.close()

    with sqlite3.connect(database_path) as connection:
        revision_after, writer_epoch_after = connection.execute(
            "SELECT revision, writer_epoch FROM NioIngestMeta"
        ).fetchone()
    assert type(error) is FreshIngestionRequired
    assert e2ee_constructions == []
    _assert_read_only_preflight(statements)
    assert (revision_after, writer_epoch_after) == (
        revision_before,
        writer_epoch_before,
    )
    assert _sqlite_artifacts(database_path) == bytes_before
    assert _all_schema_objects(database_path) == schema_before


_DISCOVERY_ACCOUNT_ID = "@discovery:example.org"
_DISCOVERY_DEVICE_ID = "DISCOVERY"
_DISCOVERY_CLASSIC = ClassicSourceConfig(30_000, b"{}")
_DISCOVERY_SLIDING = SlidingSourceConfig(
    30_000,
    "discovery",
    b"{}",
    b"{}",
    b"{}",
    2,
)


def _discovery_config(
    transport: TransportKind,
) -> ClassicSourceConfig | SlidingSourceConfig:
    if transport is TransportKind.CLASSIC:
        return _DISCOVERY_CLASSIC
    return _DISCOVERY_SLIDING


def _discovery_adapter(
    stream_id: UUID,
    transport: TransportKind,
) -> ClassicSource | SlidingSource:
    config = _discovery_config(transport)
    if transport is TransportKind.CLASSIC:
        assert type(config) is ClassicSourceConfig
        return ClassicSource(stream_id, config, _DISCOVERY_ACCOUNT_ID)
    assert type(config) is SlidingSourceConfig
    return SlidingSource(stream_id, config, _DISCOVERY_ACCOUNT_ID)


def _discovery_body(
    request: NetworkRequest,
    sequence: int,
    *,
    crypto: bool = False,
    nonempty: bool = False,
) -> bytes:
    if request.transport is TransportKind.CLASSIC:
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
        if nonempty:
            body["account_data"] = {
                "events": [{"content": {"enabled": True}, "type": "m.push_rules"}]
            }
        return canonical_json(body)

    assert request.body is not None
    request_body = json.loads(request.body)
    body = {
        "lists": {RESERVED_ALL_ROOMS_LIST: {"count": 0}},
        "pos": f"p{sequence}",
        "txn_id": request_body["txn_id"],
    }
    extensions: dict[str, object] = {}
    if crypto:
        extensions["to_device"] = {
            "events": [
                {
                    "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                    "type": "m.room_key",
                }
            ],
            "next_batch": f"td{sequence}",
        }
    if nonempty:
        extensions["account_data"] = {
            "global": [{"content": {"enabled": True}, "type": "m.push_rules"}]
        }
    if extensions:
        body["extensions"] = extensions
    return canonical_json(body)


def _open_discovery_journal(
    store_path: Path,
    transport: TransportKind,
    *,
    statements: list[str] | None = None,
):
    return open_ingestion_store(
        store_path,
        account_id=_DISCOVERY_ACCOUNT_ID,
        device_id=_DISCOVERY_DEVICE_ID,
        source=_discovery_config(transport),
        pickle_key="discovery-secret",
        database_name="discovery.db",
        statement_observer=statements.append if statements is not None else None,
    )


def _stage_discovery_frame(
    journal: SqliteIngestionJournal,
    transport: TransportKind,
    sequence: int,
    *,
    crypto: bool = False,
    nonempty: bool = False,
) -> tuple[StagedFrame, SyncFrame]:
    owner = journal.load_owner()
    prior = journal.load_source()
    adapter = _discovery_adapter(owner.stream_id, transport)
    request = adapter.plan_request(prior, prior.next_request_id)
    assert request is not None
    body = _discovery_body(
        request,
        sequence,
        crypto=crypto,
        nonempty=nonempty,
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
    staged = StagedFrame(
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
    committed = journal.stage_source_response(
        expected_revision=owner.revision,
        writer_epoch=owner.writer_epoch,
        source=successor,
        frame=staged,
    )
    return replace(staged, staged_revision=committed.revision), normalized.frame


def _frame_storage_row(
    journal: SqliteIngestionJournal,
    frame_id: UUID,
) -> tuple[object, ...] | None:
    with journal._owner.read():
        row = journal._execute(
            "SELECT frame_id, source_epoch, request_id, staged_revision, "
            "payload_ciphertext, payload_sha256, room_materialized_revision, "
            "drain_header_ciphertext FROM NioIngestFrame "
            "WHERE account_id = ? AND frame_id = ?",
            (journal.account_id, str(frame_id)),
        ).fetchone()
    return None if row is None else tuple(row)


def _canonical_expected_drain_header(
    row: tuple[object, ...],
    room_materialized_revision: int | None,
) -> bytes:
    return json.dumps(
        [
            row[1],
            row[2],
            row[3],
            base64.b64encode(row[5]).decode("ascii"),
            len(row[4]),
            room_materialized_revision,
        ],
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _materialize(
    journal: SqliteIngestionJournal,
    *,
    expected_revision: int | None = None,
    writer_epoch: UUID | None = None,
) -> MaterializeResult:
    owner = journal.load_owner()
    return journal.materialize_oldest_frame(
        expected_revision=(
            owner.revision if expected_revision is None else expected_revision
        ),
        writer_epoch=owner.writer_epoch if writer_epoch is None else writer_epoch,
        limits=MaterializerLimits(),
    )


def _materializer_dml(statements: list[str]) -> tuple[str, ...]:
    return tuple(
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
    )


def test_materializer_no_frame_catches_spurious_writer_or_revision_advance(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        owner_before = journal.load_owner()
        source_before = journal.load_source()
        statements.clear()

        def reject_writer(_self: object) -> object:
            raise AssertionError("empty discovery entered journal_write")

        monkeypatch.setattr(type(journal._owner), "journal_write", reject_writer)

        result = _materialize(journal)

        assert result == MaterializeResult(MaterializeStatus.IDLE, None, None)
        assert journal.load_owner() == owner_before
        assert journal.load_source() == source_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "transport",
    [TransportKind.CLASSIC, TransportKind.SLIDING],
    ids=["classic", "sliding"],
)
@pytest.mark.parametrize("crypto", [False, True], ids=["plain", "crypto"])
def test_materializer_empty_fate_catches_missing_meta_cas_raw_delete_or_crypto_reseal(
    tmp_path: Path,
    transport: TransportKind,
    crypto: bool,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(tmp_path, transport, statements=statements)
    journal = bootstrap._journal
    try:
        staged, normalized = _stage_discovery_frame(
            journal,
            transport,
            1,
            crypto=crypto,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        )
        assert proposal.room_proposals == ()
        assert proposal.descriptors == ()
        assert proposal.crypto_deferred is crypto
        owner_before = journal.load_owner()
        source_before = journal.load_source()
        row_before = _frame_storage_row(journal, staged.frame_id)
        assert row_before is not None
        statements.clear()

        result = _materialize(journal)

        expected_revision = owner_before.revision + 1
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            expected_revision,
        )
        assert journal.load_owner() == replace(
            owner_before,
            revision=expected_revision,
        )
        assert journal.load_source() == source_before
        row_after = _frame_storage_row(journal, staged.frame_id)
        dml = _materializer_dml(statements)
        assert len(dml) == 2
        assert sum("UPDATE NioIngestMeta" in statement for statement in dml) == 1
        if not crypto:
            assert row_after is None
            assert (
                sum("DELETE FROM NioIngestFrame" in statement for statement in dml) == 1
            )
        else:
            assert row_after is not None
            assert row_after[:6] == row_before[:6]
            assert row_after[6] == expected_revision
            assert row_after[7] != row_before[7]
            assert sum("UPDATE NioIngestFrame" in statement for statement in dml) == 1
            assert (
                EncryptedRowCodec(
                    "discovery-secret",
                    journal.account_id,
                    owner_before.stream_id,
                ).decrypt(
                    "NioIngestFrameDrainHeader",
                    (staged.frame_id,),
                    row_after[7],
                    hashlib.sha256(b"").digest(),
                    header=_canonical_expected_drain_header(
                        row_after,
                        expected_revision,
                    ),
                )
                == b""
            )

        owner_after = journal.load_owner()
        retained_after = _frame_storage_row(journal, staged.frame_id)
        statements.clear()
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.IDLE,
            None,
            None,
        )
        assert journal.load_owner() == owner_after
        assert _frame_storage_row(journal, staged.frame_id) == retained_after
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "transport",
    [TransportKind.CLASSIC, TransportKind.SLIDING],
    ids=["classic", "sliding"],
)
def test_materializer_retained_crypto_frame_catches_head_of_line_blocking(
    tmp_path: Path,
    transport: TransportKind,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(tmp_path, transport, statements=statements)
    journal = bootstrap._journal
    try:
        crypto_frame, _ = _stage_discovery_frame(
            journal,
            transport,
            1,
            crypto=True,
        )
        _retain_discovery_frames(journal, (crypto_frame.frame_id,))
        crypto_row = _frame_storage_row(journal, crypto_frame.frame_id)
        assert crypto_row is not None
        plain_frame, _ = _stage_discovery_frame(journal, transport, 2)
        owner_before = journal.load_owner()
        crypto_row_before = _frame_storage_row(journal, crypto_frame.frame_id)
        assert crypto_row_before == crypto_row
        statements.clear()

        plain_result = _materialize(journal)

        assert plain_result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            plain_frame.frame_id,
            owner_before.revision + 1,
        )
        assert journal.load_owner() == replace(
            owner_before,
            revision=owner_before.revision + 1,
        )
        assert _frame_storage_row(journal, crypto_frame.frame_id) == crypto_row_before
        assert _frame_storage_row(journal, plain_frame.frame_id) is None
        dml = _materializer_dml(statements)
        assert len(dml) == 2
        assert sum("UPDATE NioIngestMeta" in statement for statement in dml) == 1
        assert sum("DELETE FROM NioIngestFrame" in statement for statement in dml) == 1
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.IDLE,
            None,
            None,
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "transport",
    [TransportKind.CLASSIC, TransportKind.SLIDING],
    ids=["classic", "sliding"],
)
def test_materializer_nonempty_selected_frame_catches_entry_into_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transport: TransportKind,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(tmp_path, transport, statements=statements)
    journal = bootstrap._journal
    try:
        staged, normalized = _stage_discovery_frame(
            journal,
            transport,
            1,
            crypto=True,
            nonempty=True,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        )
        assert len(proposal.descriptors) == 1
        assert proposal.crypto_deferred
        owner_before = journal.load_owner()
        row_before = _frame_storage_row(journal, staged.frame_id)
        statements.clear()

        def reject_writer(_self: object) -> object:
            raise AssertionError("nonempty frame entered journal_write")

        monkeypatch.setattr(type(journal._owner), "journal_write", reject_writer)
        with pytest.raises((JournalIntegrityError, ReducerInputError)):
            _materialize(journal)

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == row_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


def _retain_discovery_frames(
    journal: SqliteIngestionJournal,
    frame_ids: tuple[UUID, ...],
) -> int:
    retained_revision = journal.load_owner().revision
    for frame_id in frame_ids:
        owner = journal.load_owner()
        retained_revision = owner.revision + 1
        row = _frame_storage_row(journal, frame_id)
        assert row is not None
        proof = journal._codec.encrypt(
            "NioIngestFrameDrainHeader",
            (frame_id,),
            b"",
            digest=hashlib.sha256(b"").digest(),
            header=_canonical_expected_drain_header(row, retained_revision),
        )
        with journal._owner.journal_write():
            updated = journal._execute(
                "UPDATE NioIngestMeta SET revision = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    retained_revision,
                    journal.account_id,
                    owner.revision,
                    str(owner.writer_epoch),
                ),
            )
            assert updated.rowcount == 1
            updated = journal._execute(
                "UPDATE NioIngestFrame SET room_materialized_revision = ?, "
                "drain_header_ciphertext = ? "
                "WHERE account_id = ? AND frame_id = ?",
                (
                    retained_revision,
                    proof,
                    journal.account_id,
                    str(frame_id),
                ),
            )
            assert updated.rowcount == 1
    return retained_revision


def _stage_retained_then_plain(
    journal: SqliteIngestionJournal,
    transport: TransportKind = TransportKind.CLASSIC,
) -> tuple[StagedFrame, StagedFrame]:
    earlier, _ = _stage_discovery_frame(
        journal,
        transport,
        1,
        crypto=True,
    )
    _retain_discovery_frames(journal, (earlier.frame_id,))
    selected, _ = _stage_discovery_frame(journal, transport, 2)
    return earlier, selected


def _flip_first(value: bytes) -> bytes:
    return bytes((value[0] ^ 1,)) + value[1:]


def _corrupt_discovery_frame(
    database_path: Path,
    frame_id: UUID,
    mutation: str,
) -> None:
    with sqlite3.connect(database_path) as connection:
        row = connection.execute(
            "SELECT source_epoch, request_id, staged_revision, payload_ciphertext, "
            "payload_sha256, room_materialized_revision, drain_header_ciphertext "
            "FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (_DISCOVERY_ACCOUNT_ID, str(frame_id)),
        ).fetchone()
        assert row is not None
        if mutation == "source-epoch":
            column, value = "source_epoch", row[0] + 1
        elif mutation == "request-id":
            column, value = "request_id", row[1] + 1
        elif mutation == "staged-revision":
            column, value = "staged_revision", row[2] + 1
        elif mutation == "clear-flag":
            column, value = "room_materialized_revision", None
        elif mutation == "proof":
            column, value = "drain_header_ciphertext", _flip_first(row[6])
        elif mutation == "pk-spelling":
            column, value = "frame_id", str(frame_id).upper()
        elif mutation == "payload-digest":
            column, value = "payload_sha256", _flip_first(row[4])
        elif mutation == "payload-length":
            column, value = "payload_ciphertext", row[3] + b"x"
        elif mutation == "selected-ciphertext":
            column, value = "payload_ciphertext", _flip_first(row[3])
        else:
            raise AssertionError(f"unknown test mutation: {mutation}")
        updated = connection.execute(
            f"UPDATE NioIngestFrame SET {column} = ? "
            "WHERE account_id = ? AND frame_id = ?",
            (value, _DISCOVERY_ACCOUNT_ID, str(frame_id)),
        )
        assert updated.rowcount == 1


@pytest.mark.parametrize(
    ("mutation", "target"),
    [
        pytest.param("source-epoch", "earlier", id="clear-source-epoch-aad"),
        pytest.param("request-id", "earlier", id="clear-request-id-aad"),
        pytest.param("staged-revision", "earlier", id="clear-staged-revision-aad"),
        pytest.param("clear-flag", "earlier", id="clear-hand-off-flag-aad"),
        pytest.param("proof", "earlier", id="drain-proof-ciphertext"),
        pytest.param("pk-spelling", "earlier", id="primary-key-spelling"),
        pytest.param("payload-digest", "earlier", id="payload-digest-aad"),
        pytest.param("payload-length", "earlier", id="payload-length-aad"),
        pytest.param("selected-ciphertext", "selected", id="selected-ciphertext"),
    ],
)
def test_materializer_complete_proof_scan_catches_corruption_before_later_dml(
    tmp_path: Path,
    mutation: str,
    target: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        earlier, selected = _stage_retained_then_plain(journal)
        owner_before = journal.load_owner()
        selected_before = _frame_storage_row(journal, selected.frame_id)
        assert selected_before is not None
        _corrupt_discovery_frame(
            bootstrap.database_path,
            earlier.frame_id if target == "earlier" else selected.frame_id,
            mutation,
        )
        selected_after_corruption = _frame_storage_row(journal, selected.frame_id)
        assert selected_after_corruption is not None
        statements.clear()

        with pytest.raises(JournalIntegrityError):
            _materialize(journal)

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, selected.frame_id) == (
            selected_before if target == "earlier" else selected_after_corruption
        )
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize("stale_fence", ["revision", "epoch"])
def test_materializer_stale_fence_catches_entry_into_business_dml(
    tmp_path: Path,
    stale_fence: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        staged, _ = _stage_discovery_frame(journal, TransportKind.CLASSIC, 1)
        owner_before = journal.load_owner()
        row_before = _frame_storage_row(journal, staged.frame_id)
        statements.clear()

        with pytest.raises(JournalConflictError):
            _materialize(
                journal,
                expected_revision=(
                    owner_before.revision - 1
                    if stale_fence == "revision"
                    else owner_before.revision
                ),
                writer_epoch=(
                    uuid4() if stale_fence == "epoch" else owner_before.writer_epoch
                ),
            )

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == row_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


def _apply_discovery_race(
    journal: SqliteIngestionJournal,
    earlier: StagedFrame,
    selected: StagedFrame,
    race: str,
) -> None:
    database_path = journal.database_path
    if race == "earlier-valid-flag-change":
        row = _frame_storage_row(journal, earlier.frame_id)
        assert row is not None
        proof = journal._codec.encrypt(
            "NioIngestFrameDrainHeader",
            (earlier.frame_id,),
            b"",
            digest=hashlib.sha256(b"").digest(),
            header=_canonical_expected_drain_header(row, None),
        )
        with sqlite3.connect(database_path) as connection:
            updated = connection.execute(
                "UPDATE NioIngestFrame SET room_materialized_revision = NULL, "
                "drain_header_ciphertext = ? WHERE account_id = ? AND frame_id = ?",
                (proof, journal.account_id, str(earlier.frame_id)),
            )
            assert updated.rowcount == 1
        return
    if race == "earlier-removal":
        with sqlite3.connect(database_path) as connection:
            deleted = connection.execute(
                "DELETE FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
                (journal.account_id, str(earlier.frame_id)),
            )
            assert deleted.rowcount == 1
        return
    if race == "selected-removal":
        with sqlite3.connect(database_path) as connection:
            deleted = connection.execute(
                "DELETE FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
                (journal.account_id, str(selected.frame_id)),
            )
            assert deleted.rowcount == 1
        return
    target = earlier if race.startswith("earlier-") else selected
    mutation = {
        "earlier-header": "request-id",
        "earlier-proof": "proof",
        "selected-row-change": "selected-ciphertext",
    }[race]
    _corrupt_discovery_frame(database_path, target.frame_id, mutation)


@pytest.mark.parametrize(
    "race",
    [
        "earlier-removal",
        "earlier-header",
        "earlier-proof",
        "earlier-valid-flag-change",
        "selected-row-change",
        "selected-removal",
    ],
)
def test_materializer_writer_full_set_revalidation_catches_read_to_write_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        earlier, selected = _stage_retained_then_plain(journal)
        owner_before = journal.load_owner()
        selected_prepared = False
        raced = False
        real_decrypt = EncryptedRowCodec.decrypt
        real_journal_write = type(journal._owner).journal_write

        def observe_selected_decrypt(
            codec: EncryptedRowCodec,
            table: str,
            primary_key: tuple[str | int | UUID, ...],
            ciphertext: bytes,
            digest: bytes,
            header: bytes = b"",
        ) -> bytes:
            nonlocal selected_prepared
            payload = real_decrypt(
                codec,
                table,
                primary_key,
                ciphertext,
                digest,
                header,
            )
            if table == "NioIngestFrame":
                assert journal._owner._outer_scope != "journal_write"
                selected_prepared = True
            return payload

        @contextmanager
        def inject_writer_boundary(owner: object) -> Iterator[None]:
            nonlocal raced
            assert owner is journal._owner
            assert selected_prepared
            assert not raced
            raced = True
            _apply_discovery_race(journal, earlier, selected, race)
            with real_journal_write(journal._owner):
                yield

        monkeypatch.setattr(EncryptedRowCodec, "decrypt", observe_selected_decrypt)
        monkeypatch.setattr(
            type(journal._owner),
            "journal_write",
            inject_writer_boundary,
        )
        statements.clear()
        with pytest.raises(JournalIntegrityError):
            _materialize(journal)

        assert selected_prepared
        assert raced
        assert journal.load_owner() == owner_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


class _ReverseDrainRows:
    def __init__(self, cursor: sqlite3.Cursor) -> None:
        self._cursor = cursor

    def fetchall(self) -> list[sqlite3.Row]:
        return sorted(
            self._cursor.fetchall(),
            key=itemgetter(
                "staged_revision",
                "source_epoch",
                "request_id",
                "frame_id",
            ),
            reverse=True,
        )


def _is_frame_discovery_select(statement: str) -> bool:
    lowered = " ".join(statement.lower().split())
    return lowered.startswith("select ") and " from nioingestframe" in lowered


def _is_frame_header_select(statement: str) -> bool:
    lowered = statement.lower()
    return (
        _is_frame_discovery_select(statement)
        and "drain_header_ciphertext" in lowered
        and "length(payload_ciphertext)" in lowered
    )


_SQL_IDENTIFIER = r'(?:[A-Z_][A-Z0-9_$]*|"(?:[^"]|"")+"|`[^`]+`|\[[^\]]+\])'
_PROJECTION_WILDCARD = re.compile(
    rf"(?<![A-Z0-9_$])(?:{_SQL_IDENTIFIER}\s*\.\s*)?\*(?![A-Z0-9_$])",
    re.IGNORECASE,
)
_PAYLOAD_LENGTH = re.compile(
    rf"\bLENGTH\s*\(\s*(?:{_SQL_IDENTIFIER}\s*\.\s*)?" r"PAYLOAD_CIPHERTEXT\s*\)",
    re.IGNORECASE,
)


def _frame_select_projection(statement: str) -> str:
    normalized = " ".join(statement.split())
    return re.split(
        r"\s+FROM\s+NIOINGESTFRAME\b",
        normalized,
        maxsplit=1,
        flags=re.IGNORECASE,
    )[0]


def _projection_fetches_full_payload(projection: str) -> bool:
    without_derived_length = _PAYLOAD_LENGTH.sub("", projection)
    return bool(_PROJECTION_WILDCARD.search(projection)) or bool(
        re.search(r"\bPAYLOAD_CIPHERTEXT\b", without_derived_length, re.IGNORECASE)
    )


def test_materializer_bounded_queries_catch_filtered_ordered_or_partial_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        assert all(
            _PROJECTION_WILDCARD.search(projection)
            for projection in ("SELECT *", "SELECT NioIngestFrame.*", "SELECT f.*")
        )
        assert not _PROJECTION_WILDCARD.search(
            "SELECT frame_id, LENGTH(payload_ciphertext)"
        )
        assert not _projection_fetches_full_payload(
            "SELECT frame_id, LENGTH(payload_ciphertext)"
        )
        assert _projection_fetches_full_payload("SELECT frame_id, payload_ciphertext")
        frames = tuple(
            _stage_discovery_frame(
                journal,
                TransportKind.CLASSIC,
                sequence,
                crypto=sequence < 255,
            )[0]
            for sequence in range(1, 257)
        )
        _retain_discovery_frames(
            journal,
            tuple(frame.frame_id for frame in frames[:-2]),
        )
        selected = frames[-2]
        later = frames[-1]
        selected_row = _frame_storage_row(journal, selected.frame_id)
        later_row = _frame_storage_row(journal, later.frame_id)
        assert selected_row is not None
        assert later_row is not None
        assert (
            selected_row[3],
            selected_row[1],
            selected_row[2],
            selected_row[0],
        ) < (
            later_row[3],
            later_row[1],
            later_row[2],
            later_row[0],
        )
        queries: list[tuple[str | None, str, tuple[object, ...]]] = []
        decrypts: list[tuple[str | None, str, tuple[str | int | UUID, ...]]] = []
        real_execute = journal._execute
        real_decrypt = EncryptedRowCodec.decrypt

        def trace_execute(
            statement: str,
            parameters: tuple[object, ...] = (),
        ) -> sqlite3.Cursor | _ReverseDrainRows:
            scope = journal._owner._outer_scope
            queries.append((scope, statement, parameters))
            cursor = real_execute(statement, parameters)
            if _is_frame_header_select(statement):
                assert scope in {"read", "journal_write"}
                return _ReverseDrainRows(cursor)
            return cursor

        def trace_decrypt(
            codec: EncryptedRowCodec,
            table: str,
            primary_key: tuple[str | int | UUID, ...],
            ciphertext: bytes,
            digest: bytes,
            header: bytes = b"",
        ) -> bytes:
            decrypts.append((journal._owner._outer_scope, table, primary_key))
            return real_decrypt(
                codec,
                table,
                primary_key,
                ciphertext,
                digest,
                header,
            )

        monkeypatch.setattr(journal, "_execute", trace_execute)
        monkeypatch.setattr(EncryptedRowCodec, "decrypt", trace_decrypt)

        result = _materialize(journal)

        assert result.frame_id == selected.frame_id
        materialize_queries = tuple(queries)
        assert _frame_storage_row(journal, selected.frame_id) is None
        assert _frame_storage_row(journal, later.frame_id) == later_row
        frame_selects = [
            item for item in materialize_queries if _is_frame_discovery_select(item[1])
        ]
        header_queries = [
            item for item in frame_selects if _is_frame_header_select(item[1])
        ]
        assert [scope for scope, _, _ in header_queries] == [
            "read",
            "journal_write",
        ]
        for _, statement, parameters in header_queries:
            upper = " ".join(statement.upper().split())
            projection = _frame_select_projection(statement)
            assert not _PROJECTION_WILDCARD.search(projection)
            assert _PAYLOAD_LENGTH.search(projection)
            assert not _projection_fetches_full_payload(projection)
            assert "ORDER BY" not in upper
            if "LIMIT ?" in upper:
                assert parameters[-1] == 257
            else:
                assert "LIMIT 257" in upper
        selected_queries = [
            item for item in frame_selects if item not in header_queries
        ]
        assert [scope for scope, _, _ in selected_queries] == [
            "read",
            "journal_write",
        ]
        for _, statement, _ in selected_queries:
            projection = _frame_select_projection(statement)
            assert _projection_fetches_full_payload(projection)
        assert len(frame_selects) == 4
        for _, statement, _ in frame_selects:
            upper = statement.upper()
            assert "ORDER BY" not in upper
            assert "WHERE" in upper
            predicate = upper.split("WHERE", 1)[1]
            assert "ROOM_MATERIALIZED_REVISION" not in predicate
        frame_decrypts = [
            item
            for item in decrypts
            if item[1] in {"NioIngestFrameDrainHeader", "NioIngestFrame"}
        ]
        expected_ids = frozenset(frame.frame_id for frame in frames)
        read_proofs = frame_decrypts[:256]
        assert len(read_proofs) == 256
        assert {scope for scope, _, _ in read_proofs} == {"read"}
        assert {table for _, table, _ in read_proofs} == {"NioIngestFrameDrainHeader"}
        assert frozenset(primary_key[0] for _, _, primary_key in read_proofs) == (
            expected_ids
        )
        selected_scope, selected_table, selected_key = frame_decrypts[256]
        assert selected_scope != "journal_write"
        assert selected_table == "NioIngestFrame"
        assert selected_key == (selected.frame_id,)
        writer_proofs = frame_decrypts[257:]
        assert len(writer_proofs) == 256
        assert {scope for scope, _, _ in writer_proofs} == {"journal_write"}
        assert {table for _, table, _ in writer_proofs} == {"NioIngestFrameDrainHeader"}
        assert frozenset(primary_key[0] for _, _, primary_key in writer_proofs) == (
            expected_ids
        )
    finally:
        bootstrap.close()


def test_materializer_limit_257_catches_raw_set_truncation_before_payload_or_dml(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        frames = tuple(
            _stage_discovery_frame(
                journal,
                TransportKind.CLASSIC,
                sequence,
            )[0]
            for sequence in range(1, 258)
        )
        assert len(frames) == 257
        with journal._owner.read():
            stored_count = journal._execute(
                "SELECT COUNT(*) FROM NioIngestFrame WHERE account_id = ?",
                (journal.account_id,),
            ).fetchone()[0]
        assert stored_count == 257
        owner_before = journal.load_owner()
        payload_decrypts: list[tuple[str | None, tuple[str | int | UUID, ...]]] = []
        real_decrypt = EncryptedRowCodec.decrypt

        def trace_decrypt(
            codec: EncryptedRowCodec,
            table: str,
            primary_key: tuple[str | int | UUID, ...],
            ciphertext: bytes,
            digest: bytes,
            header: bytes = b"",
        ) -> bytes:
            if table == "NioIngestFrame":
                payload_decrypts.append((journal._owner._outer_scope, primary_key))
            return real_decrypt(
                codec,
                table,
                primary_key,
                ciphertext,
                digest,
                header,
            )

        def reject_writer(_self: object) -> object:
            raise AssertionError("257-row discovery entered journal_write")

        monkeypatch.setattr(EncryptedRowCodec, "decrypt", trace_decrypt)
        monkeypatch.setattr(type(journal._owner), "journal_write", reject_writer)
        statements.clear()

        with pytest.raises(JournalIntegrityError):
            _materialize(journal)

        assert payload_decrypts == []
        assert journal.load_owner() == owner_before
        with journal._owner.read():
            assert (
                journal._execute(
                    "SELECT COUNT(*) FROM NioIngestFrame WHERE account_id = ?",
                    (journal.account_id,),
                ).fetchone()[0]
                == 257
            )
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()
