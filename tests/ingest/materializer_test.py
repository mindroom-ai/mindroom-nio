"""Task 6 private materializer contract RED tests."""

import base64
import hashlib
import importlib
import inspect
import json
import multiprocessing
import os
import re
import shutil
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import FrozenInstanceError, dataclass, fields, is_dataclass, replace
from enum import StrEnum
from operator import itemgetter
from pathlib import Path
from types import ModuleType
from typing import NoReturn, get_type_hints
from uuid import UUID, uuid4, uuid5

import pytest
from peewee import OperationalError as PeeweeOperationalError

import nio.ingest as ingest
import nio.ingest.classic as classic_module
import nio.ingest.ports as ports_module
import nio.ingest.sliding as sliding_module
import nio.store as store
from nio.event_provenance import TimelineEventProvenance
from nio.exceptions import LocalProtocolError
from nio.ingest import source
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.errors import (
    FreshIngestionRequired,
    JournalConflictError,
    JournalIntegrityError,
)
from nio.ingest.membership import MembershipObservation
from nio.ingest.model import (
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RecordOrigin,
    TransportKind,
)
from nio.ingest.ports import (
    NetworkRequest,
    NetworkResult,
    StagedSourceResponse,
    _frame_id_for_response,
)
from nio.ingest.reducer import (
    DescriptorRoute,
    HydrationIntent,
    LossProposal,
    MembershipBaseline,
    RecoveryGap,
    RecoveryRelease,
    ReducerInputError,
    RoomContinuity,
    reduce_staged_frame,
)
from nio.ingest.serialization import _loss_id, _record_to_dict
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
from nio.ingest.state import OwnerView, SourceState, StagedFrame
from nio.store import SqliteStore
from nio.store._sync_journal import SqliteIngestionJournal
from nio.store._sync_journal_codec import EncryptedRowCodec
from nio.store._sync_journal_plan import AuthenticatedWork, plan_frame_materialization
from nio.store._sync_journal_port import IngestionJournal
from nio.store._sync_journal_rows import _canonical_internal, _frame_envelope
from nio.store._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
    RoomAggregateValue,
)
from nio.store.sync_journal import StoreBootstrap, open_ingestion_store
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
_LIMIT_CEILINGS = (
    1 * 1024 * 1024,
    10_000,
    32 * 1024 * 1024,
    20_000,
    64 * 1024 * 1024,
    20_000,
    64 * 1024 * 1024,
)
_LIMIT_FIELDS = (
    "max_record_canonical_bytes",
    "max_held_work_count",
    "max_held_work_canonical_bytes",
    "max_ready_work_count",
    "max_ready_work_canonical_bytes",
    "max_total_work_count",
    "max_total_work_canonical_bytes",
)
_FRAME_ID = UUID("12345678-1234-5678-1234-567812345678")
_STREAM_ID = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")
_SOURCE_BODY_LIMIT = 16 * 1024 * 1024
_FRAME_ENVELOPE_LIMIT = 24 * 1024 * 1024
_WORK_CIPHERTEXT_LIMIT = 1 * 1024 * 1024 + 29
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
    "NioIngestRoomAggregate",
    "NioIngestRoomAggregate_intent",
    "NioIngestWork",
    "NioIngestWork_ready",
    "NioIngestWork_held_release",
    "NioIngestWork_frame_kind",
)
_AGGREGATE_COLUMNS = (
    ("account_id", "TEXT", True, 1),
    ("room_id", "TEXT", True, 2),
    ("updated_revision", "INTEGER", True, 0),
    ("intent_kind", "TEXT", False, 0),
    ("payload_ciphertext", "BLOB", True, 0),
    ("payload_sha256", "BLOB", True, 0),
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
_WORK_COLUMNS = (
    ("account_id", "TEXT", True, 1),
    ("work_id", "TEXT", True, 2),
    ("kind", "TEXT", True, 0),
    ("status", "TEXT", True, 0),
    ("frame_id", "TEXT", True, 0),
    ("room_id", "TEXT", False, 0),
    ("membership_epoch", "INTEGER", False, 0),
    ("room_sequence", "INTEGER", False, 0),
    ("ready_revision", "INTEGER", False, 0),
    ("ready_ordinal", "INTEGER", False, 0),
    ("created_revision", "INTEGER", True, 0),
    ("payload_ciphertext", "BLOB", True, 0),
    ("payload_sha256", "BLOB", True, 0),
)
_TASK6_SCHEMA_OBJECTS = frozenset(
    {
        ("table", "NioIngestMeta"),
        ("table", "NioIngestSourceState"),
        ("table", "NioIngestFrame"),
        ("table", "NioIngestRoomAggregate"),
        ("table", "NioIngestWork"),
        ("index", "NioIngestFrame_drain"),
        ("index", "NioIngestRoomAggregate_intent"),
        ("index", "NioIngestWork_ready"),
        ("index", "NioIngestWork_held_release"),
        ("index", "NioIngestWork_frame_kind"),
    }
)
_PRE_TASK4_WORK_ONLY_SCHEMA_OBJECTS = _TASK6_SCHEMA_OBJECTS - {
    ("table", "NioIngestRoomAggregate"),
    ("index", "NioIngestRoomAggregate_intent"),
}

# Frozen Task2 Frame-only physical schema. This fixture intentionally does not
# derive from SCHEMA_SQL: a pre-Task3 store must be refused rather than silently
# treated as a Work-capable owner.
_PRE_TASK3_FRAME_ONLY_DDL = (
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
)


def _values() -> ModuleType:
    return importlib.import_module("nio.store._sync_journal_values")


def _rows() -> ModuleType:
    return importlib.import_module("nio.store._sync_journal_rows")


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
        "RoomAggregateValue",
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
    _assert_frozen_slotted(
        values.RoomAggregateValue,
        (
            "continuity",
            "next_room_sequence",
            "updated_revision",
            "pending_hydration",
        ),
    )


_AGGREGATE_ROOM_ID = "!aggregate:example.org"
_HYDRATION_ID = UUID("12345678-1234-5678-9234-567812345678")


def _hydrating_aggregate_value() -> object:
    values = _values()
    origin = RecordOrigin(TransportKind.CLASSIC, 4, 9, 0)
    return values.RoomAggregateValue(
        RoomContinuity(
            _AGGREGATE_ROOM_ID,
            2,
            "join",
            None,
            None,
            _HYDRATION_ID,
        ),
        3,
        7,
        HydrationIntent(_HYDRATION_ID, origin),
    )


def test_contract_room_aggregate_value_catches_invalid_counter_or_intent_relation() -> (
    None
):
    values = _values()
    origin = RecordOrigin(TransportKind.CLASSIC, 4, 9, 0)
    hydration = HydrationIntent(_HYDRATION_ID, origin)
    clear = RoomContinuity(
        _AGGREGATE_ROOM_ID,
        2,
        "join",
        MembershipBaseline("$member", "s0"),
        None,
        None,
    )
    gap = RecoveryGap(
        UUID("22345678-1234-5678-9234-567812345678"),
        _AGGREGATE_ROOM_ID,
        2,
        origin,
        "s0",
        "s1",
    )

    assert values.RoomAggregateValue(clear, 0, 1, None).continuity == clear
    assert _hydrating_aggregate_value().pending_hydration == hydration
    assert (
        values.RoomAggregateValue(
            replace(clear, baseline=None, gap=gap), 0, 1, None
        ).continuity.gap
        == gap
    )
    for arguments, error_type in (
        ((object(), 0, 1, None), TypeError),
        ((clear, True, 1, None), TypeError),
        ((clear, -1, 1, None), ValueError),
        ((clear, 0, True, None), TypeError),
        ((clear, 0, 0, None), ValueError),
        ((clear, 0, 1, hydration), ValueError),
        ((replace(clear, baseline=None, gap=gap), 0, 1, hydration), ValueError),
        (
            (
                replace(clear, baseline=None, hydration_id=_HYDRATION_ID),
                0,
                1,
                None,
            ),
            ValueError,
        ),
        (
            (
                replace(clear, baseline=None, hydration_id=_HYDRATION_ID),
                0,
                1,
                HydrationIntent(UUID(int=1), origin),
            ),
            ValueError,
        ),
    ):
        with pytest.raises(error_type):
            values.RoomAggregateValue(*arguments)


def _expected_aggregate_plaintext() -> bytes:
    return canonical_json(
        {
            "continuity": {
                "baseline": None,
                "gap": None,
                "hydration_id": str(_HYDRATION_ID),
                "membership": "join",
                "membership_epoch": 2,
                "room_id": _AGGREGATE_ROOM_ID,
            },
            "next_room_sequence": 3,
            "pending_hydration": {
                "hydration_id": str(_HYDRATION_ID),
                "origin": {
                    "frame_index": 0,
                    "origin_type": "transport",
                    "request_id": 9,
                    "source_epoch": 4,
                    "transport": "classic",
                },
            },
            "updated_revision": 7,
        }
    )


def test_contract_aggregate_plaintext_catches_shape_or_canonical_reencode_drift() -> (
    None
):
    rows = _rows()
    value = _hydrating_aggregate_value()
    expected = _expected_aggregate_plaintext()

    assert rows._canonical_room_aggregate_plaintext(value) == expected
    assert (
        rows._room_aggregate_value_from_plaintext(
            _AGGREGATE_ROOM_ID,
            7,
            "hydration",
            expected,
        )
        == value
    )
    with pytest.raises(ValueError, match=r".+"):
        rows._room_aggregate_value_from_plaintext(
            _AGGREGATE_ROOM_ID,
            7,
            "hydration",
            expected.replace(b'{"continuity":', b'{ "continuity":', 1),
        )
    for room_id, revision, intent_kind in (
        ("!other:example.org", 7, "hydration"),
        (_AGGREGATE_ROOM_ID, 8, "hydration"),
        (_AGGREGATE_ROOM_ID, 7, None),
        (_AGGREGATE_ROOM_ID, 7, "recovery"),
    ):
        with pytest.raises(ValueError, match=r".+"):
            rows._room_aggregate_value_from_plaintext(
                room_id,
                revision,
                intent_kind,
                expected,
            )


def test_contract_null_aggregate_plaintext_is_exact_and_strict() -> None:
    rows = _rows()
    value = _values().RoomAggregateValue(
        RoomContinuity(
            _AGGREGATE_ROOM_ID,
            3,
            "leave",
            None,
            None,
            None,
        ),
        5,
        8,
        None,
    )
    expected = (
        b'{"continuity":{"baseline":null,"gap":null,"hydration_id":null,'
        b'"membership":"leave","membership_epoch":3,'
        b'"room_id":"!aggregate:example.org"},"next_room_sequence":5,'
        b'"pending_hydration":null,"updated_revision":8}'
    )

    assert rows._canonical_room_aggregate_plaintext(value) == expected
    assert (
        rows._room_aggregate_value_from_plaintext(
            _AGGREGATE_ROOM_ID,
            8,
            None,
            expected,
        )
        == value
    )

    malformed = json.loads(expected)
    del malformed["pending_hydration"]
    barrier = json.loads(expected)
    barrier["continuity"]["hydration_id"] = str(_HYDRATION_ID)
    wrong_type = json.loads(expected)
    wrong_type["continuity"]["membership_epoch"] = True
    for room_id, revision, intent_kind, plaintext in (
        (
            _AGGREGATE_ROOM_ID,
            8,
            None,
            expected.replace(b'{"continuity":', b'{ "continuity":', 1),
        ),
        (_AGGREGATE_ROOM_ID, 8, None, canonical_json(malformed)),
        (_AGGREGATE_ROOM_ID, 8, None, canonical_json(barrier)),
        (_AGGREGATE_ROOM_ID, 8, None, canonical_json(wrong_type)),
        ("!other:example.org", 8, None, expected),
        (_AGGREGATE_ROOM_ID, 9, None, expected),
        (_AGGREGATE_ROOM_ID, 8, "hydration", expected),
        (_AGGREGATE_ROOM_ID, 8, "recovery", expected),
    ):
        with pytest.raises(ValueError, match=r".+"):
            rows._room_aggregate_value_from_plaintext(
                room_id,
                revision,
                intent_kind,
                plaintext,
            )


@pytest.mark.parametrize(
    "corruption",
    [
        "wrong-key",
        "missing-key",
        "uuid-spelling",
        "wrong-type",
        "negative-counter",
        "negative-source-epoch",
        "negative-request-id",
        "negative-frame-index",
    ],
)
def test_contract_aggregate_plaintext_rejects_malformed_live_hydration(
    corruption: str,
) -> None:
    value = json.loads(_expected_aggregate_plaintext())
    if corruption == "wrong-key":
        value["pending"] = value.pop("pending_hydration")
    elif corruption == "missing-key":
        del value["continuity"]["gap"]
    elif corruption == "uuid-spelling":
        value["pending_hydration"]["hydration_id"] = f"{{{_HYDRATION_ID}}}"
    elif corruption == "wrong-type":
        value["continuity"]["membership_epoch"] = True
    elif corruption == "negative-counter":
        value["next_room_sequence"] = -1
    else:
        field = corruption.removeprefix("negative-").replace("-", "_")
        value["pending_hydration"]["origin"][field] = -1

    with pytest.raises(ValueError, match=r".+"):
        _rows()._room_aggregate_value_from_plaintext(
            _AGGREGATE_ROOM_ID,
            7,
            "hydration",
            canonical_json(value),
        )


def test_materializer_aggregate_load_rejects_origin_transport_mismatch(
    tmp_path: Path,
) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        staged, _normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_present=True,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        row = _aggregate_rows(journal)[0]
        plaintext, _value = _decrypt_aggregate(journal, row)
        payload = json.loads(plaintext)
        payload["pending_hydration"]["origin"]["transport"] = "sliding"
        forged = canonical_json(payload)
        ciphertext, digest = journal._codec.seal(
            "NioIngestRoomAggregate",
            (row[0],),
            forged,
            header=_canonical_internal([row[0], row[1], row[2]]),
        )
        with journal._owner.journal_write():
            updated = journal._execute(
                "UPDATE NioIngestRoomAggregate SET payload_ciphertext = ?, "
                "payload_sha256 = ? WHERE account_id = ? AND room_id = ?",
                (ciphertext, digest, journal.account_id, row[0]),
            )
            assert updated.rowcount == 1

        with pytest.raises(JournalIntegrityError):
            journal._load_room_aggregate(journal.load_owner(), row[0])
        assert _frame_storage_row(journal, staged.frame_id) is None
    finally:
        bootstrap.close()


def test_contract_aggregate_codec_catches_table_pk_aad_cipher_or_digest_drift() -> None:
    plaintext = _expected_aggregate_plaintext()
    codec = EncryptedRowCodec("aggregate-secret", _SCHEMA_ACCOUNT_ID, _STREAM_ID)
    header = _canonical_internal([_AGGREGATE_ROOM_ID, 7, "hydration"])
    ciphertext, digest = codec.seal(
        "NioIngestRoomAggregate",
        (_AGGREGATE_ROOM_ID,),
        plaintext,
        header=header,
    )
    assert (
        codec.decrypt(
            "NioIngestRoomAggregate",
            (_AGGREGATE_ROOM_ID,),
            ciphertext,
            digest,
            header=header,
        )
        == plaintext
    )
    corruptions = (
        ("NioIngestWork", (_AGGREGATE_ROOM_ID,), ciphertext, digest, header),
        ("NioIngestRoomAggregate", ("!other:example.org",), ciphertext, digest, header),
        (
            "NioIngestRoomAggregate",
            (_AGGREGATE_ROOM_ID,),
            ciphertext,
            digest,
            _canonical_internal([_AGGREGATE_ROOM_ID, 8, "hydration"]),
        ),
        (
            "NioIngestRoomAggregate",
            (_AGGREGATE_ROOM_ID,),
            bytes((ciphertext[0] ^ 1,)) + ciphertext[1:],
            digest,
            header,
        ),
        (
            "NioIngestRoomAggregate",
            (_AGGREGATE_ROOM_ID,),
            ciphertext,
            bytes((digest[0] ^ 1,)) + digest[1:],
            header,
        ),
    )
    for (
        table,
        primary_key,
        stored_ciphertext,
        stored_digest,
        stored_header,
    ) in corruptions:
        with pytest.raises(JournalIntegrityError):
            codec.decrypt(
                table,
                primary_key,
                stored_ciphertext,
                stored_digest,
                header=stored_header,
            )


def _event_record() -> EventRecord:
    return EventRecord(
        "12345678-1234-5678-1234-567812345679",
        RecordKind.GLOBAL_ACCOUNT_DATA,
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 2),
        None,
        None,
        None,
        None,
        None,
        b"{}",
        None,
    )


def _expected_event_work_plaintext(record: EventRecord) -> bytes:
    assert type(record.origin) is RecordOrigin
    return canonical_json(
        {
            "kind": "event",
            "value": {
                "record_type": "event",
                "record_id": record.record_id,
                "kind": record.kind.value,
                "origin": {
                    "origin_type": "transport",
                    "transport": record.origin.transport.value,
                    "source_epoch": record.origin.source_epoch,
                    "request_id": record.origin.request_id,
                    "frame_index": record.origin.frame_index,
                },
                "room_id": record.room_id,
                "membership_epoch": record.membership_epoch,
                "room_sequence": record.room_sequence,
                "event_id": record.event_id,
                "provenance": (
                    record.provenance.value if record.provenance is not None else None
                ),
                "source_json": base64.b64encode(record.source_json).decode("ascii"),
                "clear_json": (
                    base64.b64encode(record.clear_json).decode("ascii")
                    if record.clear_json is not None
                    else None
                ),
            },
        }
    )


def _expected_loss_work_plaintext(record: LossRecord) -> bytes:
    assert type(record.origin) is RecordOrigin
    return canonical_json(
        {
            "kind": "loss",
            "value": {
                "record_type": "loss",
                "loss_id": record.loss_id,
                "origin": {
                    "origin_type": "transport",
                    "transport": record.origin.transport.value,
                    "source_epoch": record.origin.source_epoch,
                    "request_id": record.origin.request_id,
                    "frame_index": record.origin.frame_index,
                },
                "room_id": record.room_id,
                "membership_epoch": record.membership_epoch,
                "reason": record.reason.value,
                "boundary": {
                    "prior_event_id": record.boundary.prior_event_id,
                    "prior_origin_server_ts": record.boundary.prior_origin_server_ts,
                    "start_token": record.boundary.start_token,
                    "target_token": record.boundary.target_token,
                },
                "detail_json": base64.b64encode(record.detail_json).decode("ascii"),
            },
        }
    )


def test_contract_work_plaintext_catches_kind_or_record_encoding_drift() -> None:
    rows = _rows()
    value = _event_record()
    expected = (
        b'{"kind":"event","value":{"clear_json":null,"event_id":null,'
        b'"kind":"global_account_data","membership_epoch":null,'
        b'"origin":{"frame_index":2,'
        b'"origin_type":"transport","request_id":9,"source_epoch":4,'
        b'"transport":"classic"},"provenance":null,'
        b'"record_id":"12345678-1234-5678-1234-567812345679",'
        b'"record_type":"event","room_id":null,"room_sequence":null,'
        b'"source_json":"e30="}}'
    )

    assert _expected_event_work_plaintext(value) == expected
    plaintext = rows._canonical_work_plaintext("event", value)

    assert plaintext == expected
    decoded = rows._work_value_from_plaintext(
        _STREAM_ID,
        value.record_id,
        "event",
        plaintext,
    )
    assert decoded == value
    noncanonical = plaintext.replace(b'{"kind":', b'{ "kind":', 1)
    with pytest.raises(ValueError, match=r".+"):
        rows._work_value_from_plaintext(
            _STREAM_ID,
            value.record_id,
            "event",
            noncanonical,
        )


def test_contract_work_plaintext_supports_loss_and_room_ready_event() -> None:
    rows = _rows()
    room_event = EventRecord(
        "32345678-1234-5678-9234-567812345678",
        RecordKind.STATE,
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 1),
        _AGGREGATE_ROOM_ID,
        3,
        5,
        None,
        None,
        b"{}",
        None,
    )
    room_plaintext = _expected_event_work_plaintext(room_event)
    assert rows._canonical_work_plaintext("event", room_event) == room_plaintext
    assert (
        rows._work_value_from_plaintext(
            _STREAM_ID,
            room_event.record_id,
            "event",
            room_plaintext,
        )
        == room_event
    )

    loss_without_id = LossRecord(
        "",
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 0),
        _AGGREGATE_ROOM_ID,
        2,
        LossReason.UNVERIFIABLE,
        LossBoundary(None, None, None, None),
        b"{}",
    )
    assert _loss_id(_STREAM_ID, loss_without_id) == (
        "04401626-ee8d-5953-a38f-06d618e9e40f"
    )
    loss = replace(
        loss_without_id,
        loss_id="04401626-ee8d-5953-a38f-06d618e9e40f",
    )
    expected_loss = (
        b'{"kind":"loss","value":{"boundary":{"prior_event_id":null,'
        b'"prior_origin_server_ts":null,"start_token":null,"target_token":null},'
        b'"detail_json":"e30=",'
        b'"loss_id":"04401626-ee8d-5953-a38f-06d618e9e40f",'
        b'"membership_epoch":2,"origin":{"frame_index":0,'
        b'"origin_type":"transport","request_id":9,"source_epoch":4,'
        b'"transport":"classic"},"reason":"unverifiable",'
        b'"record_type":"loss","room_id":"!aggregate:example.org"}}'
    )
    assert _expected_loss_work_plaintext(loss) == expected_loss
    assert rows._canonical_work_plaintext("loss", loss) == expected_loss
    assert (
        rows._work_value_from_plaintext(
            _STREAM_ID,
            loss.loss_id,
            "loss",
            expected_loss,
        )
        == loss
    )
    with pytest.raises(ValueError, match=r".+"):
        rows._work_value_from_plaintext(
            _STREAM_ID,
            loss.loss_id,
            "loss",
            expected_loss.replace(b'{"kind":', b'{ "kind":', 1),
        )


@pytest.mark.parametrize("kind", ["recovery", "hydration", "crypto_frame"])
def test_contract_work_plaintext_rejects_deferred_roles(kind: str) -> None:
    rows = _rows()
    with pytest.raises((TypeError, ValueError)):
        rows._canonical_work_plaintext(kind, _event_record())


def test_contract_work_decode_catches_duplicate_identity_or_kind_corruption() -> None:
    rows = _rows()
    event = _event_record()
    payload = rows._canonical_work_plaintext("event", event)
    with pytest.raises(ValueError, match=r".+"):
        rows._work_value_from_plaintext(
            _STREAM_ID,
            "22345678-1234-5678-1234-567812345679",
            "event",
            payload,
        )
    with pytest.raises(ValueError, match=r".+"):
        rows._work_value_from_plaintext(
            _STREAM_ID,
            event.record_id,
            "loss",
            payload,
        )


def test_contract_materializer_limits_have_exact_defaults_ceilings_and_strict_types() -> (
    None
):
    values = _values()

    limits = values.MaterializerLimits()
    assert tuple(getattr(limits, name) for name in _LIMIT_FIELDS) == _LIMIT_DEFAULTS
    for field_name, ceiling in zip(
        _LIMIT_FIELDS,
        _LIMIT_CEILINGS,
        strict=True,
    ):
        assert getattr(values.MaterializerLimits(**{field_name: 1}), field_name) == 1
        at_ceiling = values.MaterializerLimits(**{field_name: ceiling})
        assert getattr(at_ceiling, field_name) == ceiling
        with pytest.raises(ValueError, match=r".+"):
            values.MaterializerLimits(**{field_name: ceiling + 1})
    with pytest.raises(FrozenInstanceError):
        limits.max_total_work_count = 1  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        limits.unknown = 1  # type: ignore[attr-defined]

    for field_name, default in zip(_LIMIT_FIELDS, _LIMIT_DEFAULTS, strict=True):
        with pytest.raises(TypeError):
            values.MaterializerLimits(**{field_name: True})
        with pytest.raises(TypeError):
            values.MaterializerLimits(**{field_name: float(default)})
        with pytest.raises(ValueError, match=r".+"):
            values.MaterializerLimits(**{field_name: 0})


def _pending_hydration_planner_case(
    *,
    global_ready_count: int = 0,
) -> tuple[SyncFrame, RoomAggregateValue, AuthenticatedWork]:
    room_id = "!planner-capacity:example.org"
    frame_origin = RecordOrigin(TransportKind.CLASSIC, 1, 2, 0)
    hydration_origin = replace(frame_origin, request_id=1)
    hydration_id = uuid5(_FRAME_ID, "planner-pending-hydration")
    continuity = RoomContinuity(room_id, 0, "join", None, None, hydration_id)
    aggregate = RoomAggregateValue(
        continuity,
        1,
        1,
        HydrationIntent(hydration_id, hydration_origin),
    )
    segment = RoomSegment(
        room_id,
        RoomSection.JOIN,
        (),
        (),
        (b"{}",),
        False,
        None,
        False,
        False,
        0,
        MembershipObservation(
            "join",
            None,
            None,
            None,
            None,
            False,
            False,
            False,
            False,
        ),
    )
    frame = SyncFrame(
        _FRAME_ID,
        frame_origin,
        b'{"next_batch":"s1"}',
        b'{"next_batch":"s2"}',
        b"p" * 32,
        (),
        b'{"changed":[],"left":[]}',
        b"{}",
        b"null",
        (segment,),
        (),
        (b"{}",) * global_ready_count,
        (),
    )
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (continuity,),
    )
    assert len(proposal.room_proposals) == 1
    room = proposal.room_proposals[0]
    assert room.before == continuity
    assert room.after == continuity
    assert room.hydration is not None
    assert room.hydration.hydration_id == hydration_id
    assert room.retirement_epoch is None
    assert room.losses == ()
    assert room.release is RecoveryRelease.NONE
    assert proposal.descriptors[0].route is DescriptorRoute.HOLD_FOR_HYDRATION
    held = EventRecord(
        str(uuid5(_FRAME_ID, "planner-existing-held")),
        RecordKind.ROOM_ACCOUNT_DATA,
        hydration_origin,
        room_id,
        0,
        0,
        None,
        None,
        b"{}",
        None,
    )
    held_plaintext = _expected_event_work_plaintext(held)
    return frame, aggregate, AuthenticatedWork(held, "held", len(held_plaintext))


def _pending_hydration_ephemeral_planner_case() -> (
    tuple[SyncFrame, RoomAggregateValue, AuthenticatedWork]
):
    base, aggregate, held = _pending_hydration_planner_case()
    cursor = SlidingCursor(
        "p1",
        None,
        UUID("336f12d0-c282-4594-8654-948a60a73ee9"),
        "planner",
        1,
        2,
        SlidingRangeAckMode.TXN_ECHO,
        False,
    )
    frame_origin = RecordOrigin(TransportKind.SLIDING, 1, 2, 0)
    hydration_origin = replace(frame_origin, request_id=1)
    pending = aggregate.pending_hydration
    assert pending is not None
    frame = replace(
        base,
        origin=frame_origin,
        request_cursor_json=canonical_sliding_cursor(cursor),
        candidate_cursor_json=canonical_sliding_cursor(replace(cursor, pos="p2")),
        room_segments=(),
        ephemeral_json=(
            _ephemeral_envelope(
                aggregate.continuity.room_id,
                {"content": {}, "type": "m.typing"},
            ),
        ),
    )
    aggregate = replace(
        aggregate,
        pending_hydration=HydrationIntent(
            pending.hydration_id,
            hydration_origin,
        ),
    )
    held_value = replace(held.value, origin=hydration_origin)
    held = replace(
        held,
        value=held_value,
        canonical_size=len(_expected_event_work_plaintext(held_value)),
    )
    return frame, aggregate, held


def _planner_ready_work(
    frame: SyncFrame,
    index: int,
    *,
    canonical_size: int | None = None,
) -> AuthenticatedWork:
    value = EventRecord(
        str(uuid5(frame.frame_id, f"planner-existing-ready:{index}")),
        RecordKind.GLOBAL_ACCOUNT_DATA,
        frame.origin,
        None,
        None,
        None,
        None,
        None,
        b"{}",
        None,
    )
    size = len(_expected_event_work_plaintext(value))
    return AuthenticatedWork(
        value,
        "ready",
        size if canonical_size is None else canonical_size,
    )


def test_materializer_empty_pending_hydration_ignores_existing_held_watermark() -> None:
    frame, aggregate, selected_held = _pending_hydration_planner_case()
    frame = replace(
        frame,
        room_segments=(replace(frame.room_segments[0], room_account_data_json=()),),
    )
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    assert proposal.descriptors == ()
    assert len(proposal.room_proposals) == 1
    room = proposal.room_proposals[0]
    assert room.before == aggregate.continuity == room.after
    assert room.hydration is not None
    unrelated_value = replace(
        selected_held.value,
        record_id=str(uuid5(frame.frame_id, "planner-unrelated-held")),
        room_id="!planner-unrelated:example.org",
    )
    unrelated_held = replace(
        selected_held,
        value=unrelated_value,
        canonical_size=len(_expected_event_work_plaintext(unrelated_value)),
    )

    plan = plan_frame_materialization(
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(selected_held, unrelated_held),
        revision=2,
        limits=replace(MaterializerLimits(), max_held_work_count=1),
    )

    assert plan is not None
    assert plan.room_values == ()
    assert plan.work_inserts == ()
    assert plan.work_releases == ()
    assert plan.crypto_deferred is False


def test_materializer_null_ephemeral_only_ignores_unrelated_held_watermark() -> None:
    frame, pending, selected_held = _pending_hydration_ephemeral_planner_case()
    aggregate = replace(
        pending,
        continuity=replace(pending.continuity, hydration_id=None),
        pending_hydration=None,
    )
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    assert proposal.room_proposals == ()
    assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
        DescriptorRoute.READY,
    )
    unrelated_values = tuple(
        replace(
            selected_held.value,
            record_id=str(uuid5(frame.frame_id, f"unrelated-held:{index}")),
            room_id=f"!unrelated-held-{index}:example.org",
        )
        for index in range(2)
    )
    unrelated = tuple(
        replace(
            selected_held,
            value=value,
            canonical_size=len(_expected_event_work_plaintext(value)),
        )
        for value in unrelated_values
    )
    descriptor = proposal.descriptors[0]
    expected = EventRecord(
        str(uuid5(frame.frame_id, f"event:{descriptor.descriptor_key}")),
        RecordKind.EPHEMERAL,
        frame.origin,
        aggregate.continuity.room_id,
        aggregate.continuity.membership_epoch,
        aggregate.next_room_sequence,
        None,
        None,
        descriptor.source_json,
        None,
    )
    plaintext = _expected_event_work_plaintext(expected)

    plan = plan_frame_materialization(
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=unrelated,
        revision=2,
        limits=replace(
            MaterializerLimits(),
            max_held_work_count=1,
            max_ready_work_count=1,
            max_total_work_count=3,
        ),
    )

    assert plan is not None
    assert plan.room_values == (
        replace(
            aggregate,
            next_room_sequence=aggregate.next_room_sequence + 1,
            updated_revision=2,
        ),
    )
    assert plan.work_inserts == ((expected, plaintext, 0),)
    assert plan.work_releases == ()
    assert plan.crypto_deferred is False


@pytest.mark.parametrize(
    "metric",
    ["count", "canonical-bytes"],
    ids=("count", "canonical-bytes"),
)
def test_materializer_terminal_plan_obeys_immutable_total_exact_boundary(
    metric: str,
) -> None:
    frame, aggregate, selected_held = _pending_hydration_ephemeral_planner_case()
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    assert frame.origin.transport is TransportKind.SLIDING
    assert frame.room_segments == ()
    assert proposal.room_proposals == ()
    assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
        DescriptorRoute.HOLD_FOR_HYDRATION,
    )
    loss_without_id = LossRecord(
        "",
        frame.origin,
        aggregate.continuity.room_id,
        aggregate.continuity.membership_epoch,
        LossReason.EVENT_LIMIT,
        LossBoundary(None, None, None, None),
        b"{}",
    )
    expected_loss = replace(
        loss_without_id,
        loss_id=_loss_id(_STREAM_ID, loss_without_id),
    )
    loss_plaintext = _expected_loss_work_plaintext(expected_loss)
    hard = MaterializerLimits()
    if metric == "count":
        ready_count = hard.max_total_work_count - 2
        exact_work = (selected_held,) + tuple(
            _planner_ready_work(frame, index) for index in range(ready_count)
        )
        one_over_work = exact_work + (_planner_ready_work(frame, ready_count),)
        assert len(exact_work) + 1 == hard.max_total_work_count
        assert len(one_over_work) + 1 == hard.max_total_work_count + 1
    else:
        assert metric == "canonical-bytes"
        remaining = (
            hard.max_total_work_canonical_bytes
            - selected_held.canonical_size
            - len(loss_plaintext)
        )
        ready: list[AuthenticatedWork] = []
        while remaining:
            size = min(hard.max_record_canonical_bytes, remaining)
            ready.append(_planner_ready_work(frame, len(ready), canonical_size=size))
            remaining -= size
        exact_work = (selected_held, *ready)
        assert ready[-1].canonical_size < hard.max_record_canonical_bytes
        one_over_work = (
            *exact_work[:-1],
            replace(
                exact_work[-1],
                canonical_size=exact_work[-1].canonical_size + 1,
            ),
        )
        assert (
            sum(item.canonical_size for item in exact_work) + len(loss_plaintext)
            == hard.max_total_work_canonical_bytes
        )
        assert sum(item.canonical_size for item in one_over_work) + len(
            loss_plaintext
        ) == (hard.max_total_work_canonical_bytes + 1)
    assert all(type(item.value) is EventRecord for item in exact_work)
    assert len(
        {item.value.record_id for item in exact_work if type(item.value) is EventRecord}
    ) == len(exact_work)
    limits = replace(
        MaterializerLimits(),
        max_held_work_count=1,
        max_ready_work_count=1,
        max_ready_work_canonical_bytes=1,
        max_total_work_count=1,
        max_total_work_canonical_bytes=1,
    )

    plan = plan_frame_materialization(
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=exact_work,
        revision=2,
        limits=limits,
    )

    assert plan is not None
    assert plan.room_values == (
        RoomAggregateValue(
            replace(aggregate.continuity, hydration_id=None),
            aggregate.next_room_sequence,
            2,
            None,
        ),
    )
    assert plan.work_inserts == ((expected_loss, loss_plaintext, 0),)
    assert plan.work_releases == (
        (
            selected_held.value,
            _expected_event_work_plaintext(selected_held.value),
            1,
        ),
    )
    assert plan.crypto_deferred is False
    assert (
        plan_frame_materialization(
            stream_id=_STREAM_ID,
            frame=frame,
            aggregates=(aggregate,),
            work=one_over_work,
            revision=2,
            limits=limits,
        )
        is None
    )


def test_materializer_terminal_replacement_obeys_hard_addition_count_boundary() -> None:
    hard = MaterializerLimits()
    base, aggregate, selected_held = _pending_hydration_ephemeral_planner_case()
    proposal = reduce_staged_frame(
        _STREAM_ID,
        base.frame_id,
        base,
        (aggregate.continuity,),
    )
    assert base.origin.transport is TransportKind.SLIDING
    assert base.room_segments == ()
    assert proposal.room_proposals == ()
    assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
        DescriptorRoute.HOLD_FOR_HYDRATION,
    )
    limits = replace(
        hard,
        max_held_work_count=1,
        max_ready_work_count=1,
        max_ready_work_canonical_bytes=1,
        max_total_work_count=1,
        max_total_work_canonical_bytes=1,
    )
    exact = replace(
        base,
        global_account_data_json=(b"{}",) * (hard.max_held_work_count - 1),
    )

    plan = plan_frame_materialization(
        stream_id=_STREAM_ID,
        frame=exact,
        aggregates=(aggregate,),
        work=(selected_held,),
        revision=2,
        limits=limits,
    )

    assert plan is not None
    assert len(plan.work_inserts) == hard.max_held_work_count
    assert sum(len(item[1]) for item in plan.work_inserts) < (
        hard.max_held_work_canonical_bytes
    )
    one_over = replace(
        base,
        global_account_data_json=(b"{}",) * hard.max_held_work_count,
    )
    with pytest.raises(ValueError, match="hard addition envelope"):
        plan_frame_materialization(
            stream_id=_STREAM_ID,
            frame=one_over,
            aggregates=(aggregate,),
            work=(selected_held,),
            revision=2,
            limits=limits,
        )


@pytest.mark.parametrize("pending", [True, False], ids=("pending", "null"))
def test_materializer_ephemeral_only_room_oversize_is_a_room_loss(
    pending: bool,
) -> None:
    base, pending_aggregate, selected_held = _pending_hydration_ephemeral_planner_case()
    aggregate = (
        pending_aggregate
        if pending
        else replace(
            pending_aggregate,
            continuity=replace(
                pending_aggregate.continuity,
                hydration_id=None,
            ),
            pending_hydration=None,
        )
    )
    frame = replace(
        base,
        room_segments=(),
        ephemeral_json=(
            _ephemeral_envelope(
                aggregate.continuity.room_id,
                {"content": {"padding": "x" * 512}, "type": "m.typing"},
            ),
            _ephemeral_envelope(
                aggregate.continuity.room_id,
                {"content": {}, "type": "m.x"},
            ),
        ),
    )
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    assert frame.room_segments == ()
    assert proposal.room_proposals == ()
    expected_route = (
        DescriptorRoute.HOLD_FOR_HYDRATION if pending else DescriptorRoute.READY
    )
    assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
        expected_route,
        expected_route,
    )
    incoming = tuple(
        EventRecord(
            str(uuid5(frame.frame_id, f"event:frame:{frame.frame_id}:{index}")),
            RecordKind.EPHEMERAL,
            replace(frame.origin, frame_index=index),
            aggregate.continuity.room_id,
            aggregate.continuity.membership_epoch,
            aggregate.next_room_sequence + index,
            None,
            None,
            descriptor.source_json,
            None,
        )
        for index, descriptor in enumerate(proposal.descriptors)
    )
    loss_without_id = LossRecord(
        "",
        frame.origin,
        aggregate.continuity.room_id,
        aggregate.continuity.membership_epoch,
        LossReason.OVERSIZED_EVENT,
        LossBoundary(None, None, None, None),
        b"{}",
    )
    expected_loss = replace(
        loss_without_id,
        loss_id=_loss_id(_STREAM_ID, loss_without_id),
    )
    loss_plaintext = _expected_loss_work_plaintext(expected_loss)
    incoming_sizes = tuple(
        len(_expected_event_work_plaintext(record)) for record in incoming
    )
    assert incoming_sizes[0] > len(loss_plaintext)
    assert incoming_sizes[1] <= len(loss_plaintext)
    limits = replace(
        MaterializerLimits(),
        max_record_canonical_bytes=len(loss_plaintext),
        max_held_work_count=1,
        max_ready_work_count=1,
        max_ready_work_canonical_bytes=1,
        max_total_work_count=1,
        max_total_work_canonical_bytes=1,
    )

    plan = plan_frame_materialization(
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(selected_held,) if pending else (),
        revision=2,
        limits=limits,
    )

    assert plan is not None
    assert plan.work_inserts == ((expected_loss, loss_plaintext, 0),)
    assert plan.work_releases == (
        (
            (
                selected_held.value,
                _expected_event_work_plaintext(selected_held.value),
                1,
            ),
        )
        if pending
        else ()
    )
    assert plan.room_values == (
        (
            RoomAggregateValue(
                replace(aggregate.continuity, hydration_id=None),
                aggregate.next_room_sequence,
                2,
                None,
            ),
        )
        if pending
        else ()
    )
    assert plan.crypto_deferred is False
    with pytest.raises(ValueError, match="canonical byte limit"):
        plan_frame_materialization(
            stream_id=_STREAM_ID,
            frame=frame,
            aggregates=(aggregate,),
            work=(selected_held,) if pending else (),
            revision=2,
            limits=replace(
                limits,
                max_record_canonical_bytes=len(loss_plaintext) - 1,
            ),
        )


def test_materializer_ephemeral_only_terminal_candidate_does_not_swallow_global_oversize() -> (
    None
):
    base, aggregate, selected_held = _pending_hydration_ephemeral_planner_case()
    frame = replace(
        base,
        room_segments=(),
        ephemeral_json=(
            _ephemeral_envelope(
                aggregate.continuity.room_id,
                {"content": {}, "type": "m.typing"},
            ),
        ),
        global_account_data_json=(
            canonical_json({"content": {"padding": "x" * 512}, "type": "m.push_rules"}),
        ),
    )
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    assert proposal.room_proposals == ()
    assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
        DescriptorRoute.HOLD_FOR_HYDRATION,
        DescriptorRoute.READY,
    )
    records = tuple(
        EventRecord(
            str(uuid5(frame.frame_id, f"event:frame:{frame.frame_id}:{index}")),
            descriptor.kind,
            replace(frame.origin, frame_index=index),
            descriptor.room_id,
            (
                aggregate.continuity.membership_epoch
                if descriptor.room_id is not None
                else None
            ),
            aggregate.next_room_sequence if descriptor.room_id is not None else None,
            None,
            None,
            descriptor.source_json,
            None,
        )
        for index, descriptor in enumerate(proposal.descriptors)
    )
    room_bytes, global_bytes = (
        len(_expected_event_work_plaintext(record)) for record in records
    )
    loss_without_id = LossRecord(
        "",
        frame.origin,
        aggregate.continuity.room_id,
        aggregate.continuity.membership_epoch,
        LossReason.EVENT_LIMIT,
        LossBoundary(None, None, None, None),
        b"{}",
    )
    loss = replace(loss_without_id, loss_id=_loss_id(_STREAM_ID, loss_without_id))
    assert len(_expected_loss_work_plaintext(loss)) <= room_bytes
    assert room_bytes < global_bytes

    with pytest.raises(ValueError, match="canonical byte limit"):
        plan_frame_materialization(
            stream_id=_STREAM_ID,
            frame=frame,
            aggregates=(aggregate,),
            work=(selected_held,),
            revision=2,
            limits=replace(
                MaterializerLimits(),
                max_record_canonical_bytes=room_bytes,
                max_held_work_count=1,
            ),
        )


@pytest.mark.parametrize(
    "case",
    [
        "absent-aggregate",
        "two-rooms",
        "segment-and-ephemeral",
        "recovery-gap",
        "null-with-held",
    ],
    ids=(
        "absent-aggregate",
        "two-rooms",
        "segment-and-e",
        "recovery-gap",
        "null-with-held",
    ),
)
def test_materializer_ephemeral_only_rejects_ambiguous_ownership(case: str) -> None:
    base, pending, selected_held = _pending_hydration_ephemeral_planner_case()
    first_room = pending.continuity.room_id
    null = replace(
        pending,
        continuity=replace(pending.continuity, hydration_id=None),
        pending_hydration=None,
    )
    second_room = "!planner-ephemeral-second:example.org"
    second = RoomAggregateValue(
        RoomContinuity(second_room, 0, "join", None, None, None),
        0,
        1,
        None,
    )
    if case == "absent-aggregate":
        frame = replace(
            base,
            room_segments=(),
            ephemeral_json=(
                _ephemeral_envelope(
                    first_room,
                    {"content": {}, "type": "m.typing"},
                ),
            ),
        )
        aggregates: tuple[RoomAggregateValue, ...] = ()
        work: tuple[AuthenticatedWork, ...] = ()
    elif case == "two-rooms":
        frame = replace(
            base,
            room_segments=(),
            ephemeral_json=(
                _ephemeral_envelope(
                    first_room,
                    {"content": {}, "type": "m.typing"},
                ),
                _ephemeral_envelope(
                    second_room,
                    {"content": {}, "type": "m.receipt"},
                ),
            ),
        )
        aggregates = (null, second)
        work = ()
        proposal = reduce_staged_frame(
            _STREAM_ID,
            frame.frame_id,
            frame,
            tuple(item.continuity for item in aggregates),
        )
        assert proposal.room_proposals == ()
        assert {descriptor.room_id for descriptor in proposal.descriptors} == {
            first_room,
            second_room,
        }
    elif case == "segment-and-ephemeral":
        segment_base, _, _ = _pending_hydration_planner_case()
        frame = replace(
            base,
            room_segments=segment_base.room_segments,
            ephemeral_json=(
                _ephemeral_envelope(
                    second_room,
                    {"content": {}, "type": "m.typing"},
                ),
            ),
        )
        aggregates = (pending, second)
        work = (selected_held,)
        proposal = reduce_staged_frame(
            _STREAM_ID,
            frame.frame_id,
            frame,
            tuple(item.continuity for item in aggregates),
        )
        assert len(proposal.room_proposals) == 1
        assert {descriptor.room_id for descriptor in proposal.descriptors} == {
            first_room,
            second_room,
        }
    elif case == "recovery-gap":
        gap = RecoveryGap(
            uuid5(base.frame_id, "planner-ephemeral-gap"),
            first_room,
            null.continuity.membership_epoch,
            replace(base.origin, request_id=1),
            "p0",
            "p1",
        )
        frame = base
        aggregates = (
            replace(
                null,
                continuity=replace(null.continuity, gap=gap),
            ),
        )
        work = ()
        proposal = reduce_staged_frame(
            _STREAM_ID,
            frame.frame_id,
            frame,
            tuple(item.continuity for item in aggregates),
        )
        assert proposal.room_proposals == ()
        assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_GAP,
        )
    else:
        assert case == "null-with-held"
        frame = replace(
            base,
            room_segments=(),
            ephemeral_json=(
                _ephemeral_envelope(
                    first_room,
                    {"content": {}, "type": "m.typing"},
                ),
            ),
        )
        aggregates = (null,)
        work = (selected_held,)

    with pytest.raises(ValueError, match=r".+"):
        plan_frame_materialization(
            stream_id=_STREAM_ID,
            frame=frame,
            aggregates=aggregates,
            work=work,
            revision=2,
            limits=MaterializerLimits(),
        )


def test_materializer_projected_held_count_includes_unrelated_rooms() -> None:
    frame = _frame(
        (
            _ephemeral_envelope(
                "!segment-a:example.org",
                {"type": "m.typing"},
            ),
        )
    )
    existing = EventRecord(
        str(uuid5(frame.frame_id, "existing-held")),
        RecordKind.EPHEMERAL,
        RecordOrigin(TransportKind.CLASSIC, 1, 1, 0),
        "!unrelated:example.org",
        0,
        0,
        None,
        None,
        b'{"type":"m.typing"}',
        None,
    )

    with pytest.raises(ValueError, match="HELD Work exceeds.*capacity"):
        plan_frame_materialization(
            stream_id=_STREAM_ID,
            frame=frame,
            aggregates=(),
            work=(AuthenticatedWork(existing, "held", 1),),
            revision=1,
            limits=replace(MaterializerLimits(), max_held_work_count=1),
        )


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
        assert (
            _table_columns(connection, "NioIngestRoomAggregate") == _AGGREGATE_COLUMNS
        )
        assert _table_columns(connection, "NioIngestWork") == _WORK_COLUMNS
        assert _foreign_keys(connection, "NioIngestFrame") == (
            ("NioIngestMeta", "account_id", "account_id"),
        )
        assert _foreign_keys(connection, "NioIngestWork") == (
            ("NioIngestMeta", "account_id", "account_id"),
        )
        assert _foreign_keys(connection, "NioIngestRoomAggregate") == (
            ("NioIngestMeta", "account_id", "account_id"),
        )
        assert _index_columns(connection, "NioIngestFrame_drain") == (
            "account_id",
            "staged_revision",
            "source_epoch",
            "request_id",
            "frame_id",
        )
        assert _index_columns(connection, "NioIngestWork_ready") == (
            "account_id",
            "status",
            "ready_revision",
            "ready_ordinal",
            "work_id",
        )
        assert _index_columns(connection, "NioIngestRoomAggregate_intent") == (
            "account_id",
            "intent_kind",
            "room_id",
        )
        assert _index_columns(connection, "NioIngestWork_held_release") == (
            "account_id",
            "room_id",
            "membership_epoch",
            "status",
            "room_sequence",
            "work_id",
        )
        assert _index_columns(connection, "NioIngestWork_frame_kind") == (
            "account_id",
            "frame_id",
            "kind",
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


def _insert_work(
    connection: sqlite3.Connection,
    **overrides: object,
) -> None:
    row: dict[str, object] = {
        "account_id": _SCHEMA_ACCOUNT_ID,
        "work_id": "12345678-1234-5678-1234-567812345679",
        "kind": "event",
        "status": "ready",
        "frame_id": _SCHEMA_FRAME_ID,
        "room_id": None,
        "membership_epoch": None,
        "room_sequence": None,
        "ready_revision": 1,
        "ready_ordinal": 0,
        "created_revision": 1,
        "payload_ciphertext": b"w" * 29,
        "payload_sha256": b"d" * 32,
    }
    row.update(overrides)
    connection.execute(
        "INSERT INTO NioIngestWork ("
        + ", ".join(row)
        + ") VALUES ("
        + ", ".join("?" for _ in row)
        + ")",
        tuple(row.values()),
    )


def _insert_work_zeroblob(
    connection: sqlite3.Connection,
    *,
    work_id: str,
    ready_ordinal: int,
    ciphertext_bytes: int,
) -> None:
    connection.execute(
        """INSERT INTO NioIngestWork(
            account_id, work_id, kind, status, frame_id, room_id,
            membership_epoch, room_sequence, ready_revision, ready_ordinal,
            created_revision, payload_ciphertext, payload_sha256
        ) VALUES (?, ?, 'event', 'ready', ?, NULL, NULL, NULL, 1, ?, 1,
                  zeroblob(?), ?)""",
        (
            _SCHEMA_ACCOUNT_ID,
            work_id,
            _SCHEMA_FRAME_ID,
            ready_ordinal,
            ciphertext_bytes,
            b"d" * 32,
        ),
    )


@pytest.mark.parametrize(
    "overrides",
    [
        {"account_id": None},
        {"account_id": ""},
        {"account_id": "@missing:example.org"},
        {"work_id": None},
        {"work_id": ""},
        {"kind": "recovery"},
        {"kind": "hydration"},
        {"kind": "crypto_frame"},
        {"status": "pending"},
        {"status": "marker"},
        {"frame_id": None},
        {"frame_id": ""},
        {"frame_id": b"frame"},
        {"room_id": ""},
        {"membership_epoch": -1},
        {"membership_epoch": 0.5},
        {"room_sequence": -1},
        {"room_sequence": 0.5},
        {"ready_revision": 0},
        {"ready_revision": 1.5},
        {"ready_ordinal": -1},
        {"ready_ordinal": 0.5},
        {"created_revision": None},
        {"created_revision": 0},
        {"created_revision": 1.5},
        {"payload_ciphertext": b"w" * 28},
        {"payload_sha256": b"d" * 31},
        {"status": "held"},
        {"ready_revision": None},
        {"ready_ordinal": None},
        {"room_id": "!room:example.org"},
        {"kind": "loss", "room_id": None},
        {
            "kind": "loss",
            "room_id": "!room:example.org",
            "membership_epoch": 0,
            "room_sequence": 0,
        },
    ],
)
def test_contract_task6_ddl_work_constraints_catch_invalid_role_metadata(
    overrides: dict[str, object],
) -> None:
    connection = _open_task6_schema()
    try:
        with pytest.raises(sqlite3.IntegrityError, match=_SQLITE_CONSTRAINT_MATCH):
            _insert_work(connection, **overrides)
    finally:
        connection.close()


@pytest.mark.parametrize(
    "overrides",
    [
        {},
        {
            "work_id": "event-room-ready",
            "room_id": "!room:example.org",
            "membership_epoch": 0,
            "room_sequence": 7,
        },
        {
            "work_id": "event-room-held",
            "status": "held",
            "room_id": "!room:example.org",
            "membership_epoch": 0,
            "room_sequence": 7,
            "ready_revision": None,
            "ready_ordinal": None,
        },
        {
            "work_id": "loss-ready",
            "kind": "loss",
            "room_id": "!room:example.org",
            "membership_epoch": 0,
        },
    ],
    ids=("global-ready-event", "room-ready-event", "room-held-event", "ready-loss"),
)
def test_contract_task6_ddl_work_accepts_only_event_and_loss_relations(
    overrides: dict[str, object],
) -> None:
    connection = _open_task6_schema()
    try:
        _insert_work(connection, **overrides)
    finally:
        connection.close()


def test_contract_task6_ddl_work_ciphertext_ceiling_is_exact() -> None:
    connection = _open_task6_schema()
    try:
        _insert_work_zeroblob(
            connection,
            work_id="work-maximum",
            ready_ordinal=0,
            ciphertext_bytes=_WORK_CIPHERTEXT_LIMIT,
        )
        with pytest.raises(sqlite3.IntegrityError, match=_SQLITE_CONSTRAINT_MATCH):
            _insert_work_zeroblob(
                connection,
                work_id="work-over-maximum",
                ready_ordinal=1,
                ciphertext_bytes=_WORK_CIPHERTEXT_LIMIT + 1,
            )
    finally:
        connection.close()


def test_contract_task6_ddl_work_ready_pair_is_unique_per_account() -> None:
    connection = _open_task6_schema()
    try:
        _insert_work(connection)
        with pytest.raises(sqlite3.IntegrityError, match=_SQLITE_CONSTRAINT_MATCH):
            _insert_work(connection, work_id="different-id")
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
    # WAL read marks live in transient -shm bytes; only durable files are evidence.
    paths = (
        database_path,
        database_path.with_name(database_path.name + "-wal"),
    )
    return {path.name: path.read_bytes() for path in paths if path.exists()}


def _assert_persisted_task6_topology(database_path: Path) -> None:
    assert _ingestion_schema_objects(database_path) == _TASK6_SCHEMA_OBJECTS
    with sqlite3.connect(database_path) as connection:
        assert _table_columns(connection, "NioIngestFrame") == _FRAME_COLUMNS
        assert _table_columns(connection, "NioIngestRoomAggregate") == (
            _AGGREGATE_COLUMNS
        )
        assert _table_columns(connection, "NioIngestWork") == _WORK_COLUMNS
        assert _foreign_keys(connection, "NioIngestRoomAggregate") == (
            ("NioIngestMeta", "account_id", "account_id"),
        )
        assert _index_columns(connection, "NioIngestFrame_drain") == (
            "account_id",
            "staged_revision",
            "source_epoch",
            "request_id",
            "frame_id",
        )
        assert _index_columns(connection, "NioIngestRoomAggregate_intent") == (
            "account_id",
            "intent_kind",
            "room_id",
        )
        assert _index_columns(connection, "NioIngestWork_ready") == (
            "account_id",
            "status",
            "ready_revision",
            "ready_ordinal",
            "work_id",
        )
        assert _index_columns(connection, "NioIngestWork_held_release") == (
            "account_id",
            "room_id",
            "membership_epoch",
            "status",
            "room_sequence",
            "work_id",
        )
        assert _index_columns(connection, "NioIngestWork_frame_kind") == (
            "account_id",
            "frame_id",
            "kind",
        )


def _create_pre_task3_frame_only_store(database_path: Path) -> tuple[int, str]:
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
        for statement in _PRE_TASK3_FRAME_ONLY_DDL:
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


def _create_pre_task4_work_only_store(database_path: Path) -> tuple[int, str]:
    bootstrap = _open_task6_bootstrap(
        database_path.parent,
        database_name=database_path.name,
    )
    try:
        owner = bootstrap._journal.load_owner()
    finally:
        bootstrap.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute("DROP INDEX IF EXISTS NioIngestRoomAggregate_intent")
        connection.execute("DROP TABLE IF EXISTS NioIngestRoomAggregate")
        connection.execute("PRAGMA wal_checkpoint(TRUNCATE)")
    return owner.revision, str(owner.writer_epoch)


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


def test_preflight_rejects_valid_pre_task3_frame_only_store_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "pre-task6.db"
    revision_before, writer_epoch_before = _create_pre_task3_frame_only_store(
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


def test_preflight_rejects_valid_pre_task4_work_only_store_without_mutation(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "pre-task4.db"
    revision_before, writer_epoch_before = _create_pre_task4_work_only_store(
        database_path
    )
    assert (
        _ingestion_schema_objects(database_path) == _PRE_TASK4_WORK_ONLY_SCHEMA_OBJECTS
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
        raise AssertionError("pre-Task4 preflight constructed an E2EE store")

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
    room_present: bool = False,
    room_nonempty: bool = False,
    room_ephemeral: bool = False,
    ephemeral_only: bool = False,
    room_membership: str = "join",
    global_ready_count: int | None = None,
    padding_bytes: int = 0,
) -> bytes:
    if room_membership not in {"join", "leave"}:
        raise ValueError("discovery room membership must be join or leave")
    global_events = [
        {
            "content": {
                "generation": sequence,
                "index": index,
                "padding": "x" * padding_bytes,
            },
            "type": "m.push_rules",
        }
        for index in range(global_ready_count or 0)
    ]
    presence_events = (
        [
            {
                "content": {"presence": "online"},
                "sender": "@friend:example.org",
                "type": "m.presence",
            }
        ]
        if global_ready_count is not None
        else []
    )
    room_event = {
        "content": {"body": "held", "msgtype": "m.text"},
        "type": "m.room.message",
    }
    ephemeral_event = {
        "content": {"user_ids": ["@friend:example.org"]},
        "type": "m.typing",
    }
    receipt_event = {
        "content": {"generation": sequence},
        "type": "m.receipt",
    }
    if request.transport is TransportKind.CLASSIC:
        if ephemeral_only:
            raise ValueError("ephemeral-only discovery is Sliding-specific")
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
        if global_ready_count is not None:
            body["account_data"] = {"events": global_events}
            body["presence"] = {"events": presence_events}
        elif nonempty:
            body["account_data"] = {
                "events": [{"content": {"enabled": True}, "type": "m.push_rules"}]
            }
        if room_present and not room_nonempty:
            body["rooms"] = {room_membership: {"!unsupported:example.org": {}}}
        elif room_nonempty:
            room: dict[str, object] = {"timeline": {"events": [room_event]}}
            if room_ephemeral:
                room["ephemeral"] = {"events": [ephemeral_event]}
            body["rooms"] = {
                room_membership: {
                    "!unsupported:example.org": room,
                }
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
    if global_ready_count is not None:
        extensions["account_data"] = {"global": global_events}
        extensions["presence"] = {"events": presence_events}
    elif nonempty:
        extensions["account_data"] = {
            "global": [{"content": {"enabled": True}, "type": "m.push_rules"}]
        }
    if room_present and not room_nonempty:
        body["lists"] = {RESERVED_ALL_ROOMS_LIST: {"count": 1}}
        body["rooms"] = {"!unsupported:example.org": {"membership": room_membership}}
        if room_ephemeral:
            extensions["typing"] = {
                "rooms": {"!unsupported:example.org": ephemeral_event}
            }
    elif room_nonempty:
        body["lists"] = {RESERVED_ALL_ROOMS_LIST: {"count": 1}}
        body["rooms"] = {
            "!unsupported:example.org": {
                "membership": room_membership,
                "timeline": [room_event],
            }
        }
        if room_ephemeral:
            extensions["typing"] = {
                "rooms": {"!unsupported:example.org": ephemeral_event}
            }
    if ephemeral_only:
        extensions["typing"] = {"rooms": {"!unsupported:example.org": ephemeral_event}}
        extensions["receipts"] = {"rooms": {"!unsupported:example.org": receipt_event}}
    if extensions:
        body["extensions"] = extensions
    return canonical_json(body)


def _open_discovery_journal(
    store_path: Path,
    transport: TransportKind,
    *,
    statements: list[str] | None = None,
    sqlite_busy_timeout_ms: int = 2_000,
):
    return open_ingestion_store(
        store_path,
        account_id=_DISCOVERY_ACCOUNT_ID,
        device_id=_DISCOVERY_DEVICE_ID,
        source=_discovery_config(transport),
        pickle_key="discovery-secret",
        database_name="discovery.db",
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        statement_observer=statements.append if statements is not None else None,
    )


def _stage_discovery_frame(
    journal: SqliteIngestionJournal,
    transport: TransportKind,
    sequence: int,
    *,
    crypto: bool = False,
    nonempty: bool = False,
    room_present: bool = False,
    room_nonempty: bool = False,
    room_ephemeral: bool = False,
    ephemeral_only: bool = False,
    room_membership: str = "join",
    global_ready_count: int | None = None,
    padding_bytes: int = 0,
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
        room_present=room_present,
        room_nonempty=room_nonempty,
        room_ephemeral=room_ephemeral,
        ephemeral_only=ephemeral_only,
        room_membership=room_membership,
        global_ready_count=global_ready_count,
        padding_bytes=padding_bytes,
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


def _planned_global_event_records(
    journal: SqliteIngestionJournal,
    staged: StagedFrame,
    normalized: SyncFrame,
) -> tuple[EventRecord, ...]:
    proposal = reduce_staged_frame(
        journal.load_owner().stream_id,
        staged.frame_id,
        normalized,
        (),
    )
    assert proposal.room_proposals == ()
    assert all(descriptor.room_id is None for descriptor in proposal.descriptors)
    return tuple(
        EventRecord(
            str(uuid5(staged.frame_id, f"event:frame:{staged.frame_id}:{index}")),
            descriptor.kind,
            replace(normalized.origin, frame_index=index),
            None,
            None,
            None,
            None,
            None,
            descriptor.source_json,
            None,
        )
        for index, descriptor in enumerate(proposal.descriptors)
    )


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
    limits: MaterializerLimits | None = None,
) -> MaterializeResult:
    owner = journal.load_owner()
    return journal.materialize_oldest_frame(
        expected_revision=(
            owner.revision if expected_revision is None else expected_revision
        ),
        writer_epoch=owner.writer_epoch if writer_epoch is None else writer_epoch,
        limits=limits or MaterializerLimits(),
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
                "NioIngestRoomAggregate",
                "NioIngestWork",
            )
        )
    )


def _aggregate_rows(journal: SqliteIngestionJournal) -> tuple[tuple[object, ...], ...]:
    with journal._owner.read():
        rows = journal._execute(
            "SELECT room_id, updated_revision, intent_kind, payload_ciphertext, "
            "payload_sha256 FROM NioIngestRoomAggregate "
            "WHERE account_id = ? ORDER BY room_id",
            (journal.account_id,),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _decrypt_aggregate(
    journal: SqliteIngestionJournal,
    row: tuple[object, ...],
) -> tuple[bytes, object]:
    plaintext = journal._codec.decrypt(
        "NioIngestRoomAggregate",
        (row[0],),
        row[3],
        row[4],
        header=_canonical_internal([row[0], row[1], row[2]]),
    )
    return plaintext, _rows()._room_aggregate_value_from_plaintext(
        row[0],
        row[1],
        row[2],
        plaintext,
    )


def _work_rows(journal: SqliteIngestionJournal) -> tuple[tuple[object, ...], ...]:
    with journal._owner.read():
        rows = journal._execute(
            "SELECT work_id, kind, status, frame_id, room_id, membership_epoch, "
            "room_sequence, ready_revision, ready_ordinal, created_revision, "
            "payload_ciphertext, payload_sha256 FROM NioIngestWork "
            "WHERE account_id = ? ORDER BY ready_revision, ready_ordinal, work_id",
            (journal.account_id,),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _work_header(
    account_id: str,
    row: tuple[object, ...],
) -> bytes:
    return _canonical_internal([account_id, *row[:10]])


def _decrypt_work(
    journal: SqliteIngestionJournal,
    row: tuple[object, ...],
) -> tuple[bytes, EventRecord | LossRecord]:
    plaintext = journal._codec.decrypt(
        "NioIngestWork",
        (row[0],),
        row[10],
        row[11],
        header=_work_header(journal.account_id, row),
    )
    value = _rows()._work_value_from_plaintext(
        journal.load_owner().stream_id,
        row[0],
        row[1],
        plaintext,
    )
    assert type(value) in (EventRecord, LossRecord)
    return plaintext, value


def _decrypt_event_work(
    journal: SqliteIngestionJournal,
    row: tuple[object, ...],
) -> tuple[bytes, EventRecord]:
    plaintext, value = _decrypt_work(journal, row)
    assert type(value) is EventRecord
    return plaintext, value


def _insert_authenticated_event_work(
    journal: SqliteIngestionJournal,
    record: EventRecord,
    *,
    frame_id: UUID,
    ready_revision: int | None,
    ready_ordinal: int | None,
    created_revision: int,
    status: str = "ready",
) -> None:
    values = _authenticated_event_work_values(
        journal,
        record,
        frame_id=frame_id,
        ready_revision=ready_revision,
        ready_ordinal=ready_ordinal,
        created_revision=created_revision,
        status=status,
    )
    with journal._owner.journal_write():
        inserted = journal._execute(
            "INSERT INTO NioIngestWork("
            "account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload_ciphertext, payload_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )
        assert inserted.rowcount == 1


def _authenticated_event_work_values(
    journal: SqliteIngestionJournal,
    record: EventRecord,
    *,
    frame_id: UUID,
    ready_revision: int | None,
    ready_ordinal: int | None,
    created_revision: int,
    status: str = "ready",
) -> tuple[object, ...]:
    return _sealed_work_values(
        journal,
        work_id=record.record_id,
        kind="event",
        status=status,
        frame_id=str(frame_id),
        room_id=record.room_id,
        membership_epoch=record.membership_epoch,
        room_sequence=record.room_sequence,
        ready_revision=ready_revision,
        ready_ordinal=ready_ordinal,
        created_revision=created_revision,
        plaintext=_rows()._canonical_work_plaintext("event", record),
    )


def _sealed_work_values(
    journal: SqliteIngestionJournal,
    *,
    work_id: str,
    kind: str,
    status: str,
    frame_id: str,
    room_id: str | None,
    membership_epoch: int | None,
    room_sequence: int | None,
    ready_revision: int | None,
    ready_ordinal: int | None,
    created_revision: int,
    plaintext: bytes,
) -> tuple[object, ...]:
    header_values: tuple[object, ...] = (
        work_id,
        kind,
        status,
        frame_id,
        room_id,
        membership_epoch,
        room_sequence,
        ready_revision,
        ready_ordinal,
        created_revision,
    )
    ciphertext, digest = journal._codec.seal(
        "NioIngestWork",
        (work_id,),
        plaintext,
        header=_canonical_internal([journal.account_id, *header_values]),
    )
    return (journal.account_id, *header_values, ciphertext, digest)


@pytest.mark.parametrize(
    ("transport", "crypto"),
    [
        (TransportKind.CLASSIC, False),
        (TransportKind.SLIDING, True),
    ],
    ids=("classic-plain", "sliding-crypto"),
)
def test_materializer_empty_first_seen_room_creates_hydration_owner(
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
            room_present=True,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        )
        assert proposal.descriptors == ()
        assert len(proposal.room_proposals) == 1
        room = proposal.room_proposals[0]
        assert room.before is None
        assert room.hydration is not None
        assert proposal.crypto_deferred is crypto

        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        assert raw_before is not None
        statements.clear()

        result = _materialize(journal)

        revision = owner_before.revision + 1
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            revision,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        plaintext, aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        expected = _values().RoomAggregateValue(
            room.after,
            0,
            revision,
            room.hydration,
        )
        assert aggregate == expected
        assert plaintext == _rows()._canonical_room_aggregate_plaintext(expected)
        assert _work_rows(journal) == ()

        raw_after = _frame_storage_row(journal, staged.frame_id)
        if crypto:
            assert raw_after is not None
            assert raw_after[:6] == raw_before[:6]
            assert raw_after[6] == revision
            assert raw_after[7] != raw_before[7]
        else:
            assert raw_after is None
        dml = _materializer_dml(statements)
        assert sum("UPDATE NioIngestMeta" in statement for statement in dml) == 1
        assert sum("NioIngestRoomAggregate" in statement for statement in dml) == 1
        assert sum("NioIngestFrame" in statement for statement in dml) == 1
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.IDLE,
            None,
            None,
        )
    finally:
        bootstrap.close()


def test_materializer_pending_ephemeral_only_room_holds_before_ready_globals(
    tmp_path: Path,
) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.SLIDING)
    journal = bootstrap._journal
    try:
        first, _ = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            1,
            room_nonempty=True,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, before = _decrypt_aggregate(journal, aggregate_rows[0])
        assert type(before) is _values().RoomAggregateValue
        assert before.next_room_sequence == 1
        assert before.pending_hydration is not None
        first_work = _work_rows(journal)
        assert len(first_work) == 1
        assert first_work[0][2] == "held"

        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            2,
            ephemeral_only=True,
            global_ready_count=1,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (before.continuity,),
        )
        assert normalized.room_segments == ()
        assert proposal.room_proposals == ()
        assert tuple(descriptor.kind for descriptor in proposal.descriptors) == (
            RecordKind.EPHEMERAL,
            RecordKind.EPHEMERAL,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordKind.PRESENCE,
        )
        assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        assert tuple(
            descriptor.source_json for descriptor in proposal.descriptors[:2]
        ) == (
            b'{"content":{"user_ids":["@friend:example.org"]},' b'"type":"m.typing"}',
            b'{"content":{"generation":2},"type":"m.receipt"}',
        )
        raw_before = _frame_storage_row(journal, staged.frame_id)
        assert raw_before is not None

        result = _materialize(journal)

        revision = 4
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            revision,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, after = _decrypt_aggregate(journal, aggregate_rows[0])
        assert after == _values().RoomAggregateValue(
            before.continuity,
            3,
            revision,
            before.pending_hydration,
        )

        expected: list[EventRecord] = []
        room_sequence = before.next_room_sequence
        ready_ordinal = 0
        for index, descriptor in enumerate(proposal.descriptors):
            is_room = descriptor.room_id is not None
            expected.append(
                EventRecord(
                    str(
                        uuid5(
                            staged.frame_id,
                            f"event:frame:{staged.frame_id}:{index}",
                        )
                    ),
                    descriptor.kind,
                    replace(normalized.origin, frame_index=index),
                    descriptor.room_id,
                    before.continuity.membership_epoch if is_room else None,
                    room_sequence if is_room else None,
                    None,
                    None,
                    descriptor.source_json,
                    None,
                )
            )
            if is_room:
                room_sequence += 1
            else:
                ready_ordinal += 1
        assert tuple(record.origin.frame_index for record in expected) == (0, 1, 2, 3)
        assert tuple(record.room_sequence for record in expected[:2]) == (1, 2)
        rows = {
            str(row[0]): row
            for row in _work_rows(journal)
            if row[3] == str(staged.frame_id)
        }
        assert set(rows) == {record.record_id for record in expected}
        assert (
            tuple(row for row in _work_rows(journal) if row[3] == str(first.frame_id))
            == first_work
        )
        ready_ordinal = 0
        for record in expected:
            row = rows[record.record_id]
            is_room = record.room_id is not None
            assert row[1:10] == (
                "event",
                "held" if is_room else "ready",
                str(staged.frame_id),
                record.room_id,
                record.membership_epoch,
                record.room_sequence,
                None if is_room else revision,
                None if is_room else ready_ordinal,
                revision,
            )
            assert _decrypt_event_work(journal, row) == (
                _expected_event_work_plaintext(record),
                record,
            )
            if not is_room:
                ready_ordinal += 1
        assert tuple(
            record.origin.frame_index for record in expected if record.room_id is None
        ) == (2, 3)
        assert _frame_storage_row(journal, staged.frame_id) is None
    finally:
        bootstrap.close()


def test_materializer_null_ephemeral_only_room_backpressures_then_readies_in_order(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.SLIDING,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        first, _ = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            1,
            room_nonempty=True,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        transition, _ = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            2,
            room_nonempty=True,
            room_membership="leave",
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            transition.frame_id,
            4,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, before = _decrypt_aggregate(journal, aggregate_rows[0])
        assert type(before) is _values().RoomAggregateValue
        assert before.pending_hydration is None
        assert before.continuity.hydration_id is None
        existing_work = _work_rows(journal)
        assert existing_work
        assert all(row[2] == "ready" for row in existing_work)

        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            3,
            crypto=True,
            ephemeral_only=True,
            global_ready_count=1,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (before.continuity,),
        )
        assert normalized.room_segments == ()
        assert proposal.room_proposals == ()
        assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
            DescriptorRoute.READY,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        raw_before = _frame_storage_row(journal, staged.frame_id)
        assert raw_before is not None
        owner_before = journal.load_owner()
        aggregate_before = _aggregate_rows(journal)
        work_before = _work_rows(journal)
        projected_count = len(work_before) + len(proposal.descriptors)
        statements.clear()

        ready_limited = replace(
            MaterializerLimits(),
            max_ready_work_count=projected_count - 1,
            max_total_work_count=projected_count,
        )
        assert _materialize(journal, limits=ready_limited) == MaterializeResult(
            MaterializeStatus.AT_CAPACITY,
            staged.frame_id,
            None,
        )
        assert journal.load_owner() == owner_before
        assert _aggregate_rows(journal) == aggregate_before
        assert _work_rows(journal) == work_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _materializer_dml(statements) == ()

        statements.clear()
        total_limited = replace(
            ready_limited,
            max_ready_work_count=projected_count,
            max_total_work_count=projected_count - 1,
        )
        assert _materialize(journal, limits=total_limited) == MaterializeResult(
            MaterializeStatus.AT_CAPACITY,
            staged.frame_id,
            None,
        )
        assert journal.load_owner() == owner_before
        assert _aggregate_rows(journal) == aggregate_before
        assert _work_rows(journal) == work_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _materializer_dml(statements) == ()

        statements.clear()
        result = _materialize(
            journal,
            limits=replace(
                total_limited,
                max_ready_work_count=projected_count,
                max_total_work_count=projected_count,
            ),
        )

        revision = owner_before.revision + 1
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            revision,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, after = _decrypt_aggregate(journal, aggregate_rows[0])
        assert after == _values().RoomAggregateValue(
            before.continuity,
            before.next_room_sequence + 2,
            revision,
            None,
        )
        target_rows = tuple(
            row for row in _work_rows(journal) if row[3] == str(staged.frame_id)
        )
        assert len(target_rows) == 4
        assert tuple(row[8] for row in target_rows) == (0, 1, 2, 3)
        expected_ids = tuple(
            str(
                uuid5(
                    staged.frame_id,
                    f"event:frame:{staged.frame_id}:{index}",
                )
            )
            for index in range(4)
        )
        assert tuple(row[0] for row in target_rows) == expected_ids
        expected_sequences = (before.next_room_sequence, before.next_room_sequence + 1)
        for index, (row, descriptor) in enumerate(
            zip(target_rows, proposal.descriptors, strict=True)
        ):
            is_room = index < 2
            record = EventRecord(
                expected_ids[index],
                descriptor.kind,
                replace(normalized.origin, frame_index=index),
                descriptor.room_id,
                before.continuity.membership_epoch if is_room else None,
                expected_sequences[index] if is_room else None,
                None,
                None,
                descriptor.source_json,
                None,
            )
            assert row[1:10] == (
                "event",
                "ready",
                str(staged.frame_id),
                record.room_id,
                record.membership_epoch,
                record.room_sequence,
                revision,
                index,
                revision,
            )
            assert _decrypt_event_work(journal, row) == (
                _expected_event_work_plaintext(record),
                record,
            )
        assert tuple(row[8] for row in target_rows[2:]) == (2, 3)
        raw_after = _frame_storage_row(journal, staged.frame_id)
        assert raw_after is not None
        assert raw_after[:6] == raw_before[:6]
        assert raw_after[6] == revision
        assert raw_after[7] != raw_before[7]
        assert (
            EncryptedRowCodec(
                "discovery-secret",
                journal.account_id,
                journal.load_owner().stream_id,
            ).decrypt(
                "NioIngestFrameDrainHeader",
                (staged.frame_id,),
                raw_after[7],
                hashlib.sha256(b"").digest(),
                header=_canonical_expected_drain_header(raw_after, revision),
            )
            == b""
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "held_trigger",
    ["count", "canonical-bytes"],
    ids=("count", "canonical-bytes"),
)
def test_materializer_pending_ephemeral_only_held_overflow_is_terminal(
    tmp_path: Path,
    held_trigger: str,
) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.SLIDING)
    journal = bootstrap._journal
    try:
        first, first_normalized = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            1,
            room_nonempty=True,
            room_ephemeral=True,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, before = _decrypt_aggregate(journal, aggregate_rows[0])
        assert type(before) is _values().RoomAggregateValue
        assert before.pending_hydration is not None
        selected_rows = tuple(row for row in _work_rows(journal) if row[2] == "held")
        assert len(selected_rows) == 2
        selected = tuple(
            sorted(
                ((row, *_decrypt_event_work(journal, row)) for row in selected_rows),
                key=lambda item: (item[2].room_sequence, item[2].record_id),
            )
        )
        assert tuple(item[2].room_sequence for item in selected) == (0, 1)
        unrelated = EventRecord(
            str(uuid5(first.frame_id, "ephemeral-only-unrelated-held")),
            RecordKind.ROOM_ACCOUNT_DATA,
            replace(first_normalized.origin, frame_index=2),
            "!ephemeral-only-unrelated:example.org",
            0,
            0,
            None,
            None,
            b'{"content":{},"type":"m.tag"}',
            None,
        )
        unrelated_plaintext = _expected_event_work_plaintext(unrelated)
        _insert_authenticated_event_work(
            journal,
            unrelated,
            frame_id=first.frame_id,
            ready_revision=None,
            ready_ordinal=None,
            created_revision=2,
            status="held",
        )
        unrelated_before = next(
            row for row in _work_rows(journal) if row[0] == unrelated.record_id
        )

        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            2,
            ephemeral_only=True,
            global_ready_count=1,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (before.continuity,),
        )
        assert normalized.room_segments == ()
        assert proposal.room_proposals == ()
        assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        incoming = tuple(
            EventRecord(
                str(
                    uuid5(
                        staged.frame_id,
                        f"event:frame:{staged.frame_id}:{index}",
                    )
                ),
                descriptor.kind,
                replace(normalized.origin, frame_index=index),
                descriptor.room_id,
                before.continuity.membership_epoch,
                before.next_room_sequence + index,
                None,
                None,
                descriptor.source_json,
                None,
            )
            for index, descriptor in enumerate(proposal.descriptors[:2])
        )
        globals_ = tuple(
            EventRecord(
                str(
                    uuid5(
                        staged.frame_id,
                        f"event:frame:{staged.frame_id}:{index}",
                    )
                ),
                descriptor.kind,
                replace(normalized.origin, frame_index=index),
                None,
                None,
                None,
                None,
                None,
                descriptor.source_json,
                None,
            )
            for index, descriptor in enumerate(proposal.descriptors)
            if descriptor.room_id is None
        )
        assert tuple(record.origin.frame_index for record in globals_) == (2, 3)
        loss_without_id = LossRecord(
            "",
            normalized.origin,
            before.continuity.room_id,
            before.continuity.membership_epoch,
            LossReason.EVENT_LIMIT,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        expected_loss = replace(
            loss_without_id,
            loss_id=_loss_id(journal.load_owner().stream_id, loss_without_id),
        )
        selected_boundary_bytes = sum(len(item[1]) for item in selected) + sum(
            len(_expected_event_work_plaintext(record)) for record in incoming
        )
        capacity: dict[str, int] = {
            "max_ready_work_count": 1,
            "max_ready_work_canonical_bytes": 1,
            "max_total_work_count": 1,
            "max_total_work_canonical_bytes": 1,
        }
        if held_trigger == "count":
            capacity["max_held_work_count"] = 4
            assert len(selected) + len(incoming) == 4
            assert len(selected) + len(incoming) + 1 == 5
        else:
            assert held_trigger == "canonical-bytes"
            capacity["max_held_work_canonical_bytes"] = (
                selected_boundary_bytes + len(unrelated_plaintext) - 1
            )
            assert selected_boundary_bytes < capacity["max_held_work_canonical_bytes"]
            assert selected_boundary_bytes + len(unrelated_plaintext) == (
                capacity["max_held_work_canonical_bytes"] + 1
            )
        raw_before = _frame_storage_row(journal, staged.frame_id)
        assert raw_before is not None
        owner_before = journal.load_owner()

        result = _materialize(
            journal,
            limits=replace(MaterializerLimits(), **capacity),
        )

        revision = owner_before.revision + 1
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            revision,
        )
        _, after = _decrypt_aggregate(journal, _aggregate_rows(journal)[0])
        assert after == _values().RoomAggregateValue(
            replace(before.continuity, hydration_id=None),
            before.next_room_sequence,
            revision,
            None,
        )
        rows = _work_rows(journal)
        assert {record.record_id for record in incoming}.isdisjoint(
            str(row[0]) for row in rows
        )
        unrelated_after = next(row for row in rows if row[0] == unrelated.record_id)
        assert unrelated_after == unrelated_before
        ready = tuple(row for row in rows if row[2] == "ready")
        expected_records: tuple[EventRecord | LossRecord, ...] = (
            expected_loss,
            *(item[2] for item in selected),
            *globals_,
        )
        assert tuple(row[8] for row in ready) == tuple(range(5))
        assert tuple(row[0] for row in ready) == tuple(
            record.loss_id if type(record) is LossRecord else record.record_id
            for record in expected_records
        )
        for row, record in zip(ready, expected_records, strict=True):
            assert _decrypt_work(journal, row) == (
                (
                    _expected_loss_work_plaintext(record)
                    if type(record) is LossRecord
                    else _expected_event_work_plaintext(record)
                ),
                record,
            )
        for ordinal, (old_row, old_plaintext, old_record) in enumerate(selected, 1):
            released = ready[ordinal]
            assert released[:2] == old_row[:2]
            assert released[2] == "ready"
            assert released[3:7] == old_row[3:7]
            assert released[7:10] == (revision, ordinal, old_row[9])
            assert released[11] == old_row[11]
            assert _decrypt_event_work(journal, released) == (
                old_plaintext,
                old_record,
            )
        assert _frame_storage_row(journal, staged.frame_id) is None
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("transport", "crypto"),
    [
        (TransportKind.CLASSIC, False),
        (TransportKind.SLIDING, True),
    ],
    ids=("classic-plain", "sliding-crypto"),
)
def test_materializer_repeated_hydration_preserves_original_intent_and_continuity(
    tmp_path: Path,
    transport: TransportKind,
    crypto: bool,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        transport,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        first, first_normalized = _stage_discovery_frame(
            journal,
            transport,
            1,
            crypto=crypto,
            room_nonempty=True,
            global_ready_count=1,
        )
        assert first.staged_revision == 1
        first_proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            first.frame_id,
            first_normalized,
            (),
        )
        assert tuple(descriptor.route for descriptor in first_proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        first_room = first_proposal.room_proposals[0]
        assert first_room.hydration is not None
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )

        first_aggregate_rows = _aggregate_rows(journal)
        assert len(first_aggregate_rows) == 1
        _, first_aggregate = _decrypt_aggregate(journal, first_aggregate_rows[0])
        assert first_aggregate == _values().RoomAggregateValue(
            first_room.after,
            1,
            2,
            first_room.hydration,
        )
        stored_pending = first_aggregate.pending_hydration
        assert stored_pending is not None
        first_work = _work_rows(journal)
        assert len(first_work) == 3

        second, second_normalized = _stage_discovery_frame(
            journal,
            transport,
            2,
            crypto=crypto,
            room_nonempty=True,
            global_ready_count=1,
        )
        assert second.staged_revision == 3
        second_proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            second.frame_id,
            second_normalized,
            (first_aggregate.continuity,),
        )
        assert tuple(
            descriptor.route for descriptor in second_proposal.descriptors
        ) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        second_room = second_proposal.room_proposals[0]
        assert second_room.before == first_aggregate.continuity
        assert second_room.hydration is not None
        assert second_room.hydration.hydration_id == stored_pending.hydration_id
        assert second_room.hydration.origin == second_normalized.origin
        assert second_room.hydration.origin != stored_pending.origin
        second_raw_before = _frame_storage_row(journal, second.frame_id)
        assert second_raw_before is not None
        room_descriptor = second_proposal.descriptors[0]
        assert room_descriptor.room_id == second_room.after.room_id
        boundary_record = EventRecord(
            str(uuid5(second.frame_id, f"event:{room_descriptor.descriptor_key}")),
            room_descriptor.kind,
            replace(second_normalized.origin, frame_index=0),
            room_descriptor.room_id,
            second_room.after.membership_epoch,
            1,
            None,
            room_descriptor.provenance,
            room_descriptor.source_json,
            None,
        )
        held_boundary_bytes = sum(
            len(_decrypt_event_work(journal, row)[0])
            for row in first_work
            if row[2] == "held"
        ) + len(_expected_event_work_plaintext(boundary_record))
        statements.clear()

        assert _materialize(
            journal,
            limits=replace(
                MaterializerLimits(),
                max_held_work_count=2,
                max_held_work_canonical_bytes=held_boundary_bytes,
            ),
        ) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            second.frame_id,
            4,
        )
        aggregate_dml = tuple(
            statement.strip().upper()
            for statement in _materializer_dml(statements)
            if "NioIngestRoomAggregate" in statement
        )
        assert len(aggregate_dml) == 1
        assert aggregate_dml[0].startswith("UPDATE NIOINGESTROOMAGGREGATE ")
        assert not any(
            keyword in aggregate_dml[0] for keyword in ("INSERT", "REPLACE", "UPSERT")
        )

        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        assert aggregate_rows[0][:3] == (second_room.after.room_id, 4, "hydration")
        aggregate_plaintext, aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        expected_aggregate = _values().RoomAggregateValue(
            second_room.after,
            2,
            4,
            stored_pending,
        )
        assert aggregate == expected_aggregate
        assert aggregate.pending_hydration == first_room.hydration
        assert aggregate.pending_hydration != second_room.hydration
        assert aggregate_plaintext == _rows()._canonical_room_aggregate_plaintext(
            expected_aggregate
        )

        work_rows = _work_rows(journal)
        assert len(work_rows) == 6
        expected_work_ids = {
            str(uuid5(frame_id, f"event:{descriptor.descriptor_key}"))
            for frame_id, proposal in (
                (first.frame_id, first_proposal),
                (second.frame_id, second_proposal),
            )
            for descriptor in proposal.descriptors
        }
        assert {str(row[0]) for row in work_rows} == expected_work_ids
        assert {tuple(row) for row in work_rows if row[3] == str(first.frame_id)} == {
            tuple(row) for row in first_work
        }
        second_rows = {
            str(row[0]): row for row in work_rows if row[3] == str(second.frame_id)
        }
        assert len(second_rows) == 3
        expected_second_ids: list[str] = []
        ready_ordinal = 0
        for index, descriptor in enumerate(second_proposal.descriptors):
            work_id = str(uuid5(second.frame_id, f"event:{descriptor.descriptor_key}"))
            expected_second_ids.append(work_id)
            is_room = descriptor.room_id is not None
            expected_record = EventRecord(
                work_id,
                descriptor.kind,
                replace(second_normalized.origin, frame_index=index),
                descriptor.room_id,
                second_room.after.membership_epoch if is_room else None,
                1 if is_room else None,
                None,
                descriptor.provenance,
                descriptor.source_json,
                None,
            )
            row = second_rows[work_id]
            assert row[1:10] == (
                "event",
                "held" if is_room else "ready",
                str(second.frame_id),
                descriptor.room_id,
                second_room.after.membership_epoch if is_room else None,
                1 if is_room else None,
                None if is_room else 4,
                None if is_room else ready_ordinal,
                4,
            )
            plaintext, record = _decrypt_event_work(journal, row)
            assert record == expected_record
            assert plaintext == _rows()._canonical_work_plaintext(
                "event", expected_record
            )
            if not is_room:
                ready_ordinal += 1
        assert set(second_rows) == set(expected_second_ids)
        assert tuple(
            row[8]
            for row in work_rows
            if row[3] == str(second.frame_id) and row[2] == "ready"
        ) == (0, 1)
        assert sorted(int(row[6]) for row in work_rows if row[2] == "held") == [0, 1]

        second_raw_after = _frame_storage_row(journal, second.frame_id)
        if crypto:
            assert second_raw_after is not None
            assert second_raw_after[:6] == second_raw_before[:6]
            assert second_raw_after[6] == 4
            assert second_raw_after[7] != second_raw_before[7]
            assert (
                EncryptedRowCodec(
                    "discovery-secret",
                    journal.account_id,
                    journal.load_owner().stream_id,
                ).decrypt(
                    "NioIngestFrameDrainHeader",
                    (second.frame_id,),
                    second_raw_after[7],
                    hashlib.sha256(b"").digest(),
                    header=_canonical_expected_drain_header(second_raw_after, 4),
                )
                == b""
            )
        else:
            assert second_raw_after is None
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("transport", "crypto"),
    [
        (TransportKind.CLASSIC, False),
        (TransportKind.SLIDING, True),
    ],
    ids=("classic-plain", "sliding-crypto"),
)
@pytest.mark.parametrize(
    "held_trigger",
    ["count", "canonical-bytes"],
    ids=("count", "canonical-bytes"),
)
def test_materializer_pending_hydration_held_overflow_is_one_terminal_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transport: TransportKind,
    crypto: bool,
    held_trigger: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        transport,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        first, first_normalized = _stage_discovery_frame(
            journal,
            transport,
            1,
            crypto=crypto,
            room_nonempty=True,
            room_ephemeral=True,
        )
        first_proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            first.frame_id,
            first_normalized,
            (),
        )
        assert tuple(item.route for item in first_proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.HOLD_FOR_HYDRATION,
        )
        assert tuple(item.kind for item in first_proposal.descriptors) == (
            RecordKind.TIMELINE,
            RecordKind.EPHEMERAL,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )

        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, first_aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        assert type(first_aggregate) is _values().RoomAggregateValue
        assert first_aggregate.next_room_sequence == 2
        assert first_aggregate.pending_hydration is not None
        held_rows = tuple(row for row in _work_rows(journal) if row[2] == "held")
        assert len(held_rows) == 2
        held_records = tuple(_decrypt_event_work(journal, row) for row in held_rows)
        held_by_sequence = tuple(
            sorted(
                zip(held_rows, held_records, strict=True),
                key=lambda item: (item[1][1].room_sequence, item[1][1].record_id),
            )
        )
        assert tuple(item[1][1].room_sequence for item in held_by_sequence) == (0, 1)
        unrelated_record = EventRecord(
            str(uuid5(first.frame_id, "unrelated-held")),
            RecordKind.ROOM_ACCOUNT_DATA,
            replace(first_normalized.origin, frame_index=2),
            "!unrelated:example.org",
            0,
            0,
            None,
            None,
            b'{"content":{"tags":{}},"type":"m.tag"}',
            None,
        )
        unrelated_plaintext = _expected_event_work_plaintext(unrelated_record)
        _insert_authenticated_event_work(
            journal,
            unrelated_record,
            frame_id=first.frame_id,
            ready_revision=None,
            ready_ordinal=None,
            created_revision=2,
            status="held",
        )
        unrelated_storage_before = next(
            row for row in _work_rows(journal) if row[0] == unrelated_record.record_id
        )
        assert _decrypt_event_work(journal, unrelated_storage_before) == (
            unrelated_plaintext,
            unrelated_record,
        )

        second, second_normalized = _stage_discovery_frame(
            journal,
            transport,
            2,
            crypto=crypto,
            room_nonempty=True,
            room_ephemeral=True,
            global_ready_count=1,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            second.frame_id,
            second_normalized,
            (first_aggregate.continuity,),
        )
        assert len(proposal.room_proposals) == 1
        room = proposal.room_proposals[0]
        assert room.before == first_aggregate.continuity
        assert room.after == room.before
        assert room.recovery is None
        assert room.hydration is not None
        assert room.hydration.hydration_id == room.before.hydration_id
        assert room.hydration.origin == second_normalized.origin
        assert room.retirement_epoch is None
        assert room.losses == ()
        assert room.release is RecoveryRelease.NONE
        assert tuple(item.route for item in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        assert tuple(item.kind for item in proposal.descriptors) == (
            RecordKind.TIMELINE,
            RecordKind.EPHEMERAL,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordKind.PRESENCE,
        )
        expected_incoming = tuple(
            EventRecord(
                str(uuid5(second.frame_id, f"event:{descriptor.descriptor_key}")),
                descriptor.kind,
                replace(second_normalized.origin, frame_index=index),
                room.after.room_id,
                room.after.membership_epoch,
                first_aggregate.next_room_sequence + room_index,
                None,
                descriptor.provenance,
                descriptor.source_json,
                None,
            )
            for room_index, (index, descriptor) in enumerate(
                (
                    (index, descriptor)
                    for index, descriptor in enumerate(proposal.descriptors)
                    if descriptor.room_id == room.after.room_id
                )
            )
        )
        assert len(expected_incoming) == 2
        incoming_room_ids = {record.record_id for record in expected_incoming}

        expected_globals = tuple(
            EventRecord(
                str(uuid5(second.frame_id, f"event:{descriptor.descriptor_key}")),
                descriptor.kind,
                replace(second_normalized.origin, frame_index=index),
                None,
                None,
                None,
                None,
                None,
                descriptor.source_json,
                None,
            )
            for index, descriptor in enumerate(proposal.descriptors)
            if descriptor.room_id is None
        )
        assert tuple(record.origin.frame_index for record in expected_globals) == (2, 3)
        loss_without_id = LossRecord(
            "",
            second_normalized.origin,
            room.after.room_id,
            room.after.membership_epoch,
            LossReason.EVENT_LIMIT,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        expected_loss = replace(
            loss_without_id,
            loss_id=_loss_id(journal.load_owner().stream_id, loss_without_id),
        )
        owner_before = journal.load_owner()
        assert owner_before.revision == 3
        raw_before = _frame_storage_row(journal, second.frame_id)
        assert raw_before is not None
        writer_entries: list[None] = []
        real_journal_write = type(journal._owner).journal_write

        @contextmanager
        def count_writer(owner: object) -> Iterator[None]:
            assert owner is journal._owner
            writer_entries.append(None)
            with real_journal_write(journal._owner):
                yield

        selected_held_boundary_bytes = sum(
            len(item[1][0]) for item in held_by_sequence
        ) + sum(
            len(_expected_event_work_plaintext(record)) for record in expected_incoming
        )
        capacity_limits: dict[str, int] = {
            "max_ready_work_count": 1,
            "max_ready_work_canonical_bytes": 1,
            "max_total_work_count": 1,
            "max_total_work_canonical_bytes": 1,
        }
        if held_trigger == "count":
            capacity_limits["max_held_work_count"] = 4
            assert len(held_by_sequence) + len(expected_incoming) == 4
            assert len(held_by_sequence) + len(expected_incoming) + 1 == 5
        else:
            assert held_trigger == "canonical-bytes"
            capacity_limits["max_held_work_canonical_bytes"] = (
                selected_held_boundary_bytes + len(unrelated_plaintext) - 1
            )
            assert selected_held_boundary_bytes < (
                capacity_limits["max_held_work_canonical_bytes"]
            )
            assert selected_held_boundary_bytes + len(unrelated_plaintext) == (
                capacity_limits["max_held_work_canonical_bytes"] + 1
            )

        statements.clear()
        with monkeypatch.context() as guard:
            guard.setattr(type(journal._owner), "journal_write", count_writer)
            result = _materialize(
                journal,
                limits=replace(MaterializerLimits(), **capacity_limits),
            )

        revision = 4
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            second.frame_id,
            revision,
        )
        assert writer_entries == [None]
        assert journal.load_owner() == replace(owner_before, revision=revision)

        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        assert aggregate_rows[0][:3] == (room.after.room_id, revision, None)
        aggregate_plaintext, aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        expected_aggregate = _values().RoomAggregateValue(
            replace(room.after, hydration_id=None),
            first_aggregate.next_room_sequence,
            revision,
            None,
        )
        assert aggregate == expected_aggregate
        assert aggregate_plaintext == _rows()._canonical_room_aggregate_plaintext(
            expected_aggregate
        )

        work_rows = _work_rows(journal)
        assert len(work_rows) == 6
        assert incoming_room_ids.isdisjoint(str(row[0]) for row in work_rows)
        unrelated_storage_after = next(
            row for row in work_rows if row[0] == unrelated_record.record_id
        )
        assert unrelated_storage_after == unrelated_storage_before
        assert unrelated_storage_after[2] == "held"
        ready_rows = tuple(row for row in work_rows if row[2] == "ready")
        expected_records: tuple[EventRecord | LossRecord, ...] = (
            expected_loss,
            *(item[1][1] for item in held_by_sequence),
            *expected_globals,
        )
        assert len(ready_rows) == 5
        assert tuple(row[8] for row in ready_rows) == tuple(range(5))
        assert tuple(row[0] for row in ready_rows) == tuple(
            record.loss_id if type(record) is LossRecord else record.record_id
            for record in expected_records
        )
        assert tuple(row[1] for row in ready_rows) == (
            "loss",
            "event",
            "event",
            "event",
            "event",
        )
        assert tuple(row[3] for row in ready_rows) == (
            str(second.frame_id),
            str(first.frame_id),
            str(first.frame_id),
            str(second.frame_id),
            str(second.frame_id),
        )
        assert tuple(row[9] for row in ready_rows) == (
            revision,
            2,
            2,
            revision,
            revision,
        )
        for row, record in zip(ready_rows, expected_records, strict=True):
            plaintext, stored = _decrypt_work(journal, row)
            assert stored == record
            assert plaintext == (
                _expected_loss_work_plaintext(record)
                if type(record) is LossRecord
                else _expected_event_work_plaintext(record)
            )

        for ordinal, (old_row, (old_plaintext, old_record)) in enumerate(
            held_by_sequence,
            1,
        ):
            released = ready_rows[ordinal]
            assert released[:2] == old_row[:2]
            assert released[2] == "ready"
            assert released[3:7] == old_row[3:7]
            assert released[7:10] == (revision, ordinal, old_row[9])
            assert released[10] != old_row[10]
            assert released[11] == old_row[11]
            assert _decrypt_event_work(journal, released) == (
                old_plaintext,
                old_record,
            )

        raw_after = _frame_storage_row(journal, second.frame_id)
        if crypto:
            assert raw_after is not None
            assert raw_after[:6] == raw_before[:6]
            assert raw_after[6] == revision
            assert raw_after[7] != raw_before[7]
            assert (
                EncryptedRowCodec(
                    "discovery-secret",
                    journal.account_id,
                    journal.load_owner().stream_id,
                ).decrypt(
                    "NioIngestFrameDrainHeader",
                    (second.frame_id,),
                    raw_after[7],
                    hashlib.sha256(b"").digest(),
                    header=_canonical_expected_drain_header(raw_after, revision),
                )
                == b""
            )
        else:
            assert raw_after is None

        dml = _materializer_dml(statements)
        assert sum("UPDATE NioIngestMeta" in statement for statement in dml) == 1
        assert sum("NioIngestRoomAggregate" in statement for statement in dml) == 1
        assert sum("NioIngestFrame" in statement for statement in dml) == 1
    finally:
        bootstrap.close()


def test_materializer_pending_hydration_terminal_does_not_swallow_global_oversize(
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
        first, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=True,
            room_ephemeral=True,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        held_before = tuple(row for row in _work_rows(journal) if row[2] == "held")
        assert len(held_before) == 2

        second, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            room_nonempty=True,
            room_ephemeral=True,
            global_ready_count=1,
            padding_bytes=256,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            second.frame_id,
            normalized,
            (aggregate.continuity,),
        )
        assert tuple(item.route for item in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        records: list[EventRecord] = []
        room_sequence = aggregate.next_room_sequence
        for index, descriptor in enumerate(proposal.descriptors):
            is_room = descriptor.room_id is not None
            records.append(
                EventRecord(
                    str(uuid5(second.frame_id, f"event:{descriptor.descriptor_key}")),
                    descriptor.kind,
                    replace(normalized.origin, frame_index=index),
                    descriptor.room_id,
                    aggregate.continuity.membership_epoch if is_room else None,
                    room_sequence if is_room else None,
                    None,
                    descriptor.provenance,
                    descriptor.source_json,
                    None,
                )
            )
            if is_room:
                room_sequence += 1
        oversized_global = next(
            record
            for record in records
            if record.kind is RecordKind.GLOBAL_ACCOUNT_DATA
        )
        loss_without_id = LossRecord(
            "",
            normalized.origin,
            aggregate.continuity.room_id,
            aggregate.continuity.membership_epoch,
            LossReason.EVENT_LIMIT,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        expected_loss = replace(
            loss_without_id,
            loss_id=_loss_id(journal.load_owner().stream_id, loss_without_id),
        )
        max_record_bytes = max(
            len(_expected_loss_work_plaintext(expected_loss)),
            *(
                len(_expected_event_work_plaintext(record))
                for record in records
                if record is not oversized_global
            ),
        )
        assert len(_expected_event_work_plaintext(oversized_global)) > max_record_bytes
        assert (
            len(held_before)
            + sum(descriptor.room_id is not None for descriptor in proposal.descriptors)
            == 4
        )
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, second.frame_id)
        aggregate_before = _aggregate_rows(journal)
        work_before = _work_rows(journal)

        def reject_writer(_owner: object) -> NoReturn:
            raise AssertionError("oversized global terminal candidate entered writer")

        monkeypatch.setattr(type(journal._owner), "journal_write", reject_writer)
        statements.clear()

        with pytest.raises(JournalIntegrityError):
            _materialize(
                journal,
                limits=replace(
                    MaterializerLimits(),
                    max_record_canonical_bytes=max_record_bytes,
                    max_held_work_count=3,
                ),
            )

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, second.frame_id) == raw_before
        assert _aggregate_rows(journal) == aggregate_before
        assert _work_rows(journal) == work_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


def test_materializer_pending_hydration_room_oversize_precedes_held_limit(
    tmp_path: Path,
) -> None:
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
    )
    journal = bootstrap._journal
    try:
        first, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=True,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, first_aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        held_before = tuple(row for row in _work_rows(journal) if row[2] == "held")
        assert len(held_before) == 1
        held_plaintext, held_record = _decrypt_event_work(journal, held_before[0])

        second, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            room_nonempty=True,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            second.frame_id,
            normalized,
            (first_aggregate.continuity,),
        )
        assert len(proposal.room_proposals) == 1
        room = proposal.room_proposals[0]
        assert room.after.hydration_id == room.before.hydration_id
        assert room.retirement_epoch is None
        assert room.losses == ()
        assert room.release is RecoveryRelease.NONE
        assert len(proposal.descriptors) == 1
        descriptor = proposal.descriptors[0]
        assert descriptor.route is DescriptorRoute.HOLD_FOR_HYDRATION
        incoming = EventRecord(
            str(uuid5(second.frame_id, f"event:{descriptor.descriptor_key}")),
            descriptor.kind,
            replace(normalized.origin, frame_index=0),
            room.after.room_id,
            room.after.membership_epoch,
            first_aggregate.next_room_sequence,
            None,
            descriptor.provenance,
            descriptor.source_json,
            None,
        )
        loss_without_id = LossRecord(
            "",
            normalized.origin,
            room.after.room_id,
            room.after.membership_epoch,
            LossReason.OVERSIZED_EVENT,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        expected_loss = replace(
            loss_without_id,
            loss_id=_loss_id(journal.load_owner().stream_id, loss_without_id),
        )
        loss_plaintext = _expected_loss_work_plaintext(expected_loss)
        incoming_plaintext = _expected_event_work_plaintext(incoming)
        assert len(loss_plaintext) < len(incoming_plaintext)
        max_record_bytes = len(loss_plaintext)
        assert max_record_bytes < len(incoming_plaintext)
        owner_before = journal.load_owner()

        result = _materialize(
            journal,
            limits=replace(
                MaterializerLimits(),
                max_record_canonical_bytes=max_record_bytes,
                max_held_work_count=1,
            ),
        )

        revision = owner_before.revision + 1
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            second.frame_id,
            revision,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        assert aggregate == _values().RoomAggregateValue(
            replace(room.after, hydration_id=None),
            first_aggregate.next_room_sequence,
            revision,
            None,
        )
        work_rows = _work_rows(journal)
        assert incoming.record_id not in {str(row[0]) for row in work_rows}
        ready_rows = tuple(row for row in work_rows if row[2] == "ready")
        assert tuple(row[0] for row in ready_rows) == (
            expected_loss.loss_id,
            held_record.record_id,
        )
        assert tuple(row[8] for row in ready_rows) == (0, 1)
        assert _decrypt_work(journal, ready_rows[0]) == (
            loss_plaintext,
            expected_loss,
        )
        assert _decrypt_event_work(journal, ready_rows[1]) == (
            held_plaintext,
            held_record,
        )
        assert _frame_storage_row(journal, second.frame_id) is None
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("transport", "crypto"),
    [
        (TransportKind.CLASSIC, False),
        (TransportKind.SLIDING, True),
    ],
    ids=("classic-plain", "sliding-crypto"),
)
def test_materializer_pending_hydration_membership_transition_is_one_ordered_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    transport: TransportKind,
    crypto: bool,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        transport,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        first, first_normalized = _stage_discovery_frame(
            journal,
            transport,
            1,
            crypto=crypto,
            room_nonempty=True,
            global_ready_count=1,
        )
        first_proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            first.frame_id,
            first_normalized,
            (),
        )
        first_room = first_proposal.room_proposals[0]
        assert first_room.hydration is not None
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, first_aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        first_work = _work_rows(journal)
        held_before = tuple(row for row in first_work if row[2] == "held")
        assert len(held_before) == 1
        held_row = held_before[0]
        held_plaintext, held_record = _decrypt_event_work(journal, held_row)
        assert held_record.room_sequence == 0

        transition, transition_normalized = _stage_discovery_frame(
            journal,
            transport,
            2,
            crypto=crypto,
            room_nonempty=True,
            room_membership="leave",
            global_ready_count=1,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            transition.frame_id,
            transition_normalized,
            (first_aggregate.continuity,),
        )
        assert len(proposal.room_proposals) == 1
        room = proposal.room_proposals[0]
        assert room.before == first_aggregate.continuity
        assert room.after == RoomContinuity(
            room.after.room_id,
            1,
            "leave",
            None,
            None,
            None,
        )
        assert room.retirement_epoch == 0
        assert room.recovery is None
        assert room.hydration is None
        assert room.release is RecoveryRelease.LOSS_THEN_HELD
        assert len(room.losses) == 1
        assert room.losses[0].reason is LossReason.UNVERIFIABLE
        assert room.losses[0].boundary == LossBoundary(None, None, None, None)
        assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_RETIREMENT,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        assert tuple(descriptor.kind for descriptor in proposal.descriptors) == (
            RecordKind.TIMELINE,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordKind.PRESENCE,
        )
        owner_before = journal.load_owner()
        assert owner_before.revision == 3
        raw_before = _frame_storage_row(journal, transition.frame_id)
        assert raw_before is not None
        writer_entries: list[None] = []
        real_journal_write = type(journal._owner).journal_write

        @contextmanager
        def count_writer(owner: object) -> Iterator[None]:
            assert owner is journal._owner
            writer_entries.append(None)
            with real_journal_write(journal._owner):
                yield

        statements.clear()
        with monkeypatch.context() as guard:
            guard.setattr(type(journal._owner), "journal_write", count_writer)
            result = _materialize(
                journal,
                limits=replace(
                    MaterializerLimits(),
                    max_held_work_count=1,
                    max_held_work_canonical_bytes=1,
                    max_ready_work_count=1,
                    max_ready_work_canonical_bytes=1,
                    max_total_work_count=1,
                    max_total_work_canonical_bytes=1,
                ),
            )

        revision = 4
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            transition.frame_id,
            revision,
        )
        assert writer_entries == [None]
        assert journal.load_owner() == replace(owner_before, revision=revision)
        raw_after = _frame_storage_row(journal, transition.frame_id)
        if crypto:
            assert raw_after is not None
            assert raw_after[:6] == raw_before[:6]
            assert raw_after[6] == revision
            assert raw_after[7] != raw_before[7]
            assert (
                EncryptedRowCodec(
                    "discovery-secret",
                    journal.account_id,
                    journal.load_owner().stream_id,
                ).decrypt(
                    "NioIngestFrameDrainHeader",
                    (transition.frame_id,),
                    raw_after[7],
                    hashlib.sha256(b"").digest(),
                    header=_canonical_expected_drain_header(raw_after, revision),
                )
                == b""
            )
        else:
            assert raw_after is None

        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        assert aggregate_rows[0][:3] == (room.after.room_id, revision, None)
        aggregate_plaintext, aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        expected_aggregate = _values().RoomAggregateValue(
            room.after,
            3,
            revision,
            None,
        )
        assert aggregate == expected_aggregate
        assert aggregate_plaintext == _rows()._canonical_room_aggregate_plaintext(
            expected_aggregate
        )

        loss_without_id = LossRecord(
            "",
            transition_normalized.origin,
            room.after.room_id,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        loss = replace(
            loss_without_id,
            loss_id=_loss_id(journal.load_owner().stream_id, loss_without_id),
        )
        lifecycle_id = str(
            uuid5(
                transition.frame_id,
                f"lifecycle:{room.after.room_id}:0:1",
            )
        )
        lifecycle = EventRecord(
            lifecycle_id,
            RecordKind.ROOM_LIFECYCLE,
            transition_normalized.origin,
            room.after.room_id,
            1,
            1,
            None,
            None,
            canonical_json(
                {
                    "membership": "leave",
                    "membership_epoch": 1,
                    "previous_membership_epoch": 0,
                }
            ),
            None,
        )
        descriptor_records: list[EventRecord] = []
        for index, descriptor in enumerate(proposal.descriptors):
            is_room = descriptor.room_id is not None
            descriptor_records.append(
                EventRecord(
                    str(
                        uuid5(
                            transition.frame_id,
                            f"event:{descriptor.descriptor_key}",
                        )
                    ),
                    descriptor.kind,
                    replace(transition_normalized.origin, frame_index=index),
                    descriptor.room_id,
                    1 if is_room else None,
                    2 if is_room else None,
                    None,
                    descriptor.provenance,
                    descriptor.source_json,
                    None,
                )
            )
        expected_records: tuple[EventRecord | LossRecord, ...] = (
            loss,
            held_record,
            lifecycle,
            *descriptor_records,
        )
        ready_rows = tuple(row for row in _work_rows(journal) if row[7] == revision)
        assert tuple(row[8] for row in ready_rows) == tuple(range(6))
        assert tuple(row[0] for row in ready_rows) == tuple(
            record.loss_id if type(record) is LossRecord else record.record_id
            for record in expected_records
        )
        assert tuple(row[1] for row in ready_rows) == (
            "loss",
            "event",
            "event",
            "event",
            "event",
            "event",
        )
        assert tuple(row[2] for row in ready_rows) == ("ready",) * 6
        assert tuple(row[3] for row in ready_rows) == (
            str(transition.frame_id),
            str(first.frame_id),
            str(transition.frame_id),
            str(transition.frame_id),
            str(transition.frame_id),
            str(transition.frame_id),
        )
        assert tuple(row[4:7] for row in ready_rows) == (
            (room.after.room_id, 0, None),
            (room.after.room_id, 0, 0),
            (room.after.room_id, 1, 1),
            (room.after.room_id, 1, 2),
            (None, None, None),
            (None, None, None),
        )
        assert tuple(row[9] for row in ready_rows) == (
            revision,
            2,
            revision,
            revision,
            revision,
            revision,
        )
        for row, expected_record in zip(
            ready_rows,
            expected_records,
            strict=True,
        ):
            plaintext, record = _decrypt_work(journal, row)
            assert record == expected_record
            assert plaintext == (
                _expected_loss_work_plaintext(expected_record)
                if type(expected_record) is LossRecord
                else _expected_event_work_plaintext(expected_record)
            )

        released = ready_rows[1]
        assert released[:7] == (
            held_row[0],
            held_row[1],
            "ready",
            held_row[3],
            held_row[4],
            held_row[5],
            held_row[6],
        )
        assert released[7:10] == (revision, 1, held_row[9])
        assert released[10] != held_row[10]
        assert released[11] == held_row[11]
        assert _decrypt_work(journal, released)[0] == held_plaintext
        with pytest.raises(JournalIntegrityError):
            journal._codec.decrypt(
                "NioIngestWork",
                (released[0],),
                released[10],
                released[11],
                header=_work_header(journal.account_id, held_row),
            )

        dml = _materializer_dml(statements)
        assert sum("UPDATE NioIngestMeta" in statement for statement in dml) == 1
        aggregate_dml = tuple(
            statement.strip().upper()
            for statement in dml
            if "NioIngestRoomAggregate" in statement
        )
        assert len(aggregate_dml) == 1
        assert aggregate_dml[0].startswith("UPDATE NIOINGESTROOMAGGREGATE ")
        assert not any(
            keyword in aggregate_dml[0] for keyword in ("INSERT", "REPLACE", "UPSERT")
        )
        work_updates = tuple(
            statement.strip().upper()
            for statement in dml
            if statement.lstrip().upper().startswith("UPDATE NIOINGESTWORK")
        )
        assert len(work_updates) == 1
        assert not any(
            keyword in work_updates[0] for keyword in ("INSERT", "REPLACE", "UPSERT")
        )
        assert (
            sum(
                statement.lstrip().upper().startswith("INSERT INTO NIOINGESTWORK")
                for statement in dml
            )
            == 5
        )
    finally:
        bootstrap.close()


def test_materializer_pending_hydration_release_revalidates_held_row_at_writer(
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
        first, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=True,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        aggregate_before = _aggregate_rows(journal)
        assert len(aggregate_before) == 1
        _, aggregate = _decrypt_aggregate(journal, aggregate_before[0])
        work_before = _work_rows(journal)
        held_before = tuple(row for row in work_before if row[2] == "held")
        assert len(held_before) == 1
        target = held_before[0]
        plaintext, value = _decrypt_event_work(journal, target)

        transition, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            room_nonempty=True,
            room_membership="leave",
        )
        room = reduce_staged_frame(
            journal.load_owner().stream_id,
            transition.frame_id,
            normalized,
            (aggregate.continuity,),
        ).room_proposals[0]
        assert room.release is RecoveryRelease.LOSS_THEN_HELD
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, transition.frame_id)
        assert raw_before is not None
        raced = False
        real_journal_write = type(journal._owner).journal_write

        @contextmanager
        def reseal_held_before_writer(owner: object) -> Iterator[None]:
            nonlocal raced
            assert owner is journal._owner
            assert not raced
            raced = True
            ciphertext, digest = journal._codec.seal(
                "NioIngestWork",
                (target[0],),
                plaintext,
                header=_work_header(journal.account_id, target),
            )
            assert ciphertext != target[10]
            assert digest == target[11]
            with sqlite3.connect(journal.database_path) as connection:
                updated = connection.execute(
                    "UPDATE NioIngestWork SET payload_ciphertext = ?, "
                    "payload_sha256 = ? WHERE account_id = ? AND work_id = ?",
                    (
                        ciphertext,
                        digest,
                        journal.account_id,
                        target[0],
                    ),
                )
                assert updated.rowcount == 1
            with real_journal_write(journal._owner):
                yield

        monkeypatch.setattr(
            type(journal._owner),
            "journal_write",
            reseal_held_before_writer,
        )
        statements.clear()

        with pytest.raises(
            JournalIntegrityError,
            match="Work inventory snapshot changed",
        ):
            _materialize(journal)

        assert raced
        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, transition.frame_id) == raw_before
        assert _aggregate_rows(journal) == aggregate_before
        work_after = _work_rows(journal)
        changed = next(row for row in work_after if row[0] == target[0])
        assert changed[:10] == target[:10]
        assert changed[10] != target[10]
        assert changed[11] == target[11]
        assert _decrypt_event_work(journal, changed) == (plaintext, value)
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


def test_materializer_repeated_hydration_rejects_aggregate_reseal_at_writer_boundary(
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
        first, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_present=True,
        )
        assert first.staged_revision == 1
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        aggregate_before = _aggregate_rows(journal)
        assert len(aggregate_before) == 1
        plaintext_before, value_before = _decrypt_aggregate(
            journal, aggregate_before[0]
        )

        second, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            room_present=True,
        )
        assert second.staged_revision == 3
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, second.frame_id)
        assert raw_before is not None
        real_journal_write = type(journal._owner).journal_write
        raced = False

        @contextmanager
        def reseal_aggregate_before_writer(owner: object) -> Iterator[None]:
            nonlocal raced
            assert owner is journal._owner
            assert not raced
            raced = True
            row = aggregate_before[0]
            ciphertext, digest = journal._codec.seal(
                "NioIngestRoomAggregate",
                (row[0],),
                plaintext_before,
                header=_canonical_internal([row[0], row[1], row[2]]),
            )
            assert ciphertext != row[3]
            assert digest == row[4]
            with sqlite3.connect(journal.database_path) as connection:
                updated = connection.execute(
                    "UPDATE NioIngestRoomAggregate SET payload_ciphertext = ?, "
                    "payload_sha256 = ? WHERE account_id = ? AND room_id = ?",
                    (ciphertext, digest, journal.account_id, row[0]),
                )
                assert updated.rowcount == 1
            with real_journal_write(journal._owner):
                yield

        monkeypatch.setattr(
            type(journal._owner),
            "journal_write",
            reseal_aggregate_before_writer,
        )
        statements.clear()
        with pytest.raises(
            JournalIntegrityError,
            match=r"[Aa]ggregate snapshot changed",
        ):
            _materialize(journal)

        assert raced
        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, second.frame_id) == raw_before
        assert _work_rows(journal) == ()
        aggregate_after = _aggregate_rows(journal)
        assert aggregate_after[0][:3] == aggregate_before[0][:3]
        assert aggregate_after[0][3] != aggregate_before[0][3]
        assert aggregate_after[0][4] == aggregate_before[0][4]
        plaintext_after, value_after = _decrypt_aggregate(journal, aggregate_after[0])
        assert plaintext_after == plaintext_before
        assert value_after == value_before
        assert _materializer_dml(statements) == ()

        monkeypatch.undo()
        statements.clear()
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            second.frame_id,
            4,
        )
        assert _aggregate_rows(journal) == aggregate_after
        assert not any(
            "NioIngestRoomAggregate" in statement
            for statement in _materializer_dml(statements)
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "transport",
    [TransportKind.CLASSIC, TransportKind.SLIDING],
    ids=("classic", "sliding"),
)
@pytest.mark.parametrize("crypto", [False, True], ids=("plain", "crypto"))
def test_materializer_global_ready_catches_wrong_order_or_partial_owner_commit(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
            global_ready_count=2,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        )
        assert proposal.room_proposals == ()
        assert tuple(descriptor.kind for descriptor in proposal.descriptors) == (
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordKind.PRESENCE,
        )
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        assert raw_before is not None
        writer_entries: list[None] = []
        real_journal_write = type(journal._owner).journal_write

        @contextmanager
        def count_writer(owner: object) -> Iterator[None]:
            assert owner is journal._owner
            writer_entries.append(None)
            with real_journal_write(journal._owner):
                yield

        statements.clear()
        with monkeypatch.context() as guard:
            guard.setattr(type(journal._owner), "journal_write", count_writer)
            result = _materialize(journal)

        revision = owner_before.revision + 1
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            revision,
        )
        assert writer_entries == [None]
        assert journal.load_owner() == replace(owner_before, revision=revision)
        rows = _work_rows(journal)
        assert len(rows) == 3
        assert tuple(row[8] for row in rows) == (0, 1, 2)
        assert tuple(row[7] for row in rows) == (revision,) * 3
        assert tuple(row[9] for row in rows) == (revision,) * 3
        assert tuple(row[1:3] for row in rows) == (("event", "ready"),) * 3
        assert tuple(row[3] for row in rows) == (str(staged.frame_id),) * 3
        assert all(row[4:7] == (None, None, None) for row in rows)
        expected_sources = (
            b'{"content":{"generation":1,"index":0,"padding":""},'
            b'"type":"m.push_rules"}',
            b'{"content":{"generation":1,"index":1,"padding":""},'
            b'"type":"m.push_rules"}',
            b'{"content":{"presence":"online"},'
            b'"sender":"@friend:example.org","type":"m.presence"}',
        )
        for index, (row, source_json) in enumerate(
            zip(rows, expected_sources, strict=True)
        ):
            expected_id = str(
                uuid5(
                    staged.frame_id,
                    f"event:frame:{staged.frame_id}:{index}",
                )
            )
            assert row[0] == expected_id
            plaintext, record = _decrypt_event_work(journal, row)
            assert plaintext == _rows()._canonical_work_plaintext("event", record)
            assert record == EventRecord(
                expected_id,
                (RecordKind.GLOBAL_ACCOUNT_DATA if index < 2 else RecordKind.PRESENCE),
                RecordOrigin(
                    transport,
                    normalized.origin.source_epoch,
                    normalized.origin.request_id,
                    index,
                ),
                None,
                None,
                None,
                None,
                None,
                source_json,
                None,
            )

        raw_after = _frame_storage_row(journal, staged.frame_id)
        if crypto:
            assert raw_after is not None
            assert raw_after[:6] == raw_before[:6]
            assert raw_after[6] == revision
            assert raw_after[7] != raw_before[7]
        else:
            assert raw_after is None
        dml = _materializer_dml(statements)
        assert sum("UPDATE NioIngestMeta" in statement for statement in dml) == 1
        assert any("NioIngestWork" in statement for statement in dml)
        frame_dml_index = next(
            index
            for index, statement in enumerate(dml)
            if "NioIngestFrame" in statement
        )
        assert all(
            index < frame_dml_index
            for index, statement in enumerate(dml)
            if "NioIngestWork" in statement
        )

        owner_after = journal.load_owner()
        work_after = _work_rows(journal)
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.IDLE,
            None,
            None,
        )
        assert journal.load_owner() == owner_after
        assert _work_rows(journal) == work_after

        later, _ = _stage_discovery_frame(
            journal,
            transport,
            2,
            global_ready_count=1,
        )
        later_before = journal.load_owner()
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            later.frame_id,
            later_before.revision + 1,
        )
        assert len(_work_rows(journal)) == 5
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "transport",
    [TransportKind.CLASSIC, TransportKind.SLIDING],
    ids=("classic", "sliding"),
)
def test_materializer_record_limit_accepts_exact_length_and_rejects_one_less(
    tmp_path: Path,
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
            global_ready_count=1,
            padding_bytes=64,
        )
        expected_plaintexts = tuple(
            _expected_event_work_plaintext(record)
            for record in _planned_global_event_records(journal, staged, normalized)
        )
        exact_limit = max(map(len, expected_plaintexts))
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        statements.clear()

        with pytest.raises(JournalIntegrityError):
            _materialize(
                journal,
                limits=replace(
                    MaterializerLimits(),
                    max_record_canonical_bytes=exact_limit - 1,
                ),
            )

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _materializer_dml(statements) == ()

        result = _materialize(
            journal,
            limits=replace(
                MaterializerLimits(),
                max_record_canonical_bytes=exact_limit,
            ),
        )

        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            owner_before.revision + 1,
        )
        stored_plaintexts = tuple(
            _decrypt_event_work(journal, row)[0] for row in _work_rows(journal)
        )
        assert stored_plaintexts == expected_plaintexts
        assert max(map(len, stored_plaintexts)) == exact_limit
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "transport",
    [TransportKind.CLASSIC, TransportKind.SLIDING],
    ids=("classic", "sliding"),
)
def test_materializer_global_oversize_catches_writer_entry_or_partial_dml(
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
            global_ready_count=1,
            padding_bytes=256,
        )
        descriptor = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        ).descriptors[0]
        expected_id = str(uuid5(staged.frame_id, f"event:frame:{staged.frame_id}:0"))
        expected = EventRecord(
            expected_id,
            descriptor.kind,
            replace(normalized.origin, frame_index=0),
            None,
            None,
            None,
            None,
            None,
            descriptor.source_json,
            None,
        )
        assert len(_rows()._canonical_work_plaintext("event", expected)) > 128
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        statements.clear()

        def reject_writer(_self: object) -> object:
            raise AssertionError("oversized global record entered journal_write")

        monkeypatch.setattr(type(journal._owner), "journal_write", reject_writer)
        with pytest.raises(JournalIntegrityError):
            _materialize(
                journal,
                limits=replace(
                    MaterializerLimits(),
                    max_record_canonical_bytes=128,
                ),
            )

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _work_rows(journal) == ()
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("limit_name", "limit_value"),
    [
        ("max_ready_work_count", 4),
        ("max_ready_work_canonical_bytes", 1),
        ("max_total_work_count", 4),
        ("max_total_work_canonical_bytes", 1),
    ],
)
def test_materializer_global_capacity_catches_partial_plan_or_caller_limited_scan(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    limit_name: str,
    limit_value: int,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=2,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        inventory_before = _work_rows(journal)
        assert len(inventory_before) == 3
        staged, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            global_ready_count=1,
        )
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        queries: list[tuple[str, tuple[object, ...]]] = []
        work_decrypts: list[str] = []
        incrementally_fetched: list[int] = []
        real_execute = journal._execute
        real_decrypt = EncryptedRowCodec.decrypt

        class IncrementalInventoryCursor:
            def __init__(self, cursor: sqlite3.Cursor) -> None:
                self._cursor = cursor
                self._fetched = 0
                incrementally_fetched.append(0)

            def fetchone(self) -> sqlite3.Row | None:
                row = self._cursor.fetchone()
                if row is not None:
                    self._fetched += 1
                    incrementally_fetched[-1] = self._fetched
                return row

            def __iter__(self) -> Iterator[sqlite3.Row]:
                return self

            def __next__(self) -> sqlite3.Row:
                row = self.fetchone()
                if row is None:
                    raise StopIteration
                return row

            def fetchall(self) -> NoReturn:
                raise AssertionError("Work inventory used unbounded fetchall")

        def trace_execute(
            statement: str,
            parameters: tuple[object, ...] = (),
        ) -> object:
            cursor = real_execute(statement, parameters)
            if statement.lstrip().upper().startswith("SELECT") and (
                "NIOINGESTWORK" in statement.upper()
            ):
                queries.append((statement, parameters))
                if "LIMIT" in statement.upper():
                    return IncrementalInventoryCursor(cursor)
            return cursor

        def trace_decrypt(
            codec: EncryptedRowCodec,
            table: str,
            primary_key: tuple[str | int | UUID, ...],
            ciphertext: bytes,
            digest: bytes,
            header: bytes = b"",
        ) -> bytes:
            if table == "NioIngestWork":
                work_decrypts.append(str(primary_key[0]))
            return real_decrypt(codec, table, primary_key, ciphertext, digest, header)

        statements.clear()
        with monkeypatch.context() as guard:
            guard.setattr(journal, "_execute", trace_execute)
            guard.setattr(EncryptedRowCodec, "decrypt", trace_decrypt)
            result = _materialize(
                journal,
                limits=replace(MaterializerLimits(), **{limit_name: limit_value}),
            )

        assert result == MaterializeResult(
            MaterializeStatus.AT_CAPACITY,
            staged.frame_id,
            None,
        )
        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _work_rows(journal) == inventory_before
        assert _materializer_dml(statements) == ()
        assert sorted(work_decrypts) == sorted(str(row[0]) for row in inventory_before)
        assert len(queries) == 1
        assert incrementally_fetched == [len(inventory_before)]
        statement, parameters = queries[0]
        normalized_sql = " ".join(statement.upper().split())
        assert "COUNT(" not in normalized_sql
        assert "SUM(" not in normalized_sql
        assert "ORDER BY" not in normalized_sql
        projection, predicate = normalized_sql.split(" FROM NIOINGESTWORK", 1)
        assert "LENGTH(" not in projection
        if not re.fullmatch(r"SELECT (?:NIOINGESTWORK\.)?\*", projection):
            for column_name, _type, _not_null, _primary_key in _WORK_COLUMNS:
                assert re.search(rf"\b{column_name.upper()}\b", projection)
        assert re.search(r"\bWHERE\s+ACCOUNT_ID\s*=\s*\?", predicate)
        assert " AND " not in predicate
        assert " OR " not in predicate
        for forbidden in ("KIND", "STATUS", "READY_REVISION", "READY_ORDINAL"):
            assert not re.search(rf"\b{forbidden}\b", predicate)
        assert "LIMIT 20001" in normalized_sql or (
            "LIMIT ?" in normalized_sql and parameters[-1] == 20_001
        )
    finally:
        bootstrap.close()


def test_materializer_held_only_plan_ignores_existing_ready_watermarks(
    tmp_path: Path,
) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        ready_before = tuple(row for row in _work_rows(journal) if row[2] == "ready")
        assert len(ready_before) == 2

        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            room_nonempty=True,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        )
        assert tuple(item.route for item in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
        )
        owner_before = journal.load_owner()

        result = _materialize(
            journal,
            limits=replace(
                MaterializerLimits(),
                max_ready_work_count=1,
                max_ready_work_canonical_bytes=1,
            ),
        )

        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            owner_before.revision + 1,
        )
        assert tuple(row for row in _work_rows(journal) if row[2] == "ready") == (
            ready_before
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("constant_name", "expected", "narrowed"),
    [
        ("_MAX_HELD_WORK_COUNT", 10_000, 0),
        ("_MAX_HELD_WORK_CANONICAL_BYTES", 32 * 1024 * 1024, 1),
    ],
    ids=("count", "canonical-bytes"),
)
def test_materializer_authenticated_held_inventory_has_one_hard_global_envelope(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    constant_name: str,
    expected: int,
    narrowed: int,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=True,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        held_before = tuple(row for row in _work_rows(journal) if row[2] == "held")
        assert len(held_before) == 1

        staged, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            room_present=True,
        )
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        aggregate_before = _aggregate_rows(journal)
        work_before = _work_rows(journal)
        rows_module = _rows()
        assert getattr(rows_module, constant_name) == expected
        monkeypatch.setattr(rows_module, constant_name, narrowed)
        statements.clear()

        with pytest.raises(
            JournalIntegrityError,
            match="HELD Work exceeds immutable capacity",
        ):
            _materialize(journal)

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _aggregate_rows(journal) == aggregate_before
        assert _work_rows(journal) == work_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("limit_name", "metric"),
    [
        ("max_ready_work_count", "count"),
        ("max_total_work_count", "count"),
        ("max_ready_work_canonical_bytes", "bytes"),
        ("max_total_work_canonical_bytes", "bytes"),
    ],
)
@pytest.mark.parametrize(
    ("projected_excess", "expected_status"),
    [(0, MaterializeStatus.MATERIALIZED), (1, MaterializeStatus.AT_CAPACITY)],
    ids=("exact", "one-over"),
)
def test_materializer_projected_global_capacity_has_inclusive_caller_boundary(
    tmp_path: Path,
    limit_name: str,
    metric: str,
    projected_excess: int,
    expected_status: MaterializeStatus,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        inventory_before = _work_rows(journal)
        assert len(inventory_before) == 2
        inventory_bytes = sum(
            len(_decrypt_event_work(journal, row)[0]) for row in inventory_before
        )
        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            global_ready_count=1,
        )
        planned_plaintexts = tuple(
            _expected_event_work_plaintext(record)
            for record in _planned_global_event_records(journal, staged, normalized)
        )
        expected_work_count = len(inventory_before) + len(planned_plaintexts)
        projected = (
            expected_work_count
            if metric == "count"
            else inventory_bytes + sum(map(len, planned_plaintexts))
        )
        limit_value = projected - projected_excess
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        statements.clear()

        result = _materialize(
            journal,
            limits=replace(MaterializerLimits(), **{limit_name: limit_value}),
        )

        assert result.status is expected_status
        assert result.frame_id == staged.frame_id
        if expected_status is MaterializeStatus.AT_CAPACITY:
            assert result.revision is None
            assert journal.load_owner() == owner_before
            assert _frame_storage_row(journal, staged.frame_id) == raw_before
            assert _work_rows(journal) == inventory_before
            assert _materializer_dml(statements) == ()
        else:
            assert result.revision == owner_before.revision + 1
            assert journal.load_owner().revision == owner_before.revision + 1
            assert _frame_storage_row(journal, staged.frame_id) is None
            assert len(_work_rows(journal)) == expected_work_count
    finally:
        bootstrap.close()


def test_materializer_caller_ready_bytes_watermark_is_not_an_integrity_cap(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        revision = journal.load_owner().revision
        storage_rows: list[tuple[object, ...]] = []
        plaintext_lengths: list[int] = []
        for index in range(17):
            record = EventRecord(
                str(uuid5(staged.frame_id, f"corrupt-existing:{index}")),
                RecordKind.GLOBAL_ACCOUNT_DATA,
                replace(normalized.origin, frame_index=index),
                None,
                None,
                None,
                None,
                None,
                b'{"padding":"' + (b"x" * 750_000) + b'"}',
                None,
            )
            plaintext = _expected_event_work_plaintext(record)
            plaintext_lengths.append(len(plaintext))
            storage_rows.append(
                _authenticated_event_work_values(
                    journal,
                    record,
                    frame_id=staged.frame_id,
                    ready_revision=revision,
                    ready_ordinal=index,
                    created_revision=revision,
                )
            )
        limits = MaterializerLimits()
        assert max(plaintext_lengths) <= limits.max_record_canonical_bytes
        assert sum(plaintext_lengths) > limits.max_ready_work_canonical_bytes
        with journal._owner.journal_write():
            for values in storage_rows:
                inserted = journal._execute(
                    "INSERT INTO NioIngestWork("
                    "account_id, work_id, kind, status, frame_id, room_id, "
                    "membership_epoch, room_sequence, ready_revision, "
                    "ready_ordinal, created_revision, payload_ciphertext, "
                    "payload_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, "
                    "?, ?, ?)",
                    values,
                )
                assert inserted.rowcount == 1

        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        work_before = _work_rows(journal)
        statements.clear()

        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.AT_CAPACITY,
            staged.frame_id,
            None,
        )

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _work_rows(journal) == work_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


def test_materializer_caller_ready_count_watermark_is_not_an_integrity_cap(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        revision = journal.load_owner().revision
        insert_sql = (
            "INSERT INTO NioIngestWork("
            "account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload_ciphertext, payload_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        with journal._owner.journal_write():
            for index in range(MaterializerLimits().max_ready_work_count + 1):
                record = EventRecord(
                    str(uuid5(staged.frame_id, f"corrupt-ready-count:{index}")),
                    RecordKind.GLOBAL_ACCOUNT_DATA,
                    replace(normalized.origin, frame_index=index),
                    None,
                    None,
                    None,
                    None,
                    None,
                    b"{}",
                    None,
                )
                inserted = journal._execute(
                    insert_sql,
                    _authenticated_event_work_values(
                        journal,
                        record,
                        frame_id=staged.frame_id,
                        ready_revision=revision,
                        ready_ordinal=index,
                        created_revision=revision,
                    ),
                )
                assert inserted.rowcount == 1
        with journal._owner.read():
            work_count = journal._execute(
                "SELECT COUNT(*) FROM NioIngestWork WHERE account_id = ?",
                (journal.account_id,),
            ).fetchone()[0]
        assert work_count == MaterializerLimits().max_ready_work_count + 1
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        statements.clear()

        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.AT_CAPACITY,
            staged.frame_id,
            None,
        )

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize("payload_case", ["exact", "different"])
def test_materializer_planned_id_collision_catches_any_preexisting_id_as_corruption(
    tmp_path: Path,
    payload_case: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        descriptor = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        ).descriptors[0]
        colliding_id = str(uuid5(staged.frame_id, f"event:frame:{staged.frame_id}:0"))
        revision = journal.load_owner().revision
        source_json = (
            descriptor.source_json
            if payload_case == "exact"
            else b'{"content":{"forged":true},"type":"m.push_rules"}'
        )
        colliding = EventRecord(
            colliding_id,
            descriptor.kind,
            replace(normalized.origin, frame_index=0),
            None,
            None,
            None,
            None,
            None,
            source_json,
            None,
        )
        _insert_authenticated_event_work(
            journal,
            colliding,
            frame_id=staged.frame_id,
            ready_revision=revision,
            ready_ordinal=99,
            created_revision=revision,
        )
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        work_before = _work_rows(journal)
        statements.clear()

        with pytest.raises(JournalIntegrityError):
            _materialize(journal)

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _work_rows(journal) == work_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "mutation",
    [
        "primary-key",
        "aad-frame",
        "aad-room-fields",
        "aad-role",
        "aad-ready-revision",
        "aad-ready-ordinal",
        "aad-created-revision",
        "ciphertext",
        "digest",
        "table-domain",
        "clear-payload-identity",
    ],
)
def test_materializer_authenticated_inventory_catches_work_row_corruption(
    tmp_path: Path,
    mutation: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        staged, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            global_ready_count=1,
        )
        target = _work_rows(journal)[0]
        column: str
        value: object
        if mutation == "primary-key":
            column, value = "work_id", str(uuid4())
        elif mutation == "aad-frame":
            column, value = "frame_id", str(uuid4())
        elif mutation in {"aad-room-fields", "aad-role"}:
            assignments = (
                "room_id = ?, membership_epoch = ?, room_sequence = ?"
                if mutation == "aad-room-fields"
                else "kind = ?, room_id = ?, membership_epoch = ?"
            )
            parameters: tuple[object, ...] = (
                ("!forged:example.org", 0, 0)
                if mutation == "aad-room-fields"
                else ("loss", "!forged:example.org", 0)
            )
            with journal._owner.journal_write():
                updated = journal._execute(
                    f"UPDATE NioIngestWork SET {assignments} "
                    "WHERE account_id = ? AND work_id = ?",
                    (*parameters, journal.account_id, target[0]),
                )
                assert updated.rowcount == 1
            column, value = "", None
        elif mutation == "aad-ready-revision":
            column, value = "ready_revision", target[7] + 1
        elif mutation == "aad-ready-ordinal":
            column, value = "ready_ordinal", 77
        elif mutation == "aad-created-revision":
            column, value = "created_revision", target[9] + 1
        elif mutation == "ciphertext":
            column, value = "payload_ciphertext", _flip_first(target[10])
        elif mutation == "digest":
            column, value = "payload_sha256", _flip_first(target[11])
        else:
            plaintext, record = _decrypt_event_work(journal, target)
            if mutation == "table-domain":
                ciphertext, digest = journal._codec.seal(
                    "NioIngestFrame",
                    (target[0],),
                    plaintext,
                    header=_work_header(journal.account_id, target),
                )
            else:
                mismatched = replace(
                    record,
                    room_id="!forged:example.org",
                    membership_epoch=0,
                    room_sequence=0,
                )
                ciphertext, digest = journal._codec.seal(
                    "NioIngestWork",
                    (target[0],),
                    _rows()._canonical_work_plaintext("event", mismatched),
                    header=_work_header(journal.account_id, target),
                )
            with journal._owner.journal_write():
                updated = journal._execute(
                    "UPDATE NioIngestWork SET payload_ciphertext = ?, "
                    "payload_sha256 = ? WHERE account_id = ? AND work_id = ?",
                    (ciphertext, digest, journal.account_id, target[0]),
                )
                assert updated.rowcount == 1
            column = ""
            value = None
        if column:
            with journal._owner.journal_write():
                updated = journal._execute(
                    f"UPDATE NioIngestWork SET {column} = ? "
                    "WHERE account_id = ? AND work_id = ?",
                    (value, journal.account_id, target[0]),
                )
                assert updated.rowcount == 1
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        corrupt_before = _work_rows(journal)
        statements.clear()

        with pytest.raises(JournalIntegrityError):
            _materialize(journal)

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _work_rows(journal) == corrupt_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "semantic_case",
    [
        "noncanonical-wrapper",
        "noncanonical-work-id",
        "noncanonical-frame-id",
        "future-ready-revision",
        "created-after-ready",
        "room-ready",
        "loss",
    ],
)
def test_materializer_authenticated_inventory_rejects_semantically_invalid_work(
    tmp_path: Path,
    semantic_case: str,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        first, first_normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        inventory = _work_rows(journal)
        assert len(inventory) == 2
        target = inventory[0]
        plaintext, _record = _decrypt_event_work(journal, target)
        staged, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            global_ready_count=1,
        )
        owner = journal.load_owner()
        insert_sql = (
            "INSERT INTO NioIngestWork("
            "account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload_ciphertext, payload_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )

        if semantic_case in {
            "noncanonical-wrapper",
            "noncanonical-work-id",
            "noncanonical-frame-id",
            "future-ready-revision",
            "created-after-ready",
        }:
            changed_work_id = str(target[0])
            changed_frame_id = str(target[3])
            changed_ready_revision = int(target[7])
            changed_created_revision = int(target[9])
            changed_plaintext = plaintext
            if semantic_case == "noncanonical-wrapper":
                changed_plaintext = plaintext.replace(b'{"kind":', b'{ "kind":', 1)
            elif semantic_case == "noncanonical-work-id":
                changed_work_id = "{" + str(target[0]) + "}"
                changed_plaintext = plaintext.replace(
                    str(target[0]).encode(),
                    changed_work_id.encode(),
                    1,
                )
            elif semantic_case == "noncanonical-frame-id":
                changed_frame_id = "{" + str(target[3]) + "}"
            elif semantic_case == "future-ready-revision":
                changed_ready_revision = owner.revision + 1
            else:
                changed_created_revision = changed_ready_revision + 1
            changed_values = _sealed_work_values(
                journal,
                work_id=changed_work_id,
                kind=str(target[1]),
                status=str(target[2]),
                frame_id=changed_frame_id,
                room_id=target[4],
                membership_epoch=target[5],
                room_sequence=target[6],
                ready_revision=changed_ready_revision,
                ready_ordinal=int(target[8]),
                created_revision=changed_created_revision,
                plaintext=changed_plaintext,
            )
            with journal._owner.journal_write():
                updated = journal._execute(
                    "UPDATE NioIngestWork SET work_id = ?, kind = ?, status = ?, "
                    "frame_id = ?, room_id = ?, membership_epoch = ?, "
                    "room_sequence = ?, ready_revision = ?, ready_ordinal = ?, "
                    "created_revision = ?, payload_ciphertext = ?, "
                    "payload_sha256 = ? WHERE account_id = ? AND work_id = ?",
                    (*changed_values[1:], journal.account_id, target[0]),
                )
                assert updated.rowcount == 1
        else:
            room_id = "!deferred:example.org"
            work_id = str(uuid5(first.frame_id, f"deferred:{semantic_case}"))
            origin = replace(first_normalized.origin, frame_index=99)
            ready_revision: int | None = owner.revision
            ready_ordinal: int | None = 100
            room_sequence: int | None = 0
            kind = "event"
            status = "ready"
            if semantic_case == "loss":
                loss = LossRecord(
                    work_id,
                    origin,
                    room_id,
                    0,
                    LossReason.EVENT_LIMIT,
                    LossBoundary(None, None, None, None),
                    b"{}",
                )
                kind = "loss"
                room_sequence = None
                plaintext_to_insert = canonical_json(
                    {"kind": "loss", "value": _record_to_dict(loss)}
                )
            else:
                event = EventRecord(
                    work_id,
                    RecordKind.TIMELINE,
                    origin,
                    room_id,
                    0,
                    0,
                    None,
                    None,
                    b'{"content":{"body":"deferred"},' b'"type":"m.room.message"}',
                    None,
                )
                plaintext_to_insert = canonical_json(
                    {"kind": "event", "value": _record_to_dict(event)}
                )
            values = _sealed_work_values(
                journal,
                work_id=work_id,
                kind=kind,
                status=status,
                frame_id=str(first.frame_id),
                room_id=room_id,
                membership_epoch=0,
                room_sequence=room_sequence,
                ready_revision=ready_revision,
                ready_ordinal=ready_ordinal,
                created_revision=owner.revision,
                plaintext=plaintext_to_insert,
            )
            with journal._owner.journal_write():
                inserted = journal._execute(insert_sql, values)
                assert inserted.rowcount == 1

        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        corrupt_before = _work_rows(journal)
        statements.clear()

        with pytest.raises(JournalIntegrityError):
            _materialize(journal)

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _work_rows(journal) == corrupt_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


def test_materializer_inventory_catches_oversized_ciphertext_before_decrypt(
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
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        staged, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            global_ready_count=1,
        )
        target = _work_rows(journal)[0]
        with sqlite3.connect(journal.database_path) as connection:
            connection.execute("PRAGMA ignore_check_constraints = ON")
            updated = connection.execute(
                "UPDATE NioIngestWork SET payload_ciphertext = ? "
                "WHERE account_id = ? AND work_id = ?",
                (
                    b"x" * (_WORK_CIPHERTEXT_LIMIT + 1),
                    journal.account_id,
                    target[0],
                ),
            )
            assert updated.rowcount == 1
            connection.execute("PRAGMA ignore_check_constraints = OFF")
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        real_decrypt = EncryptedRowCodec.decrypt

        def reject_target_decrypt(
            codec: EncryptedRowCodec,
            table: str,
            primary_key: tuple[str | int | UUID, ...],
            ciphertext: bytes,
            digest: bytes,
            header: bytes = b"",
        ) -> bytes:
            if table == "NioIngestWork" and primary_key == (target[0],):
                raise AssertionError("oversized Work ciphertext reached AES-GCM")
            return real_decrypt(codec, table, primary_key, ciphertext, digest, header)

        monkeypatch.setattr(EncryptedRowCodec, "decrypt", reject_target_decrypt)
        statements.clear()
        with pytest.raises(JournalIntegrityError):
            _materialize(journal)

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "race",
    [
        "removal",
        "frame-id",
        "work-id-reorder",
        "ready-revision",
        "ready-ordinal",
        "created-revision",
        "same-plaintext-reseal",
        "payload-reseal",
    ],
)
def test_materializer_writer_revalidation_catches_exact_work_row_race(
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
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        inventory_before = _work_rows(journal)
        assert len(inventory_before) == 2
        target = min(inventory_before, key=lambda row: str(row[0]))
        plaintext, record = _decrypt_event_work(journal, target)
        staged, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            global_ready_count=1,
        )
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        preflight_decoded = False
        race_applied = False
        writer_decrypts: list[str] = []
        real_decrypt = EncryptedRowCodec.decrypt
        real_journal_write = type(journal._owner).journal_write
        reordered_work_id = "ffffffff-ffff-4fff-bfff-ffffffffffff"
        assert reordered_work_id > max(str(row[0]) for row in inventory_before)

        def observe_preflight(
            codec: EncryptedRowCodec,
            table: str,
            primary_key: tuple[str | int | UUID, ...],
            ciphertext: bytes,
            digest: bytes,
            header: bytes = b"",
        ) -> bytes:
            nonlocal preflight_decoded
            plaintext = real_decrypt(
                codec,
                table,
                primary_key,
                ciphertext,
                digest,
                header,
            )
            if table == "NioIngestWork":
                if journal._owner._outer_scope == "journal_write":
                    writer_decrypts.append(str(primary_key[0]))
                else:
                    preflight_decoded = True
            return plaintext

        @contextmanager
        def remove_before_writer(owner: object) -> Iterator[None]:
            nonlocal race_applied
            assert owner is journal._owner
            assert preflight_decoded
            assert not race_applied
            race_applied = True
            assignments: str
            parameters: tuple[object, ...]
            if race == "frame-id":
                changed = list(target)
                changed[3] = str(uuid4())
                ciphertext, digest = journal._codec.seal(
                    "NioIngestWork",
                    (target[0],),
                    plaintext,
                    header=_work_header(journal.account_id, tuple(changed)),
                )
                assignments = "frame_id = ?, payload_ciphertext = ?, payload_sha256 = ?"
                parameters = (changed[3], ciphertext, digest)
            elif race == "work-id-reorder":
                changed = list(target)
                changed[0] = reordered_work_id
                changed_plaintext = _rows()._canonical_work_plaintext(
                    "event",
                    replace(record, record_id=reordered_work_id),
                )
                ciphertext, digest = journal._codec.seal(
                    "NioIngestWork",
                    (reordered_work_id,),
                    changed_plaintext,
                    header=_work_header(journal.account_id, tuple(changed)),
                )
                assignments = "work_id = ?, payload_ciphertext = ?, payload_sha256 = ?"
                parameters = (reordered_work_id, ciphertext, digest)
            elif race in {"ready-revision", "ready-ordinal", "created-revision"}:
                changed = list(target)
                if race == "ready-revision":
                    assert int(target[7]) < owner_before.revision
                    assert int(target[9]) <= owner_before.revision
                    changed[7] = owner_before.revision
                elif race == "ready-ordinal":
                    changed[8] = 77
                else:
                    assert int(target[7]) > 1
                    changed[9] = 1
                ciphertext, digest = journal._codec.seal(
                    "NioIngestWork",
                    (target[0],),
                    plaintext,
                    header=_work_header(journal.account_id, tuple(changed)),
                )
                column_index = {
                    "ready-revision": ("ready_revision", 7),
                    "ready-ordinal": ("ready_ordinal", 8),
                    "created-revision": ("created_revision", 9),
                }[race]
                assignments = (
                    f"{column_index[0]} = ?, payload_ciphertext = ?, "
                    "payload_sha256 = ?"
                )
                parameters = (changed[column_index[1]], ciphertext, digest)
            elif race == "same-plaintext-reseal":
                ciphertext, digest = journal._codec.seal(
                    "NioIngestWork",
                    (target[0],),
                    plaintext,
                    header=_work_header(journal.account_id, target),
                )
                assert ciphertext != target[10]
                assert digest == target[11]
                assignments = "payload_ciphertext = ?, payload_sha256 = ?"
                parameters = (ciphertext, digest)
            elif race == "payload-reseal":
                changed_plaintext = _rows()._canonical_work_plaintext(
                    "event",
                    replace(
                        record,
                        source_json=b'{"content":{"raced":true},'
                        b'"type":"m.push_rules"}',
                    ),
                )
                ciphertext, digest = journal._codec.seal(
                    "NioIngestWork",
                    (target[0],),
                    changed_plaintext,
                    header=_work_header(journal.account_id, target),
                )
                assert digest != target[11]
                assignments = "payload_ciphertext = ?, payload_sha256 = ?"
                parameters = (ciphertext, digest)
            else:
                assignments = ""
                parameters = ()
            with sqlite3.connect(journal.database_path) as connection:
                if race == "removal":
                    changed_row = connection.execute(
                        "DELETE FROM NioIngestWork "
                        "WHERE account_id = ? AND work_id = ?",
                        (journal.account_id, target[0]),
                    )
                else:
                    changed_row = connection.execute(
                        f"UPDATE NioIngestWork SET {assignments} "
                        "WHERE account_id = ? AND work_id = ?",
                        (*parameters, journal.account_id, target[0]),
                    )
                assert changed_row.rowcount == 1
            with real_journal_write(journal._owner):
                yield

        monkeypatch.setattr(EncryptedRowCodec, "decrypt", observe_preflight)
        monkeypatch.setattr(
            type(journal._owner),
            "journal_write",
            remove_before_writer,
        )
        statements.clear()
        with pytest.raises(JournalIntegrityError):
            _materialize(journal)

        assert preflight_decoded
        assert race_applied
        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        current_inventory = _work_rows(journal)
        assert writer_decrypts == sorted(str(row[0]) for row in current_inventory)
        matching = [row for row in current_inventory if row[0] == target[0]]
        if race in {"removal", "work-id-reorder"}:
            assert matching == []
            if race == "work-id-reorder":
                assert any(row[0] == reordered_work_id for row in current_inventory)
        else:
            assert len(matching) == 1
            assert matching[0] != target
            if race == "ready-revision":
                assert matching[0][7] == owner_before.revision
                assert matching[0][9] == target[9]
                assert int(matching[0][9]) <= int(matching[0][7])
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


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
    ("transport", "crypto"),
    [
        (TransportKind.CLASSIC, False),
        (TransportKind.SLIDING, True),
    ],
    ids=("classic-plain", "sliding-crypto"),
)
def test_materializer_first_seen_room_catches_missing_hydration_owner(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
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
            room_nonempty=True,
            global_ready_count=1,
        )
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        )
        assert tuple(descriptor.kind for descriptor in proposal.descriptors) == (
            RecordKind.TIMELINE,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordKind.PRESENCE,
        )
        assert len(proposal.room_proposals) == 1
        room_proposal = proposal.room_proposals[0]
        assert room_proposal.hydration is not None
        assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        assert proposal.crypto_deferred is crypto
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        assert raw_before is not None
        writer_entries: list[None] = []
        real_journal_write = type(journal._owner).journal_write

        @contextmanager
        def count_writer(owner: object) -> Iterator[None]:
            assert owner is journal._owner
            writer_entries.append(None)
            with real_journal_write(journal._owner):
                yield

        statements.clear()
        with monkeypatch.context() as guard:
            guard.setattr(type(journal._owner), "journal_write", count_writer)
            result = _materialize(journal)

        revision = owner_before.revision + 1
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            revision,
        )
        assert writer_entries == [None]
        assert journal.load_owner() == replace(owner_before, revision=revision)

        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        aggregate_row = aggregate_rows[0]
        assert aggregate_row[:3] == (
            room_proposal.after.room_id,
            revision,
            "hydration",
        )
        aggregate_plaintext, aggregate = _decrypt_aggregate(journal, aggregate_row)
        expected_aggregate = _values().RoomAggregateValue(
            room_proposal.after,
            1,
            revision,
            room_proposal.hydration,
        )
        assert aggregate == expected_aggregate
        assert aggregate_plaintext == _rows()._canonical_room_aggregate_plaintext(
            expected_aggregate
        )

        work_rows = _work_rows(journal)
        assert len(work_rows) == 3
        rows_by_id = {str(row[0]): row for row in work_rows}
        expected_records: list[EventRecord] = []
        room_sequence = 0
        for index, descriptor in enumerate(proposal.descriptors):
            record_id = str(
                uuid5(staged.frame_id, f"event:{descriptor.descriptor_key}")
            )
            is_room = descriptor.room_id is not None
            expected_record = EventRecord(
                record_id,
                descriptor.kind,
                replace(normalized.origin, frame_index=index),
                descriptor.room_id,
                room_proposal.after.membership_epoch if is_room else None,
                room_sequence if is_room else None,
                None,
                descriptor.provenance,
                descriptor.source_json,
                None,
            )
            expected_records.append(expected_record)
            row = rows_by_id[record_id]
            assert row[1:10] == (
                "event",
                "held" if is_room else "ready",
                str(staged.frame_id),
                descriptor.room_id,
                room_proposal.after.membership_epoch if is_room else None,
                room_sequence if is_room else None,
                None if is_room else revision,
                None if is_room else index - 1,
                revision,
            )
            plaintext, record = _decrypt_event_work(journal, row)
            assert record == expected_record
            assert plaintext == _rows()._canonical_work_plaintext(
                "event", expected_record
            )
            if is_room:
                room_sequence += 1
        ready_rows = tuple(row for row in work_rows if row[2] == "ready")
        assert tuple(row[0] for row in ready_rows) == tuple(
            record.record_id for record in expected_records[1:]
        )
        assert tuple(row[8] for row in ready_rows) == (0, 1)

        raw_after = _frame_storage_row(journal, staged.frame_id)
        if crypto:
            assert raw_after is not None
            assert raw_after[:6] == raw_before[:6]
            assert raw_after[6] == revision
            assert raw_after[7] != raw_before[7]
        else:
            assert raw_after is None

        dml = _materializer_dml(statements)
        assert sum("UPDATE NioIngestMeta" in statement for statement in dml) == 1
        assert any("NioIngestRoomAggregate" in statement for statement in dml)
        assert any("NioIngestWork" in statement for statement in dml)
        frame_dml_index = next(
            index
            for index, statement in enumerate(dml)
            if "NioIngestFrame" in statement
        )
        assert all(
            index < frame_dml_index
            for index, statement in enumerate(dml)
            if "NioIngestRoomAggregate" in statement or "NioIngestWork" in statement
        )

        owner_after = journal.load_owner()
        aggregate_after = _aggregate_rows(journal)
        work_after = _work_rows(journal)
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.IDLE,
            None,
            None,
        )
        assert journal.load_owner() == owner_after
        assert _aggregate_rows(journal) == aggregate_after
        assert _work_rows(journal) == work_after

        later, _ = _stage_discovery_frame(
            journal,
            transport,
            2,
            global_ready_count=1,
        )
        later_owner = journal.load_owner()
        later_raw = _frame_storage_row(journal, later.frame_id)
        assert later_raw is not None
        held_before = next(row for row in work_after if row[2] == "held")
        statements.clear()
        assert _materialize(
            journal,
            limits=replace(
                MaterializerLimits(),
                max_ready_work_count=4,
                max_total_work_count=4,
            ),
        ) == MaterializeResult(MaterializeStatus.AT_CAPACITY, later.frame_id, None)
        assert journal.load_owner() == later_owner
        assert _frame_storage_row(journal, later.frame_id) == later_raw
        assert _aggregate_rows(journal) == aggregate_after
        assert _work_rows(journal) == work_after
        assert _materializer_dml(statements) == ()

        later_result = _materialize(
            journal,
            limits=replace(
                MaterializerLimits(),
                max_ready_work_count=4,
                max_total_work_count=5,
            ),
        )
        assert later_result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            later.frame_id,
            later_owner.revision + 1,
        )
        assert _aggregate_rows(journal) == aggregate_after
        later_work = _work_rows(journal)
        assert len(later_work) == 5
        assert (
            next(row for row in later_work if row[0] == held_before[0]) == held_before
        )
        later_ready = tuple(
            row
            for row in later_work
            if row[7] == later_result.revision and row[2] == "ready"
        )
        assert tuple(row[8] for row in later_ready) == (0, 1)
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "transport",
    [TransportKind.CLASSIC, TransportKind.SLIDING],
    ids=("classic", "sliding"),
)
def test_materializer_empty_first_seen_room_catches_orphan_held_work(
    tmp_path: Path,
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
            room_present=True,
        )
        assert len(normalized.room_segments) == 1
        segment = normalized.room_segments[0]
        assert segment.room_id == "!unsupported:example.org"
        assert segment.state_json == ()
        assert segment.timeline_json == ()
        assert segment.room_account_data_json == ()
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            staged.frame_id,
            normalized,
            (),
        )
        assert proposal.descriptors == ()
        assert len(proposal.room_proposals) == 1
        room_proposal = proposal.room_proposals[0]
        assert room_proposal.before is None
        assert room_proposal.hydration is not None

        owner_before = journal.load_owner()
        orphan_frame_id = uuid5(staged.frame_id, "orphan-held-frame")
        orphan_work_id = str(uuid5(orphan_frame_id, "event:orphan-held"))
        orphan = EventRecord(
            orphan_work_id,
            RecordKind.STATE,
            replace(normalized.origin, frame_index=0),
            segment.room_id,
            room_proposal.after.membership_epoch,
            0,
            None,
            None,
            canonical_json(
                {
                    "content": {"name": "orphan"},
                    "state_key": "",
                    "type": "m.room.name",
                }
            ),
            None,
        )
        _insert_authenticated_event_work(
            journal,
            orphan,
            frame_id=orphan_frame_id,
            ready_revision=None,
            ready_ordinal=None,
            created_revision=owner_before.revision,
            status="held",
        )
        raw_before = _frame_storage_row(journal, staged.frame_id)
        work_before = _work_rows(journal)
        assert raw_before is not None
        assert len(work_before) == 1
        assert _decrypt_event_work(journal, work_before[0])[1] == orphan
        assert _aggregate_rows(journal) == ()
        statements.clear()

        with pytest.raises(JournalIntegrityError, match="orphan HELD Work"):
            _materialize(journal)

        assert journal.load_owner() == owner_before
        assert _frame_storage_row(journal, staged.frame_id) == raw_before
        assert _work_rows(journal) == work_before
        assert _aggregate_rows(journal) == ()
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


_ATOMICITY_H1_CLASSIC_PLAIN = "h1-classic-plain"
_ATOMICITY_RETIREMENT_SLIDING_CRYPTO = "retirement-sliding-crypto"
_MATERIALIZER_CRASH_EXIT_CODE = 87

_MATERIALIZER_ROLLBACK_CASES = (
    pytest.param(
        _ATOMICITY_H1_CLASSIC_PLAIN,
        "aggregate_insert",
        1,
        id="h1-aggregate-insert",
    ),
    *(
        pytest.param(
            _ATOMICITY_H1_CLASSIC_PLAIN,
            "work_insert",
            occurrence,
            id=f"h1-work-insert-{occurrence}",
        )
        for occurrence in range(1, 3)
    ),
    pytest.param(
        _ATOMICITY_H1_CLASSIC_PLAIN,
        "frame_delete",
        1,
        id="h1-frame-delete",
    ),
    pytest.param(
        _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
        "meta_revision_epoch_cas",
        1,
        id="retirement-meta-cas",
    ),
    pytest.param(
        _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
        "aggregate_update",
        1,
        id="retirement-aggregate-update",
    ),
    *(
        pytest.param(
            _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
            "work_insert",
            occurrence,
            id=f"retirement-work-insert-{occurrence}",
        )
        for occurrence in range(1, 5)
    ),
    pytest.param(
        _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
        "work_release",
        1,
        id="retirement-work-release",
    ),
    pytest.param(
        _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
        "frame_crypto_retain",
        1,
        id="retirement-frame-crypto-retain",
    ),
    pytest.param(
        _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
        "before_commit",
        1,
        id="retirement-before-commit",
    ),
)
_MATERIALIZER_CRASH_CASES = (
    *_MATERIALIZER_ROLLBACK_CASES,
    pytest.param(
        _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
        "commit",
        1,
        id="retirement-commit",
    ),
)

type _MaterializerStorageGraph = tuple[
    OwnerView,
    SourceState,
    tuple[tuple[object, ...], ...],
    tuple[tuple[object, ...], ...],
    tuple[tuple[object, ...], ...],
]


@dataclass(frozen=True, slots=True)
class _MaterializerAtomicityCase:
    bootstrap: StoreBootstrap
    selected: StagedFrame
    normalized: SyncFrame
    first: StagedFrame | None = None
    first_normalized: SyncFrame | None = None


@dataclass(frozen=True, slots=True)
class _ExpectedMaterializerWork:
    value: EventRecord | LossRecord
    status: str
    frame_id: UUID
    ready_revision: int | None
    ready_ordinal: int | None
    created_revision: int


class _InjectedMaterializerFailure(RuntimeError):
    pass


def _materializer_frame_rows(
    journal: SqliteIngestionJournal,
) -> tuple[tuple[object, ...], ...]:
    with journal._owner.read():
        rows = journal._execute(
            "SELECT frame_id, source_epoch, request_id, staged_revision, "
            "payload_ciphertext, payload_sha256, room_materialized_revision, "
            "drain_header_ciphertext FROM NioIngestFrame "
            "WHERE account_id = ? "
            "ORDER BY staged_revision, source_epoch, request_id, frame_id",
            (journal.account_id,),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _materializer_storage_graph(
    journal: SqliteIngestionJournal,
) -> _MaterializerStorageGraph:
    return (
        journal.load_owner(),
        journal.load_source(),
        _materializer_frame_rows(journal),
        _aggregate_rows(journal),
        _work_rows(journal),
    )


def _assert_materializer_reopened_graph(
    journal: SqliteIngestionJournal,
    expected: _MaterializerStorageGraph,
) -> None:
    actual = _materializer_storage_graph(journal)
    assert actual[0] == replace(expected[0], writer_epoch=journal.writer_epoch)
    assert actual[1:] == expected[1:]


def _prepare_materializer_atomicity_case(
    store_path: Path,
    scenario: str,
    *,
    statements: list[str] | None = None,
    sqlite_busy_timeout_ms: int = 2_000,
) -> _MaterializerAtomicityCase:
    if scenario == _ATOMICITY_H1_CLASSIC_PLAIN:
        transport = TransportKind.CLASSIC
        crypto = False
    elif scenario == _ATOMICITY_RETIREMENT_SLIDING_CRYPTO:
        transport = TransportKind.SLIDING
        crypto = True
    else:
        raise AssertionError(f"unknown materializer atomicity scenario: {scenario}")

    bootstrap = _open_discovery_journal(
        store_path,
        transport,
        statements=statements,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
    )
    journal = bootstrap._journal
    try:
        if scenario == _ATOMICITY_RETIREMENT_SLIDING_CRYPTO:
            first, first_normalized = _stage_discovery_frame(
                journal,
                transport,
                1,
                crypto=crypto,
                room_nonempty=True,
                global_ready_count=0,
            )
            assert _materialize(journal) == MaterializeResult(
                MaterializeStatus.MATERIALIZED,
                first.frame_id,
                2,
            )
            selected, normalized = _stage_discovery_frame(
                journal,
                transport,
                2,
                crypto=crypto,
                room_nonempty=True,
                room_membership="leave",
                global_ready_count=0,
            )
        else:
            first = None
            first_normalized = None
            selected, normalized = _stage_discovery_frame(
                journal,
                transport,
                1,
                crypto=crypto,
                room_nonempty=True,
                global_ready_count=0,
            )
        return _MaterializerAtomicityCase(
            bootstrap,
            selected,
            normalized,
            first,
            first_normalized,
        )
    except BaseException:
        bootstrap.close()
        raise


def _atomicity_h1_expectations(
    stream_id: UUID,
    staged: StagedFrame,
    normalized: SyncFrame,
    revision: int,
) -> tuple[RoomAggregateValue, tuple[_ExpectedMaterializerWork, ...]]:
    assert normalized.frame_id == staged.frame_id
    assert len(normalized.room_segments) == 1
    room_id = normalized.room_segments[0].room_id
    hydration_id = uuid5(
        stream_id,
        f"hydrate:{room_id}:0:{staged.frame_id}",
    )
    continuity = RoomContinuity(room_id, 0, "join", None, None, hydration_id)
    hydration = HydrationIntent(hydration_id, normalized.origin)
    proposal = reduce_staged_frame(stream_id, staged.frame_id, normalized, ())
    assert len(proposal.room_proposals) == 1
    room = proposal.room_proposals[0]
    assert room.before is None
    assert room.after == continuity
    assert room.hydration == hydration
    assert room.recovery is None
    assert room.retirement_epoch is None
    assert room.losses == ()
    assert room.release is RecoveryRelease.NONE
    assert tuple(descriptor.kind for descriptor in proposal.descriptors) == (
        RecordKind.TIMELINE,
        RecordKind.PRESENCE,
    )
    assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
        DescriptorRoute.HOLD_FOR_HYDRATION,
        DescriptorRoute.READY,
    )

    expected_work: list[_ExpectedMaterializerWork] = []
    next_room_sequence = 0
    next_ready_ordinal = 0
    for index, descriptor in enumerate(proposal.descriptors):
        is_room = descriptor.room_id is not None
        record = EventRecord(
            str(uuid5(staged.frame_id, f"event:{descriptor.descriptor_key}")),
            descriptor.kind,
            replace(normalized.origin, frame_index=index),
            descriptor.room_id,
            continuity.membership_epoch if is_room else None,
            next_room_sequence if is_room else None,
            None,
            descriptor.provenance,
            descriptor.source_json,
            None,
        )
        ready = descriptor.route is DescriptorRoute.READY
        expected_work.append(
            _ExpectedMaterializerWork(
                record,
                "ready" if ready else "held",
                staged.frame_id,
                revision if ready else None,
                next_ready_ordinal if ready else None,
                revision,
            )
        )
        if is_room:
            next_room_sequence += 1
        else:
            next_ready_ordinal += 1
    assert next_room_sequence == 1
    assert next_ready_ordinal == 1
    return (
        RoomAggregateValue(
            continuity,
            next_room_sequence,
            revision,
            hydration,
        ),
        tuple(expected_work),
    )


def _atomicity_retirement_expectations(
    stream_id: UUID,
    case: _MaterializerAtomicityCase,
    revision: int,
) -> tuple[RoomAggregateValue, tuple[_ExpectedMaterializerWork, ...]]:
    first = case.first
    first_normalized = case.first_normalized
    assert first is not None
    assert first_normalized is not None
    first_revision = revision - 2
    first_aggregate, first_work = _atomicity_h1_expectations(
        stream_id,
        first,
        first_normalized,
        first_revision,
    )
    assert len(first_work) == 2
    held, prior_ready = first_work
    assert held.status == "held"
    assert prior_ready.status == "ready"

    proposal = reduce_staged_frame(
        stream_id,
        case.selected.frame_id,
        case.normalized,
        (first_aggregate.continuity,),
    )
    assert len(proposal.room_proposals) == 1
    room = proposal.room_proposals[0]
    before = first_aggregate.continuity
    after = RoomContinuity(before.room_id, 1, "leave", None, None, None)
    assert room.before == before
    assert room.after == after
    assert room.recovery is None
    assert room.hydration is None
    assert room.retirement_epoch == 0
    assert room.release is RecoveryRelease.LOSS_THEN_HELD
    assert len(room.losses) == 1
    assert room.losses[0].reason is LossReason.UNVERIFIABLE
    assert room.losses[0].boundary == LossBoundary(None, None, None, None)
    assert tuple(descriptor.kind for descriptor in proposal.descriptors) == (
        RecordKind.TIMELINE,
        RecordKind.PRESENCE,
    )
    assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
        DescriptorRoute.HOLD_FOR_RETIREMENT,
        DescriptorRoute.READY,
    )

    loss_without_id = LossRecord(
        "",
        case.normalized.origin,
        before.room_id,
        before.membership_epoch,
        LossReason.UNVERIFIABLE,
        LossBoundary(None, None, None, None),
        b"{}",
    )
    loss = replace(
        loss_without_id,
        loss_id=_loss_id(stream_id, loss_without_id),
    )
    lifecycle = EventRecord(
        str(
            uuid5(
                case.selected.frame_id,
                f"lifecycle:{before.room_id}:{before.membership_epoch}:1",
            )
        ),
        RecordKind.ROOM_LIFECYCLE,
        case.normalized.origin,
        before.room_id,
        1,
        first_aggregate.next_room_sequence,
        None,
        None,
        canonical_json(
            {
                "membership": "leave",
                "membership_epoch": 1,
                "previous_membership_epoch": before.membership_epoch,
            }
        ),
        None,
    )
    expected: list[_ExpectedMaterializerWork] = [
        prior_ready,
        _ExpectedMaterializerWork(
            loss,
            "ready",
            case.selected.frame_id,
            revision,
            0,
            revision,
        ),
        replace(
            held,
            status="ready",
            ready_revision=revision,
            ready_ordinal=1,
        ),
        _ExpectedMaterializerWork(
            lifecycle,
            "ready",
            case.selected.frame_id,
            revision,
            2,
            revision,
        ),
    ]
    next_room_sequence = first_aggregate.next_room_sequence + 1
    next_ready_ordinal = 3
    for index, descriptor in enumerate(proposal.descriptors):
        is_room = descriptor.room_id is not None
        record = EventRecord(
            str(
                uuid5(
                    case.selected.frame_id,
                    f"event:{descriptor.descriptor_key}",
                )
            ),
            descriptor.kind,
            replace(case.normalized.origin, frame_index=index),
            descriptor.room_id,
            after.membership_epoch if is_room else None,
            next_room_sequence if is_room else None,
            None,
            descriptor.provenance,
            descriptor.source_json,
            None,
        )
        expected.append(
            _ExpectedMaterializerWork(
                record,
                "ready",
                case.selected.frame_id,
                revision,
                next_ready_ordinal,
                revision,
            )
        )
        if is_room:
            next_room_sequence += 1
        next_ready_ordinal += 1
    assert next_room_sequence == 3
    assert next_ready_ordinal == 5
    return (
        RoomAggregateValue(after, next_room_sequence, revision, None),
        tuple(expected),
    )


def _expected_materializer_work_id(
    expected: _ExpectedMaterializerWork,
) -> str:
    value = expected.value
    return value.record_id if type(value) is EventRecord else value.loss_id


def _assert_exact_materializer_work(
    journal: SqliteIngestionJournal,
    rows: tuple[tuple[object, ...], ...],
    expected: tuple[_ExpectedMaterializerWork, ...],
    old_rows: tuple[tuple[object, ...], ...],
) -> None:
    assert tuple(str(row[0]) for row in rows) == tuple(
        _expected_materializer_work_id(item) for item in expected
    )
    old_by_id = {str(row[0]): row for row in old_rows}
    for row, item in zip(rows, expected, strict=True):
        value = item.value
        work_id = _expected_materializer_work_id(item)
        kind = "event" if type(value) is EventRecord else "loss"
        room_sequence = value.room_sequence if type(value) is EventRecord else None
        assert row[:10] == (
            work_id,
            kind,
            item.status,
            str(item.frame_id),
            value.room_id,
            value.membership_epoch,
            room_sequence,
            item.ready_revision,
            item.ready_ordinal,
            item.created_revision,
        )
        plaintext, stored = _decrypt_work(journal, row)
        assert stored == value
        assert plaintext == (
            _expected_event_work_plaintext(value)
            if type(value) is EventRecord
            else _expected_loss_work_plaintext(value)
        )
        if work_id in old_by_id:
            old = old_by_id[work_id]
            if old[:10] == row[:10]:
                assert row == old
            else:
                assert row[10] != old[10]
                assert row[11] == old[11]


def _assert_materializer_committed_graph(
    journal: SqliteIngestionJournal,
    scenario: str,
    case: _MaterializerAtomicityCase,
    old_graph: _MaterializerStorageGraph,
) -> None:
    old_owner, old_source, old_frames, _old_aggregates, old_work = old_graph
    owner, source, frames, aggregates, work = _materializer_storage_graph(journal)
    revision = old_owner.revision + 1
    assert owner == replace(
        old_owner,
        revision=revision,
        writer_epoch=journal.writer_epoch,
    )
    assert source == old_source
    assert len(aggregates) == 1

    if scenario == _ATOMICITY_H1_CLASSIC_PLAIN:
        assert old_owner.revision == 1
        expected_aggregate, expected_work = _atomicity_h1_expectations(
            owner.stream_id,
            case.selected,
            case.normalized,
            revision,
        )
        assert frames == ()
        assert tuple(row[0] for row in old_frames) == (str(case.selected.frame_id),)
        assert _frame_storage_row(journal, case.selected.frame_id) is None
        expected_intent = "hydration"
    else:
        assert scenario == _ATOMICITY_RETIREMENT_SLIDING_CRYPTO
        assert old_owner.revision == 3
        expected_aggregate, expected_work = _atomicity_retirement_expectations(
            owner.stream_id,
            case,
            revision,
        )
        first = case.first
        assert first is not None
        old_by_id = {str(row[0]): row for row in old_frames}
        current_by_id = {str(row[0]): row for row in frames}
        assert set(old_by_id) == {str(first.frame_id), str(case.selected.frame_id)}
        assert set(current_by_id) == set(old_by_id)
        assert current_by_id[str(first.frame_id)] == old_by_id[str(first.frame_id)]
        selected_before = old_by_id[str(case.selected.frame_id)]
        selected_after = current_by_id[str(case.selected.frame_id)]
        assert selected_before[6] is None
        assert selected_after[:6] == selected_before[:6]
        assert selected_after[6] == revision
        assert selected_after[7] != selected_before[7]
        assert journal.load_frame(first.frame_id) == first
        assert journal.load_frame(case.selected.frame_id) == case.selected
        assert (
            EncryptedRowCodec(
                "discovery-secret",
                journal.account_id,
                owner.stream_id,
            ).decrypt(
                "NioIngestFrameDrainHeader",
                (case.selected.frame_id,),
                selected_after[7],
                hashlib.sha256(b"").digest(),
                header=_canonical_expected_drain_header(selected_after, revision),
            )
            == b""
        )
        expected_intent = None

    assert aggregates[0][:3] == (
        expected_aggregate.continuity.room_id,
        revision,
        expected_intent,
    )
    aggregate_plaintext, aggregate = _decrypt_aggregate(journal, aggregates[0])
    assert aggregate == expected_aggregate
    assert aggregate_plaintext == _rows()._canonical_room_aggregate_plaintext(
        expected_aggregate
    )
    _assert_exact_materializer_work(journal, work, expected_work, old_work)


def test_materializer_empty_retirement_anchors_lifecycle_before_later_work(
    tmp_path: Path,
) -> None:
    room_id = "!unsupported:example.org"
    first_origin = RecordOrigin(TransportKind.SLIDING, 0, 0, 0)
    retirement_origin = RecordOrigin(TransportKind.SLIDING, 0, 1, 0)
    first_timeline_json = (
        b'{"content":{"body":"held","msgtype":"m.text"},' b'"type":"m.room.message"}'
    )
    ephemeral_json = (
        b'{"content":{"user_ids":["@friend:example.org"]},' b'"type":"m.typing"}'
    )
    global_json = (
        b'{"content":{"generation":2,"index":0,"padding":""},' b'"type":"m.push_rules"}'
    )
    presence_json = (
        b'{"content":{"presence":"online"},'
        b'"sender":"@friend:example.org","type":"m.presence"}'
    )
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.SLIDING)
    journal = bootstrap._journal
    try:
        first, first_normalized = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            1,
            crypto=True,
            room_nonempty=True,
        )
        first_proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            first.frame_id,
            first_normalized,
            (),
        )
        assert len(first_proposal.room_proposals) == 1
        assert len(first_proposal.descriptors) == 1
        first_room = first_proposal.room_proposals[0]
        first_descriptor = first_proposal.descriptors[0]
        expected_hydration_id = uuid5(
            journal.load_owner().stream_id,
            f"hydrate:{room_id}:0:{first.frame_id}",
        )
        expected_first_continuity = RoomContinuity(
            room_id,
            0,
            "join",
            None,
            None,
            expected_hydration_id,
        )
        expected_hydration = HydrationIntent(expected_hydration_id, first_origin)
        assert first_normalized.origin == first_origin
        assert first_room.before is None
        assert first_room.after == expected_first_continuity
        assert first_room.hydration == expected_hydration
        assert first_descriptor.kind is RecordKind.TIMELINE
        assert first_descriptor.room_id == room_id
        assert first_descriptor.source_json == first_timeline_json
        assert first_descriptor.provenance is TimelineEventProvenance.HISTORY
        assert first_descriptor.descriptor_key == f"frame:{first.frame_id}:0"
        assert first_descriptor.route is DescriptorRoute.HOLD_FOR_HYDRATION
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )

        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, first_aggregate = _decrypt_aggregate(journal, aggregate_rows[0])
        assert first_aggregate == RoomAggregateValue(
            expected_first_continuity,
            1,
            2,
            expected_hydration,
        )
        first_raw_before = _frame_storage_row(journal, first.frame_id)
        assert first_raw_before is not None
        first_work = _work_rows(journal)
        assert len(first_work) == 1
        held = EventRecord(
            str(uuid5(first.frame_id, f"event:frame:{first.frame_id}:0")),
            RecordKind.TIMELINE,
            first_origin,
            room_id,
            0,
            0,
            None,
            TimelineEventProvenance.HISTORY,
            first_timeline_json,
            None,
        )
        assert first_work[0][:10] == (
            held.record_id,
            "event",
            "held",
            str(first.frame_id),
            room_id,
            0,
            0,
            None,
            None,
            2,
        )
        assert _decrypt_event_work(journal, first_work[0]) == (
            _expected_event_work_plaintext(held),
            held,
        )

        selected, normalized = _stage_discovery_frame(
            journal,
            TransportKind.SLIDING,
            2,
            crypto=True,
            room_present=True,
            room_ephemeral=True,
            room_membership="leave",
            global_ready_count=1,
        )
        assert normalized.origin == retirement_origin
        assert len(normalized.room_segments) == 1
        segment = normalized.room_segments[0]
        assert segment.room_id == room_id
        assert segment.state_json == ()
        assert segment.timeline_json == ()
        assert segment.room_account_data_json == ()
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            selected.frame_id,
            normalized,
            (first_aggregate.continuity,),
        )
        assert len(proposal.room_proposals) == 1
        room = proposal.room_proposals[0]
        assert room.before == first_aggregate.continuity
        expected_continuity = RoomContinuity(
            room_id,
            1,
            "leave",
            None,
            None,
            None,
        )
        assert room.after == expected_continuity
        assert room.retirement_epoch == 0
        assert room.losses == (
            LossProposal(
                room_id,
                0,
                LossReason.UNVERIFIABLE,
                LossBoundary(None, None, None, None),
            ),
        )
        assert room.release is RecoveryRelease.LOSS_THEN_HELD
        assert tuple(descriptor.kind for descriptor in proposal.descriptors) == (
            RecordKind.EPHEMERAL,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordKind.PRESENCE,
        )
        assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_RETIREMENT,
            DescriptorRoute.READY,
            DescriptorRoute.READY,
        )
        assert tuple(descriptor.room_id for descriptor in proposal.descriptors) == (
            room_id,
            None,
            None,
        )
        assert tuple(
            descriptor.descriptor_key for descriptor in proposal.descriptors
        ) == (
            f"frame:{selected.frame_id}:0",
            f"frame:{selected.frame_id}:1",
            f"frame:{selected.frame_id}:2",
        )
        assert tuple(descriptor.source_json for descriptor in proposal.descriptors) == (
            ephemeral_json,
            global_json,
            presence_json,
        )
        assert tuple(descriptor.provenance for descriptor in proposal.descriptors) == (
            None,
            None,
            None,
        )

        old_graph = _materializer_storage_graph(journal)
        old_owner, old_source, old_frames, _old_aggregates, old_work = old_graph
        assert old_owner.revision == 3
        old_frames_by_id = {str(row[0]): row for row in old_frames}
        assert old_frames_by_id[str(first.frame_id)] == first_raw_before
        selected_before = old_frames_by_id[str(selected.frame_id)]
        assert selected_before[6] is None

        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            4,
        )

        owner, source, frames, aggregates, work = _materializer_storage_graph(journal)
        assert owner == replace(old_owner, revision=4)
        assert source == old_source
        assert len(aggregates) == 1
        expected_aggregate = RoomAggregateValue(expected_continuity, 3, 4, None)
        assert aggregates[0][:3] == (room_id, 4, None)
        aggregate_plaintext, aggregate = _decrypt_aggregate(journal, aggregates[0])
        assert aggregate == expected_aggregate
        assert aggregate_plaintext == _rows()._canonical_room_aggregate_plaintext(
            expected_aggregate
        )

        loss_without_id = LossRecord(
            "",
            retirement_origin,
            room_id,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        loss = replace(
            loss_without_id,
            loss_id=_loss_id(owner.stream_id, loss_without_id),
        )
        lifecycle = EventRecord(
            str(uuid5(selected.frame_id, f"lifecycle:{room_id}:0:1")),
            RecordKind.ROOM_LIFECYCLE,
            retirement_origin,
            room_id,
            1,
            1,
            None,
            None,
            canonical_json(
                {
                    "membership": "leave",
                    "membership_epoch": 1,
                    "previous_membership_epoch": 0,
                }
            ),
            None,
        )
        ephemeral = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:0")),
            RecordKind.EPHEMERAL,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 0),
            room_id,
            1,
            2,
            None,
            None,
            ephemeral_json,
            None,
        )
        global_account_data = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:1")),
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 1),
            None,
            None,
            None,
            None,
            None,
            global_json,
            None,
        )
        presence = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:2")),
            RecordKind.PRESENCE,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 2),
            None,
            None,
            None,
            None,
            None,
            presence_json,
            None,
        )
        expected_work = (
            _ExpectedMaterializerWork(loss, "ready", selected.frame_id, 4, 0, 4),
            _ExpectedMaterializerWork(held, "ready", first.frame_id, 4, 1, 2),
            _ExpectedMaterializerWork(
                lifecycle,
                "ready",
                selected.frame_id,
                4,
                2,
                4,
            ),
            _ExpectedMaterializerWork(
                ephemeral,
                "ready",
                selected.frame_id,
                4,
                3,
                4,
            ),
            _ExpectedMaterializerWork(
                global_account_data,
                "ready",
                selected.frame_id,
                4,
                4,
                4,
            ),
            _ExpectedMaterializerWork(
                presence,
                "ready",
                selected.frame_id,
                4,
                5,
                4,
            ),
        )
        _assert_exact_materializer_work(journal, work, expected_work, old_work)
        assert tuple(row[8] for row in work) == tuple(range(6))
        assert tuple(row[6] for row in work[1:4]) == (0, 1, 2)

        frames_by_id = {str(row[0]): row for row in frames}
        assert frames_by_id[str(first.frame_id)] == first_raw_before
        selected_after = frames_by_id[str(selected.frame_id)]
        assert selected_after[:6] == selected_before[:6]
        assert selected_after[6] == 4
        assert selected_after[7] != selected_before[7]
        assert journal.load_frame(first.frame_id) == first
        assert journal.load_frame(selected.frame_id) == selected
        assert (
            EncryptedRowCodec(
                "discovery-secret",
                journal.account_id,
                owner.stream_id,
            ).decrypt(
                "NioIngestFrameDrainHeader",
                (selected.frame_id,),
                selected_after[7],
                hashlib.sha256(b"").digest(),
                header=_canonical_expected_drain_header(selected_after, 4),
            )
            == b""
        )

        committed_graph = _materializer_storage_graph(journal)
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.IDLE,
            None,
            None,
        )
        assert _materializer_storage_graph(journal) == committed_graph
    finally:
        bootstrap.close()


def _expected_materializer_hook_labels(scenario: str) -> tuple[str, ...]:
    # Both live one-room paths have exactly one Aggregate write. A
    # multi-Aggregate occurrence does not exist at this checkpoint.
    if scenario == _ATOMICITY_H1_CLASSIC_PLAIN:
        return (
            "meta_revision_epoch_cas",
            "aggregate_insert",
            "work_insert",
            "work_insert",
            "frame_delete",
            "before_commit",
            "commit",
        )
    assert scenario == _ATOMICITY_RETIREMENT_SLIDING_CRYPTO
    return (
        "meta_revision_epoch_cas",
        "aggregate_update",
        "work_insert",
        "work_insert",
        "work_insert",
        "work_insert",
        "work_release",
        "frame_crypto_retain",
        "before_commit",
        "commit",
    )


def _expected_materializer_hook_prefix(
    scenario: str,
    boundary: str,
    occurrence: int,
) -> tuple[str, ...]:
    labels = _expected_materializer_hook_labels(scenario)
    seen = 0
    for index, label in enumerate(labels):
        if label == boundary:
            seen += 1
            if seen == occurrence:
                return labels[: index + 1]
    raise AssertionError(f"missing {boundary}:{occurrence} in {scenario}")


def _classified_materializer_trace(events: list[str]) -> tuple[str, ...]:
    classified: list[str] = []
    for event in events:
        if event.startswith("hook:"):
            classified.append(event)
            continue
        sql = " ".join(event.upper().split())
        if sql == "BEGIN IMMEDIATE":
            classified.append("transaction:begin_immediate")
        elif sql == "COMMIT":
            classified.append("transaction:commit")
        elif sql.startswith("UPDATE NIOINGESTMETA SET REVISION ="):
            classified.append("dml:meta_revision_epoch_cas")
        elif sql.startswith("INSERT INTO NIOINGESTROOMAGGREGATE"):
            classified.append("dml:aggregate_insert")
        elif sql.startswith("UPDATE NIOINGESTROOMAGGREGATE SET"):
            classified.append("dml:aggregate_update")
        elif sql.startswith("INSERT INTO NIOINGESTWORK"):
            classified.append("dml:work_insert")
        elif sql.startswith("UPDATE NIOINGESTWORK SET STATUS = 'READY'"):
            classified.append("dml:work_release")
        elif sql.startswith("DELETE FROM NIOINGESTFRAME"):
            classified.append("dml:frame_delete")
        elif sql.startswith("UPDATE NIOINGESTFRAME SET ROOM_MATERIALIZED_REVISION ="):
            classified.append("dml:frame_crypto_retain")
    return tuple(classified)


def _expected_materializer_trace(scenario: str) -> tuple[str, ...]:
    expected = ["transaction:begin_immediate"]
    for label in _expected_materializer_hook_labels(scenario):
        if label == "before_commit":
            expected.append("hook:before_commit")
        elif label == "commit":
            expected.extend(("transaction:commit", "hook:commit"))
        else:
            expected.extend((f"dml:{label}", f"hook:{label}"))
    return tuple(expected)


@pytest.mark.parametrize(
    "scenario",
    [
        pytest.param(_ATOMICITY_H1_CLASSIC_PLAIN, id="h1-insert"),
        pytest.param(
            _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
            id="retirement-update",
        ),
    ],
)
def test_materializer_success_hook_immediately_follows_each_business_dml(
    tmp_path: Path,
    scenario: str,
) -> None:
    events: list[str] = []
    case = _prepare_materializer_atomicity_case(
        tmp_path,
        scenario,
        statements=events,
    )
    bootstrap = case.bootstrap
    journal = bootstrap._journal
    try:
        old_graph = _materializer_storage_graph(journal)
        events.clear()
        journal.set_transition_statement_hook(
            lambda label: events.append(f"hook:{label}")
        )

        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            case.selected.frame_id,
            old_graph[0].revision + 1,
        )

        assert _classified_materializer_trace(events) == (
            _expected_materializer_trace(scenario)
        )
        _assert_materializer_committed_graph(journal, scenario, case, old_graph)
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    ("scenario", "boundary", "occurrence"),
    _MATERIALIZER_ROLLBACK_CASES,
)
def test_materializer_in_process_atomicity_failure_rolls_back_and_retry_commits_once(
    tmp_path: Path,
    scenario: str,
    boundary: str,
    occurrence: int,
) -> None:
    case = _prepare_materializer_atomicity_case(tmp_path, scenario)
    bootstrap = case.bootstrap
    selected = case.selected
    journal = bootstrap._journal
    try:
        old_graph = _materializer_storage_graph(journal)
        observed = 0

        def fail_at_boundary(label: str) -> None:
            nonlocal observed
            if label != boundary:
                return
            observed += 1
            if observed == occurrence:
                raise _InjectedMaterializerFailure(f"{label}:{occurrence}")

        journal.set_transition_statement_hook(fail_at_boundary)
        with pytest.raises(
            _InjectedMaterializerFailure,
            match=rf"^{re.escape(boundary)}:{occurrence}$",
        ):
            _materialize(journal)

        assert observed == occurrence
        assert _materializer_storage_graph(journal) == old_graph

        journal.set_transition_statement_hook(None)
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            old_graph[0].revision + 1,
        )
        _assert_materializer_committed_graph(journal, scenario, case, old_graph)
        committed_graph = _materializer_storage_graph(journal)
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.IDLE,
            None,
            None,
        )
        assert _materializer_storage_graph(journal) == committed_graph
    finally:
        bootstrap.close()


def _kill_materializer_at_boundary(
    store_path: Path,
    scenario: str,
    boundary: str,
    occurrence: int,
    sequence_path: Path,
) -> None:
    transport = (
        TransportKind.CLASSIC
        if scenario == _ATOMICITY_H1_CLASSIC_PLAIN
        else TransportKind.SLIDING
    )
    bootstrap = _open_discovery_journal(store_path, transport)
    journal = bootstrap._journal
    observed = 0

    def kill_at_boundary(label: str) -> None:
        nonlocal observed
        with sequence_path.open("a", encoding="utf-8") as sequence:
            sequence.write(f"{label}\n")
            sequence.flush()
            os.fsync(sequence.fileno())
        if label == boundary:
            observed += 1
            if observed == occurrence:
                os._exit(_MATERIALIZER_CRASH_EXIT_CODE)

    journal.set_transition_statement_hook(kill_at_boundary)
    _materialize(journal)
    bootstrap.close()


@pytest.mark.parametrize(
    ("scenario", "boundary", "occurrence"),
    _MATERIALIZER_CRASH_CASES,
)
def test_materializer_crash_boundary_reopens_old_or_complete_new_graph(
    tmp_path: Path,
    scenario: str,
    boundary: str,
    occurrence: int,
) -> None:
    store_path = tmp_path / f"{scenario}-{boundary}-{occurrence}"
    case = _prepare_materializer_atomicity_case(store_path, scenario)
    bootstrap = case.bootstrap
    selected = case.selected
    old_graph = _materializer_storage_graph(bootstrap._journal)
    bootstrap.close()
    sequence_path = store_path / "materializer-hook-sequence.txt"

    process = multiprocessing.get_context("spawn").Process(
        target=_kill_materializer_at_boundary,
        args=(store_path, scenario, boundary, occurrence, sequence_path),
    )
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("materializer crash-injection child did not exit")
    observed = tuple(sequence_path.read_text(encoding="utf-8").splitlines())
    assert (process.exitcode, observed) == (
        _MATERIALIZER_CRASH_EXIT_CODE,
        _expected_materializer_hook_prefix(scenario, boundary, occurrence),
    )

    transport = (
        TransportKind.CLASSIC
        if scenario == _ATOMICITY_H1_CLASSIC_PLAIN
        else TransportKind.SLIDING
    )
    reopened = _open_discovery_journal(store_path, transport)
    journal = reopened._journal
    try:
        if boundary == "commit":
            _assert_materializer_committed_graph(
                journal,
                scenario,
                case,
                old_graph,
            )
            committed_graph = _materializer_storage_graph(journal)
            assert _materialize(journal) == MaterializeResult(
                MaterializeStatus.IDLE,
                None,
                None,
            )
            assert _materializer_storage_graph(journal) == committed_graph
        else:
            _assert_materializer_reopened_graph(journal, old_graph)
            assert _materialize(journal) == MaterializeResult(
                MaterializeStatus.MATERIALIZED,
                selected.frame_id,
                old_graph[0].revision + 1,
            )
            _assert_materializer_committed_graph(
                journal,
                scenario,
                case,
                old_graph,
            )
            committed_graph = _materializer_storage_graph(journal)
            assert _materialize(journal) == MaterializeResult(
                MaterializeStatus.IDLE,
                None,
                None,
            )
            assert _materializer_storage_graph(journal) == committed_graph
    finally:
        reopened.close()


def test_materializer_external_write_lock_waits_only_at_writer_and_retry_commits(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
        sqlite_busy_timeout_ms=100,
    )
    journal = bootstrap._journal
    external = sqlite3.connect(
        bootstrap.database_path,
        isolation_level=None,
        timeout=0,
    )
    try:
        selected, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=True,
            global_ready_count=0,
        )
        case = _MaterializerAtomicityCase(bootstrap, selected, normalized)
        old_graph = _materializer_storage_graph(journal)
        selected_decrypt_scopes: list[str | None] = []
        real_decrypt = EncryptedRowCodec.decrypt

        def observe_selected_decrypt(
            codec: EncryptedRowCodec,
            table: str,
            primary_key: tuple[str | int | UUID, ...],
            ciphertext: bytes,
            digest: bytes,
            header: bytes = b"",
        ) -> bytes:
            if table == "NioIngestFrame" and primary_key == (selected.frame_id,):
                selected_decrypt_scopes.append(journal._owner._outer_scope)
            return real_decrypt(
                codec,
                table,
                primary_key,
                ciphertext,
                digest,
                header,
            )

        monkeypatch.setattr(EncryptedRowCodec, "decrypt", observe_selected_decrypt)
        external.execute("BEGIN IMMEDIATE")
        statements.clear()
        started = time.monotonic()
        with pytest.raises(
            (sqlite3.OperationalError, PeeweeOperationalError),
            match="locked",
        ):
            _materialize(journal)
        elapsed = time.monotonic() - started

        assert 0.075 <= elapsed <= 0.500
        assert selected_decrypt_scopes == ["read"]
        assert _materializer_storage_graph(journal) == old_graph
        assert _materializer_dml(statements) == ()

        external.rollback()
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            old_graph[0].revision + 1,
        )
        _assert_materializer_committed_graph(
            journal,
            _ATOMICITY_H1_CLASSIC_PLAIN,
            case,
            old_graph,
        )
    finally:
        if external.in_transaction:
            external.rollback()
        external.close()
        bootstrap.close()


def _materializer_path_identity(path: Path) -> tuple[int, int]:
    metadata = path.stat()
    return metadata.st_dev, metadata.st_ino


@contextmanager
def _replaced_materializer_path_identity(path: Path) -> Iterator[None]:
    backup = path.with_name(f".{path.name}.materializer-test-backup")
    assert not backup.exists()
    os.replace(path, backup)
    try:
        if backup.stat().st_size:
            shutil.copyfile(backup, path)
        else:
            path.touch()
        yield
    finally:
        path.unlink(missing_ok=True)
        os.replace(backup, path)


@pytest.mark.parametrize(
    ("fence", "error_type"),
    [
        pytest.param("revision", JournalConflictError, id="revision"),
        pytest.param("writer-epoch", LocalProtocolError, id="writer-epoch"),
        pytest.param("lock-file", LocalProtocolError, id="lock-file"),
        pytest.param("database-inode", LocalProtocolError, id="database-inode"),
    ],
)
def test_materializer_writer_boundary_fence_race_writes_no_partial_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fence: str,
    error_type: type[BaseException],
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        selected, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=True,
            global_ready_count=0,
        )
        case = _MaterializerAtomicityCase(bootstrap, selected, normalized)
        old_graph = _materializer_storage_graph(journal)
        old_owner = old_graph[0]
        lock_path = journal._owner._lock.path
        old_lock_identity = _materializer_path_identity(lock_path)
        old_database_identity = _materializer_path_identity(journal.database_path)
        raced = False
        real_journal_write = type(journal._owner).journal_write

        @contextmanager
        def race_before_writer(owner: object) -> Iterator[None]:
            nonlocal raced
            assert owner is journal._owner
            assert not raced
            raced = True
            if fence in {"lock-file", "database-inode"}:
                path = lock_path if fence == "lock-file" else journal.database_path
                with _replaced_materializer_path_identity(path):
                    with real_journal_write(journal._owner):
                        yield
                return
            with sqlite3.connect(journal.database_path) as connection:
                if fence == "revision":
                    connection.execute(
                        "UPDATE NioIngestMeta SET revision = ? WHERE account_id = ?",
                        (old_owner.revision + 1, journal.account_id),
                    )
                else:
                    connection.execute(
                        "UPDATE NioIngestMeta SET writer_epoch = ? WHERE account_id = ?",
                        (str(uuid4()), journal.account_id),
                    )
            with real_journal_write(journal._owner):
                yield

        monkeypatch.setattr(
            type(journal._owner),
            "journal_write",
            race_before_writer,
        )
        statements.clear()
        with pytest.raises(error_type):
            _materialize(journal)
        assert raced
        assert _materializer_dml(statements) == ()
        assert _materializer_path_identity(lock_path) == old_lock_identity
        assert (
            _materializer_path_identity(journal.database_path) == old_database_identity
        )

        if fence in {"revision", "writer-epoch"}:
            with sqlite3.connect(journal.database_path) as connection:
                connection.execute(
                    "UPDATE NioIngestMeta SET revision = ?, writer_epoch = ? "
                    "WHERE account_id = ?",
                    (
                        old_owner.revision,
                        str(old_owner.writer_epoch),
                        journal.account_id,
                    ),
                )
        assert _materializer_storage_graph(journal) == old_graph

        monkeypatch.undo()
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            old_owner.revision + 1,
        )
        _assert_materializer_committed_graph(
            journal,
            _ATOMICITY_H1_CLASSIC_PLAIN,
            case,
            old_graph,
        )
    finally:
        bootstrap.close()
