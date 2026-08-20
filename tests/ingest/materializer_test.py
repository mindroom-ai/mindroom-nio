"""Task 6 private materializer contract RED tests."""

import base64
import hashlib
import importlib
import json
import multiprocessing
import os
import re
import shutil
import sqlite3
import time
from collections.abc import Iterator
from contextlib import contextmanager
from dataclasses import dataclass, replace
from operator import itemgetter
from pathlib import Path
from types import ModuleType
from typing import NoReturn
from uuid import UUID, uuid4, uuid5

import pytest
from peewee import OperationalError as PeeweeOperationalError

import nio.ingest as ingest
import nio.ingest.classic as classic_module
import nio.ingest.ports as ports_module
import nio.ingest.sliding as sliding_module
import nio.store as store
import nio.store._sync_journal_plan as journal_plan_module
from nio.event_provenance import TimelineEventProvenance
from nio.exceptions import LocalProtocolError
from nio.ingest import source
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.errors import (
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
    RoomSnapshot,
    TransportKind,
    _CallbackRoute,
    _DecryptedToDeviceKind,
    _DecryptionDisposition,
    _MembershipProvenance,
    _MembershipSourceKind,
    _PreparationPhase,
    _PreparedCryptoDelta,
    _PreparedIngestionFrame,
    _PreparedIngestionRecord,
    _PreparedKeyClaim,
    _PreparedMegolmRerequest,
    _PreparedMembershipTransition,
    _PreparedQueuedToDeviceMessage,
    _PreparedWaitingKeyRequest,
    _QueuedToDeviceSubtype,
)
from nio.ingest.ports import (
    NetworkRequest,
    NetworkResult,
    StagedSourceResponse,
    _frame_id_for_response,
)
from nio.ingest.reducer import (
    DescriptorRoute,
    FrameProposal,
    HydrationIntent,
    LossProposal,
    MembershipBaseline,
    PreparedRecordStep,
    PreparedRecoveryStep,
    PreparedTransitionStep,
    RecoveryGap,
    RecoveryRelease,
    ReducerInputError,
    RoomContinuity,
    RoomProposal,
    reduce_prepared_frame,
    reduce_staged_frame,
)
from nio.ingest.serialization import _loss_id
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
from nio.store import _sync_journal_rows as journal_rows_module
from nio.store._sync_journal import SqliteIngestionJournal
from nio.store._sync_journal_plan import AuthenticatedWork, plan_frame_materialization
from nio.store._sync_journal_rows import _canonical_internal, _frame_envelope
from nio.store._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
    RoomAggregateValue,
)
from nio.store.sync_journal import StoreBootstrap, open_ingestion_store

_FRAME_ID = UUID("12345678-1234-5678-1234-567812345678")
_STREAM_ID = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")
_CONSUMER_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
_SOURCE_BODY_LIMIT = 16 * 1024 * 1024
_FRAME_ENVELOPE_LIMIT = 24 * 1024 * 1024
_WORK_PAYLOAD_LIMIT = 1 * 1024 * 1024
_PLANNER_ACCOUNT_ID = "@planner:example.org"
_PLANNER_EXISTING_FRAME_ID = UUID("12345678-1234-5678-1234-567812345681")
_PLANNER_READY_FRAME_ID = UUID("12345678-1234-5678-1234-567812345682")
_AGGREGATE_COLUMNS = (
    ("account_id", "TEXT", True, 1),
    ("room_id", "TEXT", True, 2),
    ("updated_revision", "INTEGER", True, 0),
    ("intent_kind", "TEXT", False, 0),
    ("payload", "BLOB", True, 0),
    ("payload_sha256", "BLOB", True, 0),
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
    ("payload", "BLOB", True, 0),
    ("payload_sha256", "BLOB", True, 0),
)


def _values() -> ModuleType:
    return importlib.import_module("nio.store._sync_journal_values")


def _rows() -> ModuleType:
    return importlib.import_module("nio.store._sync_journal_rows")


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


_AGGREGATE_ROOM_ID = "!aggregate:example.org"
_HYDRATION_ID = UUID("12345678-1234-5678-9234-567812345678")


@pytest.mark.parametrize(
    "corruption",
    (
        "payload-only",
        "digest-only",
        "recomputed-noncanonical",
        "semantic",
        "clear-account_id",
        "context-stream_id",
        "context-transport_kind",
        "clear-room_id",
        "clear-updated_revision",
        "clear-intent_kind",
    ),
)
def test_materializer_plaintext_aggregate_load_rejects_accidental_corruption(
    tmp_path: Path,
    corruption: str,
) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_present=True,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        with journal._owner.read():
            row = journal._execute(
                "SELECT * FROM NioIngestRoomAggregate WHERE account_id = ?",
                (journal.account_id,),
            ).fetchone()
        assert row is not None
        assert tuple(row.keys()) == tuple(column[0] for column in _AGGREGATE_COLUMNS)
        payload = bytes(row["payload"])
        digest = bytes(row["payload_sha256"])
        lookup_room_id = row["room_id"]
        if corruption == "payload-only":
            payload = _flip_first(payload)
        elif corruption == "digest-only":
            digest = _flip_first(digest)
        elif corruption == "recomputed-noncanonical":
            payload += b" "
            digest = hashlib.sha256(payload).digest()
        elif corruption == "semantic":
            envelope = json.loads(payload)
            envelope["schema_version"] = 2
            payload = _canonical_internal(envelope)
            digest = hashlib.sha256(payload).digest()
        elif corruption.startswith("context-"):
            envelope = json.loads(payload)
            field = corruption.removeprefix("context-")
            envelope[field] = (
                str(uuid4()) if field == "stream_id" else TransportKind.SLIDING.value
            )
            payload = _canonical_internal(envelope)
            digest = hashlib.sha256(payload).digest()
        if corruption.startswith("clear-"):
            field = corruption.removeprefix("clear-")
            with sqlite3.connect(journal.database_path) as connection:
                if field == "account_id":
                    changed = "@drift:example.org"
                    connection.execute("PRAGMA foreign_keys = OFF")
                    owner_update = connection.execute(
                        "UPDATE NioIngestMeta SET account_id = ? WHERE account_id = ?",
                        (changed, journal.account_id),
                    )
                    assert owner_update.rowcount == 1
                    row_update = connection.execute(
                        "UPDATE NioIngestRoomAggregate SET account_id = ? "
                        "WHERE account_id = ? AND room_id = ?",
                        (changed, journal.account_id, lookup_room_id),
                    )
                    assert row_update.rowcount == 1
                    journal.account_id = changed
                    journal._owner._account_id = changed
                else:
                    changed = {
                        "room_id": "!drift:example.org",
                        "updated_revision": row["updated_revision"] + 1,
                        "intent_kind": None,
                    }[field]
                    if field == "updated_revision":
                        owner_update = connection.execute(
                            "UPDATE NioIngestMeta SET revision = ? "
                            "WHERE account_id = ?",
                            (changed, journal.account_id),
                        )
                        assert owner_update.rowcount == 1
                    row_update = connection.execute(
                        f"UPDATE NioIngestRoomAggregate SET {field} = ? "
                        "WHERE account_id = ? AND room_id = ?",
                        (changed, journal.account_id, lookup_room_id),
                    )
                    assert row_update.rowcount == 1
                    if field == "room_id":
                        lookup_room_id = changed
                stored = connection.execute(
                    "SELECT payload, payload_sha256 FROM NioIngestRoomAggregate "
                    "WHERE account_id = ? AND room_id = ?",
                    (journal.account_id, lookup_room_id),
                ).fetchone()
                assert stored == (payload, digest)
        else:
            with journal._owner.journal_write():
                updated = journal._execute(
                    "UPDATE NioIngestRoomAggregate SET payload = ?, "
                    "payload_sha256 = ? WHERE account_id = ? AND room_id = ?",
                    (payload, digest, journal.account_id, lookup_room_id),
                )
                assert updated.rowcount == 1

        owner = journal.load_owner()
        with journal._owner.read(), pytest.raises(JournalIntegrityError):
            journal._load_room_aggregate(owner, lookup_room_id)
    finally:
        bootstrap.close()


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


def _expected_stored_work_payload(
    *,
    account_id: str,
    stream_id: UUID,
    transport_kind: TransportKind,
    frame_id: UUID,
    value: EventRecord | LossRecord,
    status: str,
    ready_revision: int | None,
    ready_ordinal: int | None,
    created_revision: int,
) -> bytes:
    kind = "event" if type(value) is EventRecord else "loss"
    inner = (
        _expected_event_work_plaintext(value)
        if type(value) is EventRecord
        else _expected_loss_work_plaintext(value)
    )
    return _canonical_internal(
        {
            "schema_version": 1,
            "row_kind": "work",
            "account_id": account_id,
            "stream_id": str(stream_id),
            "transport_kind": transport_kind.value,
            "work_id": (
                value.record_id if type(value) is EventRecord else value.loss_id
            ),
            "kind": kind,
            "status": status,
            "frame_id": str(frame_id),
            "room_id": value.room_id,
            "membership_epoch": value.membership_epoch,
            "room_sequence": (
                value.room_sequence if type(value) is EventRecord else None
            ),
            "ready_revision": ready_revision,
            "ready_ordinal": ready_ordinal,
            "created_revision": created_revision,
            "value": json.loads(inner),
        }
    )


def _expected_planned_stored_work_payload(
    *,
    account_id: str,
    stream_id: UUID = _STREAM_ID,
    frame: SyncFrame,
    value: EventRecord | LossRecord,
    ordinal: int | None,
    revision: int,
) -> bytes:
    return _expected_stored_work_payload(
        account_id=account_id,
        stream_id=stream_id,
        transport_kind=frame.origin.transport,
        frame_id=frame.frame_id,
        value=value,
        status="held" if ordinal is None else "ready",
        ready_revision=None if ordinal is None else revision,
        ready_ordinal=ordinal,
        created_revision=revision,
    )


def test_stored_work_release_row_keeps_authenticated_identity() -> None:
    """A release retains its authenticated frame and creation revision."""

    value = _event_record()
    plaintext = _expected_event_work_plaintext(value)
    frame_id = UUID("12345678-1234-5678-1234-567812345680")
    stored = journal_plan_module._stored_work_release_row(
        journal_plan_module.PlannedWork(value, plaintext, 7),
        AuthenticatedWork(
            value,
            "held",
            1,
            plaintext=plaintext,
            frame_id=frame_id,
            created_revision=3,
        ),
        revision=11,
    )

    assert stored == journal_plan_module._StoredWorkRow(
        value.record_id,
        "event",
        "ready",
        frame_id,
        None,
        None,
        None,
        11,
        7,
        3,
        plaintext,
    )


def test_materializer_release_preserves_authenticated_envelope_bytes(
    tmp_path: Path,
) -> None:
    """A released HELD event keeps its original authenticated row identity."""

    case = _prepare_materializer_atomicity_case(
        tmp_path,
        _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
    )
    bootstrap = case.bootstrap
    journal = bootstrap._journal
    try:
        first = case.first
        first_normalized = case.first_normalized
        assert first is not None
        assert first_normalized is not None
        held = EventRecord(
            str(uuid5(first.frame_id, f"event:frame:{first.frame_id}:0")),
            RecordKind.TIMELINE,
            first_normalized.origin,
            "!unsupported:example.org",
            0,
            0,
            None,
            TimelineEventProvenance.HISTORY,
            b'{"content":{"body":"held","msgtype":"m.text"},'
            b'"type":"m.room.message"}',
            None,
        )
        owner_before = journal.load_owner()
        release_revision = owner_before.revision + 1

        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            case.selected.frame_id,
            release_revision,
        )
        row = next(row for row in _work_rows(journal) if row[0] == held.record_id)
        expected_payload = _expected_plaintext_materializer_envelope(
            row_kind="work",
            owner=owner_before,
            clear_fields=(
                ("work_id", held.record_id),
                ("kind", "event"),
                ("status", "ready"),
                ("frame_id", str(first.frame_id)),
                ("room_id", "!unsupported:example.org"),
                ("membership_epoch", 0),
                ("room_sequence", 0),
                ("ready_revision", release_revision),
                ("ready_ordinal", 1),
                ("created_revision", 2),
            ),
            value={
                "kind": "event",
                "value": {
                    "clear_json": None,
                    "event_id": None,
                    "kind": "timeline",
                    "membership_epoch": 0,
                    "origin": {
                        "frame_index": 0,
                        "origin_type": "transport",
                        "request_id": 0,
                        "source_epoch": 0,
                        "transport": "sliding",
                    },
                    "provenance": "history",
                    "record_id": held.record_id,
                    "record_type": "event",
                    "room_id": "!unsupported:example.org",
                    "room_sequence": 0,
                    "source_json": base64.b64encode(held.source_json).decode("ascii"),
                },
            },
        )
        assert row[:10] == (
            held.record_id,
            "event",
            "ready",
            str(first.frame_id),
            "!unsupported:example.org",
            0,
            0,
            release_revision,
            1,
            2,
        )
        assert row[10] == expected_payload
        assert row[11] == hashlib.sha256(expected_payload).digest()
    finally:
        bootstrap.close()


def _pending_hydration_planner_case(
    *,
    global_ready_count: int = 0,
    account_id: str = _PLANNER_ACCOUNT_ID,
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
    held_payload = _expected_stored_work_payload(
        account_id=account_id,
        stream_id=_STREAM_ID,
        transport_kind=TransportKind.CLASSIC,
        frame_id=_PLANNER_EXISTING_FRAME_ID,
        value=held,
        status="held",
        ready_revision=None,
        ready_ordinal=None,
        created_revision=1,
    )
    return (
        frame,
        aggregate,
        AuthenticatedWork(
            held,
            "held",
            len(held_payload),
        ),
    )


def _pending_hydration_ephemeral_planner_case(
    *, account_id: str = _PLANNER_ACCOUNT_ID
) -> tuple[SyncFrame, RoomAggregateValue, AuthenticatedWork]:
    base, aggregate, held = _pending_hydration_planner_case(account_id=account_id)
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
        canonical_size=len(
            _expected_stored_work_payload(
                account_id=account_id,
                stream_id=_STREAM_ID,
                transport_kind=TransportKind.SLIDING,
                frame_id=_PLANNER_EXISTING_FRAME_ID,
                value=held_value,
                status="held",
                ready_revision=None,
                ready_ordinal=None,
                created_revision=1,
            )
        ),
    )
    return frame, aggregate, held


def _retirement_capacity_planner_case(
    successor_json: tuple[bytes, ...],
    *,
    room_id: str = "!planner-retire:example.org",
    account_id: str = _PLANNER_ACCOUNT_ID,
) -> tuple[SyncFrame, RoomAggregateValue, AuthenticatedWork]:
    frame_origin = RecordOrigin(TransportKind.CLASSIC, 1, 2, 0)
    hydration_origin = RecordOrigin(TransportKind.CLASSIC, 1, 1, 0)
    hydration_id = uuid5(_STREAM_ID, "planner-retirement-hydration")
    continuity = RoomContinuity(room_id, 0, "join", None, None, hydration_id)
    aggregate = RoomAggregateValue(
        continuity,
        1,
        1,
        HydrationIntent(hydration_id, hydration_origin),
    )
    segment = RoomSegment(
        room_id,
        RoomSection.LEAVE,
        (),
        successor_json,
        (),
        False,
        None,
        False,
        False,
        len(successor_json),
        MembershipObservation(
            "leave",
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
        b"r" * 32,
        (),
        b'{"changed":[],"left":[]}',
        b"{}",
        b"null",
        (segment,),
        (),
        (b'{"content":{"generation":2},"type":"m.push_rules"}',),
        (),
    )
    held = EventRecord(
        str(uuid5(_FRAME_ID, "planner-retirement-held")),
        RecordKind.TIMELINE,
        hydration_origin,
        room_id,
        0,
        0,
        None,
        TimelineEventProvenance.HISTORY,
        b'{"content":{"body":"old"},"type":"m.room.message"}',
        None,
    )
    held_payload = _expected_stored_work_payload(
        account_id=account_id,
        stream_id=_STREAM_ID,
        transport_kind=TransportKind.CLASSIC,
        frame_id=_PLANNER_EXISTING_FRAME_ID,
        value=held,
        status="held",
        ready_revision=None,
        ready_ordinal=None,
        created_revision=1,
    )
    return (
        frame,
        aggregate,
        AuthenticatedWork(
            held,
            "held",
            len(held_payload),
        ),
    )


def _expected_retirement_capacity_records(
    successor_json: tuple[bytes, ...],
    global_json: tuple[bytes, ...],
    *,
    room_id: str = "!planner-retire:example.org",
    capacity_reason: LossReason = LossReason.EVENT_LIMIT,
) -> tuple[
    LossRecord,
    EventRecord,
    tuple[EventRecord, ...],
    tuple[EventRecord, ...],
    LossRecord,
]:
    origin = RecordOrigin(TransportKind.CLASSIC, 1, 2, 0)
    old_loss_without_id = LossRecord(
        "",
        origin,
        room_id,
        0,
        LossReason.UNVERIFIABLE,
        LossBoundary(None, None, None, None),
        b"{}",
    )
    old_loss = replace(
        old_loss_without_id,
        loss_id=_loss_id(_STREAM_ID, old_loss_without_id),
    )
    lifecycle = EventRecord(
        str(uuid5(_FRAME_ID, f"lifecycle:{room_id}:0:1")),
        RecordKind.ROOM_LIFECYCLE,
        origin,
        room_id,
        1,
        1,
        None,
        None,
        b'{"membership":"leave","membership_epoch":1,"previous_membership_epoch":0}',
        None,
    )
    successors = tuple(
        EventRecord(
            str(uuid5(_FRAME_ID, f"event:frame:{_FRAME_ID}:{index}")),
            RecordKind.TIMELINE,
            replace(origin, frame_index=index),
            room_id,
            1,
            index + 2,
            None,
            TimelineEventProvenance.LIVE,
            source_json,
            None,
        )
        for index, source_json in enumerate(successor_json)
    )
    globals_ = tuple(
        EventRecord(
            str(
                uuid5(
                    _FRAME_ID,
                    f"event:frame:{_FRAME_ID}:{len(successor_json) + index}",
                )
            ),
            RecordKind.GLOBAL_ACCOUNT_DATA,
            replace(origin, frame_index=len(successor_json) + index),
            None,
            None,
            None,
            None,
            None,
            source_json,
            None,
        )
        for index, source_json in enumerate(global_json)
    )
    capacity_loss_without_id = LossRecord(
        "",
        origin,
        room_id,
        1,
        capacity_reason,
        LossBoundary(None, None, None, None),
        b"{}",
    )
    capacity_loss = replace(
        capacity_loss_without_id,
        loss_id=_loss_id(_STREAM_ID, capacity_loss_without_id),
    )
    return old_loss, lifecycle, successors, globals_, capacity_loss


def _planner_ready_work(
    frame: SyncFrame,
    index: int,
    *,
    account_id: str = _PLANNER_ACCOUNT_ID,
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
    size = len(
        _expected_stored_work_payload(
            account_id=account_id,
            stream_id=_STREAM_ID,
            transport_kind=frame.origin.transport,
            frame_id=_PLANNER_READY_FRAME_ID,
            value=value,
            status="ready",
            ready_revision=1,
            ready_ordinal=index,
            created_revision=1,
        )
    )
    return AuthenticatedWork(
        value,
        "ready",
        size if canonical_size is None else canonical_size,
    )


_PREPARED_ROOM_ID = "!prepared:example.org"
_PREPARED_RECORD_IDS = (
    "d73ad21a-0139-53c4-ba03-f4a54604cce2",
    "4d4ea470-f18f-58d6-9df2-1f3cb8e1cd03",
    "b1992056-403e-5680-9dc8-7afc397cc799",
    "d4f5c251-5c82-5e7d-a3b7-1ae61c48fe48",
    "b3277f8a-1586-5a4a-8798-4351c7273854",
    "bf52e195-2ba6-5ebf-a65e-336f1aac8761",
    "9798b4d0-a2fc-5182-8cb2-2a1014e6c30f",
    "13a91a09-6a8d-5d10-8646-4dd93262a35e",
    "6ee55fe0-a7c5-5bed-983d-e6f2be6d7e61",
)
_PREPARED_TRANSITION_IDS = (
    "6de0ffa6-03bd-5c98-9557-50fc3375f36e",
    "c7723ba3-ebb3-5f2c-adb3-17e459874e98",
    "ca5c3383-7248-5f16-805e-aee50e28daf9",
    "f0da0e9c-c1f2-5cfc-85b9-4f3283a44839",
)
_PREPARED_ORIGIN = RecordOrigin(TransportKind.CLASSIC, 0, 1, 0)


@dataclass(frozen=True)
class _PreparedPlannerFixture:
    frame: SyncFrame
    prepared: _PreparedIngestionFrame
    aggregate: RoomAggregateValue


def _prepared_observation(
    room_membership: str | None = None,
    event_membership: str | None = None,
    event_id: str | None = None,
    previous_membership: str | None = None,
    *,
    live: bool = False,
    unparsed: bool = False,
) -> MembershipObservation:
    return MembershipObservation(
        room_membership,
        event_membership,
        event_id,
        previous_membership,
        None,
        live,
        False,
        False,
        unparsed,
    )


def _prepared_segment(
    observation: MembershipObservation,
    *,
    room_id: str = _PREPARED_ROOM_ID,
    section: RoomSection = RoomSection.UNCHANGED,
    state: tuple[bytes, ...] = (),
    timeline: tuple[bytes, ...] = (),
    room_account_data: tuple[bytes, ...] = (),
    live_event_count: int = 0,
) -> RoomSegment:
    return RoomSegment(
        room_id,
        section,
        state,
        timeline,
        room_account_data,
        False,
        None,
        False,
        False,
        live_event_count,
        observation,
    )


def _prepared_frame(
    *,
    room_segments: tuple[RoomSegment, ...] = (),
    to_device: tuple[bytes, ...] = (),
    ephemeral: tuple[bytes, ...] = (),
    global_account_data: tuple[bytes, ...] = (),
    presence: tuple[bytes, ...] = (),
) -> SyncFrame:
    return SyncFrame(
        _FRAME_ID,
        _PREPARED_ORIGIN,
        b'{"next_batch":"s0"}',
        b'{"next_batch":"s1"}',
        b"p" * 32,
        to_device,
        b'{"changed":[],"left":[]}',
        b"{}",
        b"null",
        room_segments,
        ephemeral,
        global_account_data,
        presence,
    )


def _prepared_aggregate(
    *,
    epoch: int,
    membership: str,
    next_sequence: int,
    room_id: str = _PREPARED_ROOM_ID,
    baseline: MembershipBaseline | None = None,
    gap: RecoveryGap | None = None,
    hydration_name: str | None = None,
    hydration_origin: RecordOrigin = RecordOrigin(TransportKind.CLASSIC, 0, 0, 0),
) -> RoomAggregateValue:
    hydration_id = None if hydration_name is None else uuid5(_STREAM_ID, hydration_name)
    return RoomAggregateValue(
        RoomContinuity(room_id, epoch, membership, baseline, gap, hydration_id),
        next_sequence,
        1,
        (
            None
            if hydration_id is None
            else HydrationIntent(hydration_id, hydration_origin)
        ),
    )


def _prepared_payload(
    frame: SyncFrame,
    records: tuple[_PreparedIngestionRecord, ...] = (),
    transitions: tuple[_PreparedMembershipTransition, ...] = (),
) -> _PreparedIngestionFrame:
    return _PreparedIngestionFrame(
        frame.frame_id,
        frame.origin.transport,
        frame.origin.source_epoch,
        frame.origin.request_id,
        2,
        frame.request_cursor_json,
        frame.candidate_cursor_json,
        frame.source_sha256,
        "s1",
        records,
        transitions,
        (),
        _PreparedCryptoDelta((), (), None, b"{}", b"null", (), ()),
    )


def _plan_prepared(
    case: _PreparedPlannerFixture,
    *,
    frame: SyncFrame | None = None,
    prepared: _PreparedIngestionFrame | None = None,
    aggregates: tuple[RoomAggregateValue, ...] | None = None,
    work: tuple[AuthenticatedWork, ...] = (),
    limits: MaterializerLimits | None = None,
) -> journal_plan_module.MaterializationPlan | None:
    return journal_plan_module.plan_prepared_frame_materialization(
        account_id=_PLANNER_ACCOUNT_ID,
        stream_id=_STREAM_ID,
        frame=case.frame if frame is None else frame,
        prepared=case.prepared if prepared is None else prepared,
        aggregates=(case.aggregate,) if aggregates is None else aggregates,
        work=work,
        revision=3,
        limits=MaterializerLimits() if limits is None else limits,
    )


def _prepared_metadata(
    record_id: str,
    event_type: str,
    route: _CallbackRoute | None,
    *,
    phase: _PreparationPhase = _PreparationPhase.SOURCE,
    decryption: _DecryptionDisposition = _DecryptionDisposition.NONE,
    verified: bool | None = None,
    decrypted_kind: _DecryptedToDeviceKind | None = None,
) -> journal_plan_module._PreparedWorkMetadata:
    return journal_plan_module._PreparedWorkMetadata(
        record_id,
        phase,
        event_type,
        decryption,
        verified,
        decrypted_kind,
        route,
    )


def _prepared_planned_record(
    record: _PreparedIngestionRecord,
    *,
    membership_epoch: int | None,
    room_sequence: int | None,
    ready_ordinal: int | None,
) -> journal_plan_module.PlannedWork:
    value = _prepared_work_event(
        record.record_id,
        record.kind,
        record.source_json,
        origin=record.origin,
        room_id=record.room_id,
        membership_epoch=membership_epoch,
        room_sequence=room_sequence,
        event_id=record.event_id,
        provenance=record.provenance,
        clear_json=record.clear_json,
    )
    return journal_plan_module.PlannedWork(
        value,
        journal_plan_module._canonical_work_plaintext(
            "event",
            value,
            _prepared_metadata(
                record.record_id,
                record.effective_event_type,
                record.callback_route,
                phase=record.preparation_phase,
                decryption=record.decryption,
                verified=record.decryption_verified,
                decrypted_kind=record.decrypted_to_device_kind,
            ),
        ),
        ready_ordinal,
    )


def _prepared_event(
    event_type: str,
    *,
    event_id: str | None = None,
    membership: str | None = None,
) -> bytes:
    content = {"membership": membership} if membership else {}
    value: dict[str, object] = {"content": content, "type": event_type}
    if event_id:
        value |= {"event_id": event_id, "state_key": _PLANNER_ACCOUNT_ID}
    return canonical_json(value)


def _prepared_work_event(
    record_id: str,
    kind: RecordKind,
    source_json: bytes,
    *,
    origin: RecordOrigin = _PREPARED_ORIGIN,
    room_id: str | None = None,
    membership_epoch: int | None = None,
    room_sequence: int | None = None,
    event_id: str | None = None,
    provenance: TimelineEventProvenance | None = None,
    clear_json: bytes | None = None,
) -> EventRecord:
    return EventRecord(
        record_id,
        kind,
        origin,
        room_id,
        membership_epoch,
        room_sequence,
        event_id,
        provenance,
        source_json,
        clear_json,
    )


def _prepared_room_held(
    record_id: str,
    *,
    membership_epoch: int,
    room_sequence: int,
    kind: RecordKind = RecordKind.ROOM_ACCOUNT_DATA,
    source_json: bytes | None = None,
    room_id: str = _PREPARED_ROOM_ID,
    origin: RecordOrigin = RecordOrigin(TransportKind.CLASSIC, 0, 0, 0),
    event_type: str | None = None,
    route: _CallbackRoute | None = None,
    frame_id: UUID | None = None,
    created_revision: int | None = None,
    canonical_size: int | None = None,
    exact_size_frame: SyncFrame | None = None,
) -> AuthenticatedWork:
    value = _prepared_work_event(
        record_id,
        kind,
        _prepared_event("m.tag") if source_json is None else source_json,
        origin=origin,
        room_id=room_id,
        membership_epoch=membership_epoch,
        room_sequence=room_sequence,
    )
    metadata = (
        None if event_type is None else _prepared_metadata(record_id, event_type, route)
    )
    plaintext = journal_plan_module._canonical_work_plaintext("event", value, metadata)
    if exact_size_frame is not None:
        assert frame_id is not None and created_revision is not None
        canonical_size = _prepared_stored_work_size(
            exact_size_frame,
            journal_plan_module.PlannedWork(value, plaintext, None),
            frame_id=frame_id,
            created_revision=created_revision,
        )
    return AuthenticatedWork(
        value,
        "held",
        len(plaintext) + 512 if canonical_size is None else canonical_size,
        metadata,
        plaintext,
        frame_id,
        created_revision,
    )


def _prepared_record(
    index: int,
    kind: RecordKind,
    source_json: bytes,
    *,
    event_type: str,
    room: bool = False,
    event_id: str | None = None,
    provenance: TimelineEventProvenance | None = None,
    clear_json: bytes | None = None,
    decryption: _DecryptionDisposition = _DecryptionDisposition.NONE,
    verified: bool | None = None,
    decrypted_kind: _DecryptedToDeviceKind | None = None,
    route: _CallbackRoute | None = None,
) -> _PreparedIngestionRecord:
    return _PreparedIngestionRecord(
        _PREPARED_RECORD_IDS[index],
        kind,
        replace(_PREPARED_ORIGIN, frame_index=index),
        _PreparationPhase.SOURCE,
        event_type,
        _PREPARED_ROOM_ID if room else None,
        event_id,
        provenance,
        source_json,
        clear_json,
        decryption,
        verified,
        decrypted_kind,
        route,
    )


def _prepared_transition(
    index: int,
    record: _PreparedIngestionRecord,
    previous: str | None,
    current: str,
    previous_epoch: int,
    current_epoch: int,
    source_kind: _MembershipSourceKind,
) -> _PreparedMembershipTransition:
    return _PreparedMembershipTransition(
        _PREPARED_TRANSITION_IDS[index],
        record.record_id,
        _PREPARED_ROOM_ID,
        record.event_id,
        previous,
        current,
        previous_epoch,
        current_epoch,
        source_kind,
        record.provenance,
        _MembershipProvenance.REPORTED,
        record.origin,
        record.source_json,
    )


def _prepared_planner_fixture(
    *, pending_hydration: bool = False
) -> _PreparedPlannerFixture:
    to_device_source = _prepared_event("m.room.encrypted")
    to_device_clear = _prepared_event("m.room_key")
    state_sources = (
        _prepared_event("m.room.member", event_id="$join", membership="join"),
        _prepared_event("m.room.member", event_id="$leave", membership="leave"),
    )
    timeline_sources = (
        _prepared_event("m.room.member", event_id="$rejoin", membership="join"),
        _prepared_event("m.room.member", event_id="$ban", membership="ban"),
    )
    ephemeral_source = _prepared_event("m.typing")
    room_account_data_source = _prepared_event("m.tag")
    presence_source = _prepared_event("m.presence")
    global_source = _prepared_event("m.push_rules")
    records = (
        _prepared_record(
            0,
            RecordKind.TO_DEVICE,
            to_device_source,
            event_type="m.room_key",
            clear_json=to_device_clear,
            decryption=_DecryptionDisposition.DECRYPTED,
            decrypted_kind=_DecryptedToDeviceKind.ROOM_KEY,
            route=_CallbackRoute.TO_DEVICE,
        ),
        _prepared_record(
            1,
            RecordKind.STATE,
            state_sources[0],
            event_type="m.room.member",
            room=True,
            event_id="$join",
        ),
        _prepared_record(
            2,
            RecordKind.STATE,
            state_sources[1],
            event_type="m.room.member",
            room=True,
            event_id="$leave",
        ),
        _prepared_record(
            3,
            RecordKind.TIMELINE,
            timeline_sources[0],
            event_type="m.room.member",
            room=True,
            event_id="$rejoin",
            provenance=TimelineEventProvenance.LIVE,
            route=_CallbackRoute.EVENT,
        ),
        _prepared_record(
            4,
            RecordKind.TIMELINE,
            timeline_sources[1],
            event_type="m.room.member",
            room=True,
            event_id="$ban",
            provenance=TimelineEventProvenance.LIVE,
            route=_CallbackRoute.EVENT,
        ),
        _prepared_record(
            5,
            RecordKind.EPHEMERAL,
            ephemeral_source,
            event_type="m.typing",
            room=True,
            route=_CallbackRoute.EPHEMERAL,
        ),
        _prepared_record(
            6,
            RecordKind.ROOM_ACCOUNT_DATA,
            room_account_data_source,
            event_type="m.tag",
            room=True,
            route=_CallbackRoute.ROOM_ACCOUNT_DATA,
        ),
        _prepared_record(
            7,
            RecordKind.PRESENCE,
            presence_source,
            event_type="m.presence",
            route=_CallbackRoute.PRESENCE,
        ),
        _prepared_record(
            8,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            global_source,
            event_type="m.push_rules",
            route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
        ),
    )
    transitions = (
        _prepared_transition(
            0, records[1], "invite", "join", 5, 5, _MembershipSourceKind.STATE
        ),
        _prepared_transition(
            1, records[2], "join", "leave", 5, 6, _MembershipSourceKind.STATE
        ),
        _prepared_transition(
            2, records[3], "leave", "join", 6, 6, _MembershipSourceKind.TIMELINE
        ),
        _prepared_transition(
            3, records[4], "join", "ban", 6, 7, _MembershipSourceKind.TIMELINE
        ),
    )
    segment = _prepared_segment(
        _prepared_observation("join", "ban", "$ban", "join", live=True),
        section=RoomSection.JOIN,
        state=state_sources,
        timeline=timeline_sources,
        room_account_data=(room_account_data_source,),
        live_event_count=2,
    )
    frame = _prepared_frame(
        room_segments=(segment,),
        to_device=(to_device_source,),
        ephemeral=(
            _ephemeral_envelope(_PREPARED_ROOM_ID, json.loads(ephemeral_source)),
        ),
        global_account_data=(global_source,),
        presence=(presence_source,),
    )
    prepared = _prepared_payload(frame, records, transitions)
    return _PreparedPlannerFixture(
        frame,
        prepared,
        _prepared_aggregate(
            epoch=5,
            membership="invite",
            next_sequence=7,
            baseline=None if pending_hydration else MembershipBaseline("$invite", None),
            hydration_name=(
                "prepared-pending-hydration" if pending_hydration else None
            ),
        ),
    )


def _prepared_sliding_timeline_case(
    event_ids: tuple[str | None, ...],
    *,
    initial: bool,
    live_event_count: int,
) -> _PreparedPlannerFixture:
    timeline = tuple(
        _prepared_event("m.room.message", event_id=event_id) for event_id in event_ids
    )
    segment = _prepared_segment(
        _prepared_observation("join", "join", "$member"),
        section=RoomSection.JOIN,
        timeline=timeline,
        live_event_count=live_event_count,
    )
    if initial:
        segment = replace(
            segment,
            initial=True,
            membership_observation=replace(
                segment.membership_observation,
                is_initial=True,
            ),
        )
    frame = replace(
        _prepared_frame(room_segments=(segment,)),
        origin=RecordOrigin(
            TransportKind.SLIDING,
            1 if initial else 0,
            0 if initial else 1,
            0,
        ),
        request_cursor_json=b"{}",
        candidate_cursor_json=b"{}",
    )
    records = tuple(
        _prepared_record(
            index,
            RecordKind.TIMELINE,
            source_json,
            event_type="m.room.message",
            room=True,
            event_id=event_id,
            provenance=(
                TimelineEventProvenance.HISTORY
                if initial
                else TimelineEventProvenance.LIVE
            ),
            route=_CallbackRoute.EVENT,
        )._replace(origin=replace(frame.origin, frame_index=index))
        for index, (event_id, source_json) in enumerate(
            zip(event_ids, timeline, strict=True)
        )
    )
    aggregate = _prepared_aggregate(
        epoch=3,
        membership="join",
        next_sequence=4,
        baseline=MembershipBaseline("$member", "room-old"),
    )
    aggregate = replace(
        aggregate,
        continuity=replace(
            aggregate.continuity,
            last_timeline_event_id="$anchor",
        ),
    )
    return _PreparedPlannerFixture(
        frame,
        _prepared_payload(frame, records)._replace(compatibility_token=None),
        aggregate,
    )


def test_prepared_reduction_preserves_linear_transition_order_and_epochs() -> None:
    case = _prepared_planner_fixture()

    reduction = reduce_prepared_frame(
        _STREAM_ID, case.frame, case.prepared, (case.aggregate.continuity,)
    )

    assert [
        (
            type(step).__name__,
            (
                step.transition.transition_id
                if hasattr(step, "transition")
                else step.record.record_id
            ),
            step.membership_epoch,
        )
        for step in reduction.linear_steps
    ] == [
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[0], None),
        ("PreparedTransitionStep", _PREPARED_TRANSITION_IDS[0], 5),
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[1], 5),
        ("PreparedTransitionStep", _PREPARED_TRANSITION_IDS[1], 6),
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[2], 6),
        ("PreparedTransitionStep", _PREPARED_TRANSITION_IDS[2], 6),
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[3], 6),
        ("PreparedTransitionStep", _PREPARED_TRANSITION_IDS[3], 7),
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[4], 7),
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[5], 7),
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[6], 7),
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[7], None),
        ("PreparedRecordStep", _PREPARED_RECORD_IDS[8], None),
    ]
    assert reduction.room_results[0].after == RoomContinuity(
        _PREPARED_ROOM_ID,
        7,
        "ban",
        None,
        None,
        None,
        last_timeline_event_id="$ban",
    )


def test_prepared_reduction_preserves_ephemeral_only_room_ownership() -> None:
    source_json = _prepared_event("m.typing")
    frame = _prepared_frame(
        ephemeral=(_ephemeral_envelope(_PREPARED_ROOM_ID, json.loads(source_json)),)
    )
    record = _prepared_record(
        0,
        RecordKind.EPHEMERAL,
        source_json,
        event_type="m.typing",
        room=True,
        route=_CallbackRoute.EPHEMERAL,
    )
    prepared = _prepared_payload(frame, (record,))
    before = RoomContinuity(
        _PREPARED_ROOM_ID,
        3,
        "join",
        MembershipBaseline("$join", None),
        None,
        None,
    )

    reduction = reduce_prepared_frame(_STREAM_ID, frame, prepared, (before,))

    assert reduction.linear_steps == (
        PreparedRecordStep(record, DescriptorRoute.READY, before.membership_epoch),
    )
    assert reduction.room_results == (
        RoomProposal(before, before, None, None, None, (), RecoveryRelease.NONE),
    )


def test_prepared_reduction_consumes_first_seen_hydration_on_departure() -> None:
    join_source = _prepared_event("m.room.member", event_id="$join", membership="join")
    leave_source = _prepared_event(
        "m.room.member", event_id="$leave", membership="leave"
    )
    segment = _prepared_segment(
        _prepared_observation("leave", "leave", "$leave", "join", live=True),
        section=RoomSection.LEAVE,
        state=(join_source,),
        timeline=(leave_source,),
        live_event_count=1,
    )
    frame = _prepared_frame(room_segments=(segment,))
    records = (
        _prepared_record(
            0,
            RecordKind.STATE,
            join_source,
            event_type="m.room.member",
            room=True,
            event_id="$join",
        ),
        _prepared_record(
            1,
            RecordKind.TIMELINE,
            leave_source,
            event_type="m.room.member",
            room=True,
            event_id="$leave",
            provenance=TimelineEventProvenance.LIVE,
            route=_CallbackRoute.EVENT,
        ),
    )
    transitions = (
        _prepared_transition(
            0, records[0], None, "join", 0, 0, _MembershipSourceKind.STATE
        ),
        _prepared_transition(
            1,
            records[1],
            "join",
            "leave",
            0,
            1,
            _MembershipSourceKind.TIMELINE,
        ),
    )
    prepared = _prepared_payload(frame, records, transitions)

    reduction = reduce_prepared_frame(_STREAM_ID, frame, prepared, ())

    assert reduction.linear_steps == (
        PreparedTransitionStep(transitions[0], 0, None, None),
        PreparedRecordStep(records[0], DescriptorRoute.HOLD_FOR_HYDRATION, 0),
        PreparedTransitionStep(
            transitions[1],
            1,
            LossProposal(
                _PREPARED_ROOM_ID,
                0,
                LossReason.UNVERIFIABLE,
                LossBoundary(None, None, None, None),
            ),
            0,
        ),
        PreparedRecordStep(records[1], DescriptorRoute.READY, 1),
    )
    assert reduction.room_results == (
        RoomProposal(
            None,
            RoomContinuity(
                _PREPARED_ROOM_ID,
                1,
                "leave",
                None,
                None,
                None,
                last_timeline_event_id="$leave",
            ),
            None,
            None,
            None,
            (),
            RecoveryRelease.NONE,
        ),
    )


def _prepared_section_transition(
    frame: SyncFrame,
    index: int,
    previous: str,
    current: str,
    previous_epoch: int,
    current_epoch: int,
    *,
    room_id: str = _PREPARED_ROOM_ID,
    frame_index: int = 1,
) -> _PreparedMembershipTransition:
    return _PreparedMembershipTransition(
        str(uuid5(frame.frame_id, f"record:transition:{index}")),
        None,
        room_id,
        None,
        previous,
        current,
        previous_epoch,
        current_epoch,
        _MembershipSourceKind.SECTION,
        None,
        _MembershipProvenance.REPORTED,
        replace(frame.origin, frame_index=frame_index),
        None,
    )


@pytest.mark.parametrize(
    "malformation",
    (
        "decreasing_anchors",
        "duplicate_section",
        "mixed_section_and_linked",
        "current_membership_domain",
        "previous_membership_domain",
        "no_op",
    ),
)
def test_prepared_reduction_rejects_ambiguous_transition_order(
    malformation: str,
) -> None:
    case = _prepared_planner_fixture()
    expected = "order|section"
    if malformation == "decreasing_anchors":
        transitions = tuple(
            transition._replace(
                transition_id=str(
                    uuid5(case.frame.frame_id, f"record:transition:{index}")
                )
            )
            for index, transition in enumerate(
                (
                    case.prepared.membership_transitions[1],
                    case.prepared.membership_transitions[0],
                    *case.prepared.membership_transitions[2:],
                )
            )
        )
    elif malformation in {"duplicate_section", "mixed_section_and_linked"}:
        section = _prepared_section_transition(case.frame, 0, "invite", "join", 5, 5)
        if malformation == "duplicate_section":
            transitions = (
                section,
                _prepared_section_transition(case.frame, 1, "join", "ban", 5, 6),
            )
        else:
            linked = case.prepared.membership_transitions[3]._replace(
                transition_id=str(uuid5(case.frame.frame_id, "record:transition:1")),
                previous_epoch=5,
                current_epoch=6,
            )
            transitions = (section, linked)
    else:
        transitions_list = list(case.prepared.membership_transitions)
        if malformation == "current_membership_domain":
            transitions_list[0] = transitions_list[0]._replace(
                current_membership="joined"
            )
            expected = "domain"
        elif malformation == "previous_membership_domain":
            transitions_list[0] = transitions_list[0]._replace(
                previous_membership="invited"
            )
            expected = "domain"
        else:
            transitions_list[0] = transitions_list[0]._replace(
                current_membership="invite"
            )
            expected = "change"
        transitions = tuple(transitions_list)

    with pytest.raises(ReducerInputError, match=expected):
        reduce_prepared_frame(
            _STREAM_ID,
            case.frame,
            case.prepared._replace(membership_transitions=transitions),
            (case.aggregate.continuity,),
        )


@pytest.mark.parametrize("malformation", ("reversed_sections", "linked_before_section"))
def test_prepared_reduction_rejects_reordered_equal_anchor_transitions(
    malformation: str,
) -> None:
    room_ids = ("!anchor-a:example.org", "!anchor-b:example.org")
    member_source = _prepared_event(
        "m.room.member", event_id="$anchor-b", membership="join"
    )
    records = (
        (
            _prepared_record(
                0,
                RecordKind.STATE,
                member_source,
                event_type="m.room.member",
                room=True,
                event_id="$anchor-b",
            )._replace(room_id=room_ids[1]),
        )
        if malformation == "linked_before_section"
        else ()
    )
    segments = (
        _prepared_segment(
            _prepared_observation("join"),
            room_id=room_ids[0],
            section=RoomSection.JOIN,
        ),
        _prepared_segment(
            _prepared_observation(
                "join",
                ("join" if records else None),
                ("$anchor-b" if records else None),
                "invite",
            ),
            room_id=room_ids[1],
            section=RoomSection.JOIN,
            state=((member_source,) if records else ()),
        ),
    )
    frame = _prepared_frame(room_segments=segments)

    if records:
        record = records[0]
        linked = _PreparedMembershipTransition(
            str(uuid5(frame.frame_id, "record:transition:0")),
            record.record_id,
            room_ids[1],
            record.event_id,
            "invite",
            "join",
            2,
            2,
            _MembershipSourceKind.STATE,
            None,
            _MembershipProvenance.REPORTED,
            record.origin,
            record.source_json,
        )
        transitions = (
            linked,
            _prepared_section_transition(
                frame,
                1,
                "invite",
                "join",
                2,
                2,
                room_id=room_ids[0],
                frame_index=0,
            ),
        )
    else:
        transitions = tuple(
            _prepared_section_transition(
                frame,
                index,
                "invite",
                "join",
                2,
                2,
                room_id=room_id,
                frame_index=0,
            )
            for index, room_id in enumerate(reversed(room_ids))
        )
    prepared = _prepared_payload(frame, records, transitions)
    rooms = tuple(
        RoomContinuity(
            room_id,
            2,
            "invite",
            MembershipBaseline(f"$invite-{index}", None),
            None,
            None,
        )
        for index, room_id in enumerate(room_ids)
    )

    with pytest.raises(ReducerInputError, match="order"):
        reduce_prepared_frame(_STREAM_ID, frame, prepared, rooms)


@pytest.mark.parametrize(
    "malformation",
    (
        "event_type",
        "membership",
        "source_kind",
        "timeline_provenance",
        "clear_event_id",
        "previous_epoch_negative",
        "current_epoch_negative",
    ),
)
def test_prepared_reduction_authenticates_membership_evidence(
    malformation: str,
) -> None:
    case = _prepared_planner_fixture()
    expected = "evidence|source kind"
    records = list(case.prepared.records)
    transitions = list(case.prepared.membership_transitions)
    frame = case.frame
    if malformation in {"event_type", "membership"}:
        event_type = "m.room.topic" if malformation == "event_type" else "m.room.member"
        membership = "leave" if malformation == "membership" else "join"
        source_json = _prepared_event(
            event_type, event_id="$join", membership=membership
        )
        records[1] = records[1]._replace(
            source_json=source_json, effective_event_type=event_type
        )
        transitions[0] = transitions[0]._replace(source_json=source_json)
        segment = replace(
            frame.room_segments[0],
            state_json=(source_json, frame.room_segments[0].state_json[1]),
        )
        frame = replace(frame, room_segments=(segment,))
    elif malformation == "source_kind":
        transitions[0] = transitions[0]._replace(
            source_kind=_MembershipSourceKind.TIMELINE,
            timeline_provenance=TimelineEventProvenance.LIVE,
        )
    elif malformation == "timeline_provenance":
        transitions[2] = transitions[2]._replace(
            timeline_provenance=TimelineEventProvenance.HISTORY
        )
    elif malformation == "clear_event_id":
        encrypted = canonical_json(
            {
                "content": {"ciphertext": "opaque"},
                "event_id": "$rejoin",
                "type": "m.room.encrypted",
            }
        )
        clear = _prepared_event("m.room.member", event_id="$wrong", membership="join")
        records[3] = records[3]._replace(
            source_json=encrypted,
            clear_json=clear,
            decryption=_DecryptionDisposition.DECRYPTED,
        )
        transitions[2] = transitions[2]._replace(source_json=clear)
        segment = replace(
            frame.room_segments[0],
            timeline_json=(encrypted, frame.room_segments[0].timeline_json[1]),
        )
        frame = replace(frame, room_segments=(segment,))
        expected = "evidence"
    else:
        field, value = {
            "previous_epoch_negative": ("previous_epoch", -1),
            "current_epoch_negative": ("current_epoch", -1),
        }[malformation]
        transitions[0] = transitions[0]._replace(**{field: value})
        expected = "epoch"
    prepared = case.prepared._replace(
        records=tuple(records), membership_transitions=tuple(transitions)
    )

    with pytest.raises(ReducerInputError, match=expected):
        reduce_prepared_frame(_STREAM_ID, frame, prepared, (case.aggregate.continuity,))


@pytest.mark.parametrize(
    ("field", "value"),
    (
        ("record_id", UUID("12345678-1234-5678-1234-567812345699")),
        ("kind", "to_device"),
        ("origin", "origin"),
        ("preparation_phase", "source"),
        ("room_id", 1),
        ("event_id", 1),
        ("provenance", "live"),
        ("source_json", bytearray(b"{}")),
        ("clear_json", "{}"),
        ("decryption", "decrypted"),
        ("decryption_verified", 1),
        ("decrypted_to_device_kind", "room_key"),
        ("callback_route", "to_device"),
    ),
)
def test_prepared_reduction_rejects_malformed_record_carrier(
    field: str,
    value: object,
) -> None:
    case = _prepared_planner_fixture()
    records = list(case.prepared.records)
    records[0] = records[0]._replace(**{field: value})

    with pytest.raises(ReducerInputError):
        reduce_prepared_frame(
            _STREAM_ID,
            case.frame,
            case.prepared._replace(records=tuple(records)),
            (case.aggregate.continuity,),
        )


class _PreparedString(str):
    pass


@pytest.mark.parametrize(
    "field",
    (
        "transition_id",
        "source_record_id",
        "room_id",
        "event_id",
        "previous_membership",
        "current_membership",
        "previous_epoch",
        "current_epoch",
        "source_kind",
        "timeline_provenance",
        "membership_provenance",
        "origin",
        "source_json",
    ),
)
def test_prepared_reduction_rejects_malformed_transition_carrier(field: str) -> None:
    case = _prepared_planner_fixture()
    transition = case.prepared.membership_transitions[0]
    value: object = getattr(transition, field)
    if field in {
        "transition_id",
        "source_record_id",
        "room_id",
        "event_id",
        "previous_membership",
        "current_membership",
    }:
        assert type(value) is str
        value = _PreparedString(value)
    elif field in {"previous_epoch", "current_epoch"}:
        value = True
    elif field == "source_kind":
        value = "state"
    elif field == "timeline_provenance":
        value = "live"
    elif field == "membership_provenance":
        value = "reported"
    elif field == "origin":
        value = "origin"
    else:
        assert type(value) is bytes
        value = bytearray(value)

    with pytest.raises(ReducerInputError, match="carrier"):
        reduce_prepared_frame(
            _STREAM_ID,
            case.frame,
            case.prepared._replace(
                membership_transitions=(transition._replace(**{field: value}),)
                + case.prepared.membership_transitions[1:]
            ),
            (case.aggregate.continuity,),
        )


def _prepared_crypto_shape(malformation: str) -> _PreparedCryptoDelta:
    rerequest = _PreparedMegolmRerequest(
        b"{}", "!room:example.org", "$event", "@u:example.org", "D", "k", "s", "a"
    )
    waiting = _PreparedWaitingKeyRequest(
        b"{}", "@u:example.org", "D", "r", "!room:example.org", "k", "s", "a"
    )
    claim = _PreparedKeyClaim(
        "@u:example.org", "D", False, False, (waiting,), (rerequest,)
    )
    message = _PreparedQueuedToDeviceMessage(
        _QueuedToDeviceSubtype.GENERIC,
        "m.test",
        "@u:example.org",
        "D",
        b"{}",
        None,
        None,
        None,
        None,
        (rerequest,),
    )
    if malformation == "claim_bool":
        claim = claim._replace(was_wedged=0)
    elif malformation == "waiting_container":
        claim = claim._replace(waiting_key_requests=[waiting])
    elif malformation == "waiting_bytes":
        claim = claim._replace(
            waiting_key_requests=(waiting._replace(source_json=bytearray(b"{}")),)
        )
    elif malformation == "rerequest_string":
        claim = claim._replace(rerequest_events=(rerequest._replace(room_id=1),))
    elif malformation == "message_subtype":
        message = message._replace(subtype="generic")
    elif malformation == "message_optional":
        message = message._replace(request_id=1)
    elif malformation == "message_rerequests":
        message = message._replace(rerequest_events=[rerequest])
    return _PreparedCryptoDelta((), (), None, b"{}", b"null", (claim,), (message,))


def _prepared_snapshot(room_id: str) -> RoomSnapshot:
    return RoomSnapshot(
        room_id,
        5,
        _PLANNER_ACCOUNT_ID,
        "join",
        False,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        (),
    )


def _malformed_prepared_frame(
    case: _PreparedPlannerFixture, name: str
) -> _PreparedIngestionFrame:
    prepared = case.prepared
    fields: dict[str, tuple[str, object]] = {
        "transport": ("transport", "classic"),
        "source_epoch": ("source_epoch", False),
        "request_id": ("request_id", True),
        "request_cursor": (
            "request_cursor_json",
            bytearray(prepared.request_cursor_json),
        ),
        "candidate_cursor": (
            "candidate_cursor_json",
            bytearray(prepared.candidate_cursor_json),
        ),
        "source_sha256": ("source_sha256", bytearray(prepared.source_sha256)),
        "records_container": ("records", list(prepared.records)),
        "transitions_container": (
            "membership_transitions",
            list(prepared.membership_transitions),
        ),
        "snapshots_container": ("room_snapshots", []),
        "snapshot_item": ("room_snapshots", (object(),)),
        "snapshot_duplicate": (
            "room_snapshots",
            (_prepared_snapshot(_PREPARED_ROOM_ID),) * 2,
        ),
        "snapshot_foreign": (
            "room_snapshots",
            (_prepared_snapshot("!foreign:example.org"),),
        ),
        "crypto_type": ("crypto_delta", tuple(prepared.crypto_delta)),
        "crypto_container": (
            "crypto_delta",
            prepared.crypto_delta._replace(encrypted_room_ids=[]),
        ),
    }
    field, value = fields.get(name, ("crypto_delta", _prepared_crypto_shape(name)))
    replacements = {field: value}
    if name == "transport":
        replacements["compatibility_token"] = None
    return prepared._replace(**replacements)


@pytest.mark.parametrize(
    "malformation",
    (
        "transport",
        "source_epoch",
        "request_id",
        "request_cursor",
        "candidate_cursor",
        "source_sha256",
        "records_container",
        "transitions_container",
        "snapshots_container",
        "snapshot_item",
        "snapshot_duplicate",
        "snapshot_foreign",
        "crypto_type",
        "crypto_container",
        "claim_bool",
        "waiting_container",
        "waiting_bytes",
        "rerequest_string",
        "message_subtype",
        "message_optional",
        "message_rerequests",
    ),
)
def test_prepared_reduction_rejects_malformed_frame_carrier(
    malformation: str,
) -> None:
    case = _prepared_planner_fixture()
    expected = (
        "ownership"
        if malformation in {"snapshot_duplicate", "snapshot_foreign"}
        else "carrier"
    )
    with pytest.raises(ReducerInputError, match=expected):
        reduce_prepared_frame(
            _STREAM_ID,
            case.frame,
            _malformed_prepared_frame(case, malformation),
            (case.aggregate.continuity,),
        )


def test_prepared_reduction_accepts_populated_structural_carriers() -> None:
    case = _prepared_planner_fixture()
    prepared = case.prepared._replace(
        room_snapshots=(_prepared_snapshot(_PREPARED_ROOM_ID),),
        crypto_delta=_prepared_crypto_shape("valid"),
    )

    reduction = reduce_prepared_frame(
        _STREAM_ID, case.frame, prepared, (case.aggregate.continuity,)
    )

    assert reduction.crypto_deferred is True


_OUTBOUND_CODEC_FRAME_ID = UUID("2ad46b4e-a042-4ba3-9ff8-e33e1b544bfd")


def _outbound_codec_bytes(value: object) -> bytes:
    return canonical_json(value)


def _outbound_codec_b64(value: bytes) -> str:
    return base64.b64encode(value).decode("ascii")


def _outbound_codec_transaction_id(index: int, body_json: bytes) -> str:
    return str(
        uuid5(
            _OUTBOUND_CODEC_FRAME_ID,
            "nio.ingest.outbound-maintenance.v1:"
            f"to-device:{index}:{hashlib.sha256(body_json).hexdigest()}",
        )
    )


def _full_outbound_codec_case() -> tuple[object, dict[str, object]]:
    algorithm = "m.megolm.v1.aes-sha2"
    claim_user = "@claim:example.org"
    claim_device = "CLAIM"
    claim_room = "!claim:example.org"
    claim_session = "claim-session"
    waiting_source = _outbound_codec_bytes(
        {
            "content": {
                "action": "request",
                "body": {
                    "algorithm": algorithm,
                    "room_id": claim_room,
                    "sender_key": "claim-sender-key",
                    "session_id": claim_session,
                },
                "request_id": "waiting-request",
                "requesting_device_id": claim_device,
                "org.example.extra": True,
            },
            "sender": claim_user,
            "type": "m.room_key_request",
            "unsigned": {"age": 1},
        }
    )
    claim_rerequest_source = _outbound_codec_bytes(
        {
            "content": {
                "algorithm": algorithm,
                "device_id": claim_device,
                "sender_key": "claim-sender-key",
                "session_id": claim_session,
                "ciphertext": {"preserved": True},
            },
            "event_id": "$claim-event",
            "origin_server_ts": 1,
            "sender": claim_user,
            "type": "m.room.encrypted",
        }
    )
    claim_rerequest = {
        "source_json": _outbound_codec_b64(claim_rerequest_source),
        "room_id": claim_room,
        "event_id": "$claim-event",
        "sender_user_id": claim_user,
        "sender_device_id": claim_device,
        "sender_key": "claim-sender-key",
        "session_id": claim_session,
        "algorithm": algorithm,
    }
    claim_context = {
        "claims": [
            {
                "user_id": claim_user,
                "device_id": claim_device,
                "was_wedged": True,
                "was_waiting": True,
                "waiting_key_requests": [
                    {
                        "source_json": _outbound_codec_b64(waiting_source),
                        "sender_user_id": claim_user,
                        "requesting_device_id": claim_device,
                        "request_id": "waiting-request",
                        "room_id": claim_room,
                        "sender_key": "claim-sender-key",
                        "session_id": claim_session,
                        "algorithm": algorithm,
                    }
                ],
                "rerequest_events": [claim_rerequest],
            }
        ]
    }

    dummy_user = "@dummy:example.org"
    dummy_device = "DUMMY"
    dummy_room = "!dummy:example.org"
    dummy_rerequest_source = _outbound_codec_bytes(
        {
            "content": {
                "algorithm": algorithm,
                "device_id": dummy_device,
                "sender_key": "dummy-sender-key",
                "session_id": "dummy-session",
            },
            "event_id": "$dummy-event",
            "room_id": dummy_room,
            "sender": dummy_user,
            "type": "m.room.encrypted",
        }
    )
    dummy_context = {
        "subtype": "dummy",
        "rerequest_events": [
            {
                "source_json": _outbound_codec_b64(dummy_rerequest_source),
                "room_id": dummy_room,
                "event_id": "$dummy-event",
                "sender_user_id": dummy_user,
                "sender_device_id": dummy_device,
                "sender_key": "dummy-sender-key",
                "session_id": "dummy-session",
                "algorithm": algorithm,
            }
        ],
    }
    room_key_user = "@room-key:example.org"
    room_key_device = "ROOMKEY"
    room_key_content = {
        "action": "request",
        "body": {
            "algorithm": algorithm,
            "room_id": "!room-key:example.org",
            "sender_key": "room-key-sender-key",
            "session_id": "room-key-session",
        },
        "request_id": "room-key-request",
        "requesting_device_id": "OWNER",
    }

    bodies = (
        _outbound_codec_bytes(
            {
                "device_keys": {
                    "algorithms": ["m.olm.v1.curve25519-aes-sha2"],
                    "device_id": "OWNER",
                    "keys": {"curve25519:OWNER": "curve-key"},
                    "user_id": "@owner:example.org",
                },
                "fallback_keys": {
                    "signed_curve25519:fallback": {"key": "fallback-key"}
                },
                "one_time_keys": {"signed_curve25519:otk": {"key": "one-time-key"}},
            }
        ),
        _outbound_codec_bytes(
            {
                "device_keys": {
                    "@query-a:example.org": [],
                    "@query-b:example.org": [],
                }
            }
        ),
        _outbound_codec_bytes(
            {"one_time_keys": {claim_user: {claim_device: "signed_curve25519"}}}
        ),
        _outbound_codec_bytes(
            {"messages": {"@generic:example.org": {"GENERIC": {"value": "generic"}}}}
        ),
        _outbound_codec_bytes(
            {
                "messages": {
                    dummy_user: {
                        dummy_device: {
                            "algorithm": "m.olm.v1.curve25519-aes-sha2",
                            "ciphertext": {
                                "dummy-curve-key": {
                                    "body": "dummy-ciphertext",
                                    "type": 0,
                                }
                            },
                            "sender_key": "owner-curve-key",
                        }
                    }
                }
            }
        ),
        _outbound_codec_bytes(
            {"messages": {room_key_user: {room_key_device: room_key_content}}}
        ),
    )
    contexts: tuple[object | None, ...] = (
        None,
        None,
        claim_context,
        {"subtype": "generic"},
        dummy_context,
        {
            "subtype": "room_key_request",
            "request_id": "room-key-request",
            "session_id": "room-key-session",
            "room_id": "!room-key:example.org",
            "algorithm": algorithm,
        },
    )
    kinds = (
        "key_upload",
        "key_query",
        "key_claim",
        "to_device",
        "to_device",
        "to_device",
    )
    event_types = (
        None,
        None,
        None,
        "org.example.generic",
        "m.room.encrypted",
        "m.room_key_request",
    )
    operations = tuple(
        journal_rows_module._OutboundOperation(
            kind,
            "pending",
            body,
            (
                None
                if kind != "to_device"
                else _outbound_codec_transaction_id(index, body)
            ),
            event_types[index],
            contexts[index],
        )
        for index, (kind, body) in enumerate(zip(kinds, bodies, strict=True))
    )
    expected_operations = [
        {
            "kind": operation.kind,
            "state": "pending",
            "body_json": _outbound_codec_b64(operation.body_json),
            "transaction_id": operation.transaction_id,
            "event_type": operation.event_type,
            "context": operation.context,
        }
        for operation in operations
    ]
    return (
        journal_rows_module._OutboundMaintenance(operations),
        {"version": 1, "operations": expected_operations},
    )


def test_outbound_maintenance_codec_round_trips_full_frozen_plan() -> None:
    maintenance, expected = _full_outbound_codec_case()

    encoded = journal_rows_module._outbound_maintenance_to_dict(
        maintenance,
        frame_id=_OUTBOUND_CODEC_FRAME_ID,
    )

    assert encoded == expected
    assert tuple(encoded) == ("version", "operations")
    assert tuple(operation["kind"] for operation in encoded["operations"]) == (
        "key_upload",
        "key_query",
        "key_claim",
        "to_device",
        "to_device",
        "to_device",
    )
    assert all(
        tuple(operation)
        == (
            "kind",
            "state",
            "body_json",
            "transaction_id",
            "event_type",
            "context",
        )
        for operation in encoded["operations"]
    )
    claim_context = encoded["operations"][2]["context"]
    assert tuple(claim_context) == ("claims",)
    claim = claim_context["claims"][0]
    assert tuple(claim) == (
        "user_id",
        "device_id",
        "was_wedged",
        "was_waiting",
        "waiting_key_requests",
        "rerequest_events",
    )
    assert tuple(claim["waiting_key_requests"][0]) == (
        "source_json",
        "sender_user_id",
        "requesting_device_id",
        "request_id",
        "room_id",
        "sender_key",
        "session_id",
        "algorithm",
    )
    assert tuple(claim["rerequest_events"][0]) == (
        "source_json",
        "room_id",
        "event_id",
        "sender_user_id",
        "sender_device_id",
        "sender_key",
        "session_id",
        "algorithm",
    )
    assert tuple(encoded["operations"][3]["context"]) == ("subtype",)
    assert tuple(encoded["operations"][4]["context"]) == (
        "subtype",
        "rerequest_events",
    )
    assert tuple(encoded["operations"][5]["context"]) == (
        "subtype",
        "request_id",
        "session_id",
        "room_id",
        "algorithm",
    )
    assert (
        journal_rows_module._outbound_maintenance_from_dict(
            encoded,
            frame_id=_OUTBOUND_CODEC_FRAME_ID,
        )
        == maintenance
    )

    settled_prefix = journal_rows_module._OutboundMaintenance(
        tuple(
            (
                operation._replace(state="settled", context=None)
                if index < 3
                else operation
            )
            for index, operation in enumerate(maintenance.operations)
        )
    )
    settled_encoded = journal_rows_module._outbound_maintenance_to_dict(
        settled_prefix,
        frame_id=_OUTBOUND_CODEC_FRAME_ID,
    )
    assert tuple(operation["state"] for operation in settled_encoded["operations"]) == (
        "settled",
        "settled",
        "settled",
        "pending",
        "pending",
        "pending",
    )
    assert (
        journal_rows_module._outbound_maintenance_from_dict(
            settled_encoded,
            frame_id=_OUTBOUND_CODEC_FRAME_ID,
        )
        == settled_prefix
    )

    all_settled = journal_rows_module._OutboundMaintenance(
        tuple(
            operation._replace(state="settled", context=None)
            for operation in maintenance.operations
        )
    )
    all_settled_encoded = journal_rows_module._outbound_maintenance_to_dict(
        all_settled,
        frame_id=_OUTBOUND_CODEC_FRAME_ID,
    )
    assert (
        journal_rows_module._outbound_maintenance_from_dict(
            all_settled_encoded,
            frame_id=_OUTBOUND_CODEC_FRAME_ID,
        )
        == all_settled
    )

    empty = journal_rows_module._OutboundMaintenance(())
    assert (
        journal_rows_module._outbound_maintenance_from_dict(
            journal_rows_module._outbound_maintenance_to_dict(
                empty,
                frame_id=_OUTBOUND_CODEC_FRAME_ID,
            ),
            frame_id=_OUTBOUND_CODEC_FRAME_ID,
        )
        == empty
    )

    with pytest.raises(ValueError):
        journal_rows_module._outbound_maintenance_from_dict(
            encoded,
            frame_id=UUID("5ba72364-44f0-4938-b647-8b5b19c72570"),
        )


def _refresh_outbound_codec_transaction_ids(operations: list[object]) -> None:
    for index, operation in enumerate(operations):
        if operation["kind"] != "to_device":
            continue
        body_json = base64.b64decode(operation["body_json"], validate=True)
        operation["transaction_id"] = _outbound_codec_transaction_id(index, body_json)


def _mutated_outbound_codec_value(mutation: str) -> dict[str, object]:
    _, expected = _full_outbound_codec_case()
    value = json.loads(json.dumps(expected))
    operations = value["operations"]
    assert type(operations) is list

    if mutation == "kind_reorder":
        operations[0], operations[1] = operations[1], operations[0]
    elif mutation == "duplicate_singleton":
        operations.insert(2, dict(operations[1]))
        _refresh_outbound_codec_transaction_ids(operations)
    elif mutation == "state_order":
        operations[1]["state"] = "settled"
    elif mutation == "invalid_state":
        operations[0]["state"] = "failed"
    elif mutation == "upload_unknown":
        operations[0]["body_json"] = _outbound_codec_b64(
            _outbound_codec_bytes({"unknown": {}})
        )
    elif mutation == "upload_empty":
        operations[0]["body_json"] = _outbound_codec_b64(_outbound_codec_bytes({}))
    elif mutation in {
        "upload_missing_one_time_keys",
        "upload_empty_one_time_keys",
        "upload_empty_fallback_keys",
    }:
        operation = operations[0]
        body = json.loads(base64.b64decode(operation["body_json"], validate=True))
        if mutation == "upload_missing_one_time_keys":
            del body["one_time_keys"]
        elif mutation == "upload_empty_one_time_keys":
            body["one_time_keys"] = {}
        else:
            body["fallback_keys"] = {}
        operation["body_json"] = _outbound_codec_b64(_outbound_codec_bytes(body))
    elif mutation == "query_body":
        operations[1]["body_json"] = _outbound_codec_b64(
            _outbound_codec_bytes(
                {"device_keys": {"@query-a:example.org": ["QUERY-A"]}}
            )
        )
    elif mutation == "non_to_device_transaction":
        operations[1]["transaction_id"] = "12345678-1234-5678-9234-567812345678"
    elif mutation == "non_to_device_event_type":
        operations[1]["event_type"] = "m.invalid"
    elif mutation == "claim_target":
        claim = operations[2]
        body = {"one_time_keys": {"@other:example.org": {"OTHER": "signed_curve25519"}}}
        claim["body_json"] = _outbound_codec_b64(_outbound_codec_bytes(body))
    elif mutation == "claim_key_type":
        claim = operations[2]
        body = {"one_time_keys": {"@claim:example.org": {"CLAIM": "curve25519"}}}
        claim["body_json"] = _outbound_codec_b64(_outbound_codec_bytes(body))
    elif mutation == "duplicate_claim":
        claim_context = operations[2]["context"]
        claim_context["claims"].append(dict(claim_context["claims"][0]))
    elif mutation == "duplicate_waiting":
        claim_context = operations[2]["context"]
        waiting = claim_context["claims"][0]["waiting_key_requests"]
        waiting.append(dict(waiting[0]))
    elif mutation == "claim_without_reason":
        claim_context = operations[2]["context"]
        claim = claim_context["claims"][0]
        claim["was_wedged"] = False
        claim["was_waiting"] = False
        claim["waiting_key_requests"] = []
        claim["rerequest_events"] = []
    elif mutation == "rerequests_without_wedge":
        claim_context = operations[2]["context"]
        claim_context["claims"][0]["was_wedged"] = False
    elif mutation == "waiting_source":
        claim_context = operations[2]["context"]
        claim_context["claims"][0]["waiting_key_requests"][0][
            "request_id"
        ] = "other-request"
    elif mutation == "rerequest_source":
        claim_context = operations[2]["context"]
        claim_context["claims"][0]["rerequest_events"][0][
            "sender_key"
        ] = "other-sender-key"
    elif mutation == "claim_key_order":
        claim_context = operations[2]["context"]
        claim = claim_context["claims"][0]
        claim_context["claims"][0] = dict(reversed(tuple(claim.items())))
    elif mutation == "waiting_key_order":
        claim_context = operations[2]["context"]
        waiting = claim_context["claims"][0]["waiting_key_requests"][0]
        claim_context["claims"][0]["waiting_key_requests"][0] = dict(
            reversed(tuple(waiting.items()))
        )
    elif mutation == "rerequest_key_order":
        claim_context = operations[2]["context"]
        rerequest = claim_context["claims"][0]["rerequest_events"][0]
        claim_context["claims"][0]["rerequest_events"][0] = dict(
            reversed(tuple(rerequest.items()))
        )
    elif mutation == "context_missing_key":
        claim_context = operations[2]["context"]
        del claim_context["claims"][0]["waiting_key_requests"][0]["algorithm"]
    elif mutation == "context_extra_key":
        claim_context = operations[2]["context"]
        claim_context["claims"][0]["waiting_key_requests"][0]["extra"] = None
    elif mutation == "duplicate_rerequest_owner":
        claim_context = operations[2]["context"]
        dummy_context = operations[4]["context"]
        dummy_context["rerequest_events"] = [
            dict(claim_context["claims"][0]["rerequest_events"][0])
        ]
        operation = operations[4]
        body = json.loads(base64.b64decode(operation["body_json"], validate=True))
        content = body["messages"]["@dummy:example.org"]["DUMMY"]
        body["messages"] = {"@claim:example.org": {"CLAIM": content}}
        body_json = _outbound_codec_bytes(body)
        operation["body_json"] = _outbound_codec_b64(body_json)
        operation["transaction_id"] = _outbound_codec_transaction_id(4, body_json)
    elif mutation == "claim_rerequests_with_empty_first_dummy":
        dummy_context = operations[4]["context"]
        dummy_context["rerequest_events"] = []
        operation = operations[4]
        body = json.loads(base64.b64decode(operation["body_json"], validate=True))
        content = body["messages"]["@dummy:example.org"]["DUMMY"]
        body["messages"] = {"@claim:example.org": {"CLAIM": content}}
        body_json = _outbound_codec_bytes(body)
        operation["body_json"] = _outbound_codec_b64(body_json)
        operation["transaction_id"] = _outbound_codec_transaction_id(4, body_json)
    elif mutation == "to_device_transaction":
        operations[3]["transaction_id"] = "12345678-1234-5678-9234-567812345678"
    elif mutation == "to_device_two_recipients":
        operation = operations[3]
        body = json.loads(base64.b64decode(operation["body_json"], validate=True))
        body["messages"]["@other:example.org"] = {"OTHER": {"value": "other"}}
        body_json = _outbound_codec_bytes(body)
        operation["body_json"] = _outbound_codec_b64(body_json)
        operation["transaction_id"] = _outbound_codec_transaction_id(3, body_json)
    elif mutation == "to_device_two_devices":
        operation = operations[3]
        body = json.loads(base64.b64decode(operation["body_json"], validate=True))
        body["messages"]["@generic:example.org"]["OTHER"] = {"value": "other"}
        body_json = _outbound_codec_bytes(body)
        operation["body_json"] = _outbound_codec_b64(body_json)
        operation["transaction_id"] = _outbound_codec_transaction_id(3, body_json)
    elif mutation == "to_device_recipient":
        operation = operations[4]
        body = json.loads(base64.b64decode(operation["body_json"], validate=True))
        target = body["messages"].pop("@dummy:example.org")
        body["messages"]["@other:example.org"] = target
        body_json = _outbound_codec_bytes(body)
        operation["body_json"] = _outbound_codec_b64(body_json)
        operation["transaction_id"] = _outbound_codec_transaction_id(4, body_json)
    elif mutation == "to_device_content":
        operation = operations[5]
        body = json.loads(base64.b64decode(operation["body_json"], validate=True))
        content = body["messages"]["@room-key:example.org"]["ROOMKEY"]
        content["body"]["session_id"] = "other-session"
        body_json = _outbound_codec_bytes(body)
        operation["body_json"] = _outbound_codec_b64(body_json)
        operation["transaction_id"] = _outbound_codec_transaction_id(5, body_json)
    elif mutation == "to_device_event_type":
        operations[5]["event_type"] = "m.room.encrypted"
    elif mutation == "dummy_event_type":
        operations[4]["event_type"] = "org.example.generic"
    elif mutation == "room_key_context":
        operations[5]["context"]["algorithm"] = "m.megolm.v2"
    elif mutation == "settled_context":
        for operation in operations:
            operation["state"] = "settled"
            if operation is not operations[3]:
                operation["context"] = None
    else:
        raise AssertionError(f"unknown outbound mutation: {mutation}")
    return value


@pytest.mark.parametrize(
    "mutation",
    (
        "kind_reorder",
        "duplicate_singleton",
        "state_order",
        "invalid_state",
        "upload_unknown",
        "upload_empty",
        "upload_missing_one_time_keys",
        "upload_empty_one_time_keys",
        "upload_empty_fallback_keys",
        "query_body",
        "non_to_device_transaction",
        "non_to_device_event_type",
        "claim_target",
        "claim_key_type",
        "duplicate_claim",
        "duplicate_waiting",
        "claim_without_reason",
        "rerequests_without_wedge",
        "waiting_source",
        "rerequest_source",
        "claim_key_order",
        "waiting_key_order",
        "rerequest_key_order",
        "context_missing_key",
        "context_extra_key",
        "duplicate_rerequest_owner",
        "claim_rerequests_with_empty_first_dummy",
        "to_device_transaction",
        "to_device_two_recipients",
        "to_device_two_devices",
        "to_device_recipient",
        "to_device_content",
        "to_device_event_type",
        "dummy_event_type",
        "room_key_context",
        "settled_context",
    ),
)
def test_outbound_maintenance_codec_rejects_semantic_mutation(
    mutation: str,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        journal_rows_module._outbound_maintenance_from_dict(
            _mutated_outbound_codec_value(mutation),
            frame_id=_OUTBOUND_CODEC_FRAME_ID,
        )


def test_prepared_reduction_rejects_section_without_membership_claim() -> None:
    source_json = _prepared_event("m.tag")
    segment = _prepared_segment(
        _prepared_observation(), room_account_data=(source_json,)
    )
    frame = _prepared_frame(room_segments=(segment,))
    record = _prepared_record(
        0,
        RecordKind.ROOM_ACCOUNT_DATA,
        source_json,
        event_type="m.tag",
        room=True,
        route=_CallbackRoute.ROOM_ACCOUNT_DATA,
    )
    transition = _prepared_section_transition(
        frame, 0, "join", "leave", 3, 4, frame_index=0
    )
    prepared = _prepared_payload(frame, (record,), (transition,))
    before = RoomContinuity(
        _PREPARED_ROOM_ID,
        3,
        "join",
        MembershipBaseline("$join", None),
        None,
        None,
    )

    with pytest.raises(ReducerInputError, match="section"):
        reduce_prepared_frame(_STREAM_ID, frame, prepared, (before,))


def test_prepared_reduction_preserves_mixed_empty_segment_action_order() -> None:
    transition_room = "!transition:example.org"
    recovery_room = "!recovery:example.org"
    presence_source = _prepared_event("m.presence")
    segments = (
        _prepared_segment(
            _prepared_observation("join"),
            room_id=transition_room,
            section=RoomSection.JOIN,
        ),
        _prepared_segment(
            _prepared_observation("join", unparsed=True),
            room_id=recovery_room,
            section=RoomSection.JOIN,
        ),
    )
    frame = _prepared_frame(room_segments=segments, presence=(presence_source,))
    record = _prepared_record(
        0,
        RecordKind.PRESENCE,
        presence_source,
        event_type="m.presence",
        route=_CallbackRoute.PRESENCE,
    )
    transition = _prepared_section_transition(
        frame,
        0,
        "invite",
        "join",
        2,
        2,
        room_id=transition_room,
        frame_index=0,
    )
    prepared = _prepared_payload(frame, (record,), (transition,))
    gap = RecoveryGap(
        uuid5(_STREAM_ID, "mixed-empty-recovery"),
        recovery_room,
        7,
        RecordOrigin(TransportKind.CLASSIC, 0, 0, 0),
        "s0",
        "s1",
    )
    rooms = (
        RoomContinuity(
            transition_room,
            2,
            "invite",
            MembershipBaseline("$invite", None),
            None,
            None,
        ),
        RoomContinuity(
            recovery_room,
            7,
            "join",
            MembershipBaseline("$join", "s0"),
            gap,
            None,
        ),
    )

    reduction = reduce_prepared_frame(_STREAM_ID, frame, prepared, rooms)

    assert [
        (
            type(step).__name__,
            (
                step.transition.room_id
                if type(step) is PreparedTransitionStep
                else step.room_id if type(step) is PreparedRecoveryStep else None
            ),
        )
        for step in reduction.linear_steps
    ] == [
        ("PreparedTransitionStep", transition_room),
        ("PreparedRecoveryStep", recovery_room),
        ("PreparedRecordStep", None),
    ]


def test_prepared_plan_preserves_all_task4c_record_ids_metadata_and_transition_order() -> (
    None
):
    case = _prepared_planner_fixture()

    plan = _plan_prepared(case)

    assert plan is not None
    assert plan.work_releases == ()
    assert [item.value.record_id for item in plan.work_inserts] == [
        _PREPARED_RECORD_IDS[0],
        _PREPARED_TRANSITION_IDS[0],
        _PREPARED_RECORD_IDS[1],
        _PREPARED_TRANSITION_IDS[1],
        _PREPARED_RECORD_IDS[2],
        _PREPARED_TRANSITION_IDS[2],
        _PREPARED_RECORD_IDS[3],
        _PREPARED_TRANSITION_IDS[3],
        *_PREPARED_RECORD_IDS[4:],
    ]
    assert [item.ready_ordinal for item in plan.work_inserts] == list(range(13))
    assert [item.value.membership_epoch for item in plan.work_inserts] == [
        None,
        5,
        5,
        6,
        6,
        6,
        6,
        7,
        7,
        7,
        7,
        None,
        None,
    ]
    assert [item.value.room_sequence for item in plan.work_inserts] == [
        None,
        7,
        8,
        9,
        10,
        11,
        12,
        13,
        14,
        15,
        16,
        None,
        None,
    ]
    source_work = {
        item.value.record_id: item for item in plan.work_inserts if item.metadata
    }
    assert tuple(source_work) == _PREPARED_RECORD_IDS
    for prepared_record in case.prepared.records:
        item = source_work[prepared_record.record_id]
        assert (
            item.value.kind,
            item.value.origin,
            item.value.event_id,
            item.value.provenance,
            item.value.source_json,
            item.value.clear_json,
            tuple(item.metadata),
        ) == (
            prepared_record.kind,
            prepared_record.origin,
            prepared_record.event_id,
            prepared_record.provenance,
            prepared_record.source_json,
            prepared_record.clear_json,
            (
                prepared_record.record_id,
                prepared_record.preparation_phase,
                prepared_record.effective_event_type,
                prepared_record.decryption,
                prepared_record.decryption_verified,
                prepared_record.decrypted_to_device_kind,
                prepared_record.callback_route,
            ),
        )
        assert json.loads(item.plaintext)["preparation"]["record_id"] == (
            prepared_record.record_id
        )
    assert {item.value.kind for item in plan.work_inserts} == set(RecordKind)
    lifecycle_work = tuple(
        item.value
        for item in plan.work_inserts
        if item.value.kind is RecordKind.ROOM_LIFECYCLE
    )
    assert len(lifecycle_work) == len(case.prepared.membership_transitions)
    for value, transition in zip(
        lifecycle_work, case.prepared.membership_transitions, strict=True
    ):
        assert (
            value.record_id,
            value.origin,
            value.room_id,
            value.membership_epoch,
            value.event_id,
            value.provenance,
            value.clear_json,
            json.loads(value.source_json),
        ) == (
            transition.transition_id,
            transition.origin,
            transition.room_id,
            transition.current_epoch,
            None,
            None,
            None,
            {
                "event_id": transition.event_id,
                "membership": transition.current_membership,
                "membership_epoch": transition.current_epoch,
                "membership_provenance": transition.membership_provenance.value,
                "previous_membership": transition.previous_membership,
                "previous_membership_epoch": transition.previous_epoch,
                "source_kind": transition.source_kind.value,
                "source_record_id": transition.source_record_id,
                "timeline_provenance": (
                    transition.timeline_provenance.value
                    if transition.timeline_provenance is not None
                    else None
                ),
            },
        )
    assert plan.room_values == (
        RoomAggregateValue(
            RoomContinuity(
                _PREPARED_ROOM_ID,
                7,
                "ban",
                None,
                None,
                None,
                last_timeline_event_id="$ban",
            ),
            17,
            3,
            None,
        ),
    )


def _synthetic_prepared_case(
    phase: _PreparationPhase,
    effective_event_type: str,
    *,
    visible_type: str | None,
    route: _CallbackRoute | None = _CallbackRoute.TO_DEVICE,
) -> tuple[SyncFrame, _PreparedIngestionFrame, _PreparedIngestionRecord]:
    frame = _prepared_frame()
    source: dict[str, object] = {
        "content": {},
        "sender": _PLANNER_ACCOUNT_ID,
    }
    if visible_type is not None:
        source["type"] = visible_type
    record = _PreparedIngestionRecord(
        str(uuid5(frame.frame_id, f"record:{phase.value}:0")),
        RecordKind.TO_DEVICE,
        frame.origin,
        phase,
        effective_event_type,
        None,
        None,
        None,
        canonical_json(source),
        None,
        _DecryptionDisposition.NONE,
        None,
        None,
        route,
    )
    return frame, _prepared_payload(frame, (record,)), record


def test_prepared_reduction_accepts_typeless_expired_verification_cancel() -> None:
    frame, prepared, record = _synthetic_prepared_case(
        _PreparationPhase.EXPIRED_VERIFICATION,
        "m.key.verification.cancel",
        visible_type=None,
    )

    reduction = reduce_prepared_frame(_STREAM_ID, frame, prepared, ())

    assert reduction.linear_steps[0].record == record


@pytest.mark.parametrize(
    ("phase", "effective_event_type"),
    (
        (_PreparationPhase.EXPIRED_VERIFICATION, "m.room_key_request"),
        (_PreparationPhase.COLLECTED_KEY_REQUEST, "m.room_key_request"),
    ),
)
def test_prepared_reduction_rejects_other_typeless_synthetic_records(
    phase: _PreparationPhase,
    effective_event_type: str,
) -> None:
    frame, prepared, _record = _synthetic_prepared_case(
        phase,
        effective_event_type,
        visible_type=None,
    )

    with pytest.raises(ReducerInputError, match="type"):
        reduce_prepared_frame(_STREAM_ID, frame, prepared, ())


@pytest.mark.parametrize(
    "malformation",
    (
        "expired_wrong_type",
        "collected_wrong_type",
        "synthetic_missing_route",
        "decrypted_state",
    ),
)
def test_prepared_reduction_rejects_self_unloadable_work_metadata(
    malformation: str,
) -> None:
    if malformation == "decrypted_state":
        case = _prepared_planner_fixture()
        records = list(case.prepared.records)
        index = next(
            index
            for index, record in enumerate(records)
            if record.kind is RecordKind.STATE
        )
        records[index] = records[index]._replace(
            clear_json=records[index].source_json,
            decryption=_DecryptionDisposition.DECRYPTED,
        )
        frame = case.frame
        prepared = case.prepared._replace(records=tuple(records))
        rooms = (case.aggregate.continuity,)
    else:
        phase = (
            _PreparationPhase.EXPIRED_VERIFICATION
            if malformation != "collected_wrong_type"
            else _PreparationPhase.COLLECTED_KEY_REQUEST
        )
        event_type = (
            "m.room_key_request"
            if malformation == "expired_wrong_type"
            else (
                "m.foo"
                if malformation == "collected_wrong_type"
                else "m.key.verification.cancel"
            )
        )
        frame, prepared, _record = _synthetic_prepared_case(
            phase,
            event_type,
            visible_type=event_type,
            route=(
                None
                if malformation == "synthetic_missing_route"
                else _CallbackRoute.TO_DEVICE
            ),
        )
        rooms = ()

    with pytest.raises(ReducerInputError):
        reduce_prepared_frame(_STREAM_ID, frame, prepared, rooms)


def test_prepared_planner_rejects_foreign_membership_state_key() -> None:
    case = _prepared_planner_fixture()
    source_json = canonical_json(
        {
            "content": {"membership": "join"},
            "event_id": "$join",
            "state_key": "@mallory:example.org",
            "type": "m.room.member",
        }
    )
    records = list(case.prepared.records)
    records[1] = records[1]._replace(source_json=source_json)
    transitions = list(case.prepared.membership_transitions)
    transitions[0] = transitions[0]._replace(source_json=source_json)
    segment = replace(
        case.frame.room_segments[0],
        state_json=(source_json, case.frame.room_segments[0].state_json[1]),
    )
    frame = replace(case.frame, room_segments=(segment,))
    prepared = case.prepared._replace(
        records=tuple(records), membership_transitions=tuple(transitions)
    )

    with pytest.raises(ValueError, match="state_key|owner"):
        _plan_prepared(case, frame=frame, prepared=prepared)


def test_work_plaintext_keeps_legacy_bytes_and_authenticates_preparation() -> None:
    value = _event_record()
    expected_legacy = _expected_event_work_plaintext(value)
    assert (
        journal_plan_module._canonical_work_plaintext("event", value) == expected_legacy
    )
    metadata = _prepared_metadata(
        value.record_id, "m.push_rules", _CallbackRoute.GLOBAL_ACCOUNT_DATA
    )

    prepared_plaintext = journal_plan_module._canonical_work_plaintext(
        "event", value, metadata
    )

    assert prepared_plaintext == canonical_json(
        {
            "kind": "event",
            "preparation": {
                "callback_route": "global_account_data",
                "decrypted_to_device_kind": None,
                "decryption": "none",
                "decryption_verified": None,
                "effective_event_type": "m.push_rules",
                "preparation_phase": "source",
                "record_id": value.record_id,
            },
            "value": json.loads(expected_legacy)["value"],
        }
    )


def _prepared_work_decode_case() -> tuple[
    EventRecord,
    journal_plan_module._PreparedWorkMetadata,
    bytes,
]:
    case = _prepared_planner_fixture()
    plan = _plan_prepared(case)
    assert plan is not None
    item = next(item for item in plan.work_inserts if item.metadata is not None)
    assert type(item.value) is EventRecord
    assert item.metadata is not None
    return item.value, item.metadata, item.plaintext


def test_prepared_work_decoder_round_trips_without_changing_legacy_api() -> None:
    value, metadata, plaintext = _prepared_work_decode_case()

    decoded = journal_rows_module._decode_work_plaintext(
        _STREAM_ID, value.record_id, "event", plaintext
    )

    assert tuple(decoded) == (value, metadata)
    assert (
        journal_rows_module._work_value_from_plaintext(
            _STREAM_ID, value.record_id, "event", plaintext
        )
        == value
    )


@pytest.mark.parametrize(
    "mismatch",
    (
        "metadata_to_value",
        "value_to_outer",
        "outer_work_id",
        "lifecycle_preparation",
        "synthetic_route",
    ),
)
def test_prepared_work_decoder_rejects_identity_mismatch(mismatch: str) -> None:
    value, metadata, plaintext = _prepared_work_decode_case()
    other_id = "12345678-1234-5678-1234-567812345699"
    work_id = value.record_id
    if mismatch == "metadata_to_value":
        wrapper = json.loads(plaintext)
        wrapper["preparation"]["record_id"] = other_id
        plaintext = canonical_json(wrapper)
    elif mismatch == "value_to_outer":
        other_value = replace(value, record_id=other_id)
        other_metadata = metadata._replace(record_id=other_id)
        plaintext = journal_plan_module._canonical_work_plaintext(
            "event", other_value, other_metadata
        )
    elif mismatch == "outer_work_id":
        work_id = other_id
    elif mismatch == "lifecycle_preparation":
        value = _prepared_work_event(
            other_id,
            RecordKind.ROOM_LIFECYCLE,
            _prepared_event("m.room.member"),
            room_id=_PREPARED_ROOM_ID,
            membership_epoch=5,
            room_sequence=0,
        )
        metadata = _prepared_metadata(other_id, "m.room.member", None)
        work_id = other_id
        plaintext = journal_plan_module._canonical_work_plaintext(
            "event", value, metadata
        )
    else:
        value = _prepared_work_event(
            other_id,
            RecordKind.TO_DEVICE,
            canonical_json({"content": {}, "sender": _PLANNER_ACCOUNT_ID}),
        )
        metadata = _prepared_metadata(
            other_id,
            "m.key.verification.cancel",
            None,
            phase=_PreparationPhase.EXPIRED_VERIFICATION,
        )
        work_id = other_id
        plaintext = journal_plan_module._canonical_work_plaintext(
            "event", value, metadata
        )

    with pytest.raises(ValueError, match="Work plaintext"):
        journal_rows_module._decode_work_plaintext(
            _STREAM_ID, work_id, "event", plaintext
        )


def test_prepared_held_release_retains_authenticated_preparation_bytes() -> None:
    case = _prepared_planner_fixture(pending_hydration=True)
    held = _prepared_room_held(
        "12345678-1234-5678-1234-567812345689",
        membership_epoch=5,
        room_sequence=0,
        event_type="m.tag",
        route=_CallbackRoute.ROOM_ACCOUNT_DATA,
        frame_id=UUID("12345678-1234-5678-1234-567812345680"),
        created_revision=1,
    )
    value, metadata = held.value, held.metadata

    plan = _plan_prepared(case, work=(held,))

    assert plan is not None
    release = next(
        item for item in plan.work_releases if item.value.record_id == value.record_id
    )
    assert (release.metadata, release.plaintext) == (metadata, held.plaintext)


@pytest.mark.parametrize("path", ("retirement", "capacity"))
def test_legacy_planner_releases_retain_authenticated_preparation_bytes(
    path: str,
) -> None:
    if path == "retirement":
        frame, aggregate, held = _retirement_capacity_planner_case(())
        value = held.value
        assert type(value) is EventRecord
        metadata = _prepared_metadata(
            value.record_id, "m.room.message", _CallbackRoute.EVENT
        )
        limits = MaterializerLimits()
    else:
        frame, aggregate, held = _pending_hydration_planner_case()
        original = held.value
        assert type(original) is EventRecord
        value = replace(original, source_json=_prepared_event("m.tag"))
        metadata = _prepared_metadata(
            value.record_id, "m.tag", _CallbackRoute.ROOM_ACCOUNT_DATA
        )
        limits = replace(MaterializerLimits(), max_held_work_count=1)
    plaintext = journal_plan_module._canonical_work_plaintext("event", value, metadata)
    held = replace(
        held,
        value=value,
        canonical_size=len(plaintext) + 512,
        metadata=metadata,
        plaintext=plaintext,
        frame_id=_PLANNER_EXISTING_FRAME_ID,
        created_revision=1,
    )

    plan = plan_frame_materialization(
        account_id=_PLANNER_ACCOUNT_ID,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(held,),
        revision=2,
        limits=limits,
    )

    assert plan is not None
    release = plan.work_releases[0]
    assert (release.value, release.metadata, release.plaintext) == (
        value,
        metadata,
        plaintext,
    )


def _prepared_release_barrier_case(
    *, topic_padding: int = 0, held_padding: int = 0
) -> tuple[_PreparedPlannerFixture, AuthenticatedWork]:
    topic_source = canonical_json(
        {
            "content": {"padding": "x" * topic_padding},
            "type": "m.room.name",
        }
    )
    join_source = _prepared_event("m.room.member", event_id="$join", membership="join")
    segment = _prepared_segment(
        _prepared_observation("join", "join", "$join", "invite"),
        section=RoomSection.JOIN,
        state=(topic_source, join_source),
    )
    frame = _prepared_frame(room_segments=(segment,))
    records = (
        _prepared_record(
            0,
            RecordKind.STATE,
            topic_source,
            event_type="m.room.name",
            room=True,
        ),
        _prepared_record(
            1,
            RecordKind.STATE,
            join_source,
            event_type="m.room.member",
            room=True,
            event_id="$join",
        ),
    )
    transition = _prepared_transition(
        0, records[1], "invite", "join", 5, 5, _MembershipSourceKind.STATE
    )
    prepared = _prepared_payload(frame, records, (transition,))
    aggregate = _prepared_aggregate(
        epoch=5,
        membership="invite",
        next_sequence=1,
        hydration_name="prepared-release-hydration",
    )
    held = _prepared_room_held(
        "12345678-1234-5678-1234-567812345689",
        source_json=canonical_json(
            {
                "content": {"padding": "y" * held_padding},
                "type": "m.tag",
            }
        ),
        membership_epoch=5,
        room_sequence=0,
        event_type="m.tag",
        route=_CallbackRoute.ROOM_ACCOUNT_DATA,
        frame_id=UUID("12345678-1234-5678-1234-567812345680"),
        created_revision=1,
    )
    return _PreparedPlannerFixture(frame, prepared, aggregate), held


def _prepared_stored_work_size(
    frame: SyncFrame,
    item: journal_plan_module.PlannedWork,
    *,
    frame_id: UUID | None = None,
    created_revision: int = 3,
) -> int:
    value = item.value
    header = _canonical_internal(
        (
            value.record_id if type(value) is EventRecord else value.loss_id,
            "event" if type(value) is EventRecord else "loss",
            "held" if item.ready_ordinal is None else "ready",
            str(frame.frame_id if frame_id is None else frame_id),
            value.room_id,
            value.membership_epoch,
            value.room_sequence if type(value) is EventRecord else None,
            3 if item.ready_ordinal is not None else None,
            item.ready_ordinal,
            created_revision,
        )
    )
    return len(
        journal_plan_module._row(
            (_PLANNER_ACCOUNT_ID, _STREAM_ID, frame.origin.transport),
            "NioIngestWork",
            item.plaintext,
            header=header,
        )[0]
    )


def _prepared_unrelated_held(index: int) -> AuthenticatedWork:
    return _prepared_room_held(
        str(uuid5(_STREAM_ID, f"prepared-unrelated-held:{index}")),
        kind=RecordKind.EPHEMERAL,
        source_json=_prepared_event("m.typing"),
        origin=RecordOrigin(TransportKind.CLASSIC, 0, 0, index),
        room_id="!prepared-unrelated:example.org",
        membership_epoch=9,
        room_sequence=index,
    )


def test_prepared_pending_hydration_orders_loss_all_releases_then_lifecycle() -> None:
    case, held = _prepared_release_barrier_case()
    records = case.prepared.records
    transition = case.prepared.membership_transitions[0]
    old_value = held.value

    plan = _plan_prepared(case, work=(held,))

    assert plan is not None
    ordered = sorted(
        (*plan.work_inserts, *plan.work_releases),
        key=lambda item: item.ready_ordinal,
    )
    assert [
        (
            type(item.value).__name__,
            (
                item.value.reason.value
                if type(item.value) is LossRecord
                else item.value.record_id
            ),
        )
        for item in ordered
    ] == [
        ("LossRecord", "unverifiable"),
        ("EventRecord", old_value.record_id),
        ("EventRecord", records[0].record_id),
        ("EventRecord", transition.transition_id),
        ("EventRecord", records[1].record_id),
    ]
    assert [item.ready_ordinal for item in ordered] == list(range(5))
    promoted = next(
        item
        for item in plan.work_inserts
        if item.value.record_id == records[0].record_id
    )
    assert promoted.metadata is not None
    assert plan.room_values == (
        RoomAggregateValue(
            RoomContinuity(_PREPARED_ROOM_ID, 5, "join", None, None, None),
            4,
            3,
            None,
        ),
    )


def test_prepared_buffered_promotion_uses_final_ready_row_capacity() -> None:
    case, held = _prepared_release_barrier_case(
        topic_padding=4096,
        held_padding=8192,
    )
    baseline = _plan_prepared(case, work=(held,))
    assert baseline is not None
    buffered_id = case.prepared.records[0].record_id
    promoted = next(
        item for item in baseline.work_inserts if item.value.record_id == buffered_id
    )
    final_ready_size = _prepared_stored_work_size(case.frame, promoted)

    exact = _plan_prepared(
        case,
        work=(held,),
        limits=replace(
            MaterializerLimits(), max_record_canonical_bytes=final_ready_size
        ),
    )

    assert exact is not None
    assert any(item.value.record_id == buffered_id for item in exact.work_inserts)

    under = _plan_prepared(
        case,
        work=(held,),
        limits=replace(
            MaterializerLimits(), max_record_canonical_bytes=final_ready_size - 1
        ),
    )

    assert under is not None
    ordered = sorted(
        (*under.work_inserts, *under.work_releases),
        key=lambda item: item.ready_ordinal,
    )
    assert [item.ready_ordinal for item in ordered] == list(range(len(ordered)))
    assert [
        item.value.reason for item in ordered if type(item.value) is LossRecord
    ] == [LossReason.UNVERIFIABLE, LossReason.OVERSIZED_EVENT]
    assert [item.value.kind for item in ordered if type(item.value) is EventRecord] == [
        RecordKind.ROOM_ACCOUNT_DATA,
        RecordKind.ROOM_LIFECYCLE,
    ]
    release = under.work_releases[0]
    assert (release.value, release.plaintext) == (held.value, held.plaintext)
    assert under.room_values == (
        RoomAggregateValue(
            RoomContinuity(_PREPARED_ROOM_ID, 5, "join", None, None, None),
            2,
            3,
            None,
        ),
    )


def test_prepared_capacity_loss_follows_all_room_lifecycle_fences() -> None:
    case = _prepared_planner_fixture()
    oversized_source = canonical_json(
        {"content": {"padding": "z" * 4096}, "type": "m.tag"}
    )
    other_room_id = "!prepared-other:example.org"
    other_source = _prepared_event("m.typing")
    segment = replace(
        case.frame.room_segments[0],
        room_account_data_json=(oversized_source,),
    )
    frame = replace(
        case.frame,
        room_segments=(segment,),
        ephemeral_json=case.frame.ephemeral_json
        + (_ephemeral_envelope(other_room_id, json.loads(other_source)),),
    )
    records = list(case.prepared.records)
    records[6] = records[6]._replace(source_json=oversized_source)
    other_record = _prepared_record(
        7,
        RecordKind.EPHEMERAL,
        other_source,
        event_type="m.typing",
        room=True,
        route=_CallbackRoute.EPHEMERAL,
    )._replace(room_id=other_room_id)
    records[7] = records[7]._replace(
        record_id=_PREPARED_RECORD_IDS[8],
        origin=replace(records[7].origin, frame_index=8),
    )
    records[8] = records[8]._replace(
        record_id=str(uuid5(frame.frame_id, "record:source:9")),
        origin=replace(records[8].origin, frame_index=9),
    )
    records.insert(7, other_record)
    prepared = case.prepared._replace(records=tuple(records))
    other_aggregate = RoomAggregateValue(
        RoomContinuity(
            other_room_id,
            2,
            "join",
            MembershipBaseline("$other-join", None),
            None,
            None,
        ),
        4,
        1,
        None,
    )
    baseline = _plan_prepared(
        case,
        frame=frame,
        prepared=prepared,
        aggregates=(case.aggregate, other_aggregate),
    )
    assert baseline is not None
    oversized = next(
        item
        for item in baseline.work_inserts
        if item.value.record_id == records[6].record_id
    )
    exact_size = _prepared_stored_work_size(frame, oversized)

    plan = _plan_prepared(
        case,
        frame=frame,
        prepared=prepared,
        aggregates=(case.aggregate, other_aggregate),
        limits=replace(MaterializerLimits(), max_record_canonical_bytes=exact_size - 1),
    )

    assert plan is not None
    ordered = sorted(plan.work_inserts, key=lambda item: item.ready_ordinal)
    lifecycle_positions = [
        index
        for index, item in enumerate(ordered)
        if type(item.value) is EventRecord
        and item.value.kind is RecordKind.ROOM_LIFECYCLE
    ]
    capacity_position, capacity_loss = next(
        (index, item.value)
        for index, item in enumerate(ordered)
        if type(item.value) is LossRecord
        and item.value.reason is LossReason.OVERSIZED_EVENT
    )
    later_accountwide_positions = [
        index
        for index, item in enumerate(ordered)
        if type(item.value) is EventRecord
        and item.value.kind in (RecordKind.PRESENCE, RecordKind.GLOBAL_ACCOUNT_DATA)
    ]
    assert len(lifecycle_positions) == 4
    assert (
        max(lifecycle_positions) < capacity_position < min(later_accountwide_positions)
    )
    assert capacity_loss.membership_epoch == 7
    terminal_source_ids = {
        record.record_id
        for record in prepared.records
        if record.room_id == _PREPARED_ROOM_ID
    }
    assert terminal_source_ids.isdisjoint(
        item.value.record_id for item in ordered if type(item.value) is EventRecord
    )
    assert any(
        type(item.value) is EventRecord
        and item.value.record_id == other_record.record_id
        for item in ordered
    )
    primary_value = next(
        value
        for value in plan.room_values
        if value.continuity.room_id == _PREPARED_ROOM_ID
    )
    assert primary_value == RoomAggregateValue(
        RoomContinuity(
            _PREPARED_ROOM_ID,
            7,
            "ban",
            None,
            None,
            None,
            last_timeline_event_id="$ban",
        ),
        case.aggregate.next_room_sequence + 4,
        3,
        None,
    )


@pytest.mark.parametrize(
    ("limited_field", "unrelated_count"),
    (
        ("max_record_canonical_bytes", 0),
        ("max_ready_work_count", 0),
        ("max_ready_work_canonical_bytes", 0),
        ("max_held_work_count", 2),
        ("max_held_work_canonical_bytes", 1),
        ("max_total_work_count", 0),
        ("max_total_work_canonical_bytes", 0),
    ),
)
def test_prepared_mandatory_barrier_uses_immutable_capacity_envelope(
    limited_field: str,
    unrelated_count: int,
) -> None:
    case, held = _prepared_release_barrier_case()
    unrelated = tuple(
        _prepared_unrelated_held(index) for index in range(unrelated_count)
    )

    plan = _plan_prepared(
        case,
        work=(held, *unrelated),
        limits=replace(MaterializerLimits(), **{limited_field: 1}),
    )

    assert plan is not None
    ordered = sorted(
        (*plan.work_inserts, *plan.work_releases),
        key=lambda item: item.ready_ordinal,
    )
    assert any(
        type(item.value) is LossRecord and item.value.reason is LossReason.UNVERIFIABLE
        for item in ordered
    )
    assert any(
        type(item.value) is EventRecord and item.value.kind is RecordKind.ROOM_LIFECYCLE
        for item in ordered
    )
    assert any(item.value == held.value for item in plan.work_releases)


def test_prepared_terminal_reordinal_rechecks_final_immutable_record_size() -> None:
    case = _prepared_pending_room_case()
    aggregate = replace(case.aggregate, next_room_sequence=999)
    global_record = case.prepared.records[1]
    hard_limit = MaterializerLimits().max_record_canonical_bytes

    def target(
        padding: int,
        frame_index: int,
    ) -> tuple[bytes, _PreparedIngestionRecord, journal_plan_module.PlannedWork]:
        source_json = canonical_json(
            {"content": {"padding": "w" * padding}, "type": "m.push_rules"}
        )
        record = global_record._replace(
            record_id=str(uuid5(case.frame.frame_id, f"record:source:{frame_index}")),
            origin=replace(global_record.origin, frame_index=frame_index),
            source_json=source_json,
        )
        return (
            source_json,
            record,
            _prepared_planned_record(
                record,
                membership_epoch=None,
                room_sequence=None,
                ready_ordinal=0,
            ),
        )

    low, high = 0, hard_limit
    while low < high:
        middle = (low + high + 1) // 2
        _source, _record, candidate = target(middle, 1)
        if _prepared_stored_work_size(case.frame, candidate) <= hard_limit:
            low = middle
        else:
            high = middle - 1
    source_json, record, preliminary = target(low, 1)
    final = preliminary._replace(ready_ordinal=1000)
    assert _prepared_stored_work_size(case.frame, preliminary) <= hard_limit
    assert _prepared_stored_work_size(case.frame, final) > hard_limit

    frame = replace(
        case.frame,
        global_account_data_json=(source_json,),
    )
    prepared = case.prepared._replace(records=(case.prepared.records[0], record))
    held = tuple(
        _prepared_room_held(
            str(uuid5(_STREAM_ID, f"prepared-terminal-held:{index}")),
            membership_epoch=5,
            room_sequence=index,
            origin=RecordOrigin(TransportKind.CLASSIC, 0, 0, index),
            frame_id=uuid5(_STREAM_ID, f"prepared-terminal-frame:{index}"),
            created_revision=1,
        )
        for index in range(999)
    )

    with pytest.raises(ValueError, match="immutable"):
        _plan_prepared(
            case,
            frame=frame,
            prepared=prepared,
            aggregates=(aggregate,),
            work=held,
            limits=replace(MaterializerLimits(), max_held_work_count=999),
        )


def test_prepared_held_capacity_terminalizes_only_overflowing_room() -> None:
    room_ids = (_PREPARED_ROOM_ID, "!prepared-second:example.org")
    sources = (_prepared_event("m.tag"), _prepared_event("m.tag"))
    segments = tuple(
        _prepared_segment(
            _prepared_observation(),
            room_id=room_id,
            room_account_data=(source_json,),
        )
        for room_id, source_json in zip(room_ids, sources, strict=True)
    )
    global_source = _prepared_event("m.push_rules")
    frame = _prepared_frame(
        room_segments=segments, global_account_data=(global_source,)
    )
    records = (
        _prepared_record(
            0,
            RecordKind.ROOM_ACCOUNT_DATA,
            sources[0],
            event_type="m.tag",
            room=True,
            route=_CallbackRoute.ROOM_ACCOUNT_DATA,
        ),
        _prepared_record(
            1,
            RecordKind.ROOM_ACCOUNT_DATA,
            sources[1],
            event_type="m.tag",
            room=True,
            route=_CallbackRoute.ROOM_ACCOUNT_DATA,
        )._replace(room_id=room_ids[1]),
        _prepared_record(
            2,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            global_source,
            event_type="m.push_rules",
            route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
        ),
    )
    prepared = _prepared_payload(frame, records)
    aggregates = tuple(
        _prepared_aggregate(
            room_id=room_id,
            epoch=5,
            membership="join",
            next_sequence=0,
            hydration_name=f"prepared-capacity:{room_id}",
            hydration_origin=RecordOrigin(TransportKind.CLASSIC, 0, 0, index),
        )
        for index, room_id in enumerate(room_ids)
    )

    plan = _plan_prepared(
        _PreparedPlannerFixture(frame, prepared, aggregates[0]),
        aggregates=aggregates,
        limits=replace(MaterializerLimits(), max_held_work_count=1),
    )

    assert plan is not None
    first = next(
        item
        for item in plan.work_inserts
        if type(item.value) is EventRecord
        and item.value.record_id == records[0].record_id
    )
    assert first.ready_ordinal is None
    assert not any(
        type(item.value) is EventRecord and item.value.record_id == records[1].record_id
        for item in plan.work_inserts
    )
    loss = next(
        item.value for item in plan.work_inserts if type(item.value) is LossRecord
    )
    assert (loss.room_id, loss.reason) == (room_ids[1], LossReason.EVENT_LIMIT)
    assert any(
        type(item.value) is EventRecord and item.value.record_id == records[2].record_id
        for item in plan.work_inserts
    )
    assert plan.room_values == (
        RoomAggregateValue(
            aggregates[0].continuity,
            1,
            3,
            aggregates[0].pending_hydration,
        ),
        RoomAggregateValue(
            replace(aggregates[1].continuity, hydration_id=None),
            0,
            3,
            None,
        ),
    )


def _prepared_gap_case(
    *, empty: bool
) -> tuple[_PreparedPlannerFixture, AuthenticatedWork]:
    room_source = _prepared_event("m.room.name")
    presence_source = _prepared_event("m.presence") if empty else None
    global_source = _prepared_event("m.push_rules") if empty else None
    segment = _prepared_segment(
        _prepared_observation("join", unparsed=True),
        section=RoomSection.JOIN,
        state=() if empty else (room_source,),
    )
    frame = _prepared_frame(
        room_segments=(segment,),
        global_account_data=(() if global_source is None else (global_source,)),
        presence=(() if presence_source is None else (presence_source,)),
    )
    records = (
        (
            _prepared_record(
                0,
                RecordKind.PRESENCE,
                presence_source,
                event_type="m.presence",
                route=_CallbackRoute.PRESENCE,
            ),
            _prepared_record(
                1,
                RecordKind.GLOBAL_ACCOUNT_DATA,
                global_source,
                event_type="m.push_rules",
                route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
            ),
        )
        if presence_source is not None and global_source is not None
        else (
            _prepared_record(
                0,
                RecordKind.STATE,
                room_source,
                event_type="m.room.name",
                room=True,
            ),
        )
    )
    prepared = _prepared_payload(frame, records)
    gap = RecoveryGap(
        uuid5(_STREAM_ID, f"prepared-{'empty-' if empty else ''}gap"),
        _PREPARED_ROOM_ID,
        3,
        RecordOrigin(TransportKind.CLASSIC, 0, 0, 0),
        "s0",
        "s1",
    )
    aggregate = _prepared_aggregate(
        epoch=3,
        membership="join",
        next_sequence=1,
        baseline=MembershipBaseline("$join", "s0"),
        gap=gap,
    )
    held = _prepared_room_held(
        "12345678-1234-5678-1234-567812345689",
        kind=RecordKind.STATE,
        source_json=_prepared_event("m.room.topic"),
        membership_epoch=3,
        room_sequence=0,
        frame_id=UUID("12345678-1234-5678-1234-567812345680"),
        created_revision=1,
    )
    return _PreparedPlannerFixture(frame, prepared, aggregate), held


def test_prepared_gap_fallback_persists_loss_release_and_new_hydration() -> None:
    case, held = _prepared_gap_case(empty=False)

    plan = _plan_prepared(case, work=(held,))

    assert plan is not None
    loss = next(
        item.value for item in plan.work_inserts if type(item.value) is LossRecord
    )
    assert (loss.reason, loss.boundary) == (
        LossReason.BASELINE_LOST,
        LossBoundary(None, None, "s0", "s1"),
    )
    assert (plan.work_releases[0].value, plan.work_releases[0].ready_ordinal) == (
        held.value,
        1,
    )
    incoming = next(
        item
        for item in plan.work_inserts
        if type(item.value) is EventRecord
        and item.value.record_id == case.prepared.records[0].record_id
    )
    assert incoming.ready_ordinal is None
    assert plan.room_values[0].continuity.hydration_id is not None
    assert plan.room_values[0].pending_hydration is not None


def test_prepared_new_gap_is_lost_and_released_without_persisting_gap() -> None:
    source_json = _prepared_event(
        "m.room.member",
        event_id="$member",
        membership="join",
    )
    segment = replace(
        _prepared_segment(
            _prepared_observation("join", "join", "$member"),
            section=RoomSection.JOIN,
            state=(source_json,),
        ),
        timeline_limited=True,
        timeline_prev_batch="room-new",
    )
    frame = replace(
        _prepared_frame(room_segments=(segment,)),
        origin=RecordOrigin(TransportKind.SLIDING, 0, 1, 0),
    )
    record = _prepared_record(
        0,
        RecordKind.STATE,
        source_json,
        event_type="m.room.member",
        room=True,
        event_id="$member",
    )._replace(
        origin=frame.origin,
    )
    aggregate = _prepared_aggregate(
        epoch=3,
        membership="join",
        next_sequence=4,
        baseline=MembershipBaseline("$member", "room-old"),
    )
    case = _PreparedPlannerFixture(
        frame,
        _prepared_payload(frame, (record,))._replace(compatibility_token=None),
        aggregate,
    )

    plan = _plan_prepared(case)

    assert plan is not None
    assert tuple(
        journal_rows_module._canonical_room_aggregate_plaintext(value)
        for value in plan.room_values
    )
    assert plan.room_values == (
        RoomAggregateValue(
            RoomContinuity(
                _PREPARED_ROOM_ID,
                3,
                "join",
                MembershipBaseline("$member", "room-new"),
                None,
                None,
            ),
            5,
            3,
            None,
        ),
    )
    assert plan.work_releases == ()
    assert len(plan.work_inserts) == 2
    loss_item, event_item = plan.work_inserts
    assert loss_item.ready_ordinal == 0
    assert type(loss_item.value) is LossRecord
    assert (
        loss_item.value.origin,
        loss_item.value.room_id,
        loss_item.value.membership_epoch,
        loss_item.value.reason,
        loss_item.value.boundary,
    ) == (
        frame.origin,
        _PREPARED_ROOM_ID,
        3,
        LossReason.BASELINE_LOST,
        LossBoundary(None, None, "room-old", "room-new"),
    )
    assert event_item.ready_ordinal == 1
    assert type(event_item.value) is EventRecord
    assert (
        event_item.value.record_id,
        event_item.value.membership_epoch,
        event_item.value.room_sequence,
    ) == (record.record_id, 3, 4)


def test_prepared_reopened_initial_window_materializes_exact_recovered_suffix() -> None:
    case = _prepared_sliding_timeline_case(
        ("$old", "$anchor", "$recovered-1", "$recovered-2"),
        initial=True,
        live_event_count=0,
    )

    plan = _plan_prepared(case)

    assert plan is not None
    assert not any(type(item.value) is LossRecord for item in plan.work_inserts)
    assert [
        item.value.provenance
        for item in plan.work_inserts
        if type(item.value) is EventRecord
    ] == [
        TimelineEventProvenance.HISTORY,
        TimelineEventProvenance.HISTORY,
        TimelineEventProvenance.RECOVERED,
        TimelineEventProvenance.RECOVERED,
    ]
    assert plan.room_values == (
        replace(
            case.aggregate,
            continuity=replace(
                case.aggregate.continuity,
                last_timeline_event_id="$recovered-2",
            ),
            next_room_sequence=8,
            updated_revision=3,
        ),
    )


def test_prepared_unanchorable_live_tail_clears_timeline_boundary() -> None:
    case = _prepared_sliding_timeline_case(
        ("$seen", None),
        initial=False,
        live_event_count=2,
    )

    plan = _plan_prepared(case)

    assert plan is not None
    assert plan.room_values[0].continuity.last_timeline_event_id is None


def test_prepared_empty_gap_barrier_precedes_later_accountwide_work() -> None:
    case, held = _prepared_gap_case(empty=True)

    plan = _plan_prepared(case, work=(held,))

    assert plan is not None
    ordered = sorted(
        (*plan.work_inserts, *plan.work_releases),
        key=lambda item: item.ready_ordinal,
    )
    assert [
        (
            item.value.kind.value
            if type(item.value) is EventRecord
            else item.value.reason.value
        )
        for item in ordered
    ] == [
        "baseline_lost",
        RecordKind.STATE.value,
        RecordKind.PRESENCE.value,
        RecordKind.GLOBAL_ACCOUNT_DATA.value,
    ]


def test_prepared_planner_leaves_unrelated_held_work_untouched() -> None:
    case = _prepared_planner_fixture()
    held = _prepared_room_held(
        "12345678-1234-5678-1234-567812345689",
        kind=RecordKind.STATE,
        source_json=_prepared_event("m.room.topic"),
        room_id="!unrelated:example.org",
        membership_epoch=8,
        room_sequence=0,
    )

    plan = _plan_prepared(case, work=(held,))

    assert plan is not None
    assert plan.work_releases == ()


@pytest.mark.parametrize(
    ("membership_epoch", "room_sequence"), ((4, 0), (5, 7), (5, 0))
)
def test_prepared_planner_rejects_selected_held_outside_aggregate_bounds(
    membership_epoch: int,
    room_sequence: int,
) -> None:
    case = _prepared_planner_fixture()
    held = _prepared_room_held(
        "12345678-1234-5678-1234-567812345689",
        kind=RecordKind.STATE,
        source_json=_prepared_event("m.room.topic"),
        membership_epoch=membership_epoch,
        room_sequence=room_sequence,
    )

    with pytest.raises(ValueError, match="Aggregate|barrier"):
        _plan_prepared(case, work=(held,))


def _prepared_pending_room_case(
    *, source_count: int = 1, include_global: bool = True
) -> _PreparedPlannerFixture:
    room_sources = tuple(_prepared_event("m.tag") for _ in range(source_count))
    global_sources = (_prepared_event("m.push_rules"),) if include_global else ()
    segment = _prepared_segment(_prepared_observation(), room_account_data=room_sources)
    frame = _prepared_frame(
        room_segments=(segment,), global_account_data=global_sources
    )
    records = tuple(
        _prepared_record(
            index,
            RecordKind.ROOM_ACCOUNT_DATA,
            source_json,
            event_type="m.tag",
            room=True,
            route=_CallbackRoute.ROOM_ACCOUNT_DATA,
        )
        for index, source_json in enumerate(room_sources)
    ) + tuple(
        _prepared_record(
            source_count + index,
            RecordKind.GLOBAL_ACCOUNT_DATA,
            source_json,
            event_type="m.push_rules",
            route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
        )
        for index, source_json in enumerate(global_sources)
    )
    prepared = _prepared_payload(frame, records)
    aggregate = _prepared_aggregate(
        epoch=5,
        membership="join",
        next_sequence=0,
        hydration_name="prepared-capacity-hydration",
    )
    return _PreparedPlannerFixture(frame, prepared, aggregate)


def _prepared_prior_held(
    case: _PreparedPlannerFixture,
) -> tuple[_PreparedPlannerFixture, AuthenticatedWork]:
    aggregate = replace(case.aggregate, next_room_sequence=1)
    case = replace(case, aggregate=aggregate)
    frame_id = UUID("12345678-1234-5678-1234-567812345680")
    created_revision = 1
    return case, _prepared_room_held(
        "12345678-1234-5678-1234-567812345689",
        membership_epoch=aggregate.continuity.membership_epoch,
        room_sequence=0,
        event_type="m.tag",
        route=_CallbackRoute.ROOM_ACCOUNT_DATA,
        frame_id=frame_id,
        created_revision=created_revision,
        exact_size_frame=case.frame,
    )


@pytest.mark.parametrize(
    ("limited_field", "reason"),
    (
        ("max_record_canonical_bytes", LossReason.OVERSIZED_EVENT),
        ("max_held_work_canonical_bytes", LossReason.EVENT_LIMIT),
    ),
)
def test_prepared_room_capacity_uses_final_wrapped_row_and_terminal_loss(
    limited_field: str,
    reason: LossReason,
) -> None:
    case = _prepared_pending_room_case()
    held: AuthenticatedWork | None = None
    if reason is LossReason.EVENT_LIMIT:
        case, held = _prepared_prior_held(case)
    record = case.prepared.records[0]
    exact_size = _prepared_stored_work_size(
        case.frame,
        _prepared_planned_record(
            record,
            membership_epoch=case.aggregate.continuity.membership_epoch,
            room_sequence=case.aggregate.next_room_sequence,
            ready_ordinal=None,
        ),
    )
    if held is not None:
        exact_size += held.canonical_size
    exact_limits = replace(MaterializerLimits(), **{limited_field: exact_size})

    exact = _plan_prepared(
        case,
        work=(() if held is None else (held,)),
        limits=exact_limits,
    )

    assert exact is not None
    room_item = next(
        item
        for item in exact.work_inserts
        if type(item.value) is EventRecord and item.value.room_id is not None
    )
    assert room_item.ready_ordinal is None

    under = _plan_prepared(
        case,
        work=(() if held is None else (held,)),
        limits=replace(exact_limits, **{limited_field: exact_size - 1}),
    )

    assert under is not None
    assert [
        item.value.kind
        for item in under.work_inserts
        if type(item.value) is EventRecord
    ] == [RecordKind.GLOBAL_ACCOUNT_DATA]
    loss = next(
        item.value for item in under.work_inserts if type(item.value) is LossRecord
    )
    assert (loss.reason, loss.membership_epoch) == (reason, 5)
    ordered = sorted(
        (*under.work_inserts, *under.work_releases),
        key=lambda item: item.ready_ordinal,
    )
    assert [item.ready_ordinal for item in ordered] == list(range(len(ordered)))
    if held is not None:
        assert len(under.work_releases) == 1
        release = under.work_releases[0]
        assert (
            release.value,
            release.plaintext,
            release.ready_ordinal,
        ) == (held.value, held.plaintext, 1)
    assert under.room_values == (
        RoomAggregateValue(
            RoomContinuity(_PREPARED_ROOM_ID, 5, "join", None, None, None),
            case.aggregate.next_room_sequence,
            3,
            None,
        ),
    )


@dataclass(frozen=True)
class _FinalRowCapacityCase:
    case_id: str
    status: str
    limit_delta: int
    plan_is_admitted: bool
    loss_reason: LossReason | None


_FINAL_ROW_CAPACITY_CASES = (
    _FinalRowCapacityCase("exact-final-ready-row", "ready", 0, True, None),
    _FinalRowCapacityCase("one-over-final-ready-row", "ready", -1, False, None),
    _FinalRowCapacityCase("exact-final-held-row", "held", 0, True, None),
    _FinalRowCapacityCase(
        "one-over-final-held-row",
        "held",
        -1,
        True,
        LossReason.EVENT_LIMIT,
    ),
)


@pytest.mark.parametrize(
    "case",
    _FINAL_ROW_CAPACITY_CASES,
    ids=lambda case: case.case_id,
)
def test_materializer_final_row_capacity_matrix(
    case: _FinalRowCapacityCase,
) -> None:
    """Each named READY/HELD case owns its final Work-row byte boundary."""

    account_id = "@matrix-final-ready:example.org"
    frame, aggregate, held = _pending_hydration_planner_case(
        global_ready_count=1,
        account_id=account_id,
    )
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    descriptor_index, descriptor = next(
        (index, descriptor)
        for index, descriptor in enumerate(proposal.descriptors)
        if (descriptor.room_id is None) is (case.status == "ready")
    )
    expected = EventRecord(
        str(uuid5(frame.frame_id, f"event:{descriptor.descriptor_key}")),
        descriptor.kind,
        replace(frame.origin, frame_index=descriptor_index),
        descriptor.room_id if case.status == "held" else None,
        aggregate.continuity.membership_epoch if case.status == "held" else None,
        aggregate.next_room_sequence if case.status == "held" else None,
        None,
        descriptor.provenance if case.status == "held" else None,
        descriptor.source_json,
        None,
    )
    final_row = _expected_stored_work_payload(
        account_id=account_id,
        stream_id=_STREAM_ID,
        transport_kind=TransportKind.CLASSIC,
        frame_id=frame.frame_id,
        value=expected,
        status=case.status,
        ready_revision=2 if case.status == "ready" else None,
        ready_ordinal=0 if case.status == "ready" else None,
        created_revision=2,
    )
    assert len(_expected_event_work_plaintext(expected)) < len(final_row)
    existing_work: tuple[AuthenticatedWork, ...] = (held,)
    limits = replace(
        MaterializerLimits(),
        max_ready_work_canonical_bytes=len(final_row) + case.limit_delta,
    )
    if case.status == "held":
        old_row = _expected_stored_work_payload(
            account_id=account_id,
            stream_id=_STREAM_ID,
            transport_kind=TransportKind.CLASSIC,
            frame_id=_PLANNER_EXISTING_FRAME_ID,
            value=held.value,
            status="held",
            ready_revision=None,
            ready_ordinal=None,
            created_revision=1,
        )
        existing_work = (replace(held, canonical_size=len(old_row)),)
        limits = replace(
            MaterializerLimits(),
            max_held_work_canonical_bytes=(
                len(old_row) + len(final_row) + case.limit_delta
            ),
        )

    plan = plan_frame_materialization(
        account_id=account_id,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=existing_work,
        revision=2,
        limits=limits,
    )

    assert (plan is not None) is case.plan_is_admitted
    if case.loss_reason is not None:
        assert plan is not None
        assert any(
            type(value) is LossRecord and value.reason is case.loss_reason
            for value, _payload, _ordinal in plan.work_inserts
        )
    elif plan is not None:
        assert not any(
            type(value) is LossRecord and value.reason is LossReason.EVENT_LIMIT
            for value, _payload, _ordinal in plan.work_inserts
        )


def test_materializer_record_limit_counts_final_stored_work_payload() -> None:
    """RED: a room record over the stored-row limit gets a bounded loss."""

    account_id = "@planner-record-outer:example.org"
    frame, aggregate, held = _pending_hydration_planner_case(account_id=account_id)
    source_json = b'{"content":{"padding":"' + (b"x" * 2_000) + b'"}}'
    frame = replace(
        frame,
        room_segments=(
            replace(frame.room_segments[0], room_account_data_json=(source_json,)),
        ),
    )
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    descriptor_index, descriptor = next(
        (index, descriptor)
        for index, descriptor in enumerate(proposal.descriptors)
        if descriptor.room_id is not None
    )
    incoming = EventRecord(
        str(uuid5(frame.frame_id, f"event:{descriptor.descriptor_key}")),
        descriptor.kind,
        replace(frame.origin, frame_index=descriptor_index),
        descriptor.room_id,
        aggregate.continuity.membership_epoch,
        aggregate.next_room_sequence,
        None,
        descriptor.provenance,
        descriptor.source_json,
        None,
    )
    incoming_inner = _expected_event_work_plaintext(incoming)
    incoming_outer = _expected_stored_work_payload(
        account_id=account_id,
        stream_id=_STREAM_ID,
        transport_kind=TransportKind.CLASSIC,
        frame_id=frame.frame_id,
        value=incoming,
        status="held",
        ready_revision=None,
        ready_ordinal=None,
        created_revision=2,
    )
    limit = len(incoming_outer) - 1
    assert len(incoming_inner) < limit
    loss_without_id = LossRecord(
        "",
        frame.origin,
        incoming.room_id,
        incoming.membership_epoch,
        LossReason.OVERSIZED_EVENT,
        LossBoundary(None, None, None, None),
        b"{}",
    )
    loss = replace(
        loss_without_id,
        loss_id=_loss_id(_STREAM_ID, loss_without_id),
    )
    loss_outer = _expected_stored_work_payload(
        account_id=account_id,
        stream_id=_STREAM_ID,
        transport_kind=TransportKind.CLASSIC,
        frame_id=frame.frame_id,
        value=loss,
        status="ready",
        ready_revision=2,
        ready_ordinal=0,
        created_revision=2,
    )
    assert len(loss_outer) <= limit
    old_frame_id = _PLANNER_EXISTING_FRAME_ID
    old_held_payload = _expected_stored_work_payload(
        account_id=account_id,
        stream_id=_STREAM_ID,
        transport_kind=TransportKind.CLASSIC,
        frame_id=old_frame_id,
        value=held.value,
        status="held",
        ready_revision=None,
        ready_ordinal=None,
        created_revision=1,
    )

    exact = plan_frame_materialization(
        account_id=account_id,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(
            replace(
                held,
                canonical_size=len(old_held_payload),
            ),
        ),
        revision=2,
        limits=replace(
            MaterializerLimits(),
            max_record_canonical_bytes=len(incoming_outer),
        ),
    )
    assert exact is not None
    assert not any(
        type(value) is LossRecord and value.reason is LossReason.OVERSIZED_EVENT
        for value, _payload, _ordinal in exact.work_inserts
    )

    one_over = plan_frame_materialization(
        account_id=account_id,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(
            replace(
                held,
                canonical_size=len(old_held_payload),
            ),
        ),
        revision=2,
        limits=replace(
            MaterializerLimits(),
            max_record_canonical_bytes=limit,
        ),
    )

    assert one_over is not None
    assert any(
        type(value) is LossRecord and value.reason is LossReason.OVERSIZED_EVENT
        for value, _payload, _ordinal in one_over.work_inserts
    )


@pytest.mark.parametrize("extra_byte", (0, 1), ids=("exact", "over"))
def test_materializer_total_capacity_counts_rebuilt_release_payload(
    extra_byte: int,
) -> None:
    """RED: HELD-to-READY outer-byte growth participates in hard total."""

    account_id = "@planner-release:example.org"
    revision = 10**20
    frame, aggregate, held = _retirement_capacity_planner_case(
        (),
        account_id=account_id,
    )
    old_frame_id = _PLANNER_EXISTING_FRAME_ID
    old_payload = _expected_stored_work_payload(
        account_id=account_id,
        stream_id=_STREAM_ID,
        transport_kind=TransportKind.CLASSIC,
        frame_id=old_frame_id,
        value=held.value,
        status="held",
        ready_revision=None,
        ready_ordinal=None,
        created_revision=1,
    )
    selected_held = replace(
        held,
        canonical_size=len(old_payload),
    )
    old_loss, lifecycle, successors, globals_, _capacity_loss = (
        _expected_retirement_capacity_records(
            (),
            frame.global_account_data_json,
        )
    )
    assert successors == ()
    inserted = (
        (old_loss, 0),
        (lifecycle, 2),
        (globals_[0], 3),
    )
    inserted_bytes = sum(
        len(
            _expected_stored_work_payload(
                account_id=account_id,
                stream_id=_STREAM_ID,
                transport_kind=TransportKind.CLASSIC,
                frame_id=frame.frame_id,
                value=value,
                status="ready",
                ready_revision=revision,
                ready_ordinal=ordinal,
                created_revision=revision,
            )
        )
        for value, ordinal in inserted
    )
    release_bytes = len(
        _expected_stored_work_payload(
            account_id=account_id,
            stream_id=_STREAM_ID,
            transport_kind=TransportKind.CLASSIC,
            frame_id=old_frame_id,
            value=held.value,
            status="ready",
            ready_revision=revision,
            ready_ordinal=1,
            created_revision=1,
        )
    )
    assert release_bytes > len(old_payload)
    remaining = (
        MaterializerLimits().max_total_work_canonical_bytes
        - inserted_bytes
        - release_bytes
    )
    assert remaining > 0
    filler_count = 65
    quotient, remainder = divmod(remaining, filler_count)
    filler_sizes = [
        quotient + (1 if index < remainder else 0) for index in range(filler_count)
    ]
    filler_sizes[-1] += extra_byte
    assert max(filler_sizes) <= MaterializerLimits().max_record_canonical_bytes
    ready = tuple(
        _planner_ready_work(
            frame,
            index,
            account_id=account_id,
            canonical_size=size,
        )
        for index, size in enumerate(filler_sizes)
    )

    plan = plan_frame_materialization(
        account_id=account_id,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(selected_held, *ready),
        revision=revision,
        limits=MaterializerLimits(),
    )

    assert (plan is None) is bool(extra_byte)


@pytest.mark.parametrize("extra_byte", (0, 1), ids=("exact", "one-over"))
def test_materializer_rebuilt_release_obeys_immutable_record_limit(
    extra_byte: int,
) -> None:
    """RED: an oversized mandatory HELD-to-READY rebuild fails closed."""

    account_id = "@planner-release-record:example.org"
    frame, aggregate, held = _retirement_capacity_planner_case(
        (),
        account_id=account_id,
    )
    revision = 2**63 - 1
    empty_source = b'{"content":{"padding":""},"type":"m.room.message"}'
    base_value = replace(
        held.value,
        origin=replace(held.value.origin, frame_index=0),
        source_json=empty_source,
    )

    def release_payload(value: EventRecord) -> bytes:
        return _expected_stored_work_payload(
            account_id=account_id,
            stream_id=_STREAM_ID,
            transport_kind=frame.origin.transport,
            frame_id=_PLANNER_EXISTING_FRAME_ID,
            value=value,
            status="ready",
            ready_revision=revision,
            ready_ordinal=1,
            created_revision=1,
        )

    hard_limit = MaterializerLimits().max_record_canonical_bytes
    target = hard_limit + extra_byte
    padding_length = 3 * ((target - len(release_payload(base_value))) // 4)
    value = replace(
        base_value,
        source_json=(
            b'{"content":{"padding":"'
            + b"x" * padding_length
            + b'"},"type":"m.room.message"}'
        ),
    )
    remaining = target - len(release_payload(value))
    assert 0 <= remaining <= 3
    value = replace(value, origin=replace(value.origin, frame_index=10**remaining))
    rebuilt_payload = release_payload(value)
    assert len(rebuilt_payload) == target
    held_payload = _expected_stored_work_payload(
        account_id=account_id,
        stream_id=_STREAM_ID,
        transport_kind=frame.origin.transport,
        frame_id=_PLANNER_EXISTING_FRAME_ID,
        value=value,
        status="held",
        ready_revision=None,
        ready_ordinal=None,
        created_revision=1,
    )
    assert len(held_payload) <= hard_limit
    selected_held = replace(
        held,
        value=value,
        canonical_size=len(held_payload),
    )

    if extra_byte:
        with pytest.raises(ValueError):
            plan_frame_materialization(
                account_id=account_id,
                stream_id=_STREAM_ID,
                frame=frame,
                aggregates=(aggregate,),
                work=(selected_held,),
                revision=revision,
                limits=MaterializerLimits(),
            )
        return

    plan = plan_frame_materialization(
        account_id=account_id,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(selected_held,),
        revision=revision,
        limits=MaterializerLimits(),
    )

    assert plan is not None
    assert plan.work_releases == ((value, _expected_event_work_plaintext(value), 1),)


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
        canonical_size=len(
            _expected_stored_work_payload(
                account_id=_PLANNER_ACCOUNT_ID,
                stream_id=_STREAM_ID,
                transport_kind=frame.origin.transport,
                frame_id=_PLANNER_EXISTING_FRAME_ID,
                value=unrelated_value,
                status="held",
                ready_revision=None,
                ready_ordinal=None,
                created_revision=1,
            )
        ),
    )

    plan = plan_frame_materialization(
        account_id=_PLANNER_ACCOUNT_ID,
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
            canonical_size=len(
                _expected_stored_work_payload(
                    account_id=_PLANNER_ACCOUNT_ID,
                    stream_id=_STREAM_ID,
                    transport_kind=frame.origin.transport,
                    frame_id=_PLANNER_EXISTING_FRAME_ID,
                    value=value,
                    status="held",
                    ready_revision=None,
                    ready_ordinal=None,
                    created_revision=1,
                )
            ),
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
        account_id=_PLANNER_ACCOUNT_ID,
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
    loss_payload_size = len(
        _expected_planned_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            frame=frame,
            value=expected_loss,
            ordinal=0,
            revision=2,
        )
    )
    release_payload_size = len(
        _expected_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            stream_id=_STREAM_ID,
            transport_kind=frame.origin.transport,
            frame_id=_PLANNER_EXISTING_FRAME_ID,
            value=selected_held.value,
            status="ready",
            ready_revision=2,
            ready_ordinal=1,
            created_revision=1,
        )
    )
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
            - release_payload_size
            - loss_payload_size
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
            sum(item.canonical_size for item in exact_work[1:])
            + release_payload_size
            + loss_payload_size
            == hard.max_total_work_canonical_bytes
        )
        assert (
            sum(item.canonical_size for item in one_over_work[1:])
            + release_payload_size
            + loss_payload_size
            == hard.max_total_work_canonical_bytes + 1
        )
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
        account_id=_PLANNER_ACCOUNT_ID,
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
            account_id=_PLANNER_ACCOUNT_ID,
            stream_id=_STREAM_ID,
            frame=frame,
            aggregates=(aggregate,),
            work=one_over_work,
            revision=2,
            limits=limits,
        )
        is None
    )


@pytest.mark.parametrize(
    ("case", "successor_count", "global_count"),
    [
        pytest.param("pre-capacity-exact", 9_997, 1, id="pre-capacity-exact"),
        pytest.param("pre-capacity-one-over", 9_998, 1, id="pre-capacity-one-over"),
        pytest.param("replacement-one-over", 1, 9_998, id="replacement-one-over"),
    ],
)
def test_materializer_retirement_immutable_addition_count_boundary(
    monkeypatch: pytest.MonkeyPatch,
    case: str,
    successor_count: int,
    global_count: int,
) -> None:
    room_id = "!planner-retire:example.org"
    successor_payload = b'{"content":{"body":"successor"},"type":"m.room.message"}'
    global_payload = b'{"content":{"generation":2},"type":"m.push_rules"}'
    successor_json = (successor_payload,) * successor_count
    global_json = (global_payload,) * global_count
    frame, aggregate, held = _retirement_capacity_planner_case(successor_json)
    frame = replace(frame, global_account_data_json=global_json)
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    successor_continuity = RoomContinuity(
        room_id,
        1,
        "leave",
        None,
        None,
        None,
    )
    assert len(proposal.room_proposals) == 1
    room = proposal.room_proposals[0]
    assert room.before == aggregate.continuity
    assert room.after == successor_continuity
    assert room.hydration is None
    assert room.recovery is None
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
    assert tuple(
        (
            descriptor.kind,
            descriptor.room_id,
            descriptor.source_json,
            descriptor.provenance,
            descriptor.descriptor_key,
            descriptor.route,
        )
        for descriptor in proposal.descriptors
    ) == (
        *(
            (
                RecordKind.TIMELINE,
                room_id,
                successor_payload,
                TimelineEventProvenance.LIVE,
                f"frame:{_FRAME_ID}:{index}",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            )
            for index in range(successor_count)
        ),
        *(
            (
                RecordKind.GLOBAL_ACCOUNT_DATA,
                None,
                global_payload,
                None,
                f"frame:{_FRAME_ID}:{successor_count + index}",
                DescriptorRoute.READY,
            )
            for index in range(global_count)
        ),
    )
    old_loss, lifecycle, successors, globals_, capacity_loss = (
        _expected_retirement_capacity_records(successor_json, global_json)
    )
    old_loss_plaintext = _expected_loss_work_plaintext(old_loss)
    lifecycle_plaintext = _expected_event_work_plaintext(lifecycle)
    successor_plaintexts = tuple(
        _expected_event_work_plaintext(record) for record in successors
    )
    global_plaintexts = tuple(
        _expected_event_work_plaintext(record) for record in globals_
    )
    capacity_loss_plaintext = _expected_loss_work_plaintext(capacity_loss)
    hard = MaterializerLimits()
    assert hard.max_held_work_count == 10_000
    pre_capacity_count = 2 + len(successors) + len(globals_)
    replacement_count = 3 + len(globals_)
    if case == "pre-capacity-exact":
        assert pre_capacity_count == hard.max_held_work_count
        assert replacement_count == 4
    elif case == "pre-capacity-one-over":
        assert pre_capacity_count == hard.max_held_work_count + 1
        assert replacement_count == 4
    else:
        assert case == "replacement-one-over"
        assert pre_capacity_count == hard.max_held_work_count + 1
        assert replacement_count == hard.max_held_work_count + 1
    caller_limits = replace(
        hard,
        max_held_work_count=1,
        max_held_work_canonical_bytes=1,
        max_ready_work_count=1,
        max_ready_work_canonical_bytes=1,
        max_total_work_count=1,
        max_total_work_canonical_bytes=1,
    )

    if case == "replacement-one-over":
        planner_module = importlib.import_module("nio.store._sync_journal_plan")
        real_canonical_work_plaintext = getattr(
            planner_module,
            "_canonical_work_plaintext",
        )
        encoded_work_ids: list[str] = []

        def observe_canonical_work_plaintext(
            kind: str,
            value: EventRecord | LossRecord,
        ) -> bytes:
            encoded_work_ids.append(
                value.record_id if type(value) is EventRecord else value.loss_id
            )
            return real_canonical_work_plaintext(kind, value)

        with monkeypatch.context() as guard:
            guard.setattr(
                planner_module,
                "_canonical_work_plaintext",
                observe_canonical_work_plaintext,
            )
            with pytest.raises(
                ValueError,
                match="selected frame Work exceeds the hard addition envelope",
            ):
                plan_frame_materialization(
                    account_id=_PLANNER_ACCOUNT_ID,
                    stream_id=_STREAM_ID,
                    frame=frame,
                    aggregates=(aggregate,),
                    work=(held,),
                    revision=2,
                    limits=caller_limits,
                )
        assert capacity_loss.loss_id in encoded_work_ids
        return

    plan = plan_frame_materialization(
        account_id=_PLANNER_ACCOUNT_ID,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(held,),
        revision=2,
        limits=caller_limits,
    )

    assert plan is not None
    assert plan.work_releases == (
        (held.value, _expected_event_work_plaintext(held.value), 1),
    )
    if case == "pre-capacity-exact":
        assert plan.room_values == (
            RoomAggregateValue(
                successor_continuity,
                2 + successor_count,
                2,
                None,
            ),
        )
        assert plan.work_inserts == (
            (old_loss, old_loss_plaintext, 0),
            (lifecycle, lifecycle_plaintext, 2),
            *(
                (record, plaintext, index + 3)
                for index, (record, plaintext) in enumerate(
                    zip(successors, successor_plaintexts, strict=True)
                )
            ),
            *(
                (record, plaintext, successor_count + index + 3)
                for index, (record, plaintext) in enumerate(
                    zip(globals_, global_plaintexts, strict=True)
                )
            ),
        )
    else:
        assert case == "pre-capacity-one-over"
        assert plan.room_values == (
            RoomAggregateValue(successor_continuity, 2, 2, None),
        )
        assert plan.work_inserts == (
            (old_loss, old_loss_plaintext, 0),
            (lifecycle, lifecycle_plaintext, 2),
            (capacity_loss, capacity_loss_plaintext, 3),
            *(
                (record, plaintext, index + 4)
                for index, (record, plaintext) in enumerate(
                    zip(globals_, global_plaintexts, strict=True)
                )
            ),
        )
    assert plan.crypto_deferred is False


@pytest.mark.parametrize(
    ("room_body_length", "tuned_padding", "excess"),
    [
        pytest.param(257_677, 47, 0, id="exact"),
        pytest.param(257_677, 50, 4, id="over"),
    ],
)
def test_materializer_retirement_immutable_addition_byte_boundary(
    room_body_length: int,
    tuned_padding: int,
    excess: int,
) -> None:
    room_id = f"!{'r' * room_body_length}:example.org"
    ordinary_successor = b'{"content":{"body":"successor"},"type":"m.room.message"}'
    tuned_successor = (
        b'{"content":{"body":"'
        + (b"x" * tuned_padding)
        + b'"},"type":"m.room.message"}'
    )
    successor_json = (ordinary_successor,) * 62 + (tuned_successor,)
    global_json = (b'{"content":{"generation":2},"type":"m.push_rules"}',)
    frame, aggregate, held = _retirement_capacity_planner_case(
        successor_json,
        room_id=room_id,
    )
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    assert len(proposal.room_proposals) == 1
    room = proposal.room_proposals[0]
    successor_continuity = RoomContinuity(
        room_id,
        1,
        "leave",
        None,
        None,
        None,
    )
    assert room.before == aggregate.continuity
    assert room.after == successor_continuity
    assert room.hydration is None
    assert room.recovery is None
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
    assert tuple(
        (
            descriptor.kind,
            descriptor.room_id,
            descriptor.source_json,
            descriptor.provenance,
            descriptor.descriptor_key,
            descriptor.route,
        )
        for descriptor in proposal.descriptors
    ) == (
        *(
            (
                RecordKind.TIMELINE,
                room_id,
                source_json,
                TimelineEventProvenance.LIVE,
                f"frame:{_FRAME_ID}:{index}",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            )
            for index, source_json in enumerate(successor_json)
        ),
        (
            RecordKind.GLOBAL_ACCOUNT_DATA,
            None,
            global_json[0],
            None,
            f"frame:{_FRAME_ID}:63",
            DescriptorRoute.READY,
        ),
    )
    old_loss, lifecycle, successors, globals_, capacity_loss = (
        _expected_retirement_capacity_records(
            successor_json,
            global_json,
            room_id=room_id,
        )
    )
    old_loss_plaintext = _expected_loss_work_plaintext(old_loss)
    lifecycle_plaintext = _expected_event_work_plaintext(lifecycle)
    successor_plaintexts = tuple(
        _expected_event_work_plaintext(record) for record in successors
    )
    global_plaintexts = tuple(
        _expected_event_work_plaintext(record) for record in globals_
    )
    capacity_loss_plaintext = _expected_loss_work_plaintext(capacity_loss)
    normal_work = (
        (old_loss, 0),
        (lifecycle, 2),
        *((record, index + 3) for index, record in enumerate(successors)),
        (globals_[0], 66),
    )
    replacement_work = (
        (old_loss, 0),
        (lifecycle, 2),
        (capacity_loss, 3),
        (globals_[0], 4),
    )
    normal_payloads = tuple(
        _expected_planned_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            frame=frame,
            value=value,
            ordinal=ordinal,
            revision=2,
        )
        for value, ordinal in normal_work
    )
    replacement_payloads = tuple(
        _expected_planned_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            frame=frame,
            value=value,
            ordinal=ordinal,
            revision=2,
        )
        for value, ordinal in replacement_work
    )
    hard = MaterializerLimits()
    assert hard.max_record_canonical_bytes == 1 * 1024 * 1024
    assert hard.max_held_work_canonical_bytes == 32 * 1024 * 1024
    assert len(normal_payloads) == 66
    assert all(
        len(payload) <= hard.max_record_canonical_bytes for payload in normal_payloads
    )
    assert sum(map(len, normal_payloads)) == (
        hard.max_held_work_canonical_bytes + excess
    )
    assert sum(map(len, replacement_payloads)) < (hard.max_held_work_canonical_bytes)
    caller_limits = replace(
        hard,
        max_held_work_count=1,
        max_held_work_canonical_bytes=1,
        max_ready_work_count=1,
        max_ready_work_canonical_bytes=1,
        max_total_work_count=1,
        max_total_work_canonical_bytes=1,
    )

    plan = plan_frame_materialization(
        account_id=_PLANNER_ACCOUNT_ID,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=(held,),
        revision=2,
        limits=caller_limits,
    )

    assert plan is not None
    assert plan.work_releases == (
        (held.value, _expected_event_work_plaintext(held.value), 1),
    )
    if excess == 0:
        assert plan.room_values == (
            RoomAggregateValue(
                successor_continuity,
                65,
                2,
                None,
            ),
        )
        assert plan.work_inserts == (
            (old_loss, old_loss_plaintext, 0),
            (lifecycle, lifecycle_plaintext, 2),
            *(
                (record, plaintext, index + 3)
                for index, (record, plaintext) in enumerate(
                    zip(successors, successor_plaintexts, strict=True)
                )
            ),
            (globals_[0], global_plaintexts[0], 66),
        )
    else:
        assert excess == 4
        assert plan.room_values == (
            RoomAggregateValue(successor_continuity, 2, 2, None),
        )
        assert plan.work_inserts == (
            (old_loss, old_loss_plaintext, 0),
            (lifecycle, lifecycle_plaintext, 2),
            (capacity_loss, capacity_loss_plaintext, 3),
            (globals_[0], global_plaintexts[0], 4),
        )
    assert plan.crypto_deferred is False


@pytest.mark.parametrize("excess", [0, 1], ids=("exact", "one-over"))
def test_materializer_retirement_replacement_obeys_immutable_total_byte_boundary(
    excess: int,
) -> None:
    room_id = "!planner-retire:example.org"
    successor_json = (
        b'{"content":{"body":"' + (b"x" * 512) + b'"},"type":"m.room.message"}',
    )
    global_json = (b'{"content":{"generation":2},"type":"m.push_rules"}',)
    frame, aggregate, held = _retirement_capacity_planner_case(successor_json)
    proposal = reduce_staged_frame(
        _STREAM_ID,
        frame.frame_id,
        frame,
        (aggregate.continuity,),
    )
    assert len(proposal.room_proposals) == 1
    room = proposal.room_proposals[0]
    successor_continuity = RoomContinuity(
        room_id,
        1,
        "leave",
        None,
        None,
        None,
    )
    assert room.before == aggregate.continuity
    assert room.after == successor_continuity
    assert room.hydration is None
    assert room.recovery is None
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
    assert tuple(
        (
            descriptor.kind,
            descriptor.room_id,
            descriptor.source_json,
            descriptor.provenance,
            descriptor.descriptor_key,
            descriptor.route,
        )
        for descriptor in proposal.descriptors
    ) == (
        (
            RecordKind.TIMELINE,
            room_id,
            successor_json[0],
            TimelineEventProvenance.LIVE,
            f"frame:{_FRAME_ID}:0",
            DescriptorRoute.HOLD_FOR_RETIREMENT,
        ),
        (
            RecordKind.GLOBAL_ACCOUNT_DATA,
            None,
            global_json[0],
            None,
            f"frame:{_FRAME_ID}:1",
            DescriptorRoute.READY,
        ),
    )
    old_loss, lifecycle, successors, globals_, capacity_loss = (
        _expected_retirement_capacity_records(
            successor_json,
            global_json,
            capacity_reason=LossReason.OVERSIZED_EVENT,
        )
    )
    old_loss_plaintext = _expected_loss_work_plaintext(old_loss)
    lifecycle_plaintext = _expected_event_work_plaintext(lifecycle)
    global_plaintext = _expected_event_work_plaintext(globals_[0])
    capacity_loss_plaintext = _expected_loss_work_plaintext(capacity_loss)
    replacement_work = (
        (old_loss, 0),
        (lifecycle, 2),
        (capacity_loss, 3),
        (globals_[0], 4),
    )
    replacement_payloads = tuple(
        _expected_planned_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            frame=frame,
            value=value,
            ordinal=ordinal,
            revision=2,
        )
        for value, ordinal in replacement_work
    )
    release_payload_size = len(
        _expected_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            stream_id=_STREAM_ID,
            transport_kind=frame.origin.transport,
            frame_id=_PLANNER_EXISTING_FRAME_ID,
            value=held.value,
            status="ready",
            ready_revision=2,
            ready_ordinal=1,
            created_revision=1,
        )
    )
    successor_payload_size = len(
        _expected_planned_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            frame=frame,
            value=successors[0],
            ordinal=3,
            revision=2,
        )
    )
    max_record_bytes = max(
        release_payload_size,
        *(map(len, replacement_payloads)),
    )
    assert successor_payload_size > max_record_bytes
    hard = MaterializerLimits()
    assert hard.max_record_canonical_bytes == 1 * 1024 * 1024
    assert hard.max_total_work_canonical_bytes == 64 * 1024 * 1024
    exact_ready_bytes = (
        hard.max_total_work_canonical_bytes
        - release_payload_size
        - sum(map(len, replacement_payloads))
    )
    ready_sizes: list[int] = []
    remaining = exact_ready_bytes
    while remaining:
        size = min(hard.max_record_canonical_bytes, remaining)
        ready_sizes.append(size)
        remaining -= size
    assert len(ready_sizes) == 64
    assert ready_sizes[-1] < hard.max_record_canonical_bytes
    ready_sizes[-1] += excess
    assert all(1 <= size <= hard.max_record_canonical_bytes for size in ready_sizes)
    ready = tuple(
        _planner_ready_work(frame, index, canonical_size=size)
        for index, size in enumerate(ready_sizes)
    )
    work = (held, *ready)
    assert all(type(item.value) is EventRecord for item in work)
    work_ids = tuple(
        item.value.record_id for item in work if type(item.value) is EventRecord
    )
    assert len(set(work_ids)) == len(work)
    assert (
        sum(item.canonical_size for item in ready)
        + release_payload_size
        + sum(map(len, replacement_payloads))
        == hard.max_total_work_canonical_bytes + excess
    )
    caller_limits = replace(
        hard,
        max_record_canonical_bytes=max_record_bytes,
        max_held_work_count=1,
        max_held_work_canonical_bytes=1,
        max_ready_work_count=1,
        max_ready_work_canonical_bytes=1,
        max_total_work_count=1,
        max_total_work_canonical_bytes=1,
    )

    plan = plan_frame_materialization(
        account_id=_PLANNER_ACCOUNT_ID,
        stream_id=_STREAM_ID,
        frame=frame,
        aggregates=(aggregate,),
        work=work,
        revision=2,
        limits=caller_limits,
    )

    if excess:
        assert plan is None
        return
    assert plan is not None
    assert plan.room_values == (RoomAggregateValue(successor_continuity, 2, 2, None),)
    assert plan.work_inserts == (
        (old_loss, old_loss_plaintext, 0),
        (lifecycle, lifecycle_plaintext, 2),
        (capacity_loss, capacity_loss_plaintext, 3),
        (globals_[0], global_plaintext, 4),
    )
    assert plan.work_releases == (
        (held.value, _expected_event_work_plaintext(held.value), 1),
    )
    assert successors[0].record_id not in {
        item.record_id
        for item, _plaintext, _ordinal in plan.work_inserts
        if type(item) is EventRecord
    }
    assert plan.crypto_deferred is False


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
        account_id=_PLANNER_ACCOUNT_ID,
        stream_id=_STREAM_ID,
        frame=exact,
        aggregates=(aggregate,),
        work=(selected_held,),
        revision=2,
        limits=limits,
    )

    assert plan is not None
    assert len(plan.work_inserts) == hard.max_held_work_count
    one_over = replace(
        base,
        global_account_data_json=(b"{}",) * hard.max_held_work_count,
    )
    with pytest.raises(ValueError, match="hard addition envelope"):
        plan_frame_materialization(
            account_id=_PLANNER_ACCOUNT_ID,
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
    loss_payload_size = len(
        _expected_planned_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            frame=frame,
            value=expected_loss,
            ordinal=0,
            revision=2,
        )
    )
    incoming_sizes = tuple(
        len(
            _expected_planned_stored_work_payload(
                account_id=_PLANNER_ACCOUNT_ID,
                frame=frame,
                value=record,
                ordinal=None if pending else index,
                revision=2,
            )
        )
        for index, record in enumerate(incoming)
    )
    assert incoming_sizes[0] > loss_payload_size
    assert incoming_sizes[1] <= loss_payload_size
    limits = replace(
        MaterializerLimits(),
        max_record_canonical_bytes=loss_payload_size,
        max_held_work_count=1,
        max_ready_work_count=1,
        max_ready_work_canonical_bytes=1,
        max_total_work_count=1,
        max_total_work_canonical_bytes=1,
    )

    plan = plan_frame_materialization(
        account_id=_PLANNER_ACCOUNT_ID,
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
            account_id=_PLANNER_ACCOUNT_ID,
            stream_id=_STREAM_ID,
            frame=frame,
            aggregates=(aggregate,),
            work=(selected_held,) if pending else (),
            revision=2,
            limits=replace(
                limits,
                max_record_canonical_bytes=loss_payload_size - 1,
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
    room_bytes = len(
        _expected_planned_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            frame=frame,
            value=records[0],
            ordinal=None,
            revision=2,
        )
    )
    global_bytes = len(
        _expected_planned_stored_work_payload(
            account_id=_PLANNER_ACCOUNT_ID,
            frame=frame,
            value=records[1],
            ordinal=0,
            revision=2,
        )
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
    assert (
        len(
            _expected_planned_stored_work_payload(
                account_id=_PLANNER_ACCOUNT_ID,
                frame=frame,
                value=loss,
                ordinal=0,
                revision=2,
            )
        )
        <= room_bytes
    )
    assert room_bytes < global_bytes

    with pytest.raises(ValueError, match="canonical byte limit"):
        plan_frame_materialization(
            account_id=_PLANNER_ACCOUNT_ID,
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
            account_id=_PLANNER_ACCOUNT_ID,
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
            account_id=_PLANNER_ACCOUNT_ID,
            stream_id=_STREAM_ID,
            frame=frame,
            aggregates=(),
            work=(
                AuthenticatedWork(
                    existing,
                    "held",
                    1,
                ),
            ),
            revision=1,
            limits=replace(MaterializerLimits(), max_held_work_count=1),
        )


def test_blocked_result_invariant_has_a_neutral_message() -> None:
    with pytest.raises(ValueError, match="blocked materialization has only a frame"):
        MaterializeResult(MaterializeStatus.BLOCKED, None, None)


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
    assert source.MAX_STORED_FRAME_PAYLOAD_BYTES == 24 * 1024 * 1024
    for package in (ingest, store):
        assert not hasattr(package, "MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES")
        assert not hasattr(package, "MAX_STORED_FRAME_PAYLOAD_BYTES")


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
        consumer_generation=_CONSUMER_GENERATION,
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
        stored_frame = replace(frame, staged_revision=owner_before.revision + 1)
        stored_payload = _expected_plaintext_materializer_envelope(
            row_kind="frame",
            owner=owner_before,
            clear_fields=(
                ("frame_id", str(stored_frame.frame_id)),
                ("source_epoch", oversized_request.source_epoch),
                ("request_id", oversized_request.request_id),
                ("staged_revision", stored_frame.staged_revision),
            ),
            value=_frame_envelope(stored_frame),
        )
        assert len(stored_payload) > _FRAME_ENVELOPE_LIMIT

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
    account_id: str = _DISCOVERY_ACCOUNT_ID,
) -> ClassicSource | SlidingSource:
    config = _discovery_config(transport)
    if transport is TransportKind.CLASSIC:
        assert type(config) is ClassicSourceConfig
        return ClassicSource(stream_id, config, account_id)
    assert type(config) is SlidingSourceConfig
    return SlidingSource(stream_id, config, account_id)


def _discovery_body(
    request: NetworkRequest,
    sequence: int,
    *,
    crypto: bool = False,
    nonempty: bool = False,
    room_present: bool = False,
    room_nonempty: bool = False,
    room_prev_batch: str | None = None,
    room_padding_bytes: int = 0,
    room_ephemeral: bool = False,
    room_feature: str | None = None,
    ephemeral_only: bool = False,
    presence_only: bool = False,
    room_membership: str = "join",
    global_ready_count: int | None = None,
    padding_bytes: int = 0,
    to_device_json: tuple[bytes, ...] = (),
    device_list_delta_json: bytes = b'{"changed":[],"left":[]}',
    one_time_key_counts_json: bytes = b"{}",
    unused_fallback_key_types_json: bytes = b"null",
) -> bytes:
    if room_membership not in {"join", "invite", "knock", "leave"}:
        raise ValueError("discovery room membership is invalid")
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
    room_content = {"body": "held", "msgtype": "m.text"}
    if room_padding_bytes:
        room_content["padding"] = "x" * room_padding_bytes
    room_event = {
        "content": room_content,
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
        elif presence_only:
            body["presence"] = {
                "events": [
                    {
                        "content": {"presence": "online"},
                        "sender": "@friend:example.org",
                        "type": "m.presence",
                    }
                ]
            }
        elif nonempty:
            body["account_data"] = {
                "events": [{"content": {"enabled": True}, "type": "m.push_rules"}]
            }
        if room_present and not room_nonempty and room_feature is None:
            body["rooms"] = {room_membership: {"!unsupported:example.org": {}}}
        elif room_nonempty or room_feature is not None:
            event = (
                {
                    "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                    "type": "m.room.encrypted",
                }
                if room_feature == "encrypted"
                else room_event
            )
            timeline: dict[str, object] = {"events": [event]}
            if room_prev_batch is not None:
                timeline["prev_batch"] = room_prev_batch
            room: dict[str, object] = (
                {"timeline": timeline}
                if room_nonempty or room_feature in {"encrypted", "history"}
                else {}
            )
            if room_feature == "history":
                room["timeline"] = {
                    "events": [event],
                    "limited": True,
                    "prev_batch": "history",
                }
            elif room_feature == "state":
                room["state"] = {
                    "events": [
                        {
                            "content": {"name": "Room"},
                            "state_key": "",
                            "type": "m.room.name",
                        }
                    ]
                }
            elif room_feature == "account-data":
                room["account_data"] = {
                    "events": [{"content": {"tags": {}}, "type": "m.tag"}]
                }
            if room_ephemeral:
                room["ephemeral"] = {"events": [ephemeral_event]}
            body["rooms"] = {
                room_membership: {
                    "!unsupported:example.org": room,
                }
            }
        if to_device_json:
            body["to_device"] = {
                "events": [json.loads(payload) for payload in to_device_json]
            }
        if device_list_delta_json != b'{"changed":[],"left":[]}':
            body["device_lists"] = json.loads(device_list_delta_json)
        if one_time_key_counts_json != b"{}":
            body["device_one_time_keys_count"] = json.loads(one_time_key_counts_json)
        if unused_fallback_key_types_json != b"null":
            body["device_unused_fallback_key_types"] = json.loads(
                unused_fallback_key_types_json
            )
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
    if to_device_json:
        extensions["to_device"] = {
            "events": [json.loads(payload) for payload in to_device_json],
            "next_batch": f"td{sequence}",
        }
    if (
        device_list_delta_json != b'{"changed":[],"left":[]}'
        or one_time_key_counts_json != b"{}"
        or unused_fallback_key_types_json != b"null"
    ):
        e2ee = {
            "device_lists": json.loads(device_list_delta_json),
            "device_one_time_keys_count": json.loads(one_time_key_counts_json),
        }
        if unused_fallback_key_types_json != b"null":
            e2ee["device_unused_fallback_key_types"] = json.loads(
                unused_fallback_key_types_json
            )
        extensions["e2ee"] = e2ee
    if extensions:
        body["extensions"] = extensions
    return canonical_json(body)


def _open_discovery_journal(
    store_path: Path,
    transport: TransportKind,
    *,
    statements: list[str] | None = None,
    sqlite_busy_timeout_ms: int = 2_000,
    account_id: str = _DISCOVERY_ACCOUNT_ID,
):
    return open_ingestion_store(
        store_path,
        account_id=account_id,
        device_id=_DISCOVERY_DEVICE_ID,
        consumer_generation=_CONSUMER_GENERATION,
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
    room_prev_batch: str | None = None,
    room_padding_bytes: int = 0,
    room_ephemeral: bool = False,
    room_feature: str | None = None,
    ephemeral_only: bool = False,
    presence_only: bool = False,
    room_membership: str = "join",
    global_ready_count: int | None = None,
    padding_bytes: int = 0,
    account_id: str = _DISCOVERY_ACCOUNT_ID,
    to_device_json: tuple[bytes, ...] = (),
    device_list_delta_json: bytes = b'{"changed":[],"left":[]}',
    one_time_key_counts_json: bytes = b"{}",
    unused_fallback_key_types_json: bytes = b"null",
) -> tuple[StagedFrame, SyncFrame]:
    owner = journal.load_owner()
    prior = journal.load_source()
    adapter = _discovery_adapter(owner.stream_id, transport, account_id)
    request = adapter.plan_request(prior, prior.next_request_id)
    assert request is not None
    body = _discovery_body(
        request,
        sequence,
        crypto=crypto,
        nonempty=nonempty,
        room_present=room_present,
        room_nonempty=room_nonempty,
        room_prev_batch=room_prev_batch,
        room_padding_bytes=room_padding_bytes,
        room_ephemeral=room_ephemeral,
        room_feature=room_feature,
        ephemeral_only=ephemeral_only,
        presence_only=presence_only,
        room_membership=room_membership,
        global_ready_count=global_ready_count,
        padding_bytes=padding_bytes,
        to_device_json=to_device_json,
        device_list_delta_json=device_list_delta_json,
        one_time_key_counts_json=one_time_key_counts_json,
        unused_fallback_key_types_json=unused_fallback_key_types_json,
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
        source=successor,
        frame=staged,
    )
    return replace(staged, staged_revision=committed.revision), normalized.frame


def _stage_discovery_rooms_frame(
    journal: SqliteIngestionJournal,
    transport: TransportKind,
    sequence: int,
    *,
    rooms: tuple[tuple[str, str, str | tuple[str, ...]], ...],
    crypto: bool = False,
    global_tail: bool = False,
    global_padding_bytes: int = 0,
) -> tuple[StagedFrame, SyncFrame]:
    owner = journal.load_owner()
    prior = journal.load_source()
    adapter = _discovery_adapter(owner.stream_id, transport)
    request = adapter.plan_request(prior, prior.next_request_id)
    assert request is not None

    def room_event(body: str) -> dict[str, object]:
        return {
            "content": {"body": body, "msgtype": "m.text"},
            "type": "m.room.message",
        }

    global_event = {
        "content": {
            "generation": sequence,
            "index": 0,
            "padding": "x" * global_padding_bytes,
        },
        "type": "m.push_rules",
    }
    presence_event = {
        "content": {"presence": "online"},
        "sender": "@friend:example.org",
        "type": "m.presence",
    }
    if transport is TransportKind.CLASSIC:
        room_sections: dict[str, dict[str, object]] = {}
        for room_id, membership, event_bodies in rooms:
            bodies = (event_bodies,) if type(event_bodies) is str else event_bodies
            room_sections.setdefault(membership, {})[room_id] = {
                "timeline": {"events": [room_event(body) for body in bodies]}
            }
        response: dict[str, object] = {
            "next_batch": f"s{sequence}",
            "rooms": room_sections,
        }
        if crypto:
            response["to_device"] = {
                "events": [
                    {
                        "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                        "type": "m.room_key",
                    }
                ]
            }
        if global_tail:
            response["account_data"] = {"events": [global_event]}
            response["presence"] = {"events": [presence_event]}
    else:
        assert request.body is not None
        request_body = json.loads(request.body)
        sliding_rooms: dict[str, object] = {}
        for room_id, membership, event_bodies in rooms:
            bodies = (event_bodies,) if type(event_bodies) is str else event_bodies
            sliding_rooms[room_id] = {
                "membership": membership,
                "timeline": [room_event(body) for body in bodies],
            }
        response = {
            "lists": {RESERVED_ALL_ROOMS_LIST: {"count": len(rooms)}},
            "pos": f"p{sequence}",
            "rooms": sliding_rooms,
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
        if global_tail:
            extensions["account_data"] = {"global": [global_event]}
            extensions["presence"] = {"events": [presence_event]}
        if extensions:
            response["extensions"] = extensions

    normalized = adapter.normalize(
        request,
        NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            canonical_json(response),
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
            "payload, payload_sha256, room_materialized_revision, "
            "drain_header_sha256, callbacks_claimed_revision "
            "FROM NioIngestFrame "
            "WHERE account_id = ? AND frame_id = ?",
            (journal.account_id, str(frame_id)),
        ).fetchone()
    return None if row is None else tuple(row)


def _canonical_expected_drain_header(
    journal: SqliteIngestionJournal,
    row: tuple[object, ...],
    room_materialized_revision: int | None,
) -> bytes:
    owner = journal.load_owner()
    return _canonical_internal(
        {
            "schema_version": 1,
            "row_kind": "frame",
            "account_id": owner.account_id,
            "stream_id": str(owner.stream_id),
            "transport_kind": owner.transport_kind.value,
            "frame_id": row[0],
            "source_epoch": row[1],
            "request_id": row[2],
            "staged_revision": row[3],
            "payload_sha256": base64.b64encode(row[5]).decode("ascii"),
            "payload_length": len(row[4]),
            "room_materialized_revision": room_materialized_revision,
            "callbacks_claimed_revision": row[8],
        }
    )


def _materialize(
    journal: SqliteIngestionJournal,
    *,
    limits: MaterializerLimits | None = None,
) -> MaterializeResult:
    return journal.materialize_oldest_frame(
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


def _delivery_frontier(
    journal: SqliteIngestionJournal,
) -> tuple[object, ...]:
    with journal._owner.read():
        row = journal._execute(
            "SELECT delivery_next_sequence, delivery_acknowledged_sha256, "
            "delivery_outstanding_work_id, delivery_outstanding_ready_revision, "
            "delivery_outstanding_ready_ordinal, delivery_outstanding_batch_sha256 "
            "FROM NioIngestMeta WHERE account_id = ?",
            (journal.account_id,),
        ).fetchone()
    assert row is not None
    return tuple(row)


def _apply_discovery_hydration(journal: SqliteIngestionJournal) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    pending = journal.load_pending_hydrations(limit=2)
    assert len(pending) == 1
    result = normalize_hydration_response(
        pending[0],
        own_user_id=_DISCOVERY_ACCOUNT_ID,
        response_body=(
            b'[{"content":{"membership":"join"},"event_id":"$discovery-member",'
            b'"state_key":"@discovery:example.org","type":"m.room.member"}]'
        ),
    )
    assert journal.apply_hydration_result(result=result) is not None
    hydrated = _aggregate_rows(journal)
    assert len(hydrated) == 1 and hydrated[0][2] is None
    assert _decode_aggregate(journal, hydrated[0])[1].pending_hydration is None
    assert _work_rows(journal) == ()


def _install_discovery_hydration_baseline(
    journal: SqliteIngestionJournal,
) -> None:
    _stage_discovery_frame(journal, TransportKind.CLASSIC, 1, room_present=True)
    assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
    _apply_discovery_hydration(journal)


def test_hydration_release_retains_authenticated_preparation_bytes(
    tmp_path: Path,
) -> None:
    from nio.ingest.hydration import normalize_hydration_response

    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        staged, _ = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=True,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        held = next(row for row in _work_rows(journal) if row[2] == "held")
        _, value = _decode_event_work(journal, held)
        visible = json.loads(value.source_json)
        metadata = journal_plan_module._PreparedWorkMetadata(
            value.record_id,
            _PreparationPhase.SOURCE,
            visible["type"],
            _DecryptionDisposition.NONE,
            None,
            None,
            (
                _CallbackRoute.EVENT
                if value.kind in (RecordKind.STATE, RecordKind.TIMELINE)
                else _CallbackRoute.ROOM_ACCOUNT_DATA
            ),
        )
        plaintext = journal_plan_module._canonical_work_plaintext(
            "event", value, metadata
        )
        sealed = _sealed_work_values(
            journal,
            work_id=str(held[0]),
            kind=str(held[1]),
            status=str(held[2]),
            frame_id=str(held[3]),
            room_id=held[4],
            membership_epoch=held[5],
            room_sequence=held[6],
            ready_revision=held[7],
            ready_ordinal=held[8],
            created_revision=int(held[9]),
            plaintext=plaintext,
        )
        with journal._owner.journal_write():
            updated = journal._execute(
                "UPDATE NioIngestWork SET payload = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND work_id = ?",
                (sealed[-2], sealed[-1], journal.account_id, held[0]),
            )
            assert updated.rowcount == 1

        pending = journal.load_pending_hydrations(limit=1)
        assert len(pending) == 1
        result = normalize_hydration_response(
            pending[0],
            own_user_id=_DISCOVERY_ACCOUNT_ID,
            response_body=(
                b'[{"content":{"membership":"join"},'
                b'"event_id":"$prepared-hydration","state_key":'
                b'"@discovery:example.org","type":"m.room.member"}]'
            ),
        )

        assert journal.apply_hydration_result(result=result) is not None

        released = next(row for row in _work_rows(journal) if row[0] == value.record_id)
        assert released[2] == "ready"
        assert (released[3], released[9]) == (held[3], held[9])
        assert _decode_work(journal, released) == (plaintext, value)
    finally:
        bootstrap.close()


def test_materializer_public_call_internalizes_owner_cas(tmp_path: Path) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        staged, _ = _stage_discovery_frame(journal, TransportKind.CLASSIC, 1)
        owner_before = journal.load_owner()

        result = journal.materialize_oldest_frame(limits=MaterializerLimits())

        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            owner_before.revision + 1,
        )
        assert journal.load_owner().revision == owner_before.revision + 1
        assert _frame_storage_row(journal, staged.frame_id) is None
    finally:
        bootstrap.close()


def _aggregate_rows(journal: SqliteIngestionJournal) -> tuple[tuple[object, ...], ...]:
    with journal._owner.read():
        rows = journal._execute(
            "SELECT room_id, updated_revision, intent_kind, payload, "
            "payload_sha256 FROM NioIngestRoomAggregate "
            "WHERE account_id = ? ORDER BY room_id",
            (journal.account_id,),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _decode_aggregate(
    journal: SqliteIngestionJournal,
    row: tuple[object, ...],
) -> tuple[bytes, object]:
    payload = bytes(row[3])
    assert row[4] == hashlib.sha256(payload).digest()
    envelope = json.loads(payload)
    plaintext = _canonical_internal(envelope["value"])
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
            "payload, payload_sha256 FROM NioIngestWork "
            "WHERE account_id = ? ORDER BY ready_revision, ready_ordinal, work_id",
            (journal.account_id,),
        ).fetchall()
    return tuple(tuple(row) for row in rows)


def _decode_work(
    journal: SqliteIngestionJournal,
    row: tuple[object, ...],
) -> tuple[bytes, EventRecord | LossRecord]:
    payload = bytes(row[10])
    assert row[11] == hashlib.sha256(payload).digest()
    envelope = json.loads(payload)
    plaintext = _canonical_internal(envelope["value"])
    value = _rows()._work_value_from_plaintext(
        journal.load_owner().stream_id,
        row[0],
        row[1],
        plaintext,
    )
    assert type(value) in (EventRecord, LossRecord)
    return plaintext, value


def _decode_event_work(
    journal: SqliteIngestionJournal,
    row: tuple[object, ...],
) -> tuple[bytes, EventRecord]:
    plaintext, value = _decode_work(journal, row)
    assert type(value) is EventRecord
    return plaintext, value


def _insert_verified_event_work(
    journal: SqliteIngestionJournal,
    record: EventRecord,
    *,
    frame_id: UUID,
    ready_revision: int | None,
    ready_ordinal: int | None,
    created_revision: int,
    status: str = "ready",
) -> None:
    values = _verified_event_work_values(
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
            "created_revision, payload, payload_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            values,
        )
        assert inserted.rowcount == 1


def _verified_event_work_values(
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
    owner = journal.load_owner()
    payload = _canonical_internal(
        {
            "schema_version": 1,
            "row_kind": "work",
            "account_id": journal.account_id,
            "stream_id": str(owner.stream_id),
            "transport_kind": owner.transport_kind.value,
            **dict(
                zip(
                    (column[0] for column in _WORK_COLUMNS[1:-2]),
                    header_values,
                    strict=True,
                )
            ),
            "value": json.loads(plaintext),
        }
    )
    return (
        journal.account_id,
        *header_values,
        payload,
        hashlib.sha256(payload).digest(),
    )


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
        plaintext, aggregate = _decode_aggregate(journal, aggregate_rows[0])
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
        _, before = _decode_aggregate(journal, aggregate_rows[0])
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
            b'{"content":{"user_ids":["@friend:example.org"]},"type":"m.typing"}',
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
        _, after = _decode_aggregate(journal, aggregate_rows[0])
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
            assert _decode_event_work(journal, row) == (
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
        _, before = _decode_aggregate(journal, aggregate_rows[0])
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
        _, after = _decode_aggregate(journal, aggregate_rows[0])
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
            assert _decode_event_work(journal, row) == (
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
            raw_after[7]
            == hashlib.sha256(
                _canonical_expected_drain_header(journal, raw_after, revision)
            ).digest()
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
        _, before = _decode_aggregate(journal, aggregate_rows[0])
        assert type(before) is _values().RoomAggregateValue
        assert before.pending_hydration is not None
        selected_rows = tuple(row for row in _work_rows(journal) if row[2] == "held")
        assert len(selected_rows) == 2
        selected = tuple(
            sorted(
                ((row, *_decode_event_work(journal, row)) for row in selected_rows),
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
        _insert_verified_event_work(
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
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        selected_boundary_bytes = sum(
            len(bytes(item[0][10])) for item in selected
        ) + sum(
            len(
                _expected_planned_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    frame=normalized,
                    value=record,
                    ordinal=None,
                    revision=revision,
                )
            )
            for record in incoming
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
                selected_boundary_bytes + len(bytes(unrelated_before[10])) - 1
            )
            assert selected_boundary_bytes < capacity["max_held_work_canonical_bytes"]
            assert selected_boundary_bytes + len(bytes(unrelated_before[10])) == (
                capacity["max_held_work_canonical_bytes"] + 1
            )
        raw_before = _frame_storage_row(journal, staged.frame_id)
        assert raw_before is not None

        result = _materialize(
            journal,
            limits=replace(MaterializerLimits(), **capacity),
        )

        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            revision,
        )
        _, after = _decode_aggregate(journal, _aggregate_rows(journal)[0])
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
            assert _decode_work(journal, row) == (
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
            assert released[10] != old_row[10]
            assert released[11] != old_row[11]
            assert _decode_event_work(journal, released) == (
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
        _, first_aggregate = _decode_aggregate(journal, first_aggregate_rows[0])
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
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        held_boundary_bytes = sum(
            len(bytes(row[10])) for row in first_work if row[2] == "held"
        ) + len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=second_normalized,
                value=boundary_record,
                ordinal=None,
                revision=revision,
            )
        )
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
        aggregate_plaintext, aggregate = _decode_aggregate(journal, aggregate_rows[0])
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
            plaintext, record = _decode_event_work(journal, row)
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
                second_raw_after[7]
                == hashlib.sha256(
                    _canonical_expected_drain_header(journal, second_raw_after, 4)
                ).digest()
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
        _, first_aggregate = _decode_aggregate(journal, aggregate_rows[0])
        assert type(first_aggregate) is _values().RoomAggregateValue
        assert first_aggregate.next_room_sequence == 2
        assert first_aggregate.pending_hydration is not None
        held_rows = tuple(row for row in _work_rows(journal) if row[2] == "held")
        assert len(held_rows) == 2
        held_records = tuple(_decode_event_work(journal, row) for row in held_rows)
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
        _insert_verified_event_work(
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
        assert _decode_event_work(journal, unrelated_storage_before) == (
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

        revision = owner_before.revision + 1
        selected_held_boundary_bytes = sum(
            len(bytes(item[0][10])) for item in held_by_sequence
        ) + sum(
            len(
                _expected_planned_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    frame=second_normalized,
                    value=record,
                    ordinal=None,
                    revision=revision,
                )
            )
            for record in expected_incoming
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
                selected_held_boundary_bytes
                + len(bytes(unrelated_storage_before[10]))
                - 1
            )
            assert selected_held_boundary_bytes < (
                capacity_limits["max_held_work_canonical_bytes"]
            )
            assert selected_held_boundary_bytes + len(
                bytes(unrelated_storage_before[10])
            ) == (capacity_limits["max_held_work_canonical_bytes"] + 1)

        statements.clear()
        with monkeypatch.context() as guard:
            guard.setattr(type(journal._owner), "journal_write", count_writer)
            result = _materialize(
                journal,
                limits=replace(MaterializerLimits(), **capacity_limits),
            )

        assert revision == 4
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
        aggregate_plaintext, aggregate = _decode_aggregate(journal, aggregate_rows[0])
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
            plaintext, stored = _decode_work(journal, row)
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
            assert released[11] != old_row[11]
            assert _decode_event_work(journal, released) == (
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
                raw_after[7]
                == hashlib.sha256(
                    _canonical_expected_drain_header(journal, raw_after, revision)
                ).digest()
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
        _, aggregate = _decode_aggregate(journal, aggregate_rows[0])
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
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        loss_payload_size = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=expected_loss,
                ordinal=0,
                revision=revision,
            )
        )
        release_payload_sizes = tuple(
            len(
                _expected_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    transport_kind=normalized.origin.transport,
                    frame_id=UUID(str(row[3])),
                    value=_decode_event_work(journal, row)[1],
                    status="ready",
                    ready_revision=revision,
                    ready_ordinal=ordinal,
                    created_revision=int(row[9]),
                )
            )
            for ordinal, row in enumerate(held_before, 1)
        )
        ready_ordinal = 3
        record_payload_sizes: dict[str, int] = {}
        for record in records:
            ordinal = None if record.room_id is not None else ready_ordinal
            if ordinal is not None:
                ready_ordinal += 1
            record_payload_sizes[record.record_id] = len(
                _expected_planned_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    frame=normalized,
                    value=record,
                    ordinal=ordinal,
                    revision=revision,
                )
            )
        max_record_bytes = max(
            loss_payload_size,
            *release_payload_sizes,
            *(
                record_payload_sizes[record.record_id]
                for record in records
                if record is not oversized_global
            ),
        )
        assert record_payload_sizes[oversized_global.record_id] > max_record_bytes
        assert (
            len(held_before)
            + sum(descriptor.room_id is not None for descriptor in proposal.descriptors)
            == 4
        )
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
        _, first_aggregate = _decode_aggregate(journal, aggregate_rows[0])
        held_before = tuple(row for row in _work_rows(journal) if row[2] == "held")
        assert len(held_before) == 1
        held_plaintext, held_record = _decode_event_work(journal, held_before[0])

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
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        loss_payload_size = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=expected_loss,
                ordinal=0,
                revision=revision,
            )
        )
        release_payload_size = len(
            _expected_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                transport_kind=normalized.origin.transport,
                frame_id=UUID(str(held_before[0][3])),
                value=held_record,
                status="ready",
                ready_revision=revision,
                ready_ordinal=1,
                created_revision=int(held_before[0][9]),
            )
        )
        incoming_payload_size = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=incoming,
                ordinal=None,
                revision=revision,
            )
        )
        max_record_bytes = max(loss_payload_size, release_payload_size)
        assert max_record_bytes < incoming_payload_size

        result = _materialize(
            journal,
            limits=replace(
                MaterializerLimits(),
                max_record_canonical_bytes=max_record_bytes,
                max_held_work_count=1,
            ),
        )

        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            second.frame_id,
            revision,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        _, aggregate = _decode_aggregate(journal, aggregate_rows[0])
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
        assert _decode_work(journal, ready_rows[0]) == (
            loss_plaintext,
            expected_loss,
        )
        assert _decode_event_work(journal, ready_rows[1]) == (
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
        _, first_aggregate = _decode_aggregate(journal, aggregate_rows[0])
        first_work = _work_rows(journal)
        held_before = tuple(row for row in first_work if row[2] == "held")
        assert len(held_before) == 1
        held_row = held_before[0]
        held_plaintext, held_record = _decode_event_work(journal, held_row)
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
                raw_after[7]
                == hashlib.sha256(
                    _canonical_expected_drain_header(journal, raw_after, revision)
                ).digest()
            )
        else:
            assert raw_after is None

        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        assert aggregate_rows[0][:3] == (room.after.room_id, revision, None)
        aggregate_plaintext, aggregate = _decode_aggregate(journal, aggregate_rows[0])
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
            plaintext, record = _decode_work(journal, row)
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
        assert released[11] != held_row[11]
        assert _decode_work(journal, released)[0] == held_plaintext

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


def test_materializer_pending_hydration_release_revalidates_changed_held_row_at_writer(
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
        _, aggregate = _decode_aggregate(journal, aggregate_before[0])
        work_before = _work_rows(journal)
        held_before = tuple(row for row in work_before if row[2] == "held")
        assert len(held_before) == 1
        target = held_before[0]
        _, value = _decode_event_work(journal, target)

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
        changed_value = replace(
            value,
            source_json=b'{"content":{"raced":true},"type":"m.room.message"}',
        )
        changed_plaintext = _rows()._canonical_work_plaintext("event", changed_value)
        raced = False
        real_journal_write = type(journal._owner).journal_write

        @contextmanager
        def change_held_before_writer(owner: object) -> Iterator[None]:
            nonlocal raced
            assert owner is journal._owner
            assert not raced
            raced = True
            stored = _sealed_work_values(
                journal,
                work_id=str(target[0]),
                kind=str(target[1]),
                status=str(target[2]),
                frame_id=str(target[3]),
                room_id=target[4],
                membership_epoch=target[5],
                room_sequence=target[6],
                ready_revision=target[7],
                ready_ordinal=target[8],
                created_revision=int(target[9]),
                plaintext=changed_plaintext,
            )
            payload, digest = stored[-2:]
            assert payload != target[10]
            assert digest != target[11]
            with sqlite3.connect(journal.database_path) as connection:
                updated = connection.execute(
                    "UPDATE NioIngestWork SET payload = ?, "
                    "payload_sha256 = ? WHERE account_id = ? AND work_id = ?",
                    (
                        payload,
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
            change_held_before_writer,
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
        assert changed[11] != target[11]
        assert _decode_event_work(journal, changed) == (
            changed_plaintext,
            changed_value,
        )
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


def test_materializer_repeated_hydration_rejects_changed_aggregate_at_writer_boundary(
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
        _, value_before = _decode_aggregate(journal, aggregate_before[0])

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
        row = aggregate_before[0]
        changed_value = replace(
            value_before,
            next_room_sequence=value_before.next_room_sequence + 1,
        )
        changed_plaintext = _rows()._canonical_room_aggregate_plaintext(changed_value)
        payload = _expected_plaintext_materializer_envelope(
            row_kind="aggregate",
            owner=owner_before,
            clear_fields=(
                ("room_id", row[0]),
                ("updated_revision", row[1]),
                ("intent_kind", row[2]),
            ),
            value=json.loads(changed_plaintext),
        )
        digest = hashlib.sha256(payload).digest()
        real_journal_write = type(journal._owner).journal_write
        raced = False

        @contextmanager
        def change_aggregate_before_writer(owner: object) -> Iterator[None]:
            nonlocal raced
            assert owner is journal._owner
            assert not raced
            raced = True
            assert payload != row[3]
            assert digest != row[4]
            with sqlite3.connect(journal.database_path) as connection:
                updated = connection.execute(
                    "UPDATE NioIngestRoomAggregate SET payload = ?, "
                    "payload_sha256 = ? WHERE account_id = ? AND room_id = ?",
                    (payload, digest, journal.account_id, row[0]),
                )
                assert updated.rowcount == 1
            with real_journal_write(journal._owner):
                yield

        monkeypatch.setattr(
            type(journal._owner),
            "journal_write",
            change_aggregate_before_writer,
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
        assert aggregate_after[0][4] != aggregate_before[0][4]
        plaintext_after, value_after = _decode_aggregate(journal, aggregate_after[0])
        assert plaintext_after == changed_plaintext
        assert value_after == changed_value
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
            plaintext, record = _decode_event_work(journal, row)
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
        expected_records = _planned_global_event_records(journal, staged, normalized)
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        expected_payloads = tuple(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=record,
                ordinal=ordinal,
                revision=revision,
            )
            for ordinal, record in enumerate(expected_records)
        )
        exact_limit = max(map(len, expected_payloads))
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
        stored_rows = _work_rows(journal)
        assert tuple(_decode_event_work(journal, row)[1] for row in stored_rows) == (
            expected_records
        )
        assert tuple(bytes(row[10]) for row in stored_rows) == expected_payloads
        assert max(len(bytes(row[10])) for row in stored_rows) == exact_limit
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
        owner_before = journal.load_owner()
        assert (
            len(
                _expected_planned_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    frame=normalized,
                    value=expected,
                    ordinal=0,
                    revision=owner_before.revision + 1,
                )
            )
            > 128
        )
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
        decoded_work: list[str] = []
        incrementally_fetched: list[int] = []
        real_execute = journal._execute
        real_decode_work = journal_rows_module._decode_work_plaintext

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

        def trace_decode_work(
            stream_id: UUID,
            work_id: str,
            kind: str,
            plaintext: bytes,
        ) -> journal_rows_module._DecodedWork:
            decoded_work.append(work_id)
            return real_decode_work(stream_id, work_id, kind, plaintext)

        statements.clear()
        with monkeypatch.context() as guard:
            guard.setattr(journal, "_execute", trace_execute)
            guard.setattr(
                journal_rows_module,
                "_decode_work_plaintext",
                trace_decode_work,
            )
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
        assert sorted(decoded_work) == sorted(str(row[0]) for row in inventory_before)
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
        assert predicate == " LIMIT 20001"
        assert parameters == ()
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
def test_materializer_verified_held_inventory_has_one_hard_global_envelope(
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
        inventory_bytes = sum(len(bytes(row[10])) for row in inventory_before)
        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            global_ready_count=1,
        )
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        planned_records = _planned_global_event_records(journal, staged, normalized)
        planned_payloads = tuple(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=record,
                ordinal=ordinal,
                revision=revision,
            )
            for ordinal, record in enumerate(planned_records)
        )
        expected_work_count = len(inventory_before) + len(planned_payloads)
        projected = (
            expected_work_count
            if metric == "count"
            else inventory_bytes + sum(map(len, planned_payloads))
        )
        limit_value = projected - projected_excess
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
        payload_lengths: list[int] = []
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
            values = _verified_event_work_values(
                journal,
                record,
                frame_id=staged.frame_id,
                ready_revision=revision,
                ready_ordinal=index,
                created_revision=revision,
            )
            storage_rows.append(values)
            payload_lengths.append(len(bytes(values[-2])))
        limits = MaterializerLimits()
        assert max(payload_lengths) <= limits.max_record_canonical_bytes
        assert sum(payload_lengths) > limits.max_ready_work_canonical_bytes
        with journal._owner.journal_write():
            for values in storage_rows:
                inserted = journal._execute(
                    "INSERT INTO NioIngestWork("
                    "account_id, work_id, kind, status, frame_id, room_id, "
                    "membership_epoch, room_sequence, ready_revision, "
                    "ready_ordinal, created_revision, payload, "
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
            "created_revision, payload, payload_sha256) "
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
                    _verified_event_work_values(
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
        _insert_verified_event_work(
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
    "corruption",
    (
        "payload-only",
        "digest-only",
        "recomputed-noncanonical",
        "semantic",
        "clear-account_id",
        "context-stream_id",
        "context-transport_kind",
        "clear-work_id",
        "clear-kind",
        "clear-status",
        "clear-frame_id",
        "clear-room_id",
        "clear-membership_epoch",
        "clear-room_sequence",
        "clear-ready_revision",
        "clear-ready_ordinal",
        "clear-created_revision",
    ),
)
def test_materializer_plaintext_inventory_catches_work_row_corruption(
    tmp_path: Path,
    corruption: str,
) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        room_clear_case = corruption in {
            "clear-kind",
            "clear-status",
            "clear-room_id",
            "clear-membership_epoch",
            "clear-room_sequence",
        }
        _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=room_clear_case,
            global_ready_count=None if room_clear_case else 1,
        )
        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        with journal._owner.read():
            row = journal._execute(
                "SELECT * FROM NioIngestWork WHERE account_id = ? "
                + ("AND status = 'held' " if room_clear_case else "")
                + "ORDER BY work_id LIMIT 1",
                (journal.account_id,),
            ).fetchone()
        assert row is not None
        assert tuple(row.keys()) == tuple(column[0] for column in _WORK_COLUMNS)
        payload = bytes(row["payload"])
        digest = bytes(row["payload_sha256"])
        lookup_work_id = row["work_id"]
        if corruption == "payload-only":
            payload = _flip_first(payload)
        elif corruption == "digest-only":
            digest = _flip_first(digest)
        elif corruption == "recomputed-noncanonical":
            payload += b" "
            digest = hashlib.sha256(payload).digest()
        elif corruption == "semantic":
            envelope = json.loads(payload)
            envelope["schema_version"] = 2
            payload = _canonical_internal(envelope)
            digest = hashlib.sha256(payload).digest()
        elif corruption.startswith("context-"):
            envelope = json.loads(payload)
            field = corruption.removeprefix("context-")
            envelope[field] = (
                str(uuid4()) if field == "stream_id" else TransportKind.SLIDING.value
            )
            payload = _canonical_internal(envelope)
            digest = hashlib.sha256(payload).digest()
        if corruption.startswith("clear-"):
            field = corruption.removeprefix("clear-")
            with sqlite3.connect(journal.database_path) as connection:
                if field == "account_id":
                    changed = "@drift:example.org"
                    connection.execute("PRAGMA foreign_keys = OFF")
                    owner_update = connection.execute(
                        "UPDATE NioIngestMeta SET account_id = ? WHERE account_id = ?",
                        (changed, journal.account_id),
                    )
                    assert owner_update.rowcount == 1
                    row_update = connection.execute(
                        "UPDATE NioIngestWork SET account_id = ? "
                        "WHERE account_id = ? AND work_id = ?",
                        (changed, journal.account_id, lookup_work_id),
                    )
                    assert row_update.rowcount == 1
                    journal.account_id = changed
                    journal._owner._account_id = changed
                else:
                    changed = {
                        "work_id": str(uuid4()),
                        "kind": "loss",
                        "status": "ready",
                        "frame_id": str(uuid4()),
                        "room_id": "!drift:example.org",
                        "membership_epoch": (
                            0
                            if row["membership_epoch"] is None
                            else row["membership_epoch"] + 1
                        ),
                        "room_sequence": (
                            0
                            if row["room_sequence"] is None
                            else row["room_sequence"] + 1
                        ),
                        "ready_revision": (
                            1
                            if row["ready_revision"] is None
                            else row["ready_revision"] + 1
                        ),
                        "ready_ordinal": (
                            0
                            if row["ready_ordinal"] is None
                            else row["ready_ordinal"] + 100
                        ),
                        "created_revision": row["created_revision"] - 1,
                    }[field]
                    assert changed != row[field]
                    if field == "ready_revision":
                        owner_update = connection.execute(
                            "UPDATE NioIngestMeta SET revision = ? "
                            "WHERE account_id = ?",
                            (changed, journal.account_id),
                        )
                        assert owner_update.rowcount == 1
                    if field == "status":
                        assert row["status"] == "held"
                        assert row["ready_revision"] is None
                        assert row["ready_ordinal"] is None
                        owner = journal.load_owner()
                        row_update = connection.execute(
                            "UPDATE NioIngestWork SET status = ?, "
                            "ready_revision = ?, ready_ordinal = ? "
                            "WHERE account_id = ? AND work_id = ?",
                            (
                                changed,
                                owner.revision,
                                10_000,
                                journal.account_id,
                                lookup_work_id,
                            ),
                        )
                    elif field == "kind":
                        assert row["kind"] == "event"
                        assert row["status"] == "held"
                        assert row["room_id"] is not None
                        assert row["membership_epoch"] is not None
                        assert row["room_sequence"] is not None
                        owner = journal.load_owner()
                        row_update = connection.execute(
                            "UPDATE NioIngestWork SET kind = ?, status = 'ready', "
                            "room_sequence = NULL, ready_revision = ?, "
                            "ready_ordinal = ? WHERE account_id = ? AND work_id = ?",
                            (
                                changed,
                                owner.revision,
                                10_001,
                                journal.account_id,
                                lookup_work_id,
                            ),
                        )
                    else:
                        row_update = connection.execute(
                            f"UPDATE NioIngestWork SET {field} = ? "
                            "WHERE account_id = ? AND work_id = ?",
                            (changed, journal.account_id, lookup_work_id),
                        )
                    assert row_update.rowcount == 1
                    if field == "work_id":
                        lookup_work_id = changed
                stored = connection.execute(
                    "SELECT payload, payload_sha256 FROM NioIngestWork "
                    "WHERE account_id = ? AND work_id = ?",
                    (journal.account_id, lookup_work_id),
                ).fetchone()
                assert stored == (payload, digest)
        else:
            with journal._owner.journal_write():
                updated = journal._execute(
                    "UPDATE NioIngestWork SET payload = ?, payload_sha256 = ? "
                    "WHERE account_id = ? AND work_id = ?",
                    (payload, digest, journal.account_id, lookup_work_id),
                )
                assert updated.rowcount == 1

        owner = journal.load_owner()
        with journal._owner.read(), pytest.raises(JournalIntegrityError):
            journal._load_task3_work_inventory(owner)
    finally:
        bootstrap.close()


@pytest.mark.parametrize(
    "semantic_case",
    (
        "noncanonical-wrapper",
        "noncanonical-work-id",
        "noncanonical-frame-id",
        "future-ready-revision",
        "created-after-ready",
        "room-ready",
        "loss",
    ),
)
def test_materializer_plaintext_inventory_rejects_semantically_invalid_work(
    tmp_path: Path,
    semantic_case: str,
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
        with journal._owner.read():
            row = journal._execute(
                "SELECT * FROM NioIngestWork WHERE account_id = ? ORDER BY work_id "
                "LIMIT 1",
                (journal.account_id,),
            ).fetchone()
        assert row is not None
        assert tuple(row.keys()) == tuple(column[0] for column in _WORK_COLUMNS)
        old_work_id = row["work_id"]
        payload = bytes(row["payload"])
        envelope = json.loads(payload)
        assignments = ["payload = ?", "payload_sha256 = ?"]
        clear_values: list[object] = []

        if semantic_case == "noncanonical-wrapper":
            payload += b" "
        elif semantic_case == "noncanonical-work-id":
            changed = "{" + row["work_id"] + "}"
            envelope["work_id"] = changed
            envelope["value"]["value"]["record_id"] = changed
            assignments.append("work_id = ?")
            clear_values.append(changed)
        elif semantic_case == "noncanonical-frame-id":
            changed = "{" + row["frame_id"] + "}"
            envelope["frame_id"] = changed
            assignments.append("frame_id = ?")
            clear_values.append(changed)
        elif semantic_case == "future-ready-revision":
            changed = journal.load_owner().revision + 1
            envelope["ready_revision"] = changed
            assignments.append("ready_revision = ?")
            clear_values.append(changed)
        elif semantic_case == "created-after-ready":
            changed = row["ready_revision"] + 1
            envelope["created_revision"] = changed
            assignments.append("created_revision = ?")
            clear_values.append(changed)
        elif semantic_case == "room-ready":
            changes = {
                "room_id": "!invalid-ready:example.org",
                "membership_epoch": 0,
                "room_sequence": 0,
            }
            envelope.update(changes)
            envelope["value"]["value"].update(changes)
            assignments.extend(
                (
                    "room_id = ?",
                    "membership_epoch = ?",
                    "room_sequence = ?",
                )
            )
            clear_values.extend(changes.values())
        else:
            assert semantic_case == "loss"
            changes = {
                "kind": "loss",
                "room_id": "!invalid-loss:example.org",
                "membership_epoch": 0,
                "room_sequence": None,
            }
            envelope.update(changes)
            envelope["value"]["kind"] = "loss"
            envelope["value"]["value"].update(
                {
                    "room_id": changes["room_id"],
                    "membership_epoch": 0,
                    "room_sequence": None,
                }
            )
            assignments.extend(
                (
                    "kind = ?",
                    "room_id = ?",
                    "membership_epoch = ?",
                    "room_sequence = ?",
                )
            )
            clear_values.extend(changes.values())

        if semantic_case != "noncanonical-wrapper":
            payload = _canonical_internal(envelope)
        parameters = [
            payload,
            hashlib.sha256(payload).digest(),
            *clear_values,
            journal.account_id,
            old_work_id,
        ]
        with journal._owner.journal_write():
            updated = journal._execute(
                f"UPDATE NioIngestWork SET {', '.join(assignments)} "
                "WHERE account_id = ? AND work_id = ?",
                tuple(parameters),
            )
            assert updated.rowcount == 1

        owner = journal.load_owner()
        with journal._owner.read(), pytest.raises(JournalIntegrityError):
            journal._load_task3_work_inventory(owner)
    finally:
        bootstrap.close()


def test_materializer_inventory_catches_oversized_payload_before_decode(
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
                "UPDATE NioIngestWork SET payload = ? "
                "WHERE account_id = ? AND work_id = ?",
                (
                    b"x" * (_WORK_PAYLOAD_LIMIT + 1),
                    journal.account_id,
                    target[0],
                ),
            )
            assert updated.rowcount == 1
            connection.execute("PRAGMA ignore_check_constraints = OFF")
        owner_before = journal.load_owner()
        raw_before = _frame_storage_row(journal, staged.frame_id)
        real_decode_work = journal_rows_module._decode_work_plaintext

        def reject_target_decode(
            stream_id: UUID,
            work_id: str,
            kind: str,
            plaintext: bytes,
        ) -> journal_rows_module._DecodedWork:
            if work_id == target[0]:
                raise AssertionError("oversized Work payload reached JSON decode")
            return real_decode_work(stream_id, work_id, kind, plaintext)

        monkeypatch.setattr(
            journal_rows_module,
            "_decode_work_plaintext",
            reject_target_decode,
        )
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
        "payload",
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
        plaintext, record = _decode_event_work(journal, target)
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
        writer_decodes: list[str] = []
        real_decode_work = journal_rows_module._decode_work_plaintext
        real_journal_write = type(journal._owner).journal_write
        reordered_work_id = "ffffffff-ffff-4fff-bfff-ffffffffffff"
        assert reordered_work_id > max(str(row[0]) for row in inventory_before)

        def observe_preflight(
            stream_id: UUID,
            work_id: str,
            kind: str,
            stored_plaintext: bytes,
        ) -> journal_rows_module._DecodedWork:
            nonlocal preflight_decoded
            value = real_decode_work(
                stream_id,
                work_id,
                kind,
                stored_plaintext,
            )
            if journal._owner._outer_scope == "journal_write":
                writer_decodes.append(work_id)
            else:
                preflight_decoded = True
            return value

        def stored_bytes(
            changed: list[object],
            stored_plaintext: bytes,
        ) -> tuple[object, object]:
            values = _sealed_work_values(
                journal,
                work_id=str(changed[0]),
                kind=str(changed[1]),
                status=str(changed[2]),
                frame_id=str(changed[3]),
                room_id=changed[4],
                membership_epoch=changed[5],
                room_sequence=changed[6],
                ready_revision=changed[7],
                ready_ordinal=changed[8],
                created_revision=int(changed[9]),
                plaintext=stored_plaintext,
            )
            return values[-2], values[-1]

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
                payload, digest = stored_bytes(changed, plaintext)
                assignments = "frame_id = ?, payload = ?, payload_sha256 = ?"
                parameters = (changed[3], payload, digest)
            elif race == "work-id-reorder":
                changed = list(target)
                changed[0] = reordered_work_id
                changed_plaintext = _rows()._canonical_work_plaintext(
                    "event",
                    replace(record, record_id=reordered_work_id),
                )
                payload, digest = stored_bytes(changed, changed_plaintext)
                assignments = "work_id = ?, payload = ?, payload_sha256 = ?"
                parameters = (reordered_work_id, payload, digest)
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
                payload, digest = stored_bytes(changed, plaintext)
                column_index = {
                    "ready-revision": ("ready_revision", 7),
                    "ready-ordinal": ("ready_ordinal", 8),
                    "created-revision": ("created_revision", 9),
                }[race]
                assignments = f"{column_index[0]} = ?, payload = ?, payload_sha256 = ?"
                parameters = (changed[column_index[1]], payload, digest)
            elif race == "payload":
                changed_plaintext = _rows()._canonical_work_plaintext(
                    "event",
                    replace(
                        record,
                        source_json=b'{"content":{"raced":true},"type":"m.push_rules"}',
                    ),
                )
                payload, digest = stored_bytes(list(target), changed_plaintext)
                assert digest != target[11]
                assignments = "payload = ?, payload_sha256 = ?"
                parameters = (payload, digest)
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

        monkeypatch.setattr(
            journal_rows_module,
            "_decode_work_plaintext",
            observe_preflight,
        )
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
        assert writer_decodes == sorted(str(row[0]) for row in current_inventory)
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
def test_materializer_empty_fate_catches_missing_meta_cas_raw_delete_or_crypto_header_update(
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
                row_after[7]
                == hashlib.sha256(
                    _canonical_expected_drain_header(
                        journal,
                        row_after,
                        expected_revision,
                    )
                ).digest()
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
        aggregate_plaintext, aggregate = _decode_aggregate(journal, aggregate_row)
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
            plaintext, record = _decode_event_work(journal, row)
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
        _insert_verified_event_work(
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
        assert _decode_event_work(journal, work_before[0])[1] == orphan
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
        header_sha256 = hashlib.sha256(
            _canonical_expected_drain_header(journal, row, retained_revision)
        ).digest()
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
                "drain_header_sha256 = ? WHERE account_id = ? AND frame_id = ?",
                (
                    retained_revision,
                    header_sha256,
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
            "SELECT source_epoch, request_id, staged_revision, payload, "
            "payload_sha256, room_materialized_revision, drain_header_sha256 "
            "FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (_DISCOVERY_ACCOUNT_ID, str(frame_id)),
        ).fetchone()
        assert row is not None
        if mutation == "account-id":
            column, value = "account_id", "@drift:example.org"
        elif mutation == "source-epoch":
            column, value = "source_epoch", row[0] + 1
        elif mutation == "request-id":
            column, value = "request_id", row[1] + 1
        elif mutation == "staged-revision":
            column, value = "staged_revision", row[2] + 1
        elif mutation == "clear-flag":
            column, value = "room_materialized_revision", None
        elif mutation == "header-digest":
            column, value = "drain_header_sha256", _flip_first(row[6])
        elif mutation == "pk-spelling":
            column, value = "frame_id", str(frame_id).upper()
        elif mutation == "payload-digest":
            column, value = "payload_sha256", _flip_first(row[4])
        elif mutation == "payload-length":
            column, value = "payload", row[3] + b"x"
        elif mutation == "selected-payload":
            column, value = "payload", _flip_first(row[3])
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
        pytest.param("account-id", "earlier", id="header-account-id"),
        pytest.param("source-epoch", "earlier", id="header-source-epoch"),
        pytest.param("request-id", "earlier", id="header-request-id"),
        pytest.param("staged-revision", "earlier", id="header-staged-revision"),
        pytest.param("clear-flag", "earlier", id="header-materialized-revision"),
        pytest.param("header-digest", "earlier", id="header-sha256"),
        pytest.param("pk-spelling", "earlier", id="primary-key-spelling"),
        pytest.param("payload-digest", "earlier", id="header-payload-digest"),
        pytest.param("payload-length", "earlier", id="header-payload-length"),
        pytest.param("selected-payload", "selected", id="selected-payload"),
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
        header_sha256 = hashlib.sha256(
            _canonical_expected_drain_header(journal, row, None)
        ).digest()
        with sqlite3.connect(database_path) as connection:
            updated = connection.execute(
                "UPDATE NioIngestFrame SET room_materialized_revision = NULL, "
                "drain_header_sha256 = ? WHERE account_id = ? AND frame_id = ?",
                (header_sha256, journal.account_id, str(earlier.frame_id)),
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
    row = _frame_storage_row(journal, target.frame_id)
    assert row is not None
    if race == "earlier-header":
        changed = list(row)
        changed[2] += 1
        header_sha256 = hashlib.sha256(
            _canonical_expected_drain_header(journal, tuple(changed), changed[6])
        ).digest()
        assignments = "request_id = ?, drain_header_sha256 = ?"
        values = (changed[2], header_sha256)
    elif race == "earlier-proof":
        assignments = "drain_header_sha256 = ?"
        values = (_flip_first(row[7]),)
    else:
        assert race == "selected-row-change"
        changed = list(row)
        changed[4] = _flip_first(changed[4])
        changed[5] = hashlib.sha256(changed[4]).digest()
        changed[7] = hashlib.sha256(
            _canonical_expected_drain_header(journal, tuple(changed), changed[6])
        ).digest()
        assignments = "payload = ?, payload_sha256 = ?, drain_header_sha256 = ?"
        values = (changed[4], changed[5], changed[7])
    with sqlite3.connect(database_path) as connection:
        updated = connection.execute(
            f"UPDATE NioIngestFrame SET {assignments} "
            "WHERE account_id = ? AND frame_id = ?",
            (*values, journal.account_id, str(target.frame_id)),
        )
        assert updated.rowcount == 1


@pytest.mark.parametrize(
    ("race", "scenario"),
    [
        *(
            pytest.param(race, "h1-classic-plain", id=race)
            for race in (
                "earlier-removal",
                "earlier-header",
                "earlier-proof",
                "earlier-valid-flag-change",
                "selected-row-change",
                "selected-removal",
            )
        ),
        pytest.param(
            "selected-row-change",
            "post-hydration-classic-ready",
            id="post_hydration_ready",
        ),
    ],
)
def test_materializer_writer_full_set_revalidation_catches_read_to_write_race(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race: str,
    scenario: str,
) -> None:
    statements: list[str] = []
    case = (
        _prepare_materializer_atomicity_case(tmp_path, scenario, statements=statements)
        if scenario == _ATOMICITY_POST_HYDRATION_CLASSIC_READY
        else None
    )
    bootstrap = (
        case.bootstrap
        if case is not None
        else _open_discovery_journal(
            tmp_path, TransportKind.CLASSIC, statements=statements
        )
    )
    journal = bootstrap._journal
    try:
        earlier, selected = (
            (case.selected, case.selected)
            if case is not None
            else _stage_retained_then_plain(journal)
        )
        owner_before = journal.load_owner()
        selected_prepared = False
        raced = False
        real_decode = journal._decode_frame_row
        real_journal_write = type(journal._owner).journal_write

        def observe_selected_decode(
            frame_id: UUID,
            row: object,
            owner: OwnerView,
            *,
            drain_header_authenticated: bool = False,
        ) -> StagedFrame:
            nonlocal selected_prepared
            frame = real_decode(
                frame_id,
                row,
                owner,
                drain_header_authenticated=drain_header_authenticated,
            )
            if frame_id == selected.frame_id:
                assert journal._owner._outer_scope != "journal_write"
                selected_prepared = True
            return frame

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

        monkeypatch.setattr(journal, "_decode_frame_row", observe_selected_decode)
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
        and "drain_header_sha256" in lowered
        and bool(_PAYLOAD_LENGTH.search(statement))
    )


_SQL_IDENTIFIER = r'(?:[A-Z_][A-Z0-9_$]*|"(?:[^"]|"")+"|`[^`]+`|\[[^\]]+\])'
_PROJECTION_WILDCARD = re.compile(
    rf"(?<![A-Z0-9_$])(?:{_SQL_IDENTIFIER}\s*\.\s*)?\*(?![A-Z0-9_$])",
    re.IGNORECASE,
)
_PAYLOAD_LENGTH = re.compile(
    rf"\bLENGTH\s*\(\s*(?:{_SQL_IDENTIFIER}\s*\.\s*)?" r"PAYLOAD\s*\)",
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
        re.search(r"\bPAYLOAD\b", without_derived_length, re.IGNORECASE)
    )


def _trace_plaintext_frame_validation(
    journal: SqliteIngestionJournal,
    monkeypatch: pytest.MonkeyPatch,
) -> list[tuple[str | None, str, UUID]]:
    trace: list[tuple[str | None, str, UUID]] = []
    real_decode_header = journal._decode_frame_drain_row
    real_decode_payload = journal._decode_frame_row

    def trace_header(
        row: object,
        owner: OwnerView,
        payload_length: object,
        *,
        authenticate: bool,
    ) -> object:
        decoded = real_decode_header(
            row,
            owner,
            payload_length,
            authenticate=authenticate,
        )
        if authenticate:
            trace.append((journal._owner._outer_scope, "header", decoded.frame_id))
        return decoded

    def trace_payload(
        frame_id: UUID,
        row: object,
        owner: OwnerView,
        *,
        drain_header_authenticated: bool = False,
    ) -> StagedFrame:
        decoded = real_decode_payload(
            frame_id,
            row,
            owner,
            drain_header_authenticated=drain_header_authenticated,
        )
        trace.append((journal._owner._outer_scope, "payload", frame_id))
        return decoded

    monkeypatch.setattr(journal, "_decode_frame_drain_row", trace_header)
    monkeypatch.setattr(journal, "_decode_frame_row", trace_payload)
    return trace


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
        assert not _PROJECTION_WILDCARD.search("SELECT frame_id, LENGTH(payload)")
        assert not _projection_fetches_full_payload("SELECT frame_id, LENGTH(payload)")
        assert _projection_fetches_full_payload("SELECT frame_id, payload")
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
        real_execute = journal._execute

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

        monkeypatch.setattr(journal, "_execute", trace_execute)
        validations = _trace_plaintext_frame_validation(journal, monkeypatch)

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
            for column in (
                "account_id",
                "frame_id",
                "source_epoch",
                "request_id",
                "staged_revision",
                "payload_sha256",
                "room_materialized_revision",
                "drain_header_sha256",
            ):
                assert column in projection.lower()
            assert "ORDER BY" not in upper
            assert "WHERE" not in upper
            assert upper.endswith("LIMIT ?")
            assert parameters == (259,)
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
            assert "WHERE" in statement.upper()
        assert len(frame_selects) == 4
        for _, statement, _ in selected_queries:
            predicate = statement.upper().split("WHERE", 1)[1]
            assert "ROOM_MATERIALIZED_REVISION" not in predicate
        expected_ids = frozenset(frame.frame_id for frame in frames)
        read_proofs = validations[:256]
        assert {scope for scope, _, _ in read_proofs} == {"read"}
        assert {kind for _, kind, _ in read_proofs} == {"header"}
        assert frozenset(frame_id for _, _, frame_id in read_proofs) == expected_ids
        selected_scope, selected_kind, selected_key = validations[256]
        assert selected_scope != "journal_write"
        assert selected_kind == "payload"
        assert selected_key == selected.frame_id
        writer_proofs = validations[257:]
        assert len(writer_proofs) == 256
        assert {scope for scope, _, _ in writer_proofs} == {"journal_write"}
        assert {kind for _, kind, _ in writer_proofs} == {"header"}
        assert frozenset(frame_id for _, _, frame_id in writer_proofs) == expected_ids
        assert len(validations) == 513
    finally:
        bootstrap.close()


def test_materializer_exact_max_frame_scan_validates_headers_without_raw_backlog(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        staged_frames = tuple(
            _stage_discovery_frame(
                journal,
                TransportKind.CLASSIC,
                sequence,
                crypto=sequence < 256,
            )
            for sequence in range(1, 257)
        )
        frames = tuple(staged for staged, _ in staged_frames)
        normalized_frames = tuple(normalized for _, normalized in staged_frames)
        assert len(frames) == 256
        assert all(
            frame.to_device_json
            == (
                b'{"content":{"algorithm":"m.megolm.v1.aes-sha2"},"type":"m.room_key"}',
            )
            for frame in normalized_frames[:-1]
        )
        assert normalized_frames[-1].to_device_json == ()
        _retain_discovery_frames(
            journal,
            tuple(frame.frame_id for frame in frames[:-1]),
        )
        selected = frames[-1]
        retained_rows = tuple(
            _frame_storage_row(journal, frame.frame_id) for frame in frames[:-1]
        )
        selected_row = _frame_storage_row(journal, selected.frame_id)
        assert all(
            row is not None and type(row[6]) is int and row[6] > 0
            for row in retained_rows
        )
        assert selected_row is not None
        assert selected_row[6] is None
        owner_before = journal.load_owner()
        source_before = journal.load_source()
        assert _aggregate_rows(journal) == ()
        assert _work_rows(journal) == ()
        queries: list[tuple[str | None, str, tuple[object, ...]]] = []
        real_execute = journal._execute

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

        monkeypatch.setattr(journal, "_execute", trace_execute)
        validations = _trace_plaintext_frame_validation(journal, monkeypatch)

        result = _materialize(journal)

        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            owner_before.revision + 1,
        )
        materialize_queries = tuple(queries)
        assert len(materialize_queries) == 8
        assert _frame_storage_row(journal, selected.frame_id) is None
        assert (
            tuple(_frame_storage_row(journal, frame.frame_id) for frame in frames[:-1])
            == retained_rows
        )
        assert journal.load_owner() == replace(
            owner_before,
            revision=owner_before.revision + 1,
        )
        assert journal.load_source() == source_before
        assert _aggregate_rows(journal) == ()
        assert _work_rows(journal) == ()
        assert not any(
            table in statement.lower()
            for _, statement, _ in materialize_queries
            for table in ("nioingestroomaggregate", "nioingestwork")
        )
        dml = tuple(
            statement
            for _, statement, _ in materialize_queries
            if statement.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        )
        assert len(dml) == 2
        assert "UPDATE NioIngestMeta SET revision" in dml[0]
        assert "DELETE FROM NioIngestFrame" in dml[1]
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
            for column in (
                "account_id",
                "frame_id",
                "source_epoch",
                "request_id",
                "staged_revision",
                "payload_sha256",
                "room_materialized_revision",
                "drain_header_sha256",
            ):
                assert column in projection.lower()
            assert "ORDER BY" not in upper
            assert "WHERE" not in upper
            assert upper.endswith("LIMIT ?")
            assert parameters == (259,)
        selected_queries = [
            item for item in frame_selects if item not in header_queries
        ]
        assert [scope for scope, _, _ in selected_queries] == [
            "read",
            "journal_write",
        ]
        for _, statement, parameters in selected_queries:
            projection = _frame_select_projection(statement)
            assert _projection_fetches_full_payload(projection)
            assert "WHERE" in statement.upper()
            assert parameters == (journal.account_id, str(selected.frame_id))
        assert len(frame_selects) == 4
        for _, statement, _ in selected_queries:
            predicate = statement.upper().split("WHERE", 1)[1]
            assert "ROOM_MATERIALIZED_REVISION" not in predicate
        expected_ids = frozenset(frame.frame_id for frame in frames)
        read_proofs = validations[:256]
        assert len(read_proofs) == 256
        assert {scope for scope, _, _ in read_proofs} == {"read"}
        assert {kind for _, kind, _ in read_proofs} == {"header"}
        assert frozenset(frame_id for _, _, frame_id in read_proofs) == expected_ids
        selected_scope, selected_kind, selected_key = validations[256]
        assert selected_scope == "read"
        assert selected_kind == "payload"
        assert selected_key == selected.frame_id
        writer_proofs = validations[257:]
        assert len(writer_proofs) == 256
        assert {scope for scope, _, _ in writer_proofs} == {"journal_write"}
        assert {kind for _, kind, _ in writer_proofs} == {"header"}
        assert frozenset(frame_id for _, _, frame_id in writer_proofs) == expected_ids
        assert len(validations) == 513
        assert not any(
            scope == "journal_write" and kind == "payload"
            for scope, kind, _ in validations
        )
    finally:
        bootstrap.close()


def test_materializer_max_retained_plaintext_backlog_size_is_arithmetic_only() -> None:
    retained_frame_count = 255
    frame_payload_bytes = 24 * 1024 * 1024

    retained_bytes = retained_frame_count * frame_payload_bytes

    assert retained_bytes == 6_417_285_120
    assert retained_bytes / (1024**3) == 5.9765625


def test_materializer_accepts_exact_24_mib_plaintext_selected_frame(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    config = ClassicSourceConfig(30_000, b'{"padding":""}')
    statements: list[str] = []
    bootstrap = open_ingestion_store(
        tmp_path,
        account_id="@envelope:example.org",
        device_id="ENVELOPE",
        consumer_generation=_CONSUMER_GENERATION,
        source=config,
        pickle_key="envelope-secret",
        database_name="envelope.db",
        statement_observer=statements.append,
    )
    journal = bootstrap._journal
    try:
        owner = journal.load_owner()
        source_state = journal.load_source()

        def candidate(
            padding_bytes: int,
        ) -> tuple[StagedFrame, SyncFrame, SourceState, bytes]:
            target_config = ClassicSourceConfig(
                30_000,
                b'{"padding":"' + (b"x" * padding_bytes) + b'"}',
            )
            adapter = ClassicSource(owner.stream_id, target_config, owner.account_id)
            request = adapter.plan_request(source_state, source_state.next_request_id)
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
            staged = StagedFrame(
                normalized.frame.frame_id,
                StagedSourceResponse(
                    request,
                    normalized.response_body,
                    normalized.frame.source_sha256,
                ),
            )
            stored = replace(staged, staged_revision=owner.revision + 1)
            payload = _expected_plaintext_materializer_envelope(
                row_kind="frame",
                owner=owner,
                clear_fields=(
                    ("frame_id", str(stored.frame_id)),
                    ("source_epoch", request.source_epoch),
                    ("request_id", request.request_id),
                    ("staged_revision", stored.staged_revision),
                ),
                value=_frame_envelope(stored),
            )
            successor = SourceState(
                source_state.source_epoch,
                source_state.transport_kind,
                normalized.frame.candidate_cursor_json,
                source_state.next_request_id + 1,
                source_state.active,
            )
            return staged, normalized.frame, successor, payload

        _, _, _, base_payload = candidate(0)
        padding_bytes = _FRAME_ENVELOPE_LIMIT - len(base_payload)
        assert padding_bytes > 0
        staged, normalized, proposed_source, expected_payload = candidate(padding_bytes)
        assert len(expected_payload) == _FRAME_ENVELOPE_LIMIT
        assert normalized.room_segments == ()
        assert normalized.to_device_json == ()
        assert normalized.ephemeral_json == ()
        assert normalized.global_account_data_json == ()
        assert normalized.presence_json == ()
        proposal = reduce_staged_frame(
            owner.stream_id,
            staged.frame_id,
            normalized,
            (),
        )
        assert proposal.room_proposals == ()
        assert proposal.descriptors == ()
        staged_result = journal.stage_source_response(
            source=proposed_source,
            frame=staged,
        )
        stored = _frame_storage_row(journal, staged.frame_id)
        assert stored is not None
        assert stored[4] == expected_payload
        assert len(stored[4]) == _FRAME_ENVELOPE_LIMIT
        assert stored[5] == hashlib.sha256(expected_payload).digest()
        assert stored[6] is None
        owner_before = journal.load_owner()
        source_before = journal.load_source()
        assert staged_result.revision == owner_before.revision
        assert source_before == proposed_source
        assert _aggregate_rows(journal) == ()
        assert _work_rows(journal) == ()
        payload_decodes: list[tuple[str | None, UUID, int]] = []
        renormalizations: list[tuple[str | None, UUID]] = []
        real_decode_frame_row = journal._decode_frame_row
        real_renormalized_frame = journal._renormalized_frame

        def trace_decode_frame_row(
            frame_id: UUID,
            row: object,
            current_owner: OwnerView,
            *,
            drain_header_authenticated: bool = False,
        ) -> StagedFrame:
            payload_decodes.append(
                (
                    journal._owner._outer_scope,
                    frame_id,
                    len(row["payload"]),
                )
            )
            return real_decode_frame_row(
                frame_id,
                row,
                current_owner,
                drain_header_authenticated=drain_header_authenticated,
            )

        def trace_renormalized_frame(
            current_owner: OwnerView,
            current_frame: StagedFrame,
        ) -> SyncFrame:
            renormalizations.append(
                (
                    journal._owner._outer_scope,
                    current_frame.frame_id,
                )
            )
            return real_renormalized_frame(current_owner, current_frame)

        statements.clear()
        monkeypatch.setattr(journal, "_decode_frame_row", trace_decode_frame_row)
        monkeypatch.setattr(journal, "_renormalized_frame", trace_renormalized_frame)

        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            staged.frame_id,
            owner_before.revision + 1,
        )

        assert payload_decodes == [("read", staged.frame_id, _FRAME_ENVELOPE_LIMIT)]
        assert renormalizations == [("read", staged.frame_id)]
        materialize_statements = tuple(statements)
        assert not any(
            table in statement.lower()
            for statement in materialize_statements
            for table in ("nioingestroomaggregate", "nioingestwork")
        )
        assert _frame_storage_row(journal, staged.frame_id) is None
        assert journal.load_owner() == replace(
            owner_before,
            revision=owner_before.revision + 1,
        )
        assert journal.load_source() == source_before
        assert _aggregate_rows(journal) == ()
        assert _work_rows(journal) == ()
    finally:
        bootstrap.close()


def test_materializer_limit_258_catches_raw_set_truncation_before_payload_or_dml(
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
        with pytest.raises(
            JournalIntegrityError,
            match="staged frame count exceeds the 257 frame cap",
        ):
            _stage_discovery_frame(
                journal,
                TransportKind.CLASSIC,
                258,
            )
        with sqlite3.connect(tmp_path / "discovery.db") as connection:
            connection.execute(
                "INSERT INTO NioIngestFrame ("
                "account_id, frame_id, source_epoch, request_id, "
                "staged_revision, payload, payload_sha256, "
                "drain_header_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
                (
                    journal.account_id,
                    str(uuid4()),
                    0,
                    258,
                    1,
                    b"x",
                    bytes(32),
                    bytes(32),
                ),
            )
        with journal._owner.read():
            stored_count = journal._execute(
                "SELECT COUNT(*) FROM NioIngestFrame WHERE account_id = ?",
                (journal.account_id,),
            ).fetchone()[0]
        assert stored_count == 258
        owner_before = journal.load_owner()
        payload_decodes: list[tuple[str | None, UUID]] = []
        real_decode_frame_row = journal._decode_frame_row

        def trace_decode_frame_row(
            frame_id: UUID,
            row: object,
            current_owner: OwnerView,
            *,
            drain_header_authenticated: bool = False,
        ) -> StagedFrame:
            payload_decodes.append((journal._owner._outer_scope, frame_id))
            return real_decode_frame_row(
                frame_id,
                row,
                current_owner,
                drain_header_authenticated=drain_header_authenticated,
            )

        def reject_writer(_self: object) -> object:
            raise AssertionError("258-row discovery entered journal_write")

        monkeypatch.setattr(journal, "_decode_frame_row", trace_decode_frame_row)
        monkeypatch.setattr(type(journal._owner), "journal_write", reject_writer)
        statements.clear()

        with pytest.raises(
            JournalIntegrityError,
            match="^persisted frame drain row is invalid$",
        ):
            _materialize(journal)

        assert payload_decodes == []
        assert journal.load_owner() == owner_before
        with journal._owner.read():
            assert (
                journal._execute(
                    "SELECT COUNT(*) FROM NioIngestFrame WHERE account_id = ?",
                    (journal.account_id,),
                ).fetchone()[0]
                == 258
            )
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


_ATOMICITY_H1_CLASSIC_PLAIN = "h1-classic-plain"
_ATOMICITY_RETIREMENT_SLIDING_CRYPTO = "retirement-sliding-crypto"
_ATOMICITY_POST_HYDRATION_CLASSIC_READY = "post-hydration-classic-ready"
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
    *(
        pytest.param(
            _ATOMICITY_POST_HYDRATION_CLASSIC_READY,
            boundary,
            1,
            id=f"post_hydration_ready-{boundary.replace('_', '-')}",
        )
        for boundary in (
            "meta_revision_epoch_cas",
            "aggregate_update",
            "work_insert",
            "frame_delete",
            "before_commit",
        )
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
    pytest.param(
        _ATOMICITY_POST_HYDRATION_CLASSIC_READY,
        "commit",
        1,
        id="post_hydration_ready-commit",
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


def _atomicity_transport(scenario: str) -> TransportKind:
    if scenario in (
        _ATOMICITY_H1_CLASSIC_PLAIN,
        _ATOMICITY_POST_HYDRATION_CLASSIC_READY,
    ):
        return TransportKind.CLASSIC
    assert scenario == _ATOMICITY_RETIREMENT_SLIDING_CRYPTO
    return TransportKind.SLIDING


def _materializer_frame_rows(
    journal: SqliteIngestionJournal,
) -> tuple[tuple[object, ...], ...]:
    with journal._owner.read():
        rows = journal._execute(
            "SELECT frame_id, source_epoch, request_id, staged_revision, "
            "payload, payload_sha256, room_materialized_revision, "
            "drain_header_sha256, callbacks_claimed_revision "
            "FROM NioIngestFrame "
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
    *,
    sliding_reopens: int = 0,
) -> None:
    actual = _materializer_storage_graph(journal)
    if sliding_reopens:
        _assert_materializer_sliding_reopen_state(
            actual[0],
            actual[1],
            expected[0],
            expected[1],
            sliding_reopens=sliding_reopens,
            revision_delta=sliding_reopens,
        )
    else:
        assert actual[0] == replace(expected[0], writer_epoch=journal.writer_epoch)
        assert actual[1] == expected[1]
    assert actual[2:] == expected[2:]


def _assert_materializer_sliding_reopen_state(
    owner: OwnerView,
    source_state: SourceState,
    old_owner: OwnerView,
    old_source: SourceState,
    *,
    sliding_reopens: int,
    revision_delta: int,
) -> None:
    assert sliding_reopens > 0
    assert owner == replace(
        old_owner,
        revision=old_owner.revision + revision_delta,
        writer_epoch=owner.writer_epoch,
        next_source_epoch=old_owner.next_source_epoch + sliding_reopens,
    )
    assert owner.writer_epoch != old_owner.writer_epoch
    assert source_state == replace(
        old_source,
        source_epoch=old_owner.next_source_epoch + sliding_reopens - 1,
        cursor_json=source_state.cursor_json,
        next_request_id=0,
    )
    old_cursor = sliding_module._sliding_cursor_from_json(old_source.cursor_json)
    cursor = sliding_module._sliding_cursor_from_json(source_state.cursor_json)
    assert cursor == SlidingCursor(
        pos=None,
        to_device_since=old_cursor.to_device_since,
        connection_instance=cursor.connection_instance,
        connection_name=old_cursor.connection_name,
        all_rooms_range_end=old_cursor.all_rooms_range_end,
        all_rooms_page_size=old_cursor.all_rooms_page_size,
        all_rooms_range_ack_mode=SlidingRangeAckMode.UNKNOWN,
        all_rooms_coverage_complete=False,
    )


def _prepare_materializer_atomicity_case(
    store_path: Path,
    scenario: str,
    *,
    statements: list[str] | None = None,
    sqlite_busy_timeout_ms: int = 2_000,
    room_padding_bytes: int = 0,
) -> _MaterializerAtomicityCase:
    transport = _atomicity_transport(scenario)
    crypto = scenario == _ATOMICITY_RETIREMENT_SLIDING_CRYPTO

    bootstrap = _open_discovery_journal(
        store_path,
        transport,
        statements=statements,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
    )
    journal = bootstrap._journal
    try:
        if scenario == _ATOMICITY_POST_HYDRATION_CLASSIC_READY:
            _install_discovery_hydration_baseline(journal)
            first = None
            first_normalized = None
            selected, normalized = _stage_discovery_frame(
                journal,
                transport,
                2,
                room_nonempty=True,
                room_prev_batch="live-prev",
                room_padding_bytes=room_padding_bytes,
            )
        elif scenario == _ATOMICITY_RETIREMENT_SLIDING_CRYPTO:
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


@contextmanager
def _post_hydration_ready_case(
    store_path: Path,
    *,
    statements: list[str] | None = None,
    room_padding_bytes: int = 0,
) -> Iterator[tuple[_MaterializerAtomicityCase, SqliteIngestionJournal]]:
    case = _prepare_materializer_atomicity_case(
        store_path,
        _ATOMICITY_POST_HYDRATION_CLASSIC_READY,
        statements=statements,
        room_padding_bytes=room_padding_bytes,
    )
    try:
        yield case, case.bootstrap._journal
    finally:
        case.bootstrap.close()


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


def _atomicity_post_hydration_ready_expectations(
    stream_id: UUID,
    staged: StagedFrame,
    normalized: SyncFrame,
    revision: int,
) -> tuple[RoomAggregateValue, tuple[_ExpectedMaterializerWork, ...]]:
    assert normalized.frame_id == staged.frame_id
    assert len(normalized.room_segments) == 1
    segment = normalized.room_segments[0]
    assert segment.timeline_prev_batch is not None
    before = RoomContinuity(
        segment.room_id,
        0,
        "join",
        MembershipBaseline("$discovery-member", None),
        None,
        None,
    )
    after = replace(
        before,
        baseline=MembershipBaseline("$discovery-member", segment.timeline_prev_batch),
    )
    proposal = reduce_staged_frame(stream_id, staged.frame_id, normalized, (before,))
    assert len(proposal.room_proposals) == len(proposal.descriptors) == 1
    room, descriptor = proposal.room_proposals[0], proposal.descriptors[0]
    assert (
        room.before,
        room.after,
        room.recovery,
        room.hydration,
        room.retirement_epoch,
        room.losses,
        room.release,
    ) == (before, after, None, None, None, (), RecoveryRelease.NONE)
    assert (
        descriptor.kind,
        descriptor.room_id,
        descriptor.provenance,
        descriptor.route,
    ) == (
        RecordKind.TIMELINE,
        after.room_id,
        TimelineEventProvenance.LIVE,
        DescriptorRoute.READY,
    )
    record = EventRecord(
        str(uuid5(staged.frame_id, f"event:{descriptor.descriptor_key}")),
        descriptor.kind,
        replace(normalized.origin, frame_index=0),
        after.room_id,
        0,
        0,
        None,
        descriptor.provenance,
        descriptor.source_json,
        None,
    )
    work = _ExpectedMaterializerWork(
        record, "ready", staged.frame_id, revision, 0, revision
    )
    return RoomAggregateValue(after, 1, revision, None), (work,)


def _planned_ready_size(
    journal: SqliteIngestionJournal,
    case: _MaterializerAtomicityCase,
    value: EventRecord | LossRecord,
    revision: int,
) -> int:
    return len(
        _expected_planned_stored_work_payload(
            account_id=journal.account_id,
            stream_id=journal.stream_id,
            frame=case.normalized,
            value=value,
            ordinal=0,
            revision=revision,
        )
    )


def _atomicity_retirement_expectations(
    stream_id: UUID,
    case: _MaterializerAtomicityCase,
    revision: int,
    first_revision: int,
) -> tuple[RoomAggregateValue, tuple[_ExpectedMaterializerWork, ...]]:
    first = case.first
    first_normalized = case.first_normalized
    assert first is not None
    assert first_normalized is not None
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
        plaintext, stored = _decode_work(journal, row)
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
                assert row[11] != old[11]


def _assert_materializer_committed_graph(
    journal: SqliteIngestionJournal,
    scenario: str,
    case: _MaterializerAtomicityCase,
    old_graph: _MaterializerStorageGraph,
    *,
    sliding_reopens_before: int = 0,
    sliding_reopens_after: int = 0,
) -> None:
    old_owner, old_source, old_frames, old_aggregates, old_work = old_graph
    owner, source, frames, aggregates, work = _materializer_storage_graph(journal)
    revision = old_owner.revision + sliding_reopens_before + 1
    sliding_reopens = sliding_reopens_before + sliding_reopens_after
    if sliding_reopens:
        _assert_materializer_sliding_reopen_state(
            owner,
            source,
            old_owner,
            old_source,
            sliding_reopens=sliding_reopens,
            revision_delta=sliding_reopens + 1,
        )
    else:
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
    elif scenario == _ATOMICITY_POST_HYDRATION_CLASSIC_READY:
        assert old_owner.revision in (4, 6)
        expected_aggregate, expected_work = (
            _atomicity_post_hydration_ready_expectations(
                owner.stream_id,
                case.selected,
                case.normalized,
                revision,
            )
        )
        assert frames == ()
        assert tuple(row[0] for row in old_frames) == (str(case.selected.frame_id),)
        assert _frame_storage_row(journal, case.selected.frame_id) is None
        expected_intent = None
        assert work[:-1] == old_work
        work = work[-1:]
        old_work = ()
    else:
        assert scenario == _ATOMICITY_RETIREMENT_SLIDING_CRYPTO
        assert len(old_aggregates) == 1
        first_revision = old_aggregates[0][1]
        assert type(first_revision) is int
        expected_aggregate, expected_work = _atomicity_retirement_expectations(
            owner.stream_id,
            case,
            revision,
            first_revision,
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
            selected_after[7]
            == hashlib.sha256(
                _canonical_expected_drain_header(journal, selected_after, revision)
            ).digest()
        )
        expected_intent = None

    assert aggregates[0][:3] == (
        expected_aggregate.continuity.room_id,
        revision,
        expected_intent,
    )
    aggregate_plaintext, aggregate = _decode_aggregate(journal, aggregates[0])
    assert aggregate == expected_aggregate
    assert aggregate_plaintext == _rows()._canonical_room_aggregate_plaintext(
        expected_aggregate
    )
    _assert_exact_materializer_work(journal, work, expected_work, old_work)


def test_materializer_post_hydration_plain_ready_uses_generic_planner(
    tmp_path: Path,
) -> None:
    from nio.ingest.serialization import canonical_batch_payload

    with _post_hydration_ready_case(tmp_path) as (case, journal):
        old_graph = _materializer_storage_graph(journal)
        frontier = _delivery_frontier(journal)
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            case.selected.frame_id,
            old_graph[0].revision + 1,
        )
        _assert_materializer_committed_graph(
            journal,
            _ATOMICITY_POST_HYDRATION_CLASSIC_READY,
            case,
            old_graph,
        )
        assert _delivery_frontier(journal) == frontier
        row = _work_rows(journal)[0]
        record = _decode_event_work(journal, row)[1]
        batch = journal.next_batch()
        assert batch is not None and batch.records == (record,)
        assert (
            batch.ref.sha256 == hashlib.sha256(canonical_batch_payload(batch)).digest()
        )
        journal.acknowledge_batch(batch.ref)
        assert _work_rows(journal) == ()
        assert journal.next_batch() is None


def _replace_authenticated_aggregate(
    journal: SqliteIngestionJournal, value: RoomAggregateValue
) -> None:
    owner, room_id = journal.load_owner(), value.continuity.room_id
    intent = "hydration" if value.pending_hydration else None
    payload, digest = journal._payload(
        owner,
        "NioIngestRoomAggregate",
        _rows()._canonical_room_aggregate_plaintext(value),
        header=_canonical_internal([room_id, value.updated_revision, intent]),
    )
    with journal._owner.journal_write():
        assert (
            journal._execute(
                "INSERT OR REPLACE INTO NioIngestRoomAggregate VALUES(?,?,?,?,?,?)",
                (
                    journal.account_id,
                    room_id,
                    value.updated_revision,
                    intent,
                    payload,
                    digest,
                ),
            ).rowcount
            == 1
        )


def _malformed_ready_proposal(
    case: str,
    proposal: FrameProposal,
    aggregate: RoomAggregateValue,
    origin: RecordOrigin,
) -> FrameProposal:
    room = proposal.room_proposals[0]
    before = room.before
    assert before is not None
    if case in ("pending-hydration", "missing-before-baseline"):
        room = replace(
            room,
            before=aggregate.continuity,
            after=replace(
                aggregate.continuity,
                baseline=MembershipBaseline("$discovery-member", "live-prev"),
                hydration_id=None,
            ),
        )
    else:
        gap = RecoveryGap(
            uuid5(proposal.frame_id, "malformed-gap"),
            room.after.room_id,
            0,
            origin,
            "start",
            "target",
        )
        hydration_id = uuid5(proposal.frame_id, "malformed-hydration")
        loss = LossProposal(
            room.after.room_id,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
        )
        overrides: dict[str, dict[str, object]] = {
            "missing-aggregate": {},
            "before-mismatch": {"before": replace(before, membership_epoch=1)},
            "missing-after-baseline": {"after": replace(room.after, baseline=None)},
            "unchanged-continuity": {"after": before},
            "nonbaseline-continuity-change": {
                "after": replace(room.after, membership="leave")
            },
            "gap": {"after": replace(room.after, gap=gap)},
            "recovery": {"recovery": gap},
            "hydration": {
                "hydration": HydrationIntent(hydration_id, origin),
                "after": replace(room.after, hydration_id=hydration_id),
            },
            "retirement": {"retirement_epoch": 0},
            "loss": {"losses": (loss,)},
            "release": {"release": RecoveryRelease.LOSS_THEN_HELD},
        }
        if case in overrides:
            room = replace(room, **overrides[case])
        elif case == "second-room":
            second = "!ready-second:example.org"
            return replace(
                proposal,
                room_proposals=(
                    room,
                    replace(
                        room,
                        before=replace(before, room_id=second),
                        after=replace(room.after, room_id=second),
                    ),
                ),
            )
        elif case == "wrong-descriptor-route":
            descriptor = replace(
                proposal.descriptors[0], route=DescriptorRoute.HOLD_FOR_HYDRATION
            )
            return replace(proposal, descriptors=(descriptor,))
        else:
            raise AssertionError(case)
    return replace(proposal, room_proposals=(room,))


@pytest.mark.parametrize(
    "case",
    (
        "missing-aggregate",
        "before-mismatch",
        "pending-hydration",
        "missing-before-baseline",
        "missing-after-baseline",
        "unchanged-continuity",
        "nonbaseline-continuity-change",
        "gap",
        "hydration",
        "recovery",
        "retirement",
        "loss",
        "release",
        "second-room",
        "wrong-descriptor-route",
    ),
)
def test_materializer_post_hydration_ready_rejects_malformed_proposals(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    case: str,
) -> None:
    statements: list[str] = []
    with _post_hydration_ready_case(tmp_path, statements=statements) as (
        prepared,
        journal,
    ):
        aggregate = _decode_aggregate(journal, _aggregate_rows(journal)[0])[1]
        assert type(aggregate) is RoomAggregateValue
        proposal = reduce_staged_frame(
            journal.stream_id,
            prepared.selected.frame_id,
            prepared.normalized,
            (aggregate.continuity,),
        )
        if case == "missing-aggregate":
            with journal._owner.journal_write():
                assert (
                    journal._execute(
                        "DELETE FROM NioIngestRoomAggregate WHERE account_id=?",
                        (journal.account_id,),
                    ).rowcount
                    == 1
                )
        elif case in ("pending-hydration", "missing-before-baseline"):
            hydration_id = uuid5(prepared.selected.frame_id, "pending")
            aggregate = replace(
                aggregate,
                continuity=replace(
                    aggregate.continuity,
                    baseline=None,
                    hydration_id=(
                        hydration_id if case == "pending-hydration" else None
                    ),
                ),
                pending_hydration=(
                    HydrationIntent(hydration_id, prepared.normalized.origin)
                    if case == "pending-hydration"
                    else None
                ),
            )
            _replace_authenticated_aggregate(journal, aggregate)
        elif case == "second-room":
            second_room = "!ready-second:example.org"
            _replace_authenticated_aggregate(
                journal,
                replace(
                    aggregate,
                    continuity=replace(aggregate.continuity, room_id=second_room),
                ),
            )
            monkeypatch.setattr(
                "nio.store._sync_journal._frame_room_ids",
                lambda _frame: (aggregate.continuity.room_id, second_room),
            )
        proposal = _malformed_ready_proposal(
            case, proposal, aggregate, prepared.normalized.origin
        )
        proposals = (proposal,)
        if case == "missing-after-baseline":
            room = proposal.room_proposals[0]
            proposals += (
                replace(
                    proposal,
                    room_proposals=(
                        replace(
                            room,
                            after=replace(
                                room.after,
                                baseline=MembershipBaseline(None, "live-prev"),
                            ),
                        ),
                    ),
                ),
            )
        graph = _materializer_storage_graph(journal)
        for candidate in proposals:
            statements.clear()
            monkeypatch.setattr(
                "nio.store._sync_journal.ingest_reducer.reduce_staged_frame",
                lambda *_args, proposal=candidate: proposal,
            )
            with pytest.raises(JournalIntegrityError):
                _materialize(journal)
            assert _materializer_storage_graph(journal) == graph
            assert _materializer_dml(statements) == ()


def test_materializer_post_hydration_ready_rejects_orphan_held_work(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    with _post_hydration_ready_case(tmp_path, statements=statements) as (case, journal):
        aggregate = _decode_aggregate(journal, _aggregate_rows(journal)[0])[1]
        assert type(aggregate) is RoomAggregateValue
        aggregate = replace(aggregate, next_room_sequence=1)
        _replace_authenticated_aggregate(journal, aggregate)
        held = EventRecord(
            str(uuid5(case.selected.frame_id, "orphan-held")),
            RecordKind.TIMELINE,
            replace(case.normalized.origin, request_id=1),
            aggregate.continuity.room_id,
            0,
            0,
            None,
            TimelineEventProvenance.HISTORY,
            b"{}",
            None,
        )
        _insert_verified_event_work(
            journal,
            held,
            frame_id=_PLANNER_EXISTING_FRAME_ID,
            ready_revision=None,
            ready_ordinal=None,
            created_revision=1,
            status="held",
        )
        graph = _materializer_storage_graph(journal)
        statements.clear()
        with pytest.raises(
            JournalIntegrityError,
            match="^READY room has orphan HELD Work$",
        ):
            _materialize(journal)
        assert _materializer_storage_graph(journal) == graph
        assert _materializer_dml(statements) == ()


@pytest.mark.parametrize(
    ("limit_name", "one_over"),
    tuple(
        pytest.param(name, over, id=f"{label}-{'one-over' if over else 'exact'}")
        for name, label in (
            ("max_ready_work_count", "ready-count"),
            ("max_ready_work_canonical_bytes", "ready-bytes"),
            ("max_total_work_count", "total-count"),
            ("max_total_work_canonical_bytes", "total-bytes"),
        )
        for over in (False, True)
    ),
)
def test_materializer_post_hydration_ready_capacity_exact_and_one_over(
    tmp_path: Path,
    limit_name: str,
    one_over: bool,
) -> None:
    statements: list[str] = []
    with _post_hydration_ready_case(tmp_path, statements=statements) as (case, journal):
        owner = journal.load_owner()
        _expected_aggregate, expected_work = (
            _atomicity_post_hydration_ready_expectations(
                owner.stream_id, case.selected, case.normalized, owner.revision + 1
            )
        )
        incoming = expected_work[0].value
        assert type(incoming) is EventRecord
        existing = replace(
            _event_record(),
            record_id=str(uuid5(case.selected.frame_id, "capacity-existing")),
        )
        _insert_verified_event_work(
            journal,
            existing,
            frame_id=_PLANNER_EXISTING_FRAME_ID,
            ready_revision=1,
            ready_ordinal=0,
            created_revision=1,
        )
        existing_row = _work_rows(journal)[0]
        incoming_bytes = _planned_ready_size(
            journal, case, incoming, owner.revision + 1
        )
        exact = (
            2
            if limit_name.endswith("count")
            else len(bytes(existing_row[10])) + incoming_bytes
        )
        limits = replace(MaterializerLimits(), **{limit_name: exact - int(one_over)})
        graph = _materializer_storage_graph(journal)
        statements.clear()
        result = _materialize(journal, limits=limits)
        if one_over:
            assert result == MaterializeResult(
                MaterializeStatus.AT_CAPACITY, case.selected.frame_id, None
            )
            assert _materializer_storage_graph(journal) == graph
            assert _materializer_dml(statements) == ()
            batch = journal.next_batch()
            assert batch is not None and batch.records == (existing,)
            journal.acknowledge_batch(batch.ref)
            statements.clear()
            graph = _materializer_storage_graph(journal)
            assert graph[0].revision == 6 and graph[4] == ()
            result = _materialize(journal, limits=limits)
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            case.selected.frame_id,
            graph[0].revision + 1,
        )
        rows = _work_rows(journal)
        assert len(rows) == 1 + int(not one_over)
        _assert_materializer_committed_graph(
            journal,
            _ATOMICITY_POST_HYDRATION_CLASSIC_READY,
            case,
            graph,
        )
        committed = _materializer_storage_graph(journal)
        assert _materialize(journal, limits=limits).status is MaterializeStatus.IDLE
        assert _materializer_storage_graph(journal) == committed


def test_materializer_post_hydration_ready_record_limit_is_atomic(
    tmp_path: Path,
) -> None:
    for one_over in (False, True):
        statements: list[str] = []
        with _post_hydration_ready_case(
            tmp_path / str(int(one_over)),
            statements=statements,
            room_padding_bytes=256,
        ) as (case, journal):
            owner = journal.load_owner()
            aggregate, work = _atomicity_post_hydration_ready_expectations(
                owner.stream_id, case.selected, case.normalized, owner.revision + 1
            )
            event = work[0].value
            assert type(event) is EventRecord
            event_bytes = _planned_ready_size(journal, case, event, owner.revision + 1)
            loss = LossRecord(
                "",
                case.normalized.origin,
                event.room_id,
                0,
                LossReason.OVERSIZED_EVENT,
                LossBoundary(None, None, None, None),
                b"{}",
            )
            loss = replace(loss, loss_id=_loss_id(owner.stream_id, loss))
            assert (
                _planned_ready_size(journal, case, loss, owner.revision + 1)
                < event_bytes
            )
            statements.clear()
            result = _materialize(
                journal,
                limits=replace(
                    MaterializerLimits(),
                    max_record_canonical_bytes=event_bytes - int(one_over),
                ),
            )
            revision = owner.revision + 1
            assert result == MaterializeResult(
                MaterializeStatus.MATERIALIZED, case.selected.frame_id, revision
            )
            expected = (
                _ExpectedMaterializerWork(
                    loss, "ready", case.selected.frame_id, revision, 0, revision
                )
                if one_over
                else work[0]
            )
            graph = _materializer_storage_graph(journal)
            assert graph[2] == () and len(graph[3]) == len(graph[4]) == 1
            _assert_exact_materializer_work(journal, graph[4], (expected,), ())
            assert _decode_aggregate(journal, _aggregate_rows(journal)[0])[1] == (
                replace(aggregate, next_room_sequence=0) if one_over else aggregate
            )
            dml = _materializer_dml(statements)
            assert tuple(
                sum(table in statement for statement in dml)
                for table in (
                    "NioIngestMeta",
                    "NioIngestRoomAggregate",
                    "NioIngestWork",
                    "NioIngestFrame",
                )
            ) == (1, 1, 1, 1)
            assert _materialize(journal).status is MaterializeStatus.IDLE
            assert _materializer_storage_graph(journal) == graph


def test_materializer_empty_retirement_anchors_lifecycle_before_later_work(
    tmp_path: Path,
) -> None:
    room_id = "!unsupported:example.org"
    first_origin = RecordOrigin(TransportKind.SLIDING, 0, 0, 0)
    retirement_origin = RecordOrigin(TransportKind.SLIDING, 0, 1, 0)
    first_timeline_json = (
        b'{"content":{"body":"held","msgtype":"m.text"},"type":"m.room.message"}'
    )
    ephemeral_json = (
        b'{"content":{"user_ids":["@friend:example.org"]},"type":"m.typing"}'
    )
    global_json = (
        b'{"content":{"generation":2,"index":0,"padding":""},"type":"m.push_rules"}'
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
        _, first_aggregate = _decode_aggregate(journal, aggregate_rows[0])
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
        assert _decode_event_work(journal, first_work[0]) == (
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
        aggregate_plaintext, aggregate = _decode_aggregate(journal, aggregates[0])
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
            selected_after[7]
            == hashlib.sha256(
                _canonical_expected_drain_header(journal, selected_after, 4)
            ).digest()
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


def test_materializer_two_room_classic_plain_retirement_preserves_other_h1(
    tmp_path: Path,
) -> None:
    room_a = "!a-retire:example.org"
    room_b = "!b-hydrate:example.org"
    first_origin = RecordOrigin(TransportKind.CLASSIC, 0, 0, 0)
    selected_origin = RecordOrigin(TransportKind.CLASSIC, 0, 1, 0)
    first_a_json = (
        b'{"content":{"body":"classic-a-held","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    selected_b_json = (
        b'{"content":{"body":"classic-b-h1","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    selected_a_json = (
        b'{"content":{"body":"classic-a-retired","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    global_json = (
        b'{"content":{"generation":2,"index":0,"padding":""},"type":"m.push_rules"}'
    )
    presence_json = (
        b'{"content":{"presence":"online"},'
        b'"sender":"@friend:example.org","type":"m.presence"}'
    )
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        stream_id = journal.load_owner().stream_id
        first, first_normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            rooms=((room_a, "join", "classic-a-held"),),
        )
        assert first_normalized.origin == first_origin
        assert tuple(segment.room_id for segment in first_normalized.room_segments) == (
            room_a,
        )
        assert first_normalized.room_segments[0].timeline_json == (first_a_json,)
        first_hydration_id = uuid5(
            stream_id,
            f"hydrate:{room_a}:0:{first.frame_id}",
        )
        first_continuity = RoomContinuity(
            room_a,
            0,
            "join",
            None,
            None,
            first_hydration_id,
        )
        first_hydration = HydrationIntent(first_hydration_id, first_origin)
        first_proposal = reduce_staged_frame(
            stream_id,
            first.frame_id,
            first_normalized,
            (),
        )
        assert first_proposal.crypto_deferred is False
        assert len(first_proposal.room_proposals) == 1
        first_room = first_proposal.room_proposals[0]
        assert first_room.before is None
        assert first_room.after == first_continuity
        assert first_room.hydration == first_hydration
        assert first_room.recovery is None
        assert first_room.retirement_epoch is None
        assert first_room.losses == ()
        assert first_room.release is RecoveryRelease.NONE
        assert len(first_proposal.descriptors) == 1
        first_descriptor = first_proposal.descriptors[0]
        assert (
            first_descriptor.kind,
            first_descriptor.room_id,
            first_descriptor.source_json,
            first_descriptor.provenance,
            first_descriptor.descriptor_key,
            first_descriptor.route,
        ) == (
            RecordKind.TIMELINE,
            room_a,
            first_a_json,
            TimelineEventProvenance.HISTORY,
            f"frame:{first.frame_id}:0",
            DescriptorRoute.HOLD_FOR_HYDRATION,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        assert _frame_storage_row(journal, first.frame_id) is None
        first_aggregate = RoomAggregateValue(
            first_continuity,
            1,
            2,
            first_hydration,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert len(aggregate_rows) == 1
        first_plaintext, stored_first_aggregate = _decode_aggregate(
            journal,
            aggregate_rows[0],
        )
        assert aggregate_rows[0][:3] == (room_a, 2, "hydration")
        assert stored_first_aggregate == first_aggregate
        assert first_plaintext == _rows()._canonical_room_aggregate_plaintext(
            first_aggregate
        )
        first_held = EventRecord(
            str(uuid5(first.frame_id, f"event:frame:{first.frame_id}:0")),
            RecordKind.TIMELINE,
            first_origin,
            room_a,
            0,
            0,
            None,
            TimelineEventProvenance.HISTORY,
            first_a_json,
            None,
        )
        first_work = _work_rows(journal)
        _assert_exact_materializer_work(
            journal,
            first_work,
            (
                _ExpectedMaterializerWork(
                    first_held,
                    "held",
                    first.frame_id,
                    None,
                    None,
                    2,
                ),
            ),
            (),
        )

        selected, normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            rooms=(
                (room_a, "leave", "classic-a-retired"),
                (room_b, "join", "classic-b-h1"),
            ),
            global_tail=True,
        )
        assert normalized.origin == selected_origin
        assert tuple(segment.room_id for segment in normalized.room_segments) == (
            room_b,
            room_a,
        )
        assert tuple(segment.timeline_json for segment in normalized.room_segments) == (
            (selected_b_json,),
            (selected_a_json,),
        )
        proposal = reduce_staged_frame(
            stream_id,
            selected.frame_id,
            normalized,
            (first_continuity,),
        )
        assert proposal.crypto_deferred is False
        assert len(proposal.room_proposals) == 2
        room_b_hydration_id = uuid5(
            stream_id,
            f"hydrate:{room_b}:0:{selected.frame_id}",
        )
        room_b_continuity = RoomContinuity(
            room_b,
            0,
            "join",
            None,
            None,
            room_b_hydration_id,
        )
        room_b_hydration = HydrationIntent(room_b_hydration_id, selected_origin)
        room_b_proposal, room_a_proposal = proposal.room_proposals
        assert room_b_proposal.before is None
        assert room_b_proposal.after == room_b_continuity
        assert room_b_proposal.hydration == room_b_hydration
        assert room_b_proposal.recovery is None
        assert room_b_proposal.retirement_epoch is None
        assert room_b_proposal.losses == ()
        assert room_b_proposal.release is RecoveryRelease.NONE
        room_a_continuity = RoomContinuity(
            room_a,
            1,
            "leave",
            None,
            None,
            None,
        )
        assert room_a_proposal.before == first_continuity
        assert room_a_proposal.after == room_a_continuity
        assert room_a_proposal.hydration is None
        assert room_a_proposal.recovery is None
        assert room_a_proposal.retirement_epoch == 0
        assert room_a_proposal.losses == (
            LossProposal(
                room_a,
                0,
                LossReason.UNVERIFIABLE,
                LossBoundary(None, None, None, None),
            ),
        )
        assert room_a_proposal.release is RecoveryRelease.LOSS_THEN_HELD
        assert tuple(
            (
                descriptor.kind,
                descriptor.room_id,
                descriptor.source_json,
                descriptor.provenance,
                descriptor.descriptor_key,
                descriptor.route,
            )
            for descriptor in proposal.descriptors
        ) == (
            (
                RecordKind.TIMELINE,
                room_b,
                selected_b_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:0",
                DescriptorRoute.HOLD_FOR_HYDRATION,
            ),
            (
                RecordKind.TIMELINE,
                room_a,
                selected_a_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:1",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            ),
            (
                RecordKind.GLOBAL_ACCOUNT_DATA,
                None,
                global_json,
                None,
                f"frame:{selected.frame_id}:2",
                DescriptorRoute.READY,
            ),
            (
                RecordKind.PRESENCE,
                None,
                presence_json,
                None,
                f"frame:{selected.frame_id}:3",
                DescriptorRoute.READY,
            ),
        )

        old_graph = _materializer_storage_graph(journal)
        old_owner, old_source, old_frames, old_aggregates, old_work = old_graph
        assert old_owner.revision == 3
        assert old_aggregates == aggregate_rows
        assert old_work == first_work
        assert len(old_frames) == 1
        selected_before = old_frames[0]
        assert selected_before[0] == str(selected.frame_id)
        assert selected_before[6] is None
        statements.clear()
        try:
            result = _materialize(journal)
        except JournalIntegrityError as error:
            if str(error) != "this checkpoint retires exactly one room":
                raise AssertionError("unexpected two-room retirement RED") from error
            assert _materializer_storage_graph(journal) == old_graph
            assert _frame_storage_row(journal, selected.frame_id) == selected_before
            assert _materializer_dml(statements) == ()
            raise
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            4,
        )

        owner, source, frames, aggregates, work = _materializer_storage_graph(journal)
        assert owner == replace(old_owner, revision=4)
        assert source == old_source
        assert frames == ()
        expected_a_aggregate = RoomAggregateValue(room_a_continuity, 3, 4, None)
        expected_b_aggregate = RoomAggregateValue(
            room_b_continuity,
            1,
            4,
            room_b_hydration,
        )
        assert tuple(row[:3] for row in aggregates) == (
            (room_a, 4, None),
            (room_b, 4, "hydration"),
        )
        for row, expected in zip(
            aggregates,
            (expected_a_aggregate, expected_b_aggregate),
            strict=True,
        ):
            plaintext, stored = _decode_aggregate(journal, row)
            assert stored == expected
            assert plaintext == _rows()._canonical_room_aggregate_plaintext(expected)

        loss_without_id = LossRecord(
            "",
            selected_origin,
            room_a,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        loss = replace(
            loss_without_id,
            loss_id=_loss_id(stream_id, loss_without_id),
        )
        lifecycle = EventRecord(
            str(uuid5(selected.frame_id, f"lifecycle:{room_a}:0:1")),
            RecordKind.ROOM_LIFECYCLE,
            selected_origin,
            room_a,
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
        selected_b = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:0")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 0, 1, 0),
            room_b,
            0,
            0,
            None,
            TimelineEventProvenance.LIVE,
            selected_b_json,
            None,
        )
        selected_a = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:1")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 0, 1, 1),
            room_a,
            1,
            2,
            None,
            TimelineEventProvenance.LIVE,
            selected_a_json,
            None,
        )
        global_account_data = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:2")),
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordOrigin(TransportKind.CLASSIC, 0, 1, 2),
            None,
            None,
            None,
            None,
            None,
            global_json,
            None,
        )
        presence = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:3")),
            RecordKind.PRESENCE,
            RecordOrigin(TransportKind.CLASSIC, 0, 1, 3),
            None,
            None,
            None,
            None,
            None,
            presence_json,
            None,
        )
        expected_work = (
            _ExpectedMaterializerWork(
                selected_b,
                "held",
                selected.frame_id,
                None,
                None,
                4,
            ),
            _ExpectedMaterializerWork(loss, "ready", selected.frame_id, 4, 0, 4),
            _ExpectedMaterializerWork(first_held, "ready", first.frame_id, 4, 1, 2),
            _ExpectedMaterializerWork(
                lifecycle,
                "ready",
                selected.frame_id,
                4,
                2,
                4,
            ),
            _ExpectedMaterializerWork(
                selected_a,
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
        assert tuple(row[8] for row in work if row[2] == "ready") == tuple(range(6))
        assert tuple((row[4], row[6]) for row in work if row[4] is not None) == (
            (room_b, 0),
            (room_a, None),
            (room_a, 0),
            (room_a, 1),
            (room_a, 2),
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


def test_materializer_two_room_sliding_crypto_retirement_preserves_other_h2(
    tmp_path: Path,
) -> None:
    room_a = "!a-retire:example.org"
    room_b = "!b-hydrate:example.org"
    first_origin = RecordOrigin(TransportKind.SLIDING, 0, 0, 0)
    selected_origin = RecordOrigin(TransportKind.SLIDING, 0, 1, 0)
    first_a_json = (
        b'{"content":{"body":"sliding-a-held","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    first_b_json = (
        b'{"content":{"body":"sliding-b-held","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    selected_b_json = (
        b'{"content":{"body":"sliding-b-h2","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    selected_a_json = (
        b'{"content":{"body":"sliding-a-retired","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    global_json = (
        b'{"content":{"generation":2,"index":0,"padding":""},"type":"m.push_rules"}'
    )
    presence_json = (
        b'{"content":{"presence":"online"},'
        b'"sender":"@friend:example.org","type":"m.presence"}'
    )
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.SLIDING,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        stream_id = journal.load_owner().stream_id
        first, first_normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.SLIDING,
            1,
            rooms=(
                (room_a, "join", "sliding-a-held"),
                (room_b, "join", "sliding-b-held"),
            ),
            crypto=True,
        )
        assert first_normalized.origin == first_origin
        assert tuple(segment.room_id for segment in first_normalized.room_segments) == (
            room_a,
            room_b,
        )
        assert tuple(
            segment.timeline_json for segment in first_normalized.room_segments
        ) == ((first_a_json,), (first_b_json,))
        first_a_hydration_id = uuid5(
            stream_id,
            f"hydrate:{room_a}:0:{first.frame_id}",
        )
        first_b_hydration_id = uuid5(
            stream_id,
            f"hydrate:{room_b}:0:{first.frame_id}",
        )
        first_a_continuity = RoomContinuity(
            room_a,
            0,
            "join",
            None,
            None,
            first_a_hydration_id,
        )
        first_b_continuity = RoomContinuity(
            room_b,
            0,
            "join",
            None,
            None,
            first_b_hydration_id,
        )
        first_a_hydration = HydrationIntent(first_a_hydration_id, first_origin)
        first_b_hydration = HydrationIntent(first_b_hydration_id, first_origin)
        first_proposal = reduce_staged_frame(
            stream_id,
            first.frame_id,
            first_normalized,
            (),
        )
        assert first_proposal.crypto_deferred is True
        assert tuple(
            (
                room.before,
                room.after,
                room.hydration,
                room.recovery,
                room.retirement_epoch,
                room.losses,
                room.release,
            )
            for room in first_proposal.room_proposals
        ) == (
            (
                None,
                first_a_continuity,
                first_a_hydration,
                None,
                None,
                (),
                RecoveryRelease.NONE,
            ),
            (
                None,
                first_b_continuity,
                first_b_hydration,
                None,
                None,
                (),
                RecoveryRelease.NONE,
            ),
        )
        assert tuple(
            (
                descriptor.kind,
                descriptor.room_id,
                descriptor.source_json,
                descriptor.provenance,
                descriptor.descriptor_key,
                descriptor.route,
            )
            for descriptor in first_proposal.descriptors
        ) == (
            (
                RecordKind.TIMELINE,
                room_a,
                first_a_json,
                TimelineEventProvenance.HISTORY,
                f"frame:{first.frame_id}:0",
                DescriptorRoute.HOLD_FOR_HYDRATION,
            ),
            (
                RecordKind.TIMELINE,
                room_b,
                first_b_json,
                TimelineEventProvenance.HISTORY,
                f"frame:{first.frame_id}:1",
                DescriptorRoute.HOLD_FOR_HYDRATION,
            ),
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        first_raw_before = _frame_storage_row(journal, first.frame_id)
        assert first_raw_before is not None
        assert first_raw_before[6] == 2
        first_a_aggregate = RoomAggregateValue(
            first_a_continuity,
            1,
            2,
            first_a_hydration,
        )
        first_b_aggregate = RoomAggregateValue(
            first_b_continuity,
            1,
            2,
            first_b_hydration,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert tuple(row[:3] for row in aggregate_rows) == (
            (room_a, 2, "hydration"),
            (room_b, 2, "hydration"),
        )
        for row, expected in zip(
            aggregate_rows,
            (first_a_aggregate, first_b_aggregate),
            strict=True,
        ):
            plaintext, stored = _decode_aggregate(journal, row)
            assert stored == expected
            assert plaintext == _rows()._canonical_room_aggregate_plaintext(expected)
        first_a_held = EventRecord(
            str(uuid5(first.frame_id, f"event:frame:{first.frame_id}:0")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.SLIDING, 0, 0, 0),
            room_a,
            0,
            0,
            None,
            TimelineEventProvenance.HISTORY,
            first_a_json,
            None,
        )
        first_b_held = EventRecord(
            str(uuid5(first.frame_id, f"event:frame:{first.frame_id}:1")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.SLIDING, 0, 0, 1),
            room_b,
            0,
            0,
            None,
            TimelineEventProvenance.HISTORY,
            first_b_json,
            None,
        )
        first_expected_work = tuple(
            sorted(
                (
                    _ExpectedMaterializerWork(
                        first_a_held,
                        "held",
                        first.frame_id,
                        None,
                        None,
                        2,
                    ),
                    _ExpectedMaterializerWork(
                        first_b_held,
                        "held",
                        first.frame_id,
                        None,
                        None,
                        2,
                    ),
                ),
                key=_expected_materializer_work_id,
            )
        )
        first_work = _work_rows(journal)
        _assert_exact_materializer_work(
            journal,
            first_work,
            first_expected_work,
            (),
        )

        selected, normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.SLIDING,
            2,
            rooms=(
                (room_a, "leave", "sliding-a-retired"),
                (room_b, "join", "sliding-b-h2"),
            ),
            crypto=True,
            global_tail=True,
        )
        assert normalized.origin == selected_origin
        assert tuple(segment.room_id for segment in normalized.room_segments) == (
            room_b,
            room_a,
        )
        assert tuple(segment.timeline_json for segment in normalized.room_segments) == (
            (selected_b_json,),
            (selected_a_json,),
        )
        proposal = reduce_staged_frame(
            stream_id,
            selected.frame_id,
            normalized,
            (first_a_continuity, first_b_continuity),
        )
        assert proposal.crypto_deferred is True
        assert len(proposal.room_proposals) == 2
        room_b_proposal, room_a_proposal = proposal.room_proposals
        assert room_b_proposal.before == first_b_continuity
        assert room_b_proposal.after == first_b_continuity
        assert room_b_proposal.hydration == HydrationIntent(
            first_b_hydration_id,
            selected_origin,
        )
        assert room_b_proposal.recovery is None
        assert room_b_proposal.retirement_epoch is None
        assert room_b_proposal.losses == ()
        assert room_b_proposal.release is RecoveryRelease.NONE
        room_a_continuity = RoomContinuity(
            room_a,
            1,
            "leave",
            None,
            None,
            None,
        )
        assert room_a_proposal.before == first_a_continuity
        assert room_a_proposal.after == room_a_continuity
        assert room_a_proposal.hydration is None
        assert room_a_proposal.recovery is None
        assert room_a_proposal.retirement_epoch == 0
        assert room_a_proposal.losses == (
            LossProposal(
                room_a,
                0,
                LossReason.UNVERIFIABLE,
                LossBoundary(None, None, None, None),
            ),
        )
        assert room_a_proposal.release is RecoveryRelease.LOSS_THEN_HELD
        assert tuple(
            (
                descriptor.kind,
                descriptor.room_id,
                descriptor.source_json,
                descriptor.provenance,
                descriptor.descriptor_key,
                descriptor.route,
            )
            for descriptor in proposal.descriptors
        ) == (
            (
                RecordKind.TIMELINE,
                room_b,
                selected_b_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:0",
                DescriptorRoute.HOLD_FOR_HYDRATION,
            ),
            (
                RecordKind.TIMELINE,
                room_a,
                selected_a_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:1",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            ),
            (
                RecordKind.GLOBAL_ACCOUNT_DATA,
                None,
                global_json,
                None,
                f"frame:{selected.frame_id}:2",
                DescriptorRoute.READY,
            ),
            (
                RecordKind.PRESENCE,
                None,
                presence_json,
                None,
                f"frame:{selected.frame_id}:3",
                DescriptorRoute.READY,
            ),
        )

        old_graph = _materializer_storage_graph(journal)
        old_owner, old_source, old_frames, old_aggregates, old_work = old_graph
        assert old_owner.revision == 3
        assert old_aggregates == aggregate_rows
        assert old_work == first_work
        old_frames_by_id = {str(row[0]): row for row in old_frames}
        assert set(old_frames_by_id) == {
            str(first.frame_id),
            str(selected.frame_id),
        }
        assert old_frames_by_id[str(first.frame_id)] == first_raw_before
        selected_before = old_frames_by_id[str(selected.frame_id)]
        assert selected_before[6] is None
        statements.clear()
        try:
            result = _materialize(journal)
        except JournalIntegrityError as error:
            if str(error) != "this checkpoint retires exactly one room":
                raise AssertionError("unexpected two-room retirement RED") from error
            assert _materializer_storage_graph(journal) == old_graph
            assert _frame_storage_row(journal, first.frame_id) == first_raw_before
            assert _frame_storage_row(journal, selected.frame_id) == selected_before
            assert _materializer_dml(statements) == ()
            raise
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            4,
        )

        owner, source, frames, aggregates, work = _materializer_storage_graph(journal)
        assert owner == replace(old_owner, revision=4)
        assert source == old_source
        expected_a_aggregate = RoomAggregateValue(room_a_continuity, 3, 4, None)
        expected_b_aggregate = RoomAggregateValue(
            first_b_continuity,
            2,
            4,
            first_b_hydration,
        )
        assert tuple(row[:3] for row in aggregates) == (
            (room_a, 4, None),
            (room_b, 4, "hydration"),
        )
        for row, expected in zip(
            aggregates,
            (expected_a_aggregate, expected_b_aggregate),
            strict=True,
        ):
            plaintext, stored = _decode_aggregate(journal, row)
            assert stored == expected
            assert plaintext == _rows()._canonical_room_aggregate_plaintext(expected)

        loss_without_id = LossRecord(
            "",
            selected_origin,
            room_a,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        loss = replace(
            loss_without_id,
            loss_id=_loss_id(stream_id, loss_without_id),
        )
        lifecycle = EventRecord(
            str(uuid5(selected.frame_id, f"lifecycle:{room_a}:0:1")),
            RecordKind.ROOM_LIFECYCLE,
            selected_origin,
            room_a,
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
        selected_b = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:0")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 0),
            room_b,
            0,
            1,
            None,
            TimelineEventProvenance.LIVE,
            selected_b_json,
            None,
        )
        selected_a = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:1")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 1),
            room_a,
            1,
            2,
            None,
            TimelineEventProvenance.LIVE,
            selected_a_json,
            None,
        )
        global_account_data = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:2")),
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 2),
            None,
            None,
            None,
            None,
            None,
            global_json,
            None,
        )
        presence = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:3")),
            RecordKind.PRESENCE,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 3),
            None,
            None,
            None,
            None,
            None,
            presence_json,
            None,
        )
        held_expected = tuple(
            sorted(
                (
                    _ExpectedMaterializerWork(
                        first_b_held,
                        "held",
                        first.frame_id,
                        None,
                        None,
                        2,
                    ),
                    _ExpectedMaterializerWork(
                        selected_b,
                        "held",
                        selected.frame_id,
                        None,
                        None,
                        4,
                    ),
                ),
                key=_expected_materializer_work_id,
            )
        )
        expected_work = (
            *held_expected,
            _ExpectedMaterializerWork(loss, "ready", selected.frame_id, 4, 0, 4),
            _ExpectedMaterializerWork(
                first_a_held,
                "ready",
                first.frame_id,
                4,
                1,
                2,
            ),
            _ExpectedMaterializerWork(
                lifecycle,
                "ready",
                selected.frame_id,
                4,
                2,
                4,
            ),
            _ExpectedMaterializerWork(
                selected_a,
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
        old_work_by_id = {str(row[0]): row for row in old_work}
        work_by_id = {str(row[0]): row for row in work}
        assert (
            work_by_id[first_b_held.record_id] == old_work_by_id[first_b_held.record_id]
        )
        assert work_by_id[first_a_held.record_id][2:10] == (
            "ready",
            str(first.frame_id),
            room_a,
            0,
            0,
            4,
            1,
            2,
        )
        assert tuple(row[8] for row in work if row[2] == "ready") == tuple(range(6))
        assert sorted((row[6], row[2], row[8]) for row in work if row[4] == room_b) == [
            (0, "held", None),
            (1, "held", None),
        ]

        frames_by_id = {str(row[0]): row for row in frames}
        assert frames_by_id[str(first.frame_id)] == first_raw_before
        selected_after = frames_by_id[str(selected.frame_id)]
        assert selected_after[:6] == selected_before[:6]
        assert selected_after[6] == 4
        assert selected_after[7] != selected_before[7]
        assert journal.load_frame(first.frame_id) == first
        assert journal.load_frame(selected.frame_id) == selected
        assert (
            selected_after[7]
            == hashlib.sha256(
                _canonical_expected_drain_header(journal, selected_after, 4)
            ).digest()
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


def test_materializer_retirement_successor_oversize_becomes_epoch_capacity_loss(
    tmp_path: Path,
) -> None:
    room_a = "!a-retire:example.org"
    room_b = "!b-hydrate:example.org"
    first_origin = RecordOrigin(TransportKind.SLIDING, 0, 0, 0)
    selected_origin = RecordOrigin(TransportKind.SLIDING, 0, 1, 0)
    first_a_json = (
        b'{"content":{"body":"capacity-a-held-0","msgtype":"m.text"},'
        b'"type":"m.room.message"}',
        b'{"content":{"body":"capacity-a-held-1","msgtype":"m.text"},'
        b'"type":"m.room.message"}',
    )
    first_b_json = (
        b'{"content":{"body":"capacity-b-held","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    selected_b_json = (
        b'{"content":{"body":"capacity-b-h2","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    oversized_a_json = (
        b'{"content":{"body":"'
        + (b"x" * 512)
        + b'","msgtype":"m.text"},"type":"m.room.message"}'
    )
    global_json = (
        b'{"content":{"generation":2,"index":0,"padding":""},"type":"m.push_rules"}'
    )
    presence_json = (
        b'{"content":{"presence":"online"},'
        b'"sender":"@friend:example.org","type":"m.presence"}'
    )
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.SLIDING,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        stream_id = journal.load_owner().stream_id
        first, first_normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.SLIDING,
            1,
            rooms=(
                (
                    room_a,
                    "join",
                    ("capacity-a-held-0", "capacity-a-held-1"),
                ),
                (room_b, "join", "capacity-b-held"),
            ),
            crypto=True,
        )
        assert first_normalized.origin == first_origin
        assert tuple(segment.room_id for segment in first_normalized.room_segments) == (
            room_a,
            room_b,
        )
        assert tuple(
            segment.timeline_json for segment in first_normalized.room_segments
        ) == (first_a_json, (first_b_json,))
        first_a_hydration_id = uuid5(
            stream_id,
            f"hydrate:{room_a}:0:{first.frame_id}",
        )
        first_b_hydration_id = uuid5(
            stream_id,
            f"hydrate:{room_b}:0:{first.frame_id}",
        )
        first_a_continuity = RoomContinuity(
            room_a,
            0,
            "join",
            None,
            None,
            first_a_hydration_id,
        )
        first_b_continuity = RoomContinuity(
            room_b,
            0,
            "join",
            None,
            None,
            first_b_hydration_id,
        )
        first_a_hydration = HydrationIntent(first_a_hydration_id, first_origin)
        first_b_hydration = HydrationIntent(first_b_hydration_id, first_origin)
        first_proposal = reduce_staged_frame(
            stream_id,
            first.frame_id,
            first_normalized,
            (),
        )
        assert first_proposal.crypto_deferred is True
        assert tuple(
            (room.before, room.after, room.hydration, room.release)
            for room in first_proposal.room_proposals
        ) == (
            (
                None,
                first_a_continuity,
                first_a_hydration,
                RecoveryRelease.NONE,
            ),
            (
                None,
                first_b_continuity,
                first_b_hydration,
                RecoveryRelease.NONE,
            ),
        )
        assert tuple(
            (
                descriptor.kind,
                descriptor.room_id,
                descriptor.source_json,
                descriptor.provenance,
                descriptor.descriptor_key,
                descriptor.route,
            )
            for descriptor in first_proposal.descriptors
        ) == (
            (
                RecordKind.TIMELINE,
                room_a,
                first_a_json[0],
                TimelineEventProvenance.HISTORY,
                f"frame:{first.frame_id}:0",
                DescriptorRoute.HOLD_FOR_HYDRATION,
            ),
            (
                RecordKind.TIMELINE,
                room_a,
                first_a_json[1],
                TimelineEventProvenance.HISTORY,
                f"frame:{first.frame_id}:1",
                DescriptorRoute.HOLD_FOR_HYDRATION,
            ),
            (
                RecordKind.TIMELINE,
                room_b,
                first_b_json,
                TimelineEventProvenance.HISTORY,
                f"frame:{first.frame_id}:2",
                DescriptorRoute.HOLD_FOR_HYDRATION,
            ),
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        first_raw_before = _frame_storage_row(journal, first.frame_id)
        assert first_raw_before is not None
        assert first_raw_before[6] == 2
        first_a_aggregate = RoomAggregateValue(
            first_a_continuity,
            2,
            2,
            first_a_hydration,
        )
        first_b_aggregate = RoomAggregateValue(
            first_b_continuity,
            1,
            2,
            first_b_hydration,
        )
        aggregate_rows = _aggregate_rows(journal)
        assert tuple(row[:3] for row in aggregate_rows) == (
            (room_a, 2, "hydration"),
            (room_b, 2, "hydration"),
        )
        for row, expected in zip(
            aggregate_rows,
            (first_a_aggregate, first_b_aggregate),
            strict=True,
        ):
            plaintext, stored = _decode_aggregate(journal, row)
            assert stored == expected
            assert plaintext == _rows()._canonical_room_aggregate_plaintext(expected)
        first_a_held = tuple(
            EventRecord(
                str(uuid5(first.frame_id, f"event:frame:{first.frame_id}:{index}")),
                RecordKind.TIMELINE,
                RecordOrigin(TransportKind.SLIDING, 0, 0, index),
                room_a,
                0,
                index,
                None,
                TimelineEventProvenance.HISTORY,
                source_json,
                None,
            )
            for index, source_json in enumerate(first_a_json)
        )
        first_b_held = EventRecord(
            str(uuid5(first.frame_id, f"event:frame:{first.frame_id}:2")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.SLIDING, 0, 0, 2),
            room_b,
            0,
            0,
            None,
            TimelineEventProvenance.HISTORY,
            first_b_json,
            None,
        )
        first_expected_work = tuple(
            sorted(
                (
                    *(
                        _ExpectedMaterializerWork(
                            value,
                            "held",
                            first.frame_id,
                            None,
                            None,
                            2,
                        )
                        for value in first_a_held
                    ),
                    _ExpectedMaterializerWork(
                        first_b_held,
                        "held",
                        first.frame_id,
                        None,
                        None,
                        2,
                    ),
                ),
                key=_expected_materializer_work_id,
            )
        )
        first_work = _work_rows(journal)
        _assert_exact_materializer_work(
            journal,
            first_work,
            first_expected_work,
            (),
        )

        selected, normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.SLIDING,
            2,
            rooms=(
                (room_a, "leave", "x" * 512),
                (room_b, "join", "capacity-b-h2"),
            ),
            crypto=True,
            global_tail=True,
        )
        assert normalized.origin == selected_origin
        assert tuple(segment.room_id for segment in normalized.room_segments) == (
            room_b,
            room_a,
        )
        assert tuple(segment.timeline_json for segment in normalized.room_segments) == (
            (selected_b_json,),
            (oversized_a_json,),
        )
        proposal = reduce_staged_frame(
            stream_id,
            selected.frame_id,
            normalized,
            (first_a_continuity, first_b_continuity),
        )
        assert proposal.crypto_deferred is True
        room_b_proposal, room_a_proposal = proposal.room_proposals
        assert room_b_proposal.before == first_b_continuity
        assert room_b_proposal.after == first_b_continuity
        assert room_b_proposal.hydration == HydrationIntent(
            first_b_hydration_id,
            selected_origin,
        )
        assert room_b_proposal.retirement_epoch is None
        assert room_b_proposal.losses == ()
        assert room_b_proposal.release is RecoveryRelease.NONE
        room_a_continuity = RoomContinuity(
            room_a,
            1,
            "leave",
            None,
            None,
            None,
        )
        assert room_a_proposal.before == first_a_continuity
        assert room_a_proposal.after == room_a_continuity
        assert room_a_proposal.hydration is None
        assert room_a_proposal.recovery is None
        assert room_a_proposal.retirement_epoch == 0
        assert room_a_proposal.losses == (
            LossProposal(
                room_a,
                0,
                LossReason.UNVERIFIABLE,
                LossBoundary(None, None, None, None),
            ),
        )
        assert room_a_proposal.release is RecoveryRelease.LOSS_THEN_HELD
        assert tuple(
            (
                descriptor.kind,
                descriptor.room_id,
                descriptor.source_json,
                descriptor.provenance,
                descriptor.descriptor_key,
                descriptor.route,
            )
            for descriptor in proposal.descriptors
        ) == (
            (
                RecordKind.TIMELINE,
                room_b,
                selected_b_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:0",
                DescriptorRoute.HOLD_FOR_HYDRATION,
            ),
            (
                RecordKind.TIMELINE,
                room_a,
                oversized_a_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:1",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            ),
            (
                RecordKind.GLOBAL_ACCOUNT_DATA,
                None,
                global_json,
                None,
                f"frame:{selected.frame_id}:2",
                DescriptorRoute.READY,
            ),
            (
                RecordKind.PRESENCE,
                None,
                presence_json,
                None,
                f"frame:{selected.frame_id}:3",
                DescriptorRoute.READY,
            ),
        )

        old_loss_without_id = LossRecord(
            "",
            selected_origin,
            room_a,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        old_loss = replace(
            old_loss_without_id,
            loss_id=_loss_id(stream_id, old_loss_without_id),
        )
        lifecycle = EventRecord(
            str(uuid5(selected.frame_id, f"lifecycle:{room_a}:0:1")),
            RecordKind.ROOM_LIFECYCLE,
            selected_origin,
            room_a,
            1,
            2,
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
        capacity_loss_without_id = LossRecord(
            "",
            selected_origin,
            room_a,
            1,
            LossReason.OVERSIZED_EVENT,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        capacity_loss = replace(
            capacity_loss_without_id,
            loss_id=_loss_id(stream_id, capacity_loss_without_id),
        )
        selected_b = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:0")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 0),
            room_b,
            0,
            1,
            None,
            TimelineEventProvenance.LIVE,
            selected_b_json,
            None,
        )
        oversized_a = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:1")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 1),
            room_a,
            1,
            3,
            None,
            TimelineEventProvenance.LIVE,
            oversized_a_json,
            None,
        )
        global_account_data = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:2")),
            RecordKind.GLOBAL_ACCOUNT_DATA,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 2),
            None,
            None,
            None,
            None,
            None,
            global_json,
            None,
        )
        presence = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:3")),
            RecordKind.PRESENCE,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 3),
            None,
            None,
            None,
            None,
            None,
            presence_json,
            None,
        )
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        allowed_work = (
            (old_loss, 0),
            (lifecycle, 3),
            (capacity_loss, 4),
            (selected_b, None),
            (global_account_data, 5),
            (presence, 6),
        )
        allowed_payload_sizes = tuple(
            len(
                _expected_planned_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    frame=normalized,
                    value=value,
                    ordinal=ordinal,
                    revision=revision,
                )
            )
            for value, ordinal in allowed_work
        )
        first_work_by_id = {str(row[0]): row for row in first_work}
        release_payload_sizes = tuple(
            len(
                _expected_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    transport_kind=normalized.origin.transport,
                    frame_id=UUID(str(first_work_by_id[value.record_id][3])),
                    value=value,
                    status="ready",
                    ready_revision=revision,
                    ready_ordinal=ordinal,
                    created_revision=int(first_work_by_id[value.record_id][9]),
                )
            )
            for ordinal, value in enumerate(first_a_held, 1)
        )
        max_record_bytes = max(*allowed_payload_sizes, *release_payload_sizes)
        oversized_payload_size = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=oversized_a,
                ordinal=4,
                revision=revision,
            )
        )
        assert oversized_payload_size > max_record_bytes

        old_graph = _materializer_storage_graph(journal)
        old_owner, old_source, old_frames, old_aggregates, old_work = old_graph
        assert old_owner.revision == 3
        assert old_aggregates == aggregate_rows
        assert old_work == first_work
        old_frames_by_id = {str(row[0]): row for row in old_frames}
        assert old_frames_by_id[str(first.frame_id)] == first_raw_before
        selected_before = old_frames_by_id[str(selected.frame_id)]
        assert selected_before[6] is None
        statements.clear()
        try:
            result = _materialize(
                journal,
                limits=replace(
                    MaterializerLimits(),
                    max_record_canonical_bytes=max_record_bytes,
                ),
            )
        except JournalIntegrityError as error:
            if str(error) != "planned Work record exceeds the canonical byte limit":
                raise AssertionError("unexpected successor oversize RED") from error
            assert _materializer_storage_graph(journal) == old_graph
            assert _materializer_dml(statements) == ()
            raise
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            4,
        )

        owner, source, frames, aggregates, work = _materializer_storage_graph(journal)
        assert owner == replace(old_owner, revision=4)
        assert source == old_source
        expected_a_aggregate = RoomAggregateValue(room_a_continuity, 3, 4, None)
        expected_b_aggregate = RoomAggregateValue(
            first_b_continuity,
            2,
            4,
            first_b_hydration,
        )
        assert tuple(row[:3] for row in aggregates) == (
            (room_a, 4, None),
            (room_b, 4, "hydration"),
        )
        for row, expected in zip(
            aggregates,
            (expected_a_aggregate, expected_b_aggregate),
            strict=True,
        ):
            plaintext, stored = _decode_aggregate(journal, row)
            assert stored == expected
            assert plaintext == _rows()._canonical_room_aggregate_plaintext(expected)

        held_expected = tuple(
            sorted(
                (
                    _ExpectedMaterializerWork(
                        first_b_held,
                        "held",
                        first.frame_id,
                        None,
                        None,
                        2,
                    ),
                    _ExpectedMaterializerWork(
                        selected_b,
                        "held",
                        selected.frame_id,
                        None,
                        None,
                        4,
                    ),
                ),
                key=_expected_materializer_work_id,
            )
        )
        expected_work = (
            *held_expected,
            _ExpectedMaterializerWork(old_loss, "ready", selected.frame_id, 4, 0, 4),
            _ExpectedMaterializerWork(
                first_a_held[0],
                "ready",
                first.frame_id,
                4,
                1,
                2,
            ),
            _ExpectedMaterializerWork(
                first_a_held[1],
                "ready",
                first.frame_id,
                4,
                2,
                2,
            ),
            _ExpectedMaterializerWork(
                lifecycle,
                "ready",
                selected.frame_id,
                4,
                3,
                4,
            ),
            _ExpectedMaterializerWork(
                capacity_loss,
                "ready",
                selected.frame_id,
                4,
                4,
                4,
            ),
            _ExpectedMaterializerWork(
                global_account_data,
                "ready",
                selected.frame_id,
                4,
                5,
                4,
            ),
            _ExpectedMaterializerWork(
                presence,
                "ready",
                selected.frame_id,
                4,
                6,
                4,
            ),
        )
        _assert_exact_materializer_work(journal, work, expected_work, old_work)
        assert oversized_a.record_id not in {str(row[0]) for row in work}
        assert tuple(row[8] for row in work if row[2] == "ready") == tuple(range(7))
        work_by_id = {str(row[0]): row for row in work}
        old_work_by_id = {str(row[0]): row for row in old_work}
        assert (
            work_by_id[first_b_held.record_id] == old_work_by_id[first_b_held.record_id]
        )

        frames_by_id = {str(row[0]): row for row in frames}
        assert frames_by_id[str(first.frame_id)] == first_raw_before
        selected_after = frames_by_id[str(selected.frame_id)]
        assert selected_after[:6] == selected_before[:6]
        assert selected_after[6] == 4
        assert selected_after[7] != selected_before[7]
        assert journal.load_frame(first.frame_id) == first
        assert journal.load_frame(selected.frame_id) == selected
        assert (
            selected_after[7]
            == hashlib.sha256(
                _canonical_expected_drain_header(journal, selected_after, 4)
            ).digest()
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


@pytest.mark.parametrize(
    "boundary",
    ["old-loss", "lifecycle"],
    ids=("old-loss", "lifecycle"),
)
def test_materializer_retirement_mandatory_record_oversize_fails_before_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    boundary: str,
) -> None:
    statements: list[str] = []
    case = _prepare_materializer_atomicity_case(
        tmp_path,
        _ATOMICITY_RETIREMENT_SLIDING_CRYPTO,
        statements=statements,
    )
    bootstrap = case.bootstrap
    journal = bootstrap._journal
    try:
        room_id = "!unsupported:example.org"
        origin = RecordOrigin(TransportKind.SLIDING, 0, 1, 0)
        stream_id = journal.load_owner().stream_id
        old_loss_without_id = LossRecord(
            "",
            origin,
            room_id,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        old_loss = replace(
            old_loss_without_id,
            loss_id=_loss_id(stream_id, old_loss_without_id),
        )
        capacity_loss_without_id = LossRecord(
            "",
            origin,
            room_id,
            1,
            LossReason.EVENT_LIMIT,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        capacity_loss = replace(
            capacity_loss_without_id,
            loss_id=_loss_id(stream_id, capacity_loss_without_id),
        )
        lifecycle = EventRecord(
            str(uuid5(case.selected.frame_id, f"lifecycle:{room_id}:0:1")),
            RecordKind.ROOM_LIFECYCLE,
            origin,
            room_id,
            1,
            1,
            None,
            None,
            b'{"membership":"leave","membership_epoch":1,'
            b'"previous_membership_epoch":0}',
            None,
        )
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        capacity_loss_bytes = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=case.normalized,
                value=capacity_loss,
                ordinal=3,
                revision=revision,
            )
        )
        old_loss_bytes = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=case.normalized,
                value=old_loss,
                ordinal=0,
                revision=revision,
            )
        )
        lifecycle_bytes = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=case.normalized,
                value=lifecycle,
                ordinal=2,
                revision=revision,
            )
        )
        assert capacity_loss_bytes <= old_loss_bytes < lifecycle_bytes
        limit = old_loss_bytes - 1 if boundary == "old-loss" else lifecycle_bytes - 1
        if boundary == "lifecycle":
            assert limit >= old_loss_bytes
        else:
            assert boundary == "old-loss"

        proposal = reduce_staged_frame(
            stream_id,
            case.selected.frame_id,
            case.normalized,
            (_decode_aggregate(journal, _aggregate_rows(journal)[0])[1].continuity,),
        )
        assert len(proposal.room_proposals) == 1
        room = proposal.room_proposals[0]
        assert room.retirement_epoch == 0
        assert room.release is RecoveryRelease.LOSS_THEN_HELD
        assert tuple(descriptor.route for descriptor in proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_RETIREMENT,
            DescriptorRoute.READY,
        )
        old_graph = _materializer_storage_graph(journal)
        raw_before = _frame_storage_row(journal, case.selected.frame_id)
        assert raw_before is not None
        assert raw_before[6] is None
        assert journal.load_frame(case.selected.frame_id) == case.selected

        def reject_writer(_self: object) -> object:
            raise AssertionError("oversized mandatory record entered journal_write")

        statements.clear()
        monkeypatch.setattr(type(journal._owner), "journal_write", reject_writer)
        with pytest.raises(
            JournalIntegrityError,
            match="planned Work record exceeds the canonical byte limit",
        ):
            _materialize(
                journal,
                limits=replace(
                    MaterializerLimits(),
                    max_record_canonical_bytes=limit,
                ),
            )

        assert _materializer_storage_graph(journal) == old_graph
        assert _frame_storage_row(journal, case.selected.frame_id) == raw_before
        assert journal.load_frame(case.selected.frame_id) == case.selected
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


def test_materializer_retirement_global_oversize_fails_before_writer(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.SLIDING,
        statements=statements,
    )
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
        assert first_proposal.room_proposals[0].hydration is not None
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        aggregate_row = _aggregate_rows(journal)[0]
        _, first_aggregate = _decode_aggregate(journal, aggregate_row)
        assert type(first_aggregate) is RoomAggregateValue
        selected, normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.SLIDING,
            2,
            rooms=(("!unsupported:example.org", "leave", "x" * 256),),
            crypto=True,
            global_tail=True,
            global_padding_bytes=512,
        )
        room_id = "!unsupported:example.org"
        origin = RecordOrigin(TransportKind.SLIDING, 0, 1, 0)
        timeline_json = (
            b'{"content":{"body":"'
            + (b"x" * 256)
            + b'","msgtype":"m.text"},"type":"m.room.message"}'
        )
        global_json = (
            b'{"content":{"generation":2,"index":0,"padding":"'
            + (b"x" * 512)
            + b'"},"type":"m.push_rules"}'
        )
        presence_json = (
            b'{"content":{"presence":"online"},'
            b'"sender":"@friend:example.org","type":"m.presence"}'
        )
        assert normalized.origin == origin
        proposal = reduce_staged_frame(
            journal.load_owner().stream_id,
            selected.frame_id,
            normalized,
            (first_aggregate.continuity,),
        )
        room = proposal.room_proposals[0]
        assert room.retirement_epoch == 0
        assert room.release is RecoveryRelease.LOSS_THEN_HELD
        assert tuple(
            (
                descriptor.kind,
                descriptor.room_id,
                descriptor.source_json,
                descriptor.provenance,
                descriptor.descriptor_key,
                descriptor.route,
            )
            for descriptor in proposal.descriptors
        ) == (
            (
                RecordKind.TIMELINE,
                room_id,
                timeline_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:0",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            ),
            (
                RecordKind.GLOBAL_ACCOUNT_DATA,
                None,
                global_json,
                None,
                f"frame:{selected.frame_id}:1",
                DescriptorRoute.READY,
            ),
            (
                RecordKind.PRESENCE,
                None,
                presence_json,
                None,
                f"frame:{selected.frame_id}:2",
                DescriptorRoute.READY,
            ),
        )
        old_loss_without_id = LossRecord(
            "",
            origin,
            room_id,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        old_loss = replace(
            old_loss_without_id,
            loss_id=_loss_id(journal.load_owner().stream_id, old_loss_without_id),
        )
        lifecycle = EventRecord(
            str(uuid5(selected.frame_id, f"lifecycle:{room_id}:0:1")),
            RecordKind.ROOM_LIFECYCLE,
            origin,
            room_id,
            1,
            1,
            None,
            None,
            b'{"membership":"leave","membership_epoch":1,'
            b'"previous_membership_epoch":0}',
            None,
        )
        successor = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:0")),
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.SLIDING, 0, 1, 0),
            room_id,
            1,
            2,
            None,
            TimelineEventProvenance.LIVE,
            timeline_json,
            None,
        )
        capacity_loss_without_id = LossRecord(
            "",
            origin,
            room_id,
            1,
            LossReason.OVERSIZED_EVENT,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        capacity_loss = replace(
            capacity_loss_without_id,
            loss_id=_loss_id(
                journal.load_owner().stream_id,
                capacity_loss_without_id,
            ),
        )
        oversized_global = EventRecord(
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
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        allowed_work = (
            (old_loss, 0),
            (lifecycle, 2),
            (capacity_loss, 3),
            (presence, 4),
        )
        allowed_sizes = tuple(
            len(
                _expected_planned_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    frame=normalized,
                    value=value,
                    ordinal=ordinal,
                    revision=revision,
                )
            )
            for value, ordinal in allowed_work
        )
        held_row = next(row for row in _work_rows(journal) if row[2] == "held")
        held_value = _decode_event_work(journal, held_row)[1]
        release_size = len(
            _expected_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                transport_kind=normalized.origin.transport,
                frame_id=UUID(str(held_row[3])),
                value=held_value,
                status="ready",
                ready_revision=revision,
                ready_ordinal=1,
                created_revision=int(held_row[9]),
            )
        )
        max_record_bytes = max(*allowed_sizes, release_size)
        successor_size = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=successor,
                ordinal=3,
                revision=revision,
            )
        )
        oversized_global_size = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=oversized_global,
                ordinal=3,
                revision=revision,
            )
        )
        assert successor_size > max_record_bytes
        assert oversized_global_size > max_record_bytes

        old_graph = _materializer_storage_graph(journal)
        raw_before = _frame_storage_row(journal, selected.frame_id)
        assert raw_before is not None
        assert raw_before[6] is None
        assert journal.load_frame(selected.frame_id) == selected

        def reject_writer(_self: object) -> object:
            raise AssertionError("oversized global entered journal_write")

        planner_module = importlib.import_module("nio.store._sync_journal_plan")
        real_canonical_work_plaintext = getattr(
            planner_module,
            "_canonical_work_plaintext",
        )
        encoded_work_ids: list[str] = []

        def observe_canonical_work_plaintext(
            kind: str,
            value: EventRecord | LossRecord,
        ) -> bytes:
            encoded_work_ids.append(
                value.record_id if type(value) is EventRecord else value.loss_id
            )
            return real_canonical_work_plaintext(kind, value)

        statements.clear()
        monkeypatch.setattr(type(journal._owner), "journal_write", reject_writer)
        with monkeypatch.context() as guard:
            guard.setattr(
                planner_module,
                "_canonical_work_plaintext",
                observe_canonical_work_plaintext,
            )
            with pytest.raises(
                JournalIntegrityError,
                match="planned Work record exceeds the canonical byte limit",
            ):
                _materialize(
                    journal,
                    limits=replace(
                        MaterializerLimits(),
                        max_record_canonical_bytes=max_record_bytes,
                    ),
                )

        assert successor.record_id in encoded_work_ids
        assert oversized_global.record_id in encoded_work_ids
        assert encoded_work_ids.index(successor.record_id) < encoded_work_ids.index(
            oversized_global.record_id
        )

        assert _materializer_storage_graph(journal) == old_graph
        assert _frame_storage_row(journal, selected.frame_id) == raw_before
        assert journal.load_frame(selected.frame_id) == selected
        assert _materializer_dml(statements) == ()
    finally:
        bootstrap.close()


@pytest.mark.parametrize("excess", [0, 1], ids=("exact", "one-over"))
def test_materializer_retirement_replacement_obeys_immutable_total_count_boundary(
    tmp_path: Path,
    excess: int,
) -> None:
    room_id = "!retirement-total:example.org"
    first_origin = RecordOrigin(TransportKind.SLIDING, 0, 0, 0)
    selected_origin = RecordOrigin(TransportKind.SLIDING, 0, 1, 0)
    first_json = (
        b'{"content":{"body":"total-old","msgtype":"m.text"},"type":"m.room.message"}'
    )
    successor_json = (
        b'{"content":{"body":"'
        + (b"x" * 512)
        + b'","msgtype":"m.text"},"type":"m.room.message"}'
    )
    global_json = (
        b'{"content":{"generation":2,"index":0,"padding":""},"type":"m.push_rules"}'
    )
    presence_json = (
        b'{"content":{"presence":"online"},'
        b'"sender":"@friend:example.org","type":"m.presence"}'
    )
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.SLIDING,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        stream_id = journal.load_owner().stream_id
        first, first_normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.SLIDING,
            1,
            rooms=((room_id, "join", "total-old"),),
            crypto=True,
        )
        assert first_normalized.origin == first_origin
        first_hydration_id = uuid5(
            stream_id,
            f"hydrate:{room_id}:0:{first.frame_id}",
        )
        first_continuity = RoomContinuity(
            room_id,
            0,
            "join",
            None,
            None,
            first_hydration_id,
        )
        assert (
            reduce_staged_frame(
                stream_id,
                first.frame_id,
                first_normalized,
                (),
            )
            .descriptors[0]
            .route
            is DescriptorRoute.HOLD_FOR_HYDRATION
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        first_raw_before = _frame_storage_row(journal, first.frame_id)
        assert first_raw_before is not None
        first_aggregate_row = _aggregate_rows(journal)[0]
        _, first_aggregate = _decode_aggregate(journal, first_aggregate_row)
        assert first_aggregate == RoomAggregateValue(
            first_continuity,
            1,
            2,
            HydrationIntent(first_hydration_id, first_origin),
        )
        first_held = EventRecord(
            str(uuid5(first.frame_id, f"event:frame:{first.frame_id}:0")),
            RecordKind.TIMELINE,
            first_origin,
            room_id,
            0,
            0,
            None,
            TimelineEventProvenance.HISTORY,
            first_json,
            None,
        )
        first_work = _work_rows(journal)
        assert len(first_work) == 1
        assert _decode_event_work(journal, first_work[0]) == (
            _expected_event_work_plaintext(first_held),
            first_held,
        )

        selected, normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.SLIDING,
            2,
            rooms=((room_id, "leave", "x" * 512),),
            crypto=True,
            global_tail=True,
        )
        assert normalized.origin == selected_origin
        proposal = reduce_staged_frame(
            stream_id,
            selected.frame_id,
            normalized,
            (first_continuity,),
        )
        successor_continuity = RoomContinuity(
            room_id,
            1,
            "leave",
            None,
            None,
            None,
        )
        assert len(proposal.room_proposals) == 1
        room = proposal.room_proposals[0]
        assert room.before == first_continuity
        assert room.after == successor_continuity
        assert room.hydration is None
        assert room.recovery is None
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
        assert tuple(
            (
                descriptor.kind,
                descriptor.room_id,
                descriptor.source_json,
                descriptor.provenance,
                descriptor.descriptor_key,
                descriptor.route,
            )
            for descriptor in proposal.descriptors
        ) == (
            (
                RecordKind.TIMELINE,
                room_id,
                successor_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:0",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            ),
            (
                RecordKind.GLOBAL_ACCOUNT_DATA,
                None,
                global_json,
                None,
                f"frame:{selected.frame_id}:1",
                DescriptorRoute.READY,
            ),
            (
                RecordKind.PRESENCE,
                None,
                presence_json,
                None,
                f"frame:{selected.frame_id}:2",
                DescriptorRoute.READY,
            ),
        )
        old_loss_without_id = LossRecord(
            "",
            selected_origin,
            room_id,
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        old_loss = replace(
            old_loss_without_id,
            loss_id=_loss_id(stream_id, old_loss_without_id),
        )
        lifecycle = EventRecord(
            str(uuid5(selected.frame_id, f"lifecycle:{room_id}:0:1")),
            RecordKind.ROOM_LIFECYCLE,
            selected_origin,
            room_id,
            1,
            1,
            None,
            None,
            b'{"membership":"leave","membership_epoch":1,'
            b'"previous_membership_epoch":0}',
            None,
        )
        successor = EventRecord(
            str(uuid5(selected.frame_id, f"event:frame:{selected.frame_id}:0")),
            RecordKind.TIMELINE,
            selected_origin,
            room_id,
            1,
            2,
            None,
            TimelineEventProvenance.LIVE,
            successor_json,
            None,
        )
        capacity_loss_without_id = LossRecord(
            "",
            selected_origin,
            room_id,
            1,
            LossReason.OVERSIZED_EVENT,
            LossBoundary(None, None, None, None),
            b"{}",
        )
        capacity_loss = replace(
            capacity_loss_without_id,
            loss_id=_loss_id(stream_id, capacity_loss_without_id),
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
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        required_work = (
            (old_loss, 0),
            (lifecycle, 2),
            (capacity_loss, 3),
            (global_account_data, 4),
            (presence, 5),
        )
        required_payload_sizes = tuple(
            len(
                _expected_planned_stored_work_payload(
                    account_id=journal.account_id,
                    stream_id=owner_before.stream_id,
                    frame=normalized,
                    value=value,
                    ordinal=ordinal,
                    revision=revision,
                )
            )
            for value, ordinal in required_work
        )
        release_payload_size = len(
            _expected_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                transport_kind=normalized.origin.transport,
                frame_id=UUID(str(first_work[0][3])),
                value=first_held,
                status="ready",
                ready_revision=revision,
                ready_ordinal=1,
                created_revision=int(first_work[0][9]),
            )
        )
        max_record_bytes = max(*required_payload_sizes, release_payload_size)
        successor_payload_size = len(
            _expected_planned_stored_work_payload(
                account_id=journal.account_id,
                stream_id=owner_before.stream_id,
                frame=normalized,
                value=successor,
                ordinal=3,
                revision=revision,
            )
        )
        assert successor_payload_size > max_record_bytes

        hard = MaterializerLimits()
        assert hard.max_total_work_count == 20_000
        replacement_insert_count = 5
        seed_count = hard.max_total_work_count - 1 - replacement_insert_count + excess
        assert seed_count == 19_994 + excess
        seed_work_ids: list[str] = []
        insert_sql = (
            "INSERT INTO NioIngestWork("
            "account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload, payload_sha256) "
            "VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)"
        )
        with journal._owner.journal_write():
            for index in range(seed_count):
                seed = EventRecord(
                    str(uuid5(first.frame_id, f"retirement-total-seed:{index}")),
                    RecordKind.GLOBAL_ACCOUNT_DATA,
                    replace(first_origin, frame_index=index),
                    None,
                    None,
                    None,
                    None,
                    None,
                    b"{}",
                    None,
                )
                seed_work_ids.append(seed.record_id)
                inserted = journal._execute(
                    insert_sql,
                    _verified_event_work_values(
                        journal,
                        seed,
                        frame_id=first.frame_id,
                        ready_revision=2,
                        ready_ordinal=index,
                        created_revision=2,
                    ),
                )
                assert inserted.rowcount == 1

        selected_raw_before = _frame_storage_row(journal, selected.frame_id)
        assert selected_raw_before is not None
        assert selected_raw_before[6] is None
        assert journal.load_frame(selected.frame_id) == selected
        old_graph = _materializer_storage_graph(journal)
        assert len(old_graph[4]) + replacement_insert_count == (
            hard.max_total_work_count + excess
        )
        statements.clear()
        limits = replace(
            hard,
            max_record_canonical_bytes=max_record_bytes,
            max_held_work_count=1,
            max_held_work_canonical_bytes=1,
            max_ready_work_count=1,
            max_ready_work_canonical_bytes=1,
            max_total_work_count=1,
            max_total_work_canonical_bytes=1,
        )

        result = _materialize(journal, limits=limits)

        if excess:
            assert result == MaterializeResult(
                MaterializeStatus.AT_CAPACITY,
                selected.frame_id,
                None,
            )
            assert _materializer_storage_graph(journal) == old_graph
            assert _frame_storage_row(journal, first.frame_id) == first_raw_before
            assert _frame_storage_row(journal, selected.frame_id) == selected_raw_before
            assert _materializer_dml(statements) == ()
            removed_seed_id = seed_work_ids.pop()
            with journal._owner.journal_write():
                removed = journal._execute(
                    "DELETE FROM NioIngestWork WHERE account_id = ? AND work_id = ?",
                    (journal.account_id, removed_seed_id),
                )
                assert removed.rowcount == 1
            commit_old_graph = _materializer_storage_graph(journal)
            assert len(commit_old_graph[4]) + replacement_insert_count == (
                hard.max_total_work_count
            )
            statements.clear()
            result = _materialize(journal, limits=limits)
        else:
            commit_old_graph = old_graph
        assert result == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            selected.frame_id,
            4,
        )

        owner, source, frames, aggregates, work = _materializer_storage_graph(journal)
        old_owner, old_source, _old_frames, _old_aggregates, commit_old_work = (
            commit_old_graph
        )
        assert owner == replace(old_owner, revision=4)
        assert source == old_source
        assert len(work) == hard.max_total_work_count
        assert len(aggregates) == 1
        aggregate_plaintext, stored_aggregate = _decode_aggregate(
            journal,
            aggregates[0],
        )
        expected_aggregate = RoomAggregateValue(
            successor_continuity,
            2,
            4,
            None,
        )
        assert stored_aggregate == expected_aggregate
        assert aggregate_plaintext == _rows()._canonical_room_aggregate_plaintext(
            expected_aggregate
        )
        expected_relevant = (
            _ExpectedMaterializerWork(
                old_loss,
                "ready",
                selected.frame_id,
                4,
                0,
                4,
            ),
            _ExpectedMaterializerWork(
                first_held,
                "ready",
                first.frame_id,
                4,
                1,
                2,
            ),
            _ExpectedMaterializerWork(
                lifecycle,
                "ready",
                selected.frame_id,
                4,
                2,
                4,
            ),
            _ExpectedMaterializerWork(
                capacity_loss,
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
        relevant_ids = {
            _expected_materializer_work_id(item) for item in expected_relevant
        }
        relevant_rows = tuple(row for row in work if str(row[0]) in relevant_ids)
        _assert_exact_materializer_work(
            journal,
            relevant_rows,
            expected_relevant,
            commit_old_work,
        )
        assert successor.record_id not in {str(row[0]) for row in work}
        work_by_id = {str(row[0]): row for row in work}
        old_work_by_id = {str(row[0]): row for row in commit_old_work}
        assert set(seed_work_ids) <= work_by_id.keys()
        assert all(
            work_by_id[work_id] == old_work_by_id[work_id] for work_id in seed_work_ids
        )
        assert tuple(
            row[8] for row in work if row[7] == 4 and row[2] == "ready"
        ) == tuple(range(6))

        frames_by_id = {str(row[0]): row for row in frames}
        assert frames_by_id[str(first.frame_id)] == first_raw_before
        selected_after = frames_by_id[str(selected.frame_id)]
        assert selected_after[:6] == selected_raw_before[:6]
        assert selected_after[6] == 4
        assert selected_after[7] != selected_raw_before[7]
        assert journal.load_frame(first.frame_id) == first
        assert journal.load_frame(selected.frame_id) == selected
        assert (
            selected_after[7]
            == hashlib.sha256(
                _canonical_expected_drain_header(journal, selected_after, 4)
            ).digest()
        )
        committed_graph = _materializer_storage_graph(journal)
        assert _materialize(journal, limits=limits) == MaterializeResult(
            MaterializeStatus.IDLE,
            None,
            None,
        )
        assert _materializer_storage_graph(journal) == committed_graph
    finally:
        bootstrap.close()


def test_materializer_rejects_two_pending_hydration_retirements_before_writer(
    tmp_path: Path,
) -> None:
    room_a = "!a-retire:example.org"
    room_b = "!b-retire:example.org"
    selected_origin = RecordOrigin(TransportKind.CLASSIC, 0, 1, 0)
    selected_a_json = (
        b'{"content":{"body":"classic-a-retired","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    selected_b_json = (
        b'{"content":{"body":"classic-b-retired","msgtype":"m.text"},'
        b'"type":"m.room.message"}'
    )
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        tmp_path,
        TransportKind.CLASSIC,
        statements=statements,
    )
    journal = bootstrap._journal
    try:
        stream_id = journal.load_owner().stream_id
        first, first_normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            rooms=(
                (room_a, "join", "classic-a-held"),
                (room_b, "join", "classic-b-held"),
            ),
        )
        first_origin = RecordOrigin(TransportKind.CLASSIC, 0, 0, 0)
        first_continuities = tuple(
            RoomContinuity(
                room_id,
                0,
                "join",
                None,
                None,
                uuid5(stream_id, f"hydrate:{room_id}:0:{first.frame_id}"),
            )
            for room_id in (room_a, room_b)
        )
        first_proposal = reduce_staged_frame(
            stream_id,
            first.frame_id,
            first_normalized,
            (),
        )
        assert tuple(room.after for room in first_proposal.room_proposals) == (
            first_continuities
        )
        assert tuple(room.hydration for room in first_proposal.room_proposals) == tuple(
            HydrationIntent(continuity.hydration_id, first_origin)
            for continuity in first_continuities
            if continuity.hydration_id is not None
        )
        assert tuple(descriptor.route for descriptor in first_proposal.descriptors) == (
            DescriptorRoute.HOLD_FOR_HYDRATION,
            DescriptorRoute.HOLD_FOR_HYDRATION,
        )
        assert _materialize(journal) == MaterializeResult(
            MaterializeStatus.MATERIALIZED,
            first.frame_id,
            2,
        )
        assert _frame_storage_row(journal, first.frame_id) is None
        aggregate_rows = _aggregate_rows(journal)
        assert tuple(row[:3] for row in aggregate_rows) == (
            (room_a, 2, "hydration"),
            (room_b, 2, "hydration"),
        )
        for row, continuity in zip(
            aggregate_rows,
            first_continuities,
            strict=True,
        ):
            assert continuity.hydration_id is not None
            _, aggregate = _decode_aggregate(journal, row)
            assert aggregate == RoomAggregateValue(
                continuity,
                1,
                2,
                HydrationIntent(continuity.hydration_id, first_origin),
            )
        assert len(_work_rows(journal)) == 2
        assert all(row[2] == "held" for row in _work_rows(journal))

        selected, normalized = _stage_discovery_rooms_frame(
            journal,
            TransportKind.CLASSIC,
            2,
            rooms=(
                (room_a, "leave", "classic-a-retired"),
                (room_b, "leave", "classic-b-retired"),
            ),
        )
        assert normalized.origin == selected_origin
        assert tuple(segment.room_id for segment in normalized.room_segments) == (
            room_a,
            room_b,
        )
        assert tuple(segment.timeline_json for segment in normalized.room_segments) == (
            (selected_a_json,),
            (selected_b_json,),
        )
        proposal = reduce_staged_frame(
            stream_id,
            selected.frame_id,
            normalized,
            first_continuities,
        )
        assert proposal.crypto_deferred is False
        for room, before in zip(
            proposal.room_proposals,
            first_continuities,
            strict=True,
        ):
            assert room.before == before
            assert room.after == RoomContinuity(
                before.room_id,
                1,
                "leave",
                None,
                None,
                None,
            )
            assert room.hydration is None
            assert room.recovery is None
            assert room.retirement_epoch == 0
            assert room.losses == (
                LossProposal(
                    before.room_id,
                    0,
                    LossReason.UNVERIFIABLE,
                    LossBoundary(None, None, None, None),
                ),
            )
            assert room.release is RecoveryRelease.LOSS_THEN_HELD
        assert tuple(
            (
                descriptor.kind,
                descriptor.room_id,
                descriptor.source_json,
                descriptor.provenance,
                descriptor.descriptor_key,
                descriptor.route,
            )
            for descriptor in proposal.descriptors
        ) == (
            (
                RecordKind.TIMELINE,
                room_a,
                selected_a_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:0",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            ),
            (
                RecordKind.TIMELINE,
                room_b,
                selected_b_json,
                TimelineEventProvenance.LIVE,
                f"frame:{selected.frame_id}:1",
                DescriptorRoute.HOLD_FOR_RETIREMENT,
            ),
        )

        old_graph = _materializer_storage_graph(journal)
        selected_before = _frame_storage_row(journal, selected.frame_id)
        assert old_graph[0].revision == 3
        assert selected_before is not None
        assert selected_before[6] is None
        assert journal.load_frame(selected.frame_id) == selected
        statements.clear()
        with pytest.raises(
            JournalIntegrityError,
            match="^this checkpoint retires exactly one room$",
        ):
            _materialize(journal)
        assert _materializer_storage_graph(journal) == old_graph
        assert _frame_storage_row(journal, selected.frame_id) == selected_before
        assert journal.load_frame(selected.frame_id) == selected
        assert _materializer_dml(statements) == ()
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
    if scenario == _ATOMICITY_POST_HYDRATION_CLASSIC_READY:
        return (
            "meta_revision_epoch_cas",
            "aggregate_update",
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


def _kill_materializer_at_boundary(
    store_path: Path,
    scenario: str,
    boundary: str,
    occurrence: int,
    sequence_path: Path,
) -> None:
    transport = _atomicity_transport(scenario)
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

    transport = _atomicity_transport(scenario)
    reopened = _open_discovery_journal(store_path, transport)
    journal = reopened._journal
    try:
        sliding_reopens = 2 if transport is TransportKind.SLIDING else 0
        if boundary == "commit":
            _assert_materializer_committed_graph(
                journal,
                scenario,
                case,
                old_graph,
                sliding_reopens_before=sliding_reopens // 2,
                sliding_reopens_after=sliding_reopens // 2,
            )
            committed_graph = _materializer_storage_graph(journal)
            assert _materialize(journal) == MaterializeResult(
                MaterializeStatus.IDLE,
                None,
                None,
            )
            assert _materializer_storage_graph(journal) == committed_graph
        else:
            _assert_materializer_reopened_graph(
                journal,
                old_graph,
                sliding_reopens=sliding_reopens,
            )
            assert _materialize(journal) == MaterializeResult(
                MaterializeStatus.MATERIALIZED,
                selected.frame_id,
                old_graph[0].revision + sliding_reopens + 1,
            )
            _assert_materializer_committed_graph(
                journal,
                scenario,
                case,
                old_graph,
                sliding_reopens_before=sliding_reopens,
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
        selected_decode_scopes: list[str | None] = []
        real_decode_frame_row = journal._decode_frame_row

        def observe_selected_decode(
            frame_id: UUID,
            row: object,
            current_owner: OwnerView,
            *,
            drain_header_authenticated: bool = False,
        ) -> StagedFrame:
            if frame_id == selected.frame_id:
                selected_decode_scopes.append(journal._owner._outer_scope)
            return real_decode_frame_row(
                frame_id,
                row,
                current_owner,
                drain_header_authenticated=drain_header_authenticated,
            )

        monkeypatch.setattr(journal, "_decode_frame_row", observe_selected_decode)
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
        assert selected_decode_scopes == ["read"]
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
    ("fence", "error_type", "scenario"),
    [
        pytest.param(
            "revision", JournalConflictError, _ATOMICITY_H1_CLASSIC_PLAIN, id="revision"
        ),
        pytest.param(
            "writer-epoch",
            LocalProtocolError,
            _ATOMICITY_H1_CLASSIC_PLAIN,
            id="writer-epoch",
        ),
        pytest.param(
            "lock-file", LocalProtocolError, _ATOMICITY_H1_CLASSIC_PLAIN, id="lock-file"
        ),
        pytest.param(
            "database-inode",
            LocalProtocolError,
            _ATOMICITY_H1_CLASSIC_PLAIN,
            id="database-inode",
        ),
        pytest.param(
            "revision",
            JournalConflictError,
            _ATOMICITY_POST_HYDRATION_CLASSIC_READY,
            id="post_hydration_ready",
        ),
    ],
)
def test_materializer_writer_boundary_fence_race_writes_no_partial_graph(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    fence: str,
    error_type: type[BaseException],
    scenario: str,
) -> None:
    statements: list[str] = []
    case = _prepare_materializer_atomicity_case(
        tmp_path, scenario, statements=statements
    )
    bootstrap = case.bootstrap
    journal = bootstrap._journal
    try:
        selected = case.selected
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
            scenario,
            case,
            old_graph,
        )
    finally:
        bootstrap.close()


def _expected_plaintext_materializer_envelope(
    *,
    row_kind: str,
    owner: OwnerView,
    clear_fields: tuple[tuple[str, object], ...],
    value: object,
) -> bytes:
    """Independent canonical envelope for persisted materializer rows."""

    return json.dumps(
        {
            "schema_version": 1,
            "row_kind": row_kind,
            "account_id": owner.account_id,
            "stream_id": str(owner.stream_id),
            "transport_kind": owner.transport_kind.value,
            **dict(clear_fields),
            "value": value,
        },
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _expected_plaintext_aggregate_value(value: RoomAggregateValue) -> object:
    hydration = value.pending_hydration
    continuity = value.continuity
    return {
        "continuity": {
            "baseline": None,
            "gap": None,
            "hydration_id": str(hydration.hydration_id) if hydration else None,
            "membership": continuity.membership,
            "membership_epoch": continuity.membership_epoch,
            "room_id": continuity.room_id,
        },
        "next_room_sequence": value.next_room_sequence,
        "pending_hydration": (
            {
                "hydration_id": str(hydration.hydration_id),
                "origin": {
                    "frame_index": hydration.origin.frame_index,
                    "origin_type": "transport",
                    "request_id": hydration.origin.request_id,
                    "source_epoch": hydration.origin.source_epoch,
                    "transport": hydration.origin.transport.value,
                },
            }
            if hydration is not None
            else None
        ),
        "updated_revision": value.updated_revision,
    }


def test_v1_aggregate_and_work_store_exact_envelopes_and_swaps_fail_equality(
    tmp_path: Path,
) -> None:
    """RED: stored Work routing is content-bound without claiming tamper proofing."""

    bootstrap = _open_discovery_journal(tmp_path, TransportKind.CLASSIC)
    journal = bootstrap._journal
    try:
        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            room_nonempty=True,
            room_ephemeral=True,
        )
        owner_before = journal.load_owner()
        revision = owner_before.revision + 1
        plan = plan_frame_materialization(
            account_id=owner_before.account_id,
            stream_id=owner_before.stream_id,
            frame=normalized,
            aggregates=(),
            work=(),
            revision=revision,
            limits=MaterializerLimits(),
        )
        assert plan is not None
        assert len(plan.room_values) == 1
        assert len(plan.work_inserts) == 2
        assert _materialize(journal).revision == revision

        with journal._owner.read():
            aggregate_row = journal._execute(
                "SELECT * FROM NioIngestRoomAggregate WHERE account_id = ?",
                (journal.account_id,),
            ).fetchone()
            work_rows = journal._execute(
                "SELECT * FROM NioIngestWork WHERE account_id = ? ORDER BY work_id",
                (journal.account_id,),
            ).fetchall()
        assert aggregate_row is not None
        assert tuple(aggregate_row.keys()) == tuple(
            column[0] for column in _AGGREGATE_COLUMNS
        )
        assert len(work_rows) == 2
        assert all(
            tuple(row.keys()) == tuple(column[0] for column in _WORK_COLUMNS)
            for row in work_rows
        )

        aggregate = plan.room_values[0]
        expected_aggregate = _expected_plaintext_materializer_envelope(
            row_kind="aggregate",
            owner=owner_before,
            clear_fields=(
                ("room_id", aggregate.continuity.room_id),
                ("updated_revision", aggregate.updated_revision),
                (
                    "intent_kind",
                    "hydration" if aggregate.pending_hydration is not None else None,
                ),
            ),
            value=_expected_plaintext_aggregate_value(aggregate),
        )
        assert aggregate_row["payload"] == expected_aggregate
        assert (
            aggregate_row["payload_sha256"]
            == hashlib.sha256(expected_aggregate).digest()
        )

        planned_by_id = {
            value.record_id: (value, ordinal)
            for value, _inner_payload, ordinal in plan.work_inserts
            if type(value) is EventRecord
        }
        assert len(planned_by_id) == 2
        for row in work_rows:
            value, ordinal = planned_by_id[row["work_id"]]
            status = "held" if ordinal is None else "ready"
            expected_work = _expected_plaintext_materializer_envelope(
                row_kind="work",
                owner=owner_before,
                clear_fields=(
                    ("work_id", value.record_id),
                    ("kind", "event"),
                    ("status", status),
                    ("frame_id", str(staged.frame_id)),
                    ("room_id", value.room_id),
                    ("membership_epoch", value.membership_epoch),
                    ("room_sequence", value.room_sequence),
                    ("ready_revision", None if ordinal is None else revision),
                    ("ready_ordinal", ordinal),
                    ("created_revision", revision),
                ),
                value=json.loads(_expected_event_work_plaintext(value)),
            )
            assert row["payload"] == expected_work
            assert row["payload_sha256"] == hashlib.sha256(expected_work).digest()

        other_room_id = "!plaintext-swap:example.org"
        other_aggregate_envelope = json.loads(expected_aggregate)
        other_aggregate_envelope["room_id"] = other_room_id
        other_aggregate_envelope["value"]["continuity"]["room_id"] = other_room_id
        other_aggregate = _canonical_internal(other_aggregate_envelope)
        other_aggregate_sha256 = hashlib.sha256(other_aggregate).digest()
        with journal._owner.journal_write():
            inserted = journal._execute(
                "INSERT INTO NioIngestRoomAggregate(account_id, room_id, "
                "updated_revision, intent_kind, payload, payload_sha256) "
                "VALUES (?, ?, ?, ?, ?, ?)",
                (
                    journal.account_id,
                    other_room_id,
                    aggregate_row["updated_revision"],
                    aggregate_row["intent_kind"],
                    other_aggregate,
                    other_aggregate_sha256,
                ),
            )
            assert inserted.rowcount == 1
            journal._execute(
                "UPDATE NioIngestRoomAggregate SET payload = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND room_id = ?",
                (
                    other_aggregate,
                    other_aggregate_sha256,
                    journal.account_id,
                    aggregate_row["room_id"],
                ),
            )
            journal._execute(
                "UPDATE NioIngestRoomAggregate SET payload = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND room_id = ?",
                (
                    expected_aggregate,
                    hashlib.sha256(expected_aggregate).digest(),
                    journal.account_id,
                    other_room_id,
                ),
            )
        owner = journal.load_owner()
        with journal._owner.read(), pytest.raises(JournalIntegrityError):
            journal._load_room_aggregate(
                owner,
                aggregate_row["room_id"],
            )

        first, second = work_rows
        with journal._owner.journal_write():
            journal._execute(
                "UPDATE NioIngestWork SET payload = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND work_id = ?",
                (
                    second["payload"],
                    second["payload_sha256"],
                    journal.account_id,
                    first["work_id"],
                ),
            )
            journal._execute(
                "UPDATE NioIngestWork SET payload = ?, payload_sha256 = ? "
                "WHERE account_id = ? AND work_id = ?",
                (
                    first["payload"],
                    first["payload_sha256"],
                    journal.account_id,
                    second["work_id"],
                ),
            )
        owner = journal.load_owner()
        with journal._owner.read(), pytest.raises(JournalIntegrityError):
            journal._load_task3_work_inventory(owner)
    finally:
        bootstrap.close()


def _planned_capacity_work_payload(
    journal: SqliteIngestionJournal,
    staged: StagedFrame,
    normalized: SyncFrame,
) -> tuple[bytes, EventRecord, int, OwnerView]:
    owner = journal.load_owner()
    revision = owner.revision + 1
    proposal = reduce_staged_frame(owner.stream_id, normalized.frame_id, normalized, ())
    padded = tuple(
        (index, descriptor)
        for index, descriptor in enumerate(proposal.descriptors)
        if b'"padding"' in descriptor.source_json
    )
    assert len(padded) == 1
    ordinal, descriptor = padded[0]
    assert descriptor.room_id is None
    value = EventRecord(
        str(uuid5(staged.frame_id, f"event:{descriptor.descriptor_key}")),
        descriptor.kind,
        replace(normalized.origin, frame_index=ordinal),
        None,
        None,
        None,
        None,
        None,
        descriptor.source_json,
        None,
    )
    payload = _expected_plaintext_materializer_envelope(
        row_kind="work",
        owner=owner,
        clear_fields=(
            ("work_id", value.record_id),
            ("kind", "event"),
            ("status", "ready"),
            ("frame_id", str(staged.frame_id)),
            ("room_id", value.room_id),
            ("membership_epoch", value.membership_epoch),
            ("room_sequence", value.room_sequence),
            ("ready_revision", revision),
            ("ready_ordinal", ordinal),
            ("created_revision", revision),
        ),
        value=json.loads(_expected_event_work_plaintext(value)),
    )
    return payload, value, ordinal, owner


def _capacity_work_payload_for_record(
    *,
    owner: OwnerView,
    staged: StagedFrame,
    value: EventRecord,
    ordinal: int,
) -> bytes:
    revision = owner.revision + 1
    return _expected_plaintext_materializer_envelope(
        row_kind="work",
        owner=owner,
        clear_fields=(
            ("work_id", value.record_id),
            ("kind", "event"),
            ("status", "ready"),
            ("frame_id", str(staged.frame_id)),
            ("room_id", value.room_id),
            ("membership_epoch", value.membership_epoch),
            ("room_sequence", value.room_sequence),
            ("ready_revision", revision),
            ("ready_ordinal", ordinal),
            ("created_revision", revision),
        ),
        value=json.loads(_expected_event_work_plaintext(value)),
    )


@pytest.mark.parametrize("extra_byte", (0, 1), ids=("exact", "over"))
def test_v1_work_runtime_cap_counts_final_stored_canonical_envelope_before_write(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    extra_byte: int,
) -> None:
    """RED: exact 1 MiB Work is accepted and +1 stays pre-transaction."""

    sizing_path = tmp_path / "sizing"
    sizing_path.mkdir()
    sizing = _open_discovery_journal(sizing_path, TransportKind.CLASSIC)
    try:
        sizing_staged, sizing_normalized = _stage_discovery_frame(
            sizing._journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
            padding_bytes=0,
        )
        base_payload, base_value, base_ordinal, base_owner = (
            _planned_capacity_work_payload(
                sizing._journal,
                sizing_staged,
                sizing_normalized,
            )
        )
    finally:
        sizing.close()

    exact_bytes = 1024 * 1024
    account_padding = (exact_bytes - len(base_payload)) % 4
    sized_owner = replace(
        base_owner,
        account_id=base_owner.account_id + ("x" * account_padding),
    )
    source_envelope = json.loads(base_value.source_json)

    def payload_for_padding(padding: int) -> bytes:
        envelope = json.loads(canonical_json(source_envelope))
        envelope["content"]["padding"] = "x" * padding
        value = replace(base_value, source_json=canonical_json(envelope))
        return _capacity_work_payload_for_record(
            owner=sized_owner,
            staged=sizing_staged,
            value=value,
            ordinal=base_ordinal,
        )

    low = 0
    high = exact_bytes
    while low < high:
        middle = (low + high) // 2
        if len(payload_for_padding(middle)) < exact_bytes:
            low = middle + 1
        else:
            high = middle
    padding_bytes = low
    assert len(payload_for_padding(padding_bytes)) == exact_bytes
    target_bytes = exact_bytes + extra_byte
    account_id = sized_owner.account_id + ("x" * extra_byte)
    actual_path = tmp_path / "actual"
    actual_path.mkdir()
    statements: list[str] = []
    bootstrap = _open_discovery_journal(
        actual_path,
        TransportKind.CLASSIC,
        statements=statements,
        account_id=account_id,
    )
    journal = bootstrap._journal
    try:
        staged, normalized = _stage_discovery_frame(
            journal,
            TransportKind.CLASSIC,
            1,
            global_ready_count=1,
            padding_bytes=padding_bytes,
            account_id=account_id,
        )
        payload, _value, _ordinal, _owner = _planned_capacity_work_payload(
            journal,
            staged,
            normalized,
        )
        assert len(payload) == target_bytes
        owner_before = journal.load_owner()
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
        statements.clear()
        if extra_byte:
            with pytest.raises(JournalIntegrityError):
                _materialize(journal)
            assert writer_entries == 0
            assert journal.load_owner() == owner_before
            assert _materializer_dml(statements) == ()
            return

        assert _materialize(journal).status is MaterializeStatus.MATERIALIZED
        assert writer_entries == 1
        with journal._owner.read():
            rows = journal._execute(
                "SELECT * FROM NioIngestWork WHERE account_id = ?",
                (journal.account_id,),
            ).fetchall()
        assert rows
        assert all(
            tuple(row.keys()) == tuple(column[0] for column in _WORK_COLUMNS)
            for row in rows
        )
        stored_payloads = tuple(bytes(row["payload"]) for row in rows)
        assert payload in stored_payloads
        assert len(payload) == 1024 * 1024
    finally:
        bootstrap.close()
