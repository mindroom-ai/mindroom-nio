"""Task 6 private materializer contract RED tests."""

import base64
import hashlib
import json
from dataclasses import dataclass, replace
from uuid import UUID, uuid5

import pytest

import nio.ingest as ingest
import nio.ingest.classic as classic_module
import nio.ingest.ports as ports_module
import nio.ingest.sliding as sliding_module
import nio.store as store
import nio.store._sync_journal_format as journal_format_module
import nio.store._sync_journal_plan as journal_plan_module
from nio.event_provenance import TimelineEventProvenance
from nio.ingest import source
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.errors import JournalCapacityError, JournalIntegrityError
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
)
from nio.ingest.sliding import (
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
from nio.store._sync_journal_plan import AuthenticatedWork
from nio.store._sync_journal_rows import _canonical_internal, _frame_envelope
from nio.store._sync_journal_values import (
    MaterializeResult,
    MaterializerLimits,
    MaterializeStatus,
    RoomAggregateValue,
)

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
    )


def _ephemeral_envelope(room_id: str, event: dict[str, object]) -> bytes:
    return canonical_json({"event": event, "room_id": room_id})


_AGGREGATE_ROOM_ID = "!aggregate:example.org"
_HYDRATION_ID = UUID("12345678-1234-5678-9234-567812345678")


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
    metadata = _prepared_metadata(
        record.record_id,
        record.effective_event_type,
        record.callback_route,
        phase=record.preparation_phase,
        decryption=record.decryption,
        verified=record.decryption_verified,
        decrypted_kind=record.decrypted_to_device_kind,
    )
    return journal_plan_module.PlannedWork(
        value,
        journal_plan_module._canonical_work_plaintext("event", value, metadata),
        ready_ordinal,
        metadata,
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
    ephemeral_source = _prepared_event("m.receipt")
    room_account_data_source = _prepared_event("m.tag")
    extra_global_source = _prepared_event("org.example.global")
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
            event_type="m.receipt",
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
            RecordKind.GLOBAL_ACCOUNT_DATA,
            extra_global_source,
            event_type="org.example.global",
            route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
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
        global_account_data=(extra_global_source, global_source),
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
    source_json = _prepared_event("m.receipt")
    frame = _prepared_frame(
        ephemeral=(_ephemeral_envelope(_PREPARED_ROOM_ID, json.loads(source_json)),)
    )
    record = _prepared_record(
        0,
        RecordKind.EPHEMERAL,
        source_json,
        event_type="m.receipt",
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
    extra_global_source = _prepared_event("org.example.global")
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
    frame = _prepared_frame(
        room_segments=segments, global_account_data=(extra_global_source,)
    )
    record = _prepared_record(
        0,
        RecordKind.GLOBAL_ACCOUNT_DATA,
        extra_global_source,
        event_type="org.example.global",
        route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
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


def test_prepared_work_decoder_returns_validated_value_metadata_and_bytes(
    monkeypatch,
) -> None:
    value, metadata, plaintext = _prepared_work_decode_case()
    real_loads = json.loads
    wrapper_decodes = 0

    def counted_loads(data, *args, **kwargs):
        nonlocal wrapper_decodes
        if data == plaintext.decode("utf-8"):
            wrapper_decodes += 1
        return real_loads(data, *args, **kwargs)

    monkeypatch.setattr(json, "loads", counted_loads)

    decoded = journal_rows_module._decode_work_plaintext(
        _STREAM_ID, value.record_id, "event", plaintext
    )

    assert decoded.value == value
    assert decoded.metadata == metadata
    authenticated = AuthenticatedWork(
        decoded.value, "ready", len(plaintext), decoded.metadata, plaintext
    )
    assert authenticated.value == value
    assert wrapper_decodes == 1
    assert decoded.plaintext == plaintext
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


@pytest.mark.parametrize(
    ("owner", "stored"),
    (
        (
            ("@planner-事件:example.org", _STREAM_ID, TransportKind.CLASSIC),
            journal_plan_module._StoredWorkRow(
                "$event-事件",
                "event",
                "ready",
                _FRAME_ID,
                "!room-事件:example.org",
                2**63 - 1,
                2**63 - 1,
                2**63 - 1,
                2**63 - 1,
                2**63 - 1,
                b'{"kind":"event","value":{}}',
            ),
        ),
        (
            ("@planner-事件:example.org", _STREAM_ID, TransportKind.SLIDING),
            journal_plan_module._StoredWorkRow(
                "$loss-事件",
                "loss",
                "held",
                _FRAME_ID,
                None,
                None,
                None,
                None,
                None,
                2**63 - 1,
                b'{"kind":"loss","value":{}}',
            ),
        ),
    ),
)
def test_stored_work_size_matches_persisted_envelope(
    owner: tuple[str, UUID, TransportKind],
    stored: journal_plan_module._StoredWorkRow,
) -> None:
    payload, _ = journal_plan_module._row(
        owner,
        "NioIngestWork",
        stored.plaintext,
        header=_canonical_internal(stored.clear_values),
    )

    assert journal_plan_module._stored_work_size(owner, stored) == len(payload)


def test_stored_work_size_does_not_hash_payload(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    owner = (_PLANNER_ACCOUNT_ID, _STREAM_ID, TransportKind.CLASSIC)
    stored = journal_plan_module._StoredWorkRow(
        "$event",
        "event",
        "ready",
        _FRAME_ID,
        "!room:example.org",
        0,
        0,
        1,
        0,
        1,
        b'{"kind":"event","value":{}}',
    )
    payload, _ = journal_plan_module._row(
        owner,
        "NioIngestWork",
        stored.plaintext,
        header=_canonical_internal(stored.clear_values),
    )

    def reject_hash(*args: object, **kwargs: object) -> None:
        raise AssertionError("size calculation hashed stored payload")

    monkeypatch.setattr(journal_format_module.hashlib, "sha256", reject_hash)

    assert journal_plan_module._stored_work_size(owner, stored) == len(payload)


def _prepared_unrelated_held(index: int) -> AuthenticatedWork:
    return _prepared_room_held(
        str(uuid5(_STREAM_ID, f"prepared-unrelated-held:{index}")),
        kind=RecordKind.EPHEMERAL,
        source_json=_prepared_event("m.receipt"),
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
    other_source = _prepared_event("m.receipt")
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
        event_type="m.receipt",
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
        and item.value.kind is RecordKind.GLOBAL_ACCOUNT_DATA
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

    with pytest.raises(JournalCapacityError, match="immutable"):
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
    extra_global_source = _prepared_event("org.example.global") if empty else None
    global_source = _prepared_event("m.push_rules") if empty else None
    segment = _prepared_segment(
        _prepared_observation("join", unparsed=True),
        section=RoomSection.JOIN,
        state=() if empty else (room_source,),
    )
    frame = _prepared_frame(
        room_segments=(segment,),
        global_account_data=(
            () if global_source is None else (extra_global_source, global_source)
        ),
    )
    records = (
        (
            _prepared_record(
                0,
                RecordKind.GLOBAL_ACCOUNT_DATA,
                extra_global_source,
                event_type="org.example.global",
                route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
            ),
            _prepared_record(
                1,
                RecordKind.GLOBAL_ACCOUNT_DATA,
                global_source,
                event_type="m.push_rules",
                route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
            ),
        )
        if extra_global_source is not None and global_source is not None
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
        RecordKind.GLOBAL_ACCOUNT_DATA.value,
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


def test_prepared_held_admission_reserves_largest_promotion_header() -> None:
    case = _prepared_pending_room_case(include_global=False)
    held_plan = _plan_prepared(case)
    assert held_plan is not None
    item = held_plan.work_inserts[0]
    assert item.ready_ordinal is None
    stored = journal_plan_module._stored_work_insert_row(item, case.frame.frame_id, 3)
    owner = (_PLANNER_ACCOUNT_ID, _STREAM_ID, case.frame.origin.transport)
    held_size = journal_plan_module._stored_work_size(owner, stored)
    promoted_size = journal_plan_module._stored_work_size(
        owner,
        replace(
            stored, status="ready", ready_revision=2**63 - 1, ready_ordinal=2**63 - 1
        ),
    )
    assert promoted_size == held_size + 31

    under = _plan_prepared(
        case,
        limits=replace(
            MaterializerLimits(), max_record_canonical_bytes=promoted_size - 1
        ),
    )
    assert under is not None
    assert len(under.work_inserts) == 1
    assert type(under.work_inserts[0].value) is LossRecord
    assert under.work_inserts[0].value.reason is LossReason.OVERSIZED_EVENT
    exact = _plan_prepared(
        case,
        limits=replace(MaterializerLimits(), max_record_canonical_bytes=promoted_size),
    )
    assert exact is not None
    assert exact.work_inserts == held_plan.work_inserts


@pytest.mark.parametrize("existing_held", (False, True))
def test_prepared_total_admission_reserves_all_remaining_held(
    existing_held: bool,
) -> None:
    case = _prepared_pending_room_case(include_global=False)
    work = ()
    if existing_held:
        case, held = _prepared_prior_held(case)
        work = (held,)
    baseline = _plan_prepared(case, work=work)
    assert baseline is not None
    actual_size = sum(item.canonical_size for item in work) + sum(
        _prepared_stored_work_size(case.frame, item) for item in baseline.work_inserts
    )
    reserved_size = actual_size + 31 * (len(work) + len(baseline.work_inserts))
    exact = _plan_prepared(
        case,
        work=work,
        limits=replace(
            MaterializerLimits(), max_total_work_canonical_bytes=reserved_size
        ),
    )
    assert exact == baseline
    with pytest.raises(JournalCapacityError, match="total"):
        _plan_prepared(
            case,
            work=work,
            limits=replace(
                MaterializerLimits(), max_total_work_canonical_bytes=reserved_size - 1
            ),
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
    if limited_field == "max_record_canonical_bytes":
        exact_size += 31  # Admit the largest possible READY promotion header.
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


def test_blocked_result_invariant_has_a_neutral_message() -> None:
    with pytest.raises(ValueError, match="blocked materialization has only a frame"):
        MaterializeResult(MaterializeStatus.BLOCKED, None, None)


def test_source_discovery_normalized_ephemeral_envelopes_are_canonical_ordered_pairs() -> (
    None
):
    first_event = {"content": {"user_ids": ["@a:example.org"]}, "type": "m.receipt"}
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
        b'{"event":{"type":"m.receipt"},"room_id":"!ephemeral:example.org","extra":0}',
        b'{"room_id":"!ephemeral:example.org","event":{"type":"m.receipt"}}',
        b'{"event":[],"room_id":"!ephemeral:example.org"}',
        b'{"event":{"type":"m.receipt"},"room_id":""}',
        b"{",
    ],
)
def test_source_discovery_normalized_ephemeral_envelopes_reject_invalid_canonical_envelopes(
    payload: bytes,
) -> None:
    with pytest.raises(ValueError, match=r".+"):
        _normalized_ephemeral_envelopes((payload,))


def test_source_discovery_frame_room_ids_keep_segment_then_ephemeral_order() -> None:
    first_event = {"type": "m.receipt"}
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


def test_materializer_max_retained_plaintext_backlog_size_is_arithmetic_only() -> None:
    retained_frame_count = 255
    frame_payload_bytes = 24 * 1024 * 1024

    retained_bytes = retained_frame_count * frame_payload_bytes

    assert retained_bytes == 6_417_285_120
    assert retained_bytes / (1024**3) == 5.9765625


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
