from dataclasses import FrozenInstanceError, fields, replace
from enum import StrEnum
from uuid import UUID

import pytest

import nio.ingest.state as ingest_state
from nio import TimelineEventProvenance as TopLevelTimelineEventProvenance
from nio.ingest import (
    ConsumerBinding,
    ConsumerBootstrap,
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RecordOrigin,
    RoomHydrationStatus,
    RoomMemberSnapshot,
    RoomSnapshot,
    SystemOrigin,
    SystemOriginKind,
    TimelineEventProvenance,
    TransportKind,
)
from nio.ingest.membership import MembershipBaseline
from nio.ingest.recovery import RecoveryGap
from nio.ingest.serialization import _loss_id, batch_from_records
from nio.ingest.state import (
    LaneRecord,
    LaneRecordKey,
    LaneRecordSection,
    LaneStatus,
    ReadyRecord,
    ReleasePhase,
    RoomAggregate,
    RoomLane,
    RoomState,
)

JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")
OPERATION_ID = UUID("33333333-3333-3333-3333-333333333333")
GAP_ID = UUID("44444444-4444-4444-4444-444444444444")
EFFECT_ID = UUID("55555555-5555-5555-5555-555555555555")
FRAME_ID = UUID("66666666-6666-6666-6666-666666666666")


class ForeignWireValue(StrEnum):
    CLASSIC = "classic"
    FRESH_START = "fresh_start"
    TIMELINE = "timeline"
    LIVE = "live"
    FETCH_FAILED = "fetch_failed"
    JOIN = "join"


def test_wire_enums_have_stable_string_values() -> None:
    assert issubclass(TransportKind, StrEnum)
    assert {member.name: member.value for member in TransportKind} == {
        "CLASSIC": "classic",
        "SLIDING": "sliding",
    }
    assert {member.name: member.value for member in RecordKind} == {
        "TIMELINE": "timeline",
        "STATE": "state",
        "EPHEMERAL": "ephemeral",
        "ROOM_ACCOUNT_DATA": "room_account_data",
        "GLOBAL_ACCOUNT_DATA": "global_account_data",
        "PRESENCE": "presence",
        "TO_DEVICE": "to_device",
        "ROOM_LIFECYCLE": "room_lifecycle",
        "ROOM_READINESS": "room_readiness",
        "DECRYPTION_UPDATE": "decryption_update",
    }
    assert {member.name: member.value for member in SystemOriginKind} == {
        "FRESH_START": "fresh_start",
        "MEMBERSHIP_CHANGE": "membership_change",
        "ROOM_HYDRATION": "room_hydration",
        "STORE_VALIDATION": "store_validation",
    }
    assert {member.name: member.value for member in LossReason} == {
        "EVENT_LIMIT": "event_limit",
        "FETCH_FAILED": "fetch_failed",
        "BASELINE_LOST": "baseline_lost",
        "UNVERIFIABLE": "unverifiable",
        "CORRUPT_STORED_RECORD": "corrupt_stored_record",
        "OVERSIZED_EVENT": "oversized_event",
    }
    assert {member.name: member.value for member in TimelineEventProvenance} == {
        "LIVE": "live",
        "RECOVERED": "recovered",
        "HISTORY": "history",
    }
    assert {member.name: member.value for member in RoomHydrationStatus} == {
        "PENDING": "pending",
        "READY": "ready",
        "UNAVAILABLE": "unavailable",
    }
    attach_status = getattr(ingest_state, "ConsumerAttachStatus", None)
    assert attach_status is not None
    assert {member.name: member.value for member in attach_status} == {
        "UNBOUND": "unbound",
        "ATTACHING": "attaching",
        "ATTACHED": "attached",
    }
    assert TimelineEventProvenance is TopLevelTimelineEventProvenance


def test_wire_dataclasses_are_frozen_and_slotted() -> None:
    values = (
        ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        ConsumerBootstrap(
            OPERATION_ID,
            ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
            1,
            ("!room:example.org",),
            b"digest",
        ),
        RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
        SystemOrigin(SystemOriginKind.FRESH_START, OPERATION_ID),
        EventRecord(
            "$event",
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            b"{}",
            None,
        ),
        RoomMemberSnapshot("@alice:example.org", "join", "Alice", None, 0),
        RoomSnapshot(
            "!room:example.org",
            1,
            "@me:example.org",
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
        ),
        LossBoundary(None, None, None, None),
        LossRecord(
            "loss-id",
            RecordOrigin(TransportKind.SLIDING, 1, 2, 3),
            "!room:example.org",
            1,
            LossReason.FETCH_FAILED,
            LossBoundary(None, None, None, None),
            b"{}",
        ),
    )

    for value in values:
        assert not hasattr(value, "__dict__")
        with pytest.raises(FrozenInstanceError):
            setattr(value, fields(value)[0].name, "mutable")


def test_collection_fields_require_tuples() -> None:
    with pytest.raises(TypeError, match="baseline_room_ids must be a tuple"):
        ConsumerBootstrap(
            OPERATION_ID,
            ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
            1,
            ["!room:example.org"],  # type: ignore[arg-type]
            b"digest",
        )

    with pytest.raises(TypeError, match="members must be a tuple"):
        RoomSnapshot(
            "!room:example.org",
            1,
            "@me:example.org",
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
            [],  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "make_value",
    [
        lambda: ConsumerBootstrap(
            OPERATION_ID,
            ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
            1,
            (),
            bytearray(b"digest"),  # type: ignore[arg-type]
        ),
        lambda: EventRecord(
            "$event",
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            bytearray(b"{}"),  # type: ignore[arg-type]
            None,
        ),
        lambda: EventRecord(
            "$event",
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            b"{}",
            memoryview(b"{}"),  # type: ignore[arg-type]
        ),
        lambda: RoomSnapshot(
            "!room:example.org",
            1,
            "@me:example.org",
            "join",
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            bytearray(b"{}"),  # type: ignore[arg-type]
            (),
        ),
        lambda: LossRecord(
            "loss-id",
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            LossReason.FETCH_FAILED,
            LossBoundary(None, None, None, None),
            bytearray(b"{}"),  # type: ignore[arg-type]
        ),
    ],
)
def test_public_bytes_fields_require_exact_immutable_bytes(make_value) -> None:
    with pytest.raises(TypeError, match="bytes"):
        make_value()


def test_collection_fields_validate_every_nested_element() -> None:
    with pytest.raises(TypeError, match="baseline_room_ids.*str"):
        ConsumerBootstrap(
            OPERATION_ID,
            ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
            1,
            (object(),),  # type: ignore[arg-type]
            b"digest",
        )

    with pytest.raises(TypeError, match="members.*RoomMemberSnapshot"):
        RoomSnapshot(
            "!room:example.org",
            1,
            "@me:example.org",
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
            (object(),),  # type: ignore[arg-type]
        )


def _membership_baseline() -> MembershipBaseline:
    return MembershipBaseline(
        "!room:example.org",
        3,
        7,
        "old-prev",
        "$member",
    )


def _recovery_gap() -> RecoveryGap:
    return RecoveryGap(
        GAP_ID,
        "!room:example.org",
        4,
        7,
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 2),
        "$member",
        "old-prev",
        "new-prev",
        "cursor-1",
        ("old-prev", "cursor-1"),
        1,
        8,
        EFFECT_ID,
    )


def _lane_event(
    *,
    room_id: str = "!room:example.org",
    provenance: TimelineEventProvenance = TimelineEventProvenance.LIVE,
) -> EventRecord:
    return EventRecord(
        "$event",
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 3),
        room_id,
        7,
        1,
        "$event",
        provenance,
        b"{}",
        None,
    )


def _lane_loss() -> LossRecord:
    return LossRecord(
        "loss-id",
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 3),
        "!room:example.org",
        7,
        LossReason.FETCH_FAILED,
        LossBoundary(None, None, "old-prev", "new-prev"),
        b"{}",
    )


def test_recovery_carriers_are_frozen_slotted_and_exact() -> None:
    baseline = _membership_baseline()
    gap = _recovery_gap()

    assert not hasattr(baseline, "__dict__")
    assert not hasattr(gap, "__dict__")
    with pytest.raises(FrozenInstanceError):
        baseline.prev_batch = "changed"  # type: ignore[misc]
    with pytest.raises(FrozenInstanceError):
        gap.cursor_token = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError, match="source_epoch"):
        MembershipBaseline(
            baseline.room_id,
            True,  # type: ignore[arg-type]
            baseline.membership_epoch,
            baseline.prev_batch,
            baseline.membership_event_id,
        )
    with pytest.raises(TypeError, match="seen_cursor_tokens"):
        RecoveryGap(
            gap.gap_id,
            gap.room_id,
            gap.opening_source_epoch,
            gap.membership_epoch,
            gap.origin,
            gap.membership_event_id,
            gap.start_token,
            gap.target_token,
            gap.cursor_token,
            list(gap.seen_cursor_tokens),  # type: ignore[arg-type]
            gap.pages_committed,
            gap.recovered_record_count,
            gap.in_flight_effect_id,
        )


@pytest.mark.parametrize(
    ("changes", "message"),
    (
        ({"start_token": ""}, "start_token"),
        ({"seen_cursor_tokens": ()}, "seen_cursor_tokens"),
        (
            {"seen_cursor_tokens": ("old-prev", "cursor-1", "cursor-1")},
            "unique",
        ),
        ({"seen_cursor_tokens": ("wrong", "cursor-1")}, "start_token"),
        ({"seen_cursor_tokens": ("old-prev", "wrong")}, "cursor_token"),
        ({"pages_committed": -1}, "nonnegative"),
        ({"pages_committed": 2}, "cursor history"),
        (
            {"origin": RecordOrigin(TransportKind.CLASSIC, 5, 9, 2)},
            "opening_source_epoch",
        ),
    ),
)
def test_recovery_gap_rejects_ambiguous_cursor_or_counter_state(
    changes: dict[str, object],
    message: str,
) -> None:
    from dataclasses import replace

    with pytest.raises((TypeError, ValueError), match=message):
        replace(_recovery_gap(), **changes)


@pytest.mark.parametrize(
    "changes",
    (
        {"opening_source_epoch": True},
        {"membership_epoch": True},
        {"pages_committed": True},
        {"recovered_record_count": True},
    ),
)
def test_recovery_gap_rejects_bool_counters(changes: dict[str, object]) -> None:
    from dataclasses import replace

    with pytest.raises(TypeError):
        replace(_recovery_gap(), **changes)


def test_recovery_gap_distinguishes_wrong_cursor_types_from_empty_tokens() -> None:
    from dataclasses import replace

    with pytest.raises(TypeError, match="seen_cursor_tokens"):
        replace(_recovery_gap(), seen_cursor_tokens=("old-prev", 1))
    with pytest.raises(ValueError, match="seen_cursor_tokens"):
        replace(_recovery_gap(), seen_cursor_tokens=("old-prev", ""))


def _model_snapshot() -> RoomSnapshot:
    return RoomSnapshot(
        "!room:example.org",
        7,
        "@me:example.org",
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


def test_room_state_requires_consistent_hydration_and_nonnegative_counters() -> None:
    with pytest.raises(ValueError, match="READY.*snapshot"):
        RoomState(
            "!room:example.org",
            7,
            1,
            RoomHydrationStatus.READY,
            None,
            _membership_baseline(),
        )
    with pytest.raises(ValueError, match="UNAVAILABLE.*snapshot"):
        RoomState(
            "!room:example.org",
            7,
            1,
            RoomHydrationStatus.UNAVAILABLE,
            _model_snapshot(),
            None,
        )
    with pytest.raises(ValueError, match="membership baseline room/epoch"):
        RoomState(
            "!other:example.org",
            7,
            1,
            RoomHydrationStatus.PENDING,
            None,
            _membership_baseline(),
        )
    for field_name in (
        "current_membership_epoch",
        "next_room_sequence",
        "updated_revision",
    ):
        values = {
            "room_id": "!room:example.org",
            "current_membership_epoch": 7,
            "next_room_sequence": 1,
            "hydration_status": RoomHydrationStatus.READY,
            "snapshot": _model_snapshot(),
            "membership_baseline": _membership_baseline(),
            "updated_revision": 0,
        }
        values[field_name] = -1
        with pytest.raises(ValueError, match="nonnegative"):
            RoomState(**values)


def test_room_lane_requires_gap_containment_phase_and_nonnegative_counters() -> None:
    with pytest.raises(ValueError, match="recovery gap room/epoch"):
        RoomLane(
            "!room:example.org",
            8,
            LaneStatus.ACTIVE,
            release_phase=ReleasePhase.RECOVERING,
            recovery_gap=_recovery_gap(),
        )
    with pytest.raises(ValueError, match="RECOVERING.*recovery gap"):
        RoomLane(
            "!room:example.org",
            7,
            LaneStatus.ACTIVE,
            release_phase=ReleasePhase.RECOVERING,
        )
    with pytest.raises(ValueError, match="recovery gap.*RECOVERING"):
        RoomLane(
            "!room:example.org",
            7,
            LaneStatus.ACTIVE,
            recovery_gap=_recovery_gap(),
        )
    for field_name in (
        "membership_epoch",
        "held_record_count",
        "held_canonical_bytes",
        "ready_order",
        "next_held_ordinal",
        "successor_membership_epoch",
        "updated_revision",
    ):
        values = {
            "room_id": "!room:example.org",
            "membership_epoch": 7,
            "lane_status": LaneStatus.ACTIVE,
            "held_record_count": 0,
            "held_canonical_bytes": 0,
            "release_phase": ReleasePhase.IDLE,
            "ready_order": None,
            "next_held_ordinal": 0,
            "successor_membership_epoch": None,
            "recovery_gap": None,
            "pending_lifecycle": None,
            "updated_revision": 0,
        }
        values[field_name] = -1
        with pytest.raises(ValueError, match="nonnegative"):
            RoomLane(**values)


@pytest.mark.parametrize(
    ("release_phase", "ready_order", "recovery_gap"),
    (
        (ReleasePhase.IDLE, None, None),
        (ReleasePhase.RECOVERING, None, _recovery_gap()),
        (ReleasePhase.RELEASING_RECOVERED, 0, None),
        (ReleasePhase.RELEASING_TERMINAL, 0, None),
    ),
)
def test_room_lane_ready_head_exactly_matches_release_phase(
    release_phase: ReleasePhase,
    ready_order: int | None,
    recovery_gap: RecoveryGap | None,
) -> None:
    lane = RoomLane(
        "!room:example.org",
        7,
        LaneStatus.ACTIVE,
        release_phase=release_phase,
        ready_order=ready_order,
        recovery_gap=recovery_gap,
    )

    assert lane.ready_order == ready_order
    invalid_ready_order = 0 if ready_order is None else None
    with pytest.raises(ValueError, match="ready_order"):
        replace(lane, ready_order=invalid_ready_order)


def test_room_lane_owns_exact_local_topology_and_held_counters() -> None:
    lifecycle = replace(
        _lane_event(),
        kind=RecordKind.ROOM_LIFECYCLE,
        membership_epoch=7,
    )
    retiring = RoomLane(
        "!room:example.org",
        6,
        LaneStatus.RETIRING,
        successor_membership_epoch=7,
        pending_lifecycle=lifecycle,
    )

    assert retiring.pending_lifecycle == lifecycle
    invalid_topologies = (
        {"lane_status": LaneStatus.ACTIVE},
        {"successor_membership_epoch": None},
        {"pending_lifecycle": None},
        {"pending_lifecycle": replace(lifecycle, kind=RecordKind.TIMELINE)},
        {"pending_lifecycle": replace(lifecycle, room_id="!other:example.org")},
        {"pending_lifecycle": replace(lifecycle, membership_epoch=8)},
        {
            "release_phase": ReleasePhase.RECOVERING,
            "recovery_gap": replace(_recovery_gap(), membership_epoch=6),
        },
    )
    for changes in invalid_topologies:
        with pytest.raises(ValueError, match="active|retiring|lifecycle|gap"):
            replace(retiring, **changes)

    for changes in (
        {"held_record_count": 1, "held_canonical_bytes": 0},
        {"held_record_count": 0, "held_canonical_bytes": 1},
        {
            "held_record_count": 2,
            "held_canonical_bytes": 10,
            "next_held_ordinal": 1,
        },
    ):
        with pytest.raises(ValueError, match="held"):
            RoomLane(
                "!room:example.org",
                7,
                LaneStatus.ACTIVE,
                **changes,
            )


def test_room_carriers_reject_empty_room_ids() -> None:
    with pytest.raises(ValueError, match="room_id"):
        RoomState(
            "",
            0,
            0,
            RoomHydrationStatus.PENDING,
            None,
        )
    with pytest.raises(ValueError, match="room_id"):
        RoomLane("", 0, LaneStatus.ACTIVE)


def test_room_aggregate_constructor_enforces_the_full_epoch_chain() -> None:
    state = RoomState(
        "!room:example.org",
        7,
        1,
        RoomHydrationStatus.READY,
        _model_snapshot(),
        _membership_baseline(),
    )
    active = RoomLane(
        "!room:example.org",
        7,
        LaneStatus.ACTIVE,
        release_phase=ReleasePhase.RECOVERING,
        recovery_gap=_recovery_gap(),
    )
    aggregate = RoomAggregate(state, active, ())

    assert aggregate.active_lane == active
    with pytest.raises(ValueError, match="baseline"):
        RoomAggregate(replace(state, membership_baseline=None), active, ())
    with pytest.raises(ValueError, match="start token"):
        RoomAggregate(
            state,
            replace(
                active,
                recovery_gap=replace(
                    _recovery_gap(),
                    start_token="other",
                    seen_cursor_tokens=("other", "cursor-1"),
                ),
            ),
            (),
        )
    retiring = RoomLane(
        "!room:example.org",
        6,
        LaneStatus.RETIRING,
        successor_membership_epoch=7,
        pending_lifecycle=replace(
            _lane_event(),
            kind=RecordKind.ROOM_LIFECYCLE,
            membership_epoch=7,
        ),
    )
    assert RoomAggregate(state, active, (retiring,)).retiring_lanes == (retiring,)
    wrong_chain = replace(
        retiring,
        successor_membership_epoch=8,
        pending_lifecycle=replace(retiring.pending_lifecycle, membership_epoch=8),
    )
    with pytest.raises(ValueError, match="successor"):
        RoomAggregate(state, active, (wrong_chain,))


def test_lane_record_section_controls_record_and_source_identity() -> None:
    event = _lane_event()
    recovered_event = _lane_event(provenance=TimelineEventProvenance.RECOVERED)
    loss = _lane_loss()

    held = LaneRecord(
        LaneRecordKey("!room:example.org", 7, LaneRecordSection.HELD, 0, 1),
        event,
        FRAME_ID,
        None,
        10,
    )
    recovered = LaneRecord(
        LaneRecordKey("!room:example.org", 7, LaneRecordSection.RECOVERED, 1, 1),
        recovered_event,
        None,
        EFFECT_ID,
        10,
    )
    terminal = LaneRecord(
        LaneRecordKey("!room:example.org", 7, LaneRecordSection.LOSS, 0, 0),
        loss,
        None,
        EFFECT_ID,
        10,
    )

    assert (held.record, recovered.record, terminal.record) == (
        event,
        recovered_event,
        loss,
    )
    with pytest.raises(ValueError, match="LOSS.*LossRecord"):
        LaneRecord(terminal.key, event, None, None, 10)
    with pytest.raises(ValueError, match="RECOVERED.*EventRecord"):
        LaneRecord(recovered.key, loss, None, EFFECT_ID, 10)
    with pytest.raises(ValueError, match="HELD.*source_frame_id"):
        LaneRecord(held.key, event, None, None, 10)
    with pytest.raises(ValueError, match="RECOVERED.*source_effect_id"):
        LaneRecord(recovered.key, recovered_event, None, None, 10)
    with pytest.raises(ValueError, match="RECOVERED.*provenance"):
        LaneRecord(recovered.key, event, None, EFFECT_ID, 10)
    with pytest.raises(ValueError, match="HELD.*provenance"):
        LaneRecord(held.key, recovered_event, FRAME_ID, None, 10)
    with pytest.raises(ValueError, match="source identity"):
        LaneRecord(terminal.key, loss, FRAME_ID, EFFECT_ID, 10)
    with pytest.raises(ValueError, match="exactly one source pointer"):
        LaneRecord(terminal.key, loss, None, None, 10)
    system_loss = replace(
        loss,
        origin=SystemOrigin(SystemOriginKind.STORE_VALIDATION, OPERATION_ID),
    )
    assert LaneRecord(terminal.key, system_loss, None, None, 10).record == system_loss
    with pytest.raises(ValueError, match="system-derived.*source pointer"):
        LaneRecord(terminal.key, system_loss, FRAME_ID, None, 10)
    with pytest.raises(ValueError, match="room/epoch"):
        LaneRecord(
            held.key,
            _lane_event(room_id="!other:example.org"),
            FRAME_ID,
            None,
            10,
        )
    with pytest.raises(ValueError, match="canonical_bytes"):
        LaneRecord(held.key, event, FRAME_ID, None, 0)
    with pytest.raises(ValueError, match="item identity"):
        LaneRecord(held.key, replace(event, record_id=""), FRAME_ID, None, 10)


@pytest.mark.parametrize(
    "record",
    (
        EventRecord(
            "system-ready",
            RecordKind.ROOM_READINESS,
            SystemOrigin(SystemOriginKind.ROOM_HYDRATION, OPERATION_ID),
            "!room:example.org",
            7,
            3,
            None,
            None,
            b"{}",
            None,
        ),
        replace(
            _lane_loss(),
            origin=SystemOrigin(SystemOriginKind.STORE_VALIDATION, OPERATION_ID),
        ),
    ),
)
def test_system_origin_ready_records_cannot_claim_a_source_frame(
    record: EventRecord | LossRecord,
) -> None:
    with pytest.raises(ValueError, match="SystemOrigin.*source_frame_id"):
        ReadyRecord(0, record, FRAME_ID)

    assert ReadyRecord(0, record).source_frame_id is None
    assert ReadyRecord(0, _lane_event()).source_frame_id is None


@pytest.mark.parametrize(
    "changes",
    (
        {"ready_order": False},
        {"ready_order": -1},
        {"record": object()},
        {"source_frame_id": "frame"},
        {"canonical_bytes": False},
        {"canonical_bytes": -1},
        {"created_revision": False},
        {"created_revision": -1},
    ),
)
def test_ready_record_validates_exact_nonnegative_carrier_fields(
    changes: dict[str, object],
) -> None:
    values = {
        "ready_order": 0,
        "record": _lane_event(),
        "source_frame_id": None,
        "canonical_bytes": 0,
        "created_revision": 0,
        **changes,
    }

    with pytest.raises((TypeError, ValueError)):
        ReadyRecord(**values)


def test_batch_materialization_carriers_are_exact_frozen_and_slotted() -> None:
    ready_key_type = getattr(ingest_state, "ReadyRecordKey", None)
    materialization_type = getattr(ingest_state, "BatchMaterialization", None)
    assert ready_key_type is not None
    assert materialization_type is not None

    ready_key = ready_key_type("$event")
    lane_key = LaneRecordKey(
        "!room:example.org",
        7,
        LaneRecordSection.HELD,
        0,
        1,
    )
    batch = batch_from_records(
        account_id="@alice:example.org",
        device_id="DEVICE",
        consumer=ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        stream_id=JOURNAL_GENERATION,
        sequence=1,
        created_revision=1,
        records=(
            _lane_event(),
            replace(
                _lane_event(room_id="!other:example.org"),
                record_id="$other-event",
            ),
        ),
    )
    materialization = materialization_type(batch, (ready_key, lane_key))

    assert not hasattr(ready_key, "__dict__")
    assert not hasattr(materialization, "__dict__")
    with pytest.raises(FrozenInstanceError):
        ready_key.item_id = "changed"  # type: ignore[misc]
    with pytest.raises(TypeError, match="item_id"):
        ready_key_type(1)
    with pytest.raises(ValueError, match="item_id"):
        ready_key_type("")
    with pytest.raises(TypeError, match="batch"):
        materialization_type(object(), (ready_key,))
    with pytest.raises(TypeError, match="sources must be a tuple"):
        materialization_type(batch, [ready_key, lane_key])
    with pytest.raises(TypeError, match="sources.*ReadyRecordKey or LaneRecordKey"):
        materialization_type(batch, (object(), object()))
    with pytest.raises(ValueError, match="one source per batch record"):
        materialization_type(batch, (ready_key,))
    with pytest.raises(ValueError, match="duplicate"):
        materialization_type(batch, (ready_key, ready_key))

    too_many_records = tuple(
        replace(_lane_event(), record_id=f"$event-{ordinal}") for ordinal in range(257)
    )
    too_large = batch_from_records(
        account_id="@alice:example.org",
        device_id="DEVICE",
        consumer=ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        stream_id=JOURNAL_GENERATION,
        sequence=1,
        created_revision=1,
        records=too_many_records,
    )
    with pytest.raises(ValueError, match="256"):
        materialization_type(
            too_large,
            tuple(ready_key_type(record.record_id) for record in too_many_records),
        )

    loss = _lane_loss()
    loss = replace(loss, loss_id=_loss_id(JOURNAL_GENERATION, loss))
    duplicate_item_batch = batch_from_records(
        account_id="@alice:example.org",
        device_id="DEVICE",
        consumer=ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        stream_id=JOURNAL_GENERATION,
        sequence=1,
        created_revision=1,
        records=(replace(_lane_event(), record_id=loss.loss_id), loss),
    )
    with pytest.raises(ValueError, match="item identities"):
        materialization_type(
            duplicate_item_batch,
            (
                LaneRecordKey(
                    "!room:example.org",
                    7,
                    LaneRecordSection.HELD,
                    0,
                    1,
                ),
                LaneRecordKey(
                    "!room:example.org",
                    7,
                    LaneRecordSection.LOSS,
                    0,
                    0,
                ),
            ),
        )


def test_journal_transition_exposes_only_the_atomic_batch_transfer() -> None:
    transition_fields = {field.name for field in fields(ingest_state.JournalTransition)}

    assert "batch_materialization" in transition_fields
    assert "batches" not in transition_fields
    assert "losses" not in transition_fields
    assert "lane_record_deletes" not in transition_fields
    with pytest.raises(TypeError, match="batch_materialization"):
        ingest_state.JournalTransition(batch_materialization=object())


@pytest.mark.parametrize(
    "make_value",
    [
        lambda: RecordOrigin(ForeignWireValue.CLASSIC, 1, 2, 3),  # type: ignore[arg-type]
        lambda: SystemOrigin(ForeignWireValue.FRESH_START, OPERATION_ID),  # type: ignore[arg-type]
        lambda: EventRecord(
            "$event",
            ForeignWireValue.TIMELINE,  # type: ignore[arg-type]
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            b"{}",
            None,
        ),
        lambda: EventRecord(
            "$event",
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            ForeignWireValue.LIVE,  # type: ignore[arg-type]
            b"{}",
            None,
        ),
        lambda: LossRecord(
            "loss-id",
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            ForeignWireValue.FETCH_FAILED,  # type: ignore[arg-type]
            LossBoundary(None, None, None, None),
            b"{}",
        ),
        lambda: RoomMemberSnapshot(
            "@alice:example.org",
            ForeignWireValue.JOIN,  # type: ignore[arg-type]
            "Alice",
            None,
            0,
        ),
    ],
)
def test_direct_construction_rejects_foreign_str_enum_values(make_value) -> None:
    with pytest.raises(TypeError, match="must be"):
        make_value()


@pytest.mark.parametrize(
    ("kind_value", "origin_kind_value", "event_id"),
    (
        ("room_lifecycle", "membership_change", None),
        ("state", "room_hydration", "$state"),
        ("room_readiness", "room_hydration", None),
    ),
)
def test_event_record_accepts_only_scoped_system_origin_pairs(
    kind_value: str,
    origin_kind_value: str,
    event_id: str | None,
) -> None:
    kind = RecordKind(kind_value)
    origin_kind = SystemOriginKind(origin_kind_value)
    system_record = EventRecord(
        "system-record",
        kind,
        SystemOrigin(origin_kind, OPERATION_ID),
        "!room:example.org",
        7,
        3,
        event_id,
        TimelineEventProvenance.HISTORY,
        b"{}",
        b"{}",
    )
    transport_record = replace(
        system_record,
        origin=RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
    )

    assert system_record.origin == SystemOrigin(origin_kind, OPERATION_ID)
    assert transport_record.origin == RecordOrigin(TransportKind.CLASSIC, 1, 2, 3)
    if kind is RecordKind.ROOM_LIFECYCLE:
        observed_lifecycle = replace(transport_record, event_id="$event")
        assert observed_lifecycle.event_id == "$event"


def test_event_record_system_origin_whitelist_is_exhaustive() -> None:
    allowed = {
        (RecordKind.ROOM_LIFECYCLE, SystemOriginKind.MEMBERSHIP_CHANGE),
        (RecordKind.STATE, SystemOriginKind.ROOM_HYDRATION),
        (RecordKind.ROOM_READINESS, SystemOriginKind.ROOM_HYDRATION),
    }
    for kind in RecordKind:
        for origin_kind in SystemOriginKind:
            event_id = "$state" if kind is RecordKind.STATE else None
            arguments = (
                "system-record",
                kind,
                SystemOrigin(origin_kind, OPERATION_ID),
                "!room:example.org",
                7,
                3,
                event_id,
                None,
                b"{}",
                None,
            )
            if (kind, origin_kind) in allowed:
                assert EventRecord(*arguments).kind is kind
            else:
                with pytest.raises(ValueError, match="SystemOrigin"):
                    EventRecord(*arguments)


@pytest.mark.parametrize(
    ("kind_value", "origin_kind_value", "event_id"),
    (
        ("room_lifecycle", "membership_change", None),
        ("state", "room_hydration", "$state"),
        ("room_readiness", "room_hydration", None),
    ),
)
@pytest.mark.parametrize(
    "changes",
    (
        {"room_id": None},
        {"room_id": ""},
        {"membership_epoch": None},
        {"membership_epoch": -1},
        {"room_sequence": None},
        {"room_sequence": -1},
    ),
)
def test_system_event_records_require_room_epoch_and_sequence_scope(
    kind_value: str,
    origin_kind_value: str,
    event_id: str | None,
    changes: dict[str, object],
) -> None:
    record = EventRecord(
        "system-record",
        RecordKind(kind_value),
        SystemOrigin(SystemOriginKind(origin_kind_value), OPERATION_ID),
        "!room:example.org",
        7,
        3,
        event_id,
        None,
        b"{}",
        None,
    )

    with pytest.raises(ValueError, match="room_id|membership_epoch|room_sequence"):
        replace(record, **changes)


@pytest.mark.parametrize(
    ("kind_value", "event_id"),
    (
        ("state", None),
        ("state", ""),
        ("room_lifecycle", "$event"),
        ("room_readiness", "$event"),
    ),
)
def test_system_event_record_event_id_rules_are_exact(
    kind_value: str,
    event_id: str | None,
) -> None:
    kind = RecordKind(kind_value)
    origin_kind = (
        SystemOriginKind.MEMBERSHIP_CHANGE
        if kind is RecordKind.ROOM_LIFECYCLE
        else SystemOriginKind.ROOM_HYDRATION
    )

    with pytest.raises(ValueError, match="event_id"):
        EventRecord(
            "system-record",
            kind,
            SystemOrigin(origin_kind, OPERATION_ID),
            "!room:example.org",
            7,
            3,
            event_id,
            None,
            b"{}",
            None,
        )


def test_room_loss_requires_membership_epoch() -> None:
    with pytest.raises(ValueError, match="membership_epoch"):
        LossRecord(
            "loss-id",
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            None,  # type: ignore[arg-type]
            LossReason.FETCH_FAILED,
            LossBoundary(None, None, None, None),
            b"{}",
        )


def test_room_snapshot_has_only_deeply_immutable_state() -> None:
    snapshot = RoomSnapshot(
        "!room:example.org",
        3,
        "@me:example.org",
        "join",
        True,
        None,
        None,
        "A topic",
        "mxc://example.org/avatar",
        "invite",
        "12",
        "forbidden",
        b'{"users":{}}',
        (
            RoomMemberSnapshot("@me:example.org", "join", "Me", None, 100),
            RoomMemberSnapshot("@alice:example.org", "join", "Alice", None, 0),
        ),
    )

    assert snapshot.own_user_id == "@me:example.org"
    assert isinstance(snapshot.members, tuple)
    assert all(not isinstance(value, (dict, list, set)) for value in snapshot.members)
    assert {field.name for field in fields(RoomSnapshot)} == {
        "room_id",
        "membership_epoch",
        "own_user_id",
        "own_membership",
        "encrypted",
        "name",
        "canonical_alias",
        "topic",
        "avatar_url",
        "join_rule",
        "room_version",
        "guest_access",
        "power_levels_json",
        "members",
    }


def test_room_snapshot_derives_current_matrix_room_names_from_full_membership() -> None:
    members = (
        RoomMemberSnapshot("@me:example.org", "join", "Me", None, 0),
        RoomMemberSnapshot("@alice:example.org", "join", "Alice", None, 0),
        RoomMemberSnapshot("@malory:example.org", "invite", "Alice", None, 0),
        RoomMemberSnapshot("@gone:example.org", "leave", "Gone", None, 0),
    )
    snapshot = RoomSnapshot(
        "!room:example.org",
        1,
        "@me:example.org",
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
        members,
    )

    assert snapshot.member_count == 3
    assert snapshot.is_group
    assert snapshot.display_name == (
        "Alice (@alice:example.org) and Alice (@malory:example.org)"
    )


@pytest.mark.parametrize(
    ("name", "alias", "members", "expected"),
    [
        ("Room name", "#alias:example.org", (), "Room name"),
        (None, "#alias:example.org", (), "#alias:example.org"),
        (
            None,
            None,
            (RoomMemberSnapshot("@me:example.org", "join", "Me", None, 0),),
            "Empty Room",
        ),
        (
            None,
            None,
            tuple(
                [RoomMemberSnapshot("@me:example.org", "join", "Me", None, 0)]
                + [
                    RoomMemberSnapshot(
                        f"@user{index}:example.org",
                        "join",
                        f"User {index}",
                        None,
                        0,
                    )
                    for index in range(1, 8)
                ]
            ),
            "User 1, User 2, User 3, User 4, User 5 and 2 others",
        ),
    ],
)
def test_room_snapshot_display_name_matches_matrix_room_fallbacks(
    name: str | None,
    alias: str | None,
    members: tuple[RoomMemberSnapshot, ...],
    expected: str,
) -> None:
    snapshot = RoomSnapshot(
        "!room:example.org",
        1,
        "@me:example.org",
        "join",
        False,
        name,
        alias,
        None,
        None,
        None,
        None,
        None,
        None,
        members,
    )

    assert snapshot.display_name == expected
    assert snapshot.is_group is (not name and not alias)
