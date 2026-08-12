"""Contract tests for the private, pure staged-frame reducer."""

import hashlib
import inspect
from dataclasses import replace
from uuid import UUID, uuid5

import pytest
from hypothesis import given
from hypothesis import strategies as st

from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.membership import MembershipObservation
from nio.ingest.model import (
    LossBoundary,
    LossReason,
    RecordKind,
    RecordOrigin,
    TransportKind,
)
from nio.ingest.ports import NetworkResult, StagedSourceResponse
from nio.ingest.reducer import (
    DescriptorRoute,
    FrameProposal,
    HydrationIntent,
    LossProposal,
    MembershipBaseline,
    RecordDescriptor,
    RecoveryGap,
    RecoveryRelease,
    ReducerInputError,
    RoomContinuity,
    RoomProposal,
    reduce_staged_frame,
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
    SyncFrame,
    canonical_classic_cursor,
    renormalize_staged_frame,
)
from nio.ingest.state import SourceState, StagedFrame

STREAM_ID = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")
CONNECTION_ID = UUID("236f12d0-c282-4594-8654-948a60a73ee9")
OTHER_FRAME_ID = UUID("00000000-0000-0000-0000-000000000001")


def _classic_staged_frame() -> tuple[StagedFrame, object]:
    source = ClassicSource(
        STREAM_ID,
        ClassicSourceConfig(30_000, b"{}"),
        "@me:example.org",
    )
    request = source.plan_request(
        SourceState(
            4,
            TransportKind.CLASSIC,
            canonical_classic_cursor(ClassicCursor(None)),
            9,
            True,
        ),
        9,
    )
    assert request is not None
    body = b'{"next_batch":"s1","rooms":{"join":{}}}'
    normalized = source.normalize(
        request,
        NetworkResult(
            STREAM_ID,
            TransportKind.CLASSIC,
            4,
            9,
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
            request, normalized.response_body, normalized.frame.source_sha256
        ),
    )
    return staged, renormalize_staged_frame(source, staged)


def _sliding_staged_frame() -> tuple[StagedFrame, object]:
    source = SlidingSource(
        STREAM_ID,
        SlidingSourceConfig(
            30_000,
            "worker",
            b"{}",
            b"{}",
            b"{}",
            2,
        ),
        "@me:example.org",
    )
    cursor = SlidingCursor(
        None,
        None,
        CONNECTION_ID,
        "worker",
        1,
        2,
        SlidingRangeAckMode.UNKNOWN,
        False,
    )
    request = source.plan_request(
        SourceState(
            4, TransportKind.SLIDING, canonical_sliding_cursor(cursor), 9, True
        ),
        9,
    )
    assert request is not None
    assert request.body is not None
    body = (
        b'{"lists":{"__nio_all_rooms_v1":{"count":0}},"pos":"p1",'
        + b'"txn_id":'
        + request.body.split(b'"txn_id":"', 1)[1].split(b'"', 1)[0].join((b'"', b'"'))
        + b"}"
    )
    normalized = source.normalize(
        request,
        NetworkResult(
            STREAM_ID,
            TransportKind.SLIDING,
            4,
            9,
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
            request, normalized.response_body, normalized.frame.source_sha256
        ),
    )
    return staged, renormalize_staged_frame(source, staged)


def _state() -> RoomContinuity:
    return RoomContinuity(
        "!room:example.org",
        0,
        "join",
        MembershipBaseline("$member", "s0"),
        None,
        None,
    )


def _gap() -> RecoveryGap:
    return RecoveryGap(
        UUID("aaaaaaaa-aaaa-aaaa-aaaa-aaaaaaaaaaaa"),
        "!room:example.org",
        0,
        RecordOrigin(TransportKind.CLASSIC, 3, 8, 0),
        "s0",
        "s1",
    )


def _mixed_frame() -> SyncFrame:
    observation = MembershipObservation(
        "join", "join", "$member", None, None, False, False, False, False
    )
    segment = RoomSegment(
        "!room:example.org",
        RoomSection.JOIN,
        (b'{"type":"state"}',),
        (b'{"n":1}', b'{"n":2}'),
        (b'{"type":"account"}',),
        False,
        "s1",
        False,
        False,
        1,
        observation,
    )
    return SyncFrame(
        UUID("12345678-1234-5678-1234-567812345678"),
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 0),
        b'{"next_batch":"s0"}',
        b'{"next_batch":"s1"}',
        hashlib.sha256(b"mixed").digest(),
        (),
        b'{"changed":[],"left":[]}',
        b"{}",
        b"null",
        (segment,),
        (b'{"event":{"type":"ephemeral"},"room_id":"!room:example.org"}',),
        (b'{"type":"global"}',),
        (b'{"type":"presence"}',),
    )


def test_contract_private_values_are_frozen_slotted_and_validate_shape() -> None:
    assert tuple(DescriptorRoute) == (
        DescriptorRoute.READY,
        DescriptorRoute.HOLD_FOR_GAP,
        DescriptorRoute.HOLD_FOR_HYDRATION,
        DescriptorRoute.HOLD_FOR_RETIREMENT,
        DescriptorRoute.RELEASE_AFTER_LOSS,
    )
    assert tuple(RecoveryRelease) == (
        RecoveryRelease.NONE,
        RecoveryRelease.LOSS_THEN_HELD,
    )
    with pytest.raises((TypeError, ValueError)):
        MembershipBaseline(1, None)  # type: ignore[arg-type]
    with pytest.raises((TypeError, ValueError)):
        RoomContinuity("", 0, None, None, None, None)
    with pytest.raises((TypeError, ValueError)):
        RecordDescriptor(  # type: ignore[arg-type]
            TransportKind.CLASSIC,
            None,
            b"{}",
            None,
            "key",
            DescriptorRoute.READY,
        )


def test_contract_exposes_exact_proposal_values_and_entrypoints() -> None:
    expected = {
        "reduce_staged_frame": (
            "stream_id",
            "staged_frame_id",
            "frame",
            "rooms",
        ),
    }
    assert (
        tuple(inspect.signature(reduce_staged_frame).parameters)
        == expected["reduce_staged_frame"]
    )
    assert {
        value.__name__
        for value in (
            FrameProposal,
            RoomProposal,
            RecoveryGap,
            HydrationIntent,
            LossProposal,
        )
    } == {
        "FrameProposal",
        "RoomProposal",
        "RecoveryGap",
        "HydrationIntent",
        "LossProposal",
    }


def test_contract_shape_validation_rejects_bool_optional_tuple_and_carrier_drift() -> (
    None
):
    with pytest.raises(TypeError):
        RoomContinuity("!room:example.org", True, None, None, None, None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        MembershipBaseline(1, None)  # type: ignore[arg-type]
    with pytest.raises(TypeError):
        RoomProposal(  # type: ignore[arg-type]
            _state(),
            _state(),
            None,
            None,
            None,
            (object(),),
            RecoveryRelease.NONE,
        )
    with pytest.raises(TypeError):
        FrameProposal(  # type: ignore[arg-type]
            UUID("00000000-0000-0000-0000-000000000003"),
            b"x" * 32,
            (),
            (object(),),
            False,
        )


def test_staged_identity_mismatch_fails_without_changing_inputs() -> None:
    staged, frame = _classic_staged_frame()
    state = _state()
    before = (staged, frame, state)

    with pytest.raises(ReducerInputError):
        reduce_staged_frame(STREAM_ID, OTHER_FRAME_ID, frame, (state,))

    assert (staged, frame, state) == before


@pytest.mark.parametrize("fixture", [_classic_staged_frame, _sliding_staged_frame])
def test_determinism_uses_restart_renormalized_frame_without_mutating_inputs(
    fixture,
) -> None:
    staged, frame = fixture()
    before = (staged, frame)

    first = reduce_staged_frame(STREAM_ID, staged.frame_id, frame, ())
    second = reduce_staged_frame(STREAM_ID, staged.frame_id, frame, ())

    assert first == second
    assert (staged, frame) == before


def test_order_and_provenance_preserve_the_certified_frame_sequence() -> None:
    frame = _mixed_frame()

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))

    assert [
        (record.kind, record.room_id, record.source_json)
        for record in proposal.descriptors
    ] == [
        (RecordKind.STATE, "!room:example.org", b'{"type":"state"}'),
        (RecordKind.TIMELINE, "!room:example.org", b'{"n":1}'),
        (RecordKind.TIMELINE, "!room:example.org", b'{"n":2}'),
        (RecordKind.ROOM_ACCOUNT_DATA, "!room:example.org", b'{"type":"account"}'),
        (RecordKind.EPHEMERAL, "!room:example.org", b'{"type":"ephemeral"}'),
        (RecordKind.GLOBAL_ACCOUNT_DATA, None, b'{"type":"global"}'),
        (RecordKind.PRESENCE, None, b'{"type":"presence"}'),
    ]
    assert [record.descriptor_key for record in proposal.descriptors] == [
        f"frame:{frame.frame_id}:{ordinal}" for ordinal in range(7)
    ]
    assert [record.provenance for record in proposal.descriptors] == [
        None,
        "history",
        "live",
        None,
        None,
        None,
        None,
    ]


def test_ephemeral_only_room_without_prior_continuity_fails_closed() -> None:
    frame = replace(_mixed_frame(), room_segments=())

    with pytest.raises(ReducerInputError):
        reduce_staged_frame(STREAM_ID, frame.frame_id, frame, ())


@pytest.mark.parametrize(
    "changes",
    [
        {"to_device_json": (b'{"type":"m.room_key"}',)},
        {"device_list_delta_json": b'{"changed":["@a:example.org"],"left":[]}'},
        {"one_time_key_counts_json": b'{"signed_curve25519":0}'},
        {"unused_fallback_key_types_json": b'["signed_curve25519"]'},
    ],
)
def test_crypto_deferred_uses_only_source_certified_empty_controls(changes) -> None:
    empty = _mixed_frame()

    assert (
        reduce_staged_frame(
            STREAM_ID, empty.frame_id, empty, (_state(),)
        ).crypto_deferred
        is False
    )
    changed = replace(empty, **changes)
    assert (
        reduce_staged_frame(
            STREAM_ID, changed.frame_id, changed, (_state(),)
        ).crypto_deferred
        is True
    )


def test_classic_zero_otk_snapshot_remains_globally_crypto_deferred() -> None:
    frame = replace(
        _mixed_frame(),
        origin=RecordOrigin(TransportKind.CLASSIC, 3, 8, 0),
        one_time_key_counts_json=b'{"signed_curve25519":0}',
    )

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))

    assert proposal.crypto_deferred is True


@pytest.mark.parametrize("transport", tuple(TransportKind))
@pytest.mark.parametrize("timeline_index", (0, 1), ids=("history", "live"))
def test_encrypted_timeline_is_crypto_deferred_for_every_transport_and_position(
    transport: TransportKind,
    timeline_index: int,
) -> None:
    frame = _mixed_frame()
    timeline = list(frame.room_segments[0].timeline_json)
    timeline[timeline_index] = b'{"type":"m.room.encrypted"}'
    segment = replace(frame.room_segments[0], timeline_json=tuple(timeline))
    frame = replace(
        frame,
        origin=replace(frame.origin, transport=transport),
        room_segments=(segment,),
    )

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))

    assert proposal.crypto_deferred is True


@pytest.mark.parametrize(
    "payload",
    (b"not-json", b"[]", b'{"type": "m.room.encrypted"}'),
    ids=("invalid-json", "not-object", "noncanonical"),
)
def test_invalid_timeline_carrier_fails_reduction(payload: bytes) -> None:
    frame = _mixed_frame()
    segment = replace(frame.room_segments[0], timeline_json=(payload,))
    frame = replace(frame, room_segments=(segment,))

    with pytest.raises(ReducerInputError, match="timeline"):
        reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))


def test_membership_first_room_segment_hydrates_without_prior_continuity() -> None:
    frame = _mixed_frame()

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, ())

    hydration_id = uuid5(
        STREAM_ID,
        f"hydrate:!room:example.org:0:{frame.frame_id}",
    )
    room = proposal.room_proposals[0]
    assert room.before is None
    assert room.after == RoomContinuity(
        "!room:example.org",
        0,
        "join",
        None,
        None,
        hydration_id,
    )
    assert room.hydration == HydrationIntent(hydration_id, frame.origin)
    assert room.release is RecoveryRelease.NONE
    assert [record.route for record in proposal.descriptors[:5]] == [
        DescriptorRoute.HOLD_FOR_HYDRATION,
    ] * 5
    assert [record.route for record in proposal.descriptors[5:]] == [
        DescriptorRoute.READY,
        DescriptorRoute.READY,
    ]


@pytest.mark.parametrize(
    ("prior", "claim", "section"),
    [
        ("join", "leave", RoomSection.LEAVE),
        ("leave", "join", RoomSection.JOIN),
        ("invite", "join", RoomSection.JOIN),
    ],
)
def test_membership_transition_advances_epoch_and_holds_successor(
    prior, claim, section
) -> None:
    frame = _mixed_frame()
    observation = MembershipObservation(
        claim, claim, "$membership", prior, None, False, False, False, False
    )
    segment = replace(
        frame.room_segments[0],
        section=section,
        membership_observation=observation,
    )
    frame = replace(frame, room_segments=(segment,))
    state = replace(_state(), membership=prior)

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (state,))

    room = proposal.room_proposals[0]
    assert room.retirement_epoch == 0
    assert room.after.membership_epoch == 1
    assert room.after.membership == claim
    assert [record.route for record in proposal.descriptors[:5]] == [
        DescriptorRoute.HOLD_FOR_RETIREMENT,
    ] * 5


def test_membership_first_claim_and_unparsed_event_do_not_retire_epoch_zero() -> None:
    frame = _mixed_frame()
    unknown = RoomContinuity("!room:example.org", 0, None, None, None, None)
    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (unknown,))
    hydration_id = uuid5(
        STREAM_ID,
        f"hydrate:!room:example.org:0:{frame.frame_id}",
    )
    room = proposal.room_proposals[0]
    assert room.before == unknown
    assert room.after == RoomContinuity(
        "!room:example.org", 0, "join", None, None, hydration_id
    )
    assert room.hydration == HydrationIntent(hydration_id, frame.origin)
    assert room.retirement_epoch is None
    assert all(
        record.route is DescriptorRoute.HOLD_FOR_HYDRATION
        for record in proposal.descriptors[:5]
    )

    observation = MembershipObservation(
        "leave", "join", None, None, None, False, False, False, True
    )
    segment = replace(
        frame.room_segments[0],
        section=RoomSection.LEAVE,
        membership_observation=observation,
    )
    proposal = reduce_staged_frame(
        STREAM_ID, frame.frame_id, replace(frame, room_segments=(segment,)), (_state(),)
    )
    assert proposal.room_proposals[0].after.membership == "leave"


def test_membership_echo_updates_baseline_and_is_ready() -> None:
    frame = _mixed_frame()

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))

    room = proposal.room_proposals[0]
    assert room.before == _state()
    assert room.after == replace(_state(), baseline=MembershipBaseline("$member", "s1"))
    assert room.recovery is None
    assert room.hydration is None
    assert room.losses == ()
    assert all(
        descriptor.route is DescriptorRoute.READY
        for descriptor in proposal.descriptors[:5]
    )


def test_trusted_classic_discontinuity_creates_one_gap() -> None:
    frame = _mixed_frame()
    segment = replace(frame.room_segments[0], timeline_limited=True)
    frame = replace(frame, room_segments=(segment,))

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))

    gap_id = uuid5(
        STREAM_ID,
        f"gap:!room:example.org:0:{frame.frame_id}:s0:s1",
    )
    gap = RecoveryGap(gap_id, "!room:example.org", 0, frame.origin, "s0", "s1")
    room = proposal.room_proposals[0]
    assert room.after.gap == gap
    assert room.recovery == gap
    assert room.losses == ()
    assert all(
        descriptor.route is DescriptorRoute.HOLD_FOR_GAP
        for descriptor in proposal.descriptors[:5]
    )


def test_sliding_discontinuity_uses_prior_room_window_not_source_cursor() -> None:
    frame = _mixed_frame()
    segment = replace(frame.room_segments[0], timeline_limited=True)
    frame = replace(
        frame,
        origin=RecordOrigin(TransportKind.SLIDING, 4, 9, 0),
        request_cursor_json=b'"not-a-sliding-cursor"',
        candidate_cursor_json=b'"not-a-sliding-cursor"',
        room_segments=(segment,),
    )
    state = replace(_state(), baseline=MembershipBaseline("$member", "room-old"))

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (state,))

    assert proposal.room_proposals[0].recovery == RecoveryGap(
        uuid5(
            STREAM_ID,
            f"gap:!room:example.org:0:{frame.frame_id}:room-old:s1",
        ),
        "!room:example.org",
        0,
        frame.origin,
        "room-old",
        "s1",
    )


def test_unverifiable_discontinuity_releases_after_one_loss() -> None:
    frame = _mixed_frame()
    segment = replace(
        frame.room_segments[0], timeline_limited=True, timeline_prev_batch=None
    )
    frame = replace(
        frame,
        request_cursor_json=canonical_classic_cursor(ClassicCursor(None)),
        candidate_cursor_json=canonical_classic_cursor(ClassicCursor(None)),
        room_segments=(segment,),
    )

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))

    room = proposal.room_proposals[0]
    assert room.release is RecoveryRelease.LOSS_THEN_HELD
    assert room.losses == (
        LossProposal(
            "!room:example.org",
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
        ),
    )
    assert all(
        descriptor.route is DescriptorRoute.RELEASE_AFTER_LOSS
        for descriptor in proposal.descriptors[:5]
    )


def test_existing_hydration_and_gap_are_not_duplicated() -> None:
    frame = _mixed_frame()
    hydration_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    hydrating = replace(_state(), baseline=None, hydration_id=hydration_id)
    hydrated = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (hydrating,))
    room = hydrated.room_proposals[0]
    assert room.after.hydration_id == hydration_id
    assert room.hydration == HydrationIntent(hydration_id, frame.origin)
    assert all(
        descriptor.route is DescriptorRoute.HOLD_FOR_HYDRATION
        for descriptor in hydrated.descriptors[:5]
    )

    gapped = replace(_state(), gap=_gap())
    held = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (gapped,))
    room = held.room_proposals[0]
    assert room.after.gap == _gap()
    assert room.recovery is None
    assert all(
        descriptor.route is DescriptorRoute.HOLD_FOR_GAP
        for descriptor in held.descriptors[:5]
    )


def test_unparsed_or_transition_evidence_invalidates_old_gap_once() -> None:
    frame = _mixed_frame()
    gapped = replace(_state(), gap=_gap())
    unparsed = MembershipObservation(
        "join", "leave", None, None, None, False, False, False, True
    )
    proposal = reduce_staged_frame(
        STREAM_ID,
        frame.frame_id,
        replace(
            frame,
            room_segments=(
                replace(frame.room_segments[0], membership_observation=unparsed),
            ),
        ),
        (gapped,),
    )
    room = proposal.room_proposals[0]
    assert room.after.gap is None
    assert room.after.hydration_id is not None
    assert room.release is RecoveryRelease.LOSS_THEN_HELD
    assert [loss.reason for loss in room.losses] == [LossReason.BASELINE_LOST]
    assert all(
        descriptor.route is DescriptorRoute.HOLD_FOR_HYDRATION
        for descriptor in proposal.descriptors[:5]
    )

    transition = MembershipObservation(
        "leave", "leave", "$leave", "join", None, False, False, False, False
    )
    proposal = reduce_staged_frame(
        STREAM_ID,
        frame.frame_id,
        replace(
            frame,
            room_segments=(
                replace(
                    frame.room_segments[0],
                    section=RoomSection.LEAVE,
                    membership_observation=transition,
                ),
            ),
        ),
        (gapped,),
    )
    room = proposal.room_proposals[0]
    assert room.after.membership_epoch == 1
    assert room.retirement_epoch == 0
    assert room.release is RecoveryRelease.LOSS_THEN_HELD
    assert [loss.reason for loss in room.losses] == [LossReason.BASELINE_LOST]
    assert all(
        descriptor.route is DescriptorRoute.HOLD_FOR_RETIREMENT
        for descriptor in proposal.descriptors[:5]
    )


def test_membership_transition_terminates_pending_hydration_before_retirement() -> None:
    frame = _mixed_frame()
    hydration_id = UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb")
    hydrating = replace(_state(), baseline=None, hydration_id=hydration_id)
    observation = MembershipObservation(
        "leave", "leave", "$leave", "join", None, False, False, False, False
    )
    segment = replace(
        frame.room_segments[0],
        section=RoomSection.LEAVE,
        membership_observation=observation,
    )
    frame = replace(frame, room_segments=(segment,))

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (hydrating,))

    room = proposal.room_proposals[0]
    assert room.retirement_epoch == 0
    assert room.after.membership_epoch == 1
    assert room.after.hydration_id is None
    assert room.losses == (
        LossProposal(
            "!room:example.org",
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
        ),
    )
    assert room.release is RecoveryRelease.LOSS_THEN_HELD


def test_classic_gap_falls_back_to_candidate_cursor_target() -> None:
    frame = _mixed_frame()
    segment = replace(
        frame.room_segments[0], timeline_limited=True, timeline_prev_batch=None
    )
    frame = replace(frame, room_segments=(segment,))

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))

    assert proposal.room_proposals[0].recovery is not None
    assert proposal.room_proposals[0].recovery.start_token == "s0"
    assert proposal.room_proposals[0].recovery.target_token == "s1"


@pytest.mark.parametrize(
    ("state", "route"),
    [
        (_state(), DescriptorRoute.READY),
        (replace(_state(), gap=_gap()), DescriptorRoute.HOLD_FOR_GAP),
        (
            replace(
                _state(),
                baseline=None,
                hydration_id=UUID("bbbbbbbb-bbbb-bbbb-bbbb-bbbbbbbbbbbb"),
            ),
            DescriptorRoute.HOLD_FOR_HYDRATION,
        ),
    ],
)
def test_ephemeral_only_room_inherits_existing_barrier(state, route) -> None:
    frame = replace(_mixed_frame(), room_segments=())

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (state,))

    assert proposal.room_proposals == ()
    assert proposal.descriptors[0].kind is RecordKind.EPHEMERAL
    assert proposal.descriptors[0].route is route


@pytest.mark.parametrize("flag", ["initial", "expanded_timeline"])
def test_trusted_initial_or_expanded_segment_uses_gap_grammar(flag) -> None:
    frame = _mixed_frame()
    observation = replace(
        frame.room_segments[0].membership_observation,
        is_initial=flag == "initial",
        is_expanded_timeline=flag == "expanded_timeline",
    )
    segment = replace(
        frame.room_segments[0],
        **{flag: True},
        membership_observation=observation,
    )
    frame = replace(frame, room_segments=(segment,))

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))

    assert proposal.room_proposals[0].recovery is not None
    assert all(
        descriptor.route is DescriptorRoute.HOLD_FOR_GAP
        for descriptor in proposal.descriptors[:5]
    )


@pytest.mark.parametrize("flag", ["initial", "expanded_timeline"])
def test_untrusted_initial_or_expanded_segment_hydrates(flag) -> None:
    frame = _mixed_frame()
    observation = replace(
        frame.room_segments[0].membership_observation,
        is_initial=flag == "initial",
        is_expanded_timeline=flag == "expanded_timeline",
    )
    segment = replace(
        frame.room_segments[0],
        **{flag: True},
        membership_observation=observation,
    )
    frame = replace(frame, room_segments=(segment,))
    state = replace(_state(), baseline=None)

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (state,))

    room = proposal.room_proposals[0]
    assert room.hydration is not None
    assert room.losses == ()
    assert all(
        descriptor.route is DescriptorRoute.HOLD_FOR_HYDRATION
        for descriptor in proposal.descriptors[:5]
    )


@pytest.mark.parametrize(
    "state",
    [
        replace(_state(), baseline=MembershipBaseline(None, "s0")),
        replace(_state(), baseline=MembershipBaseline("$old", "s0")),
        replace(_state(), membership=None),
    ],
)
def test_untrusted_membership_baseline_hydrates_instead_of_gapping(state) -> None:
    frame = _mixed_frame()
    segment = replace(frame.room_segments[0], timeline_limited=True)
    if state.baseline is not None and state.baseline.membership_event_id == "$old":
        segment = replace(
            segment,
            membership_observation=MembershipObservation(
                "join", "join", "$new", None, None, False, False, False, False
            ),
        )
    frame = replace(frame, room_segments=(segment,))

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (state,))

    room = proposal.room_proposals[0]
    assert room.recovery is None
    assert room.hydration is not None
    assert room.after.baseline is None
    assert all(
        descriptor.route is DescriptorRoute.HOLD_FOR_HYDRATION
        for descriptor in proposal.descriptors[:5]
    )


@pytest.mark.parametrize(
    "observation",
    [
        MembershipObservation(
            "join", "join", "$old", None, None, False, False, False, False
        ),
        MembershipObservation(
            "join", "join", "$new", "join", "$old", False, False, False, False
        ),
        MembershipObservation(
            "join", "join", "$new", None, None, True, False, False, False
        ),
    ],
)
def test_exact_linked_or_live_membership_evidence_updates_trusted_baseline(
    observation,
) -> None:
    frame = _mixed_frame()
    segment = replace(frame.room_segments[0], membership_observation=observation)
    frame = replace(frame, room_segments=(segment,))
    state = replace(_state(), baseline=MembershipBaseline("$old", "s0"))

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (state,))

    room = proposal.room_proposals[0]
    assert room.hydration is None
    assert room.after.baseline == MembershipBaseline(observation.event_id, "s1")
    assert all(
        descriptor.route is DescriptorRoute.READY
        for descriptor in proposal.descriptors[:5]
    )


def test_malformed_ephemeral_envelope_fails_the_whole_frame() -> None:
    frame = replace(
        _mixed_frame(),
        ephemeral_json=(b'{"event":[],"room_id":"!room:example.org"}',),
    )

    with pytest.raises(ReducerInputError):
        reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (_state(),))


@given(st.booleans())
def test_room_state_input_order_does_not_change_frame_order(reverse) -> None:
    frame = _mixed_frame()
    other_observation = MembershipObservation(
        "join", "join", "$other", None, None, False, False, False, False
    )
    other_segment = replace(
        frame.room_segments[0],
        room_id="!other:example.org",
        membership_observation=other_observation,
    )
    frame = replace(frame, room_segments=(*frame.room_segments, other_segment))
    states = (
        _state(),
        RoomContinuity(
            "!other:example.org",
            0,
            "join",
            MembershipBaseline("$other", "s0"),
            None,
            None,
        ),
    )
    supplied = tuple(reversed(states)) if reverse else states

    proposal = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, supplied)
    canonical = reduce_staged_frame(STREAM_ID, frame.frame_id, frame, states)

    assert proposal == canonical
    assert [room.after.room_id for room in proposal.room_proposals] == [
        "!room:example.org",
        "!other:example.org",
    ]
    with pytest.raises(ReducerInputError):
        reduce_staged_frame(STREAM_ID, frame.frame_id, frame, (states[0], states[0]))
