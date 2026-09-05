"""Shared membership and recovery planning contracts."""

import hashlib
from dataclasses import replace
from uuid import UUID, uuid5
import pytest
from nio.ingest.membership import MembershipObservation
from nio.ingest.model import LossBoundary, LossReason, RecordOrigin, TransportKind
from nio.ingest.reducer import (
    DescriptorRoute,
    HydrationIntent,
    LossProposal,
    MembershipBaseline,
    RecoveryGap,
    RecoveryRelease,
    RoomContinuity,
    _plan_room,
)
from nio.ingest.source import (
    ClassicCursor,
    RoomSection,
    RoomSegment,
    SyncFrame,
    canonical_classic_cursor,
)

STREAM_ID = UUID("96afc18d-22c3-45a6-a7ba-5cb49f28c900")


def _state() -> RoomContinuity:
    return RoomContinuity(
        "!room:example.org", 0, "join", MembershipBaseline("$member", "s0"), None, None
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
    )


def test_membership_first_room_segment_hydrates_without_prior_continuity() -> None:
    frame = _mixed_frame()
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], None)
    hydration_id = uuid5(STREAM_ID, f"hydrate:!room:example.org:0:{frame.frame_id}")
    assert room.before is None
    assert room.after == RoomContinuity(
        "!room:example.org", 0, "join", None, None, hydration_id
    )
    assert room.hydration == HydrationIntent(hydration_id, frame.origin)
    assert room.release is RecoveryRelease.NONE
    assert route is DescriptorRoute.HOLD_FOR_HYDRATION


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
        frame.room_segments[0], section=section, membership_observation=observation
    )
    frame = replace(frame, room_segments=(segment,))
    state = replace(_state(), membership=prior)
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], state)
    assert room.retirement_epoch == 0
    assert room.after.membership_epoch == 1
    assert room.after.membership == claim
    assert route is DescriptorRoute.HOLD_FOR_RETIREMENT


def test_membership_first_claim_and_unparsed_event_do_not_retire_epoch_zero() -> None:
    frame = _mixed_frame()
    unknown = RoomContinuity("!room:example.org", 0, None, None, None, None)
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], unknown)
    hydration_id = uuid5(STREAM_ID, f"hydrate:!room:example.org:0:{frame.frame_id}")
    assert room.before == unknown
    assert room.after == RoomContinuity(
        "!room:example.org", 0, "join", None, None, hydration_id
    )
    assert room.hydration == HydrationIntent(hydration_id, frame.origin)
    assert room.retirement_epoch is None
    observation = MembershipObservation(
        "leave", "join", None, None, None, False, False, False, True
    )
    segment = replace(
        frame.room_segments[0],
        section=RoomSection.LEAVE,
        membership_observation=observation,
    )
    room, route = _plan_room(
        STREAM_ID,
        replace(frame, room_segments=(segment,)),
        segment,
        _state(),
    )
    assert room.after.membership == "leave"


def test_membership_echo_updates_baseline_and_is_ready() -> None:
    frame = _mixed_frame()
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], _state())
    assert room.before == _state()
    assert room.after == replace(_state(), baseline=MembershipBaseline("$member", "s1"))
    assert room.recovery is None
    assert room.hydration is None
    assert room.losses == ()
    assert route is DescriptorRoute.READY


def test_trusted_classic_discontinuity_creates_one_gap() -> None:
    frame = _mixed_frame()
    segment = replace(frame.room_segments[0], timeline_limited=True)
    frame = replace(frame, room_segments=(segment,))
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], _state())
    gap_id = uuid5(STREAM_ID, f"gap:!room:example.org:0:{frame.frame_id}:s0:s1")
    gap = RecoveryGap(gap_id, "!room:example.org", 0, frame.origin, "s0", "s1")
    assert room.after.gap == gap
    assert room.recovery == gap
    assert room.losses == ()
    assert route is DescriptorRoute.HOLD_FOR_GAP


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
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], state)
    assert room.recovery == RecoveryGap(
        uuid5(STREAM_ID, f"gap:!room:example.org:0:{frame.frame_id}:room-old:s1"),
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
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], _state())
    assert room.release is RecoveryRelease.LOSS_THEN_HELD
    assert room.losses == (
        LossProposal(
            "!room:example.org",
            0,
            LossReason.UNVERIFIABLE,
            LossBoundary(None, None, None, None),
        ),
    )
    assert route is DescriptorRoute.RELEASE_AFTER_LOSS


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
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], hydrating)
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
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], _state())
    assert room.recovery is not None
    assert room.recovery.start_token == "s0"
    assert room.recovery.target_token == "s1"


@pytest.mark.parametrize("flag", ["initial", "expanded_timeline"])
def test_untrusted_initial_or_expanded_segment_hydrates(flag) -> None:
    frame = _mixed_frame()
    observation = replace(
        frame.room_segments[0].membership_observation,
        is_initial=flag == "initial",
        is_expanded_timeline=flag == "expanded_timeline",
    )
    segment = replace(
        frame.room_segments[0], **{flag: True}, membership_observation=observation
    )
    frame = replace(frame, room_segments=(segment,))
    state = replace(_state(), baseline=None)
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], state)
    assert room.hydration is not None
    assert room.losses == ()
    assert route is DescriptorRoute.HOLD_FOR_HYDRATION


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
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], state)
    assert room.recovery is None
    assert room.hydration is not None
    assert room.after.baseline is None
    assert route is DescriptorRoute.HOLD_FOR_HYDRATION


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
    room, route = _plan_room(STREAM_ID, frame, frame.room_segments[0], state)
    assert room.hydration is None
    assert room.after.baseline == MembershipBaseline(observation.event_id, "s1")
    assert route is DescriptorRoute.READY
