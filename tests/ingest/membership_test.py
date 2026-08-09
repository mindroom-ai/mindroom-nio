from dataclasses import FrozenInstanceError

import pytest

from nio.ingest.membership import (
    MembershipBaseline,
    MembershipObservation,
    MembershipProof,
    MembershipProofKind,
    advance_membership_baseline,
    membership_recovery_cursor,
    prove_membership,
)


def observation(
    *,
    room_membership: str | None = "join",
    event_membership: str | None = None,
    event_id: str | None = None,
    previous_membership: str | None = None,
    replaces_state: str | None = None,
    is_live: bool = False,
    is_initial: bool = False,
    is_expanded_timeline: bool = False,
    is_unparsed: bool = False,
) -> MembershipObservation:
    return MembershipObservation(
        room_membership=room_membership,
        event_membership=event_membership,
        event_id=event_id,
        previous_membership=previous_membership,
        replaces_state=replaces_state,
        is_live=is_live,
        is_initial=is_initial,
        is_expanded_timeline=is_expanded_timeline,
        is_unparsed=is_unparsed,
    )


def test_membership_values_are_frozen_slotted_and_exact() -> None:
    baseline = MembershipBaseline("w1", "$member")
    evidence = observation(event_membership="join", event_id="$member")
    proof = MembershipProof(MembershipProofKind.CONTINUES, "$member")

    assert not hasattr(baseline, "__dict__")
    assert not hasattr(evidence, "__dict__")
    assert not hasattr(proof, "__dict__")
    with pytest.raises(FrozenInstanceError):
        baseline.prev_batch = "w2"  # type: ignore[misc]
    with pytest.raises(TypeError):
        MembershipObservation(
            room_membership="join",
            event_membership=None,
            event_id=None,
            previous_membership=None,
            replaces_state=None,
            is_live=1,  # type: ignore[arg-type]
            is_initial=False,
            is_expanded_timeline=False,
            is_unparsed=False,
        )


@pytest.mark.parametrize(
    ("baseline", "evidence", "expected"),
    [
        pytest.param(
            None,
            observation(event_membership="leave", event_id="$leave"),
            MembershipProof(MembershipProofKind.DEPARTED, None),
            id="explicit-membership-loss",
        ),
        pytest.param(
            None,
            observation(event_membership="join", event_id="$m1"),
            MembershipProof(MembershipProofKind.ESTABLISHED, "$m1"),
            id="current-membership-establishes-first-baseline",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(event_membership="join", event_id="$m1"),
            MembershipProof(MembershipProofKind.CONTINUES, "$m1"),
            id="exact-membership-event-continues",
        ),
        pytest.param(
            MembershipBaseline("w1", "$old"),
            observation(
                event_membership="join",
                event_id="$new",
                is_live=True,
            ),
            MembershipProof(MembershipProofKind.CHANGED, "$new"),
            id="live-own-join-proves-new-membership-but-breaks-continuity",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(
                event_membership="join",
                event_id="$m1",
                is_live=True,
            ),
            MembershipProof(MembershipProofKind.CHANGED, "$m1"),
            id="live-redelivery-of-the-baseline-still-breaks-continuity",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(
                event_membership="join",
                event_id="$m2",
                previous_membership="join",
                replaces_state="$m1",
                is_live=True,
            ),
            MembershipProof(MembershipProofKind.CHANGED, "$m2"),
            id="live-linked-rotation-still-breaks-continuity",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(
                event_membership="join",
                event_id="$m2",
                previous_membership="join",
                replaces_state="$m1",
            ),
            MembershipProof(MembershipProofKind.CONTINUES, "$m2"),
            id="linked-join-to-join-rotation-continues",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(
                event_membership="join",
                event_id="$m2",
                previous_membership="join",
                replaces_state="$other",
            ),
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="rotation-replacing-another-event-is-unsafe",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(
                event_membership="join",
                event_id="$m2",
                previous_membership="invite",
                replaces_state="$m1",
            ),
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="rotation-from-non-join-is-unsafe",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(event_membership="join", event_id="$m2"),
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="unlinked-required-state-join-cannot-date-a-rejoin",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(is_unparsed=True),
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="unparsed-own-membership-stub-is-unsafe",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(),
            MembershipProof(MembershipProofKind.CONTINUES, "$m1"),
            id="ordinary-delta-without-membership-change-continues",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(is_initial=True),
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="initial-window-without-membership-proof-is-unsafe",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(is_expanded_timeline=True),
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="expanded-window-without-membership-proof-is-unsafe",
        ),
        pytest.param(
            None,
            observation(),
            MembershipProof(MembershipProofKind.UNSAFE, None),
            id="no-membership-event-and-no-baseline-is-unsafe",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(room_membership="invite"),
            MembershipProof(MembershipProofKind.DEPARTED, None),
            id="invite-crosses-membership-epoch",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(room_membership="invite", is_unparsed=True),
            MembershipProof(MembershipProofKind.DEPARTED, None),
            id="authoritative-room-departure-outranks-an-unparsed-stub",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(
                room_membership="leave",
                event_membership="join",
                event_id="$contradictory",
            ),
            MembershipProof(MembershipProofKind.DEPARTED, None),
            id="authoritative-room-departure-outranks-a-contradictory-join",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(room_membership="knock"),
            MembershipProof(MembershipProofKind.DEPARTED, None),
            id="knock-crosses-membership-epoch",
        ),
        pytest.param(
            MembershipBaseline("w1", "$m1"),
            observation(room_membership="ban"),
            MembershipProof(MembershipProofKind.DEPARTED, None),
            id="ban-crosses-membership-epoch",
        ),
    ],
)
def test_membership_epoch_proof_rules(
    baseline: MembershipBaseline | None,
    evidence: MembershipObservation,
    expected: MembershipProof,
) -> None:
    assert prove_membership(baseline, evidence) == expected


def test_recovery_uses_old_window_only_for_proven_continuity() -> None:
    baseline = MembershipBaseline("old-prev", "$m1")

    assert (
        membership_recovery_cursor(
            baseline,
            MembershipProof(MembershipProofKind.CONTINUES, "$m1"),
            discontinuity=True,
            current_prev_batch="new-prev",
        )
        == "old-prev"
    )
    for proof, discontinuity, current_prev_batch in (
        (MembershipProof(MembershipProofKind.CHANGED, "$m2"), True, "new-prev"),
        (MembershipProof(MembershipProofKind.UNSAFE, None), True, "new-prev"),
        (MembershipProof(MembershipProofKind.CONTINUES, "$m1"), False, "new-prev"),
        (MembershipProof(MembershipProofKind.CONTINUES, "$m1"), True, None),
    ):
        assert (
            membership_recovery_cursor(
                baseline,
                proof,
                discontinuity=discontinuity,
                current_prev_batch=current_prev_batch,
            )
            is None
        )


def test_baseline_advance_forgets_departures_and_rebinds_join_transitions() -> None:
    baseline = MembershipBaseline("old-prev", "$m1")

    assert advance_membership_baseline(
        baseline,
        MembershipProof(MembershipProofKind.CONTINUES, "$m2"),
        current_prev_batch=None,
    ) == MembershipBaseline("old-prev", "$m2")
    assert advance_membership_baseline(
        baseline,
        MembershipProof(MembershipProofKind.CHANGED, "$m2"),
        current_prev_batch="new-prev",
    ) == MembershipBaseline("new-prev", "$m2")
    assert (
        advance_membership_baseline(
            baseline,
            MembershipProof(MembershipProofKind.CHANGED, "$m2"),
            current_prev_batch=None,
        )
        is None
    )
    for kind in (MembershipProofKind.DEPARTED, MembershipProofKind.UNSAFE):
        assert (
            advance_membership_baseline(
                baseline,
                MembershipProof(kind, None),
                current_prev_batch="new-prev",
            )
            is None
        )
