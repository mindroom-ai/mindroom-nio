from dataclasses import FrozenInstanceError, fields

import pytest

from nio.ingest.membership import MembershipObservation


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


def test_membership_observation_is_exact_frozen_and_slotted() -> None:
    evidence = observation(event_membership="join", event_id="$member")

    assert tuple(field.name for field in fields(MembershipObservation)) == (
        "room_membership",
        "event_membership",
        "event_id",
        "previous_membership",
        "replaces_state",
        "is_live",
        "is_initial",
        "is_expanded_timeline",
        "is_unparsed",
    )
    assert not hasattr(evidence, "__dict__")
    with pytest.raises(FrozenInstanceError):
        evidence.event_id = "$other"  # type: ignore[misc]
    with pytest.raises(TypeError):
        observation(is_live=1)  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("field_name", "value"),
    (
        ("room_membership", "unknown"),
        ("event_membership", "unknown"),
        ("event_id", 1),
        ("previous_membership", 1),
        ("replaces_state", 1),
    ),
)
def test_membership_observation_rejects_invalid_values(
    field_name: str,
    value: object,
) -> None:
    with pytest.raises((TypeError, ValueError)):
        observation(**{field_name: value})
