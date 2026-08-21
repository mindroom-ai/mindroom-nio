from dataclasses import dataclass
from typing import Any


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _require_optional_string(value: object, field_name: str) -> None:
    if value is not None:
        _require_exact(value, str, field_name)


@dataclass(frozen=True, slots=True)
class MembershipObservation:
    room_membership: str | None
    event_membership: str | None
    event_id: str | None
    previous_membership: str | None
    replaces_state: str | None
    is_live: bool
    is_initial: bool
    is_expanded_timeline: bool
    is_unparsed: bool

    def __post_init__(self) -> None:
        allowed = {None, "join", "invite", "knock", "leave", "ban"}
        _require_optional_string(self.room_membership, "room_membership")
        _require_optional_string(self.event_membership, "event_membership")
        if self.room_membership not in allowed:
            raise ValueError("unknown room membership")
        if self.event_membership not in allowed:
            raise ValueError("unknown membership event value")
        _require_optional_string(self.event_id, "event_id")
        _require_optional_string(self.previous_membership, "previous_membership")
        _require_optional_string(self.replaces_state, "replaces_state")
        _require_exact(self.is_live, bool, "is_live")
        _require_exact(self.is_initial, bool, "is_initial")
        _require_exact(
            self.is_expanded_timeline,
            bool,
            "is_expanded_timeline",
        )
        _require_exact(self.is_unparsed, bool, "is_unparsed")


@dataclass(frozen=True, slots=True)
class _OwnMembershipEvidence:
    membership: str | None
    event_id: str | None
    previous_membership: str | None
    replaces_state: str | None
    is_live: bool
    is_unparsed: bool


_MEMBERSHIP_VALUES = {"join", "invite", "knock", "leave", "ban"}


def _latest_own_membership(
    state_events: list[dict[str, Any]],
    timeline_events: list[dict[str, Any]],
    live_event_count: int,
    own_user_id: str,
) -> _OwnMembershipEvidence | None:
    candidates: list[tuple[dict[str, Any], bool]] = [
        (event, False) for event in state_events
    ]
    if live_event_count:
        candidates.extend(
            (event, True) for event in timeline_events[-live_event_count:]
        )
    for event, is_live in reversed(candidates):
        if (
            event.get("type") != "m.room.member"
            or event.get("state_key") != own_user_id
        ):
            continue
        try:
            content = event["content"]
            if type(content) is not dict:
                raise TypeError
            membership = content.get("membership")
            if type(membership) is not str or membership not in _MEMBERSHIP_VALUES:
                raise TypeError
            event_id = event.get("event_id")
            if event_id is not None and type(event_id) is not str:
                raise TypeError
            unsigned = event.get("unsigned", {})
            if type(unsigned) is not dict:
                raise TypeError
            previous = unsigned.get("prev_content", {})
            if type(previous) is not dict:
                raise TypeError
            previous_membership = previous.get("membership")
            if previous_membership is not None and (
                type(previous_membership) is not str
                or previous_membership not in _MEMBERSHIP_VALUES
            ):
                raise TypeError
            replaces_state = unsigned.get("replaces_state")
            if replaces_state is not None and type(replaces_state) is not str:
                raise TypeError
        except (KeyError, TypeError):
            return _OwnMembershipEvidence(None, None, None, None, is_live, True)
        return _OwnMembershipEvidence(
            membership,
            event_id,
            previous_membership,
            replaces_state,
            is_live,
            False,
        )
    return None


def _membership_observation(
    room_membership: str | None,
    own_membership: _OwnMembershipEvidence | None,
    *,
    is_initial: bool,
    is_expanded_timeline: bool,
) -> MembershipObservation:
    if own_membership is None:
        return MembershipObservation(
            room_membership,
            None,
            None,
            None,
            None,
            False,
            is_initial,
            is_expanded_timeline,
            False,
        )
    return MembershipObservation(
        room_membership,
        own_membership.membership,
        own_membership.event_id,
        own_membership.previous_membership,
        own_membership.replaces_state,
        own_membership.is_live,
        is_initial,
        is_expanded_timeline,
        own_membership.is_unparsed,
    )
