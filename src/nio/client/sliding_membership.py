"""Pure sliding-sync membership and recovery-window decisions."""

from collections.abc import Collection, Mapping, Sequence

from ..events import BadEventType, Event, RoomMemberEvent
from ..responses import (
    SlidingSyncResponse,
    SlidingSyncRoom,
    SlidingSyncStateStub,
)
from ..sliding_sync_tokens import SlidingWindowToken
from .sync_recovery import is_own_join


def sliding_room_is_invite(room: SlidingSyncRoom) -> bool:
    return room.membership == "invite" or (
        room.membership is None and bool(room.stripped_state)
    )


def sliding_recovery_membership(room: SlidingSyncRoom) -> str:
    return "invite" if sliding_room_is_invite(room) else room.membership or "join"


def sliding_live_event_count(room: SlidingSyncRoom) -> int:
    """Normalize the live tail represented by deployed server responses."""
    if room.num_live is None:
        return 0 if room.initial or room.expanded_timeline else len(room.timeline)
    return min(max(room.num_live, 0), len(room.timeline))


def sliding_live_timeline(
    room: SlidingSyncRoom,
) -> Sequence[Event | BadEventType]:
    live_event_count = sliding_live_event_count(room)
    return room.timeline[-live_event_count:] if live_event_count else ()


def sliding_live_own_join(room: SlidingSyncRoom, user_id: str | None) -> bool:
    return any(is_own_join(event, user_id) for event in sliding_live_timeline(room))


def sliding_membership_event(
    room: SlidingSyncRoom,
    user_id: str | None,
) -> RoomMemberEvent | SlidingSyncStateStub | None:
    """Return exact current own membership, excluding historical timeline."""
    events = (*room.required_state, *sliding_live_timeline(room))
    return next(
        (
            event
            for event in reversed(events)
            if (isinstance(event, RoomMemberEvent) and event.state_key == user_id)
            or (
                isinstance(event, SlidingSyncStateStub)
                and event.type == "m.room.member"
                and event.state_key == user_id
            )
        ),
        None,
    )


def sliding_membership_proof(
    room_id: str,
    room: SlidingSyncRoom,
    *,
    user_id: str | None,
    window_tokens: Mapping[str, SlidingWindowToken],
) -> str | None:
    """Return exact current own-membership identity for this room delta."""
    membership_event = sliding_membership_event(room, user_id)
    if isinstance(membership_event, RoomMemberEvent):
        if membership_event.membership != "join":
            return None
        held = window_tokens.get(room_id)
        if (
            held is None
            or membership_event.event_id == held.membership_event_id
            or sliding_live_own_join(room, user_id)
        ):
            return membership_event.event_id
        unsigned = membership_event.source.get("unsigned", {})
        if (
            membership_event.prev_membership == "join"
            and isinstance(unsigned, dict)
            and unsigned.get("replaces_state") == held.membership_event_id
        ):
            return membership_event.event_id
        return None
    if membership_event is not None or room.initial:
        return None
    held = window_tokens.get(room_id)
    return held.membership_event_id if held else None


def sliding_membership_continues(
    room_id: str,
    room: SlidingSyncRoom,
    *,
    user_id: str | None,
    window_tokens: Mapping[str, SlidingWindowToken],
) -> bool:
    return sliding_membership_proof(
        room_id,
        room,
        user_id=user_id,
        window_tokens=window_tokens,
    ) is not None and not sliding_live_own_join(room, user_id)


def sliding_recovery_cursor(
    room_id: str,
    room: SlidingSyncRoom,
    *,
    user_id: str | None,
    window_tokens: Mapping[str, SlidingWindowToken],
) -> str | None:
    """Return the token a limited sliding window's recovery walk starts from.

    Simplified Sliding Sync has no equivalent of the ``since`` token a
    /v3/sync gap walks forward from: a ``pos`` is a connection cursor, not a
    /messages token. Chaining each room's ``prev_batch`` across responses
    turns consecutive windows into an ordinary forward walk. The overlap is
    re-fetched and removed by the usual de-duplication.

    A walk is planned for a discontinuity only when the held token belongs to
    the same proven membership. The token may be durable when recovery
    persistence is enabled, or memory-only otherwise.
    """
    if not (room.limited or room.initial) or not room.prev_batch:
        return None
    if sliding_room_is_invite(room) or room.membership in ("leave", "ban"):
        return None
    window_token = window_tokens.get(room_id)
    if not window_token:
        return None
    if not sliding_membership_continues(
        room_id,
        room,
        user_id=user_id,
        window_tokens=window_tokens,
    ):
        return None
    return window_token.token


def sliding_unrecoverable_discontinuity_room_ids(
    response: SlidingSyncResponse,
    *,
    user_id: str | None,
    window_tokens: Mapping[str, SlidingWindowToken],
    known_room_ids: Collection[str],
    current_room_ids: frozenset[str] | None,
) -> frozenset[str]:
    """Return discontinuities whose held baseline cannot be trusted."""
    return frozenset(
        room_id
        for room_id, room in response.rooms.items()
        if (current_room_ids is None or room_id in current_room_ids)
        and (room.limited or room.initial)
        and (room_id in window_tokens or room_id in known_room_ids)
        and not sliding_live_own_join(room, user_id)
        and not sliding_room_is_invite(room)
        and room.membership not in ("leave", "ban")
        and sliding_recovery_cursor(
            room_id,
            room,
            user_id=user_id,
            window_tokens=window_tokens,
        )
        is None
    )


def plan_sliding_prev_batches(
    response: SlidingSyncResponse,
    *,
    user_id: str | None,
    window_tokens: Mapping[str, SlidingWindowToken],
    current_room_ids: frozenset[str] | None,
) -> tuple[dict[str, SlidingWindowToken], list[str]]:
    """Work out each room's next walk baseline, without applying it."""
    recorded: dict[str, SlidingWindowToken] = {}
    forgotten: list[str] = []
    for room_id, room in response.rooms.items():
        if current_room_ids is not None and room_id not in current_room_ids:
            continue
        membership_event_id = sliding_membership_proof(
            room_id,
            room,
            user_id=user_id,
            window_tokens=window_tokens,
        )
        window_token = window_tokens.get(room_id)
        if sliding_room_is_invite(room) or room.membership in ("leave", "ban"):
            # A later join must not reuse a token from the old membership.
            forgotten.append(room_id)
        elif membership_event_id is None:
            forgotten.append(room_id)
        else:
            if sliding_live_own_join(room, user_id):
                forgotten.append(room_id)
            if room.prev_batch:
                recorded[room_id] = SlidingWindowToken(
                    room.prev_batch,
                    membership_event_id,
                )
            elif (
                window_token and window_token.membership_event_id != membership_event_id
            ):
                recorded[room_id] = SlidingWindowToken(
                    window_token.token,
                    membership_event_id,
                )
    return recorded, forgotten
