"""Reject room slices issued before a successful local membership reset."""

from collections.abc import Iterable
from dataclasses import dataclass, field


@dataclass
class SyncResetFence:
    """Monotonic request IDs and reset cutoffs for affected rooms."""

    request_id: int = 0
    room_cutoffs: dict[str, int] = field(default_factory=dict)


def issue_sync_request(state: SyncResetFence) -> int:
    state.request_id += 1
    return state.request_id


def mark_room_reset(state: SyncResetFence, room_id: str) -> None:
    state.room_cutoffs[room_id] = issue_sync_request(state)


def accept_reset_safe_rooms(
    state: SyncResetFence,
    room_ids: Iterable[str],
    request_id: int | None,
) -> frozenset[str]:
    """Return room slices not issued before their latest local reset."""
    return frozenset(
        room_id
        for room_id in room_ids
        if request_id is None or request_id >= state.room_cutoffs.get(room_id, 0)
    )
