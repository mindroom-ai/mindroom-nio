"""Pure request-order state for sync response room effects."""

from collections.abc import Iterable
from dataclasses import dataclass, field


@dataclass
class SyncResponseOrder:
    """Monotonic request issuance and the newest accepted request per room."""

    issuance: int = 0
    room_floors: dict[str, int] = field(default_factory=dict)


def issue_sync_request(state: SyncResponseOrder) -> int:
    state.issuance += 1
    return state.issuance


def mark_room_reset(state: SyncResponseOrder, room_id: str) -> None:
    state.room_floors[room_id] = issue_sync_request(state)


def accept_response_rooms(
    state: SyncResponseOrder,
    room_ids: Iterable[str],
    request_issuance: int | None,
) -> frozenset[str]:
    """Return room slices that may apply and advance their issuance floor."""
    accepted = frozenset(
        room_id
        for room_id in room_ids
        if request_issuance is None
        or request_issuance >= state.room_floors.get(room_id, 0)
    )
    if request_issuance is not None:
        for room_id in accepted:
            state.room_floors[room_id] = request_issuance
    return accepted
