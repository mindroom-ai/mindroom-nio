"""Reject room slices issued before a successful local membership reset."""

from collections.abc import Hashable, Iterable
from dataclasses import dataclass, field


@dataclass(frozen=True)
class SyncRequestId:
    """Globally sequenced request belonging to one ordered sync stream."""

    sequence: int
    stream: Hashable


@dataclass
class SyncResetFence:
    """Reset and cross-stream floors for overlapping sync requests."""

    request_id: int = 0
    active_request_ids: set[SyncRequestId] = field(default_factory=set)
    room_cutoffs: dict[str, int] = field(default_factory=dict)
    to_device_floor: int = 0
    one_time_key_count_floors: dict[str, int] = field(default_factory=dict)


def issue_sync_request(
    state: SyncResetFence,
    stream: Hashable,
) -> SyncRequestId:
    state.request_id += 1
    request_id = SyncRequestId(state.request_id, stream)
    state.active_request_ids.add(request_id)
    return request_id


def _prune_obsolete_floors(state: SyncResetFence) -> None:
    oldest_active = min(
        (request.sequence for request in state.active_request_ids),
        default=state.request_id + 1,
    )
    state.room_cutoffs = {
        room_id: floor
        for room_id, floor in state.room_cutoffs.items()
        if floor > oldest_active
    }
    if state.to_device_floor <= oldest_active:
        state.to_device_floor = 0
    state.one_time_key_count_floors = {
        component: floor
        for component, floor in state.one_time_key_count_floors.items()
        if floor > oldest_active
    }


def finish_sync_request(state: SyncResetFence, request_id: SyncRequestId) -> None:
    """Release one request and floors that no older response can cross."""
    state.active_request_ids.discard(request_id)
    _prune_obsolete_floors(state)


def mark_room_reset(state: SyncResetFence, room_id: str) -> None:
    state.request_id += 1
    state.room_cutoffs[room_id] = state.request_id
    _prune_obsolete_floors(state)


def accept_reset_safe_rooms(
    state: SyncResetFence,
    room_ids: Iterable[str],
    request_id: SyncRequestId | None,
) -> frozenset[str]:
    """Return room slices not issued before their latest local reset."""
    return frozenset(
        room_id
        for room_id in room_ids
        if request_id is None
        or request_id.sequence >= state.room_cutoffs.get(room_id, 0)
    )


def accept_current_to_device_token(
    state: SyncResetFence,
    *,
    present: bool,
    request_id: SyncRequestId | None,
) -> bool:
    """Accept a global to-device cursor only from the newest issued request."""
    if not present:
        return False
    if request_id is None:
        return True
    if request_id.sequence < state.to_device_floor:
        return False
    state.to_device_floor = request_id.sequence
    return True


def accept_current_one_time_key_counts(
    state: SyncResetFence,
    components: Iterable[str],
    request_id: SyncRequestId | None,
) -> frozenset[str]:
    """Return count algorithms not superseded by a newer applied request."""
    if request_id is None:
        return frozenset(components)
    return frozenset(
        component
        for component in components
        if request_id.sequence >= state.one_time_key_count_floors.get(component, 0)
    )


def commit_one_time_key_counts(
    state: SyncResetFence,
    components: Iterable[str],
    request_id: SyncRequestId,
) -> None:
    """Commit applied algorithm counts without rewinding their floors."""
    for component in components:
        state.one_time_key_count_floors[component] = max(
            state.one_time_key_count_floors.get(component, 0),
            request_id.sequence,
        )
