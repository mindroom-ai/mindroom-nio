"""Reject room slices issued before a successful local membership reset."""

from collections.abc import Hashable, Iterable
from dataclasses import dataclass, field


@dataclass
class SyncResetFence:
    """Bounded ordering floors for overlapping sync requests."""

    request_id: int = 0
    active_request_ids: set[int] = field(default_factory=set)
    room_cutoffs: dict[str, int] = field(default_factory=dict)
    room_component_floors: dict[str, int] = field(default_factory=dict)
    account_data_floors: dict[Hashable, int] = field(default_factory=dict)
    to_device_floor: int = 0


def issue_sync_request(state: SyncResetFence) -> int:
    state.request_id += 1
    state.active_request_ids.add(state.request_id)
    return state.request_id


def _prune_obsolete_floors(state: SyncResetFence) -> None:
    oldest_active = min(state.active_request_ids, default=state.request_id + 1)
    state.room_cutoffs = {
        room_id: floor
        for room_id, floor in state.room_cutoffs.items()
        if floor > oldest_active
    }
    state.room_component_floors = {
        room_id: floor
        for room_id, floor in state.room_component_floors.items()
        if floor > oldest_active
    }
    state.account_data_floors = {
        component: floor
        for component, floor in state.account_data_floors.items()
        if floor > oldest_active
    }
    if state.to_device_floor <= oldest_active:
        state.to_device_floor = 0


def finish_sync_request(state: SyncResetFence, request_id: int) -> None:
    """Release one request and floors that no older response can cross."""
    state.active_request_ids.discard(request_id)
    _prune_obsolete_floors(state)


def mark_room_reset(state: SyncResetFence, room_id: str) -> None:
    state.request_id += 1
    state.room_cutoffs[room_id] = state.request_id
    state.room_component_floors.pop(room_id, None)
    _prune_obsolete_floors(state)


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


def accept_current_components(
    state: SyncResetFence,
    room_ids: Iterable[str],
    *,
    has_to_device_token: bool,
    request_id: int | None,
) -> tuple[frozenset[str], bool]:
    """Accept snapshot components without dropping independent event streams."""
    if request_id is None:
        return frozenset(room_ids), has_to_device_token

    accepted_rooms = frozenset(
        room_id
        for room_id in room_ids
        if request_id >= state.room_component_floors.get(room_id, 0)
    )
    for room_id in accepted_rooms:
        state.room_component_floors[room_id] = request_id

    accept_to_device_token = has_to_device_token and request_id >= state.to_device_floor
    if accept_to_device_token:
        state.to_device_floor = request_id
    return accepted_rooms, accept_to_device_token


def accept_current_account_data(
    state: SyncResetFence,
    components: Iterable[Hashable],
    request_id: int | None,
) -> frozenset[Hashable]:
    """Accept type-keyed account data not superseded by a newer request."""
    if request_id is None:
        return frozenset(components)

    accepted = frozenset(
        component
        for component in components
        if request_id >= state.account_data_floors.get(component, 0)
    )
    for component in accepted:
        state.account_data_floors[component] = request_id
    return accepted
