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
    """Bounded ordering floors for overlapping sync requests."""

    request_id: int = 0
    active_request_ids: set[SyncRequestId] = field(default_factory=set)
    room_cutoffs: dict[str, int] = field(default_factory=dict)
    room_component_floors: dict[tuple[Hashable, str], int] = field(default_factory=dict)
    account_data_floors: dict[tuple[Hashable, Hashable], int] = field(
        default_factory=dict
    )
    to_device_floor: int = 0
    one_time_key_count_floor: int = 0


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
    if state.one_time_key_count_floor <= oldest_active:
        state.one_time_key_count_floor = 0


def finish_sync_request(state: SyncResetFence, request_id: SyncRequestId) -> None:
    """Release one request and floors that no older response can cross."""
    state.active_request_ids.discard(request_id)
    _prune_obsolete_floors(state)


def mark_room_reset(state: SyncResetFence, room_id: str) -> None:
    state.request_id += 1
    state.room_cutoffs[room_id] = state.request_id
    state.room_component_floors = {
        component: floor
        for component, floor in state.room_component_floors.items()
        if component[1] != room_id
    }
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


def accept_current_components(
    state: SyncResetFence,
    room_ids: Iterable[str],
    *,
    has_to_device_token: bool,
    request_id: SyncRequestId | None,
) -> tuple[frozenset[str], bool]:
    """Accept snapshot components without dropping independent event streams."""
    if request_id is None:
        return frozenset(room_ids), has_to_device_token

    accepted_rooms = frozenset(
        room_id
        for room_id in room_ids
        if request_id.sequence
        >= state.room_component_floors.get((request_id.stream, room_id), 0)
    )
    for room_id in accepted_rooms:
        state.room_component_floors[(request_id.stream, room_id)] = request_id.sequence

    accept_to_device_token = (
        has_to_device_token and request_id.sequence >= state.to_device_floor
    )
    if accept_to_device_token:
        state.to_device_floor = request_id.sequence
    return accepted_rooms, accept_to_device_token


def accept_current_account_data(
    state: SyncResetFence,
    components: Iterable[Hashable],
    request_id: SyncRequestId | None,
) -> frozenset[Hashable]:
    """Accept type-keyed account data not superseded by a newer request."""
    if request_id is None:
        return frozenset(components)

    accepted = frozenset(
        component
        for component in components
        if request_id.sequence
        >= state.account_data_floors.get((request_id.stream, component), 0)
    )
    for component in accepted:
        state.account_data_floors[(request_id.stream, component)] = request_id.sequence
    return accepted


def accept_current_one_time_key_count(
    state: SyncResetFence,
    *,
    present: bool,
    request_id: SyncRequestId | None,
) -> bool:
    """Check a global count snapshot against the newest applied request."""
    if not present or request_id is None:
        return present
    return request_id.sequence >= state.one_time_key_count_floor


def commit_one_time_key_count(
    state: SyncResetFence,
    request_id: SyncRequestId,
) -> None:
    """Commit an applied global count snapshot without rewinding its floor."""
    state.one_time_key_count_floor = max(
        state.one_time_key_count_floor,
        request_id.sequence,
    )
