"""Build sync response views protected from reachable reset and stream races."""

from dataclasses import dataclass, replace

from ..events import AccountDataEvent, BadEventType
from ..responses import (
    DeviceOneTimeKeyCount,
    Rooms,
    SlidingSyncResponse,
    SyncResponse,
)
from .sync_reset_fence import (
    SyncRequestId,
    SyncResetFence,
    accept_current_one_time_key_counts,
    accept_current_to_device_token,
    accept_reset_safe_rooms,
)


@dataclass(frozen=True)
class OneTimeKeyCountCommit:
    components: frozenset[str]
    request_id: SyncRequestId


@dataclass(frozen=True)
class OrderedResponseView:
    response: SyncResponse | SlidingSyncResponse
    current_room_ids: frozenset[str]
    one_time_key_count_commit: OneTimeKeyCountCommit | None


def account_data_kind(
    event: AccountDataEvent | BadEventType,
) -> tuple[str, str | None]:
    """Return the parsed class and wire type of type-keyed account data."""
    wire_type = event.source.get("type")
    if not isinstance(wire_type, str):
        wire_type = getattr(event, "type", None)
    return (
        type(event).__name__,
        wire_type if isinstance(wire_type, str) else None,
    )


def response_room_ids(
    response: SyncResponse | SlidingSyncResponse,
) -> frozenset[str]:
    if isinstance(response, SyncResponse):
        return frozenset(
            response.rooms.join.keys()
            | response.rooms.invite.keys()
            | response.rooms.leave.keys()
        )
    return frozenset(response.rooms.keys() | response.room_account_data.keys())


def ordered_response_view(
    state: SyncResetFence,
    response: SyncResponse | SlidingSyncResponse,
    request_id: SyncRequestId | None,
) -> OrderedResponseView:
    """Filter a sync response against local resets and cross-stream snapshots."""
    one_time_key_count_components = frozenset(
        component
        for component, count in (
            ("curve25519", response.device_key_count.curve25519),
            ("signed_curve25519", response.device_key_count.signed_curve25519),
        )
        if count is not None
    )
    accepted_one_time_key_count_components = accept_current_one_time_key_counts(
        state,
        one_time_key_count_components,
        request_id,
    )
    if accepted_one_time_key_count_components != one_time_key_count_components:
        response = replace(
            response,
            device_key_count=DeviceOneTimeKeyCount(
                (
                    response.device_key_count.curve25519
                    if "curve25519" in accepted_one_time_key_count_components
                    else None
                ),
                (
                    response.device_key_count.signed_curve25519
                    if "signed_curve25519" in accepted_one_time_key_count_components
                    else None
                ),
            ),
        )
    one_time_key_count_commit = (
        OneTimeKeyCountCommit(
            accepted_one_time_key_count_components,
            request_id,
        )
        if accepted_one_time_key_count_components and request_id is not None
        else None
    )
    room_ids = response_room_ids(response)
    accepted = accept_reset_safe_rooms(
        state,
        room_ids,
        request_id,
    )
    if isinstance(response, SyncResponse):
        current_rooms = accepted
        return OrderedResponseView(
            replace(
                response,
                rooms=Rooms(
                    {
                        room_id: info
                        for room_id, info in response.rooms.invite.items()
                        if room_id in accepted
                    },
                    {
                        room_id: info
                        for room_id, info in response.rooms.join.items()
                        if room_id in accepted
                    },
                    {
                        room_id: info
                        for room_id, info in response.rooms.leave.items()
                        if room_id in accepted
                    },
                ),
            ),
            current_rooms,
            one_time_key_count_commit,
        )

    current_rooms = frozenset(
        room_id for room_id in response.rooms if room_id in accepted
    )
    accept_to_device_token = accept_current_to_device_token(
        state,
        present=response.to_device_next_batch is not None,
        request_id=request_id,
    )
    return OrderedResponseView(
        replace(
            response,
            rooms={
                room_id: room
                for room_id, room in response.rooms.items()
                if room_id in accepted
            },
            room_account_data={
                room_id: events
                for room_id, events in response.room_account_data.items()
                if room_id in accepted
            },
            to_device_next_batch=(
                response.to_device_next_batch if accept_to_device_token else None
            ),
        ),
        current_rooms,
        one_time_key_count_commit,
    )
