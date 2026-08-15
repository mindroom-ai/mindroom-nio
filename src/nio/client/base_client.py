# Copyright © 2018, 2019 Damir Jelić <poljar@termina.org.uk>
#
# Permission to use, copy, modify, and/or distribute this software for
# any purpose with or without fee is hereby granted, provided that the
# above copyright notice and this permission notice appear in all copies.
#
# THE SOFTWARE IS PROVIDED "AS IS" AND THE AUTHOR DISCLAIMS ALL WARRANTIES
# WITH REGARD TO THIS SOFTWARE INCLUDING ALL IMPLIED WARRANTIES OF
# MERCHANTABILITY AND FITNESS. IN NO EVENT SHALL THE AUTHOR BE LIABLE FOR ANY
# SPECIAL, DIRECT, INDIRECT, OR CONSEQUENTIAL DAMAGES OR ANY DAMAGES WHATSOEVER
# RESULTING FROM LOSS OF USE, DATA OR PROFITS, WHETHER IN AN ACTION OF
# CONTRACT, NEGLIGENCE OR OTHER TORTIOUS ACTION, ARISING OUT OF OR IN
# CONNECTION WITH THE USE OR PERFORMANCE OF THIS SOFTWARE.

from __future__ import annotations

import asyncio
import inspect
import logging
from collections import defaultdict
from collections.abc import Awaitable, Callable, Coroutine
from copy import deepcopy
from dataclasses import dataclass, field, replace
from functools import wraps
from typing import (
    TYPE_CHECKING,
    Any,
    cast,
)
from uuid import uuid5

from ..crypto import ENCRYPTION_ENABLED, DeviceStore, OutgoingKeyRequest
from ..event_provenance import TimelineEventProvenance
from ..events import (
    AccountDataEvent,
    BadEvent,
    BadEventType,
    DefaultLevels,
    DummyEvent,
    EphemeralEvent,
    Event,
    ForwardedRoomKeyEvent,
    InviteEvent,
    KeyVerificationCancel,
    MegolmEvent,
    PowerLevels,
    PresenceEvent,
    RoomEncryptionEvent,
    RoomKeyEvent,
    RoomKeyRequest,
    RoomKeyRequestCancellation,
    RoomMemberEvent,
    ToDeviceEvent,
    UnknownBadEvent,
    UnknownToDeviceEvent,
)
from ..exceptions import EncryptionError, LocalProtocolError, MembersSyncError
from ..ingest._json import canonical_json as _canonical_ingestion_json
from ..ingest._json import load_json as _load_ingestion_json
from ..ingest.model import (
    RecordKind,
    RoomMemberSnapshot,
    RoomSnapshot,
    TransportKind,
    _CallbackRoute,
    _DecryptedToDeviceKind,
    _DecryptionDisposition,
    _MembershipProvenance,
    _MembershipSourceKind,
    _PreparationPhase,
    _PreparedCryptoDelta,
    _PreparedIngestionFrame,
    _PreparedIngestionRecord,
    _PreparedKeyClaim,
    _PreparedMegolmRerequest,
    _PreparedMembershipTransition,
    _PreparedQueuedToDeviceMessage,
    _PreparedWaitingKeyRequest,
    _QueuedToDeviceSubtype,
)
from ..ingest.reducer import RoomContinuity
from ..ingest.source import (
    RoomSection,
    RoomSegment,
    SyncFrame,
    _normalized_ephemeral_envelopes,
)
from ..responses import (
    ErrorResponse,
    JoinedMembersResponse,
    KeysClaimResponse,
    KeysQueryResponse,
    KeysUploadResponse,
    LoginResponse,
    LogoutResponse,
    PresenceGetResponse,
    RegisterResponse,
    Response,
    RoomContextResponse,
    RoomForgetResponse,
    RoomGetEventResponse,
    RoomInfo,
    RoomKeyRequestResponse,
    RoomMessagesResponse,
    ShareGroupSessionResponse,
    SlidingSyncResponse,
    SyncResponse,
    ToDeviceResponse,
    WhoamiResponse,
)
from ..rooms import MatrixInvitedRoom, MatrixRoom, MatrixUser

if ENCRYPTION_ENABLED:
    from ..crypto import Olm
    from ..store import DefaultStore, MatrixStore, SqliteMemoryStore, SqliteStore
if TYPE_CHECKING:
    from ..crypto import OlmDevice, Sas


from ..event_builders import DummyMessage, RoomKeyRequestMessage, ToDeviceMessage

logger = logging.getLogger(__name__)

_DECRYPTED_TO_DEVICE_KINDS = {
    RoomKeyEvent: _DecryptedToDeviceKind.ROOM_KEY,
    ForwardedRoomKeyEvent: _DecryptedToDeviceKind.FORWARDED_ROOM_KEY,
    DummyEvent: _DecryptedToDeviceKind.DUMMY,
    UnknownToDeviceEvent: _DecryptedToDeviceKind.UNKNOWN,
    BadEvent: _DecryptedToDeviceKind.BAD,
    UnknownBadEvent: _DecryptedToDeviceKind.UNKNOWN_BAD,
}


def _canonical_ingestion_object(payload: bytes, field_name: str) -> dict[Any, Any]:
    value = _load_ingestion_json(payload, field_name)
    if type(value) is not dict or _canonical_ingestion_json(value) != payload:
        raise ValueError(f"{field_name} must be a canonical JSON object")
    return value


def _matrix_event_id(value: dict[Any, Any]) -> str | None:
    event_id = value.get("event_id")
    return event_id if type(event_id) is str and event_id else None


def _require_ingestion_event_type(
    value: dict[Any, Any],
    field_name: str,
) -> dict[Any, Any]:
    if type(value.get("type")) is not str or not value["type"]:
        raise ValueError(f"{field_name} type must be nonempty")
    return value


def _nonempty_string_list(value: object) -> bool:
    return type(value) is list and all(type(item) is str and item for item in value)


def _has_fields(value: object, **expected: object) -> bool:
    return type(value) is dict and all(
        value.get(name) == expected_value for name, expected_value in expected.items()
    )


def _require_parsed_ingestion_event(
    event: object,
    expected: type,
    field_name: str,
) -> object:
    if type(event) is not expected:
        raise ValueError(f"{field_name} is invalid")
    return event


_ParsedIngestionEvent = tuple[bytes, dict[Any, Any], Any]
_ParsedIngestionRoomSegment = tuple[
    RoomSegment,
    tuple[_ParsedIngestionEvent, ...],
    tuple[_ParsedIngestionEvent, ...],
    tuple[_ParsedIngestionEvent, ...],
]


def _parse_ingestion_events(
    payloads: tuple[bytes, ...],
    field_name: str,
    parser: Callable[[dict[Any, Any]], object],
) -> tuple[_ParsedIngestionEvent, ...]:
    return tuple(
        (
            payload,
            raw := _require_ingestion_event_type(
                _canonical_ingestion_object(payload, field_name),
                field_name,
            ),
            parser(deepcopy(raw)),
        )
        for payload in payloads
    )


def _parse_ingestion_room_segment(
    segment: RoomSegment,
) -> _ParsedIngestionRoomSegment:
    state_parser = (
        InviteEvent.parse_event
        if segment.section in {RoomSection.INVITE, RoomSection.KNOCK}
        else Event.parse_event
    )
    return (
        segment,
        _parse_ingestion_events(segment.state_json, "state event", state_parser),
        _parse_ingestion_events(
            segment.timeline_json,
            "timeline event",
            Event.parse_event,
        ),
        _parse_ingestion_events(
            segment.room_account_data_json,
            "room account-data event",
            AccountDataEvent.parse_event,
        ),
    )


def _encrypted_room_ids_from_parsed_segments(
    parsed_segments: tuple[_ParsedIngestionRoomSegment, ...],
    live_room_ids: set[str],
) -> tuple[str, ...]:
    return tuple(
        sorted(
            {
                segment.room_id
                for segment, state, timeline, _account_data in parsed_segments
                if segment.room_id in live_room_ids
                and any(
                    isinstance(event, RoomEncryptionEvent)
                    for _payload, _raw, event in (*state, *timeline)
                )
            }
        )
    )


def _prepared_frame_encrypted_room_ids(
    frame: SyncFrame,
    live_room_ids: set[str],
) -> tuple[str, ...]:
    """Rebuild the frame-local encryption effect from authenticated source."""
    return _encrypted_room_ids_from_parsed_segments(
        tuple(
            _parse_ingestion_room_segment(segment) for segment in frame.room_segments
        ),
        live_room_ids,
    )


def _prepared_waiting_key_request(
    event: RoomKeyRequest,
) -> _PreparedWaitingKeyRequest:
    if type(event.source) is not dict:
        raise TypeError("waiting room-key-request source must be a dict")
    source_json = _canonical_ingestion_json(event.source)
    raw = _canonical_ingestion_object(source_json, "waiting room-key request")
    content = raw.get("content")
    body = content.get("body") if type(content) is dict else None
    if (
        not _has_fields(raw, type="m.room_key_request", sender=event.sender)
        or not _has_fields(
            content,
            action="request",
            requesting_device_id=event.requesting_device_id,
            request_id=event.request_id,
        )
        or not _has_fields(
            body,
            room_id=event.room_id,
            sender_key=event.sender_key,
            session_id=event.session_id,
            algorithm=event.algorithm,
        )
    ):
        raise ValueError("waiting room-key-request fields disagree with source")
    return _PreparedWaitingKeyRequest(
        source_json,
        *(
            getattr(event, name)
            for name in (
                "sender",
                "requesting_device_id",
                "request_id",
                "room_id",
                "sender_key",
                "session_id",
                "algorithm",
            )
        ),
    )


def _prepared_megolm_rerequest(
    event: MegolmEvent,
) -> _PreparedMegolmRerequest:
    if type(event.source) is not dict:
        raise TypeError("Megolm rerequest source must be a dict")
    source_json = _canonical_ingestion_json(event.source)
    raw = _canonical_ingestion_object(source_json, "Megolm rerequest event")
    content = raw.get("content")
    if (
        not _has_fields(
            raw,
            type="m.room.encrypted",
            event_id=event.event_id,
            sender=event.sender,
        )
        or ("room_id" in raw and raw.get("room_id") != event.room_id)
        or not _has_fields(
            content,
            device_id=event.device_id,
            sender_key=event.sender_key,
            session_id=event.session_id,
            algorithm=event.algorithm,
        )
        or type(event.room_id) is not str
        or not event.room_id
    ):
        raise ValueError("Megolm rerequest fields disagree with source")
    return _PreparedMegolmRerequest(
        source_json,
        *(
            getattr(event, name)
            for name in (
                "room_id",
                "event_id",
                "sender",
                "device_id",
                "sender_key",
                "session_id",
                "algorithm",
            )
        ),
    )


def _prepared_queued_to_device_message(
    message: ToDeviceMessage,
    rerequest_events: tuple[_PreparedMegolmRerequest, ...],
) -> _PreparedQueuedToDeviceMessage:
    for field_name, value in (
        ("event type", message.type),
        ("recipient user ID", message.recipient),
        ("recipient device ID", message.recipient_device),
    ):
        if type(value) is not str or not value:
            raise ValueError(f"queued to-device {field_name} must be nonempty")
    if type(message.content) is not dict:
        raise TypeError("queued to-device content must be a dict")

    request_context: tuple[str | None, str | None, str | None, str | None]
    if isinstance(message, DummyMessage):
        subtype = _QueuedToDeviceSubtype.DUMMY
        request_context = (None, None, None, None)
    elif isinstance(message, RoomKeyRequestMessage):
        subtype = _QueuedToDeviceSubtype.ROOM_KEY_REQUEST
        request_context = (
            message.request_id,
            message.session_id,
            message.room_id,
            message.algorithm,
        )
        body = message.content.get("body")
        if not _has_fields(
            message.content,
            action="request",
            request_id=message.request_id,
        ) or not _has_fields(
            body,
            session_id=message.session_id,
            room_id=message.room_id,
            algorithm=message.algorithm,
        ):
            raise ValueError("queued room-key-request context disagrees with content")
    elif type(message) is ToDeviceMessage:
        subtype = _QueuedToDeviceSubtype.GENERIC
        request_context = (None, None, None, None)
    else:
        raise TypeError("queued to-device message subtype is unsupported")

    return _PreparedQueuedToDeviceMessage(
        subtype,
        message.type,
        message.recipient,
        message.recipient_device,
        _canonical_ingestion_json(message.content),
        request_context[0],
        request_context[1],
        request_context[2],
        request_context[3],
        rerequest_events,
    )


def _prepared_crypto_delta_snapshot(
    frame: SyncFrame,
    olm: Olm,
    encrypted_room_ids: tuple[str, ...],
) -> _PreparedCryptoDelta:
    """Freeze the Task4C-local crypto state without consuming any of it."""
    key_claim_map = (
        olm.get_users_for_key_claiming()
        if olm.wedged_devices or olm.key_request_devices_no_session
        else {}
    )
    if not isinstance(key_claim_map, dict):
        raise TypeError("key-claim map must be a dict")
    wedged_targets = {
        (device.user_id, device.device_id) for device in olm.wedged_devices
    }
    waiting_targets = {
        (device.user_id, device.device_id)
        for device in olm.key_request_devices_no_session
    }
    first_dummy_targets = {
        (message.recipient, message.recipient_device)
        for message in olm.outgoing_to_device_messages
        if isinstance(message, DummyMessage)
    }
    rerequest_buckets: dict[
        tuple[str, str],
        tuple[_PreparedMegolmRerequest, ...],
    ] = {}
    for target, queued_events in olm.key_re_requests_events.items():
        prepared_events: list[_PreparedMegolmRerequest] = []
        seen_sessions: set[str] = set()
        for event in queued_events:
            if type(event) is not MegolmEvent:
                raise TypeError("Megolm rerequest event is invalid")
            if event.session_id in seen_sessions:
                continue
            prepared_event = _prepared_megolm_rerequest(event)
            if (
                prepared_event.sender_user_id,
                prepared_event.sender_device_id,
            ) != target:
                raise ValueError("Megolm rerequest disagrees with bucket target")
            seen_sessions.add(prepared_event.session_id)
            prepared_events.append(prepared_event)
        rerequest_buckets[target] = tuple(prepared_events)

    key_claims: list[_PreparedKeyClaim] = []
    for user_id in sorted(key_claim_map):
        device_ids = key_claim_map[user_id]
        for device_id in sorted(set(device_ids)):
            target = (user_id, device_id)
            waiting = olm.key_requests_waiting_for_session.get(target, {})
            waiting_requests: list[_PreparedWaitingKeyRequest] = []
            for request_id, event in waiting.items():
                if type(request_id) is not str or type(event) is not RoomKeyRequest:
                    raise TypeError("waiting room-key request is invalid")
                prepared_request = _prepared_waiting_key_request(event)
                if (request_id, user_id, device_id) != (
                    prepared_request.request_id,
                    prepared_request.sender_user_id,
                    prepared_request.requesting_device_id,
                ):
                    raise ValueError(
                        "waiting room-key request disagrees with claim target"
                    )
                waiting_requests.append(prepared_request)
            key_claims.append(
                _PreparedKeyClaim(
                    user_id,
                    device_id,
                    target in wedged_targets,
                    target in waiting_targets,
                    tuple(waiting_requests),
                    (
                        ()
                        if target in first_dummy_targets
                        else rerequest_buckets.get(target, ())
                    ),
                )
            )
    owned_waiting_targets = {
        (claim.user_id, claim.device_id) for claim in key_claims if claim.was_waiting
    }
    if any(
        requests and target not in owned_waiting_targets
        for target, requests in olm.key_requests_waiting_for_session.items()
    ):
        raise ValueError("waiting room-key-request bucket has no claim owner")

    bound_dummy_targets: set[tuple[str, str]] = set()
    queued_to_device_messages: list[_PreparedQueuedToDeviceMessage] = []
    for message in olm.outgoing_to_device_messages:
        rerequest_events: tuple[_PreparedMegolmRerequest, ...] = ()
        if isinstance(message, DummyMessage):
            target = (message.recipient, message.recipient_device)
            if target not in bound_dummy_targets:
                bound_dummy_targets.add(target)
                rerequest_events = rerequest_buckets.get(target, ())
        queued_to_device_messages.append(
            _prepared_queued_to_device_message(message, rerequest_events)
        )
    owned_rerequest_targets = bound_dummy_targets | {
        (claim.user_id, claim.device_id)
        for claim in key_claims
        if claim.rerequest_events
    }
    if any(
        events and target not in owned_rerequest_targets
        for target, events in rerequest_buckets.items()
    ):
        raise ValueError("Megolm rerequest bucket has no claim or dummy owner")
    return _PreparedCryptoDelta(
        encrypted_room_ids,
        tuple(sorted(olm.users_for_key_query)),
        olm.uploaded_key_count,
        frame.one_time_key_counts_json,
        frame.unused_fallback_key_types_json,
        tuple(key_claims),
        tuple(queued_to_device_messages),
    )


def _room_snapshot(
    room: MatrixRoom,
    membership_epoch: int,
    own_membership: str | None,
) -> RoomSnapshot:
    defaults = room.power_levels.defaults
    members = tuple(
        RoomMemberSnapshot(
            user.user_id,
            "invite" if user.invited else "join",
            user.display_name,
            user.avatar_url,
            user.power_level,
        )
        for user in sorted(room.users.values(), key=lambda value: value.user_id)
    )
    return RoomSnapshot(
        room.room_id,
        membership_epoch,
        room.own_user_id,
        own_membership,
        room.encrypted,
        room.name,
        room.canonical_alias,
        room.topic,
        room.room_avatar_url,
        room.join_rule,
        room.room_version,
        room.guest_access,
        _canonical_ingestion_json(
            {
                "ban": defaults.ban,
                "creators": room.power_levels.creators,
                "events": room.power_levels.events,
                "events_default": defaults.events_default,
                "invite": defaults.invite,
                "kick": defaults.kick,
                "notifications": defaults.notifications,
                "redact": defaults.redact,
                "state_default": defaults.state_default,
                "users": room.power_levels.users,
                "users_default": defaults.users_default,
            }
        ),
        members,
    )


def _room_from_snapshot(
    snapshot: RoomSnapshot,
    *,
    invited: bool,
) -> MatrixRoom:
    """Reconstruct exactly the MatrixRoom state carried by a durable snapshot."""
    room: MatrixRoom = (
        MatrixInvitedRoom(snapshot.room_id, snapshot.own_user_id)
        if invited
        else MatrixRoom(
            snapshot.room_id,
            snapshot.own_user_id,
            snapshot.encrypted,
        )
    )
    room.encrypted = snapshot.encrypted
    room.name = snapshot.name
    room.canonical_alias = snapshot.canonical_alias
    room.topic = snapshot.topic
    room.room_avatar_url = snapshot.avatar_url
    room.join_rule = snapshot.join_rule  # type: ignore[assignment]
    room.room_version = snapshot.room_version  # type: ignore[assignment]
    room.guest_access = snapshot.guest_access  # type: ignore[assignment]

    if snapshot.power_levels_json is not None:
        value = _canonical_ingestion_object(
            snapshot.power_levels_json,
            "room snapshot power levels",
        )
        fields = {
            "ban",
            "creators",
            "events",
            "events_default",
            "invite",
            "kick",
            "notifications",
            "redact",
            "state_default",
            "users",
            "users_default",
        }
        integers = (
            "ban",
            "events_default",
            "invite",
            "kick",
            "redact",
            "state_default",
            "users_default",
        )

        def integer_map(name: str) -> dict[str, int]:
            mapping = value.get(name)
            if type(mapping) is not dict or any(
                type(key) is not str or type(level) is not int
                for key, level in mapping.items()
            ):
                raise ValueError(f"room snapshot {name} must map strings to integers")
            return dict(mapping)

        creators = value.get("creators")
        if set(value) != fields or any(
            type(value.get(name)) is not int for name in integers
        ):
            raise ValueError("room snapshot power levels are invalid")
        if type(creators) is not dict or any(
            type(user_id) is not str or creator is not True
            for user_id, creator in creators.items()
        ):
            raise ValueError("room snapshot creators are invalid")
        defaults = DefaultLevels(
            value["ban"],
            value["invite"],
            value["kick"],
            value["redact"],
            value["state_default"],
            value["events_default"],
            value["users_default"],
            integer_map("notifications"),
        )
        room.power_levels = PowerLevels(
            defaults,
            integer_map("users"),
            integer_map("events"),
            dict(creators),
        )
        room.creators = set(creators)

    for member in snapshot.members:
        if member.membership not in {"join", "invite"} or not room.add_member(
            member.user_id,
            member.display_name,
            member.avatar_url,
            invited=member.membership == "invite",
        ):
            raise ValueError("room snapshot members are invalid")
        room.users[member.user_id].power_level = member.power_level
    return room


_ROOM_AUXILIARY_FIELDS = (
    "federate",
    "room_type",
    "history_visibility",
    "parents",
    "children",
    "typing_users",
    "read_receipts",
    "threaded_read_receipts",
    "summary",
    "fully_read_marker",
    "tags",
    "unread_notifications",
    "unread_highlights",
    "members_synced",
    "replacement_room",
)


def _overlay_room_snapshot(
    room: MatrixRoom | None,
    snapshot: RoomSnapshot,
    *,
    invited: bool,
) -> MatrixRoom:
    """Apply snapshot-owned projection while preserving live auxiliary state."""
    if type(snapshot) is not RoomSnapshot or type(invited) is not bool:
        raise TypeError("room snapshot overlay inputs are invalid")
    candidate = _room_from_snapshot(snapshot, invited=invited)
    if (
        candidate.room_id != snapshot.room_id
        or candidate.own_user_id != snapshot.own_user_id
        or _room_snapshot(
            candidate,
            snapshot.membership_epoch,
            snapshot.own_membership,
        )
        != snapshot
    ):
        raise ValueError("room snapshot does not reconstruct exactly")
    if room is None:
        return candidate
    if type(room) not in (MatrixRoom, MatrixInvitedRoom) or (
        room.room_id,
        room.own_user_id,
    ) != (snapshot.room_id, snapshot.own_user_id):
        raise ValueError("existing room identity is invalid")
    if type(room.users) is not dict or any(
        type(user_id) is not str
        or type(user) is not MatrixUser
        or user.user_id != user_id
        for user_id, user in room.users.items()
    ):
        raise ValueError("existing room member identities are invalid")

    if type(room) is not type(candidate):
        for field_name in _ROOM_AUXILIARY_FIELDS:
            setattr(candidate, field_name, getattr(room, field_name))
        for user_id, user in candidate.users.items():
            current = room.users.get(user_id)
            if current is not None:
                (
                    user.presence,
                    user.last_active_ago,
                    user.currently_active,
                    user.status_msg,
                ) = (
                    current.presence,
                    current.last_active_ago,
                    current.currently_active,
                    current.status_msg,
                )
        return candidate

    projected_users: dict[str, MatrixUser] = {}
    projected_invited_users: dict[str, MatrixUser] = {}
    user_updates: list[tuple[MatrixUser, MatrixUser]] = []
    for user_id, snapshot_user in candidate.users.items():
        projected_user = room.users.get(user_id)
        if projected_user is None:
            projected_user = snapshot_user
        else:
            user_updates.append((projected_user, snapshot_user))
        projected_users[user_id] = projected_user
        if snapshot_user.invited:
            projected_invited_users[user_id] = projected_user

    for user, snapshot_user in user_updates:
        user.display_name = snapshot_user.display_name
        user.avatar_url = snapshot_user.avatar_url
        user.power_level = snapshot_user.power_level
        user.invited = snapshot_user.invited
    room.encrypted = candidate.encrypted
    room.name = candidate.name
    room.canonical_alias = candidate.canonical_alias
    room.topic = candidate.topic
    room.room_avatar_url = candidate.room_avatar_url
    room.join_rule = candidate.join_rule
    room.room_version = candidate.room_version
    room.guest_access = candidate.guest_access
    room.power_levels = candidate.power_levels
    room.creators = candidate.creators
    room.users = projected_users
    room.invited_users = projected_invited_users
    room.names = candidate.names
    return room


@dataclass(frozen=True)
class _IngestionStoreSnapshot:
    store: MatrixStore | None
    olm: Olm | None
    rooms: dict[str, MatrixRoom]
    invited_rooms: dict[str, MatrixInvitedRoom]
    encrypted_rooms: set[str]
    next_batch: str
    loaded_sync_token: str | None
    recovery: object | None
    sliding_tokens: object | None


def logged_in(func):
    @wraps(func)
    def wrapper(self: Client, *args, **kwargs):
        self._assert_logged_in()
        return func(self, *args, **kwargs)

    return wrapper


def logged_in_async(func):
    if inspect.isasyncgenfunction(func):

        @wraps(func)
        async def wrapper_async_gen(self, *args, **kwargs):
            self._assert_logged_in()
            async for item in func(self, *args, **kwargs):
                yield item

        return wrapper_async_gen

    else:

        @wraps(func)
        async def wrapper(self, *args, **kwargs):
            self._assert_logged_in()
            return await func(self, *args, **kwargs)

        return wrapper


def store_loaded(fn):
    @wraps(fn)
    def inner(self, *args, **kwargs):
        if not self.store or not self.olm:
            raise LocalProtocolError("Matrix store and olm account is not loaded.")
        return fn(self, *args, **kwargs)

    return inner


@dataclass
class ClientCallback:
    """nio internal callback class."""

    func: Callable[..., None] | Callable[..., Awaitable[None]] = field()
    filter: tuple[type, ...] | type | None = None

    def _execute(
        self,
        event,
        room: MatrixRoom | None = None,
        *callback_args,
    ) -> Awaitable | None:
        """
        Checks the filter and executes the function once.
        sync_execute and async_execute will each determine
        how to run an awaitable if one is returned.
        """
        if self.filter is None or isinstance(event, self.filter):
            if room:
                return self.func(room, event, *callback_args)
            else:
                return self.func(event, *callback_args)

    def sync_execute(
        self,
        event,
        room: MatrixRoom | None = None,
        *callback_args,
    ) -> None:
        """Execute callback from synchronous context."""
        result = self._execute(event, room, *callback_args)
        if inspect.iscoroutine(result):
            asyncio.run(result)

    async def async_execute(
        self,
        event,
        room: MatrixRoom | None = None,
        *callback_args,
    ) -> None:
        """Execute callback from asynchronous context."""
        result = self._execute(event, room, *callback_args)
        if inspect.isawaitable(result):
            await result


@dataclass(frozen=True)
class ClientConfig:
    """nio client configuration.

    Attributes:
        store (MatrixStore, optional): The store that should be used for state
            storage.
        store_name (str, optional): Filename that should be used for the
            store.
        encryption_enabled (bool, optional): Should end to end encryption be
            used.
        pickle_key (str, optional): A passphrase that will be used to encrypt
            end to end encryption keys.
        store_sync_tokens (bool, optional): Should the client store and restore
            sync tokens.
        custom_headers (Dict[str, str]): A dictionary of custom http headers.
        replace_rotated_device_keys (bool, optional): What to do when a device
            re-uploads different, validly self-signed identity keys under an
            existing device id (e.g. a client that kept its access token but
            lost its crypto store and re-registered its identity). If False
            (default, matching upstream nio and matrix-rust-sdk) the update is
            ignored and the stale identity is kept forever, which permanently
            breaks olm-authenticated exchanges with that device. If True the
            stored identity is replaced, any earned trust is reset to unset
            (blacklists are kept), and the change is logged as a warning.
            Only enable this for trust-on-first-use deployments that never
            rely on device verification.

    Raises an ImportWarning if encryption_enabled is true but the dependencies
    for encryption aren't installed.

    """

    store: type[MatrixStore] | None = DefaultStore if ENCRYPTION_ENABLED else None

    encryption_enabled: bool = ENCRYPTION_ENABLED

    store_name: str = ""
    pickle_key: str = "DEFAULT_KEY"
    store_sync_tokens: bool = False
    custom_headers: dict[str, str] | None = None
    replace_rotated_device_keys: bool = False

    def __post_init__(self):
        if not ENCRYPTION_ENABLED and self.encryption_enabled:
            raise ImportWarning(
                "Encryption is enabled in the client "
                "configuration but dependencies for E2E "
                "encryption aren't installed."
            )


class Client:
    """Matrix no-IO client.

    Attributes:
       access_token (str): Token authorizing the user with the server. Is set
           after logging in.
       user_id (str): The full mxid of the current user. This is set after
           logging in.
       next_batch (str): The current sync token.
       rooms (Dict[str, MatrixRoom)): A dictionary containing a mapping of room
           ids to MatrixRoom objects. All the rooms a user is joined to will be
           here after a sync.
       invited_rooms (Dict[str, MatrixInvitedRoom)): A dictionary containing
           a mapping of room ids to MatrixInvitedRoom objects. All the rooms
           a user is invited to will be here after a sync.

    Args:
       user (str): User that will be used to log in.
       device_id (str, optional): An unique identifier that distinguishes
           this client instance. If not set the server will provide one after
           log in.
       store_dir (str, optional): The directory that should be used for state
           storage.
       config (ClientConfig, optional): Configuration for the client.

    """

    def __init__(
        self,
        user: str,
        device_id: str | None = None,
        store_path: str | None = "",
        config: ClientConfig | None = None,
    ):
        self.user = user
        self.device_id = device_id
        self.store_path = store_path
        self.olm: Olm | None = None
        self.store: MatrixStore | None = None
        self._ingestion_store_snapshot: _IngestionStoreSnapshot | None = None
        self._ingestion_poisoned = False
        self.config = config or ClientConfig()

        self.user_id = ""
        # TODO Turn this into a optional string.
        self.access_token: str = ""
        self.next_batch = ""
        self.loaded_sync_token = ""

        self.rooms: dict[str, MatrixRoom] = {}
        self.invited_rooms: dict[str, MatrixInvitedRoom] = {}
        self.encrypted_rooms: set[str] = set()

        self.event_callbacks: list[ClientCallback] = []
        self.ephemeral_callbacks: list[ClientCallback] = []
        self.to_device_callbacks: list[ClientCallback] = []
        self.presence_callbacks: list[ClientCallback] = []
        self.global_account_data_callbacks: list[ClientCallback] = []
        self.room_account_data_callbacks: list[ClientCallback] = []

    @property
    def logged_in(self) -> bool:
        """Check if we are logged in.

        Returns True if the client is logged in to the server, False otherwise.
        """
        return bool(self.access_token)

    @property  # type: ignore
    @store_loaded
    def device_store(self) -> DeviceStore:
        """Store containing known devices.

        Returns a ``DeviceStore`` holding all known olm devices.
        """
        assert self.olm
        return self.olm.device_store

    @property  # type: ignore
    @store_loaded
    def olm_account_shared(self) -> bool:
        """Check if the clients Olm account is shared with the server.

        Returns True if the Olm account is shared, False otherwise.
        """
        assert self.olm
        return self.olm.account.shared

    @property
    def users_for_key_query(self) -> set[str]:
        """Users for whom we should make a key query."""
        if not self.olm:
            return set()

        return self.olm.users_for_key_query

    @property
    def should_upload_keys(self) -> bool:
        """Check if the client should upload encryption keys.

        Returns True if encryption keys need to be uploaded, false otherwise.
        """
        if not self.olm:
            return False

        return self.olm.should_upload_keys

    @property
    def should_query_keys(self) -> bool:
        """Check if the client should make a key query call to the server.

        Returns True if a key query is necessary, false otherwise.
        """
        if not self.olm:
            return False

        return self.olm.should_query_keys

    @property
    def should_claim_keys(self) -> bool:
        """Check if the client should claim one-time keys for some users.

        This should be periodically checked and if true a keys claim request
        should be made with the return value of a
        `get_users_for_key_claiming()` call as the payload.

        Keys need to be claimed for various reasons. Every time we need to send
        an encrypted message to a device and we don't have a working Olm
        session with them we need to claim one-time keys to create a new Olm
        session.

        Returns True if a key query is necessary, false otherwise.
        """
        if not self.olm:
            return False

        return bool(self.olm.wedged_devices or self.olm.key_request_devices_no_session)

    @property
    def outgoing_key_requests(self) -> dict[str, OutgoingKeyRequest]:
        """Our active key requests that we made."""
        return self.olm.outgoing_key_requests if self.olm else {}

    @property
    def key_verifications(self) -> dict[str, Sas]:
        """Key verifications that the client is participating in."""
        return self.olm.key_verifications if self.olm else {}

    @property
    def outgoing_to_device_messages(self) -> list[ToDeviceMessage]:
        """To-device messages that we need to send out."""
        return self.olm.outgoing_to_device_messages if self.olm else []

    def _assert_logged_in(self):
        """Assert that the client is logged in."""
        if not self.logged_in:
            raise LocalProtocolError("Not logged in.")

    def get_active_sas(self, user_id: str, device_id: str) -> Sas | None:
        """Find a non-canceled SAS verification object for the provided user.

        Args:
            user_id (str): The user for which we should find a SAS verification
                object.
            device_id (str): The device_id for which we should find the SAS
                verification object.

        Returns the object if it's found, otherwise None.
        """
        if not self.olm:
            return None

        return self.olm.get_active_sas(user_id, device_id)

    def load_store(self):
        """Load the session store and olm account.

        If the SqliteMemoryStore is set as the store a store path isn't
        required, if no store path is provided and a store class that requires
        a path is used this method will be a no op.

        This method does nothing if the store is already loaded.

        Raises LocalProtocolError if a store class, user_id and device_id are
            not set.
        """
        if self.store:
            return

        if not self.user_id:
            raise LocalProtocolError("User id is not set")

        if not self.device_id:
            raise LocalProtocolError("Device id is not set")

        if not self.config.store:
            raise LocalProtocolError("No store class was provided in the config.")

        if self.config.encryption_enabled:
            if self.config.store is SqliteMemoryStore:
                self.store = self.config.store(
                    self.user_id,
                    self.device_id,
                    self.config.pickle_key,
                )
            else:
                if not self.store_path:
                    return

                self.store = self.config.store(
                    self.user_id,
                    self.device_id,
                    self.store_path,
                    self.config.pickle_key,
                    self.config.store_name,
                )
            assert self.store

            self.olm = Olm(
                self.user_id,
                self.device_id,
                self.store,
                replace_rotated_device_keys=self.config.replace_rotated_device_keys,
            )
            self.encrypted_rooms = self.store.load_encrypted_rooms()

            if self.config.store_sync_tokens:
                self.loaded_sync_token = self.store.load_sync_token()

    def _assert_ingestion_not_poisoned(self) -> None:
        if self._ingestion_poisoned:
            raise LocalProtocolError(
                "owned ingestion session is poisoned; reopen with a fresh client"
            )

    def _poison_ingestion(self) -> None:
        self._ingestion_poisoned = True

    def _attach_ingestion_store(
        self,
        store: MatrixStore,
        *,
        rooms: dict[str, MatrixRoom] | None = None,
        invited_rooms: dict[str, MatrixInvitedRoom] | None = None,
    ) -> None:
        """Attach one exact borrowed SqliteStore before constructing Olm."""
        self._assert_ingestion_not_poisoned()
        if type(store) is not SqliteStore:
            raise LocalProtocolError("owned ingestion requires exact SqliteStore")
        if (
            self.store is not None
            or self.olm is not None
            or self._ingestion_store_snapshot is not None
        ):
            raise LocalProtocolError("client already has a Matrix store or Olm account")
        if not self.config.encryption_enabled:
            raise LocalProtocolError("owned ingestion requires encryption")
        device_id = self.device_id
        if type(device_id) is not str:
            raise LocalProtocolError("owned ingestion requires a bound device id")
        if (store.user_id, store.device_id) != (self.user_id, self.device_id):
            raise LocalProtocolError("borrowed store identity does not match client")
        if (rooms is None) != (invited_rooms is None):
            raise LocalProtocolError(
                "owned room restore maps must be supplied together"
            )
        attached_rooms = self.rooms if rooms is None else rooms
        attached_invited_rooms = (
            self.invited_rooms if invited_rooms is None else invited_rooms
        )
        if set(attached_rooms) & set(attached_invited_rooms):
            raise LocalProtocolError("owned room restore maps overlap")

        previous_store = self.store
        previous_olm = self.olm
        previous_rooms = self.rooms
        previous_invited_rooms = self.invited_rooms
        previous_encrypted_rooms = self.encrypted_rooms
        previous_next_batch = self.next_batch
        previous_loaded_sync_token = self.loaded_sync_token
        previous_recovery = getattr(self, "_recovery", None)
        previous_sliding_tokens = getattr(self, "_sliding_room_prev_batch", None)
        snapshot = _IngestionStoreSnapshot(
            previous_store,
            previous_olm,
            previous_rooms,
            previous_invited_rooms,
            previous_encrypted_rooms,
            previous_next_batch,
            previous_loaded_sync_token,
            previous_recovery,
            previous_sliding_tokens,
        )
        try:
            self.store = store
            olm = Olm._from_persisted_account(
                self.user_id,
                device_id,
                store,
                replace_rotated_device_keys=self.config.replace_rotated_device_keys,
            )
            encrypted_rooms = store.load_encrypted_rooms()
            loaded_sync_token = previous_loaded_sync_token
            if self.config.store_sync_tokens:
                loaded_sync_token = store.load_sync_token()
            recovery = previous_recovery
            sliding_tokens = previous_sliding_tokens
            recovery_enabled = getattr(self, "_recovery_persistence_enabled", False)
            if recovery_enabled:
                from .sync_recovery import RecoveryState, load_recovery_state

                recovery = RecoveryState(
                    max_held_events=self.config.backfill_max_events  # type: ignore[attr-defined]
                )
                load_recovery_state(recovery, *store.load_sync_recovery())
                sliding_tokens = dict(store.load_sliding_window_tokens())
            self.olm = olm
            self.rooms = attached_rooms
            self.invited_rooms = attached_invited_rooms
            self.encrypted_rooms = encrypted_rooms
            self.loaded_sync_token = loaded_sync_token
            if previous_recovery is not None:
                setattr(self, "_recovery", recovery)
            if previous_sliding_tokens is not None:
                setattr(self, "_sliding_room_prev_batch", sliding_tokens)
            self._ingestion_store_snapshot = snapshot
        except BaseException:
            self.olm = previous_olm
            self.store = previous_store
            self.rooms = previous_rooms
            self.invited_rooms = previous_invited_rooms
            self.encrypted_rooms = previous_encrypted_rooms
            self.next_batch = previous_next_batch
            self.loaded_sync_token = previous_loaded_sync_token
            if previous_recovery is not None:
                setattr(self, "_recovery", previous_recovery)
            if previous_sliding_tokens is not None:
                setattr(self, "_sliding_room_prev_batch", previous_sliding_tokens)
            raise

    def _detach_ingestion_store(self, store: MatrixStore) -> None:
        """Detach the exact borrowed store without touching client HTTP state."""
        snapshot = self._ingestion_store_snapshot
        if (
            snapshot is None
            or self.store is not store
            or self.olm is None
            or self.olm.store is not store
        ):
            raise LocalProtocolError("client no longer owns the borrowed store")
        self.olm = None
        self.store = None
        self.rooms = snapshot.rooms
        self.invited_rooms = snapshot.invited_rooms
        self.encrypted_rooms = snapshot.encrypted_rooms
        self.next_batch = snapshot.next_batch
        self.loaded_sync_token = cast(Any, snapshot.loaded_sync_token)
        if snapshot.recovery is not None:
            setattr(self, "_recovery", snapshot.recovery)
        if snapshot.sliding_tokens is not None:
            setattr(self, "_sliding_room_prev_batch", snapshot.sliding_tokens)
        self._ingestion_store_snapshot = None

    def restore_login(
        self,
        user_id: str,
        device_id: str,
        access_token: str,
    ):
        """Restore a previous login to the homeserver.

        Args:
           user_id (str): The full mxid of the current user.
           device_id (str): An unique identifier that distinguishes
               this client instance.
           access_token (str): Token authorizing the user with the server.
        """
        self.user_id = user_id
        self.device_id = device_id
        self.access_token = access_token
        if ENCRYPTION_ENABLED:
            self.load_store()

    def room_contains_unverified(self, room_id: str) -> bool:
        """Check if a room contains unverified devices.

        Args:
            room_id (str): Room id of the room that should be checked.

        Returns True if the room contains unverified devices, false otherwise.
        Returns False if no Olm session is loaded or if the room isn't
        encrypted.
        """
        try:
            room = self.rooms[room_id]
        except KeyError:
            raise LocalProtocolError(f"No room found with room id {room_id}")

        if not room.encrypted:
            return False

        if not self.olm:
            return False

        for user in room.users:
            if not self.olm.user_fully_verified(user):
                return True

        return False

    def _invalidate_session_for_member_event(self, room_id: str):
        if not self.olm:
            return
        self.invalidate_outbound_session(room_id)

    @store_loaded
    def invalidate_outbound_session(self, room_id: str):
        """Explicitly remove encryption keys for a room.

        Args:
            room_id (str): Room id for the room the encryption keys should be
                removed.
        """
        assert self.olm
        session = self.olm.outbound_group_sessions.pop(room_id, None)

        # There is no need to invalidate the session if it was never
        # shared, put it back where it was.
        if session and not session.shared:
            self.olm.outbound_group_sessions[room_id] = session
        elif session:
            logger.info(f"Invalidating session for {room_id}")

    def _invalidate_outbound_sessions(self, device: OlmDevice) -> None:
        assert self.olm

        for room in self.rooms.values():
            if device.user_id in room.users:
                self.invalidate_outbound_session(room.room_id)

    @store_loaded
    def verify_device(self, device: OlmDevice) -> bool:
        """Mark a device as verified.

        A device needs to be either trusted/ignored or blacklisted to either
        share room encryption keys with it or not.
        This method adds the device to the trusted devices and enables sharing
        room encryption keys with it.

        Args:
            device (OlmDevice): The device which should be added to the trust
                list.

        Returns true if the device was verified, false if it was already
        verified.
        """
        assert self.olm

        changed = self.olm.verify_device(device)
        if changed:
            self._invalidate_outbound_sessions(device)

        return changed

    @store_loaded
    def unverify_device(self, device: OlmDevice) -> bool:
        """Unmark a device as verified.

        This method removes the device from the trusted devices and disables
        sharing room encryption keys with it. It also invalidates any
        encryption keys for rooms that the device takes part of.

        Args:
            device (OlmDevice): The device which should be added to the trust
                list.

        Returns true if the device was unverified, false if it was already
        unverified.
        """
        assert self.olm

        changed = self.olm.unverify_device(device)
        if changed:
            self._invalidate_outbound_sessions(device)

        return changed

    @store_loaded
    def blacklist_device(self, device: OlmDevice) -> bool:
        """Mark a device as blacklisted.

        Devices on the blacklist will not receive room encryption keys and
        therefore won't be able to decrypt messages coming from this client.

        Args:
            device (OlmDevice): The device which should be added to the
                blacklist.

        Returns true if the device was added, false if it was on the blacklist
        already.
        """
        assert self.olm

        changed = self.olm.blacklist_device(device)
        if changed:
            self._invalidate_outbound_sessions(device)

        return changed

    @store_loaded
    def unblacklist_device(self, device: OlmDevice) -> bool:
        """Unmark a device as blacklisted.

        Args:
            device (OlmDevice): The device which should be removed from the
                blacklist.

        Returns true if the device was removed, false if it wasn't on the
        blacklist and no removal happened.
        """
        assert self.olm

        changed = self.olm.unblacklist_device(device)
        if changed:
            self._invalidate_outbound_sessions(device)

        return changed

    @store_loaded
    def ignore_device(self, device: OlmDevice) -> bool:
        """Mark a device as ignored.

        Ignored devices will still receive room encryption keys, despire not
        being verified.

        Args:
            device (OlmDevice): the device to ignore

        Returns true if device is ignored, or false if it is already on the
        list of ignored devices.
        """
        assert self.olm

        changed = self.olm.ignore_device(device)
        if changed:
            self._invalidate_outbound_sessions(device)

        return changed

    @store_loaded
    def unignore_device(self, device: OlmDevice) -> bool:
        """Unmark a device as ignored.

        Args:
            device (OlmDevice): The device which should be removed from the
                list of ignored devices.

        Returns true if the device was removed, false if it wasn't on the
        list and no removal happened.
        """
        assert self.olm

        changed = self.olm.unignore_device(device)
        if changed:
            self._invalidate_outbound_sessions(device)

        return changed

    def _handle_register(self, response: RegisterResponse | ErrorResponse) -> None:
        if isinstance(response, ErrorResponse):
            return

        self.restore_login(response.user_id, response.device_id, response.access_token)

    def _handle_login(self, response: LoginResponse | ErrorResponse):
        if isinstance(response, ErrorResponse):
            return

        self.restore_login(response.user_id, response.device_id, response.access_token)

    def _handle_logout(self, response: LogoutResponse | ErrorResponse):
        if not isinstance(response, ErrorResponse):
            self.access_token = ""

    @store_loaded
    def decrypt_event(self, event: MegolmEvent) -> Event | BadEventType:
        """Try to decrypt an undecrypted megolm event.

        Args:
            event (MegolmEvent): Event that should be decrypted.

        Returns the decrypted event, raises EncryptionError if there was an
        error while decrypting.
        """
        if not isinstance(event, MegolmEvent):
            raise ValueError(
                "Invalid event, this function can only decrypt " "MegolmEvents"
            )

        assert self.olm
        return self.olm.decrypt_megolm_event(event)

    def _prepare_ingestion_frame(
        self,
        frame: SyncFrame,
        staged_revision: int,
        prior_continuities: tuple[RoomContinuity, ...],
    ) -> _PreparedIngestionFrame:
        """Apply one owned durable Frame without invoking callbacks."""
        self._assert_ingestion_not_poisoned()
        store = self.store
        olm = self.olm
        if (
            self._ingestion_store_snapshot is None
            or type(store) is not SqliteStore
            or olm is None
            or type(olm) is not Olm
            or olm.store is not store
        ):
            raise LocalProtocolError(
                "ingestion preparation requires the exact attached store and Olm"
            )
        if type(frame) is not SyncFrame:
            raise TypeError("frame must be SyncFrame")
        if type(staged_revision) is not int or staged_revision < 1:
            raise ValueError("staged_revision must be a positive integer")
        if type(prior_continuities) is not tuple or any(
            type(value) is not RoomContinuity for value in prior_continuities
        ):
            raise TypeError("prior_continuities must contain RoomContinuity values")
        prior_by_room = {value.room_id: value for value in prior_continuities}
        if len(prior_by_room) != len(prior_continuities):
            raise ValueError("prior_continuities must have unique room IDs")

        _canonical_ingestion_object(frame.request_cursor_json, "request cursor")
        candidate_cursor = _canonical_ingestion_object(
            frame.candidate_cursor_json, "candidate cursor"
        )
        if frame.origin.transport is TransportKind.CLASSIC:
            if set(candidate_cursor) != {"next_batch"}:
                raise ValueError("Classic candidate cursor is invalid")
            compatibility_token = candidate_cursor["next_batch"]
            if type(compatibility_token) is not str or not compatibility_token:
                raise ValueError("Classic candidate token must be nonempty")
        else:
            compatibility_token = None

        device_list_delta = _canonical_ingestion_object(
            frame.device_list_delta_json, "device-list delta"
        )
        if set(device_list_delta) != {"changed", "left"} or not all(
            _nonempty_string_list(value) for value in device_list_delta.values()
        ):
            raise ValueError("device-list delta is invalid")
        key_counts = _canonical_ingestion_object(
            frame.one_time_key_counts_json, "one-time-key counts"
        )
        if any(
            type(algorithm) is not str
            or not algorithm
            or type(count) is not int
            or count < 0
            for algorithm, count in key_counts.items()
        ):
            raise ValueError("one-time-key counts are invalid")
        fallback = _load_ingestion_json(
            frame.unused_fallback_key_types_json,
            "unused fallback key types",
        )
        if _canonical_ingestion_json(
            fallback
        ) != frame.unused_fallback_key_types_json or (
            fallback is not None and not _nonempty_string_list(fallback)
        ):
            raise ValueError("unused fallback key types are invalid")

        parsed_to_device = _parse_ingestion_events(
            frame.to_device_json,
            "to-device event",
            ToDeviceEvent.parse_event,
        )
        parsed_segments = tuple(
            _parse_ingestion_room_segment(segment) for segment in frame.room_segments
        )

        ephemeral_by_room: dict[str, list[_ParsedIngestionEvent]] = defaultdict(list)
        for room_id, payload in _normalized_ephemeral_envelopes(frame.ephemeral_json):
            value = _require_ingestion_event_type(
                _canonical_ingestion_object(payload, "ephemeral event"),
                "ephemeral event",
            )
            ephemeral_by_room[room_id].append(
                (payload, value, EphemeralEvent.parse_event(deepcopy(value)))
            )
        parsed_presence = _parse_ingestion_events(
            frame.presence_json,
            "presence event",
            lambda value: _require_parsed_ingestion_event(
                PresenceEvent.from_dict(value), PresenceEvent, "presence event"
            ),
        )
        parsed_global_account_data = _parse_ingestion_events(
            frame.global_account_data_json,
            "global account-data event",
            AccountDataEvent.parse_event,
        )

        if compatibility_token is not None:
            self.next_batch = compatibility_token

        records: list[_PreparedIngestionRecord] = []
        transitions: list[_PreparedMembershipTransition] = []
        snapshots: list[RoomSnapshot] = []
        encrypted_room_ids: set[str] = set()
        record_phase_indexes: dict[_PreparationPhase, int] = defaultdict(int)

        def pending_record_id(phase: _PreparationPhase) -> str:
            return str(
                uuid5(
                    frame.frame_id,
                    f"record:{phase.value}:{record_phase_indexes[phase]}",
                )
            )

        def append_record(
            kind: RecordKind,
            payload: bytes,
            raw: dict[Any, Any],
            room_id: str | None = None,
            provenance: TimelineEventProvenance | None = None,
            callback_route: _CallbackRoute | None = None,
            clear_json: bytes | None = None,
            decryption: _DecryptionDisposition = _DecryptionDisposition.NONE,
            decryption_verified: bool | None = None,
            decrypted_to_device_kind: _DecryptedToDeviceKind | None = None,
            phase: _PreparationPhase = _PreparationPhase.SOURCE,
            explicit_event_type: str | None = None,
        ) -> _PreparedIngestionRecord:
            if (
                decryption_verified is not None
                and type(decryption_verified) is not bool
            ):
                raise TypeError("decryption_verified must be bool or None")
            if (decrypted_to_device_kind is not None) != (
                kind is RecordKind.TO_DEVICE
                and decryption is _DecryptionDisposition.DECRYPTED
            ):
                raise ValueError("decrypted to-device kind disagrees with disposition")
            index = len(records)
            record_id = pending_record_id(phase)
            record_phase_indexes[phase] += 1
            effective_raw = (
                _canonical_ingestion_object(clear_json, "clear event")
                if clear_json is not None
                else raw
            )
            visible_event_type = effective_raw.get("type")
            effective_event_type = explicit_event_type or visible_event_type
            if type(effective_event_type) is not str or not effective_event_type:
                raise ValueError("effective event type is unavailable")
            if (
                visible_event_type is not None
                and visible_event_type != effective_event_type
            ):
                raise ValueError("effective event type disagrees with event JSON")
            record = _PreparedIngestionRecord(
                record_id,
                kind,
                replace(frame.origin, frame_index=index),
                phase,
                effective_event_type,
                room_id,
                _matrix_event_id(raw),
                provenance,
                payload,
                clear_json,
                decryption,
                decryption_verified,
                decrypted_to_device_kind,
                callback_route,
            )
            records.append(record)
            return record

        def append_synthetic_to_device_records(
            events: object,
            phase: _PreparationPhase,
        ) -> None:
            if type(events) is not list:
                raise TypeError(f"{phase} outputs must be a list")
            for event in events:
                if not isinstance(event, ToDeviceEvent):
                    raise TypeError(f"{phase} outputs must be to-device events")
                if type(event.source) is not dict:
                    raise TypeError(f"{phase} event source must be a dict")
                payload = _canonical_ingestion_json(event.source)
                raw = _canonical_ingestion_object(payload, f"{phase.value} event")
                source_event_type = raw.get("type")
                if source_event_type is None and isinstance(
                    event,
                    KeyVerificationCancel,
                ):
                    source_event_type = "m.key.verification.cancel"
                append_record(
                    RecordKind.TO_DEVICE,
                    payload,
                    raw,
                    callback_route=_CallbackRoute.TO_DEVICE,
                    phase=phase,
                    explicit_event_type=source_event_type,
                )

        for payload, raw, event in parsed_to_device:
            decrypted_event = (
                self._handle_decrypt_to_device(event)
                if isinstance(event, ToDeviceEvent)
                else None
            )
            effective_event = decrypted_event if decrypted_event is not None else event
            if decrypted_event is not None:
                decrypted_source = getattr(decrypted_event, "source", None)
                if type(decrypted_source) is not dict:
                    raise TypeError("decrypted to-device event source must be a dict")
                clear_json = _canonical_ingestion_json(decrypted_source)
            else:
                clear_json = None
            callback_route = (
                None
                if effective_event is None
                or isinstance(
                    effective_event, (RoomKeyRequest, RoomKeyRequestCancellation)
                )
                else _CallbackRoute.TO_DEVICE
            )
            disposition = (
                _DecryptionDisposition.DECRYPTED
                if clear_json is not None
                else _DecryptionDisposition.NONE
            )
            if decrypted_event is None:
                decrypted_to_device_kind = None
            else:
                try:
                    decrypted_to_device_kind = _DECRYPTED_TO_DEVICE_KINDS[
                        type(decrypted_event)
                    ]
                except KeyError as error:
                    raise TypeError(
                        "unsupported decrypted to-device event type"
                    ) from error
            append_record(
                RecordKind.TO_DEVICE,
                payload,
                raw,
                callback_route=callback_route,
                clear_json=clear_json,
                decryption=disposition,
                decrypted_to_device_kind=decrypted_to_device_kind,
            )

        continuities = {
            room_id: (continuity.membership_epoch, continuity.membership)
            for room_id, continuity in prior_by_room.items()
        }
        for segment_value, state, timeline, account_data in parsed_segments:
            segment = segment_value
            segment_frame_index = len(records)
            continuities.setdefault(segment.room_id, (0, None))
            has_parseable_own_member = False

            room: MatrixRoom | None
            if segment.section in {RoomSection.INVITE, RoomSection.KNOCK}:
                room = self._get_invited_room(segment.room_id)
            elif segment.section is RoomSection.JOIN:
                if segment.room_id in self.invited_rooms:
                    del self.invited_rooms[segment.room_id]
                room = self.rooms.setdefault(
                    segment.room_id,
                    MatrixRoom(
                        segment.room_id,
                        self.user_id,
                        segment.room_id in self.encrypted_rooms,
                    ),
                )
            else:
                room = self.rooms.get(segment.room_id) or self.invited_rooms.get(
                    segment.room_id
                )
            callback_room = (
                segment.section in {RoomSection.JOIN, RoomSection.UNCHANGED}
                and segment.room_id in self.rooms
            )

            def append_transition(
                current: str,
                source_kind: _MembershipSourceKind,
                frame_index: int,
                event_id: str | None = None,
                source_record_id: str | None = None,
                timeline_provenance: TimelineEventProvenance | None = None,
                source_json: bytes | None = None,
            ) -> None:
                previous_epoch, previous = continuities[segment.room_id]
                if current == previous:
                    return
                current_epoch = previous_epoch + int(
                    previous == "join" and current != "join"
                )
                transitions.append(
                    _PreparedMembershipTransition(
                        str(
                            uuid5(
                                frame.frame_id, f"record:transition:{len(transitions)}"
                            )
                        ),
                        source_record_id,
                        segment.room_id,
                        event_id,
                        previous,
                        current,
                        previous_epoch,
                        current_epoch,
                        source_kind,
                        timeline_provenance,
                        _MembershipProvenance.REPORTED,
                        replace(frame.origin, frame_index=frame_index),
                        source_json,
                    )
                )
                continuities[segment.room_id] = (current_epoch, current)

            def capture_transition(
                payload: bytes,
                raw: dict[Any, Any],
                next_record_id: str,
                source_kind: _MembershipSourceKind,
                timeline_provenance: TimelineEventProvenance | None,
            ) -> None:
                nonlocal has_parseable_own_member
                if (
                    raw.get("type") != "m.room.member"
                    or raw.get("state_key") != self.user_id
                    or type(raw.get("content")) is not dict
                    or type(raw["content"].get("membership")) is not str
                    or not raw["content"]["membership"]
                ):
                    return
                has_parseable_own_member = True
                append_transition(
                    raw["content"]["membership"],
                    source_kind,
                    len(records),
                    _matrix_event_id(raw),
                    next_record_id,
                    timeline_provenance,
                    payload,
                )

            for payload, raw, event in state:
                capture_transition(
                    payload,
                    raw,
                    pending_record_id(_PreparationPhase.SOURCE),
                    _MembershipSourceKind.STATE,
                    None,
                )
                if room is not None and isinstance(event, InviteEvent):
                    cast(MatrixInvitedRoom, room).handle_event(cast(Event, event))
                elif room is not None and isinstance(event, RoomMemberEvent):
                    if room.handle_membership(event):
                        self._invalidate_session_for_member_event(segment.room_id)
                elif room is not None and isinstance(event, RoomEncryptionEvent):
                    encrypted_room_ids.add(segment.room_id)
                    room.handle_event(event)
                elif room is not None and isinstance(event, Event):
                    room.handle_event(event)
                append_record(
                    RecordKind.STATE,
                    payload,
                    raw,
                    segment.room_id,
                    callback_route=(
                        _CallbackRoute.EVENT
                        if segment.section is RoomSection.INVITE and event
                        else None
                    ),
                )

            history_count = len(timeline) - segment.live_event_count
            for timeline_index, (payload, raw, event) in enumerate(timeline):
                timeline_provenance = (
                    TimelineEventProvenance.HISTORY
                    if timeline_index < history_count
                    else TimelineEventProvenance.LIVE
                )
                decrypted_room_event = None
                if room is not None and isinstance(event, (Event, BadEventType)):
                    decrypted_room_event = self._handle_timeline_event(
                        event,
                        segment.room_id,
                        room,
                        encrypted_room_ids,
                    )
                clear_json = (
                    _canonical_ingestion_json(decrypted_room_event.source)
                    if isinstance(decrypted_room_event, (Event, BadEventType))
                    else None
                )
                effective_raw = (
                    _canonical_ingestion_object(
                        clear_json,
                        "decrypted timeline event",
                    )
                    if clear_json is not None
                    else raw
                )
                capture_transition(
                    clear_json if clear_json is not None else payload,
                    effective_raw,
                    pending_record_id(_PreparationPhase.SOURCE),
                    _MembershipSourceKind.TIMELINE,
                    timeline_provenance,
                )
                append_record(
                    RecordKind.TIMELINE,
                    payload,
                    raw,
                    segment.room_id,
                    timeline_provenance,
                    _CallbackRoute.EVENT if callback_room else None,
                    clear_json,
                    (
                        _DecryptionDisposition.DECRYPTED
                        if clear_json is not None
                        else (
                            _DecryptionDisposition.MEGOLM_FAILED
                            if isinstance(event, MegolmEvent)
                            else _DecryptionDisposition.NONE
                        )
                    ),
                    (
                        getattr(decrypted_room_event, "verified", None)
                        if isinstance(decrypted_room_event, (Event, BadEventType))
                        else None
                    ),
                )

            for payload, raw, event in ephemeral_by_room.pop(segment.room_id, []):
                if room is not None and isinstance(event, EphemeralEvent):
                    room.handle_ephemeral_event(event)
                append_record(
                    RecordKind.EPHEMERAL,
                    payload,
                    raw,
                    segment.room_id,
                    callback_route=(
                        _CallbackRoute.EPHEMERAL if callback_room and event else None
                    ),
                )
            for payload, raw, event in account_data:
                if room is not None and isinstance(
                    event, (AccountDataEvent, BadEventType)
                ):
                    room.handle_account_data(event)
                append_record(
                    RecordKind.ROOM_ACCOUNT_DATA,
                    payload,
                    raw,
                    segment.room_id,
                    callback_route=(
                        _CallbackRoute.ROOM_ACCOUNT_DATA if callback_room else None
                    ),
                )
            section_membership = segment.membership_observation.room_membership
            if not has_parseable_own_member and section_membership is not None:
                append_transition(
                    section_membership,
                    _MembershipSourceKind.SECTION,
                    segment_frame_index,
                )
            if room is not None and room.encrypted:
                olm.update_tracked_users(room)
            if room is not None:
                snapshots.append(_room_snapshot(room, *continuities[segment.room_id]))

        for room_id, events in ephemeral_by_room.items():
            room = self.rooms.get(room_id) or self.invited_rooms.get(room_id)
            for payload, raw, event in events:
                if room is not None and isinstance(event, EphemeralEvent):
                    room.handle_ephemeral_event(event)
                append_record(
                    RecordKind.EPHEMERAL,
                    payload,
                    raw,
                    room_id,
                    callback_route=(
                        _CallbackRoute.EPHEMERAL
                        if room_id in self.rooms and event
                        else None
                    ),
                )

        self.encrypted_rooms.update(encrypted_room_ids)
        for payload, raw, event in parsed_presence:
            for room in self.rooms.values():
                user = room.users.get(event.user_id)
                if user is None:
                    continue
                user.presence = event.presence
                user.last_active_ago = event.last_active_ago
                user.currently_active = event.currently_active
                user.status_msg = event.status_msg
            append_record(
                RecordKind.PRESENCE,
                payload,
                raw,
                callback_route=_CallbackRoute.PRESENCE,
            )
        for payload, raw, _event in parsed_global_account_data:
            append_record(
                RecordKind.GLOBAL_ACCOUNT_DATA,
                payload,
                raw,
                callback_route=_CallbackRoute.GLOBAL_ACCOUNT_DATA,
            )

        append_synthetic_to_device_records(
            olm.clear_verifications(),
            _PreparationPhase.EXPIRED_VERIFICATION,
        )

        signed_curve_count = key_counts.get("signed_curve25519")
        if signed_curve_count is not None:
            olm.uploaded_key_count = signed_curve_count
        changed_user_ids = {
            user_id
            for user_id in (*device_list_delta["changed"], *device_list_delta["left"])
            if any(
                room.encrypted and user_id in room.users for room in self.rooms.values()
            )
        }
        olm.add_changed_users(changed_user_ids)
        append_synthetic_to_device_records(
            olm.collect_key_requests(),
            _PreparationPhase.COLLECTED_KEY_REQUEST,
        )

        frozen_encrypted_room_ids = tuple(sorted(encrypted_room_ids))
        if frozen_encrypted_room_ids != _encrypted_room_ids_from_parsed_segments(
            parsed_segments,
            set(self.rooms) | set(self.invited_rooms),
        ):
            raise ValueError("frame-local encrypted-room effect is inconsistent")
        return _PreparedIngestionFrame(
            frame.frame_id,
            frame.origin.transport,
            frame.origin.source_epoch,
            frame.origin.request_id,
            staged_revision,
            frame.request_cursor_json,
            frame.candidate_cursor_json,
            frame.source_sha256,
            compatibility_token,
            tuple(records),
            tuple(transitions),
            tuple(snapshots),
            _prepared_crypto_delta_snapshot(
                frame,
                olm,
                frozen_encrypted_room_ids,
            ),
        )

    def _handle_decrypt_to_device(
        self, to_device_event: ToDeviceEvent
    ) -> ToDeviceEvent | BadEventType | None:
        if self.olm:
            return self.olm.handle_to_device_event(to_device_event)

        return None

    def _replace_decrypted_to_device(
        self,
        decrypted_events: list[tuple[int, ToDeviceEvent]],
        response: SyncResponse | SlidingSyncResponse,
    ):
        # Replace the encrypted to_device events with decrypted ones
        for decrypted_event in decrypted_events:
            index, event = decrypted_event
            response.to_device_events[index] = event

    def _handle_to_device(self, response: SyncResponse | SlidingSyncResponse):
        decrypted_to_device = []

        for index, to_device_event in enumerate(response.to_device_events):
            decrypted_event = self._handle_decrypt_to_device(to_device_event)

            if decrypted_event:
                decrypted_to_device.append(
                    (index, cast(ToDeviceEvent, decrypted_event))
                )
                to_device_event = cast(ToDeviceEvent, decrypted_event)

            # Do not pass room key request events to our user here. We don't
            # want to notify them about requests that get automatically handled
            # or canceled right away.
            if isinstance(
                to_device_event, (RoomKeyRequest, RoomKeyRequestCancellation)
            ):
                continue

            self._on_to_device(to_device_event)

        self._replace_decrypted_to_device(decrypted_to_device, response)

    def _get_invited_room(self, room_id: str) -> MatrixInvitedRoom:
        if room_id not in self.invited_rooms:
            logger.info(f"New invited room {room_id}")
            self.invited_rooms[room_id] = MatrixInvitedRoom(room_id, self.user_id)

        return self.invited_rooms[room_id]

    def _handle_invited_rooms(self, response: SyncResponse):
        for room_id, info in response.rooms.invite.items():
            room = self._get_invited_room(room_id)

            for event in info.invite_state:
                room.handle_event(event)
                self._on_invited_rooms(event, room)

    def _handle_joined_state(
        self, room_id: str, join_info: RoomInfo, encrypted_rooms: set[str]
    ):
        if room_id in self.invited_rooms:
            del self.invited_rooms[room_id]

        if room_id not in self.rooms:
            logger.info(f"New joined room {room_id}")
            self.rooms[room_id] = MatrixRoom(
                room_id, self.user_id, room_id in self.encrypted_rooms
            )

        room = self.rooms[room_id]

        for event in join_info.state:
            if isinstance(event, RoomEncryptionEvent):
                encrypted_rooms.add(room_id)

            if isinstance(event, RoomMemberEvent):
                if room.handle_membership(event):
                    self._invalidate_session_for_member_event(room_id)
            else:
                room.handle_event(event)

        if join_info.summary:
            room.update_summary(join_info.summary)

        if join_info.unread_notifications:
            room.update_unread_notifications(join_info.unread_notifications)

    def _handle_timeline_event(
        self,
        event: Event | BadEventType,
        room_id: str,
        room: MatrixRoom,
        encrypted_rooms: set[str],
    ) -> Event | BadEventType | None:
        decrypted_event = None

        if isinstance(event, MegolmEvent) and self.olm:
            event.room_id = room_id
            decrypted_event = self.olm._decrypt_megolm_no_error(event)

            if decrypted_event:
                event = decrypted_event

        elif isinstance(event, RoomEncryptionEvent):
            encrypted_rooms.add(room_id)

        if isinstance(event, RoomMemberEvent):
            if room.handle_membership(event):
                self._invalidate_session_for_member_event(room_id)

        elif isinstance(event, (UnknownBadEvent, BadEvent)):
            pass

        else:
            room.handle_event(event)

        return decrypted_event

    def _handle_joined_rooms(self, response: SyncResponse):
        encrypted_rooms: set[str] = set()

        for room_id, join_info in response.rooms.join.items():
            self._handle_joined_state(room_id, join_info, encrypted_rooms)

            room = self.rooms[room_id]
            decrypted_events: list[tuple[int, Event | BadEventType]] = []

            for index, event in enumerate(join_info.timeline.events):
                decrypted_event = self._handle_timeline_event(
                    event, room_id, room, encrypted_rooms
                )

                if decrypted_event:
                    event = decrypted_event
                    decrypted_events.append((index, decrypted_event))

                self._on_event(event, room)

            # Replace the Megolm events with decrypted ones
            for index, event in decrypted_events:
                join_info.timeline.events[index] = event

            for event in join_info.ephemeral:
                room.handle_ephemeral_event(event)
                self._on_ephemeral(event, room)

            for event in join_info.account_data:
                room.handle_account_data(event)
                self._on_room_account_data(event, room)

            if room.encrypted and self.olm is not None:
                self.olm.update_tracked_users(room)

        self.encrypted_rooms.update(encrypted_rooms)

        if self.store:
            self.store.save_encrypted_rooms(encrypted_rooms)

    def _handle_presence_events(self, response: SyncResponse):
        for event in response.presence_events:
            for room_id in self.rooms.keys():
                if event.user_id not in self.rooms[room_id].users:
                    continue

                self.rooms[room_id].users[event.user_id].presence = event.presence
                self.rooms[room_id].users[
                    event.user_id
                ].last_active_ago = event.last_active_ago
                self.rooms[room_id].users[
                    event.user_id
                ].currently_active = event.currently_active
                self.rooms[room_id].users[event.user_id].status_msg = event.status_msg

            self._on_presence(event)

    def _handle_global_account_data_events(
        self,
        response: SyncResponse,
    ) -> None:
        for event in response.account_data_events:
            self._on_global_account_data(event)

    def _handle_expired_verifications(self):
        expired_verifications = self.olm.clear_verifications()

        for event in expired_verifications:
            self._on_expired_verifications(event)

    def _handle_olm_events(self, response: SyncResponse | SlidingSyncResponse) -> None:
        assert self.olm

        changed_users = set()
        if response.device_key_count.signed_curve25519 is not None:
            self.olm.uploaded_key_count = response.device_key_count.signed_curve25519

        for user in response.device_list.changed:
            for room in self.rooms.values():
                if not room.encrypted:
                    continue

                if user in room.users:
                    changed_users.add(user)

        for user in response.device_list.left:
            for room in self.rooms.values():
                if not room.encrypted:
                    continue

                if user in room.users:
                    changed_users.add(user)

        self.olm.add_changed_users(changed_users)

    def _on_to_device(self, event: ToDeviceEvent):
        for cb in self.to_device_callbacks:
            cb.sync_execute(event)

    def _on_invited_rooms(self, event: Event, room: MatrixRoom):
        for cb in self.event_callbacks:
            cb.sync_execute(event, room)

    def _on_event(self, event: Event, room: MatrixRoom):
        for cb in self.event_callbacks:
            cb.sync_execute(event, room)

    def _on_ephemeral(self, event: EphemeralEvent, room: MatrixRoom):
        for cb in self.ephemeral_callbacks:
            cb.sync_execute(event, room)

    def _on_room_account_data(
        self, event: AccountDataEvent | BadEventType, room: MatrixRoom
    ):
        for cb in self.room_account_data_callbacks:
            cb.sync_execute(event, room)

    def _on_presence(self, event: PresenceEvent):
        for cb in self.presence_callbacks:
            cb.sync_execute(event)

    def _on_global_account_data(self, event: AccountDataEvent):
        for cb in self.global_account_data_callbacks:
            cb.sync_execute(event)

    def _on_expired_verifications(self, event: ToDeviceEvent):
        for cb in self.to_device_callbacks:
            cb.sync_execute(event)

    def _handle_sync(self, response: SyncResponse) -> None | Coroutine[Any, Any, None]:
        # We already received such a sync response, do nothing in that case.
        if self.next_batch == response.next_batch:
            return None

        self.next_batch = response.next_batch

        if self.config.store_sync_tokens and self.store:
            self.store.save_sync_token(self.next_batch)

        self._handle_to_device(response)

        self._handle_invited_rooms(response)

        self._handle_joined_rooms(response)

        self._handle_presence_events(response)

        self._handle_global_account_data_events(response)

        if self.olm:
            self._handle_expired_verifications()
            self._handle_olm_events(response)
            self._collect_key_requests()

        return None

    def _collect_key_requests(self):
        events = self.olm.collect_key_requests()
        for event in events:
            self._on_to_device(event)

    def _decrypt_event_array(self, array: list[Event | BadEventType]):
        if not self.olm:
            return

        decrypted_events = []

        for index, event in enumerate(array):
            if isinstance(event, MegolmEvent):
                new_event = self.olm._decrypt_megolm_no_error(event)
                if new_event:
                    decrypted_events.append((index, new_event))

        for decrypted_event in decrypted_events:
            index, event = decrypted_event
            array[index] = event

    def _handle_context_response(self, response: RoomContextResponse):
        if isinstance(response.event, MegolmEvent):
            if self.olm:
                decrypted_event = self.olm._decrypt_megolm_no_error(response.event)
                response.event = decrypted_event

        self._decrypt_event_array(response.events_after)
        self._decrypt_event_array(response.events_before)

    def _handle_messages_response(self, response: RoomMessagesResponse):
        decrypted_events = []

        for index, event in enumerate(response.chunk):
            if isinstance(event, MegolmEvent) and self.olm:
                new_event = self.olm._decrypt_megolm_no_error(event)
                if new_event:
                    decrypted_events.append((index, new_event))

        for index, event in decrypted_events:
            response.chunk[index] = event

    def _handle_olm_response(
        self,
        response: (
            ShareGroupSessionResponse
            | KeysClaimResponse
            | KeysQueryResponse
            | KeysUploadResponse
            | RoomKeyRequestResponse
            | ToDeviceResponse
        ),
    ):
        if not self.olm:
            return

        self.olm.handle_response(response)

        if isinstance(response, ShareGroupSessionResponse):
            room_id = response.room_id
            session = self.olm.outbound_group_sessions.get(room_id, None)
            room = self.rooms.get(room_id, None)

            if not session or not room:
                return

            session.users_shared_with.update(response.users_shared_with)
            users = room.users

            for user_id in users:
                for device in self.device_store.active_user_devices(user_id):
                    user = (user_id, device.id)
                    if (
                        user not in session.users_shared_with
                        and user not in session.users_ignored
                    ):
                        return

            logger.info(f"Marking outbound group session for room {room_id} as shared")
            session.shared = True

        elif isinstance(response, KeysQueryResponse):
            for user_id in response.changed:
                for room in self.rooms.values():
                    if room.encrypted and user_id in room.users:
                        self.invalidate_outbound_session(room.room_id)

    def _handle_joined_members(self, response: JoinedMembersResponse):
        if response.room_id not in self.rooms:
            return

        room = self.rooms[response.room_id]

        joined_user_ids = {m.user_id for m in response.members}

        for user_id in tuple(room.users):
            invited = room.users[user_id].invited

            if not invited and user_id not in joined_user_ids:
                room.remove_member(user_id)

        for member in response.members:
            room.add_member(member.user_id, member.display_name, member.avatar_url)

        room.members_synced = True

        if room.encrypted and self.olm is not None:
            self.olm.update_tracked_users(room)

    def _handle_room_forget_response(self, response: RoomForgetResponse):
        self.encrypted_rooms.discard(response.room_id)

        if response.room_id in self.rooms:
            room = self.rooms.pop(response.room_id)

            if room.encrypted and self.store:
                self.store.delete_encrypted_room(room.room_id)

        elif response.room_id in self.invited_rooms:
            del self.invited_rooms[response.room_id]

    def _handle_presence_response(self, response: PresenceGetResponse):
        for room_id in self.rooms.keys():
            if response.user_id not in self.rooms[room_id].users:
                continue

            self.rooms[room_id].users[response.user_id].presence = response.presence
            self.rooms[room_id].users[
                response.user_id
            ].last_active_ago = response.last_active_ago
            self.rooms[room_id].users[response.user_id].currently_active = (
                response.currently_active or False
            )
            self.rooms[room_id].users[response.user_id].status_msg = response.status_msg

    def _handle_whoami_response(self, response: WhoamiResponse):
        self.user_id = response.user_id
        self.device_id = response.device_id or self.device_id
        # self.is_guest = response.is_guest

    def receive_response(self, response: Response) -> None | Coroutine[Any, Any, None]:
        """Receive a Matrix Response and change the client state accordingly.

        Some responses will get edited for the callers convenience e.g. sync
        responses that contain encrypted messages. The encrypted messages will
        be replaced by decrypted ones if decryption is possible.

        Args:
            response (Response): the response that we wish the client to handle
        """
        if not isinstance(response, Response):
            raise ValueError("Invalid response received")

        if isinstance(response, LoginResponse):
            self._handle_login(response)
        elif isinstance(response, LogoutResponse):
            self._handle_logout(response)
        elif isinstance(response, RegisterResponse):
            self._handle_register(response)
        elif isinstance(response, SyncResponse):
            self._handle_sync(response)
        elif isinstance(response, RoomMessagesResponse):
            self._handle_messages_response(response)
        elif isinstance(response, RoomContextResponse):
            self._handle_context_response(response)
        elif isinstance(response, KeysUploadResponse):
            self._handle_olm_response(response)
        elif isinstance(response, KeysQueryResponse):
            self._handle_olm_response(response)
        elif isinstance(response, KeysClaimResponse):
            self._handle_olm_response(response)
        elif isinstance(response, ShareGroupSessionResponse):
            self._handle_olm_response(response)
        elif isinstance(response, JoinedMembersResponse):
            self._handle_joined_members(response)
        elif isinstance(response, RoomKeyRequestResponse):
            self._handle_olm_response(response)
        elif isinstance(response, RoomForgetResponse):
            self._handle_room_forget_response(response)
        elif isinstance(response, ToDeviceResponse):
            self._handle_olm_response(response)
        elif isinstance(response, RoomGetEventResponse):
            if isinstance(response.event, MegolmEvent) and self.olm is not None:
                try:
                    response.event = self.decrypt_event(response.event)
                except EncryptionError:
                    pass
        elif isinstance(response, PresenceGetResponse):
            self._handle_presence_response(response)
        elif isinstance(response, WhoamiResponse):
            self._handle_whoami_response(response)
        elif isinstance(response, ErrorResponse):
            if response.soft_logout:
                self.access_token = ""

        return None

    @store_loaded
    def export_keys(self, outfile: str, passphrase: str, count: int = 10000):
        """Export all the Megolm decryption keys of this device.

        The keys will be encrypted using the passphrase.

        Note that this does not save other information such as the private
        identity keys of the device.

        Args:
            outfile (str): The file to write the keys to.
            passphrase (str): The encryption passphrase.
            count (int): Optional. Round count for the underlying key
                derivation. It is not recommended to specify it unless
                absolutely sure of the consequences.
        """
        assert self.olm
        self.olm.export_keys(outfile, passphrase, count=count)

    @store_loaded
    def import_keys(self, infile: str, passphrase: str):
        """Import Megolm decryption keys.

        The keys will be added to the current instance as well as written to
        database.

        Args:
            infile (str): The file containing the keys.
            passphrase (str): The decryption passphrase.

        Raises `EncryptionError` if the file is invalid or couldn't be
            decrypted.

        Raises the usual file errors if the file couldn't be opened.
        """
        assert self.olm
        self.olm.import_keys(infile, passphrase)

    @store_loaded
    def get_missing_sessions(self, room_id: str) -> dict[str, list[str]]:
        """Get users and devices for which we don't have active Olm sessions.

        Args:
            room_id (str): The room id of the room for which we should get the
                users with missing Olm sessions.

        Raises `LocalProtocolError` if the room with the provided room id is
            not found or the room is not encrypted.
        """
        assert self.olm

        if room_id not in self.rooms:
            raise LocalProtocolError(f"No room found with room id {room_id}")
        room = self.rooms[room_id]

        if not room.encrypted:
            raise LocalProtocolError(f"Room with id {room_id} is not encrypted")

        return self.olm.get_missing_sessions(list(room.users))

    @store_loaded
    def get_users_for_key_claiming(self) -> dict[str, list[str]]:
        """Get the content for a key claim request that needs to be made.

        Returns a dictionary containing users as the keys and a list of devices
        for which we will claim one-time keys.

        Raises a LocalProtocolError if no key claim request needs to be made.
        """
        assert self.olm
        return self.olm.get_users_for_key_claiming()

    @store_loaded
    def encrypt(
        self, room_id: str, message_type: str, content: dict[Any, Any]
    ) -> tuple[str, dict[str, str]]:
        """Encrypt a message to be sent to the provided room.

        Args:
            room_id (str): The room id of the room where the message will be
                sent.
            message_type (str): The type of the message.
            content (str): The dictionary containing the content of the
                message.

        Raises `GroupEncryptionError` if the group session for the provided
        room isn't shared yet.

        Raises `MembersSyncError` if the room is encrypted but the room members
        aren't fully loaded due to member lazy loading.

        Returns a tuple containing the new message type and the new encrypted
        content.
        """
        assert self.olm

        try:
            room = self.rooms[room_id]
        except KeyError:
            raise LocalProtocolError(f"No such room with id {room_id} found.")

        if not room.encrypted:
            raise LocalProtocolError(f"Room {room_id} is not encrypted")

        if not room.members_synced:
            raise MembersSyncError(
                "The room is encrypted and the members " "aren't fully synced."
            )

        encrypted_content = self.olm.group_encrypt(
            room_id,
            {"content": content, "type": message_type},
        )

        # The relationship needs to be sent unencrypted, so put it in the
        # unencrypted content.
        if "m.relates_to" in content:
            encrypted_content["m.relates_to"] = content["m.relates_to"]

        message_type = "m.room.encrypted"

        return message_type, encrypted_content

    def add_event_callback(
        self,
        callback: Callable[[MatrixRoom, Event], Awaitable[None] | None],
        filter: type[Event] | tuple[type[Event], None],
    ) -> None:
        """Add a callback that will be executed on room events.

        The callback can be used on joined rooms as well as on invited rooms.
        The room parameter for the callback will have a different type
        depending on if the room is joined or invited.

        Args:
            callback (Callable[[MatrixRoom, Event], Optional[Awaitable[None]]]): A
                function that will be called if the event type in the filter
                argument is found in a room timeline.

            filter (Union[Type[Event], Tuple[Type[Event], ...]]):
                The event type or a tuple
                containing multiple types for which the function will be
                called.

        """
        cb = ClientCallback(callback, filter)
        self.event_callbacks.append(cb)

    def add_ephemeral_callback(
        self,
        callback: Callable[[MatrixRoom, EphemeralEvent], None],
        filter: type[EphemeralEvent] | tuple[type[EphemeralEvent], ...],
    ) -> None:
        """Add a callback that will be executed on ephemeral room events.

        Args:
            callback (Callable[MatrixRoom, EphemeralEvent]):
                A function that will be
                called if the event type in the filter argument is found in the
                ephemeral room event list.

            filter
            (Union[Type[EphemeralEvent], Tuple[Type[EphemeralEvent], ...]]):
                The event type or a tuple containing
                multiple types for which the function will be called.

        """
        cb = ClientCallback(callback, filter)
        self.ephemeral_callbacks.append(cb)

    def add_global_account_data_callback(
        self,
        callback: Callable[[AccountDataEvent], None],
        filter: type[AccountDataEvent] | tuple[type[AccountDataEvent], ...],
    ) -> None:
        """Add a callback that will be executed on global account data events.

        Args:
            callback (Callable[[AccountDataEvent], None]):
                A function that will be
                called if the event type in the filter argument is found in
                the account data event list.

            filter
            (Union[Type[AccountDataEvent], Tuple[Type[AccountDataEvent, ...]]):
                The event type or a tuple
                containing multiple types for which the function
                will be called.

        """
        cb = ClientCallback(callback, filter)
        self.global_account_data_callbacks.append(cb)

    def add_room_account_data_callback(
        self,
        callback: Callable[[MatrixRoom, AccountDataEvent], None],
        filter: type[AccountDataEvent] | tuple[type[AccountDataEvent], ...],
    ) -> None:
        """Add a callback that will be executed on room account data events.

        Args:
            callback (Callable[[MatrixRoom, AccountDataEvent], None]):
                A function that will be
                called if the event type in the filter argument is found in
                the room account data event list.

            filter
            (Union[Type[AccountDataEvent], Tuple[Type[AccountDataEvent, ...]]):
                The event type or a tuple
                containing multiple types for which the function
                will be called.

        """
        cb = ClientCallback(callback, filter)
        self.room_account_data_callbacks.append(cb)

    def add_to_device_callback(
        self,
        callback: Callable[[ToDeviceEvent], None],
        filter: type[ToDeviceEvent] | tuple[type[ToDeviceEvent], ...],
    ) -> None:
        """Add a callback that will be executed on to-device events.

        Args:
            callback (Callable[[ToDeviceEvent], None]): A function that will be
                called if the event type in the filter argument is found in
                the to-device part of the sync response.

            filter
            (Union[Type[ToDeviceEvent], Tuple[Type[ToDeviceEvent], ...]]):
                The event type or a tuple
                containing multiple types for which the function
                will be called.

        """
        cb = ClientCallback(callback, filter)
        self.to_device_callbacks.append(cb)

    def add_presence_callback(
        self,
        callback: Callable[[PresenceEvent], None],
        filter: type | tuple[type],
    ):
        """Add a callback that will be executed on presence events.

        Args:
            callback (Callable[[PresenceEvent], None]): A function that will be
                called if the event type in the filter argument is found in
                the presence part of the sync response.
            filter (Union[Type, Tuple[Type]]): The event type or a tuple
                containing multiple types for which the function
                will be called.

        """
        cb = ClientCallback(callback, filter)
        self.presence_callbacks.append(cb)

    @store_loaded
    def create_key_verification(self, device: OlmDevice) -> ToDeviceMessage:
        """Start a new key verification process with the given device.

        Args:
            device (OlmDevice): The device which we would like to verify

        Returns a ``ToDeviceMessage`` that should be sent to to the homeserver.
        """
        assert self.olm
        return self.olm.create_sas(device)

    @store_loaded
    def confirm_key_verification(self, transaction_id: str) -> ToDeviceMessage:
        """Confirm that the short auth string of a key verification matches.

        Args:
            transaction_id (str): The transaction id of the interactive key
                verification.

        Returns a ``ToDeviceMessage`` that should be sent to the homeserver.

        If the other user already confirmed the short auth string on their side
        this function will also verify the device that is partaking in the
        verification process.
        """
        if transaction_id not in self.key_verifications:
            raise LocalProtocolError(
                f"Key verification with the transaction id {transaction_id} does not exist."
            )

        sas = self.key_verifications[transaction_id]

        sas.accept_sas()
        message = sas.get_mac()

        if sas.verified:
            self.verify_device(sas.other_olm_device)

        return message

    def room_devices(self, room_id: str) -> dict[str, dict[str, OlmDevice]]:
        """Get all Olm devices participating in a room.

        Args:
            room_id (str): The id of the room for which we would like to
                collect all the devices.

        Returns a dictionary holding the user as the key and a dictionary of
        the device id as the key and OlmDevice as the value.

        Raises LocalProtocolError if no room is found with the given room_id.
        """
        devices: dict[str, dict[str, OlmDevice]] = defaultdict(dict)

        if not self.olm:
            return devices

        try:
            room = self.rooms[room_id]
        except KeyError:
            raise LocalProtocolError(f"No room found with room id {room_id}")

        if not room.encrypted:
            return devices

        users = room.users.keys()

        for user in users:
            user_devices = self.device_store.active_user_devices(user)
            devices[user] = {d.id: d for d in user_devices}

        return devices

    @store_loaded
    def get_active_key_requests(
        self, user_id: str, device_id: str
    ) -> list[RoomKeyRequest]:
        """Get key requests from a device that are waiting for verification.

        Args:
            user_id (str): The id of the user for which we would like to find
                the active key requests.
            device_id (str): The id of the device for which we would like to
                find the active key requests.

        Example:
            >>> # A to-device callback that verifies devices that
            >>> # request room keys and continues the room key sharing process.
            >>> # Note that a single user/device can have multiple key requests
            >>> # queued up.
            >>>   def key_share_cb(event):
            ...       user_id = event.sender
            ...       device_id = event.requesting_device_id
            ...       device = client.device_store[user_id][device_id]
            ...       client.verify_device(device)
            ...       for request in client.get_active_key_requests(
            ...           user_id, device_id):
            ...           client.continue_key_share(request)
            >>>   client.add_to_device_callback(key_share_cb)

        Returns:
            list: A list of actively waiting key requests from the given user.

        """
        assert self.olm
        return self.olm.get_active_key_requests(user_id, device_id)

    @store_loaded
    def continue_key_share(self, event: RoomKeyRequest) -> bool:
        """Continue a previously interrupted key share event.

        To handle room key requests properly client users need to add a
        callback for RoomKeyRequest:

            >>> client.add_to_device_callback(callback, RoomKeyRequest)

        This callback will be run only if a room key request needs user
        interaction, that is if a room key request is coming from an untrusted
        device.

        After a user has verified the requesting device the key sharing can be
        continued using this method:

            >>> client.continue_key_share(room_key_request)

        Args:
            event (RoomKeyRequest): The event which we would like to continue.

        If the key share event is continued successfully a to-device message
        will be queued up in the `client.outgoing_to_device_messages` list
        waiting to be sent out

        Returns:
            bool: True if the request was continued, False otherwise.

        """
        assert self.olm
        return self.olm.continue_key_share(event)

    @store_loaded
    def cancel_key_share(self, event: RoomKeyRequest) -> bool:
        """Cancel a previously interrupted key share event.

        This method is the counterpart to the `continue_key_share()` method. If
        a user choses not to verify a device and does not want to share room
        keys with such a device it should cancel the request with this method.

            >>> client.cancel_key_share(room_key_request)

        Args:
            event (RoomKeyRequest): The event which we would like to cancel.

        Returns:
            bool: True if the request was cancelled, False otherwise.

        """
        assert self.olm
        return self.olm.cancel_key_share(event)
