"""Freeze sync observations and replay them through ordinary event parsers."""

from __future__ import annotations

from copy import deepcopy
from typing import TYPE_CHECKING, Any

from ..events import (
    AccountDataEvent,
    DummyEvent,
    Event,
    ForwardedRoomKeyEvent,
    InviteEvent,
    InviteMemberEvent,
    KeyVerificationCancel,
    RoomKeyEvent,
    ToDeviceEvent,
)
from ..responses import SlidingSyncStateStub
from .model import CryptoEvidence, RecordKind, SyncRecord

if TYPE_CHECKING:
    from ..client.base_client import _SyncItem


def freeze_event(item: _SyncItem) -> SyncRecord:
    """Detach mutable input at its yield, before later state or trust changes."""
    if item.route in ("presence", "ephemeral"):
        raise ValueError("transient observations are not durable records")
    event = item.event
    room_id = item.room.room_id if item.room else None
    if event is None:
        return SyncRecord(
            RecordKind.ROOM_LIFECYCLE, room_id, {"membership": item.section}
        )

    event_source = getattr(event, "source", {})
    payload = deepcopy(event_source)
    codec = None
    if isinstance(event, SlidingSyncStateStub):
        codec = "state_stub"
        payload = {"type": event.type, "state_key": event.state_key}
    elif isinstance(event, InviteEvent):
        codec = "invite"
        if isinstance(event, InviteMemberEvent):
            payload["content"] = deepcopy(event.content)
    elif isinstance(event, ForwardedRoomKeyEvent):
        codec = "forwarded_room_key"
    elif isinstance(event, RoomKeyEvent):
        codec = "room_key"
    elif isinstance(event, DummyEvent):
        codec = "dummy"
    elif isinstance(event, KeyVerificationCancel) and not payload:
        # Local timeouts have no server event to parse on restart.
        payload = {
            "type": "m.key.verification.cancel",
            "sender": event.sender,
            "content": {
                "transaction_id": event.transaction_id,
                "code": event.code,
                "reason": event.reason,
            },
        }

    decrypted = isinstance(event, Event) and event.decrypted
    encrypted_envelope = (
        item.source is not None
        and item.source.get("type") == "m.room.encrypted"
        and event_source.get("type") != "m.room.encrypted"
    )
    crypto = None
    if decrypted or isinstance(event, (RoomKeyEvent, DummyEvent)) or encrypted_envelope:
        envelope_content = item.source.get("content", {}) if item.source else {}
        crypto = CryptoEvidence(
            getattr(event, "verified", None),
            getattr(event, "sender_key", None)
            or envelope_content.get("sender_key", ""),
            getattr(event, "session_id", None),
        )
    # Plaintext observations normally share the parsed event source. Detach
    # that tree only once, without comparing potentially large event bodies.
    source = (
        payload
        if item.source is None or item.source is event_source
        else deepcopy(item.source)
    )
    clear = (
        payload
        if decrypted
        or codec in ("room_key", "forwarded_room_key", "dummy")
        or encrypted_envelope
        else None
    )
    if item.route in ("to_device", "expired_verification"):
        kind = RecordKind.TO_DEVICE
    elif item.route == "room_account_data":
        kind = RecordKind.ROOM_ACCOUNT_DATA
    elif item.route == "global_account_data":
        kind = RecordKind.GLOBAL_ACCOUNT_DATA
    elif item.route == "event":
        kind = RecordKind.TIMELINE
    else:
        kind = RecordKind.STATE
    return SyncRecord(
        kind,
        room_id,
        source,
        clear=clear,
        crypto=crypto,
        provenance=getattr(event, "timeline_provenance", None),
        route=item.route,
        codec=codec,
    )


def restore_event(record: SyncRecord) -> object:
    """Reconstruct a committed event without consulting mutable crypto state."""
    payload: dict[str, Any] = deepcopy(
        record.clear if record.clear is not None else record.source
    )
    if record.codec in ("room_key", "forwarded_room_key", "dummy"):
        if record.crypto is None:
            raise ValueError("sanitized to-device record requires crypto evidence")
        if record.codec == "dummy":
            return DummyEvent(
                payload,
                payload["sender"],
                record.crypto.sender_key,
                payload["sender_device"],
            )
        cls = (
            ForwardedRoomKeyEvent
            if record.codec == "forwarded_room_key"
            else RoomKeyEvent
        )
        content = payload["content"]
        return cls(
            payload,
            payload["sender"],
            record.crypto.sender_key,
            content["room_id"],
            content["session_id"],
            content["algorithm"],
        )
    if record.codec == "invite":
        return InviteEvent.parse_event(payload)
    if record.codec == "state_stub":
        return SlidingSyncStateStub(payload["type"], payload["state_key"])
    if record.codec is not None:
        raise ValueError(f"unsupported stored event codec: {record.codec}")
    if record.kind == RecordKind.TO_DEVICE:
        return ToDeviceEvent.parse_event(payload)
    if record.kind in (RecordKind.ROOM_ACCOUNT_DATA, RecordKind.GLOBAL_ACCOUNT_DATA):
        return AccountDataEvent.parse_event(payload)
    if record.kind not in (RecordKind.TIMELINE, RecordKind.STATE):
        return payload
    event = (
        Event.parse_decrypted_event(payload)
        if record.clear is not None
        else Event.parse_event(payload)
    )
    if record.crypto is not None and isinstance(event, Event):
        event.decrypted = True
        event.verified = record.crypto.verified  # type: ignore[assignment]
        event.sender_key = record.crypto.sender_key
        event.session_id = record.crypto.session_id
    return event
