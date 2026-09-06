"""One collector freezes shared nio observations inside the caller transaction."""

from __future__ import annotations

from collections.abc import Iterable
from dataclasses import replace
from typing import TYPE_CHECKING

from ..client.base_client import _SyncItem
from ..event_provenance import TimelineEventProvenance
from ..events import InviteMemberEvent, RoomMemberEvent
from ..rooms import MatrixRoom
from .codec import freeze_event
from .model import RecordKind, SyncRecord

if TYPE_CHECKING:
    from .client import DurableSync


class Processor:
    def __init__(
        self,
        session: DurableSync,
        history_rooms: list[str],
        explicit_memberships: set[str],
        provenance: TimelineEventProvenance = TimelineEventProvenance.LIVE,
    ):
        self.session = session
        self.history_rooms = history_rooms
        self.explicit_memberships = explicit_memberships
        self.provenance = provenance
        self.rooms: dict[str, MatrixRoom] = {}
        self.members: set[tuple[str, str]] = set()
        self.records: list[SyncRecord] = []

    def flush(self) -> None:
        if self.records:
            self.session._publish_records(tuple(self.records))
            self.records.clear()

    def consume(self, items: Iterable[_SyncItem]) -> None:
        session = self.session
        for item in items:
            if item.route in ("presence", "ephemeral"):
                continue
            room = item.room
            room_id = room.room_id if room else None
            change = None
            if room is not None and room_id is not None:
                self.rooms[room_id] = room
                if item.event is None:
                    if item.section == "leave":
                        if session._metadata.get(room_id, {}).get("membership") in (
                            "leave",
                            "ban",
                        ):
                            continue
                    elif room_id in self.explicit_memberships:
                        continue
                    assert item.section is not None
                    change = session._change_membership(room_id, item.section)
                    if change is None:
                        continue
                if isinstance(item.event, (RoomMemberEvent, InviteMemberEvent)):
                    self.members.add((room_id, item.event.state_key))
                    if item.event.state_key == session.client.user_id:
                        self.explicit_memberships.add(room_id)
                        change = session._change_membership(
                            room_id, item.event.membership
                        )
                if change is not None and change.previous == "join":
                    session._metadata[room_id]["baseline"] = False
                    if room_id not in self.history_rooms:
                        self.history_rooms.append(room_id)
                if change is not None or isinstance(
                    item.event, (RoomMemberEvent, InviteMemberEvent)
                ):
                    session._outbound.member_cache.pop(room_id, None)
            record = freeze_event(item)
            if room_id is not None:
                record = replace(
                    record,
                    membership=change,
                    membership_epoch=session._metadata.get(room_id, {}).get(
                        "membership_epoch", 0
                    ),
                    provenance=(
                        (
                            TimelineEventProvenance.HISTORY
                            if room_id in self.history_rooms
                            else self.provenance
                        )
                        if record.kind is RecordKind.TIMELINE
                        else record.provenance
                    ),
                )
            barrier = change is not None or isinstance(
                item.event, (RoomMemberEvent, InviteMemberEvent)
            )
            if barrier:
                self.flush()
            self.records.append(record)
            if barrier or len(self.records) >= session.config.max_batch_records:
                self.flush()
        self.flush()

    def save(self) -> None:
        # Rejoin can replace a room while the iterator is running.
        for room_id in self.rooms:
            if room_id in self.session.client.rooms:
                self.rooms[room_id] = self.session.client.rooms[room_id]
        self.session._save_rooms(self.rooms, self.members)
