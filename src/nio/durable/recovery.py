"""Bounded chronological pagination around the shared nio event iterators."""

from __future__ import annotations

import json
from typing import TYPE_CHECKING, Any

from ..api import Api, MessageDirection
from ..event_provenance import TimelineEventProvenance
from ..events import BadEvent, RoomMemberEvent, UnknownBadEvent
from ..exceptions import LocalProtocolError
from ..responses import RoomInfo, RoomMessagesResponse, SyncResponse, Timeline
from .model import OwnMembership, RecordKind, SyncRecord, encode_json
from .processor import Processor
from .projection import restore_room
from .transport import ResponseTooLarge

if TYPE_CHECKING:
    from .client import DurableSync

PAGE_BYTES = 2 * 1024 * 1024
CONTROL_BYTES = 2 * 1024 * 1024


class Recovery:
    def __init__(self, session: DurableSync):
        self.session = session
        self.response: SyncResponse | None = None
        self.resets: set[str] = set()

    def needs_full_state(self) -> bool:
        session = self.session
        return bool(session.cursor) and (
            not session._metadata
            or any(
                value.get("membership") == "join" and not value.get("baseline")
                for value in session._metadata.values()
            )
        )

    def _rooms(self) -> list[tuple[str, RoomInfo, str]]:
        assert self.response is not None
        return [
            (room_id, info, section)
            for section, rooms in (
                ("join", self.response.rooms.join),
                ("leave", self.response.rooms.leave),
            )
            for room_id, info in rooms.items()
        ]

    def _candidates(self) -> list[tuple[str, RoomInfo, str]]:
        return [
            (room_id, info, section)
            for room_id, info, section in self._rooms()
            if info.timeline.limited
        ]

    def _poison(self) -> None:
        self.session.client._dispose()
        self.session._store.close()
        self.session._changed.set()

    def _loss(
        self,
        room_id: str,
        state: dict[str, Any],
        reason: str,
        membership: OwnMembership | None = None,
    ) -> None:
        session = self.session
        session._metadata.setdefault(room_id, {})["baseline"] = False
        if room_id not in state["history_rooms"]:
            state["history_rooms"].append(room_id)
        session._publish_records(
            (
                SyncRecord(
                    RecordKind.LOSS,
                    room_id,
                    {"reason": reason, "pages": state.get("pages", 0)},
                    membership=membership,
                    membership_epoch=session._metadata[room_id].get(
                        "membership_epoch", 0
                    ),
                ),
            )
        )

    def prepare(self, response: SyncResponse | None = None) -> None:
        session = self.session
        pending = session._store.input
        if pending is None or pending[1].get("phase") == "prepared":
            return
        if response is not None:
            self.response = response
        if self.response is None:
            self.response = session._decode_response(pending[0])[0]
        state = pending[1]
        if state.get("phase") == "recover":
            return
        try:
            with session._store.transaction():
                if "phase" not in state:
                    state.update(
                        phase="tail",
                        old_cursor=session.cursor,
                        room_index=0,
                        history_rooms=[
                            room_id
                            for room_id, _, _ in self._rooms()
                            if not session._metadata.get(room_id, {}).get("baseline")
                        ],
                        explicit_rooms=[],
                        applied_ids={},
                    )
                    state["unknown_rooms"] = list(state["history_rooms"])
                    if session.cursor and self._candidates():
                        state["phase"] = "recover"
                        self._next_room(state, 0)
                        processor = Processor(session, state["history_rooms"], set())
                        processor.consume(session.client._iter_to_device(self.response))
                        for room_id in state["unknown_rooms"]:
                            self._loss(room_id, state, "unknown authorization baseline")
                        processor.save()
                        session._crypto.capture()
                        session._store.save_continuation(state)
                        session._changed.set()
                        return
                    if session.cursor:
                        for room_id in state["unknown_rooms"]:
                            self._loss(room_id, state, "unknown authorization baseline")
                self._tail(state)
        except BaseException:
            self._poison()
            raise
        session._changed.set()

    def _next_room(self, state: dict[str, Any], index: int) -> None:
        state.update(
            room_index=index,
            **{
                "from": state["old_cursor"],
                "pages": 0,
                "seen_tokens": [state["old_cursor"]],
                "limit": min(self.session.config.recovery_page_size, 100),
            },
        )
        if index >= len(self._candidates()):
            state["phase"] = "tail"

    def _tail(self, state: dict[str, Any]) -> None:
        session = self.session
        assert self.response is not None
        response = self.response
        # A committed recovery prologue must never decrypt its Olm input twice.
        if "from" in state:
            response.to_device_events.clear()
        explicit = set(state["explicit_rooms"])
        for room_id, info, _ in self._rooms():
            applied = set(state["applied_ids"].get(room_id, []))
            # State is authoritative at the tail boundary, even when a room
            # reset discarded an earlier application of the same event.
            info.timeline.events = [
                event
                for event in info.timeline.events
                if event.source.get("event_id") not in applied
            ]
            for event in info.state:
                room = session.client.rooms.get(room_id)
                if room is not None:
                    session._timeline_room(room_id, room, event)
            if any(
                isinstance(event, RoomMemberEvent)
                and event.state_key == session.client.user_id
                for event in (*info.state, *info.timeline.events)
            ):
                explicit.add(room_id)
            if room_id not in session.client.rooms:
                session._store.database.execute_sql(
                    "DELETE FROM NioDurableMember WHERE room_id=?", (room_id,)
                )
        self.resets.clear()
        processor = Processor(
            session,
            state["history_rooms"],
            explicit,
            suppress_ids={
                room_id: set(ids) for room_id, ids in state["applied_ids"].items()
            },
        )
        processor.consume(session.client._iter_sync(response, include_left=True))
        complete_state = state.get("full_state") or (
            state["old_cursor"] is None and session.config.sync_filter is None
        )
        for room_id, _, section in self._rooms():
            metadata = session._metadata.setdefault(room_id, {})
            if section == "join" and complete_state and room_id not in self.resets:
                metadata["baseline"] = metadata.get("membership") == "join"
            if room_id in session.client.rooms:
                processor.rooms[room_id] = session.client.rooms[room_id]
        processor.save()
        # Pre-echo joined history must not restore a locally revoked room.
        for room_id in processor.rooms:
            if session._metadata[room_id].get("membership") in ("leave", "ban"):
                session.client.rooms.pop(room_id, None)
                session.client.invited_rooms.pop(room_id, None)
        session._crypto.capture()
        session._store.set_cursor(response.next_batch)
        session._store.save_continuation({"phase": "prepared"})
        session.client.next_batch = response.next_batch

    async def advance(self) -> None:
        self.prepare()
        session = self.session
        pending = session._store.input
        assert pending is not None
        if session._store.has_batches() or pending[1].get("phase") != "recover":
            return
        state = pending[1]
        room_id, info, section = self._candidates()[state["room_index"]]
        target = info.timeline.prev_batch
        error = None
        page = None
        if room_id in state["unknown_rooms"]:
            # Already reported at capture; current state cannot fill this interval.
            self._commit_page(state, room_id, section, [], None, done=True)
            return
        if not isinstance(target, str) or not target or len(target) > 8192:
            error = "missing or oversized history boundary"
        elif state["from"] == target:
            self._commit_page(state, room_id, section, [], None, done=True)
            return
        elif state["pages"] >= min(session.config.max_recovery_pages, 1000):
            error = "history page limit exhausted"
        else:
            # An exclusive `to` can stop before the retained tail. Page forward
            # until its event IDs overlap, keeping the same page/control bounds.
            method, path = Api.room_messages(
                "",
                room_id,
                start=state["from"],
                direction=MessageDirection.front,
                limit=state["limit"],
            )
            try:
                body = await session._transport.request(
                    method, path, max_bytes=PAGE_BYTES
                )
                raw = json.loads(body)
                if (
                    not isinstance(raw, dict)
                    or raw.get("start") != state["from"]
                    or not isinstance(raw.get("chunk"), list)
                ):
                    raise ValueError("invalid page envelope")
                if len(raw["chunk"]) > state["limit"]:
                    raise ResponseTooLarge
                if any(
                    not isinstance(event, dict)
                    or not isinstance(event.get("event_id"), str)
                    or not event["event_id"]
                    or len(event["event_id"]) > 8192
                    for event in raw["chunk"]
                ):
                    raise ValueError("invalid event identity")
                page = RoomMessagesResponse.from_dict(raw, room_id)
                if not isinstance(page, RoomMessagesResponse) or len(page.chunk) != len(
                    raw["chunk"]
                ):
                    raise ValueError("invalid history events")
                if any(
                    isinstance(event, (BadEvent, UnknownBadEvent))
                    for event in page.chunk
                ):
                    raise ValueError("malformed history event")
            except ResponseTooLarge:
                if state["limit"] > 1:
                    state["limit"] = max(1, state["limit"] // 2)
                    with session._store.transaction():
                        session._store.save_continuation(state)
                    return
                error = "oversized history event"
            except (LocalProtocolError, ValueError, TypeError, KeyError):
                error = "history unavailable or malformed"
        if error is not None:
            self._commit_page(state, room_id, section, [], None, done=True, loss=error)
            return
        assert isinstance(page, RoomMessagesResponse)
        tail_ids = {event.source.get("event_id") for event in info.timeline.events}
        previous_ids = state["applied_ids"].get(room_id, [])
        seen = set(previous_ids)
        events = []
        overlap = False
        for event in page.chunk:
            event_id = event.source["event_id"]
            if event_id in tail_ids:
                overlap = True
                break
            if event_id not in seen:
                events.append(event)
                seen.add(event_id)
        done = overlap or page.end == target
        if not done and (
            not isinstance(page.end, str)
            or not page.end
            or len(page.end) > 8192
            or page.end in state["seen_tokens"]
        ):
            error = "history boundary missing or token cycle"
        state["pages"] += 1
        state["applied_ids"][room_id] = list(seen)
        if page.end and not done and error is None:
            state["seen_tokens"].append(page.end)
        if len(encode_json(state).encode()) > CONTROL_BYTES:
            # Stop before applying this page; no unbounded identity ledger.
            state["applied_ids"][room_id] = previous_ids
            error = "history continuation bound exhausted"
            events = []
        self._commit_page(
            state,
            room_id,
            section,
            events,
            page.end,
            done=done or error is not None,
            loss=error,
        )

    def _commit_page(
        self,
        state: dict[str, Any],
        room_id: str,
        section: str,
        events: list[Any],
        end: str | None,
        *,
        done: bool,
        loss: str | None = None,
    ) -> None:
        session = self.session
        try:
            with session._store.transaction():
                processor = Processor(
                    session,
                    state["history_rooms"],
                    set(state["explicit_rooms"]),
                    TimelineEventProvenance.RECOVERED,
                    stop_on_oversized=True,
                )
                if events:
                    if room_id not in session.client.rooms:
                        members = {
                            user_id: json.loads(member)
                            for user_id, member in session._store.database.execute_sql(
                                "SELECT user_id,member FROM NioDurableMember WHERE room_id=?",
                                (room_id,),
                            )
                        }
                        session.client.rooms[room_id] = restore_room(
                            room_id, session._metadata[room_id], members
                        )
                    room = session.client.rooms[room_id]
                    encrypted: set[str] = set()
                    info = RoomInfo(Timeline(events, False, None), [], [], [])
                    processor.consume(
                        session.client._iter_room_timeline(
                            room_id, info, room, encrypted, section
                        )
                    )
                    if processor.oversized:
                        loss = "oversized recovered event"
                        done = True
                        # LOSS represents the already-applied oversized event;
                        # retain its identity and exclude only untouched suffix.
                        untouched = {
                            event.source["event_id"]
                            for event in events[processor.processed_records + 1 :]
                        }
                        state["applied_ids"][room_id] = [
                            event_id
                            for event_id in state["applied_ids"][room_id]
                            if event_id not in untouched
                        ]
                    room = session.client.rooms[room_id]
                    if room.encrypted and session.client.olm:
                        session.client.olm.update_tracked_users(room)
                    session.client.encrypted_rooms.update(encrypted)
                    session._store.matrix.save_encrypted_rooms(encrypted)
                if loss:
                    self._loss(room_id, state, loss, processor.oversized_membership)
                    if room_id in session.client.rooms:
                        processor.rooms[room_id] = session.client.rooms[room_id]
                state["explicit_rooms"] = list(processor.explicit_memberships)
                processor.save()
                session._crypto.capture()
                if done:
                    self._next_room(state, state["room_index"] + 1)
                else:
                    state["from"] = end
                session._store.save_continuation(state)
        except BaseException:
            self._poison()
            raise
        session._changed.set()
