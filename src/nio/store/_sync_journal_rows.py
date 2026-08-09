from __future__ import annotations

import hashlib
import hmac
import sqlite3
from dataclasses import replace
from uuid import UUID

from ..exceptions import LocalProtocolError
from ..ingest.errors import JournalIntegrityError
from ..ingest.model import (
    EventRecord,
    LossReason,
    LossRecord,
    RecordOrigin,
    RoomHydrationStatus,
    SyncBatch,
    SystemOrigin,
    TransportKind,
)
from ..ingest.serialization import (
    _batch_from_payload,
    _boundary_from_dict,
    _boundary_to_dict,
    _canonical_json,
    _decoded_bytes,
    _encoded_bytes,
    _load_json_object,
    _loss_id,
    _membership_baseline_from_dict,
    _membership_baseline_to_dict,
    _origin_from_dict,
    _origin_to_dict,
    _record_from_dict,
    _record_from_exact_dict,
    _record_to_dict,
    _recovery_gap_from_dict,
    _recovery_gap_to_dict,
    _room_snapshot_from_dict,
    _room_snapshot_to_dict,
    _system_event_record_id,
    _validate_batch,
    canonical_batch_payload,
)
from ..ingest.state import (
    LaneRecord,
    LaneRecordKey,
    LaneRecordSection,
    LaneStatus,
    OwnerView,
    ReadyRecord,
    ReleasePhase,
    RoomAggregate,
    RoomLane,
    RoomState,
    SourceState,
    StagedFrame,
)
from ._sync_journal_preflight import _validate_source_cursor

_FRAME_FIELDS = {"source_epoch", "request_id", "payload", "staged_revision"}
_READY_FIELDS = {"ready_order", "record", "source_frame_id", "created_revision"}
_ROOM_STATE_FIELDS = (
    "room_id",
    "current_membership_epoch",
    "next_room_sequence",
    "hydration_status",
    "snapshot",
    "membership_baseline",
    "updated_revision",
)
_ROOM_LANE_FIELDS = (
    "room_id",
    "membership_epoch",
    "lane_status",
    "held_record_count",
    "held_canonical_bytes",
    "release_phase",
    "ready_order",
    "next_held_ordinal",
    "successor_membership_epoch",
    "recovery_gap",
    "pending_lifecycle",
    "updated_revision",
)
_LANE_RECORD_FIELDS = (
    "room_id",
    "membership_epoch",
    "section",
    "page_ordinal",
    "record_ordinal",
    "item_id",
    "item_kind",
    "source_frame_id",
    "source_effect_id",
    "canonical_bytes",
    "record",
    "created_revision",
)


class JournalRows:
    def _open_payload(self, domain, primary_key, row, prefix):
        return self._codec.decrypt(
            domain,
            primary_key,
            bytes(row[f"{prefix}_ciphertext"]),
            bytes(row[f"{prefix}_sha256"]),
        )

    @staticmethod
    def _revalidate_insert(cursor, load, expected, message):
        if cursor.rowcount == 0 and load() != expected:
            raise JournalIntegrityError(message)

    @staticmethod
    def _validate_record_transport(
        record: EventRecord | LossRecord,
        transport_kind: TransportKind,
    ) -> None:
        origin = record.origin
        if type(origin) is RecordOrigin and origin.transport is not transport_kind:
            raise JournalIntegrityError(
                "record origin transport does not match immutable journal transport"
            )

    def _validate_room_lane_transport(
        self,
        lane: RoomLane,
        transport_kind: TransportKind,
    ) -> None:
        try:
            replace(lane)
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(str(error)) from error
        if (
            lane.recovery_gap is not None
            and lane.recovery_gap.origin.transport is not transport_kind
        ):
            raise JournalIntegrityError(
                "recovery gap transport does not match immutable journal transport"
            )
        if lane.pending_lifecycle is not None:
            self._validate_record_transport(
                lane.pending_lifecycle,
                transport_kind,
            )
            self._validated_record_id(lane.pending_lifecycle)

    @classmethod
    def _validate_lane_record_transport(
        cls,
        lane_record: LaneRecord,
        transport_kind: TransportKind,
    ) -> None:
        try:
            replace(lane_record)
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(str(error)) from error
        cls._validate_record_transport(lane_record.record, transport_kind)

    @staticmethod
    def _room_state_payload(state: RoomState, revision: int) -> bytes:
        return _canonical_json(
            {
                "room_id": state.room_id,
                "current_membership_epoch": state.current_membership_epoch,
                "next_room_sequence": state.next_room_sequence,
                "hydration_status": state.hydration_status.value,
                "snapshot": (
                    _room_snapshot_to_dict(state.snapshot)
                    if state.snapshot is not None
                    else None
                ),
                "membership_baseline": (
                    _membership_baseline_to_dict(state.membership_baseline)
                    if state.membership_baseline is not None
                    else None
                ),
                "updated_revision": revision,
            }
        )

    @staticmethod
    def _room_lane_payload(lane: RoomLane, revision: int) -> bytes:
        return _canonical_json(
            {
                "room_id": lane.room_id,
                "membership_epoch": lane.membership_epoch,
                "lane_status": lane.lane_status.value,
                "held_record_count": lane.held_record_count,
                "held_canonical_bytes": lane.held_canonical_bytes,
                "release_phase": lane.release_phase.value,
                "ready_order": lane.ready_order,
                "next_held_ordinal": lane.next_held_ordinal,
                "successor_membership_epoch": lane.successor_membership_epoch,
                "recovery_gap": (
                    _recovery_gap_to_dict(lane.recovery_gap)
                    if lane.recovery_gap is not None
                    else None
                ),
                "pending_lifecycle": (
                    _record_to_dict(lane.pending_lifecycle)
                    if lane.pending_lifecycle is not None
                    else None
                ),
                "updated_revision": revision,
            }
        )

    def _write_room_state(self, state: RoomState, revision: int) -> None:
        payload = self._room_state_payload(state, revision)
        ciphertext, digest = self._codec.seal(
            "NioIngestRoomState", (state.room_id,), payload
        )
        self._transition_execute(
            "room_state",
            """INSERT INTO NioIngestRoomState (
                account_id, room_id, current_membership_epoch,
                next_room_sequence, hydration_status, state_ciphertext,
                state_sha256, updated_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, room_id) DO UPDATE SET
                current_membership_epoch = excluded.current_membership_epoch,
                next_room_sequence = excluded.next_room_sequence,
                hydration_status = excluded.hydration_status, state_ciphertext = excluded.state_ciphertext,
                state_sha256 = excluded.state_sha256, updated_revision = excluded.updated_revision""",
            (
                self.account_id,
                state.room_id,
                state.current_membership_epoch,
                state.next_room_sequence,
                state.hydration_status.value,
                ciphertext,
                digest,
                revision,
            ),
        )

    def _write_room_lane(
        self,
        lane: RoomLane,
        revision: int,
        transport_kind: TransportKind,
    ) -> None:
        self._validate_room_lane_transport(lane, transport_kind)
        payload = self._room_lane_payload(lane, revision)
        ciphertext, digest = self._codec.seal(
            "NioIngestRoomLane",
            (lane.room_id, lane.membership_epoch),
            payload,
        )
        self._transition_execute(
            "room_lane",
            """INSERT INTO NioIngestRoomLane (
                account_id, room_id, membership_epoch, lane_status,
                held_record_count, held_canonical_bytes, release_phase,
                ready_order, next_held_ordinal, successor_membership_epoch,
                lane_state_ciphertext, lane_state_sha256,
                updated_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, room_id, membership_epoch) DO UPDATE SET
                lane_status = excluded.lane_status, held_record_count = excluded.held_record_count,
                held_canonical_bytes = excluded.held_canonical_bytes, release_phase = excluded.release_phase,
                ready_order = excluded.ready_order, next_held_ordinal = excluded.next_held_ordinal,
                successor_membership_epoch = excluded.successor_membership_epoch,
                lane_state_ciphertext = excluded.lane_state_ciphertext,
                lane_state_sha256 = excluded.lane_state_sha256,
                updated_revision = excluded.updated_revision""",
            (
                self.account_id,
                lane.room_id,
                lane.membership_epoch,
                lane.lane_status.value,
                lane.held_record_count,
                lane.held_canonical_bytes,
                lane.release_phase.value,
                lane.ready_order,
                lane.next_held_ordinal,
                lane.successor_membership_epoch,
                ciphertext,
                digest,
                revision,
            ),
        )

    def _write_loss(self, loss: LossRecord, revision: int) -> None:
        if loss.loss_id != _loss_id(self.stream_id, loss):
            raise JournalIntegrityError("loss_id does not match loss contents")
        origin_payload = _canonical_json(_origin_to_dict(loss.origin))
        boundary_payload = _canonical_json(_boundary_to_dict(loss.boundary))
        detail_payload = loss.detail_json
        primary_key = (loss.loss_id,)
        origin_ciphertext, origin_digest = self._codec.seal(
            "NioIngestLoss.origin", primary_key, origin_payload
        )
        boundary_ciphertext, boundary_digest = self._codec.seal(
            "NioIngestLoss.boundary", primary_key, boundary_payload
        )
        detail_ciphertext, detail_digest = self._codec.seal(
            "NioIngestLoss.detail", primary_key, detail_payload
        )
        loss_digest = hashlib.sha256(_canonical_json(_record_to_dict(loss))).digest()
        cursor = self._transition_execute(
            "loss",
            """INSERT INTO NioIngestLoss (
                account_id, loss_id, room_id, membership_epoch, reason,
                origin_ciphertext, origin_sha256, boundary_ciphertext,
                boundary_sha256, detail_ciphertext, detail_sha256,
                loss_sha256, detected_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, loss_id) DO NOTHING""",
            (
                self.account_id,
                loss.loss_id,
                loss.room_id,
                loss.membership_epoch,
                loss.reason.value,
                origin_ciphertext,
                origin_digest,
                boundary_ciphertext,
                boundary_digest,
                detail_ciphertext,
                detail_digest,
                loss_digest,
                revision,
            ),
        )
        self._revalidate_insert(
            cursor,
            lambda: self.load_loss(loss.loss_id),
            loss,
            "loss_id collides with different authenticated contents",
        )

    def _validated_record_id(self, record: EventRecord | LossRecord) -> str:
        if isinstance(record, EventRecord):
            if type(record.origin) is SystemOrigin:
                try:
                    expected_id = _system_event_record_id(self.stream_id, record)
                except (TypeError, ValueError) as error:
                    raise JournalIntegrityError(
                        "system EventRecord is invalid"
                    ) from error
                if record.record_id != expected_id:
                    raise JournalIntegrityError(
                        "record_id does not match system event contents"
                    )
            return record.record_id
        if record.loss_id != _loss_id(self.stream_id, record):
            raise JournalIntegrityError("loss_id does not match loss contents")
        return record.loss_id

    def _validate_ready_record(self, ready: ReadyRecord) -> str:
        try:
            replace(ready)
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(str(error)) from error
        return self._validated_record_id(ready.record)

    @staticmethod
    def _validate_batch_integrity(batch: SyncBatch) -> None:
        try:
            _validate_batch(batch)
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(str(error)) from error

    def _write_ready(self, ready: ReadyRecord, revision: int) -> None:
        record_id = self._validate_ready_record(ready)
        record_payload = _canonical_json(_record_to_dict(ready.record))
        payload = _canonical_json(
            {
                "ready_order": ready.ready_order,
                "record": _record_to_dict(ready.record),
                "source_frame_id": (
                    str(ready.source_frame_id) if ready.source_frame_id else None
                ),
                "created_revision": revision,
            }
        )
        ciphertext, digest = self._codec.seal(
            "NioIngestReadyRecord", (record_id,), payload
        )
        canonical_bytes = len(record_payload)
        if ready.canonical_bytes not in (0, canonical_bytes):
            raise JournalIntegrityError(
                "ready canonical_bytes does not match canonical payload"
            )
        cursor = self._transition_execute(
            "ready_record",
            """INSERT INTO NioIngestReadyRecord (
                account_id, ready_order, record_id, source_frame_id, room_id,
                membership_epoch, room_sequence, payload_ciphertext,
                payload_sha256, canonical_bytes, created_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, record_id) DO NOTHING""",
            (
                self.account_id,
                ready.ready_order,
                record_id,
                str(ready.source_frame_id) if ready.source_frame_id else None,
                ready.record.room_id,
                ready.record.membership_epoch,
                (
                    ready.record.room_sequence
                    if isinstance(ready.record, EventRecord)
                    else None
                ),
                ciphertext,
                digest,
                canonical_bytes,
                revision,
            ),
        )

        def load_ready():
            row = self.connection.execute(
                "SELECT * FROM NioIngestReadyRecord "
                "WHERE account_id = ? AND record_id = ?",
                (self.account_id, record_id),
            ).fetchone()
            return self._decode_ready_row(row) if row is not None else None

        self._revalidate_insert(
            cursor,
            load_ready,
            replace(ready, canonical_bytes=canonical_bytes, created_revision=revision),
            "ready record_id or ready_order collides with different contents",
        )

    @staticmethod
    def _lane_record_primary_key(
        key: LaneRecordKey,
    ) -> tuple[str | int, ...]:
        return (
            key.room_id,
            key.membership_epoch,
            key.section.value,
            key.page_ordinal,
            key.record_ordinal,
        )

    def _lane_record_payload(
        self,
        lane_record: LaneRecord,
        item_id: str,
        item_kind: str,
        revision: int,
    ) -> bytes:
        key = lane_record.key
        return _canonical_json(
            {
                "room_id": key.room_id,
                "membership_epoch": key.membership_epoch,
                "section": key.section.value,
                "page_ordinal": key.page_ordinal,
                "record_ordinal": key.record_ordinal,
                "item_id": item_id,
                "item_kind": item_kind,
                "source_frame_id": (
                    str(lane_record.source_frame_id)
                    if lane_record.source_frame_id is not None
                    else None
                ),
                "source_effect_id": (
                    str(lane_record.source_effect_id)
                    if lane_record.source_effect_id is not None
                    else None
                ),
                "canonical_bytes": lane_record.canonical_bytes,
                "record": _record_to_dict(lane_record.record),
                "created_revision": revision,
            }
        )

    def _write_lane_record(
        self,
        lane_record: LaneRecord,
        revision: int,
    ) -> None:
        item_id = self._validated_record_id(lane_record.record)
        item_kind = "event" if type(lane_record.record) is EventRecord else "loss"
        record_payload = _canonical_json(_record_to_dict(lane_record.record))
        if lane_record.canonical_bytes != len(record_payload):
            raise JournalIntegrityError(
                "lane record canonical_bytes does not match canonical payload"
            )
        payload = self._lane_record_payload(
            lane_record,
            item_id,
            item_kind,
            revision,
        )
        primary_key = self._lane_record_primary_key(lane_record.key)
        ciphertext, digest = self._codec.seal(
            "NioIngestLaneRecord",
            primary_key,
            payload,
        )
        cursor = self._transition_execute(
            "lane_record_insert",
            """INSERT INTO NioIngestLaneRecord (
                account_id, room_id, membership_epoch, section,
                page_ordinal, record_ordinal, item_id, item_kind,
                source_frame_id, source_effect_id, payload_ciphertext,
                payload_sha256, canonical_bytes, created_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT DO NOTHING""",
            (
                self.account_id,
                lane_record.key.room_id,
                lane_record.key.membership_epoch,
                lane_record.key.section.value,
                lane_record.key.page_ordinal,
                lane_record.key.record_ordinal,
                item_id,
                item_kind,
                (
                    str(lane_record.source_frame_id)
                    if lane_record.source_frame_id is not None
                    else None
                ),
                (
                    str(lane_record.source_effect_id)
                    if lane_record.source_effect_id is not None
                    else None
                ),
                ciphertext,
                digest,
                lane_record.canonical_bytes,
                revision,
            ),
        )
        if cursor.rowcount == 0:
            by_key = self.load_lane_record(lane_record.key)
            by_item = self._load_lane_record_by_item_id(item_id)
            if by_key != lane_record or by_item != lane_record:
                raise JournalIntegrityError(
                    "lane record key or item identity collides with different contents"
                )

    def _decode_lane_record_row(
        self,
        row: sqlite3.Row,
        transport_kind: TransportKind,
    ) -> LaneRecord:
        try:
            key = LaneRecordKey(
                row["room_id"],
                row["membership_epoch"],
                LaneRecordSection(row["section"]),
                row["page_ordinal"],
                row["record_ordinal"],
            )
            payload = self._open_payload(
                "NioIngestLaneRecord",
                self._lane_record_primary_key(key),
                row,
                "payload",
            )
            envelope = _load_json_object(payload)
            if tuple(envelope) != _LANE_RECORD_FIELDS:
                raise ValueError("lane record envelope fields are not canonical")
            source_frame_value = envelope["source_frame_id"]
            source_effect_value = envelope["source_effect_id"]
            source_frame_id = (
                UUID(source_frame_value) if source_frame_value is not None else None
            )
            source_effect_id = (
                UUID(source_effect_value) if source_effect_value is not None else None
            )
            record = _record_from_exact_dict(envelope["record"])
            lane_record = LaneRecord(
                LaneRecordKey(
                    envelope["room_id"],
                    envelope["membership_epoch"],
                    LaneRecordSection(envelope["section"]),
                    envelope["page_ordinal"],
                    envelope["record_ordinal"],
                ),
                record,
                source_frame_id,
                source_effect_id,
                envelope["canonical_bytes"],
            )
            created_revision = envelope["created_revision"]
            if type(created_revision) is not int or created_revision < 0:
                raise ValueError("lane record created_revision is invalid")
        except (AttributeError, TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "lane record authenticated envelope is invalid"
            ) from error

        item_id = self._validated_record_id(record)
        item_kind = "event" if type(record) is EventRecord else "loss"
        record_payload = _canonical_json(_record_to_dict(record))
        if lane_record.canonical_bytes != len(record_payload):
            raise JournalIntegrityError(
                "lane record canonical_bytes does not match canonical payload"
            )
        if payload != self._lane_record_payload(
            lane_record,
            item_id,
            item_kind,
            created_revision,
        ):
            raise JournalIntegrityError(
                "lane record authenticated envelope is not canonical"
            )
        authenticated = (
            lane_record.key.room_id,
            lane_record.key.membership_epoch,
            lane_record.key.section.value,
            lane_record.key.page_ordinal,
            lane_record.key.record_ordinal,
            item_id,
            item_kind,
            str(source_frame_id) if source_frame_id is not None else None,
            str(source_effect_id) if source_effect_id is not None else None,
            lane_record.canonical_bytes,
            created_revision,
        )
        columns = tuple(
            row[name]
            for name in (
                "room_id",
                "membership_epoch",
                "section",
                "page_ordinal",
                "record_ordinal",
                "item_id",
                "item_kind",
                "source_frame_id",
                "source_effect_id",
                "canonical_bytes",
                "created_revision",
            )
        )
        if authenticated != columns:
            raise JournalIntegrityError(
                "lane record columns do not match authenticated payload"
            )
        self._validate_lane_record_transport(lane_record, transport_kind)
        return lane_record

    def load_lane_record(self, key: LaneRecordKey) -> LaneRecord | None:
        owner = self._require_attached()
        if type(key) is not LaneRecordKey:
            raise TypeError("key must be LaneRecordKey")
        row = self.connection.execute(
            "SELECT * FROM NioIngestLaneRecord WHERE account_id = ? "
            "AND room_id = ? AND membership_epoch = ? AND section = ? "
            "AND page_ordinal = ? AND record_ordinal = ?",
            (self.account_id, *self._lane_record_primary_key(key)),
        ).fetchone()
        return (
            self._decode_lane_record_row(row, owner.transport_kind)
            if row is not None
            else None
        )

    def _load_lane_record_by_item_id(self, item_id: str) -> LaneRecord | None:
        owner = self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestLaneRecord " "WHERE account_id = ? AND item_id = ?",
            (self.account_id, item_id),
        ).fetchone()
        return (
            self._decode_lane_record_row(row, owner.transport_kind)
            if row is not None
            else None
        )

    def list_lane_records(
        self,
        room_id: str,
        membership_epoch: int,
        section: LaneRecordSection | None = None,
    ) -> tuple[LaneRecord, ...]:
        owner = self._require_attached()
        if type(room_id) is not str or not room_id:
            raise TypeError("room_id must be a nonempty str")
        if type(membership_epoch) is not int or membership_epoch < 0:
            raise TypeError("membership_epoch must be a nonnegative int")
        if section is not None and type(section) is not LaneRecordSection:
            raise TypeError("section must be LaneRecordSection or None")
        parameters: tuple[object, ...] = (
            self.account_id,
            room_id,
            membership_epoch,
        )
        section_clause = ""
        if section is not None:
            section_clause = " AND section = ?"
            parameters += (section.value,)
        rows = self.connection.execute(
            "SELECT * FROM NioIngestLaneRecord WHERE account_id = ? "
            "AND room_id = ? AND membership_epoch = ?"
            f"{section_clause} ORDER BY CASE section "
            "WHEN 'loss' THEN 0 WHEN 'recovered' THEN 1 WHEN 'held' THEN 2 END, "
            "page_ordinal, record_ordinal, item_id",
            parameters,
        ).fetchall()
        return tuple(
            self._decode_lane_record_row(row, owner.transport_kind) for row in rows
        )

    def _delete_lane_record(self, key: LaneRecordKey) -> None:
        cursor = self._transition_execute(
            "lane_record_delete",
            "DELETE FROM NioIngestLaneRecord WHERE account_id = ? "
            "AND room_id = ? AND membership_epoch = ? AND section = ? "
            "AND page_ordinal = ? AND record_ordinal = ?",
            (self.account_id, *self._lane_record_primary_key(key)),
        )
        if cursor.rowcount != 1:
            raise JournalIntegrityError("lane record delete target is missing")

    def _write_source(self, source: SourceState) -> None:
        ciphertext, digest = self._codec.seal(
            "NioIngestSourceState", (self.account_id,), source.cursor_json
        )
        self._transition_execute(
            "source_state",
            """INSERT INTO NioIngestSourceState (
                account_id, source_epoch, cursor_ciphertext, cursor_sha256,
                next_request_id, active
            ) VALUES (?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                source_epoch = excluded.source_epoch,
                cursor_ciphertext = excluded.cursor_ciphertext, cursor_sha256 = excluded.cursor_sha256,
                next_request_id = excluded.next_request_id, active = excluded.active""",
            (
                self.account_id,
                source.source_epoch,
                ciphertext,
                digest,
                source.next_request_id,
                int(source.active),
            ),
        )

    def load_source(self) -> SourceState:
        owner = self.load_owner()
        row = self.connection.execute(
            "SELECT * FROM NioIngestSourceState WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if row is None:
            raise JournalIntegrityError("ingestion source row is missing")
        cursor = self._open_payload(
            "NioIngestSourceState", (self.account_id,), row, "cursor"
        )
        try:
            _validate_source_cursor(owner.transport_kind, cursor)
        except LocalProtocolError as error:
            raise JournalIntegrityError(str(error)) from error
        return SourceState(
            row["source_epoch"],
            owner.transport_kind,
            cursor,
            row["next_request_id"],
            bool(row["active"]),
        )

    def _write_frame(self, frame: StagedFrame, revision: int) -> None:
        payload = _canonical_json(
            {
                "source_epoch": frame.source_epoch,
                "request_id": frame.request_id,
                "payload": _encoded_bytes(frame.payload),
                "staged_revision": revision,
            }
        )
        ciphertext, digest = self._codec.seal(
            "NioIngestFrame", (frame.frame_id,), payload
        )
        cursor = self._transition_execute(
            "frame",
            """INSERT INTO NioIngestFrame (
                account_id, frame_id, source_epoch, request_id,
                payload_ciphertext, payload_sha256, staged_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, frame_id) DO NOTHING""",
            (
                self.account_id,
                str(frame.frame_id),
                frame.source_epoch,
                frame.request_id,
                ciphertext,
                digest,
                revision,
            ),
        )
        self._revalidate_insert(
            cursor,
            lambda: self.load_frame(frame.frame_id),
            replace(frame, staged_revision=revision),
            "frame_id collides with different authenticated contents",
        )

    def load_frame(self, frame_id: UUID) -> StagedFrame | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (self.account_id, str(frame_id)),
        ).fetchone()
        if row is None:
            return None
        payload = self._open_payload("NioIngestFrame", (frame_id,), row, "payload")
        envelope = _load_json_object(payload)
        if set(envelope) != _FRAME_FIELDS:
            raise JournalIntegrityError("frame authenticated envelope is invalid")
        names = ("source_epoch", "request_id", "staged_revision")
        metadata = tuple(envelope[name] for name in names)
        if any(type(value) is not int for value in metadata):
            raise JournalIntegrityError("frame authenticated metadata is invalid")
        try:
            frame_payload = _decoded_bytes(envelope["payload"], "frame payload")
        except ValueError as error:
            raise JournalIntegrityError(
                "frame authenticated payload is invalid"
            ) from error
        assert frame_payload is not None
        if metadata != tuple(row[name] for name in names):
            raise JournalIntegrityError(
                "frame columns do not match authenticated metadata"
            )
        return StagedFrame(
            frame_id, metadata[0], metadata[1], frame_payload, metadata[2]
        )

    def _write_batch(
        self,
        batch: SyncBatch,
        revision: int,
        owner: OwnerView,
    ) -> None:
        self._validate_batch_integrity(batch)
        if (
            batch.account_id != self.account_id
            or batch.device_id != self.device_id
            or batch.consumer != owner.binding
            or batch.ref.stream_id != owner.stream_id
        ):
            raise JournalIntegrityError("batch owner identity does not match journal")
        if batch.created_revision != revision:
            raise JournalIntegrityError(
                "batch created_revision does not match commit revision"
            )
        payload = canonical_batch_payload(batch)
        digest = hashlib.sha256(payload).digest()
        if not hmac.compare_digest(digest, batch.ref.sha256):
            raise JournalIntegrityError("batch payload digest does not match reference")
        ciphertext = self._codec.encrypt(
            "NioIngestBatch",
            (batch.ref.sequence,),
            payload,
            digest,
        )
        self._transition_execute(
            "batch",
            """INSERT INTO NioIngestBatch (
                account_id, sequence, batch_id, payload_ciphertext,
                payload_sha256, created_revision, acknowledged_revision
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)""",
            (
                self.account_id,
                batch.ref.sequence,
                str(batch.ref.batch_id),
                ciphertext,
                digest,
                revision,
            ),
        )

    def _decode_batch(self, row: sqlite3.Row) -> SyncBatch:
        digest = bytes(row["payload_sha256"])
        payload = self._open_payload(
            "NioIngestBatch", (row["sequence"],), row, "payload"
        )
        try:
            batch = _batch_from_payload(payload)
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(str(error)) from error
        self._validate_batch_integrity(batch)
        if (
            batch.ref.sequence != row["sequence"]
            or str(batch.ref.batch_id) != row["batch_id"]
            or not hmac.compare_digest(batch.ref.sha256, digest)
        ):
            raise JournalIntegrityError("batch identity does not match stored row")
        if batch.created_revision != row["created_revision"]:
            raise JournalIntegrityError(
                "batch created_revision does not match stored row"
            )
        owner = self._require_attached()
        if (
            batch.account_id != owner.account_id
            or batch.device_id != owner.device_id
            or batch.consumer != owner.binding
            or batch.ref.stream_id != owner.stream_id
        ):
            raise JournalIntegrityError("batch owner does not match journal owner")
        return batch

    def _decode_ready_row(self, row: sqlite3.Row) -> ReadyRecord:
        payload = self._open_payload(
            "NioIngestReadyRecord", (row["record_id"],), row, "payload"
        )
        envelope = _load_json_object(payload)
        if set(envelope) != _READY_FIELDS:
            raise JournalIntegrityError("ready authenticated envelope is invalid")
        ready_order = envelope["ready_order"]
        created_revision = envelope["created_revision"]
        source_frame_value = envelope["source_frame_id"]
        if type(ready_order) is not int or type(created_revision) is not int:
            raise JournalIntegrityError("ready authenticated metadata is invalid")
        try:
            source_frame_id = (
                UUID(source_frame_value) if source_frame_value is not None else None
            )
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError("ready source_frame_id is invalid") from error
        try:
            record = _record_from_dict(envelope["record"])
            record_payload = _canonical_json(_record_to_dict(record))
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "ready authenticated record is invalid"
            ) from error
        if row["canonical_bytes"] != len(record_payload):
            raise JournalIntegrityError(
                "ready canonical_bytes does not match canonical payload"
            )
        actual_id = self._validated_record_id(record)
        if actual_id != row["record_id"]:
            raise JournalIntegrityError("ready record identity does not match row")
        expected_sequence = (
            record.room_sequence if isinstance(record, EventRecord) else None
        )
        authenticated = (
            record.room_id,
            record.membership_epoch,
            expected_sequence,
            ready_order,
            str(source_frame_id) if source_frame_id is not None else None,
            created_revision,
        )
        columns = tuple(
            row[name]
            for name in (
                "room_id",
                "membership_epoch",
                "room_sequence",
                "ready_order",
                "source_frame_id",
                "created_revision",
            )
        )
        if authenticated != columns:
            raise JournalIntegrityError(
                "ready record columns do not match authenticated payload"
            )
        try:
            ready = ReadyRecord(
                ready_order,
                record,
                source_frame_id,
                row["canonical_bytes"],
                created_revision,
            )
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(str(error)) from error
        self._validate_ready_record(ready)
        return ready

    def load_ready_heads(self, limit: int) -> tuple[ReadyRecord, ...]:
        self._require_attached()
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be positive")
        rows = self.connection.execute(
            """SELECT ready_order, record_id, source_frame_id, room_id,
                   membership_epoch, room_sequence, payload_ciphertext,
                   payload_sha256, canonical_bytes, created_revision
            FROM NioIngestReadyRecord
            WHERE account_id = ?
            ORDER BY ready_order, record_id
            LIMIT ?""",
            (self.account_id, limit),
        ).fetchall()
        return tuple(self._decode_ready_row(row) for row in rows)

    def _decode_room_state(self, row: sqlite3.Row) -> RoomState:
        try:
            payload = self._open_payload(
                "NioIngestRoomState", (row["room_id"],), row, "state"
            )
            envelope = _load_json_object(payload)
            if tuple(envelope) != _ROOM_STATE_FIELDS:
                raise ValueError("room state envelope fields are not canonical")
            snapshot_value = envelope["snapshot"]
            baseline_value = envelope["membership_baseline"]
            state = RoomState(
                room_id=envelope["room_id"],
                current_membership_epoch=envelope["current_membership_epoch"],
                next_room_sequence=envelope["next_room_sequence"],
                hydration_status=RoomHydrationStatus(envelope["hydration_status"]),
                snapshot=(
                    _room_snapshot_from_dict(snapshot_value)
                    if snapshot_value is not None
                    else None
                ),
                membership_baseline=(
                    _membership_baseline_from_dict(baseline_value)
                    if baseline_value is not None
                    else None
                ),
                updated_revision=envelope["updated_revision"],
            )
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "room state authenticated envelope is invalid"
            ) from error
        if payload != self._room_state_payload(state, state.updated_revision):
            raise JournalIntegrityError(
                "room state authenticated envelope is not canonical"
            )
        authenticated = (
            state.room_id,
            state.current_membership_epoch,
            state.next_room_sequence,
            state.hydration_status.value,
            state.updated_revision,
        )
        columns = tuple(
            row[name]
            for name in (
                "room_id",
                "current_membership_epoch",
                "next_room_sequence",
                "hydration_status",
                "updated_revision",
            )
        )
        if authenticated != columns:
            raise JournalIntegrityError(
                "room state columns do not match authenticated payload"
            )
        return state

    def _decode_room_lane(
        self,
        row: sqlite3.Row,
        transport_kind: TransportKind,
    ) -> RoomLane:
        try:
            payload = self._open_payload(
                "NioIngestRoomLane",
                (row["room_id"], row["membership_epoch"]),
                row,
                "lane_state",
            )
            envelope = _load_json_object(payload)
            if tuple(envelope) != _ROOM_LANE_FIELDS:
                raise ValueError("room lane envelope fields are not canonical")
            gap_value = envelope["recovery_gap"]
            lifecycle_value = envelope["pending_lifecycle"]
            lifecycle = (
                _record_from_exact_dict(lifecycle_value)
                if lifecycle_value is not None
                else None
            )
            if lifecycle is not None and type(lifecycle) is not EventRecord:
                raise ValueError("lifecycle barrier is not an EventRecord")
            lane = RoomLane(
                room_id=envelope["room_id"],
                membership_epoch=envelope["membership_epoch"],
                lane_status=LaneStatus(envelope["lane_status"]),
                held_record_count=envelope["held_record_count"],
                held_canonical_bytes=envelope["held_canonical_bytes"],
                release_phase=ReleasePhase(envelope["release_phase"]),
                ready_order=envelope["ready_order"],
                next_held_ordinal=envelope["next_held_ordinal"],
                successor_membership_epoch=envelope["successor_membership_epoch"],
                recovery_gap=(
                    _recovery_gap_from_dict(gap_value)
                    if gap_value is not None
                    else None
                ),
                pending_lifecycle=lifecycle,
                updated_revision=envelope["updated_revision"],
            )
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "room lane authenticated envelope is invalid"
            ) from error
        if payload != self._room_lane_payload(lane, lane.updated_revision):
            raise JournalIntegrityError(
                "room lane authenticated envelope is not canonical"
            )
        authenticated = (
            lane.room_id,
            lane.membership_epoch,
            lane.lane_status.value,
            lane.held_record_count,
            lane.held_canonical_bytes,
            lane.release_phase.value,
            lane.ready_order,
            lane.next_held_ordinal,
            lane.successor_membership_epoch,
            lane.updated_revision,
        )
        columns = tuple(
            row[name]
            for name in (
                "room_id",
                "membership_epoch",
                "lane_status",
                "held_record_count",
                "held_canonical_bytes",
                "release_phase",
                "ready_order",
                "next_held_ordinal",
                "successor_membership_epoch",
                "updated_revision",
            )
        )
        if authenticated != columns:
            raise JournalIntegrityError(
                "room lane columns do not match authenticated payload"
            )
        self._validate_room_lane_transport(lane, transport_kind)
        return lane

    @staticmethod
    def _validate_room_aggregate(state: RoomState, lanes: tuple[RoomLane, ...]) -> None:
        if not lanes:
            raise JournalIntegrityError("room state has no membership lane")
        try:
            RoomAggregate(state, lanes[-1], lanes[:-1])
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(str(error)) from error

    def load_rooms(
        self,
        room_ids: frozenset[str],
    ) -> dict[str, RoomAggregate]:
        owner = self._require_attached()
        if type(room_ids) is not frozenset or any(
            type(room_id) is not str for room_id in room_ids
        ):
            raise TypeError("room_ids must be a frozenset of str")
        if not room_ids:
            return {}
        ordered_ids = tuple(sorted(room_ids))
        placeholders = ",".join("?" for _ in ordered_ids)
        state_rows = self.connection.execute(
            "SELECT * FROM NioIngestRoomState "
            f"WHERE account_id = ? AND room_id IN ({placeholders}) "
            "ORDER BY room_id",
            (self.account_id, *ordered_ids),
        ).fetchall()
        lane_rows = self.connection.execute(
            "SELECT * FROM NioIngestRoomLane "
            f"WHERE account_id = ? AND room_id IN ({placeholders}) "
            "ORDER BY room_id, membership_epoch",
            (self.account_id, *ordered_ids),
        ).fetchall()
        states = {row["room_id"]: self._decode_room_state(row) for row in state_rows}
        lanes_by_room: dict[str, list[RoomLane]] = {}
        for row in lane_rows:
            lanes_by_room.setdefault(row["room_id"], []).append(
                self._decode_room_lane(row, owner.transport_kind)
            )
        if set(lanes_by_room) - set(states):
            raise JournalIntegrityError("membership lane exists without room state")

        aggregates: dict[str, RoomAggregate] = {}
        for room_id, state in states.items():
            lanes = tuple(lanes_by_room.get(room_id, ()))
            self._validate_room_aggregate(state, lanes)
            aggregates[room_id] = RoomAggregate(state, lanes[-1], lanes[:-1])
        return aggregates

    def load_loss(self, loss_id: str) -> LossRecord | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestLoss WHERE account_id = ? AND loss_id = ?",
            (self.account_id, loss_id),
        ).fetchone()
        if row is None:
            return None
        primary_key = (loss_id,)
        origin_payload, boundary_payload, detail = (
            self._open_payload(f"NioIngestLoss.{field}", primary_key, row, field)
            for field in ("origin", "boundary", "detail")
        )
        loss = LossRecord(
            loss_id,
            _origin_from_dict(_load_json_object(origin_payload)),
            row["room_id"],
            row["membership_epoch"],
            LossReason(row["reason"]),
            _boundary_from_dict(_load_json_object(boundary_payload)),
            detail,
        )
        loss_digest = hashlib.sha256(_canonical_json(_record_to_dict(loss))).digest()
        if not hmac.compare_digest(loss_digest, bytes(row["loss_sha256"])):
            raise JournalIntegrityError("whole loss digest mismatch")
        if loss.loss_id != _loss_id(self.stream_id, loss):
            raise JournalIntegrityError("loss_id does not match loss contents")
        return loss
