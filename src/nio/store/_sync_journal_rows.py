from __future__ import annotations

import hashlib
import hmac
import sqlite3
from dataclasses import replace
from uuid import UUID

from ..ingest.errors import JournalIntegrityError
from ..ingest.model import (
    EventRecord,
    LossReason,
    LossRecord,
    RecordKind,
    RoomHydrationStatus,
    SyncBatch,
    TransportKind,
)
from ..ingest.serialization import (
    _batch_from_payload,
    _boundary_from_dict,
    _boundary_to_dict,
    _canonical_json,
    _canonical_room_snapshot_payload,
    _decoded_bytes,
    _encoded_bytes,
    _load_json_object,
    _loss_id,
    _origin_from_dict,
    _origin_to_dict,
    _record_from_dict,
    _record_to_dict,
    _room_snapshot_from_payload,
    canonical_batch_payload,
)
from ..ingest.state import (
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

_FRAME_FIELDS = {"source_epoch", "request_id", "payload", "staged_revision"}
_READY_FIELDS = {"ready_order", "record", "source_frame_id", "created_revision"}


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

    def _write_room_state(self, state: RoomState, revision: int) -> None:
        if state.snapshot is None:
            ciphertext = digest = None
        else:
            payload = _canonical_room_snapshot_payload(state.snapshot)
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
                state_sha256 = excluded.state_sha256, updated_revision = excluded.updated_revision
            """,
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

    def _write_room_lane(self, lane: RoomLane, revision: int) -> None:
        if lane.pending_lifecycle is None:
            ciphertext = digest = None
        else:
            payload = _canonical_json(_record_to_dict(lane.pending_lifecycle))
            ciphertext, digest = self._codec.seal(
                "NioIngestRoomLane.lifecycle",
                (lane.room_id, lane.membership_epoch),
                payload,
            )
        self._transition_execute(
            "room_lane",
            """INSERT INTO NioIngestRoomLane (
                account_id, room_id, membership_epoch, lane_status,
                held_record_count, held_canonical_bytes, release_phase,
                release_loss_id, ready_order, next_held_ordinal,
                next_recovery_page, successor_membership_epoch,
                pending_lifecycle_ciphertext, pending_lifecycle_sha256,
                updated_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, room_id, membership_epoch) DO UPDATE SET
                lane_status = excluded.lane_status, held_record_count = excluded.held_record_count,
                held_canonical_bytes = excluded.held_canonical_bytes, release_phase = excluded.release_phase,
                release_loss_id = excluded.release_loss_id,
                ready_order = excluded.ready_order, next_held_ordinal = excluded.next_held_ordinal,
                next_recovery_page = excluded.next_recovery_page,
                successor_membership_epoch = excluded.successor_membership_epoch,
                pending_lifecycle_ciphertext = excluded.pending_lifecycle_ciphertext,
                pending_lifecycle_sha256 = excluded.pending_lifecycle_sha256, updated_revision = excluded.updated_revision
            """,
            (
                self.account_id,
                lane.room_id,
                lane.membership_epoch,
                lane.lane_status.value,
                lane.held_record_count,
                lane.held_canonical_bytes,
                lane.release_phase.value,
                lane.release_loss_id,
                lane.ready_order,
                lane.next_held_ordinal,
                lane.next_recovery_page,
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
            return record.record_id
        if record.loss_id != _loss_id(self.stream_id, record):
            raise JournalIntegrityError("loss_id does not match loss contents")
        return record.loss_id

    def _write_ready(self, ready: ReadyRecord, revision: int) -> None:
        record_id = self._validated_record_id(ready.record)
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

    def _write_source(self, source: SourceState) -> None:
        ciphertext, digest = self._codec.seal(
            "NioIngestSourceState", (self.account_id,), source.cursor_json
        )
        self._transition_execute(
            "source_state",
            """INSERT INTO NioIngestSourceState (
                account_id, source_epoch, transport_kind, cursor_ciphertext,
                cursor_sha256, next_request_id, active
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                source_epoch = excluded.source_epoch, transport_kind = excluded.transport_kind,
                cursor_ciphertext = excluded.cursor_ciphertext, cursor_sha256 = excluded.cursor_sha256,
                next_request_id = excluded.next_request_id, active = excluded.active
            """,
            (
                self.account_id,
                source.source_epoch,
                source.transport_kind.value,
                ciphertext,
                digest,
                source.next_request_id,
                int(source.active),
            ),
        )

    def load_source(self) -> SourceState | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestSourceState WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if row is None:
            return None
        cursor = self._open_payload(
            "NioIngestSourceState", (self.account_id,), row, "cursor"
        )
        return SourceState(
            row["source_epoch"],
            TransportKind(row["transport_kind"]),
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
        batch = _batch_from_payload(payload)
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
        record = _record_from_dict(envelope["record"])
        record_payload = _canonical_json(_record_to_dict(record))
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
        return ReadyRecord(
            ready_order,
            record,
            source_frame_id,
            row["canonical_bytes"],
            created_revision,
        )

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
        ciphertext = row["state_ciphertext"]
        digest_value = row["state_sha256"]
        if (ciphertext is None) != (digest_value is None):
            raise JournalIntegrityError("partial encrypted room state")
        snapshot = None
        if ciphertext is not None:
            payload = self._open_payload(
                "NioIngestRoomState", (row["room_id"],), row, "state"
            )
            snapshot = _room_snapshot_from_payload(payload)
        return RoomState(
            row["room_id"],
            row["current_membership_epoch"],
            row["next_room_sequence"],
            RoomHydrationStatus(row["hydration_status"]),
            snapshot,
            row["updated_revision"],
        )

    def _decode_room_lane(self, row: sqlite3.Row) -> RoomLane:
        ciphertext = row["pending_lifecycle_ciphertext"]
        digest_value = row["pending_lifecycle_sha256"]
        if (ciphertext is None) != (digest_value is None):
            raise JournalIntegrityError("partial encrypted lifecycle barrier")
        lifecycle = None
        if ciphertext is not None:
            payload = self._open_payload(
                "NioIngestRoomLane.lifecycle",
                (row["room_id"], row["membership_epoch"]),
                row,
                "pending_lifecycle",
            )
            decoded = _record_from_dict(_load_json_object(payload))
            if not isinstance(decoded, EventRecord):
                raise JournalIntegrityError("lifecycle barrier is not an event record")
            lifecycle = decoded
        return RoomLane(
            room_id=row["room_id"],
            membership_epoch=row["membership_epoch"],
            lane_status=LaneStatus(row["lane_status"]),
            held_record_count=row["held_record_count"],
            held_canonical_bytes=row["held_canonical_bytes"],
            release_phase=ReleasePhase(row["release_phase"]),
            release_loss_id=row["release_loss_id"],
            ready_order=row["ready_order"],
            next_held_ordinal=row["next_held_ordinal"],
            next_recovery_page=row["next_recovery_page"],
            successor_membership_epoch=row["successor_membership_epoch"],
            pending_lifecycle=lifecycle,
            updated_revision=row["updated_revision"],
        )

    @staticmethod
    def _validate_room_aggregate(state: RoomState, lanes: tuple[RoomLane, ...]) -> None:
        if not lanes:
            raise JournalIntegrityError("room state has no membership lane")
        ordered = tuple(sorted(lanes, key=lambda lane: lane.membership_epoch))
        if ordered != lanes:
            raise JournalIntegrityError("room membership lanes are not ordered")
        if any(
            right.membership_epoch != left.membership_epoch + 1
            for left, right in zip(ordered, ordered[1:], strict=False)
        ):
            raise JournalIntegrityError("room membership lane chain is not gap-free")
        active = tuple(
            lane for lane in ordered if lane.lane_status is LaneStatus.ACTIVE
        )
        if len(active) != 1:
            raise JournalIntegrityError("room must have exactly one active lane")
        if active[0].membership_epoch != state.current_membership_epoch:
            raise JournalIntegrityError(
                "active lane does not match current membership epoch"
            )
        if active[0] is not ordered[-1]:
            raise JournalIntegrityError("active lane must be the final epoch")

        for lane, successor in zip(ordered, ordered[1:], strict=False):
            lifecycle = lane.pending_lifecycle
            if lane.lane_status is not LaneStatus.RETIRING:
                raise JournalIntegrityError("predecessor lane must be retiring")
            if lane.successor_membership_epoch != successor.membership_epoch:
                raise JournalIntegrityError("retiring lane successor is not gap-free")
            if (
                lifecycle is None
                or lifecycle.kind is not RecordKind.ROOM_LIFECYCLE
                or lifecycle.room_id != state.room_id
                or lifecycle.membership_epoch != successor.membership_epoch
            ):
                raise JournalIntegrityError(
                    "retiring lane lifecycle barrier is invalid"
                )
        if (
            active[0].successor_membership_epoch is not None
            or active[0].pending_lifecycle is not None
        ):
            raise JournalIntegrityError("active lane cannot have a successor barrier")

    def load_rooms(
        self,
        room_ids: frozenset[str],
    ) -> dict[str, RoomAggregate]:
        self._require_attached()
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
                self._decode_room_lane(row)
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
