from __future__ import annotations

import base64
import json
import sqlite3
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, cast
from uuid import UUID

from ..ingest._json import canonical_json, load_internal_json
from ..ingest.errors import JournalIntegrityError
from ..ingest.hydration import PendingHydration
from ..ingest.model import (
    EventRecord,
    LossRecord,
    RecordKind,
    RecordOrigin,
    TransportKind,
)
from ..ingest.ports import (
    NetworkRequest,
    StagedSourceResponse,
)
from ..ingest.reducer import HydrationIntent, MembershipBaseline, RoomContinuity
from ..ingest.serialization import (
    _loss_id,
    _origin_from_dict,
    _origin_to_dict,
    _record_from_dict,
)
from ..ingest.source import MAX_STORED_FRAME_PAYLOAD_BYTES
from ..ingest.state import OwnerView, SourceState, StagedFrame
from ._sync_journal_plan import (
    AuthenticatedWork,
    _prepared_metadata_from_plaintext,
    _PreparedWorkMetadata,
)
from ._sync_journal_plan import (
    _canonical_work_plaintext as _canonical_work_plaintext,
)
from ._sync_journal_preflight import _row, _validate_source_cursor
from ._sync_journal_values import RoomAggregateValue

if TYPE_CHECKING:
    from collections.abc import Mapping


_NORMALIZATION_VERSION = 1
_NETWORK_REQUEST_FIELDS = (
    "stream_id",
    "transport",
    "source_epoch",
    "request_id",
    "method",
    "path",
    "query",
    "body",
    "timeout_ms",
    "request_cursor_json",
)
_FRAME_FIELDS = (
    "normalization_version",
    "request",
    "response_body",
    "source_sha256",
)
# IngestionConfig caps the staged backlog at 256 frames. Reading one extra row
# keeps account identity classification bounded while detecting corrupt overflow.
_MAX_STAGED_FRAMES = 256
_FRAME_CLASSIFICATION_LIMIT = _MAX_STAGED_FRAMES + 1
_MAX_WORK_PAYLOAD_BYTES = 1024 * 1024
_MAX_HELD_WORK_COUNT = 10_000
_MAX_HELD_WORK_CANONICAL_BYTES = 32 * 1024 * 1024
_MAX_TOTAL_WORK_COUNT = 20_000
_MAX_TOTAL_WORK_CANONICAL_BYTES = 64 * 1024 * 1024
_Owner = tuple[str, UUID, TransportKind]


def _canonical_internal(value: object) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


class _DecodedWork(NamedTuple):
    value: EventRecord | LossRecord
    metadata: _PreparedWorkMetadata | None


def _decode_work_plaintext(
    stream_id: UUID,
    work_id: str,
    kind: str,
    plaintext: bytes,
) -> _DecodedWork:
    if type(stream_id) is not UUID:
        raise TypeError("stream_id must be UUID")
    if type(work_id) is not str or type(kind) is not str:
        raise TypeError("work identity must contain strings")
    if type(plaintext) is not bytes:
        raise TypeError("work plaintext must be bytes")
    if kind not in ("event", "loss"):
        raise ValueError("unsupported Work kind")
    try:
        wrapper = load_internal_json(plaintext, "work plaintext")
        if type(wrapper) is not dict or wrapper.get("kind") != kind:
            raise ValueError("work wrapper kind is invalid")
        value = _record_from_dict(wrapper.get("value"), exact=False)
        metadata = _prepared_metadata_from_plaintext(value, plaintext)
        identity = value.record_id if isinstance(value, EventRecord) else value.loss_id
        if (
            type(value) not in (EventRecord, LossRecord)
            or work_id != str(UUID(work_id))
            or identity != work_id
            or type(value) is LossRecord
            and _loss_id(stream_id, value) != work_id
        ):
            raise ValueError("work plaintext is not canonical")
        return _DecodedWork(value, metadata)
    except (AttributeError, TypeError, ValueError) as error:
        raise ValueError("Work plaintext is invalid") from error


def _work_value_from_plaintext(
    stream_id: UUID,
    work_id: str,
    kind: str,
    plaintext: bytes,
) -> EventRecord | LossRecord:
    return _decode_work_plaintext(stream_id, work_id, kind, plaintext).value


def _aggregate_uuid(value: object, label: str) -> UUID:
    if type(value) is not str:
        raise ValueError(f"{label} must be a UUID string")
    parsed = UUID(value)
    if value != str(parsed):
        raise ValueError(f"{label} is not canonical")
    return parsed


def _canonical_room_aggregate_plaintext(value: RoomAggregateValue) -> bytes:
    if type(value) is not RoomAggregateValue:
        raise TypeError("aggregate value must be RoomAggregateValue")
    hydration = value.pending_hydration
    continuity = value.continuity
    if continuity.gap is not None:
        raise ValueError("this checkpoint persists no recovery gap")
    baseline = continuity.baseline
    if baseline is not None and baseline.membership_event_id is None:
        raise ValueError("unsupported Aggregate baseline")
    if baseline is not None and hydration is not None:
        raise ValueError("baseline and pending hydration are mutually exclusive")
    return canonical_json(
        {
            "continuity": {
                "baseline": (
                    {
                        "membership_event_id": baseline.membership_event_id,
                        "window_token": baseline.window_token,
                    }
                    if baseline is not None
                    else None
                ),
                "gap": None,
                "hydration_id": str(hydration.hydration_id) if hydration else None,
                "membership": continuity.membership,
                "membership_epoch": continuity.membership_epoch,
                "room_id": continuity.room_id,
            },
            "next_room_sequence": value.next_room_sequence,
            "pending_hydration": (
                {
                    "hydration_id": str(hydration.hydration_id),
                    "origin": _origin_to_dict(hydration.origin),
                }
                if hydration is not None
                else None
            ),
            "updated_revision": value.updated_revision,
        }
    )


def _room_aggregate_value_from_plaintext(
    room_id: str,
    updated_revision: int,
    intent_kind: str | None,
    plaintext: bytes,
) -> RoomAggregateValue:
    try:
        if type(room_id) is not str or not room_id:
            raise ValueError("aggregate room_id is invalid")
        if type(updated_revision) is not int or updated_revision < 1:
            raise ValueError("aggregate revision is invalid")
        if intent_kind not in (None, "hydration"):
            raise ValueError("unsupported Aggregate intent kind")
        if type(plaintext) is not bytes:
            raise TypeError("aggregate plaintext must be bytes")
        decoded = load_internal_json(plaintext, "room aggregate plaintext")
        if type(decoded) is not dict or type(decoded.get("continuity")) is not dict:
            raise ValueError("Aggregate and continuity must be objects")
        aggregate = cast("dict[str, object]", decoded)
        continuity = cast("dict[str, object]", aggregate["continuity"])
        if continuity["gap"] is not None:
            raise ValueError("Aggregate gap must be null")
        baseline_value = continuity["baseline"]
        baseline = None
        if baseline_value is not None:
            if type(baseline_value) is not dict:
                raise ValueError("Aggregate baseline must be an object")
            baseline_map = cast("dict[str, object]", baseline_value)
            baseline = MembershipBaseline(
                cast("str", baseline_map["membership_event_id"]),
                cast("str | None", baseline_map["window_token"]),
            )
            if baseline.membership_event_id is None:
                raise ValueError("unsupported Aggregate baseline")

        raw_hydration_id = continuity["hydration_id"]
        hydration_id = (
            _aggregate_uuid(raw_hydration_id, "hydration_id")
            if raw_hydration_id is not None
            else None
        )
        state = RoomContinuity(
            cast("str", continuity["room_id"]),
            cast("int", continuity["membership_epoch"]),
            cast("str | None", continuity["membership"]),
            baseline,
            None,
            hydration_id,
        )

        pending_value = aggregate["pending_hydration"]
        if intent_kind is None:
            if pending_value is not None or hydration_id is not None:
                raise ValueError("NULL Aggregate must have no hydration barrier")
            pending = None
        else:
            if type(pending_value) is not dict or hydration_id is None:
                raise ValueError("hydration Aggregate requires its pending intent")
            pending_map = cast("dict[str, object]", pending_value)
            pending_origin = _origin_from_dict(pending_map["origin"], exact=False)
            if (
                type(pending_origin) is not RecordOrigin
                or min(
                    pending_origin.source_epoch,
                    pending_origin.request_id,
                    pending_origin.frame_index,
                )
                < 0
            ):
                raise ValueError("hydration origin must be a transport origin")
            pending = HydrationIntent(
                _aggregate_uuid(pending_map["hydration_id"], "hydration_id"),
                pending_origin,
            )

        value = RoomAggregateValue(
            state,
            cast("int", aggregate["next_room_sequence"]),
            cast("int", aggregate["updated_revision"]),
            pending,
        )
        if (
            value.continuity.room_id != room_id
            or value.updated_revision != updated_revision
            or plaintext != _canonical_room_aggregate_plaintext(value)
        ):
            raise ValueError("aggregate plaintext does not match its row")
        return value
    except (AttributeError, KeyError, TypeError, ValueError) as error:
        raise ValueError("room Aggregate plaintext is invalid") from error


def _encoded_bytes(value: bytes | None) -> str | None:
    return base64.b64encode(value).decode("ascii") if value is not None else None


def _decoded_bytes(
    value: object,
    field_name: str,
    *,
    optional: bool = False,
) -> bytes | None:
    if value is None and optional:
        return None
    if type(value) is not str:
        raise ValueError(f"{field_name} must be a base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except ValueError as error:
        raise ValueError(f"{field_name} must be valid base64") from error


def _network_request_to_dict(request: NetworkRequest) -> dict[str, object]:
    if type(request) is not NetworkRequest:
        raise TypeError("request must be NetworkRequest")
    return {
        "stream_id": str(request.stream_id),
        "transport": request.transport.value,
        "source_epoch": request.source_epoch,
        "request_id": request.request_id,
        "method": request.method,
        "path": request.path,
        "query": [[key, value] for key, value in request.query],
        "body": _encoded_bytes(request.body),
        "timeout_ms": request.timeout_ms,
        "request_cursor_json": _encoded_bytes(request.request_cursor_json),
    }


def _network_request_from_dict(value: object) -> NetworkRequest:
    if type(value) is not dict or tuple(value) != _NETWORK_REQUEST_FIELDS:
        raise ValueError("network request fields are not canonical")
    query = value["query"]
    if type(query) is not list:
        raise ValueError("network request query must be an array")
    pairs: list[tuple[str, str]] = []
    for pair in query:
        if type(pair) is not list or len(pair) != 2:
            raise ValueError("network request query entries are invalid")
        key, item = pair
        if type(key) is not str or type(item) is not str:
            raise ValueError("network request query entries are invalid")
        pairs.append((key, item))
    for name in ("source_epoch", "request_id", "timeout_ms"):
        if type(value[name]) is not int:
            raise ValueError("network request integer fields are invalid")
    for name in ("stream_id", "transport", "method", "path"):
        if type(value[name]) is not str:
            raise ValueError("network request string fields are invalid")
    cursor = _decoded_bytes(value["request_cursor_json"], "request cursor")
    assert cursor is not None
    try:
        request = NetworkRequest(
            UUID(value["stream_id"]),
            TransportKind(value["transport"]),
            value["source_epoch"],
            value["request_id"],
            value["method"],
            value["path"],
            tuple(pairs),
            _decoded_bytes(value["body"], "request body", optional=True),
            value["timeout_ms"],
            cursor,
        )
    except (TypeError, ValueError) as error:
        raise ValueError("network request is invalid") from error
    if value != _network_request_to_dict(request):
        raise ValueError("network request is not canonical")
    return request


def _frame_envelope(frame: StagedFrame) -> dict[str, object]:
    return {
        "normalization_version": _NORMALIZATION_VERSION,
        "request": _network_request_to_dict(frame.response.request),
        "response_body": _encoded_bytes(frame.response.response_body),
        "source_sha256": _encoded_bytes(frame.response.source_sha256),
    }


def _frame_payload(frame: StagedFrame, rev: int, owner: _Owner) -> tuple[bytes, bytes]:
    value = _canonical_internal(_frame_envelope(frame))
    stored = _row(owner, "NioIngestFrame", value, header=_frame_header(frame, rev))
    if len(stored[0]) > MAX_STORED_FRAME_PAYLOAD_BYTES:
        raise JournalIntegrityError("staged frame envelope exceeds 24 MiB")
    return stored


def _frame_response_from_envelope(value: object) -> StagedSourceResponse:
    if type(value) is not dict or tuple(value) != _FRAME_FIELDS:
        raise ValueError("frame authenticated envelope is invalid")
    if type(value["normalization_version"]) is not int or (
        value["normalization_version"] != _NORMALIZATION_VERSION
    ):
        raise ValueError("frame normalization version is invalid")
    body = _decoded_bytes(value["response_body"], "response body")
    digest = _decoded_bytes(value["source_sha256"], "source digest")
    assert body is not None
    assert digest is not None
    response = StagedSourceResponse(
        _network_request_from_dict(value["request"]),
        body,
        digest,
    )
    return response


def _source_header(source: SourceState) -> bytes:
    return _canonical_internal(
        [
            source.source_epoch,
            source.next_request_id,
            source.active,
        ]
    )


def _frame_header(frame: StagedFrame, staged_revision: int) -> bytes:
    request = frame.response.request
    return _canonical_internal(
        [
            str(frame.frame_id),
            request.source_epoch,
            request.request_id,
            staged_revision,
        ]
    )


def _frame_drain_sha256(
    owner: OwnerView,
    *,
    frame_id: UUID,
    source_epoch: int,
    request_id: int,
    staged_revision: int,
    payload_sha256: bytes,
    payload_length: int,
    room_materialized_revision: int | None,
    callbacks_claimed_revision: int | None,
) -> bytes:
    clear = (
        str(frame_id),
        source_epoch,
        request_id,
        staged_revision,
        base64.b64encode(payload_sha256).decode(),
        payload_length,
        room_materialized_revision,
        callbacks_claimed_revision,
    )
    bound = owner.account_id, owner.stream_id, owner.transport_kind
    return _row(bound, "NioIngestFrameDrainHeader", None, header=clear)


class _FrameDrainRow(NamedTuple):
    account_id: str
    raw_frame_id: str
    frame_id: UUID
    source_epoch: int
    request_id: int
    staged_revision: int
    payload_sha256: bytes
    payload_length: int
    room_materialized_revision: int | None
    callbacks_claimed_revision: int | None
    drain_header_sha256: bytes


class _Task3WorkInventory(NamedTuple):
    storage_rows: tuple[tuple[object, ...], ...]
    work: tuple[AuthenticatedWork, ...]


class JournalRows:
    account_id: str
    device_id: str

    def _payload(self, o: OwnerView, *args: Any, **kwargs: Any) -> Any:
        return _row((self.account_id, o.stream_id, o.transport_kind), *args, **kwargs)

    def _meta(self) -> sqlite3.Row:
        rows = self._execute(  # type: ignore[attr-defined]
            "SELECT * FROM NioIngestMeta LIMIT 2"
        ).fetchall()
        if len(rows) != 1:
            raise JournalIntegrityError(
                "ingestion-v1 marker row cardinality is not one"
            )
        return rows[0]

    def _decode_owner_row(self, row: Mapping[str, object]) -> OwnerView:
        try:
            owner = OwnerView(
                row["account_id"],
                row["device_id"],
                row["schema_version"],
                UUID(row["stream_id"]),
                UUID(row["consumer_generation"]),
                TransportKind(cast("str", row["transport_kind"])),
                row["revision"],
                UUID(row["writer_epoch"]),
                row["next_source_epoch"],
            )
            if type(row["created_at_ns"]) is not int or row["created_at_ns"] < 0:
                raise ValueError("created_at_ns is invalid")
            if (row["stream_id"], row["consumer_generation"], row["writer_epoch"]) != (
                str(owner.stream_id),
                str(owner.consumer_generation),
                str(owner.writer_epoch),
            ):
                raise ValueError("owner UUID is not canonical")
        except (AttributeError, TypeError, ValueError) as error:
            raise JournalIntegrityError("ingestion owner row is invalid") from error
        if owner.account_id != self.account_id or owner.device_id != self.device_id:
            raise JournalIntegrityError("ingestion owner identity changed")
        return owner

    def load_owner(self) -> OwnerView:
        with self._read():  # type: ignore[attr-defined]
            return self._decode_owner_row(self._meta())

    def _load_stage_snapshot(self) -> tuple[OwnerView, SourceState]:
        rows = self._execute(  # type: ignore[attr-defined]
            "SELECT m.*, s.account_id AS joined_source_account_id, "
            "s.source_epoch AS joined_source_epoch, "
            "s.payload AS joined_payload, "
            "s.payload_sha256 AS joined_payload_sha256, "
            "s.next_request_id AS joined_next_request_id, "
            "s.active AS joined_active FROM NioIngestMeta AS m "
            "CROSS JOIN NioIngestSourceState AS s"
        ).fetchall()
        if len(rows) != 1:
            raise JournalIntegrityError(
                "ingestion stage snapshot cardinality is not one"
            )
        row = rows[0]
        owner = self._decode_owner_row(row)
        source_row = {
            "account_id": row["joined_source_account_id"],
            "source_epoch": row["joined_source_epoch"],
            "payload": row["joined_payload"],
            "payload_sha256": row["joined_payload_sha256"],
            "next_request_id": row["joined_next_request_id"],
            "active": row["joined_active"],
        }
        return owner, self._decode_source_row(source_row, owner)

    def _decode_source_row(
        self,
        row: Mapping[str, object],
        owner: OwnerView,
    ) -> SourceState:
        try:
            active = row["active"]
            if type(active) is not int or active not in (0, 1):
                raise ValueError("source active column is invalid")
            clear = SourceState(
                row["source_epoch"],
                owner.transport_kind,
                b"",
                row["next_request_id"],
                bool(active),
            )
            cursor_json = self._payload(
                owner,
                "NioIngestSourceState",
                row["payload"],
                row["payload_sha256"],
                header=_source_header(clear),
            )
            source = SourceState(
                clear.source_epoch,
                clear.transport_kind,
                cursor_json,
                clear.next_request_id,
                clear.active,
            )
            _validate_source_cursor(source.transport_kind, source.cursor_json)
            if row["account_id"] != self.account_id:
                raise ValueError("source account_id changed")
            return source
        except JournalIntegrityError:
            raise
        except (KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError("persisted source state is invalid") from error

    def load_source(self) -> SourceState:
        with self._read():  # type: ignore[attr-defined]
            owner = self.load_owner()
            rows = self._execute(  # type: ignore[attr-defined]
                "SELECT * FROM NioIngestSourceState"
            ).fetchall()
            if len(rows) != 1:
                raise JournalIntegrityError(
                    "ingestion source row cardinality is not one"
                )
            return self._decode_source_row(rows[0], owner)

    def _write_source(self, source: SourceState, owner: OwnerView) -> sqlite3.Cursor:
        _validate_source_cursor(source.transport_kind, source.cursor_json)
        payload, digest = self._payload(
            owner,
            "NioIngestSourceState",
            source.cursor_json,
            header=_source_header(source),
        )
        return self._execute(  # type: ignore[attr-defined]
            "INSERT INTO NioIngestSourceState ("
            "account_id, source_epoch, payload, payload_sha256, "
            "next_request_id, active) VALUES (?, ?, ?, ?, ?, ?) "
            "ON CONFLICT(account_id) DO UPDATE SET "
            "source_epoch = excluded.source_epoch, "
            "payload = excluded.payload, "
            "payload_sha256 = excluded.payload_sha256, "
            "next_request_id = excluded.next_request_id, "
            "active = excluded.active",
            (
                self.account_id,
                source.source_epoch,
                payload,
                digest,
                source.next_request_id,
                int(source.active),
            ),
        )

    @staticmethod
    def _parse_frame_id(
        row: Mapping[str, object],
    ) -> tuple[str, UUID]:
        try:
            raw_frame_id = row["frame_id"]
            if type(raw_frame_id) is not str:
                raise TypeError
            stored_id = UUID(raw_frame_id)
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError("persisted frame_id is invalid") from error
        return raw_frame_id, stored_id

    def _decode_frame_drain_row(
        self,
        row: Mapping[str, object],
        owner: OwnerView,
        payload_length: object,
        *,
        authenticate: bool,
    ) -> _FrameDrainRow:
        raw_frame_id, frame_id = self._parse_frame_id(row)
        try:
            account_id = row["account_id"]
            source_epoch = row["source_epoch"]
            request_id = row["request_id"]
            staged_revision = row["staged_revision"]
            payload_sha256 = row["payload_sha256"]
            room_materialized_revision = row["room_materialized_revision"]
            callbacks_claimed_revision = row["callbacks_claimed_revision"]
            drain_header_sha256 = row["drain_header_sha256"]
            if (
                type(account_id) is not str
                or account_id != self.account_id
                or raw_frame_id != str(frame_id)
                or type(source_epoch) is not int
                or source_epoch < 0
                or type(request_id) is not int
                or request_id < 0
                or type(staged_revision) is not int
                or staged_revision < 1
                or type(payload_sha256) is not bytes
                or len(payload_sha256) != 32
                or type(payload_length) is not int
                or not 0 < payload_length <= MAX_STORED_FRAME_PAYLOAD_BYTES
                or type(drain_header_sha256) is not bytes
                or len(drain_header_sha256) != 32
            ):
                raise ValueError("frame drain columns are invalid")
            if room_materialized_revision is not None and (
                type(room_materialized_revision) is not int
                or room_materialized_revision < 1
            ):
                raise ValueError("frame materialized revision is invalid")
            if callbacks_claimed_revision is not None and (
                type(callbacks_claimed_revision) is not int
                or callbacks_claimed_revision < 1
            ):
                raise ValueError("frame callback claim revision is invalid")
            drain_row = _FrameDrainRow(
                account_id,
                raw_frame_id,
                frame_id,
                source_epoch,
                request_id,
                staged_revision,
                payload_sha256,
                payload_length,
                room_materialized_revision,
                callbacks_claimed_revision,
                drain_header_sha256,
            )
            if authenticate and (
                _frame_drain_sha256(
                    owner,
                    frame_id=drain_row.frame_id,
                    source_epoch=drain_row.source_epoch,
                    request_id=drain_row.request_id,
                    staged_revision=drain_row.staged_revision,
                    payload_sha256=drain_row.payload_sha256,
                    payload_length=drain_row.payload_length,
                    room_materialized_revision=drain_row.room_materialized_revision,
                    callbacks_claimed_revision=drain_row.callbacks_claimed_revision,
                )
                != drain_header_sha256
            ):
                raise ValueError("frame drain header proof is not empty")
            if room_materialized_revision is not None and not (
                staged_revision < room_materialized_revision <= owner.revision
            ):
                raise ValueError("frame materialized revision is invalid")
            if callbacks_claimed_revision is not None and not (
                staged_revision < callbacks_claimed_revision <= owner.revision
            ):
                raise ValueError("frame callback claim revision is invalid")
            return drain_row
        except JournalIntegrityError:
            raise
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "persisted frame drain row is invalid"
            ) from error

    def _load_authenticated_frame_headers(
        self,
        owner: OwnerView,
    ) -> tuple[_FrameDrainRow, ...]:
        rows = self._execute(  # type: ignore[attr-defined]
            "SELECT account_id, frame_id, source_epoch, request_id, "
            "staged_revision, payload_sha256, "
            "LENGTH(payload) AS payload_length, "
            "room_materialized_revision, callbacks_claimed_revision, "
            "drain_header_sha256 "
            "FROM NioIngestFrame LIMIT ?",
            (_FRAME_CLASSIFICATION_LIMIT,),
        ).fetchall()
        if len(rows) > _MAX_STAGED_FRAMES:
            raise JournalIntegrityError("staged frame count exceeds the 256 frame cap")
        decoded = tuple(
            self._decode_frame_drain_row(
                cast("Mapping[str, object]", row),
                owner,
                row["payload_length"],
                authenticate=True,
            )
            for row in rows
        )
        if len({row.frame_id for row in decoded}) != len(decoded):
            raise JournalIntegrityError("frame_id has multiple textual identities")
        return tuple(
            sorted(
                decoded,
                key=lambda row: (
                    row.staged_revision,
                    row.source_epoch,
                    row.request_id,
                    row.raw_frame_id,
                ),
            )
        )

    def _load_task3_work_inventory(
        self,
        owner: OwnerView,
    ) -> _Task3WorkInventory:
        cursor = self._execute(  # type: ignore[attr-defined]
            "SELECT account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload, payload_sha256 "
            "FROM NioIngestWork LIMIT 20001",
        )
        storage_rows: list[tuple[object, ...]] = []
        payload_bytes = 0
        while (fetched := cursor.fetchone()) is not None:
            row = tuple(fetched)
            try:
                (
                    account_id,
                    work_id,
                    kind,
                    status,
                    raw_frame_id,
                    room_id,
                    membership_epoch,
                    room_sequence,
                    ready_revision,
                    ready_ordinal,
                    created_revision,
                    payload,
                    digest,
                ) = row
                if (
                    account_id != self.account_id
                    or type(work_id) is not str
                    or work_id != str(UUID(work_id))
                    or type(raw_frame_id) is not str
                    or raw_frame_id != str(UUID(raw_frame_id))
                    or kind not in ("event", "loss")
                    or status not in ("ready", "held")
                    or type(created_revision) is not int
                    or not 1 <= created_revision <= owner.revision
                    or type(payload) is not bytes
                    or not 0 < len(payload) <= _MAX_WORK_PAYLOAD_BYTES
                    or type(digest) is not bytes
                    or len(digest) != 32
                ):
                    raise ValueError("Work columns are invalid")
                if status == "ready":
                    if (
                        type(ready_revision) is not int
                        or not created_revision <= ready_revision <= owner.revision
                        or type(ready_ordinal) is not int
                        or ready_ordinal < 0
                    ):
                        raise ValueError("READY Work columns are invalid")
                elif (
                    kind != "event"
                    or type(room_id) is not str
                    or not room_id
                    or type(membership_epoch) is not int
                    or membership_epoch < 0
                    or type(room_sequence) is not int
                    or room_sequence < 0
                    or ready_revision is not None
                    or ready_ordinal is not None
                ):
                    raise ValueError("HELD Work columns are invalid")
            except (AttributeError, TypeError, ValueError) as error:
                raise JournalIntegrityError("invalid Work row") from error
            storage_rows.append(row)
            payload_bytes += len(payload)
            if (
                len(storage_rows) > _MAX_TOTAL_WORK_COUNT
                or payload_bytes > _MAX_TOTAL_WORK_CANONICAL_BYTES
            ):
                break

        storage_rows.sort(key=lambda row: cast("str", row[1]))
        canonical_bytes = 0
        work: list[AuthenticatedWork] = []
        for row in storage_rows:
            try:
                plaintext = self._payload(
                    owner,
                    "NioIngestWork",
                    row[11],
                    row[12],
                    header=_canonical_internal(row[1:11]),
                )
                decoded = _decode_work_plaintext(
                    owner.stream_id,
                    cast("str", row[1]),
                    cast("str", row[2]),
                    plaintext,
                )
                value = decoded.value
                origin = value.origin
                if (
                    type(origin) is not RecordOrigin
                    or origin.transport is not owner.transport_kind
                    or min(origin.source_epoch, origin.request_id, origin.frame_index)
                    < 0
                ):
                    raise ValueError("Work origin is invalid")
                if type(value) is LossRecord:
                    if (
                        row[3] != "ready"
                        or not value.room_id
                        or value.membership_epoch < 0
                        or (value.room_id, value.membership_epoch, None)
                        != tuple(row[5:8])
                        or value.detail_json != b"{}"
                    ):
                        raise ValueError("READY loss Work value is invalid")
                elif type(value) is EventRecord:
                    room_value = (
                        value.room_id,
                        value.membership_epoch,
                        value.room_sequence,
                    )
                    room_kinds = (
                        RecordKind.STATE,
                        RecordKind.TIMELINE,
                        RecordKind.EPHEMERAL,
                        RecordKind.ROOM_ACCOUNT_DATA,
                    )
                    if decoded.metadata is None and (
                        value.event_id is not None or value.clear_json is not None
                    ):
                        raise ValueError("Work event value is invalid")
                    if row[3] == "held":
                        valid = value.kind in room_kinds and room_value == tuple(
                            row[5:8]
                        )
                    elif value.room_id is None:
                        valid = value.kind in (
                            RecordKind.GLOBAL_ACCOUNT_DATA,
                            RecordKind.PRESENCE,
                            *(
                                (RecordKind.TO_DEVICE,)
                                if decoded.metadata is not None
                                else ()
                            ),
                        ) and room_value == (None, None, None)
                    else:
                        valid = (
                            bool(value.room_id)
                            and value.membership_epoch is not None
                            and value.membership_epoch >= 0
                            and value.room_sequence is not None
                            and value.room_sequence >= 0
                            and value.kind in (*room_kinds, RecordKind.ROOM_LIFECYCLE)
                            and room_value == tuple(row[5:8])
                        )
                    if not valid or (
                        (value.kind is RecordKind.TIMELINE)
                        != (value.provenance is not None)
                    ):
                        raise ValueError("Work event value is invalid")
                else:
                    raise ValueError("unsupported Work value")
            except (TypeError, ValueError) as error:
                raise JournalIntegrityError("invalid Work value") from error
            work.append(
                AuthenticatedWork(
                    value,
                    cast("Literal['ready', 'held']", row[3]),
                    len(cast("bytes", row[11])),
                    decoded.metadata,
                    plaintext,
                    UUID(cast("str", row[4])),
                    cast("int", row[10]),
                )
            )
            canonical_bytes += len(cast("bytes", row[11]))
        held = tuple(item for item in work if item.status == "held")
        if (
            len(held) > _MAX_HELD_WORK_COUNT
            or sum(item.canonical_size for item in held)
            > _MAX_HELD_WORK_CANONICAL_BYTES
        ):
            raise JournalIntegrityError("HELD Work exceeds immutable capacity")
        if (
            len(storage_rows) > _MAX_TOTAL_WORK_COUNT
            or canonical_bytes > _MAX_TOTAL_WORK_CANONICAL_BYTES
        ):
            raise JournalIntegrityError("total Work exceeds immutable capacity")
        return _Task3WorkInventory(tuple(storage_rows), tuple(work))

    def _load_room_aggregate(
        self,
        owner: OwnerView,
        room_id: str,
    ) -> tuple[tuple[object, ...], RoomAggregateValue] | None:
        row = self._execute(  # type: ignore[attr-defined]
            "SELECT account_id, room_id, updated_revision, intent_kind, "
            "payload, payload_sha256 "
            "FROM NioIngestRoomAggregate WHERE account_id = ? AND room_id = ?",
            (self.account_id, room_id),
        ).fetchone()
        if row is None:
            return None
        stored = tuple(row)
        try:
            account_id, stored_room, revision, kind, payload, digest = stored
            if (
                account_id != self.account_id
                or stored_room != room_id
                or type(revision) is not int
                or not 1 <= revision <= owner.revision
                or kind not in (None, "hydration")
                or type(payload) is not bytes
                or not payload
                or type(digest) is not bytes
                or len(digest) != 32
            ):
                raise ValueError("Aggregate columns are invalid")
            plaintext = self._payload(
                owner,
                "NioIngestRoomAggregate",
                payload,
                digest,
                header=_canonical_internal([room_id, revision, kind]),
            )
            value = _room_aggregate_value_from_plaintext(
                room_id,
                revision,
                kind,
                plaintext,
            )
            if value.pending_hydration is not None and (
                value.pending_hydration.origin.transport is not owner.transport_kind
            ):
                raise ValueError("Aggregate origin transport does not match owner")
            return stored, value
        except JournalIntegrityError:
            raise
        except (AttributeError, TypeError, ValueError) as error:
            raise JournalIntegrityError("invalid room Aggregate row") from error

    def load_pending_hydrations(self, *, limit: int) -> tuple[PendingHydration, ...]:
        if type(limit) is not int:
            raise TypeError("limit must be int")
        if not 1 <= limit <= 2:
            raise ValueError("limit must be between 1 and 2")
        with self._read():  # type: ignore[attr-defined]
            owner = self._decode_owner_row(self._meta())
            rows = self._execute(  # type: ignore[attr-defined]
                "SELECT account_id, room_id, updated_revision, intent_kind, "
                "payload, payload_sha256 FROM NioIngestRoomAggregate "
                "WHERE account_id = ? AND intent_kind = 'hydration' "
                "ORDER BY room_id LIMIT ?",
                (self.account_id, limit),
            ).fetchall()
            pending: list[PendingHydration] = []
            for row in rows:
                room_id = row["room_id"]
                loaded = self._load_room_aggregate(owner, room_id)
                if loaded is None or tuple(row) != loaded[0]:
                    raise JournalIntegrityError("hydration candidate snapshot changed")
                value = loaded[1]
                if value.pending_hydration is None:
                    raise JournalIntegrityError("hydration candidate has no intent")
                pending.append(
                    PendingHydration(value.continuity, value.pending_hydration)
                )
            return tuple(pending)

    def _frame_drain_row_from_full(
        self,
        row: Mapping[str, object],
        owner: OwnerView,
        *,
        authenticate: bool,
    ) -> _FrameDrainRow:
        try:
            payload = row["payload"]
            if type(payload) is not bytes:
                raise TypeError("frame payload is invalid")
        except (KeyError, TypeError) as error:
            raise JournalIntegrityError(
                "persisted frame drain row is invalid"
            ) from error
        return self._decode_frame_drain_row(
            row,
            owner,
            len(payload),
            authenticate=authenticate,
        )

    def _decode_frame_row(
        self,
        frame_id: UUID,
        row: Mapping[str, object],
        owner: OwnerView,
        *,
        drain_header_authenticated: bool = False,
    ) -> StagedFrame:
        try:
            drain_row = self._frame_drain_row_from_full(
                row,
                owner,
                authenticate=not drain_header_authenticated,
            )
            if drain_row.frame_id != frame_id:
                raise ValueError("selected frame identity changed")
            payload = self._payload(
                owner,
                "NioIngestFrame",
                row["payload"],
                drain_row.payload_sha256,
                header=_canonical_internal(
                    [
                        str(frame_id),
                        drain_row.source_epoch,
                        drain_row.request_id,
                        drain_row.staged_revision,
                    ]
                ),
            )
            value = load_internal_json(payload, "frame envelope")
            response = _frame_response_from_envelope(value)
            frame = StagedFrame(frame_id, response, drain_row.staged_revision)
            if value != _frame_envelope(frame):
                raise ValueError("frame envelope is not canonical")
            request = response.request
            if (
                request.stream_id != owner.stream_id
                or request.transport is not owner.transport_kind
            ):
                raise ValueError("frame request does not match journal owner")
            if (request.source_epoch, request.request_id) != (
                drain_row.source_epoch,
                drain_row.request_id,
            ):
                raise ValueError("frame columns do not match stored metadata")
            return frame
        except JournalIntegrityError:
            raise
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError("persisted staged frame is invalid") from error

    def _classify_frame_ids(self) -> frozenset[UUID]:
        rows = self._execute(  # type: ignore[attr-defined]
            "SELECT CASE account_id WHEN ? THEN frame_id END AS frame_id "
            "FROM NioIngestFrame LIMIT ?",
            (self.account_id, _FRAME_CLASSIFICATION_LIMIT),
        ).fetchall()
        if len(rows) > _MAX_STAGED_FRAMES:
            raise JournalIntegrityError("staged frame count exceeds the 256 frame cap")

        classified = tuple(self._parse_frame_id(row) for row in rows)
        identities = [stored_id for _, stored_id in classified]
        if len(identities) != len(set(identities)):
            raise JournalIntegrityError("frame_id has multiple textual identities")
        for raw_frame_id, stored_id in classified:
            if raw_frame_id != str(stored_id):
                raise JournalIntegrityError("persisted frame_id is not canonical")
        return frozenset(identities)

    def _frame_row(self, frame_id: UUID) -> sqlite3.Row:
        row = self._execute(  # type: ignore[attr-defined]
            "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (self.account_id, str(frame_id)),
        ).fetchone()
        if row is None:
            raise JournalIntegrityError("classified frame_id row is missing")
        return row

    def _load_frame_with_owner(
        self,
        frame_id: UUID,
        owner: OwnerView,
    ) -> StagedFrame | None:
        if frame_id not in self._classify_frame_ids():
            return None
        return self._decode_frame_row(frame_id, self._frame_row(frame_id), owner)

    def load_frame(self, frame_id: UUID) -> StagedFrame | None:
        if type(frame_id) is not UUID:
            raise TypeError("frame_id must be UUID")
        with self._read():  # type: ignore[attr-defined]
            return self._load_frame_with_owner(frame_id, self.load_owner())

    def list_frames(self, limit: int) -> tuple[StagedFrame, ...]:
        if type(limit) is not int or not 1 <= limit <= 256:
            raise ValueError("frame limit must be an integer from 1 through 256")
        with self._read():  # type: ignore[attr-defined]
            owner = self.load_owner()
            self._classify_frame_ids()
            rows = self._execute(  # type: ignore[attr-defined]
                "SELECT * FROM NioIngestFrame WHERE account_id = ? "
                "ORDER BY staged_revision, source_epoch, request_id, frame_id LIMIT ?",
                (self.account_id, limit),
            ).fetchall()
            return tuple(
                self._decode_frame_row(self._parse_frame_id(row)[1], row, owner)
                for row in rows
            )

    def _write_frame(
        self,
        frame: StagedFrame,
        staged_revision: int,
        owner: OwnerView,
        payload_owner: _Owner,
    ) -> sqlite3.Cursor:
        payload, digest = _frame_payload(frame, staged_revision, payload_owner)
        request = frame.response.request
        drain_header_sha256 = _frame_drain_sha256(
            owner,
            frame_id=frame.frame_id,
            source_epoch=request.source_epoch,
            request_id=request.request_id,
            staged_revision=staged_revision,
            payload_sha256=digest,
            payload_length=len(payload),
            room_materialized_revision=None,
            callbacks_claimed_revision=None,
        )
        return self._execute(  # type: ignore[attr-defined]
            "INSERT INTO NioIngestFrame ("
            "account_id, frame_id, source_epoch, request_id, "
            "staged_revision, payload, payload_sha256, "
            "room_materialized_revision, callbacks_claimed_revision, "
            "drain_header_sha256"
            ") VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (
                self.account_id,
                str(frame.frame_id),
                request.source_epoch,
                request.request_id,
                staged_revision,
                payload,
                digest,
                None,
                None,
                drain_header_sha256,
            ),
        )
