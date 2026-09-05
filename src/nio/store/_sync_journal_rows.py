from __future__ import annotations

import base64
import sqlite3
from hashlib import sha256
from typing import TYPE_CHECKING, Any, Literal, NamedTuple, cast
from uuid import UUID, uuid5

from ..ingest._json import canonical_json, load_internal_json, load_json
from ..ingest.errors import JournalIntegrityError
from ..ingest.hydration import PendingHydration
from ..ingest.model import (
    EventRecord,
    LossRecord,
    RecordKind,
    RecordOrigin,
    SystemOrigin,
    SystemOriginKind,
    TransportKind,
)
from ..ingest.ports import (
    NetworkRequest,
    StagedSourceResponse,
)
from ..ingest.reducer import HydrationIntent, MembershipBaseline, RoomContinuity
from ..ingest.serialization import (
    _ROOM_SNAPSHOT_FIELDS,
    _loss_id,
    _origin_from_dict,
    _origin_to_dict,
    _record_from_dict,
    _room_snapshot_from_dict,
    _room_snapshot_to_dict,
)
from ..ingest.source import MAX_STORED_FRAME_PAYLOAD_BYTES, _classic_cursor_from_json
from ..ingest.state import OwnerView, SourceState, StagedFrame
from ._sync_journal_format import (
    _canonical_internal as _canonical_internal,
)
from ._sync_journal_format import _row as _row
from ._sync_journal_format import _source_header as _source_header
from ._sync_journal_format import _validate_source_cursor as _validate_source_cursor
from ._sync_journal_plan import (
    AuthenticatedWork,
    _decode_prepared_metadata,
    _PreparedWorkMetadata,
)
from ._sync_journal_plan import (
    _canonical_work_plaintext as _canonical_work_plaintext,
)
from ._sync_journal_values import (
    RoomAggregateValue,
    _LocalMembershipIntent,
    _PendingLocalMembership,
)

if TYPE_CHECKING:
    from collections.abc import Mapping
    from contextlib import AbstractContextManager


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
_QUIESCE_FRAME_FIELDS = (*_FRAME_FIELDS, "quiesce_reserved")
_RECOVERY_FRAME_FIELDS = (*_FRAME_FIELDS, "recovery_json")
_FRAME_ENVELOPES = (
    _FRAME_FIELDS,
    _QUIESCE_FRAME_FIELDS,
    _RECOVERY_FRAME_FIELDS,
    (*_RECOVERY_FRAME_FIELDS, "quiesce_reserved"),
)
_PREPARED_FRAME_FIELDS = (
    "prepared_version",
    "request_cursor_json",
    "candidate_cursor_json",
    "source_sha256",
    "compatibility_token",
    "outbound_maintenance",
)
_OUTBOUND_MAINTENANCE_FIELDS = ("version", "operations")
_OUTBOUND_OPERATION_FIELDS = (
    "kind",
    "state",
    "body_json",
    "transaction_id",
    "event_type",
    "context",
)
_OUTBOUND_CLAIM_CONTEXT_FIELDS = ("claims",)
_OUTBOUND_CLAIM_FIELDS = (
    "user_id",
    "device_id",
    "was_wedged",
    "was_waiting",
    "waiting_key_requests",
    "rerequest_events",
)
_OUTBOUND_WAITING_FIELDS = (
    "source_json",
    "sender_user_id",
    "requesting_device_id",
    "request_id",
    "room_id",
    "sender_key",
    "session_id",
    "algorithm",
)
_OUTBOUND_REREQUEST_FIELDS = (
    "source_json",
    "room_id",
    "event_id",
    "sender_user_id",
    "sender_device_id",
    "sender_key",
    "session_id",
    "algorithm",
)
_OUTBOUND_GENERIC_CONTEXT_FIELDS = ("subtype",)
_OUTBOUND_DUMMY_CONTEXT_FIELDS = ("subtype", "rerequest_events")
_OUTBOUND_ROOM_KEY_CONTEXT_FIELDS = (
    "subtype",
    "request_id",
    "session_id",
    "room_id",
    "algorithm",
)
# IngestionConfig caps ordinary staging at 256 frames. Clean shutdown may
# reserve exactly one additional staged source response, while the oldest
# Frame can remain prepared until its Work drains. One further row detects
# corrupt overflow without an unbounded inventory scan.
_MAX_DURABLE_STAGED_FRAMES = 257
_MAX_DURABLE_FRAME_ROWS = _MAX_DURABLE_STAGED_FRAMES + 1
_FRAME_CLASSIFICATION_LIMIT = _MAX_DURABLE_FRAME_ROWS + 1
_MAX_WORK_PAYLOAD_BYTES = 1024 * 1024
_MAX_HELD_WORK_COUNT = 10_000
_MAX_HELD_WORK_CANONICAL_BYTES = 32 * 1024 * 1024
_MAX_TOTAL_WORK_COUNT = 20_000
_MAX_TOTAL_WORK_CANONICAL_BYTES = 64 * 1024 * 1024
_Owner = tuple[str, UUID, TransportKind]


class _DecodedWork(NamedTuple):
    value: EventRecord | LossRecord
    metadata: _PreparedWorkMetadata | None
    plaintext: bytes


class _OutboundOperation(NamedTuple):
    kind: str
    state: str
    body_json: bytes
    transaction_id: str | None
    event_type: str | None
    context: object | None


class _OutboundMaintenance(NamedTuple):
    operations: tuple[_OutboundOperation, ...]


class _PendingOutboundMaintenance(NamedTuple):
    frame_id: UUID
    stream_id: UUID
    transport: TransportKind
    source_epoch: int
    request_id: int
    request_cursor_json: bytes
    operation_index: int
    operation_count: int
    operation: _OutboundOperation


class _PreparedFrameState(NamedTuple):
    request_cursor_json: bytes
    candidate_cursor_json: bytes
    source_sha256: bytes
    compatibility_token: str | None
    outbound_maintenance: _OutboundMaintenance


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
        if set(wrapper) == {"kind", "value"}:
            metadata = None
        elif (
            set(wrapper) == {"kind", "preparation", "value"}
            and type(value) is EventRecord
        ):
            metadata = _decode_prepared_metadata(value, wrapper["preparation"])
        else:
            raise ValueError("work wrapper shape is invalid")
        if plaintext != _canonical_work_plaintext(kind, value, metadata):
            raise ValueError("work plaintext is not canonical")
        identity = value.record_id if isinstance(value, EventRecord) else value.loss_id
        if (
            type(value) not in (EventRecord, LossRecord)
            or work_id != str(UUID(work_id))
            or identity != work_id
            or type(value) is LossRecord
            and _loss_id(stream_id, value) != work_id
        ):
            raise ValueError("work plaintext is not canonical")
        return _DecodedWork(value, metadata, plaintext)
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
    local = value.pending_local_membership
    continuity = value.continuity
    if continuity.gap is not None:
        raise ValueError("this checkpoint persists no recovery gap")
    baseline = continuity.baseline
    if baseline is not None and baseline.membership_event_id is None:
        raise ValueError("unsupported Aggregate baseline")
    if baseline is not None and hydration is not None:
        raise ValueError("baseline and pending hydration are mutually exclusive")
    continuity_payload: dict[str, object] = {
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
    }
    if continuity.last_timeline_event_id is not None:
        continuity_payload["last_timeline_event_id"] = continuity.last_timeline_event_id
    payload: dict[str, object] = {
        "continuity": continuity_payload,
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
    if local is not None:
        payload["pending_local_membership"] = {
            "current_membership": local.current_membership,
            "operation_id": str(local.operation_id),
            "previous_epoch": local.previous_epoch,
            "previous_membership": local.previous_membership,
        }
    if value.room_snapshot is not None:
        payload["room_snapshot"] = _room_snapshot_to_dict(value.room_snapshot)
    return canonical_json(payload)


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
        if intent_kind not in (None, "hydration", "local_membership"):
            raise ValueError("unsupported Aggregate intent kind")
        if type(plaintext) is not bytes:
            raise TypeError("aggregate plaintext must be bytes")
        decoded = load_internal_json(plaintext, "room aggregate plaintext")
        if type(decoded) is not dict or type(decoded.get("continuity")) is not dict:
            raise ValueError("Aggregate and continuity must be objects")
        aggregate = cast("dict[str, object]", decoded)
        fields = tuple(aggregate)
        legacy_fields = (
            "continuity",
            "next_room_sequence",
            "pending_hydration",
            "updated_revision",
        )
        extended_fields = (
            "continuity",
            "next_room_sequence",
            "pending_hydration",
            "room_snapshot",
            "updated_revision",
        )
        local_fields = (
            "continuity",
            "next_room_sequence",
            "pending_hydration",
            "pending_local_membership",
            "updated_revision",
        )
        local_snapshot_fields = (
            "continuity",
            "next_room_sequence",
            "pending_hydration",
            "pending_local_membership",
            "room_snapshot",
            "updated_revision",
        )
        if fields not in (
            legacy_fields,
            extended_fields,
            local_fields,
            local_snapshot_fields,
        ):
            raise ValueError("Aggregate fields are not canonical")
        continuity = cast("dict[str, object]", aggregate["continuity"])
        legacy_continuity_fields = {
            "baseline",
            "gap",
            "hydration_id",
            "membership",
            "membership_epoch",
            "room_id",
        }
        if set(continuity) not in (
            legacy_continuity_fields,
            legacy_continuity_fields | {"last_timeline_event_id"},
        ):
            raise ValueError("Aggregate continuity fields are invalid")
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
            cast("str | None", continuity.get("last_timeline_event_id")),
        )

        pending_value = aggregate["pending_hydration"]
        local_value = aggregate.get("pending_local_membership")
        if intent_kind is None:
            if (
                pending_value is not None
                or hydration_id is not None
                or local_value is not None
            ):
                raise ValueError("NULL Aggregate must have no hydration barrier")
            pending = None
            local = None
        elif intent_kind == "hydration":
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
            if local_value is not None:
                raise ValueError("hydration Aggregate has a local intent")
            local = None
        else:
            if pending_value is not None or hydration_id is not None:
                raise ValueError("local Aggregate has a hydration intent")
            if type(local_value) is not dict or tuple(local_value) != (
                "current_membership",
                "operation_id",
                "previous_epoch",
                "previous_membership",
            ):
                raise ValueError("local Aggregate requires its pending intent")
            local_map = cast("dict[str, object]", local_value)
            local = _LocalMembershipIntent(
                _aggregate_uuid(local_map["operation_id"], "operation_id"),
                cast("str", local_map["previous_membership"]),
                cast("int", local_map["previous_epoch"]),
                cast("str", local_map["current_membership"]),
            )
            pending = None

        snapshot_value = aggregate.get("room_snapshot")
        if fields in (extended_fields, local_snapshot_fields):
            if type(snapshot_value) is not dict:
                raise ValueError("Aggregate room snapshot must be an object")
            snapshot_map = cast("dict[str, object]", snapshot_value)
            if set(snapshot_map) != set(_ROOM_SNAPSHOT_FIELDS):
                raise ValueError("Aggregate room snapshot fields are invalid")
            room_snapshot = _room_snapshot_from_dict(
                {name: snapshot_map[name] for name in _ROOM_SNAPSHOT_FIELDS}
            )
            if room_snapshot.room_id != room_id:
                raise ValueError("Aggregate room snapshot does not match its row")
        else:
            room_snapshot = None

        value = RoomAggregateValue(
            state,
            cast("int", aggregate["next_room_sequence"]),
            cast("int", aggregate["updated_revision"]),
            pending,
            room_snapshot,
            local,
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


def _frame_envelope(
    frame: StagedFrame,
    *,
    quiesce_reserved: bool = False,
) -> dict[str, object]:
    if type(quiesce_reserved) is not bool:
        raise TypeError("quiesce_reserved must be bool")
    envelope: dict[str, object] = {
        "normalization_version": _NORMALIZATION_VERSION,
        "request": _network_request_to_dict(frame.response.request),
        "response_body": _encoded_bytes(frame.response.response_body),
        "source_sha256": _encoded_bytes(frame.response.source_sha256),
    }
    if frame.response.recovery_json is not None:
        envelope["recovery_json"] = _encoded_bytes(frame.response.recovery_json)
    if quiesce_reserved:
        envelope["quiesce_reserved"] = True
    return envelope


def _frame_payload(
    frame: StagedFrame,
    rev: int,
    owner: _Owner,
    *,
    quiesce_reserved: bool = False,
) -> tuple[bytes, bytes]:
    value = _canonical_internal(
        _frame_envelope(frame, quiesce_reserved=quiesce_reserved)
    )
    stored = _row(owner, "NioIngestFrame", value, header=_frame_header(frame, rev))
    if len(stored[0]) > MAX_STORED_FRAME_PAYLOAD_BYTES:
        raise JournalIntegrityError("staged frame envelope exceeds 24 MiB")
    return stored


def _outbound_exact_dict(
    value: object,
    fields: tuple[str, ...],
    label: str,
) -> dict[str, object]:
    if type(value) is not dict or tuple(value) != fields:
        raise ValueError(f"{label} fields are invalid")
    return cast("dict[str, object]", value)


def _outbound_exact_list(value: object, label: str) -> list[object]:
    if type(value) is not list:
        raise ValueError(f"{label} must be an array")
    return cast("list[object]", value)


def _outbound_nonempty_string(value: object, label: str) -> str:
    if type(value) is not str or not value:
        raise ValueError(f"{label} must be a nonempty string")
    return value


def _outbound_context_source(value: object, label: str) -> dict[str, object]:
    source_json = _decoded_bytes(value, f"{label} source")
    if source_json is None or value != _encoded_bytes(source_json):
        raise ValueError(f"{label} source encoding is invalid")
    source = load_json(source_json, f"{label} source")
    if type(source) is not dict or source_json != canonical_json(source):
        raise ValueError(f"{label} source is not a canonical object")
    return cast("dict[str, object]", source)


def _outbound_event_fields(
    value: object,
    *,
    event_type: str,
    sender: str,
    label: str,
) -> tuple[dict[str, object], dict[str, object]]:
    source = _outbound_context_source(value, label)
    content = source.get("content")
    if (
        source.get("type") != event_type
        or source.get("sender") != sender
        or type(content) is not dict
    ):
        raise ValueError(f"{label} source fields disagree")
    return source, cast("dict[str, object]", content)


def _outbound_rerequests(
    value: object,
    *,
    target: tuple[str, str],
    label: str,
) -> tuple[tuple[str, str, str], ...]:
    entries = _outbound_exact_list(value, label)
    identities: list[tuple[str, str, str]] = []
    for entry_value in entries:
        entry = _outbound_exact_dict(
            entry_value,
            _OUTBOUND_REREQUEST_FIELDS,
            f"{label} entry",
        )
        values = {
            name: _outbound_nonempty_string(entry[name], f"{label} {name}")
            for name in _OUTBOUND_REREQUEST_FIELDS[1:]
        }
        if (values["sender_user_id"], values["sender_device_id"]) != target:
            raise ValueError(f"{label} target disagrees")
        source, content = _outbound_event_fields(
            entry["source_json"],
            event_type="m.room.encrypted",
            sender=values["sender_user_id"],
            label=label,
        )
        if (
            source.get("event_id") != values["event_id"]
            or "room_id" in source
            and source.get("room_id") != values["room_id"]
            or content.get("device_id") != values["sender_device_id"]
            or content.get("sender_key") != values["sender_key"]
            or content.get("session_id") != values["session_id"]
            or content.get("algorithm") != values["algorithm"]
        ):
            raise ValueError(f"{label} source fields disagree")
        identities.append((target[0], target[1], values["session_id"]))
    if len(identities) != len(set(identities)):
        raise ValueError(f"{label} entries are duplicated")
    return tuple(identities)


def _outbound_waiting_requests(
    value: object,
    *,
    target: tuple[str, str],
) -> None:
    entries = _outbound_exact_list(value, "waiting key requests")
    request_ids: list[str] = []
    for entry_value in entries:
        entry = _outbound_exact_dict(
            entry_value,
            _OUTBOUND_WAITING_FIELDS,
            "waiting key request",
        )
        values = {
            name: _outbound_nonempty_string(
                entry[name],
                f"waiting key request {name}",
            )
            for name in _OUTBOUND_WAITING_FIELDS[1:]
        }
        if (values["sender_user_id"], values["requesting_device_id"]) != target:
            raise ValueError("waiting key request target disagrees")
        _, content = _outbound_event_fields(
            entry["source_json"],
            event_type="m.room_key_request",
            sender=values["sender_user_id"],
            label="waiting key request",
        )
        body = content.get("body")
        if (
            content.get("action") != "request"
            or content.get("requesting_device_id") != values["requesting_device_id"]
            or content.get("request_id") != values["request_id"]
            or type(body) is not dict
        ):
            raise ValueError("waiting key request source fields disagree")
        body_fields = cast("dict[str, object]", body)
        if any(
            body_fields.get(name) != values[name]
            for name in ("room_id", "sender_key", "session_id", "algorithm")
        ):
            raise ValueError("waiting key request body fields disagree")
        request_ids.append(values["request_id"])
    if len(request_ids) != len(set(request_ids)):
        raise ValueError("waiting key requests are duplicated")


def _outbound_upload_body(body: dict[str, object]) -> None:
    allowed = {"device_keys", "fallback_keys", "one_time_keys"}
    if (
        "one_time_keys" not in body
        or not set(body) <= allowed
        or any(type(value) is not dict or not value for value in body.values())
    ):
        raise ValueError("key-upload body is invalid")


def _outbound_query_body(body: dict[str, object]) -> None:
    if tuple(body) != ("device_keys",):
        raise ValueError("key-query body fields are invalid")
    users = body["device_keys"]
    if type(users) is not dict or not users:
        raise ValueError("key-query targets are invalid")
    if any(
        type(user_id) is not str
        or not user_id
        or type(device_ids) is not list
        or device_ids
        for user_id, device_ids in users.items()
    ):
        raise ValueError("key-query targets are invalid")


def _outbound_claim_targets(
    body: dict[str, object],
) -> tuple[tuple[str, str], ...]:
    if tuple(body) != ("one_time_keys",):
        raise ValueError("key-claim body fields are invalid")
    users = body["one_time_keys"]
    if type(users) is not dict or not users:
        raise ValueError("key-claim targets are invalid")
    targets: list[tuple[str, str]] = []
    for user_id, devices in users.items():
        user = _outbound_nonempty_string(user_id, "claim user ID")
        if type(devices) is not dict or not devices:
            raise ValueError("key-claim devices are invalid")
        for device_id, key_type in devices.items():
            device = _outbound_nonempty_string(device_id, "claim device ID")
            if type(key_type) is not str or key_type != "signed_curve25519":
                raise ValueError("key-claim key type is invalid")
            targets.append((user, device))
    return tuple(targets)


def _outbound_claim_context(
    value: object,
    *,
    body_targets: tuple[tuple[str, str], ...],
    rerequest_owners: set[tuple[str, str]],
) -> None:
    context = _outbound_exact_dict(
        value,
        _OUTBOUND_CLAIM_CONTEXT_FIELDS,
        "key-claim context",
    )
    claims = _outbound_exact_list(context["claims"], "key claims")
    targets: list[tuple[str, str]] = []
    for claim_value in claims:
        claim = _outbound_exact_dict(
            claim_value,
            _OUTBOUND_CLAIM_FIELDS,
            "key claim",
        )
        target = (
            _outbound_nonempty_string(claim["user_id"], "claim user ID"),
            _outbound_nonempty_string(claim["device_id"], "claim device ID"),
        )
        if (
            type(claim["was_wedged"]) is not bool
            or type(claim["was_waiting"]) is not bool
        ):
            raise ValueError("key-claim flags are invalid")
        if claim["was_wedged"] is not True and claim["was_waiting"] is not True:
            raise ValueError("key claim has no preparation reason")
        _outbound_waiting_requests(
            claim["waiting_key_requests"],
            target=target,
        )
        waiting = cast("list[object]", claim["waiting_key_requests"])
        if waiting and claim["was_waiting"] is not True:
            raise ValueError("waiting key requests lack a waiting claim")
        rerequests = _outbound_rerequests(
            claim["rerequest_events"],
            target=target,
            label="claim rerequest",
        )
        if rerequests and claim["was_wedged"] is not True:
            raise ValueError("claim rerequests lack a wedged target")
        if rerequests:
            if target in rerequest_owners:
                raise ValueError("rerequest target has multiple owners")
            rerequest_owners.add(target)
        targets.append(target)
    if (
        tuple(targets) != body_targets
        or targets != sorted(targets)
        or len(targets) != len(set(targets))
    ):
        raise ValueError("key-claim context targets disagree with body")


def _outbound_to_device_body(
    body: dict[str, object],
) -> tuple[str, str, dict[str, object]]:
    if tuple(body) != ("messages",):
        raise ValueError("to-device body fields are invalid")
    messages = body["messages"]
    if type(messages) is not dict or len(messages) != 1:
        raise ValueError("to-device body must have one recipient")
    recipient, devices = next(iter(messages.items()))
    recipient_id = _outbound_nonempty_string(recipient, "to-device recipient")
    if type(devices) is not dict or len(devices) != 1:
        raise ValueError("to-device body must have one device")
    device, content = next(iter(devices.items()))
    device_id = _outbound_nonempty_string(device, "to-device device")
    if type(content) is not dict:
        raise ValueError("to-device content must be an object")
    return recipient_id, device_id, cast("dict[str, object]", content)


def _outbound_dummy_content(content: dict[str, object]) -> None:
    if tuple(content) != ("algorithm", "ciphertext", "sender_key"):
        raise ValueError("dummy ciphertext fields are invalid")
    _outbound_nonempty_string(content["algorithm"], "dummy algorithm")
    _outbound_nonempty_string(content["sender_key"], "dummy sender key")
    ciphertext = content["ciphertext"]
    if type(ciphertext) is not dict or not ciphertext:
        raise ValueError("dummy ciphertext is invalid")
    for curve_key, message_value in ciphertext.items():
        _outbound_nonempty_string(curve_key, "dummy recipient key")
        message = _outbound_exact_dict(
            message_value,
            ("body", "type"),
            "dummy ciphertext",
        )
        _outbound_nonempty_string(message["body"], "dummy ciphertext body")
        if type(message["type"]) is not int or message["type"] not in (0, 1):
            raise ValueError("dummy ciphertext type is invalid")


def _outbound_room_key_context(
    value: object,
    *,
    event_type: str,
    content: dict[str, object],
) -> None:
    context = _outbound_exact_dict(
        value,
        _OUTBOUND_ROOM_KEY_CONTEXT_FIELDS,
        "room-key-request context",
    )
    if context["subtype"] != "room_key_request" or event_type != "m.room_key_request":
        raise ValueError("room-key-request type is invalid")
    values = {
        name: _outbound_nonempty_string(
            context[name],
            f"room-key-request {name}",
        )
        for name in _OUTBOUND_ROOM_KEY_CONTEXT_FIELDS[1:]
    }
    body = content.get("body")
    if (
        tuple(content) != ("action", "body", "request_id", "requesting_device_id")
        or content.get("action") != "request"
        or content.get("request_id") != values["request_id"]
        or type(content.get("requesting_device_id")) is not str
        or not content["requesting_device_id"]
        or type(body) is not dict
    ):
        raise ValueError("room-key-request content is invalid")
    body_fields = cast("dict[str, object]", body)
    if (
        tuple(body_fields) != ("algorithm", "room_id", "sender_key", "session_id")
        or body_fields.get("algorithm") != values["algorithm"]
        or body_fields.get("room_id") != values["room_id"]
        or body_fields.get("session_id") != values["session_id"]
        or type(body_fields.get("sender_key")) is not str
        or not body_fields["sender_key"]
    ):
        raise ValueError("room-key-request body is invalid")


def _outbound_to_device_context(
    operation: _OutboundOperation,
    *,
    recipient: str,
    device: str,
    content: dict[str, object],
    rerequest_owners: set[tuple[str, str]],
    dummy_targets: set[tuple[str, str]],
) -> None:
    event_type = _outbound_nonempty_string(
        operation.event_type,
        "to-device event type",
    )
    context = operation.context
    if type(context) is not dict:
        raise ValueError("pending to-device context must be an object")
    subtype = context.get("subtype")
    target = (recipient, device)
    if subtype == "generic":
        _outbound_exact_dict(
            context,
            _OUTBOUND_GENERIC_CONTEXT_FIELDS,
            "generic to-device context",
        )
        return
    if subtype == "dummy":
        context = _outbound_exact_dict(
            context,
            _OUTBOUND_DUMMY_CONTEXT_FIELDS,
            "dummy to-device context",
        )
        if event_type != "m.room.encrypted":
            raise ValueError("dummy to-device event type is invalid")
        _outbound_dummy_content(content)
        rerequests = _outbound_rerequests(
            context["rerequest_events"],
            target=target,
            label="dummy rerequest",
        )
        first_dummy = target not in dummy_targets
        dummy_targets.add(target)
        if first_dummy and target in rerequest_owners:
            raise ValueError("claim rerequests conflict with first dummy owner")
        if rerequests and not first_dummy:
            raise ValueError("rerequest target has multiple owners")
        if rerequests:
            rerequest_owners.add(target)
        return
    if subtype == "room_key_request":
        _outbound_room_key_context(
            context,
            event_type=event_type,
            content=content,
        )
        return
    raise ValueError("to-device context subtype is invalid")


def _validate_outbound_maintenance(
    maintenance: _OutboundMaintenance,
    *,
    frame_id: UUID,
) -> None:
    if type(frame_id) is not UUID:
        raise TypeError("frame_id must be UUID")
    if (
        type(maintenance) is not _OutboundMaintenance
        or type(maintenance.operations) is not tuple
    ):
        raise TypeError("outbound maintenance plan is invalid")
    ranks = {"key_upload": 0, "key_query": 1, "key_claim": 2, "to_device": 3}
    prior_rank = -1
    singleton_kinds: set[str] = set()
    pending_seen = False
    rerequest_owners: set[tuple[str, str]] = set()
    dummy_targets: set[tuple[str, str]] = set()
    for index, operation in enumerate(maintenance.operations):
        if (
            type(operation) is not _OutboundOperation
            or type(operation.kind) is not str
            or operation.kind not in ranks
            or type(operation.state) is not str
            or operation.state not in {"pending", "settled"}
            or type(operation.body_json) is not bytes
            or operation.transaction_id is not None
            and type(operation.transaction_id) is not str
            or operation.event_type is not None
            and type(operation.event_type) is not str
        ):
            raise TypeError("outbound maintenance operation is invalid")
        rank = ranks[operation.kind]
        if rank < prior_rank:
            raise ValueError("outbound maintenance operation order is invalid")
        prior_rank = rank
        if operation.kind != "to_device":
            if operation.kind in singleton_kinds:
                raise ValueError("outbound maintenance singleton is duplicated")
            singleton_kinds.add(operation.kind)
        if operation.state == "pending":
            pending_seen = True
        elif pending_seen:
            raise ValueError("outbound maintenance states are not a settled prefix")
        if operation.state == "settled" and operation.context is not None:
            raise ValueError("settled operation context must be null")

        body_value = load_json(operation.body_json, "outbound operation body")
        if type(body_value) is not dict or operation.body_json != canonical_json(
            body_value
        ):
            raise ValueError("outbound operation body is not canonical")
        body = cast("dict[str, object]", body_value)
        if operation.kind != "to_device":
            if operation.transaction_id is not None or operation.event_type is not None:
                raise ValueError("non-to-device operation has transport identity")
            if operation.kind in {"key_upload", "key_query"} and (
                operation.context is not None
            ):
                raise ValueError("upload/query operation context must be null")

        if operation.kind == "key_upload":
            _outbound_upload_body(body)
        elif operation.kind == "key_query":
            _outbound_query_body(body)
        elif operation.kind == "key_claim":
            claim_targets = _outbound_claim_targets(body)
            if operation.state == "pending":
                _outbound_claim_context(
                    operation.context,
                    body_targets=claim_targets,
                    rerequest_owners=rerequest_owners,
                )
        else:
            recipient, device, content = _outbound_to_device_body(body)
            expected_transaction_id = str(
                uuid5(
                    frame_id,
                    "nio.ingest.outbound-maintenance.v1:"
                    f"to-device:{index}:{sha256(operation.body_json).hexdigest()}",
                )
            )
            if operation.transaction_id != expected_transaction_id:
                raise ValueError("to-device transaction ID is invalid")
            _outbound_nonempty_string(
                operation.event_type,
                "to-device event type",
            )
            if operation.state == "pending":
                _outbound_to_device_context(
                    operation,
                    recipient=recipient,
                    device=device,
                    content=content,
                    rerequest_owners=rerequest_owners,
                    dummy_targets=dummy_targets,
                )


def _outbound_maintenance_to_dict(
    maintenance: _OutboundMaintenance,
    *,
    frame_id: UUID,
) -> dict[str, object]:
    _validate_outbound_maintenance(maintenance, frame_id=frame_id)
    return {
        "version": 1,
        "operations": [
            {
                "kind": operation.kind,
                "state": operation.state,
                "body_json": _encoded_bytes(operation.body_json),
                "transaction_id": operation.transaction_id,
                "event_type": operation.event_type,
                "context": operation.context,
            }
            for operation in maintenance.operations
        ],
    }


def _outbound_maintenance_from_dict(
    value: object,
    *,
    frame_id: UUID,
) -> _OutboundMaintenance:
    envelope = _outbound_exact_dict(
        value,
        _OUTBOUND_MAINTENANCE_FIELDS,
        "outbound maintenance",
    )
    if type(envelope["version"]) is not int or envelope["version"] != 1:
        raise ValueError("outbound maintenance version is invalid")
    operation_values = _outbound_exact_list(
        envelope["operations"],
        "outbound maintenance operations",
    )
    operations: list[_OutboundOperation] = []
    for operation_value in operation_values:
        operation = _outbound_exact_dict(
            operation_value,
            _OUTBOUND_OPERATION_FIELDS,
            "outbound operation",
        )
        body = _decoded_bytes(operation["body_json"], "operation body")
        if body is None:
            raise ValueError("outbound operation body is invalid")
        kind = operation["kind"]
        state = operation["state"]
        transaction_id = operation["transaction_id"]
        event_type = operation["event_type"]
        if type(kind) is not str or type(state) is not str:
            raise ValueError("outbound operation identity is invalid")
        if transaction_id is not None and type(transaction_id) is not str:
            raise ValueError("outbound transaction ID is invalid")
        if event_type is not None and type(event_type) is not str:
            raise ValueError("outbound event type is invalid")
        operations.append(
            _OutboundOperation(
                kind,
                state,
                body,
                transaction_id,
                event_type,
                operation["context"],
            )
        )
    maintenance = _OutboundMaintenance(tuple(operations))
    if value != _outbound_maintenance_to_dict(maintenance, frame_id=frame_id):
        raise ValueError("outbound maintenance is not canonical")
    return maintenance


def _prepared_frame_envelope(
    state: _PreparedFrameState,
    *,
    frame_id: UUID,
) -> dict[str, object]:
    if type(state) is not _PreparedFrameState:
        raise TypeError("prepared frame state is invalid")
    return {
        "prepared_version": 1,
        "request_cursor_json": _encoded_bytes(state.request_cursor_json),
        "candidate_cursor_json": _encoded_bytes(state.candidate_cursor_json),
        "source_sha256": _encoded_bytes(state.source_sha256),
        "compatibility_token": state.compatibility_token,
        "outbound_maintenance": _outbound_maintenance_to_dict(
            state.outbound_maintenance,
            frame_id=frame_id,
        ),
    }


def _prepared_frame_state_from_envelope(
    value: object,
    *,
    owner: OwnerView,
    frame_id: UUID,
    source_epoch: int,
    request_id: int,
) -> _PreparedFrameState:
    if type(value) is not dict or tuple(value) != _PREPARED_FRAME_FIELDS:
        raise ValueError("prepared frame fields are invalid")
    if type(value["prepared_version"]) is not int or value["prepared_version"] != 1:
        raise ValueError("prepared frame version is invalid")
    request_cursor = _decoded_bytes(value["request_cursor_json"], "request cursor")
    candidate_cursor = _decoded_bytes(
        value["candidate_cursor_json"], "candidate cursor"
    )
    source_sha256 = _decoded_bytes(value["source_sha256"], "source digest")
    assert request_cursor is not None
    assert candidate_cursor is not None
    assert source_sha256 is not None
    compatibility_token = value["compatibility_token"]
    if compatibility_token is not None and type(compatibility_token) is not str:
        raise ValueError("prepared compatibility token is invalid")
    if len(source_sha256) != 32 or frame_id != uuid5(
        owner.stream_id,
        f"{source_epoch}:{request_id}:{source_sha256.hex()}",
    ):
        raise ValueError("prepared frame source identity is invalid")
    _validate_source_cursor(owner.transport_kind, request_cursor)
    _validate_source_cursor(owner.transport_kind, candidate_cursor)
    if owner.transport_kind is TransportKind.CLASSIC:
        classic_cursor = _classic_cursor_from_json(candidate_cursor)
        if (
            type(classic_cursor.next_batch) is not str
            or not classic_cursor.next_batch
            or compatibility_token != classic_cursor.next_batch
        ):
            raise ValueError("prepared Classic compatibility token is invalid")
    elif compatibility_token is not None:
        raise ValueError("prepared Sliding compatibility token must be null")
    state = _PreparedFrameState(
        request_cursor,
        candidate_cursor,
        source_sha256,
        compatibility_token,
        _outbound_maintenance_from_dict(
            value["outbound_maintenance"],
            frame_id=frame_id,
        ),
    )
    if value != _prepared_frame_envelope(state, frame_id=frame_id):
        raise ValueError("prepared frame envelope is not canonical")
    return state


def _prepared_frame_payload(
    *,
    owner: _Owner,
    frame_id: UUID,
    source_epoch: int,
    request_id: int,
    staged_revision: int,
    request_cursor_json: bytes,
    candidate_cursor_json: bytes,
    source_sha256: bytes,
    compatibility_token: str | None,
    outbound_maintenance: _OutboundMaintenance,
) -> tuple[bytes, bytes]:
    value = _canonical_internal(
        _prepared_frame_envelope(
            _PreparedFrameState(
                request_cursor_json,
                candidate_cursor_json,
                source_sha256,
                compatibility_token,
                outbound_maintenance,
            ),
            frame_id=frame_id,
        )
    )
    stored = _row(
        owner,
        "NioIngestFrame",
        value,
        header=_canonical_internal(
            [str(frame_id), source_epoch, request_id, staged_revision]
        ),
    )
    if len(stored[0]) > MAX_STORED_FRAME_PAYLOAD_BYTES:
        raise JournalIntegrityError("prepared frame envelope exceeds 24 MiB")
    return stored


def _frame_response_from_envelope(
    value: object,
) -> tuple[StagedSourceResponse, bool]:
    if type(value) is not dict or tuple(value) not in _FRAME_ENVELOPES:
        raise ValueError("frame authenticated envelope is invalid")
    quiesce_reserved = "quiesce_reserved" in value
    if quiesce_reserved and value["quiesce_reserved"] is not True:
        raise ValueError("frame quiesce reservation is invalid")
    if type(value["normalization_version"]) is not int or (
        value["normalization_version"] != _NORMALIZATION_VERSION
    ):
        raise ValueError("frame normalization version is invalid")
    body = _decoded_bytes(value["response_body"], "response body")
    digest = _decoded_bytes(value["source_sha256"], "source digest")
    assert body is not None
    assert digest is not None
    return (
        StagedSourceResponse(
            _network_request_from_dict(value["request"]),
            body,
            digest,
            _decoded_bytes(
                value.get("recovery_json"), "history capture", optional=True
            ),
        ),
        quiesce_reserved,
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


class _Task3WorkRow(NamedTuple):
    account_id: str
    work_id: str
    kind: Literal["event", "loss"]
    status: Literal["ready", "held"]
    raw_frame_id: str
    room_id: str | None
    membership_epoch: int | None
    room_sequence: int | None
    ready_revision: int | None
    ready_ordinal: int | None
    created_revision: int
    payload: bytes
    digest: bytes


class JournalRows:
    account_id: str
    device_id: str

    def __init__(self) -> None:
        self._frame_cache: (
            tuple[
                _Owner,
                tuple[tuple[str, object], ...],
                tuple[StagedFrame | _PreparedFrameState, bool],
            ]
            | None
        ) = None
        self._work_cache: (
            tuple[
                tuple[str, UUID, TransportKind],
                _Task3WorkRow,
                AuthenticatedWork,
            ]
            | None
        ) = None
        self._room_aggregate_cache: (
            tuple[
                tuple[str, UUID, TransportKind],
                tuple[object, ...],
                RoomAggregateValue,
            ]
            | None
        ) = None

    if TYPE_CHECKING:

        def _execute(
            self,
            statement: str,
            parameters: tuple[object, ...] = (),
        ) -> sqlite3.Cursor: ...

        def _read(self) -> AbstractContextManager[None]: ...

    def _payload(self, o: OwnerView, *args: Any, **kwargs: Any) -> Any:
        return _row((self.account_id, o.stream_id, o.transport_kind), *args, **kwargs)

    def _meta(self) -> sqlite3.Row:
        rows = self._execute("SELECT * FROM NioIngestMeta LIMIT 2").fetchall()
        if len(rows) != 1:
            raise JournalIntegrityError(
                "ingestion-v1 marker row cardinality is not one"
            )
        return rows[0]

    def _decode_owner_row(self, row: Mapping[str, object]) -> OwnerView:
        try:
            owner = OwnerView(
                cast("str", row["account_id"]),
                cast("str", row["device_id"]),
                cast("int", row["schema_version"]),
                UUID(cast("str", row["stream_id"])),
                UUID(cast("str", row["consumer_generation"])),
                TransportKind(cast("str", row["transport_kind"])),
                cast("int", row["revision"]),
                UUID(cast("str", row["writer_epoch"])),
                cast("int", row["next_source_epoch"]),
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
        with self._read():
            return self._decode_owner_row(cast("Mapping[str, object]", self._meta()))

    def _load_stage_snapshot(self) -> tuple[OwnerView, SourceState]:
        rows = self._execute(
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
                cast("int", row["source_epoch"]),
                owner.transport_kind,
                b"",
                cast("int", row["next_request_id"]),
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
        with self._read():
            owner = self.load_owner()
            rows = self._execute("SELECT * FROM NioIngestSourceState").fetchall()
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
        return self._execute(
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
        rows = self._execute(
            "SELECT account_id, frame_id, source_epoch, request_id, "
            "staged_revision, payload_sha256, "
            "LENGTH(payload) AS payload_length, "
            "room_materialized_revision, callbacks_claimed_revision, "
            "drain_header_sha256 "
            "FROM NioIngestFrame LIMIT ?",
            (_FRAME_CLASSIFICATION_LIMIT,),
        ).fetchall()
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
        if (
            sum(row.room_materialized_revision is None for row in decoded)
            > _MAX_DURABLE_STAGED_FRAMES
        ):
            raise JournalIntegrityError("staged frame count exceeds the 257 frame cap")
        if len(decoded) > _MAX_DURABLE_FRAME_ROWS:
            raise JournalIntegrityError("Frame row count exceeds the 258 row cap")
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

    def _validate_task3_work_row(
        self,
        owner: OwnerView,
        row: tuple[object, ...],
    ) -> _Task3WorkRow:
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
        return cast("_Task3WorkRow", row)

    def _decode_task3_work_row(
        self,
        owner: OwnerView,
        row: tuple[object, ...],
    ) -> AuthenticatedWork:
        stored = self._validate_task3_work_row(owner, row)
        authentication = (
            owner.account_id,
            owner.stream_id,
            owner.transport_kind,
        )
        cached = self._work_cache
        if cached is not None and cached[:2] == (authentication, stored):
            return cached[2]
        (
            _account_id,
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
        ) = stored
        local_header_invalid = False
        try:
            plaintext = self._payload(
                owner,
                "NioIngestWork",
                payload,
                digest,
                header=_canonical_internal(stored[1:11]),
            )
            decoded = _decode_work_plaintext(
                owner.stream_id,
                work_id,
                kind,
                plaintext,
            )
            value = decoded.value
            origin = value.origin
            if type(origin) is RecordOrigin:
                valid_origin = (
                    origin.transport is owner.transport_kind
                    and min(
                        origin.source_epoch,
                        origin.request_id,
                        origin.frame_index,
                    )
                    >= 0
                )
            elif type(origin) is SystemOrigin:
                valid_origin = (
                    type(value) is EventRecord
                    and origin.kind is SystemOriginKind.MEMBERSHIP_CHANGE
                    and value.kind is RecordKind.ROOM_LIFECYCLE
                    and status == "ready"
                    and raw_frame_id == str(origin.operation_id)
                    and decoded.metadata is None
                )
                local_header_invalid = valid_origin and (
                    ready_revision != created_revision or ready_ordinal != 0
                )
            else:
                valid_origin = False
            if not valid_origin:
                raise ValueError("Work origin is invalid")
            if type(value) is LossRecord:
                if (
                    status != "ready"
                    or not value.room_id
                    or value.membership_epoch < 0
                    or (value.room_id, value.membership_epoch, None)
                    != (room_id, membership_epoch, room_sequence)
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
                if status == "held":
                    valid = value.kind in room_kinds and room_value == (
                        room_id,
                        membership_epoch,
                        room_sequence,
                    )
                elif value.room_id is None:
                    valid = value.kind in (
                        RecordKind.GLOBAL_ACCOUNT_DATA,
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
                        and room_value == (room_id, membership_epoch, room_sequence)
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
        if local_header_invalid:
            raise JournalIntegrityError("local READY Work header is invalid")
        authenticated = AuthenticatedWork(
            value,
            status,
            len(payload),
            decoded.metadata,
            decoded.plaintext,
            UUID(raw_frame_id),
            created_revision,
        )
        self._work_cache = authentication, stored, authenticated
        return authenticated

    def _load_delivery_work(
        self,
        owner: OwnerView,
        work_id: str | None,
    ) -> tuple[tuple[object, ...], AuthenticatedWork] | None:
        foreign = self._execute(
            "SELECT account_id FROM NioIngestWork WHERE account_id < ? "
            "UNION ALL SELECT account_id FROM NioIngestWork WHERE account_id > ? "
            "LIMIT 1",
            (self.account_id, self.account_id),
        ).fetchone()
        if foreign is not None:
            raise JournalIntegrityError("invalid Work row")
        columns = (
            "account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload, payload_sha256 "
        )
        if work_id is None:
            row = self._execute(
                f"SELECT {columns}FROM NioIngestWork "
                "WHERE account_id = ? AND status = 'ready' "
                "ORDER BY ready_revision, ready_ordinal, work_id LIMIT 1",
                (self.account_id,),
            ).fetchone()
        else:
            row = self._execute(
                f"SELECT {columns}FROM NioIngestWork "
                "WHERE account_id = ? AND work_id = ? LIMIT 1",
                (self.account_id, work_id),
            ).fetchone()
        if row is None:
            return None
        stored = tuple(row)
        return stored, self._decode_task3_work_row(owner, stored)

    def _load_delivery_work_count(self) -> int:
        row = self._execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()
        if (
            row is None
            or len(row) != 1
            or type(row[0]) is not int
            or not 0 <= row[0] <= _MAX_TOTAL_WORK_COUNT
        ):
            raise JournalIntegrityError("total Work exceeds immutable capacity")
        return row[0]

    def _load_task3_work_inventory(
        self,
        owner: OwnerView,
    ) -> _Task3WorkInventory:
        cursor = self._execute(
            "SELECT account_id, work_id, kind, status, frame_id, room_id, "
            "membership_epoch, room_sequence, ready_revision, ready_ordinal, "
            "created_revision, payload, payload_sha256 "
            "FROM NioIngestWork LIMIT 20001",
        )
        storage_rows: list[tuple[object, ...]] = []
        payload_bytes = 0
        while (fetched := cursor.fetchone()) is not None:
            row = tuple(fetched)
            self._validate_task3_work_row(owner, row)
            storage_rows.append(row)
            payload_bytes += len(cast("bytes", row[11]))
            if (
                len(storage_rows) > _MAX_TOTAL_WORK_COUNT
                or payload_bytes > _MAX_TOTAL_WORK_CANONICAL_BYTES
            ):
                break

        storage_rows.sort(key=lambda row: cast("str", row[1]))
        work = tuple(self._decode_task3_work_row(owner, row) for row in storage_rows)
        held = tuple(item for item in work if item.status == "held")
        if (
            len(held) > _MAX_HELD_WORK_COUNT
            or sum(item.canonical_size for item in held)
            > _MAX_HELD_WORK_CANONICAL_BYTES
        ):
            raise JournalIntegrityError("HELD Work exceeds immutable capacity")
        if (
            len(storage_rows) > _MAX_TOTAL_WORK_COUNT
            or payload_bytes > _MAX_TOTAL_WORK_CANONICAL_BYTES
        ):
            raise JournalIntegrityError("total Work exceeds immutable capacity")
        return _Task3WorkInventory(tuple(storage_rows), work)

    def _load_room_aggregate(
        self,
        owner: OwnerView,
        room_id: str,
    ) -> tuple[tuple[object, ...], RoomAggregateValue] | None:
        row = self._execute(
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
                or kind not in (None, "hydration", "local_membership")
                or type(payload) is not bytes
                or not payload
                or type(digest) is not bytes
                or len(digest) != 32
            ):
                raise ValueError("Aggregate columns are invalid")
            authentication = (
                owner.account_id,
                owner.stream_id,
                owner.transport_kind,
            )
            cached = self._room_aggregate_cache
            if cached is not None and cached[:2] == (authentication, stored):
                return stored, cached[2]
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
            if value.room_snapshot is not None and (
                value.room_snapshot.own_user_id != owner.account_id
            ):
                raise JournalIntegrityError(
                    "Aggregate snapshot owner does not match journal"
                )
            if value.pending_hydration is not None and (
                value.pending_hydration.origin.transport is not owner.transport_kind
            ):
                raise ValueError("Aggregate origin transport does not match owner")
            self._room_aggregate_cache = authentication, stored, value
            return stored, value
        except JournalIntegrityError:
            raise
        except (AttributeError, TypeError, ValueError) as error:
            raise JournalIntegrityError("invalid room Aggregate row") from error

    def _load_pending_local_membership_intents(
        self,
        *,
        limit: int,
    ) -> tuple[_PendingLocalMembership, ...]:
        if type(limit) is not int:
            raise TypeError("limit must be int")
        if not 1 <= limit <= 2:
            raise ValueError("limit must be between 1 and 2")
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            rows = self._execute(
                "SELECT account_id, room_id, updated_revision, intent_kind, "
                "payload, payload_sha256 FROM NioIngestRoomAggregate "
                "WHERE account_id = ? AND intent_kind = 'local_membership' "
                "ORDER BY room_id LIMIT ?",
                (self.account_id, limit),
            ).fetchall()
            pending: list[_PendingLocalMembership] = []
            for row in rows:
                room_id = row["room_id"]
                loaded = self._load_room_aggregate(owner, room_id)
                if loaded is None or tuple(row) != loaded[0]:
                    raise JournalIntegrityError(
                        "local membership candidate snapshot changed"
                    )
                value = loaded[1]
                intent = value.pending_local_membership
                if intent is None:
                    raise JournalIntegrityError(
                        "local membership candidate has no intent"
                    )
                pending.append(
                    _PendingLocalMembership(room_id, value.continuity, intent)
                )
            return tuple(pending)

    def load_pending_hydrations(self, *, limit: int) -> tuple[PendingHydration, ...]:
        if type(limit) is not int:
            raise TypeError("limit must be int")
        if not 1 <= limit <= 2:
            raise ValueError("limit must be between 1 and 2")
        with self._read():
            owner = self._decode_owner_row(cast("Mapping[str, object]", self._meta()))
            rows = self._execute(
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

    def _decode_frame_state_with_reservation(
        self,
        frame_id: UUID,
        row: Mapping[str, object],
        owner: OwnerView,
        *,
        drain_header_authenticated: bool = False,
    ) -> tuple[StagedFrame | _PreparedFrameState, bool]:
        try:
            drain_row = self._frame_drain_row_from_full(
                row,
                owner,
                authenticate=not drain_header_authenticated,
            )
            if drain_row.frame_id != frame_id:
                raise ValueError("selected frame identity changed")
            authentication = owner.account_id, owner.stream_id, owner.transport_kind
            stored = tuple((key, row[key]) for key in row.keys())
            cached = self._frame_cache
            if cached is not None and cached[:2] == (authentication, stored):
                return cached[2]
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
            if type(value) is dict and tuple(value) in _FRAME_ENVELOPES:
                response, quiesce_reserved = _frame_response_from_envelope(value)
                frame = StagedFrame(frame_id, response, drain_row.staged_revision)
                if value != _frame_envelope(
                    frame,
                    quiesce_reserved=quiesce_reserved,
                ):
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
                self._frame_cache = authentication, stored, (frame, quiesce_reserved)
                return frame, quiesce_reserved
            if drain_row.room_materialized_revision is None:
                raise ValueError("prepared frame has no materialized revision")
            prepared = _prepared_frame_state_from_envelope(
                value,
                owner=owner,
                frame_id=frame_id,
                source_epoch=drain_row.source_epoch,
                request_id=drain_row.request_id,
            )
            self._frame_cache = (
                (authentication, stored, (prepared, False))
                if not prepared.outbound_maintenance.operations
                else None
            )
            return prepared, False
        except JournalIntegrityError:
            raise
        except (AttributeError, KeyError, TypeError, ValueError) as error:
            raise JournalIntegrityError("persisted Frame is invalid") from error

    def _decode_frame_state(
        self,
        frame_id: UUID,
        row: Mapping[str, object],
        owner: OwnerView,
        *,
        drain_header_authenticated: bool = False,
    ) -> StagedFrame | _PreparedFrameState:
        return self._decode_frame_state_with_reservation(
            frame_id,
            row,
            owner,
            drain_header_authenticated=drain_header_authenticated,
        )[0]

    def _decode_frame_row(
        self,
        frame_id: UUID,
        row: Mapping[str, object],
        owner: OwnerView,
        *,
        drain_header_authenticated: bool = False,
    ) -> StagedFrame:
        value = self._decode_frame_state(
            frame_id,
            row,
            owner,
            drain_header_authenticated=drain_header_authenticated,
        )
        if type(value) is not StagedFrame:
            raise JournalIntegrityError("persisted Frame is not staged")
        return value

    def _classify_frame_ids(self, owner: OwnerView) -> frozenset[UUID]:
        rows = self._execute(
            "SELECT CASE account_id WHEN ? THEN frame_id END AS frame_id "
            "FROM NioIngestFrame LIMIT ?",
            (self.account_id, _FRAME_CLASSIFICATION_LIMIT),
        ).fetchall()
        classified = tuple(self._parse_frame_id(row) for row in rows)
        identities = [stored_id for _, stored_id in classified]
        if len(identities) != len(set(identities)):
            raise JournalIntegrityError("frame_id has multiple textual identities")
        for raw_frame_id, stored_id in classified:
            if raw_frame_id != str(stored_id):
                raise JournalIntegrityError("persisted frame_id is not canonical")
        if len(classified) > _MAX_DURABLE_STAGED_FRAMES:
            headers = self._load_authenticated_frame_headers(owner)
            if {header.frame_id for header in headers} != set(identities):
                raise JournalIntegrityError("authenticated Frame inventory changed")
        return frozenset(identities)

    def _frame_row(self, frame_id: UUID) -> sqlite3.Row:
        row = self._execute(
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
        if frame_id not in self._classify_frame_ids(owner):
            return None
        value = self._decode_frame_state(
            frame_id,
            cast("Mapping[str, object]", self._frame_row(frame_id)),
            owner,
        )
        return value if type(value) is StagedFrame else None

    def _load_prepared_frame_with_owner(
        self,
        frame_id: UUID,
        owner: OwnerView,
    ) -> _PreparedFrameState | None:
        if frame_id not in self._classify_frame_ids(owner):
            return None
        value = self._decode_frame_state(
            frame_id,
            cast("Mapping[str, object]", self._frame_row(frame_id)),
            owner,
        )
        return value if type(value) is _PreparedFrameState else None

    def load_frame(self, frame_id: UUID) -> StagedFrame | None:
        if type(frame_id) is not UUID:
            raise TypeError("frame_id must be UUID")
        with self._read():
            return self._load_frame_with_owner(frame_id, self.load_owner())

    def list_frames(self, limit: int) -> tuple[StagedFrame, ...]:
        if type(limit) is not int or not 1 <= limit <= 257:
            raise ValueError("frame limit must be an integer from 1 through 257")
        with self._read():
            owner = self.load_owner()
            frame_ids = self._classify_frame_ids(owner)
            headers = self._load_authenticated_frame_headers(owner)
            if frame_ids != {header.frame_id for header in headers}:
                raise JournalIntegrityError("authenticated Frame inventory changed")
            rows = self._execute(
                "SELECT * FROM NioIngestFrame WHERE account_id = ? "
                "ORDER BY staged_revision, source_epoch, request_id, frame_id "
                "LIMIT ?",
                (self.account_id, _MAX_DURABLE_FRAME_ROWS),
            ).fetchall()
            values = tuple(
                self._decode_frame_state(self._parse_frame_id(row)[1], row, owner)
                for row in rows
            )
            return tuple(value for value in values if type(value) is StagedFrame)[
                :limit
            ]

    def has_reserved_quiesce_response(self) -> bool:
        with self._read():
            owner = self.load_owner()
            headers = self._load_authenticated_frame_headers(owner)
            staged = tuple(
                header
                for header in headers
                if header.room_materialized_revision is None
            )
            if not staged:
                return False
            selected = staged[-1]
            selected_row = cast(
                "Mapping[str, object]",
                self._frame_row(selected.frame_id),
            )
            if (
                self._frame_drain_row_from_full(
                    selected_row,
                    owner,
                    authenticate=False,
                )
                != selected
            ):
                raise JournalIntegrityError("reserved Frame header snapshot changed")
            value, quiesce_reserved = self._decode_frame_state_with_reservation(
                selected.frame_id,
                selected_row,
                owner,
                drain_header_authenticated=True,
            )
            if type(value) is not StagedFrame:
                raise JournalIntegrityError("reserved Frame is not staged")
            return quiesce_reserved

    def _write_frame(
        self,
        frame: StagedFrame,
        staged_revision: int,
        owner: OwnerView,
        payload_owner: _Owner,
        *,
        quiesce_reserved: bool = False,
    ) -> sqlite3.Cursor:
        payload, digest = _frame_payload(
            frame,
            staged_revision,
            payload_owner,
            quiesce_reserved=quiesce_reserved,
        )
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
        return self._execute(
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
