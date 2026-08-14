import base64
import hashlib
import hmac
import json
from typing import Any
from uuid import UUID, uuid5

from .errors import BatchIntegrityError
from .model import (
    BatchRef,
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RecordOrigin,
    RoomMemberSnapshot,
    RoomSnapshot,
    SyncBatch,
    SystemOrigin,
    SystemOriginKind,
    TimelineEventProvenance,
    TransportKind,
)

SCHEMA_VERSION = 1

_ROOM_SNAPSHOT_FIELDS = (
    "room_id",
    "membership_epoch",
    "own_user_id",
    "own_membership",
    "encrypted",
    "name",
    "canonical_alias",
    "topic",
    "avatar_url",
    "join_rule",
    "room_version",
    "guest_access",
    "power_levels_json",
    "members",
)
_TRANSPORT_ORIGIN_FIELDS = (
    "origin_type",
    "transport",
    "source_epoch",
    "request_id",
    "frame_index",
)
_SYSTEM_ORIGIN_FIELDS = (
    "origin_type",
    "kind",
    "operation_id",
)


def _canonical_json(value: dict[str, Any]) -> bytes:
    return json.dumps(
        value,
        ensure_ascii=False,
        allow_nan=False,
        separators=(",", ":"),
    ).encode("utf-8")


def _encoded_bytes(value: bytes | None) -> str | None:
    if value is None:
        return None
    return base64.b64encode(value).decode("ascii")


def _decoded_bytes(
    value: object, field: str, *, optional: bool = False
) -> bytes | None:
    if value is None and optional:
        return None
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a base64 string")
    try:
        return base64.b64decode(value, validate=True)
    except ValueError as error:
        raise ValueError(f"{field} must be valid base64") from error


def _origin_to_dict(origin: RecordOrigin | SystemOrigin) -> dict[str, object]:
    if isinstance(origin, RecordOrigin):
        return {
            "origin_type": "transport",
            "transport": origin.transport.value,
            "source_epoch": origin.source_epoch,
            "request_id": origin.request_id,
            "frame_index": origin.frame_index,
        }
    return {
        "origin_type": "system",
        "kind": origin.kind.value,
        "operation_id": str(origin.operation_id),
    }


def _origin_from_dict(
    value: object, *, exact: bool = True
) -> RecordOrigin | SystemOrigin:
    origin = _dict(value, "origin")
    origin_type = origin.get("origin_type")
    if origin_type == "transport":
        if exact and tuple(origin) != _TRANSPORT_ORIGIN_FIELDS:
            raise ValueError("transport origin fields are not canonical")
        return RecordOrigin(
            TransportKind(_string(origin.get("transport"), "transport")),
            _integer(origin.get("source_epoch"), "source_epoch"),
            _integer(origin.get("request_id"), "request_id"),
            _integer(origin.get("frame_index"), "frame_index"),
        )
    if origin_type == "system":
        if exact and tuple(origin) != _SYSTEM_ORIGIN_FIELDS:
            raise ValueError("system origin fields are not canonical")
        return SystemOrigin(
            SystemOriginKind(_string(origin.get("kind"), "kind")),
            UUID(_string(origin.get("operation_id"), "operation_id")),
        )
    raise ValueError("origin_type must be 'transport' or 'system'")


def _boundary_to_dict(boundary: LossBoundary) -> dict[str, object]:
    return {
        "prior_event_id": boundary.prior_event_id,
        "prior_origin_server_ts": boundary.prior_origin_server_ts,
        "start_token": boundary.start_token,
        "target_token": boundary.target_token,
    }


def _boundary_from_dict(value: object) -> LossBoundary:
    boundary = _dict(value, "boundary")
    return LossBoundary(
        _optional_string(boundary.get("prior_event_id"), "prior_event_id"),
        _optional_integer(
            boundary.get("prior_origin_server_ts"), "prior_origin_server_ts"
        ),
        _optional_string(boundary.get("start_token"), "start_token"),
        _optional_string(boundary.get("target_token"), "target_token"),
    )


def _record_to_dict(record: EventRecord | LossRecord) -> dict[str, object]:
    if isinstance(record, EventRecord):
        _require_optional_integer(record.membership_epoch, "membership_epoch")
        _require_optional_integer(record.room_sequence, "room_sequence")
        return {
            "record_type": "event",
            "record_id": record.record_id,
            "kind": record.kind.value,
            "origin": _origin_to_dict(record.origin),
            "room_id": record.room_id,
            "membership_epoch": record.membership_epoch,
            "room_sequence": record.room_sequence,
            "event_id": record.event_id,
            "provenance": (
                record.provenance.value if record.provenance is not None else None
            ),
            "source_json": _encoded_bytes(record.source_json),
            "clear_json": _encoded_bytes(record.clear_json),
        }
    _require_integer(record.membership_epoch, "membership_epoch")
    _require_optional_integer(
        record.boundary.prior_origin_server_ts,
        "prior_origin_server_ts",
    )
    return {
        "record_type": "loss",
        "loss_id": record.loss_id,
        "origin": _origin_to_dict(record.origin),
        "room_id": record.room_id,
        "membership_epoch": record.membership_epoch,
        "reason": record.reason.value,
        "boundary": _boundary_to_dict(record.boundary),
        "detail_json": _encoded_bytes(record.detail_json),
    }


def _record_from_dict(value: object, *, exact: bool = True) -> EventRecord | LossRecord:
    record = _dict(value, "record")
    record_type = record.get("record_type")
    if record_type == "event":
        origin = _origin_from_dict(record.get("origin"), exact=exact)
        return EventRecord(
            _string(record.get("record_id"), "record_id"),
            RecordKind(_string(record.get("kind"), "kind")),
            origin,
            _optional_string(record.get("room_id"), "room_id"),
            _optional_integer(record.get("membership_epoch"), "membership_epoch"),
            _optional_integer(record.get("room_sequence"), "room_sequence"),
            _optional_string(record.get("event_id"), "event_id"),
            (
                TimelineEventProvenance(_string(record.get("provenance"), "provenance"))
                if record.get("provenance") is not None
                else None
            ),
            _required_decoded_bytes(record.get("source_json"), "source_json"),
            _decoded_bytes(record.get("clear_json"), "clear_json", optional=True),
        )
    if record_type == "loss":
        return LossRecord(
            _string(record.get("loss_id"), "loss_id"),
            _origin_from_dict(record.get("origin"), exact=exact),
            _string(record.get("room_id"), "room_id"),
            _integer(record.get("membership_epoch"), "membership_epoch"),
            LossReason(_string(record.get("reason"), "reason")),
            _boundary_from_dict(record.get("boundary")),
            _required_decoded_bytes(record.get("detail_json"), "detail_json"),
        )
    raise ValueError("record_type must be 'event' or 'loss'")


def _record_from_exact_dict(value: object) -> EventRecord | LossRecord:
    payload = _dict(value, "record")
    record = _record_from_dict(payload)
    canonical = _record_to_dict(record)
    if tuple(payload) != tuple(canonical) or payload != canonical:
        raise ValueError("record fields are not canonical")
    return record


def _batch_dict(
    *,
    schema_version: int,
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    stream_id: UUID,
    sequence: int,
    created_revision: int,
    records: tuple[EventRecord | LossRecord, ...],
) -> dict[str, object]:
    _require_integer(schema_version, "schema_version")
    _require_integer(sequence, "sequence")
    _require_integer(created_revision, "created_revision")
    return {
        "schema_version": schema_version,
        "account_id": account_id,
        "device_id": device_id,
        "consumer_generation": str(consumer_generation),
        "stream_id": str(stream_id),
        "sequence": sequence,
        "created_revision": created_revision,
        "records": [_record_to_dict(record) for record in records],
    }


def canonical_batch_payload(batch: SyncBatch) -> bytes:
    """Return the canonical identity payload for a durable sync batch."""
    return _canonical_json(
        _batch_dict(
            schema_version=batch.schema_version,
            account_id=batch.account_id,
            device_id=batch.device_id,
            consumer_generation=batch.consumer_generation,
            stream_id=batch.ref.stream_id,
            sequence=batch.ref.sequence,
            created_revision=batch.created_revision,
            records=batch.records,
        )
    )


def batch_from_records(
    *,
    account_id: str,
    device_id: str,
    consumer_generation: UUID,
    stream_id: UUID,
    sequence: int,
    created_revision: int,
    records: tuple[EventRecord | LossRecord, ...],
) -> SyncBatch:
    """Build a checked batch from records for ingestion internals."""
    if not isinstance(records, tuple):
        raise TypeError("records must be a tuple")
    if not records:
        raise ValueError("SyncBatch requires at least one record")
    payload = _canonical_json(
        _batch_dict(
            schema_version=SCHEMA_VERSION,
            account_id=account_id,
            device_id=device_id,
            consumer_generation=consumer_generation,
            stream_id=stream_id,
            sequence=sequence,
            created_revision=created_revision,
            records=records,
        )
    )
    digest = hashlib.sha256(payload).digest()
    ref = BatchRef(
        stream_id,
        sequence,
        uuid5(stream_id, f"{sequence}:{digest.hex()}"),
        digest,
    )
    return SyncBatch(
        SCHEMA_VERSION,
        account_id,
        device_id,
        consumer_generation,
        ref,
        created_revision,
        records,
    )


def _validate_batch(batch: SyncBatch) -> None:
    _require_integer(batch.schema_version, "schema_version")
    if batch.schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version: {batch.schema_version}")
    _require_integer(batch.ref.sequence, "sequence")
    _require_integer(batch.created_revision, "created_revision")
    if not batch.account_id or not batch.device_id:
        raise ValueError("batch account_id and device_id must be nonempty")
    if not 0 <= batch.ref.sequence <= 2**63 - 1:
        raise ValueError("sequence must fit a nonnegative SQLite integer")
    if batch.created_revision < 1:
        raise ValueError("created_revision must be positive")
    if not isinstance(batch.records, tuple):
        raise TypeError("records must be a tuple")
    if not batch.records:
        raise ValueError("SyncBatch requires at least one record")

    for record in batch.records:
        if isinstance(record, LossRecord):
            expected_loss_id = _loss_id(batch.ref.stream_id, record)
            if not hmac.compare_digest(record.loss_id, expected_loss_id):
                raise BatchIntegrityError("loss_id does not match loss contents")

    digest = hashlib.sha256(canonical_batch_payload(batch)).digest()
    if not hmac.compare_digest(batch.ref.sha256, digest):
        raise BatchIntegrityError("batch sha256 does not match canonical payload")
    expected_batch_id = uuid5(
        batch.ref.stream_id,
        f"{batch.ref.sequence}:{digest.hex()}",
    )
    if batch.ref.batch_id != expected_batch_id:
        raise BatchIntegrityError("batch_id does not match canonical payload")


def _loss_id(stream_id: UUID, record: LossRecord) -> str:
    if isinstance(record.origin, RecordOrigin):
        origin_id = (
            f"transport:{record.origin.transport.value}:{record.origin.source_epoch}:"
            f"{record.origin.request_id}:{record.origin.frame_index}"
        )
    else:
        origin_id = f"system:{record.origin.kind.value}:{record.origin.operation_id}"
    boundary_digest = hashlib.sha256(
        _canonical_json(_boundary_to_dict(record.boundary))
    ).hexdigest()
    return str(
        uuid5(
            stream_id,
            f"{record.room_id}:{record.membership_epoch}:{origin_id}:"
            f"{record.reason.value}:{boundary_digest}",
        )
    )


def _batch_from_payload(payload: bytes) -> SyncBatch:
    root = _load_json_object(payload)
    schema_version = _integer(root.get("schema_version"), "schema_version")
    if schema_version != SCHEMA_VERSION:
        raise ValueError(f"unsupported schema_version: {schema_version}")
    consumer_text = _string(root.get("consumer_generation"), "consumer_generation")
    consumer_generation = UUID(consumer_text)
    records_value = root.get("records")
    if not isinstance(records_value, list):
        raise ValueError("records must be an array")
    records = tuple(_record_from_dict(record) for record in records_value)
    batch = batch_from_records(
        account_id=_string(root.get("account_id"), "account_id"),
        device_id=_string(root.get("device_id"), "device_id"),
        consumer_generation=consumer_generation,
        stream_id=UUID(_string(root.get("stream_id"), "stream_id")),
        sequence=_integer(root.get("sequence"), "sequence"),
        created_revision=_integer(root.get("created_revision"), "created_revision"),
        records=records,
    )
    if payload != canonical_batch_payload(batch):
        raise ValueError("batch payload is not canonical")
    return batch


def _room_member_to_dict(member: RoomMemberSnapshot) -> dict[str, object]:
    _require_integer(member.power_level, "power_level")
    return {
        "user_id": member.user_id,
        "membership": member.membership,
        "display_name": member.display_name,
        "avatar_url": member.avatar_url,
        "power_level": member.power_level,
    }


def _room_snapshot_to_dict(snapshot: RoomSnapshot) -> dict[str, object]:
    return {
        "room_id": snapshot.room_id,
        "membership_epoch": snapshot.membership_epoch,
        "own_user_id": snapshot.own_user_id,
        "own_membership": snapshot.own_membership,
        "encrypted": snapshot.encrypted,
        "name": snapshot.name,
        "canonical_alias": snapshot.canonical_alias,
        "topic": snapshot.topic,
        "avatar_url": snapshot.avatar_url,
        "join_rule": snapshot.join_rule,
        "room_version": snapshot.room_version,
        "guest_access": snapshot.guest_access,
        "power_levels_json": _encoded_bytes(snapshot.power_levels_json),
        "members": [_room_member_to_dict(member) for member in snapshot.members],
    }


def _canonical_room_snapshot_payload(snapshot: RoomSnapshot) -> bytes:
    return _canonical_json(_room_snapshot_to_dict(snapshot))


def _room_snapshot_from_payload(payload: bytes) -> RoomSnapshot:
    snapshot = _room_snapshot_from_dict(_load_json_object(payload))
    if payload != _canonical_room_snapshot_payload(snapshot):
        raise ValueError("room snapshot payload is not canonical")
    return snapshot


def _room_snapshot_from_dict(value: object) -> RoomSnapshot:
    root = _dict(value, "room_snapshot")
    if tuple(root) != _ROOM_SNAPSHOT_FIELDS:
        raise ValueError("room snapshot fields are not canonical")
    members_value = root.get("members")
    if not isinstance(members_value, list):
        raise ValueError("members must be an array")
    members = tuple(_room_member_from_dict(member) for member in members_value)
    return RoomSnapshot(
        _string(root.get("room_id"), "room_id"),
        _integer(root.get("membership_epoch"), "membership_epoch"),
        _string(root.get("own_user_id"), "own_user_id"),
        _optional_string(root.get("own_membership"), "own_membership"),
        _boolean(root.get("encrypted"), "encrypted"),
        _optional_string(root.get("name"), "name"),
        _optional_string(root.get("canonical_alias"), "canonical_alias"),
        _optional_string(root.get("topic"), "topic"),
        _optional_string(root.get("avatar_url"), "avatar_url"),
        _optional_string(root.get("join_rule"), "join_rule"),
        _optional_string(root.get("room_version"), "room_version"),
        _optional_string(root.get("guest_access"), "guest_access"),
        _decoded_bytes(
            root.get("power_levels_json"), "power_levels_json", optional=True
        ),
        members,
    )


def _room_member_from_dict(value: object) -> RoomMemberSnapshot:
    member = _dict(value, "member")
    return RoomMemberSnapshot(
        _string(member.get("user_id"), "user_id"),
        _string(member.get("membership"), "membership"),
        _optional_string(member.get("display_name"), "display_name"),
        _optional_string(member.get("avatar_url"), "avatar_url"),
        _integer(member.get("power_level"), "power_level"),
    )


def _load_json_object(payload: bytes) -> dict[str, object]:
    try:
        value = json.loads(
            payload,
            parse_constant=lambda constant: (_ for _ in ()).throw(
                ValueError(f"non-finite JSON number: {constant}")
            ),
        )
    except (UnicodeDecodeError, json.JSONDecodeError) as error:
        raise ValueError("payload must be valid UTF-8 JSON") from error
    return _dict(value, "payload")


def _dict(value: object, field: str) -> dict[str, object]:
    if not isinstance(value, dict) or not all(isinstance(key, str) for key in value):
        raise ValueError(f"{field} must be an object with string keys")
    return value


def _string(value: object, field: str) -> str:
    if not isinstance(value, str):
        raise ValueError(f"{field} must be a string")
    return value


def _optional_string(value: object, field: str) -> str | None:
    if value is None:
        return None
    return _string(value, field)


def _integer(value: object, field: str) -> int:
    if not isinstance(value, int) or isinstance(value, bool):
        raise ValueError(f"{field} must be an integer")
    return value


def _require_integer(value: object, field: str) -> None:
    if not isinstance(value, int) or isinstance(value, bool):
        raise TypeError(f"{field} must be an integer")


def _require_optional_integer(value: object, field: str) -> None:
    if value is not None:
        _require_integer(value, field)


def _optional_integer(value: object, field: str) -> int | None:
    if value is None:
        return None
    return _integer(value, field)


def _boolean(value: object, field: str) -> bool:
    if not isinstance(value, bool):
        raise ValueError(f"{field} must be a boolean")
    return value


def _required_decoded_bytes(value: object, field: str) -> bytes:
    decoded = _decoded_bytes(value, field)
    assert decoded is not None
    return decoded
