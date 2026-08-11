from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from ..event_provenance import TimelineEventProvenance


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _require_optional_exact(value: object, expected: type, field_name: str) -> None:
    if value is not None:
        _require_exact(value, expected, field_name)


def _require_tuple_of(
    value: object,
    element_types: tuple[type, ...],
    field_name: str,
    element_name: str,
) -> None:
    if type(value) is not tuple:
        raise TypeError(f"{field_name} must be a tuple")
    if any(type(element) not in element_types for element in value):
        raise TypeError(f"{field_name} elements must be {element_name}")


class TransportKind(StrEnum):
    CLASSIC = "classic"
    SLIDING = "sliding"


class RecordKind(StrEnum):
    TIMELINE = "timeline"
    STATE = "state"
    EPHEMERAL = "ephemeral"
    ROOM_ACCOUNT_DATA = "room_account_data"
    GLOBAL_ACCOUNT_DATA = "global_account_data"
    PRESENCE = "presence"
    TO_DEVICE = "to_device"
    ROOM_LIFECYCLE = "room_lifecycle"


class SystemOriginKind(StrEnum):
    FRESH_START = "fresh_start"
    CONSUMER_RESET = "consumer_reset"
    MEMBERSHIP_CHANGE = "membership_change"
    SOURCE_REBIND = "source_rebind"
    STORE_VALIDATION = "store_validation"


class LossReason(StrEnum):
    EVENT_LIMIT = "event_limit"
    FETCH_FAILED = "fetch_failed"
    BASELINE_LOST = "baseline_lost"
    UNVERIFIABLE = "unverifiable"
    CORRUPT_STORED_RECORD = "corrupt_stored_record"
    OVERSIZED_EVENT = "oversized_event"


class RoomHydrationStatus(StrEnum):
    PENDING = "pending"
    READY = "ready"
    UNAVAILABLE = "unavailable"


@dataclass(frozen=True, slots=True)
class ConsumerBinding:
    journal_generation: UUID
    consumer_generation: UUID

    def __post_init__(self) -> None:
        _require_exact(self.journal_generation, UUID, "journal_generation")
        _require_exact(self.consumer_generation, UUID, "consumer_generation")


@dataclass(frozen=True, slots=True)
class ConsumerBootstrap:
    binding_operation_id: UUID
    binding: ConsumerBinding
    first_sequence: int
    baseline_room_ids: tuple[str, ...]
    baseline_sha256: bytes

    def __post_init__(self) -> None:
        _require_exact(self.binding_operation_id, UUID, "binding_operation_id")
        _require_exact(self.binding, ConsumerBinding, "binding")
        _require_exact(self.first_sequence, int, "first_sequence")
        _require_tuple_of(self.baseline_room_ids, (str,), "baseline_room_ids", "str")
        _require_exact(self.baseline_sha256, bytes, "baseline_sha256")


@dataclass(frozen=True, slots=True)
class RecordOrigin:
    transport: TransportKind
    source_epoch: int
    request_id: int
    frame_index: int

    def __post_init__(self) -> None:
        _require_exact(self.transport, TransportKind, "transport")
        _require_exact(self.source_epoch, int, "source_epoch")
        _require_exact(self.request_id, int, "request_id")
        _require_exact(self.frame_index, int, "frame_index")


@dataclass(frozen=True, slots=True)
class SystemOrigin:
    kind: SystemOriginKind
    operation_id: UUID

    def __post_init__(self) -> None:
        _require_exact(self.kind, SystemOriginKind, "kind")
        _require_exact(self.operation_id, UUID, "operation_id")


@dataclass(frozen=True, slots=True)
class EventRecord:
    record_id: str
    kind: RecordKind
    origin: RecordOrigin
    room_id: str | None
    membership_epoch: int | None
    room_sequence: int | None
    event_id: str | None
    provenance: TimelineEventProvenance | None
    source_json: bytes
    clear_json: bytes | None

    def __post_init__(self) -> None:
        _require_exact(self.record_id, str, "record_id")
        _require_exact(self.kind, RecordKind, "kind")
        if type(self.origin) is not RecordOrigin:
            raise TypeError("EventRecord origin must be a RecordOrigin")
        _require_optional_exact(self.room_id, str, "room_id")
        _require_optional_exact(self.membership_epoch, int, "membership_epoch")
        _require_optional_exact(self.room_sequence, int, "room_sequence")
        _require_optional_exact(self.event_id, str, "event_id")
        _require_optional_exact(
            self.provenance,
            TimelineEventProvenance,
            "provenance",
        )
        _require_exact(self.source_json, bytes, "source_json")
        _require_optional_exact(self.clear_json, bytes, "clear_json")


@dataclass(frozen=True, slots=True)
class RoomMemberSnapshot:
    user_id: str
    membership: str
    display_name: str | None
    avatar_url: str | None
    power_level: int

    def __post_init__(self) -> None:
        _require_exact(self.user_id, str, "user_id")
        _require_exact(self.membership, str, "membership")
        _require_optional_exact(self.display_name, str, "display_name")
        _require_optional_exact(self.avatar_url, str, "avatar_url")
        _require_exact(self.power_level, int, "power_level")

    @property
    def name(self) -> str:
        return self.display_name or self.user_id


@dataclass(frozen=True, slots=True)
class RoomSnapshot:
    room_id: str
    membership_epoch: int
    own_user_id: str
    own_membership: str | None
    encrypted: bool
    name: str | None
    canonical_alias: str | None
    topic: str | None
    avatar_url: str | None
    join_rule: str | None
    room_version: str | None
    guest_access: str | None
    power_levels_json: bytes | None
    members: tuple[RoomMemberSnapshot, ...]

    def __post_init__(self) -> None:
        _require_exact(self.room_id, str, "room_id")
        _require_exact(self.membership_epoch, int, "membership_epoch")
        _require_exact(self.own_user_id, str, "own_user_id")
        _require_optional_exact(self.own_membership, str, "own_membership")
        _require_exact(self.encrypted, bool, "encrypted")
        _require_optional_exact(self.name, str, "name")
        _require_optional_exact(self.canonical_alias, str, "canonical_alias")
        _require_optional_exact(self.topic, str, "topic")
        _require_optional_exact(self.avatar_url, str, "avatar_url")
        _require_optional_exact(self.join_rule, str, "join_rule")
        _require_optional_exact(self.room_version, str, "room_version")
        _require_optional_exact(self.guest_access, str, "guest_access")
        _require_optional_exact(
            self.power_levels_json,
            bytes,
            "power_levels_json",
        )
        _require_tuple_of(
            self.members,
            (RoomMemberSnapshot,),
            "members",
            "RoomMemberSnapshot",
        )

    @property
    def _active_members(self) -> tuple[RoomMemberSnapshot, ...]:
        return tuple(
            member for member in self.members if member.membership in {"join", "invite"}
        )

    @property
    def member_count(self) -> int:
        return len(self._active_members)

    @property
    def is_group(self) -> bool:
        return not (self.name or self.canonical_alias)

    @property
    def display_name(self) -> str:
        named_room = self.name or self.canonical_alias
        if named_room:
            return named_room

        members = tuple(
            member
            for member in self._active_members
            if member.user_id != self.own_user_id
        )
        if not members:
            return "Empty Room"

        name_counts: dict[str, int] = {}
        for member in self._active_members:
            name_counts[member.name] = name_counts.get(member.name, 0) + 1

        def user_name(member: RoomMemberSnapshot) -> str:
            if name_counts[member.name] > 1 and member.display_name:
                return f"{member.display_name} ({member.user_id})"
            return member.name

        names = sorted(user_name(member) for member in members)
        shown_names = names[:5]
        others = len(names) - len(shown_names)
        if others:
            return (
                f"{', '.join(shown_names)} and {others} "
                f"other{'' if others == 1 else 's'}"
            )
        if len(shown_names) == 1:
            return shown_names[0]
        return f"{', '.join(shown_names[:-1])} and {shown_names[-1]}"


@dataclass(frozen=True, slots=True)
class LossBoundary:
    prior_event_id: str | None
    prior_origin_server_ts: int | None
    start_token: str | None
    target_token: str | None

    def __post_init__(self) -> None:
        _require_optional_exact(self.prior_event_id, str, "prior_event_id")
        _require_optional_exact(
            self.prior_origin_server_ts,
            int,
            "prior_origin_server_ts",
        )
        _require_optional_exact(self.start_token, str, "start_token")
        _require_optional_exact(self.target_token, str, "target_token")


@dataclass(frozen=True, slots=True)
class LossRecord:
    loss_id: str
    origin: RecordOrigin | SystemOrigin
    room_id: str
    membership_epoch: int
    reason: LossReason
    boundary: LossBoundary
    detail_json: bytes

    def __post_init__(self) -> None:
        if self.membership_epoch is None:
            raise ValueError("room loss membership_epoch is required")
        _require_exact(self.loss_id, str, "loss_id")
        if type(self.origin) not in (RecordOrigin, SystemOrigin):
            raise TypeError("origin must be RecordOrigin or SystemOrigin")
        _require_exact(self.room_id, str, "room_id")
        _require_exact(self.membership_epoch, int, "membership_epoch")
        _require_exact(self.reason, LossReason, "reason")
        _require_exact(self.boundary, LossBoundary, "boundary")
        _require_exact(self.detail_json, bytes, "detail_json")


@dataclass(frozen=True, slots=True)
class BatchRef:
    stream_id: UUID
    sequence: int
    batch_id: UUID
    sha256: bytes

    def __post_init__(self) -> None:
        _require_exact(self.stream_id, UUID, "stream_id")
        _require_exact(self.sequence, int, "sequence")
        _require_exact(self.batch_id, UUID, "batch_id")
        _require_exact(self.sha256, bytes, "sha256")


@dataclass(frozen=True, slots=True)
class SyncBatch:
    schema_version: int
    account_id: str
    device_id: str
    consumer: ConsumerBinding
    ref: BatchRef
    created_revision: int
    records: tuple[EventRecord | LossRecord, ...]

    def __post_init__(self) -> None:
        from .serialization import _validate_batch

        _require_exact(self.schema_version, int, "schema_version")
        _require_exact(self.account_id, str, "account_id")
        _require_exact(self.device_id, str, "device_id")
        _require_exact(self.consumer, ConsumerBinding, "consumer")
        _require_exact(self.ref, BatchRef, "ref")
        _require_exact(self.created_revision, int, "created_revision")
        _require_tuple_of(
            self.records,
            (EventRecord, LossRecord),
            "records",
            "EventRecord or LossRecord",
        )
        _validate_batch(self)
