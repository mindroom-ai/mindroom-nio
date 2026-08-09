from dataclasses import dataclass
from enum import StrEnum
from uuid import UUID

from ..event_provenance import TimelineEventProvenance


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
    DECRYPTION_UPDATE = "decryption_update"


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


@dataclass(frozen=True, slots=True)
class ConsumerBootstrap:
    binding_operation_id: UUID
    binding: ConsumerBinding
    first_sequence: int
    baseline_room_ids: tuple[str, ...]
    baseline_sha256: bytes

    def __post_init__(self) -> None:
        if not isinstance(self.baseline_room_ids, tuple):
            raise TypeError("baseline_room_ids must be a tuple")


@dataclass(frozen=True, slots=True)
class RecordOrigin:
    transport: TransportKind
    source_epoch: int
    request_id: int
    frame_index: int


@dataclass(frozen=True, slots=True)
class SystemOrigin:
    kind: SystemOriginKind
    operation_id: UUID


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
        if not isinstance(self.origin, RecordOrigin):
            raise TypeError("EventRecord origin must be a RecordOrigin")


@dataclass(frozen=True, slots=True)
class RoomMemberSnapshot:
    user_id: str
    membership: str
    display_name: str | None
    avatar_url: str | None
    power_level: int

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
        if not isinstance(self.members, tuple):
            raise TypeError("members must be a tuple")

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


@dataclass(frozen=True, slots=True)
class BatchRef:
    stream_id: UUID
    sequence: int
    batch_id: UUID
    sha256: bytes


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

        _validate_batch(self)
