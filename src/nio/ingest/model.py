from dataclasses import dataclass
from enum import StrEnum
from typing import NamedTuple
from uuid import UUID, uuid5

from ..event_provenance import TimelineEventProvenance
from ._json import canonical_json, load_internal_json


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


class _CallbackRoute(StrEnum):
    EVENT = "event"
    EPHEMERAL = "ephemeral"
    ROOM_ACCOUNT_DATA = "room_account_data"
    GLOBAL_ACCOUNT_DATA = "global_account_data"
    PRESENCE = "presence"
    TO_DEVICE = "to_device"


class _DecryptionDisposition(StrEnum):
    NONE = "none"
    DECRYPTED = "decrypted"
    MEGOLM_FAILED = "megolm_failed"


class _DecryptedToDeviceKind(StrEnum):
    ROOM_KEY = "room_key"
    FORWARDED_ROOM_KEY = "forwarded_room_key"
    DUMMY = "dummy"
    UNKNOWN = "unknown"
    BAD = "bad"
    UNKNOWN_BAD = "unknown_bad"


class _MembershipSourceKind(StrEnum):
    STATE = "state"
    TIMELINE = "timeline"
    SECTION = "section"


class _MembershipProvenance(StrEnum):
    REPORTED = "reported"


class _PreparationPhase(StrEnum):
    SOURCE = "source"
    EXPIRED_VERIFICATION = "expired_verification"
    COLLECTED_KEY_REQUEST = "collected_key_request"


class _QueuedToDeviceSubtype(StrEnum):
    GENERIC = "generic"
    DUMMY = "dummy"
    ROOM_KEY_REQUEST = "room_key_request"


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


_LOCAL_MEMBERSHIPS = frozenset({"invite", "join", "knock", "leave", "ban"})
_LOCAL_MEMBERSHIP_SOURCE_FIELDS = (
    "event_id",
    "membership",
    "membership_epoch",
    "membership_provenance",
    "previous_membership",
    "previous_membership_epoch",
    "source_kind",
    "source_record_id",
    "timeline_provenance",
)
_LOCAL_ROOM_LIFECYCLE_DOMAIN = "nio:room-lifecycle:v1"


class _LocalMembershipEvidence(NamedTuple):
    previous_membership: str
    previous_epoch: int
    current_membership: str
    current_epoch: int


def _local_membership_transition_epoch(
    previous_membership: str,
    previous_epoch: int,
    current_membership: str,
) -> int:
    _require_exact(previous_membership, str, "previous_membership")
    _require_exact(previous_epoch, int, "previous_epoch")
    _require_exact(current_membership, str, "current_membership")
    if (
        previous_membership not in _LOCAL_MEMBERSHIPS
        or current_membership not in {"join", "leave"}
        or previous_membership == current_membership
        or previous_epoch < 0
    ):
        raise ValueError("local membership transition is invalid")
    return previous_epoch + int(
        previous_membership == "join" and current_membership != "join"
    )


def _local_membership_predecessor_matches(
    observed_membership: str | None,
    observed_epoch: int,
    *,
    previous_membership: str,
    previous_epoch: int,
    current_membership: str,
) -> bool:
    _local_membership_transition_epoch(
        previous_membership,
        previous_epoch,
        current_membership,
    )
    if (
        type(observed_membership) is not str
        or observed_membership not in _LOCAL_MEMBERSHIPS
    ):
        return False
    if type(observed_epoch) is not int or observed_epoch != previous_epoch:
        return False
    if observed_membership == previous_membership:
        return True
    return (
        previous_membership == "leave"
        and current_membership == "join"
        and observed_membership in {"invite", "knock"}
    )


def _local_membership_record_id(operation_id: UUID) -> str:
    _require_exact(operation_id, UUID, "operation_id")
    return str(uuid5(operation_id, _LOCAL_ROOM_LIFECYCLE_DOMAIN))


def _local_membership_source_json(
    previous_membership: str,
    previous_epoch: int,
    current_membership: str,
) -> bytes:
    membership_epoch = _local_membership_transition_epoch(
        previous_membership,
        previous_epoch,
        current_membership,
    )
    return canonical_json(
        {
            "event_id": None,
            "membership": current_membership,
            "membership_epoch": membership_epoch,
            "membership_provenance": "local",
            "previous_membership": previous_membership,
            "previous_membership_epoch": previous_epoch,
            "source_kind": "local",
            "source_record_id": None,
            "timeline_provenance": None,
        }
    )


def _local_membership_evidence(source_json: bytes) -> _LocalMembershipEvidence:
    source = load_internal_json(
        source_json,
        "local membership lifecycle source",
    )
    if type(source) is not dict or tuple(source) != _LOCAL_MEMBERSHIP_SOURCE_FIELDS:
        raise ValueError("local membership lifecycle source is invalid")
    previous_membership = source["previous_membership"]
    previous_epoch = source["previous_membership_epoch"]
    current_membership = source["membership"]
    if (
        type(previous_membership) is not str
        or type(previous_epoch) is not int
        or type(current_membership) is not str
    ):
        raise ValueError("local membership lifecycle source is invalid")
    current_epoch = _local_membership_transition_epoch(
        previous_membership,
        previous_epoch,
        current_membership,
    )
    expected = {
        "event_id": None,
        "membership": current_membership,
        "membership_epoch": current_epoch,
        "membership_provenance": "local",
        "previous_membership": previous_membership,
        "previous_membership_epoch": previous_epoch,
        "source_kind": "local",
        "source_record_id": None,
        "timeline_provenance": None,
    }
    if source != expected or source_json != canonical_json(expected):
        raise ValueError("local membership lifecycle source is invalid")
    return _LocalMembershipEvidence(
        previous_membership,
        previous_epoch,
        current_membership,
        current_epoch,
    )


@dataclass(frozen=True, slots=True)
class EventRecord:
    record_id: str
    kind: RecordKind
    origin: RecordOrigin | SystemOrigin
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
        if type(self.origin) not in (RecordOrigin, SystemOrigin):
            raise TypeError("EventRecord origin must be a RecordOrigin")
        if type(self.origin) is SystemOrigin and (
            self.kind is not RecordKind.ROOM_LIFECYCLE
            or self.origin.kind is not SystemOriginKind.MEMBERSHIP_CHANGE
        ):
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
        if type(self.origin) is SystemOrigin:
            self._validate_local_membership_lifecycle()

    def _validate_local_membership_lifecycle(self) -> None:
        origin = self.origin
        assert type(origin) is SystemOrigin
        if (
            not self.room_id
            or self.membership_epoch is None
            or self.membership_epoch < 0
            or self.room_sequence is None
            or self.room_sequence < 0
            or self.event_id is not None
            or self.provenance is not None
            or self.clear_json is not None
            or self.record_id != _local_membership_record_id(origin.operation_id)
        ):
            raise ValueError("local membership lifecycle EventRecord is invalid")
        evidence = _local_membership_evidence(self.source_json)
        if self.membership_epoch != evidence.current_epoch:
            raise ValueError("local membership lifecycle source is invalid")


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


class _PreparedIngestionRecord(NamedTuple):
    record_id: str
    kind: RecordKind
    origin: RecordOrigin
    preparation_phase: _PreparationPhase
    effective_event_type: str
    room_id: str | None
    event_id: str | None
    provenance: TimelineEventProvenance | None
    source_json: bytes
    clear_json: bytes | None
    decryption: _DecryptionDisposition
    decryption_verified: bool | None
    decrypted_to_device_kind: _DecryptedToDeviceKind | None
    callback_route: _CallbackRoute | None


class _PreparedMembershipTransition(NamedTuple):
    transition_id: str
    source_record_id: str | None
    room_id: str
    event_id: str | None
    previous_membership: str | None
    current_membership: str
    previous_epoch: int
    current_epoch: int
    source_kind: _MembershipSourceKind
    timeline_provenance: TimelineEventProvenance | None
    membership_provenance: _MembershipProvenance
    origin: RecordOrigin
    source_json: bytes | None


class _PreparedWaitingKeyRequest(NamedTuple):
    source_json: bytes
    sender_user_id: str
    requesting_device_id: str
    request_id: str
    room_id: str
    sender_key: str
    session_id: str
    algorithm: str


class _PreparedMegolmRerequest(NamedTuple):
    source_json: bytes
    room_id: str
    event_id: str
    sender_user_id: str
    sender_device_id: str
    sender_key: str
    session_id: str
    algorithm: str


class _PreparedKeyClaim(NamedTuple):
    user_id: str
    device_id: str
    was_wedged: bool
    was_waiting: bool
    waiting_key_requests: tuple[_PreparedWaitingKeyRequest, ...]
    rerequest_events: tuple[_PreparedMegolmRerequest, ...]


class _PreparedQueuedToDeviceMessage(NamedTuple):
    subtype: _QueuedToDeviceSubtype
    event_type: str
    recipient_user_id: str
    recipient_device_id: str
    content_json: bytes
    request_id: str | None
    session_id: str | None
    room_id: str | None
    algorithm: str | None
    rerequest_events: tuple[_PreparedMegolmRerequest, ...]


class _PreparedCryptoDelta(NamedTuple):
    encrypted_room_ids: tuple[str, ...]
    users_for_key_query: tuple[str, ...]
    uploaded_key_count: int | None
    one_time_key_counts_json: bytes
    unused_fallback_key_types_json: bytes
    key_claims: tuple[_PreparedKeyClaim, ...]
    queued_to_device_messages: tuple[_PreparedQueuedToDeviceMessage, ...]


class _PreparedIngestionFrame(NamedTuple):
    frame_id: UUID
    transport: TransportKind
    source_epoch: int
    request_id: int
    staged_revision: int
    request_cursor_json: bytes
    candidate_cursor_json: bytes
    source_sha256: bytes
    compatibility_token: str | None
    records: tuple[_PreparedIngestionRecord, ...]
    membership_transitions: tuple[_PreparedMembershipTransition, ...]
    room_snapshots: tuple[RoomSnapshot, ...]
    crypto_delta: _PreparedCryptoDelta


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
        if type(self.origin) is SystemOrigin and (
            self.origin.kind is SystemOriginKind.MEMBERSHIP_CHANGE
        ):
            raise ValueError(
                "membership-change SystemOrigin is reserved for lifecycle events"
            )
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
        if not 0 <= self.sequence <= 2**63 - 1 or len(self.sha256) != 32:
            raise ValueError("batch reference sequence or digest is invalid")


@dataclass(frozen=True, slots=True)
class SyncBatch:
    schema_version: int
    account_id: str
    device_id: str
    consumer_generation: UUID
    ref: BatchRef
    created_revision: int
    records: tuple[EventRecord | LossRecord, ...]

    def __post_init__(self) -> None:
        from .serialization import _validate_batch

        _require_exact(self.schema_version, int, "schema_version")
        _require_exact(self.account_id, str, "account_id")
        _require_exact(self.device_id, str, "device_id")
        _require_exact(self.consumer_generation, UUID, "consumer_generation")
        _require_exact(self.ref, BatchRef, "ref")
        _require_exact(self.created_revision, int, "created_revision")
        _require_tuple_of(
            self.records,
            (EventRecord, LossRecord),
            "records",
            "EventRecord or LossRecord",
        )
        _validate_batch(self)
