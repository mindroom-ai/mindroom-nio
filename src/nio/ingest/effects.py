"""Cycle-safe immutable values for durable ingestion network effects."""

from __future__ import annotations

from dataclasses import dataclass
from enum import StrEnum
from typing import cast
from uuid import UUID, uuid5

from ._json import canonical_json, load_json
from .model import TransportKind
from .ports import NetworkFailureKind


def _require_exact(value: object, expected: type, field_name: str) -> None:
    if type(value) is not expected:
        raise TypeError(f"{field_name} must be {expected.__name__}")


def _require_nonempty_string(value: object, field_name: str) -> None:
    _require_exact(value, str, field_name)
    if not value:
        raise ValueError(f"{field_name} must not be empty")


def _require_nonnegative(value: object, field_name: str) -> None:
    _require_exact(value, int, field_name)
    if cast(int, value) < 0:
        raise ValueError(f"{field_name} must be nonnegative")


def _require_positive(value: object, field_name: str) -> None:
    _require_exact(value, int, field_name)
    if cast(int, value) <= 0:
        raise ValueError(f"{field_name} must be positive")


def _require_request_identity(
    effect_id: object,
    stream_id: object,
    transport: object,
    room_id: object,
    membership_epoch: object,
) -> None:
    _require_exact(effect_id, UUID, "effect_id")
    _require_exact(stream_id, UUID, "stream_id")
    _require_exact(transport, TransportKind, "transport")
    _require_nonempty_string(room_id, "room_id")
    _require_nonnegative(membership_epoch, "membership_epoch")


def _require_canonical_room_id(value: object) -> None:
    _require_nonempty_string(value, "room_id")
    room_id = cast(str, value)
    try:
        encoded = room_id.encode("utf-8")
    except UnicodeEncodeError as error:
        raise ValueError("room_id must be a canonical Matrix room ID") from error
    if (
        not room_id.startswith("!")
        or len(room_id) == 1
        or "\0" in room_id
        or len(encoded) > 255
    ):
        raise ValueError("room_id must be a canonical Matrix room ID")


def _require_canonical_json_object(data: object, field_name: str) -> None:
    _require_exact(data, bytes, field_name)
    assert isinstance(data, bytes)
    try:
        value = load_json(data, field_name)
    except ValueError as error:
        raise ValueError(f"{field_name} must contain canonical JSON") from error
    if type(value) is not dict:
        raise ValueError(f"{field_name} must contain a JSON object")
    try:
        canonical = canonical_json(value)
    except (UnicodeEncodeError, ValueError) as error:
        raise ValueError(f"{field_name} exceeds the JSON nesting limit") from error
    if data != canonical:
        raise ValueError(f"{field_name} must contain canonical JSON")


class MembershipAction(StrEnum):
    JOIN = "join"
    LEAVE = "leave"
    FORGET = "forget"


class MembershipDeliveryState(StrEnum):
    READY = "ready"
    DISPATCHED_UNCONFIRMED = "dispatched_unconfirmed"


class MembershipOperationResolution(StrEnum):
    RETRY = "retry"
    SUPERSEDE = "supersede"


class MembershipOperationResolutionOutcome(StrEnum):
    READY = "ready"
    SUPERSEDED = "superseded"
    ABSENT = "absent"


class NetworkEffectKind(StrEnum):
    RECOVERY = "recovery"
    ROOM_HYDRATION = "room_hydration"
    MEMBERSHIP = "membership"


def _recovery_effect_id_from_gap_id(gap_id: UUID, page_ordinal: int) -> UUID:
    _require_exact(gap_id, UUID, "gap_id")
    _require_nonnegative(page_ordinal, "page_ordinal")
    return uuid5(gap_id, f"page:{page_ordinal}")


def _room_hydration_effect_id(
    stream_id: UUID,
    room_id: str,
    membership_epoch: int,
) -> UUID:
    _require_exact(stream_id, UUID, "stream_id")
    _require_nonempty_string(room_id, "room_id")
    _require_nonnegative(membership_epoch, "membership_epoch")
    return uuid5(stream_id, f"hydrate:{room_id}:{membership_epoch}")


@dataclass(frozen=True, slots=True)
class RecoveryRequest:
    effect_id: UUID
    stream_id: UUID
    transport: TransportKind
    room_id: str
    membership_epoch: int
    gap_id: UUID
    page_ordinal: int
    from_token: str
    to_token: str
    limit: int
    timeout_ms: int

    def __post_init__(self) -> None:
        _require_request_identity(
            self.effect_id,
            self.stream_id,
            self.transport,
            self.room_id,
            self.membership_epoch,
        )
        _require_exact(self.gap_id, UUID, "gap_id")
        _require_nonnegative(self.page_ordinal, "page_ordinal")
        _require_nonempty_string(self.from_token, "from_token")
        _require_nonempty_string(self.to_token, "to_token")
        _require_positive(self.limit, "limit")
        _require_nonnegative(self.timeout_ms, "timeout_ms")
        if self.effect_id != _recovery_effect_id_from_gap_id(
            self.gap_id,
            self.page_ordinal,
        ):
            raise ValueError("effect_id does not match recovery page identity")


@dataclass(frozen=True, slots=True)
class RoomHydrationRequest:
    effect_id: UUID
    stream_id: UUID
    transport: TransportKind
    room_id: str
    membership_epoch: int
    timeout_ms: int

    def __post_init__(self) -> None:
        _require_request_identity(
            self.effect_id,
            self.stream_id,
            self.transport,
            self.room_id,
            self.membership_epoch,
        )
        _require_nonnegative(self.timeout_ms, "timeout_ms")
        if self.effect_id != _room_hydration_effect_id(
            self.stream_id,
            self.room_id,
            self.membership_epoch,
        ):
            raise ValueError("effect_id does not match room hydration identity")


@dataclass(frozen=True, slots=True)
class MembershipRequest:
    effect_id: UUID
    stream_id: UUID
    transport: TransportKind
    room_id: str
    membership_epoch: int
    action: MembershipAction
    request_body: bytes
    timeout_ms: int

    def __post_init__(self) -> None:
        _require_request_identity(
            self.effect_id,
            self.stream_id,
            self.transport,
            self.room_id,
            self.membership_epoch,
        )
        _require_canonical_room_id(self.room_id)
        _require_exact(self.action, MembershipAction, "action")
        _require_canonical_json_object(self.request_body, "request_body")
        _require_nonnegative(self.timeout_ms, "timeout_ms")


NetworkEffectRequest = RecoveryRequest | RoomHydrationRequest | MembershipRequest


@dataclass(frozen=True, slots=True)
class PersistedNetworkEffect:
    request: NetworkEffectRequest
    attempt_ordinal: int
    membership_delivery_state: MembershipDeliveryState | None
    prior_delivery_uncertain: bool | None

    def __post_init__(self) -> None:
        if type(self.request) not in (
            RecoveryRequest,
            RoomHydrationRequest,
            MembershipRequest,
        ):
            raise TypeError("request must be a NetworkEffectRequest")
        _require_nonnegative(self.attempt_ordinal, "attempt_ordinal")
        if type(self.request) is MembershipRequest:
            _require_exact(
                self.membership_delivery_state,
                MembershipDeliveryState,
                "membership_delivery_state",
            )
            _require_exact(
                self.prior_delivery_uncertain,
                bool,
                "prior_delivery_uncertain",
            )
            if (
                self.membership_delivery_state
                is MembershipDeliveryState.DISPATCHED_UNCONFIRMED
                and (
                    self.attempt_ordinal == 0
                    or (self.prior_delivery_uncertain and self.attempt_ordinal < 2)
                )
            ):
                raise ValueError(
                    "DISPATCHED_UNCONFIRMED membership effect has an invalid attempt"
                )
            if self.attempt_ordinal == 0 and self.prior_delivery_uncertain:
                raise ValueError(
                    "initial membership effect cannot have prior delivery uncertainty"
                )
        elif (
            self.attempt_ordinal != 0
            or self.membership_delivery_state is not None
            or self.prior_delivery_uncertain is not None
        ):
            raise ValueError(
                "recovery and hydration effects cannot carry delivery state"
            )


@dataclass(frozen=True, slots=True)
class MembershipOperationRef:
    effect_id: UUID
    room_id: str
    membership_epoch: int
    attempt_ordinal: int
    request_sha256: bytes

    def __post_init__(self) -> None:
        _require_exact(self.effect_id, UUID, "effect_id")
        _require_canonical_room_id(self.room_id)
        _require_nonnegative(self.membership_epoch, "membership_epoch")
        _require_positive(self.attempt_ordinal, "attempt_ordinal")
        _require_exact(self.request_sha256, bytes, "request_sha256")
        if len(self.request_sha256) != 32:
            raise ValueError("request_sha256 must be a SHA-256 digest")


@dataclass(frozen=True, slots=True)
class MembershipOperationStatus:
    ref: MembershipOperationRef
    action: MembershipAction
    delivery_state: MembershipDeliveryState
    prior_delivery_uncertain: bool

    def __post_init__(self) -> None:
        _require_exact(self.ref, MembershipOperationRef, "ref")
        _require_exact(self.action, MembershipAction, "action")
        _require_exact(
            self.delivery_state,
            MembershipDeliveryState,
            "delivery_state",
        )
        _require_exact(
            self.prior_delivery_uncertain,
            bool,
            "prior_delivery_uncertain",
        )
        if self.delivery_state is not MembershipDeliveryState.DISPATCHED_UNCONFIRMED:
            raise ValueError(
                "membership operation status must describe an unconfirmed delivery"
            )


@dataclass(frozen=True, slots=True)
class NetworkEffectResult:
    effect_id: UUID
    kind: NetworkEffectKind
    stream_id: UUID
    transport: TransportKind
    attempt_ordinal: int | None
    status_code: int | None
    body: bytes
    failure: NetworkFailureKind | None

    def __post_init__(self) -> None:
        _require_exact(self.effect_id, UUID, "effect_id")
        _require_exact(self.kind, NetworkEffectKind, "kind")
        _require_exact(self.stream_id, UUID, "stream_id")
        _require_exact(self.transport, TransportKind, "transport")
        _require_exact(self.body, bytes, "body")
        if self.kind is NetworkEffectKind.MEMBERSHIP:
            _require_positive(self.attempt_ordinal, "attempt_ordinal")
        elif self.attempt_ordinal is not None:
            raise ValueError(
                "recovery and hydration results cannot carry attempt_ordinal"
            )

        if self.status_code is None:
            _require_exact(self.failure, NetworkFailureKind, "failure")
            if self.body:
                raise ValueError("transport failure body must be empty")
        else:
            _require_exact(self.status_code, int, "status_code")
            if not 100 <= self.status_code <= 599:
                raise ValueError("status_code must be between 100 and 599")
            if self.failure is not None:
                raise ValueError("HTTP result cannot carry a transport failure")
