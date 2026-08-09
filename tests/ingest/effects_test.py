"""Pure durable-network-effect value contract tests."""

from dataclasses import FrozenInstanceError, fields, replace
from enum import StrEnum
from uuid import UUID, uuid5

import pytest

from nio.ingest import TransportKind
from nio.ingest.effects import (
    MembershipAction,
    MembershipDeliveryState,
    MembershipOperationRef,
    MembershipOperationResolution,
    MembershipOperationResolutionOutcome,
    MembershipOperationStatus,
    MembershipRequest,
    NetworkEffectKind,
    NetworkEffectResult,
    PersistedNetworkEffect,
    RecoveryRequest,
    RoomHydrationRequest,
)
from nio.ingest.ports import NetworkFailureKind
from nio.ingest.source import canonical_json, load_json

STREAM_ID = UUID("33333333-3333-3333-3333-333333333333")
GAP_ID = UUID("44444444-4444-4444-4444-444444444444")
RECOVERY_EFFECT_ID = UUID("b04e2186-5aa7-5dda-89b4-ad06e4735295")
HYDRATION_EFFECT_ID = UUID("88f8dfbc-f0e5-5916-b727-8fee8e00be20")
MEMBERSHIP_EFFECT_ID = UUID("55555555-5555-5555-5555-555555555555")
OTHER_ID = UUID("66666666-6666-6666-6666-666666666666")


class ForeignWireValue(StrEnum):
    CLASSIC = "classic"
    JOIN = "join"
    READY = "ready"
    MEMBERSHIP = "membership"
    TIMEOUT = "timeout"


def _recovery() -> RecoveryRequest:
    return RecoveryRequest(
        RECOVERY_EFFECT_ID,
        STREAM_ID,
        TransportKind.CLASSIC,
        "!room:example.org",
        9,
        GAP_ID,
        7,
        "from-token",
        "to-token",
        100,
        30_000,
    )


def _hydration() -> RoomHydrationRequest:
    return RoomHydrationRequest(
        HYDRATION_EFFECT_ID,
        STREAM_ID,
        TransportKind.CLASSIC,
        "!room:example.org",
        9,
        30_000,
    )


def _membership() -> MembershipRequest:
    return MembershipRequest(
        MEMBERSHIP_EFFECT_ID,
        STREAM_ID,
        TransportKind.CLASSIC,
        "!room:example.org",
        9,
        MembershipAction.JOIN,
        b'{"reason":"maintenance","via":["one.example","two.example"]}',
        30_000,
    )


def test_network_effect_enums_and_persisted_shape_are_exact() -> None:
    for enum_type in (
        MembershipAction,
        MembershipDeliveryState,
        MembershipOperationResolution,
        MembershipOperationResolutionOutcome,
        NetworkEffectKind,
    ):
        assert issubclass(enum_type, StrEnum)
    assert {member.name: member.value for member in MembershipAction} == {
        "JOIN": "join",
        "LEAVE": "leave",
        "FORGET": "forget",
    }
    assert {member.name: member.value for member in MembershipDeliveryState} == {
        "READY": "ready",
        "DISPATCHED_UNCONFIRMED": "dispatched_unconfirmed",
    }
    assert {member.name: member.value for member in NetworkEffectKind} == {
        "RECOVERY": "recovery",
        "ROOM_HYDRATION": "room_hydration",
        "MEMBERSHIP": "membership",
    }
    assert {member.name: member.value for member in MembershipOperationResolution} == {
        "RETRY": "retry",
        "SUPERSEDE": "supersede",
    }
    assert {
        member.name: member.value for member in MembershipOperationResolutionOutcome
    } == {"READY": "ready", "SUPERSEDED": "superseded", "ABSENT": "absent"}
    expected_fields = {
        RecoveryRequest: (
            "effect_id",
            "stream_id",
            "transport",
            "room_id",
            "membership_epoch",
            "gap_id",
            "page_ordinal",
            "from_token",
            "to_token",
            "limit",
            "timeout_ms",
        ),
        RoomHydrationRequest: (
            "effect_id",
            "stream_id",
            "transport",
            "room_id",
            "membership_epoch",
            "timeout_ms",
        ),
        MembershipRequest: (
            "effect_id",
            "stream_id",
            "transport",
            "room_id",
            "membership_epoch",
            "action",
            "request_body",
            "timeout_ms",
        ),
        PersistedNetworkEffect: (
            "request",
            "attempt_ordinal",
            "membership_delivery_state",
            "prior_delivery_uncertain",
        ),
        MembershipOperationRef: (
            "effect_id",
            "room_id",
            "membership_epoch",
            "attempt_ordinal",
            "request_sha256",
        ),
        MembershipOperationStatus: (
            "ref",
            "action",
            "delivery_state",
            "prior_delivery_uncertain",
        ),
        NetworkEffectResult: (
            "effect_id",
            "kind",
            "stream_id",
            "transport",
            "attempt_ordinal",
            "status_code",
            "body",
            "failure",
        ),
    }
    for value_type, expected in expected_fields.items():
        assert tuple(field.name for field in fields(value_type)) == expected


def test_recovery_and_hydration_requests_enforce_literal_deterministic_ids() -> None:
    recovery = _recovery()
    hydration = _hydration()

    assert recovery.effect_id == RECOVERY_EFFECT_ID
    assert hydration.effect_id == HYDRATION_EFFECT_ID
    for value in (recovery, hydration):
        assert not hasattr(value, "__dict__")
        with pytest.raises(FrozenInstanceError):
            value.room_id = "!changed:example.org"


def test_membership_request_is_frozen_slotted_and_keeps_canonical_body() -> None:
    request = _membership()

    assert request.request_body == (
        b'{"reason":"maintenance","via":["one.example","two.example"]}'
    )
    assert not hasattr(request, "__dict__")
    with pytest.raises(FrozenInstanceError):
        request.attempt_ordinal = 1


def test_effect_ids_match_frozen_wire_formulas() -> None:
    assert uuid5(GAP_ID, "page:7") == RECOVERY_EFFECT_ID
    assert uuid5(STREAM_ID, "hydrate:!room:example.org:9") == HYDRATION_EFFECT_ID


@pytest.mark.parametrize(
    ("changes", "error"),
    (
        ({"effect_id": str(RECOVERY_EFFECT_ID)}, TypeError),
        ({"stream_id": str(STREAM_ID)}, TypeError),
        ({"transport": ForeignWireValue.CLASSIC}, TypeError),
        ({"room_id": 1}, TypeError),
        ({"room_id": ""}, ValueError),
        ({"membership_epoch": True}, TypeError),
        ({"membership_epoch": -1}, ValueError),
        ({"gap_id": str(GAP_ID)}, TypeError),
        ({"page_ordinal": False}, TypeError),
        ({"page_ordinal": -1}, ValueError),
        ({"from_token": 1}, TypeError),
        ({"from_token": ""}, ValueError),
        ({"to_token": 1}, TypeError),
        ({"to_token": ""}, ValueError),
        ({"limit": True}, TypeError),
        ({"limit": 0}, ValueError),
        ({"timeout_ms": False}, TypeError),
        ({"timeout_ms": -1}, ValueError),
    ),
)
def test_recovery_request_rejects_inexact_or_invalid_fields(
    changes: dict[str, object],
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        replace(_recovery(), **changes)


@pytest.mark.parametrize(
    "changes",
    (
        {"gap_id": OTHER_ID},
        {"page_ordinal": 8},
    ),
)
def test_recovery_request_rejects_stale_deterministic_id(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="effect_id"):
        replace(_recovery(), **changes)


@pytest.mark.parametrize(
    ("changes", "error"),
    (
        ({"effect_id": str(HYDRATION_EFFECT_ID)}, TypeError),
        ({"stream_id": str(STREAM_ID)}, TypeError),
        ({"transport": ForeignWireValue.CLASSIC}, TypeError),
        ({"room_id": 1}, TypeError),
        ({"room_id": ""}, ValueError),
        ({"membership_epoch": True}, TypeError),
        ({"membership_epoch": -1}, ValueError),
        ({"timeout_ms": False}, TypeError),
        ({"timeout_ms": -1}, ValueError),
    ),
)
def test_hydration_request_rejects_inexact_or_invalid_fields(
    changes: dict[str, object],
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        replace(_hydration(), **changes)


@pytest.mark.parametrize(
    "changes",
    (
        {"stream_id": OTHER_ID},
        {"room_id": "!other:example.org"},
        {"membership_epoch": 10},
    ),
)
def test_hydration_request_rejects_stale_deterministic_id(
    changes: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="effect_id"):
        replace(_hydration(), **changes)


@pytest.mark.parametrize(
    "room_id",
    (
        "",
        "room:example.org",
        "#alias:example.org",
        "!",
        "!room\0suffix",
        "!\ud800",
        "!" + ("a" * 255),
    ),
)
def test_membership_request_requires_a_canonical_room_id(room_id: str) -> None:
    with pytest.raises(ValueError, match="room_id"):
        replace(_membership(), room_id=room_id)


@pytest.mark.parametrize(
    "room_id",
    (
        "!opaque_id",
        "!room:example.org",
        "!room:",
        "!opaque room",
        "!" + ("a" * 254),
    ),
)
def test_membership_request_accepts_opaque_room_ids(room_id: str) -> None:
    assert replace(_membership(), room_id=room_id).room_id == room_id


@pytest.mark.parametrize(
    "request_body",
    (
        b"",
        b"[]",
        b"{ }",
        b'{"via":[],"reason":"x"}',
        b'{"number":1.5}',
        b'{"reason":"one","reason":"two"}',
        b'{"value":NaN}',
        b"\xff",
    ),
)
def test_membership_request_rejects_noncanonical_json_object_body(
    request_body: bytes,
) -> None:
    with pytest.raises(ValueError, match="request_body"):
        replace(_membership(), request_body=request_body)


def test_membership_body_uses_the_source_matrix_canonical_json_limits() -> None:
    canonical = '{"reason":"é","via":["one.example"]}'.encode()
    assert canonical_json(load_json(canonical, "source JSON")) == canonical
    assert replace(_membership(), request_body=canonical).request_body == canonical

    too_deep = b'{"value":' + (b"[" * 257) + b"0" + (b"]" * 257) + b"}"
    with pytest.raises(ValueError, match="nesting"):
        load_json(too_deep, "source JSON")
    with pytest.raises(ValueError, match="request_body"):
        replace(_membership(), request_body=too_deep)


@pytest.mark.parametrize(
    ("changes", "error"),
    (
        ({"effect_id": str(MEMBERSHIP_EFFECT_ID)}, TypeError),
        ({"stream_id": str(STREAM_ID)}, TypeError),
        ({"transport": ForeignWireValue.CLASSIC}, TypeError),
        ({"room_id": 1}, TypeError),
        ({"membership_epoch": False}, TypeError),
        ({"membership_epoch": -1}, ValueError),
        ({"action": ForeignWireValue.JOIN}, TypeError),
        ({"request_body": bytearray(b"{}")}, TypeError),
        ({"timeout_ms": False}, TypeError),
        ({"timeout_ms": -1}, ValueError),
    ),
)
def test_membership_request_rejects_inexact_or_invalid_fields(
    changes: dict[str, object],
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        replace(_membership(), **changes)


@pytest.mark.parametrize(
    ("effect", "attempt", "delivery", "prior"),
    (
        (_recovery(), 0, None, None),
        (_hydration(), 0, None, None),
        (_membership(), 0, MembershipDeliveryState.READY, False),
        (_membership(), 2, MembershipDeliveryState.READY, False),
        (_membership(), 1, MembershipDeliveryState.READY, True),
        (
            _membership(),
            1,
            MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
            False,
        ),
        (
            _membership(),
            3,
            MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
            True,
        ),
    ),
)
def test_persisted_effect_accepts_only_reachable_delivery_states(
    effect: RecoveryRequest | RoomHydrationRequest | MembershipRequest,
    attempt: int,
    delivery: MembershipDeliveryState | None,
    prior: bool | None,
) -> None:
    value = PersistedNetworkEffect(effect, attempt, delivery, prior)

    assert not hasattr(value, "__dict__")


@pytest.mark.parametrize(
    ("effect", "attempt", "delivery", "prior", "error"),
    (
        (object(), 0, None, None, TypeError),
        (_recovery(), True, None, None, TypeError),
        (_recovery(), -1, None, None, ValueError),
        (_recovery(), 1, None, None, ValueError),
        (_hydration(), 0, MembershipDeliveryState.READY, None, ValueError),
        (_recovery(), 0, None, False, ValueError),
        (_membership(), 0, None, False, TypeError),
        (_membership(), 0, MembershipDeliveryState.READY, None, TypeError),
        (
            _membership(),
            0,
            MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
            False,
            ValueError,
        ),
        (_membership(), 0, MembershipDeliveryState.READY, True, ValueError),
        (
            _membership(),
            1,
            MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
            True,
            ValueError,
        ),
        (_membership(), 1, ForeignWireValue.READY, False, TypeError),
    ),
)
def test_persisted_effect_rejects_inexact_or_impossible_delivery_state(
    effect: object,
    attempt: object,
    delivery: object,
    prior: object,
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        PersistedNetworkEffect(effect, attempt, delivery, prior)  # type: ignore[arg-type]


def _operation_ref() -> MembershipOperationRef:
    return MembershipOperationRef(
        MEMBERSHIP_EFFECT_ID,
        "!room:example.org",
        9,
        1,
        b"r" * 32,
    )


def test_membership_operation_ref_and_uncertain_status_are_exact() -> None:
    ref = _operation_ref()
    status = MembershipOperationStatus(
        ref,
        MembershipAction.JOIN,
        MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
        False,
    )

    assert status.ref == ref
    for value in (ref, status):
        assert not hasattr(value, "__dict__")


@pytest.mark.parametrize(
    ("changes", "error"),
    (
        ({"effect_id": str(MEMBERSHIP_EFFECT_ID)}, TypeError),
        ({"room_id": "#alias:example.org"}, ValueError),
        ({"membership_epoch": True}, TypeError),
        ({"membership_epoch": -1}, ValueError),
        ({"attempt_ordinal": False}, TypeError),
        ({"attempt_ordinal": 0}, ValueError),
        ({"request_sha256": bytearray(b"r" * 32)}, TypeError),
        ({"request_sha256": b"short"}, ValueError),
    ),
)
def test_membership_operation_ref_rejects_inexact_or_stale_shape(
    changes: dict[str, object],
    error: type[Exception],
) -> None:
    with pytest.raises(error):
        replace(_operation_ref(), **changes)


@pytest.mark.parametrize(
    "changes",
    (
        {"ref": object()},
        {"action": ForeignWireValue.JOIN},
        {"delivery_state": MembershipDeliveryState.READY},
        {"delivery_state": ForeignWireValue.READY},
        {"prior_delivery_uncertain": 0},
    ),
)
def test_uncertain_membership_status_rejects_wrong_shape(
    changes: dict[str, object],
) -> None:
    status = MembershipOperationStatus(
        _operation_ref(),
        MembershipAction.JOIN,
        MembershipDeliveryState.DISPATCHED_UNCONFIRMED,
        False,
    )
    with pytest.raises((TypeError, ValueError)):
        replace(status, **changes)


@pytest.mark.parametrize(
    "result",
    (
        NetworkEffectResult(
            RECOVERY_EFFECT_ID,
            NetworkEffectKind.RECOVERY,
            STREAM_ID,
            TransportKind.CLASSIC,
            None,
            200,
            b"{}",
            None,
        ),
        NetworkEffectResult(
            HYDRATION_EFFECT_ID,
            NetworkEffectKind.ROOM_HYDRATION,
            STREAM_ID,
            TransportKind.CLASSIC,
            None,
            None,
            b"",
            NetworkFailureKind.TIMEOUT,
        ),
        NetworkEffectResult(
            MEMBERSHIP_EFFECT_ID,
            NetworkEffectKind.MEMBERSHIP,
            STREAM_ID,
            TransportKind.CLASSIC,
            1,
            403,
            b'{"errcode":"M_FORBIDDEN"}',
            None,
        ),
    ),
)
def test_network_effect_result_accepts_exact_http_or_transport_outcome(
    result: NetworkEffectResult,
) -> None:
    assert not hasattr(result, "__dict__")


@pytest.mark.parametrize(
    "changes",
    (
        {"effect_id": str(RECOVERY_EFFECT_ID)},
        {"kind": ForeignWireValue.MEMBERSHIP},
        {"stream_id": str(STREAM_ID)},
        {"transport": ForeignWireValue.CLASSIC},
        {"attempt_ordinal": True},
        {"attempt_ordinal": -1},
        {"attempt_ordinal": 1},
        {"status_code": True},
        {"status_code": 99},
        {"status_code": 600},
        {"status_code": None, "failure": None},
        {"failure": ForeignWireValue.TIMEOUT},
        {"failure": NetworkFailureKind.TIMEOUT},
        {"status_code": None, "failure": NetworkFailureKind.TIMEOUT, "body": b"x"},
        {"body": bytearray(b"{}")},
    ),
)
def test_nonmembership_result_rejects_inexact_or_contradictory_fields(
    changes: dict[str, object],
) -> None:
    result = NetworkEffectResult(
        RECOVERY_EFFECT_ID,
        NetworkEffectKind.RECOVERY,
        STREAM_ID,
        TransportKind.CLASSIC,
        None,
        200,
        b"{}",
        None,
    )
    with pytest.raises((TypeError, ValueError)):
        replace(result, **changes)


@pytest.mark.parametrize("attempt", (None, 0, True, -1))
def test_membership_result_requires_a_positive_exact_attempt(attempt: object) -> None:
    with pytest.raises((TypeError, ValueError)):
        NetworkEffectResult(
            MEMBERSHIP_EFFECT_ID,
            NetworkEffectKind.MEMBERSHIP,
            STREAM_ID,
            TransportKind.CLASSIC,
            attempt,  # type: ignore[arg-type]
            200,
            b"{}",
            None,
        )
