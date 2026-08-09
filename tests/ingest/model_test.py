import hashlib
from dataclasses import FrozenInstanceError, fields, replace
from enum import StrEnum
from uuid import UUID, uuid5

import pytest

from nio import TimelineEventProvenance as TopLevelTimelineEventProvenance
from nio.ingest import (
    ConsumerBinding,
    ConsumerBootstrap,
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RecordOrigin,
    RoomHydrationStatus,
    RoomMemberSnapshot,
    RoomSnapshot,
    SystemOrigin,
    SystemOriginKind,
    TimelineEventProvenance,
    TransportKind,
)
from nio.ingest.membership import MembershipObservation
import nio.ingest.ports as ingest_ports
from nio.ingest.ports import NetworkRequest, _frame_id_for_response
from nio.ingest.source import RoomSection, RoomSegment
from nio.ingest.state import StagedFrame

JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")
OPERATION_ID = UUID("33333333-3333-3333-3333-333333333333")
FRAME_ID = UUID("66666666-6666-6666-6666-666666666666")
STREAM_ID = UUID("77777777-7777-7777-7777-777777777777")


class ForeignWireValue(StrEnum):
    CLASSIC = "classic"
    FRESH_START = "fresh_start"
    TIMELINE = "timeline"
    LIVE = "live"
    FETCH_FAILED = "fetch_failed"
    JOIN = "join"


def test_wire_enums_have_stable_string_values() -> None:
    assert issubclass(TransportKind, StrEnum)
    assert {member.name: member.value for member in TransportKind} == {
        "CLASSIC": "classic",
        "SLIDING": "sliding",
    }
    assert {member.name: member.value for member in RecordKind} == {
        "TIMELINE": "timeline",
        "STATE": "state",
        "EPHEMERAL": "ephemeral",
        "ROOM_ACCOUNT_DATA": "room_account_data",
        "GLOBAL_ACCOUNT_DATA": "global_account_data",
        "PRESENCE": "presence",
        "TO_DEVICE": "to_device",
        "ROOM_LIFECYCLE": "room_lifecycle",
        "DECRYPTION_UPDATE": "decryption_update",
    }
    assert {member.name: member.value for member in SystemOriginKind} == {
        "FRESH_START": "fresh_start",
        "CONSUMER_RESET": "consumer_reset",
        "MEMBERSHIP_CHANGE": "membership_change",
        "SOURCE_REBIND": "source_rebind",
        "STORE_VALIDATION": "store_validation",
    }
    assert {member.name: member.value for member in LossReason} == {
        "EVENT_LIMIT": "event_limit",
        "FETCH_FAILED": "fetch_failed",
        "BASELINE_LOST": "baseline_lost",
        "UNVERIFIABLE": "unverifiable",
        "CORRUPT_STORED_RECORD": "corrupt_stored_record",
        "OVERSIZED_EVENT": "oversized_event",
    }
    assert {member.name: member.value for member in TimelineEventProvenance} == {
        "LIVE": "live",
        "RECOVERED": "recovered",
        "HISTORY": "history",
    }
    assert {member.name: member.value for member in RoomHydrationStatus} == {
        "PENDING": "pending",
        "READY": "ready",
        "UNAVAILABLE": "unavailable",
    }
    assert TimelineEventProvenance is TopLevelTimelineEventProvenance


def test_wire_dataclasses_are_frozen_and_slotted() -> None:
    values = (
        ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        ConsumerBootstrap(
            OPERATION_ID,
            ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
            1,
            ("!room:example.org",),
            b"digest",
        ),
        RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
        SystemOrigin(SystemOriginKind.FRESH_START, OPERATION_ID),
        EventRecord(
            "$event",
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            b"{}",
            None,
        ),
        RoomMemberSnapshot("@alice:example.org", "join", "Alice", None, 0),
        RoomSnapshot(
            "!room:example.org",
            1,
            "@me:example.org",
            "join",
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            (),
        ),
        LossBoundary(None, None, None, None),
        LossRecord(
            "loss-id",
            RecordOrigin(TransportKind.SLIDING, 1, 2, 3),
            "!room:example.org",
            1,
            LossReason.FETCH_FAILED,
            LossBoundary(None, None, None, None),
            b"{}",
        ),
    )

    for value in values:
        assert not hasattr(value, "__dict__")
        with pytest.raises(FrozenInstanceError):
            setattr(value, fields(value)[0].name, "mutable")


def test_staged_source_response_is_exact_canonical_and_digest_bound() -> None:
    body = b'{"next_batch":"s1"}'
    request = NetworkRequest(
        STREAM_ID,
        TransportKind.CLASSIC,
        4,
        9,
        "GET",
        "/_matrix/client/v3/sync",
        (),
        None,
        30_000,
        b'{"next_batch":null}',
    )
    staged_class = getattr(ingest_ports, "StagedSourceResponse", None)
    assert staged_class is not None
    staged = staged_class(request, body, hashlib.sha256(body).digest())

    assert tuple(field.name for field in fields(staged_class)) == (
        "request",
        "response_body",
        "source_sha256",
    )
    assert not hasattr(staged, "__dict__")
    with pytest.raises(FrozenInstanceError):
        staged.response_body = b"{}"  # type: ignore[misc]
    noncanonical = b'{ "next_batch":"s1"}'
    with pytest.raises(ValueError, match="canonical"):
        staged_class(request, noncanonical, hashlib.sha256(noncanonical).digest())
    with pytest.raises(ValueError, match="digest"):
        staged_class(request, body, b"x" * 32)
    with pytest.raises(ValueError, match="32"):
        staged_class(request, body, b"x")


def test_staged_frame_wraps_only_the_frozen_response_and_derived_identity() -> None:
    body = b'{"next_batch":"s1"}'
    request = NetworkRequest(
        STREAM_ID,
        TransportKind.CLASSIC,
        4,
        9,
        "GET",
        "/_matrix/client/v3/sync",
        (),
        None,
        30_000,
        b'{"next_batch":null}',
    )
    response = ingest_ports.StagedSourceResponse(
        request, body, hashlib.sha256(body).digest()
    )
    frame_id = uuid5(STREAM_ID, f"4:9:{response.source_sha256.hex()}")
    assert _frame_id_for_response(request, response.source_sha256) == frame_id
    frame = StagedFrame(frame_id, response)

    assert tuple(field.name for field in fields(StagedFrame)) == (
        "frame_id",
        "response",
        "staged_revision",
    )
    assert frame.staged_revision == 0
    assert not hasattr(frame, "__dict__")
    with pytest.raises(FrozenInstanceError):
        frame.frame_id = FRAME_ID  # type: ignore[misc]
    with pytest.raises(ValueError, match="frame_id"):
        StagedFrame(FRAME_ID, response)
    with pytest.raises(TypeError, match="response"):
        StagedFrame(frame_id, object())  # type: ignore[arg-type]


def test_room_segment_carries_an_exact_membership_observation() -> None:
    observation = MembershipObservation(
        "join", None, None, None, None, False, False, False, False
    )
    segment = RoomSegment(
        "!room:example.org",
        RoomSection.JOIN,
        (),
        (),
        (),
        False,
        None,
        False,
        False,
        0,
        observation,
    )

    assert tuple(field.name for field in fields(RoomSegment)) == (
        "room_id",
        "section",
        "state_json",
        "timeline_json",
        "room_account_data_json",
        "timeline_limited",
        "timeline_prev_batch",
        "initial",
        "expanded_timeline",
        "live_event_count",
        "membership_observation",
    )
    with pytest.raises(
        TypeError, match="membership_observation must be MembershipObservation"
    ):
        replace(segment, membership_observation=object())  # type: ignore[arg-type]

    conflicting = replace(observation, is_initial=True)
    with pytest.raises(ValueError, match="membership observation initial flag"):
        replace(segment, membership_observation=conflicting)


def test_collection_fields_require_tuples() -> None:
    with pytest.raises(TypeError, match="baseline_room_ids must be a tuple"):
        ConsumerBootstrap(
            OPERATION_ID,
            ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
            1,
            ["!room:example.org"],  # type: ignore[arg-type]
            b"digest",
        )

    with pytest.raises(TypeError, match="members must be a tuple"):
        RoomSnapshot(
            "!room:example.org",
            1,
            "@me:example.org",
            "join",
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            [],  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "make_value",
    [
        lambda: ConsumerBootstrap(
            OPERATION_ID,
            ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
            1,
            (),
            bytearray(b"digest"),  # type: ignore[arg-type]
        ),
        lambda: EventRecord(
            "$event",
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            bytearray(b"{}"),  # type: ignore[arg-type]
            None,
        ),
        lambda: EventRecord(
            "$event",
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            b"{}",
            memoryview(b"{}"),  # type: ignore[arg-type]
        ),
        lambda: RoomSnapshot(
            "!room:example.org",
            1,
            "@me:example.org",
            "join",
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            bytearray(b"{}"),  # type: ignore[arg-type]
            (),
        ),
        lambda: LossRecord(
            "loss-id",
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            LossReason.FETCH_FAILED,
            LossBoundary(None, None, None, None),
            bytearray(b"{}"),  # type: ignore[arg-type]
        ),
    ],
)
def test_public_bytes_fields_require_exact_immutable_bytes(make_value) -> None:
    with pytest.raises(TypeError, match="bytes"):
        make_value()


def test_collection_fields_validate_every_nested_element() -> None:
    with pytest.raises(TypeError, match="baseline_room_ids.*str"):
        ConsumerBootstrap(
            OPERATION_ID,
            ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
            1,
            (object(),),  # type: ignore[arg-type]
            b"digest",
        )

    with pytest.raises(TypeError, match="members.*RoomMemberSnapshot"):
        RoomSnapshot(
            "!room:example.org",
            1,
            "@me:example.org",
            "join",
            False,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            None,
            (object(),),  # type: ignore[arg-type]
        )


@pytest.mark.parametrize(
    "make_value",
    [
        lambda: RecordOrigin(ForeignWireValue.CLASSIC, 1, 2, 3),  # type: ignore[arg-type]
        lambda: SystemOrigin(ForeignWireValue.FRESH_START, OPERATION_ID),  # type: ignore[arg-type]
        lambda: EventRecord(
            "$event",
            ForeignWireValue.TIMELINE,  # type: ignore[arg-type]
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            b"{}",
            None,
        ),
        lambda: EventRecord(
            "$event",
            RecordKind.TIMELINE,
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            1,
            "$event",
            ForeignWireValue.LIVE,  # type: ignore[arg-type]
            b"{}",
            None,
        ),
        lambda: LossRecord(
            "loss-id",
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            1,
            ForeignWireValue.FETCH_FAILED,  # type: ignore[arg-type]
            LossBoundary(None, None, None, None),
            b"{}",
        ),
        lambda: RoomMemberSnapshot(
            "@alice:example.org",
            ForeignWireValue.JOIN,  # type: ignore[arg-type]
            "Alice",
            None,
            0,
        ),
    ],
)
def test_direct_construction_rejects_foreign_str_enum_values(make_value) -> None:
    with pytest.raises(TypeError, match="must be"):
        make_value()


def test_event_record_rejects_system_origin() -> None:
    with pytest.raises(TypeError, match="EventRecord origin must be a RecordOrigin"):
        EventRecord(
            "$event",
            RecordKind.TIMELINE,
            SystemOrigin(SystemOriginKind.FRESH_START, OPERATION_ID),  # type: ignore[arg-type]
            "!room:example.org",
            1,
            1,
            "$event",
            TimelineEventProvenance.LIVE,
            b"{}",
            None,
        )


def test_room_loss_requires_membership_epoch() -> None:
    with pytest.raises(ValueError, match="membership_epoch"):
        LossRecord(
            "loss-id",
            RecordOrigin(TransportKind.CLASSIC, 1, 2, 3),
            "!room:example.org",
            None,  # type: ignore[arg-type]
            LossReason.FETCH_FAILED,
            LossBoundary(None, None, None, None),
            b"{}",
        )


def test_room_snapshot_has_only_deeply_immutable_state() -> None:
    snapshot = RoomSnapshot(
        "!room:example.org",
        3,
        "@me:example.org",
        "join",
        True,
        None,
        None,
        "A topic",
        "mxc://example.org/avatar",
        "invite",
        "12",
        "forbidden",
        b'{"users":{}}',
        (
            RoomMemberSnapshot("@me:example.org", "join", "Me", None, 100),
            RoomMemberSnapshot("@alice:example.org", "join", "Alice", None, 0),
        ),
    )

    assert snapshot.own_user_id == "@me:example.org"
    assert isinstance(snapshot.members, tuple)
    assert all(not isinstance(value, (dict, list, set)) for value in snapshot.members)
    assert {field.name for field in fields(RoomSnapshot)} == {
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
    }


def test_room_snapshot_derives_current_matrix_room_names_from_full_membership() -> None:
    members = (
        RoomMemberSnapshot("@me:example.org", "join", "Me", None, 0),
        RoomMemberSnapshot("@alice:example.org", "join", "Alice", None, 0),
        RoomMemberSnapshot("@malory:example.org", "invite", "Alice", None, 0),
        RoomMemberSnapshot("@gone:example.org", "leave", "Gone", None, 0),
    )
    snapshot = RoomSnapshot(
        "!room:example.org",
        1,
        "@me:example.org",
        "join",
        False,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        None,
        members,
    )

    assert snapshot.member_count == 3
    assert snapshot.is_group
    assert snapshot.display_name == (
        "Alice (@alice:example.org) and Alice (@malory:example.org)"
    )


@pytest.mark.parametrize(
    ("name", "alias", "members", "expected"),
    [
        ("Room name", "#alias:example.org", (), "Room name"),
        (None, "#alias:example.org", (), "#alias:example.org"),
        (
            None,
            None,
            (RoomMemberSnapshot("@me:example.org", "join", "Me", None, 0),),
            "Empty Room",
        ),
        (
            None,
            None,
            tuple(
                [RoomMemberSnapshot("@me:example.org", "join", "Me", None, 0)]
                + [
                    RoomMemberSnapshot(
                        f"@user{index}:example.org",
                        "join",
                        f"User {index}",
                        None,
                        0,
                    )
                    for index in range(1, 8)
                ]
            ),
            "User 1, User 2, User 3, User 4, User 5 and 2 others",
        ),
    ],
)
def test_room_snapshot_display_name_matches_matrix_room_fallbacks(
    name: str | None,
    alias: str | None,
    members: tuple[RoomMemberSnapshot, ...],
    expected: str,
) -> None:
    snapshot = RoomSnapshot(
        "!room:example.org",
        1,
        "@me:example.org",
        "join",
        False,
        name,
        alias,
        None,
        None,
        None,
        None,
        None,
        None,
        members,
    )

    assert snapshot.display_name == expected
    assert snapshot.is_group is (not name and not alias)
