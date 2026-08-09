from dataclasses import FrozenInstanceError, fields
from enum import StrEnum
from uuid import UUID

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

JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")
OPERATION_ID = UUID("33333333-3333-3333-3333-333333333333")


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
