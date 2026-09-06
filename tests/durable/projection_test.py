"""Restart keeps the live room projection and rebuilds member indexes."""

import json

from nio.durable.projection import encode_member, encode_room, restore_room
from nio.events import Event, InviteEvent
from nio.rooms import MatrixInvitedRoom, MatrixRoom


def state(kind, content, state_key=""):
    return Event.parse_event(
        {
            "type": kind,
            "content": content,
            "state_key": state_key,
            "event_id": "$state:x",
            "sender": "@alice:x",
            "origin_server_ts": 1,
        }
    )


def restart(room, **kwargs):
    metadata = json.loads(json.dumps(encode_room(room, **kwargs)))
    members = {
        user_id: json.loads(json.dumps(encode_member(room, user_id)))
        for user_id in room.users
    }
    return restore_room(room.room_id, metadata, members)


def test_restart_preserves_merged_power_encryption_creators_and_names():
    room = MatrixRoom("!r:x", "@me:x")
    room.handle_event(state("m.room.create", {"room_version": "12"}))
    for user_id in ("@alice:x", "@bob:x"):
        room.handle_membership(
            state(
                "m.room.member", {"membership": "join", "displayname": "Same"}, user_id
            )
        )
    room.handle_event(
        state(
            "m.room.power_levels",
            {"users": {"@bob:x": 75}, "events": {"m.room.topic": 70}},
        )
    )
    room.handle_event(state("m.room.power_levels", {"users": {"@carol:x": 60}}))
    room.handle_event(state("m.room.encryption", {"algorithm": "m.megolm.v1.aes-sha2"}))
    restored = restart(room, membership="join", membership_epoch=3)
    assert restored.encrypted is True
    assert restored.own_user_id == "@me:x"
    assert restored.supports_room_version_12 is True
    assert restored.power_levels.can_user_send_state("@bob:x", "m.room.topic")
    assert not restored.power_levels.can_user_send_state("@carol:x", "m.room.topic")
    assert restored.power_levels.get_user_level("@alice:x") == 2**53 + 1
    assert restored.users["@bob:x"].power_level == 75
    assert restored.user_name("@alice:x") == "Same (@alice:x)"
    assert restored.user_name("@bob:x") == "Same (@bob:x)"
    assert restored.members_synced is False
    restored.handle_membership(
        state("m.room.member", {"membership": "leave"}, "@bob:x")
    )
    assert restored.user_name("@alice:x") == "Same"
    assert encode_member(restored, "@bob:x") is None


def test_departure_delta_removes_persisted_member():
    room = MatrixRoom("!r:x", "@me:x")
    room.add_member("@alice:x", "Alice", None)
    room.add_member("@bob:x", "Bob", None)
    members = {user_id: encode_member(room, user_id) for user_id in room.users}
    room.handle_membership(state("m.room.member", {"membership": "leave"}, "@bob:x"))
    delta = encode_member(room, "@bob:x")
    if delta is None:
        del members["@bob:x"]
    restored = restore_room(room.room_id, encode_room(room), members)
    assert restored.display_name == "Alice"
    assert restored.joined_count == 1
    assert "@bob:x" not in restored.users


def test_invited_room_restores_inviter_and_invited_index():
    room = MatrixInvitedRoom("!r:x", "@me:x")
    room.handle_event(
        InviteEvent.parse_event(
            {
                "type": "m.room.member",
                "sender": "@alice:x",
                "state_key": "@me:x",
                "content": {"membership": "invite"},
            }
        )
    )
    restored = restart(room, membership="invite")
    assert type(restored) is MatrixInvitedRoom
    assert restored.inviter == "@alice:x"
    assert restored.invited_users["@me:x"] is restored.users["@me:x"]
    assert restored.invited_count == 1


def test_member_completeness_requires_explicit_full_persistence():
    room = MatrixRoom("!r:x", "@me:x")
    room.add_member("@alice:x", "Alice", None)
    room.members_synced = True
    assert restart(room).members_synced is False
    assert restart(room, members_complete=True).members_synced is True
    metadata = encode_room(room, members_complete=True)
    assert restore_room(room.room_id, metadata, {}).members_synced is False


def test_message_metadata_does_not_walk_or_embed_members():
    class NoWalkMembers(dict):
        def __iter__(self):
            raise AssertionError("metadata scanned members")

        def values(self):
            raise AssertionError("metadata scanned members")

        def items(self):
            raise AssertionError("metadata scanned members")

    room = MatrixRoom("!r:x", "@me:x")
    room.add_member("@alice:x", "Alice", None)
    room.users = NoWalkMembers(room.users)
    metadata = encode_room(room)
    assert "Alice" not in json.dumps(metadata)
    assert encode_member(room, "@alice:x")["display_name"] == "Alice"


def test_power_metadata_updates_existing_member_rows_without_rewriting_them():
    room = MatrixRoom("!r:x", "@me:x")
    room.add_member("@alice:x", "Alice", None)
    members = {"@alice:x": encode_member(room, "@alice:x")}
    room.handle_event(state("m.room.power_levels", {"users": {"@alice:x": 75}}))
    restored = restore_room(room.room_id, encode_room(room), members)
    assert restored.users["@alice:x"].power_level == 75
    assert restored.power_levels.can_user_ban("@alice:x")


def test_display_summary_account_metadata_and_space_relations_survive_restart():
    from nio.events import AccountDataEvent
    from nio.responses import RoomSummary, UnreadNotifications

    room = MatrixRoom("!r:x", "@me:x")
    room.update_summary(RoomSummary(1, 5, ["@alice:x"]))
    room.update_unread_notifications(UnreadNotifications(8, 2))
    room.handle_account_data(
        AccountDataEvent.parse_event(
            {"type": "m.fully_read", "content": {"event_id": "$read:x"}}
        )
    )
    room.handle_event(state("m.room.topic", {"topic": "Topic"}))
    room.handle_event(state("m.space.parent", {"via": ["x"]}, "!parent:x"))
    restored = restart(room)
    assert restored.member_count == 6
    assert restored.summary.heroes == ["@alice:x"]
    assert restored.fully_read_marker == "$read:x"
    assert restored.unread_notifications == 8
    assert restored.unread_highlights == 2
    assert restored.topic == "Topic"
    assert restored.parents == {"!parent:x"}
