"""Canonical room power must win over older member snapshots on restart."""

import json

import pytest

from .client_test import ROOM, USER, open_session
from .recovery_test import member
from .sliding_test import settings, settle, window


@pytest.mark.asyncio
async def test_deleted_power_state_does_not_restore_old_member_level(tmp_path):
    session = open_session(tmp_path, config=settings())
    power = {
        "type": "m.room.power_levels",
        "state_key": "",
        "event_id": "$power",
        "sender": USER,
        "origin_server_ts": 1,
        "content": {"users": {USER: 75}},
    }
    try:
        await session._accept_response(window(state=[power, member("$join", "join")]))
        await settle(session)
        assert session.client.rooms[ROOM].users[USER].power_level == 75
        deleted = json.dumps(
            {
                "pos": "p2",
                "rooms": {
                    ROOM: {
                        "required_state": [
                            {"type": "m.room.power_levels", "state_key": ""}
                        ]
                    }
                },
            }
        ).encode()
        await session._accept_response(deleted)
        await settle(session)
        room = session.client.rooms[ROOM]
        assert room.users[USER].power_level == 0
        assert room.power_levels.get_user_level(USER) == 0
    finally:
        await session.close()
    reopened = open_session(tmp_path, config=settings())
    try:
        room = reopened.client.rooms[ROOM]
        assert room.power_levels.get_user_level(USER) == 0
        assert room.users[USER].power_level == 0
    finally:
        await reopened.close()
