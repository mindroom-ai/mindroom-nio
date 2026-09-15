"""A to-device source never widens its ownership to personal rooms."""

import asyncio
import json
from uuid import uuid4

import pytest
from aiohttp import web

from nio.durable import DurableSyncConfig
from nio.exceptions import LocalProtocolError

from .client_test import ROOM, client, open_session, response
from .runner_test import homeserver


@pytest.mark.asyncio
async def test_to_device_source_keeps_filter_after_cursor_and_restart(tmp_path):
    config = DurableSyncConfig(to_device_only=True)
    session = open_session(tmp_path, config=config)
    await session._accept_response(b'{"next_batch":"s1"}')
    with session._store.transaction():
        session._store.finish_input()
    await session.close()
    await session.client.close()
    polled = asyncio.Event()
    release = asyncio.Event()
    seen = []

    async def sync(request):
        seen.append(dict(request.query))
        polled.set()
        await release.wait()
        return web.json_response({"next_batch": "s2"})

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, config)
        runner = asyncio.create_task(session.run())
        try:
            async with asyncio.timeout(5):
                await polled.wait()
            assert seen[0]["since"] == "s1"
            assert "full_state" not in seen[0]
            assert json.loads(seen[0]["filter"]) == {
                "room": {"rooms": []},
                "presence": {"types": []},
                "account_data": {"types": []},
            }
            await session.quiesce()
            await runner
        finally:
            release.set()
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_to_device_source_does_not_project_unrequested_rooms(tmp_path):
    session = open_session(tmp_path, config=DurableSyncConfig(to_device_only=True))
    try:
        await session._accept_response(response())
        assert await session.next_batch() is None
        assert ROOM not in session.client.rooms
        assert session._store.load_room_metadata() == {}
        assert not session._recovery.needs_full_state()
        with pytest.raises(LocalProtocolError, match="to-device"):
            await session.change_membership(
                operation_id=uuid4(),
                room_id=ROOM,
                previous_membership=None,
                previous_epoch=0,
                current_membership="join",
            )
    finally:
        await session.close()
        await session.client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("first_mode", [False, True])
async def test_to_device_source_cannot_change_existing_stream_ownership(
    tmp_path, first_mode
):
    session = open_session(
        tmp_path, config=DurableSyncConfig(to_device_only=first_mode)
    )
    await session._capture_response(response())
    await session.close()
    await session.client.close()
    with pytest.raises(LocalProtocolError, match="transport"):
        open_session(tmp_path, config=DurableSyncConfig(to_device_only=not first_mode))
    session = open_session(
        tmp_path, config=DurableSyncConfig(to_device_only=first_mode)
    )
    try:
        assert session._store.input is not None
    finally:
        await session.close()
        await session.client.close()


def test_to_device_source_rejects_custom_filter():
    with pytest.raises(ValueError, match="to-device"):
        DurableSyncConfig(to_device_only=True, sync_filter="saved-filter")
