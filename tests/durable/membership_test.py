"""Local membership intent survives an uncertain HTTP response."""

import asyncio
from uuid import UUID

import pytest
from aiohttp import web

from .client_test import ROOM, client, open_session, response
from .runner_test import drain_sync, homeserver

OPERATION = UUID("78b8f003-ce2a-4f57-8e2f-9f85673343a4")


@pytest.mark.asyncio
async def test_local_leave_retries_after_restart_and_emits_ordered_tenure_change(
    tmp_path,
):
    accepted = asyncio.Event()
    release = asyncio.Event()
    stop = asyncio.Event()
    calls = []

    async def sync(request):
        if request.query.get("since"):
            await stop.wait()
        return web.Response(body=response())

    async def membership(request):
        calls.append((request.method, request.path, await request.text()))
        if len(calls) == 1:
            accepted.set()
            await release.wait()
        return web.json_response({})

    async with homeserver(sync, membership=membership) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        runner = asyncio.create_task(session.run())
        try:
            await drain_sync(session)
            operation = asyncio.create_task(
                session.change_membership(
                    operation_id=OPERATION,
                    room_id=ROOM,
                    previous_membership="join",
                    previous_epoch=0,
                    current_membership="leave",
                )
            )
            async with asyncio.timeout(5):
                await accepted.wait()
            operation.cancel()
            with pytest.raises(asyncio.CancelledError):
                await operation
        finally:
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)
        release.set()
        nio_client = client()
        nio_client.homeserver = url
        reopened = open_session(tmp_path, nio_client)
        runner = asyncio.create_task(reopened.run())
        try:
            async with asyncio.timeout(5):
                await reopened.wait_for_work()
                batch = await reopened.next_batch()
            changes = [
                record.membership for record in batch.records if record.membership
            ]
            assert [
                (x.previous, x.current, x.current_epoch, x.source) for x in changes
            ] == [("join", "leave", 1, "local")]
            assert (
                calls == [("POST", f"/_matrix/client/v3/rooms/{ROOM}/leave", "{}")] * 2
            )
            await reopened.ack(batch)
            await reopened.wait_for_membership_idle()
            await reopened.quiesce()
            await runner
            assert ROOM not in nio_client.rooms
        finally:
            stop.set()
            await reopened.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_stale_local_expectation_cannot_send_or_advance_membership(tmp_path):
    session = open_session(tmp_path)
    await session._accept_response(response())
    try:
        result = await session.change_membership(
            operation_id=OPERATION,
            room_id=ROOM,
            previous_membership="join",
            previous_epoch=7,
            current_membership="leave",
        )
        assert result is False
        assert session.client.rooms[ROOM].users
    finally:
        await session.close()
