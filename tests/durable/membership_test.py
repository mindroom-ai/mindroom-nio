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
            await reopened.quiesce()
            assert reopened._read_local_intent()["sequence"] == batch.sequence
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


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_acknowledged_local_intent_gates_next_command_until_echo(
    tmp_path, restart
):
    import json

    from nio import TimelineEventProvenance

    from .recovery_test import baseline, member, message

    queue = asyncio.Queue()
    calls = []

    async def sync(request):
        return web.Response(body=await queue.get())

    async def membership(request):
        calls.append(request.path)
        return web.json_response({})

    def echo(token, section, events):
        return json.dumps(
            {
                "next_batch": token,
                "rooms": {
                    section: {
                        ROOM: {"state": {"events": []}, "timeline": {"events": events}}
                    }
                },
            }
        ).encode()

    async with homeserver(sync, membership=membership) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        assert await session.change_membership(
            operation_id=OPERATION,
            room_id=ROOM,
            previous_membership="join",
            previous_epoch=0,
            current_membership="leave",
        )
        await session.ack(await session.next_batch())
        if restart:
            await session.close()
            await nio_client.close()
            nio_client = client()
            nio_client.homeserver = url
            session = open_session(tmp_path, nio_client)
        runner = asyncio.create_task(session.run())
        operation = asyncio.create_task(
            session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=1,
                current_membership="join",
            )
        )
        try:
            # The second command must wait even though its predecessor was ACKed.
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(asyncio.shield(operation), 0.05)
            assert len(calls) == 1
            await queue.put(
                echo(
                    "s2",
                    "leave",
                    [
                        member("$old-join", "join"),
                        message("$uncertain"),
                        member("$leave-echo", "leave"),
                    ],
                )
            )
            records = await drain_sync(session)
            assert not [r for r in records if r.membership]
            assert (
                next(
                    r for r in records if r.source.get("event_id") == "$uncertain"
                ).provenance
                is TimelineEventProvenance.HISTORY
            )
            assert await asyncio.wait_for(operation, 5)
            batch = await session.next_batch()
            assert batch.records[0].membership.current_epoch == 1
            await session.ack(batch)
            await queue.put(
                echo(
                    "s3",
                    "join",
                    [
                        member("$old-leave", "leave"),
                        message("$before-join"),
                        member("$join-echo", "join"),
                        member("$real-leave", "leave"),
                        member("$real-rejoin", "join"),
                    ],
                )
            )
            records = await drain_sync(session)
            assert [
                (r.source.get("event_id"), r.membership.current_epoch)
                for r in records
                if r.membership
            ] == [("$real-leave", 2), ("$real-rejoin", 2)]
            await session.wait_for_membership_idle()
            assert len(calls) == 2
            assert session._metadata[ROOM]["membership_epoch"] == 2
            await session.quiesce()
            await runner
        finally:
            operation.cancel()
            await asyncio.gather(operation, return_exceptions=True)
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("explicit_echo", [False, True])
async def test_local_leave_waits_through_old_joined_state_and_accepts_standalone_echo(
    tmp_path, explicit_echo
):
    import json

    from .recovery_test import baseline, member

    async def membership(request):
        return web.json_response({})

    async with homeserver(None, membership=membership) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        try:
            assert await session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=0,
                current_membership="leave",
            )
            await session.ack(await session.next_batch())
            await session._accept_response(
                response(token="old", messages=0), full_state=True
            )
            while batch := await session.next_batch():
                assert not [r for r in batch.records if r.membership]
                await session.ack(batch)
            assert ROOM not in nio_client.rooms
            assert not session._metadata[ROOM]["baseline"]
            with pytest.raises(TimeoutError):
                await asyncio.wait_for(session.wait_for_membership_idle(), 0.05)
            assert not session._read_local_intent().get("observed")
            with session._store.transaction():
                session._store.finish_input()
            body = {
                "next_batch": "echo",
                "rooms": {
                    "leave": {
                        ROOM: {
                            "state": {"events": []},
                            "timeline": {
                                "events": (
                                    [member("$echo", "leave")] if explicit_echo else []
                                )
                            },
                        }
                    }
                },
            }
            await session._accept_response(json.dumps(body).encode())
            while batch := await session.next_batch():
                assert not [r for r in batch.records if r.membership]
                await session.ack(batch)
            await asyncio.wait_for(session.wait_for_membership_idle(), 5)
            assert session._metadata[ROOM]["membership_epoch"] == 1
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("delay_http", [False, True])
async def test_local_command_discards_completed_poll_before_acceptance(
    tmp_path, delay_http
):
    import json

    from .recovery_test import baseline, member

    session = open_session(tmp_path)
    await baseline(session)
    poll_finished = asyncio.Event()
    http_started = asyncio.Event()
    release_http = asyncio.Event()
    stop_poll = asyncio.Event()
    polls = 0

    async def request(_method, path, *_args, **_kwargs):
        nonlocal polls
        if "/sync" in path:
            polls += 1
            if polls > 1:
                await stop_poll.wait()
            body = json.loads(response(token="before-local-http", messages=0))
            body["rooms"]["join"][ROOM]["timeline"]["events"] = [
                member("$pre-command-leave", "leave")
            ]
            poll_finished.set()
            return json.dumps(body).encode()
        http_started.set()
        if delay_http:
            await release_http.wait()
        return b"{}"

    session._transport.request = request
    session._maintain_crypto = lambda: asyncio.sleep(0)

    async def local():
        # Woken by the poll task before its awaiting runner gets CPU again.
        await poll_finished.wait()
        return await session.change_membership(
            operation_id=OPERATION,
            room_id=ROOM,
            previous_membership="join",
            previous_epoch=0,
            current_membership="leave",
        )

    command = asyncio.create_task(local())
    runner = asyncio.create_task(session.run())
    try:
        await asyncio.wait_for(http_started.wait(), 5)
        await asyncio.sleep(0)
        assert session.cursor == "s1"
        assert session._store.input is None
        assert session._metadata[ROOM]["membership"] == (
            "join" if delay_http else "leave"
        )
        release_http.set()
        assert await asyncio.wait_for(command, 5)
        batch = await session.next_batch()
        assert [
            (r.membership.source, r.membership.current_epoch) for r in batch.records
        ] == [("local", 1)]
        await session.ack(batch)
        assert not session._read_local_intent().get("observed")
        await session.quiesce()
        await runner
    finally:
        release_http.set()
        stop_poll.set()
        command.cancel()
        await asyncio.gather(command, return_exceptions=True)
        await session.close()
        await asyncio.gather(runner, return_exceptions=True)
