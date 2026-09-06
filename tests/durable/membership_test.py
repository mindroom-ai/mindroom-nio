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
async def test_acknowledged_local_intent_gates_next_command_until_boundary(
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

    def boundary(token, section, events):
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
                boundary(
                    "s2",
                    "leave",
                    [
                        member("$old-join", "join"),
                        message("$uncertain"),
                        member("$final-leave", "leave"),
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
                boundary(
                    "s3",
                    "join",
                    [
                        member("$old-leave", "leave"),
                        message("$before-join"),
                        member("$intermediate-join", "join"),
                        member("$real-leave", "leave"),
                        member("$real-rejoin", "join"),
                    ],
                )
            )
            records = await drain_sync(session)
            assert not [r for r in records if r.membership]
            assert {
                r.provenance
                for r in records
                if r.source.get("event_id")
                in {
                    "$old-leave",
                    "$before-join",
                    "$intermediate-join",
                    "$real-leave",
                    "$real-rejoin",
                }
            } == {TimelineEventProvenance.HISTORY}
            await session.wait_for_membership_idle()
            assert len(calls) == 2
            assert session._metadata[ROOM]["membership_epoch"] == 1
            await queue.put(
                boundary(
                    "s4",
                    "join",
                    [
                        member("$later-leave", "leave"),
                        member("$later-rejoin", "join"),
                    ],
                )
            )
            records = await drain_sync(session)
            assert [
                (r.source.get("event_id"), r.membership.current_epoch)
                for r in records
                if r.membership
            ] == [("$later-leave", 2), ("$later-rejoin", 2)]
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
@pytest.mark.parametrize("explicit_membership", [False, True])
async def test_local_leave_accepts_authoritative_leave_boundary(
    tmp_path, explicit_membership
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
            body = {
                "next_batch": "boundary",
                "rooms": {
                    "leave": {
                        ROOM: {
                            "state": {"events": []},
                            "timeline": {
                                "events": (
                                    [member("$reported-leave", "leave")]
                                    if explicit_membership
                                    else []
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
async def test_local_leave_ignores_membership_cycle_before_final_boundary(tmp_path):
    import json

    from .recovery_test import baseline, limited, member

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def membership(request):
        if request.path.endswith("/leave"):
            return web.json_response({})
        raise AssertionError("unknown-baseline history must not be fetched")

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
        body = json.loads(limited(section="leave"))
        body["rooms"]["leave"][ROOM]["timeline"]["events"] = [
            member("$earlier-leave", "leave"),
            member("$earlier-rejoin", "join"),
            member("$final-leave", "leave"),
        ]
        session._capture_response(json.dumps(body).encode())
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            assert not [record for record in records if record.membership]
            assert session._metadata[ROOM]["membership"] == "leave"
            assert session._metadata[ROOM]["membership_epoch"] == 1
            await session.wait_for_membership_idle()
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_local_leave_reconciles_authoritative_rejoin_once(tmp_path):
    import json

    from nio import TimelineEventProvenance
    from nio.durable.model import RecordKind

    from .recovery_test import baseline, member

    queue = asyncio.Queue()

    async def sync(request):
        return web.Response(body=await queue.get())

    async def membership(request):
        return web.json_response({})

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
        runner = asyncio.create_task(session.run())
        body = json.loads(response(token="after-rejoin"))
        body["rooms"]["join"][ROOM]["timeline"]["events"].insert(
            0, member("$stale-member", "join", "@old:example.org")
        )
        await queue.put(json.dumps(body).encode())
        try:
            records = await drain_sync(session)
            changes = [record.membership for record in records if record.membership]
            assert [
                (change.previous, change.current, change.current_epoch)
                for change in changes
            ] == [("leave", "join", 1)]
            assert {
                record.provenance
                for record in records
                if record.kind is RecordKind.TIMELINE
            } == {TimelineEventProvenance.HISTORY}
            assert session._metadata[ROOM]["membership"] == "join"
            assert session._metadata[ROOM]["membership_epoch"] == 1
            assert not session._metadata[ROOM]["baseline"]
            assert "@old:example.org" not in session.client.rooms[ROOM].users
            await session.wait_for_membership_idle()
            await session.quiesce()
            await runner
        finally:
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)
    reopened = open_session(tmp_path)
    try:
        assert "@old:example.org" not in reopened.client.rooms[ROOM].users
        assert not reopened._metadata[ROOM]["baseline"]
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_local_leave_reconciles_reinvite_before_next_local_join(tmp_path):
    import json
    from types import SimpleNamespace

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
            invite = {
                "next_batch": "after-invite",
                "rooms": {
                    "invite": {
                        ROOM: {
                            "invite_state": {"events": [member("$invite", "invite")]}
                        }
                    }
                },
            }
            await session._accept_response(json.dumps(invite).encode())
            records = []
            while batch := await session.next_batch():
                records.extend(batch.records)
                await session.ack(batch)
            with session._store.transaction():
                session._store.finish_input()
            changes = [record.membership for record in records if record.membership]
            assert [
                (change.previous, change.current, change.current_epoch)
                for change in changes
            ] == [("leave", "invite", 1)]
            await session.wait_for_membership_idle()
            assert await session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=1,
                current_membership="join",
            )
            await session.ack(await session.next_batch())
            assert session.client.olm
            session.client.olm.outbound_group_sessions[ROOM] = SimpleNamespace(
                shared=True
            )
            invite["next_batch"] = "after-second-invite"
            await session._accept_response(json.dumps(invite).encode())
            records = []
            while batch := await session.next_batch():
                records.extend(batch.records)
                await session.ack(batch)
            changes = [record.membership for record in records if record.membership]
            assert [
                (change.previous, change.current, change.current_epoch)
                for change in changes
            ] == [("join", "invite", 2)]
            assert ROOM not in session.client.rooms
            assert ROOM in session.client.invited_rooms
            assert ROOM not in session.client.olm.outbound_group_sessions
        finally:
            await session.close()
            await nio_client.close()

    reopened = open_session(tmp_path)
    try:
        assert ROOM not in reopened.client.rooms
        assert ROOM in reopened.client.invited_rooms
        assert reopened._metadata[ROOM]["membership"] == "invite"
        assert reopened._metadata[ROOM]["membership_epoch"] == 2
    finally:
        await reopened.close()


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
