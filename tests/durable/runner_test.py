"""Exercise the durable runner against real HTTP and SQLite boundaries."""

import asyncio
import json
from contextlib import asynccontextmanager, nullcontext

import pytest
from aiohttp import web

from nio import TimelineEventProvenance
from nio.durable.codec import restore_event
from nio.durable.model import RecordKind

from .client_test import (
    ROOM,
    USER,
    capture_unprepared,
    client,
    open_session,
    response,
)


@asynccontextmanager
async def homeserver(sync, *, query=None, membership=None):
    requests = []

    async def handle(request):
        requests.append((request.path, dict(request.query)))
        assert request.headers["Authorization"] == "Bearer test-token"
        assert "access_token" not in request.query
        if request.path.endswith("/sync"):
            return await sync(request)
        if request.path.endswith("/keys/upload"):
            return web.json_response({"one_time_key_counts": {"signed_curve25519": 50}})
        if request.path.endswith("/keys/query"):
            if query is not None:
                return await query(request)
            return web.json_response({"device_keys": {USER: {}}})
        if membership is not None:
            return await membership(request)
        raise AssertionError(request.path)

    application = web.Application()
    application.router.add_route("*", "/{path:.*}", handle)
    runner = web.AppRunner(application)
    await runner.setup()
    server = web.TCPSite(runner, "127.0.0.1", 0)
    await server.start()
    try:
        port = server._server.sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}", requests
    finally:
        await runner.cleanup()


async def drain_sync(session):
    records = []
    async with asyncio.timeout(5):
        while True:
            await session.wait_for_work()
            batch = await session.next_batch()
            records.extend(batch.records)
            await session.ack(batch)
            if batch.completes_sync:
                return records


@pytest.mark.asyncio
async def test_partial_key_query_defers_missing_users_without_blocking_completion(
    tmp_path,
):
    queries = 0
    stop = asyncio.Event()

    async def query(request):
        nonlocal queries
        queries += 1
        if queries > 2:
            return web.Response(status=400)
        return web.json_response({"device_keys": {}, "failures": {"example.org": {}}})

    async def sync(request):
        if request.query.get("since"):
            await stop.wait()
        return web.Response(body=response())

    async with homeserver(sync, query=query) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        nio_client.olm.users_for_key_query.add(USER)
        task = asyncio.create_task(session.run())
        try:
            await drain_sync(session)
            await session.quiesce()
            await task
            assert queries == 1
            assert nio_client.olm.users_for_key_query == {USER}
        finally:
            stop.set()
            await session.close()
            await nio_client.close()
    reopened = open_session(tmp_path)
    try:
        assert reopened.client.olm.users_for_key_query == {USER}
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_runner_commits_before_advancing_cursor_and_finishes_empty_sync(tmp_path):
    second_poll = asyncio.Event()
    release = asyncio.Event()

    async def sync(request):
        if request.query.get("since") == "s1":
            second_poll.set()
            await release.wait()
            return web.json_response({"next_batch": "s2"})
        return web.Response(body=response())

    async with homeserver(sync) as (url, requests):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        task = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            assert [
                restore_event(x).body for x in records if x.kind is RecordKind.TIMELINE
            ] == ["hello 0"]
            async with asyncio.timeout(5):
                await second_poll.wait()
            assert session.cursor == "s1"
            assert session._store.input is None
            release.set()
            assert await drain_sync(session) == []
            await session.quiesce()
            await task
            assert session.cursor == "s2"
            assert any(path.endswith("/keys/upload") for path, _ in requests)
        finally:
            release.set()
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_runner_drains_retained_output_before_another_poll(tmp_path):
    session = open_session(tmp_path)
    await session._accept_response(response())
    first = await session.next_batch()
    await session.close()
    polled = asyncio.Event()

    async def sync(request):
        polled.set()
        return web.json_response({"next_batch": "s2"})

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        reopened = open_session(tmp_path, nio_client)
        task = asyncio.create_task(reopened.run())
        try:
            assert await reopened.next_batch() == first
            assert not polled.is_set()
            records = await drain_sync(reopened)
            assert any(x.kind is RecordKind.TIMELINE for x in records)
            await reopened.quiesce()
            await task
        finally:
            await reopened.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_rejoin_discards_old_members_and_does_not_mark_initial_tail_live(
    tmp_path,
):
    first = json.loads(response())
    old_member = dict(first["rooms"]["join"][ROOM]["state"]["events"][0])
    old_member.update(state_key="@old:example.org", event_id="$old")
    first["rooms"]["join"][ROOM]["state"]["events"].append(old_member)
    departure = json.loads(response(token="s2", messages=0))
    info = departure["rooms"].pop("join")
    info[ROOM]["state"]["events"][0]["content"]["membership"] = "leave"
    departure["rooms"]["leave"] = info
    responses = [first, departure, json.loads(response(token="s3"))]
    completed = asyncio.Event()

    async def sync(request):
        if responses:
            return web.json_response(responses.pop(0))
        await completed.wait()
        return web.json_response({"next_batch": "s4"})

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        task = asyncio.create_task(session.run())
        try:
            joined = await drain_sync(session)
            left = await drain_sync(session)
            records = await drain_sync(session)
            assert [
                record.membership.current_epoch
                for group in (joined, left, records)
                for record in group
                if record.membership is not None
            ] == [0, 1, 1]
            assert {x.provenance for x in records if x.kind is RecordKind.TIMELINE} == {
                TimelineEventProvenance.HISTORY
            }
            await session.quiesce()
            await task
        finally:
            completed.set()
            await session.close()
            await nio_client.close()
    reopened = open_session(tmp_path)
    try:
        assert set(reopened.client.rooms[ROOM].users) == {USER}
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_quiesce_waits_for_completion_acknowledgement(tmp_path):
    async def sync(request):
        raise AssertionError("quiesce must not start another source request")

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await session._accept_response(response())
        runner = asyncio.create_task(session.run())
        stopping = asyncio.create_task(session.quiesce())
        try:
            async with asyncio.timeout(5):
                while True:
                    await session.wait_for_work()
                    batch = await session.next_batch()
                    if batch.completes_sync:
                        break
                    await session.ack(batch)
            assert not runner.done()
            await session.ack(batch)
            await stopping
            await runner
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_empty_sync_commits_once_before_its_acknowledgement(tmp_path):
    empty_responses = 3
    blocked = asyncio.Event()
    stop = asyncio.Event()

    async def sync(request):
        since = request.query.get("since")
        if since is None:
            return web.Response(body=response())
        if int(since[1:]) > empty_responses:
            blocked.set()
            await stop.wait()
        return web.json_response({"next_batch": f"s{int(since[1:]) + 1}"})

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        runner = asyncio.create_task(session.run())
        try:
            await drain_sync(session)
            statements = []
            session._store.database.connection().set_trace_callback(statements.append)
            for _ in range(empty_responses):
                assert await drain_sync(session) == []
            async with asyncio.timeout(5):
                await blocked.wait()
            # Capture, preparation and completion share one commit; the
            # consumer's acknowledgement is the other.
            assert statements.count("COMMIT") == 2 * empty_responses
            assert session.cursor == f"s{empty_responses + 1}"
            assert session._store.input is None
        finally:
            stop.set()
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("held", [False, True])
async def test_sync_preparation_retains_a_due_crypto_request_unless_the_lock_is_held(
    tmp_path, held
):
    session = open_session(tmp_path)
    try:
        # A new device's key upload is due after this otherwise empty sync.
        async with session._crypto_lock if held else nullcontext():
            await session._accept_response(b'{"next_batch":"s1"}')
        pending = session._crypto._pending()
        assert (pending[0].kind if pending else None) == (None if held else "upload")
        # The sync stays incomplete until the runner's maintenance pass.
        assert session._store.has_input()
        assert await session.next_batch() is None
        assert session.cursor == "s1"
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_runner_completes_empty_input_left_unprepared_by_an_earlier_release(
    tmp_path,
):
    polled = asyncio.Event()
    stop = asyncio.Event()

    async def sync(request):
        since = request.query.get("since")
        if since == "s2":
            polled.set()
        if since is not None:
            await stop.wait()
        return web.Response(body=response())

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        runner = asyncio.create_task(session.run())
        await drain_sync(session)
        await session.quiesce()
        await runner
        capture_unprepared(session, b'{"next_batch":"s2"}')
        await session.close()
        await nio_client.close()
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        runner = asyncio.create_task(session.run())
        try:
            assert await drain_sync(session) == []
            async with asyncio.timeout(5):
                await polled.wait()
            assert session.cursor == "s2"
            assert not runner.done()
        finally:
            stop.set()
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)
