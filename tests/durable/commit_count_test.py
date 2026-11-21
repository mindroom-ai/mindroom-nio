"""Count the commits a sync cycle costs; each writing commit is an fsync."""

import asyncio
import json
import threading

import pytest
from aiohttp import web
from peewee import SqliteDatabase

from nio.durable.model import RecordKind

from .client_test import ROOM, client, open_session, response
from .recovery_test import message
from .runner_test import drain_sync, held_polls, homeserver
from .sliding_test import settings, window


def classic(index, *event_ids):
    rooms = {"join": {ROOM: {"timeline": {"events": [message(x) for x in event_ids]}}}}
    return json.dumps(
        {"next_batch": f"s{index}", **({"rooms": rooms} if event_ids else {})}
    ).encode()


def sliding(index, *event_ids):
    if event_ids:
        return window(*event_ids, pos=f"p{index}", initial=False, num_live=1, state=[])
    return json.dumps(
        {
            "pos": f"p{index}",
            "rooms": {},
            "extensions": {"to_device": {"next_batch": "td1", "events": []}},
        }
    ).encode()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("first", "body", "config"),
    [(response(), classic, None), (window("$old"), sliding, settings())],
    ids=["classic", "sliding"],
)
async def test_idle_sync_cycle_commits_once(tmp_path, first, body, config):
    sync, next_poll, answers = held_polls()
    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, config)
        runner = asyncio.create_task(session.run())
        try:
            (await next_poll()).set_result(web.Response(body=first))
            await drain_sync(session)
            answer = await next_poll()
            connection = session._store.database.connection()
            commits = []

            def record(statement):
                if statement == "COMMIT":
                    commits.append(connection.total_changes)

            connection.set_trace_callback(record)

            async def cycle(*event_ids):
                """Return a cycle's commits and how many of them wrote rows."""
                nonlocal answer
                before = connection.total_changes
                del commits[:]
                index = len(answers)
                answer.set_result(web.Response(body=body(index, *event_ids)))
                records = await drain_sync(session)
                assert [
                    record.source["event_id"]
                    for record in records
                    if record.kind is RecordKind.TIMELINE
                ] == list(event_ids)
                # The runner polls again only after the consumer's acknowledgement.
                answer = await next_poll()
                assert session.cursor in (f"s{index}", f"p{index}")
                assert session._store.input is None
                changes = [before, *commits]
                return len(commits), sum(a < b for a, b in zip(changes, changes[1:]))

            # Capture, preparation, completion and the previous cycle's
            # acknowledgement share the one commit of an idle cycle.
            assert [await cycle() for _ in range(3)] == [(1, 1)] * 3
            # A message adds its batch's acknowledgement, the completion
            # commit and a maintenance transaction that writes nothing.
            assert await cycle("$new") == (4, 3)
            assert await cycle() == (1, 1)
        finally:
            await session.close()
            for held in answers:
                if not held.done():
                    held.set_result(web.Response(body=b"{}"))
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_sync_cycle_commits_wait_for_the_disk_off_the_event_loop(
    tmp_path, monkeypatch
):
    loop = asyncio.get_running_loop()
    commit = SqliteDatabase.commit
    events = []

    def slow_disk(database):
        if threading.current_thread() is threading.main_thread():
            return commit(database)
        # This commit cannot finish until the event loop runs a callback.
        ran = threading.Event()
        loop.call_soon_threadsafe(ran.set)
        waited = ran.wait(5)
        result = commit(database)
        loop.call_soon_threadsafe(events.append, ("durable", waited))
        return result

    sync, next_poll, answers = held_polls()

    async def observed_sync(request):
        events.append(("poll",))
        return await sync(request)

    async with homeserver(observed_sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        runner = asyncio.create_task(session.run())
        try:
            (await next_poll()).set_result(web.Response(body=response()))
            await drain_sync(session)
            answer = await next_poll()
            monkeypatch.setattr(SqliteDatabase, "commit", slow_disk)
            events.clear()
            answer.set_result(web.Response(body=classic(1, "$new")))
            async with asyncio.timeout(5):
                while True:
                    await session.wait_for_work()
                    batch = await session.next_batch()
                    events.append(("delivered", batch.completes_sync))
                    await session.ack(batch)
                    if batch.completes_sync:
                        break
            await next_poll()
            # Preparation, the message's acknowledgement and completion each
            # commit off the loop, and nothing reaches the consumer or the
            # server before the commit it follows from is durable.
            assert events == [
                ("durable", True),
                ("delivered", False),
                ("durable", True),
                ("durable", True),
                ("delivered", True),
                ("poll",),
            ]
        finally:
            await session.close()
            for held in answers:
                if not held.done():
                    held.set_result(web.Response(body=b"{}"))
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_runner_cancelled_during_its_commit_can_run_again(tmp_path, monkeypatch):
    started, release = threading.Event(), threading.Event()
    commit = SqliteDatabase.commit

    def held(database):
        if threading.current_thread() is not threading.main_thread():
            started.set()
            assert release.wait(5)
        return commit(database)

    monkeypatch.setattr(SqliteDatabase, "commit", held)
    sync, next_poll, answers = held_polls()
    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        runner = asyncio.create_task(session.run())
        try:
            (await next_poll()).set_result(web.Response(body=response()))
            assert await asyncio.to_thread(started.wait, 5)
            # Cancel the runner while the capture commit waits for the disk.
            runner.cancel()
            await asyncio.gather(runner, return_exceptions=True)
            release.set()
            runner = asyncio.create_task(session.run())
            records = await drain_sync(session)
            assert [r.kind for r in records].count(RecordKind.TIMELINE) == 1
            await next_poll()
            assert session.cursor == "s1"
        finally:
            release.set()
            await session.close()
            for held_answer in answers:
                if not held_answer.done():
                    held_answer.set_result(web.Response(body=b"{}"))
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)
