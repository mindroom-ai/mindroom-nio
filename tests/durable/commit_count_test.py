"""Count the commits a sync cycle costs; each writing commit is an fsync."""

import asyncio
import json

import pytest
from aiohttp import web

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
