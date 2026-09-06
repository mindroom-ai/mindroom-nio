"""Disposal, shutdown and malformed state cannot advance durable authority."""

import asyncio
import json
import logging

import pytest
from aiohttp import web

from nio import TimelineEventProvenance
from nio.durable.model import RecordKind
from nio.exceptions import LocalProtocolError

from .client_test import ROOM, USER, client, open_session, response
from .crypto_test import publish_account
from .recovery_test import baseline, member, message
from .runner_test import homeserver
from .sliding_test import settings, settle, window


@pytest.mark.asyncio
@pytest.mark.parametrize("end", ["rollback", "close"])
@pytest.mark.parametrize("operation", ["encrypt", "room_send", "send"])
async def test_disposed_client_cannot_encrypt_or_send(
    tmp_path, monkeypatch, end, operation
):
    sent = []

    async def http(request):
        sent.append(await request.text())
        return web.json_response({"event_id": "$unexpected"})

    async with homeserver(None, membership=http) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        room = nio_client.rooms[ROOM]
        room.encrypted = operation == "encrypt"
        room.members_synced = True
        nio_client.olm.create_outbound_group_session(ROOM)
        nio_client.olm.outbound_group_sessions[ROOM].shared = True
        try:
            if end == "close":
                await session.close()
            else:

                def fail_commit():
                    raise RuntimeError("commit failed")

                with monkeypatch.context() as failing:
                    failing.setattr(session._store.database, "commit", fail_commit)
                    with pytest.raises(RuntimeError, match="commit failed"):
                        with session._outbound.transaction():
                            room.users[USER].display_name = "uncommitted"

            with pytest.raises(LocalProtocolError, match="poisoned"):
                if operation == "encrypt":
                    nio_client.encrypt(ROOM, "m.room.message", {"body": "secret"})
                elif operation == "room_send":
                    await nio_client.room_send(
                        ROOM, "m.room.message", {"body": "secret"}
                    )
                else:
                    result = await nio_client.send(
                        "PUT",
                        "/send",
                        "secret",
                        headers={"Authorization": "Bearer test-token"},
                    )
                    result.release()
            assert sent == []
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_disposal_during_send_preparation_prevents_later_http(tmp_path):
    sent = []

    async def http(request):
        sent.append(await request.text())
        return web.json_response({"event_id": "$unexpected"})

    class ShareBarrier(asyncio.Event):
        def __init__(self):
            super().__init__()
            self.waiting = asyncio.Event()

        async def wait(self):
            self.waiting.set()
            return await super().wait()

    async with homeserver(None, membership=http) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        room = nio_client.rooms[ROOM]
        room.encrypted = room.members_synced = True
        sharing = ShareBarrier()
        nio_client.sharing_session[ROOM] = sharing
        task = asyncio.create_task(
            nio_client.room_send(ROOM, "m.reaction", {"body": "secret"})
        )
        try:
            async with asyncio.timeout(5):
                await sharing.waiting.wait()
                await session.close()
                sharing.set()
                with pytest.raises(LocalProtocolError, match="poisoned"):
                    await task
            assert sent == []
        finally:
            sharing.set()
            await asyncio.gather(task, return_exceptions=True)
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_quiesce_discards_a_completed_but_unaccepted_poll(tmp_path, monkeypatch):
    session = open_session(tmp_path)
    await baseline(session)
    with session._store.transaction():
        publish_account(session.client.olm)
        session.client.olm.users_for_key_query.clear()
        session._crypto.capture()
    poll_finished = asyncio.Event()

    async def request(method, path, *_args, **_kwargs):
        assert method == "GET" and "/sync" in path
        poll_finished.set()
        return response(token="unaccepted")

    monkeypatch.setattr(session._transport, "request", request)

    async def stop():
        await poll_finished.wait()
        assert session._poll.done()
        assert session._store.input is None
        await session.quiesce()

    stopping = asyncio.create_task(stop())
    runner = asyncio.create_task(session.run())
    try:
        async with asyncio.timeout(2):
            await stopping
            await runner
        assert session.cursor == "s1"
        assert await session.next_batch() is None
        assert session._store.input is None
    finally:
        stopping.cancel()
        await session.close()
        await asyncio.gather(stopping, runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("sliding", [False, True])
@pytest.mark.parametrize("location", ["state", "timeline"])
@pytest.mark.parametrize("bad", ["membership", "missing-key", "power"])
async def test_malformed_state_cannot_preserve_live_authorization(
    tmp_path, sliding, location, bad
):
    config = settings() if sliding else None
    session = open_session(tmp_path, config=config)
    await session._accept_response(window("$old") if sliding else response())
    await settle(session)
    old_cursor = session.cursor
    event = member("$invalid", "leave")
    if bad == "membership":
        event["content"]["membership"] = "not-a-membership"
    elif bad == "missing-key":
        event.pop("state_key")
    else:
        event.update(
            type="m.room.power_levels", state_key="", content={"users": "invalid"}
        )
    root = json.loads(
        window("$after", pos="p2", initial=False, num_live=1, state=[])
        if sliding
        else response(token="s2", messages=0)
    )
    info = root["rooms"][ROOM] if sliding else root["rooms"]["join"][ROOM]
    timeline = info["timeline"] if sliding else info["timeline"]["events"]
    timeline[:] = [message("$after")]
    if location == "state":
        if sliding:
            info["required_state"] = [event]
        else:
            info["state"]["events"] = [event]
    else:
        timeline.insert(0, event)
        if sliding:
            info["num_live"] = 2
    try:
        with pytest.raises(LocalProtocolError, match="malformed"):
            await session._accept_response(json.dumps(root).encode())
    finally:
        await session.close()
    reopened = open_session(tmp_path, config=config)
    try:
        assert reopened.cursor == old_cursor
        assert await reopened.next_batch() is None
    finally:
        await reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("sliding", [False, True])
async def test_malformed_message_does_not_revoke_valid_room_state(tmp_path, sliding):
    session = open_session(tmp_path, config=settings() if sliding else None)
    await session._accept_response(window("$old") if sliding else response())
    await settle(session)
    root = json.loads(
        window("$bad", "$after", pos="p2", initial=False, num_live=2, state=[])
        if sliding
        else response(token="s2", messages=0)
    )
    info = root["rooms"][ROOM] if sliding else root["rooms"]["join"][ROOM]
    timeline = info["timeline"] if sliding else info["timeline"]["events"]
    timeline[:] = [message("$bad"), message("$after")]
    timeline[0]["content"]["body"] = []
    try:
        await session._accept_response(json.dumps(root).encode())
        records = await settle(session)
        assert (
            next(
                r
                for r in records
                if r.kind is RecordKind.TIMELINE and r.source["event_id"] == "$after"
            ).provenance
            is TimelineEventProvenance.LIVE
        )
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("level", [logging.WARNING, logging.DEBUG])
async def test_malformed_response_diagnostics_do_not_log_payloads(
    tmp_path, caplog, level
):
    session = open_session(tmp_path)
    root = json.loads(response())
    root["rooms"]["join"][ROOM]["state"]["events"] = "payload-secret"
    try:
        with caplog.at_level(level):
            with pytest.raises(LocalProtocolError, match="malformed"):
                await session._accept_response(json.dumps(root).encode())
        assert "payload-secret" not in caplog.text
        assert "ValidationError" in caplog.text
    finally:
        await session.close()
