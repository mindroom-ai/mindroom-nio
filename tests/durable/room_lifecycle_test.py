"""Room replacement must preserve encryption and discard stale authorization."""

import asyncio
import json

import pytest
from aiohttp import web

from nio import TimelineEventProvenance
from nio.durable.model import RecordKind
from nio.exceptions import LocalProtocolError

from .client_test import ROOM, USER, client, open_session, response
from .membership_test import OPERATION
from .recovery_test import baseline, member, message
from .runner_test import drain_sync, homeserver
from .sliding_test import settings, settle, window


async def local_membership(session, target):
    assert await session.change_membership(
        operation_id=OPERATION,
        room_id=ROOM,
        previous_membership="join",
        previous_epoch=0,
        current_membership=target,
    )
    await session.ack(await session.next_batch())


@pytest.mark.asyncio
async def test_new_local_join_waits_for_room_state_before_plaintext_send(tmp_path):
    sent = []

    async def endpoint(request):
        if "/send/" in request.path:
            sent.append(await request.json())
            return web.json_response({"event_id": "$sent"})
        return web.json_response({})

    async with homeserver(None, membership=endpoint) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        try:
            assert await session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership=None,
                previous_epoch=0,
                current_membership="join",
            )
            await session.ack(await session.next_batch())
            with pytest.raises(LocalProtocolError, match="room state"):
                await nio_client.room_send(
                    ROOM, "m.room.message", {"msgtype": "m.text", "body": "waiting"}
                )
            assert sent == []
            await session._accept_response(response(messages=0), full_state=True)
            await settle(session)
            await nio_client.room_send(
                ROOM, "m.room.message", {"msgtype": "m.text", "body": "ready"}
            )
            assert sent == [{"msgtype": "m.text", "body": "ready"}]
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_local_join_never_sends_plaintext_for_known_encrypted_room(
    tmp_path, restart
):
    sent = []

    async def endpoint(request):
        if request.path.endswith("/joined_members"):
            return web.json_response({"joined": {USER: {}}})
        if "/send/" in request.path:
            sent.append((request.path, await request.json()))
            return web.json_response({"event_id": "$sent"})
        return web.json_response({})

    async with homeserver(None, membership=endpoint) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        body = json.loads(response(messages=0))
        body["rooms"]["join"][ROOM]["state"]["events"].append(
            {
                "type": "m.room.encryption",
                "state_key": "",
                "sender": USER,
                "event_id": "$encryption",
                "origin_server_ts": 1,
                "content": {"algorithm": "m.megolm.v1.aes-sha2"},
            }
        )
        await session._accept_response(json.dumps(body).encode())
        await settle(session)
        try:
            assert nio_client.rooms[ROOM].encrypted
            await local_membership(session, "join")
            if restart:
                await session.close()
                await nio_client.close()
                nio_client = client()
                nio_client.homeserver = url
                session = open_session(tmp_path, nio_client)
            await nio_client.room_send(
                ROOM, "m.room.message", {"msgtype": "m.text", "body": "private reply"}
            )
            assert len(sent) == 1
            path, content = sent[0]
            assert "/send/m.room.encrypted/" in path
            assert "body" not in content
            assert content["algorithm"] == "m.megolm.v1.aes-sha2"
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("limited", [False, True])
async def test_local_join_discards_sliding_proof_with_room_projection(
    tmp_path, limited
):
    async def endpoint(request):
        if request.path.endswith("/messages"):
            return web.json_response(
                {"start": "w1", "end": "w2", "chunk": [message("$gap")]}
            )
        return web.json_response({})

    async with homeserver(None, membership=endpoint) as (url, requests):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        try:
            await session._accept_response(window("$old"))
            await settle(session)
            await local_membership(session, "join")
            body = json.loads(
                window(
                    "$tail",
                    pos="p2",
                    initial=False,
                    prev_batch="w2",
                    state=[],
                    num_live=1,
                )
            )
            body["rooms"][ROOM]["limited"] = limited
            session._capture_response(json.dumps(body).encode())
            session._quiescing = True
            runner = asyncio.create_task(session.run())
            records = await drain_sync(session)
            await runner
            timeline = [
                record for record in records if record.kind is RecordKind.TIMELINE
            ]
            assert [
                (record.source["event_id"], record.provenance) for record in timeline
            ] == [("$tail", TimelineEventProvenance.HISTORY)]
            assert not any(path.endswith("/messages") for path, _ in requests)
            assert not session._metadata[ROOM]["baseline"]

            await session._accept_response(
                window(
                    "$partial",
                    pos="p3",
                    initial=False,
                    state=[member("$partial-join", "join")],
                    num_live=1,
                )
            )
            records = await settle(session)
            partial = next(
                record
                for record in records
                if record.source.get("event_id") == "$partial"
            )
            assert partial.provenance is TimelineEventProvenance.HISTORY
            assert not session._metadata[ROOM]["baseline"]

            await session._accept_response(window("$initial", pos="p4"))
            records = await settle(session)
            initial = next(
                record
                for record in records
                if record.source.get("event_id") == "$initial"
            )
            assert initial.provenance is TimelineEventProvenance.HISTORY
            assert session._metadata[ROOM]["baseline"]
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("sliding", [False, True])
@pytest.mark.parametrize("local_leave", [False, True])
async def test_reinvite_replaces_persisted_joined_members(
    tmp_path, sliding, local_leave
):
    async def endpoint(request):
        return web.json_response({})

    config = settings() if sliding else None
    async with homeserver(None, membership=endpoint) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, config)
        try:
            if sliding:
                await session._accept_response(
                    window(
                        state=[
                            member("$join", "join"),
                            member("$old", "join", "@old:example.org"),
                        ]
                    )
                )
                await settle(session)
            else:
                await baseline(session)
            if local_leave:
                await local_membership(session, "leave")
            invite = member("$invite", "invite")
            raw = (
                {
                    "pos": "p2",
                    "rooms": {
                        ROOM: {"membership": "invite", "stripped_state": [invite]}
                    },
                }
                if sliding
                else {
                    "next_batch": "s2",
                    "rooms": {"invite": {ROOM: {"invite_state": {"events": [invite]}}}},
                }
            )
            await session._accept_response(json.dumps(raw).encode())
            await settle(session)
            assert ROOM not in nio_client.rooms
            assert set(nio_client.invited_rooms[ROOM].users) == {USER}
        finally:
            await session.close()
            await nio_client.close()

    reopened = open_session(tmp_path, config=config)
    try:
        assert ROOM not in reopened.client.rooms
        assert set(reopened.client.invited_rooms[ROOM].users) == {USER}
        assert reopened._metadata[ROOM]["membership"] == "invite"
    finally:
        await reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("placement", ["state", "timeline"])
async def test_classic_local_join_reconciles_explicit_ban(tmp_path, placement):
    async def endpoint(request):
        return web.json_response({})

    async with homeserver(None, membership=endpoint) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        try:
            await baseline(session)
            await local_membership(session, "join")
            info = {"state": {"events": []}, "timeline": {"events": []}}
            info[placement]["events"] = [member("$ban", "ban")]
            await session._accept_response(
                json.dumps(
                    {"next_batch": "s2", "rooms": {"leave": {ROOM: info}}}
                ).encode()
            )
            records = await settle(session)
            changes = [record.membership for record in records if record.membership]
            assert [
                (change.previous, change.current, change.current_epoch)
                for change in changes
            ] == [("join", "ban", 1)]
            assert session._metadata[ROOM]["membership"] == "ban"
        finally:
            await session.close()
            await nio_client.close()
    reopened = open_session(tmp_path)
    try:
        assert reopened._metadata[ROOM]["membership"] == "ban"
        assert ROOM not in reopened.client.rooms
    finally:
        await reopened.close()
