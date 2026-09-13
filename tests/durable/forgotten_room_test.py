"""Complete inventories reconcile departures forgotten by another client."""

import asyncio
import json

import pytest
from aiohttp import web

from nio.durable import DurableSyncConfig
from nio.durable.model import OwnMembership, RecordKind
from nio.crypto import OutboundGroupSession

from .client_test import ROOM, client, open_session, response
from .membership_test import OPERATION
from .recovery_test import baseline
from .runner_test import drain_sync, homeserver
from .sliding_test import settings, settle, window


@pytest.mark.asyncio
async def test_forgotten_room_stops_full_state_loop_and_restores_filter(tmp_path):
    sync_filter = {"room": {"timeline": {"limit": 10}}}
    requests_seen = []
    next_poll = asyncio.Event()
    stop = asyncio.Event()

    async def sync(request):
        requests_seen.append(dict(request.query))
        if len(requests_seen) > 1:
            next_poll.set()
            await stop.wait()
        return web.json_response({"next_batch": "s2"})

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(
            tmp_path, nio_client, DurableSyncConfig(sync_filter=sync_filter)
        )
        await baseline(session)
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            async with asyncio.timeout(5):
                await next_poll.wait()
            assert [r.get("full_state") for r in requests_seen] == ["true", None]
            assert "filter" not in requests_seen[0]
            assert json.loads(requests_seen[1]["filter"]) == sync_filter
            assert requests_seen[1]["since"] == "s2"
            assert [r.kind for r in records] == [
                RecordKind.LOSS,
                RecordKind.ROOM_LIFECYCLE,
            ]
            assert records[1].membership == OwnMembership("join", "leave", 0, 1)
            assert ROOM not in nio_client.rooms
            await session.quiesce()
            await runner
        finally:
            stop.set()
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("known_baseline", [False, True])
@pytest.mark.parametrize("restart", ["none", "captured", "prepared"])
async def test_missing_room_departure_survives_replay(
    tmp_path, known_baseline, restart
):
    config = DurableSyncConfig(sync_filter=None if known_baseline else "saved-filter")
    session = open_session(tmp_path, config=config)
    try:
        await baseline(session)
        # Keep another joined room in the authoritative inventory.
        body = json.loads(response(token="s2", messages=0))
        body["rooms"]["join"]["!other:example.org"] = body["rooms"]["join"].pop(ROOM)
        await session._capture_response(json.dumps(body).encode(), full_state=True)
        if restart == "prepared":
            await session._prepare_pending()
        if restart != "none":
            await session.close()
            await session.client.close()
            session = open_session(tmp_path, config=config)
        await session._prepare_pending()
        records = await settle(session)
        missing = [r for r in records if r.room_id == ROOM]
        assert [r.kind for r in missing] == [
            RecordKind.LOSS,
            RecordKind.ROOM_LIFECYCLE,
        ]
        assert missing[0].source["reason"] == "joined room absent from complete sync"
        assert missing[1].membership == OwnMembership("join", "leave", 0, 1)
        assert ROOM not in session.client.rooms
        assert session._metadata[ROOM]["nonjoined_cursor"] == "s2"
        assert "!other:example.org" in session.client.rooms
        assert not session._recovery.needs_full_state()

        # Another complete inventory must not repeat the loss or tenure change.
        body["next_batch"] = "s3"
        await session._accept_response(json.dumps(body).encode(), full_state=True)
        assert not [r for r in await settle(session) if r.room_id == ROOM]
        await session.close()
        await session.client.close()
        session = open_session(tmp_path, config=config)
        assert session._metadata[ROOM]["membership"] == "leave"
        assert session._metadata[ROOM]["membership_epoch"] == 1
        assert ROOM not in session.client.rooms
    finally:
        await session.close()
        await session.client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("sync_filter", [None, {"room": {"rooms": []}}, "saved-filter"])
async def test_incremental_omission_does_not_end_membership(tmp_path, sync_filter):
    session = open_session(tmp_path, config=DurableSyncConfig(sync_filter=sync_filter))
    try:
        await baseline(session)
        await session._accept_response(b'{"next_batch":"s2"}')
        assert await settle(session) == []
        assert ROOM in session.client.rooms
        assert session._metadata[ROOM]["membership"] == "join"
        assert session._metadata[ROOM]["membership_epoch"] == 0
    finally:
        await session.close()
        await session.client.close()


@pytest.mark.asyncio
async def test_sliding_omission_does_not_end_membership(tmp_path):
    session = open_session(tmp_path, config=settings())
    try:
        await session._accept_response(window(initial=True))
        await settle(session)
        await session._accept_response(b'{"pos":"p2","rooms":{}}')
        assert await settle(session) == []
        assert ROOM in session.client.rooms
        assert session._metadata[ROOM]["membership"] == "join"
    finally:
        await session.close()
        await session.client.close()


@pytest.mark.asyncio
async def test_forgotten_local_join_releases_pending_membership_intent(tmp_path):
    async def membership(request):
        return web.json_response({"room_id": ROOM})

    async with homeserver(None, membership=membership) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        try:
            with session._store.transaction():
                session._store.set_cursor("before-join")
            assert await session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
            batch = await session.next_batch()
            await session.ack(batch)
            await session._accept_response(b'{"next_batch":"s2"}', full_state=True)
            records = await settle(session)
            assert [r.membership for r in records if r.membership] == [
                OwnMembership("join", "leave", 0, 1)
            ]
            async with asyncio.timeout(1):
                await session.wait_for_membership_idle()
            assert ROOM not in nio_client.rooms
            assert not session._recovery.needs_full_state()
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_reopened_filtered_initial_sync_cannot_prove_departure(tmp_path):
    async def membership(request):
        return web.json_response({"room_id": ROOM})

    async with homeserver(None, membership=membership) as (url, _):
        session = open_session(
            tmp_path, config=DurableSyncConfig(sync_filter={"room": {"rooms": []}})
        )
        session.client.homeserver = url
        try:
            assert await session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership=None,
                previous_epoch=0,
                current_membership="join",
            )
            await session.ack(await session.next_batch())
            await session._capture_response(b'{"next_batch":"s1"}')
            await session.close()
            await session.client.close()
            # Current configuration does not describe the captured request.
            session = open_session(tmp_path)
            await session._prepare_pending()
            assert await settle(session) == []
            assert session._metadata[ROOM]["membership"] == "join"
            assert session._recovery.needs_full_state()
        finally:
            await session.close()
            await session.client.close()


@pytest.mark.asyncio
async def test_missing_room_invalidates_outbound_recipients(tmp_path):
    session = open_session(tmp_path)
    try:
        await baseline(session)
        group = OutboundGroupSession()
        group.shared = True
        session.client.olm.outbound_group_sessions[ROOM] = group
        session._outbound.member_cache[ROOM] = ["@old:example.org"]
        await session._accept_response(b'{"next_batch":"s2"}', full_state=True)
        assert ROOM not in session.client.olm.outbound_group_sessions
        assert ROOM not in session._outbound.member_cache
    finally:
        await session.close()
        await session.client.close()


@pytest.mark.asyncio
async def test_missing_room_departure_rolls_back_with_cursor(tmp_path, monkeypatch):
    session = open_session(tmp_path)
    try:
        await baseline(session)
        await session._capture_response(b'{"next_batch":"s2"}', full_state=True)

        def fail_cursor(token):
            raise RuntimeError("interrupted tail")

        with monkeypatch.context() as failing:
            failing.setattr(session._store, "set_cursor", fail_cursor)
            with pytest.raises(RuntimeError, match="interrupted tail"):
                await session._prepare_pending()
        await session.close()
        await session.client.close()
        session = open_session(tmp_path)
        assert session.cursor == "s1"
        assert session._metadata[ROOM]["membership"] == "join"
        assert await session.next_batch() is None
        await session._prepare_pending()
        records = await settle(session)
        assert [r.membership for r in records if r.membership] == [
            OwnMembership("join", "leave", 0, 1)
        ]
        assert session.cursor == "s2"
        assert ROOM not in session.client.rooms
    finally:
        await session.close()
        await session.client.close()
