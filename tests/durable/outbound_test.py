"""Public crypto operations retain facts before HTTP and survive restart."""

import asyncio
import json
from dataclasses import replace

import pytest
import vodozemac
from aiohttp import web
from unpaddedbase64 import decode_base64

from nio.crypto import Olm, TrustState
from nio.store import SqliteMemoryStore
from nio.durable.codec import restore_event
from nio.events import RoomKeyRequest
from nio.event_builders import ToDeviceMessage
from nio.exceptions import LocalProtocolError, OlmUnverifiedDeviceError
from nio.rooms import MatrixRoom

from .client_test import ROOM, USER, client, open_session
from .crypto_test import missing_event, publish_account, queue_dummy, room_key_request
from .runner_test import drain_sync, homeserver


@pytest.mark.asyncio
@pytest.mark.parametrize("cancel", [False, True])
async def test_public_key_share_decision_survives_owned_runner_restart(
    tmp_path, cancel
):
    session = open_session(tmp_path)
    with session._store.transaction():
        publish_account(session.client.olm)
        peer, device = queue_dummy(session.client.olm)
        session.client.olm.outgoing_to_device_messages.clear()
        request, group = room_key_request(session.client.olm)
        group.shared = True
        ciphertext = group.encrypt("retained room secret")
        session._crypto.capture()
    await session._accept_response(
        json.dumps(
            {"next_batch": "s1", "to_device": {"events": [request.source]}}
        ).encode()
    )
    batch = await session.next_batch()
    assert batch is not None
    replayed = next(restore_event(record) for record in batch.records if record.route)
    assert isinstance(replayed, RoomKeyRequest)
    await session.close()

    session = open_session(tmp_path)
    try:
        batch = await session.next_batch()
        replayed = next(
            restore_event(record) for record in batch.records if record.route
        )
        assert session.client.get_active_key_requests(USER, "OTHER") == [replayed]
        session.client.verify_device(session.client.olm.device_store[USER]["OTHER"])
        decide = (
            session.client.cancel_key_share
            if cancel
            else session.client.continue_key_share
        )
        assert decide(replayed)
        await session.ack(batch)
    finally:
        await session.close()

    sent = []
    stop = asyncio.Event()

    async def sync(_):
        await stop.wait()
        return web.json_response({"next_batch": "s2"})

    async def send(request):
        sent.append(await request.json())
        return web.json_response({})

    async with homeserver(sync, membership=send) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        task = asyncio.create_task(session.run())
        try:
            await drain_sync(session)
            await session.quiesce()
            await task
            assert nio_client.get_active_key_requests(USER, "OTHER") == []
            if cancel:
                assert sent == []
            else:
                assert len(sent) == 1
                content = sent[0]["messages"][USER]["OTHER"]
                encrypted = content["ciphertext"][device.curve25519]
                message = vodozemac.AnyOlmMessage.from_parts(
                    encrypted["type"], decode_base64(encrypted["body"])
                )
                _, clear = peer.create_inbound_session(content["sender_key"], message)
                share = json.loads(clear)
                assert share["type"] == "m.forwarded_room_key"
                inbound = vodozemac.InboundGroupSession.import_session(
                    vodozemac.ExportedSessionKey(share["content"]["session_key"])
                )
                assert (
                    inbound.decrypt(
                        vodozemac.MegolmMessage.from_base64(ciphertext)
                    ).plaintext
                    == b"retained room secret"
                )
        finally:
            stop.set()
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_public_to_device_retries_exact_request_after_cancel(tmp_path):
    received = asyncio.Event()
    release = asyncio.Event()
    requests = []

    async def send(request):
        requests.append((request.path, await request.text()))
        if len(requests) == 1:
            received.set()
            await release.wait()
        return web.json_response({})

    async with homeserver(None, membership=send) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        message = ToDeviceMessage("org.example.notice", USER, "OTHER", {"value": 1})
        first = asyncio.create_task(nio_client.to_device(message, "stable-send"))
        try:
            async with asyncio.timeout(3):
                await received.wait()
            first.cancel()
            with pytest.raises(asyncio.CancelledError):
                await first
            release.set()
            await nio_client.to_device(message, "stable-send")
            assert len(requests) == 2
            assert requests[0] == requests[1]
            assert not nio_client.outgoing_to_device_messages
        finally:
            release.set()
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_unverified_group_share_leaves_session_usable(tmp_path):
    sent = []

    async def send(request):
        sent.append(await request.json())
        return web.json_response({})

    async with homeserver(None, membership=send) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        with session._store.transaction():
            publish_account(nio_client.olm)
            _, device = queue_dummy(nio_client.olm)
            nio_client.olm.outgoing_to_device_messages.clear()
            session._crypto.capture()
        room = MatrixRoom(ROOM, USER, encrypted=True)
        room.add_member(USER, "Alice", None)
        room.members_synced = True
        nio_client.rooms[ROOM] = room
        try:
            with pytest.raises(OlmUnverifiedDeviceError):
                await nio_client.share_group_session(ROOM)
            assert sent == []
            nio_client.verify_device(device)
            result = await nio_client.share_group_session(ROOM)
            assert result.users_shared_with == {(USER, "OTHER")}
            assert nio_client.olm.outbound_group_sessions[ROOM].shared
            assert len(sent) == 1
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_outgoing_member_query_cannot_rewrite_recovery_projection(tmp_path):
    async def members(_):
        return web.json_response(
            {
                "joined": {
                    "@new:example.org": {"display_name": "New", "avatar_url": None}
                }
            }
        )

    async with homeserver(None, membership=members) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        room = MatrixRoom(ROOM, USER, encrypted=True)
        room.add_member(USER, "Alice", None)
        nio_client.rooms[ROOM] = room
        try:
            result = await nio_client.joined_members(ROOM)
            assert [member.user_id for member in result.members] == ["@new:example.org"]
            assert set(room.users) == {USER}
            assert not room.members_synced
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_group_ciphertext_survives_cancel_and_restart(tmp_path):
    received = asyncio.Event()
    release = asyncio.Event()
    requests = []

    async def send(request):
        requests.append((request.path, await request.text()))
        if len(requests) == 1:
            received.set()
            await release.wait()
        return web.json_response({})

    async with homeserver(None, membership=send) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        with session._store.transaction():
            publish_account(nio_client.olm)
            peer, device = queue_dummy(nio_client.olm)
            nio_client.olm.outgoing_to_device_messages.clear()
            nio_client.verify_device(device)
            session._crypto.capture()
        room = MatrixRoom(ROOM, USER, encrypted=True)
        room.add_member(USER, "Alice", None)
        room.members_synced = True
        nio_client.rooms[ROOM] = room
        sharing = asyncio.create_task(nio_client.share_group_session(ROOM))
        try:
            async with asyncio.timeout(3):
                await received.wait()
            sharing.cancel()
            with pytest.raises(asyncio.CancelledError):
                await sharing
            await session.close()
            await nio_client.close()
            nio_client = client()
            nio_client.homeserver = url
            session = open_session(tmp_path, nio_client)
            release.set()
            await session._maintain_crypto()
            assert len(requests) == 2
            assert requests[0] == requests[1]
            content = json.loads(requests[1][1])["messages"][USER]["OTHER"]
            encrypted = content["ciphertext"][device.curve25519]
            message = vodozemac.AnyOlmMessage.from_parts(
                encrypted["type"], decode_base64(encrypted["body"])
            )
            _, clear = peer.create_inbound_session(content["sender_key"], message)
            assert json.loads(clear)["type"] == "m.room_key"
        finally:
            release.set()
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_invalidation_during_group_http_prevents_completion(tmp_path):
    nio_client = client()
    replacement = None

    async def send(_):
        nonlocal replacement
        nio_client.invalidate_outbound_session(ROOM)
        replacement = nio_client.olm.outbound_group_sessions.get(ROOM)
        return web.json_response({})

    async with homeserver(None, membership=send) as (url, _):
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        with session._store.transaction():
            publish_account(nio_client.olm)
            _, device = queue_dummy(nio_client.olm)
            nio_client.olm.outgoing_to_device_messages.clear()
            nio_client.verify_device(device)
            session._crypto.capture()
        room = MatrixRoom(ROOM, USER, encrypted=True)
        room.add_member(USER, "Alice", None)
        room.members_synced = True
        nio_client.rooms[ROOM] = room
        try:
            with pytest.raises(LocalProtocolError, match="changed while sharing"):
                await nio_client.share_group_session(ROOM)
            assert replacement is None
            assert session._crypto._pending() is None
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_public_key_calls_retain_requests_before_http(tmp_path):
    from contextlib import asynccontextmanager

    @asynccontextmanager
    async def server():
        async def handle(request):
            pending = session._crypto._pending()[0]
            assert not session._store.database.in_transaction()
            assert pending.path == request.path
            assert json.loads(pending.body) == await request.json()
            if pending.kind == "upload":
                return web.json_response(
                    {"one_time_key_counts": {"signed_curve25519": 50}}
                )
            if pending.kind == "query":
                return web.json_response({"device_keys": {USER: {}}})
            if pending.kind == "claim":
                return web.json_response({"one_time_keys": {}})
            return web.json_response({})

        app = web.Application()
        app.router.add_route("*", "/{path:.*}", handle)
        runner = web.AppRunner(app)
        await runner.setup()
        site = web.TCPSite(runner, "127.0.0.1", 0)
        await site.start()
        try:
            yield f"http://127.0.0.1:{site._server.sockets[0].getsockname()[1]}"
        finally:
            await runner.cleanup()

    async with server() as url:
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        try:
            await nio_client.keys_upload()
            nio_client.olm.users_for_key_query.add(USER)
            await nio_client.keys_query()
            await nio_client.keys_claim({USER: ["OTHER"]})
            with session._store.transaction():
                _, device = queue_dummy(nio_client.olm)
                nio_client.olm.outgoing_to_device_messages.clear()
                session._crypto.capture()
            event = missing_event(nio_client.olm, device)
            result = await nio_client.request_room_key(event)
            assert result.session_id == event.session_id
        finally:
            await session.close()
            await nio_client.close()
    session = open_session(tmp_path)
    try:
        assert session.client.olm.account.shared
        assert session.client.olm.users_for_key_query == set()
        assert session.client.outgoing_key_requests[event.session_id].room_id == ROOM
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "existing_session,claim_available", [(False, True), (False, False), (True, True)]
)
async def test_approval_wakes_idle_long_poll_to_send_key(
    tmp_path, existing_session, claim_available
):
    from nio.api import Api
    from nio.crypto import OlmAccount, OlmDevice

    polling = asyncio.Event()
    release = asyncio.Event()
    sent = asyncio.Event()
    claims = 0

    async def sync(_):
        polling.set()
        await release.wait()
        return web.json_response({"next_batch": "s2"})

    async def send(request):
        nonlocal claims
        if request.path.endswith("/keys/claim"):
            claims += 1
            if not claim_available:
                return web.json_response({"one_time_keys": {}})
            key_id, key = next(iter(peer.one_time_keys["curve25519"].items()))
            signed = {"key": key}
            signed["signatures"] = {
                USER: {"ed25519:OTHER": peer.sign(Api.to_canonical_json(signed))}
            }
            return web.json_response(
                {
                    "one_time_keys": {
                        USER: {"OTHER": {f"signed_curve25519:{key_id}": signed}}
                    }
                }
            )
        sent.set()
        return web.json_response({})

    async with homeserver(sync, membership=send) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        with session._store.transaction():
            publish_account(nio_client.olm)
            if existing_session:
                peer, device = queue_dummy(nio_client.olm)
            else:
                peer = OlmAccount()
                peer.generate_one_time_keys(1)
                device = OlmDevice(USER, "OTHER", peer.identity_keys)
                nio_client.olm.device_store.add(device)
                nio_client.store.save_device_keys({USER: {"OTHER": device}})
            nio_client.olm.outgoing_to_device_messages.clear()
            request, _ = room_key_request(nio_client.olm)
            session._crypto.capture()
        await session._accept_response(
            json.dumps(
                {"next_batch": "s1", "to_device": {"events": [request.source]}}
            ).encode()
        )
        task = asyncio.create_task(session.run())
        try:
            await drain_sync(session)
            async with asyncio.timeout(2):
                await polling.wait()
            polling.clear()
            nio_client.verify_device(device)
            assert nio_client.continue_key_share(request)
            async with asyncio.timeout(1):
                if claim_available:
                    await sent.wait()
                else:
                    await polling.wait()
                    assert not sent.is_set()
                    assert claims == 1
            await session.quiesce()
            await task
        finally:
            release.set()
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("revoke", ["blacklist", "membership", "rotation"])
async def test_cached_only_recipient_revocation_invalidates_group(tmp_path, revoke):
    recipient = "@peer:example.org"

    async def http(request):
        if request.path.endswith("/joined_members"):
            return web.json_response(
                {"joined": {recipient: {"display_name": "Alice", "avatar_url": None}}}
            )
        return web.json_response({})

    rotated = Olm(recipient, "OTHER", SqliteMemoryStore(recipient, "OTHER"))
    rotated_keys = rotated.share_keys()["device_keys"]

    async def query(_request):
        return web.json_response(
            {"device_keys": {recipient: {"OTHER": rotated_keys}}, "failures": {}}
        )

    async with homeserver(None, membership=http, query=query) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        nio_client.config = replace(nio_client.config, replace_rotated_device_keys=True)
        session = open_session(tmp_path, nio_client)
        with session._store.transaction():
            publish_account(nio_client.olm)
            _, device = queue_dummy(nio_client.olm, user_id=recipient)
            nio_client.olm.outgoing_to_device_messages.clear()
            nio_client.verify_device(device)
            session._crypto.capture()
        room = MatrixRoom(ROOM, USER, encrypted=True)
        room.add_member("@old:example.org", "Old", None)
        nio_client.rooms[ROOM] = room
        try:
            await nio_client.share_group_session(ROOM)
            assert nio_client.olm.outbound_group_sessions[ROOM].shared
            assert recipient not in room.users
            if revoke == "blacklist":
                assert nio_client.blacklist_device(device)
            elif revoke == "rotation":
                nio_client.olm.users_for_key_query.add(recipient)
                response = await nio_client.keys_query()
                assert "OTHER" in response.changed[recipient]
                current = nio_client.device_store[recipient]["OTHER"]
                assert current.ed25519 == rotated_keys["keys"]["ed25519:OTHER"]
                assert current.trust_state is TrustState.unset
            else:
                # Cached current recipient is absent from the historical projection.
                # Ordinary remove_member therefore reports no local change.
                await session._accept_response(
                    json.dumps(
                        {
                            "next_batch": "s1",
                            "rooms": {
                                "join": {
                                    ROOM: {
                                        "timeline": {
                                            "events": [
                                                {
                                                    "type": "m.room.member",
                                                    "state_key": recipient,
                                                    "sender": recipient,
                                                    "event_id": "$departure",
                                                    "origin_server_ts": 1,
                                                    "content": {"membership": "leave"},
                                                }
                                            ]
                                        }
                                    }
                                }
                            },
                        }
                    ).encode()
                )
            assert ROOM not in nio_client.olm.outbound_group_sessions
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_sync_device_change_queries_cached_recipient_after_successful_query(
    tmp_path, restart
):
    recipient = "@peer:example.org"
    unrelated = "@untracked:example.org"
    source = asyncio.Queue()
    queried = []
    rotated = Olm(recipient, "OTHER", SqliteMemoryStore(recipient, "OTHER"))
    rotated_keys = rotated.share_keys()["device_keys"]
    current_keys = None

    async def sync(request):
        return web.Response(body=await source.get())

    async def query(request):
        users = (await request.json())["device_keys"]
        queried.append(set(users))
        return web.json_response(
            {
                "device_keys": {
                    user: {"OTHER": current_keys} if user == recipient else {}
                    for user in users
                },
                "failures": {},
            }
        )

    async def http(request):
        if request.path.endswith("/joined_members"):
            return web.json_response(
                {"joined": {recipient: {"display_name": "Peer", "avatar_url": None}}}
            )
        if request.path.endswith("/keys/claim"):
            keys = rotated.share_keys()["one_time_keys"]
            key_id = next(iter(keys))
            return web.json_response(
                {
                    "one_time_keys": {recipient: {"OTHER": {key_id: keys[key_id]}}},
                    "failures": {},
                }
            )
        return web.json_response({})

    async with homeserver(sync, query=query, membership=http) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        nio_client.config = replace(nio_client.config, replace_rotated_device_keys=True)
        session = open_session(tmp_path, nio_client)
        with session._store.transaction():
            publish_account(nio_client.olm)
            peer, device = queue_dummy(nio_client.olm, user_id=recipient)
            nio_client.olm.outgoing_to_device_messages.clear()
            old = Olm(recipient, "OTHER", SqliteMemoryStore(recipient, "OTHER"))
            old.account = peer
            current_keys = old.share_keys()["device_keys"]
            room = MatrixRoom(ROOM, USER, encrypted=True)
            room.add_member("@old:example.org", "Old", None)
            nio_client.rooms[ROOM] = room
            session._metadata[ROOM] = {
                "membership": "join",
                "membership_epoch": 0,
                "baseline": True,
            }
            session._save_rooms({ROOM: room}, {(ROOM, "@old:example.org")})
            session._crypto.capture()
        await nio_client.joined_members(ROOM)
        await nio_client.keys_query()
        assert queried == [{recipient}]
        assert not nio_client.olm.users_for_key_query
        assert recipient in nio_client.olm.tracked_users
        nio_client.verify_device(device)
        await nio_client.share_group_session(ROOM)
        assert nio_client.olm.outbound_group_sessions[ROOM].shared
        assert recipient not in room.users
        if restart:
            await session.close()
            await nio_client.close()
            nio_client = client()
            nio_client.homeserver = url
            nio_client.config = replace(
                nio_client.config, replace_rotated_device_keys=True
            )
            session = open_session(tmp_path, nio_client)
            assert not session._outbound.member_cache
            assert not nio_client.olm.outbound_group_sessions
            assert recipient not in nio_client.rooms[ROOM].users
        current_keys = rotated_keys
        runner = asyncio.create_task(session.run())
        try:
            await source.put(
                json.dumps(
                    {
                        "next_batch": "changed",
                        "device_lists": {"changed": [recipient, unrelated], "left": []},
                    }
                ).encode()
            )
            await drain_sync(session)
            assert queried == [{recipient}, {recipient}]
            current = nio_client.device_store[recipient]["OTHER"]
            assert current.ed25519 == rotated_keys["keys"]["ed25519:OTHER"]
            assert current.trust_state is TrustState.unset
            assert ROOM not in nio_client.olm.outbound_group_sessions
            with pytest.raises(OlmUnverifiedDeviceError):
                await nio_client.share_group_session(ROOM)
            await session.quiesce()
            await runner
        finally:
            source.put_nowait(b'{"next_batch":"shutdown"}')
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)
