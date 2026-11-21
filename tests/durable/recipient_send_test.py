"""Fresh encryption recipients do not replace the durable room projection."""

import asyncio
import json

import pytest
import vodozemac
from aiohttp import web
from unpaddedbase64 import decode_base64

from nio import AsyncClient
from nio.crypto import Olm
from nio.exceptions import (
    LocalProtocolError,
    MembersSyncError,
    RoomRecipientsChangedError,
)
from nio.responses import RoomSendResponse
from nio.rooms import MatrixRoom
from nio.store import SqliteMemoryStore

from .client_test import ROOM, USER, client, open_session, response
from .crypto_test import publish_account, queue_dummy
from .recovery_test import limited, member
from .runner_test import homeserver
from .sliding_test import settings, settle, window

ENCRYPTION = {
    "type": "m.room.encryption",
    "state_key": "",
    "sender": USER,
    "event_id": "$encryption",
    "origin_server_ts": 1,
    "content": {"algorithm": "m.megolm.v1.aes-sha2"},
}


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ["room_send", "room_encrypt"])
async def test_room_send_uses_fresh_recipients_without_replacing_room_members(
    tmp_path, operation
):
    recipient = "@peer:example.org"
    peer = Olm(recipient, "OTHER", SqliteMemoryStore(recipient, "OTHER"))
    keys = peer.share_keys()
    sent = []
    shared = []

    async def query(request):
        assert set((await request.json())["device_keys"]) == {recipient}
        return web.json_response(
            {"device_keys": {recipient: {"OTHER": keys["device_keys"]}}, "failures": {}}
        )

    async def http(request):
        if request.path.endswith("/joined_members"):
            return web.json_response(
                {"joined": {recipient: {"display_name": "Peer", "avatar_url": None}}}
            )
        if request.path.endswith("/keys/claim"):
            assert (await request.json())["one_time_keys"] == {
                recipient: {"OTHER": "signed_curve25519"}
            }
            key_id = next(iter(keys["one_time_keys"]))
            return web.json_response(
                {
                    "one_time_keys": {
                        recipient: {"OTHER": {key_id: keys["one_time_keys"][key_id]}}
                    },
                    "failures": {},
                }
            )
        if "/sendToDevice/m.room.encrypted/" in request.path:
            messages = (await request.json())["messages"]
            assert set(messages) == {recipient}
            shared.append(messages[recipient]["OTHER"])
            return web.json_response({})
        if request.path.endswith("/send/m.room.encrypted/fresh-send"):
            sent.append(await request.json())
            return web.json_response({"event_id": "$sent"})
        raise AssertionError(request.path)

    async with homeserver(None, query=query, membership=http) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        room = MatrixRoom(ROOM, USER, encrypted=True)
        room.add_member("@old:example.org", "Old", None)
        nio_client.rooms[ROOM] = room
        try:
            if operation == "room_send":
                result = await nio_client.room_send(
                    ROOM,
                    "m.room.message",
                    {"msgtype": "m.text", "body": "fresh recipient secret"},
                    tx_id="fresh-send",
                    ignore_unverified_devices=True,
                )
                assert isinstance(result, RoomSendResponse)
                assert result.event_id == "$sent"
            else:
                sent.append(
                    await nio_client.room_encrypt(
                        ROOM,
                        "m.room.message",
                        {"msgtype": "m.text", "body": "fresh recipient secret"},
                        ignore_unverified_devices=True,
                    )
                )
            assert not room.members_synced
            assert set(room.users) == {"@old:example.org"}
            assert len(shared) == len(sent) == 1
            content = shared[0]
            encrypted = content["ciphertext"][peer.account.identity_keys["curve25519"]]
            message = vodozemac.AnyOlmMessage.from_parts(
                encrypted["type"], decode_base64(encrypted["body"])
            )
            _, clear = peer.account.create_inbound_session(
                content["sender_key"], message
            )
            share = json.loads(clear)
            assert share["type"] == "m.room_key"
            inbound = vodozemac.InboundGroupSession(
                vodozemac.SessionKey(share["content"]["session_key"])
            )
            assert sent[0]["algorithm"] == "m.megolm.v1.aes-sha2"
            plaintext = inbound.decrypt(
                vodozemac.MegolmMessage.from_base64(sent[0]["ciphertext"])
            ).plaintext
            assert json.loads(plaintext) == {
                "room_id": ROOM,
                "type": "m.room.message",
                "content": {"msgtype": "m.text", "body": "fresh recipient secret"},
            }
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "race",
    [
        "duplicate-at-lookup",
        "maintenance-at-retry-lookup",
        "query-fails-at-retry",
        "equal-lookups-with-already-shared-session",
        "leave-at-claim",
        "leave-at-wait-with-complete-members",
        "two-joins-while-waiting",
        "two-joins-while-waiting-with-complete-members",
        "duplicate-at-share",
        "retry-applies-share-policy",
        "repeated",
    ],
)
async def test_room_send_retries_sliding_recipient_invalidation(
    tmp_path, monkeypatch, race
):
    recipient, other = "@peer:example.org", "@other:example.org"
    members = {USER, recipient}
    own_join = member("$join", "join")
    other_join = member("$other-join", "join", other)
    state = [own_join, member("$peer", "join", recipient), ENCRYPTION]
    if race == "leave-at-claim":
        members.add(other)
        state.append(other_join)
    keys = {
        user: Olm(user, "OTHER", SqliteMemoryStore(user, "OTHER")).share_keys()
        for user in (recipient, other)
    }
    lookups = 0
    queries = []
    shares = []
    sent = []
    tasks = []
    same_lookup = race == "equal-lookups-with-already-shared-session"
    complete = race.endswith("-with-complete-members")
    leave_at_wait = race == "leave-at-wait-with-complete-members"
    two_joins = race.startswith("two-joins-while-waiting")

    async def start_task(coro):
        started = asyncio.Event()

        async def run():
            started.set()
            return await coro

        task = asyncio.create_task(run())
        tasks.append(task)
        await started.wait()
        return task

    async def sync(event, pos):
        await session._accept_response(window(pos=pos, initial=False, state=[event]))
        await settle(session)

    async def query(request):
        requested = (await request.json())["device_keys"]
        queries.append(set(requested))
        if race == "query-fails-at-retry" or (leave_at_wait and len(queries) > 1):
            if leave_at_wait:
                assert set(requested) == {other}
            return web.json_response(
                {"errcode": "M_FORBIDDEN", "error": "Denied"}, status=403
            )
        if same_lookup and len(queries) == 2:
            # A new device-list notification makes a spurious retry observable.
            nio_client.olm.users_for_key_query.add(recipient)
        return web.json_response(
            {
                "device_keys": {
                    user: {"OTHER": keys[user]["device_keys"]} if user in keys else {}
                    for user in requested
                },
                "failures": {},
            }
        )

    async def http(request):
        nonlocal lookups
        if request.path.endswith("/joined_members"):
            lookups += 1
            joined = {user: {} for user in members}
            if (race == "repeated" and lookups <= 2) or (
                race
                in (
                    "duplicate-at-lookup",
                    "maintenance-at-retry-lookup",
                    "query-fails-at-retry",
                )
                and lookups == 1
            ):
                await sync(own_join, f"lookup-{lookups}")
            if race == "maintenance-at-retry-lookup" and lookups == 2:
                # Queue maintenance behind the replacement lookup before it replies.
                await start_task(session._maintain_crypto())
            if same_lookup and lookups == 2:
                joined = dict(reversed(tuple(joined.items())))
            return web.json_response({"joined": joined})
        if request.path.endswith("/keys/claim"):
            requested = (await request.json())["one_time_keys"]
            if race == "leave-at-claim" and other in members:
                assert other in requested
                members.remove(other)
                await sync(member("$leave", "leave", other), "leave")
            return web.json_response(
                {
                    "one_time_keys": {
                        user: {"OTHER": keys[user]["one_time_keys"]}
                        for user in requested
                    },
                    "failures": {},
                }
            )
        if "/sendToDevice/m.room.encrypted/" in request.path:
            shares.append(set((await request.json())["messages"]))
            if (
                race in ("duplicate-at-share", "retry-applies-share-policy")
                and len(shares) == 1
            ):
                await sync(own_join, "during-share")
            return web.json_response({})
        if "/send/" in request.path:
            sent.append((request.path, await request.json()))
            return web.json_response({"event_id": "$sent"})
        raise AssertionError(request.path)

    class PolicyClient(AsyncClient):
        share_attempts = 0

        async def share_group_session(self, room_id, ignore_unverified_devices=False):
            self.share_attempts += 1
            if self.share_attempts == 2:
                raise LocalProtocolError("subclass denied key sharing")
            return await super().share_group_session(room_id, ignore_unverified_devices)

    async with homeserver(None, query=query, membership=http) as (url, _):
        if race == "retry-applies-share-policy":
            nio_client = PolicyClient(url, USER, "ALICE")
            nio_client.restore_login(USER, "ALICE", "test-token")
        else:
            nio_client = client()
            nio_client.homeserver = url
        session = open_session(
            tmp_path,
            nio_client,
            config=settings(
                room_subscriptions={
                    ROOM: {"required_state": [["m.room.member", "$LAZY"]]}
                }
            ),
        )
        try:
            await session._accept_response(window(state=state))
            await settle(session)
            if two_joins:
                await nio_client.keys_query()
                nio_client.rooms[ROOM].members_synced = complete
                if not complete:
                    await nio_client.joined_members(ROOM)
                third = "@third:example.org"
                first, second = asyncio.Event(), asyncio.Event()
                nio_client.sharing_session[ROOM] = first

                async def first_share_ends():
                    members.add(other)
                    await sync(other_join, "first-join")
                    nio_client.sharing_session[ROOM] = second
                    first.set()

                async def second_share_ends():
                    members.add(third)
                    await sync(member("$third-join", "join", third), "second-join")
                    nio_client.sharing_session.pop(ROOM).set()

                monkeypatch.setattr(first, "wait", first_share_ends)
                monkeypatch.setattr(second, "wait", second_share_ends)
            if leave_at_wait:
                # Track the unrelated user's devices without adding them to this room.
                await nio_client.keys_query({USER, recipient, other})
                nio_client.rooms[ROOM].members_synced = True
                sharing = asyncio.Event()
                nio_client.sharing_session[ROOM] = sharing

                async def finish_share_after_leave():
                    members.remove(recipient)
                    update = json.loads(
                        window(
                            pos="leave",
                            initial=False,
                            state=[member("$leave", "leave", recipient)],
                        )
                    )
                    update["extensions"]["e2ee"] = {
                        "device_lists": {"changed": [other], "left": []}
                    }
                    await session._accept_response(json.dumps(update).encode())
                    await settle(session)
                    assert set(nio_client.rooms[ROOM].users) == {USER}
                    assert nio_client.olm.users_for_key_query == {other}
                    nio_client.sharing_session.pop(ROOM).set()

                monkeypatch.setattr(sharing, "wait", finish_share_after_leave)

            async def send_notice(tx_id="notice"):
                return await nio_client.room_send(
                    ROOM,
                    "m.room.message",
                    {"msgtype": "m.notice", "body": "secret notice"},
                    tx_id=tx_id,
                    ignore_unverified_devices=True,
                )

            if same_lookup:
                assert isinstance(await send_notice("first"), RoomSendResponse)
                assert not nio_client.olm.should_share_group_session(ROOM)
                nio_client.olm.users_for_key_query.add(recipient)
                async with session._crypto_lock:
                    sending = await start_task(send_notice())
                    refresh = await start_task(nio_client.joined_members(ROOM))
                assert isinstance(await sending, RoomSendResponse)
                await refresh
                assert queries == [{USER, recipient}, {recipient}]
                assert shares == [{recipient}]
                assert [path.rsplit("/", 2)[-2:] for path, _ in sent] == [
                    ["m.room.encrypted", "first"],
                    ["m.room.encrypted", "notice"],
                ]
                assert sent[0][1]["session_id"] == sent[1][1]["session_id"]
            elif race == "query-fails-at-retry":
                with pytest.raises(LocalProtocolError, match="status=403"):
                    await send_notice()
                assert queries == [{USER, recipient}]
                assert not shares and not sent
            elif race == "repeated":
                with pytest.raises(RoomRecipientsChangedError):
                    await send_notice()
                assert not shares and not sent
            elif race == "retry-applies-share-policy":
                with pytest.raises(LocalProtocolError, match="subclass denied"):
                    await send_notice()
                assert shares == [{recipient}]
                assert not sent
            else:
                result = await send_notice()
                assert isinstance(result, RoomSendResponse)
                assert result.event_id == "$sent"
                if leave_at_wait:
                    assert not shares
                    assert queries == [{USER, recipient, other}]
                elif two_joins:
                    assert first.is_set() and second.is_set()
                    assert shares == [{recipient}]
                elif race == "duplicate-at-share":
                    assert shares == [{recipient}, {recipient}]
                else:
                    assert shares == [{recipient}]
                [(path, payload)] = sent
                assert path.endswith("/send/m.room.encrypted/notice")
                assert payload["algorithm"] == "m.megolm.v1.aes-sha2"
                assert payload["ciphertext"]
                assert "body" not in payload
                if race == "maintenance-at-retry-lookup":
                    await tasks[0]
                    assert queries == [{USER, recipient}]
                    with pytest.raises(
                        LocalProtocolError, match="No key query required"
                    ):
                        await nio_client.keys_query()
            assert lookups == (0 if complete else 2)
        finally:
            await asyncio.gather(*tasks, return_exceptions=True)
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_room_send_retry_stops_after_recovery_observes_own_leave(tmp_path):
    peer = "@peer:example.org"
    lookups = 0

    async def observe_leave():
        await session._accept_response(limited(section="leave"))
        await session._recovery.advance()
        # Recovery has committed the leave but has not reached the tail.
        assert session._metadata[ROOM]["membership"] == "leave"
        assert nio_client.rooms[ROOM].encrypted

    async def http(request):
        nonlocal lookups
        if request.path.endswith("/joined_members"):
            lookups += 1
            if lookups == 1:
                await observe_leave()
            return web.json_response({"joined": {USER: {}, peer: {}}})
        if request.path.endswith("/messages"):
            return web.json_response(
                {"start": "s1", "end": "tail", "chunk": [member("$leave", "leave")]}
            )
        if "/send/" in request.path:
            return web.json_response({"event_id": "$sent"})
        if "/sendToDevice/" in request.path:
            return web.json_response({})
        raise AssertionError(request.path)

    async with homeserver(None, membership=http) as (url, requests):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        try:
            initial = json.loads(response(messages=0))
            initial["rooms"]["join"][ROOM]["state"]["events"].append(ENCRYPTION)
            await session._accept_response(
                json.dumps(initial).encode(), full_state=True
            )
            await settle(session)
            with session._outbound.transaction():
                publish_account(nio_client.olm)
                _, device = queue_dummy(nio_client.olm, user_id=peer)
                nio_client.verify_device(device)
                nio_client.olm.tracked_users.add(peer)
                nio_client.olm.outgoing_to_device_messages.clear()
                session._crypto.capture()
            with pytest.raises(LocalProtocolError, match="No longer joined"):
                await nio_client.room_send(
                    ROOM, "m.room.message", {"msgtype": "m.notice", "body": "secret"}
                )
            assert not any(
                "/send/" in path or "/sendToDevice/" in path for path, _ in requests
            )
            assert lookups == 1
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("refresh", ["remove", "add", "profile", "uncached-remove"])
async def test_recipient_refresh_rotates_only_for_changed_or_unknown_members(
    tmp_path, refresh
):
    first, second = "@first:example.org", "@second:example.org"
    members = [first] if refresh == "add" else [first, second]
    sent = []
    shares = []

    async def http(request):
        if request.path.endswith("/joined_members"):
            return web.json_response(
                {
                    "joined": {
                        user: {"display_name": f"Name {len(sent)}", "avatar_url": None}
                        for user in members
                    }
                }
            )
        if "/sendToDevice/m.room.encrypted/" in request.path:
            shares.append((await request.json())["messages"])
            return web.json_response({})
        if "/send/m.room.encrypted/" in request.path:
            sent.append(await request.json())
            return web.json_response({"event_id": f"$sent-{len(sent)}"})
        raise AssertionError(request.path)

    async with homeserver(None, membership=http) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        with session._outbound.transaction():
            publish_account(nio_client.olm)
            peers = {}
            for user in (first, second):
                peers[user], device = queue_dummy(nio_client.olm, user_id=user)
                nio_client.verify_device(device)
            nio_client.olm.outgoing_to_device_messages.clear()
            session._crypto.capture()
        room = MatrixRoom(ROOM, USER, encrypted=True)
        room.add_member("@historical:example.org", "Historical", None)
        nio_client.rooms[ROOM] = room
        try:
            await nio_client.room_send(
                ROOM, "m.room.message", {"msgtype": "m.text", "body": "before"}
            )
            content = shares[0][first]["OTHER"]
            ciphertext = content["ciphertext"][peers[first].identity_keys["curve25519"]]
            _, clear = peers[first].create_inbound_session(
                content["sender_key"],
                vodozemac.AnyOlmMessage.from_parts(
                    ciphertext["type"], decode_base64(ciphertext["body"])
                ),
            )
            inbound = vodozemac.InboundGroupSession(
                vodozemac.SessionKey(json.loads(clear)["content"]["session_key"])
            )
            assert (
                json.loads(
                    inbound.decrypt(
                        vodozemac.MegolmMessage.from_base64(sent[0]["ciphertext"])
                    ).plaintext
                )["content"]["body"]
                == "before"
            )

            if refresh == "uncached-remove":
                session._outbound.member_cache.pop(ROOM)
                members = []
            elif refresh == "remove":
                members = [second]
            else:
                # Same members, reversed and renamed, must keep their session.
                members = [second, first]
            await nio_client.joined_members(ROOM)
            await nio_client.room_send(
                ROOM, "m.room.message", {"msgtype": "m.text", "body": "after"}
            )
            assert set(room.users) == {"@historical:example.org"}
            assert not room.members_synced
            if refresh == "profile":
                assert sent[1]["session_id"] == sent[0]["session_id"]
                assert len(shares) == 1
            else:
                assert sent[1]["session_id"] != sent[0]["session_id"]
                if members:
                    assert len(shares) == 2
                    assert set(shares[1]) == set(members)
                else:
                    assert len(shares) == 1
                with pytest.raises(vodozemac.MegolmDecryptionException):
                    inbound.decrypt(
                        vodozemac.MegolmMessage.from_base64(sent[1]["ciphertext"])
                    )
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("membership", ["legacy", "missing", "pending"])
async def test_encrypt_rejects_incomplete_recipient_lookup(tmp_path, membership):
    nio_client = client()
    session = None
    if membership == "legacy":
        nio_client.store = SqliteMemoryStore(USER, "ALICE")
        nio_client.olm = Olm(USER, "ALICE", nio_client.store)
    else:
        session = open_session(tmp_path, nio_client)
        if membership == "pending":
            session._outbound.member_cache[ROOM] = None
    nio_client.rooms[ROOM] = MatrixRoom(ROOM, USER, encrypted=True)
    try:
        with pytest.raises(MembersSyncError):
            nio_client.encrypt(
                ROOM, "m.room.message", {"msgtype": "m.text", "body": "secret"}
            )
    finally:
        if session is not None:
            await session.close()
        await nio_client.close()
