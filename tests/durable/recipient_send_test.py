"""Fresh encryption recipients do not replace the durable room projection."""

import json

import pytest
import vodozemac
from aiohttp import web
from unpaddedbase64 import decode_base64

from nio.crypto import Olm
from nio.exceptions import MembersSyncError
from nio.responses import RoomSendResponse
from nio.rooms import MatrixRoom
from nio.store import SqliteMemoryStore

from .client_test import ROOM, USER, client, open_session
from .runner_test import homeserver


@pytest.mark.asyncio
async def test_room_send_uses_fresh_recipients_without_replacing_room_members(tmp_path):
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
            result = await nio_client.room_send(
                ROOM,
                "m.room.message",
                {"msgtype": "m.text", "body": "fresh recipient secret"},
                tx_id="fresh-send",
                ignore_unverified_devices=True,
            )
            assert isinstance(result, RoomSendResponse)
            assert result.event_id == "$sent"
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
