"""Pinned custom to-device operations across ordinary and durable owners."""

import asyncio
import json
import signal
from copy import deepcopy
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from nio import (
    AsyncClient,
    AsyncClientConfig,
    KeysQueryResponse,
    LocalProtocolError,
    ToDeviceMessage,
    ToDeviceEvent,
    ToDeviceResponse,
)
from nio.crypto import Olm
from nio.store import SqliteMemoryStore

from .client_test import CONSUMER, USER, client, open_session
from .runner_test import homeserver

PEER = "@peer:example.org"


@pytest.fixture
def peer():
    store = SqliteMemoryStore(PEER, "OTHER", "")
    result = Olm(PEER, "OTHER", store)
    keys = result.share_keys()
    try:
        yield result, keys
    finally:
        store.database.close()


def query_body(keys):
    return {"device_keys": {PEER: {"OTHER": deepcopy(keys["device_keys"])}}}


def notice(value=1):
    return ToDeviceMessage("org.example.notice", PEER, "OTHER", {"value": value})


@asynccontextmanager
async def transport_client(tmp_path, url, durable):
    if durable:
        result = client()
        result.homeserver = url
        owner = open_session(tmp_path, result)
    else:
        result = AsyncClient(
            url, USER, "ALICE", config=AsyncClientConfig(store=SqliteMemoryStore)
        )
        result.restore_login(USER, "ALICE", "test-token")
        owner = None
    try:
        yield result, owner
    finally:
        if owner is not None:
            await owner.close()
        elif result.store is not None:
            result.store.database.close()
        await result.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_explicit_query_discovers_user_without_shared_room(tmp_path, durable):
    bodies = []

    async def query(request):
        body = await request.json()
        bodies.append(body)
        return web.json_response({"device_keys": {PEER: {}}})

    async with homeserver(None, query=query) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, _):
            response = await nio_client.keys_query(user_set={PEER})
            assert isinstance(response, KeysQueryResponse)
            assert bodies == [{"device_keys": {PEER: []}}]
            assert PEER in nio_client.olm.tracked_users
            with pytest.raises(LocalProtocolError):
                await nio_client.keys_query(user_set=set())


@pytest.mark.asyncio
async def test_explicit_query_does_not_return_unrelated_pending_query(tmp_path):
    bodies = []

    async def query(request):
        body = await request.json()
        bodies.append(body)
        return web.json_response(
            {"device_keys": {user: {} for user in body["device_keys"]}}
        )

    async with homeserver(None, query=query) as (url, _):
        async with transport_client(tmp_path, url, True) as (nio_client, owner):
            with owner._store.transaction():
                nio_client.olm.users_for_key_query.add(USER)
                owner._crypto.enqueue_query()
            response = await nio_client.keys_query(user_set={PEER})
            assert list(response.device_keys) == [PEER]
            assert [body["device_keys"] for body in bodies] == [{USER: []}, {PEER: []}]


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
@pytest.mark.parametrize(
    "body",
    [
        {"device_keys": {}, "failures": {"example.org": {}}},
        # Servers may omit device_keys when every queried server failed.
        {"failures": {"example.org": {}}},
    ],
)
async def test_explicit_query_keeps_missing_user_dirty(tmp_path, durable, body):
    async def query(request):
        return web.json_response(body)

    async with homeserver(None, query=query) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, _):
            await nio_client.keys_query(user_set={PEER})
            assert PEER in nio_client.olm.users_for_key_query


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_encrypted_send_discovers_claims_and_delivers_custom_payload(
    tmp_path, peer, durable
):
    peer_olm, keys = peer
    requests = []

    async def query(request):
        requests.append("query")
        return web.json_response(query_body(keys))

    async def send(request):
        body = await request.json()
        if request.path.endswith("/keys/claim"):
            requests.append("claim")
            assert body["one_time_keys"] == {PEER: {"OTHER": "signed_curve25519"}}
            first = next(iter(keys["one_time_keys"].items()))
            return web.json_response(
                {"one_time_keys": {PEER: {"OTHER": dict([first])}}}
            )
        requests.append("send")
        event = ToDeviceEvent.parse_event(
            {
                "sender": USER,
                "type": "m.room.encrypted",
                "content": body["messages"][PEER]["OTHER"],
            }
        )
        clear = peer_olm.decrypt_event(event)
        assert clear.source["content"] == {"value": 1}
        assert clear.source["type"] == "org.example.notice"
        return web.json_response({})

    async with homeserver(None, query=query, membership=send) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, _):
            response = await nio_client.encrypted_to_device(
                notice(), recipient_ed25519=peer_olm.account.identity_keys["ed25519"]
            )
            assert isinstance(response, ToDeviceResponse)
            assert requests == ["query", "claim", "send"]
            assert not nio_client.olm.device_store[PEER]["OTHER"].verified


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
@pytest.mark.parametrize(
    "rejection",
    [
        "pin",
        "missing",
        "failure",
        "signature",
        "blacklist",
        "rotation",
        "malformed_key",
    ],
)
async def test_encrypted_send_never_uses_stale_or_unauthorized_cached_identity(
    tmp_path, peer, durable, rejection
):
    peer_olm, keys = peer
    body = query_body(keys)
    if rejection == "missing":
        body = {"device_keys": {}}
    elif rejection == "failure":
        body["failures"] = {"example.org": {}}
    elif rejection == "signature":
        body["device_keys"][PEER]["OTHER"]["signatures"][PEER][
            "ed25519:OTHER"
        ] = "invalid"
    elif rejection == "rotation":
        replacement_store = SqliteMemoryStore(PEER, "OTHER", "")
        try:
            replacement = Olm(PEER, "OTHER", replacement_store)
            body = query_body(replacement.share_keys())
        finally:
            replacement_store.database.close()
    elif rejection == "malformed_key":
        body["device_keys"][PEER]["OTHER"]["keys"]["ed25519:OTHER"] = "rotated"

    async def query(request):
        return web.json_response(body)

    async def send(request):
        pytest.fail("unauthorized identity reached claim/send")

    async with homeserver(None, query=query, membership=send) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, owner):
            if owner is not None:
                with owner._store.transaction():
                    nio_client._handle_olm_response(
                        KeysQueryResponse.from_dict(query_body(keys))
                    )
            else:
                await nio_client.receive_response(
                    KeysQueryResponse.from_dict(query_body(keys))
                )
            device = nio_client.olm.device_store[PEER]["OTHER"]
            if rejection == "blacklist":
                nio_client.blacklist_device(device)
            pin = (
                "wrong"
                if rejection == "pin"
                else peer_olm.account.identity_keys["ed25519"]
            )
            with pytest.raises(LocalProtocolError):
                await nio_client.encrypted_to_device(notice(), recipient_ed25519=pin)


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_encrypted_send_cancel_retries_exact_ciphertext_and_rejects_changed_body(
    tmp_path, peer, durable
):
    peer_olm, keys = peer
    entered = asyncio.Event()
    release = asyncio.Event()
    requests = []
    queries = []

    async def query(request):
        queries.append(await request.json())
        return web.json_response(query_body(keys))

    async def send(request):
        if request.path.endswith("/keys/claim"):
            first = next(iter(keys["one_time_keys"].items()))
            return web.json_response(
                {"one_time_keys": {PEER: {"OTHER": dict([first])}}}
            )
        requests.append((request.path, await request.text()))
        if len(requests) == 1:
            entered.set()
            await release.wait()
        return web.json_response({})

    async with homeserver(None, query=query, membership=send) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, _):
            pin = peer_olm.account.identity_keys["ed25519"]
            task = asyncio.create_task(
                nio_client.encrypted_to_device(
                    notice(), recipient_ed25519=pin, tx_id="operation"
                )
            )
            try:
                async with asyncio.timeout(3):
                    await entered.wait()
                task.cancel()
                with pytest.raises(asyncio.CancelledError):
                    await task
                with pytest.raises(LocalProtocolError):
                    await nio_client.encrypted_to_device(
                        notice(2), recipient_ed25519=pin, tx_id="operation"
                    )
                release.set()
                await nio_client.encrypted_to_device(
                    notice(), recipient_ed25519=pin, tx_id="operation"
                )
                assert requests[0] == requests[1]
                assert len(requests) == 2
                assert len(queries) == 1
            finally:
                release.set()
                task.cancel()


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_explicit_query_preserves_invalidation_arriving_during_http(
    tmp_path, durable
):
    async def query(request):
        nio_client.olm.users_for_key_query.add(PEER)
        return web.json_response({"device_keys": {PEER: {}}})

    async with homeserver(None, query=query) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, _):
            await nio_client.keys_query({PEER})
            assert PEER in nio_client.olm.users_for_key_query


@pytest.mark.asyncio
async def test_no_argument_query_can_resume_pending_request_without_dirty_users(
    tmp_path,
):
    async def query(request):
        return web.json_response({"device_keys": {PEER: {}}})

    async with homeserver(None, query=query) as (url, _):
        async with transport_client(tmp_path, url, True) as (nio_client, owner):
            with owner._store.transaction():
                owner._crypto.enqueue_query({PEER})
            assert not nio_client.olm.users_for_key_query
            response = await nio_client.keys_query()
            assert PEER in response.device_keys


def saved_session(owner):
    return owner._store.database.execute_sql(
        "SELECT session FROM olmsessions"
    ).fetchone()[0]


async def seed_session(nio_client, owner, keys):
    with owner._store.transaction():
        nio_client._handle_olm_response(KeysQueryResponse.from_dict(query_body(keys)))
        device = nio_client.olm.device_store[PEER]["OTHER"]
        key = next(iter(keys["one_time_keys"].values()))["key"]
        nio_client.olm.create_session(key, device.curve25519)
        owner._crypto.capture()


@pytest.mark.asyncio
async def test_encrypted_send_rollback_keeps_session_and_request_atomic(
    tmp_path, peer, monkeypatch
):
    peer_olm, keys = peer

    async def query(request):
        return web.json_response(query_body(keys))

    async def send(request):
        pytest.fail("uncommitted encrypted request reached HTTP")

    async with homeserver(None, query=query, membership=send) as (url, _):
        async with transport_client(tmp_path, url, True) as (nio_client, owner):
            await seed_session(nio_client, owner, keys)
            before = saved_session(owner)
            original = owner._crypto.enqueue_message

            def fail_after_retention(*args, **kwargs):
                original(*args, **kwargs)
                raise RuntimeError("injected encryption transaction failure")

            monkeypatch.setattr(owner._crypto, "enqueue_message", fail_after_retention)
            with pytest.raises(RuntimeError, match="injected"):
                await nio_client.encrypted_to_device(
                    notice(),
                    recipient_ed25519=peer_olm.account.identity_keys["ed25519"],
                )
            with pytest.raises(LocalProtocolError):
                await nio_client.keys_query({PEER})
        async with transport_client(tmp_path, url, True) as (_, owner):
            assert saved_session(owner) == before
            assert owner._crypto._pending() is None


@pytest.mark.asyncio
@pytest.mark.parametrize("before_http", [False, True])
async def test_encrypted_send_reopen_replays_committed_ciphertext(
    tmp_path, peer, monkeypatch, before_http
):
    peer_olm, keys = peer
    requests = []

    async def query(request):
        return web.json_response(query_body(keys))

    async def send(request):
        requests.append((request.path, await request.text()))
        return web.json_response({})

    async with homeserver(None, query=query, membership=send) as (url, _):
        async with transport_client(tmp_path, url, True) as (nio_client, owner):
            await seed_session(nio_client, owner, keys)
            before = saved_session(owner)
            request_http = owner._transport.request

            async def interrupt_request(method, path, body=None):
                if "/sendToDevice/" in path:
                    if not before_http:
                        await request_http(method, path, body)
                    raise asyncio.CancelledError
                return await request_http(method, path, body)

            monkeypatch.setattr(owner._transport, "request", interrupt_request)
            with pytest.raises(asyncio.CancelledError):
                await nio_client.encrypted_to_device(
                    notice(),
                    recipient_ed25519=peer_olm.account.identity_keys["ed25519"],
                    tx_id="reopen",
                )
            pending = owner._crypto._pending()[0]
            assert saved_session(owner) != before
        async with transport_client(tmp_path, url, True) as (nio_client, owner):
            assert owner._crypto._pending()[0] == pending
            with pytest.raises(LocalProtocolError):
                await nio_client.encrypted_to_device(
                    notice(2),
                    recipient_ed25519=peer_olm.account.identity_keys["ed25519"],
                    tx_id="reopen",
                )
            await nio_client.encrypted_to_device(
                notice(),
                recipient_ed25519=peer_olm.account.identity_keys["ed25519"],
                tx_id="reopen",
            )
            assert owner._crypto._pending() is None
            assert requests[-1] == (pending.path, pending.body)
            if not before_http:
                assert requests[0] == requests[1]
            encrypted = json.loads(pending.body)["messages"][PEER]["OTHER"]
            event = ToDeviceEvent.parse_event(
                {"sender": USER, "type": "m.room.encrypted", "content": encrypted}
            )
            assert peer_olm.decrypt_event(event).source["content"] == {"value": 1}


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_encrypted_send_rechecks_blacklist_after_claim_await(
    tmp_path, peer, durable
):
    peer_olm, keys = peer

    async def query(request):
        return web.json_response(query_body(keys))

    async def send(request):
        assert request.path.endswith("/keys/claim")
        nio_client.blacklist_device(nio_client.olm.device_store[PEER]["OTHER"])
        first = next(iter(keys["one_time_keys"].items()))
        return web.json_response({"one_time_keys": {PEER: {"OTHER": dict([first])}}})

    async with homeserver(None, query=query, membership=send) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, _):
            with pytest.raises(LocalProtocolError):
                await nio_client.encrypted_to_device(
                    notice(),
                    recipient_ed25519=peer_olm.account.identity_keys["ed25519"],
                )


KILLED_SEND_WORKER = """
import asyncio, os, signal, sys
from pathlib import Path
from uuid import UUID
from nio import AsyncClient, ToDeviceMessage
from nio.durable import open_durable_sync

async def main():
    path, url, user, consumer, pin, checkpoint = sys.argv[1:]
    client = AsyncClient(url, user, "ALICE", store_path=None)
    client.restore_login(user, "ALICE", "test-token")
    owner = open_durable_sync(client, consumer_id=UUID(consumer), store_path=Path(path))
    if checkpoint == "before_commit":
        enqueue = owner._crypto.enqueue_message
        def interrupted_enqueue(*args, **kwargs):
            enqueue(*args, **kwargs)
            os.kill(os.getpid(), signal.SIGKILL)
        owner._crypto.enqueue_message = interrupted_enqueue
    else:
        request = owner._transport.request
        async def interrupted_request(method, path, body=None):
            if "/sendToDevice/" in path:
                if checkpoint == "after_http":
                    await request(method, path, body)
                os.kill(os.getpid(), signal.SIGKILL)
            return await request(method, path, body)
        owner._transport.request = interrupted_request
    await client.encrypted_to_device(
        ToDeviceMessage("org.example.notice", "@peer:example.org", "OTHER", {"value": 1}),
        recipient_ed25519=pin, tx_id="killed-operation",
    )

asyncio.run(main())
"""


@pytest.mark.asyncio
@pytest.mark.parametrize("checkpoint", ["before_commit", "after_commit", "after_http"])
async def test_process_kill_preserves_atomic_encryption_and_exact_replay(
    tmp_path, peer, checkpoint
):
    peer_olm, keys = peer
    requests = []

    async def query(request):
        return web.json_response(query_body(keys))

    async def send(request):
        requests.append((request.path, await request.text()))
        return web.json_response({})

    async with homeserver(None, query=query, membership=send) as (url, _):
        async with transport_client(tmp_path, url, True) as (nio_client, owner):
            await seed_session(nio_client, owner, keys)
            before = saved_session(owner)
        process = await asyncio.create_subprocess_exec(
            "uv",
            "run",
            "--no-sync",
            "python",
            "-c",
            KILLED_SEND_WORKER,
            str(tmp_path),
            url,
            USER,
            str(CONSUMER),
            peer_olm.account.identity_keys["ed25519"],
            checkpoint,
            stdout=asyncio.subprocess.PIPE,
            stderr=asyncio.subprocess.PIPE,
        )
        try:
            async with asyncio.timeout(15):
                _, stderr = await process.communicate()
            assert process.returncode in (
                -signal.SIGKILL,
                128 + signal.SIGKILL,
            ), stderr.decode()
        finally:
            if process.returncode is None:
                process.kill()
                await process.wait()
        async with transport_client(tmp_path, url, True) as (nio_client, owner):
            pending = owner._crypto._pending()
            if checkpoint == "before_commit":
                assert pending is None
                assert saved_session(owner) == before
            else:
                assert pending is not None
                assert saved_session(owner) != before
            await nio_client.encrypted_to_device(
                notice(),
                recipient_ed25519=peer_olm.account.identity_keys["ed25519"],
                tx_id="killed-operation",
            )
            if pending:
                assert requests[-1] == (pending[0].path, pending[0].body)
            if checkpoint == "after_http":
                assert requests[0] == requests[1]
            encrypted = json.loads(requests[-1][1])["messages"][PEER]["OTHER"]
            event = ToDeviceEvent.parse_event(
                {"sender": USER, "type": "m.room.encrypted", "content": encrypted}
            )
            assert peer_olm.decrypt_event(event).source["content"] == {"value": 1}


@pytest.mark.asyncio
async def test_corrupt_encrypted_fingerprint_fails_durable_restore(tmp_path):
    async with transport_client(tmp_path, "https://example.org", True) as (_, owner):
        with owner._store.transaction():
            owner._crypto.enqueue_message(notice(), encrypted_fingerprint="invalid")
    with pytest.raises(LocalProtocolError, match="fingerprint"):
        async with transport_client(tmp_path, "https://example.org", True):
            pass


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_unsolicited_query_devices_do_not_modify_other_users(
    tmp_path, peer, durable
):
    _, keys = peer

    async def query(request):
        return web.json_response(query_body(keys))

    async with homeserver(None, query=query) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, _):
            response = await nio_client.keys_query({USER})
            assert response.device_keys == {}
            assert not nio_client.olm.device_store[PEER]


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
async def test_empty_key_claim_fails_without_disposing_client(tmp_path, peer, durable):
    peer_olm, keys = peer

    async def query(request):
        return web.json_response(query_body(keys))

    async def send(request):
        assert request.path.endswith("/keys/claim")
        return web.json_response({"one_time_keys": {}, "failures": {"example.org": {}}})

    async with homeserver(None, query=query, membership=send) as (url, _):
        async with transport_client(tmp_path, url, durable) as (nio_client, _):
            with pytest.raises(LocalProtocolError, match="session"):
                await nio_client.encrypted_to_device(
                    notice(),
                    recipient_ed25519=peer_olm.account.identity_keys["ed25519"],
                )
            assert isinstance(await nio_client.keys_query({PEER}), KeysQueryResponse)


@pytest.mark.asyncio
@pytest.mark.parametrize("durable", [False, True])
@pytest.mark.parametrize("invalid", ["wildcard", "empty_device", "encrypted", "nan"])
async def test_invalid_encrypted_send_never_starts_http(tmp_path, durable, invalid):
    message = notice()
    if invalid == "wildcard":
        message.recipient_device = "*"
    elif invalid == "empty_device":
        message.recipient_device = ""
    elif invalid == "encrypted":
        message.type = "m.room.encrypted"
    else:
        message.content = {"value": float("nan")}
    async with transport_client(tmp_path, "https://example.invalid", durable) as (
        nio_client,
        _,
    ):
        with pytest.raises(LocalProtocolError):
            await nio_client.encrypted_to_device(message, recipient_ed25519="pinned")
