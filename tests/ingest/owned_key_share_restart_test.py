"""Interactive own-device key sharing survives owned-store reconstruction."""

from uuid import uuid4
from types import SimpleNamespace
import json
import os
import sqlite3
import re

import pytest
from coordinator_test import (
    ACCOUNT,
    DEVICE,
    ROOM,
    open_owned_bootstrap,
    open_owned_session,
    owned_client,
    reopen_owned_bootstrap,
    stage_classic,
    _raw_e2ee_rows,
)

from nio.crypto import Olm, OlmDevice
from nio import AsyncClient
from nio.events import (
    RoomKeyRequest,
    RoomKeyRequestCancellation,
    ToDeviceEvent,
    ForwardedRoomKeyEvent,
    Event,
)
from nio.event_builders import RoomKeyRequestMessage
from nio.responses import ToDeviceResponse
from nio.exceptions import LocalProtocolError
from nio.ingest.errors import JournalIntegrityError
from nio.ingest.source import canonical_json
from nio.store import SqliteMemoryStore
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus


@pytest.mark.asyncio
@pytest.mark.parametrize("callback_acknowledged", [False, True])
async def test_owned_pending_key_share_survives_restart(
    tmp_path, callback_acknowledged
):
    generation = uuid4()
    bootstrap, _ = open_owned_bootstrap(tmp_path, generation)
    client = owned_client(tmp_path)
    session = open_owned_session(client, bootstrap, generation)
    remote_store = SqliteMemoryStore(ACCOUNT, "OTHER", "remote-secret")
    remote = Olm(ACCOUNT, "OTHER", remote_store)
    callbacks = []

    async def remember_request(event):
        callbacks.append(event)

    client.add_to_device_callback(remember_request, RoomKeyRequest)
    try:
        olm = client.olm
        assert olm is not None
        olm.account.shared = True
        olm.uploaded_key_count = olm.account.max_one_time_keys
        olm.save_account()
        remote_device = OlmDevice(ACCOUNT, "OTHER", remote.account.identity_keys)
        client.store.save_device_keys({ACCOUNT: {"OTHER": remote_device}})
        olm.device_store.add(remote_device)
        remote.account.generate_one_time_keys(1)
        remote_key = next(iter(remote.account.one_time_keys["curve25519"].values()))
        olm.create_session(remote_key, remote_device.curve25519)
        olm.create_outbound_group_session(ROOM)
        group_session = olm.outbound_group_sessions[ROOM]
        source = {
            "type": "m.room_key_request",
            "sender": ACCOUNT,
            "content": {
                "action": "request",
                "request_id": "interactive-request",
                "requesting_device_id": "OTHER",
                "body": {
                    "algorithm": "m.megolm.v1.aes-sha2",
                    "room_id": ROOM,
                    "sender_key": olm.account.identity_keys["curve25519"],
                    "session_id": group_session.id,
                },
            },
        }
        staged = stage_classic(
            bootstrap,
            {
                "device_lists": {"changed": [], "left": []},
                "device_one_time_keys_count": {
                    "signed_curve25519": olm.account.max_one_time_keys
                },
                "device_unused_fallback_key_types": [],
                "next_batch": "interactive-request-token",
                "rooms": {},
                "to_device": {"events": [source]},
            },
        )
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        pending = client.get_active_key_requests(ACCOUNT, "OTHER")
        assert len(pending) == 1
        request = pending[0]
        assert request.source == source
        assert client.outgoing_to_device_messages == []
        source_batch = session.next_batch(max_records=1)
        assert source_batch is not None
        await session._settle_batch(
            source_batch, receipt_new=True, semantic_event_new=False
        )
        assert callbacks == []
        callback_batch = session.next_batch(max_records=1)
        assert callback_batch is not None
        if callback_acknowledged:
            await session._settle_batch(
                callback_batch, receipt_new=True, semantic_event_new=False
            )
            assert callbacks == [request]
            assert await session._advance_blocked_frame(staged.frame_id)
            assert session._journal.list_frames(1) == ()
        assert client.get_active_key_requests(ACCOUNT, "OTHER") == [request]
    finally:
        await session.close()

    reopened = reopen_owned_bootstrap(tmp_path, generation)
    restored = owned_client(tmp_path)
    session = open_owned_session(restored, reopened, generation)
    try:
        if not callback_acknowledged:
            restored.add_to_device_callback(remember_request, RoomKeyRequest)
            replay = session.next_batch(max_records=1)
            assert replay == callback_batch
            await session._settle_batch(
                replay, receipt_new=True, semantic_event_new=False
            )
            assert callbacks == [request]
        else:
            assert session.next_batch() is None
        device = restored.device_store[ACCOUNT]["OTHER"]
        assert not device.verified
        assert restored.outgoing_to_device_messages == []
        # Unknown requests must not replace a still-pending approval after reopen.
        assert restored.continue_key_share(request) is False
        assert restored.get_active_key_requests(ACCOUNT, "OTHER") == [request]
        assert restored.verify_device(device)
        assert restored.continue_key_share(request)
        assert restored.get_active_key_requests(ACCOUNT, "OTHER") == []
        messages = restored.outgoing_to_device_messages
        assert len(messages) == 1
        assert (
            messages[0].type,
            messages[0].recipient,
            messages[0].recipient_device,
        ) == (
            "m.room.encrypted",
            ACCOUNT,
            "OTHER",
        )
    finally:
        await session.close()
        remote_store.database.close()


async def _prepared_share(path, *, missing_session=False, client_type=None):
    generation = uuid4()
    bootstrap, _ = open_owned_bootstrap(path, generation)
    client = (
        owned_client(path)
        if client_type is None
        else owned_client(path, client_type=client_type)
    )
    session = open_owned_session(client, bootstrap, generation)
    remote_store = SqliteMemoryStore(ACCOUNT, "OTHER", "remote-secret")
    remote = Olm(ACCOUNT, "OTHER", remote_store)
    olm = client.olm
    olm.account.shared = True
    olm.uploaded_key_count = olm.account.max_one_time_keys
    olm.save_account()
    remote_device = OlmDevice(ACCOUNT, "OTHER", remote.account.identity_keys)
    local_device = OlmDevice(ACCOUNT, DEVICE, olm.account.identity_keys)
    for machine, device in ((olm, remote_device), (remote, local_device)):
        machine.device_store.add(device)
        machine.store.save_device_keys({device.user_id: {device.id: device}})
    remote.verify_device(local_device)
    remote.account.generate_one_time_keys(1)
    keys = remote.share_keys()["one_time_keys"]
    if not missing_session:
        olm.create_session(next(iter(keys.values()))["key"], remote_device.curve25519)
    olm.create_outbound_group_session(ROOM)
    group = olm.outbound_group_sessions[ROOM]
    group.shared = True
    encrypted = olm.group_encrypt(
        ROOM,
        {
            "type": "m.room.message",
            "content": {"msgtype": "m.text", "body": "recoverable secret"},
        },
    )
    content = {
        "action": "request",
        "request_id": group.id,
        "requesting_device_id": "OTHER",
        "body": {
            "algorithm": "m.megolm.v1.aes-sha2",
            "room_id": ROOM,
            "sender_key": local_device.curve25519,
            "session_id": group.id,
        },
    }
    message = RoomKeyRequestMessage(
        "m.room_key_request",
        ACCOUNT,
        DEVICE,
        content,
        group.id,
        group.id,
        ROOM,
        "m.megolm.v1.aes-sha2",
    )
    remote.outgoing_to_device_messages.append(message)
    remote.handle_response(ToDeviceResponse.from_dict({}, message))
    case = SimpleNamespace(
        generation=generation,
        bootstrap=bootstrap,
        client=client,
        session=session,
        remote=remote,
        remote_store=remote_store,
        encrypted=encrypted,
        claim=canonical_json(
            {"failures": {}, "one_time_keys": {ACCOUNT: {"OTHER": keys}}}
        ),
        path=path,
    )
    source = {"type": "m.room_key_request", "sender": ACCOUNT, "content": content}
    case.frame = _stage_share(case, [source], "initial")
    assert (
        session._materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )
    case.request = client.get_active_key_requests(ACCOUNT, "OTHER")[0]
    return case


def _stage_share(case, events, token):
    return stage_classic(
        case.bootstrap,
        {
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {
                "signed_curve25519": case.client.olm.account.max_one_time_keys,
            },
            "device_unused_fallback_key_types": [],
            "next_batch": token,
            "rooms": {},
            "to_device": {"events": events},
        },
    )


async def _reopen_share(case):
    if not case.session._closed:
        await case.session.close()
    case.bootstrap = reopen_owned_bootstrap(case.path, case.generation)
    case.client = owned_client(case.path)
    case.session = open_owned_session(case.client, case.bootstrap, case.generation)


async def _settle_share_callbacks(case):
    while (batch := case.session.next_batch(max_records=1)) is not None:
        await case.session._settle_batch(
            batch, receipt_new=True, semantic_event_new=False
        )


@pytest.mark.asyncio
async def test_interactive_claim_rechecks_changed_trust_and_retains_approval(tmp_path):
    case = await _prepared_share(tmp_path, missing_session=True)
    try:
        await _settle_share_callbacks(case)
        await case.session._advance_blocked_frame(case.frame.frame_id)
        device = case.client.device_store[ACCOUNT]["OTHER"]
        case.client.verify_device(device)
        assert case.client.continue_key_share(case.request)
        await _reopen_share(case)
        frame = _stage_share(case, [], "claim")
        assert (
            case.session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        device = case.client.device_store[ACCOUNT]["OTHER"]
        case.client.unverify_device(device)
        await _reopen_share(case)
        requests = []

        async def respond(request, **kwargs):
            assert request.path == "/_matrix/client/v3/keys/claim"
            requests.append(request)
            return case.session._network_result(request, 200, case.claim)

        case.session._request = respond
        assert await case.session._advance_blocked_frame(frame.frame_id)
        assert len(requests) == 1
        assert case.client.outgoing_to_device_messages == []
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == [case.request]
        assert case.session.next_batch() is None
        await case.session._advance_blocked_frame(frame.frame_id)
        await _reopen_share(case)
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == [case.request]
        assert not case.client.continue_key_share(case.request)
    finally:
        await case.session.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "cancel_kind", ["local", "incoming", "other-device", "other-user"]
)
async def test_cancelled_approval_is_not_resurrected_by_restart(tmp_path, cancel_kind):
    case = await _prepared_share(tmp_path)
    try:
        if cancel_kind == "local":
            assert case.client.cancel_key_share(case.request)
            assert not case.client.cancel_key_share(case.request)
            await _reopen_share(case)
            await _settle_share_callbacks(case)
            await case.session._advance_blocked_frame(case.frame.frame_id)
        else:
            await _settle_share_callbacks(case)
            await case.session._advance_blocked_frame(case.frame.frame_id)
            cancellation = {
                "type": "m.room_key_request",
                "sender": (
                    "@stranger:example.org" if cancel_kind == "other-user" else ACCOUNT
                ),
                "content": {
                    "action": "request_cancellation",
                    "request_id": case.request.request_id,
                    "requesting_device_id": (
                        "DIFFERENT" if cancel_kind == "other-device" else "OTHER"
                    ),
                },
            }
            frame = _stage_share(case, [cancellation], "cancel")
            assert (
                case.session._materialize_oldest_frame(
                    limits=MaterializerLimits()
                ).status
                is MaterializeStatus.MATERIALIZED
            )
            await _reopen_share(case)
            callbacks = []
            case.client.add_to_device_callback(
                callbacks.append, RoomKeyRequestCancellation
            )
            await _settle_share_callbacks(case)
            await case.session._advance_blocked_frame(frame.frame_id)
            assert len(callbacks) == (1 if cancel_kind == "incoming" else 0)
        await _reopen_share(case)
        expected = [] if cancel_kind in {"local", "incoming"} else [case.request]
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == expected
        assert case.client.outgoing_to_device_messages == []
        if not expected:
            case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
            with pytest.raises(LocalProtocolError, match="No such pending"):
                case.client.continue_key_share(case.request)
    finally:
        await case.session.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "field", ["payload", "payload_sha256", "request_id", "updated_revision"]
)
async def test_corrupt_approval_is_rejected_before_continuation_crypto(tmp_path, field):
    case = await _prepared_share(tmp_path)
    connection = case.session._owned_store.database.connection()
    try:
        case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
        before = _raw_e2ee_rows(connection)
        row = connection.execute("SELECT * FROM NioIngestKeyShare").fetchone()
        original = row[field]
        bad = {
            "payload": b"invalid",
            "payload_sha256": b"x" * 32,
            "request_id": "different-request",
            "updated_revision": case.session._journal.load_owner().revision + 1,
        }[field]
        with sqlite3.connect(tmp_path / "owned.db") as external:
            external.execute(f"UPDATE NioIngestKeyShare SET {field} = ?", (bad,))
        with pytest.raises(JournalIntegrityError):
            case.client.continue_key_share(case.request)
        assert _raw_e2ee_rows(connection) == before
        assert case.client.outgoing_to_device_messages == []
        with sqlite3.connect(tmp_path / "owned.db") as external:
            external.execute(f"UPDATE NioIngestKeyShare SET {field} = ?", (original,))
        case.client._assert_ingestion_not_poisoned()
        assert case.client.continue_key_share(case.request)
    finally:
        await case.session.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
@pytest.mark.skipif(not hasattr(os, "fork"), reason="requires process crash injection")
@pytest.mark.parametrize("commit", [False, True])
async def test_process_crash_keeps_approval_or_exact_crypto_handoff(tmp_path, commit):
    case = await _prepared_share(tmp_path)
    try:
        await _settle_share_callbacks(case)
        await case.session._advance_blocked_frame(case.frame.frame_id)
        case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
        before = _raw_e2ee_rows(case.session._owned_store.database.connection())
        await case.session.close()
        child = os.fork()
        if child == 0:
            try:
                bootstrap = reopen_owned_bootstrap(tmp_path, case.generation)
                client = owned_client(tmp_path)
                session = open_owned_session(client, bootstrap, case.generation)
                if not commit:

                    def crash(label):
                        if label == "key_share_upsert":
                            os._exit(71)

                    session._journal.set_transition_statement_hook(crash)
                assert client.continue_key_share(case.request)
                os._exit(72)
            except BaseException:
                os._exit(73)
        _, status = os.waitpid(child, 0)
        assert os.waitstatus_to_exitcode(status) == (72 if commit else 71)
        await _reopen_share(case)
        after = _raw_e2ee_rows(case.session._owned_store.database.connection())
        assert (after == before) is (not commit)
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == (
            [] if commit else [case.request]
        )
        assert len(case.client.outgoing_to_device_messages) == (1 if commit else 0)
        if commit:
            ciphertext = tuple(case.client.outgoing_to_device_messages)
            await _reopen_share(case)
            assert tuple(case.client.outgoing_to_device_messages) == ciphertext
    finally:
        await case.session.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["callback", "acknowledgment"])
async def test_callback_retry_reuses_committed_key_share_before_ack(tmp_path, failure):
    case = await _prepared_share(tmp_path)
    callbacks = []

    async def approve(event):
        assert not case.session._owned_store.database.connection().in_transaction
        case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
        assert case.client.continue_key_share(event)
        callbacks.append(event)
        if len(callbacks) == 1:
            if failure == "callback":
                raise RuntimeError("interrupted after approval")

            def interrupt_ack(label):
                if label == "delivery_work_delete":
                    raise RuntimeError("interrupted after approval")

            case.session._journal.set_transition_statement_hook(interrupt_ack)

    try:
        case.client.add_to_device_callback(approve, RoomKeyRequest)
        source = case.session.next_batch(max_records=1)
        await case.session._settle_batch(
            source, receipt_new=True, semantic_event_new=False
        )
        batch = case.session.next_batch(max_records=1)
        with pytest.raises(RuntimeError, match="interrupted after approval"):
            await case.session._settle_batch(
                batch, receipt_new=True, semantic_event_new=False
            )
        ciphertext = tuple(case.client.outgoing_to_device_messages)
        before = _raw_e2ee_rows(case.session._owned_store.database.connection())
        assert len(ciphertext) == 1
        await _reopen_share(case)
        case.client.add_to_device_callback(approve, RoomKeyRequest)
        assert case.session.next_batch(max_records=1) == batch
        await case.session._settle_batch(
            batch, receipt_new=True, semantic_event_new=False
        )
        assert _raw_e2ee_rows(case.session._owned_store.database.connection()) == before
        assert tuple(case.client.outgoing_to_device_messages) == ciphertext
        assert callbacks == [case.request, case.request]
        await case.session._advance_blocked_frame(case.frame.frame_id)
        await _reopen_share(case)
        assert case.session.next_batch() is None
        assert tuple(case.client.outgoing_to_device_messages) == ciphertext
    finally:
        await case.session.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_session", [False, True])
async def test_failed_handoff_transfer_retains_approval_effects(
    tmp_path, missing_session
):
    class Interrupted(BaseException):
        pass

    case = await _prepared_share(tmp_path, missing_session=missing_session)
    try:
        await _settle_share_callbacks(case)
        await case.session._advance_blocked_frame(case.frame.frame_id)
        case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
        assert case.client.continue_key_share(case.request)
        frame = _stage_share(case, [], "handoff")
        connection = case.session._owned_store.database.connection()
        before = tuple(connection.iterdump())

        def interrupt(label):
            if label == ("key_share_upsert" if missing_session else "key_share_delete"):
                raise Interrupted

        case.session._journal.set_transition_statement_hook(interrupt)
        with pytest.raises(Interrupted):
            case.session._materialize_oldest_frame(limits=MaterializerLimits())
        assert tuple(connection.iterdump()) == before
        with pytest.raises(LocalProtocolError, match="poisoned"):
            case.client.continue_key_share(case.request)
        await _reopen_share(case)
        assert (
            case.session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        pending = case.session._journal._load_ready_outbound_maintenance()
        assert pending is not None
        assert pending.frame_id == frame.frame_id
    finally:
        await case.session.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
async def test_closed_owned_approval_handler_cannot_mutate_store(tmp_path):
    case = await _prepared_share(tmp_path)
    try:
        handler = case.client._ingestion_key_share_handler
        await case.session.close()
        with pytest.raises(LocalProtocolError, match="closed"):
            handler(case.request, False)
        with pytest.raises(LocalProtocolError, match="not loaded"):
            case.client.continue_key_share(case.request)
        await _reopen_share(case)
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == [case.request]
        assert case.client.outgoing_to_device_messages == []
    finally:
        await case.session.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
async def test_older_claim_preserves_newer_continuation_handoff(tmp_path):
    case = await _prepared_share(tmp_path, missing_session=True)
    try:
        await _settle_share_callbacks(case)
        await case.session._advance_blocked_frame(case.frame.frame_id)
        source = json.loads(canonical_json(case.request.source))
        source["content"]["request_id"] = "newer-request"
        frame = _stage_share(case, [source], "second-approval")
        case.session._materialize_oldest_frame(limits=MaterializerLimits())
        await _settle_share_callbacks(case)
        await case.session._advance_blocked_frame(frame.frame_id)
        pending = case.client.get_active_key_requests(ACCOUNT, "OTHER")
        newer = next(event for event in pending if event.request_id == "newer-request")
        case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
        assert case.client.continue_key_share(case.request)
        frame = _stage_share(case, [], "older-claim")
        case.session._materialize_oldest_frame(limits=MaterializerLimits())
        assert case.client.continue_key_share(newer)
        case.client.unverify_device(case.client.device_store[ACCOUNT]["OTHER"])

        async def respond(request, **kwargs):
            assert request.path == "/_matrix/client/v3/keys/claim"
            return case.session._network_result(request, 200, case.claim)

        case.session._request = respond
        assert await case.session._advance_blocked_frame(frame.frame_id)
        assert case.client.outgoing_to_device_messages == []
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == [case.request]
        await case.session._advance_blocked_frame(frame.frame_id)
        await _reopen_share(case)
        case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
        frame = _stage_share(case, [], "newer-handoff")
        assert (
            case.session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == [case.request]
        assert len(case.client.outgoing_to_device_messages) == 1
        pending_send = case.session._journal._load_ready_outbound_maintenance()
        assert pending_send is not None
        assert pending_send.frame_id == frame.frame_id
        assert pending_send.operation.event_type == "m.room.encrypted"
    finally:
        await case.session.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
async def test_owned_legacy_queue_drain_refuses_before_sending_retained_share(
    tmp_path, aioresponse
):
    def real_http_client():
        client = AsyncClient("https://example.org", ACCOUNT, DEVICE)
        client.restore_login(ACCOUNT, DEVICE, "access-token")
        return client

    case = await _prepared_share(tmp_path, client_type=real_http_client)
    try:
        case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
        assert case.client.continue_key_share(case.request)
        queued = tuple(case.client.outgoing_to_device_messages)
        before = tuple(case.session._owned_store.database.connection().iterdump())
        aioresponse.put(
            re.compile(
                r"https://example.org/_matrix/client/v3/sendToDevice/m.room.encrypted/.*"
            ),
            payload={},
        )
        with pytest.raises(LocalProtocolError, match="owned ingestion"):
            await case.client.send_to_device_messages()
        assert not aioresponse.requests
        assert tuple(case.client.outgoing_to_device_messages) == queued
        assert (
            tuple(case.session._owned_store.database.connection().iterdump()) == before
        )
    finally:
        await case.session.close()
        await case.client.close()
        case.remote_store.database.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("missing_session", [False, True])
async def test_continued_share_survives_handoff_and_lost_send_response(
    tmp_path, missing_session
):
    class Interrupted(BaseException):
        pass

    case = await _prepared_share(tmp_path, missing_session=missing_session)
    try:
        await _settle_share_callbacks(case)
        await case.session._advance_blocked_frame(case.frame.frame_id)
        await _reopen_share(case)
        case.client.verify_device(case.client.device_store[ACCOUNT]["OTHER"])
        assert case.client.continue_key_share(case.request)
        queued = tuple(case.client.outgoing_to_device_messages)
        assert case.client.continue_key_share(case.request)
        assert tuple(case.client.outgoing_to_device_messages) == queued
        await _reopen_share(case)
        assert tuple(case.client.outgoing_to_device_messages) == queued
        frame = _stage_share(case, [], "handoff")
        assert (
            case.session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        await _reopen_share(case)
        sent = {}
        interruptions = []
        decrypted = []

        async def respond(request, **kwargs):
            if request.path == "/_matrix/client/v3/keys/claim":
                return case.session._network_result(request, 200, case.claim)
            assert "/sendToDevice/m.room.encrypted/" in request.path
            if request.path in sent:
                assert request.body == sent[request.path]
            else:
                sent[request.path] = request.body
                content = json.loads(request.body)["messages"][ACCOUNT]["OTHER"]
                event = ToDeviceEvent.parse_event(
                    {"type": "m.room.encrypted", "sender": ACCOUNT, "content": content}
                )
                key = case.remote.decrypt_event(event)
                assert type(key) is ForwardedRoomKeyEvent
                encrypted = Event.parse_event(
                    {
                        "type": "m.room.encrypted",
                        "sender": ACCOUNT,
                        "event_id": "$recovered-secret",
                        "origin_server_ts": 1,
                        "content": case.encrypted,
                    }
                )
                encrypted.room_id = ROOM
                decrypted.append(case.remote.decrypt_event(encrypted).body)
                interruptions.append(request.path)
                raise Interrupted
            return case.session._network_result(request, 200, b"{}")

        case.session._request = respond
        if missing_session:
            assert await case.session._advance_blocked_frame(frame.frame_id)
            await _reopen_share(case)
            case.session._request = respond
        with pytest.raises(Interrupted):
            await case.session._advance_blocked_frame(frame.frame_id)
        await _reopen_share(case)
        case.session._request = respond
        assert await case.session._advance_blocked_frame(frame.frame_id)
        assert await case.session._advance_blocked_frame(frame.frame_id)
        assert case.session._journal.list_frames(1) == ()
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == []
        assert case.client.outgoing_to_device_messages == []
        assert decrypted == ["recoverable secret"]
        assert len(sent) == len(interruptions) == 1
        await _reopen_share(case)
        assert case.client.get_active_key_requests(ACCOUNT, "OTHER") == []
        assert case.client.outgoing_to_device_messages == []
    finally:
        await case.session.close()
        case.remote_store.database.close()
