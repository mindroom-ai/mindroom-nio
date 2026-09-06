"""Durable crypto retries retain private material and exact encrypted output."""

import json
from uuid import UUID

import pytest
import vodozemac
from unpaddedbase64 import decode_base64

from nio import AsyncClient
from nio.crypto import Olm, OlmAccount, OlmDevice
from nio.event_builders import RoomKeyRequestMessage
from nio.events import MegolmEvent, RoomKeyRequest, RoomKeyRequestCancellation
from nio.durable.crypto import CryptoMaintenance
from nio.durable.store import DurableStore
from nio.exceptions import LocalProtocolError
from nio.responses import ErrorResponse, KeysUploadResponse

USER = "@alice:example.org"
CONSUMER = UUID("6a45b5a6-906d-4aeb-8aa1-494b3ef9b5fd")


def open_crypto(path):
    store = DurableStore(path, user_id=USER, device_id="ALICE", consumer_id=CONSUMER)
    client = AsyncClient("https://example.org", USER, "ALICE")
    client.restore_login(USER, "ALICE", "test-token")
    client.store = store.matrix
    client.olm = Olm(USER, "ALICE", store.matrix)
    maintenance = CryptoMaintenance(client, store)
    maintenance.restore()
    return store, client.olm, maintenance


def publish_account(olm):
    olm.account.shared = True
    olm.save_account()


def queue_dummy(olm):
    peer = OlmAccount()
    peer.generate_one_time_keys(1)
    device = OlmDevice(USER, "OTHER", peer.identity_keys)
    olm.device_store.add(device)
    olm.store.save_device_keys({USER: {device.id: device}})
    session = olm.create_session(
        next(iter(peer.one_time_keys["curve25519"].values())), device.curve25519
    )
    olm._queue_dummy_message(session, device)
    return peer, device


def test_upload_retains_private_keys_and_publication_across_restart(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        request = maintenance.next_request()
    assert request.kind == "upload"
    assert "test-token" not in request.path
    public_keys = {
        key.split(":", 1)[1]: value["key"]
        for key, value in json.loads(request.body)["one_time_keys"].items()
    }
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        assert olm.account.one_time_keys["curve25519"] == public_keys
        peer = OlmAccount()
        outbound = peer.create_outbound_session(
            olm.account.identity_keys["curve25519"], next(iter(public_keys.values()))
        )
        _, clear = olm.account.create_inbound_session(
            peer.identity_keys["curve25519"], outbound.encrypt(b"private key survived")
        )
        assert clear == b"private key survived"
        with store.transaction():
            assert maintenance.next_request() == request
            response, observations = maintenance.apply(
                request,
                {"one_time_key_counts": {"signed_curve25519": len(public_keys)}},
            )
        assert isinstance(response, KeysUploadResponse)
        assert observations == []
    finally:
        store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        assert olm.account.shared
        assert olm.account.one_time_keys["curve25519"] == {}
        with store.transaction():
            assert maintenance.next_request() is None
    finally:
        store.close()


def test_encrypted_output_and_transaction_id_survive_capture_and_restart(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        publish_account(olm)
        queue_dummy(olm)
        maintenance.capture()
        request = maintenance.next_request()
        maintenance.capture()
        assert maintenance.next_request() == request
    assert request.kind == "to_device"
    assert request.request_id in request.path
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        with store.transaction():
            assert maintenance.next_request() == request
            maintenance.apply(request, {})
            assert maintenance.next_request() is None
        assert not olm.outgoing_to_device_messages
    finally:
        store.close()


def test_query_response_preserves_newer_invalidation_after_restart(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        publish_account(olm)
        olm.users_for_key_query.add(USER)
        request = maintenance.next_request()
        olm.add_changed_users({USER})
        maintenance.capture()
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        with store.transaction():
            maintenance.apply(request, {"device_keys": {USER: {}}})
            following = maintenance.next_request()
        assert following.kind == "query"
        assert following.request_id != request.request_id
        assert json.loads(following.body)["device_keys"] == {USER: []}
    finally:
        store.close()


@pytest.mark.parametrize("body", [{}, {"errcode": "M_FORBIDDEN", "error": "denied"}])
def test_failed_upload_response_retains_pending_request(tmp_path, body):
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        with store.transaction():
            request = maintenance.next_request()
            response, observations = maintenance.apply(request, body)
            assert isinstance(response, ErrorResponse)
            assert observations == []
            assert maintenance.next_request() == request
        assert not olm.account.shared
    finally:
        store.close()


def test_rollback_requires_discarding_live_crypto_and_restores_pending(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        request = maintenance.next_request()
    with pytest.raises(RuntimeError, match="rollback"):
        with store.transaction():
            maintenance.apply(
                request, {"one_time_key_counts": {"signed_curve25519": 100}}
            )
            raise RuntimeError("rollback")
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        assert not olm.account.shared
        with store.transaction():
            assert maintenance.next_request() == request
    finally:
        store.close()


def test_crypto_mutation_requires_outer_transaction(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        with pytest.raises(LocalProtocolError, match="transaction"):
            maintenance.next_request()
    finally:
        store.close()


def test_idle_crypto_poll_and_pending_retry_do_not_write_sqlite(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        with store.transaction():
            publish_account(olm)
            maintenance.capture()
        connection = store.database.connection()
        before = connection.total_changes
        with store.transaction():
            assert maintenance.next_request() is None
        assert connection.total_changes == before
        with store.transaction():
            olm.users_for_key_query.add(USER)
            pending = maintenance.next_request()
        before = connection.total_changes
        with store.transaction():
            assert maintenance.next_request() == pending
        assert connection.total_changes == before
    finally:
        store.close()


def missing_event(olm, device):
    return MegolmEvent.from_dict(
        {
            "type": "m.room.encrypted",
            "sender": USER,
            "event_id": "$missing",
            "origin_server_ts": 1,
            "room_id": "!room:example.org",
            "content": {
                "algorithm": "m.megolm.v1.aes-sha2",
                "session_id": "missing-session",
                "sender_key": device.curve25519,
                "device_id": device.id,
                "ciphertext": "missing-ciphertext",
            },
        }
    )


def test_dummy_ack_retains_rerequest_and_its_subtype_effect_after_restart(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        publish_account(olm)
        peer, device = queue_dummy(olm)
        event = missing_event(olm, device)
        olm.key_re_requests_events[(USER, device.id)].append(event)
        maintenance.capture()
        dummy = maintenance.next_request()
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        maintenance.apply(dummy, {})
        following = maintenance.next_request()
    assert (
        json.loads(following.body)["messages"][USER]["OTHER"]["body"]["session_id"]
        == "missing-session"
    )
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        assert isinstance(olm.outgoing_to_device_messages[0], RoomKeyRequestMessage)
        with store.transaction():
            assert maintenance.next_request() == following
            maintenance.apply(following, {})
        assert (
            olm.outgoing_key_requests["missing-session"].room_id == "!room:example.org"
        )
    finally:
        store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        assert "missing-session" in olm.outgoing_key_requests
        assert not olm.outgoing_to_device_messages
        assert not olm.key_re_requests_events
    finally:
        store.close()


def room_key_request(olm):
    room = "!room:example.org"
    olm.create_outbound_group_session(room)
    group = olm.outbound_group_sessions[room]
    request = RoomKeyRequest.from_dict(
        {
            "type": "m.room_key_request",
            "sender": USER,
            "content": {
                "action": "request",
                "request_id": "share-request",
                "requesting_device_id": "OTHER",
                "body": {
                    "algorithm": "m.megolm.v1.aes-sha2",
                    "room_id": room,
                    "session_id": group.id,
                    "sender_key": olm.account.identity_keys["curve25519"],
                },
            },
        }
    )
    return request, group


@pytest.mark.parametrize("cancel", [False, True])
def test_interactive_share_survives_restart_and_encrypts_for_verified_device(
    tmp_path, cancel
):
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        publish_account(olm)
        peer, device = queue_dummy(olm)
        olm.outgoing_to_device_messages.clear()
        request, group = room_key_request(olm)
        group.shared = True
        ciphertext = group.encrypt("retained room secret")
        olm.handle_to_device_event(request)
        assert olm.collect_key_requests() == [request]
        maintenance.capture()
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        assert olm.get_active_key_requests(USER, "OTHER") == [request]
        assert not maintenance.change_key_share(request)  # Still untrusted.
        olm.verify_device(olm.device_store[USER]["OTHER"])
        assert maintenance.change_key_share(request, cancel=cancel)
        outgoing = maintenance.next_request()
    if cancel:
        assert outgoing is None
    else:
        content = json.loads(outgoing.body)["messages"][USER]["OTHER"]
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
            inbound.decrypt(vodozemac.MegolmMessage.from_base64(ciphertext)).plaintext
            == b"retained room secret"
        )
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        assert olm.get_active_key_requests(USER, "OTHER") == []
        with store.transaction():
            assert maintenance.next_request() == outgoing
    finally:
        store.close()


@pytest.mark.parametrize("trusted", [True, False])
def test_waiting_share_and_signed_claim_survive_restart(tmp_path, trusted):
    store, olm, maintenance = open_crypto(tmp_path)
    peer = OlmAccount()
    peer.generate_one_time_keys(1)
    key_id, public_key = next(iter(peer.one_time_keys["curve25519"].items()))
    signed_key = {"key": public_key}
    from nio.api import Api

    signed_key["signatures"] = {
        USER: {"ed25519:OTHER": peer.sign(Api.to_canonical_json(signed_key))}
    }
    with store.transaction():
        publish_account(olm)
        device = OlmDevice(USER, "OTHER", peer.identity_keys)
        olm.device_store.add(device)
        olm.store.save_device_keys({USER: {device.id: device}})
        olm.verify_device(device)
        request, group = room_key_request(olm)
        olm.handle_to_device_event(request)
        assert olm.collect_key_requests() == []
        maintenance.capture()
        claim = maintenance.next_request()
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        assert olm.key_requests_waiting_for_session[(USER, "OTHER")] == {
            request.request_id: request
        }
        with store.transaction():
            assert maintenance.next_request() == claim
            if not trusted:
                olm.unverify_device(olm.device_store[USER]["OTHER"])
            response, observations = maintenance.apply(
                claim,
                {
                    "one_time_keys": {
                        USER: {"OTHER": {f"signed_curve25519:{key_id}": signed_key}}
                    }
                },
            )
            assert observations == ([] if trusted else [request])
            assert olm.received_key_requests == {}
            outgoing = maintenance.next_request()
        if trusted:
            assert outgoing.kind == "to_device"
        else:
            assert outgoing is None
        assert not olm.key_request_devices_no_session
        assert not olm.key_requests_waiting_for_session
    finally:
        store.close()


@pytest.mark.parametrize(
    "field,value",
    [
        ("uploaded_key_count", True),
        ("wedged", [[USER, "missing"]]),
        ("key_request_from_untrusted", [{}]),
        ("waiting", [[[USER], []]]),
    ],
)
def test_malformed_stored_authorization_facts_fail_on_restore(tmp_path, field, value):
    store, olm, maintenance = open_crypto(tmp_path)
    with store.transaction():
        maintenance.capture()
        encoded = store.database.execute_sql(
            "SELECT body FROM NioDurableCrypto WHERE kind='facts'"
        ).fetchone()[0]
        facts = json.loads(encoded)
        facts[field] = value
        store.database.execute_sql(
            "UPDATE NioDurableCrypto SET body=? WHERE kind='facts'",
            (json.dumps(facts),),
        )
    try:
        with pytest.raises(LocalProtocolError, match="crypto facts"):
            maintenance.restore()
    finally:
        store.close()


def test_prebuilt_group_body_and_explicit_message_keep_exact_ids(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    content = {"messages": {USER: {"OTHER": {"ciphertext": "already encrypted"}}}}
    with store.transaction():
        publish_account(olm)
        request = maintenance.enqueue_to_device(
            "m.room.encrypted", content, request_id="group-tx"
        )
        content["messages"].clear()
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        with store.transaction():
            assert maintenance.next_request() == request
            assert (
                json.loads(request.body)["messages"][USER]["OTHER"]["ciphertext"]
                == "already encrypted"
            )
            maintenance.apply(request, {})
            peer, device = queue_dummy(olm)
            message = olm.outgoing_to_device_messages[0]
            explicit = maintenance.enqueue_message(message, request_id="explicit-tx")
            maintenance.capture()
            assert (
                maintenance.enqueue_message(message, request_id="ignored-new-tx")
                == explicit
            )
            assert explicit.request_id == "explicit-tx"
    finally:
        store.close()


def test_pending_request_with_missing_message_fails_during_restore(tmp_path):
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        with store.transaction():
            queue_dummy(olm)
            maintenance.next_request()
            encoded = store.database.execute_sql(
                "SELECT body FROM NioDurableCrypto WHERE kind='request'"
            ).fetchone()[0]
            pending = json.loads(encoded)
            pending["message_id"] = "missing-message"
            store.database.execute_sql(
                "UPDATE NioDurableCrypto SET body=? WHERE kind='request'",
                (json.dumps(pending),),
            )
        with pytest.raises(LocalProtocolError, match="crypto"):
            maintenance.restore()
    finally:
        store.close()


@pytest.mark.parametrize("previous_empty_queue", [False, True])
def test_unknown_device_cancellation_does_not_block_restart(
    tmp_path, previous_empty_queue
):
    store, olm, maintenance = open_crypto(tmp_path)
    cancellation = RoomKeyRequestCancellation.from_dict(
        {
            "type": "m.room_key_request",
            "sender": USER,
            "content": {
                "action": "request_cancellation",
                "request_id": "missed-request",
                "requesting_device_id": "UNKNOWN",
            },
        }
    )
    with store.transaction():
        publish_account(olm)
        olm.handle_to_device_event(cancellation)
        assert olm.collect_key_requests() == []
        assert olm.key_requests_waiting_for_session[(USER, "UNKNOWN")] == {}
        maintenance.capture()
        if previous_empty_queue:
            # The previous writer retained this normal empty defaultdict entry.
            encoded = store.database.execute_sql(
                "SELECT body FROM NioDurableCrypto WHERE kind='facts'"
            ).fetchone()[0]
            facts = json.loads(encoded)
            facts["waiting"] = [[[USER, "UNKNOWN"], []]]
            store.database.execute_sql(
                "UPDATE NioDurableCrypto SET body=? WHERE kind='facts'",
                (json.dumps(facts),),
            )
    store.close()
    store, olm, maintenance = open_crypto(tmp_path)
    try:
        assert not olm.key_requests_waiting_for_session
        assert not olm.key_request_devices_no_session
        with store.transaction():
            assert maintenance.next_request() is None
    finally:
        store.close()
