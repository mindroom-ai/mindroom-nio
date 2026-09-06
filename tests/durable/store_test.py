"""Crash boundaries and ownership of the compact durable store."""

from uuid import UUID

import pytest
from vodozemac import LibolmPickleException

from nio.crypto import DeviceStore, OlmAccount, OlmDevice, TrustState
from nio.durable.model import RecordKind, SyncRecord
from nio.durable.store import DurableStore
from nio.exceptions import LocalProtocolError
from nio.store import DefaultStore, SqliteStore

USER = "@alice:example.org"
DEVICE = "ALICE"
CONSUMER = UUID("4f840c4b-c85c-4fb1-b153-0dcefe4361eb")


def open_store(path, **kwargs):
    return DurableStore(
        path,
        user_id=USER,
        device_id=DEVICE,
        consumer_id=kwargs.pop("consumer_id", CONSUMER),
        **kwargs,
    )


def message(event_id="$one"):
    return SyncRecord(
        RecordKind.TIMELINE,
        "!room:example.org",
        {"event_id": event_id, "type": "m.room.message", "content": {"body": "hello"}},
    )


def test_retained_input_and_batch_survive_reopen_without_delivery_writes(tmp_path):
    store = open_store(tmp_path)
    stream_id = store.stream_id
    with store.transaction():
        store.capture(b'{"next_batch":"s1"}')
        batch = store.publish((message(),), completes_sync=True)
        store.set_cursor("s1")
    store.close()

    reopened = open_store(tmp_path)
    try:
        assert reopened.stream_id == stream_id
        assert reopened.cursor == "s1"
        assert reopened.input == (b'{"next_batch":"s1"}', {})
        connection = reopened.database.connection()
        changes = connection.total_changes
        assert reopened.next_batch() == batch
        assert reopened.next_batch() == batch
        assert connection.total_changes == changes
        assert batch.sequence == 1
        assert batch.records[0].source["event_id"] == "$one"
        assert batch.completes_sync
    finally:
        reopened.close()


def test_crypto_and_output_rollback_together(tmp_path):
    store = open_store(tmp_path)
    account = OlmAccount()
    with store.transaction():
        store.matrix.save_account(account)
    identity = account.identity_keys
    with pytest.raises(RuntimeError, match="transaction interrupted"):
        with store.transaction():
            store.matrix.save_account(OlmAccount())
            store.publish((message(),))
            store.set_cursor("lost")
            raise RuntimeError("transaction interrupted")
    store.close()

    reopened = open_store(tmp_path)
    try:
        assert reopened.matrix.load_account().identity_keys == identity
        assert reopened.next_batch() is None
        assert reopened.cursor is None
    finally:
        reopened.close()


def test_ack_is_ordered_and_idempotent(tmp_path):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            first = store.publish((message(),))
            second = store.publish((message("$two"),))
        with pytest.raises(LocalProtocolError, match="oldest"):
            store.ack(second)
        assert store.next_batch() == first
        store.ack(first)
        store.ack(first)
        assert store.next_batch() == second
        store.ack(second)
        assert store.next_batch() is None
    finally:
        store.close()


def test_capture_cannot_overwrite_unfinished_response(tmp_path):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            store.capture(b'{"next_batch":"s1"}')
        with pytest.raises(LocalProtocolError, match="unfinished"):
            with store.transaction():
                store.capture(b'{"next_batch":"s2"}')
        assert store.input == (b'{"next_batch":"s1"}', {})
    finally:
        store.close()


def test_consumer_binding_and_store_lease(tmp_path):
    store = open_store(tmp_path)
    try:
        with pytest.raises(LocalProtocolError, match="lease"):
            open_store(tmp_path)
        with pytest.raises(LocalProtocolError, match="lease"):
            SqliteStore(USER, DEVICE, str(tmp_path))
    finally:
        store.close()
    with pytest.raises(LocalProtocolError, match="consumer"):
        open_store(tmp_path, consumer_id=UUID(int=7))
    with pytest.raises(LocalProtocolError, match="durable"):
        SqliteStore(USER, DEVICE, str(tmp_path))
    reopened = open_store(tmp_path)
    reopened.close()


def test_adoption_preserves_effective_default_store_trust(tmp_path):
    ordinary = DefaultStore(USER, DEVICE, str(tmp_path))
    account = OlmAccount()
    ordinary.save_account(account)
    devices = DeviceStore()
    for name in ("verified", "blacklisted", "ignored", "unset"):
        device = OlmDevice("@bob:example.org", name, OlmAccount().identity_keys)
        devices.add(device)
    ordinary.save_device_keys(devices)
    ordinary.verify_device(devices["@bob:example.org"]["verified"])
    ordinary.blacklist_device(devices["@bob:example.org"]["blacklisted"])
    ordinary.ignore_device(devices["@bob:example.org"]["ignored"])
    ordinary.database.close()

    store = open_store(tmp_path, source_store_class=DefaultStore)
    try:
        assert store.matrix.load_account().identity_keys == account.identity_keys
        loaded = store.matrix.load_device_keys()["@bob:example.org"]
        assert {name: device.trust_state for name, device in loaded.items()} == {
            "verified": TrustState.verified,
            "blacklisted": TrustState.blacklisted,
            "ignored": TrustState.ignored,
            "unset": TrustState.unset,
        }
    finally:
        store.close()


def test_wrong_pickle_key_does_not_adopt_existing_account(tmp_path):
    ordinary = SqliteStore(USER, DEVICE, str(tmp_path), pickle_key="correct")
    account = OlmAccount()
    ordinary.save_account(account)
    ordinary.database.close()
    with pytest.raises(LibolmPickleException):
        open_store(tmp_path, pickle_key="wrong")
    ordinary = SqliteStore(USER, DEVICE, str(tmp_path), pickle_key="correct")
    try:
        assert ordinary.load_account().identity_keys == account.identity_keys
        assert "NioDurableMeta" not in ordinary.database.get_tables()
    finally:
        ordinary.database.close()


def test_account_binding_survives_a_store_without_crypto_keys(tmp_path):
    store = open_store(tmp_path)
    store.close()
    with pytest.raises(LocalProtocolError, match="account/device"):
        DurableStore(
            tmp_path,
            user_id="@different:example.org",
            device_id=DEVICE,
            consumer_id=CONSUMER,
            database_name=f"{USER}_{DEVICE}.db",
        )


def test_closed_matrix_store_cannot_reopen_without_ownership(tmp_path):
    store = open_store(tmp_path)
    store.close()
    with pytest.raises(LocalProtocolError, match="closed"):
        store.matrix.save_account(OlmAccount())


@pytest.mark.parametrize("store_class", [SqliteStore, DefaultStore])
def test_preexisting_ordinary_handle_cannot_write_after_adoption(tmp_path, store_class):
    ordinary = store_class(USER, DEVICE, str(tmp_path))
    account = OlmAccount()
    ordinary.save_account(account)
    ordinary.database.close()
    store = open_store(tmp_path, source_store_class=store_class)
    store.close()
    with pytest.raises(LocalProtocolError, match="durable"):
        ordinary.save_account(OlmAccount())
    reopened = open_store(tmp_path)
    try:
        assert reopened.matrix.load_account().identity_keys == account.identity_keys
    finally:
        reopened.close()


def test_preexisting_default_handle_cannot_change_trust_after_adoption(tmp_path):
    ordinary = DefaultStore(USER, DEVICE, str(tmp_path))
    ordinary.save_account(OlmAccount())
    device = OlmDevice("@bob:example.org", "BOB", OlmAccount().identity_keys)
    devices = DeviceStore()
    devices.add(device)
    ordinary.save_device_keys(devices)
    ordinary.database.close()
    store = open_store(tmp_path, source_store_class=DefaultStore)
    store.close()
    with pytest.raises(LocalProtocolError, match="adoption"):
        ordinary.verify_device(device)
