"""Crash boundaries and ownership of the compact durable store."""

import asyncio
import errno
import os
import shutil
import threading
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


@pytest.mark.parametrize("sqlite_version", [(3, 42, 0), (3, 43, 0)])
def test_pending_bytes_counts_utf8_not_characters(
    tmp_path, monkeypatch, sqlite_version
):
    store = open_store(tmp_path)
    try:
        if store.database.server_version < (3, 43, 0) and sqlite_version >= (3, 43, 0):
            pytest.skip("SQLite lacks octet_length")
        monkeypatch.setattr(store.database, "server_version", sqlite_version)
        assert store.pending_bytes == 0
        with store.transaction():
            # Nine Unicode characters occupy thirteen UTF-8 bytes.
            store.database.execute_sql(
                "INSERT INTO NioDurableBatch(records,completes_sync) VALUES(?,0)",
                ('["é","😀"]',),
            )
            store.publish((), completes_sync=True)
            assert store.pending_bytes == 15
    finally:
        store.close()


def test_pending_bytes_tracks_rollback_ack_and_reopen(tmp_path):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            first = store.publish(())
        with pytest.raises(RuntimeError, match="interrupted"):
            with store.transaction():
                store.publish(())
                assert store.pending_bytes == 4
                raise RuntimeError("interrupted")
        assert store.pending_bytes == 2
        store.ack(first)
        assert store.pending_bytes == 0
        with store.transaction():
            store.publish(())
    finally:
        store.close()
    reopened = open_store(tmp_path)
    try:
        assert reopened.pending_bytes == 2
    finally:
        reopened.close()


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


def test_durable_writer_uses_full_synchronous_on_open_and_reopen(tmp_path):
    for _ in range(2):
        store = open_store(tmp_path)
        try:
            assert store.database.execute_sql("PRAGMA synchronous").fetchone()[0] == 2
            assert (
                store.database.execute_sql("PRAGMA journal_mode").fetchone()[0] == "wal"
            )
        finally:
            store.close()


def synchronous(store):
    return store.database.execute_sql("PRAGMA synchronous").fetchone()[0]


@pytest.mark.asyncio
async def test_sync_later_commits_share_one_off_loop_wal_fsync(tmp_path, monkeypatch):
    store = open_store(tmp_path)
    try:
        with store.transaction(sync_later=True):
            assert synchronous(store) == 1
            store.publish((message(),))
        with store.transaction(sync_later=True):
            store.publish(())
        with store.transaction():
            assert synchronous(store) == 2
            store.publish(())
        synced = []
        fsync = os.fsync
        monkeypatch.setattr(
            os, "fsync", lambda fd: synced.append(os.fstat(fd).st_ino) or fsync(fd)
        )
        await store.make_durable()
        await store.make_durable()
        # One fsync covers both commits; the first also syncs the new WAL's
        # directory entry.
        wal = os.stat(f"{store.path}-wal").st_ino
        assert synced == [os.stat(tmp_path).st_ino, wal]
    finally:
        store.close()


@pytest.mark.asyncio
async def test_make_durable_returns_only_when_nothing_is_left_to_sync(
    tmp_path, monkeypatch
):
    store = open_store(tmp_path)
    try:
        with store.transaction(sync_later=True):
            store.publish((message(),))
        started, release, calls = threading.Event(), threading.Event(), []
        fsync = os.fsync

        def held(fd):
            calls.append(fd)
            if len(calls) == 2:
                started.set()
                assert release.wait(5)
            fsync(fd)

        monkeypatch.setattr(os, "fsync", held)
        first = asyncio.create_task(store.make_durable())
        assert await asyncio.to_thread(started.wait, 5)
        # A commit lands while the WAL fsync is in flight.
        with store.transaction(sync_later=True):
            store.publish((message("$two"),))
        second = asyncio.create_task(store.make_durable())
        release.set()
        await first
        assert store._synced == store._commits
        await second
        # The directory, the WAL, then the WAL again for the second commit.
        assert len(calls) == 3
    finally:
        store.close()


@pytest.mark.asyncio
async def test_a_failed_wal_fsync_fails_every_later_sync(tmp_path, monkeypatch):
    store = open_store(tmp_path)
    try:
        with store.transaction(sync_later=True):
            store.publish(())

        def fail(fd):
            raise OSError(errno.EIO, "lost write")

        monkeypatch.setattr(os, "fsync", fail)
        with pytest.raises(OSError, match="lost write"):
            await store.make_durable()
        monkeypatch.undo()
        # The kernel may not report the lost write again.
        with pytest.raises(LocalProtocolError, match="could not sync"):
            await store.make_durable()
    finally:
        store.close()


@pytest.mark.asyncio
async def test_the_wal_of_a_symlinked_database_is_synced(tmp_path):
    (tmp_path / "data").mkdir()
    (tmp_path / "store").mkdir()
    (tmp_path / "store" / "link.db").symlink_to(tmp_path / "data" / "real.db")
    store = open_store(tmp_path / "store", database_name="link.db")
    try:
        with store.transaction(sync_later=True):
            store.publish(())
        await store.make_durable()
        assert (tmp_path / "data" / "real.db-wal").exists()
    finally:
        store.close()


def durable_ack(store):
    """Return the committed acknowledgement and batch count, as a crash leaves them."""
    return store.database.execute_sql(
        "SELECT acked_sequence,(SELECT COUNT(*) FROM NioDurableBatch) "
        "FROM NioDurableMeta"
    ).fetchone()


def trace(store):
    statements = []
    store.database.connection().set_trace_callback(statements.append)
    return statements


@pytest.mark.parametrize("writer", ["transaction", "close"])
def test_only_empty_batch_acknowledgement_joins_the_next_commit(tmp_path, writer):
    store = open_store(tmp_path / "store")
    try:
        with store.transaction():
            batch = store.publish((), completes_sync=True)
        statements = trace(store)
        store.ack(batch)
        assert "COMMIT" not in statements
        assert durable_ack(store) == (batch.sequence - 1, 1)
        assert (store.has_batches(), store.next_batch()) == (False, None)
        assert (store.acked_sequence, store.pending_bytes) == (batch.sequence, 0)
        # A process killed here leaves the batch for redelivery.
        shutil.copytree(tmp_path / "store", tmp_path / "killed")
        if writer == "transaction":
            with store.transaction():
                later = store.publish((message(),))
            assert statements.count("COMMIT") == 1
            assert store.next_batch() == later
            assert durable_ack(store) == (batch.sequence, 1)
    finally:
        store.close()
    reopened = open_store(tmp_path / "store")
    try:
        assert reopened.acked_sequence == batch.sequence
        assert reopened.has_batches() == (writer == "transaction")
        reopened.ack(batch)
    finally:
        reopened.close()
    killed = open_store(tmp_path / "killed")
    try:
        assert killed.acked_sequence == batch.sequence - 1
        assert killed.next_batch() == batch
        killed.ack(batch)
        assert killed.next_batch() is None
    finally:
        killed.close()


def test_acknowledgement_that_could_repeat_or_reorder_a_batch_commits_at_once(
    tmp_path,
):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            empty = store.publish((), completes_sync=True)
            full = store.publish((message(),))
        statements = trace(store)
        # Another batch is queued: its delivery must not read the empty one.
        store.ack(empty)
        assert statements.count("COMMIT") == 1
        assert durable_ack(store) == (empty.sequence, 1)
        assert store.next_batch() == full
        # Redelivery of a batch with records would repeat them.
        store.ack(full)
        assert statements.count("COMMIT") == 2
        assert durable_ack(store) == (full.sequence, 0)
    finally:
        store.close()


def test_failed_transaction_keeps_a_deferred_acknowledgement_for_the_next(tmp_path):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            batch = store.publish((), completes_sync=True)
        store.ack(batch)
        progress = store.progress_generation
        with pytest.raises(RuntimeError, match="interrupted"):
            with store.transaction():
                raise RuntimeError("interrupted")
        # The rollback kept the row, but this process never offers it again.
        assert durable_ack(store) == (batch.sequence - 1, 1)
        assert (store.acked_sequence, store.next_batch()) == (batch.sequence, None)
        assert (store.progress_generation, store.pending_bytes) == (progress, 0)
        with store.transaction():
            later = store.publish((), completes_sync=True)
        assert durable_ack(store) == (batch.sequence, 1)
        assert store.next_batch() == later
    finally:
        store.close()


def test_acknowledgement_inside_a_transaction_commits_with_it(tmp_path):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            batch = store.publish((), completes_sync=True)
        with store.transaction():
            store.ack(batch)
            later = store.publish((message(),))
        # Deferring here would leave the acknowledged row ahead of the new one.
        assert durable_ack(store) == (batch.sequence, 1)
        assert store.next_batch() == later
    finally:
        store.close()
