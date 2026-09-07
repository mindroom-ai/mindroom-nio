"""Ordinary SQLite connections exclude adoption for their physical lifetime."""

import gc
import sqlite3
import subprocess
import sys
import threading
import weakref

import pytest

from nio.crypto import OlmAccount, OlmDevice
from nio.exceptions import LocalProtocolError
from nio.store import DefaultStore, SqliteStore
from nio.store._sqlite_lease import LeasedSqliteDatabase

from .store_test import CONSUMER, DEVICE, USER, open_store


def test_two_ordinary_connections_exclude_durable_open_in_another_process(tmp_path):
    first = SqliteStore(USER, DEVICE, str(tmp_path))
    second = SqliteStore(USER, DEVICE, str(tmp_path))
    program = """
import sys
from pathlib import Path
from uuid import UUID
from nio.durable.store import DurableStore
from nio.exceptions import LocalProtocolError
try:
    store = DurableStore(Path(sys.argv[1]), user_id=sys.argv[2], device_id=sys.argv[3], consumer_id=UUID(sys.argv[4]))
except LocalProtocolError as error:
    assert 'lease' in str(error)
    sys.exit(3)
store.close()
"""

    def attempt():
        return subprocess.run(
            [sys.executable, "-c", program, str(tmp_path), USER, DEVICE, str(CONSUMER)],
            capture_output=True,
            text=True,
            timeout=20,
        )

    try:
        result = attempt()
        assert result.returncode == 3, result.stderr
        first.database.close()
        second.save_account(OlmAccount())
        result = attempt()
        assert result.returncode == 3, result.stderr
        second.database.close()
        result = attempt()
        assert result.returncode == 0, result.stderr
    finally:
        first.database.close()
        second.database.close()


def test_package_import_without_fcntl_and_ownership_failure(tmp_path):
    program = """
import sys
sys.modules['fcntl'] = None
from pathlib import Path
from uuid import UUID
import nio
from nio.durable.store import DurableStore
from nio.exceptions import LocalProtocolError
from nio.store import SqliteStore
for factory in (
    lambda: SqliteStore('@alice:example.org', 'ALICE', sys.argv[1]),
    lambda: DurableStore(Path(sys.argv[1]), user_id='@alice:example.org', device_id='ALICE', consumer_id=UUID(int=1)),
):
    try:
        factory()
    except LocalProtocolError as error:
        assert 'requires fcntl' in str(error)
    else:
        raise AssertionError('ownership succeeded without filesystem locking')
"""
    result = subprocess.run(
        [sys.executable, "-c", program, str(tmp_path)],
        capture_output=True,
        text=True,
        timeout=20,
    )
    assert result.returncode == 0, result.stderr


def test_failed_connection_initialization_releases_lease(tmp_path, monkeypatch):
    def fail(*args):
        raise RuntimeError("connection setup interrupted")

    monkeypatch.setattr(LeasedSqliteDatabase, "_add_conn_hooks", fail)
    with pytest.raises(RuntimeError, match="setup interrupted"):
        SqliteStore(USER, DEVICE, str(tmp_path))
    store = open_store(tmp_path)
    store.close()


@pytest.mark.parametrize("store_class", [SqliteStore, DefaultStore])
def test_finalization_releases_lease_only_after_last_connection_user(
    tmp_path, store_class
):
    store = store_class(USER, DEVICE, str(tmp_path))
    cursor = store.database.execute_sql("SELECT 42")
    reference = weakref.ref(store)
    del store
    gc.collect()
    assert reference() is None
    with pytest.raises(LocalProtocolError, match="lease"):
        open_store(tmp_path)
    assert cursor.fetchone() == (42,)
    del cursor
    gc.collect()
    adopted = open_store(tmp_path, source_store_class=store_class)
    adopted.close()


def test_failed_physical_close_retains_connection_lease(tmp_path):
    store = SqliteStore(USER, DEVICE, str(tmp_path))
    connection = store.database.connection()
    failures = []

    def close():
        try:
            connection.close()
        except sqlite3.ProgrammingError as error:
            failures.append(error)

    thread = threading.Thread(target=close)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(failures) == 1
    with pytest.raises(LocalProtocolError, match="lease"):
        open_store(tmp_path)
    assert store.load_account() is None
    store.database.close()
    adopted = open_store(tmp_path)
    adopted.close()


@pytest.mark.parametrize("marker", ["niodurablemeta", "nioingestmeta"])
def test_closed_ordinary_reconnect_rejects_case_insensitive_markers(tmp_path, marker):
    store = SqliteStore(USER, DEVICE, str(tmp_path))
    store.database.close()
    with sqlite3.connect(store.database_path) as connection:
        connection.execute(f"CREATE TABLE {marker} (id INTEGER)")
    with pytest.raises(LocalProtocolError, match="durable|unmerged ingestion"):
        store.save_account(OlmAccount())


def test_default_sidecar_operation_holds_lease_through_connection_close(
    tmp_path, monkeypatch
):
    store = DefaultStore(USER, DEVICE, str(tmp_path))
    device = OlmDevice("@bob:example.org", "BOB", OlmAccount().identity_keys)
    remove = store.blacklist_db.remove

    def close_during_remove(key):
        result = remove(key)
        store.database.close()
        with pytest.raises(LocalProtocolError, match="lease"):
            open_store(tmp_path, source_store_class=DefaultStore)
        return result

    monkeypatch.setattr(store.blacklist_db, "remove", close_during_remove)
    assert store.verify_device(device)
    adopted = open_store(tmp_path, source_store_class=DefaultStore)
    adopted.close()
    with pytest.raises(LocalProtocolError, match="adoption"):
        store.is_device_verified(device)


@pytest.mark.parametrize("marker", ["NioIngestMeta", "nioingestmeta"])
def test_durable_rejects_prototype_marker_before_schema_writes(tmp_path, marker):
    path = tmp_path / f"{USER}_{DEVICE}.db"
    with sqlite3.connect(path) as connection:
        connection.execute(f"CREATE TABLE {marker} (id INTEGER)")
    with pytest.raises(LocalProtocolError, match="unmerged ingestion"):
        open_store(tmp_path)
    with sqlite3.connect(path) as connection:
        assert connection.execute(
            "SELECT name FROM sqlite_master WHERE type='table'"
        ).fetchall() == [(marker,)]
