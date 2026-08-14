from __future__ import annotations

import ast
import builtins
import gc
import multiprocessing
import os
import shutil
import sqlite3
import sys
import threading
import time
import weakref
from contextlib import contextmanager
from pathlib import Path
from uuid import UUID, uuid4

import pytest
from peewee import OperationalError as PeeweeOperationalError
from peewee import SqliteDatabase
from playhouse.sqliteq import SqliteQueueDatabase

from nio.crypto import OlmAccount, OlmDevice, OutboundSession
from nio.exceptions import LocalProtocolError
from nio.ingest.config import ClassicSourceConfig
from nio.ingest.errors import FreshIngestionRequired
from nio.store import DefaultStore, SqliteMemoryStore, SqliteStore
from nio.store._sync_journal_preflight import StableFileLock
import nio.store.sync_journal as bootstrap_api
from nio.store.sync_journal import open_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
BOB_ID = "@bob:example.org"
BOB_DEVICE = "BOBDEVICE"
BOB_CURVE = "T9tOKF+TShsn6mk1zisW2IBsBbTtzDNvw99RBFMJOgI"
BOB_ONETIME = "6QlQw3mGUveS735k/JDaviuoaih5eEi6S1J65iHjfgU"
SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
CONSUMER_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
ROOT = Path(__file__).parents[2]


def _open(path: Path, statements: list[str] | None = None, *, timeout: int = 2_000):
    return open_ingestion_store(
        path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        source=SOURCE,
        database_name="journal.db",
        sqlite_busy_timeout_ms=timeout,
        statement_observer=statements.append if statements is not None else None,
    )


def _find_owner(bootstrap):
    candidates = []
    for container in (bootstrap, *vars(bootstrap).values()):
        try:
            candidates.extend(vars(container).values())
        except TypeError:
            continue
    owners = {
        id(value): value
        for value in candidates
        if type(value).__name__ == "IngestionStoreOwner"
        and all(
            callable(getattr(value, scope, None))
            for scope in ("bootstrap_write", "read", "journal_write", "e2ee_write")
        )
    }
    assert len(owners) <= 1, "bootstrap has multiple IngestionStoreOwner objects"
    return next(iter(owners.values()), None)


def _owner(bootstrap):
    owner = _find_owner(bootstrap)
    assert owner is not None, "bootstrap is missing IngestionStoreOwner"
    return owner


def _execute(database, sql: str, parameters: tuple[object, ...] = ()):
    if isinstance(database, sqlite3.Connection):
        return database.execute(sql, parameters)
    return database.execute_sql(sql, parameters)


@contextmanager
def _journal_write(bootstrap, store):
    """Reach the old split boundary while prescribing the new owner scope."""
    owner = _find_owner(bootstrap)
    if owner is None:
        yield bootstrap._journal._connection
        return
    with owner.journal_write():
        yield owner.database


def _trace_connections(bootstrap, store, statements: list[str]) -> None:
    owner = _find_owner(bootstrap)
    if owner is not None:
        connections = [owner.database._state.conn]
    else:
        connections = [bootstrap._journal._connection, store.database._state.conn]
    assert all(connection is not None for connection in connections)
    for connection in {id(value): value for value in connections}.values():
        connection.set_trace_callback(statements.append)


def _resolved_connect_target(value: str | os.PathLike[str]) -> str:
    target = os.fspath(value)
    if target == ":memory:":
        return target
    if target.startswith("file:"):
        target = target[5:].split("?", 1)[0]
    return str(Path(target).resolve())


def test_each_owner_lifetime_opens_one_file_connection(
    monkeypatch, tmp_path: Path
) -> None:
    import nio.store._sync_journal_preflight as preflight

    calls: list[str] = []
    real_connect = sqlite3.connect

    def tracked_connect(*args, **kwargs):
        target = args[0] if args else kwargs["database"]
        calls.append(_resolved_connect_target(target))
        return real_connect(*args, **kwargs)

    preflight._expected_contract.cache_clear()
    monkeypatch.setattr(sqlite3, "connect", tracked_connect)
    database_path = (tmp_path / "journal.db").resolve()

    first = _open(tmp_path)
    try:
        store = first.open_matrix_store(SqliteStore)
        first._journal.load_owner()
        with _journal_write(first, store) as database:
            _execute(
                database,
                "UPDATE NioIngestMeta SET created_at_ns = created_at_ns + 1",
            )
        account = OlmAccount()
        store.save_account(account)
        assert store.load_account() is not None
        first_file_connections = calls.count(str(database_path))
    finally:
        first.close()

    reopened = _open(tmp_path)
    try:
        store = reopened.open_matrix_store(SqliteStore)
        reopened._journal.load_source()
        with _journal_write(reopened, store) as database:
            _execute(
                database,
                "UPDATE NioIngestMeta SET created_at_ns = created_at_ns + 1",
            )
        assert store.load_account() is not None
        all_file_connections = calls.count(str(database_path))
    finally:
        reopened.close()

    assert first_file_connections == 1
    assert all_file_connections == 2
    assert calls.count(":memory:") <= 1
    assert set(calls) <= {str(database_path), ":memory:"}


def test_nonempty_reopen_rejects_legacy_store_table_before_epoch_write(
    tmp_path: Path,
) -> None:
    first = _open(tmp_path)
    database_path = first.database_path
    old_epoch = first._journal.writer_epoch
    first.close()
    with sqlite3.connect(database_path) as external:
        external.execute("CREATE TABLE synctokens (id INTEGER PRIMARY KEY)")

    statements: list[str] = []
    reopened = None
    error: BaseException | None = None
    try:
        reopened = _open(tmp_path, statements)
    except BaseException as caught:  # noqa: BLE001 - exact type asserted below
        error = caught
    finally:
        if reopened is not None:
            reopened.close()

    with sqlite3.connect(database_path) as external:
        epoch = external.execute("SELECT writer_epoch FROM NioIngestMeta").fetchone()[0]
    assert type(error) is FreshIngestionRequired
    assert epoch == str(old_epoch)
    assert not any(
        statement.lstrip().upper().startswith(("UPDATE", "INSERT", "DELETE"))
        for statement in statements
    )


def test_shared_database_is_exact_ordinary_configured_peewee(tmp_path: Path) -> None:
    bootstrap = _open(tmp_path, timeout=1_750)
    try:
        owner = _owner(bootstrap)
        store = bootstrap.open_matrix_store(SqliteStore)
        database = owner.database
        assert type(database) is SqliteDatabase
        assert store.database is database
        assert database.thread_safe is False
        assert database.autoconnect is False
        assert database._timeout == 1.75
        assert database._state.conn.row_factory is sqlite3.Row
    finally:
        bootstrap.close()


def test_two_ordinary_same_file_stores_hold_shared_lifetime_leases(
    tmp_path: Path,
) -> None:
    """Each physical ordinary connection excludes ingestion until it closes."""

    first = SqliteStore(
        ACCOUNT_ID,
        DEVICE_ID,
        str(tmp_path),
        database_name="journal.db",
    )
    second = SqliteStore(
        ACCOUNT_ID,
        DEVICE_ID,
        str(tmp_path),
        database_name="journal.db",
    )
    statements: list[str] = []
    try:
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            _open(tmp_path, statements)
        assert statements == []

        first.database.close()
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            _open(tmp_path, statements)
        assert statements == []
    finally:
        if not first.database.is_closed():
            first.database.close()
        if not second.database.is_closed():
            second.database.close()


def test_default_sidecar_write_after_database_close_rejects_exclusive_owner(
    tmp_path: Path,
) -> None:
    store = DefaultStore(
        ACCOUNT_ID,
        DEVICE_ID,
        str(tmp_path),
        database_name="journal.db",
    )
    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    store.save_account(OlmAccount())
    store.save_device_keys({BOB_ID: {BOB_DEVICE: device}})
    store.database.close()
    sidecar = tmp_path / f"{ACCOUNT_ID}_{DEVICE_ID}.trusted_devices"
    before = sidecar.read_bytes() if sidecar.exists() else None

    exclusive = StableFileLock(tmp_path / "journal.db")
    try:
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            store.verify_device(device)
        after = sidecar.read_bytes() if sidecar.exists() else None
        assert after == before
    finally:
        exclusive.close()


def test_default_sidecar_init_failure_releases_database_lease_immediately(
    monkeypatch,
    tmp_path: Path,
) -> None:
    import nio.store.database as database_module

    real_key_store = database_module.KeyStore
    calls = 0

    class SidecarLoadFailure(BaseException):
        pass

    def failing_key_store(*args, **kwargs):
        nonlocal calls
        calls += 1
        if calls == 2:
            raise SidecarLoadFailure
        return real_key_store(*args, **kwargs)

    monkeypatch.setattr(database_module, "KeyStore", failing_key_store)
    retained_error: BaseException | None = None
    try:
        DefaultStore(
            ACCOUNT_ID,
            DEVICE_ID,
            str(tmp_path),
            database_name="journal.db",
        )
    except SidecarLoadFailure as error:
        # Retaining the traceback keeps the partially constructed store alive,
        # so this proves explicit cleanup rather than refcount finalization.
        retained_error = error

    assert retained_error is not None
    exclusive = StableFileLock(tmp_path / "journal.db")
    exclusive.close()


def test_default_multifile_trust_mutation_holds_one_shared_semantic_guard(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store = DefaultStore(
        ACCOUNT_ID,
        DEVICE_ID,
        str(tmp_path),
        database_name="journal.db",
    )
    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    store.database.close()
    entered = threading.Event()
    release = threading.Event()
    errors: list[BaseException] = []
    real_remove = store.blacklist_db.remove

    def paused_remove(key):
        result = real_remove(key)
        entered.set()
        assert release.wait(timeout=5)
        return result

    monkeypatch.setattr(store.blacklist_db, "remove", paused_remove)

    def mutate() -> None:
        try:
            store.verify_device(device)
        except BaseException as error:  # noqa: BLE001 - inspected below
            errors.append(error)

    thread = threading.Thread(target=mutate)
    thread.start()
    assert entered.wait(timeout=5)
    try:
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            StableFileLock(tmp_path / "journal.db")
    finally:
        release.set()
        thread.join(timeout=5)
    assert not thread.is_alive()
    assert errors == []


def test_ordinary_reconnect_rejects_replaced_database_identity_before_sql(
    tmp_path: Path,
) -> None:
    store = SqliteStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    store.database.close()
    original = tmp_path / "journal.db"
    backup = tmp_path / "journal.original"
    original.rename(backup)
    shutil.copyfile(backup, original)
    try:
        with pytest.raises(LocalProtocolError, match="identity changed"):
            store.load_account()
    finally:
        original.unlink()
        backup.rename(original)


def test_ordinary_reconnect_rejects_retargeted_symlink_before_sql(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    for root in (first, second):
        seed = SqliteStore(ACCOUNT_ID, DEVICE_ID, str(root), database_name="journal.db")
        seed.database.close()
    alias = tmp_path / "alias.db"
    alias.symlink_to(first / "journal.db")
    store = SqliteStore(ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name=alias.name)
    store.database.close()
    alias.unlink()
    alias.symlink_to(second / "journal.db")

    with pytest.raises(LocalProtocolError, match="identity changed"):
        store.load_account()


def test_default_closed_store_rejects_retargeted_symlink_before_sidecar_io(
    tmp_path: Path,
) -> None:
    first = tmp_path / "first"
    second = tmp_path / "second"
    first.mkdir()
    second.mkdir()
    for root in (first, second):
        seed = SqliteStore(ACCOUNT_ID, DEVICE_ID, str(root), database_name="journal.db")
        seed.database.close()
    alias = tmp_path / "alias.db"
    alias.symlink_to(first / "journal.db")
    store = DefaultStore(ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name=alias.name)
    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    store.database.close()
    alias.unlink()
    alias.symlink_to(second / "journal.db")
    sidecars = tuple(
        tmp_path / f"{ACCOUNT_ID}_{DEVICE_ID}.{suffix}"
        for suffix in ("trusted_devices", "blacklisted_devices", "ignored_devices")
    )
    before = tuple(path.read_bytes() if path.exists() else None for path in sidecars)

    with pytest.raises(LocalProtocolError, match="identity changed"):
        store.is_device_verified(device)
    with pytest.raises(LocalProtocolError, match="identity changed"):
        store.verify_device(device)
    assert (
        tuple(path.read_bytes() if path.exists() else None for path in sidecars)
        == before
    )


@pytest.mark.parametrize("store_class", (SqliteStore, DefaultStore))
def test_store_finalization_releases_shared_lease(
    store_class: type[SqliteStore] | type[DefaultStore],
    tmp_path: Path,
) -> None:
    store = store_class(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    reference = weakref.ref(store)
    del store
    gc.collect()
    assert reference() is None
    lease = StableFileLock(tmp_path / "journal.db")
    lease.close()


def test_default_guard_rejects_unlinked_path_with_surviving_hardlink(
    tmp_path: Path,
) -> None:
    store = DefaultStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    store.database.close()
    primary = tmp_path / "journal.db"
    alias = tmp_path / "journal.alias"
    os.link(primary, alias)
    primary.unlink()
    with pytest.raises(LocalProtocolError, match="identity"):
        store.is_device_verified(device)
    with pytest.raises(LocalProtocolError, match="identity"):
        store.verify_device(device)
    exclusive = StableFileLock(alias)
    exclusive.close()


def test_default_whole_method_guard_survives_reentrant_database_close(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store = DefaultStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    real_remove = store.blacklist_db.remove

    def close_during_remove(key):
        result = real_remove(key)
        store.database.close()
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            StableFileLock(tmp_path / "journal.db")
        return result

    monkeypatch.setattr(store.blacklist_db, "remove", close_during_remove)
    assert store.verify_device(device)
    lease = StableFileLock(tmp_path / "journal.db")
    lease.close()


def test_default_method_cancellation_releases_transient_lease(
    monkeypatch,
    tmp_path: Path,
) -> None:
    store = DefaultStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    store.database.close()

    class Cancelled(BaseException):
        pass

    monkeypatch.setattr(
        store.blacklist_db,
        "remove",
        lambda _key: (_ for _ in ()).throw(Cancelled()),
    )
    with pytest.raises(Cancelled):
        store.verify_device(device)
    lease = StableFileLock(tmp_path / "journal.db")
    lease.close()


def test_default_direct_keystore_call_cannot_piggyback_foreign_thread_guard(
    monkeypatch,
    tmp_path: Path,
) -> None:
    from nio.store import Key

    store = DefaultStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    entered = threading.Event()
    release = threading.Event()
    real_remove = store.blacklist_db.remove

    def paused_remove(key):
        result = real_remove(key)
        entered.set()
        assert release.wait(timeout=5)
        return result

    monkeypatch.setattr(store.blacklist_db, "remove", paused_remove)
    guarded = threading.Thread(target=store.verify_device, args=(device,))
    guarded.start()
    assert entered.wait(timeout=5)
    before = list(store.trust_db._entries)
    errors: list[BaseException] = []

    def direct() -> None:
        try:
            store.trust_db.add(Key.from_olmdevice(device))
        except BaseException as error:  # noqa: BLE001 - inspected below
            errors.append(error)

    foreign = threading.Thread(target=direct)
    foreign.start()
    foreign.join(timeout=5)
    release.set()
    guarded.join(timeout=5)
    assert len(errors) == 1 and isinstance(errors[0], LocalProtocolError)
    assert store.trust_db._entries != before  # Guarded verify was the only mutation.
    store.database.close()


def _exclusive_attempt(path: str, output) -> None:
    try:
        lease = StableFileLock(Path(path))
    except BaseException as error:  # noqa: BLE001 - primitive result only
        output.send(type(error).__name__)
    else:
        lease.close()
        output.send("acquired")
    finally:
        output.close()


def _collect_last_store_reference_on_foreign_thread(
    path: str,
    ready,
    release,
) -> None:
    import nio.store._ingestion_store_owner as owner_module

    box: list[SqliteStore] = []
    constructed = threading.Event()
    finish_creator = threading.Event()

    def construct() -> None:
        box.append(SqliteStore(ACCOUNT_ID, DEVICE_ID, path, database_name="journal.db"))
        constructed.set()
        finish_creator.wait(timeout=10)

    creator = threading.Thread(target=construct)
    creator.start()
    if not constructed.wait(timeout=5) or len(box) != 1:
        ready.send("construct-failed")
        return
    store = box.pop()
    reference = weakref.ref(store)
    del store
    gc.collect()
    ready.send(
        (
            "collected" if reference() is None else "retained",
            len(owner_module._DEFERRED_LIFETIME_LEASES),
            tuple(
                (lease._fd, lease._database_fd)
                for lease in owner_module._DEFERRED_LIFETIME_LEASES
            ),
        )
    )
    release.recv()
    finish_creator.set()
    creator.join(timeout=5)


def test_shared_ordinary_lease_blocks_exclusive_across_spawn_processes(
    tmp_path: Path,
) -> None:
    store = SqliteStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    context = multiprocessing.get_context("spawn")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(
        target=_exclusive_attempt,
        args=(str(tmp_path / "journal.db"), send),
    )
    process.start()
    send.close()
    assert receive.recv() == "LocalProtocolError"
    process.join(timeout=10)
    assert process.exitcode == 0
    store.database.close()

    receive, send = context.Pipe(duplex=False)
    process = context.Process(
        target=_exclusive_attempt,
        args=(str(tmp_path / "journal.db"), send),
    )
    process.start()
    send.close()
    assert receive.recv() == "acquired"
    process.join(timeout=10)
    assert process.exitcode == 0


def test_foreign_thread_last_reference_gc_retains_lease_until_process_exit(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    ready_receive, ready_send = context.Pipe(duplex=False)
    release_receive, release_send = context.Pipe(duplex=False)
    process = context.Process(
        target=_collect_last_store_reference_on_foreign_thread,
        args=(str(tmp_path), ready_send, release_receive),
    )
    process.start()
    ready_send.close()
    release_receive.close()
    try:
        assert ready_receive.poll(10), "foreign-thread collection child hung"
        collected, deferred, descriptors = ready_receive.recv()
        assert collected == "collected"
        assert deferred == 1
        assert all(
            sidecar >= 0 and database >= 0 for sidecar, database in descriptors
        ), descriptors
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            StableFileLock(tmp_path / "journal.db")
    finally:
        release_send.send("exit")
        process.join(timeout=10)
        if process.is_alive():
            process.terminate()
            process.join(timeout=5)
    assert process.exitcode == 0
    lease = StableFileLock(tmp_path / "journal.db")
    lease.close()


@pytest.mark.parametrize("alias_kind", ("symlink", "hardlink"))
def test_ordinary_alias_shared_lease_blocks_real_path_exclusive(
    alias_kind: str,
    tmp_path: Path,
) -> None:
    seed = SqliteStore(ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db")
    seed.database.close()
    alias = tmp_path / "alias.db"
    if alias_kind == "symlink":
        alias.symlink_to(tmp_path / "journal.db")
    else:
        os.link(tmp_path / "journal.db", alias)
    store = SqliteStore(ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name=alias.name)
    try:
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            StableFileLock(tmp_path / "journal.db")
    finally:
        store.database.close()


@pytest.mark.parametrize("store_class", (SqliteStore, DefaultStore))
def test_retained_ordinary_store_rejects_post_adoption_reconnect_before_dml(
    store_class: type[SqliteStore] | type[DefaultStore],
    tmp_path: Path,
) -> None:
    store = store_class(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    store.save_account(OlmAccount())
    store.database.close()
    adopted = bootstrap_api._open_configured_ingestion_store(
        tmp_path,
        source_store_class=store_class,
        owned_store_class=SqliteStore,
        source=SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
    )
    adopted.close()
    with pytest.raises(LocalProtocolError, match="owned by ingestion"):
        store.save_account(OlmAccount())
    if store_class is DefaultStore:
        device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
        with pytest.raises(LocalProtocolError, match="after adoption"):
            store.verify_device(device)


def _ordinary_first_open(path: str, start, release, output) -> None:
    try:
        start.wait(timeout=10)
        store = SqliteStore(ACCOUNT_ID, DEVICE_ID, path, database_name="journal.db")
        opened = os.stat(Path(path) / "journal.db")
        output.send((opened.st_dev, opened.st_ino))
        release.wait(timeout=10)
        store.database.close()
    except BaseException as error:  # noqa: BLE001 - primitive result only
        output.send(type(error).__name__)
    finally:
        output.close()


def test_concurrent_first_ordinary_openers_share_created_inode(
    tmp_path: Path,
) -> None:
    context = multiprocessing.get_context("spawn")
    start = context.Barrier(3)
    release = context.Event()
    pipes = [context.Pipe(duplex=False) for _ in range(2)]
    processes = [
        context.Process(
            target=_ordinary_first_open,
            args=(str(tmp_path), start, release, send),
        )
        for _index, (_receive, send) in enumerate(pipes)
    ]
    for process in processes:
        process.start()
    for _receive, send in pipes:
        send.close()
    start.wait(timeout=10)
    identities = [receive.recv() for receive, _send in pipes]
    try:
        assert all(type(identity) is tuple for identity in identities)
        assert len(set(identities)) == 1
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            StableFileLock(tmp_path / "journal.db")
    finally:
        release.set()
        for process in processes:
            process.join(timeout=10)
    assert all(process.exitcode == 0 for process in processes)


def _fork_inherited_action(store, action: str, output) -> None:
    try:
        if action == "collect":
            del store
            gc.collect()
            output.send("collected")
        else:
            store.load_account()
            output.send("unexpected-success")
    except BaseException as error:  # noqa: BLE001 - primitive result only
        output.send(type(error).__name__)
    finally:
        output.close()


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(), reason="POSIX"
)
def test_fork_child_gc_does_not_release_parent_shared_lease(tmp_path: Path) -> None:
    store = SqliteStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    ready_read, ready_write = os.pipe()
    release_read, release_write = os.pipe()
    child = os.fork()
    if child == 0:  # pragma: no cover - assertions live in the parent
        os.close(ready_read)
        os.close(release_write)
        reference = weakref.ref(store)
        del store
        gc.collect()
        os.write(ready_write, b"D" if reference() is None else b"L")
        os.read(release_read, 1)
        os._exit(0)

    os.close(ready_write)
    os.close(release_read)
    try:
        assert os.read(ready_read, 1) == b"D"
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            StableFileLock(tmp_path / "journal.db")
    finally:
        os.write(release_write, b"X")
        _pid, status = os.waitpid(child, 0)
        os.close(ready_read)
        os.close(release_write)
        store.database.close()
    assert os.waitstatus_to_exitcode(status) == 0


@pytest.mark.skipif(
    "fork" not in multiprocessing.get_all_start_methods(), reason="POSIX"
)
@pytest.mark.parametrize("store_class", (SqliteStore, DefaultStore))
def test_closed_before_fork_store_cannot_reopen_in_child(
    store_class: type[SqliteStore] | type[DefaultStore],
    tmp_path: Path,
) -> None:
    store = store_class(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    store.database.close()
    context = multiprocessing.get_context("fork")
    receive, send = context.Pipe(duplex=False)
    process = context.Process(target=_fork_inherited_action, args=(store, "load", send))
    process.start()
    send.close()
    assert receive.poll(5), "fork child inherited operation hung"
    assert receive.recv() == "LocalProtocolError"
    process.join(timeout=10)
    assert process.exitcode == 0


def test_wrong_thread_ordinary_close_does_not_release_connection_lease(
    tmp_path: Path,
) -> None:
    store = SqliteStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    connection = store.database.connection()
    results: list[object] = []

    def close_foreign() -> None:
        try:
            results.append(connection.close())
        except BaseException as error:  # noqa: BLE001 - inspected below
            results.append(error)

    thread = threading.Thread(target=close_foreign)
    thread.start()
    thread.join(timeout=5)
    assert not thread.is_alive()
    assert len(results) == 1 and isinstance(results[0], sqlite3.ProgrammingError)
    with pytest.raises(LocalProtocolError, match="lifetime lease"):
        StableFileLock(tmp_path / "journal.db")
    assert store.load_account() is None
    store.database.close()


def _enter_scope(owner, scope: str) -> None:
    with getattr(owner, scope)():
        owner.database.execute_sql("SELECT 42")


@contextmanager
def _replaced_identity(path: Path):
    backup = path.with_name(f".{path.name}.owner-test-backup")
    os.replace(path, backup)
    if backup.stat().st_size:
        shutil.copyfile(backup, path)
    else:
        path.touch()
    try:
        yield
    finally:
        path.unlink(missing_ok=True)
        os.replace(backup, path)


def test_raw_connection_context_exit_rechecks_database_identity(
    tmp_path: Path,
) -> None:
    store = SqliteStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    connection = store.database.connection()
    connection.__enter__()
    with _replaced_identity(tmp_path / "journal.db"):
        with pytest.raises(LocalProtocolError, match="identity changed"):
            connection.__exit__(None, None, None)
    store.database.close()


def test_raw_connection_deserialize_is_fail_closed(tmp_path: Path) -> None:
    store = SqliteStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    connection = store.database.connection()
    image = connection.serialize()
    with pytest.raises(LocalProtocolError, match="deserialization.*unsupported"):
        connection.deserialize(image)
    store.database.close()


def test_raw_cursor_fetchmany_uses_arraysize_and_rechecks_identity(
    tmp_path: Path,
) -> None:
    store = SqliteStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    connection = store.database.connection()
    cursor = connection.execute("SELECT 1 UNION ALL SELECT 2 UNION ALL SELECT 3")
    cursor.arraysize = 2
    assert cursor.fetchmany() == [(1,), (2,)]

    stale = connection.execute("SELECT 1")
    with _replaced_identity(tmp_path / "journal.db"):
        with pytest.raises(LocalProtocolError, match="identity changed"):
            stale.fetchmany()
    store.database.close()


@pytest.mark.parametrize("scope", ["read", "journal_write", "e2ee_write"])
@pytest.mark.parametrize(
    "fault", ["pid", "thread", "lock_fd", "lock_path", "database_inode"]
)
def test_outer_owner_gates_fail_before_sql(
    scope: str,
    fault: str,
    monkeypatch,
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    try:
        owner = _owner(bootstrap)
        statements.clear()
        if fault == "pid":
            module = sys.modules[type(owner).__module__]
            with monkeypatch.context() as patch:
                patch.setattr(module.os, "getpid", lambda: -1)
                with pytest.raises(LocalProtocolError):
                    _enter_scope(owner, scope)
        elif fault == "thread":
            errors: list[BaseException] = []

            def enter_from_wrong_thread() -> None:
                try:
                    _enter_scope(owner, scope)
                except BaseException as error:  # noqa: BLE001 - inspected below
                    errors.append(error)

            thread = threading.Thread(target=enter_from_wrong_thread)
            thread.start()
            thread.join()
            assert len(errors) == 1
            assert isinstance(errors[0], LocalProtocolError)
        elif fault in {"lock_fd", "lock_path"}:
            message = (
                "ingestion writer lock is closed"
                if fault == "lock_fd"
                else "ingestion lock file identity changed after lock acquisition"
            )
            with monkeypatch.context() as patch:
                patch.setattr(
                    StableFileLock,
                    "assert_identity",
                    lambda _lock: (_ for _ in ()).throw(LocalProtocolError(message)),
                )
                with pytest.raises(LocalProtocolError, match=message):
                    _enter_scope(owner, scope)
        else:
            with _replaced_identity(bootstrap.database_path):
                with pytest.raises(LocalProtocolError):
                    _enter_scope(owner, scope)
        assert statements == []
    finally:
        bootstrap.close()


@pytest.mark.parametrize("scope", ["read", "journal_write", "e2ee_write"])
def test_stale_writer_epoch_emits_only_the_owner_fence(
    scope: str,
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    try:
        owner = _owner(bootstrap)
        with sqlite3.connect(bootstrap.database_path) as external:
            external.execute(
                "UPDATE NioIngestMeta SET writer_epoch = ? WHERE account_id = ?",
                (str(uuid4()), ACCOUNT_ID),
            )
        statements.clear()
        with pytest.raises(LocalProtocolError):
            _enter_scope(owner, scope)
        significant = [
            " ".join(sql.split())
            for sql in statements
            if not sql.lstrip()
            .upper()
            .startswith(("PRAGMA", "BEGIN", "COMMIT", "ROLLBACK"))
        ]
        assert len(significant) == 1
        assert "NioIngestMeta" in significant[0]
        assert "writer_epoch" in significant[0]
        assert "SELECT 42" not in statements
    finally:
        bootstrap.close()


class _SqliteSubclass(SqliteStore):
    pass


class _QueueStore(SqliteStore):
    def _create_database(self):
        return SqliteQueueDatabase(self.database_path, autostart=False)


class _CustomDefaultStore(DefaultStore):
    def _create_database(self):
        return SqliteDatabase(self.database_path)


def test_inherited_builtin_factory_subclass_holds_shared_lease(
    tmp_path: Path,
) -> None:
    store = _SqliteSubclass(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    try:
        with pytest.raises(LocalProtocolError, match="lifetime lease"):
            StableFileLock(tmp_path / "journal.db")
    finally:
        store.database.close()


def test_custom_default_factory_retains_pre_candidate_sidecar_behavior(
    tmp_path: Path,
) -> None:
    store = _CustomDefaultStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    try:
        assert store.trust_db._ownership_assertion is None
        assert store.blacklist_db._ownership_assertion is None
        assert store.ignore_db._ownership_assertion is None
        assert store.verify_device(device)
        assert store.is_device_verified(device)
        lease = StableFileLock(tmp_path / "journal.db")
        lease.close()
    finally:
        store.database.close()


@pytest.mark.parametrize(
    "store_class",
    [DefaultStore, SqliteMemoryStore, _SqliteSubclass, _QueueStore],
)
def test_bootstrap_rejects_every_nonexact_store_before_create(
    store_class,
    monkeypatch,
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    events: list[str] = []
    before_files = {path.name for path in tmp_path.iterdir()}
    real_store_create = store_class._create_database

    def tracked(name, function):
        def call(*args, **kwargs):
            events.append(name)
            return function(*args, **kwargs)

        return call

    def tracked_create(self):
        events.append("_create_database")
        return real_store_create(self)

    try:
        statements.clear()
        error: BaseException | None = None
        with monkeypatch.context() as patch:
            patch.setattr(store_class, "_create_database", tracked_create)
            patch.setattr(
                SqliteDatabase,
                "connect",
                tracked("peewee.connect", SqliteDatabase.connect),
            )
            patch.setattr(
                SqliteDatabase,
                "execute_sql",
                tracked("peewee.execute_sql", SqliteDatabase.execute_sql),
            )
            patch.setattr(
                sqlite3, "connect", tracked("sqlite3.connect", sqlite3.connect)
            )
            for namespace, names in (
                (builtins, ("open",)),
                (
                    os,
                    (
                        "open",
                        "stat",
                        "lstat",
                        "fstat",
                        "mkdir",
                        "makedirs",
                        "remove",
                        "unlink",
                        "rename",
                        "replace",
                        "link",
                        "symlink",
                        "truncate",
                        "chmod",
                        "chown",
                        "utime",
                    ),
                ),
                (os.path, ("abspath", "realpath", "join")),
                (
                    Path,
                    (
                        "exists",
                        "is_dir",
                        "is_file",
                        "iterdir",
                        "mkdir",
                        "open",
                        "read_bytes",
                        "read_text",
                        "resolve",
                        "stat",
                        "lstat",
                        "touch",
                        "write_bytes",
                        "write_text",
                        "unlink",
                        "rename",
                        "replace",
                    ),
                ),
                (shutil, ("copy", "copy2", "copyfile", "move")),
            ):
                for name in names:
                    patch.setattr(
                        namespace,
                        name,
                        tracked(
                            f"{namespace.__name__}.{name}", getattr(namespace, name)
                        ),
                    )
            try:
                bootstrap.open_matrix_store(store_class)
            except BaseException as caught:  # noqa: BLE001 - exact type asserted below
                error = caught
        assert (
            type(error),
            events,
            statements,
            {path.name for path in tmp_path.iterdir()},
        ) == (LocalProtocolError, [], [], before_files)
    finally:
        bootstrap.close()


def test_borrowed_exact_store_does_not_create_connect_or_close(
    monkeypatch, tmp_path: Path
) -> None:
    bootstrap = _open(tmp_path)
    events: list[str] = []
    real_create = SqliteStore._create_database
    real_connect = SqliteDatabase.connect
    real_close = SqliteDatabase.close
    real_sqlite_connect = sqlite3.connect

    def tracked_create(self):
        events.append("create")
        return real_create(self)

    def tracked_connect(self, *args, **kwargs):
        events.append("connect")
        return real_connect(self, *args, **kwargs)

    def tracked_close(self, *args, **kwargs):
        events.append("close")
        return real_close(self, *args, **kwargs)

    def tracked_sqlite_connect(*args, **kwargs):
        events.append("sqlite3.connect")
        return real_sqlite_connect(*args, **kwargs)

    monkeypatch.setattr(SqliteStore, "_create_database", tracked_create)
    monkeypatch.setattr(SqliteDatabase, "connect", tracked_connect)
    monkeypatch.setattr(SqliteDatabase, "close", tracked_close)
    monkeypatch.setattr(sqlite3, "connect", tracked_sqlite_connect)
    try:
        store = bootstrap.open_matrix_store(SqliteStore)
        assert events == []
        assert store.database is _owner(bootstrap).database
    finally:
        bootstrap.close()


def test_fresh_owned_store_rejects_hard_link_added_after_open(tmp_path: Path) -> None:
    bootstrap = bootstrap_api._open_fresh_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        source=SOURCE,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    alias = tmp_path / "late-hardlink.db"
    os.link(bootstrap.database_path, alias)
    try:
        with pytest.raises(LocalProtocolError, match="hard link"):
            store.load_account()
    finally:
        alias.unlink()
        bootstrap.close()


def test_fresh_owned_store_rejects_canonical_path_retargeted_to_symlink(
    tmp_path: Path,
) -> None:
    bootstrap = bootstrap_api._open_fresh_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        source=SOURCE,
        database_name="journal.db",
    )
    store = bootstrap.open_matrix_store(SqliteStore)
    database_path = bootstrap.database_path
    alias = tmp_path / "retarget.db"
    os.link(database_path, alias)
    database_path.unlink()
    database_path.symlink_to(alias.name)
    try:
        with pytest.raises(LocalProtocolError, match="regular database path"):
            store.load_account()
    finally:
        database_path.unlink()
        os.link(alias, database_path)
        alias.unlink()
        bootstrap.close()


def test_fresh_owned_store_rejects_hard_link_racing_owner_claim(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    database_path = tmp_path / "journal.db"
    alias = tmp_path / "claim-race.db"
    database_path.touch()
    real_open = bootstrap_api._SqliteIngestionJournal.open

    def raced_open(cls, database, **kwargs):
        os.link(database_path, alias)
        return real_open(database, **kwargs)

    monkeypatch.setattr(
        bootstrap_api._SqliteIngestionJournal,
        "open",
        classmethod(raced_open),
    )
    with pytest.raises(LocalProtocolError, match="singly linked"):
        bootstrap_api._open_fresh_ingestion_store(
            tmp_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            source=SOURCE,
            database_name=database_path.name,
        )
    with sqlite3.connect(database_path) as connection:
        assert connection.execute("SELECT * FROM sqlite_master").fetchall() == []

    alias.unlink()
    monkeypatch.setattr(
        bootstrap_api._SqliteIngestionJournal,
        "open",
        real_open,
    )
    retry = bootstrap_api._open_fresh_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        source=SOURCE,
        database_name=database_path.name,
    )
    retry.close()


def _mutation_case(store, name: str):
    account = OlmAccount()
    if name == "account":
        return (
            lambda: store.save_account(account),
            lambda: store.load_account() is not None,
        )

    store.save_account(account)
    if name == "session":
        session = OutboundSession(account, BOB_CURVE, BOB_ONETIME)
        return (
            lambda: store.save_session(BOB_CURVE, session),
            lambda: store.load_sessions().get(BOB_CURVE) is not None,
        )

    device = OlmDevice(BOB_ID, BOB_DEVICE, OlmAccount().identity_keys)
    devices = {BOB_ID: {BOB_DEVICE: device}}
    if name == "device_keys":
        return (
            lambda: store.save_device_keys(devices),
            lambda: BOB_ID in store.load_device_keys().users,
        )

    store.save_device_keys(devices)
    return lambda: store.verify_device(device), lambda: store.is_device_verified(device)


def _created_at(bootstrap) -> int:
    owner = _find_owner(bootstrap)
    database = owner.database if owner is not None else bootstrap._journal._connection
    row = _execute(database, "SELECT created_at_ns FROM NioIngestMeta").fetchone()
    return row[0]


def _mutate_then_fail(bootstrap, store, mutate) -> None:
    with _journal_write(bootstrap, store) as database:
        _execute(
            database,
            "UPDATE NioIngestMeta SET created_at_ns = created_at_ns + 1",
        )
        mutate()
        raise RuntimeError("injected after model DML")


@pytest.mark.parametrize("case", ["account", "session", "device_keys", "verify"])
def test_raw_journal_and_real_orm_dml_roll_back_together(
    case: str,
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    try:
        store = bootstrap.open_matrix_store(SqliteStore)
        mutate, present = _mutation_case(store, case)
        before = _created_at(bootstrap)
        with pytest.raises(RuntimeError, match="injected after model DML"):
            _mutate_then_fail(bootstrap, store, mutate)
        assert (_created_at(bootstrap), present()) == (before, False)
    finally:
        bootstrap.close()


@pytest.mark.parametrize("case", ["account", "session", "device_keys", "verify"])
def test_raw_journal_and_real_orm_dml_share_one_outer_transaction(
    case: str,
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    try:
        store = bootstrap.open_matrix_store(SqliteStore)
        mutate, present = _mutation_case(store, case)
        before = _created_at(bootstrap)
        statements: list[str] = []
        _trace_connections(bootstrap, store, statements)
        with _journal_write(bootstrap, store) as database:
            _execute(
                database,
                "UPDATE NioIngestMeta SET created_at_ns = created_at_ns + 1",
            )
            mutate()
        normalized = [" ".join(sql.upper().split()) for sql in statements]
        begins = [sql for sql in normalized if sql.startswith("BEGIN")]
        fences = [
            sql
            for sql in normalized
            if "NIOINGESTMETA" in sql and "WRITER_EPOCH" in sql
        ]
        assert (_created_at(bootstrap), present()) == (before + 1, True)
        assert begins == ["BEGIN IMMEDIATE"]
        assert len(fences) == 1
        assert any(sql.startswith("SAVEPOINT") for sql in normalized)
        assert normalized.index("BEGIN IMMEDIATE") < next(
            index
            for index, sql in enumerate(normalized)
            if "SET CREATED_AT_NS = CREATED_AT_NS + 1" in sql
        )
    finally:
        bootstrap.close()


def test_external_sqlite_write_lock_times_out_e2ee_without_partial_writes(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path, timeout=100)
    store = bootstrap.open_matrix_store(SqliteStore)
    external = sqlite3.connect(
        bootstrap.database_path,
        isolation_level=None,
        timeout=0,
    )
    try:
        external.execute("BEGIN IMMEDIATE")
        started = time.monotonic()
        with pytest.raises(
            (sqlite3.OperationalError, PeeweeOperationalError),
            match="locked",
        ):
            store.save_account(OlmAccount())
        elapsed = time.monotonic() - started

        assert 0.100 <= elapsed <= 0.350
        assert store.load_account() is None

        external.rollback()
        store.save_account(OlmAccount())
        assert store.load_account() is not None
    finally:
        if external.in_transaction:
            external.rollback()
        external.close()
        bootstrap.close()


def test_session_pickle_upgrade_load_runs_inside_write_owner_scope(
    monkeypatch,
    tmp_path: Path,
) -> None:
    import nio.store.database as store_database

    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    try:
        store = bootstrap.open_matrix_store(SqliteStore)
        account = OlmAccount()
        store.save_account(account)
        session = OutboundSession(account, BOB_CURVE, BOB_ONETIME)
        store.save_session(BOB_CURVE, session)
        session.upgrade_pickle = True
        monkeypatch.setattr(
            store_database.Session,
            "from_pickle",
            lambda *_args, **_kwargs: session,
        )

        statements.clear()
        loaded = store.load_sessions().get(BOB_CURVE)
        normalized = [" ".join(sql.upper().split()) for sql in statements]
        assert loaded is session
        assert session.upgrade_pickle is False
        assert any(sql.startswith("SAVEPOINT") for sql in normalized)
        assert any(
            sql.startswith("INSERT OR REPLACE") and "OLMSESSIONS" in sql
            for sql in normalized
        )
    finally:
        bootstrap.close()


@pytest.mark.parametrize("scope", ["read", "journal_write", "e2ee_write"])
@pytest.mark.parametrize("state", ["active", "closing", "closed"])
def test_owner_scope_lifecycle(
    scope: str,
    state: str,
    monkeypatch,
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    bootstrap.open_matrix_store(SqliteStore)
    owner = _owner(bootstrap)
    close_attempts = 0

    if state == "closing":
        real_close = owner.database.close

        def fail_once():
            nonlocal close_attempts
            close_attempts += 1
            if close_attempts == 1:
                raise RuntimeError("injected connection close failure")
            return real_close()

        monkeypatch.setattr(owner.database, "close", fail_once)
        with pytest.raises(RuntimeError, match="injected connection close failure"):
            bootstrap.close()
    elif state == "closed":
        bootstrap.close()

    statements.clear()
    try:
        if state == "active":
            _enter_scope(owner, scope)
            assert any("SELECT 42" in statement for statement in statements)
        else:
            with pytest.raises(LocalProtocolError):
                _enter_scope(owner, scope)
            assert statements == []
    finally:
        if state != "closed":
            bootstrap.close()


def test_revoked_borrowed_store_views_emit_no_sql(tmp_path: Path) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    store = bootstrap.open_matrix_store(SqliteStore)
    store._revoke_ingestion_lease()
    statements.clear()
    try:
        with pytest.raises(LocalProtocolError):
            store.load_account()
        with pytest.raises(LocalProtocolError):
            store.save_account(OlmAccount())
        assert statements == []
        for scope in ("read", "journal_write", "e2ee_write"):
            _enter_scope(_owner(bootstrap), scope)
        assert sum("SELECT 42" in statement for statement in statements) == 3
    finally:
        bootstrap.close()


def test_repeated_borrowed_initialization_rejects_before_sql(tmp_path: Path) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    try:
        store = bootstrap.open_matrix_store(SqliteStore)
        statements.clear()
        with pytest.raises(LocalProtocolError):
            store._post_init_ingestion_store(bootstrap)
        assert statements == []
    finally:
        bootstrap.close()


def test_close_orders_revoke_connection_and_lock(monkeypatch, tmp_path: Path) -> None:
    bootstrap = _open(tmp_path)
    store = bootstrap.open_matrix_store(SqliteStore)
    owner = _owner(bootstrap)
    events: list[str] = []
    real_revoke = store._revoke_ingestion_lease
    real_database_close = owner.database.close
    real_lock_close = StableFileLock.close

    def revoke():
        events.append("revoke")
        return real_revoke()

    def close_database():
        events.append("connection")
        return real_database_close()

    def close_lock(lock):
        events.append("lock")
        return real_lock_close(lock)

    monkeypatch.setattr(store, "_revoke_ingestion_lease", revoke)
    monkeypatch.setattr(owner.database, "close", close_database)
    monkeypatch.setattr(StableFileLock, "close", close_lock)
    bootstrap.close()
    assert events == ["revoke", "connection", "lock"]
    bootstrap.close()
    assert events == ["revoke", "connection", "lock"]


def test_connection_close_failure_keeps_lock_and_bootstrap_can_retry(
    monkeypatch,
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    bootstrap.open_matrix_store(SqliteStore)
    owner = _owner(bootstrap)
    real_close = owner.database.close
    attempts = 0

    def flaky_close():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("injected connection close failure")
        return real_close()

    monkeypatch.setattr(owner.database, "close", flaky_close)
    with pytest.raises(RuntimeError, match="injected connection close failure"):
        bootstrap.close()
    with pytest.raises(LocalProtocolError):
        StableFileLock(bootstrap.database_path)

    bootstrap.close()
    replacement = StableFileLock(bootstrap.database_path)
    replacement.close()
    assert attempts == 2


def test_stale_sidecar_and_close_failure_retain_exclusion_until_retry(
    monkeypatch,
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    bootstrap.open_matrix_store(SqliteStore)
    owner = _owner(bootstrap)
    real_close = owner.database.close
    attempts = 0

    def flaky_close():
        nonlocal attempts
        attempts += 1
        if attempts == 1:
            raise RuntimeError("injected connection close failure")
        return real_close()

    monkeypatch.setattr(owner.database, "close", flaky_close)
    lock_path = Path(f"{bootstrap.database_path.resolve()}.ingest.lock")
    lock_path.rename(tmp_path / "stale.ingest.lock")
    lock_path.touch()

    with pytest.raises(RuntimeError, match="injected connection close failure"):
        bootstrap.close()
    with pytest.raises(LocalProtocolError, match="lifetime lease"):
        StableFileLock(bootstrap.database_path)

    with pytest.raises(LocalProtocolError, match="identity changed"):
        bootstrap.close()
    replacement = StableFileLock(bootstrap.database_path)
    replacement.close()
    assert attempts == 2


def test_close_rejects_wrong_thread_before_sql(tmp_path: Path) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    store = bootstrap.open_matrix_store(SqliteStore)
    owner = _owner(bootstrap)
    errors: list[BaseException] = []
    statements.clear()

    def close_from_wrong_thread() -> None:
        try:
            bootstrap.close()
        except BaseException as error:  # noqa: BLE001 - inspected below
            errors.append(error)

    thread = threading.Thread(target=close_from_wrong_thread)
    thread.start()
    thread.join()
    try:
        assert len(errors) == 1
        assert isinstance(errors[0], LocalProtocolError)
        assert statements == []
        _enter_scope(owner, "read")
        assert store.load_account() is None
    finally:
        bootstrap.close()


def test_reentrant_close_waits_for_owner_scope_to_exit(tmp_path: Path) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statements)
    bootstrap.open_matrix_store(SqliteStore)
    owner = _owner(bootstrap)
    with owner.read():
        before_close = tuple(statements)
        with pytest.raises(LocalProtocolError):
            bootstrap.close()
        assert tuple(statements) == before_close
        assert not owner.database.is_closed()
        with pytest.raises(LocalProtocolError):
            StableFileLock(bootstrap.database_path)

    bootstrap.close()
    replacement = StableFileLock(bootstrap.database_path)
    replacement.close()


EXPECTED_METHOD_SCOPES = {
    **{("MatrixStore", name): "read" for name in """
            _get_account _load_persisted_sessions
            _load_persisted_inbound_group_sessions load_device_keys load_encrypted_rooms
            load_outgoing_key_requests load_sync_token
            load_sync_recovery has_real_recovery_gap load_sliding_window_tokens
        """.split()},
    **{("SqliteStore", name): "read" for name in """
            is_device_verified is_device_blacklisted is_device_ignored load_device_keys
        """.split()},
    **{("MatrixStore", name): "write" for name in """
            upgrade_to_v2 upgrade_to_v3 upgrade_to_v5 upgrade_to_v6 upgrade_to_v7
            upgrade_to_v8 upgrade_to_v9 upgrade_to_v10
            _repair_v10_recovery_abandonments save_account load_sessions save_session
            load_inbound_group_sessions save_inbound_group_session save_device_keys
            add_outgoing_key_request
            remove_outgoing_key_request save_encrypted_rooms save_sync_token
            _clear_sync_recovery save_recovery clear_recovery_abandonment
            accept_recovery_event save_sliding_window_tokens
            forget_sliding_window_token finish_recovery delete_encrypted_room
        """.split()},
    **{("SqliteStore", name): "write" for name in """
            verify_device unverify_device blacklist_device unblacklist_device
            ignore_device unignore_device ignore_devices
        """.split()},
}


def _database_tree() -> ast.Module:
    return ast.parse((ROOT / "src/nio/store/database.py").read_text())


def test_decorated_store_method_read_write_inventory_is_complete() -> None:
    actual: dict[tuple[str, str], str] = {}
    for node in _database_tree().body:
        if not isinstance(node, ast.ClassDef) or node.name not in {
            "MatrixStore",
            "SqliteStore",
        }:
            continue
        for method in node.body:
            if not isinstance(method, ast.FunctionDef):
                continue
            decorators = {ast.unparse(value) for value in method.decorator_list}
            if "use_database" in decorators:
                actual[node.name, method.name] = "read"
            if "use_database_atomic" in decorators:
                actual[node.name, method.name] = "write"
    assert actual == EXPECTED_METHOD_SCOPES


def _decorated_function_contexts(function: ast.FunctionDef) -> list[tuple[str, ...]]:
    contexts: list[tuple[str, ...]] = []

    def visit(node: ast.AST, enclosing: tuple[str, ...]) -> None:
        if isinstance(node, ast.With):
            names = tuple(
                call.func.attr
                for item in node.items
                for call in ast.walk(item.context_expr)
                if isinstance(call, ast.Call) and isinstance(call.func, ast.Attribute)
            )
            enclosing += names
        if isinstance(node, ast.Return) and any(
            isinstance(call, ast.Call)
            and isinstance(call.func, ast.Name)
            and call.func.id == "fn"
            for call in ast.walk(node)
        ):
            contexts.append(enclosing)
        for child in ast.iter_child_nodes(node):
            visit(child, enclosing)

    visit(function, ())
    return contexts


def _contains_in_order(values: tuple[str, ...], expected: tuple[str, ...]) -> bool:
    position = 0
    for value in values:
        if value == expected[position]:
            position += 1
            if position == len(expected):
                return True
    return False


def test_store_decorators_enclose_binding_and_sql_in_owner_scope() -> None:
    functions = {
        node.name: node
        for node in _database_tree().body
        if isinstance(node, ast.FunctionDef)
        and node.name in {"use_database", "use_database_atomic"}
    }

    expected = {
        "use_database": ("read", "bind_ctx"),
        "use_database_atomic": ("e2ee_write", "bind_ctx", "atomic"),
    }
    for name, chain in expected.items():
        contexts = _decorated_function_contexts(functions[name])
        assert any(_contains_in_order(context, chain) for context in contexts), (
            f"{name} never calls fn while {chain!r} are all enclosing contexts; "
            f"found {contexts!r}"
        )


def test_enclosure_check_rejects_a_sequential_owner_scope() -> None:
    broken = ast.parse("""
def use_database(fn):
    def inner(self):
        with self.owner.read():
            pass
        with self.database.bind_ctx(self.models):
            return fn(self)
    return inner
""").body[0]
    assert isinstance(broken, ast.FunctionDef)
    contexts = _decorated_function_contexts(broken)
    assert not any(
        _contains_in_order(context, ("read", "bind_ctx")) for context in contexts
    )


def test_borrowed_store_has_no_production_database_close_path() -> None:
    classes = {
        node.name: node
        for node in _database_tree().body
        if isinstance(node, ast.ClassDef)
        and node.name in {"MatrixStore", "SqliteStore"}
    }
    assert not any(
        isinstance(node, ast.FunctionDef) and node.name == "close"
        for class_node in classes.values()
        for node in class_node.body
    )
    borrowed_init = next(
        node
        for node in classes["MatrixStore"].body
        if isinstance(node, ast.FunctionDef)
        and node.name == "_post_init_ingestion_store"
    )
    assert not any(
        isinstance(node, ast.Call)
        and isinstance(node.func, ast.Attribute)
        and node.func.attr == "close"
        for node in ast.walk(borrowed_init)
    )


def test_production_database_reach_through_stays_in_exact_allowlist() -> None:
    allowlist = {
        Path("src/nio/store/_ingestion_store_owner.py"),
        Path("src/nio/store/_sync_journal_preflight.py"),
        Path("src/nio/store/_sync_journal.py"),
        Path("src/nio/store/_sync_journal_rows.py"),
        Path("src/nio/store/database.py"),
    }
    violations: list[str] = []
    for path in (ROOT / "src/nio").rglob("*.py"):
        relative = path.relative_to(ROOT)
        if relative in allowlist:
            continue
        for node in ast.walk(ast.parse(path.read_text())):
            forbidden = isinstance(node, ast.Attribute) and node.attr == "database"
            forbidden_call = (
                isinstance(node, ast.Call)
                and isinstance(node.func, ast.Attribute)
                and node.func.attr
                in {"cursor", "connection", "atomic", "commit", "rollback"}
            )
            if forbidden or forbidden_call:
                violations.append(f"{relative}:{getattr(node, 'lineno', 0)}")
    assert violations == []
