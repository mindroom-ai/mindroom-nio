from __future__ import annotations

import ast
import builtins
import os
import shutil
import sqlite3
import sys
import threading
import time
from contextlib import contextmanager
from pathlib import Path
from uuid import uuid4

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
from nio.store.sync_journal import open_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
BOB_ID = "@bob:example.org"
BOB_DEVICE = "BOBDEVICE"
BOB_CURVE = "T9tOKF+TShsn6mk1zisW2IBsBbTtzDNvw99RBFMJOgI"
BOB_ONETIME = "6QlQw3mGUveS735k/JDaviuoaih5eEi6S1J65iHjfgU"
SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
ROOT = Path(__file__).parents[2]


def _open(path: Path, statements: list[str] | None = None, *, timeout: int = 2_000):
    return open_ingestion_store(
        path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
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
            _get_account load_device_keys load_encrypted_rooms
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
