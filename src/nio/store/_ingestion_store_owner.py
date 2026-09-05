from __future__ import annotations

import os
import sqlite3
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from functools import partial
from pathlib import Path
from types import TracebackType
from typing import TYPE_CHECKING
from uuid import UUID

from peewee import SqliteDatabase

from ..exceptions import LocalProtocolError

if TYPE_CHECKING:
    from collections.abc import Callable, Iterator
    from contextlib import AbstractContextManager


FileIdentity = tuple[int, int]


@dataclass(frozen=True)
class _FreshPathWitness:
    category: str
    identity: FileIdentity | None


class StableFileLock:
    """A mode-aware lifetime lease over a stable sidecar and database inode."""

    def __init__(
        self,
        database_path: Path,
        *,
        exclusive: bool = True,
        require_database: bool = False,
        require_regular_path: bool = False,
    ) -> None:
        try:
            import fcntl
        except ImportError as error:
            raise LocalProtocolError(
                "store filesystem ownership requires fcntl support"
            ) from error
        self._fcntl = fcntl
        self.owner_pid = os.getpid()
        self.database_path = Path(os.path.abspath(database_path))
        self.exclusive = exclusive
        self.require_database = require_database
        self.require_regular_path = require_regular_path
        self._database_fd = -1
        canonical_path = Path(os.path.realpath(self.database_path))
        self.path = Path(f"{canonical_path}.ingest.lock")
        if require_database:
            try:
                self._database_fd = os.open(
                    self.database_path,
                    os.O_RDWR
                    | getattr(os, "O_CLOEXEC", 0)
                    | getattr(os, "O_NOFOLLOW", 0),
                )
            except OSError as error:
                raise LocalProtocolError(
                    "configured adoption requires an existing store"
                ) from error
            opened = os.fstat(self._database_fd)
            if (
                opened.st_size == 0
                or not stat.S_ISREG(opened.st_mode)
                or opened.st_nlink != 1
            ):
                os.close(self._database_fd)
                self._database_fd = -1
                raise LocalProtocolError(
                    "configured adoption requires a populated singly linked store"
                )
            os.set_inheritable(self._database_fd, False)
        try:
            self._fd = os.open(
                self.path,
                os.O_CREAT | os.O_RDWR | getattr(os, "O_CLOEXEC", 0),
                0o600,
            )
        except BaseException:
            if self._database_fd >= 0:
                os.close(self._database_fd)
                self._database_fd = -1
            raise
        os.set_inheritable(self._fd, False)
        operation = self._fcntl.LOCK_EX if exclusive else self._fcntl.LOCK_SH
        try:
            self._fcntl.flock(self._fd, operation | self._fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(self._fd)
            self._fd = -1
            if self._database_fd >= 0:
                os.close(self._database_fd)
                self._database_fd = -1
            raise LocalProtocolError(
                f"store lifetime lease is already held for {database_path}"
            ) from error
        locked = os.fstat(self._fd)
        self.identity = (locked.st_dev, locked.st_ino)
        self.database_identity: FileIdentity | None = None
        self._database_link_count: int | None = None
        if self._database_fd >= 0:
            operation = self._fcntl.LOCK_EX if exclusive else self._fcntl.LOCK_SH
            try:
                self._fcntl.flock(self._database_fd, operation | self._fcntl.LOCK_NB)
            except BlockingIOError as error:
                os.close(self._database_fd)
                self._database_fd = -1
                self.close()
                raise LocalProtocolError(
                    f"store lifetime lease is already held for {database_path}"
                ) from error
            opened = os.fstat(self._database_fd)
            if (require_database or require_regular_path) and opened.st_nlink != 1:
                self.close()
                raise LocalProtocolError(
                    "owned ingestion requires a singly linked database file"
                )
            self.database_identity = (opened.st_dev, opened.st_ino)
            self._database_link_count = opened.st_nlink
            self.assert_identity()
        elif self.database_path.exists():
            self.claim_database()

    def claim_database(self) -> None:
        self.assert_process_owner()
        if self._fd < 0:
            raise LocalProtocolError("store lifetime lease is closed")
        if self._database_fd >= 0:
            raise LocalProtocolError("database lifetime lease is already claimed")
        try:
            descriptor = os.open(
                self.database_path,
                os.O_RDWR
                | getattr(os, "O_CLOEXEC", 0)
                | (getattr(os, "O_NOFOLLOW", 0) if self.require_regular_path else 0),
            )
        except FileNotFoundError as error:
            raise LocalProtocolError(
                "store database identity is no longer present"
            ) from error
        except OSError as error:
            if not self.require_regular_path:
                raise
            raise LocalProtocolError(
                "owned ingestion requires a regular database path"
            ) from error
        os.set_inheritable(descriptor, False)
        operation = self._fcntl.LOCK_EX if self.exclusive else self._fcntl.LOCK_SH
        try:
            self._fcntl.flock(descriptor, operation | self._fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(descriptor)
            raise LocalProtocolError(
                f"store lifetime lease is already held for {self.database_path}"
            ) from error
        locked = os.fstat(descriptor)
        if self.require_regular_path and locked.st_nlink != 1:
            self._fcntl.flock(descriptor, self._fcntl.LOCK_UN)
            os.close(descriptor)
            raise LocalProtocolError(
                "owned ingestion requires a singly linked database file"
            )
        self._database_fd = descriptor
        self.database_identity = (locked.st_dev, locked.st_ino)
        self._database_link_count = locked.st_nlink
        try:
            self.assert_identity()
        except BaseException:
            self._fcntl.flock(descriptor, self._fcntl.LOCK_UN)
            os.close(descriptor)
            self._database_fd = -1
            self.database_identity = None
            self._database_link_count = None
            raise

    def assert_process_owner(self) -> None:
        if os.getpid() != self.owner_pid:
            raise LocalProtocolError("ownership belongs to the acquiring process")

    def assert_identity(self) -> None:
        self.assert_process_owner()
        if self._fd < 0:
            raise LocalProtocolError("store lifetime lease is closed")
        try:
            current = os.stat(self.path)
        except FileNotFoundError as error:
            raise LocalProtocolError(
                "ingestion lock file identity is no longer present"
            ) from error
        if (current.st_dev, current.st_ino) != self.identity:
            raise LocalProtocolError(
                "ingestion lock file identity changed after lock acquisition"
            )
        if self._database_fd < 0 or self.database_identity is None:
            raise LocalProtocolError("database lifetime lease is not claimed")
        try:
            database = (
                os.lstat(self.database_path)
                if self.require_regular_path
                else os.stat(self.database_path)
            )
        except FileNotFoundError as error:
            raise LocalProtocolError(
                "store database identity is no longer present"
            ) from error
        descriptor = os.fstat(self._database_fd)
        if self.require_regular_path and (
            not stat.S_ISREG(database.st_mode)
            or database.st_nlink != 1
            or descriptor.st_nlink != 1
        ):
            raise LocalProtocolError(
                "owned ingestion regular database path acquired a hard link"
            )
        identities = (
            (database.st_dev, database.st_ino),
            (descriptor.st_dev, descriptor.st_ino),
        )
        if identities != (self.database_identity, self.database_identity):
            raise LocalProtocolError("store database file identity changed")
        if (
            self._database_link_count is None
            or database.st_nlink != self._database_link_count
            or descriptor.st_nlink != self._database_link_count
        ):
            raise LocalProtocolError("store database hard link count changed")
        if self.require_database and descriptor.st_nlink != 1:
            raise LocalProtocolError(
                "configured database acquired an unsupported hard link"
            )

    def close(self) -> None:
        self.assert_process_owner()
        if self._fd < 0:
            return
        if self._database_fd >= 0:
            self._fcntl.flock(self._database_fd, self._fcntl.LOCK_UN)
            os.close(self._database_fd)
            self._database_fd = -1
            self._database_link_count = None
        self._fcntl.flock(self._fd, self._fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = -1

    def __del__(self) -> None:
        database_fd = getattr(self, "_database_fd", -1)
        sidecar_fd = getattr(self, "_fd", -1)
        same_process = os.getpid() == getattr(self, "owner_pid", None)
        for descriptor in (database_fd, sidecar_fd):
            if descriptor < 0:
                continue
            try:
                if same_process:
                    self._fcntl.flock(descriptor, self._fcntl.LOCK_UN)
                os.close(descriptor)
            except OSError:
                pass
        self._database_fd = -1
        self._fd = -1

    def __enter__(self) -> StableFileLock:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


# If sqlite refuses a final physical close because cyclic collection ran on a
# foreign thread, exclusion is safer than unlocking a database that may still
# have live C-level state. The process closes these descriptors at exit.
_DEFERRED_LIFETIME_LEASES: list[StableFileLock] = []
_ACTIVE_LIFETIME_LEASES: dict[int, StableFileLock] = {}
_DEFERRED_LIFETIME_LEASES_LOCK = threading.Lock()


class _LifetimeLeasedCursor(sqlite3.Cursor):
    """Cursor that rechecks path and inode ownership at every SQLite I/O."""

    def _assert_lifetime_lease(self) -> None:
        connection = self.connection
        if not isinstance(connection, _LifetimeLeasedConnection):
            raise LocalProtocolError("ordinary cursor has no lifetime lease")
        connection._assert_lifetime_lease()

    def execute(self, sql: str, parameters=()):  # type: ignore[no-untyped-def]
        self._assert_lifetime_lease()
        return super().execute(sql, parameters)

    def executemany(self, sql: str, parameters):  # type: ignore[no-untyped-def]
        self._assert_lifetime_lease()
        return super().executemany(sql, parameters)

    def executescript(self, sql_script: str):
        self._assert_lifetime_lease()
        return super().executescript(sql_script)

    def fetchone(self):
        self._assert_lifetime_lease()
        return super().fetchone()

    def fetchmany(self, size: int | None = None):
        self._assert_lifetime_lease()
        if size is None:
            return super().fetchmany()
        return super().fetchmany(size)

    def fetchall(self):
        self._assert_lifetime_lease()
        return super().fetchall()

    def __next__(self):
        self._assert_lifetime_lease()
        return super().__next__()


class _LifetimeLeasedConnection(sqlite3.Connection):
    """SQLite connection whose kernel lease has the same physical lifetime."""

    def __init__(self, *args, lease: StableFileLock, **kwargs) -> None:  # type: ignore[no-untyped-def]
        self._lifetime_lease: StableFileLock | None = lease
        self._lifetime_thread = threading.get_ident()
        super().__init__(*args, **kwargs)
        with _DEFERRED_LIFETIME_LEASES_LOCK:
            _ACTIVE_LIFETIME_LEASES[id(self)] = lease

    def _defer_lifetime_lease(self) -> None:
        lease = self._lifetime_lease
        if lease is None:
            return
        with _DEFERRED_LIFETIME_LEASES_LOCK:
            _ACTIVE_LIFETIME_LEASES.pop(id(self), None)
            _DEFERRED_LIFETIME_LEASES.append(lease)
        self._lifetime_lease = None

    def _assert_lifetime_lease(self) -> None:
        lease = self._lifetime_lease
        if lease is None:
            raise LocalProtocolError("store lifetime lease is closed")
        lease.assert_identity()

    def cursor(self, factory=None):  # type: ignore[no-untyped-def]
        if factory is not None:
            raise LocalProtocolError("custom ordinary SQLite cursors are unsupported")
        return super().cursor(_LifetimeLeasedCursor)

    def execute(self, *args: object, **kwargs: object):
        return self.cursor().execute(*args, **kwargs)

    def executemany(self, *args: object, **kwargs: object):
        return self.cursor().executemany(*args, **kwargs)

    def executescript(self, *args: object, **kwargs: object):
        return self.cursor().executescript(*args, **kwargs)

    def commit(self) -> None:
        self._assert_lifetime_lease()
        super().commit()

    def rollback(self) -> None:
        self._assert_lifetime_lease()
        super().rollback()

    def __enter__(self):
        self._assert_lifetime_lease()
        return super().__enter__()

    def __exit__(
        self,
        exc_type: type[BaseException] | None,
        exc_value: BaseException | None,
        traceback: TracebackType | None,
    ):
        self._assert_lifetime_lease()
        return super().__exit__(exc_type, exc_value, traceback)

    def backup(self, *args, **kwargs):  # type: ignore[no-untyped-def]
        self._assert_lifetime_lease()
        return super().backup(*args, **kwargs)

    def blobopen(self, *args: object, **kwargs: object):
        self._assert_lifetime_lease()
        raise LocalProtocolError("ordinary SQLite blob handles are unsupported")

    def serialize(self, *, name: str = "main"):
        self._assert_lifetime_lease()
        return super().serialize(name=name)

    def deserialize(self, data, *, name: str = "main"):  # type: ignore[no-untyped-def]
        self._assert_lifetime_lease()
        raise LocalProtocolError("ordinary SQLite deserialization is unsupported")

    def iterdump(self, *args: object, **kwargs: object):
        self._assert_lifetime_lease()
        raise LocalProtocolError("ordinary SQLite dump iterators are unsupported")

    def close(self) -> None:
        lease = self._lifetime_lease
        if lease is None:
            return
        super().close()
        lease.close()
        with _DEFERRED_LIFETIME_LEASES_LOCK:
            _ACTIVE_LIFETIME_LEASES.pop(id(self), None)
        self._lifetime_lease = None

    def __del__(self) -> None:
        if threading.get_ident() != getattr(self, "_lifetime_thread", None):
            self._defer_lifetime_lease()
            return
        try:
            self.close()
        except sqlite3.ProgrammingError:
            self._defer_lifetime_lease()
        except (LocalProtocolError, sqlite3.Error):
            pass


class LeasedSqliteDatabase(SqliteDatabase):
    """Ordinary MatrixStore database with one shared lease per connection."""

    def __init__(self, *args: object, **kwargs: object) -> None:
        super().__init__(*args, **kwargs)
        self._lifetime_pid = os.getpid()
        self._lifetime_identity: FileIdentity | None = None
        self._lifetime_identity_lock = threading.Lock()

    def _remember_lifetime_identity(self, identity: FileIdentity | None) -> None:
        if identity is None:
            raise LocalProtocolError("ordinary database lease has no identity")
        with self._lifetime_identity_lock:
            if self._lifetime_identity is None:
                self._lifetime_identity = identity
            elif self._lifetime_identity != identity:
                raise LocalProtocolError("ordinary database file identity changed")

    def _connect(self) -> sqlite3.Connection:
        if os.getpid() != self._lifetime_pid:
            raise LocalProtocolError(
                "ordinary store database belongs to the constructing process"
            )
        lease = StableFileLock(Path(self.database), exclusive=False)
        try:
            if lease.database_identity is None:
                if self._lifetime_identity is not None:
                    raise LocalProtocolError("ordinary database file identity changed")
                try:
                    descriptor = os.open(
                        lease.database_path,
                        os.O_CREAT | os.O_EXCL | os.O_RDWR,
                        0o600,
                    )
                except FileExistsError:
                    pass
                else:
                    os.close(descriptor)
                lease.claim_database()
            self._remember_lifetime_identity(lease.database_identity)
            parameters = dict(self.connect_params)
            parameters["factory"] = partial(_LifetimeLeasedConnection, lease=lease)
            connection = sqlite3.connect(
                self.database,
                timeout=self._timeout,
                isolation_level=None,
                **parameters,
            )
            try:
                self._add_conn_hooks(connection)
                marker = connection.execute(
                    "SELECT 1 FROM sqlite_master WHERE type = 'table' "
                    "AND name = 'NioIngestMeta' COLLATE NOCASE"
                ).fetchone()
                if marker is not None:
                    raise LocalProtocolError(
                        "this database is owned by ingestion v1; direct legacy "
                        "store access is unsupported"
                    )
            except BaseException:
                connection.close()
                raise
            return connection
        except BaseException:
            lease.close()
            raise


class IngestionStoreOwner:
    """Lifetime and transaction owner for one ordinary Peewee database."""

    def __init__(
        self,
        path: Path,
        sqlite_busy_timeout_ms: int,
        statement_observer: Callable[[str], None] | None,
        *,
        require_nonempty: bool = False,
        require_regular_path: bool = False,
    ) -> None:
        self.path = path
        self._pid = os.getpid()
        self._thread = threading.get_ident()
        self._lock = StableFileLock(
            path,
            require_database=require_nonempty,
            require_regular_path=require_regular_path,
        )
        self._state = "bootstrap"
        self._depth = 0
        self._outer_scope: str | None = None
        self._bootstrap_used = False
        self._account_id: str | None = None
        self._writer_epoch: UUID | None = None

        try:
            try:
                before = os.lstat(path)
            except FileNotFoundError:
                before = None
            if before is not None and not stat.S_ISREG(before.st_mode):
                raise LocalProtocolError("ingestion database must be a regular file")
            if require_nonempty and (before is None or before.st_size == 0):
                raise LocalProtocolError(
                    "configured adoption requires a populated regular store"
                )
            category = (
                "ABSENT"
                if before is None
                else "ZERO_LENGTH" if before.st_size == 0 else "NONEMPTY"
            )
            if before is None:
                try:
                    fd = os.open(path, os.O_CREAT | os.O_EXCL | os.O_RDWR, 0o600)
                except FileExistsError as error:
                    raise LocalProtocolError(
                        "ingestion database appeared while opening"
                    ) from error
                try:
                    claimed = os.fstat(fd)
                finally:
                    os.close(fd)
                prior_identity = (claimed.st_dev, claimed.st_ino)
            else:
                prior_identity = (before.st_dev, before.st_ino)
            if self._lock.database_identity is None:
                self._lock.claim_database()
            self._fresh_witness = _FreshPathWitness(category, prior_identity)

            self.database = SqliteDatabase(
                str(path),
                thread_safe=False,
                autoconnect=False,
                timeout=sqlite_busy_timeout_ms / 1000,
            )
            self.database.connect()
            connection = self.database.connection()
            connection.row_factory = sqlite3.Row
            if statement_observer is not None:
                connection.set_trace_callback(statement_observer)
            opened = os.lstat(path)
            if not stat.S_ISREG(opened.st_mode):
                raise LocalProtocolError(
                    "ingestion database must remain a regular file"
                )
            self._file_identity = (opened.st_dev, opened.st_ino)
            if prior_identity != self._file_identity:
                raise LocalProtocolError(
                    "ingestion database identity changed while opening"
                )
            if category in {"ABSENT", "ZERO_LENGTH"} and opened.st_size != 0:
                raise LocalProtocolError(
                    "fresh ingestion database changed before initialization"
                )
        except BaseException:
            if hasattr(self, "database") and not self.database.is_closed():
                self.database.close()
            self._lock.close()
            raise

    @property
    def is_fresh(self) -> bool:
        return self._fresh_witness.category in {"ABSENT", "ZERO_LENGTH"}

    def _assert_thread(self) -> None:
        if os.getpid() != self._pid:
            raise LocalProtocolError("store owner belongs to the opening process")
        if threading.get_ident() != self._thread:
            raise LocalProtocolError("store owner belongs to the opening thread")

    def _assert_identity(self, *, bootstrap: bool = False) -> None:
        self._assert_thread()
        if self._state not in ({"bootstrap"} if bootstrap else {"active"}):
            raise LocalProtocolError("ingestion store owner is not active")
        self._lock.assert_identity()
        try:
            current = os.stat(self.path)
        except FileNotFoundError as error:
            raise LocalProtocolError(
                "ingestion database file identity is no longer present"
            ) from error
        if (current.st_dev, current.st_ino) != self._file_identity:
            raise LocalProtocolError("ingestion database file identity changed")

    def _epoch_fence(self, *, write: bool) -> None:
        assert self._account_id is not None
        assert self._writer_epoch is not None
        if write:
            cursor = self.database.execute_sql(
                "UPDATE NioIngestMeta SET writer_epoch = writer_epoch "
                "WHERE account_id = ? AND writer_epoch = ?",
                (self._account_id, str(self._writer_epoch)),
            )
            valid = cursor.rowcount == 1
        else:
            row = self.database.execute_sql(
                "SELECT writer_epoch FROM NioIngestMeta WHERE account_id = ?",
                (self._account_id,),
            ).fetchone()
            valid = row is not None and row[0] == str(self._writer_epoch)
        if not valid:
            raise LocalProtocolError("ingestion store writer_epoch is stale")

    @contextmanager
    def bootstrap_write(self) -> Iterator[None]:
        self._assert_identity(bootstrap=True)
        if self._bootstrap_used or self._depth or self.database.transaction_depth():
            raise LocalProtocolError("ingestion bootstrap transaction is unavailable")
        self._bootstrap_used = True
        with self.database.atomic("IMMEDIATE"):
            self._depth = 1
            self._outer_scope = "bootstrap"
            try:
                yield
            finally:
                self._depth = 0
                self._outer_scope = None

    def activate(self, account_id: str, writer_epoch: UUID) -> None:
        self._assert_identity(bootstrap=True)
        if not self._bootstrap_used or self._depth:
            raise LocalProtocolError("ingestion store owner cannot activate")
        self._account_id = account_id
        self._writer_epoch = writer_epoch
        self._state = "active"

    def _handoff_writer_epoch(self, old_epoch: UUID, new_epoch: UUID) -> None:
        self._assert_identity()
        if type(old_epoch) is not UUID or type(new_epoch) is not UUID:
            raise TypeError("writer epochs must be UUID")
        if self._depth or self.database.transaction_depth():
            raise LocalProtocolError("writer epoch handoff requires a committed state")
        if self._writer_epoch != old_epoch or new_epoch == old_epoch:
            raise LocalProtocolError("writer epoch handoff is invalid")
        self._writer_epoch = new_epoch

    @contextmanager
    def read(self) -> Iterator[None]:
        self._assert_thread()
        if self._depth:
            if self._outer_scope == "bootstrap":
                raise LocalProtocolError(
                    "normal owner scope is unavailable at bootstrap"
                )
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return
        self._assert_identity()
        if self.database.transaction_depth():
            raise LocalProtocolError("ambient transaction is not owner-authorized")
        self._epoch_fence(write=False)
        self._depth = 1
        self._outer_scope = "read"
        try:
            yield
        finally:
            self._depth = 0
            self._outer_scope = None

    @contextmanager
    def _write(self, *, epoch_cas: bool, scope: str) -> Iterator[None]:
        self._assert_thread()
        if self._depth:
            if self._outer_scope in {"bootstrap", "read"}:
                raise LocalProtocolError("read owner scope cannot escalate to a write")
            self._depth += 1
            try:
                yield
            finally:
                self._depth -= 1
            return
        self._assert_identity()
        if self.database.transaction_depth():
            raise LocalProtocolError("ambient transaction is not owner-authorized")
        with self.database.atomic("IMMEDIATE"):
            self._epoch_fence(write=epoch_cas)
            self._depth = 1
            self._outer_scope = scope
            try:
                yield
            finally:
                self._depth = 0
                self._outer_scope = None

    def journal_write(self) -> AbstractContextManager[None]:
        return self._write(epoch_cas=False, scope="journal_write")

    def e2ee_write(self) -> AbstractContextManager[None]:
        return self._write(epoch_cas=True, scope="e2ee_write")

    def prepare_close(self) -> None:
        self._assert_thread()
        if self._depth:
            raise LocalProtocolError("cannot close during an owner operation")

    def close(self) -> None:
        self.prepare_close()
        if self._state == "closed":
            return
        if self._state not in {"active", "closing", "bootstrap"}:
            raise LocalProtocolError("ingestion store owner cannot close")
        identity_error: BaseException | None = None
        try:
            self._lock.assert_identity()
        except BaseException as error:
            identity_error = error
        self._state = "closing"
        try:
            if not self.database.is_closed():
                self.database.close()
        except BaseException:
            # A failed physical close may leave SQLite or a transaction live.
            # Keep exclusion for a same-owner retry even when the pathname is
            # stale; releasing here would permit a second writer to overlap it.
            raise
        self._lock.close()
        self._state = "closed"
        if identity_error is not None:
            raise identity_error
