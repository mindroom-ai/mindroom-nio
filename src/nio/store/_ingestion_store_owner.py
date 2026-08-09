from __future__ import annotations

import fcntl
import os
import sqlite3
import stat
import threading
from contextlib import contextmanager
from dataclasses import dataclass
from pathlib import Path
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
    def __init__(self, database_path: Path) -> None:
        self.owner_pid = os.getpid()
        self.path = Path(f"{database_path}.ingest.lock")
        self._fd = os.open(self.path, os.O_CREAT | os.O_RDWR, 0o600)
        try:
            fcntl.flock(self._fd, fcntl.LOCK_EX | fcntl.LOCK_NB)
        except BlockingIOError as error:
            os.close(self._fd)
            self._fd = -1
            raise LocalProtocolError(
                f"ingestion writer lock is already held for {database_path}"
            ) from error
        locked = os.fstat(self._fd)
        self.identity = (locked.st_dev, locked.st_ino)

    def assert_process_owner(self) -> None:
        if os.getpid() != self.owner_pid:
            raise LocalProtocolError("ownership belongs to the acquiring process")

    def assert_identity(self) -> None:
        self.assert_process_owner()
        if self._fd < 0:
            raise LocalProtocolError("ingestion writer lock is closed")
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

    def close(self) -> None:
        self.assert_process_owner()
        if self._fd < 0:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = -1

    def __enter__(self) -> StableFileLock:
        return self

    def __exit__(self, *_args: object) -> None:
        self.close()


class IngestionStoreOwner:
    """Lifetime and transaction owner for one ordinary Peewee database."""

    def __init__(
        self,
        path: Path,
        sqlite_busy_timeout_ms: int,
        statement_observer: Callable[[str], None] | None,
    ) -> None:
        self.path = path
        self._pid = os.getpid()
        self._thread = threading.get_ident()
        self._lock = StableFileLock(path)
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
        self._lock.assert_identity()
        self._state = "closing"
        if not self.database.is_closed():
            self.database.close()
        self._lock.close()
        self._state = "closed"
