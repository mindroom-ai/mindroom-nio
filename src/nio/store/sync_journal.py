from __future__ import annotations

import fcntl
import hashlib
import hmac
import json
import os
import sqlite3
import threading
import time
from pathlib import Path
from typing import TYPE_CHECKING, Protocol
from uuid import UUID, uuid4

from Crypto.Cipher import AES
from peewee import SqliteDatabase
from playhouse.sqliteq import SqliteQueueDatabase

from ..exceptions import LocalProtocolError
from ..ingest.errors import (
    FreshIngestionRequired,
    JournalConflictError,
    JournalIntegrityError,
)
from ..ingest.model import (
    BatchRef,
    ConsumerBinding,
    ConsumerBootstrap,
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RoomHydrationStatus,
    SystemOrigin,
    SystemOriginKind,
    SyncBatch,
    TransportKind,
)
from ..ingest.serialization import (
    _batch_from_payload,
    _boundary_from_dict,
    _boundary_to_dict,
    _canonical_json,
    _canonical_room_snapshot_payload,
    _load_json_object,
    _loss_id,
    _origin_from_dict,
    _origin_to_dict,
    _record_from_dict,
    _record_to_dict,
    _room_snapshot_from_payload,
    canonical_batch_payload,
)
from ..ingest.state import (
    AckOutcome,
    CommitResult,
    JournalTransition,
    LaneStatus,
    OwnerView,
    ReadyRecord,
    ReleasePhase,
    RoomAggregate,
    RoomLane,
    RoomState,
    SourceState,
    StagedFrame,
)
from .sync_journal_schema import (
    INGESTION_TABLES,
    META_TABLE_SQL,
    SCHEMA_SQL,
    SCHEMA_VERSION,
)

if TYPE_CHECKING:
    from collections.abc import Callable

    from .database import MatrixStore


_ROW_KEY_DOMAIN = b"mindroom-nio:ingest-row-key:v1\0"
_ROW_AAD_DOMAIN = b"mindroom-nio:ingest-row-aad:v1\0"
_ROW_NONCE_SIZE = 12
_ROW_TAG_SIZE = 16


def _length_frame(value: bytes) -> bytes:
    return len(value).to_bytes(4, "big") + value


class EncryptedRowCodec:
    """AES-GCM row codec bound to the complete persistent row identity."""

    def __init__(self, pickle_key: str, account_id: str, stream_id: UUID) -> None:
        if type(pickle_key) is not str:
            raise TypeError("pickle_key must be str")
        if type(account_id) is not str:
            raise TypeError("account_id must be str")
        if type(stream_id) is not UUID:
            raise TypeError("stream_id must be UUID")
        self._key = hashlib.sha256(
            _ROW_KEY_DOMAIN + pickle_key.encode("utf-8")
        ).digest()
        self.account_id = account_id
        self.stream_id = stream_id

    @staticmethod
    def _primary_key_payload(primary_key: tuple[str | int | UUID, ...]) -> bytes:
        if type(primary_key) is not tuple:
            raise TypeError("primary_key must be a tuple")
        values: list[str | int] = []
        for value in primary_key:
            if type(value) is UUID:
                values.append(str(value))
            elif type(value) in (str, int):
                values.append(value)
            else:
                raise TypeError("primary_key values must be str, int, or UUID")
        return json.dumps(values, separators=(",", ":"), ensure_ascii=False).encode()

    def _aad(
        self,
        table: str,
        primary_key: tuple[str | int | UUID, ...],
        digest: bytes,
    ) -> bytes:
        if type(table) is not str or not table:
            raise TypeError("table must be a nonempty str")
        if type(digest) is not bytes or len(digest) != hashlib.sha256().digest_size:
            raise TypeError("digest must be a SHA-256 bytes value")
        fields = (
            str(SCHEMA_VERSION).encode(),
            table.encode(),
            self.account_id.encode(),
            str(self.stream_id).encode(),
            self._primary_key_payload(primary_key),
            digest,
        )
        return _ROW_AAD_DOMAIN + b"".join(_length_frame(value) for value in fields)

    def encrypt(
        self,
        table: str,
        primary_key: tuple[str | int | UUID, ...],
        payload: bytes,
        digest: bytes | None = None,
    ) -> bytes:
        if type(payload) is not bytes:
            raise TypeError("payload must be bytes")
        actual_digest = hashlib.sha256(payload).digest()
        digest = actual_digest if digest is None else digest
        if not hmac.compare_digest(actual_digest, digest):
            raise JournalIntegrityError("payload digest does not match plaintext")
        nonce = os.urandom(_ROW_NONCE_SIZE)
        cipher = AES.new(self._key, AES.MODE_GCM, nonce=nonce, mac_len=_ROW_TAG_SIZE)
        cipher.update(self._aad(table, primary_key, digest))
        encrypted, tag = cipher.encrypt_and_digest(payload)
        return bytes((SCHEMA_VERSION,)) + nonce + tag + encrypted

    def decrypt(
        self,
        table: str,
        primary_key: tuple[str | int | UUID, ...],
        ciphertext: bytes,
        digest: bytes,
    ) -> bytes:
        minimum_size = 1 + _ROW_NONCE_SIZE + _ROW_TAG_SIZE
        if (
            type(ciphertext) is not bytes
            or len(ciphertext) < minimum_size
            or ciphertext[0] != SCHEMA_VERSION
        ):
            raise JournalIntegrityError("invalid encrypted ingestion row")
        nonce_end = 1 + _ROW_NONCE_SIZE
        tag_end = nonce_end + _ROW_TAG_SIZE
        cipher = AES.new(
            self._key,
            AES.MODE_GCM,
            nonce=ciphertext[1:nonce_end],
            mac_len=_ROW_TAG_SIZE,
        )
        try:
            cipher.update(self._aad(table, primary_key, digest))
            payload = cipher.decrypt_and_verify(
                ciphertext[tag_end:],
                ciphertext[nonce_end:tag_end],
            )
        except (TypeError, ValueError) as error:
            raise JournalIntegrityError(
                "ingestion row authentication failed"
            ) from error
        if not hmac.compare_digest(hashlib.sha256(payload).digest(), digest):
            raise JournalIntegrityError("ingestion row digest mismatch")
        return payload


class IngestionJournal(Protocol):
    """Durable compare-and-swap journal used by the ingestion owner."""

    def load_owner(self) -> OwnerView: ...

    def load_rooms(
        self,
        room_ids: frozenset[str],
    ) -> dict[str, RoomAggregate]: ...

    def load_ready_heads(self, limit: int) -> tuple[ReadyRecord, ...]: ...

    def commit(
        self,
        *,
        expected_revision: int,
        writer_epoch: UUID,
        transition: JournalTransition,
    ) -> CommitResult: ...

    def oldest_unacknowledged(self) -> SyncBatch | None: ...

    def acknowledge(self, ref: BatchRef) -> AckOutcome: ...


class _WriterLock:
    def __init__(self, database_path: Path) -> None:
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

    @property
    def active(self) -> bool:
        return self._fd >= 0

    def close(self) -> None:
        if self._fd < 0:
            return
        fcntl.flock(self._fd, fcntl.LOCK_UN)
        os.close(self._fd)
        self._fd = -1


def _database_path(database: str | os.PathLike[str] | SqliteDatabase) -> Path:
    if isinstance(database, SqliteQueueDatabase):
        raise LocalProtocolError(
            "SqliteQueueDatabase cannot provide atomic ingestion transactions"
        )
    if isinstance(database, SqliteDatabase):
        database = database.database
    path = os.fspath(database)
    if path == ":memory:":
        raise LocalProtocolError("the ingestion journal requires an on-disk database")
    return Path(path).resolve()


def _connect_read_only(database_path: Path) -> sqlite3.Connection:
    return sqlite3.connect(
        f"file:{database_path}?mode=ro",
        uri=True,
        isolation_level=None,
    )


def _has_v1_marker(database_path: Path) -> bool:
    if not database_path.exists() or database_path.stat().st_size == 0:
        return False
    with _connect_read_only(database_path) as connection:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master "
            "WHERE type = 'table' AND name = 'NioIngestMeta'"
        ).fetchone()
    return row is not None


def _has_any_table(database_path: Path) -> bool:
    if not database_path.exists() or database_path.stat().st_size == 0:
        return False
    with _connect_read_only(database_path) as connection:
        row = connection.execute(
            "SELECT 1 FROM sqlite_master WHERE type = 'table' LIMIT 1"
        ).fetchone()
    return row is not None


class SqliteIngestionJournal:
    """Direct-SQLite implementation of the version-1 ingestion journal."""

    def __init__(
        self,
        *,
        database_path: Path,
        account_id: str,
        device_id: str,
        pickle_key: str,
        connection: sqlite3.Connection,
        writer_lock: _WriterLock,
        writer_epoch: UUID,
        statement_observer: Callable[[str], None] | None,
        transition_statement_hook: Callable[[str], None] | None,
    ) -> None:
        self.database_path = database_path
        self.account_id = account_id
        self.device_id = device_id
        self.pickle_key = pickle_key
        self.connection = connection
        self.writer_epoch = writer_epoch
        self._writer_lock = writer_lock
        self._statement_observer = statement_observer
        self._transition_statement_hook = transition_statement_hook
        self._closed = False
        stat = os.stat(database_path)
        self._file_identity = (stat.st_dev, stat.st_ino)
        self._consumer_validated = False
        self._codec = EncryptedRowCodec(pickle_key, account_id, self.stream_id)
        self._ack_lock = threading.Lock()

    @classmethod
    def open(
        cls,
        database: str | os.PathLike[str] | SqliteDatabase,
        *,
        account_id: str,
        device_id: str,
        pickle_key: str = "",
        sqlite_busy_timeout_ms: int = 2_000,
        statement_observer: Callable[[str], None] | None = None,
        transition_statement_hook: Callable[[str], None] | None = None,
        schema_statement_hook: Callable[[str], None] | None = None,
    ) -> SqliteIngestionJournal:
        if type(account_id) is not str or not account_id:
            raise TypeError("account_id must be a nonempty str")
        if type(device_id) is not str or not device_id:
            raise TypeError("device_id must be a nonempty str")
        if type(pickle_key) is not str:
            raise TypeError("pickle_key must be str")
        if type(sqlite_busy_timeout_ms) is not int or sqlite_busy_timeout_ms <= 0:
            raise ValueError("sqlite_busy_timeout_ms must be positive")

        database_path = _database_path(database)
        database_path.parent.mkdir(parents=True, exist_ok=True)
        writer_lock = _WriterLock(database_path)
        connection: sqlite3.Connection | None = None
        try:
            marker_exists = _has_v1_marker(database_path)
            database_is_new = not _has_any_table(database_path)
            if not marker_exists and not database_is_new:
                raise FreshIngestionRequired(
                    "nonempty store has no ingestion-v1 marker; explicit fresh "
                    "initialization is required"
                )

            connection = sqlite3.connect(
                database_path,
                isolation_level=None,
                timeout=sqlite_busy_timeout_ms / 1000,
            )
            connection.row_factory = sqlite3.Row
            if statement_observer is not None:
                connection.set_trace_callback(statement_observer)
            connection.execute("PRAGMA foreign_keys = ON")
            connection.execute(f"PRAGMA busy_timeout = {sqlite_busy_timeout_ms}")

            if marker_exists:
                writer_epoch = cls._open_existing(
                    connection,
                    account_id=account_id,
                    device_id=device_id,
                )
            else:
                writer_epoch = uuid4()
                cls._create_fresh(
                    connection,
                    account_id=account_id,
                    device_id=device_id,
                    writer_epoch=writer_epoch,
                    statement_hook=schema_statement_hook,
                )

            connection.execute("PRAGMA journal_mode = WAL")
            connection.execute("PRAGMA synchronous = NORMAL")
            return cls(
                database_path=database_path,
                account_id=account_id,
                device_id=device_id,
                pickle_key=pickle_key,
                connection=connection,
                writer_lock=writer_lock,
                writer_epoch=writer_epoch,
                statement_observer=statement_observer,
                transition_statement_hook=transition_statement_hook,
            )
        except BaseException:
            if connection is not None:
                connection.close()
            writer_lock.close()
            raise

    @staticmethod
    def _create_fresh(
        connection: sqlite3.Connection,
        *,
        account_id: str,
        device_id: str,
        writer_epoch: UUID,
        statement_hook: Callable[[str], None] | None,
    ) -> None:
        try:
            connection.execute("BEGIN IMMEDIATE")
            connection.execute(META_TABLE_SQL)
            if statement_hook is not None:
                statement_hook("create_meta")
            connection.execute(
                """
                INSERT INTO NioIngestMeta (
                    account_id, device_id, schema_version, stream_id,
                    binding_operation_id, journal_generation,
                    consumer_generation, consumer_first_sequence,
                    baseline_rooms_sha256,
                    consumer_attached_revision, revision, writer_epoch,
                    next_source_epoch, next_ready_order, next_batch_sequence,
                    last_acked_sequence, last_acked_batch_id,
                    last_acked_sha256, created_at_ns
                ) VALUES (?, ?, ?, ?, ?, NULL, NULL, NULL, NULL, NULL, 0, ?, 1, 0, 1,
                          0, NULL, NULL, ?)
                """,
                (
                    account_id,
                    device_id,
                    SCHEMA_VERSION,
                    str(uuid4()),
                    str(uuid4()),
                    str(writer_epoch),
                    time.time_ns(),
                ),
            )
            if statement_hook is not None:
                statement_hook("insert_meta")
            for index, statement in enumerate(SCHEMA_SQL):
                connection.execute(statement.strip())
                if statement_hook is not None:
                    statement_hook(f"schema_{index}")
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise

    @staticmethod
    def _open_existing(
        connection: sqlite3.Connection,
        *,
        account_id: str,
        device_id: str,
    ) -> UUID:
        row = connection.execute(
            "SELECT account_id, device_id, schema_version, writer_epoch "
            "FROM NioIngestMeta"
        ).fetchone()
        if row is None:
            raise LocalProtocolError("ingestion-v1 marker row is missing")
        if row["account_id"] != account_id:
            raise LocalProtocolError("ingestion account_id does not match")
        if row["device_id"] != device_id:
            raise LocalProtocolError("ingestion device_id does not match")
        if row["schema_version"] != SCHEMA_VERSION:
            raise LocalProtocolError(
                f"unsupported ingestion schema_version {row['schema_version']}"
            )
        tables = {
            table["name"]
            for table in connection.execute(
                "SELECT name FROM sqlite_master "
                "WHERE type = 'table' AND name GLOB 'NioIngest*'"
            ).fetchall()
        }
        if tables != INGESTION_TABLES:
            raise LocalProtocolError("ingestion-v1 schema is incomplete or unexpected")

        old_epoch = row["writer_epoch"]
        writer_epoch = uuid4()
        try:
            connection.execute("BEGIN IMMEDIATE")
            cursor = connection.execute(
                "UPDATE NioIngestMeta SET writer_epoch = ? "
                "WHERE account_id = ? AND writer_epoch = ?",
                (str(writer_epoch), account_id, old_epoch),
            )
            if cursor.rowcount != 1:
                raise LocalProtocolError("persisted writer_epoch changed during open")
            connection.execute("COMMIT")
        except BaseException:
            if connection.in_transaction:
                connection.execute("ROLLBACK")
            raise
        return writer_epoch

    def _assert_open(self) -> None:
        if self._closed or not self._writer_lock.active:
            raise LocalProtocolError("ingestion journal is closed")
        try:
            stat = os.stat(self.database_path)
        except FileNotFoundError as error:
            raise LocalProtocolError(
                "ingestion database file identity is no longer present"
            ) from error
        if (stat.st_dev, stat.st_ino) != self._file_identity:
            raise LocalProtocolError(
                "ingestion database file identity changed after lock acquisition"
            )

    def set_transition_statement_hook(
        self,
        hook: Callable[[str], None] | None,
    ) -> None:
        self._assert_open()
        self._transition_statement_hook = hook

    def _transition_execute(
        self,
        label: str,
        statement: str,
        parameters: tuple[object, ...] = (),
    ) -> sqlite3.Cursor:
        cursor = self.connection.execute(statement, parameters)
        if self._transition_statement_hook is not None:
            self._transition_statement_hook(label)
        return cursor

    def _meta(self) -> sqlite3.Row:
        self._assert_open()
        row = self.connection.execute(
            "SELECT * FROM NioIngestMeta WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if row is None:
            raise LocalProtocolError("ingestion-v1 marker row disappeared")
        return row

    def load_owner(self) -> OwnerView:
        row = self._meta()
        journal_generation = row["journal_generation"]
        consumer_generation = row["consumer_generation"]
        binding_values = (
            journal_generation,
            consumer_generation,
            row["consumer_first_sequence"],
            row["baseline_rooms_sha256"],
            row["consumer_attached_revision"],
        )
        if any(value is None for value in binding_values) and any(
            value is not None for value in binding_values
        ):
            raise JournalIntegrityError("partial consumer binding in ingestion meta")
        binding = (
            ConsumerBinding(UUID(journal_generation), UUID(consumer_generation))
            if journal_generation is not None
            else None
        )
        return OwnerView(
            account_id=row["account_id"],
            device_id=row["device_id"],
            schema_version=row["schema_version"],
            stream_id=UUID(row["stream_id"]),
            binding_operation_id=UUID(row["binding_operation_id"]),
            binding=binding,
            consumer_first_sequence=row["consumer_first_sequence"],
            baseline_rooms_sha256=(
                bytes(row["baseline_rooms_sha256"])
                if row["baseline_rooms_sha256"] is not None
                else None
            ),
            consumer_attached_revision=row["consumer_attached_revision"],
            revision=row["revision"],
            writer_epoch=UUID(row["writer_epoch"]),
            next_source_epoch=row["next_source_epoch"],
            next_ready_order=row["next_ready_order"],
            next_batch_sequence=row["next_batch_sequence"],
            last_acked_sequence=row["last_acked_sequence"],
        )

    def _require_attached(self) -> OwnerView:
        owner = self.load_owner()
        if owner.binding is None:
            raise LocalProtocolError("ingestion consumer is not attached")
        if not self._consumer_validated:
            raise LocalProtocolError(
                "ingestion consumer is not validated for this owner lifetime"
            )
        return owner

    @property
    def schema_version(self) -> int:
        return int(self._meta()["schema_version"])

    @property
    def stream_id(self) -> UUID:
        return UUID(self._meta()["stream_id"])

    @property
    def binding_operation_id(self) -> UUID:
        return UUID(self._meta()["binding_operation_id"])

    @property
    def next_batch_sequence(self) -> int:
        return int(self._meta()["next_batch_sequence"])

    @staticmethod
    def _canonical_baseline(room_ids: tuple[str, ...]) -> bytes:
        if type(room_ids) is not tuple or any(
            type(room_id) is not str for room_id in room_ids
        ):
            raise TypeError("baseline_room_ids must be a tuple of str")
        if room_ids != tuple(sorted(set(room_ids))):
            raise LocalProtocolError(
                "baseline_room_ids must be sorted and contain no duplicates"
            )
        return json.dumps(
            list(room_ids),
            separators=(",", ":"),
            ensure_ascii=False,
        ).encode()

    def attach_consumer(self, consumer: ConsumerBootstrap) -> None:
        if type(consumer) is not ConsumerBootstrap:
            raise TypeError("consumer must be ConsumerBootstrap")
        baseline_payload = self._canonical_baseline(consumer.baseline_room_ids)
        baseline_digest = hashlib.sha256(baseline_payload).digest()
        if not hmac.compare_digest(baseline_digest, consumer.baseline_sha256):
            raise LocalProtocolError("baseline_sha256 does not match canonical rooms")

        owner = self.load_owner()
        if consumer.binding_operation_id != owner.binding_operation_id:
            raise LocalProtocolError("binding_operation_id does not match journal")
        if owner.binding is not None:
            if owner.binding != consumer.binding:
                raise LocalProtocolError(
                    "consumer binding does not match attached owner"
                )
            if owner.consumer_first_sequence != consumer.first_sequence:
                raise LocalProtocolError(
                    "first_sequence does not match attached owner"
                )
            assert owner.baseline_rooms_sha256 is not None
            if not hmac.compare_digest(
                owner.baseline_rooms_sha256,
                consumer.baseline_sha256,
            ):
                raise LocalProtocolError(
                    "consumer baseline does not match attached owner"
                )
            self._consumer_validated = True
            try:
                self._validate_attached_baseline(consumer)
            except BaseException:
                self._consumer_validated = False
                raise
            return
        if consumer.first_sequence != owner.next_batch_sequence:
            raise LocalProtocolError("first_sequence does not match journal")

        new_revision = owner.revision + 1
        origin = SystemOrigin(
            SystemOriginKind.FRESH_START,
            owner.binding_operation_id,
        )
        boundary = LossBoundary(None, None, None, None)
        detail = b'{"cause":"fresh_start","scope":"consumer_baseline"}'
        planned: list[tuple[RoomState, RoomLane, LossRecord, ReadyRecord]] = []
        for offset, room_id in enumerate(consumer.baseline_room_ids):
            incomplete = LossRecord(
                "",
                origin,
                room_id,
                0,
                LossReason.BASELINE_LOST,
                boundary,
                detail,
            )
            loss = LossRecord(
                _loss_id(owner.stream_id, incomplete),
                origin,
                room_id,
                0,
                LossReason.BASELINE_LOST,
                boundary,
                detail,
            )
            record_payload = _canonical_json(_record_to_dict(loss))
            planned.append(
                (
                    RoomState(
                        room_id,
                        0,
                        0,
                        RoomHydrationStatus.PENDING,
                        None,
                    ),
                    RoomLane(room_id, 0, LaneStatus.ACTIVE),
                    loss,
                    ReadyRecord(
                        owner.next_ready_order + offset,
                        loss,
                        canonical_bytes=len(record_payload),
                    ),
                )
            )

        try:
            self.connection.execute("BEGIN IMMEDIATE")
            cursor = self._transition_execute(
                "meta_attach",
                """
                UPDATE NioIngestMeta
                SET journal_generation = ?, consumer_generation = ?,
                    consumer_first_sequence = ?, baseline_rooms_sha256 = ?,
                    consumer_attached_revision = ?,
                    revision = ?, next_ready_order = ?
                WHERE account_id = ? AND revision = ? AND writer_epoch = ?
                  AND journal_generation IS NULL AND consumer_generation IS NULL
                """,
                (
                    str(consumer.binding.journal_generation),
                    str(consumer.binding.consumer_generation),
                    consumer.first_sequence,
                    consumer.baseline_sha256,
                    new_revision,
                    new_revision,
                    owner.next_ready_order + len(planned),
                    self.account_id,
                    owner.revision,
                    str(self.writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("consumer attach compare-and-swap failed")
            for state, lane, loss, ready in planned:
                self._write_room_state(state, new_revision)
                self._write_room_lane(lane, new_revision)
                self._write_loss(loss, new_revision)
                self._write_ready(ready, new_revision)
            self.connection.execute("COMMIT")
            self._consumer_validated = True
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise

    def _validate_attached_baseline(self, consumer: ConsumerBootstrap) -> None:
        room_ids = consumer.baseline_room_ids
        if not room_ids:
            return
        aggregates = self.load_rooms(frozenset(room_ids))
        if set(aggregates) != set(room_ids):
            raise JournalIntegrityError("attached baseline room plan is incomplete")
        placeholders = self.connection.execute(
            "SELECT room_id FROM NioIngestLoss "
            f"WHERE account_id = ? AND room_id IN ({','.join('?' for _ in room_ids)}) "
            "AND reason = ? AND membership_epoch = 0",
            (self.account_id, *room_ids, LossReason.BASELINE_LOST.value),
        ).fetchall()
        if {row["room_id"] for row in placeholders} != set(room_ids):
            raise JournalIntegrityError("attached baseline loss plan is incomplete")

    def _write_room_state(self, state: RoomState, revision: int) -> None:
        if state.snapshot is None:
            ciphertext = digest = None
        else:
            payload = _canonical_room_snapshot_payload(state.snapshot)
            digest = hashlib.sha256(payload).digest()
            ciphertext = self._codec.encrypt(
                "NioIngestRoomState",
                (state.room_id,),
                payload,
                digest,
            )
        self._transition_execute(
            "room_state",
            """
            INSERT INTO NioIngestRoomState (
                account_id, room_id, current_membership_epoch,
                next_room_sequence, hydration_status, state_ciphertext,
                state_sha256, updated_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, room_id) DO UPDATE SET
                current_membership_epoch = excluded.current_membership_epoch,
                next_room_sequence = excluded.next_room_sequence,
                hydration_status = excluded.hydration_status,
                state_ciphertext = excluded.state_ciphertext,
                state_sha256 = excluded.state_sha256,
                updated_revision = excluded.updated_revision
            """,
            (
                self.account_id,
                state.room_id,
                state.current_membership_epoch,
                state.next_room_sequence,
                state.hydration_status.value,
                ciphertext,
                digest,
                revision,
            ),
        )

    def _write_room_lane(self, lane: RoomLane, revision: int) -> None:
        if lane.pending_lifecycle is None:
            ciphertext = digest = None
        else:
            payload = _canonical_json(_record_to_dict(lane.pending_lifecycle))
            digest = hashlib.sha256(payload).digest()
            ciphertext = self._codec.encrypt(
                "NioIngestRoomLane.lifecycle",
                (lane.room_id, lane.membership_epoch),
                payload,
                digest,
            )
        self._transition_execute(
            "room_lane",
            """
            INSERT INTO NioIngestRoomLane (
                account_id, room_id, membership_epoch, lane_status,
                held_record_count, held_canonical_bytes, release_phase,
                release_loss_id, ready_order, next_held_ordinal,
                next_recovery_page, successor_membership_epoch,
                pending_lifecycle_ciphertext, pending_lifecycle_sha256,
                updated_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, room_id, membership_epoch) DO UPDATE SET
                lane_status = excluded.lane_status,
                held_record_count = excluded.held_record_count,
                held_canonical_bytes = excluded.held_canonical_bytes,
                release_phase = excluded.release_phase,
                release_loss_id = excluded.release_loss_id,
                ready_order = excluded.ready_order,
                next_held_ordinal = excluded.next_held_ordinal,
                next_recovery_page = excluded.next_recovery_page,
                successor_membership_epoch = excluded.successor_membership_epoch,
                pending_lifecycle_ciphertext = excluded.pending_lifecycle_ciphertext,
                pending_lifecycle_sha256 = excluded.pending_lifecycle_sha256,
                updated_revision = excluded.updated_revision
            """,
            (
                self.account_id,
                lane.room_id,
                lane.membership_epoch,
                lane.lane_status.value,
                lane.held_record_count,
                lane.held_canonical_bytes,
                lane.release_phase.value,
                lane.release_loss_id,
                lane.ready_order,
                lane.next_held_ordinal,
                lane.next_recovery_page,
                lane.successor_membership_epoch,
                ciphertext,
                digest,
                revision,
            ),
        )

    def _write_loss(self, loss: LossRecord, revision: int) -> None:
        if loss.loss_id != _loss_id(self.stream_id, loss):
            raise JournalIntegrityError("loss_id does not match loss contents")
        origin_payload = _canonical_json(_origin_to_dict(loss.origin))
        boundary_payload = _canonical_json(_boundary_to_dict(loss.boundary))
        detail_payload = loss.detail_json
        origin_digest = hashlib.sha256(origin_payload).digest()
        boundary_digest = hashlib.sha256(boundary_payload).digest()
        detail_digest = hashlib.sha256(detail_payload).digest()
        record_payload = _canonical_json(_record_to_dict(loss))
        loss_digest = hashlib.sha256(record_payload).digest()
        primary_key = (loss.loss_id,)
        cursor = self._transition_execute(
            "loss",
            """
            INSERT INTO NioIngestLoss (
                account_id, loss_id, room_id, membership_epoch, reason,
                origin_ciphertext, origin_sha256, boundary_ciphertext,
                boundary_sha256, detail_ciphertext, detail_sha256,
                loss_sha256, detected_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, loss_id) DO NOTHING
            """,
            (
                self.account_id,
                loss.loss_id,
                loss.room_id,
                loss.membership_epoch,
                loss.reason.value,
                self._codec.encrypt(
                    "NioIngestLoss.origin",
                    primary_key,
                    origin_payload,
                    origin_digest,
                ),
                origin_digest,
                self._codec.encrypt(
                    "NioIngestLoss.boundary",
                    primary_key,
                    boundary_payload,
                    boundary_digest,
                ),
                boundary_digest,
                self._codec.encrypt(
                    "NioIngestLoss.detail",
                    primary_key,
                    detail_payload,
                    detail_digest,
                ),
                detail_digest,
                loss_digest,
                revision,
            ),
        )
        if cursor.rowcount == 0:
            existing = self.load_loss(loss.loss_id)
            if existing != loss:
                raise JournalIntegrityError(
                    "loss_id collides with different authenticated contents"
                )

    def _validated_record_id(self, record: EventRecord | LossRecord) -> str:
        if isinstance(record, EventRecord):
            return record.record_id
        if record.loss_id != _loss_id(self.stream_id, record):
            raise JournalIntegrityError("loss_id does not match loss contents")
        return record.loss_id

    def _write_ready(self, ready: ReadyRecord, revision: int) -> None:
        payload = _canonical_json(_record_to_dict(ready.record))
        digest = hashlib.sha256(payload).digest()
        record_id = self._validated_record_id(ready.record)
        canonical_bytes = len(payload)
        if ready.canonical_bytes not in (0, canonical_bytes):
            raise JournalIntegrityError(
                "ready canonical_bytes does not match canonical payload"
            )
        cursor = self._transition_execute(
            "ready_record",
            """
            INSERT INTO NioIngestReadyRecord (
                account_id, ready_order, record_id, source_frame_id, room_id,
                membership_epoch, room_sequence, payload_ciphertext,
                payload_sha256, canonical_bytes, created_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, record_id) DO NOTHING
            """,
            (
                self.account_id,
                ready.ready_order,
                record_id,
                str(ready.source_frame_id) if ready.source_frame_id else None,
                ready.record.room_id,
                ready.record.membership_epoch,
                (
                    ready.record.room_sequence
                    if isinstance(ready.record, EventRecord)
                    else None
                ),
                self._codec.encrypt(
                    "NioIngestReadyRecord",
                    (record_id,),
                    payload,
                    digest,
                ),
                digest,
                canonical_bytes,
                revision,
            ),
        )
        if cursor.rowcount == 0:
            row = self.connection.execute(
                "SELECT ready_order, record_id, source_frame_id, room_id, "
                "membership_epoch, room_sequence, payload_ciphertext, "
                "payload_sha256, canonical_bytes, created_revision "
                "FROM NioIngestReadyRecord "
                "WHERE account_id = ? AND record_id = ?",
                (self.account_id, record_id),
            ).fetchone()
            existing = self._decode_ready_row(row) if row is not None else None
            if (
                existing is None
                or existing.ready_order != ready.ready_order
                or existing.record != ready.record
                or existing.source_frame_id != ready.source_frame_id
                or existing.canonical_bytes != canonical_bytes
            ):
                raise JournalIntegrityError(
                    "ready record_id or ready_order collides with different contents"
                )

    def _write_source(self, source: SourceState) -> None:
        digest = hashlib.sha256(source.cursor_json).digest()
        ciphertext = self._codec.encrypt(
            "NioIngestSourceState",
            (self.account_id,),
            source.cursor_json,
            digest,
        )
        self._transition_execute(
            "source_state",
            """
            INSERT INTO NioIngestSourceState (
                account_id, source_epoch, transport_kind, cursor_ciphertext,
                cursor_sha256, next_request_id, active
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id) DO UPDATE SET
                source_epoch = excluded.source_epoch,
                transport_kind = excluded.transport_kind,
                cursor_ciphertext = excluded.cursor_ciphertext,
                cursor_sha256 = excluded.cursor_sha256,
                next_request_id = excluded.next_request_id,
                active = excluded.active
            """,
            (
                self.account_id,
                source.source_epoch,
                source.transport_kind.value,
                ciphertext,
                digest,
                source.next_request_id,
                int(source.active),
            ),
        )

    def load_source(self) -> SourceState | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestSourceState WHERE account_id = ?",
            (self.account_id,),
        ).fetchone()
        if row is None:
            return None
        cursor = self._codec.decrypt(
            "NioIngestSourceState",
            (self.account_id,),
            bytes(row["cursor_ciphertext"]),
            bytes(row["cursor_sha256"]),
        )
        return SourceState(
            row["source_epoch"],
            TransportKind(row["transport_kind"]),
            cursor,
            row["next_request_id"],
            bool(row["active"]),
        )

    def _write_frame(self, frame: StagedFrame, revision: int) -> None:
        digest = hashlib.sha256(frame.payload).digest()
        ciphertext = self._codec.encrypt(
            "NioIngestFrame",
            (frame.frame_id,),
            frame.payload,
            digest,
        )
        cursor = self._transition_execute(
            "frame",
            """
            INSERT INTO NioIngestFrame (
                account_id, frame_id, source_epoch, request_id,
                payload_ciphertext, payload_sha256, staged_revision
            ) VALUES (?, ?, ?, ?, ?, ?, ?)
            ON CONFLICT(account_id, frame_id) DO NOTHING
            """,
            (
                self.account_id,
                str(frame.frame_id),
                frame.source_epoch,
                frame.request_id,
                ciphertext,
                digest,
                revision,
            ),
        )
        if cursor.rowcount == 0:
            existing = self.load_frame(frame.frame_id)
            if (
                existing is None
                or existing.source_epoch != frame.source_epoch
                or existing.request_id != frame.request_id
                or existing.payload != frame.payload
            ):
                raise JournalIntegrityError(
                    "frame_id collides with different authenticated contents"
                )

    def load_frame(self, frame_id: UUID) -> StagedFrame | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestFrame WHERE account_id = ? AND frame_id = ?",
            (self.account_id, str(frame_id)),
        ).fetchone()
        if row is None:
            return None
        payload = self._codec.decrypt(
            "NioIngestFrame",
            (frame_id,),
            bytes(row["payload_ciphertext"]),
            bytes(row["payload_sha256"]),
        )
        return StagedFrame(
            frame_id,
            row["source_epoch"],
            row["request_id"],
            payload,
            row["staged_revision"],
        )

    def _write_batch(
        self,
        batch: SyncBatch,
        revision: int,
        owner: OwnerView,
    ) -> None:
        if (
            batch.account_id != self.account_id
            or batch.device_id != self.device_id
            or batch.consumer != owner.binding
            or batch.ref.stream_id != owner.stream_id
        ):
            raise JournalIntegrityError("batch owner identity does not match journal")
        if batch.created_revision != revision:
            raise JournalIntegrityError(
                "batch created_revision does not match commit revision"
            )
        payload = canonical_batch_payload(batch)
        digest = hashlib.sha256(payload).digest()
        if not hmac.compare_digest(digest, batch.ref.sha256):
            raise JournalIntegrityError("batch payload digest does not match reference")
        ciphertext = self._codec.encrypt(
            "NioIngestBatch",
            (batch.ref.sequence,),
            payload,
            digest,
        )
        self._transition_execute(
            "batch",
            """
            INSERT INTO NioIngestBatch (
                account_id, sequence, batch_id, payload_ciphertext,
                payload_sha256, created_revision, acknowledged_revision
            ) VALUES (?, ?, ?, ?, ?, ?, NULL)
            """,
            (
                self.account_id,
                batch.ref.sequence,
                str(batch.ref.batch_id),
                ciphertext,
                digest,
                revision,
            ),
        )

    def _decode_batch(self, row: sqlite3.Row) -> SyncBatch:
        digest = bytes(row["payload_sha256"])
        payload = self._codec.decrypt(
            "NioIngestBatch",
            (row["sequence"],),
            bytes(row["payload_ciphertext"]),
            digest,
        )
        batch = _batch_from_payload(payload)
        if (
            batch.ref.sequence != row["sequence"]
            or str(batch.ref.batch_id) != row["batch_id"]
            or not hmac.compare_digest(batch.ref.sha256, digest)
        ):
            raise JournalIntegrityError("batch identity does not match stored row")
        if batch.created_revision != row["created_revision"]:
            raise JournalIntegrityError(
                "batch created_revision does not match stored row"
            )
        owner = self._require_attached()
        if (
            batch.account_id != owner.account_id
            or batch.device_id != owner.device_id
            or batch.consumer != owner.binding
            or batch.ref.stream_id != owner.stream_id
        ):
            raise JournalIntegrityError("batch owner does not match journal owner")
        return batch

    def _decode_ready_row(self, row: sqlite3.Row) -> ReadyRecord:
        digest = bytes(row["payload_sha256"])
        payload = self._codec.decrypt(
            "NioIngestReadyRecord",
            (row["record_id"],),
            bytes(row["payload_ciphertext"]),
            digest,
        )
        record = _record_from_dict(_load_json_object(payload))
        if row["canonical_bytes"] != len(payload):
            raise JournalIntegrityError(
                "ready canonical_bytes does not match canonical payload"
            )
        actual_id = self._validated_record_id(record)
        if actual_id != row["record_id"]:
            raise JournalIntegrityError("ready record identity does not match row")
        expected_sequence = (
            record.room_sequence if isinstance(record, EventRecord) else None
        )
        if (
            record.room_id != row["room_id"]
            or record.membership_epoch != row["membership_epoch"]
            or expected_sequence != row["room_sequence"]
        ):
            raise JournalIntegrityError(
                "ready record columns do not match authenticated payload"
            )
        return ReadyRecord(
            row["ready_order"],
            record,
            (
                UUID(row["source_frame_id"])
                if row["source_frame_id"] is not None
                else None
            ),
            row["canonical_bytes"],
            row["created_revision"],
        )

    def load_ready_heads(self, limit: int) -> tuple[ReadyRecord, ...]:
        self._require_attached()
        if type(limit) is not int or limit <= 0:
            raise ValueError("limit must be positive")
        rows = self.connection.execute(
            """
            SELECT ready_order, record_id, source_frame_id, room_id,
                   membership_epoch, room_sequence, payload_ciphertext,
                   payload_sha256, canonical_bytes, created_revision
            FROM NioIngestReadyRecord
            WHERE account_id = ?
            ORDER BY ready_order, record_id
            LIMIT ?
            """,
            (self.account_id, limit),
        ).fetchall()
        return tuple(self._decode_ready_row(row) for row in rows)

    def _decode_room_state(self, row: sqlite3.Row) -> RoomState:
        ciphertext = row["state_ciphertext"]
        digest_value = row["state_sha256"]
        if (ciphertext is None) != (digest_value is None):
            raise JournalIntegrityError("partial encrypted room state")
        snapshot = None
        if ciphertext is not None:
            payload = self._codec.decrypt(
                "NioIngestRoomState",
                (row["room_id"],),
                bytes(ciphertext),
                bytes(digest_value),
            )
            snapshot = _room_snapshot_from_payload(payload)
        return RoomState(
            row["room_id"],
            row["current_membership_epoch"],
            row["next_room_sequence"],
            RoomHydrationStatus(row["hydration_status"]),
            snapshot,
            row["updated_revision"],
        )

    def _decode_room_lane(self, row: sqlite3.Row) -> RoomLane:
        ciphertext = row["pending_lifecycle_ciphertext"]
        digest_value = row["pending_lifecycle_sha256"]
        if (ciphertext is None) != (digest_value is None):
            raise JournalIntegrityError("partial encrypted lifecycle barrier")
        lifecycle = None
        if ciphertext is not None:
            payload = self._codec.decrypt(
                "NioIngestRoomLane.lifecycle",
                (row["room_id"], row["membership_epoch"]),
                bytes(ciphertext),
                bytes(digest_value),
            )
            decoded = _record_from_dict(_load_json_object(payload))
            if not isinstance(decoded, EventRecord):
                raise JournalIntegrityError("lifecycle barrier is not an event record")
            lifecycle = decoded
        return RoomLane(
            room_id=row["room_id"],
            membership_epoch=row["membership_epoch"],
            lane_status=LaneStatus(row["lane_status"]),
            held_record_count=row["held_record_count"],
            held_canonical_bytes=row["held_canonical_bytes"],
            release_phase=ReleasePhase(row["release_phase"]),
            release_loss_id=row["release_loss_id"],
            ready_order=row["ready_order"],
            next_held_ordinal=row["next_held_ordinal"],
            next_recovery_page=row["next_recovery_page"],
            successor_membership_epoch=row["successor_membership_epoch"],
            pending_lifecycle=lifecycle,
            updated_revision=row["updated_revision"],
        )

    @staticmethod
    def _validate_room_aggregate(state: RoomState, lanes: tuple[RoomLane, ...]) -> None:
        if not lanes:
            raise JournalIntegrityError("room state has no membership lane")
        ordered = tuple(sorted(lanes, key=lambda lane: lane.membership_epoch))
        if ordered != lanes:
            raise JournalIntegrityError("room membership lanes are not ordered")
        if any(
            right.membership_epoch != left.membership_epoch + 1
            for left, right in zip(ordered, ordered[1:], strict=False)
        ):
            raise JournalIntegrityError("room membership lane chain is not gap-free")
        active = tuple(
            lane for lane in ordered if lane.lane_status is LaneStatus.ACTIVE
        )
        if len(active) != 1:
            raise JournalIntegrityError("room must have exactly one active lane")
        if active[0].membership_epoch != state.current_membership_epoch:
            raise JournalIntegrityError(
                "active lane does not match current membership epoch"
            )
        if active[0] is not ordered[-1]:
            raise JournalIntegrityError("active lane must be the final epoch")

        for lane, successor in zip(ordered, ordered[1:], strict=False):
            lifecycle = lane.pending_lifecycle
            if lane.lane_status is not LaneStatus.RETIRING:
                raise JournalIntegrityError("predecessor lane must be retiring")
            if lane.successor_membership_epoch != successor.membership_epoch:
                raise JournalIntegrityError("retiring lane successor is not gap-free")
            if (
                lifecycle is None
                or lifecycle.kind is not RecordKind.ROOM_LIFECYCLE
                or lifecycle.room_id != state.room_id
                or lifecycle.membership_epoch != successor.membership_epoch
            ):
                raise JournalIntegrityError(
                    "retiring lane lifecycle barrier is invalid"
                )
        if (
            active[0].successor_membership_epoch is not None
            or active[0].pending_lifecycle is not None
        ):
            raise JournalIntegrityError("active lane cannot have a successor barrier")

    def load_rooms(
        self,
        room_ids: frozenset[str],
    ) -> dict[str, RoomAggregate]:
        self._require_attached()
        if type(room_ids) is not frozenset or any(
            type(room_id) is not str for room_id in room_ids
        ):
            raise TypeError("room_ids must be a frozenset of str")
        if not room_ids:
            return {}
        ordered_ids = tuple(sorted(room_ids))
        placeholders = ",".join("?" for _ in ordered_ids)
        state_rows = self.connection.execute(
            "SELECT * FROM NioIngestRoomState "
            f"WHERE account_id = ? AND room_id IN ({placeholders}) "
            "ORDER BY room_id",
            (self.account_id, *ordered_ids),
        ).fetchall()
        lane_rows = self.connection.execute(
            "SELECT * FROM NioIngestRoomLane "
            f"WHERE account_id = ? AND room_id IN ({placeholders}) "
            "ORDER BY room_id, membership_epoch",
            (self.account_id, *ordered_ids),
        ).fetchall()
        states = {row["room_id"]: self._decode_room_state(row) for row in state_rows}
        lanes_by_room: dict[str, list[RoomLane]] = {}
        for row in lane_rows:
            lanes_by_room.setdefault(row["room_id"], []).append(
                self._decode_room_lane(row)
            )
        if set(lanes_by_room) - set(states):
            raise JournalIntegrityError("membership lane exists without room state")

        aggregates: dict[str, RoomAggregate] = {}
        for room_id, state in states.items():
            lanes = tuple(lanes_by_room.get(room_id, ()))
            self._validate_room_aggregate(state, lanes)
            aggregates[room_id] = RoomAggregate(
                state,
                lanes[-1],
                lanes[:-1],
            )
        return aggregates

    def load_loss(self, loss_id: str) -> LossRecord | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestLoss WHERE account_id = ? AND loss_id = ?",
            (self.account_id, loss_id),
        ).fetchone()
        if row is None:
            return None
        primary_key = (loss_id,)
        origin_payload = self._codec.decrypt(
            "NioIngestLoss.origin",
            primary_key,
            bytes(row["origin_ciphertext"]),
            bytes(row["origin_sha256"]),
        )
        boundary_payload = self._codec.decrypt(
            "NioIngestLoss.boundary",
            primary_key,
            bytes(row["boundary_ciphertext"]),
            bytes(row["boundary_sha256"]),
        )
        detail = self._codec.decrypt(
            "NioIngestLoss.detail",
            primary_key,
            bytes(row["detail_ciphertext"]),
            bytes(row["detail_sha256"]),
        )
        loss = LossRecord(
            loss_id,
            _origin_from_dict(_load_json_object(origin_payload)),
            row["room_id"],
            row["membership_epoch"],
            LossReason(row["reason"]),
            _boundary_from_dict(_load_json_object(boundary_payload)),
            detail,
        )
        loss_digest = hashlib.sha256(_canonical_json(_record_to_dict(loss))).digest()
        if not hmac.compare_digest(loss_digest, bytes(row["loss_sha256"])):
            raise JournalIntegrityError("whole loss digest mismatch")
        if loss.loss_id != _loss_id(self.stream_id, loss):
            raise JournalIntegrityError("loss_id does not match loss contents")
        return loss

    def commit(
        self,
        *,
        expected_revision: int,
        writer_epoch: UUID,
        transition: JournalTransition,
    ) -> CommitResult:
        owner = self._require_attached()
        if type(expected_revision) is not int:
            raise TypeError("expected_revision must be int")
        if type(writer_epoch) is not UUID:
            raise TypeError("writer_epoch must be UUID")
        if type(transition) is not JournalTransition:
            raise TypeError("transition must be JournalTransition")
        if owner.revision != expected_revision or writer_epoch != self.writer_epoch:
            raise JournalConflictError("journal revision or writer_epoch is stale")

        ready_orders = tuple(
            sorted(ready.ready_order for ready in transition.ready_records)
        )
        if ready_orders and ready_orders != tuple(
            range(owner.next_ready_order, owner.next_ready_order + len(ready_orders))
        ):
            raise JournalConflictError("ready_order allocation is not contiguous")
        batch_sequences = tuple(
            sorted(batch.ref.sequence for batch in transition.batches)
        )
        if batch_sequences and batch_sequences != tuple(
            range(
                owner.next_batch_sequence,
                owner.next_batch_sequence + len(batch_sequences),
            )
        ):
            raise JournalConflictError("batch sequence allocation is not contiguous")

        touched_ids = frozenset(
            [state.room_id for state in transition.room_states]
            + [lane.room_id for lane in transition.room_lanes]
        )
        proposed: dict[str, tuple[RoomState, dict[int, RoomLane]]] = {}
        if touched_ids:
            for room_id, aggregate in self.load_rooms(touched_ids).items():
                proposed[room_id] = (
                    aggregate.state,
                    {
                        lane.membership_epoch: lane
                        for lane in (*aggregate.retiring_lanes, aggregate.active_lane)
                    },
                )
            for state in transition.room_states:
                existing = proposed.get(state.room_id)
                proposed[state.room_id] = (state, existing[1] if existing else {})
            for lane in transition.room_lanes:
                existing = proposed.get(lane.room_id)
                if existing is None:
                    raise JournalIntegrityError(
                        "room lane transition requires room state"
                    )
                existing[1][lane.membership_epoch] = lane
            for state, lanes in proposed.values():
                self._validate_room_aggregate(
                    state,
                    tuple(lanes[epoch] for epoch in sorted(lanes)),
                )

        new_revision = expected_revision + 1
        try:
            self.connection.execute("BEGIN IMMEDIATE")
            cursor = self._transition_execute(
                "meta_revision",
                "UPDATE NioIngestMeta SET revision = ?, next_ready_order = ?, "
                "next_batch_sequence = ? "
                "WHERE account_id = ? AND revision = ? AND writer_epoch = ?",
                (
                    new_revision,
                    owner.next_ready_order + len(ready_orders),
                    owner.next_batch_sequence + len(batch_sequences),
                    self.account_id,
                    expected_revision,
                    str(writer_epoch),
                ),
            )
            if cursor.rowcount != 1:
                raise JournalConflictError("journal commit compare-and-swap failed")
            if transition.source_state is not None:
                self._write_source(transition.source_state)
            for state in transition.room_states:
                self._write_room_state(state, new_revision)
            for lane in transition.room_lanes:
                self._write_room_lane(lane, new_revision)
            for ready in transition.ready_records:
                self._write_ready(ready, new_revision)
            for frame in transition.frames:
                self._write_frame(frame, new_revision)
            for batch in transition.batches:
                self._write_batch(batch, new_revision, owner)
            for loss in transition.losses:
                self._write_loss(loss, new_revision)
            for frame_id in transition.delete_frame_ids:
                self._transition_execute(
                    "delete_frame",
                    "DELETE FROM NioIngestFrame "
                    "WHERE account_id = ? AND frame_id = ?",
                    (self.account_id, str(frame_id)),
                )
            self.connection.execute("COMMIT")
        except BaseException:
            if self.connection.in_transaction:
                self.connection.execute("ROLLBACK")
            raise
        return CommitResult(new_revision)

    def oldest_unacknowledged(self) -> SyncBatch | None:
        self._require_attached()
        row = self.connection.execute(
            "SELECT * FROM NioIngestBatch "
            "WHERE account_id = ? AND acknowledged_revision IS NULL "
            "ORDER BY sequence LIMIT 1",
            (self.account_id,),
        ).fetchone()
        return self._decode_batch(row) if row is not None else None

    @staticmethod
    def _reference_matches(batch: SyncBatch, ref: BatchRef) -> bool:
        return (
            batch.ref.stream_id == ref.stream_id
            and batch.ref.sequence == ref.sequence
            and batch.ref.batch_id == ref.batch_id
            and hmac.compare_digest(batch.ref.sha256, ref.sha256)
        )

    def acknowledge(self, ref: BatchRef) -> AckOutcome:
        if type(ref) is not BatchRef:
            raise TypeError("ref must be BatchRef")
        if not self._ack_lock.acquire(blocking=False):
            raise LocalProtocolError("concurrent acknowledgement is not allowed")
        try:
            owner = self._require_attached()
            if ref.stream_id != owner.stream_id:
                raise JournalConflictError("batch reference stream does not match")

            if ref.sequence < owner.last_acked_sequence:
                raise JournalConflictError("stale acknowledgement")
            if ref.sequence == owner.last_acked_sequence:
                row = self.connection.execute(
                    "SELECT * FROM NioIngestBatch "
                    "WHERE account_id = ? AND sequence = ? "
                    "AND acknowledged_revision IS NOT NULL",
                    (self.account_id, ref.sequence),
                ).fetchone()
                if row is None:
                    raise JournalIntegrityError(
                        "latest acknowledged batch row is not retained"
                    )
                batch = self._decode_batch(row)
                frontier = self._meta()
                if (
                    frontier["last_acked_batch_id"] != str(batch.ref.batch_id)
                    or frontier["last_acked_sha256"] is None
                    or not hmac.compare_digest(
                        bytes(frontier["last_acked_sha256"]),
                        batch.ref.sha256,
                    )
                ):
                    raise JournalIntegrityError(
                        "acknowledgement frontier does not match retained payload"
                    )
                if not self._reference_matches(batch, ref):
                    raise JournalConflictError(
                        "acknowledgement reference does not match payload"
                    )
                return AckOutcome.ALREADY_ACKNOWLEDGED

            if ref.sequence != owner.last_acked_sequence + 1:
                raise JournalConflictError("acknowledgement is out of order")
            row = self.connection.execute(
                "SELECT * FROM NioIngestBatch "
                "WHERE account_id = ? AND sequence = ? "
                "AND acknowledged_revision IS NULL",
                (self.account_id, ref.sequence),
            ).fetchone()
            if row is None:
                raise JournalConflictError("acknowledgement is out of order")
            batch = self._decode_batch(row)
            if not self._reference_matches(batch, ref):
                raise JournalConflictError(
                    "acknowledgement reference does not match payload"
                )

            new_revision = owner.revision + 1
            try:
                self.connection.execute("BEGIN IMMEDIATE")
                cursor = self._transition_execute(
                    "ack_meta",
                    """
                    UPDATE NioIngestMeta
                    SET revision = ?, last_acked_sequence = ?,
                        last_acked_batch_id = ?, last_acked_sha256 = ?
                    WHERE account_id = ? AND revision = ? AND writer_epoch = ?
                      AND last_acked_sequence = ?
                    """,
                    (
                        new_revision,
                        ref.sequence,
                        str(ref.batch_id),
                        ref.sha256,
                        self.account_id,
                        owner.revision,
                        str(self.writer_epoch),
                        owner.last_acked_sequence,
                    ),
                )
                if cursor.rowcount != 1:
                    raise JournalConflictError(
                        "acknowledgement compare-and-swap failed"
                    )
                cursor = self._transition_execute(
                    "ack_batch",
                    "UPDATE NioIngestBatch SET acknowledged_revision = ? "
                    "WHERE account_id = ? AND sequence = ? "
                    "AND acknowledged_revision IS NULL",
                    (new_revision, self.account_id, ref.sequence),
                )
                if cursor.rowcount != 1:
                    raise JournalConflictError("batch acknowledgement row changed")
                if owner.last_acked_sequence:
                    cursor = self._transition_execute(
                        "ack_delete_previous",
                        "DELETE FROM NioIngestBatch "
                        "WHERE account_id = ? AND sequence = ? "
                        "AND acknowledged_revision IS NOT NULL",
                        (self.account_id, owner.last_acked_sequence),
                    )
                    if cursor.rowcount != 1:
                        raise JournalIntegrityError(
                            "previous acknowledged batch row is missing"
                        )
                self.connection.execute("COMMIT")
            except BaseException:
                if self.connection.in_transaction:
                    self.connection.execute("ROLLBACK")
                raise
            return AckOutcome.ACKNOWLEDGED
        finally:
            self._ack_lock.release()

    def close(self) -> None:
        if self._closed:
            return
        self.connection.close()
        self._writer_lock.close()
        self._closed = True


class StoreBootstrap:
    """Single-owner preflight handle retaining the ingestion writer lock."""

    def __init__(self, journal: SqliteIngestionJournal) -> None:
        self._journal = journal
        self._store_opened = False

    @property
    def journal(self) -> SqliteIngestionJournal:
        self._journal._assert_open()
        return self._journal

    @property
    def database_path(self) -> Path:
        return self.journal.database_path

    @property
    def schema_version(self) -> int:
        return self.journal.schema_version

    @property
    def stream_id(self) -> UUID:
        return self.journal.stream_id

    @property
    def binding_operation_id(self) -> UUID:
        return self.journal.binding_operation_id

    @property
    def next_batch_sequence(self) -> int:
        return self.journal.next_batch_sequence

    def open_matrix_store(
        self,
        store_class: type[MatrixStore],
        *,
        pickle_key: str | None = None,
    ) -> MatrixStore:
        if self._store_opened:
            raise LocalProtocolError("StoreBootstrap can open MatrixStore only once")
        from .database import _open_matrix_store_from_ingestion

        store = _open_matrix_store_from_ingestion(
            self,
            store_class,
            self.journal.pickle_key if pickle_key is None else pickle_key,
        )
        self._store_opened = True
        return store

    async def attach_consumer(self, consumer: ConsumerBootstrap) -> None:
        self.journal.attach_consumer(consumer)

    def assert_http_enabled(self) -> None:
        self.journal._require_attached()

    def close(self) -> None:
        self._journal.close()


def open_ingestion_store(
    store_path: str | os.PathLike[str],
    *,
    account_id: str,
    device_id: str,
    pickle_key: str = "",
    database_name: str = "",
    sqlite_busy_timeout_ms: int = 2_000,
    statement_observer: Callable[[str], None] | None = None,
    transition_statement_hook: Callable[[str], None] | None = None,
    schema_statement_hook: Callable[[str], None] | None = None,
) -> StoreBootstrap:
    database_name = database_name or f"{account_id}_{device_id}.db"
    database_path = Path(store_path) / database_name
    journal = SqliteIngestionJournal.open(
        database_path,
        account_id=account_id,
        device_id=device_id,
        pickle_key=pickle_key,
        sqlite_busy_timeout_ms=sqlite_busy_timeout_ms,
        statement_observer=statement_observer,
        transition_statement_hook=transition_statement_hook,
        schema_statement_hook=schema_statement_hook,
    )
    return StoreBootstrap(journal)
