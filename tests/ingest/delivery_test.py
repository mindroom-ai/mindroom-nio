import hashlib
import json
import sqlite3
from collections.abc import Iterator
from contextlib import AbstractContextManager, closing, contextmanager
from dataclasses import fields, replace
from pathlib import Path
from uuid import UUID, uuid5

import pytest

from nio.event_provenance import TimelineEventProvenance
from nio.exceptions import LocalProtocolError
from nio.ingest._json import canonical_json
from nio.ingest.config import ClassicSourceConfig
from nio.ingest.errors import (
    BatchIntegrityError,
    JournalConflictError,
    JournalIntegrityError,
)
from nio.ingest.model import (
    BatchRef,
    EventRecord,
    RecordKind,
    RecordOrigin,
    RoomSnapshot,
    SyncBatch,
    TransportKind,
    _CallbackRoute,
    _DecryptionDisposition,
    _PreparationPhase,
)
from nio.ingest.reducer import RoomContinuity
from nio.ingest.serialization import (
    _batch_from_payload,
    batch_from_records,
    canonical_batch_payload,
)
from nio.store._sync_journal import SqliteIngestionJournal
from nio.store._sync_journal_plan import (
    AuthenticatedWork,
    _PreparedWorkMetadata,
    _canonical_work_plaintext,
)
from nio.store._sync_journal_rows import (
    _canonical_internal,
    _canonical_room_aggregate_plaintext,
)
from nio.store._sync_journal_values import RoomAggregateValue
from nio.store.sync_journal import open_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
CONSUMER_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
STREAM_ID = UUID("44444444-4444-4444-8444-444444444444")
WORK_ID = "aaaaaaaa-aaaa-4aaa-8aaa-aaaaaaaaaaaa"
SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
DELIVERY_COLUMNS = (
    "delivery_next_sequence",
    "delivery_acknowledged_sha256",
    "delivery_outstanding_work_id",
    "delivery_outstanding_ready_revision",
    "delivery_outstanding_ready_ordinal",
    "delivery_outstanding_batch_sha256",
)
GOLDEN = (
    b'{"schema_version":1,"account_id":"@alice:example.org","device_id":"DEVICE",'
    b'"consumer_generation":"22222222-2222-4222-8222-222222222222",'
    b'"stream_id":"44444444-4444-4444-8444-444444444444","sequence":0,'
    b'"created_revision":1,"records":[{"record_type":"event",'
    b'"record_id":"00000000-0000-4000-8000-000000000001","kind":"timeline",'
    b'"origin":{"origin_type":"transport","transport":"classic",'
    b'"source_epoch":0,"request_id":0,"frame_index":0},'
    b'"room_id":"!room:example.org","membership_epoch":0,"room_sequence":0,'
    b'"event_id":null,"provenance":"live","source_json":"e30=",'
    b'"clear_json":null}]}'
)


def _event() -> EventRecord:
    return EventRecord(
        "00000000-0000-4000-8000-000000000001",
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 0, 0, 0),
        "!room:example.org",
        0,
        0,
        None,
        TimelineEventProvenance.LIVE,
        b"{}",
        None,
    )


def _ready_event(number: int, padding: int = 0) -> EventRecord:
    return EventRecord(
        str(UUID(int=number)),
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 0, 0, number),
        "!room:example.org",
        0,
        number,
        None,
        TimelineEventProvenance.LIVE,
        json.dumps({"body": "x" * padding}, separators=(",", ":")).encode(),
        None,
    )


def _open(tmp_path: Path, **kwargs: object):
    return open_ingestion_store(
        tmp_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        source=SOURCE,
        database_name="journal.db",
        **kwargs,
    )


def _meta_row(database_path: Path) -> sqlite3.Row:
    with closing(sqlite3.connect(database_path)) as connection:
        connection.row_factory = sqlite3.Row
        rows = connection.execute("SELECT * FROM NioIngestMeta LIMIT 2").fetchall()
    assert len(rows) == 1
    return rows[0]


def _seed_work(
    journal: SqliteIngestionJournal,
    record: EventRecord,
    *,
    ready_revision: int | None,
    ready_ordinal: int | None,
    status: str = "ready",
    metadata: _PreparedWorkMetadata | None = None,
) -> tuple[object, ...]:
    owner = journal.load_owner()
    created_revision = ready_revision or max(owner.revision, 1)
    clear = (
        record.record_id,
        "event",
        status,
        str(UUID(int=10_000 + record.origin.frame_index)),
        record.room_id,
        record.membership_epoch,
        record.room_sequence,
        ready_revision,
        ready_ordinal,
        created_revision,
    )
    payload, digest = journal._payload(
        owner,
        "NioIngestWork",
        _canonical_work_plaintext("event", record, metadata),
        header=_canonical_internal(clear),
    )
    row = (journal.account_id, *clear, payload, digest)
    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestMeta SET revision = ? WHERE account_id = ?",
            (max(owner.revision, created_revision), journal.account_id),
        )
        journal._execute(
            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row,
        )
    return row


def _seed_room_aggregates(
    journal: SqliteIngestionJournal,
    values: tuple[RoomAggregateValue, ...],
) -> tuple[tuple[object, ...], ...]:
    owner = journal.load_owner()
    rows: list[tuple[object, ...]] = []
    for value in values:
        room_id = value.continuity.room_id
        payload, digest = journal._payload(
            owner,
            "NioIngestRoomAggregate",
            _canonical_room_aggregate_plaintext(value),
            header=_canonical_internal([room_id, value.updated_revision, None]),
        )
        rows.append(
            (
                journal.account_id,
                room_id,
                value.updated_revision,
                None,
                payload,
                digest,
            )
        )
    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestMeta SET revision = ? WHERE account_id = ?",
            (
                max(owner.revision, *(value.updated_revision for value in values)),
                journal.account_id,
            ),
        )
        for row in rows:
            journal._execute(
                "INSERT INTO NioIngestRoomAggregate VALUES (?, ?, ?, ?, ?, ?)",
                row,
            )
    return tuple(rows)


def _raw_delivery_graph(database_path: Path) -> tuple[tuple[object, ...], ...]:
    with closing(sqlite3.connect(database_path)) as connection:
        return tuple(
            tuple(row)
            for table in ("NioIngestMeta", "NioIngestWork")
            for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
        )


def _raw_journal_graph(
    database_path: Path,
) -> tuple[tuple[str, tuple[object, ...]], ...]:
    with closing(sqlite3.connect(database_path)) as connection:
        tables = tuple(
            row[0]
            for row in connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table' "
                "AND name LIKE 'NioIngest%' ORDER BY name"
            )
        )
        return tuple(
            (table, tuple(row))
            for table in tables
            for row in connection.execute(f"SELECT * FROM {table} ORDER BY rowid")
        )


def _same_length_tamper(value: object) -> bytes:
    assert type(value) is bytes and value
    return bytes((value[0] ^ 1,)) + value[1:]


def _observe_owner_scopes(
    journal: SqliteIngestionJournal,
    monkeypatch: pytest.MonkeyPatch,
) -> tuple[list[str], list[str]]:
    reads: list[str] = []
    writes: list[str] = []
    real_read = journal._owner.read
    real_write = journal._owner.journal_write

    @contextmanager
    def observed_read() -> Iterator[None]:
        reads.append("read")
        with real_read():
            yield

    def observed_write() -> AbstractContextManager[None]:
        writes.append("write")
        return real_write()

    monkeypatch.setattr(journal._owner, "read", observed_read)
    monkeypatch.setattr(journal._owner, "journal_write", observed_write)
    return reads, writes


@contextmanager
def _one_read_without_writes(
    *,
    database_path: Path,
    statements: list[str],
    reads: list[str],
    writes: list[str],
) -> Iterator[None]:
    before = _raw_journal_graph(database_path)
    statements.clear()
    reads.clear()
    writes.clear()
    yield
    assert reads == ["read"]
    assert writes == []
    assert _raw_journal_graph(database_path) == before
    assert statements
    assert all(sql.lstrip().upper().startswith("SELECT") for sql in statements)


def _delivery_frontier(database_path: Path) -> tuple[object, ...]:
    row = _meta_row(database_path)
    return tuple(row[name] for name in DELIVERY_COLUMNS)


def _replace_work(
    journal: SqliteIngestionJournal,
    old: tuple[object, ...],
    clear: tuple[object, ...],
    record: EventRecord,
) -> tuple[object, ...]:
    owner = journal.load_owner()
    payload, digest = journal._payload(
        owner,
        "NioIngestWork",
        _canonical_work_plaintext("event", record),
        header=_canonical_internal(clear),
    )
    row = (journal.account_id, *clear, payload, digest)
    with journal._owner.journal_write():
        journal._execute(
            "DELETE FROM NioIngestWork WHERE account_id = ? AND work_id = ?",
            (journal.account_id, old[1]),
        )
        journal._execute(
            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            row,
        )
    return row


def test_settlement_view_authenticates_batch_work_and_room_without_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statement_observer=statements.append)
    journal = bootstrap._journal
    room_id = "!settlement:example.org"
    source = canonical_json(
        {
            "content": {"body": "hello", "msgtype": "m.text"},
            "event_id": "$settlement",
            "sender": "@bob:example.org",
            "type": "m.room.message",
        }
    )
    record = EventRecord(
        "00000000-0000-4000-8000-000000000070",
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 0, 0, 70),
        room_id,
        0,
        0,
        "$settlement",
        TimelineEventProvenance.LIVE,
        source,
        None,
    )
    metadata = _PreparedWorkMetadata(
        record.record_id,
        _PreparationPhase.SOURCE,
        "m.room.message",
        _DecryptionDisposition.NONE,
        None,
        None,
        _CallbackRoute.EVENT,
    )
    snapshot = RoomSnapshot(
        room_id=room_id,
        membership_epoch=0,
        own_user_id=ACCOUNT_ID,
        own_membership="join",
        encrypted=True,
        name="Settlement room",
        canonical_alias=None,
        topic=None,
        avatar_url=None,
        join_rule=None,
        room_version=None,
        guest_access=None,
        power_levels_json=None,
        members=(),
    )
    aggregate = RoomAggregateValue(
        RoomContinuity(room_id, 0, "join", None, None, None),
        1,
        1,
        None,
        snapshot,
    )
    decoy = RoomAggregateValue(
        RoomContinuity("!decoy:example.org", 0, "join", None, None, None),
        0,
        1,
        None,
        None,
    )
    aggregate_rows = _seed_room_aggregates(journal, (decoy, aggregate))
    stored = _seed_work(
        journal,
        record,
        ready_revision=1,
        ready_ordinal=0,
        metadata=metadata,
    )
    batch = journal.next_batch(max_records=1)
    assert batch is not None
    plaintext = _canonical_work_plaintext("event", record, metadata)
    reads, writes = _observe_owner_scopes(journal, monkeypatch)

    with _one_read_without_writes(
        database_path=bootstrap.database_path,
        statements=statements,
        reads=reads,
        writes=writes,
    ):
        settlement = journal._load_batch_settlement(batch)  # type: ignore[attr-defined]

    assert settlement is not None
    work, room = settlement
    assert type(work) is AuthenticatedWork
    assert work.value == record
    assert work.status == "ready"
    assert work.canonical_size == len(stored[11])
    assert work.metadata == metadata
    assert work.plaintext == plaintext
    assert work.frame_id == UUID(int=10_070)
    assert work.created_revision == 1
    assert room == aggregate

    changed_record = replace(
        record,
        source_json=source.replace(b"hello", b"changed"),
    )
    competing = batch_from_records(
        account_id=batch.account_id,
        device_id=batch.device_id,
        consumer_generation=batch.consumer_generation,
        stream_id=batch.ref.stream_id,
        sequence=batch.ref.sequence,
        created_revision=batch.created_revision,
        records=(changed_record,),
    )
    assert competing.ref != batch.ref
    with _one_read_without_writes(
        database_path=bootstrap.database_path,
        statements=statements,
        reads=reads,
        writes=writes,
    ):
        with pytest.raises(BatchIntegrityError):
            journal._load_batch_settlement(competing)  # type: ignore[attr-defined]

    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestWork SET payload_sha256 = ? WHERE work_id = ?",
            (_same_length_tamper(stored[12]), record.record_id),
        )
    with _one_read_without_writes(
        database_path=bootstrap.database_path,
        statements=statements,
        reads=reads,
        writes=writes,
    ):
        with pytest.raises(JournalIntegrityError):
            journal._load_batch_settlement(batch)  # type: ignore[attr-defined]

    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestWork SET payload_sha256 = ? WHERE work_id = ?",
            (stored[12], record.record_id),
        )
        journal._execute(
            "UPDATE NioIngestRoomAggregate SET payload_sha256 = ? WHERE room_id = ?",
            (_same_length_tamper(aggregate_rows[1][5]), room_id),
        )
    with _one_read_without_writes(
        database_path=bootstrap.database_path,
        statements=statements,
        reads=reads,
        writes=writes,
    ):
        with pytest.raises(JournalIntegrityError):
            journal._load_batch_settlement(batch)  # type: ignore[attr-defined]
    bootstrap.close()


def test_restore_view_authenticates_exact_states_in_room_order_without_writes(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statement_observer=statements.append)
    journal = bootstrap._journal
    leave_room = "!a-preserved:example.org"
    join_room = "!z-snapshotless:example.org"
    stale_snapshot = RoomSnapshot(
        room_id=leave_room,
        membership_epoch=3,
        own_user_id=ACCOUNT_ID,
        own_membership="join",
        encrypted=True,
        name="Preserved room",
        canonical_alias=None,
        topic=None,
        avatar_url=None,
        join_rule=None,
        room_version=None,
        guest_access=None,
        power_levels_json=None,
        members=(),
    )
    preserved_leave = RoomAggregateValue(
        RoomContinuity(leave_room, 4, "leave", None, None, None),
        8,
        1,
        None,
        stale_snapshot,
    )
    snapshotless_join = RoomAggregateValue(
        RoomContinuity(join_room, 0, "join", None, None, None),
        1,
        2,
        None,
        None,
    )
    aggregate_rows = _seed_room_aggregates(
        journal,
        (snapshotless_join, preserved_leave),
    )
    owner = journal.load_owner()
    authenticated = tuple(
        journal._load_room_aggregate(owner, room_id)
        for room_id in (leave_room, join_room)
    )
    assert all(item is not None for item in authenticated)
    assert tuple(item[1] for item in authenticated if item is not None) == (
        preserved_leave,
        snapshotless_join,
    )
    reads, writes = _observe_owner_scopes(journal, monkeypatch)

    with _one_read_without_writes(
        database_path=bootstrap.database_path,
        statements=statements,
        reads=reads,
        writes=writes,
    ):
        states = journal._load_room_restore_view()  # type: ignore[attr-defined]

    assert states == (
        (preserved_leave.continuity, stale_snapshot),
        (snapshotless_join.continuity, None),
    )

    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestRoomAggregate SET payload_sha256 = ? WHERE room_id = ?",
            (_same_length_tamper(aggregate_rows[1][5]), leave_room),
        )
    with _one_read_without_writes(
        database_path=bootstrap.database_path,
        statements=statements,
        reads=reads,
        writes=writes,
    ):
        with pytest.raises(JournalIntegrityError):
            journal._load_room_restore_view()  # type: ignore[attr-defined]

    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestRoomAggregate SET payload_sha256 = ?, "
            "updated_revision = ? WHERE room_id = ?",
            (aggregate_rows[1][5], 2, leave_room),
        )
    with _one_read_without_writes(
        database_path=bootstrap.database_path,
        statements=statements,
        reads=reads,
        writes=writes,
    ):
        with pytest.raises(JournalIntegrityError):
            journal._load_room_restore_view()  # type: ignore[attr-defined]
    bootstrap.close()


def test_direct_generation_batch_wire_is_canonical_and_strict() -> None:
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        stream_id=STREAM_ID,
        sequence=0,
        created_revision=1,
        records=(_event(),),
    )
    digest = hashlib.sha256(GOLDEN).digest()

    assert tuple(field.name for field in fields(SyncBatch)) == (
        "schema_version",
        "account_id",
        "device_id",
        "consumer_generation",
        "ref",
        "created_revision",
        "records",
    )
    assert canonical_batch_payload(batch) == GOLDEN
    assert batch.ref == BatchRef(
        STREAM_ID,
        0,
        uuid5(STREAM_ID, f"0:{digest.hex()}"),
        digest,
    )
    assert _batch_from_payload(GOLDEN) == batch

    old_wire = GOLDEN.replace(
        b'"consumer_generation":"22222222-2222-4222-8222-222222222222",',
        b'"consumer":{"journal_generation":"11111111-1111-4111-8111-111111111111",'
        b'"consumer_generation":"22222222-2222-4222-8222-222222222222"},',
    )
    with pytest.raises(ValueError):
        _batch_from_payload(old_wire)
    braced_stream = GOLDEN.replace(str(STREAM_ID).encode(), f"{{{STREAM_ID}}}".encode())
    extra_record_field = GOLDEN.replace(
        b'"clear_json":null}', b'"clear_json":null,"extra":null}'
    )
    reordered_record_fields = GOLDEN.replace(
        b'"record_type":"event","record_id":',
        b'"record_id":"00000000-0000-4000-8000-000000000001",'
        b'"record_type":"event","discarded":',
    ).replace(b'"discarded":"00000000-0000-4000-8000-000000000001",', b"")
    for noncanonical in (braced_stream, extra_record_field, reordered_record_fields):
        with pytest.raises(ValueError):
            _batch_from_payload(noncanonical)
    with pytest.raises((TypeError, ValueError)):
        BatchRef(STREAM_ID, -1, STREAM_ID, b"short")
    with pytest.raises((TypeError, ValueError)):
        batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer_generation=CONSUMER_GENERATION,
            stream_id=STREAM_ID,
            sequence=2**63,
            created_revision=1,
            records=(_event(),),
        )


@pytest.mark.parametrize(
    ("frontier", "assignment", "parameters"),
    (
        ("fresh", "delivery_next_sequence = 1.5", ()),
        ("acknowledged", "delivery_acknowledged_sha256 = 'not-a-blob'", ()),
        ("claimed", "delivery_outstanding_work_id = ?", (WORK_ID.upper(),)),
        ("claimed", "delivery_outstanding_ready_revision = 3", ()),
        ("claimed", "delivery_outstanding_ready_ordinal = 1.5", ()),
        ("claimed", "delivery_outstanding_batch_sha256 = 'not-a-blob'", ()),
    ),
)
def test_reopen_rejects_constraint_bypassed_typed_state_corruption(
    tmp_path: Path,
    frontier: str,
    assignment: str,
    parameters: tuple[object, ...],
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    database_path = bootstrap.database_path
    if frontier != "fresh":
        _seed_work(journal, _ready_event(100), ready_revision=1, ready_ordinal=0)
        batch = journal.next_batch()
        assert batch is not None
        if frontier == "acknowledged":
            journal.acknowledge_batch(batch.ref)
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(f"UPDATE NioIngestMeta SET {assignment}", parameters)

    with pytest.raises(JournalIntegrityError):
        journal.next_batch()
    bootstrap.close()
    with pytest.raises(JournalIntegrityError):
        _open(tmp_path)


def test_complete_outstanding_frontier_enforces_ready_revision_bound(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    _seed_work(journal, _ready_event(101), ready_revision=1, ready_ordinal=0)
    assert journal.next_batch() is not None
    bootstrap.close()

    with sqlite3.connect(bootstrap.database_path) as connection:
        with pytest.raises(sqlite3.IntegrityError):
            connection.execute(
                "UPDATE NioIngestMeta SET delivery_outstanding_ready_revision = "
                "revision + 1"
            )


@pytest.mark.parametrize(
    "corruption",
    ("schema_type", "stream_text"),
)
def test_reopen_rejects_noncanonical_owner_before_delivery_decode(
    tmp_path: Path,
    corruption: str,
) -> None:
    bootstrap = _open(tmp_path)
    database_path = bootstrap.database_path
    row = dict(_meta_row(database_path))
    if corruption == "schema_type":
        row["schema_version"] = 1.0
        with pytest.raises(JournalIntegrityError):
            bootstrap._journal._decode_owner_row(row)
        bootstrap.close()
        return
    bootstrap.close()
    with sqlite3.connect(database_path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "UPDATE NioIngestMeta SET stream_id = ?", (f"{{{row['stream_id']}}}",)
        )

    with pytest.raises(JournalIntegrityError):
        _open(tmp_path)


@pytest.mark.parametrize(
    ("field", "operation"),
    tuple(
        (field, operation)
        for field in ("stream_id", "consumer_generation", "writer_epoch")
        for operation in ("replay", "ack")
    ),
)
def test_live_delivery_rejects_noncanonical_owner_uuid_before_dml(
    tmp_path: Path,
    field: str,
    operation: str,
) -> None:
    transitions: list[str] = []
    bootstrap = _open(tmp_path, transition_statement_hook=transitions.append)
    journal = bootstrap._journal
    _seed_work(journal, _ready_event(102), ready_revision=1, ready_ordinal=0)
    batch = journal.next_batch()
    assert batch is not None
    transitions.clear()
    row = _meta_row(bootstrap.database_path)
    decoded_row = dict(row)
    decoded_row[field] = f"{{{row[field]}}}"
    with pytest.raises(JournalIntegrityError):
        journal._decode_owner_row(decoded_row)
    with sqlite3.connect(bootstrap.database_path) as connection:
        connection.execute(
            f"UPDATE NioIngestMeta SET {field} = ?", (f"{{{row[field]}}}",)
        )
    corrupted = _raw_delivery_graph(bootstrap.database_path)

    expected_error = (
        LocalProtocolError if field == "writer_epoch" else JournalIntegrityError
    )
    with pytest.raises(expected_error):
        (
            journal.next_batch()
            if operation == "replay"
            else journal.acknowledge_batch(batch.ref)
        )
    assert _raw_delivery_graph(bootstrap.database_path) == corrupted
    assert transitions == []
    bootstrap.close()


def test_no_ready_work_returns_none_without_writer_or_revision(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    transitions: list[str] = []
    bootstrap = _open(
        tmp_path,
        statement_observer=statements.append,
        transition_statement_hook=transitions.append,
    )
    journal = bootstrap._journal
    database_path = bootstrap.database_path
    _seed_work(
        journal,
        _ready_event(1),
        ready_revision=None,
        ready_ordinal=None,
        status="held",
    )
    before = _raw_delivery_graph(database_path)
    statements.clear()

    assert journal.next_batch() is None
    assert _raw_delivery_graph(database_path) == before
    assert transitions == []
    assert not any(
        sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for sql in statements
    )
    assert any(
        "FROM NioIngestWork WHERE account_id = " in sql
        and "AND status = 'ready'" in sql
        and "ORDER BY ready_revision, ready_ordinal, work_id LIMIT 1" in sql
        for sql in statements
    )
    assert not any("FROM NioIngestWork LIMIT 20001" in sql for sql in statements)
    bootstrap.close()


def test_claims_only_minimum_ready_key_and_replays_identically(
    tmp_path: Path,
) -> None:
    statements: list[str] = []
    transitions: list[str] = []
    bootstrap = _open(
        tmp_path,
        statement_observer=statements.append,
        transition_statement_hook=transitions.append,
    )
    journal = bootstrap._journal
    database_path = bootstrap.database_path
    later_revision = _seed_work(
        journal, _ready_event(1), ready_revision=2, ready_ordinal=0
    )
    later_ordinal = _seed_work(
        journal, _ready_event(2), ready_revision=1, ready_ordinal=1
    )
    selected = _seed_work(journal, _ready_event(3), ready_revision=1, ready_ordinal=0)
    before_work = tuple(row for row in _raw_delivery_graph(database_path)[1:])
    statements.clear()

    batch = journal.next_batch(max_records=256)

    assert batch is not None
    assert batch.records == (_ready_event(3),)
    assert batch.ref.sequence == 0
    assert batch.created_revision == 1
    assert _delivery_frontier(database_path) == (
        1,
        None,
        _ready_event(3).record_id,
        1,
        0,
        batch.ref.sha256,
    )
    assert tuple(row for row in _raw_delivery_graph(database_path)[1:]) == before_work
    assert transitions == ["delivery_claim_meta_cas", "before_commit", "commit"]
    selects = [sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]
    assert any(
        "FROM NioIngestWork WHERE account_id = " in sql
        and "AND status = 'ready'" in sql
        and "ORDER BY ready_revision, ready_ordinal, work_id LIMIT 1" in sql
        for sql in selects
    )
    assert not any("FROM NioIngestWork LIMIT 20001" in sql for sql in selects)

    frozen_payload = canonical_batch_payload(batch)
    frozen_graph = _raw_delivery_graph(database_path)
    statements.clear()
    transitions.clear()
    replay = journal.next_batch(max_records=1, max_canonical_bytes=1)
    assert replay == batch
    assert canonical_batch_payload(replay) == frozen_payload
    assert _raw_delivery_graph(database_path) == frozen_graph
    assert transitions == []
    assert not any(
        sql.lstrip().upper().startswith(("INSERT", "UPDATE", "DELETE"))
        for sql in statements
    )
    assert later_revision != selected != later_ordinal

    bootstrap.close()
    reopened = _open(tmp_path)
    assert reopened._journal.next_batch() == batch
    appended = _seed_work(
        reopened._journal,
        _ready_event(4),
        ready_revision=reopened._journal.load_owner().revision + 1,
        ready_ordinal=0,
    )
    assert reopened._journal.next_batch() == batch
    reopened._journal.acknowledge_batch(batch.ref)
    assert appended in _raw_delivery_graph(reopened.database_path)[1:]
    reopened.close()


def test_one_delivery_cycle_authenticates_constant_work_with_large_ready_inventory(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    for index in range(1, 501):
        record = replace(
            _ready_event(index),
            kind=RecordKind.PRESENCE,
            room_id=None,
            membership_epoch=None,
            room_sequence=None,
            provenance=None,
        )
        _seed_work(
            journal,
            record,
            ready_revision=1,
            ready_ordinal=index - 1,
        )

    authenticated_work = 0
    real_payload = journal._payload

    def counting_payload(owner: object, *args: object, **kwargs: object) -> object:
        nonlocal authenticated_work
        if args and args[0] == "NioIngestWork" and len(args) >= 3:
            authenticated_work += 1
        return real_payload(owner, *args, **kwargs)  # type: ignore[arg-type]

    monkeypatch.setattr(journal, "_payload", counting_payload)

    batch = journal.next_batch(max_records=1)
    assert batch is not None
    assert batch.records == (
        replace(
            _ready_event(1),
            kind=RecordKind.PRESENCE,
            room_id=None,
            membership_epoch=None,
            room_sequence=None,
            provenance=None,
        ),
    )
    assert journal._load_batch_settlement(batch) is not None  # type: ignore[attr-defined]
    journal.acknowledge_batch(batch.ref)

    assert 0 < authenticated_work <= 5
    bootstrap.close()


def test_frame_work_lookup_authenticates_only_one_matching_row(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    statements: list[str] = []
    bootstrap = _open(tmp_path, statement_observer=statements.append)
    journal = bootstrap._journal
    target_record = _ready_event(1)
    target = _seed_work(
        journal,
        target_record,
        ready_revision=1,
        ready_ordinal=0,
    )
    for index in range(2, 502):
        _seed_work(
            journal,
            _ready_event(index),
            ready_revision=1,
            ready_ordinal=index - 1,
        )

    decoded_work_ids: list[str] = []
    real_decode = journal._decode_task3_work_row

    def record_decode(owner: object, row: tuple[object, ...]) -> AuthenticatedWork:
        decoded_work_ids.append(str(row[1]))
        return real_decode(owner, row)  # type: ignore[arg-type]

    monkeypatch.setattr(journal, "_decode_task3_work_row", record_decode)
    owner = journal.load_owner()
    statements.clear()

    with journal._owner.read():
        loaded = journal._load_frame_work(owner, UUID(str(target[4])))

    assert loaded is not None
    assert loaded[0] == target
    assert loaded[1].value == target_record
    assert decoded_work_ids == [target_record.record_id]
    selects = [sql for sql in statements if sql.lstrip().upper().startswith("SELECT")]
    assert any(
        "FROM NioIngestWork WHERE account_id = " in sql
        and "AND frame_id = " in sql
        and "LIMIT 1" in sql
        for sql in selects
    )
    assert not any("FROM NioIngestWork LIMIT 20001" in sql for sql in selects)
    bootstrap.close()


def test_frame_work_lookup_returns_none_without_decoding_unrelated_work(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    for index in range(1, 51):
        _seed_work(
            journal,
            _ready_event(index),
            ready_revision=1,
            ready_ordinal=index - 1,
        )

    monkeypatch.setattr(
        journal,
        "_decode_task3_work_row",
        lambda *_args, **_kwargs: pytest.fail("unrelated Work was decoded"),
    )
    owner = journal.load_owner()
    with journal._owner.read():
        assert journal._load_frame_work(owner, UUID(int=9_999)) is None
    bootstrap.close()


def test_frame_work_lookup_rejects_corrupt_matching_row(tmp_path: Path) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    stored = _seed_work(
        journal,
        _ready_event(1),
        ready_revision=1,
        ready_ordinal=0,
    )
    with journal._owner.journal_write():
        journal._execute(
            "UPDATE NioIngestWork SET payload = ? WHERE account_id = ? AND work_id = ?",
            (_same_length_tamper(stored[11]), journal.account_id, stored[1]),
        )

    owner = journal.load_owner()
    with journal._owner.read(), pytest.raises(JournalIntegrityError):
        journal._load_frame_work(owner, UUID(str(stored[4])))
    bootstrap.close()


def test_frame_work_lookup_rejects_foreign_account_work(tmp_path: Path) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    stored = _seed_work(
        journal,
        _ready_event(1),
        ready_revision=1,
        ready_ordinal=0,
    )
    with sqlite3.connect(bootstrap.database_path) as connection:
        connection.execute(
            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("@mallory:example.org", *stored[1:]),
        )

    owner = journal.load_owner()
    with journal._owner.read(), pytest.raises(JournalIntegrityError):
        journal._load_frame_work(owner, UUID(str(stored[4])))
    bootstrap.close()


def test_claim_enforces_exact_caller_limits_before_writer(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    database_path = bootstrap.database_path
    record = _ready_event(5, padding=32)
    _seed_work(journal, record, ready_revision=1, ready_ordinal=0)
    owner = journal.load_owner()
    expected = batch_from_records(
        account_id=owner.account_id,
        device_id=owner.device_id,
        consumer_generation=owner.consumer_generation,
        stream_id=owner.stream_id,
        sequence=0,
        created_revision=1,
        records=(record,),
    )
    exact = len(canonical_batch_payload(expected))
    before = _raw_delivery_graph(database_path)

    for name, invalid in (
        ("max_records", 0),
        ("max_records", 257),
        ("max_records", True),
        ("max_canonical_bytes", 0),
        ("max_canonical_bytes", 16 * 1024 * 1024 + 1),
        ("max_canonical_bytes", True),
    ):
        with pytest.raises(LocalProtocolError):
            journal.next_batch(**{name: invalid})
        assert _raw_delivery_graph(database_path) == before
    with pytest.raises(LocalProtocolError):
        journal.next_batch(max_canonical_bytes=exact - 1)
    assert _raw_delivery_graph(database_path) == before
    assert journal.next_batch(max_canonical_bytes=exact) == expected
    bootstrap.close()


@pytest.mark.parametrize("exhaustion", ("sequence", "revision"))
def test_claim_refuses_sequence_or_revision_exhaustion_before_dml(
    tmp_path: Path,
    exhaustion: str,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    database_path = bootstrap.database_path
    _seed_work(journal, _ready_event(6), ready_revision=1, ready_ordinal=0)
    with journal._owner.journal_write():
        if exhaustion == "sequence":
            journal._execute(
                "UPDATE NioIngestMeta SET delivery_next_sequence = ?, "
                "delivery_acknowledged_sha256 = ?",
                (2**63 - 1, b"a" * 32),
            )
        else:
            journal._execute("UPDATE NioIngestMeta SET revision = ?", (2**63 - 1,))
    before = _raw_delivery_graph(database_path)

    with pytest.raises(LocalProtocolError):
        journal.next_batch()
    assert _raw_delivery_graph(database_path) == before
    bootstrap.close()


@pytest.mark.parametrize(
    "boundary",
    ("delivery_claim_meta_cas", "before_commit", "commit"),
)
def test_claim_failure_exposes_only_unclaimed_or_complete_claim(
    tmp_path: Path,
    boundary: str,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    record = _ready_event(7)
    _seed_work(journal, record, ready_revision=1, ready_ordinal=0)
    old_frontier = _delivery_frontier(bootstrap.database_path)
    old_work = _raw_delivery_graph(bootstrap.database_path)[1:]

    def fail(label: str) -> None:
        if label == boundary:
            raise RuntimeError(boundary)

    journal.set_transition_statement_hook(fail)
    with pytest.raises(RuntimeError, match=boundary):
        journal.next_batch()
    bootstrap.close()
    reopened = _open(tmp_path)
    if boundary == "commit":
        assert _delivery_frontier(reopened.database_path)[2] == record.record_id
    else:
        assert _delivery_frontier(reopened.database_path) == old_frontier
        assert _raw_delivery_graph(reopened.database_path)[1:] == old_work
    batch = reopened._journal.next_batch()
    assert batch is not None and batch.records == (record,)
    reopened._journal.acknowledge_batch(batch.ref)
    reopened.close()


@pytest.mark.parametrize("operation", ("replay", "ack"))
@pytest.mark.parametrize("corruption", ("removed", "status", "full_key", "value"))
def test_claimed_work_corruption_fails_closed_without_delivery_dml(
    tmp_path: Path,
    operation: str,
    corruption: str,
) -> None:
    transitions: list[str] = []
    bootstrap = _open(tmp_path, transition_statement_hook=transitions.append)
    journal = bootstrap._journal
    record = _ready_event(8)
    stored = _seed_work(journal, record, ready_revision=1, ready_ordinal=0)
    batch = journal.next_batch()
    assert batch is not None
    transitions.clear()

    if corruption == "removed":
        with journal._owner.journal_write():
            journal._execute("DELETE FROM NioIngestWork")
    else:
        clear = list(stored[1:11])
        replacement = record
        if corruption == "status":
            clear[2], clear[7], clear[8] = "held", None, None
        elif corruption == "full_key":
            replacement = replace(record, record_id=str(UUID(int=800)))
            clear[0], clear[7], clear[8] = (
                replacement.record_id,
                journal.load_owner().revision,
                8,
            )
        else:
            replacement = replace(record, source_json=b'{"body":"changed"}')
        _replace_work(journal, stored, tuple(clear), replacement)
    corrupted = _raw_delivery_graph(bootstrap.database_path)

    with pytest.raises(JournalIntegrityError):
        (
            journal.next_batch()
            if operation == "replay"
            else journal.acknowledge_batch(batch.ref)
        )
    assert _raw_delivery_graph(bootstrap.database_path) == corrupted
    assert transitions == []
    bootstrap.close()


def test_foreign_account_work_is_rejected_by_global_inventory_before_claim(
    tmp_path: Path,
) -> None:
    transitions: list[str] = []
    bootstrap = _open(tmp_path, transition_statement_hook=transitions.append)
    journal = bootstrap._journal
    stored = _seed_work(journal, _ready_event(9), ready_revision=1, ready_ordinal=0)
    with sqlite3.connect(bootstrap.database_path) as connection:
        connection.execute(
            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            ("@mallory:example.org", *stored[1:]),
        )
    before = _raw_delivery_graph(bootstrap.database_path)

    with pytest.raises(JournalIntegrityError):
        journal.next_batch()
    assert _raw_delivery_graph(bootstrap.database_path) == before
    assert transitions == []
    bootstrap.close()


def test_full_inventory_rejects_non_text_work_id_before_sort_without_mutation(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    stored = list(
        _seed_work(journal, _ready_event(10), ready_revision=1, ready_ordinal=0)
    )
    stored[1] = b"non-text-work-id"
    stored[9] = 1
    with sqlite3.connect(bootstrap.database_path) as connection:
        connection.execute("PRAGMA ignore_check_constraints = ON")
        connection.execute(
            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            stored,
        )
        assert connection.execute(
            "SELECT typeof(work_id) FROM NioIngestWork WHERE typeof(work_id) <> 'text'"
        ).fetchone() == ("blob",)
    before = _raw_delivery_graph(bootstrap.database_path)
    owner = journal.load_owner()

    with journal._owner.read(), pytest.raises(JournalIntegrityError):
        journal._load_task3_work_inventory(owner)

    assert _raw_delivery_graph(bootstrap.database_path) == before
    bootstrap.close()


@pytest.mark.parametrize("race", ("meta", "state", "inventory", "ready_head"))
def test_claim_rejects_coherent_writer_entry_snapshot_races(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race: str,
) -> None:
    transitions: list[str] = []
    bootstrap = _open(tmp_path, transition_statement_hook=transitions.append)
    journal = bootstrap._journal
    _seed_work(journal, _ready_event(50), ready_revision=2, ready_ordinal=0)
    real_transaction = journal._transaction
    raced: tuple[tuple[object, ...], ...] = ()

    @contextmanager
    def racing_transaction():
        nonlocal raced
        if race in ("meta", "state"):
            owner = journal.load_owner()
            with journal._owner.journal_write():
                if race == "meta":
                    journal._execute(
                        "UPDATE NioIngestMeta SET revision = ?", (owner.revision + 1,)
                    )
                else:
                    journal._execute(
                        "UPDATE NioIngestMeta SET revision = ?, "
                        "delivery_next_sequence = 1, "
                        "delivery_acknowledged_sha256 = ?",
                        (owner.revision + 1, b"r" * 32),
                    )
        elif race == "inventory":
            _seed_work(
                journal,
                _ready_event(51),
                ready_revision=None,
                ready_ordinal=None,
                status="held",
            )
        else:
            _seed_work(journal, _ready_event(52), ready_revision=1, ready_ordinal=0)
        raced = _raw_delivery_graph(bootstrap.database_path)
        with real_transaction():
            yield

    monkeypatch.setattr(journal, "_transaction", racing_transaction)
    with pytest.raises(JournalConflictError):
        journal.next_batch()
    assert _raw_delivery_graph(bootstrap.database_path) == raced
    assert transitions == []
    bootstrap.close()


@pytest.mark.parametrize("race", ("meta", "inventory"))
def test_ack_rejects_coherent_writer_entry_snapshot_races(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
    race: str,
) -> None:
    transitions: list[str] = []
    bootstrap = _open(tmp_path, transition_statement_hook=transitions.append)
    journal = bootstrap._journal
    _seed_work(journal, _ready_event(60), ready_revision=1, ready_ordinal=0)
    batch = journal.next_batch()
    assert batch is not None
    transitions.clear()
    real_transaction = journal._transaction
    raced: tuple[tuple[object, ...], ...] = ()

    @contextmanager
    def racing_transaction():
        nonlocal raced
        if race == "meta":
            revision = journal.load_owner().revision
            with journal._owner.journal_write():
                journal._execute(
                    "UPDATE NioIngestMeta SET revision = ?", (revision + 1,)
                )
        else:
            _seed_work(
                journal,
                _ready_event(61),
                ready_revision=None,
                ready_ordinal=None,
                status="held",
            )
        raced = _raw_delivery_graph(bootstrap.database_path)
        with real_transaction():
            yield

    monkeypatch.setattr(journal, "_transaction", racing_transaction)
    with pytest.raises(JournalConflictError):
        journal.acknowledge_batch(batch.ref)
    assert _raw_delivery_graph(bootstrap.database_path) == raced
    assert transitions == []
    bootstrap.close()


def test_acknowledgement_enforces_fifo_integrity_and_retains_other_work(
    tmp_path: Path,
) -> None:
    transitions: list[str] = []
    bootstrap = _open(tmp_path, transition_statement_hook=transitions.append)
    journal = bootstrap._journal
    database_path = bootstrap.database_path
    selected = _seed_work(journal, _ready_event(10), ready_revision=1, ready_ordinal=0)
    later = _seed_work(journal, _ready_event(11), ready_revision=2, ready_ordinal=0)
    held = _seed_work(
        journal,
        _ready_event(12),
        ready_revision=None,
        ready_ordinal=None,
        status="held",
    )
    batch = journal.next_batch()
    assert batch is not None
    before = _raw_delivery_graph(database_path)
    transitions.clear()

    with pytest.raises(LocalProtocolError):
        journal.acknowledge_batch(object())  # type: ignore[arg-type]
    with pytest.raises(LocalProtocolError):
        journal.acknowledge_batch(
            BatchRef(UUID(int=999), 0, batch.ref.batch_id, batch.ref.sha256)
        )
    with pytest.raises(LocalProtocolError):
        journal.acknowledge_batch(
            BatchRef(batch.ref.stream_id, 1, batch.ref.batch_id, batch.ref.sha256)
        )
    with pytest.raises(BatchIntegrityError):
        journal.acknowledge_batch(
            BatchRef(batch.ref.stream_id, 0, batch.ref.batch_id, b"z" * 32)
        )
    with pytest.raises(BatchIntegrityError):
        journal.acknowledge_batch(
            BatchRef(batch.ref.stream_id, 0, UUID(int=998), batch.ref.sha256)
        )
    assert _raw_delivery_graph(database_path) == before
    assert transitions == []

    journal.acknowledge_batch(batch.ref)
    assert transitions == [
        "delivery_work_delete",
        "delivery_ack_meta_cas",
        "before_commit",
        "commit",
    ]
    graph = _raw_delivery_graph(database_path)
    assert selected not in graph
    assert later in graph and held in graph
    assert _delivery_frontier(database_path) == (
        1,
        batch.ref.sha256,
        None,
        None,
        None,
        None,
    )

    transitions.clear()
    acknowledged = _raw_delivery_graph(database_path)
    journal.acknowledge_batch(batch.ref)
    assert _raw_delivery_graph(database_path) == acknowledged
    assert transitions == []
    bootstrap.close()


def test_ack_duplicate_is_immediate_while_next_sequence_is_outstanding(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    first = _ready_event(20)
    _seed_work(journal, first, ready_revision=1, ready_ordinal=0)
    batch_zero = journal.next_batch()
    assert batch_zero is not None
    journal.acknowledge_batch(batch_zero.ref)
    bootstrap.close()

    reopened = _open(tmp_path)
    journal = reopened._journal
    revision = journal.load_owner().revision + 1
    _seed_work(journal, _ready_event(21), ready_revision=revision, ready_ordinal=0)
    batch_one = journal.next_batch()
    assert batch_one is not None and batch_one.ref.sequence == 1
    outstanding = _raw_delivery_graph(reopened.database_path)
    journal.acknowledge_batch(batch_zero.ref)
    assert _raw_delivery_graph(reopened.database_path) == outstanding
    journal.acknowledge_batch(batch_one.ref)
    assert _delivery_frontier(reopened.database_path) == (
        2,
        batch_one.ref.sha256,
        None,
        None,
        None,
        None,
    )
    acknowledged = _raw_delivery_graph(reopened.database_path)
    with pytest.raises(LocalProtocolError):
        journal.acknowledge_batch(batch_zero.ref)
    assert _raw_delivery_graph(reopened.database_path) == acknowledged
    reopened.close()


@pytest.mark.parametrize(
    "boundary",
    ("delivery_work_delete", "delivery_ack_meta_cas", "before_commit", "commit"),
)
def test_acknowledgement_failure_exposes_only_old_or_complete_new_graph(
    tmp_path: Path,
    boundary: str,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    _seed_work(journal, _ready_event(30), ready_revision=1, ready_ordinal=0)
    batch = journal.next_batch()
    assert batch is not None
    old_frontier = _delivery_frontier(bootstrap.database_path)
    old_work = _raw_delivery_graph(bootstrap.database_path)[1:]

    def fail(label: str) -> None:
        if label == boundary:
            raise RuntimeError(boundary)

    journal.set_transition_statement_hook(fail)
    with pytest.raises(RuntimeError, match=boundary):
        journal.acknowledge_batch(batch.ref)
    bootstrap.close()
    reopened = _open(tmp_path)
    if boundary == "commit":
        assert reopened._journal.next_batch() is None
        reopened._journal.acknowledge_batch(batch.ref)
    else:
        assert _delivery_frontier(reopened.database_path) == old_frontier
        assert _raw_delivery_graph(reopened.database_path)[1:] == old_work
        assert reopened._journal.next_batch() == batch
        reopened._journal.acknowledge_batch(batch.ref)
    assert reopened._journal.next_batch() is None
    reopened.close()


def test_acknowledgement_refuses_revision_exhaustion_before_dml(
    tmp_path: Path,
) -> None:
    bootstrap = _open(tmp_path)
    journal = bootstrap._journal
    _seed_work(journal, _ready_event(40), ready_revision=1, ready_ordinal=0)
    batch = journal.next_batch()
    assert batch is not None
    with journal._owner.journal_write():
        journal._execute("UPDATE NioIngestMeta SET revision = ?", (2**63 - 1,))
    before = _raw_delivery_graph(bootstrap.database_path)

    with pytest.raises(LocalProtocolError):
        journal.acknowledge_batch(batch.ref)
    assert _raw_delivery_graph(bootstrap.database_path) == before
    bootstrap.close()
