"""On-disk restart and transaction kill-point coverage for ingestion v1."""

import hashlib
from pathlib import Path
from uuid import UUID

import pytest

from nio.event_provenance import TimelineEventProvenance
from nio.ingest import (
    ConsumerBinding,
    ConsumerBootstrap,
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RecordOrigin,
    RoomHydrationStatus,
    RoomSnapshot,
    TransportKind,
)
from nio.ingest.serialization import _loss_id, batch_from_records
from nio.ingest.state import (
    AckOutcome,
    JournalTransition,
    LaneStatus,
    ReadyRecord,
    RoomLane,
    RoomState,
    SourceState,
    StagedFrame,
)
from nio.store.sync_journal import open_ingestion_store
from nio.store.sync_journal_schema import SCHEMA_SQL

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")
FRAME_ID = UUID("55555555-5555-5555-5555-555555555555")


class InjectedCrash(RuntimeError):
    pass


@pytest.mark.parametrize("kill_after", range(1, len(SCHEMA_SQL) + 3))
def test_fresh_schema_creation_is_atomic_at_every_statement(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"schema-kill-{kill_after}"
    seen: list[str] = []

    def inject(label: str) -> None:
        seen.append(label)
        if len(seen) == kill_after:
            raise InjectedCrash(label)

    with pytest.raises(InjectedCrash):
        open_ingestion_store(
            store_path,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            database_name="journal.db",
            schema_statement_hook=inject,
        )

    database_path = store_path / "journal.db"
    if database_path.exists():
        import sqlite3

        with sqlite3.connect(database_path) as connection:
            tables = connection.execute(
                "SELECT name FROM sqlite_master WHERE type = 'table'"
            ).fetchall()
        assert tables == []

    reopened = open_ingestion_store(
        store_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        assert reopened.schema_version == 1
    finally:
        reopened.close()


def _consumer(bootstrap) -> ConsumerBootstrap:
    return ConsumerBootstrap(
        bootstrap.binding_operation_id,
        ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        bootstrap.next_batch_sequence,
        (),
        hashlib.sha256(b"[]").digest(),
    )


def _one_room_consumer(bootstrap) -> ConsumerBootstrap:
    room_ids = ("!baseline:example.org",)
    return ConsumerBootstrap(
        bootstrap.binding_operation_id,
        ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        bootstrap.next_batch_sequence,
        room_ids,
        hashlib.sha256(b'["!baseline:example.org"]').digest(),
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kill_after", range(1, 6))
async def test_consumer_attach_is_atomic_at_every_persisted_statement(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"attach-kill-{kill_after}"
    bootstrap = open_ingestion_store(
        store_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _one_room_consumer(bootstrap)
    observed: list[str] = []

    def inject(label: str) -> None:
        observed.append(label)
        if len(observed) == kill_after:
            raise InjectedCrash(label)

    bootstrap.journal.set_transition_statement_hook(inject)
    with pytest.raises(InjectedCrash):
        await bootstrap.attach_consumer(consumer)
    bootstrap.close()

    reopened = open_ingestion_store(
        store_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        owner = reopened.journal.load_owner()
        assert owner.binding is None
        assert owner.revision == 0
        for table in (
            "NioIngestRoomState",
            "NioIngestRoomLane",
            "NioIngestReadyRecord",
            "NioIngestLoss",
        ):
            assert (
                reopened.journal.connection.execute(
                    f'SELECT COUNT(*) FROM "{table}"'
                ).fetchone()[0]
                == 0
            )
    finally:
        reopened.close()


def _snapshot(room_id: str) -> RoomSnapshot:
    return RoomSnapshot(
        room_id,
        1,
        ACCOUNT_ID,
        "join",
        True,
        "Room",
        None,
        None,
        None,
        "invite",
        "11",
        "forbidden",
        None,
        (),
    )


def _event(record_id: str, frame_index: int) -> EventRecord:
    return EventRecord(
        record_id,
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 1, 1, frame_index),
        "!room:example.org",
        1,
        frame_index,
        f"${record_id}",
        TimelineEventProvenance.LIVE,
        b'{"type":"m.room.message"}',
        None,
    )


def _transition(bootstrap) -> tuple[JournalTransition, LossRecord]:
    ready_event = _event("ready", 0)
    batch_event = _event("batch", 1)
    incomplete_loss = LossRecord(
        "",
        RecordOrigin(TransportKind.CLASSIC, 1, 1, 2),
        "!room:example.org",
        1,
        LossReason.FETCH_FAILED,
        LossBoundary("$prior", 1234, "s1", "s2"),
        b'{"errcode":"M_FORBIDDEN"}',
    )
    loss = LossRecord(
        _loss_id(bootstrap.stream_id, incomplete_loss),
        incomplete_loss.origin,
        incomplete_loss.room_id,
        incomplete_loss.membership_epoch,
        incomplete_loss.reason,
        incomplete_loss.boundary,
        incomplete_loss.detail_json,
    )
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=2,
        records=(batch_event,),
    )
    transition = JournalTransition(
        source_state=SourceState(
            1,
            TransportKind.CLASSIC,
            b'{"next_batch":"s2"}',
            2,
            True,
        ),
        room_states=(
            RoomState(
                "!room:example.org",
                1,
                2,
                RoomHydrationStatus.READY,
                _snapshot("!room:example.org"),
            ),
        ),
        room_lanes=(RoomLane("!room:example.org", 1, LaneStatus.ACTIVE),),
        ready_records=(ReadyRecord(0, ready_event, FRAME_ID),),
        frames=(StagedFrame(FRAME_ID, 1, 1, b'{"raw":true}'),),
        batches=(batch,),
        losses=(loss,),
    )
    return transition, loss


@pytest.mark.asyncio
@pytest.mark.parametrize("kill_after", range(1, 9))
async def test_transition_rolls_back_after_each_sql_statement_and_restart(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"kill-{kill_after}"
    bootstrap = open_ingestion_store(
        store_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    await bootstrap.attach_consumer(_consumer(bootstrap))
    transition, loss = _transition(bootstrap)
    observed: list[str] = []

    def inject(label: str) -> None:
        observed.append(label)
        if len(observed) == kill_after:
            raise InjectedCrash(label)

    bootstrap.journal.set_transition_statement_hook(inject)
    with pytest.raises(InjectedCrash):
        bootstrap.journal.commit(
            expected_revision=1,
            writer_epoch=bootstrap.journal.writer_epoch,
            transition=transition,
        )
    bootstrap.close()

    reopened = open_ingestion_store(
        store_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(_consumer(reopened))
        assert reopened.journal.load_owner().revision == 1
        assert reopened.journal.load_owner().next_ready_order == 0
        assert reopened.journal.load_owner().next_batch_sequence == 1
        assert reopened.journal.load_source() is None
        assert reopened.journal.load_rooms(frozenset({"!room:example.org"})) == {}
        assert reopened.journal.load_ready_heads(limit=10) == ()
        assert reopened.journal.load_frame(FRAME_ID) is None
        assert reopened.journal.oldest_unacknowledged() is None
        assert reopened.journal.load_loss(loss.loss_id) is None
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kill_after", range(1, 4))
async def test_acknowledgement_rolls_back_frontier_row_and_prior_delete(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"ack-kill-{kill_after}"
    bootstrap = open_ingestion_store(
        store_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer(bootstrap)
    await bootstrap.attach_consumer(consumer)
    batches = tuple(
        batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=sequence,
            created_revision=2,
            records=(_event(f"ack-{sequence}", sequence),),
        )
        for sequence in (1, 2)
    )
    bootstrap.journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap.journal.writer_epoch,
        transition=JournalTransition(batches=batches),
    )
    assert bootstrap.journal.acknowledge(batches[0].ref) is AckOutcome.ACKNOWLEDGED
    observed: list[str] = []

    def inject(label: str) -> None:
        observed.append(label)
        if len(observed) == kill_after:
            raise InjectedCrash(label)

    bootstrap.journal.set_transition_statement_hook(inject)
    with pytest.raises(InjectedCrash):
        bootstrap.journal.acknowledge(batches[1].ref)
    bootstrap.close()

    reopened = open_ingestion_store(
        store_path,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        owner = reopened.journal.load_owner()
        assert owner.last_acked_sequence == 1
        assert owner.revision == 3
        assert (
            reopened.journal.acknowledge(batches[0].ref)
            is AckOutcome.ALREADY_ACKNOWLEDGED
        )
        assert reopened.journal.oldest_unacknowledged() == batches[1]
        rows = reopened.journal.connection.execute(
            "SELECT sequence, acknowledged_revision FROM NioIngestBatch "
            "WHERE account_id = ? ORDER BY sequence",
            (ACCOUNT_ID,),
        ).fetchall()
        assert [(row[0], row[1] is not None) for row in rows] == [
            (1, True),
            (2, False),
        ]
    finally:
        reopened.close()
