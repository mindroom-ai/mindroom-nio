"""On-disk restart and transaction kill-point coverage for ingestion v1."""

import asyncio
import hashlib
import multiprocessing
import os
from dataclasses import replace
from pathlib import Path
from typing import Callable
from uuid import UUID, uuid5

import pytest

from nio.event_provenance import TimelineEventProvenance
from nio.exceptions import LocalProtocolError
from nio.ingest import (
    BatchRef,
    ConsumerBinding,
    ConsumerBootstrap,
    EventRecord,
    LossRecord,
    RecordKind,
    RecordOrigin,
    RoomHydrationStatus,
    RoomSnapshot,
    TransportKind,
)
from nio.ingest.config import ClassicSourceConfig
from nio.ingest.effects import (
    MembershipAction,
    MembershipDeliveryState,
    MembershipOperationRef,
    MembershipOperationResolution,
    MembershipOperationResolutionOutcome,
    MembershipRequest,
    PersistedNetworkEffect,
    RecoveryRequest,
)
from nio.ingest.membership import MembershipBaseline
from nio.ingest.ports import NetworkRequest, StagedSourceResponse
from nio.ingest.recovery import RecoveryGap
from nio.ingest.serialization import (
    _canonical_json,
    _record_to_dict,
    batch_from_records,
)
from nio.ingest.state import (
    AckOutcome,
    BatchMaterialization,
    ConsumerAttachStatus,
    JournalTransition,
    LaneRecord,
    LaneRecordKey,
    LaneRecordSection,
    LaneStatus,
    ReadyRecord,
    ReadyRecordKey,
    ReleasePhase,
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
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")


CRASH_EXIT_CODE = 86


def _exit_at_statement(kill_after: int) -> Callable[[str], None]:
    observed = 0

    def exit_process(_label: str) -> None:
        nonlocal observed
        observed += 1
        if observed == kill_after:
            os._exit(CRASH_EXIT_CODE)

    return exit_process


def _assert_process_crashed(
    target: Callable[..., None],
    *args: object,
) -> None:
    process = multiprocessing.get_context("spawn").Process(target=target, args=args)
    process.start()
    process.join(timeout=15)
    if process.is_alive():
        process.terminate()
        process.join(timeout=5)
        pytest.fail("crash-injection child did not exit")
    assert process.exitcode == CRASH_EXIT_CODE


def _kill_during_schema(store_path: Path, kill_after: int) -> None:
    open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
        schema_statement_hook=_exit_at_statement(kill_after),
    )


@pytest.mark.parametrize("kill_after", range(1, len(SCHEMA_SQL) + 4))
def test_fresh_schema_creation_is_atomic_at_every_statement(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"schema-kill-{kill_after}"
    _assert_process_crashed(_kill_during_schema, store_path, kill_after)

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
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        database_name="journal.db",
    )
    try:
        assert reopened.schema_version == 1
        assert reopened._journal.load_source() == SourceState(
            0,
            TransportKind.CLASSIC,
            b'{"next_batch":null}',
            1,
            True,
        )
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


def _many_room_consumer(bootstrap, count: int) -> ConsumerBootstrap:
    room_ids = tuple(f"!baseline-{ordinal:05d}:example.org" for ordinal in range(count))
    return ConsumerBootstrap(
        bootstrap.binding_operation_id,
        ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION),
        bootstrap.next_batch_sequence,
        room_ids,
        hashlib.sha256(_canonical_json(list(room_ids))).digest(),
    )


def _recovery_effect_graph(
    stream_id: UUID,
) -> tuple[PersistedNetworkEffect, RoomState, RoomLane]:
    room_id = "!baseline:example.org"
    gap_id = UUID("44444444-4444-4444-4444-444444444444")
    effect_id = uuid5(gap_id, "page:1")
    effect = PersistedNetworkEffect(
        RecoveryRequest(
            effect_id,
            stream_id,
            TransportKind.CLASSIC,
            room_id,
            0,
            gap_id,
            1,
            "cursor-1",
            "target-token",
            100,
            30_000,
        ),
        0,
        None,
        None,
    )
    state = RoomState(
        room_id,
        0,
        0,
        RoomHydrationStatus.PENDING,
        None,
        MembershipBaseline(room_id, 4, 0, "start-token", "$member"),
    )
    gap = RecoveryGap(
        gap_id,
        room_id,
        4,
        0,
        RecordOrigin(TransportKind.CLASSIC, 4, 9, 2),
        "$member",
        "start-token",
        "target-token",
        "cursor-1",
        ("start-token", "cursor-1"),
        1,
        8,
        effect_id,
    )
    lane = RoomLane(
        room_id,
        0,
        LaneStatus.ACTIVE,
        release_phase=ReleasePhase.RECOVERING,
        recovery_gap=gap,
    )
    return effect, state, lane


def _membership_effect(stream_id: UUID) -> PersistedNetworkEffect:
    return PersistedNetworkEffect(
        MembershipRequest(
            UUID("55555555-5555-5555-5555-555555555555"),
            stream_id,
            TransportKind.CLASSIC,
            "!baseline:example.org",
            0,
            MembershipAction.JOIN,
            b'{"reason":"restart"}',
            30_000,
        ),
        0,
        MembershipDeliveryState.READY,
        False,
    )


def _network_effect_graph_rows(journal) -> tuple[tuple[object, ...], ...]:
    rows: list[tuple[object, ...]] = []
    for table, order_by in (
        ("NioIngestRoomState", "room_id"),
        ("NioIngestRoomLane", "room_id, membership_epoch"),
        ("NioIngestNetworkEffect", "effect_id"),
    ):
        rows.extend(
            (table, *tuple(row))
            for row in journal.connection.execute(
                f'SELECT * FROM "{table}" WHERE account_id = ? ORDER BY {order_by}',
                (ACCOUNT_ID,),
            )
        )
    return tuple(rows)


def _kill_during_attach(
    store_path: Path,
    consumer: ConsumerBootstrap,
    kill_after: int,
) -> None:
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    bootstrap._journal.set_transition_statement_hook(_exit_at_statement(kill_after))
    asyncio.run(bootstrap.attach_consumer(consumer))


@pytest.mark.asyncio
@pytest.mark.parametrize("kill_after", range(1, 5))
async def test_consumer_attach_is_atomic_at_every_persisted_statement(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"attach-kill-{kill_after}"
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _one_room_consumer(bootstrap)
    bootstrap.close()
    _assert_process_crashed(_kill_during_attach, store_path, consumer, kill_after)

    reopened = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        owner = reopened._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.UNBOUND
        assert owner.consumer_attach_next_room_ordinal == 0
        assert owner.binding is None
        assert owner.revision == 0
        for table in (
            "NioIngestRoomState",
            "NioIngestRoomLane",
            "NioIngestReadyRecord",
        ):
            assert (
                reopened._journal.connection.execute(
                    f'SELECT COUNT(*) FROM "{table}"'
                ).fetchone()[0]
                == 0
            )
    finally:
        reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("kill_after", range(770, 774))
async def test_final_attach_chunk_rolls_back_and_resumes_exact_prefix_after_kill(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"attach-final-kill-{kill_after}"
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _many_room_consumer(bootstrap, 257)
    bootstrap.close()
    _assert_process_crashed(_kill_during_attach, store_path, consumer, kill_after)

    reopened = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        owner = reopened._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.ATTACHING
        assert owner.consumer_attach_next_room_ordinal == 256
        assert owner.next_ready_order == 256
        assert owner.revision == 1
        with pytest.raises(LocalProtocolError, match="consumer is not attached"):
            reopened.assert_http_enabled()
        for table in (
            "NioIngestRoomState",
            "NioIngestRoomLane",
            "NioIngestReadyRecord",
        ):
            assert (
                reopened._journal.connection.execute(
                    f'SELECT COUNT(*) FROM "{table}"'
                ).fetchone()[0]
                == 256
            )

        await reopened.attach_consumer(consumer)
        owner = reopened._journal.load_owner()
        assert owner.consumer_attach_status is ConsumerAttachStatus.ATTACHED
        assert owner.consumer_attach_next_room_ordinal == 257
        assert owner.next_ready_order == 257
        assert owner.consumer_attached_revision == 2
        assert owner.revision == 2
        for table in (
            "NioIngestRoomState",
            "NioIngestRoomLane",
            "NioIngestReadyRecord",
        ):
            assert (
                reopened._journal.connection.execute(
                    f'SELECT COUNT(*) FROM "{table}"'
                ).fetchone()[0]
                == 257
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


def _transition(stream_id: UUID) -> JournalTransition:
    request = NetworkRequest(
        stream_id,
        TransportKind.CLASSIC,
        1,
        1,
        "GET",
        "/_matrix/client/v3/sync",
        (),
        None,
        30_000,
        b'{"next_batch":null}',
    )
    body = b'{"raw":true}'
    response = StagedSourceResponse(request, body, hashlib.sha256(body).digest())
    frame_id = uuid5(stream_id, f"1:1:{response.source_sha256.hex()}")
    ready_event = _event("ready", 0)
    return JournalTransition(
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
        ready_records=(ReadyRecord(0, ready_event, frame_id),),
        frames=(StagedFrame(frame_id, response),),
    )


def _kill_during_transition(
    store_path: Path,
    consumer: ConsumerBootstrap,
    transition: JournalTransition,
    kill_after: int,
) -> None:
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    asyncio.run(bootstrap.attach_consumer(consumer))
    bootstrap._journal.set_transition_statement_hook(_exit_at_statement(kill_after))
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=transition,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kill_after", range(1, 7))
async def test_transition_rolls_back_after_each_sql_statement_and_restart(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"kill-{kill_after}"
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _consumer(bootstrap)
    await bootstrap.attach_consumer(consumer)
    transition = _transition(bootstrap.stream_id)
    bootstrap.close()
    _assert_process_crashed(
        _kill_during_transition,
        store_path,
        consumer,
        transition,
        kill_after,
    )

    reopened = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(_consumer(reopened))
        assert reopened._journal.load_owner().revision == 1
        assert reopened._journal.load_owner().next_ready_order == 0
        assert reopened._journal.load_owner().next_batch_sequence == 1
        assert reopened._journal.load_source() == SourceState(
            0,
            TransportKind.CLASSIC,
            b'{"next_batch":null}',
            1,
            True,
        )
        assert reopened._journal.load_rooms(frozenset({"!room:example.org"})) == {}
        assert reopened._journal.load_ready_heads(limit=10) == ()
        assert reopened._journal.load_frame(transition.frames[0].frame_id) is None
        assert reopened._journal.oldest_unacknowledged() is None
    finally:
        reopened.close()


def _held_lane_record(record_id: str, ordinal: int) -> LaneRecord:
    event = _event(record_id, ordinal + 10)
    return LaneRecord(
        LaneRecordKey(
            event.room_id,
            event.membership_epoch,
            LaneRecordSection.HELD,
            0,
            ordinal,
        ),
        event,
        FRAME_ID,
        None,
        len(_canonical_json(_record_to_dict(event))),
    )


def _kill_during_carrier_transition(
    store_path: Path,
    consumer: ConsumerBootstrap,
    transition: JournalTransition,
    kill_after: int,
) -> None:
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    asyncio.run(bootstrap.attach_consumer(consumer))
    bootstrap._journal.set_transition_statement_hook(_exit_at_statement(kill_after))
    bootstrap._journal.commit(
        expected_revision=2,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=transition,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize("kill_after", range(1, 9))
async def test_carrier_insert_delete_rolls_back_as_one_restart_unit(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"carrier-kill-{kill_after}"
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _one_room_consumer(bootstrap)
    await bootstrap.attach_consumer(consumer)
    ready = bootstrap._journal.load_ready_heads(limit=1)[0]
    room_id = "!room:example.org"
    existing = _held_lane_record("carrier-existing", 0)
    original_state = RoomState(
        room_id,
        1,
        1,
        RoomHydrationStatus.READY,
        _snapshot(room_id),
    )
    original_lane = RoomLane(
        room_id,
        1,
        LaneStatus.ACTIVE,
        held_record_count=1,
        held_canonical_bytes=existing.canonical_bytes,
        next_held_ordinal=1,
    )
    bootstrap._journal.commit(
        expected_revision=1,
        writer_epoch=bootstrap._journal.writer_epoch,
        transition=JournalTransition(
            room_states=(original_state,),
            room_lanes=(original_lane,),
            lane_record_inserts=(existing,),
        ),
    )
    inserted = _held_lane_record("carrier-inserted", 1)
    batch = batch_from_records(
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer=consumer.binding,
        stream_id=bootstrap.stream_id,
        sequence=1,
        created_revision=3,
        records=(ready.record, existing.record),
    )
    transition = JournalTransition(
        room_states=(replace(original_state, next_room_sequence=2),),
        room_lanes=(
            replace(
                original_lane,
                held_canonical_bytes=inserted.canonical_bytes,
                next_held_ordinal=2,
            ),
        ),
        lane_record_inserts=(inserted,),
        batch_materialization=BatchMaterialization(
            batch,
            (ReadyRecordKey(ready.record.loss_id), existing.key),
        ),
    )
    bootstrap.close()

    _assert_process_crashed(
        _kill_during_carrier_transition,
        store_path,
        consumer,
        transition,
        kill_after,
    )

    reopened = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        assert reopened._journal.load_owner().revision == 2
        aggregate = reopened._journal.load_rooms(frozenset({room_id}))[room_id]
        assert aggregate.state == original_state
        assert aggregate.active_lane == original_lane
        assert reopened._journal.load_ready_heads(limit=10) == (ready,)
        assert reopened._journal.load_lane_record(existing.key) == existing
        assert reopened._journal.load_lane_record(inserted.key) is None
        assert reopened._journal.oldest_unacknowledged() is None
        assert (
            reopened._journal.connection.execute(
                "SELECT COUNT(*) FROM NioIngestBatchItem"
            ).fetchone()[0]
            == 0
        )
    finally:
        reopened.close()


def _kill_during_acknowledgement(
    store_path: Path,
    consumer: ConsumerBootstrap,
    ref: BatchRef,
    kill_after: int,
) -> None:
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    asyncio.run(bootstrap.attach_consumer(consumer))
    bootstrap._journal.set_transition_statement_hook(_exit_at_statement(kill_after))
    bootstrap._journal.acknowledge(ref)


@pytest.mark.asyncio
@pytest.mark.parametrize("kill_after", range(1, 3))
async def test_acknowledgement_rolls_back_frontier_and_batch_cascade(
    tmp_path: Path,
    kill_after: int,
) -> None:
    store_path = tmp_path / f"ack-kill-{kill_after}"
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _many_room_consumer(bootstrap, 2)
    await bootstrap.attach_consumer(consumer)
    ready_records = bootstrap._journal.load_ready_heads(limit=2)
    batches = tuple(
        batch_from_records(
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            consumer=consumer.binding,
            stream_id=bootstrap.stream_id,
            sequence=sequence,
            created_revision=sequence + 1,
            records=(ready.record,),
        )
        for sequence, ready in enumerate(ready_records, start=1)
    )
    for sequence, (batch, ready) in enumerate(
        zip(batches, ready_records, strict=True),
        start=1,
    ):
        bootstrap._journal.commit(
            expected_revision=sequence,
            writer_epoch=bootstrap._journal.writer_epoch,
            transition=JournalTransition(
                batch_materialization=BatchMaterialization(
                    batch,
                    (ReadyRecordKey(ready.record.loss_id),),
                )
            ),
        )
    assert bootstrap._journal.acknowledge(batches[0].ref) is AckOutcome.ACKNOWLEDGED
    bootstrap.close()
    _assert_process_crashed(
        _kill_during_acknowledgement,
        store_path,
        consumer,
        batches[1].ref,
        kill_after,
    )

    reopened = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await reopened.attach_consumer(consumer)
        owner = reopened._journal.load_owner()
        assert owner.last_acked_sequence == 1
        assert owner.revision == 4
        assert (
            reopened._journal.acknowledge(batches[0].ref)
            is AckOutcome.ALREADY_ACKNOWLEDGED
        )
        assert reopened._journal.oldest_unacknowledged() == batches[1]
        rows = reopened._journal.connection.execute(
            "SELECT sequence FROM NioIngestBatch "
            "WHERE account_id = ? ORDER BY sequence",
            (ACCOUNT_ID,),
        ).fetchall()
        assert [row[0] for row in rows] == [2]
        index_rows = reopened._journal.connection.execute(
            "SELECT sequence, record_ordinal FROM NioIngestBatchItem "
            "WHERE account_id = ? ORDER BY sequence, record_ordinal",
            (ACCOUNT_ID,),
        ).fetchall()
        assert [tuple(row) for row in index_rows] == [(2, 0)]
    finally:
        reopened.close()


def _interrupt_network_effect_transition(
    store_path: Path,
    consumer: ConsumerBootstrap,
    operation: str,
    statement_ordinal: int,
) -> None:
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    asyncio.run(bootstrap.attach_consumer(consumer))
    journal = bootstrap._journal
    owner = journal.load_owner()
    recovery, recovery_state, recovery_lane = _recovery_effect_graph(
        bootstrap.stream_id
    )
    if operation == "recovery_insert":
        transition = JournalTransition(
            room_states=(recovery_state,),
            room_lanes=(recovery_lane,),
            network_effect_inserts=(recovery,),
        )
    elif operation == "membership_update":
        membership = journal.load_network_effect(
            _membership_effect(bootstrap.stream_id).request.effect_id
        )
        assert membership is not None
        transition = JournalTransition(
            network_effect_updates=(
                replace(
                    membership,
                    attempt_ordinal=1,
                    membership_delivery_state=(
                        MembershipDeliveryState.DISPATCHED_UNCONFIRMED
                    ),
                ),
            )
        )
    else:
        assert operation == "recovery_delete"
        transition = JournalTransition(
            room_lanes=(
                replace(
                    recovery_lane,
                    release_phase=ReleasePhase.IDLE,
                    recovery_gap=None,
                ),
            ),
            network_effect_deletes=(recovery.request.effect_id,),
        )
    journal.set_transition_statement_hook(_exit_at_statement(statement_ordinal))
    journal.commit(
        expected_revision=owner.revision,
        writer_epoch=journal.writer_epoch,
        transition=transition,
    )


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("operation", "statement_count"),
    (
        ("recovery_insert", 4),
        ("membership_update", 2),
        ("recovery_delete", 3),
    ),
)
async def test_network_effect_graph_rolls_back_at_every_persisted_statement(
    tmp_path: Path,
    operation: str,
    statement_count: int,
) -> None:
    store_path = tmp_path / f"effect-{operation}"
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _one_room_consumer(bootstrap)
    await bootstrap.attach_consumer(consumer)
    journal = bootstrap._journal
    if operation == "membership_update":
        membership = _membership_effect(bootstrap.stream_id)
        journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(network_effect_inserts=(membership,)),
        )
    elif operation == "recovery_delete":
        recovery, recovery_state, recovery_lane = _recovery_effect_graph(
            bootstrap.stream_id
        )
        journal.commit(
            expected_revision=1,
            writer_epoch=journal.writer_epoch,
            transition=JournalTransition(
                room_states=(recovery_state,),
                room_lanes=(recovery_lane,),
                network_effect_inserts=(recovery,),
            ),
        )
    expected_revision = journal.load_owner().revision
    expected_rows = _network_effect_graph_rows(journal)
    expected_effects = journal.list_network_effects(256)
    bootstrap.close()

    for statement_ordinal in range(1, statement_count + 1):
        _assert_process_crashed(
            _interrupt_network_effect_transition,
            store_path,
            consumer,
            operation,
            statement_ordinal,
        )
        reopened = open_ingestion_store(
            store_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            pickle_key="secret",
            database_name="journal.db",
        )
        try:
            await reopened.attach_consumer(consumer)
            assert reopened._journal.load_owner().revision == expected_revision
            assert _network_effect_graph_rows(reopened._journal) == expected_rows
            assert reopened._journal.list_network_effects(256) == expected_effects
            reopened._journal.load_rooms(frozenset({"!baseline:example.org"}))
        finally:
            reopened.close()


def _interrupt_membership_operation(
    store_path: Path,
    consumer: ConsumerBootstrap,
    operation: str,
    ref: MembershipOperationRef | None,
    statement_ordinal: int,
) -> None:
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    asyncio.run(bootstrap.attach_consumer(consumer))
    journal = bootstrap._journal
    journal.set_transition_statement_hook(_exit_at_statement(statement_ordinal))
    effect_id = _membership_effect(bootstrap.stream_id).request.effect_id
    if operation == "claim":
        journal.claim_membership_operation(effect_id)
    else:
        assert ref is not None
        resolution = (
            MembershipOperationResolution.RETRY
            if operation == "retry"
            else MembershipOperationResolution.SUPERSEDE
        )
        journal.resolve_membership_operation(ref, resolution)


@pytest.mark.asyncio
@pytest.mark.parametrize("operation", ("claim", "retry", "supersede"))
async def test_membership_operation_rolls_back_at_every_persisted_statement(
    tmp_path: Path,
    operation: str,
) -> None:
    store_path = tmp_path / f"membership-{operation}"
    bootstrap = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    consumer = _one_room_consumer(bootstrap)
    await bootstrap.attach_consumer(consumer)
    journal = bootstrap._journal
    membership = _membership_effect(bootstrap.stream_id)
    journal.commit(
        expected_revision=1,
        writer_epoch=journal.writer_epoch,
        transition=JournalTransition(network_effect_inserts=(membership,)),
    )
    ref = None
    if operation != "claim":
        _, ref = journal.claim_membership_operation(membership.request.effect_id)
    expected_revision = journal.load_owner().revision
    expected_rows = _network_effect_graph_rows(journal)
    expected_effects = journal.list_network_effects(256)
    bootstrap.close()

    for statement_ordinal in (1, 2):
        _assert_process_crashed(
            _interrupt_membership_operation,
            store_path,
            consumer,
            operation,
            ref,
            statement_ordinal,
        )
        reopened = open_ingestion_store(
            store_path,
            source=CLASSIC_SOURCE,
            account_id=ACCOUNT_ID,
            device_id=DEVICE_ID,
            pickle_key="secret",
            database_name="journal.db",
        )
        try:
            await reopened.attach_consumer(consumer)
            assert reopened._journal.load_owner().revision == expected_revision
            assert _network_effect_graph_rows(reopened._journal) == expected_rows
            assert reopened._journal.list_network_effects(256) == expected_effects
        finally:
            reopened.close()

    completed = open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        pickle_key="secret",
        database_name="journal.db",
    )
    try:
        await completed.attach_consumer(consumer)
        if operation == "claim":
            request, completed_ref = completed._journal.claim_membership_operation(
                membership.request.effect_id
            )
            assert request == membership.request
            assert completed_ref.attempt_ordinal == 1
            assert completed._journal.uncertain_membership_operations(256)[0].ref == (
                completed_ref
            )
        elif operation == "retry":
            assert ref is not None
            assert (
                completed._journal.resolve_membership_operation(
                    ref,
                    MembershipOperationResolution.RETRY,
                )
                is MembershipOperationResolutionOutcome.READY
            )
            retried = completed._journal.load_network_effect(ref.effect_id)
            assert retried is not None
            assert retried.membership_delivery_state is MembershipDeliveryState.READY
            assert retried.prior_delivery_uncertain is True
        else:
            assert ref is not None
            assert (
                completed._journal.resolve_membership_operation(
                    ref,
                    MembershipOperationResolution.SUPERSEDE,
                )
                is MembershipOperationResolutionOutcome.SUPERSEDED
            )
            assert completed._journal.load_network_effect(ref.effect_id) is None
        assert completed._journal.load_owner().revision == expected_revision + 1
    finally:
        completed.close()
