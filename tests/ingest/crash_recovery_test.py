"""On-disk restart and schema kill-point coverage for ingestion v1."""

import hashlib
import json
import multiprocessing
import os
import sqlite3
from pathlib import Path
from typing import Callable
from uuid import UUID

import pytest

from nio.event_provenance import TimelineEventProvenance
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.model import EventRecord, RecordKind, RecordOrigin, TransportKind
from nio.ingest.ports import NetworkResult, StagedSourceResponse
from nio.ingest.sliding import RESERVED_ALL_ROOMS_LIST, SlidingSource
from nio.ingest.source import canonical_json
from nio.ingest.state import SourceState, StagedFrame
from nio.store._sync_journal_plan import _canonical_work_plaintext
from nio.store._sync_journal_rows import _canonical_internal
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus
from nio.store.sync_journal import open_ingestion_store
from nio.store.sync_journal_schema import SCHEMA_SQL

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
CLASSIC_SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")
SLIDING_SOURCE = SlidingSourceConfig(
    timeout_ms=30_000,
    connection_name="worker",
    lists_json=b"{}",
    room_subscriptions_json=b"{}",
    extensions_json=b"{}",
    all_rooms_page_size=2,
)
CONSUMER_GENERATION = UUID("22222222-2222-4222-8222-222222222222")
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
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
        schema_statement_hook=_exit_at_statement(kill_after),
    )


def _open_delivery(store_path: Path, hook: Callable[[str], None] | None = None):
    return open_ingestion_store(
        store_path,
        source=CLASSIC_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
        transition_statement_hook=hook,
    )


def _open_sliding(store_path: Path, hook: Callable[[str], None] | None = None):
    return open_ingestion_store(
        store_path,
        source=SLIDING_SOURCE,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
        transition_statement_hook=hook,
    )


def _seed_positioned_sliding(bootstrap) -> None:
    journal = bootstrap._journal
    owner = journal.load_owner()
    prior = journal.load_source()
    adapter = SlidingSource(owner.stream_id, SLIDING_SOURCE, owner.account_id)
    request = adapter.plan_request(prior, prior.next_request_id)
    assert request is not None and request.body is not None
    body = canonical_json(
        {
            "pos": "p1",
            "txn_id": json.loads(request.body)["txn_id"],
            "lists": {RESERVED_ALL_ROOMS_LIST: {"count": 0}},
            "rooms": {},
            "extensions": {"to_device": {"events": [], "next_batch": "td1"}},
        }
    )
    normalized = adapter.normalize(
        request,
        NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            body,
            None,
            None,
        ),
    )
    assert normalized.frame is not None
    journal.stage_source_response(
        source=SourceState(
            prior.source_epoch,
            prior.transport_kind,
            normalized.frame.candidate_cursor_json,
            request.request_id + 1,
            prior.active,
        ),
        frame=StagedFrame(
            normalized.frame.frame_id,
            StagedSourceResponse(
                request,
                normalized.response_body,
                normalized.frame.source_sha256,
            ),
        ),
    )
    assert (
        journal.materialize_oldest_frame(limits=MaterializerLimits()).status
        is MaterializeStatus.MATERIALIZED
    )


def _kill_during_sliding_reopen(store_path: Path, boundary: str) -> None:
    def kill(label: str) -> None:
        if label == boundary:
            os._exit(CRASH_EXIT_CODE)

    _open_sliding(store_path, kill)


def _stored_sliding_reset_graph(
    database_path: Path,
) -> tuple[dict[str, object], dict[str, object], dict[str, object]]:
    with sqlite3.connect(database_path) as connection:
        connection.row_factory = sqlite3.Row
        meta_row = connection.execute("SELECT * FROM NioIngestMeta").fetchone()
        source_row = connection.execute("SELECT * FROM NioIngestSourceState").fetchone()
    assert meta_row is not None and source_row is not None
    meta = dict(meta_row)
    source = dict(source_row)
    payload = source["payload"]
    digest = source["payload_sha256"]
    assert isinstance(payload, bytes) and isinstance(digest, bytes)
    assert hashlib.sha256(payload).digest() == digest
    envelope = json.loads(payload)
    assert isinstance(envelope, dict)
    cursor = envelope["value"]
    assert isinstance(cursor, dict)
    return meta, source, cursor


def _seed_ready(bootstrap) -> EventRecord:
    journal = bootstrap._journal
    owner = journal.load_owner()
    record = EventRecord(
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
    clear = (
        record.record_id,
        "event",
        "ready",
        "00000000-0000-4000-8000-000000000002",
        record.room_id,
        record.membership_epoch,
        record.room_sequence,
        1,
        0,
        1,
    )
    payload, digest = journal._payload(
        owner,
        "NioIngestWork",
        _canonical_work_plaintext("event", record),
        header=_canonical_internal(clear),
    )
    with journal._owner.journal_write():
        journal._execute("UPDATE NioIngestMeta SET revision = 1")
        journal._execute(
            "INSERT INTO NioIngestWork VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)",
            (ACCOUNT_ID, *clear, payload, digest),
        )
    return record


def _kill_during_delivery(store_path: Path, operation: str, boundary: str) -> None:
    bootstrap = _open_delivery(store_path)
    journal = bootstrap._journal
    batch = journal.next_batch() if operation == "ack" else None

    def kill(label: str) -> None:
        if label == boundary:
            os._exit(CRASH_EXIT_CODE)

    journal.set_transition_statement_hook(kill)
    if operation == "claim":
        journal.next_batch()
    else:
        assert batch is not None
        journal.acknowledge_batch(batch.ref)


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
        consumer_generation=CONSUMER_GENERATION,
        database_name="journal.db",
    )
    try:
        assert reopened.schema_version == 1
        assert reopened._journal.load_owner().consumer_generation == CONSUMER_GENERATION
        assert reopened._journal.load_source() == SourceState(
            0,
            TransportKind.CLASSIC,
            b'{"next_batch":null}',
            0,
            True,
        )
    finally:
        reopened.close()


@pytest.mark.parametrize(
    ("operation", "boundary", "committed"),
    (
        ("claim", "delivery_claim_meta_cas", False),
        ("claim", "before_commit", False),
        ("claim", "commit", True),
        ("ack", "delivery_work_delete", False),
        ("ack", "delivery_ack_meta_cas", False),
        ("ack", "before_commit", False),
        ("ack", "commit", True),
    ),
)
def test_delivery_process_death_reopens_only_old_or_complete_new_graph(
    tmp_path: Path,
    operation: str,
    boundary: str,
    committed: bool,
) -> None:
    store_path = tmp_path / f"{operation}-{boundary}"
    bootstrap = _open_delivery(store_path)
    record = _seed_ready(bootstrap)
    claimed = bootstrap._journal.next_batch() if operation == "ack" else None
    bootstrap.close()

    _assert_process_crashed(
        _kill_during_delivery,
        store_path,
        operation,
        boundary,
    )
    with sqlite3.connect(store_path / "journal.db") as connection:
        frontier = connection.execute(
            "SELECT delivery_outstanding_work_id FROM NioIngestMeta"
        ).fetchone()
        work_count = connection.execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()
    assert frontier == (
        (record.record_id,) if committed is (operation == "claim") else (None,)
    )
    assert work_count == ((0,) if committed and operation == "ack" else (1,))

    reopened = _open_delivery(store_path)
    if operation == "claim":
        batch = reopened._journal.next_batch()
        assert batch is not None and batch.records == (record,)
        reopened._journal.acknowledge_batch(batch.ref)
    else:
        assert claimed is not None
        if committed:
            assert reopened._journal.next_batch() is None
        else:
            assert reopened._journal.next_batch() == claimed
        reopened._journal.acknowledge_batch(claimed.ref)
    assert reopened._journal.next_batch() is None
    reopened.close()


@pytest.mark.parametrize(
    ("boundary", "committed"),
    (
        ("sliding_reset_meta_cas", False),
        ("sliding_reset_source_upsert", False),
        ("before_commit", False),
        ("commit", True),
    ),
)
def test_sliding_reopen_process_death_leaves_only_old_or_complete_new_graph(
    tmp_path: Path,
    boundary: str,
    committed: bool,
) -> None:
    store_path = tmp_path / f"sliding-reset-{boundary}"
    bootstrap = _open_sliding(store_path)
    _seed_positioned_sliding(bootstrap)
    bootstrap.close()
    database_path = store_path / "journal.db"
    meta_before, source_before, cursor_before = _stored_sliding_reset_graph(
        database_path
    )

    _assert_process_crashed(
        _kill_during_sliding_reopen,
        store_path,
        boundary,
    )

    meta_after, source_after, cursor_after = _stored_sliding_reset_graph(database_path)
    if not committed:
        assert meta_after == meta_before
        assert source_after == source_before
        assert cursor_after == cursor_before
        return

    writer_after = meta_after["writer_epoch"]
    assert writer_after != meta_before["writer_epoch"]
    assert meta_after == {
        **meta_before,
        "revision": meta_before["revision"] + 1,  # type: ignore[operator]
        "writer_epoch": writer_after,
        "next_source_epoch": meta_before["next_source_epoch"] + 1,  # type: ignore[operator]
    }
    assert source_after["account_id"] == source_before["account_id"]
    assert source_after["source_epoch"] == meta_before["next_source_epoch"]
    assert source_after["next_request_id"] == 0
    assert source_after["active"] == source_before["active"]
    assert cursor_after == {
        **cursor_before,
        "pos": None,
        "connection_instance": cursor_after["connection_instance"],
        "all_rooms_range_ack_mode": "unknown",
        "all_rooms_coverage_complete": False,
    }
    assert cursor_after["connection_instance"] != cursor_before["connection_instance"]
