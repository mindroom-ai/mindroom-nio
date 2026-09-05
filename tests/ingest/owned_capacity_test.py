"""Whole-frame output admission through the owned client and SQLite."""

from dataclasses import replace
from pathlib import Path
from uuid import UUID

import pytest
from ingestion_helpers import open_test_session

from nio import AsyncClient
from nio.crypto import OlmAccount
from nio.exceptions import LocalProtocolError
from nio.ingest.config import ClassicSourceConfig, IngestionConfig
from nio.ingest.errors import JournalCapacityError
from nio.ingest.hydration import normalize_hydration_response
from nio.ingest.ports import NetworkResult, StagedSourceResponse
from nio.ingest.source import canonical_json
from nio.ingest.state import SourceState, StagedFrame
from nio.store import SqliteStore
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus
from nio.store.sync_journal import _open_configured_ingestion_store

ACCOUNT_ID = "@alice:example.org"
DEVICE_ID = "DEVICE"
ROOM_ID = "!room:example.org"
GENERATION = UUID(int=44)
SOURCE = ClassicSourceConfig(timeout_ms=30_000, filter_json=b"{}")


def _open_session(path: Path):
    bootstrap = _open_configured_ingestion_store(
        path,
        source_store_class=SqliteStore,
        owned_store_class=SqliteStore,
        account_id=ACCOUNT_ID,
        device_id=DEVICE_ID,
        consumer_generation=GENERATION,
        source=SOURCE,
        database_name="journal.db",
    )
    client = AsyncClient("https://example.org", ACCOUNT_ID, DEVICE_ID)
    client.restore_login(ACCOUNT_ID, DEVICE_ID, "test-token")
    return open_test_session(
        client,
        bootstrap,
        config=IngestionConfig(SOURCE),
        consumer_generation=GENERATION,
        stream_id=bootstrap.stream_id,
    )


def _stage(session, body):
    prior = session._journal.load_source()
    request = session._source.plan_request(prior, prior.next_request_id)
    assert request is not None
    result = session._source.normalize(
        request,
        NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            canonical_json(body),
            None,
            None,
        ),
    )
    assert result.frame is not None
    frame = result.frame
    staged = StagedFrame(
        frame.frame_id,
        StagedSourceResponse(request, result.response_body, frame.source_sha256),
    )
    session._journal.stage_source_response(
        source=SourceState(
            prior.source_epoch,
            prior.transport_kind,
            frame.candidate_cursor_json,
            request.request_id + 1,
            prior.active,
        ),
        frame=staged,
    )
    return staged


async def _settle_frame(session, frame_id):
    records = []
    while (batch := session.next_batch()) is not None:
        records.extend(batch.records)
        await session._settle_batch(batch, receipt_new=True, semantic_event_new=True)
    await session._advance_blocked_frame(frame_id)
    assert session._journal.list_frames(1) == ()
    return records


async def _baseline_session(path: Path):
    store = SqliteStore(ACCOUNT_ID, DEVICE_ID, str(path), database_name="journal.db")
    store.save_account(OlmAccount())
    store.database.close()
    session = _open_session(path)
    member = {
        "type": "m.room.member",
        "state_key": ACCOUNT_ID,
        "event_id": "$member",
        "sender": ACCOUNT_ID,
        "origin_server_ts": 1,
        "content": {"membership": "join"},
    }
    frame = _stage(
        session,
        {
            "next_batch": "s1",
            "rooms": {
                "join": {
                    ROOM_ID: {
                        "state": {"events": [member]},
                        "timeline": {
                            "events": [],
                            "limited": False,
                            "prev_batch": "t0",
                        },
                    }
                }
            },
        },
    )
    result = session._materialize_oldest_frame(limits=MaterializerLimits())
    assert result.status is MaterializeStatus.MATERIALIZED
    pending = session._journal.load_pending_hydrations(limit=2)
    assert len(pending) == 1
    hydration = normalize_hydration_response(
        pending[0], own_user_id=ACCOUNT_ID, response_body=canonical_json([member])
    )
    session._journal.apply_hydration_result(result=hydration)
    await _settle_frame(session, frame.frame_id)
    return session


def _messages(count, padding=0):
    return {
        "next_batch": "s2",
        "rooms": {
            "join": {
                ROOM_ID: {
                    "timeline": {
                        "events": [
                            {
                                "type": "m.room.message",
                                "event_id": f"$message{index}",
                                "sender": ACCOUNT_ID,
                                "origin_server_ts": 100 + index,
                                "content": {
                                    "msgtype": "m.text",
                                    "body": "x" * padding or f"Message {index}",
                                },
                            }
                            for index in range(count)
                        ],
                        "limited": False,
                        "prev_batch": "t1",
                    }
                }
            }
        },
    }


@pytest.mark.asyncio
@pytest.mark.parametrize(("count", "padding"), ((2050, 0), (40, 330_000)))
async def test_owned_whole_frame_above_former_ready_limits_restarts_and_retires(
    tmp_path, count, padding
):
    session = await _baseline_session(tmp_path)
    try:
        frame = _stage(session, _messages(count, padding))
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        assert result.frame_id == frame.frame_id
        size = session._journal._execute(
            "SELECT COUNT(*), SUM(LENGTH(payload)), MAX(LENGTH(payload)) FROM NioIngestWork"
        ).fetchone()
        assert size[0] == count
        assert size[2] <= 1024 * 1024
        assert size[1] <= 64 * 1024 * 1024
        if padding:
            assert size[1] > 16 * 1024 * 1024
        first = session.next_batch()
        assert first is not None
        first_ref, first_record = first.ref, first.records[0]
    finally:
        await session.close()

    session = _open_session(tmp_path)
    try:
        replay = session.next_batch()
        assert replay is not None
        assert replay.ref == first_ref
        assert replay.records[0] == first_record
        records = await _settle_frame(session, frame.frame_id)
        assert [record.event_id for record in records] == [
            f"$message{index}" for index in range(count)
        ]
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "limit", ({"max_total_work_count": 2}, {"max_total_work_canonical_bytes": 1})
)
async def test_owned_capacity_rejection_rolls_back_and_requires_fresh_client(
    tmp_path, limit
):
    session = await _baseline_session(tmp_path)
    limits = replace(MaterializerLimits(), **limit)
    try:
        frame = _stage(session, _messages(3))
        owner = session._journal.load_owner()
        source = session._journal.load_source()
        before_count = session._journal._execute(
            "SELECT COUNT(*) FROM NioIngestWork"
        ).fetchone()[0]
        token = session._client.store.load_sync_token()
        with pytest.raises(JournalCapacityError, match="total") as failure:
            session._materialize_oldest_frame(limits=limits)
        assert type(failure.value) is JournalCapacityError
        assert session._journal.load_owner() == owner
        assert session._journal.load_source() == source
        assert session._journal.list_frames(1)[0] == frame
        assert (
            session._journal._execute("SELECT COUNT(*) FROM NioIngestWork").fetchone()[
                0
            ]
            == before_count
        )
        assert session._client.store.load_sync_token() == token
        with pytest.raises(LocalProtocolError, match="poisoned"):
            session._materialize_oldest_frame(limits=MaterializerLimits())
        with pytest.raises(LocalProtocolError, match="poisoned"):
            session.next_batch()
    finally:
        await session.close()

    session = _open_session(tmp_path)
    try:
        assert session._journal.list_frames(1)[0] == frame
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        assert len(await _settle_frame(session, frame.frame_id)) == 3
    finally:
        await session.close()
