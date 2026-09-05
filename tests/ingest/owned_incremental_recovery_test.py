"""Incremental recovery under a backlog larger than one retained frame."""

import asyncio

import pytest
from owned_capacity_test import ROOM_ID, _baseline_session, _messages, _settle_frame

from nio.ingest.model import RecordKind
from nio.ingest.ports import NetworkResult
from nio.ingest.source import canonical_json, load_json
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus


@pytest.mark.asyncio
async def test_backlog_larger_than_frame_bound_drains_before_sync_completion(
    tmp_path, monkeypatch
):
    session = await _baseline_session(tmp_path)
    body = _messages(1)
    fresh = body["rooms"]["join"][ROOM_ID]["timeline"]["events"][0]
    fresh["event_id"] = "$fresh"
    body["rooms"]["join"][ROOM_ID]["timeline"]["limited"] = True
    history = _messages(400, padding=48_000)["rooms"]["join"][ROOM_ID]["timeline"][
        "events"
    ]
    assert len(canonical_json(history)) > 16 * 1024 * 1024
    records = []
    sync_requests = 0
    page_requests = []
    max_work_bytes = 0
    completions = []

    async def complete(value):
        completions.append(value)

    session._completion_sink = complete

    async def respond(request, **kwargs):
        nonlocal sync_requests
        query = dict(request.query)
        if request.path.endswith("/sync"):
            sync_requests += 1
            assert sync_requests == 1, "recovery must retain the original sync response"
            response = body
        else:
            assert request.path.endswith("/messages")
            assert not completions
            cursor = load_json(session._journal.load_source().cursor_json, "cursor")
            assert cursor["next_batch"] == "s1"
            start = query["from"]
            position = 0 if start == "s1" else int(start.removeprefix("p"))
            limit = int(query["limit"])
            stop = min(position + limit, len(history))
            page_requests.append((start, limit))
            response = {
                "start": start,
                "end": "t1" if stop == len(history) else f"p{stop}",
                "chunk": history[position:stop],
            }
        return NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            canonical_json(response),
            None,
            None,
        )

    monkeypatch.setattr(session, "_request", respond)
    session._quiesce_source_commit_target = session._source_commit_generation + 1
    running = asyncio.create_task(session.run())

    async def consume():
        nonlocal max_work_bytes
        while not running.done():
            stored_bytes = session._journal._execute(
                "SELECT COALESCE(SUM(LENGTH(payload)), 0) FROM NioIngestWork"
            ).fetchone()[0]
            max_work_bytes = max(max_work_bytes, stored_bytes)
            batch = session.next_batch()
            if batch is not None:
                records.extend(batch.records)
                await session._settle_batch(
                    batch, receipt_new=True, semantic_event_new=True
                )
            await asyncio.sleep(0)

    consuming = asyncio.create_task(consume())
    try:
        async with asyncio.timeout(30):
            await running
            await consuming
        staged = session._journal.list_frames(1)[0]
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        records.extend(await _settle_frame(session, staged.frame_id))
        timeline = [record for record in records if record.kind is RecordKind.TIMELINE]
        assert [record.event_id for record in timeline] == [
            *(event["event_id"] for event in history),
            "$fresh",
        ]
        assert len({record.record_id for record in timeline}) == 401
        assert max_work_bytes < 16 * 1024 * 1024
        assert len(completions) == 1
        assert session._journal.load_source().cursor_json == b'{"next_batch":"s2"}'
        assert (
            session._journal._execute(
                "SELECT COUNT(*) FROM NioIngestRecovery"
            ).fetchone()[0]
            == 0
        )
        assert page_requests[0] == ("s1", 100)
    finally:
        running.cancel()
        consuming.cancel()
        await asyncio.gather(running, consuming, return_exceptions=True)
        await session.close()
