"""Actionable Classic history recovery through real owned SQLite sessions."""

import asyncio
from dataclasses import replace

import pytest
from owned_capacity_test import (
    ROOM_ID,
    ACCOUNT_ID,
    _baseline_session,
    _messages,
    _open_session,
    _settle_frame,
)

from nio.event_provenance import TimelineEventProvenance
from nio.ingest.model import RecordKind
from nio.ingest.coordinator import IngestionSourceError
from nio.ingest import ports, recovery
from nio.ingest.ports import NetworkResult
from nio.ingest.source import canonical_json, load_json
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus


def _limited_body():
    body = _messages(2)
    timeline = body["rooms"]["join"][ROOM_ID]["timeline"]
    missed, fresh = timeline["events"]
    timeline["events"] = [fresh]
    timeline["limited"] = True
    return body, missed, fresh


async def _capture(
    session, monkeypatch, body, pages, *, recovered_records=None, on_request=None
):
    requests = []
    original_source = session._journal.load_source()

    async def respond(request, **kwargs):
        requests.append(request)
        if on_request is not None:
            on_request(request)
        assert (
            load_json(session._journal.load_source().cursor_json, "cursor")[
                "next_batch"
            ]
            == load_json(original_source.cursor_json, "cursor")["next_batch"]
        )
        if request.path.endswith("/sync"):
            response = body.pop(0) if isinstance(body, list) else body
        else:
            assert request.path.endswith("/messages")
            response = pages.pop(0)
        return NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            response if type(response) is int else 200,
            (
                response
                if type(response) is bytes
                else canonical_json({} if type(response) is int else response)
            ),
            None,
            None,
        )

    monkeypatch.setattr(session, "_request", respond)
    # Drain internal recovery Frames; leave the final quiesce response staged.
    session._quiesce_source_commit_target = session._source_commit_generation + 1
    running = asyncio.create_task(session.run())

    async def consume():
        while not running.done():
            batch = session.next_batch()
            if batch is not None:
                if recovered_records is not None:
                    recovered_records.extend(batch.records)
                await session._settle_batch(
                    batch, receipt_new=True, semantic_event_new=True
                )
            await asyncio.sleep(0)

    consuming = asyncio.create_task(consume())
    try:
        async with asyncio.timeout(10):
            await asyncio.gather(running, consuming)
    finally:
        running.cancel()
        consuming.cancel()
        await asyncio.gather(running, consuming, return_exceptions=True)
    return requests


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_limited_interval_is_actionable_and_survives_capture_restart(
    tmp_path, monkeypatch, restart
):
    session = await _baseline_session(tmp_path)
    body, missed, fresh = _limited_body()
    pages = [
        {"start": "s1", "end": "middle", "chunk": [missed]},
        {"start": "middle", "chunk": []},
    ]
    records = []
    try:
        requests = await _capture(
            session, monkeypatch, body, pages, recovered_records=records
        )
        staged = session._journal.list_frames(1)[0]
        if restart:
            await session.close()
            session = _open_session(tmp_path)
        materialized = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert materialized.status is MaterializeStatus.MATERIALIZED
        records.extend(await _settle_frame(session, staged.frame_id))
        timeline = [
            record
            for record in records
            if getattr(record, "kind", None) is RecordKind.TIMELINE
        ]
        assert [record.event_id for record in timeline] == [
            missed["event_id"],
            fresh["event_id"],
        ]
        assert [record.provenance for record in timeline] == [
            TimelineEventProvenance.RECOVERED,
            TimelineEventProvenance.LIVE,
        ]
        assert not pages
        assert [dict(request.query).get("from") for request in requests[1:]] == [
            "s1",
            "middle",
        ]
        assert all(dict(request.query)["to"] == "t1" for request in requests[1:])
        assert session._journal.load_source().cursor_json == b'{"next_batch":"s2"}'
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("configured_limit", "expected_limits"),
    [(None, [100, 50]), (5000, [100, 50]), (24, [24, 12])],
    ids=["default", "larger-user-limit", "smaller-user-limit"],
)
async def test_oversized_response_retries_same_cursor_then_recovers_gap(
    tmp_path, monkeypatch, configured_limit, expected_limits
):
    session = await _baseline_session(tmp_path)
    session._source = replace(
        session._source,
        config=replace(
            session._source.config,
            filter_json=canonical_json(
                {
                    "room": {
                        "timeline": (
                            {}
                            if configured_limit is None
                            else {"limit": configured_limit}
                        ),
                        "state": {"lazy_load_members": True},
                    },
                    "presence": {"types": []},
                }
            ),
        ),
    )
    body, missed, fresh = _limited_body()
    records = []
    try:
        requests = await _capture(
            session,
            monkeypatch,
            [b"x" * (ports.MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES + 1), body],
            [{"start": "s1", "chunk": [missed]}],
            recovered_records=records,
        )
        sync = [request for request in requests if request.path.endswith("/sync")]
        filters = [
            load_json(dict(request.query)["filter"].encode(), "filter")
            for request in sync
        ]
        assert [
            value["room"]["timeline"]["limit"] for value in filters
        ] == expected_limits
        assert all(
            value["room"]["state"] == {"lazy_load_members": True}
            and value["presence"] == {"types": []}
            for value in filters
        )
        assert [dict(request.query)["since"] for request in sync] == ["s1", "s1"]
        page_filter = load_json(dict(requests[-1].query)["filter"].encode(), "filter")
        assert page_filter == {}
        staged = session._journal.list_frames(1)[0]
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        records.extend(await _settle_frame(session, staged.frame_id))
        assert [
            record.event_id
            for record in records
            if getattr(record, "kind", None) is RecordKind.TIMELINE
        ] == [missed["event_id"], fresh["event_id"]]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_state_excluding_filter_cannot_replay_with_future_permissions(
    tmp_path, monkeypatch
):
    session = await _baseline_session(tmp_path)
    session._source = replace(
        session._source,
        config=replace(
            session._source.config,
            filter_json=canonical_json(
                {"room": {"timeline": {"types": ["m.room.message"]}}}
            ),
        ),
    )
    body, missed, _ = _limited_body()
    original = session._journal.load_source()
    try:
        with pytest.raises(IngestionSourceError, match="history recovery failed"):
            await _capture(
                session, monkeypatch, body, [{"start": "s1", "chunk": [missed]}]
            )
        assert session._journal.load_source() == original
        assert session._journal.list_frames(1) == ()
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("limit", [0, 1, 8])
async def test_irreducible_oversize_is_bounded_and_retains_cursor(
    tmp_path, monkeypatch, limit
):
    session = await _baseline_session(tmp_path)
    session._source = replace(
        session._source,
        config=replace(
            session._source.config,
            filter_json=canonical_json({"room": {"timeline": {"limit": limit}}}),
        ),
    )
    original = session._journal.load_source()
    body = b"x" * (ports.MAX_CANONICAL_STAGED_RESPONSE_BODY_BYTES + 1)
    try:
        with pytest.raises(
            IngestionSourceError, match="minimum timeline limit; source cursor retained"
        ):
            await _capture(session, monkeypatch, [body] * 4, [])
        assert session._journal.load_source() == original
        assert session._journal.list_frames(1) == ()
        assert session.next_batch() is None
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "pages",
    [
        [{"start": "wrong", "chunk": []}],
        [{"start": "s1", "end": "s1", "chunk": []}],
        [
            {"start": "s1", "end": "p1", "chunk": []},
            {"start": "p1", "end": "s1", "chunk": []},
        ],
        [{"start": "s1", "chunk": [{}]}],
        [403],
        [503, 503, 503],
    ],
)
async def test_incomplete_page_preserves_sync_cursor_without_partial_page_delivery(
    tmp_path, monkeypatch, pages
):
    session = await _baseline_session(tmp_path)
    original = session._journal.load_source()
    body, _, _ = _limited_body()
    try:
        with pytest.raises(IngestionSourceError, match="history recovery failed"):
            await _capture(session, monkeypatch, body, list(pages))
        current = load_json(session._journal.load_source().cursor_json, "cursor")
        assert current["next_batch"] == "s1"
        assert current["recovery"]["phase"] == "page"
        assert session._journal._load_classic_recovery() is not None
        assert session._journal.list_frames(1) == ()
        assert session.next_batch() is None
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_empty_advancing_page_and_overlap_preserve_one_ordered_delivery(
    tmp_path, monkeypatch
):
    session = await _baseline_session(tmp_path)
    body, missed, fresh = _limited_body()
    records = []
    try:
        await _capture(
            session,
            monkeypatch,
            body,
            [
                {"start": "s1", "end": "p1", "chunk": []},
                {"start": "p1", "end": "p2", "chunk": [missed]},
                {"start": "p2", "chunk": [missed, fresh]},
            ],
            recovered_records=records,
        )
        staged = session._journal.list_frames(1)[0]
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        records.extend(await _settle_frame(session, staged.frame_id))
        assert [
            record.event_id
            for record in records
            if getattr(record, "kind", None) is RecordKind.TIMELINE
        ] == [missed["event_id"], fresh["event_id"]]
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("bound", ["pages", "bytes"])
async def test_page_bound_keeps_completed_progress_retryable(
    tmp_path, monkeypatch, bound
):
    session = await _baseline_session(tmp_path)
    body, missed, _ = _limited_body()
    records = []
    if bound == "pages":
        monkeypatch.setattr(recovery, "_MAX_PAGES", 1)
    else:
        monkeypatch.setattr(recovery, "_MAX_PAGE_BYTES", 32)
    try:
        with pytest.raises(IngestionSourceError, match="history recovery failed"):
            await _capture(
                session,
                monkeypatch,
                body,
                [{"start": "s1", "end": "p1", "chunk": [missed]}] * 8,
                recovered_records=records,
            )
        current = load_json(session._journal.load_source().cursor_json, "cursor")
        assert current["next_batch"] == "s1"
        assert current["recovery"]["token"] == ("p1" if bound == "pages" else "s1")
        assert [record.event_id for record in records] == (
            [missed["event_id"]] if bound == "pages" else []
        )
        assert session.next_batch() is None
    finally:
        await session.close()
