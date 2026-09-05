"""Recovery suppresses only its own intermediate completion callbacks."""

import pytest
from owned_capacity_test import (
    _baseline_session,
    _messages,
    _settle_frame,
    _stage,
)
from owned_recovery_test import _capture, _limited_body

from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus


@pytest.mark.asyncio
async def test_quiesce_recovery_preserves_prior_ordinary_frame_completion(
    tmp_path, monkeypatch
):
    session = await _baseline_session(tmp_path)
    completions = []

    async def complete(value):
        completions.append(value.frame_id)

    session._completion_sink = complete
    try:
        ordinary = _stage(session, _messages(0))
        body, missed, _fresh = _limited_body()
        body["next_batch"] = "s3"
        pages = [{"start": "s2", "end": "t1", "chunk": [missed]}]
        await _capture(session, monkeypatch, body, pages)
        tail = session._journal.list_frames(1)[0]
        assert (
            session._materialize_oldest_frame(limits=MaterializerLimits()).status
            is MaterializeStatus.MATERIALIZED
        )
        await _settle_frame(session, tail.frame_id)
        assert completions == [ordinary.frame_id, tail.frame_id]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_recovery_eligibility_uses_membership_after_earlier_frames(
    tmp_path, monkeypatch
):
    from owned_capacity_test import ROOM_ID
    from nio.ingest.model import RecordKind

    session = await _baseline_session(tmp_path)
    try:
        _stage(
            session,
            {
                "next_batch": "s2",
                "rooms": {
                    "leave": {
                        ROOM_ID: {
                            "timeline": {
                                "events": [],
                                "limited": False,
                                "prev_batch": "t0",
                            }
                        }
                    }
                },
            },
        )
        body, missed, _ = _limited_body()
        body["next_batch"] = "s3"
        records = []
        requests = await _capture(
            session,
            monkeypatch,
            body,
            [{"start": "s2", "end": "t1", "chunk": [missed]}],
            recovered_records=records,
        )
        assert [request.path for request in requests] == ["/_matrix/client/v3/sync"]
        assert [record.kind for record in records] == [RecordKind.ROOM_LIFECYCLE]
    finally:
        await session.close()
