"""Recovered relation events retain their identity through delivery and restart."""

import asyncio

import pytest
from owned_capacity_test import (
    ACCOUNT_ID,
    _baseline_session,
    _open_session,
    _settle_frame,
)
from owned_recovery_test import _capture, _limited_body

from nio.event_provenance import TimelineEventProvenance
from nio.events import ReactionEvent, RedactionEvent, RoomMessageText
from nio.ingest.model import RecordKind
from nio.ingest.source import canonical_json, load_json
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
@pytest.mark.parametrize("content_redacts", [False, True])
async def test_recovered_relation_events_keep_raw_type_and_callback_class(
    tmp_path, monkeypatch, restart, content_redacts
):
    session = await _baseline_session(tmp_path)
    body, message, fresh = _limited_body()
    reaction = {
        "type": "m.reaction",
        "event_id": "$reaction",
        "sender": ACCOUNT_ID,
        "origin_server_ts": 103,
        "content": {
            "m.relates_to": {
                "rel_type": "m.annotation",
                "event_id": message["event_id"],
                "key": "managed-stream-fence",
            }
        },
    }
    redaction = {
        "type": "m.room.redaction",
        "event_id": "$redaction",
        "sender": ACCOUNT_ID,
        "origin_server_ts": 104,
        "content": {"reason": "remove reaction"},
    }
    if content_redacts:
        redaction["content"]["redacts"] = reaction["event_id"]
    else:
        redaction["redacts"] = reaction["event_id"]
    recovered = [message, reaction, redaction]
    pages = [{"start": "s1", "chunk": recovered}]
    records = []
    callbacks = []

    def collect(_room, event):
        callbacks.append(event)

    def interrupt_callback(_room, _event):
        raise asyncio.CancelledError

    session._client.add_event_callback(interrupt_callback if restart else collect, None)
    try:
        if restart:
            with pytest.raises(asyncio.CancelledError):
                await _capture(
                    session, monkeypatch, body, pages, recovered_records=records
                )
            pending = session.next_batch()
            assert pending is not None
            assert pending.records[0].event_id == message["event_id"]
            assert pending.records[0].provenance is TimelineEventProvenance.RECOVERED
            await session.close()
            session = _open_session(tmp_path)
            assert session.next_batch() == pending
            session._client.add_event_callback(collect, None)
            records.clear()
        await _capture(session, monkeypatch, body, pages, recovered_records=records)
        tail = session._journal.list_frames(1)[0]
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        records.extend(await _settle_frame(session, tail.frame_id))
        timeline = [record for record in records if record.kind is RecordKind.TIMELINE]
        assert [record.event_id for record in timeline] == [
            event["event_id"] for event in recovered + [fresh]
        ]
        assert [record.source_json for record in timeline] == [
            canonical_json(event) for event in recovered + [fresh]
        ]
        assert [
            load_json(record.source_json, "event")["type"] for record in timeline
        ] == ["m.room.message", "m.reaction", "m.room.redaction", "m.room.message"]
        assert all(record.clear_json is None for record in timeline)
        assert [record.provenance for record in timeline] == [
            TimelineEventProvenance.RECOVERED,
            TimelineEventProvenance.RECOVERED,
            TimelineEventProvenance.RECOVERED,
            TimelineEventProvenance.LIVE,
        ]
        assert [type(event) for event in callbacks] == [
            RoomMessageText,
            ReactionEvent,
            RedactionEvent,
            RoomMessageText,
        ]
        assert [event.event_id for event in callbacks] == [
            event["event_id"] for event in recovered + [fresh]
        ]
        assert callbacks[1].reacts_to == message["event_id"]
        assert callbacks[1].key == "managed-stream-fence"
        assert callbacks[2].redacts == reaction["event_id"]
        assert not pages
    finally:
        await session.close()
