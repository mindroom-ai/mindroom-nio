"""Fresh observations and committed callback replay use distinct lifetimes."""

import asyncio
import json

import pytest

from nio import PresenceEvent, RoomMessageText, TypingNoticeEvent
from nio.durable.model import RecordKind

from .client_test import ROOM, USER, client, open_session, response


def transient_body(typing=None):
    root = json.loads(response())
    root["presence"] = {
        "events": [
            {"type": "m.presence", "sender": USER, "content": {"presence": "online"}}
        ]
    }
    root["rooms"]["join"][ROOM]["ephemeral"] = {
        "events": [typing or {"type": "m.typing", "content": {"user_ids": [USER]}}]
    }
    return json.dumps(root).encode()


@pytest.mark.asyncio
async def test_fresh_transients_project_and_dispatch_but_never_replay(
    tmp_path, monkeypatch
):
    nio_client = client()
    seen = []
    nio_client.add_ephemeral_callback(
        lambda room, event: seen.append(event), TypingNoticeEvent
    )
    nio_client.add_presence_callback(lambda event: seen.append(event), PresenceEvent)
    session = open_session(tmp_path, nio_client)
    body = transient_body()
    loads = json.loads
    decoded = 0

    def counted(value, *args, **kwargs):
        nonlocal decoded
        if value == body:
            decoded += 1
        return loads(value, *args, **kwargs)

    monkeypatch.setattr(json, "loads", counted)
    await session._accept_response(body)
    assert decoded == 1
    assert len(seen) == 2
    assert nio_client.rooms[ROOM].typing_users == [USER]
    assert nio_client.rooms[ROOM].users[USER].presence == "online"
    await session.close()
    reopened = open_session(tmp_path)
    try:
        reopened._prepare_pending()
        assert reopened.client.rooms[ROOM].typing_users == []
        assert len(seen) == 2
        assert await reopened.next_batch() is not None
    finally:
        await reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("failure", ["malformed", "callback"])
async def test_transient_failure_keeps_durable_progress_and_safe_diagnostics(
    tmp_path, caplog, failure
):
    nio_client = client()

    def callback(room, event):
        raise RuntimeError("payload-secret")

    if failure == "callback":
        nio_client.add_ephemeral_callback(callback, TypingNoticeEvent)
    session = open_session(tmp_path, nio_client)
    try:
        malformed = {"type": "m.typing", "content": {"user_ids": "payload-secret"}}
        await session._accept_response(
            transient_body(malformed if failure == "malformed" else None)
        )
        records = []
        while batch := await session.next_batch():
            records.extend(batch.records)
            await session.ack(batch)
        assert any(record.kind is RecordKind.TIMELINE for record in records)
        assert "payload-secret" not in caplog.text
        assert (
            "malformed=1" if failure == "malformed" else "RuntimeError"
        ) in caplog.text
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_slow_transient_callback_cannot_hold_durable_runner(tmp_path, caplog):
    nio_client = client()
    cancelled = asyncio.Event()

    async def slow(room, event):
        try:
            await asyncio.Event().wait()
        finally:
            cancelled.set()

    nio_client.add_ephemeral_callback(slow, TypingNoticeEvent)
    session = open_session(tmp_path, nio_client)
    try:
        async with asyncio.timeout(3):
            await session._accept_response(transient_body())
        assert cancelled.is_set()
        assert await session.next_batch() is not None
        assert "timeout=True" in caplog.text
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_callback_replay_failure_leaves_committed_batch_available(tmp_path):
    session = open_session(tmp_path)
    await session._accept_response(response())
    while batch := await session.next_batch():
        if any(record.kind is RecordKind.TIMELINE for record in batch.records):
            break
        await session.ack(batch)
    await session.close()
    nio_client = client()
    seen = []

    def callback(room, event):
        seen.append(event.body)
        raise RuntimeError("callback failed")

    nio_client.add_event_callback(callback, RoomMessageText)
    reopened = open_session(tmp_path, nio_client)
    try:
        record = next(
            record for record in batch.records if record.kind is RecordKind.TIMELINE
        )
        with pytest.raises(RuntimeError, match="callback failed"):
            await reopened.dispatch(record)
        assert seen == ["hello 0"]
        assert await reopened.next_batch() == batch
        await reopened.ack(batch)
        assert await reopened.next_batch() is None
    finally:
        await reopened.close()
