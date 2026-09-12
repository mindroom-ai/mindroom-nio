"""Response parsing yields without moving durable state off its owning loop."""

import asyncio
import threading
from contextlib import asynccontextmanager

import pytest
from aiohttp import web

from nio.durable.codec import restore_event
from nio.durable.model import RecordKind
from nio.exceptions import LocalProtocolError

from .client_test import ROOM, client, open_session, response
from .membership_test import OPERATION
from .recovery_test import baseline
from .runner_test import drain_sync, homeserver
from .sliding_test import settings, window


@asynccontextmanager
async def blocked_decode(session, monkeypatch):
    loop = asyncio.get_running_loop()
    started = asyncio.Event()
    finished = asyncio.Event()
    release = threading.Event()
    decode = session._decode_response

    def blocked(body):
        loop.call_soon_threadsafe(started.set)
        try:
            if not release.wait(5):
                raise TimeoutError("decoder was not released")
            return decode(body)
        finally:
            loop.call_soon_threadsafe(finished.set)

    monkeypatch.setattr(session, "_decode_response", blocked)
    try:
        yield started, release
    finally:
        release.set()
        await asyncio.wait_for(finished.wait(), 5)


def observe_loop_progress(session, monkeypatch):
    loop = asyncio.get_running_loop()
    decode = session._decode_response
    progress = []

    def observed(body):
        heartbeat = threading.Event()
        loop.call_soon_threadsafe(heartbeat.set)
        # A watchdog bounds failure when decoding blocks the event loop.
        progress.append(heartbeat.wait(2))
        return decode(body)

    monkeypatch.setattr(session, "_decode_response", observed)
    return progress


@pytest.mark.asyncio
@pytest.mark.parametrize("sliding", [False, True])
async def test_fresh_decode_allows_loop_progress_before_durable_capture(
    tmp_path, monkeypatch, sliding
):
    session = open_session(tmp_path, config=settings() if sliding else None)
    body = window("$message-0") if sliding else response()
    progress = observe_loop_progress(session, monkeypatch)
    owner_thread = threading.get_ident()
    capture_threads = []
    capture = session._store.capture

    def record_capture(value):
        capture_threads.append(threading.get_ident())
        return capture(value)

    monkeypatch.setattr(session._store, "capture", record_capture)
    try:
        await session._accept_response(body)
        assert progress == [True]
        assert capture_threads == [owner_thread]
        records = []
        while batch := await session.next_batch():
            records.extend(batch.records)
            await session.ack(batch)
        assert [
            restore_event(record).event_id
            for record in records
            if record.kind is RecordKind.TIMELINE
        ] == ["$message-0"]
        assert session.cursor == ("p1" if sliding else "s1")
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("sliding", [False, True])
async def test_reopened_input_decode_allows_loop_progress(
    tmp_path, monkeypatch, sliding
):
    config = settings() if sliding else None
    session = open_session(tmp_path, config=config)
    body = window("$message-0") if sliding else response()
    # Reproduce a process stopping after raw input was durably captured.
    with session._store.transaction():
        session._store.capture(body)
        session._store.save_continuation(
            {"transport": "sliding" if sliding else "classic"}
        )
    await session.close()
    reopened = open_session(tmp_path, config=config)
    progress = observe_loop_progress(reopened, monkeypatch)
    try:
        await reopened._recovery.advance()
        assert progress == [True]
        records = []
        while batch := await reopened.next_batch():
            records.extend(batch.records)
            await reopened.ack(batch)
        assert [
            restore_event(record).event_id
            for record in records
            if record.kind is RecordKind.TIMELINE
        ] == ["$message-0"]
        assert reopened.cursor == ("p1" if sliding else "s1")
    finally:
        await reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("closing", [False, True])
async def test_local_membership_waits_for_decode_and_rechecks_close(
    tmp_path, monkeypatch, closing
):
    stop_poll = asyncio.Event()
    membership_calls = []

    async def sync(request):
        if request.query.get("since") != "s1":
            await stop_poll.wait()
        return web.Response(body=response(token="s2"))

    async def membership(request):
        membership_calls.append(request.path)
        return web.json_response({})

    async with homeserver(sync, membership=membership) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        command = None
        try:
            async with blocked_decode(session, monkeypatch) as (started, release):
                runner = asyncio.create_task(session.run())
                try:
                    await asyncio.wait_for(started.wait(), 5)
                    command = asyncio.create_task(
                        session.change_membership(
                            operation_id=OPERATION,
                            room_id=ROOM,
                            previous_membership="join",
                            previous_epoch=0,
                            current_membership="leave",
                        )
                    )
                    await asyncio.sleep(0)
                    assert session._read_local_intent() is None
                    if closing:
                        await session.close()
                        with pytest.raises(LocalProtocolError):
                            await asyncio.wait_for(command, 5)
                        assert membership_calls == []
                    else:
                        release.set()
                        await drain_sync(session)
                        assert await asyncio.wait_for(command, 5)
                        assert session.cursor == "s2"
                        assert membership_calls == [
                            f"/_matrix/client/v3/rooms/{ROOM}/leave"
                        ]
                finally:
                    release.set()
                    await session.close()
                    await asyncio.gather(runner, return_exceptions=True)
        finally:
            stop_poll.set()
            if command is not None:
                command.cancel()
                await asyncio.gather(command, return_exceptions=True)
            await nio_client.close()


@pytest.mark.asyncio
async def test_subscription_update_during_decode_keeps_fresh_connection(
    tmp_path, monkeypatch
):
    session = open_session(tmp_path, config=settings())
    try:
        async with blocked_decode(session, monkeypatch) as (started, release):
            operation = asyncio.create_task(
                session._accept_response(window("$message-0"))
            )
            try:
                await asyncio.wait_for(started.wait(), 5)
                await session.update_sliding_subscriptions(
                    {"!new:example.org": {"timeline_limit": 10}}
                )
                release.set()
                await operation
                assert session.cursor == "p1"
                assert session._sliding.pos is None
            finally:
                release.set()
                await asyncio.gather(operation, return_exceptions=True)
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("replay", [False, True])
@pytest.mark.parametrize("stop", ["cancel", "close"])
async def test_stopping_during_decode_preserves_replayable_cursor(
    tmp_path, monkeypatch, replay, stop
):
    session = open_session(tmp_path)
    body = response()
    if replay:
        with session._store.transaction():
            session._store.capture(body)
        await session.close()
        session = open_session(tmp_path)
    try:
        async with blocked_decode(session, monkeypatch) as (started, release):
            operation = asyncio.create_task(
                session._recovery.advance()
                if replay
                else session._accept_response(body)
            )
            try:
                await asyncio.wait_for(started.wait(), 5)
                if stop == "cancel":
                    operation.cancel()
                    with pytest.raises(asyncio.CancelledError):
                        await operation
                else:
                    await session.close()
                    release.set()
                    with pytest.raises(LocalProtocolError):
                        await operation
            finally:
                release.set()
                await asyncio.gather(operation, return_exceptions=True)
    finally:
        await session.close()
    reopened = open_session(tmp_path)
    try:
        assert reopened.cursor is None
        assert (reopened._store.input is not None) == replay
        if replay:
            await reopened._recovery.advance()
        else:
            await reopened._accept_response(body)
        records = []
        while batch := await reopened.next_batch():
            records.extend(batch.records)
            await reopened.ack(batch)
        assert [
            restore_event(record).event_id
            for record in records
            if record.kind is RecordKind.TIMELINE
        ] == ["$message-0"]
        assert reopened.cursor == "s1"
    finally:
        await reopened.close()
