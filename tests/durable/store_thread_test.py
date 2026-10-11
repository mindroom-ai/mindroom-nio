"""BEGIN and COMMIT run on the store's thread while the event loop keeps running."""

import asyncio
import sqlite3
import threading
import time

import pytest
from aiohttp import web

from nio.event_builders import ToDeviceMessage
from nio.exceptions import LocalProtocolError
from nio.rooms import MatrixRoom

from .client_test import ROOM, USER, client, open_session, response
from .crypto_test import publish_account, queue_dummy
from .recovery_test import baseline, member
from .runner_test import homeserver
from .store_test import message, open_store

SLOW = 0.3
# Generous for loaded CI; synchronous commits stall for the whole SLOW delay.
RESPONSIVE = 0.15


def store_threads(store):
    return [thread for thread in store._thread._threads if thread.is_alive()]


def slow_statement(store, statement, *, skip=0):
    """Delay a statement inside SQLite, recording which thread ran each one."""
    started = threading.Event()
    threads = []

    def trace(sql):
        nonlocal skip
        threads.append((sql, threading.current_thread().name))
        if sql == statement:
            if skip:
                skip -= 1
                return
            started.set()
            time.sleep(SLOW)

    store.database.connection().set_trace_callback(trace)
    return started, threads


class Ticker:
    """Measure the longest gap between event-loop wakeups."""

    def __init__(self):
        self.gap = 0.0
        self.last = time.monotonic()
        self.task = asyncio.create_task(self._run())

    async def _run(self):
        while True:
            await asyncio.sleep(0.005)
            self._tick()

    def _tick(self):
        now = time.monotonic()
        self.gap = max(self.gap, now - self.last)
        self.last = now

    async def stop(self):
        self._tick()
        self.task.cancel()
        await asyncio.gather(self.task, return_exceptions=True)
        return self.gap


async def until(event):
    while not event.is_set():
        await asyncio.sleep(0.001)


@pytest.mark.asyncio
async def test_slow_commit_runs_on_store_thread_while_loop_keeps_running(tmp_path):
    store = open_store(tmp_path)
    try:
        _, threads = slow_statement(store, "COMMIT")
        ticker = Ticker()
        started = time.monotonic()
        async with store.transaction():
            store.publish((message(),))
        elapsed = time.monotonic() - started
        assert await ticker.stop() < RESPONSIVE
        assert elapsed >= SLOW
        by_statement = {sql.split()[0]: name for sql, name in threads}
        assert by_statement["BEGIN"].startswith("nio-durable-store")
        assert by_statement["COMMIT"].startswith("nio-durable-store")
        assert by_statement["INSERT"] == threading.current_thread().name
        assert store.next_batch() is not None
    finally:
        store.close()


@pytest.mark.asyncio
async def test_begin_waits_for_another_writer_off_the_loop(tmp_path):
    store = open_store(tmp_path)
    writer = sqlite3.connect(store.path, check_same_thread=False)
    try:
        writer.execute("BEGIN IMMEDIATE")
        release = threading.Timer(SLOW, writer.rollback)
        release.start()
        ticker = Ticker()
        started = time.monotonic()
        async with store.transaction():
            store.publish((message(),))
        assert time.monotonic() - started >= SLOW * 0.9
        assert await ticker.stop() < RESPONSIVE
        assert store.has_batches()
        release.join()
    finally:
        writer.close()
        store.close()


@pytest.mark.asyncio
async def test_status_reads_return_committed_values_during_slow_commit(tmp_path):
    store = open_store(tmp_path)
    try:
        started, _ = slow_statement(store, "COMMIT")

        async def publish():
            async with store.transaction():
                store.set_cursor("s1")
                store.publish((message(),))

        task = asyncio.create_task(publish())
        await until(started)
        reading = time.monotonic()
        assert store.cursor is None
        assert not store.has_batches()
        assert store.progress_generation == 0
        assert time.monotonic() - reading < RESPONSIVE
        with pytest.raises(LocalProtocolError, match="requires a transaction"):
            store.set_cursor("other task")
        await task
        assert store.cursor == "s1"
        assert store.has_batches()
        assert store.progress_generation == 1
    finally:
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("statement", ["BEGIN IMMEDIATE", "COMMIT"])
async def test_cancelled_transaction_completes_before_releasing_store(
    tmp_path, statement
):
    store = open_store(tmp_path)
    try:
        started, _ = slow_statement(store, statement)
        progress = []

        async def publish():
            async with store.transaction():
                store.publish((message(),))
                progress.append("body")
            progress.append("after")
            await asyncio.sleep(10)
            progress.append("not cancelled")

        task = asyncio.create_task(publish())
        await until(started)
        task.cancel()
        with pytest.raises(asyncio.CancelledError):
            await task
        # Cancellation arrives at the first suspension after the transaction.
        assert progress == ["body", "after"]
        assert not store._lock.locked()
        assert not store.database.in_transaction()
        assert store.next_batch().sequence == 1
        store.database.connection().set_trace_callback(None)
        async with store.transaction():
            assert store.publish((message("$two"),)).sequence == 2
    finally:
        store.close()


@pytest.mark.asyncio
async def test_cancelled_rollback_and_lock_wait_leave_store_usable(tmp_path):
    store = open_store(tmp_path)
    try:
        started, _ = slow_statement(store, "ROLLBACK")

        async def fail():
            async with store.transaction():
                store.publish((message(),))
                raise RuntimeError("rollback")

        async def wait_for_lock():
            async with store.transaction():
                store.publish((message("$waited"),))

        failing = asyncio.create_task(fail())
        await until(started)
        waiting = asyncio.create_task(wait_for_lock())
        await asyncio.sleep(0)
        failing.cancel()
        waiting.cancel()
        with pytest.raises(RuntimeError, match="rollback"):
            await failing
        with pytest.raises(asyncio.CancelledError):
            await waiting
        # The waiter kept its place, committed, then saw its cancellation.
        batch = store.next_batch()
        assert batch.records[0].source["event_id"] == "$waited"
        assert not store._lock.locked()
    finally:
        store.close()


@pytest.mark.asyncio
async def test_timeout_during_commit_raises_timeout_after_commit(tmp_path):
    store = open_store(tmp_path)
    try:
        slow_statement(store, "COMMIT")
        with pytest.raises(TimeoutError):
            async with asyncio.timeout(SLOW / 3):
                async with store.transaction():
                    store.publish((message(),))
                await asyncio.sleep(10)
        assert store.has_batches()
        assert asyncio.current_task().cancelling() == 0
    finally:
        store.close()


@pytest.mark.asyncio
async def test_close_waits_for_inflight_commit_and_reaps_thread(tmp_path):
    store = open_store(tmp_path)
    started, _ = slow_statement(store, "COMMIT")

    async def publish():
        async with store.transaction():
            store.publish((message(),))

    task = asyncio.create_task(publish())
    await until(started)
    assert len(store_threads(store)) == 1
    store.close()
    assert not store_threads(store)
    await task
    with pytest.raises(LocalProtocolError, match="closed"):
        async with store.transaction():
            pass
    reopened = open_store(tmp_path)
    try:
        assert reopened.next_batch().sequence == 1
    finally:
        reopened.close()
    assert not store_threads(reopened)


@pytest.mark.asyncio
async def test_close_discards_begun_transaction_before_its_body(tmp_path):
    store = open_store(tmp_path)
    started, _ = slow_statement(store, "BEGIN IMMEDIATE")
    body = []

    async def publish():
        async with store.transaction():
            body.append(True)

    task = asyncio.create_task(publish())
    await until(started)
    store.close()
    with pytest.raises(LocalProtocolError, match="closed"):
        await task
    assert not body
    assert not store_threads(store)


@pytest.mark.asyncio
@pytest.mark.parametrize("statement", ["BEGIN IMMEDIATE", "COMMIT"])
async def test_blocking_transaction_waits_out_inflight_boundary(tmp_path, statement):
    store = open_store(tmp_path)
    try:
        started, _ = slow_statement(store, statement)

        async def publish():
            async with store.transaction():
                store.set_cursor("async")
                store.publish((message("$async"),))

        task = asyncio.create_task(publish())
        await until(started)
        # A synchronous public call, such as a device trust change.
        with store.blocking_transaction():
            store.publish((message("$blocking"),))
        await task
        assert store.cursor == "async"
        first = store.next_batch()
        await store.ack(first)
        second = store.next_batch()
        await store.ack(second)
        order = [batch.records[0].source["event_id"] for batch in (first, second)]
        # A begun transaction's body has not run, so the blocking work commits first.
        assert order == (
            ["$blocking", "$async"]
            if statement == "BEGIN IMMEDIATE"
            else ["$async", "$blocking"]
        )
        assert store.next_batch() is None
    finally:
        store.close()


@pytest.mark.asyncio
async def test_member_lookup_does_not_overwrite_invalidation_during_commit(tmp_path):
    async def members(_):
        return web.json_response(
            {"joined": {USER: {"display_name": "Alice", "avatar_url": None}}}
        )

    async with homeserver(None, membership=members) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        nio_client.rooms[ROOM] = MatrixRoom(ROOM, USER, encrypted=True)
        try:
            started, _ = slow_statement(session._store, "COMMIT")
            lookup = asyncio.create_task(session._outbound.ensure_members(ROOM))
            await until(started)
            # Sync observes a membership change while the lookup commits.
            session._outbound.member_cache.pop(ROOM, None)
            with pytest.raises(LocalProtocolError, match="membership changed"):
                await lookup
            assert ROOM not in session._outbound.member_cache
            session._assert_active()
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_group_share_rechecks_membership_after_transaction_starts(tmp_path):
    import json

    departed = "@old:example.org"
    sent = []

    async def send(request):
        sent.append(await request.json())
        return web.json_response({})

    async with homeserver(None, membership=send) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        try:
            await baseline(session)
            room = nio_client.rooms[ROOM]
            room.encrypted = True
            room.members_synced = True
            async with session._store.transaction():
                publish_account(nio_client.olm)
                _, device = queue_dummy(nio_client.olm, user_id=departed)
                nio_client.olm.outgoing_to_device_messages.clear()
                nio_client.verify_device(device)
                session._crypto.capture()
            body = json.loads(response(token="s2", messages=0))
            body["rooms"]["join"][ROOM]["state"]["events"] = []
            body["rooms"]["join"][ROOM]["timeline"]["events"] = [
                member("$departure", "leave", departed)
            ]
            await session._capture_response(json.dumps(body).encode())
            # Preparation takes the store first and removes the member while
            # sharing waits to start its transaction.
            prepared, shared = await asyncio.gather(
                session._prepare_pending(),
                nio_client.share_group_session(ROOM),
                return_exceptions=True,
            )
            assert prepared is None
            assert isinstance(shared, LocalProtocolError)
            assert "membership changed" in str(shared)
            assert departed not in room.users
            assert sent == []
            session._assert_active()
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_runner_status_check_tolerates_send_whose_commit_is_running(tmp_path):
    async def send(_):
        return web.json_response({})

    async with homeserver(None, membership=send) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        try:
            async with session._store.transaction():
                publish_account(nio_client.olm)
                session._crypto.capture()
            # The first COMMIT retains the request; the second applies the response.
            started, _ = slow_statement(session._store, "COMMIT", skip=1)
            sending = asyncio.create_task(
                nio_client.to_device(
                    ToDeviceMessage("org.example.notice", USER, "OTHER", {"value": 1}),
                    "stable-send",
                )
            )
            await until(started)
            session._quiescing = True
            await session.run()
            assert (await sending).__class__.__name__ == "ToDeviceResponse"
            assert session._crypto._pending() is None
        finally:
            await session.close()
            await nio_client.close()
