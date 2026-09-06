"""Check real-server Sliding downtime and limited-window recovery.

The server must allow dummy registration. All users and messages are synthetic.
The required store directory retains the SQLite store and payload-free result.

    uv run python scripts/check_durable_sliding.py \
        --homeserver http://127.0.0.1:8008 \
        --store-path .local-state/sliding-check
"""

from __future__ import annotations

import argparse
import asyncio
import hashlib
import json
import logging
import secrets
import sqlite3
from collections import Counter
from contextlib import AsyncExitStack, asynccontextmanager
from dataclasses import dataclass, field
from pathlib import Path
from uuid import UUID, uuid4

from aiohttp import ClientSession, TraceConfig

from nio import (
    AsyncClient,
    AsyncClientConfig,
    JoinResponse,
    RegisterResponse,
    RoomCreateResponse,
    RoomSendResponse,
    TimelineEventProvenance,
)
from nio.durable import (
    DurableSync,
    DurableSyncConfig,
    RecordKind,
    SlidingSyncConfig,
    SyncBatch,
    open_durable_sync,
)


def emit(stage: str, **fields: object) -> None:
    print(json.dumps({"stage": stage, **fields}, sort_keys=True), flush=True)


def fingerprint(value: str) -> str:
    return hashlib.sha256(value.encode()).hexdigest()[:16]


@dataclass
class Observations:
    room_id: str
    messages: Counter[str] = field(default_factory=Counter)
    actionable: Counter[str] = field(default_factory=Counter)
    recovered: int = 0
    losses: int = 0
    batches: int = 0
    completions: int = 0
    sequence: int = 0

    def record(self, batch: SyncBatch) -> None:
        assert batch.sequence > self.sequence, "batch sequence did not advance"
        self.sequence = batch.sequence
        self.batches += 1
        self.completions += batch.completes_sync
        for record in batch.records:
            self.losses += record.kind is RecordKind.LOSS
            if (
                record.kind is not RecordKind.TIMELINE
                or record.room_id != self.room_id
                or record.source.get("type") != "m.room.message"
            ):
                continue
            event_id = record.source["event_id"]
            self.messages[event_id] += 1
            if record.provenance in (
                TimelineEventProvenance.LIVE,
                TimelineEventProvenance.RECOVERED,
            ):
                self.actionable[event_id] += 1
            self.recovered += record.provenance is TimelineEventProvenance.RECOVERED

    def summary(self, expected: set[str]) -> dict[str, object]:
        return {
            "expected": len(expected),
            "observed": sum(self.messages.values()),
            "actionable": sum(self.actionable.values()),
            "recovered": self.recovered,
            "losses": self.losses,
            "batches": self.batches,
            "completed_syncs": self.completions,
            "missing": sorted(
                fingerprint(value) for value in expected - self.messages.keys()
            ),
            "unexpected": sorted(
                fingerprint(value) for value in self.messages.keys() - expected
            ),
            "duplicates": sum(count - 1 for count in self.messages.values()),
        }

    def check(self, expected: set[str], *, history: bool = False) -> None:
        assert self.losses == 0, "recovery emitted loss"
        assert self.messages == Counter(
            dict.fromkeys(expected, 1)
        ), "missing, duplicate or unexpected message"
        assert self.actionable == (
            Counter() if history else self.messages
        ), "incorrect actionability"
        if not history:
            assert self.recovered > 0, "limited window did not exercise recovery"


async def register(homeserver: str, name: str) -> AsyncClient:
    client = AsyncClient(homeserver, config=AsyncClientConfig(encryption_enabled=False))
    try:
        response = await client.register(name, secrets.token_urlsafe(32))
        assert isinstance(response, RegisterResponse), "dummy registration failed"
        return client
    except BaseException:
        await client.close()
        raise


async def send_messages(
    client: AsyncClient, room_id: str, count: int, phase: str
) -> set[str]:
    ids = set()
    for index in range(count):
        response = await client.room_send(
            room_id, "m.room.message", {"msgtype": "m.text", "body": f"{phase}-{index}"}
        )
        assert isinstance(
            response, RoomSendResponse
        ), "sending synthetic message failed"
        ids.add(response.event_id)
    assert len(ids) == count, "server reused a message event ID"
    return ids


@asynccontextmanager
async def receiver(
    homeserver: str,
    identity: tuple[str, str, str],
    consumer_id: UUID,
    store_path: Path,
    room_id: str,
    requests: Counter[str],
):
    trace = TraceConfig()

    async def request_started(_client, _context, params) -> None:
        if params.url.path.endswith("/messages"):
            requests["history_pages"] += 1
        elif params.url.path.endswith("/sync"):
            requests["sliding_polls"] += 1
            requests["initial_polls"] += "pos" not in params.url.query

    trace.on_request_start.append(request_started)
    user_id, device_id, access_token = identity
    client = AsyncClient(homeserver, user_id, device_id=device_id)
    client.restore_login(user_id, device_id, access_token)
    client.client_session = ClientSession(trace_configs=[trace])
    session = None
    try:
        session = open_durable_sync(
            client,
            consumer_id=consumer_id,
            store_path=store_path,
            config=DurableSyncConfig(
                sync_timeout_ms=1000,
                recovery_page_size=5,
                max_recovery_pages=100,
                sliding=SlidingSyncConfig(
                    conn_id="durable-sliding-check",
                    lists={},
                    room_subscriptions={
                        room_id: {
                            "timeline_limit": 1,
                            "required_state": [["*", "*"], ["m.room.member", "$ME"]],
                        }
                    },
                    extensions={
                        name: {"enabled": True}
                        for name in ("to_device", "e2ee", "account_data")
                    },
                ),
            ),
        )
        runner = asyncio.create_task(session.run())
        yield session, runner
    finally:
        try:
            if session is not None:
                await session.close()
        finally:
            await client.close()


async def next_batch(
    session: DurableSync, runner: asyncio.Task[None]
) -> SyncBatch | None:
    while True:
        if runner.done():
            await runner
            return await session.next_batch()
        if batch := await session.next_batch():
            return batch
        ready = asyncio.create_task(session.wait_for_work())
        try:
            await asyncio.wait({ready, runner}, return_when=asyncio.FIRST_COMPLETED)
        finally:
            ready.cancel()
            await asyncio.gather(ready, return_exceptions=True)


async def collect(
    session: DurableSync,
    runner: asyncio.Task[None],
    observations: Observations,
    expected: set[str],
    phase: str,
) -> SyncBatch:
    """Hold the completion batch so callers control the next real poll."""
    try:
        while True:
            batch = await next_batch(session, runner)
            assert batch is not None, "source stopped before expected messages arrived"
            observations.record(batch)
            if batch.completes_sync and expected <= observations.messages.keys():
                return batch
            await session.ack(batch)
    finally:
        emit(phase, **observations.summary(expected))


async def quiesce(
    session: DurableSync, runner: asyncio.Task[None], observations: Observations
) -> None:
    stopping = asyncio.create_task(session.quiesce())
    try:
        while batch := await next_batch(session, runner):
            observations.record(batch)
            await session.ack(batch)
        await stopping
        await runner
        assert await session.next_batch() is None, "batch debt remains after quiesce"
        assert session.cursor, "completed sync has no cursor"
    finally:
        if not stopping.done():
            stopping.cancel()
        await asyncio.gather(stopping, return_exceptions=True)


def check_store(store_path: Path) -> str:
    databases = list(store_path.glob("*.db"))
    assert len(databases) == 1, "expected one durable database"
    with sqlite3.connect(f"file:{databases[0]}?mode=ro", uri=True) as database:
        assert (
            database.execute("SELECT COUNT(*) FROM NioDurableInput").fetchone()[0] == 0
        ), "retained input debt remains"
        assert (
            database.execute("SELECT COUNT(*) FROM NioDurableBatch").fetchone()[0] == 0
        ), "output debt remains"
        cursor, acked = database.execute(
            "SELECT cursor,acked_sequence FROM NioDurableMeta WHERE id=1"
        ).fetchone()
        sequence = database.execute(
            "SELECT seq FROM sqlite_sequence WHERE name='NioDurableBatch'"
        ).fetchone()[0]
        assert cursor, "persisted cursor is missing"
        assert acked == sequence, "persisted acknowledgement is incomplete"
        return cursor


async def run(args: argparse.Namespace, directory: Path) -> dict[str, object]:
    async with AsyncExitStack() as stack:
        emit("register")
        sender = await register(args.homeserver, f"sliding-sender-{directory.name}")
        stack.push_async_callback(sender.close)
        setup_receiver = await register(
            args.homeserver, f"sliding-receiver-{directory.name}"
        )
        stack.push_async_callback(setup_receiver.close)
        created = await sender.room_create(
            name="Durable Sliding check", invite=[setup_receiver.user_id]
        )
        assert isinstance(created, RoomCreateResponse), "room creation failed"
        room_id = created.room_id
        joined = await setup_receiver.join(room_id)
        assert isinstance(joined, JoinResponse), "receiver join failed"
        identity = (
            setup_receiver.user_id,
            setup_receiver.device_id,
            setup_receiver.access_token,
        )
        await setup_receiver.close()
        consumer_id = uuid4()
        store_path = directory / "receiver"
        baseline_ids = await send_messages(sender, room_id, 1, "baseline")
        baseline = Observations(room_id)
        first_requests: Counter[str] = Counter()
        async with receiver(
            args.homeserver, identity, consumer_id, store_path, room_id, first_requests
        ) as (session, runner):
            stream_id = session.stream_id
            batch = await collect(session, runner, baseline, baseline_ids, "baseline")
            await session.ack(batch)
            await quiesce(session, runner, baseline)
            baseline.check(baseline_ids, history=True)
        old_cursor = check_store(store_path)

        emit("send_downtime", count=args.messages)
        missed = await send_messages(sender, room_id, args.messages, "downtime")
        recovered = Observations(room_id)
        live = Observations(room_id)
        requests: Counter[str] = Counter()
        async with receiver(
            args.homeserver, identity, consumer_id, store_path, room_id, requests
        ) as (session, runner):
            assert session.stream_id == stream_id, "reopen lost stream identity"
            assert session.cursor == old_cursor, "reopen lost cursor"
            barrier = await collect(session, runner, recovered, missed, "restart")
            recovered.check(missed)
            assert requests["initial_polls"] >= 1, "restart reused connection position"
            assert requests["history_pages"] > 0, "restart did not page real history"
            restart_pages = requests["history_pages"]
            restart_cursor = session.cursor
            burst = await send_messages(sender, room_id, args.messages, "live-burst")
            assert (
                await session.next_batch() == barrier
            ), "unacknowledged completion batch changed"
            assert (
                session.cursor == restart_cursor
            ), "source advanced past unacknowledged output"
            await session.ack(barrier)
            batch = await collect(session, runner, live, burst, "live_limited_window")
            await session.ack(batch)
            await quiesce(session, runner, live)
            live.check(burst)
            assert (
                requests["history_pages"] > restart_pages
            ), "live burst did not page real history"
        cursor = check_store(store_path)
        assert cursor != old_cursor, "cursor did not advance after recovery"
        return {
            "ok": True,
            "restart": recovered.summary(missed),
            "live_limited_window": live.summary(burst),
            "requests": dict(requests),
            "cursor_hash": fingerprint(cursor),
            "batch_debt": 0,
            "input_debt": 0,
        }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--homeserver", required=True)
    parser.add_argument("--store-path", type=Path, required=True)
    parser.add_argument("--messages", type=int, default=20)
    parser.add_argument("--timeout", type=float, default=90)
    args = parser.parse_args()
    if args.messages < 2 or args.timeout <= 0:
        parser.error("--messages must be at least 2 and --timeout must be positive")
    root = args.store_path.resolve()
    if any(
        root.is_relative_to(path)
        for path in ("/tmp", "/private/tmp", "/var/tmp", "/run")
    ):
        parser.error("--store-path must name a persistent directory")
    directory = root / secrets.token_hex(6)
    directory.mkdir(parents=True)
    logging.disable(logging.CRITICAL)

    async def bounded() -> dict[str, object]:
        async with asyncio.timeout(args.timeout):
            return await run(args, directory)

    try:
        result = asyncio.run(bounded())
    except Exception as error:
        result = {"ok": False, "error_type": type(error).__name__}
    result["run_id"] = directory.name
    (directory / "result.json").write_text(json.dumps(result, indent=2) + "\n")
    emit("result", **result)
    if not result["ok"]:
        raise SystemExit(1)


if __name__ == "__main__":
    main()
