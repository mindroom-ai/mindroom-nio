"""Measure event-loop lag while a durable sync session commits responses.

A local homeserver answers every /sync at once, each response carrying
``--messages`` new events, and a consumer task drains and acknowledges every
batch. A heartbeat task wakes every 5 ms and records how late it wakes. An
fsync on the event loop, such as a durable commit with ``synchronous=FULL``,
shows up as heartbeat lag of about one fsync.

Make fsync slow in one of two ways:

* On Linux, strace delays the real syscalls without root; this is the
  deterministic one::

      strace -f -qq -o /dev/null --seccomp-bpf -e trace=fsync,fdatasync \\
          -e inject=fsync,fdatasync:delay_enter=100000 \\
          uv run python scripts/bench_durable_commit_lag.py

* ``--disk-pressure`` runs a second process that keeps writing and fsyncing a
  large file next to the database, so real fsyncs queue behind its writeback.
  How much this slows fsync depends on the disk and filesystem.
"""

from __future__ import annotations

import argparse
import asyncio
import json
import platform
import sqlite3
import statistics
import subprocess
import sys
import tempfile
import time
from pathlib import Path
from uuid import UUID

from aiohttp import web

from nio import AsyncClient, AsyncClientConfig
from nio.durable import DurableSyncConfig, open_durable_sync

USER = "@bench:example.org"
ROOM = "!bench:example.org"
CONSUMER = UUID("2f1d3b52-8a0e-4f5c-9a43-6b0a1c2e7d10")
TICK = 0.005
STALL = 0.05

PRESSURE = """
import os, sys
chunk = os.urandom(8 << 20)
with open(sys.argv[1], "wb") as file:
    while True:
        for _ in range(8):
            file.write(chunk)
        file.flush()
        os.fsync(file.fileno())
        file.seek(0)
"""


def sync_body(index: int, messages: int) -> bytes:
    state = [
        {
            "type": "m.room.member",
            "state_key": USER,
            "sender": USER,
            "event_id": "$join",
            "origin_server_ts": 1,
            "content": {"membership": "join"},
        }
    ]
    events = [
        {
            "type": "m.room.message",
            "sender": USER,
            "event_id": f"$m{index}-{number}",
            "origin_server_ts": 2 + index,
            "content": {"msgtype": "m.text", "body": f"message {index}.{number}"},
        }
        for number in range(messages)
    ]
    room = {"timeline": {"events": events}}
    if index == 0:
        room["state"] = {"events": state}
    rooms = {"join": {ROOM: room}} if index == 0 or events else {}
    return json.dumps({"next_batch": f"s{index}", "rooms": rooms}).encode()


async def run(args: argparse.Namespace, directory: Path) -> dict[str, float]:
    responses = 0

    async def handle(request: web.Request) -> web.Response:
        nonlocal responses
        if request.path.endswith("/sync"):
            body = sync_body(responses, args.messages)
            responses += 1
            return web.Response(body=body, content_type="application/json")
        if request.path.endswith("/keys/upload"):
            return web.json_response({"one_time_key_counts": {"signed_curve25519": 50}})
        if request.path.endswith("/keys/query"):
            return web.json_response({"device_keys": {}})
        raise web.HTTPNotFound

    application = web.Application()
    application.router.add_route("*", "/{path:.*}", handle)
    runner = web.AppRunner(application, access_log=None)
    await runner.setup()
    site = web.TCPSite(runner, "127.0.0.1", 0)
    await site.start()
    port = site._server.sockets[0].getsockname()[1]

    client = AsyncClient(
        f"http://127.0.0.1:{port}", USER, "BENCH", config=AsyncClientConfig()
    )
    client.restore_login(USER, "BENCH", "bench-token")
    session = open_durable_sync(
        client,
        consumer_id=CONSUMER,
        store_path=directory,
        config=DurableSyncConfig(sync_timeout_ms=0),
    )
    connection = session._store.database.connection()
    commits = 0

    def trace(statement: str) -> None:
        nonlocal commits
        commits += statement == "COMMIT"

    connection.set_trace_callback(trace)
    lags: list[float] = []
    stop = asyncio.Event()

    async def heartbeat() -> None:
        while not stop.is_set():
            started = time.perf_counter()
            await asyncio.sleep(TICK)
            lags.append(time.perf_counter() - started - TICK)

    async def consume() -> None:
        while True:
            await session.wait_for_work()
            batch = await session.next_batch()
            assert batch is not None
            await session.ack(batch)

    tasks = [
        asyncio.create_task(task) for task in (session.run(), consume(), heartbeat())
    ]
    started = time.perf_counter()
    first = responses
    try:
        await asyncio.sleep(args.seconds)
    finally:
        elapsed = time.perf_counter() - started
        stop.set()
        cycles, timed_commits = responses - first, commits
        for task in tasks[:2]:
            task.cancel()
        results = await asyncio.gather(*tasks, return_exceptions=True)
        await session.close()
        await client.close()
        await runner.cleanup()
    for result in results:
        if isinstance(result, Exception):
            raise result
    stalls = [lag for lag in lags if lag >= STALL]
    return {
        "seconds": round(elapsed, 2),
        "sync_responses": cycles,
        "responses_per_second": round(cycles / elapsed, 2),
        "commits": timed_commits,
        "lag_p50_ms": round(statistics.median(lags) * 1000, 2),
        "lag_p99_ms": round(statistics.quantiles(lags, n=100)[98] * 1000, 2),
        "lag_max_ms": round(max(lags) * 1000, 2),
        f"stalls_over_{int(STALL * 1000)}ms": len(stalls),
        "stalled_seconds": round(sum(stalls), 2),
    }


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__.split("\n\n")[0])
    parser.add_argument("--seconds", type=float, default=20)
    parser.add_argument("--messages", type=int, default=1)
    parser.add_argument("--disk-pressure", action="store_true")
    parser.add_argument("--directory", type=Path, default=None)
    args = parser.parse_args()
    with tempfile.TemporaryDirectory(dir=args.directory) as name:
        directory = Path(name)
        writer = None
        if args.disk_pressure:
            writer = subprocess.Popen(
                [sys.executable, "-c", PRESSURE, str(directory / "pressure.bin")]
            )
        try:
            result = asyncio.run(run(args, directory))
            if writer is not None and writer.poll() is not None:
                raise RuntimeError("the disk pressure writer exited early")
        finally:
            if writer is not None:
                writer.kill()
                writer.wait()
    print(
        json.dumps(
            {
                "platform": platform.platform(),
                "python": platform.python_version(),
                "sqlite": sqlite3.sqlite_version,
                "messages_per_response": args.messages,
                "disk_pressure": args.disk_pressure,
                **result,
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
