#!/usr/bin/env python3
"""Minimal repro: does a limited /v3/sync window recover on this server?

One client, one room, one writer, ``backfill_limited_timelines`` on and a
one-event timeline window. Everything the flood writes must arrive
through the recovery walk. Prints what actually arrived.

    uv run python scripts/probe_classic_recovery.py \
        --homeserver http://127.0.0.1:8008
"""

import argparse
import asyncio
import secrets
import tempfile

from nio import AsyncClient, AsyncClientConfig, RoomMessageText
from nio.responses import RegisterResponse


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--homeserver", default="http://127.0.0.1:8008")
    parser.add_argument("--messages", type=int, default=40)
    parser.add_argument(
        "--transport", choices=("classic", "sliding"), default="classic"
    )
    parser.add_argument("--concurrent", action="store_true")
    parser.add_argument(
        "--restart",
        action="store_true",
        help="tear the reader down mid-flood and rebuild it from its store",
    )
    args = parser.parse_args()

    suffix = secrets.token_hex(4)
    writer = AsyncClient(args.homeserver, f"@w-{suffix}:ignored")
    resp = await writer.register(f"w-{suffix}", "probe-password")
    assert isinstance(resp, RegisterResponse), resp

    config = AsyncClientConfig(
        store_sync_tokens=True,
        # A store only exists when encryption is enabled, and without one
        # nothing about recovery is durable.
        encryption_enabled=True,
        backfill_limited_timelines=True,
        backfill_max_pages=100,
        backfill_max_events=10_000,
        backfill_page_size=20,
        backfill_timeout=60.0,
    )
    reader = AsyncClient(
        args.homeserver,
        f"@r-{suffix}:ignored",
        store_path=tempfile.mkdtemp(),
        config=config,
    )
    resp = await reader.register(f"r-{suffix}", "probe-password")
    assert isinstance(resp, RegisterResponse), resp

    # Trace the recovery walk itself: what the client asked /messages for,
    # and what came back, is the only way to see which boundary proof the
    # walk found (or failed to find).
    def traced_fetch_for(original):
        async def traced_fetch(room_id, start, end, direction, limit):
            resp = await original(room_id, start, end, direction, limit)
            bodies = [getattr(e, "body", None) for e in getattr(resp, "chunk", [])]
            print(
                f"  walk: from={start!r} to={end!r} dir={direction} -> "
                f"n={len(bodies)} end={getattr(resp, 'end', None)!r} "
                f"first={bodies[0] if bodies else None!r} "
                f"last={bodies[-1] if bodies else None!r}"
            )
            return resp

        return traced_fetch

    reader._recovery_room_messages = traced_fetch_for(reader._recovery_room_messages)

    seen = []

    async def cb(room, event):
        seen.append(event.body)

    reader.add_event_callback(cb, RoomMessageText)

    room_id = (await writer.room_create(name="probe")).room_id
    await writer.room_invite(room_id, reader.user_id)
    await reader.join(room_id)

    if args.transport == "classic":
        task = asyncio.create_task(
            reader.sync_forever(
                timeout=5_000, sync_filter={"room": {"timeline": {"limit": 1}}}
            )
        )
    else:
        task = asyncio.create_task(
            reader.sliding_sync_forever(
                timeout=5_000,
                conn_id=f"probe-{suffix}",
                lists={
                    "probe": {
                        "ranges": [[0, 19]],
                        "timeline_limit": 1,
                        "required_state": [["m.room.create", ""]],
                    }
                },
                extensions={"to_device": {"enabled": True}},
            )
        )
    await asyncio.wait_for(reader.synced.wait(), 30)

    sent = [f"probe-{i}" for i in range(args.messages)]
    if args.concurrent:
        # Concurrent writes are what actually produce limited windows: the
        # reader cannot keep up one-event-per-sync with a burst.
        semaphore = asyncio.Semaphore(16)

        async def send(body: str) -> None:
            async with semaphore:
                await writer.room_send(
                    room_id, "m.room.message", {"msgtype": "m.text", "body": body}
                )

        await asyncio.gather(*(send(body) for body in sent))
    else:
        for body in sent:
            await writer.room_send(
                room_id, "m.room.message", {"msgtype": "m.text", "body": body}
            )

    if args.restart:
        # Rebuild the reader from its store while the writer keeps going:
        # whatever the walk needs across the boundary has to come off disk.
        credentials = (reader.user_id, reader.device_id, reader.access_token)
        store_path = reader.store_path
        reader.stop_sync_forever()
        await asyncio.wait_for(task, 120)
        await reader.close()
        print(f"  restart: reader down, {len(seen)} events dispatched so far")

        for i in range(args.messages, args.messages + 20):
            body = f"probe-{i}"
            sent.append(body)
            await writer.room_send(
                room_id, "m.room.message", {"msgtype": "m.text", "body": body}
            )
        print("  restart: wrote 20 events while the reader was down")

        user_id, device_id, access_token = credentials
        reader = AsyncClient(
            args.homeserver, user_id, device_id, store_path, config=config
        )
        reader.restore_login(user_id, device_id, access_token)
        print(f"  restart: reloaded tokens {reader._sliding_room_prev_batch}")
        reader.add_event_callback(cb, RoomMessageText)
        original_fetch = reader._recovery_room_messages
        reader._recovery_room_messages = traced_fetch_for(original_fetch)
        if args.transport == "classic":
            task = asyncio.create_task(
                reader.sync_forever(
                    timeout=5_000, sync_filter={"room": {"timeline": {"limit": 1}}}
                )
            )
        else:
            task = asyncio.create_task(
                reader.sliding_sync_forever(
                    timeout=5_000,
                    conn_id=f"probe-{suffix}-restarted",
                    lists={
                        "probe": {
                            "ranges": [[0, 19]],
                            "timeline_limit": 1,
                            "required_state": [["m.room.create", ""]],
                        }
                    },
                    extensions={"to_device": {"enabled": True}},
                )
            )

    for _ in range(60):
        await asyncio.sleep(1)
        if not set(sent) - set(seen):
            break

    missing = [body for body in sent if body not in seen]
    duplicates = [body for body in set(seen) if seen.count(body) > 1]
    print(f"sent={len(sent)} dispatched={len(seen)} missing={len(missing)}")
    print(f"missing bodies: {missing[:10]}{'...' if len(missing) > 10 else ''}")
    print(f"duplicates: {duplicates[:10]}")

    reader.stop_sync_forever()
    await asyncio.wait_for(task, 60)
    await reader.close()
    await writer.close()


if __name__ == "__main__":
    asyncio.run(main())
