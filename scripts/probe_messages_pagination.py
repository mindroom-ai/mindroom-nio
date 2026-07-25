#!/usr/bin/env python3
"""Probe what a server returns for the forward /messages walk recovery uses.

The limited-timeline recovery walk pages ``/messages`` forwards from the
token the sync continued from (``from``) towards the token the limited
sync window starts at (``to``). It abandons the gap when a page comes
back with no ``end`` token or an ``end`` equal to the cursor it sent,
because it can no longer prove it reached the window. This prints what
the server under test actually does at that boundary.

    uv run python scripts/probe_messages_pagination.py \
        --homeserver http://127.0.0.1:8008
"""

import argparse
import asyncio
import secrets
import tempfile

from nio import AsyncClient, MessageDirection
from nio.responses import RegisterResponse, RoomMessagesResponse


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--homeserver", default="http://127.0.0.1:8008")
    parser.add_argument("--messages", type=int, default=40)
    args = parser.parse_args()

    name = f"probe-{secrets.token_hex(4)}"
    client = AsyncClient(
        args.homeserver, f"@{name}:ignored", store_path=tempfile.mkdtemp()
    )
    resp = await client.register(name, "probe-password")
    assert isinstance(resp, RegisterResponse), resp

    room_id = (await client.room_create(name="probe")).room_id
    sync_filter = {"room": {"timeline": {"limit": 1}}}

    first = await client.sync(timeout=0, sync_filter=sync_filter)
    since = first.next_batch
    print(f"since (sync continued from): {since}")

    for i in range(args.messages):
        await client.room_send(
            room_id, "m.room.message", {"msgtype": "m.text", "body": f"probe-{i}"}
        )

    second = await client.sync(since=since, timeout=0, sync_filter=sync_filter)
    join = second.rooms.join[room_id]
    print(f"limited: {join.timeline.limited}")
    print(f"prev_batch (window start / walk target): {join.timeline.prev_batch}")
    print(f"window events: {[getattr(e, 'body', None) for e in join.timeline.events]}")

    cursor = since
    target = join.timeline.prev_batch
    for page in range(6):
        resp = await client.room_messages(
            room_id,
            start=cursor,
            end=target,
            direction=MessageDirection.front,
            limit=10,
        )
        if not isinstance(resp, RoomMessagesResponse):
            print(f"page {page}: error {resp}")
            break
        bodies = [getattr(e, "body", None) for e in resp.chunk]
        print(
            f"page {page}: from={cursor!r} -> start={resp.start!r} "
            f"end={resp.end!r} n={len(resp.chunk)} "
            f"first={bodies[0] if bodies else None!r} "
            f"last={bodies[-1] if bodies else None!r}"
        )
        if resp.end is None:
            print("  -> no end token: recovery abandons the gap here")
            break
        if resp.end == cursor:
            print("  -> end == cursor (no progress): recovery abandons the gap here")
            break
        if resp.end == target:
            print("  -> end == target: walk reached the window boundary")
        cursor = resp.end
        if not resp.chunk:
            print("  -> empty page")
            break

    await client.close()


if __name__ == "__main__":
    asyncio.run(main())
