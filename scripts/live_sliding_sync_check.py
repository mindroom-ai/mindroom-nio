#!/usr/bin/env python3
"""Live MSC4186 sliding sync check against a real homeserver.

Exercises the parts of the sliding sync wire format that unit tests cannot
prove: that ``pos`` and ``timeout`` are actually honoured by the server
(they are query parameters; servers silently ignore body fields) and that
the response keys deployed servers emit are parsed.

Requires a homeserver with open registration and simplified sliding sync
enabled, e.g. a local Synapse with ``experimental_features:
msc3575_enabled: true`` or a Tuwunel/conduwuit with registration allowed.

Usage:
    uv run python scripts/live_sliding_sync_check.py \
        --homeserver http://127.0.0.1:8008
"""

import argparse
import asyncio
import secrets
import sys
import time

from nio import AsyncClient
from nio.responses import RegisterResponse, SlidingSyncResponse

LISTS = {
    "main": {
        "ranges": [[0, 19]],
        "timeline_limit": 10,
        "required_state": [
            ["m.room.create", ""],
            ["m.room.name", ""],
        ],
    }
}

PASSED = []


def check(name: str, condition: bool, detail: str = "") -> None:
    if not condition:
        print(f"FAIL: {name} {detail}")
        sys.exit(1)
    PASSED.append(name)
    print(f"ok: {name}")


async def register(homeserver: str, name: str) -> AsyncClient:
    client = AsyncClient(homeserver, f"@{name}:ignored")
    resp = await client.register(name, "live-check-password")
    if not isinstance(resp, RegisterResponse):
        print(f"FAIL: could not register {name}: {resp}")
        sys.exit(1)
    client.user_id = resp.user_id
    return client


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--homeserver", default="http://127.0.0.1:8008")
    args = parser.parse_args()

    suffix = secrets.token_hex(4)
    alice = await register(args.homeserver, f"ss-alice-{suffix}")
    bob = await register(args.homeserver, f"ss-bob-{suffix}")

    try:
        # Initial sync on a fresh connection.
        resp = await alice.sliding_sync(conn_id="live", timeout=0, lists=LISTS)
        check(
            "initial sliding sync succeeds",
            isinstance(resp, SlidingSyncResponse),
            repr(resp),
        )
        check("initial pos is set", bool(resp.pos))
        check("list is parsed", "main" in resp.lists)

        room_resp = await alice.room_create(name="sliding sync live check")
        room_id = room_resp.room_id
        await alice.room_send(
            room_id,
            "m.room.message",
            {"msgtype": "m.text", "body": "hello sliding sync"},
        )

        # Incremental sync: passing pos must return the new room promptly.
        pos = resp.pos
        start = time.monotonic()
        resp = await alice.sliding_sync(
            conn_id="live", pos=pos, timeout=30_000, lists=LISTS
        )
        elapsed = time.monotonic() - start
        check(
            "incremental sync returns promptly on new events",
            isinstance(resp, SlidingSyncResponse) and elapsed < 5,
            f"{resp!r} after {elapsed:.1f}s",
        )
        check("pos advances", resp.pos != pos, f"pos stuck at {pos!r}")
        check("new room is in the response", room_id in resp.rooms)

        room = resp.rooms[room_id]
        check("room name parsed", room.name == "sliding sync live check", room.name)
        check(
            "timeline parsed",
            any(
                getattr(ev, "body", None) == "hello sliding sync"
                for ev in room.timeline
            ),
            repr(room.timeline),
        )
        check("required_state parsed", len(room.required_state) > 0)
        check("joined_count parsed", room.joined_count == 1, repr(room.joined_count))
        check(
            "notification_count parsed",
            room.notification_count is not None,
            "notification_count missing: server key not parsed",
        )

        # With an up-to-date pos and no new events the server must long-poll
        # for the requested timeout. If pos/timeout were sent in the body
        # (the old bug) the server would ignore both and instantly return
        # the full initial payload again.
        pos = resp.pos
        start = time.monotonic()
        resp = await alice.sliding_sync(
            conn_id="live", pos=pos, timeout=2_000, lists=LISTS
        )
        elapsed = time.monotonic() - start
        check(
            "server honours timeout (long-poll)",
            elapsed >= 1.5,
            f"returned after {elapsed:.2f}s: timeout query parameter ignored",
        )
        check(
            "server honours pos (no data resent)",
            room_id not in resp.rooms,
            "initial room resent: pos query parameter ignored",
        )

        # Invited rooms arrive with stripped state (`invite_state` on the
        # wire from deployed servers).
        await alice.room_invite(room_id, bob.user_id)
        resp = await bob.sliding_sync(conn_id="live", timeout=10_000, lists=LISTS)
        check(
            "invited room appears for invitee",
            isinstance(resp, SlidingSyncResponse) and room_id in resp.rooms,
            repr(resp),
        )
        check(
            "invite stripped state parsed",
            len(resp.rooms[room_id].stripped_state) > 0,
            "invite_state response key not parsed",
        )

        print(f"\nall {len(PASSED)} live checks passed against {args.homeserver}")
    finally:
        await alice.close()
        await bob.close()


if __name__ == "__main__":
    asyncio.run(main())
