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

Besides the single-shot wire format checks this also exercises
``sliding_sync_forever``: invites and live messages arriving through the
loop, clean shutdown, the ``M_UNKNOWN_POS`` rejection the loop recovers
from, and a full encrypted round trip (room key over the to_device
extension, megolm timeline decryption) between two fresh store-backed
clients.

Pass ``--slam`` to additionally run a stress pass: two concurrent
long-poll sync loops (one per user, separate conn_ids) while a writer
floods rooms with messages, asserting no event is missed or duplicated
and that the loop long-polls instead of busy-looping.
"""

import argparse
import asyncio
import secrets
import statistics
import sys
import tempfile
import time

from nio import AsyncClient, InviteMemberEvent, RoomMessageText
from nio.responses import (
    RegisterResponse,
    SlidingSyncError,
    SlidingSyncResponse,
    SyncResponse,
)

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


async def register(homeserver: str, name: str, store: bool = False) -> AsyncClient:
    store_path = tempfile.mkdtemp(prefix=f"nio-live-{name}-") if store else ""
    client = AsyncClient(homeserver, f"@{name}:ignored", store_path=store_path)
    resp = await client.register(name, "live-check-password")
    if not isinstance(resp, RegisterResponse):
        print(f"FAIL: could not register {name}: {resp}")
        sys.exit(1)
    client.user_id = resp.user_id
    return client


async def _wait_for(predicate, timeout: float = 30.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        if predicate():
            return True
        await asyncio.sleep(0.2)
    return predicate()


async def forever_checks(alice: AsyncClient, bob: AsyncClient) -> None:
    """Exercise sliding_sync_forever: invites, live events, clean shutdown."""
    messages = {}
    invited_rooms = []

    async def message_cb(room, event):
        messages.setdefault(event.body, room.room_id)

    async def invite_cb(room, event):
        invited_rooms.append(room.room_id)

    bob.add_event_callback(message_cb, RoomMessageText)
    bob.add_event_callback(invite_cb, InviteMemberEvent)

    loop_task = asyncio.create_task(
        bob.sliding_sync_forever(
            timeout=30_000,
            conn_id="forever-bob",
            lists=LISTS,
            extensions={"to_device": {"enabled": True}},
        )
    )
    await asyncio.wait_for(bob.synced.wait(), 30)

    room_resp = await alice.room_create(name="forever room")
    room_id = room_resp.room_id
    await alice.room_invite(room_id, bob.user_id)

    check(
        "forever: invite arrives through the loop",
        await _wait_for(lambda: room_id in bob.invited_rooms),
        f"invited_rooms={list(bob.invited_rooms)}",
    )

    await bob.join(room_id)
    await alice.room_send(
        room_id, "m.room.message", {"msgtype": "m.text", "body": "forever-hello"}
    )

    check(
        "forever: live message dispatched by the loop",
        await _wait_for(lambda: messages.get("forever-hello") == room_id),
        repr(messages),
    )
    check(
        "forever: room state built by the loop",
        room_id in bob.rooms and bob.rooms[room_id].display_name == "forever room",
        repr(bob.rooms.get(room_id)),
    )

    bob.stop_sync_forever()
    await asyncio.wait_for(loop_task, 60)
    check("forever: loop stops on request", loop_task.done())

    # The recovery precondition the loop relies on: a well-formed position
    # the connection does not know must be rejected with M_UNKNOWN_POS,
    # which the loop answers by restarting the connection. Synapse tracks
    # positions per conn_id, so replaying a real pos on a fresh conn_id is
    # the deterministic way to trigger it; Tuwunel's positions are global,
    # so it may accept the request instead (also fine for the loop).
    probe = await alice.sliding_sync(conn_id="forever-pos-a", timeout=0, lists=LISTS)
    resp = await alice.sliding_sync(
        conn_id="forever-pos-b", pos=probe.pos, timeout=0, lists=LISTS
    )
    if isinstance(resp, SlidingSyncError):
        check(
            "forever: unknown pos rejected with M_UNKNOWN_POS",
            resp.status_code == "M_UNKNOWN_POS",
            repr(resp),
        )
    else:
        print("forever: server accepts a foreign pos, no connection reset needed")


async def encrypted_forever_checks(homeserver: str) -> None:
    """Two fresh clients with stores: e2ee round trip over the loop."""
    suffix = secrets.token_hex(4)
    alice = await register(homeserver, f"ss-enc-alice-{suffix}", store=True)
    bob = await register(homeserver, f"ss-enc-bob-{suffix}", store=True)

    if alice.olm is None or bob.olm is None:
        print("e2ee forever: encryption dependencies missing, skipping")
        await alice.close()
        await bob.close()
        return

    enc_lists = {
        "main": {
            "ranges": [[0, 19]],
            "timeline_limit": 10,
            "required_state": [
                ["m.room.create", ""],
                ["m.room.name", ""],
                ["m.room.encryption", ""],
                ["m.room.member", "$LAZY"],
            ],
        }
    }
    extensions = {
        "to_device": {"enabled": True},
        "e2ee": {"enabled": True},
        "account_data": {"enabled": True},
    }

    decrypted = {}

    async def message_cb(room, event):
        decrypted[event.body] = (room.room_id, event.decrypted)

    bob.add_event_callback(message_cb, RoomMessageText)

    loop_task = asyncio.create_task(
        bob.sliding_sync_forever(
            timeout=30_000,
            conn_id="enc-bob",
            lists=enc_lists,
            extensions=extensions,
        )
    )

    try:
        await asyncio.wait_for(bob.synced.wait(), 30)

        room_resp = await alice.room_create(
            name="encrypted forever room",
            initial_state=[
                {
                    "type": "m.room.encryption",
                    "state_key": "",
                    "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                }
            ],
        )
        room_id = room_resp.room_id
        await alice.room_invite(room_id, bob.user_id)

        check(
            "e2ee forever: invite arrives through the loop",
            await _wait_for(lambda: room_id in bob.invited_rooms),
        )
        await bob.join(room_id)

        # Alice drives sending by hand, so give her the state and keys a
        # sync loop would maintain: one sliding sync for the room state,
        # one upload for her identity keys.
        await alice.sliding_sync(
            conn_id="enc-alice", timeout=0, lists=enc_lists, extensions=extensions
        )
        check(
            "e2ee forever: sender sees the encrypted room",
            room_id in alice.rooms and alice.rooms[room_id].encrypted,
        )
        if alice.should_upload_keys:
            await alice.keys_upload()

        sent = await alice.room_send(
            room_id,
            "m.room.message",
            {"msgtype": "m.text", "body": "encrypted-forever"},
            ignore_unverified_devices=True,
        )
        check(
            "e2ee forever: encrypted event accepted",
            hasattr(sent, "event_id"),
            repr(sent),
        )

        check(
            "e2ee forever: loop decrypted the message",
            await _wait_for(
                lambda: decrypted.get("encrypted-forever") == (room_id, True)
            ),
            f"decrypted={decrypted!r}",
        )

        bob.stop_sync_forever()
        await asyncio.wait_for(loop_task, 60)
    finally:
        if not loop_task.done():
            loop_task.cancel()
        await alice.close()
        await bob.close()


SLAM_LISTS = {
    "slam": {
        "ranges": [[0, 30]],
        "timeline_limit": 50,
        "required_state": [["m.room.create", ""]],
    }
}


async def slam_reader(
    client: AsyncClient, name: str, sentinels: set, deadline: float
) -> dict:
    """Long-poll sliding sync until every sentinel body has been seen."""
    stats = {
        "name": name,
        "polls": 0,
        "events": 0,
        "duplicates": 0,
        "empty_fast": 0,
        "limited": 0,
        "errors": 0,
    }
    seen_event_ids = set()
    waiting = set(sentinels)
    pos = None

    while waiting and time.monotonic() < deadline:
        start = time.monotonic()
        resp = await client.sliding_sync(
            conn_id=f"slam-{name}", pos=pos, timeout=5_000, lists=SLAM_LISTS
        )
        elapsed = time.monotonic() - start
        stats["polls"] += 1

        if not isinstance(resp, SlidingSyncResponse):
            stats["errors"] += 1
            print(f"FAIL: {name} poll error: {resp}")
            break

        if not resp.rooms and elapsed < 0.2:
            stats["empty_fast"] += 1

        for room in resp.rooms.values():
            if room.limited:
                stats["limited"] += 1
            for ev in room.timeline:
                event_id = ev.source.get("event_id")
                if event_id in seen_event_ids:
                    stats["duplicates"] += 1
                seen_event_ids.add(event_id)
                body = getattr(ev, "body", None)
                if body and body.lstrip("* ").startswith("slam-"):
                    stats["events"] += 1
                    waiting.discard(body)

        pos = resp.pos

    stats["converged"] = not waiting
    return stats


async def slam(
    alice: AsyncClient,
    bob: AsyncClient,
    n_rooms: int,
    n_messages: int,
    n_edits: int,
) -> None:
    room_ids = []
    for i in range(n_rooms):
        room_resp = await alice.room_create(name=f"slam room {i}")
        room_ids.append(room_resp.room_id)
        await alice.room_invite(room_resp.room_id, bob.user_id)
        await bob.join(room_resp.room_id)

    sentinels = {f"slam-sentinel-{i}" for i in range(n_rooms)}
    deadline = time.monotonic() + 600
    readers = [
        asyncio.create_task(slam_reader(alice, "alice", sentinels, deadline)),
        asyncio.create_task(slam_reader(bob, "bob", sentinels, deadline)),
    ]
    # Let both readers establish their connections before the flood.
    await asyncio.sleep(1)

    send_failures = []
    semaphore = asyncio.Semaphore(32)

    async def send(room_id: str, content: dict) -> object:
        async with semaphore:
            resp = await alice.room_send(room_id, "m.room.message", content)
            if not hasattr(resp, "event_id"):
                send_failures.append(resp)
            return resp

    start = time.monotonic()
    message_resps = await asyncio.gather(
        *(
            send(
                room_ids[i % n_rooms],
                {"msgtype": "m.text", "body": f"slam-m-{i}"},
            )
            for i in range(n_messages)
        )
    )

    targets = [
        (room_ids[i % n_rooms], resp.event_id)
        for i, resp in enumerate(message_resps)
        if hasattr(resp, "event_id")
    ]
    check(
        "slam: messages were accepted",
        bool(targets),
        f"all {n_messages} message sends failed: {send_failures[:1]}",
    )
    await asyncio.gather(
        *(
            send(
                targets[j % len(targets)][0],
                {
                    "msgtype": "m.text",
                    "body": f"* slam-e-{j}",
                    "m.new_content": {"msgtype": "m.text", "body": f"slam-e-{j}"},
                    "m.relates_to": {
                        "rel_type": "m.replace",
                        "event_id": targets[j % len(targets)][1],
                    },
                },
            )
            for j in range(n_edits)
        )
    )
    write_seconds = time.monotonic() - start

    for i, room_id in enumerate(room_ids):
        await send(room_id, {"msgtype": "m.text", "body": f"slam-sentinel-{i}"})

    total_sent = n_messages + n_edits + n_rooms
    check(
        "slam: all events sent",
        not send_failures,
        f"{len(send_failures)} failures, first: {send_failures[:1]}",
    )
    print(
        f"slam: sent {total_sent} events over {n_rooms} rooms "
        f"in {write_seconds:.1f}s ({total_sent / write_seconds:.0f}/s)"
    )

    for stats in await asyncio.gather(*readers):
        name = stats["name"]
        coverage = 100 * stats["events"] / total_sent
        print(
            f"slam: {name}: {stats['polls']} polls, {stats['events']} events "
            f"({coverage:.0f}% coverage), {stats['limited']} limited gaps, "
            f"{stats['empty_fast']} empty-fast polls"
        )
        check(f"slam: {name} has no poll errors", stats["errors"] == 0)
        check(
            f"slam: {name} converged on all sentinels",
            stats["converged"],
            "sentinel messages never arrived",
        )
        check(
            f"slam: {name} saw no duplicate events",
            stats["duplicates"] == 0,
            f"{stats['duplicates']} duplicates: pos handling broken",
        )
        check(
            f"slam: {name} long-polls instead of busy-looping",
            stats["empty_fast"] <= 25,
            f"{stats['empty_fast']} instant empty responses",
        )


async def deep_checks(alice: AsyncClient, bob: AsyncClient) -> None:
    """Exercise response features the basic functional checks don't reach."""
    probe = await alice.sliding_sync(conn_id="deep-probe", timeout=0)
    server = probe.transport_response.headers.get("Server", "")
    is_synapse = "synapse" in server.lower()
    print(f"deep: server identifies as {server or 'unknown'!r}")

    # List windowing: a range smaller than the account's room count must
    # truncate rooms while count reports the full list size.
    room_ids = []
    for i in range(8):
        room_resp = await alice.room_create(name=f"window {i}")
        room_ids.append(room_resp.room_id)
    win_lists = {"win": {"ranges": [[0, 4]], "timeline_limit": 1, "required_state": []}}
    resp = await alice.sliding_sync(conn_id="deep-window", timeout=0, lists=win_lists)
    check(
        "deep: list count reports all rooms",
        "win" in resp.lists and resp.lists["win"].count >= 8,
        repr(resp.lists),
    )
    check(
        "deep: window truncates returned rooms",
        0 < len(resp.rooms) <= 5,
        f"{len(resp.rooms)} rooms for a 5-room window",
    )

    # Heroes: unnamed two-person room must surface the other member.
    room_resp = await alice.room_create()
    heroes_room = room_resp.room_id
    await alice.room_invite(heroes_room, bob.user_id)
    await bob.join(heroes_room)
    lists = {"deep": {"ranges": [[0, 19]], "timeline_limit": 5, "required_state": []}}
    resp = await alice.sliding_sync(conn_id="deep-heroes", timeout=0, lists=lists)
    room = resp.rooms.get(heroes_room)
    heroes_ok = room is not None and any(
        hero.user_id == bob.user_id for hero in room.heroes
    )
    if is_synapse:
        check("deep: heroes parsed", heroes_ok, repr(room and room.heroes))
    else:
        print(f"deep: heroes {'parsed' if heroes_ok else 'not sent by server'}")

    # Expanded timeline: raising timeline_limit on a live connection makes
    # Synapse resend the deeper timeline flagged unstable_expanded_timeline.
    small = {"ex": {"ranges": [[0, 19]], "timeline_limit": 1, "required_state": []}}
    big = {"ex": {"ranges": [[0, 19]], "timeline_limit": 20, "required_state": []}}
    first = await alice.sliding_sync(conn_id="deep-expand", timeout=0, lists=small)
    second = await alice.sliding_sync(
        conn_id="deep-expand", pos=first.pos, timeout=0, lists=big
    )
    expanded = any(r.expanded_timeline for r in second.rooms.values())
    if is_synapse:
        check("deep: unstable_expanded_timeline parsed", expanded)
    else:
        print(
            f"deep: expanded timeline {'parsed' if expanded else 'not sent by server'}"
        )

    # Extensions round-trip: account_data is the cheapest extension every
    # server populates for a fresh user (push rules at minimum).
    resp = await alice.sliding_sync(
        conn_id="deep-ext",
        timeout=0,
        lists=win_lists,
        extensions={"account_data": {"enabled": True}},
    )
    ext_ok = bool(resp.extensions.get("account_data"))
    if is_synapse:
        check("deep: account_data extension passes through", ext_ok)
    else:
        print(
            "deep: account_data extension "
            f"{'passes through' if ext_ok else 'not sent by server'}"
        )

    # Two connections for the same user advance independently.
    conn1 = await alice.sliding_sync(conn_id="deep-c1", timeout=0, lists=win_lists)
    conn2 = await alice.sliding_sync(conn_id="deep-c2", timeout=0, lists=win_lists)
    check("deep: second conn gets its own snapshot", len(conn2.rooms) > 0)
    await alice.room_send(
        room_ids[0], "m.room.message", {"msgtype": "m.text", "body": "deep-conn"}
    )
    for conn_id, pos in (("deep-c1", conn1.pos), ("deep-c2", conn2.pos)):
        resp = await alice.sliding_sync(
            conn_id=conn_id, pos=pos, timeout=10_000, lists=win_lists
        )
        check(
            f"deep: {conn_id} advances independently",
            room_ids[0] in resp.rooms,
            repr(resp.rooms.keys()),
        )

    # set_presence placement (informational: server support varies).
    await alice.sliding_sync(
        conn_id="deep-pres", timeout=0, set_presence="unavailable", lists=win_lists
    )
    try:
        presence = await alice.get_presence(alice.user_id)
        print(
            "deep: set_presence via sliding sync -> server reports "
            f"{getattr(presence, 'presence', presence)!r} (informational)"
        )
    except Exception as exc:  # presence support varies per server
        print(f"deep: presence endpoint unavailable: {exc!r}")


async def _timed(coro_factory, response_type, iterations: int):
    """Run coro_factory() `iterations` times, return (median s, body bytes)."""
    times = []
    size = 0
    for _ in range(iterations):
        start = time.monotonic()
        resp = await coro_factory()
        times.append(time.monotonic() - start)
        check(
            f"bench: {response_type.__name__} succeeds",
            isinstance(resp, response_type),
            repr(resp),
        )
        size = len(await resp.transport_response.read())
    return statistics.median(times), size


async def bench(client: AsyncClient, n_rooms: int, msgs_per_room: int = 5) -> None:
    """Compare classic /v3/sync against sliding sync on the same account."""
    semaphore = asyncio.Semaphore(8)

    async def make_room(i: int) -> None:
        async with semaphore:
            resp = await client.room_create(name=f"bench room {i}")
            for j in range(msgs_per_room):
                await client.room_send(
                    resp.room_id,
                    "m.room.message",
                    {"msgtype": "m.text", "body": f"bench-{i}-{j}"},
                )

    start = time.monotonic()
    await asyncio.gather(*(make_room(i) for i in range(n_rooms)))
    print(
        f"bench: created {n_rooms} rooms x {msgs_per_room} msgs "
        f"in {time.monotonic() - start:.0f}s"
    )

    iterations = 5
    counter = iter(range(1000))

    async def sliding_initial():
        return await client.sliding_sync(
            conn_id=f"bench-{next(counter)}", timeout=0, lists=SLAM_LISTS
        )

    async def v3_initial():
        client.next_batch = None
        return await client.sync(timeout=0)

    s_time, s_size = await _timed(sliding_initial, SlidingSyncResponse, iterations)
    v3_time, v3_size = await _timed(v3_initial, SyncResponse, iterations)

    v3_batch = client.next_batch
    inc = await client.sliding_sync(conn_id="bench-inc", timeout=0, lists=SLAM_LISTS)
    pos = inc.pos

    async def sliding_incremental():
        nonlocal pos
        resp = await client.sliding_sync(
            conn_id="bench-inc", pos=pos, timeout=0, lists=SLAM_LISTS
        )
        pos = resp.pos
        return resp

    async def v3_incremental():
        return await client.sync(timeout=0, since=v3_batch)

    si_time, si_size = await _timed(
        sliding_incremental, SlidingSyncResponse, iterations
    )
    v3i_time, v3i_size = await _timed(v3_incremental, SyncResponse, iterations)

    top_n = SLAM_LISTS["slam"]["ranges"][0][1] + 1
    print(
        f"bench: initial sync, {n_rooms}+ rooms "
        f"(medians of {iterations}, uncompressed bytes):\n"
        f"bench:   /v3/sync (all rooms):      {v3_time * 1000:7.0f} ms  "
        f"{v3_size:>12,} B\n"
        f"bench:   sliding sync (top {top_n}):    {s_time * 1000:7.0f} ms  "
        f"{s_size:>12,} B\n"
        f"bench:   -> {v3_time / s_time:.1f}x faster, "
        f"{v3_size / s_size:.1f}x less data\n"
        f"bench: incremental sync (idle connection):\n"
        f"bench:   /v3/sync:                  {v3i_time * 1000:7.0f} ms  "
        f"{v3i_size:>12,} B\n"
        f"bench:   sliding sync:              {si_time * 1000:7.0f} ms  "
        f"{si_size:>12,} B"
    )


async def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--homeserver", default="http://127.0.0.1:8008")
    parser.add_argument("--slam", action="store_true")
    parser.add_argument("--bench", action="store_true")
    parser.add_argument("--rooms", type=int, default=10)
    parser.add_argument("--messages", type=int, default=2000)
    parser.add_argument("--edits", type=int, default=10000)
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
        # NB: some servers (Tuwunel) omit `lists` entirely while the
        # account has no rooms, so the list echo is asserted after a room
        # exists instead of here.

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
        check(
            "list is parsed with room count",
            "main" in resp.lists and resp.lists["main"].count >= 1,
            repr(resp.lists),
        )

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

        await deep_checks(alice, bob)

        await forever_checks(alice, bob)

        await encrypted_forever_checks(args.homeserver)

        if args.slam:
            await slam(alice, bob, args.rooms, args.messages, args.edits)

        if args.bench:
            await bench(alice, args.rooms)

        print(f"\nall {len(PASSED)} live checks passed against {args.homeserver}")
    finally:
        await alice.close()
        await bob.close()


if __name__ == "__main__":
    asyncio.run(main())
