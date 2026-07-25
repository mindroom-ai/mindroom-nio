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
clients. Pass ``--restart-cmd`` (e.g. ``'docker restart <container>'``)
to additionally restart the homeserver under a running loop, asserting
the loop rides out the outage, recovers the connection and never
re-dispatches events across the restart.

Pass ``--slam`` to additionally run a stress pass: two concurrent
long-poll sync loops (one per user, separate conn_ids) while a writer
floods rooms with messages, asserting no event is missed or duplicated
and that the loop long-polls instead of busy-looping.

``--slam`` then runs the recovery slam, which is where
``backfill_limited_timelines`` is put under load. Several writers flood
every room in parallel with messages, edits, threaded replies,
reactions, redactions and state changes while two readers — one on
``sliding_sync_forever``, one on ``sync_forever``, both with a
one-event timeline window so every response is ``limited`` — dispatch
through /messages recovery walks. The sliding reader is torn down and
rebuilt from its store mid-flood. Afterwards every accepted write must
have been dispatched exactly once per reader, in the server's own
per-room order, with nothing left undecryptable. A final pass repeats
the flood inside an encrypted room and asserts every megolm event is
decrypted and dispatched exactly once.
"""

import argparse
import asyncio
import secrets
import statistics
import sys
import tempfile
import time
from collections import Counter, defaultdict

from nio import (
    AsyncClient,
    AsyncClientConfig,
    Event,
    InviteMemberEvent,
    MegolmEvent,
    MessageDirection,
    RoomMessageText,
)
from nio.responses import (
    RegisterResponse,
    RoomMessagesResponse,
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


async def _wait_server_up(client: AsyncClient, timeout: float = 60.0) -> bool:
    deadline = time.monotonic() + timeout
    while time.monotonic() < deadline:
        try:
            resp = await client.send("GET", "/_matrix/client/versions", timeout=2)
            if resp.status == 200:
                return True
        except Exception:
            pass
        await asyncio.sleep(0.5)
    return False


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


async def restart_checks(
    alice: AsyncClient, bob: AsyncClient, restart_cmd: str
) -> None:
    """Restart the homeserver under a running loop.

    The loop must ride out the outage (transport retries), recover the
    connection (M_UNKNOWN_POS on servers with in-memory sliding sync
    state), keep delivering afterwards, and never re-dispatch events the
    callbacks already saw even though the fresh connection re-sends each
    room's recent timeline window.
    """
    counts = {}
    errcodes = []

    async def message_cb(room, event):
        counts[event.body] = counts.get(event.body, 0) + 1

    async def error_cb(response):
        errcodes.append(response.status_code)

    bob.add_event_callback(message_cb, RoomMessageText)
    bob.add_response_callback(error_cb, SlidingSyncError)

    loop_task = asyncio.create_task(
        bob.sliding_sync_forever(
            timeout=30_000,
            conn_id="restart-bob",
            lists=LISTS,
            extensions={"to_device": {"enabled": True}},
        )
    )
    await asyncio.wait_for(bob.synced.wait(), 30)

    room_id = (await alice.room_create(name="restart room")).room_id
    await alice.room_invite(room_id, bob.user_id)
    check(
        "restart: invite arrives before the restart",
        await _wait_for(lambda: room_id in bob.invited_rooms),
    )
    await bob.join(room_id)
    await alice.room_send(
        room_id, "m.room.message", {"msgtype": "m.text", "body": "restart-before"}
    )
    check(
        "restart: message delivered before the restart",
        await _wait_for(lambda: counts.get("restart-before") == 1),
        repr(counts),
    )

    print(f"restart: running {restart_cmd!r}")
    proc = await asyncio.create_subprocess_shell(
        restart_cmd,
        stdout=asyncio.subprocess.DEVNULL,
        stderr=asyncio.subprocess.PIPE,
    )
    _, stderr = await proc.communicate()
    check(
        "restart: restart command succeeded",
        proc.returncode == 0,
        stderr.decode(errors="replace"),
    )
    check("restart: server back up", await _wait_server_up(alice))

    await alice.room_send(
        room_id, "m.room.message", {"msgtype": "m.text", "body": "restart-after"}
    )
    check(
        "restart: loop recovers and delivers after the restart",
        await _wait_for(lambda: counts.get("restart-after") == 1, timeout=90),
        repr(counts),
    )
    check(
        "restart: no message dispatched twice across the restart",
        all(count == 1 for count in counts.values()),
        repr({body: count for body, count in counts.items() if count != 1}),
    )
    if "M_UNKNOWN_POS" in errcodes:
        check("restart: loop recovered from M_UNKNOWN_POS", True)
    else:
        print(
            "restart: no M_UNKNOWN_POS observed; server kept the connection "
            f"(errcodes seen: {errcodes!r})"
        )

    bob.stop_sync_forever()
    await asyncio.wait_for(loop_task, 60)


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


# A one-event timeline window guarantees that any concurrent write burst
# leaves the sync response `limited`, which is the only way to make the
# client walk /messages for the events the window dropped.
RECOVERY_LISTS = {
    "recovery": {
        "ranges": [[0, 49]],
        "timeline_limit": 1,
        "required_state": [
            ["m.room.create", ""],
            ["m.room.member", "$LAZY"],
        ],
    }
}

RECOVERY_SYNC_FILTER = {
    "room": {"timeline": {"limit": 1}},
}


class Recorder:
    """Records every event a client dispatches, in dispatch order."""

    def __init__(self, name: str):
        self.name = name
        self.order: dict[str, list[str]] = defaultdict(list)
        self.counts: Counter = Counter()
        self.decrypted: Counter = Counter()
        self.undecryptable: set[str] = set()

    async def __call__(self, room, event) -> None:
        event_id = getattr(event, "event_id", None)
        if not event_id:
            return
        self.order[room.room_id].append(event_id)
        self.counts[event_id] += 1
        if isinstance(event, MegolmEvent):
            self.undecryptable.add(event_id)
        else:
            self.decrypted[event_id] += 1
            self.undecryptable.discard(event_id)

    def seen(self) -> set[str]:
        return set(self.counts)

    def duplicates(self, ids: set[str]) -> dict[str, int]:
        return {
            event_id: count
            for event_id, count in self.counts.items()
            if count > 1 and event_id in ids
        }


def recovery_config(max_events: int, encryption: bool = False) -> AsyncClientConfig:
    return AsyncClientConfig(
        store_sync_tokens=True,
        encryption_enabled=encryption,
        backfill_limited_timelines=True,
        backfill_max_pages=100,
        backfill_max_events=max_events,
        backfill_page_size=100,
        backfill_timeout=60.0,
    )


async def register_configured(
    homeserver: str, name: str, config: AsyncClientConfig
) -> AsyncClient:
    """Register a store-backed client with a non-default config."""
    store_path = tempfile.mkdtemp(prefix=f"nio-live-{name}-")
    client = AsyncClient(
        homeserver, f"@{name}:ignored", store_path=store_path, config=config
    )
    resp = await client.register(name, "live-check-password")
    if not isinstance(resp, RegisterResponse):
        print(f"FAIL: could not register {name}: {resp}")
        sys.exit(1)
    return client


async def canonical_order(client: AsyncClient, room_id: str) -> list[str]:
    """Every event id in the room, oldest first, as the server orders them."""
    ids: list[str] = []
    start = ""
    seen_tokens = set()
    while True:
        resp = await client.room_messages(
            room_id, start=start, direction=MessageDirection.back, limit=500
        )
        if not isinstance(resp, RoomMessagesResponse) or not resp.chunk:
            break
        ids.extend(
            event.event_id for event in resp.chunk if getattr(event, "event_id", None)
        )
        if not resp.end or resp.end in seen_tokens:
            break
        seen_tokens.add(resp.end)
        start = resp.end
    ids.reverse()
    return ids


async def flood(
    writers: list[AsyncClient],
    room_ids: list[str],
    n_messages: int,
    n_edits: int,
    n_reactions: int,
    n_redactions: int,
    n_state: int,
    label: str = "",
) -> tuple[dict[str, set[str]], list[object]]:
    """Hammer every room from every writer at once.

    Returns the event ids accepted by the server per room, plus the
    failures, so the caller can assert on exactly what was written.
    """
    sent: dict[str, set[str]] = defaultdict(set)
    failures: list[object] = []
    # Per-writer concurrency: enough parallelism to interleave writes
    # inside a single room, bounded so the connection pool is not the
    # thing under test.
    semaphores = {id(writer): asyncio.Semaphore(16) for writer in writers}

    async def do(writer: AsyncClient, room_id: str, factory) -> str | None:
        async with semaphores[id(writer)]:
            resp = await factory(writer, room_id)
        event_id = getattr(resp, "event_id", None)
        if not event_id:
            failures.append(resp)
            return None
        sent[room_id].add(event_id)
        return event_id

    def message(index: int):
        async def factory(writer: AsyncClient, room_id: str):
            return await writer.room_send(
                room_id,
                "m.room.message",
                {"msgtype": "m.text", "body": f"rslam-m-{index}"},
            )

        return factory

    message_ids = await asyncio.gather(
        *(
            do(writers[i % len(writers)], room_ids[i % len(room_ids)], message(i))
            for i in range(n_messages)
        )
    )
    targets = [
        (room_ids[i % len(room_ids)], event_id)
        for i, event_id in enumerate(message_ids)
        if event_id
    ]
    check(
        f"recovery slam: message flood accepted ({label})",
        len(targets) == n_messages,
        f"{n_messages - len(targets)} sends failed, first: {failures[:1]}",
    )

    def edit(index: int, target: tuple[str, str]):
        async def factory(writer: AsyncClient, room_id: str):
            return await writer.room_send(
                room_id,
                "m.room.message",
                {
                    "msgtype": "m.text",
                    "body": f"* rslam-e-{index}",
                    "m.new_content": {"msgtype": "m.text", "body": f"rslam-e-{index}"},
                    "m.relates_to": {
                        "rel_type": "m.replace",
                        "event_id": target[1],
                    },
                },
            )

        return factory

    def thread(index: int, target: tuple[str, str]):
        async def factory(writer: AsyncClient, room_id: str):
            return await writer.room_send(
                room_id,
                "m.room.message",
                {
                    "msgtype": "m.text",
                    "body": f"rslam-t-{index}",
                    "m.relates_to": {
                        "rel_type": "m.thread",
                        "event_id": target[1],
                        "is_falling_back": True,
                    },
                },
            )

        return factory

    def reaction(index: int, target: tuple[str, str]):
        async def factory(writer: AsyncClient, room_id: str):
            return await writer.room_send(
                room_id,
                "m.reaction",
                {
                    "m.relates_to": {
                        "rel_type": "m.annotation",
                        "event_id": target[1],
                        "key": f"\N{THUMBS UP SIGN}{index % 5}",
                    }
                },
            )

        return factory

    def redaction(target: tuple[str, str]):
        async def factory(writer: AsyncClient, room_id: str):
            return await writer.room_redact(room_id, target[1], "rslam")

        return factory

    def topic(index: int):
        async def factory(writer: AsyncClient, room_id: str):
            return await writer.room_put_state(
                room_id, "m.room.topic", {"topic": f"rslam-topic-{index}"}
            )

        return factory

    # Edits, threaded replies, reactions, redactions and state changes all
    # race each other across every room and every writer at once.
    jobs = []
    for j in range(n_edits):
        target = targets[j % len(targets)]
        writer = writers[j % len(writers)]
        kind = edit(j, target) if j % 2 else thread(j, target)
        jobs.append(do(writer, target[0], kind))
    for j in range(n_reactions):
        target = targets[(j * 7) % len(targets)]
        # A distinct key per reaction: a repeated (sender, event, key)
        # annotation is rejected, and a rejected send is not the thing
        # under test.
        writer = writers[j % len(writers)]
        jobs.append(do(writer, target[0], reaction(j, target)))
    # Redactions of another user's event and topic changes both need
    # power level 50; only the room creator has it, so those come from
    # the creator while everything else keeps racing.
    for j in range(n_redactions):
        target = targets[-(j + 1)]
        jobs.append(do(writers[0], target[0], redaction(target)))
    for j in range(n_state):
        room_id = room_ids[j % len(room_ids)]
        jobs.append(do(writers[0], room_id, topic(j)))

    await asyncio.gather(*jobs)
    return sent, failures


async def recovery_slam(
    homeserver: str,
    n_rooms: int,
    n_messages: int,
    n_edits: int,
    n_writers: int,
    allow_sliding_gap: bool = False,
) -> None:
    """Flood rooms with a one-event sync window and assert exactly-once.

    This is the check that unit tests cannot make: with
    ``backfill_limited_timelines`` enabled, every event dropped by the
    tiny timeline window has to come back through a /messages recovery
    walk, in the server's order, exactly once, on both transports, and
    across a mid-flood client restart.
    """
    suffix = secrets.token_hex(4)
    budget = (n_messages + n_edits) * 4 + 1000

    writers = [
        await register(homeserver, f"rs-w{i}-{suffix}") for i in range(n_writers)
    ]
    sliding = await register_configured(
        homeserver, f"rs-sliding-{suffix}", recovery_config(budget)
    )
    classic = await register_configured(
        homeserver, f"rs-classic-{suffix}", recovery_config(budget)
    )
    readers = [sliding, classic]

    sliding_recorder = Recorder("sliding")
    classic_recorder = Recorder("classic")
    sliding.add_event_callback(sliding_recorder, Event)
    classic.add_event_callback(classic_recorder, Event)

    room_ids: list[str] = []
    try:
        for i in range(n_rooms):
            resp = await writers[0].room_create(name=f"recovery slam {i}")
            room_ids.append(resp.room_id)
            for member in writers[1:] + readers:
                await writers[0].room_invite(resp.room_id, member.user_id)
                await member.join(resp.room_id)

        sliding_task = asyncio.create_task(
            sliding.sliding_sync_forever(
                timeout=10_000,
                conn_id=f"rslam-{suffix}",
                lists=RECOVERY_LISTS,
                extensions={"to_device": {"enabled": True}},
            )
        )
        classic_task = asyncio.create_task(
            classic.sync_forever(timeout=10_000, sync_filter=RECOVERY_SYNC_FILTER)
        )
        await asyncio.wait_for(sliding.synced.wait(), 60)
        await asyncio.wait_for(classic.synced.wait(), 60)

        # First half of the flood.
        start = time.monotonic()
        first, failures = await flood(
            writers,
            room_ids,
            n_messages // 2,
            n_edits // 2,
            n_edits // 4,
            n_rooms,
            n_rooms,
            label="before restart",
        )

        # Restart the sliding reader mid-flood, from its store, while the
        # writers keep going: durable recovery state has to survive the
        # process boundary without losing or replaying events.
        restart_credentials = (
            sliding.user_id,
            sliding.device_id,
            sliding.access_token,
            sliding.store_path,
        )
        sliding.stop_sync_forever()
        await asyncio.wait_for(sliding_task, 120)
        await sliding.close()

        user_id, device_id, access_token, store_path = restart_credentials
        sliding = AsyncClient(
            homeserver, user_id, device_id, store_path, recovery_config(budget)
        )
        sliding.restore_login(user_id, device_id, access_token)
        sliding.add_event_callback(sliding_recorder, Event)
        readers[0] = sliding
        sliding_task = asyncio.create_task(
            sliding.sliding_sync_forever(
                timeout=10_000,
                conn_id=f"rslam-restarted-{suffix}",
                lists=RECOVERY_LISTS,
                extensions={"to_device": {"enabled": True}},
            )
        )

        # Second half, against the restarted reader.
        second, more_failures = await flood(
            writers,
            room_ids,
            n_messages - n_messages // 2,
            n_edits - n_edits // 2,
            n_edits // 4,
            n_rooms,
            n_rooms,
            label="after restart",
        )
        write_seconds = time.monotonic() - start
        failures.extend(more_failures)

        sent: dict[str, set[str]] = defaultdict(set)
        for part in (first, second):
            for room_id, ids in part.items():
                sent[room_id] |= ids
        sent_ids = {event_id for ids in sent.values() for event_id in ids}
        check(
            "recovery slam: every write was accepted",
            not failures,
            f"{len(failures)} failures, first: {failures[:1]}",
        )
        print(
            f"recovery slam: wrote {len(sent_ids)} events over {n_rooms} rooms "
            f"from {n_writers} writers in {write_seconds:.1f}s "
            f"({len(sent_ids) / write_seconds:.0f}/s)"
        )

        # Sentinel per room: once it lands, that room's recovery walk has
        # caught up to the end of the flood.
        for i, room_id in enumerate(room_ids):
            resp = await writers[0].room_send(
                room_id,
                "m.room.message",
                {"msgtype": "m.text", "body": f"rslam-sentinel-{i}"},
            )
            sent[room_id].add(resp.event_id)
            sent_ids.add(resp.event_id)

        # Waiting on a reader that is known not to recover only burns the
        # deadline, so a waived sliding gap drops out of the convergence
        # condition (its counts are still reported and asserted below).
        awaited = (
            (classic_recorder,)
            if allow_sliding_gap
            else (sliding_recorder, classic_recorder)
        )
        deadline = time.monotonic() + 900
        last_report = 0.0
        while time.monotonic() < deadline:
            missing = {
                recorder.name: len(sent_ids - recorder.seen())
                for recorder in (sliding_recorder, classic_recorder)
            }
            if not any(missing[recorder.name] for recorder in awaited):
                break
            now = time.monotonic()
            if now - last_report > 15:
                last_report = now
                print(f"recovery slam: waiting, missing {missing}")
            await asyncio.sleep(1)

        # Classic first: it is the transport that plans recovery gaps, so
        # its verdict is the one that proves the walk works.
        for recorder in (classic_recorder, sliding_recorder):
            missing_ids = sent_ids - recorder.seen()
            if missing_ids and recorder is sliding_recorder and allow_sliding_gap:
                # `_handle_sliding_sync` never opens a recovery gap, so a
                # limited sliding window drops events with no walk and no
                # warning. Known hole, reported rather than asserted.
                print(
                    f"recovery slam: KNOWN GAP: sliding lost "
                    f"{len(missing_ids)}/{len(sent_ids)} events "
                    f"(sliding sync plans no recovery gap)"
                )
            else:
                check(
                    f"recovery slam: {recorder.name} lost no events",
                    not missing_ids,
                    f"{len(missing_ids)}/{len(sent_ids)} never dispatched, "
                    f"e.g. {list(missing_ids)[:3]}",
                )
            duplicates = recorder.duplicates(sent_ids)
            check(
                f"recovery slam: {recorder.name} dispatched each event once",
                not duplicates,
                f"{len(duplicates)} duplicated, e.g. {list(duplicates.items())[:3]}",
            )

        # Order: per room, the dispatch order must be the server's order.
        # Events a reader never saw cannot break its ordering, so each
        # reader is compared against the subsequence it did receive.
        for room_id in room_ids:
            canonical = [
                event_id
                for event_id in await canonical_order(writers[0], room_id)
                if event_id in sent[room_id]
            ]
            for recorder in (classic_recorder, sliding_recorder):
                expected = [
                    event_id for event_id in canonical if event_id in recorder.seen()
                ]
                got = [
                    event_id
                    for event_id in recorder.order[room_id]
                    if event_id in sent[room_id]
                ]
                if got != expected:
                    first_bad = next(
                        (i for i, (a, b) in enumerate(zip(got, expected)) if a != b),
                        min(len(got), len(expected)),
                    )
                    check(
                        f"recovery slam: {recorder.name} preserved room order",
                        False,
                        f"{room_id} diverges at index {first_bad}: "
                        f"got {got[first_bad : first_bad + 3]} "
                        f"expected {expected[first_bad : first_bad + 3]}",
                    )
        check(
            "recovery slam: both readers preserved per-room order",
            True,
        )

        for recorder in (sliding_recorder, classic_recorder):
            check(
                f"recovery slam: {recorder.name} left nothing undecryptable",
                not recorder.undecryptable,
                f"{len(recorder.undecryptable)} events stuck encrypted",
            )

        sliding.stop_sync_forever()
        classic.stop_sync_forever()
        await asyncio.wait_for(asyncio.gather(sliding_task, classic_task), 120)
    finally:
        for task in ("sliding_task", "classic_task"):
            handle = locals().get(task)
            if handle is not None and not handle.done():
                handle.cancel()
        for client in writers + readers:
            await client.close()


async def encrypted_recovery_slam(homeserver: str, n_messages: int) -> None:
    """Flood an encrypted room with a one-event window and assert decryption."""
    suffix = secrets.token_hex(4)
    alice = await register_configured(
        homeserver, f"rs-enc-a-{suffix}", recovery_config(n_messages * 4, True)
    )
    bob = await register_configured(
        homeserver, f"rs-enc-b-{suffix}", recovery_config(n_messages * 4, True)
    )

    if alice.olm is None or bob.olm is None:
        print("encrypted recovery slam: encryption dependencies missing, skipping")
        await alice.close()
        await bob.close()
        return

    lists = {
        "recovery": {
            "ranges": [[0, 19]],
            "timeline_limit": 1,
            "required_state": [
                ["m.room.create", ""],
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

    recorder = Recorder("encrypted")
    bob.add_event_callback(recorder, Event)
    loop_task = None

    try:
        loop_task = asyncio.create_task(
            bob.sliding_sync_forever(
                timeout=10_000,
                conn_id=f"rslam-enc-{suffix}",
                lists=lists,
                extensions=extensions,
            )
        )
        await asyncio.wait_for(bob.synced.wait(), 60)

        resp = await alice.room_create(
            name="encrypted recovery slam",
            initial_state=[
                {
                    "type": "m.room.encryption",
                    "state_key": "",
                    "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                }
            ],
        )
        room_id = resp.room_id
        await alice.room_invite(room_id, bob.user_id)
        check(
            "encrypted recovery slam: invite arrives",
            await _wait_for(lambda: room_id in bob.invited_rooms, 60),
        )
        await bob.join(room_id)
        await alice.sliding_sync(
            conn_id=f"rslam-enc-a-{suffix}",
            timeout=0,
            lists=lists,
            extensions=extensions,
        )
        if alice.should_upload_keys:
            await alice.keys_upload()

        sent_ids = set()
        semaphore = asyncio.Semaphore(8)

        async def send(index: int) -> None:
            async with semaphore:
                resp = await alice.room_send(
                    room_id,
                    "m.room.message",
                    {"msgtype": "m.text", "body": f"rslam-enc-{index}"},
                    ignore_unverified_devices=True,
                )
            event_id = getattr(resp, "event_id", None)
            if event_id:
                sent_ids.add(event_id)

        start = time.monotonic()
        await asyncio.gather(*(send(i) for i in range(n_messages)))
        elapsed = time.monotonic() - start
        check(
            "encrypted recovery slam: every encrypted send was accepted",
            len(sent_ids) == n_messages,
            f"{n_messages - len(sent_ids)} sends failed",
        )
        print(
            f"encrypted recovery slam: sent {n_messages} encrypted events "
            f"in {elapsed:.1f}s ({n_messages / elapsed:.0f}/s)"
        )

        converged = await _wait_for(
            lambda: (
                not (
                    sent_ids
                    - {
                        event_id
                        for event_id in recorder.decrypted
                        if recorder.decrypted[event_id]
                    }
                )
            ),
            600,
        )
        missing = sent_ids - set(recorder.decrypted)
        check(
            "encrypted recovery slam: every event was decrypted and dispatched",
            converged and not missing,
            f"{len(missing)} never arrived decrypted",
        )
        check(
            "encrypted recovery slam: no event decrypted twice",
            not [event_id for event_id in sent_ids if recorder.decrypted[event_id] > 1],
            "an event was dispatched decrypted more than once",
        )
        check(
            "encrypted recovery slam: nothing left stuck encrypted",
            not recorder.undecryptable,
            f"{len(recorder.undecryptable)} events never decrypted",
        )

        bob.stop_sync_forever()
        await asyncio.wait_for(loop_task, 120)
    finally:
        if loop_task is not None and not loop_task.done():
            loop_task.cancel()
        await alice.close()
        await bob.close()


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
        hero.user_id == bob.user_id for hero in room.heroes or []
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
    parser.add_argument(
        "--restart-cmd",
        help="shell command that restarts the homeserver (e.g. 'docker "
        "restart nio-live-synapse'); enables the mid-loop restart "
        "recovery check",
    )
    parser.add_argument("--slam", action="store_true")
    parser.add_argument("--bench", action="store_true")
    parser.add_argument("--rooms", type=int, default=10)
    parser.add_argument("--messages", type=int, default=2000)
    parser.add_argument("--edits", type=int, default=10000)
    parser.add_argument("--recovery-rooms", type=int, default=8)
    parser.add_argument("--recovery-messages", type=int, default=600)
    parser.add_argument("--recovery-edits", type=int, default=600)
    parser.add_argument("--recovery-writers", type=int, default=4)
    parser.add_argument("--recovery-encrypted-messages", type=int, default=250)
    parser.add_argument(
        "--skip-recovery-slam",
        action="store_true",
        help="run only the wire-format slam, not the backfill recovery slam",
    )
    parser.add_argument(
        "--allow-sliding-backfill-gap",
        action="store_true",
        help="report, instead of failing on, events the sliding transport "
        "drops because it never opens a recovery gap",
    )
    parser.add_argument(
        "--only-recovery-slam",
        action="store_true",
        help="skip every other check and run the backfill recovery slam alone",
    )
    args = parser.parse_args()

    if args.only_recovery_slam:
        await recovery_slam(
            args.homeserver,
            args.recovery_rooms,
            args.recovery_messages,
            args.recovery_edits,
            args.recovery_writers,
            args.allow_sliding_backfill_gap,
        )
        await encrypted_recovery_slam(args.homeserver, args.recovery_encrypted_messages)
        print(f"\nall {len(PASSED)} live checks passed against {args.homeserver}")
        return

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

        if args.restart_cmd:
            await restart_checks(alice, bob, args.restart_cmd)

        await encrypted_forever_checks(args.homeserver)

        if args.slam:
            await slam(alice, bob, args.rooms, args.messages, args.edits)

            if not args.skip_recovery_slam:
                await recovery_slam(
                    args.homeserver,
                    args.recovery_rooms,
                    args.recovery_messages,
                    args.recovery_edits,
                    args.recovery_writers,
                    args.allow_sliding_backfill_gap,
                )
                await encrypted_recovery_slam(
                    args.homeserver, args.recovery_encrypted_messages
                )

        if args.bench:
            await bench(alice, args.rooms)

        print(f"\nall {len(PASSED)} live checks passed against {args.homeserver}")
    finally:
        await alice.close()
        await bob.close()


if __name__ == "__main__":
    asyncio.run(main())
