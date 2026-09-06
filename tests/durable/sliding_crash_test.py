"""A process kill cannot separate Sliding room keys, cursor and output."""

import asyncio
import json
import os
import signal
import subprocess
import sys
from pathlib import Path

import pytest

from nio import AsyncClient, RoomKeyEvent, TimelineEventProvenance
from nio.durable import DurableSyncConfig, SlidingSyncConfig, open_durable_sync
from nio.durable.codec import restore_event
from nio.durable.model import RecordKind

if __package__:
    from .crash_test import BOB, BODY, CONSUMER, ROOM, ordinary_client, prepare
else:
    from crash_test import BOB, BODY, CONSUMER, ROOM, ordinary_client, prepare


def opened(path):
    client = AsyncClient("https://example.org", BOB, "BOB", store_path=None)
    client.restore_login(BOB, "BOB", "test-access-token")
    session = open_durable_sync(
        client,
        consumer_id=CONSUMER,
        store_path=path / "receiver",
        config=DurableSyncConfig(sliding=SlidingSyncConfig()),
    )
    return client, session


async def seeded(path):
    await prepare(path)
    client = ordinary_client(path / "receiver", BOB, "BOB")
    # This fixture has exchanged keys but has never consumed any sync input.
    client.store.save_sync_token("")
    await client.close()
    client.store.database.close()
    classic = json.loads((path / "response.json").read_text())
    room = classic["rooms"]["join"][ROOM]
    sliding = {
        "pos": "p1",
        "rooms": {
            ROOM: {
                "initial": True,
                "membership": "join",
                "prev_batch": "w1",
                "required_state": room["state"]["events"],
                "timeline": room["timeline"]["events"],
                "num_live": 0,
            }
        },
        "extensions": {
            "to_device": {**classic["to_device"], "next_batch": "td1"},
            "e2ee": {"device_one_time_keys_count": {"signed_curve25519": 50}},
        },
    }
    (path / "sliding.json").write_text(json.dumps(sliding))


async def interrupted(path, boundary):
    _, session = opened(path)
    if boundary == "capture":
        original = session._store.capture

        def capture(body):
            original(body)
            os.kill(os.getpid(), signal.SIGKILL)

        session._store.capture = capture
    session._capture_response((path / "sliding.json").read_bytes())
    if boundary == "crypto":
        original = session.client._iter_to_device

        def devices(response):
            for item in original(response):
                yield item
                if isinstance(item.event, RoomKeyEvent):
                    os.kill(os.getpid(), signal.SIGKILL)

        session.client._iter_to_device = devices
    session._prepare_pending()
    os.kill(os.getpid(), signal.SIGKILL)


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["capture", "crypto", "committed"])
async def test_sliding_key_and_message_recover_together_after_process_kill(
    tmp_path, boundary
):
    await seeded(tmp_path)
    result = await asyncio.to_thread(
        subprocess.run,
        [sys.executable, str(Path(__file__).resolve()), str(tmp_path), boundary],
        capture_output=True,
        timeout=20,
    )
    assert result.returncode == -signal.SIGKILL, result.stderr.decode()
    client, session = opened(tmp_path)
    try:
        if boundary == "capture":
            assert session._store.input is None
            assert session._sliding.device_cursor is None
            session._capture_response((tmp_path / "sliding.json").read_bytes())
        elif boundary == "committed":

            def no_decrypt(_):
                raise AssertionError("committed output must not decrypt again")

            client._iter_to_device = no_decrypt
        else:
            assert session._sliding.device_cursor is None
        session._prepare_pending()
        records = []
        while batch := await session.next_batch():
            records.extend(batch.records)
            await session.ack(batch)
        messages = [r for r in records if r.kind is RecordKind.TIMELINE]
        assert len(messages) == 1
        assert restore_event(messages[0]).body == BODY
        assert messages[0].crypto.verified is True
        assert session._sliding.device_cursor == "td1"
        assert session.cursor == "p1"
        # A response prepared after reopening never resumes an old connection.
        if boundary != "capture":
            assert session._sliding.pos is None
    finally:
        await session.close()
        await client.close()


@pytest.mark.asyncio
async def test_late_room_key_can_promote_a_previously_undecryptable_window(tmp_path):
    from .sliding_test import settle

    await seeded(tmp_path)
    raw = json.loads((tmp_path / "sliding.json").read_text())
    keys = raw["extensions"]["to_device"]["events"]
    raw["extensions"]["to_device"]["events"] = []
    client, session = opened(tmp_path)
    try:
        timeline = raw["rooms"][ROOM]["timeline"]
        raw["rooms"][ROOM]["timeline"] = []
        await session._accept_response(json.dumps(raw).encode())
        await settle(session)
        raw["pos"] = "live"
        raw["rooms"][ROOM].update(initial=False, num_live=1, timeline=timeline)
        await session._accept_response(json.dumps(raw).encode())
        first = [r for r in await settle(session) if r.kind is RecordKind.TIMELINE]
        assert len(first) == 1 and first[0].clear is None
        assert first[0].provenance is TimelineEventProvenance.LIVE
        raw["pos"] = "p2"
        raw["rooms"][ROOM]["initial"] = False
        raw["rooms"][ROOM]["num_live"] = 1
        raw["extensions"]["to_device"]["events"] = keys
        await session._accept_response(json.dumps(raw).encode())
        promoted = [r for r in await settle(session) if r.kind is RecordKind.TIMELINE]
        assert len(promoted) == 1
        assert restore_event(promoted[0]).body == BODY
    finally:
        await session.close()
        await client.close()


@pytest.mark.asyncio
async def test_expanded_window_promotes_ciphertext_before_processed_overlap(tmp_path):
    from .recovery_test import message
    from .sliding_test import settle

    await seeded(tmp_path)
    raw = json.loads((tmp_path / "sliding.json").read_text())
    keys = raw["extensions"]["to_device"]["events"]
    raw["extensions"]["to_device"]["events"] = []
    ciphertext_id = raw["rooms"][ROOM]["timeline"][0]["event_id"]
    raw["rooms"][ROOM]["timeline"].append(message("$processed"))
    client, session = opened(tmp_path)
    timeline = raw["rooms"][ROOM]["timeline"]
    raw["rooms"][ROOM]["timeline"] = []
    await session._accept_response(json.dumps(raw).encode())
    await settle(session)
    raw["pos"] = "live"
    raw["rooms"][ROOM].update(initial=False, num_live=2, timeline=timeline)
    await session._accept_response(json.dumps(raw).encode())
    first = await settle(session)
    assert (
        next(r for r in first if r.source.get("event_id") == ciphertext_id).clear
        is None
    )
    await session.close()
    await client.close()

    client, session = opened(tmp_path)
    try:
        raw["pos"] = "p2"
        raw["rooms"][ROOM].update(initial=True, num_live=0)
        raw["rooms"][ROOM]["timeline"].insert(0, message("$ancient"))
        raw["extensions"]["to_device"]["events"] = keys
        session._capture_response(json.dumps(raw).encode())
        session._prepare_pending()
        while batch := await session.next_batch():
            await session.ack(batch)
        # Equal window boundaries need no network page; finish the shared walker.
        await session._recovery.advance()
        session._prepare_pending()
        records = await settle(session)
        promoted = next(r for r in records if r.source.get("event_id") == ciphertext_id)
        ancient = next(r for r in records if r.source.get("event_id") == "$ancient")
        assert restore_event(promoted).body == BODY
        assert promoted.provenance is TimelineEventProvenance.RECOVERED
        assert ancient.provenance is TimelineEventProvenance.HISTORY
        assert not any(r.source.get("event_id") == "$processed" for r in records)
    finally:
        await session.close()
        await client.close()


if __name__ == "__main__":
    asyncio.run(interrupted(Path(sys.argv[1]), sys.argv[2]))
