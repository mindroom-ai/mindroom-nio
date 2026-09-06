"""Real process interruption must not separate a room key from its message."""

import asyncio
import json
import os
import signal
import subprocess
import sys
from pathlib import Path
from uuid import UUID

import pytest

from nio import AsyncClient, AsyncClientConfig, RoomKeyEvent
from nio.crypto import OlmDevice
from nio.durable import open_durable_sync
from nio.durable.codec import restore_event
from nio.durable.model import RecordKind
from nio.store import SqliteStore

ROOM = "!room:example.org"
ALICE, BOB = "@alice:example.org", "@bob:example.org"
BODY = "durable plaintext after restart"
CONSUMER = UUID("6b65c656-3fc1-4a03-99a3-8133d38ade70")


def ordinary_client(path, user, device):
    path.mkdir(parents=True, exist_ok=True)
    client = AsyncClient(
        "https://example.org",
        user,
        device,
        str(path),
        config=AsyncClientConfig(store=SqliteStore, store_sync_tokens=True),
    )
    client.restore_login(user, device, "test-access-token")
    return client


def durable_client(path):
    client = AsyncClient("https://example.org", BOB, "BOB", store_path=None)
    client.restore_login(BOB, "BOB", "test-access-token")
    session = open_durable_sync(
        client, consumer_id=CONSUMER, store_path=path / "receiver"
    )
    return client, session


async def prepare(path):
    alice = ordinary_client(path / "sender", ALICE, "ALICE")
    bob = ordinary_client(path / "receiver", BOB, "BOB")
    alice_device = OlmDevice(ALICE, "ALICE", alice.olm.account.identity_keys)
    bob_device = OlmDevice(BOB, "BOB", bob.olm.account.identity_keys)
    alice.olm.device_store.add(bob_device)
    bob.olm.device_store.add(alice_device)
    alice.store.save_device_keys(alice.olm.device_store)
    bob.store.save_device_keys(bob.olm.device_store)
    alice.olm.verify_device(bob_device)
    bob.olm.verify_device(alice_device)
    bob.olm.account.generate_one_time_keys(1)
    one_time = next(iter(bob.olm.account.one_time_keys["curve25519"].values()))
    bob.olm.account.mark_keys_as_published()
    bob.olm.save_account()
    alice.olm.create_session(one_time, bob_device.curve25519)
    _, to_device = alice.olm.share_group_session(ROOM, [BOB])
    alice.olm.outbound_group_sessions[ROOM].shared = True
    ciphertext = alice.olm.group_encrypt(
        ROOM, {"type": "m.room.message", "content": {"msgtype": "m.text", "body": BODY}}
    )
    response = {
        "next_batch": "s1",
        "rooms": {
            "join": {
                ROOM: {
                    "state": {
                        "events": [
                            {
                                "type": "m.room.encryption",
                                "state_key": "",
                                "event_id": "$encryption",
                                "sender": ALICE,
                                "origin_server_ts": 1,
                                "content": {"algorithm": "m.megolm.v1.aes-sha2"},
                            },
                            *[
                                {
                                    "type": "m.room.member",
                                    "state_key": user,
                                    "event_id": f"$join-{index}",
                                    "sender": user,
                                    "origin_server_ts": 1,
                                    "content": {"membership": "join"},
                                }
                                for index, user in enumerate((ALICE, BOB))
                            ],
                        ]
                    },
                    "timeline": {
                        "events": [
                            {
                                "sender": ALICE,
                                "type": "m.room.encrypted",
                                "event_id": "$encrypted-message",
                                "origin_server_ts": 2,
                                "content": ciphertext,
                            }
                        ]
                    },
                }
            }
        },
        "to_device": {
            "events": [
                {
                    "sender": ALICE,
                    "type": "m.room.encrypted",
                    "content": to_device["messages"][BOB]["BOB"],
                }
            ]
        },
        "device_one_time_keys_count": {"signed_curve25519": 50},
    }
    (path / "response.json").write_text(json.dumps(response))
    bob.store.save_sync_token("s0")
    for client in (alice, bob):
        await client.close()
        client.store.database.close()


def kill_self():
    os.kill(os.getpid(), signal.SIGKILL)


async def receive(mode, path):
    client, session = durable_client(path)
    body = (path / "response.json").read_bytes()
    if mode == "before_capture_commit":
        capture = session._store.capture

        def interrupted_capture(body):
            capture(body)
            kill_self()

        session._store.capture = interrupted_capture
    if mode == "during_prepare":
        iterate = client._iter_sync

        def interrupted_prepare(*args, **kwargs):
            for item in iterate(*args, **kwargs):
                if isinstance(item.event, RoomKeyEvent):
                    kill_self()
                yield item

        client._iter_sync = interrupted_prepare
    if mode == "after_capture":
        session._capture_response(body)
        kill_self()
    await session._accept_response(body)
    if mode == "after_prepare":
        kill_self()
    await session.close()
    await client.close()


async def restart(path):
    client, session = durable_client(path)
    cursor_before = session._store.cursor
    pending = session._store.input
    had_committed_output = session._store.next_batch() is not None
    if had_committed_output:

        def must_not_reprocess(*args, **kwargs):
            raise AssertionError("committed output was decrypted twice")

        client._iter_sync = must_not_reprocess
    if pending is None:
        # The server may safely repeat this response: s1 was never committed.
        assert cursor_before == "s0"
        await session._accept_response((path / "response.json").read_bytes())
    else:
        session._prepare_pending()
    received = []
    while batch := await session.next_batch():
        for record in batch.records:
            if record.kind is RecordKind.TIMELINE:
                event = restore_event(record)
                received.append(
                    {"type": type(event).__name__, "body": getattr(event, "body", None)}
                )
        await session.ack(batch)
    wire = json.loads((path / "response.json").read_text())
    encrypted = wire["rooms"]["join"][ROOM]["timeline"]["events"][0]["content"]
    key = client.olm.inbound_group_store.get(
        ROOM, encrypted["sender_key"], encrypted["session_id"]
    )
    (path / "restart.json").write_text(
        json.dumps(
            {
                "received": received,
                "key_present": key is not None,
                "cursor_before": cursor_before,
                "cursor_after": session._store.cursor,
                "had_committed_output": had_committed_output,
            }
        )
    )
    await session.close()
    await client.close()


def run_worker(mode, path):
    return subprocess.run(
        [
            "uv",
            "run",
            "--no-sync",
            "python",
            str(Path(__file__).resolve()),
            mode,
            str(path),
        ],
        capture_output=True,
        text=True,
        cwd=Path(__file__).resolve().parents[2],
        timeout=30,
    )


@pytest.mark.parametrize(
    "boundary",
    [
        "healthy",
        "before_capture_commit",
        "after_capture",
        "during_prepare",
        "after_prepare",
    ],
)
def test_encrypted_sync_survives_process_interruption(tmp_path, boundary):
    result = run_worker("prepare", tmp_path)
    assert result.returncode == 0, result.stderr
    result = run_worker(boundary, tmp_path)
    if boundary == "healthy":
        assert result.returncode == 0, result.stderr
    else:
        assert result.returncode in (
            -signal.SIGKILL,
            128 + signal.SIGKILL,
        ), result.stderr
    result = run_worker("restart", tmp_path)
    assert result.returncode == 0, result.stderr
    proof = json.loads((tmp_path / "restart.json").read_text())
    assert proof["received"] == [{"type": "RoomMessageText", "body": BODY}]
    assert proof["key_present"] is True
    assert proof["cursor_after"] == "s1"
    assert proof["had_committed_output"] is (boundary in ("healthy", "after_prepare"))


if __name__ == "__main__":
    mode, directory = sys.argv[1:]
    path = Path(directory)
    if mode == "prepare":
        asyncio.run(prepare(path))
    elif mode == "restart":
        asyncio.run(restart(path))
    else:
        asyncio.run(receive(mode, path))
