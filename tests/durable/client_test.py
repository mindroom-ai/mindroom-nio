"""The durable adapter owns transactions, not application callbacks."""

import json
from uuid import UUID

import pytest

from nio import AsyncClient, AsyncClientConfig, RoomMessageText
from nio.durable import DurableSyncConfig, open_durable_sync
from nio.durable.codec import restore_event
from nio.durable.model import RecordKind
from nio.exceptions import LocalProtocolError

USER = "@alice:example.org"
ROOM = "!room:example.org"
CONSUMER = UUID("87a00a8b-3c97-4a11-8d90-35e7c6978a37")


def client():
    result = AsyncClient(
        "https://example.org",
        USER,
        "ALICE",
        store_path=None,
        config=AsyncClientConfig(),
    )
    result.restore_login(USER, "ALICE", "test-token")
    return result


def response(*, token="s1", messages=1):
    return json.dumps(
        {
            "next_batch": token,
            "rooms": {
                "join": {
                    ROOM: {
                        "state": {
                            "events": [
                                {
                                    "type": "m.room.member",
                                    "state_key": USER,
                                    "sender": USER,
                                    "event_id": "$join",
                                    "origin_server_ts": 1,
                                    "content": {
                                        "membership": "join",
                                        "displayname": "Alice",
                                    },
                                }
                            ]
                        },
                        "timeline": {
                            "events": [
                                {
                                    "type": "m.room.message",
                                    "sender": USER,
                                    "event_id": f"$message-{index}",
                                    "origin_server_ts": 2 + index,
                                    "content": {
                                        "msgtype": "m.text",
                                        "body": f"hello {index}",
                                    },
                                }
                                for index in range(messages)
                            ]
                        },
                    }
                }
            },
        }
    ).encode()


def open_session(path, nio_client=None, config=None):
    return open_durable_sync(
        nio_client or client(), consumer_id=CONSUMER, store_path=path, config=config
    )


@pytest.mark.asyncio
async def test_prepare_commits_replayable_messages_without_callbacks(tmp_path):
    nio_client = client()
    callbacks = []
    nio_client.add_event_callback(
        lambda room, event: callbacks.append(event), RoomMessageText
    )
    session = open_session(tmp_path, nio_client)
    try:
        await session._accept_response(response(messages=3))
        assert callbacks == []
        batches = []
        while batch := await session.next_batch():
            batches.append(batch)
            await session.ack(batch)
        records = [
            record
            for batch in batches
            for record in batch.records
            if record.kind is RecordKind.TIMELINE
        ]
        assert [restore_event(record).body for record in records] == [
            "hello 0",
            "hello 1",
            "hello 2",
        ]
        assert nio_client.rooms[ROOM].users[USER].display_name == "Alice"
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_captured_input_is_prepared_after_restart(tmp_path):
    session = open_session(tmp_path)
    session._capture_response(response())
    await session.close()
    reopened = open_session(tmp_path)
    try:
        reopened._prepare_pending()
        events = []
        while batch := await reopened.next_batch():
            events.extend(
                restore_event(record)
                for record in batch.records
                if record.kind is RecordKind.TIMELINE
            )
            await reopened.ack(batch)
        assert [event.body for event in events] == ["hello 0"]
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_committed_output_and_room_state_reopen_without_processing_again(
    tmp_path, monkeypatch
):
    session = open_session(tmp_path)
    await session._accept_response(response())
    first = await session.next_batch()
    await session.close()
    nio_client = client()

    def must_not_process(*args, **kwargs):
        raise AssertionError("committed output was processed twice")

    monkeypatch.setattr(nio_client, "_iter_sync", must_not_process)
    reopened = open_session(tmp_path, nio_client)
    try:
        assert await reopened.next_batch() == first
        assert nio_client.rooms[ROOM].users[USER].display_name == "Alice"
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_preparation_failure_discards_mutated_client_and_retains_input(
    tmp_path, monkeypatch
):
    nio_client = client()
    session = open_session(tmp_path, nio_client)
    original = nio_client._iter_sync

    def fail_after_mutation(*args, **kwargs):
        for item in original(*args, **kwargs):
            if item.route == "event":
                raise RuntimeError("interrupted projection")
            yield item

    monkeypatch.setattr(nio_client, "_iter_sync", fail_after_mutation)
    with pytest.raises(RuntimeError, match="interrupted projection"):
        await session._accept_response(response())
    with pytest.raises(LocalProtocolError):
        await session.next_batch()
    reopened = open_session(tmp_path)
    try:
        assert reopened.client.rooms == {}
        reopened._prepare_pending()
        assert reopened.client.rooms[ROOM].users[USER].display_name == "Alice"
    finally:
        await reopened.close()
        await session.close()


@pytest.mark.asyncio
async def test_batch_count_bound_preserves_all_messages(tmp_path):
    session = open_session(tmp_path, config=DurableSyncConfig(max_batch_records=2))
    try:
        await session._accept_response(response(messages=7))
        identifiers = []
        while batch := await session.next_batch():
            assert len(batch.records) <= 2
            identifiers.extend(
                record.source["event_id"]
                for record in batch.records
                if record.kind is RecordKind.TIMELINE
            )
            await session.ack(batch)
        assert identifiers == [f"$message-{index}" for index in range(7)]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_ordinary_sync_cannot_mutate_an_attached_durable_client(tmp_path):
    nio_client = client()
    session = open_session(tmp_path, nio_client)
    try:
        with pytest.raises(LocalProtocolError, match="durable"):
            await nio_client.sync()
        assert session._store.cursor is None
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_batch_byte_bound_splits_without_dropping_messages(tmp_path):
    body = json.loads(response(messages=4))
    for event in body["rooms"]["join"][ROOM]["timeline"]["events"]:
        event["content"]["body"] = "a" * 600
    session = open_session(tmp_path, config=DurableSyncConfig(max_batch_bytes=1200))
    try:
        await session._accept_response(json.dumps(body).encode())
        sizes = session._store.database.execute_sql(
            "SELECT length(CAST(records AS BLOB)) FROM NioDurableBatch"
        ).fetchall()
        assert all(size <= 1200 for (size,) in sizes)
        identifiers = []
        while batch := await session.next_batch():
            identifiers.extend(
                record.source["event_id"]
                for record in batch.records
                if record.kind is RecordKind.TIMELINE
            )
            await session.ack(batch)
        assert identifiers == ["$message-0", "$message-1", "$message-2", "$message-3"]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_prepared_capacity_failure_retains_input_and_poisons_client(tmp_path):
    session = open_session(tmp_path, config=DurableSyncConfig(max_batch_bytes=100))
    with pytest.raises(LocalProtocolError, match="batch bound"):
        await session._accept_response(response())
    with pytest.raises(LocalProtocolError):
        await session.next_batch()
    reopened = open_session(tmp_path)
    try:
        assert reopened._store.input is not None
        assert reopened._store.next_batch() is None
        assert reopened._store.cursor is None
    finally:
        await reopened.close()
        await session.close()


@pytest.mark.asyncio
async def test_invited_room_member_projection_survives_restart(tmp_path):
    session = open_session(tmp_path)
    body = {
        "next_batch": "invited",
        "rooms": {
            "invite": {
                ROOM: {
                    "invite_state": {
                        "events": [
                            {
                                "type": "m.room.member",
                                "sender": "@bob:example.org",
                                "state_key": USER,
                                "content": {
                                    "membership": "invite",
                                    "displayname": "Invited Alice",
                                },
                            }
                        ]
                    },
                }
            }
        },
    }
    await session._accept_response(json.dumps(body).encode())
    assert (
        session.client.invited_rooms[ROOM].users[USER].display_name == "Invited Alice"
    )
    await session.close()
    reopened = open_session(tmp_path)
    try:
        assert (
            reopened.client.invited_rooms[ROOM].users[USER].display_name
            == "Invited Alice"
        )
    finally:
        await reopened.close()
