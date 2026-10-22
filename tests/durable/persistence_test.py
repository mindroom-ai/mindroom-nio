"""Reduce repeated persistence work without changing committed observations."""

import json
from contextlib import nullcontext

import pytest

from nio.exceptions import LocalProtocolError

from .client_test import ROOM, USER, open_session, response
from .recovery_test import baseline
from .store_test import message, open_store


def observe_sql(monkeypatch, store):
    statements = []
    execute = store.database.execute_sql

    def observed(sql, *args, **kwargs):
        statements.append(sql)
        return execute(sql, *args, **kwargs)

    monkeypatch.setattr(store.database, "execute_sql", observed)
    return statements


@pytest.mark.asyncio
async def test_decoded_input_is_not_loaded_again_for_prepare_or_progress(
    tmp_path, monkeypatch
):
    session = open_session(tmp_path)
    statements = observe_sql(monkeypatch, session._store)
    try:
        await session._accept_response(response(messages=0))
        while batch := await session.next_batch():
            await session.ack(batch)
        await session._recovery.advance()
        assert session.cursor == "s1"
        assert session.client.rooms[ROOM].users
        assert not any("SELECT body,continuation" in sql for sql in statements)
    finally:
        await session.close()


def test_input_status_preserves_validation_and_rollback_without_loading_body(
    tmp_path, monkeypatch
):
    store = open_store(tmp_path)
    try:
        statements = observe_sql(monkeypatch, store)
        assert not store.has_input()
        assert store.continuation is None
        with store.transaction():
            store.capture(b"x" * 1024 * 1024)
            store.save_continuation({"phase": "prepared"})
        assert store.has_input()
        assert store.continuation == {"phase": "prepared"}
        with pytest.raises(RuntimeError, match="rollback"):
            with store.transaction():
                store.finish_input()
                assert not store.has_input()
                raise RuntimeError("rollback")
        assert store.continuation == {"phase": "prepared"}
        assert not any("SELECT body,continuation" in sql for sql in statements)
        store.database.execute_sql(
            "UPDATE NioDurableInput SET continuation='[]' WHERE id=1"
        )
        # The store mirrors only its own writes, so the damaged row is read on reopen.
        store.close()
        store = open_store(tmp_path)
        with pytest.raises(LocalProtocolError, match="continuation"):
            _ = store.continuation
    finally:
        store.close()
    with pytest.raises(LocalProtocolError, match="closed"):
        store.has_input()
    with pytest.raises(LocalProtocolError, match="closed"):
        _ = store.continuation


def loop_view(store):
    return (
        store.has_batches(),
        store.has_input(),
        store.cursor,
        store.continuation,
        store.acked_sequence,
        store.progress_generation,
        store.read_local_intent(),
    )


@pytest.mark.parametrize("nested", [False, True])
def test_sync_loop_polls_read_no_sql_and_follow_commits_and_rollbacks(
    tmp_path, monkeypatch, nested
):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            store.capture(b'{"next_batch":"s1"}')
            store.set_cursor("s1")
            first = store.publish((message(),))
        statements = observe_sql(monkeypatch, store)
        for _ in range(3):
            assert loop_view(store) == (True, True, "s1", {}, 0, 1, None)
        # Only the membership intent is read again, once after each transaction.
        assert len(statements) == 1
        assert "'membership'" in statements[0]

        with pytest.raises(RuntimeError, match="rollback"):
            with store.transaction() if nested else nullcontext():
                with store.transaction():
                    store.publish(())
                    store.set_cursor("lost")
                    store.save_continuation({"phase": "lost"})
                    store.finish_input()
                    assert loop_view(store)[:4] == (True, False, "lost", None)
                    raise RuntimeError("rollback")
        assert loop_view(store) == (True, True, "s1", {}, 0, 1, None)

        store.ack(first)
        with store.transaction():
            store.save_continuation({"phase": "prepared"})
            second = store.publish(())
        statements.clear()
        assert loop_view(store) == (True, True, "s1", {"phase": "prepared"}, 1, 3, None)
        assert store.next_batch() == second
        store.ack(second)
        assert store.next_batch() is None
        assert not any(
            sql.startswith("SELECT") and "NioDurableMeta" in sql for sql in statements
        )
        expected = loop_view(store)
    finally:
        store.close()
    reopened = open_store(tmp_path)
    try:
        assert loop_view(reopened) == expected
    finally:
        reopened.close()


def test_caught_inner_failure_keeps_the_outer_transactions_writes(tmp_path):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            store.capture(b"{}")
            store.set_cursor("outer")
            store.publish((message(),))
            with pytest.raises(RuntimeError, match="inner"):
                with store.transaction():
                    store.publish(())
                    store.set_cursor("inner")
                    store.finish_input()
                    raise RuntimeError("inner")
            assert loop_view(store) == (True, True, "outer", {}, 0, 1, None)
        expected = loop_view(store)
        assert expected == (True, True, "outer", {}, 0, 1, None)
    finally:
        store.close()
    reopened = open_store(tmp_path)
    try:
        assert loop_view(reopened) == expected
    finally:
        reopened.close()


def test_durable_writes_need_the_stores_own_transaction(tmp_path):
    store = open_store(tmp_path)
    try:
        with store.database.atomic():
            with pytest.raises(LocalProtocolError, match="requires a transaction"):
                store.publish(())
            with pytest.raises(LocalProtocolError, match="another transaction"):
                with store.transaction():
                    pass
        assert not store.has_batches()
        assert store.next_batch() is None
    finally:
        store.close()


def test_failed_ack_commit_reloads_pending_batches(tmp_path, monkeypatch):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            batch = store.publish((message(),))
        commit = store.database.commit

        def fail():
            raise RuntimeError("disk full")

        monkeypatch.setattr(store.database, "commit", fail)
        with pytest.raises(RuntimeError, match="disk full"):
            store.ack(batch)
        monkeypatch.setattr(store.database, "commit", commit)
        assert store.has_batches()
        assert store.acked_sequence == 0
        assert store.next_batch() == batch
    finally:
        store.close()


@pytest.mark.asyncio
async def test_room_inventory_reads_membership_intent_once_per_transaction(
    tmp_path, monkeypatch
):
    session = open_session(tmp_path)
    body = json.loads(response(messages=0))
    room_info = body["rooms"]["join"].pop(ROOM)
    room_ids = [f"!room-{index}:example.org" for index in range(8)]
    body["rooms"]["join"] = dict.fromkeys(room_ids, room_info)
    statements = observe_sql(monkeypatch, session._store)
    try:
        await session._accept_response(json.dumps(body).encode())
        assert set(session.client.rooms) == set(room_ids)
        assert session.cursor == "s1"
        reads = [
            sql for sql in statements if "SELECT body" in sql and "'membership'" in sql
        ]
        assert len(reads) == 1
    finally:
        await session.close()


def test_membership_intent_cache_tracks_writes_deletion_and_nested_rollback(tmp_path):
    store = open_store(tmp_path)
    try:
        with store.transaction():
            assert store.read_local_intent() is None
            store.save_local_intent({"room_id": ROOM, "observed": False}, create=True)
            intent = store.read_local_intent()
            intent["observed"] = True
            assert store.read_local_intent()["observed"] is False
            store.save_local_intent(intent)
            assert store.read_local_intent()["observed"] is True
            with pytest.raises(RuntimeError, match="rollback"):
                with store.transaction():
                    store.delete_local_intent()
                    assert store.read_local_intent() is None
                    raise RuntimeError("rollback")
            assert store.read_local_intent()["observed"] is True
        with pytest.raises(RuntimeError, match="rollback"):
            with store.transaction():
                store.delete_local_intent()
                assert store.read_local_intent() is None
                raise RuntimeError("rollback")
        assert store.read_local_intent()["observed"] is True
        with store.transaction():
            store.delete_local_intent()
            assert store.read_local_intent() is None
        assert store.read_local_intent() is None
    finally:
        store.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("reopen", [False, True])
async def test_unchanged_room_projection_skips_write_but_persists_mutated_metadata(
    tmp_path, monkeypatch, reopen
):
    session = open_session(tmp_path)
    await baseline(session)
    if reopen:
        await session.close()
        session = open_session(tmp_path)
    statements = observe_sql(monkeypatch, session._store)
    try:
        with session._store.transaction():
            session._save_rooms({ROOM: session.client.rooms[ROOM]}, set())
        assert not any(
            sql.startswith("INSERT INTO NioDurableRoom") for sql in statements
        )
        # Membership code mutates the live dictionary before persistence.
        session._metadata[ROOM]["baseline"] = False
        with session._store.transaction():
            session._save_rooms({ROOM: session.client.rooms[ROOM]}, set())
        stored = session._store.database.execute_sql(
            "SELECT metadata FROM NioDurableRoom WHERE room_id=?", (ROOM,)
        ).fetchone()[0]
        assert json.loads(stored)["baseline"] is False
        assert (
            sum(sql.startswith("INSERT INTO NioDurableRoom") for sql in statements) == 1
        )
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_member_delta_is_saved_when_room_metadata_is_unchanged(
    tmp_path, monkeypatch
):
    session = open_session(tmp_path)
    await baseline(session)
    statements = observe_sql(monkeypatch, session._store)
    try:
        room = session.client.rooms[ROOM]
        room.users[USER].display_name = "New name"
        with session._store.transaction():
            session._save_rooms({ROOM: room}, {(ROOM, USER)})
        assert not any(
            sql.startswith("INSERT INTO NioDurableRoom") for sql in statements
        )
    finally:
        await session.close()
    reopened = open_session(tmp_path)
    try:
        assert reopened.client.rooms[ROOM].users[USER].display_name == "New name"
    finally:
        await reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("nested", [False, True])
async def test_rolled_back_room_projection_is_not_reused(tmp_path, nested):
    session = open_session(tmp_path)
    await baseline(session)
    room = session.client.rooms[ROOM]

    def save_topic(topic):
        room.topic = topic
        session._save_rooms({ROOM: room}, set())

    try:
        with session._store.transaction():
            save_topic("committed")
        if nested:
            with session._store.transaction():
                save_topic("outer")
                with pytest.raises(RuntimeError, match="rollback"):
                    with session._store.transaction():
                        save_topic("retry")
                        raise RuntimeError("rollback")
        else:
            with pytest.raises(RuntimeError, match="rollback"):
                with session._store.transaction():
                    save_topic("retry")
                    raise RuntimeError("rollback")
        with session._store.transaction():
            save_topic("retry")
    finally:
        await session.close()
    reopened = open_session(tmp_path)
    try:
        assert reopened.client.rooms[ROOM].topic == "retry"
    finally:
        await reopened.close()
