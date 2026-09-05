"""Recovery boundaries through the owned journal and real preparation."""

import asyncio
import base64
import json
import sqlite3

import pytest
from owned_capacity_test import (
    ACCOUNT_ID,
    DEVICE_ID,
    ROOM_ID,
    _baseline_session,
    _open_session,
    _settle_frame,
)
from owned_recovery_test import _capture, _limited_body

from nio.crypto import OlmAccount
from nio.event_provenance import TimelineEventProvenance
from nio.ingest.errors import JournalIntegrityError
from nio.ingest.hydration import normalize_hydration_response
from nio.ingest.model import RecordKind
from nio.ingest.ports import NetworkResult
from nio.ingest.source import canonical_json, load_json
from nio.store import SqliteStore
from nio.store._sync_journal_values import MaterializerLimits, MaterializeStatus


def _member(event_id, membership, timestamp):
    return {
        "type": "m.room.member",
        "state_key": ACCOUNT_ID,
        "event_id": event_id,
        "sender": ACCOUNT_ID,
        "origin_server_ts": timestamp,
        "content": {"membership": membership},
    }


@pytest.mark.asyncio
async def test_cancelled_capture_resumes_after_its_last_settled_page(
    tmp_path, monkeypatch
):
    session = await _baseline_session(tmp_path)
    original = session._journal.load_source()
    body, missed, _ = _limited_body()
    page_started = asyncio.Event()
    records = []

    async def respond(request, **kwargs):
        if request.path.endswith("/messages"):
            if dict(request.query)["from"] == "s1":
                response = {"start": "s1", "end": "middle", "chunk": [missed]}
            else:
                assert dict(request.query)["from"] == "middle"
                page_started.set()
                await asyncio.Future()
        else:
            assert request.path.endswith("/sync")
            response = body
        return NetworkResult(
            request.stream_id,
            request.transport,
            request.source_epoch,
            request.request_id,
            200,
            canonical_json(response),
            None,
            None,
        )

    monkeypatch.setattr(session, "_request", respond)
    running = asyncio.create_task(session.run())

    async def consume():
        while not running.done():
            batch = session.next_batch()
            if batch is not None:
                records.extend(batch.records)
                await session._settle_batch(
                    batch, receipt_new=True, semantic_event_new=True
                )
            await asyncio.sleep(0)

    consuming = asyncio.create_task(consume())
    try:
        async with asyncio.timeout(2):
            await page_started.wait()
        running.cancel()
        with pytest.raises(asyncio.CancelledError):
            await running
        cursor = load_json(session._journal.load_source().cursor_json, "cursor")
        assert cursor["next_batch"] == "s1"
        assert cursor["recovery"]["token"] == "middle"
        assert session._journal.list_frames(1) == ()
        assert session.next_batch() is None
        assert [record.event_id for record in records] == [missed["event_id"]]
        await session.close()
        session = _open_session(tmp_path)
        requests = await _capture(
            session,
            monkeypatch,
            body,
            [{"start": "middle", "chunk": []}],
            recovered_records=records,
        )
        assert len(requests) == 1
        assert requests[0].path.endswith("/messages")
        staged = session._journal.list_frames(1)[0]
        session._materialize_oldest_frame(limits=MaterializerLimits())
        records.extend(await _settle_frame(session, staged.frame_id))
        assert [record.event_id for record in records] == ["$message0", "$message1"]
    finally:
        consuming.cancel()
        await session.close()
        await asyncio.gather(running, consuming, return_exceptions=True)


@pytest.mark.asyncio
async def test_recovered_state_change_is_applied_between_messages(
    tmp_path, monkeypatch
):
    session = await _baseline_session(tmp_path)
    body, missed, fresh = _limited_body()
    name = {
        "type": "m.room.name",
        "state_key": "",
        "event_id": "$rename",
        "sender": ACCOUNT_ID,
        "origin_server_ts": 101,
        "content": {"name": "New name"},
    }
    body["rooms"]["join"][ROOM_ID]["state"] = {"events": [name]}
    observed = []
    records = []
    handle = session._client._handle_timeline_event

    def observe(event, room_id, room, encrypted_rooms):
        observed.append((event.event_id, room.name))
        return handle(event, room_id, room, encrypted_rooms)

    monkeypatch.setattr(session._client, "_handle_timeline_event", observe)
    try:
        await _capture(
            session,
            monkeypatch,
            body,
            [
                {"start": "s1", "end": "middle", "chunk": [missed]},
                {"start": "middle", "chunk": [name]},
            ],
            recovered_records=records,
        )
        staged = session._journal.list_frames(1)[0]
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        assert observed == [
            ("$message0", None),
            ("$rename", None),
            ("$message1", "New name"),
        ]
        records.extend(await _settle_frame(session, staged.frame_id))
        assert [
            (record.kind, record.event_id)
            for record in records
            if getattr(record, "event_id", None) == "$rename"
            and record.kind is RecordKind.TIMELINE
        ] == [(RecordKind.TIMELINE, "$rename")]
        assert session._client.rooms[ROOM_ID].name == "New name"
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_recovered_leave_rejoin_orders_lifecycle_and_fences_epochs(
    tmp_path, monkeypatch
):
    session = await _baseline_session(tmp_path)
    body, missed, fresh = _limited_body()
    leave = _member("$leave", "leave", 101)
    rejoin = _member("$rejoin", "join", 102)
    body["rooms"]["join"][ROOM_ID]["state"] = {"events": [rejoin]}
    records = []
    try:
        await _capture(
            session,
            monkeypatch,
            body,
            [
                {"start": "s1", "end": "middle", "chunk": [missed, leave]},
                {"start": "middle", "chunk": [rejoin]},
            ],
            recovered_records=records,
        )
        staged = session._journal.list_frames(1)[0]
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        records.extend(await _settle_frame(session, staged.frame_id))
        assert [
            (record.kind, record.event_id, record.membership_epoch)
            for record in records
            if record.kind in (RecordKind.TIMELINE, RecordKind.ROOM_LIFECYCLE)
        ] == [
            (RecordKind.TIMELINE, "$message0", 0),
            (RecordKind.ROOM_LIFECYCLE, None, 1),
            (RecordKind.TIMELINE, "$leave", 1),
            (RecordKind.ROOM_LIFECYCLE, None, 1),
            (RecordKind.TIMELINE, "$rejoin", 1),
            (RecordKind.TIMELINE, "$message1", 1),
        ]
        lifecycle = [
            json.loads(record.source_json)
            for record in records
            if record.kind is RecordKind.ROOM_LIFECYCLE
        ]
        assert [
            (
                value["event_id"],
                value["previous_membership"],
                value["membership"],
                value["previous_membership_epoch"],
                value["membership_epoch"],
                value["timeline_provenance"],
            )
            for value in lifecycle
        ] == [
            ("$leave", "join", "leave", 0, 1, "recovered"),
            ("$rejoin", "leave", "join", 1, 1, "recovered"),
        ]
        assert [
            record.provenance
            for record in records
            if record.kind is RecordKind.TIMELINE
        ] == [
            TimelineEventProvenance.RECOVERED,
            TimelineEventProvenance.RECOVERED,
            TimelineEventProvenance.RECOVERED,
            TimelineEventProvenance.LIVE,
        ]
        assert len({record.record_id for record in records}) == len(records)
        assert session._journal.load_pending_hydrations(limit=1) == ()
    finally:
        await session.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("corrupt", [False, True])
async def test_reopen_replays_authenticated_page_without_refetch(
    tmp_path, monkeypatch, corrupt
):
    session = await _baseline_session(tmp_path)
    body, missed, _ = _limited_body()
    materialize = session._materialize_oldest_frame

    def stop_before_page_preparation(*, limits):
        frames = session._journal.list_frames(1)
        if frames and frames[0].response.recovery_json is not None:
            raise asyncio.CancelledError
        return materialize(limits=limits)

    monkeypatch.setattr(
        session, "_materialize_oldest_frame", stop_before_page_preparation
    )
    try:
        with pytest.raises(asyncio.CancelledError):
            await _capture(
                session, monkeypatch, body, [{"start": "s1", "chunk": [missed]}]
            )
        frame_id = session._journal.list_frames(1)[0].frame_id
    finally:
        await session.close()

    if not corrupt:
        session = _open_session(tmp_path)
        records = []
        try:
            requests = await _capture(
                session, monkeypatch, body, [], recovered_records=records
            )
            assert requests == []
            tail = session._journal.list_frames(1)[0]
            session._materialize_oldest_frame(limits=MaterializerLimits())
            records.extend(await _settle_frame(session, tail.frame_id))
            assert [record.event_id for record in records] == ["$message0", "$message1"]
        finally:
            await session.close()
        return

    with sqlite3.connect(tmp_path / "journal.db") as connection:
        payload = connection.execute(
            "SELECT payload FROM NioIngestFrame WHERE frame_id = ?", (str(frame_id),)
        ).fetchone()[0]
        envelope = json.loads(payload)
        evidence = json.loads(base64.b64decode(envelope["value"]["recovery_json"]))
        evidence["start"] = "s9"
        envelope["value"]["recovery_json"] = base64.b64encode(
            canonical_json(evidence)
        ).decode()
        changed = json.dumps(
            envelope, ensure_ascii=False, separators=(",", ":")
        ).encode()
        assert len(changed) == len(payload)
        connection.execute(
            "UPDATE NioIngestFrame SET payload = ? WHERE frame_id = ?",
            (changed, str(frame_id)),
        )

    reopened = None
    try:
        with pytest.raises(JournalIntegrityError):
            reopened = _open_session(tmp_path)
            reopened._journal.list_frames(1)
    finally:
        if reopened is not None:
            await reopened.close()


@pytest.mark.asyncio
async def test_cold_sync_keeps_history_without_fetching_an_actionable_interval(
    tmp_path, monkeypatch
):
    store = SqliteStore(
        ACCOUNT_ID, DEVICE_ID, str(tmp_path), database_name="journal.db"
    )
    store.save_account(OlmAccount())
    store.database.close()
    session = _open_session(tmp_path)
    body, _, fresh = _limited_body()
    member = _member("$cold-member", "join", 1)
    body["rooms"]["join"][ROOM_ID]["state"] = {"events": [member]}
    try:
        requests = await _capture(session, monkeypatch, body, [])
        assert len(requests) == 1
        assert requests[0].path.endswith("/sync")
        assert "since" not in dict(requests[0].query)
        staged = session._journal.list_frames(1)[0]
        result = session._materialize_oldest_frame(limits=MaterializerLimits())
        assert result.status is MaterializeStatus.MATERIALIZED
        pending = session._journal.load_pending_hydrations(limit=1)
        assert len(pending) == 1
        hydrated = normalize_hydration_response(
            pending[0],
            own_user_id=ACCOUNT_ID,
            response_body=canonical_json([member]),
        )
        session._journal.apply_hydration_result(result=hydrated)
        records = await _settle_frame(session, staged.frame_id)
        assert [
            (record.event_id, record.provenance)
            for record in records
            if getattr(record, "kind", None) is RecordKind.TIMELINE
        ] == [("$message1", TimelineEventProvenance.HISTORY)]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_recovery_prologue_keys_decrypt_later_history_once(tmp_path, monkeypatch):
    import preparation_test

    session = await _baseline_session(tmp_path)
    monkeypatch.setattr(preparation_test, "DEVICE", session._client.device_id)
    to_device, encrypted, sender_store = preparation_test._same_frame_crypto_material(
        session._client
    )
    session._client.olm.save_account()
    body, _, _ = _limited_body()
    body["to_device"] = {"events": [to_device]}
    records = []
    try:
        await _capture(
            session,
            monkeypatch,
            body,
            [{"start": "s1", "chunk": [encrypted]}],
            recovered_records=records,
        )
        staged = session._journal.list_frames(1)[0]
        session._materialize_oldest_frame(limits=MaterializerLimits())
        records.extend(await _settle_frame(session, staged.frame_id))
        keys = [record for record in records if record.kind is RecordKind.TO_DEVICE]
        assert len(keys) == 1
        secret = next(record for record in records if record.event_id == "$secret")
        assert secret.provenance is TimelineEventProvenance.RECOVERED
        assert json.loads(secret.clear_json)["content"]["body"] == "same-frame secret"
        assert records.index(keys[0]) < records.index(secret)
    finally:
        sender_store.database.close()
        await session.close()


@pytest.mark.asyncio
async def test_local_departure_waits_for_the_pending_recovery_interval(
    tmp_path, monkeypatch
):
    from uuid import uuid4

    session = await _baseline_session(tmp_path)
    body, missed, _ = _limited_body()
    leave_task = None
    records = []

    def queue_departure(request):
        nonlocal leave_task
        if request.path.endswith("/messages") and leave_task is None:
            leave_task = asyncio.create_task(
                session._run_local_membership_transition(
                    operation_id=uuid4(),
                    room_id=ROOM_ID,
                    previous_membership="join",
                    previous_epoch=0,
                    current_membership="leave",
                )
            )

    try:
        requests = await _capture(
            session,
            monkeypatch,
            body,
            [
                {"start": "s1", "end": "middle", "chunk": [missed]},
                {"start": "middle", "chunk": []},
            ],
            recovered_records=records,
            on_request=queue_departure,
        )
        assert leave_task is not None and not leave_task.done()
        assert all(not request.path.endswith("/leave") for request in requests)
        tail = session._journal.list_frames(1)[0]
        session._materialize_oldest_frame(limits=MaterializerLimits())
        records.extend(await _settle_frame(session, tail.frame_id))
        assert [(record.event_id, record.membership_epoch) for record in records] == [
            ("$message0", 0),
            ("$message1", 0),
        ]
    finally:
        await session.close()
        if leave_task is not None:
            await asyncio.gather(leave_task, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize("intermediate", ["invite", "knock"])
async def test_recovered_rejoin_routes_after_an_invited_projection(
    tmp_path, monkeypatch, intermediate
):
    session = await _baseline_session(tmp_path)
    body, missed, _ = _limited_body()
    leave = _member("$leave", "leave", 101)
    invitation = _member("$invitation", intermediate, 102)
    rejoin = _member("$rejoin", "join", 103)
    records = []
    callbacks = []
    session._client.add_event_callback(
        lambda room, event: callbacks.append(event.event_id), None
    )
    try:
        await _capture(
            session,
            monkeypatch,
            body,
            [
                {"start": "s1", "end": "p1", "chunk": [missed, leave]},
                {"start": "p1", "end": "p2", "chunk": [invitation]},
                {"start": "p2", "chunk": [rejoin]},
            ],
            recovered_records=records,
        )
        tail = session._journal.list_frames(1)[0]
        session._materialize_oldest_frame(limits=MaterializerLimits())
        records.extend(await _settle_frame(session, tail.frame_id))
        assert callbacks == [
            "$message0",
            "$leave",
            "$invitation",
            "$rejoin",
            "$message1",
        ]
        assert [
            (record.event_id, record.membership_epoch)
            for record in records
            if record.kind is RecordKind.TIMELINE
        ] == [
            ("$message0", 0),
            ("$leave", 1),
            ("$invitation", 1),
            ("$rejoin", 1),
            ("$message1", 1),
        ]
    finally:
        await session.close()
