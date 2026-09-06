"""Bounded forward history uses the old projection and drains before paging."""

import asyncio
import json

import pytest
from aiohttp import web

from nio import TimelineEventProvenance
from nio.durable import DurableSyncConfig
from nio.durable.model import RecordKind

from .client_test import ROOM, USER, client, open_session, response
from .runner_test import drain_sync, homeserver


def message(event_id):
    event = json.loads(response())["rooms"]["join"][ROOM]["timeline"]["events"][0]
    event["event_id"] = event_id
    event["content"]["body"] = event_id
    return event


def member(event_id, membership, user=USER):
    event = json.loads(response())["rooms"]["join"][ROOM]["state"]["events"][0]
    event.update(event_id=event_id, state_key=user)
    event["content"]["membership"] = membership
    return event


def limited(*, section="join", state=None):
    return json.dumps(
        {
            "next_batch": "s2",
            "rooms": {
                section: {
                    ROOM: {
                        "state": {"events": state or []},
                        "timeline": {
                            "limited": True,
                            "prev_batch": "tail",
                            "events": [message("$tail")],
                        },
                    }
                }
            },
        }
    ).encode()


async def baseline(session):
    body = json.loads(response(messages=0))
    body["rooms"]["join"][ROOM]["state"]["events"].append(
        member("$old", "join", "@old:example.org")
    )
    await session._accept_response(json.dumps(body).encode())
    while batch := await session.next_batch():
        await session.ack(batch)
    with session._store.transaction():
        session._store.finish_input()


@pytest.mark.asyncio
async def test_recovery_orders_tenures_and_resets_rejoin_projection(tmp_path):
    async def sync(request):
        raise AssertionError("retained input must finish without polling")

    async def history(request):
        assert request.query["from"] == "s1"
        assert "to" not in request.query
        assert request.query["dir"] == "f"
        return web.json_response(
            {
                "start": "s1",
                "end": "tail",
                "chunk": [
                    message("$before"),
                    member("$leave", "leave"),
                    message("$outside"),
                    member("$rejoin", "join"),
                    message("$after"),
                ],
            }
        )

    async with homeserver(sync, membership=history) as (url, requests):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(limited())
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            timeline = [r for r in records if r.kind is RecordKind.TIMELINE]
            assert [r.source["event_id"] for r in timeline] == [
                "$before",
                "$leave",
                "$outside",
                "$rejoin",
                "$after",
                "$tail",
            ]
            assert [
                (r.source["event_id"], r.membership_epoch, r.provenance)
                for r in timeline
                if r.source["type"] == "m.room.message"
            ] == [
                ("$before", 0, TimelineEventProvenance.RECOVERED),
                ("$outside", 1, TimelineEventProvenance.HISTORY),
                ("$after", 1, TimelineEventProvenance.HISTORY),
                ("$tail", 1, TimelineEventProvenance.HISTORY),
            ]
            assert set(nio_client.rooms[ROOM].users) == {USER}
            assert session.cursor == "s2"
            assert len([p for p, _ in requests if p.endswith("/messages")]) == 1
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_recovery_drains_page_before_fetch_and_restarts_at_committed_token(
    tmp_path,
):
    second = asyncio.Event()

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        start = request.query["from"]
        if start == "page2":
            second.set()
        return web.json_response(
            {
                "start": start,
                "end": "page2" if start == "s1" else "tail",
                "chunk": [message("$one" if start == "s1" else "$two")],
            }
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(limited())
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            async with asyncio.timeout(5):
                await session.wait_for_work()
            batch = await session.next_batch()
            assert batch.records[0].source["event_id"] == "$one"
            assert session.cursor == "s1"
            await asyncio.sleep(0.02)
            assert not second.is_set()
        finally:
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)
        nio_client = client()
        nio_client.homeserver = url
        reopened = open_session(tmp_path, nio_client)
        reopened._quiescing = True
        runner = asyncio.create_task(reopened.run())
        try:
            assert await reopened.next_batch() == batch
            records = await drain_sync(reopened)
            await runner
            assert [
                r.source["event_id"] for r in records if r.kind is RecordKind.TIMELINE
            ] == ["$one", "$two", "$tail"]
            assert reopened.cursor == "s2"
        finally:
            await reopened.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_recovery_reaches_exclusive_boundary_by_tail_overlap(tmp_path, restart):
    calls = []
    later_member = member("$later-join", "join", "@later:example.org")
    later_message = message("$later-message")

    async def sync(request):
        raise AssertionError("retained input must finish without polling")

    async def history(request):
        calls.append(dict(request.query))
        start = request.query["from"]
        if start == "s1":
            end = "before-tail"
            events = [
                member("$old-leave", "leave", "@old:example.org"),
                message("$one"),
            ]
        elif "to" in request.query:
            # An exclusive upper bound never returns its boundary event/token.
            end, events = None, []
        else:
            end = "after-tail"
            events = [message("$tail"), later_member, later_message]
        page = {"start": start, "chunk": events}
        if end is not None:
            page["end"] = end
        return web.json_response(page)

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        runner = None
        try:
            await baseline(session)
            session._capture_response(
                limited(state=[member("$old-return", "join", "@old:example.org")])
            )
            await session._recovery.advance()
            assert session.cursor == "s1"
            assert set(nio_client.rooms[ROOM].users) == {USER}
            # The committed page must drain before any further HTTP request.
            await session._recovery.advance()
            assert len(calls) == 1
            records = []
            while batch := await session.next_batch():
                records.extend(batch.records)
                await session.ack(batch)
            if restart:
                await session.close()
                await nio_client.close()
                nio_client = client()
                nio_client.homeserver = url
                session = open_session(tmp_path, nio_client)
                assert session.cursor == "s1"
                assert set(nio_client.rooms[ROOM].users) == {USER}
            session._quiescing = True
            runner = asyncio.create_task(session.run())
            records.extend(await drain_sync(session))
            await runner
            assert not any(record.kind is RecordKind.LOSS for record in records)
            timeline = [
                record for record in records if record.kind is RecordKind.TIMELINE
            ]
            assert [
                (record.source["event_id"], record.provenance) for record in timeline
            ] == [
                ("$old-leave", TimelineEventProvenance.RECOVERED),
                ("$one", TimelineEventProvenance.RECOVERED),
                ("$tail", TimelineEventProvenance.LIVE),
            ]
            assert set(nio_client.rooms[ROOM].users) == {USER, "@old:example.org"}
            assert session.cursor == "s2"
            assert calls == [
                {"from": "s1", "dir": "f", "limit": "100"},
                {"from": "before-tail", "dir": "f", "limit": "100"},
            ]
            future = json.loads(response(token="s3", messages=0))
            future["rooms"]["join"][ROOM]["timeline"]["events"] = [
                later_member,
                later_message,
            ]
            await session._accept_response(json.dumps(future).encode())
            future_records = []
            while batch := await session.next_batch():
                future_records.extend(batch.records)
                await session.ack(batch)
            assert [
                (record.source["event_id"], record.provenance)
                for record in future_records
                if record.kind is RecordKind.TIMELINE
            ] == [
                ("$later-join", TimelineEventProvenance.LIVE),
                ("$later-message", TimelineEventProvenance.LIVE),
            ]
            assert "@later:example.org" in nio_client.rooms[ROOM].users
            assert session.cursor == "s3"
        finally:
            await session.close()
            await nio_client.close()
            if runner is not None:
                await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "mode",
    [
        "empty",
        "missing",
        "cycle",
        "forbidden",
        "cap",
        "malformed",
        "oversized",
        "overlap",
    ],
)
async def test_recovery_boundaries_and_loss_fence(tmp_path, mode):
    calls = []

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        start = request.query["from"]
        calls.append(dict(request.query))
        if mode == "forbidden":
            return web.Response(status=403)
        if mode == "oversized" and int(request.query["limit"]) > 1:
            return web.json_response(
                {
                    "start": start,
                    "end": "tail",
                    "chunk": [message(str(i)) for i in range(3)],
                }
            )
        if mode == "empty" and start == "s1":
            return web.json_response({"start": start, "end": "next", "chunk": []})
        if mode == "cycle":
            return web.json_response(
                {"start": start, "end": "next" if start == "s1" else "s1", "chunk": []}
            )
        end = None if mode == "missing" else "next" if mode == "cap" else "tail"
        events = [message("$history")]
        if mode == "overlap":
            events += [message("$tail"), member("$never-apply", "leave")]
            end = None
        if mode == "malformed":
            events = [member("$bad-member", "invalid")]
        value = {"start": start, "chunk": events}
        if end is not None:
            value["end"] = end
        return web.json_response(value)

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(
            tmp_path,
            nio_client,
            DurableSyncConfig(
                recovery_page_size=100 if mode == "overlap" else 2,
                max_recovery_pages=1 if mode == "cap" else 1000,
            ),
        )
        await baseline(session)
        session._capture_response(limited())
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            loss = [r for r in records if r.kind is RecordKind.LOSS]
            assert bool(loss) == (
                mode in {"missing", "cycle", "forbidden", "cap", "malformed"}
            )
            tail = next(r for r in records if r.source.get("event_id") == "$tail")
            assert tail.provenance is (
                TimelineEventProvenance.HISTORY
                if loss
                else TimelineEventProvenance.LIVE
            )
            assert session.cursor == "s2"
            assert session._metadata[ROOM]["membership"] == "join"
            if mode in {"empty", "cycle", "oversized"}:
                assert len(calls) == 2
            if mode == "oversized":
                assert [q["limit"] for q in calls] == ["2", "1"]
                assert {q["from"] for q in calls} == {"s1"}
            if mode == "overlap":
                assert sum(r.source.get("event_id") == "$tail" for r in records) == 1
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_authoritative_tail_state_overlap_does_not_duplicate_member_observation(
    tmp_path,
):
    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.json_response(
            {
                "start": "s1",
                "end": "tail",
                "chunk": [
                    member("$overlap", "leave", "@old:example.org"),
                    member("$newer", "join", "@old:example.org"),
                ],
            }
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(
            limited(state=[member("$newer", "join", "@old:example.org")])
        )
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            assert "@old:example.org" in nio_client.rooms[ROOM].users
            assert sum(r.source.get("event_id") == "$newer" for r in records) == 1
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_leave_section_fallback_follows_recovered_and_retained_messages(tmp_path):
    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.json_response(
            {"start": "s1", "end": "tail", "chunk": [message("$before")]}
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(limited(section="leave"))
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            assert [r.source.get("event_id") for r in records] == [
                "$before",
                "$tail",
                None,
            ]
            assert [r.membership_epoch for r in records] == [0, 0, 1]
            assert records[-1].membership.current == "leave"
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_unknown_adopted_cursor_requests_full_state_and_only_future_tail_is_live(
    tmp_path,
):
    requests_seen = []
    stop = asyncio.Event()

    async def sync(request):
        requests_seen.append(dict(request.query))
        if len(requests_seen) > 2:
            await stop.wait()
        return web.Response(body=response(token=f"s{len(requests_seen)}"))

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        with session._store.transaction():
            session._store.set_cursor("adopted")
        runner = asyncio.create_task(session.run())
        try:
            first = await drain_sync(session)
            second = await drain_sync(session)
            await session.quiesce()
            await runner
            assert requests_seen[0]["full_state"] == "true"
            assert any(r.kind is RecordKind.LOSS for r in first)
            assert {r.provenance for r in first if r.kind is RecordKind.TIMELINE} == {
                TimelineEventProvenance.HISTORY
            }
            assert {r.provenance for r in second if r.kind is RecordKind.TIMELINE} == {
                TimelineEventProvenance.LIVE
            }
        finally:
            stop.set()
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_recovery_continues_after_departure_to_find_rejoin_on_later_page(
    tmp_path,
):
    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        start = request.query["from"]
        return web.json_response(
            {
                "start": start,
                "end": "next" if start == "s1" else "tail",
                "chunk": (
                    [member("$leave", "leave")]
                    if start == "s1"
                    else [member("$rejoin", "join"), message("$after-rejoin")]
                ),
            }
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(limited())
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            assert [r.membership.current for r in records if r.membership] == [
                "leave",
                "join",
            ]
            assert "$after-rejoin" in [r.source.get("event_id") for r in records]
            assert session._metadata[ROOM]["membership_epoch"] == 1
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_committed_prologue_key_decrypts_history_after_restart(
    tmp_path, monkeypatch
):
    from .crash_test import BODY, durable_client, prepare
    from nio.durable.codec import restore_event

    await prepare(tmp_path)
    encrypted_sync = json.loads((tmp_path / "response.json").read_text())
    encrypted_event = encrypted_sync["rooms"]["join"][ROOM]["timeline"]["events"][0]
    nio_client, session = durable_client(tmp_path)
    initial = json.loads(json.dumps(encrypted_sync))
    initial.pop("to_device")
    initial["rooms"]["join"][ROOM]["timeline"]["events"] = []
    await session._accept_response(json.dumps(initial).encode(), full_state=True)
    while batch := await session.next_batch():
        await session.ack(batch)
    with session._store.transaction():
        session._store.finish_input()
    body = json.loads(limited())
    body["to_device"] = encrypted_sync["to_device"]
    session._capture_response(json.dumps(body).encode())
    session._prepare_pending()
    assert session.cursor == "s1"
    assert session._store.input[1]["phase"] == "recover"
    assert any(
        r.kind is RecordKind.TO_DEVICE for r in (await session.next_batch()).records
    )
    await session.close()
    await nio_client.close()

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.json_response(
            {"start": "s1", "end": "tail", "chunk": [encrypted_event]}
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client, reopened = durable_client(tmp_path)
        nio_client.homeserver = url
        nio_client.access_token = "test-token"
        original = nio_client._iter_to_device

        def no_redecrypt(value):
            assert not value.to_device_events
            yield from original(value)

        monkeypatch.setattr(nio_client, "_iter_to_device", no_redecrypt)
        reopened._quiescing = True
        runner = asyncio.create_task(reopened.run())
        try:
            records = await drain_sync(reopened)
            await runner
            decrypted = next(
                r for r in records if r.source.get("event_id") == "$encrypted-message"
            )
            assert restore_event(decrypted).body == BODY
            assert decrypted.provenance is TimelineEventProvenance.RECOVERED
        finally:
            await reopened.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", ["page", "tail"])
async def test_interrupted_transaction_restores_old_cursor_and_projection(
    tmp_path, monkeypatch, boundary
):
    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.json_response(
            {
                "start": "s1",
                "end": "tail",
                "chunk": [member("$gone", "leave", "@old:example.org")],
            }
        )

    async with homeserver(sync, membership=history) as (url, requests):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(limited())
        session._prepare_pending()
        original = (
            nio_client._iter_room_timeline
            if boundary == "page"
            else nio_client._iter_sync
        )

        def interrupted(*args, **kwargs):
            for item in original(*args, **kwargs):
                if item.route == "event":
                    raise RuntimeError("interrupted transaction")
                yield item

        monkeypatch.setattr(
            nio_client,
            "_iter_room_timeline" if boundary == "page" else "_iter_sync",
            interrupted,
        )
        if boundary == "tail":
            await session._recovery.advance()
            while batch := await session.next_batch():
                await session.ack(batch)
        with pytest.raises(RuntimeError, match="interrupted transaction"):
            await session._recovery.advance()
        await session.close()
        await nio_client.close()
        nio_client = client()
        nio_client.homeserver = url
        reopened = open_session(tmp_path, nio_client)
        assert reopened.cursor == "s1"
        assert ("@old:example.org" in nio_client.rooms[ROOM].users) == (
            boundary == "page"
        )
        reopened._quiescing = True
        runner = asyncio.create_task(reopened.run())
        try:
            records = await drain_sync(reopened)
            await runner
            assert sum(r.source.get("event_id") == "$gone" for r in records) == (
                1 if boundary == "page" else 0
            )
            assert "@old:example.org" not in nio_client.rooms[ROOM].users
            assert reopened.cursor == "s2"
            assert len([p for p, _ in requests if p.endswith("/messages")]) == (
                2 if boundary == "page" else 1
            )
        finally:
            await reopened.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_leave_fallback_is_not_suppressed_by_historical_self_join(tmp_path):
    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.json_response(
            {"start": "s1", "end": "tail", "chunk": [member("$still-joined", "join")]}
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(limited(section="leave"))
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            assert records[-1].membership.current == "leave"
            assert session._metadata[ROOM]["membership_epoch"] == 1
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_local_join_empty_projection_never_authorizes_initial_tail(tmp_path):
    from .membership_test import OPERATION

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def membership(request):
        return web.json_response({"room_id": ROOM})

    async with homeserver(sync, membership=membership) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        with session._store.transaction():
            session._store.set_cursor("before-join")
        try:
            assert await session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership="leave",
                previous_epoch=0,
                current_membership="join",
            )
            batch = await session.next_batch()
            await session.ack(batch)
            assert session._recovery.needs_full_state()
            await session._accept_response(response(), full_state=True)
            records = []
            while batch := await session.next_batch():
                records.extend(batch.records)
                await session.ack(batch)
            assert {r.provenance for r in records if r.kind is RecordKind.TIMELINE} == {
                TimelineEventProvenance.HISTORY
            }
            assert session._metadata[ROOM]["baseline"]
            await session.wait_for_membership_idle()
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_tail_state_rejoin_resets_projection_before_full_state(tmp_path):
    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.json_response(
            {"start": "s1", "end": "tail", "chunk": [member("$leave", "leave")]}
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(
            limited(state=[member("$current-join", "join")]), full_state=True
        )
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            assert [r.membership.current for r in records if r.membership] == [
                "leave",
                "join",
            ]
            assert set(nio_client.rooms[ROOM].users) == {USER}
            assert session._metadata[ROOM]["baseline"]
            await session.wait_for_membership_idle()
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_rejoin_restores_full_state_overlap_without_duplicate_observations(
    tmp_path, restart
):
    bob = "@bob:example.org"
    bob_join = member("$bob-join", "join", bob)
    power = {
        "type": "m.room.power_levels",
        "state_key": "",
        "sender": USER,
        "event_id": "$power",
        "origin_server_ts": 1,
        "content": {"users": {USER: 100, bob: 75}},
    }

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.json_response(
            {
                "start": "s1",
                "end": "tail",
                "chunk": [
                    bob_join,
                    power,
                    member("$departure", "leave"),
                    member("$rejoin", "join"),
                ],
            }
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client)
        await baseline(session)
        session._capture_response(
            limited(state=[bob_join, power, member("$rejoin", "join")]), full_state=True
        )
        await session._recovery.advance()
        records = []
        while batch := await session.next_batch():
            records.extend(batch.records)
            await session.ack(batch)
        assert session.cursor == "s1"
        if restart:
            await session.close()
            await nio_client.close()
            nio_client = client()
            nio_client.homeserver = url
            session = open_session(tmp_path, nio_client)
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records.extend(await drain_sync(session))
            await runner
            assert bob in nio_client.rooms[ROOM].users
            assert nio_client.rooms[ROOM].power_levels.users[bob] == 75
            assert session._metadata[ROOM]["baseline"]
            assert not session._recovery.needs_full_state()
            assert sum(r.source.get("event_id") == "$bob-join" for r in records) == 1
            assert sum(r.source.get("event_id") == "$power" for r in records) == 1
        finally:
            await session.close()
            await nio_client.close()
        reopened = open_session(tmp_path)
        try:
            assert bob in reopened.client.rooms[ROOM].users
            assert reopened.client.rooms[ROOM].power_levels.users[bob] == 75
        finally:
            await reopened.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("event_type", ["message", "member", "own_leave"])
async def test_frozen_recovered_record_overflow_commits_loss_and_resumes_tail(
    tmp_path, event_type
):
    oversized = (
        message("$too-large")
        if event_type == "message"
        else (
            member("$too-large", "leave", USER)
            if event_type == "own_leave"
            else member("$too-large", "join", "@oversized:example.org")
        )
    )
    oversized["content"]["body" if event_type == "message" else "displayname"] = (
        "x" * 900
    )
    page = json.dumps(
        {
            "start": "s1",
            "end": "tail",
            "chunk": [
                oversized,
                member(
                    "$unprocessed", "join" if event_type == "own_leave" else "leave"
                ),
            ],
        },
        separators=(",", ":"),
    ).encode()
    assert len(page) < 2 * 1024 * 1024

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.Response(body=page, content_type="application/json")

    async with homeserver(sync, membership=history) as (url, requests):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(
            tmp_path, nio_client, DurableSyncConfig(max_batch_bytes=1024)
        )
        await baseline(session)
        session._capture_response(limited())
        try:
            await session._recovery.advance()
            batch = await session.next_batch()
            assert [r.kind for r in batch.records] == [RecordKind.LOSS]
            assert session.cursor == "s1"
            assert not session._metadata[ROOM]["baseline"]
            assert session._metadata[ROOM]["membership"] == (
                "leave" if event_type == "own_leave" else "join"
            )
            if event_type == "own_leave":
                change = batch.records[0].membership
                assert (
                    change.previous,
                    change.current,
                    change.previous_epoch,
                    change.current_epoch,
                ) == ("join", "leave", 0, 1)
                assert batch.records[0].membership_epoch == 1
            if event_type == "member":
                assert "@oversized:example.org" in session.client.rooms[ROOM].users
        finally:
            await session.close()
            await nio_client.close()
        nio_client = client()
        nio_client.homeserver = url
        reopened = open_session(
            tmp_path, nio_client, DurableSyncConfig(max_batch_bytes=1024)
        )
        reopened._quiescing = True
        runner = asyncio.create_task(reopened.run())
        try:
            assert await reopened.next_batch() == batch
            if event_type == "member":
                assert "@oversized:example.org" in reopened.client.rooms[ROOM].users
            records = await drain_sync(reopened)
            await runner
            assert len([r for r in records if r.kind is RecordKind.LOSS]) == 1
            tail = next(r for r in records if r.source.get("event_id") == "$tail")
            assert tail.provenance is TimelineEventProvenance.HISTORY
            assert reopened.cursor == "s2"
            assert len([p for p, _ in requests if p.endswith("/messages")]) == 1
        finally:
            await reopened.close()
            await nio_client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("restart", [False, True])
async def test_oversized_state_covered_by_loss_is_not_published_again_at_tail(
    tmp_path, restart
):
    oversized = member("$too-large", "join", "@oversized:example.org")
    oversized["content"]["displayname"] = "x" * 900
    suffix = member("$untouched", "join", "@later:example.org")

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        return web.json_response(
            {
                "start": "s1",
                "end": "tail",
                "chunk": [message("$prefix"), oversized, suffix],
            }
        )

    async with homeserver(sync, membership=history) as (url, requests):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(
            tmp_path, nio_client, DurableSyncConfig(max_batch_bytes=1024)
        )
        await baseline(session)
        session._capture_response(limited(state=[oversized, suffix]))
        records = []
        try:
            await session._recovery.advance()
            while batch := await session.next_batch():
                records.extend(batch.records)
                await session.ack(batch)
            assert [r.kind for r in records] == [RecordKind.TIMELINE, RecordKind.LOSS]
            assert session.cursor == "s1"
            assert "@later:example.org" not in nio_client.rooms[ROOM].users
            if restart:
                await session.close()
                await nio_client.close()
                nio_client = client()
                nio_client.homeserver = url
                session = open_session(
                    tmp_path, nio_client, DurableSyncConfig(max_batch_bytes=1024)
                )
            # A state event already represented by LOSS must not overflow again.
            await session._recovery.advance()
            assert session.cursor == "s2"
            session._quiescing = True
            runner = asyncio.create_task(session.run())
            records.extend(await drain_sync(session))
            await runner
            assert session._store.input is None
            assert "@oversized:example.org" in nio_client.rooms[ROOM].users
            assert "@later:example.org" in nio_client.rooms[ROOM].users
            assert len([r for r in records if r.kind is RecordKind.LOSS]) == 1
            assert not any(r.source.get("event_id") == "$too-large" for r in records)
            assert sum(r.source.get("event_id") == "$untouched" for r in records) == 1
            assert sum(r.source.get("event_id") == "$prefix" for r in records) == 1
            assert len([p for p, _ in requests if p.endswith("/messages")]) == 1
        finally:
            await session.close()
            await nio_client.close()
