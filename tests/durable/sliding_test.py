"""Sliding windows share durable transactions and chronological recovery."""

import asyncio
import json
from urllib.parse import parse_qs, urlsplit

import pytest
from aiohttp import web

from nio import TimelineEventProvenance
from nio.durable import DurableSyncConfig, SlidingSyncConfig
from nio.durable.model import RecordKind
from nio.exceptions import LocalProtocolError

from .client_test import ROOM, USER, client, open_session
from .recovery_test import member, message
from .runner_test import drain_sync, homeserver


def settings(**kwargs):
    return DurableSyncConfig(sliding=SlidingSyncConfig(**kwargs))


def window(*ids, pos="p1", initial=True, prev_batch="w1", num_live=0, state=None):
    return json.dumps(
        {
            "pos": pos,
            "rooms": {
                ROOM: {
                    "initial": initial,
                    "membership": "join",
                    "required_state": (
                        [member("$join", "join")] if state is None else state
                    ),
                    "timeline": [message(event_id) for event_id in ids],
                    "prev_batch": prev_batch,
                    "num_live": num_live,
                    "limited": initial,
                }
            },
            "extensions": {"to_device": {"next_batch": "td1", "events": []}},
        }
    ).encode()


async def settle(session):
    records = []
    while batch := await session.next_batch():
        records.extend(batch.records)
        await session.ack(batch)
    with session._store.transaction():
        session._store.finish_input()
    session._recovery.response = None
    return records


@pytest.mark.asyncio
async def test_sliding_request_owns_device_cursor_and_preserves_caller_settings(
    tmp_path,
):
    lists = {
        "small": {"ranges": [[0, 9]], "required_state": [["m.room.member", "$LAZY"]]}
    }
    extensions = {"account_data": {"enabled": True}}
    config = settings(lists=lists, extensions=extensions)
    session = open_session(tmp_path, config=config)
    try:
        method, path, body = session._sliding.request()
        request = json.loads(body)
        assert method == "POST"
        assert "pos" not in parse_qs(urlsplit(path).query)
        assert parse_qs(urlsplit(path).query)["timeout"] == ["0"]
        assert ["m.room.member", "$ME"] in request["lists"]["small"]["required_state"]
        assert request["extensions"]["to_device"] == {"enabled": True}
        assert request["extensions"]["e2ee"] == {"enabled": True}
        assert lists == {
            "small": {
                "ranges": [[0, 9]],
                "required_state": [["m.room.member", "$LAZY"]],
            }
        }
        assert extensions == {"account_data": {"enabled": True}}
        await session._accept_response(window("$old"))
        await settle(session)
        request = json.loads(session._sliding.request()[2])
        assert request["extensions"]["to_device"]["since"] == "td1"
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_reopen_drops_only_connection_position_and_replays_committed_output(
    tmp_path,
):
    session = open_session(tmp_path, config=settings())
    await session._accept_response(window("$old"))
    batch = await session.next_batch()
    await session.close()
    reopened = open_session(tmp_path, config=settings())
    try:
        assert await reopened.next_batch() == batch
        assert "pos" not in parse_qs(urlsplit(reopened._sliding.request()[1]).query)
        assert (
            json.loads(reopened._sliding.request()[2])["extensions"]["to_device"][
                "since"
            ]
            == "td1"
        )
        assert reopened._sliding.baselines[ROOM]["token"] == "w1"
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_transport_switch_rejects_instead_of_reinterpreting_cursor(tmp_path):
    session = open_session(tmp_path, config=settings())
    await session._accept_response(window("$old"))
    await session.close()
    with pytest.raises(LocalProtocolError, match="transport"):
        open_session(tmp_path)


@pytest.mark.asyncio
async def test_expanded_window_keeps_older_membership_context_historical(tmp_path):
    """A wider subscription must not replay an old invite over a joined baseline."""

    async def sync(request):
        raise AssertionError("captured input must drain without polling")

    async def history(request):
        assert request.query["from"] == "w2"
        assert request.query["to"] == "w1"
        return web.json_response({"start": "w2", "chunk": []})

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        first = json.loads(window("$old", prev_batch="w2"))
        first["rooms"][ROOM]["timeline"].insert(0, member("$join", "join"))
        await session._accept_response(json.dumps(first).encode())
        await settle(session)
        wider = json.loads(window("$warm", pos="p2", prev_batch="w1"))
        wider["rooms"][ROOM].pop("num_live")
        wider["rooms"][ROOM]["limited"] = False
        wider["rooms"][ROOM]["timeline"] = [
            message("$ancient"),
            member("$invite", "invite"),
            member("$join", "join"),
            message("$old"),
            message("$warm"),
        ]
        session._capture_response(json.dumps(wider).encode())
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            timeline = {
                record.source["event_id"]: record
                for record in records
                if record.kind is RecordKind.TIMELINE
            }
            assert timeline["$warm"].provenance is TimelineEventProvenance.RECOVERED
            assert timeline["$warm"].membership_epoch == 0
            assert timeline["$ancient"].provenance is TimelineEventProvenance.HISTORY
            assert timeline["$invite"].membership is None
            assert session._metadata[ROOM]["membership"] == "join"
            assert not any(record.kind is RecordKind.LOSS for record in records)
        finally:
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_linked_live_profile_updates_preserve_limited_window_recovery(tmp_path):
    calls = []

    async def sync(request):
        raise AssertionError("unexpected sync")

    async def history(request):
        calls.append(dict(request.query))
        return web.json_response({"start": "w1", "end": "w2", "chunk": []})

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        await session._accept_response(window("$old", prev_batch="w1"))
        await settle(session)
        profile = member("$profile", "join")
        profile["content"]["displayname"] = "Updated"
        profile["unsigned"] = {
            "prev_content": {"membership": "join"},
            "replaces_state": "$join",
        }
        avatar = member("$avatar", "join")
        avatar["content"]["avatar_url"] = "mxc://example.org/updated"
        avatar["unsigned"] = {
            "prev_content": {"membership": "join"},
            "replaces_state": "$profile",
        }
        raw = json.loads(window(pos="p2", initial=False, prev_batch="w2", state=[]))
        raw["rooms"][ROOM].update(
            limited=True,
            num_live=3,
            timeline=[profile, avatar, message("$fresh")],
        )
        session._capture_response(json.dumps(raw).encode())
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            fresh = next(
                record
                for record in records
                if record.source.get("event_id") == "$fresh"
            )
            assert calls == [{"dir": "f", "from": "w1", "limit": "100", "to": "w2"}]
            assert fresh.provenance is TimelineEventProvenance.LIVE
            assert not any(record.kind is RecordKind.LOSS for record in records)
        finally:
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    ("evidence", "observed", "current"),
    [
        ("top-invite", True, "invite"),
        ("own-join", True, "join"),
        ("stripped-invite", True, "invite"),
        ("none", False, "leave"),
    ],
)
async def test_sliding_membership_boundary_reconciles_only_explicit_membership(
    tmp_path, evidence, observed, current
):
    from .membership_test import OPERATION

    async def membership_request(request):
        return web.json_response({})

    async with homeserver(None, membership=membership_request) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        try:
            await session._accept_response(window("$old"))
            await settle(session)
            assert await session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=0,
                current_membership="leave",
            )
            await session.ack(await session.next_batch())
            raw = json.loads(window(pos="p2", initial=False, state=[]))
            room = raw["rooms"][ROOM]
            room.pop("membership")
            if evidence == "top-invite":
                room["membership"] = "invite"
            elif evidence == "own-join":
                room["required_state"] = [member("$current-join", "join")]
            elif evidence == "stripped-invite":
                room["stripped_state"] = [member("$current-invite", "invite")]
            raw["rooms"][ROOM]["timeline"] = []
            await session._accept_response(json.dumps(raw).encode())
            await settle(session)
            assert bool(session._read_local_intent().get("observed")) is observed
            assert session._metadata[ROOM]["membership"] == current
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_sliding_rejoin_boundary_requires_new_initial_authorization_state(
    tmp_path,
):
    from .membership_test import OPERATION

    async def membership_request(request):
        return web.json_response({})

    async with homeserver(None, membership=membership_request) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        try:
            await session._accept_response(window("$old"))
            await settle(session)
            assert await session.change_membership(
                operation_id=OPERATION,
                room_id=ROOM,
                previous_membership="join",
                previous_epoch=0,
                current_membership="leave",
            )
            await session.ack(await session.next_batch())
            rejoined = json.loads(
                window("$uncertain", pos="p2", initial=False, state=[])
            )
            rejoined["rooms"][ROOM]["required_state"] = [member("$rejoin", "join")]
            await session._accept_response(json.dumps(rejoined).encode())
            await settle(session)
            assert session._sliding.pos is None
            assert ROOM not in session._sliding.baselines
            assert not session._metadata[ROOM]["baseline"]

            partial = json.loads(window("$partial", pos="p3", initial=False, state=[]))
            partial["rooms"][ROOM]["limited"] = True
            await session._accept_response(json.dumps(partial).encode())
            records = await settle(session)
            record = next(
                record
                for record in records
                if record.source.get("event_id") == "$partial"
            )
            assert record.provenance is TimelineEventProvenance.HISTORY
            assert not session._metadata[ROOM]["baseline"]

            fresh = window(
                "$initial-history",
                pos="p4",
                initial=True,
                state=[member("$fresh-join", "join")],
            )
            await session._accept_response(fresh)
            records = await settle(session)
            initial = next(
                record
                for record in records
                if record.source.get("event_id") == "$initial-history"
            )
            assert initial.provenance is TimelineEventProvenance.HISTORY
            assert session._metadata[ROOM]["baseline"]
        finally:
            await session.close()
            await nio_client.close()


@pytest.mark.asyncio
async def test_restart_recovers_downtime_without_reapplying_previous_window(tmp_path):
    calls = []

    async def sync(request):
        raise AssertionError("captured input must drain without polling")

    async def history(request):
        calls.append(dict(request.query))
        assert request.query["from"] == "w1"
        assert request.query["to"] == "w2"
        return web.json_response(
            {"start": "w1", "end": "w2", "chunk": [message("$old"), message("$missed")]}
        )

    async with homeserver(sync, membership=history) as (url, _):
        first = open_session(tmp_path, config=settings())
        await first._accept_response(window("$old"))
        assert [
            r.provenance for r in await settle(first) if r.kind is RecordKind.TIMELINE
        ] == [TimelineEventProvenance.HISTORY]
        await first.close()
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        session._capture_response(window("$tail", pos="p2", prev_batch="w2"))
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            timeline = [r for r in records if r.kind is RecordKind.TIMELINE]
            assert [(r.source["event_id"], r.provenance) for r in timeline] == [
                ("$missed", TimelineEventProvenance.RECOVERED),
                ("$tail", TimelineEventProvenance.RECOVERED),
            ]
            assert not any(r.kind is RecordKind.LOSS for r in records)
            assert len(calls) == 1
            assert session.cursor == "p2"
        finally:
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_unknown_room_account_data_survives_restart_by_wire_type(tmp_path):
    session = open_session(tmp_path, config=settings())
    await session._accept_response(
        json.dumps(
            {
                "pos": "p1",
                "extensions": {
                    "account_data": {
                        "rooms": {
                            ROOM: [
                                {
                                    "type": "m.tag",
                                    "content": {
                                        "tags": {"m.favourite": {"order": 0.2}}
                                    },
                                },
                                {"type": "custom.one", "content": {"value": 1}},
                                {"type": "custom.two", "content": {"value": 2}},
                            ]
                        }
                    }
                },
            }
        ).encode()
    )
    await settle(session)
    await session.close()
    reopened = open_session(tmp_path, config=settings())
    try:
        await reopened._accept_response(window())
        records = await settle(reopened)
        assert reopened.client.rooms[ROOM].tags == {"m.favourite": {"order": 0.2}}
        account_data = [r for r in records if r.kind is RecordKind.ROOM_ACCOUNT_DATA]
        assert len(account_data) == 3
        assert {r.source.get("type") for r in account_data} == {
            "m.tag",
            "custom.one",
            "custom.two",
        }
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_membership_deletion_revokes_authorization_and_persists_member_removal(
    tmp_path,
):
    session = open_session(tmp_path, config=settings())
    await session._accept_response(window())
    await settle(session)
    deletion = {"type": "m.room.member", "state_key": USER}
    await session._accept_response(window(pos="p2", initial=False, state=[deletion]))
    records = await settle(session)
    assert any(r.membership and r.membership.current != "join" for r in records)
    await session.close()
    reopened = open_session(tmp_path, config=settings())
    try:
        assert reopened._metadata[ROOM]["membership"] != "join"
        assert (
            ROOM not in reopened.client.rooms
            or USER not in reopened.client.rooms[ROOM].users
        )
    finally:
        await reopened.close()


@pytest.mark.asyncio
async def test_snapshot_member_deletion_persists_exact_stub(tmp_path):
    from nio.durable.codec import restore_event

    session = open_session(tmp_path, config=settings())
    try:
        await session._accept_response(
            window(
                state=[
                    member("$join", "join"),
                    {"type": "m.room.name", "state_key": ""},
                ]
            )
        )
        records = await settle(session)
        stub = next(r for r in records if r.source.get("type") == "m.room.name")
        assert stub.source == {"type": "m.room.name", "state_key": ""}
        assert restore_event(stub).type == "m.room.name"
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_expired_position_preserves_device_delivery_cursor(tmp_path):
    requests = []
    hold = asyncio.Event()

    async def sync(request):
        requests.append((dict(request.query), await request.json()))
        if len(requests) == 1:
            return web.Response(body=window("$old"))
        if len(requests) == 2:
            return web.json_response(
                {"errcode": "M_UNKNOWN_POS", "error": "payload-secret"}, status=400
            )
        if len(requests) == 3:
            return web.Response(body=window("$old", "$new", pos="p2"))
        await hold.wait()
        return web.Response(body=b"{}")

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        runner = asyncio.create_task(session.run())
        try:
            await drain_sync(session)
            records = await drain_sync(session)
            assert requests[1][0]["pos"] == "p1"
            assert "pos" not in requests[2][0]
            assert requests[2][1]["extensions"]["to_device"]["since"] == "td1"
            assert [
                (r.source["event_id"], r.provenance)
                for r in records
                if r.kind is RecordKind.TIMELINE
            ] == [("$new", TimelineEventProvenance.RECOVERED)]
        finally:
            hold.set()
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "state",
    [
        [],
        [{"type": "m.room.member", "state_key": USER}],
        [member("$other-tenure", "join")],
    ],
)
async def test_unproven_initial_snapshot_emits_loss_and_keeps_timeline_history(
    tmp_path, state
):
    session = open_session(tmp_path, config=settings())
    try:
        await session._accept_response(window("$old"))
        await settle(session)
        await session._accept_response(
            window("$new", pos="p2", prev_batch="w2", state=state)
        )
        records = await settle(session)
        assert any(r.kind is RecordKind.LOSS for r in records)
        assert [r.provenance for r in records if r.kind is RecordKind.TIMELINE] == [
            TimelineEventProvenance.HISTORY
        ]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_subscription_replacement_retains_accepted_input_and_removes_sticky_rooms(
    tmp_path,
):
    session = open_session(
        tmp_path,
        config=settings(
            room_subscriptions={"!old:example.org": {"timeline_limit": 10}}
        ),
    )
    try:
        session._capture_response(window("$accepted"))
        body = session._store.input[0]
        subscriptions = {ROOM: {"timeline_limit": 20}}
        await session.update_sliding_subscriptions(subscriptions)
        subscriptions[ROOM]["timeline_limit"] = 99
        assert session._store.input[0] == body
        session._prepare_pending()
        records = await settle(session)
        assert any(r.source.get("event_id") == "$accepted" for r in records)
        request = json.loads(session._sliding.request()[2])
        assert set(request["room_subscriptions"]) == {ROOM}
        assert request["room_subscriptions"][ROOM]["timeline_limit"] == 20
        assert "pos" not in parse_qs(urlsplit(session._sliding.request()[1]).query)
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_malformed_transient_extension_does_not_poison_durable_messages(
    tmp_path, caplog
):
    session = open_session(tmp_path, config=settings())
    try:
        raw = json.loads(window("$durable"))
        raw["extensions"]["typing"] = {"rooms": "payload-secret"}
        await session._accept_response(json.dumps(raw).encode())
        records = await settle(session)
        assert any(r.source.get("event_id") == "$durable" for r in records)
        assert "malformed=1" in caplog.text
        assert "payload-secret" not in caplog.text
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_sliding_overlap_does_not_bypass_the_bounded_history_walk(tmp_path):
    calls = []

    async def sync(request):
        raise AssertionError("unexpected poll")

    async def history(request):
        start = request.query["from"]
        calls.append(start)
        page = {"start": start, "chunk": [message("$tail")] if start == "w1" else []}
        if start == "w1":
            page["end"] = "more"
        return web.json_response(page)

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        await session._accept_response(window("$old"))
        await settle(session)
        session._capture_response(window("$tail", pos="p2", prev_batch="w2"))
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        try:
            records = await drain_sync(session)
            await runner
            assert calls == ["w1", "more"]
            assert [
                (r.source["event_id"], r.provenance)
                for r in records
                if r.kind is RecordKind.TIMELINE
            ] == [("$tail", TimelineEventProvenance.RECOVERED)]
        finally:
            await session.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_gap_restart_replays_committed_batch_then_resumes_next_page(tmp_path):
    calls = []

    async def sync(request):
        raise AssertionError("unexpected poll")

    async def history(request):
        start = request.query["from"]
        calls.append(start)
        return web.json_response(
            {
                "start": start,
                "end": "more" if start == "w1" else "w2",
                "chunk": [message("$one" if start == "w1" else "$two")],
            }
        )

    async with homeserver(sync, membership=history) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        session = open_session(tmp_path, nio_client, settings())
        await session._accept_response(window("$old"))
        await settle(session)
        session._capture_response(window("$tail", pos="p2", prev_batch="w2"))
        session._quiescing = True
        runner = asyncio.create_task(session.run())
        async with asyncio.timeout(5):
            await session.wait_for_work()
        batch = await session.next_batch()
        assert batch.records[0].source["event_id"] == "$one"
        assert calls == ["w1"]
        await session.close()
        await nio_client.close()
        await asyncio.gather(runner, return_exceptions=True)
        nio_client = client()
        nio_client.homeserver = url
        reopened = open_session(tmp_path, nio_client, settings())
        assert await reopened.next_batch() == batch
        reopened._quiescing = True
        runner = asyncio.create_task(reopened.run())
        try:
            records = await drain_sync(reopened)
            await runner
            assert calls == ["w1", "more"]
            assert [
                (r.source["event_id"], r.provenance)
                for r in records
                if r.kind is RecordKind.TIMELINE
            ] == [
                ("$one", TimelineEventProvenance.RECOVERED),
                ("$two", TimelineEventProvenance.RECOVERED),
                ("$tail", TimelineEventProvenance.RECOVERED),
            ]
            assert reopened._sliding.pos is None
        finally:
            await reopened.close()
            await nio_client.close()
            await asyncio.gather(runner, return_exceptions=True)


@pytest.mark.asyncio
async def test_lost_window_boundary_cannot_reuse_a_pre_snapshot_recovery_token(
    tmp_path,
):
    session = open_session(tmp_path, config=settings())
    try:
        await session._accept_response(window("$old"))
        await settle(session)
        missing = json.loads(window("$unknown", pos="p2"))
        missing["rooms"][ROOM].pop("prev_batch")
        await session._accept_response(json.dumps(missing).encode())
        assert any(r.kind is RecordKind.LOSS for r in await settle(session))
        await session._accept_response(window("$next", pos="p3", prev_batch="w3"))
        assert session._store.input[1]["phase"] == "prepared"
        records = await settle(session)
        assert any(r.kind is RecordKind.LOSS for r in records)
        assert [r.provenance for r in records if r.kind is RecordKind.TIMELINE] == [
            TimelineEventProvenance.HISTORY
        ]
    finally:
        await session.close()


@pytest.mark.asyncio
async def test_incremental_hero_profile_survives_restart(tmp_path):
    session = open_session(tmp_path, config=settings())
    await session._accept_response(window())
    await settle(session)
    raw = json.loads(window(pos="p2", initial=False, state=[]))
    raw["rooms"][ROOM].update(
        heroes=[{"user_id": "@bob:example.org", "displayname": "Bob"}],
        joined_count=2,
        invited_count=0,
    )
    await session._accept_response(json.dumps(raw).encode())
    await settle(session)
    assert session.client.rooms[ROOM].users["@bob:example.org"].display_name == "Bob"
    await session.close()
    reopened = open_session(tmp_path, config=settings())
    try:
        assert (
            reopened.client.rooms[ROOM].users["@bob:example.org"].display_name == "Bob"
        )
    finally:
        await reopened.close()
