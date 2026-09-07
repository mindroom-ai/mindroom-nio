"""Expanded history never becomes actionable through later keys or pagination."""

import asyncio
import json

import pytest
from aiohttp import web

from nio import TimelineEventProvenance
from nio.durable.model import RecordKind

from .recovery_test import member, message
from .runner_test import homeserver
from .sliding_crash_test import BOB, ROOM, opened, seeded
from .sliding_test import settle


async def accept(session, raw):
    session._capture_response(json.dumps(raw).encode())
    session._prepare_pending()
    records = []
    async with asyncio.timeout(5):
        while session._store.input[1]["phase"] != "prepared":
            while batch := await session.next_batch():
                records.extend(batch.records)
                await session.ack(batch)
            await session._recovery.advance()
    records.extend(await settle(session))
    return records


async def older_ciphertext(tmp_path):
    await seeded(tmp_path)
    raw = json.loads((tmp_path / "sliding.json").read_text())
    ciphertext = raw["rooms"][ROOM]["timeline"][0]
    keys = raw["extensions"]["to_device"]["events"]
    raw["extensions"]["to_device"]["events"] = []
    raw["rooms"][ROOM]["timeline"] = [message("$processed")]
    return raw, ciphertext, keys


@pytest.mark.asyncio
async def test_repeated_expanded_history_ciphertext_stays_observed_after_restart(
    tmp_path,
):
    raw, ciphertext, keys = await older_ciphertext(tmp_path)
    client, session = opened(tmp_path)
    try:
        await accept(session, raw)
        raw["pos"] = "p2"
        raw["rooms"][ROOM]["timeline"].insert(0, ciphertext)
        records = await accept(session, raw)
        ancient = next(
            record
            for record in records
            if record.source.get("event_id") == ciphertext["event_id"]
        )
        assert ancient.provenance is TimelineEventProvenance.HISTORY
        assert ancient.clear is None
    finally:
        await session.close()
        await client.close()

    client, session = opened(tmp_path)
    try:
        raw["pos"] = "p3"
        raw["extensions"]["to_device"]["events"] = keys
        records = await accept(session, raw)
        # Context already observed as history is not an automatic replay obligation.
        assert not any(
            record.source.get("event_id") == ciphertext["event_id"]
            for record in records
        )
    finally:
        await session.close()
        await client.close()


@pytest.mark.asyncio
async def test_expanded_window_retains_forward_recovery_floor_after_restart(tmp_path):
    raw, ciphertext, keys = await older_ciphertext(tmp_path)
    requests = []

    async def sync(request):
        raise AssertionError("captured input must drain without polling")

    async def history(request):
        start, target = request.query["from"], request.query["to"]
        requests.append((start, target))
        if target == "w1":
            return web.json_response({"start": start, "chunk": []})
        assert target == "w3"
        # Starting behind the established floor crosses unseen prior-tenure state.
        ancient = (
            [ciphertext, member("$ancient-invite", "invite", BOB)]
            if start == "w1"
            else []
        )
        return web.json_response(
            {
                "start": start,
                "end": "w3",
                "chunk": [*ancient, message("$processed"), message("$missed")],
            }
        )

    async with homeserver(sync, membership=history) as (url, _):
        client, session = opened(tmp_path)
        client.homeserver, client.access_token = url, "test-token"
        try:
            raw["rooms"][ROOM]["prev_batch"] = "w2"
            await accept(session, raw)
            raw["pos"] = "p2"
            raw["rooms"][ROOM]["prev_batch"] = "w1"
            raw["rooms"][ROOM]["timeline"].insert(0, ciphertext)
            records = await accept(session, raw)
            ancient = next(
                record
                for record in records
                if record.source.get("event_id") == ciphertext["event_id"]
            )
            assert ancient.provenance is TimelineEventProvenance.HISTORY
            assert ancient.clear is None
        finally:
            await session.close()
            await client.close()

        client, session = opened(tmp_path)
        client.homeserver, client.access_token = url, "test-token"
        try:
            raw["pos"] = "p3"
            raw["rooms"][ROOM]["prev_batch"] = "w3"
            raw["rooms"][ROOM]["timeline"] = [message("$fresh")]
            raw["extensions"]["to_device"]["events"] = keys
            records = await accept(session, raw)
            assert requests == [("w2", "w1"), ("w2", "w3")]
            assert [
                (
                    record.source["event_id"],
                    record.provenance,
                    record.membership_epoch,
                )
                for record in records
                if record.kind is RecordKind.TIMELINE
            ] == [
                ("$missed", TimelineEventProvenance.RECOVERED, 0),
                ("$fresh", TimelineEventProvenance.RECOVERED, 0),
            ]
            assert session._metadata[ROOM]["membership"] == "join"
        finally:
            await session.close()
            await client.close()


@pytest.mark.asyncio
@pytest.mark.parametrize("boundary", [None, "w1"], ids=["missing", "history-loss"])
async def test_expanded_window_loss_does_not_retain_old_recovery_floor(
    tmp_path, boundary
):
    raw, ciphertext, _ = await older_ciphertext(tmp_path)
    requests = []

    async def sync(request):
        raise AssertionError("captured input must drain without polling")

    async def history(request):
        start, target = request.query["from"], request.query["to"]
        requests.append((start, target))
        if target == "w1":
            return web.json_response({"errcode": "M_UNKNOWN"}, status=400)
        return web.json_response({"start": start, "end": target, "chunk": []})

    async with homeserver(sync, membership=history) as (url, _):
        client, session = opened(tmp_path)
        client.homeserver, client.access_token = url, "test-token"
        try:
            raw["rooms"][ROOM]["prev_batch"] = "w2"
            await accept(session, raw)
            raw["pos"] = "p2"
            raw["rooms"][ROOM]["prev_batch"] = boundary
            raw["rooms"][ROOM]["timeline"].insert(0, ciphertext)
            records = await accept(session, raw)
            assert any(record.kind is RecordKind.LOSS for record in records)
        finally:
            await session.close()
            await client.close()

        client, session = opened(tmp_path)
        client.homeserver, client.access_token = url, "test-token"
        try:
            raw["pos"] = "p3"
            raw["rooms"][ROOM]["prev_batch"] = "w3"
            raw["rooms"][ROOM]["timeline"] = [message("$fresh")]
            await accept(session, raw)
            assert requests == (
                [] if boundary is None else [("w2", "w1"), ("w1", "w3")]
            )
        finally:
            await session.close()
            await client.close()
