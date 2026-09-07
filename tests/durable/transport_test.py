"""Transient HTTP errors must retain the exact operation being retried."""

import asyncio

import pytest
from aiohttp import web

from nio.durable.transport import Transport
from nio.exceptions import LocalProtocolError

from .client_test import client
from .runner_test import homeserver


@pytest.mark.asyncio
async def test_rate_limit_retries_identical_body_without_leaking_credentials():
    bodies = []

    async def sync(request):
        bodies.append(await request.text())
        if len(bodies) == 1:
            return web.json_response(
                {"errcode": "M_LIMIT_EXCEEDED"},
                status=429,
                headers={"Retry-After": "0"},
            )
        return web.json_response({"ok": True})

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        try:
            result = await Transport(nio_client, 1024).request(
                "PUT", "/sync?access_token=secret", '{"exact": "body"}'
            )
            assert result == b'{"ok": true}'
            assert bodies == ['{"exact": "body"}', '{"exact": "body"}']
        finally:
            await nio_client.close()


@pytest.mark.asyncio
async def test_repeated_server_failures_stop_after_bounded_retries():
    attempts = 0

    async def sync(request):
        nonlocal attempts
        attempts += 1
        return web.Response(
            status=503, text="payload-secret", headers={"Retry-After": "0"}
        )

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        try:
            with pytest.raises(LocalProtocolError) as failure:
                await Transport(nio_client, 1024).request("GET", "/sync")
            assert attempts == 5
            assert "payload-secret" not in str(failure.value)
            assert "503" in str(failure.value)
        finally:
            await nio_client.close()


@pytest.mark.asyncio
async def test_retry_delay_is_cancellable():
    received = asyncio.Event()

    async def sync(request):
        received.set()
        return web.Response(status=429, headers={"Retry-After": "30"})

    async with homeserver(sync) as (url, _):
        nio_client = client()
        nio_client.homeserver = url
        task = asyncio.create_task(Transport(nio_client, 1024).request("GET", "/sync"))
        try:
            await received.wait()
            task.cancel()
            with pytest.raises(asyncio.CancelledError):
                await task
        finally:
            await nio_client.close()
