"""Transient HTTP errors must retain the exact operation being retried."""

import asyncio

import pytest
from aiohttp import ClientConnectionError, ClientPayloadError, web

from nio.durable.transport import ConnectionRetriesExhausted, Transport
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


@pytest.mark.asyncio
@pytest.mark.parametrize(
    "error_type", [ClientConnectionError, ClientPayloadError, TimeoutError]
)
async def test_connection_exhaustion_is_typed_and_preserves_cause(
    monkeypatch, error_type
):
    """Supervisors can distinguish exhausted transient failures from invalid state."""
    failure = error_type("offline")
    requests = []

    class OfflineClient:
        access_token = "test-token"

        async def send(self, method, path, body, **kwargs):
            requests.append((method, path, body))
            raise failure

    async def no_delay(_delay):
        pass

    monkeypatch.setattr("nio.durable.transport.asyncio.sleep", no_delay)
    with pytest.raises(ConnectionRetriesExhausted) as caught:
        await Transport(OfflineClient(), 1024).request(
            "PUT", "/sync", '{"exact":"body"}'
        )

    assert isinstance(caught.value, LocalProtocolError)
    assert str(caught.value) == "durable HTTP connection retries exhausted"
    assert caught.value.__cause__ is failure
    assert requests == [("PUT", "/sync", '{"exact":"body"}')] * 5
