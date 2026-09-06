"""Retry incomplete HTTP response bodies at the transport boundary."""

import asyncio
from contextlib import asynccontextmanager

import pytest

from nio.durable.transport import Transport

from .client_test import client


@asynccontextmanager
async def truncated_then_complete_server():
    attempts = 0

    async def handle(reader, writer):
        nonlocal attempts
        await reader.readuntil(b"\r\n\r\n")
        attempts += 1
        if attempts == 1:
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                b"Content-Length: 32\r\n"
                b"Connection: close\r\n"
                b"\r\n"
                b'{"incomplete":'
            )
        else:
            body = b'{"ok": true}'
            writer.write(
                b"HTTP/1.1 200 OK\r\n"
                + f"Content-Length: {len(body)}\r\n".encode()
                + b"\r\n"
                + body
            )
        await writer.drain()
        if attempts == 1:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, "127.0.0.1", 0)
    try:
        port = server.sockets[0].getsockname()[1]
        yield f"http://127.0.0.1:{port}", lambda: attempts
    finally:
        server.close()
        await server.wait_closed()


@pytest.mark.asyncio
async def test_truncated_response_body_is_retried_and_released():
    async with truncated_then_complete_server() as (url, attempt_count):
        nio_client = client()
        nio_client.homeserver = url
        try:
            result = await Transport(nio_client, 1024).request("GET", "/sync")

            assert result == b'{"ok": true}'
            assert attempt_count() == 2
            assert nio_client.client_session is not None
            assert not nio_client.client_session.connector._acquired
        finally:
            await nio_client.close()
