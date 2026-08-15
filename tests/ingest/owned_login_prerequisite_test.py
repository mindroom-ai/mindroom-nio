import pytest

from nio import AsyncClient, AsyncClientConfig
from nio.responses import LoginResponse
from nio.store import SqliteStore


@pytest.mark.asyncio
async def test_login_response_without_store_path_retains_credentials_storelessly(
    tmp_path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    client = AsyncClient(
        "https://example.org",
        "alice",
        config=AsyncClientConfig(store=SqliteStore),
    )

    def forbidden(*args, **kwargs):
        raise AssertionError(
            "storeless login attempted store, sync, upload, or network I/O"
        )

    monkeypatch.setattr(SqliteStore, "_create_database", forbidden)
    monkeypatch.setattr(client, "send", forbidden)
    monkeypatch.setattr(client, "sync", forbidden)
    monkeypatch.setattr(client, "keys_upload", forbidden)
    await client.receive_response(
        LoginResponse("@alice:example.org", "DEVICE", "secret-token")
    )

    assert (client.user_id, client.device_id, client.access_token) == (
        "@alice:example.org",
        "DEVICE",
        "secret-token",
    )
    assert client.store is None and client.olm is None
    assert client.client_session is None
    assert not tuple(tmp_path.iterdir())
