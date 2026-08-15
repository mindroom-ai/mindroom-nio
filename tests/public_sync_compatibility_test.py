"""Retained public sync contract at the exact pre-fork boundary."""

from inspect import Parameter, signature
import json
import re
import subprocess
import sys

import nio
import pytest
from nio.store import (
    PendingTimelineEvents,
    SlidingWindowTokens,
    SqliteMemoryStore,
    SqliteStore,
    SyncRecoveryAbandonedRooms,
    SyncRecoveryGaps,
)

_RETAINED_METHOD_PARAMETERS = {
    "add_response_callback": ("self", "func", "cb_filter"),
    "receive_response": ("self", "response"),
    "sync": (
        "self",
        "timeout",
        "sync_filter",
        "since",
        "full_state",
        "set_presence",
    ),
    "run_response_callbacks": ("self", "responses"),
    "sync_forever": (
        "self",
        "timeout",
        "sync_filter",
        "since",
        "full_state",
        "loop_sleep_time",
        "first_sync_filter",
        "set_presence",
    ),
    "stop_sync_forever": ("self",),
    "close": ("self",),
    "add_event_callback": ("self", "callback", "filter"),
    "add_ephemeral_callback": ("self", "callback", "filter"),
    "add_global_account_data_callback": ("self", "callback", "filter"),
    "add_room_account_data_callback": ("self", "callback", "filter"),
    "add_to_device_callback": ("self", "callback", "filter"),
    "add_presence_callback": ("self", "callback", "filter"),
}

_FORK_ONLY_ASYNC_CLIENT_MEMBERS = (
    "sliding_sync",
    "sliding_sync_forever",
    "add_event_admission_callback",
    "acknowledge_classic_sync",
    "reset_classic_sync_state",
    "has_uncommitted_classic_sync_state",
    "clear_persisted_sync_recovery",
)


def _classic_response(token: str, *, event_id: str | None = None) -> nio.SyncResponse:
    timeline = []
    if event_id is not None:
        timeline.append(
            {
                "content": {"body": "compatibility", "msgtype": "m.text"},
                "event_id": event_id,
                "origin_server_ts": 1,
                "sender": "@bob:example.org",
                "type": "m.room.message",
            }
        )
    rooms = (
        {
            "join": {
                "!room:example.org": {
                    "account_data": {"events": []},
                    "ephemeral": {"events": []},
                    "state": {"events": []},
                    "timeline": {
                        "events": timeline,
                        "limited": False,
                        "prev_batch": "p0",
                    },
                    "unread_notifications": {},
                }
            }
        }
        if timeline
        else {}
    )
    return nio.SyncResponse.from_dict(
        {
            "account_data": {"events": []},
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {},
            "next_batch": token,
            "presence": {"events": []},
            "rooms": rooms,
            "to_device": {"events": []},
        }
    )


def test_retained_public_sync_method_signatures_match_upstream_boundary() -> None:
    """Desktop callers keep the same method names, order, and positional shape."""
    for method_name, expected_parameters in _RETAINED_METHOD_PARAMETERS.items():
        method = getattr(nio.AsyncClient, method_name)
        parameters = tuple(signature(method).parameters.values())

        assert tuple(parameter.name for parameter in parameters) == expected_parameters
        assert all(
            parameter.kind is Parameter.POSITIONAL_OR_KEYWORD
            for parameter in parameters
        )


def test_fork_only_public_sync_surface_is_absent() -> None:
    """Durable Sliding stays private under nio.ingest, not on desktop AsyncClient."""
    assert (
        tuple(
            name
            for name in _FORK_ONLY_ASYNC_CLIENT_MEMBERS
            if hasattr(nio.AsyncClient, name)
        )
        == ()
    )
    assert not hasattr(nio, "SlidingSyncResponse")
    assert not hasattr(nio, "SlidingSyncError")
    assert not hasattr(nio, "RecoveryAbandonment")
    assert not hasattr(nio, "SlidingWindowToken")


def test_classic_response_has_no_recovery_outcome_fields() -> None:
    """Classic responses retain the pre-fork public payload contract."""
    response = _classic_response("s1")

    assert not hasattr(response, "recovered_room_ids")
    assert not hasattr(response, "unrecovered_room_ids")
    assert not hasattr(response, "abandoned_rooms")


@pytest.mark.asyncio
async def test_classic_event_callback_can_reenter_receive_response() -> None:
    """A desktop callback can synchronously apply another Classic response."""
    client = nio.AsyncClient(
        "https://example.org",
        "@alice:example.org",
        "DEVICE",
        config=nio.AsyncClientConfig(backfill_limited_timelines=True),
    )
    callback_events: list[str] = []
    nested = _classic_response("s2")

    async def callback(room: nio.MatrixRoom, event: nio.RoomMessageText) -> None:
        assert room is client.rooms["!room:example.org"]
        callback_events.append(event.event_id)
        await client.receive_response(nested)
        callback_events.append(client.next_batch)

    client.add_event_callback(callback, nio.RoomMessageText)
    try:
        await client.receive_response(_classic_response("s1", event_id="$event"))
    finally:
        await client.close()

    assert callback_events == ["$event", "s2"]
    assert client.next_batch == "s2"


@pytest.mark.asyncio
async def test_classic_callback_families_keep_upstream_order_and_projection() -> None:
    """Classic state is applied before callbacks in the retained family order."""
    room_id = "!room:example.org"
    bob = "@bob:example.org"
    response = nio.SyncResponse.from_dict(
        {
            "account_data": {
                "events": [{"content": {"value": 1}, "type": "org.example.global"}]
            },
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {},
            "next_batch": "s1",
            "presence": {
                "events": [
                    {
                        "content": {
                            "currently_active": True,
                            "last_active_ago": 7,
                            "presence": "online",
                            "status_msg": "ready",
                        },
                        "sender": bob,
                        "type": "m.presence",
                    }
                ]
            },
            "rooms": {
                "join": {
                    room_id: {
                        "account_data": {
                            "events": [
                                {
                                    "content": {"tags": {"u.compat": {"order": 1}}},
                                    "type": "m.tag",
                                }
                            ]
                        },
                        "ephemeral": {
                            "events": [
                                {
                                    "content": {"user_ids": [bob]},
                                    "type": "m.typing",
                                }
                            ]
                        },
                        "state": {
                            "events": [
                                {
                                    "content": {"membership": "join"},
                                    "event_id": "$member",
                                    "origin_server_ts": 1,
                                    "sender": bob,
                                    "state_key": bob,
                                    "type": "m.room.member",
                                }
                            ]
                        },
                        "timeline": {
                            "events": [
                                {
                                    "content": {"body": "hello", "msgtype": "m.text"},
                                    "event_id": "$message",
                                    "origin_server_ts": 2,
                                    "sender": bob,
                                    "type": "m.room.message",
                                }
                            ],
                            "limited": False,
                            "prev_batch": "p0",
                        },
                        "unread_notifications": {},
                    }
                }
            },
            "to_device": {
                "events": [
                    {
                        "content": {"value": 1},
                        "sender": bob,
                        "type": "org.example.to_device",
                    }
                ]
            },
        }
    )
    client = nio.AsyncClient(
        "https://example.org",
        "@alice:example.org",
        "DEVICE",
        config=nio.AsyncClientConfig(encryption_enabled=False),
    )
    observed: list[str] = []

    async def on_to_device(_event) -> None:
        observed.append("to_device")

    async def on_event(room, _event) -> None:
        assert room is client.rooms[room_id]
        observed.append("event")

    async def on_ephemeral(room, _event) -> None:
        assert room.typing_users == [bob]
        observed.append("ephemeral")

    async def on_room_account_data(room, _event) -> None:
        assert room.tags == {"u.compat": {"order": 1}}
        observed.append("room_account_data")

    async def on_presence(event) -> None:
        user = client.rooms[room_id].users[event.user_id]
        assert (user.presence, user.status_msg) == ("online", "ready")
        observed.append("presence")

    async def on_global(_event) -> None:
        observed.append("global_account_data")

    client.add_to_device_callback(on_to_device, None)
    client.add_event_callback(on_event, None)
    client.add_ephemeral_callback(on_ephemeral, None)
    client.add_room_account_data_callback(on_room_account_data, None)
    client.add_presence_callback(on_presence, None)
    client.add_global_account_data_callback(on_global, None)

    await client.receive_response(response)

    assert observed == [
        "to_device",
        "event",
        "ephemeral",
        "room_account_data",
        "presence",
        "global_account_data",
    ]


@pytest.mark.asyncio
async def test_response_callbacks_keep_registration_order_during_reentry() -> None:
    """A callback appended during dispatch runs after the existing callbacks."""
    client = nio.AsyncClient("https://example.org")
    response = _classic_response("s1")
    observed: list[str] = []

    async def late(_response) -> None:
        observed.append("late")

    async def first(_response) -> None:
        observed.append("first")
        client.add_response_callback(late, nio.SyncResponse)

    async def second(_response) -> None:
        observed.append("second")

    client.add_response_callback(first, nio.SyncResponse)
    client.add_response_callback(second, nio.SyncResponse)

    await client.run_response_callbacks([response])

    assert observed == ["first", "second", "late"]


@pytest.mark.asyncio
async def test_response_callback_exception_stops_later_callbacks() -> None:
    """The first callback failure propagates without running later callbacks."""
    client = nio.AsyncClient("https://example.org")
    response = _classic_response("s1")
    observed: list[str] = []

    async def first(_response) -> None:
        observed.append("first")

    async def fail(_response) -> None:
        observed.append("fail")
        raise RuntimeError("callback failed")

    async def later(_response) -> None:
        observed.append("later")

    client.add_response_callback(first, nio.SyncResponse)
    client.add_response_callback(fail, nio.SyncResponse)
    client.add_response_callback(later, nio.SyncResponse)

    with pytest.raises(RuntimeError, match="callback failed"):
        await client.run_response_callbacks([response])

    assert observed == ["first", "fail"]


@pytest.mark.asyncio
async def test_classic_sync_ignores_retired_recovery_configuration(aioresponse) -> None:
    """Fork-only recovery ownership cannot block an ordinary desktop sync."""
    client = nio.AsyncClient(
        "https://example.org",
        "@alice:example.org",
        "DEVICE",
        config=nio.AsyncClientConfig(
            backfill_limited_timelines=True,
            backfill_persist_recovery=False,
            store_sync_tokens=True,
        ),
    )
    client.access_token = "token"
    aioresponse.get(
        re.compile(r"^https://example\.org/_matrix/client/v3/sync\?since=s0$"),
        status=200,
        payload={
            "account_data": {"events": []},
            "device_lists": {"changed": [], "left": []},
            "device_one_time_keys_count": {},
            "next_batch": "s1",
            "presence": {"events": []},
            "rooms": {},
            "to_device": {"events": []},
        },
    )
    try:
        response = await client.sync(since="s0")
    finally:
        await client.close()

    assert type(response) is nio.SyncResponse
    assert response.next_batch == "s1"
    assert client.next_batch == "s1"


def test_ordinary_store_open_does_not_read_retired_recovery_state(monkeypatch) -> None:
    """Opening the desktop store loads active crypto/token state only."""

    def forbid_recovery_read(_store) -> None:
        raise AssertionError("ordinary store open read retired recovery state")

    def forbid_sliding_read(_store) -> None:
        raise AssertionError("ordinary store open read retired Sliding state")

    monkeypatch.setattr(SqliteMemoryStore, "load_sync_recovery", forbid_recovery_read)
    monkeypatch.setattr(
        SqliteMemoryStore,
        "load_sliding_window_tokens",
        forbid_sliding_read,
    )

    client = nio.AsyncClient(
        "https://example.org",
        "@alice:example.org",
        "DEVICE",
        config=nio.AsyncClientConfig(
            backfill_limited_timelines=True,
            store=SqliteMemoryStore,
            store_sync_tokens=True,
        ),
    )
    client.user_id = "@alice:example.org"

    client.load_store()

    assert type(client.store) is SqliteMemoryStore
    assert client.olm is not None


def test_fresh_desktop_store_creates_only_active_tables() -> None:
    """Historical recovery tables are retained on old stores, not created anew."""
    store = SqliteMemoryStore("@alice:example.org", "DEVICE")

    assert set(store.database.get_tables()).isdisjoint(
        {
            "pendingtimelineevents",
            "slidingwindowtokens",
            "syncrecoveryabandonedrooms",
            "syncrecoverygaps",
        }
    )


def test_existing_desktop_store_retains_inert_recovery_tables(tmp_path) -> None:
    """Opening an old higher-version store is nondestructive."""
    historical_models = (
        PendingTimelineEvents,
        SlidingWindowTokens,
        SyncRecoveryAbandonedRooms,
        SyncRecoveryGaps,
    )
    first = SqliteStore(
        "@alice:example.org",
        "DEVICE",
        str(tmp_path),
        database_name="matrix.db",
    )
    with first.database.bind_ctx(historical_models):
        first.database.create_tables(historical_models)
    first.database.close()

    reopened = SqliteStore(
        "@alice:example.org",
        "DEVICE",
        str(tmp_path),
        database_name="matrix.db",
    )

    assert {model._meta.table_name for model in historical_models} <= set(
        reopened.database.get_tables()
    )
    assert reopened.load_sync_token() is None
    reopened.database.close()


def test_desktop_import_and_client_construction_do_not_execute_recovery_modules() -> (
    None
):
    """The retained desktop entrypoint does not initialize fork recovery code."""
    program = r"""
import asyncio
import coverage
import json

targets = (
    "/client/sliding_membership.py",
    "/client/sync_recovery.py",
    "/client/sync_reset_fence.py",
    "/client/sync_response_ordering.py",
    "/recovery_abandonment.py",
    "/sliding_sync_tokens.py",
)
run = coverage.Coverage(data_file=None, source=["nio"])
run.start()
import nio
from nio.store import SqliteMemoryStore

client = nio.AsyncClient(
    "https://example.org",
    "@alice:example.org",
    "DEVICE",
    config=nio.AsyncClientConfig(encryption_enabled=False),
)
response = nio.SyncResponse.from_dict({"next_batch": "s1", "rooms": {}})
asyncio.run(client.receive_response(response))
asyncio.run(client.run_response_callbacks([response]))
SqliteMemoryStore("@alice:example.org", "DEVICE")
run.stop()
data = run.get_data()
executed = {
    target: sorted(
        line
        for filename in data.measured_files()
        if filename.endswith(target)
        for line in (data.lines(filename) or ())
    )
    for target in targets
}
print(json.dumps(executed, sort_keys=True))
"""
    result = subprocess.run(
        [sys.executable, "-c", program],
        check=True,
        capture_output=True,
        text=True,
    )

    assert json.loads(result.stdout) == {
        "/client/sliding_membership.py": [],
        "/client/sync_recovery.py": [],
        "/client/sync_reset_fence.py": [],
        "/client/sync_response_ordering.py": [],
        "/recovery_abandonment.py": [],
        "/sliding_sync_tokens.py": [],
    }
