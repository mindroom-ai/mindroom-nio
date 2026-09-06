"""Adoption must leave released recovery obligations usable by the old client."""

import sqlite3
from uuid import UUID

import pytest

from nio.crypto import DeviceStore, OlmAccount, OlmDevice, TrustState
from nio.durable.store import DurableStore
from nio.exceptions import LocalProtocolError
from nio.store import DefaultStore, SqliteStore

USER = "@alice:example.org"
DEVICE = "ALICE"
ROOM = "!room:example.org"
PICKLE_KEY = "legacy-secret"
CONSUMER = UUID("dbe7b57d-7121-43c5-9445-61241bf90072")

# Released recovery schema, copied from SQLite's schema catalog. Keeping this
# fixture independent of production models also covers tables removed at cutover.
LEGACY_SCHEMA = (
    'CREATE TABLE "pendingtimelineevents" ('
    '"id" INTEGER NOT NULL PRIMARY KEY, "room_id" TEXT NOT NULL, '
    '"generation" INTEGER NOT NULL, "sequence" INTEGER NOT NULL, '
    '"event_id" TEXT NOT NULL, "event_payload" BLOB NOT NULL, '
    '"is_live" INTEGER NOT NULL, "was_encrypted" INTEGER NOT NULL, '
    '"was_completed" INTEGER NOT NULL, "admission_accepted" INTEGER NOT NULL, '
    '"provenance" TEXT NOT NULL, "apply_room_state" INTEGER NOT NULL, '
    '"account_id" INTEGER NOT NULL, FOREIGN KEY ("account_id") '
    'REFERENCES "accounts" ("id") ON DELETE CASCADE, '
    "UNIQUE(account_id,room_id,event_id))",
    'CREATE TABLE "syncrecoverygaps" ('
    '"id" INTEGER NOT NULL PRIMARY KEY, "room_id" TEXT NOT NULL, '
    '"generation" INTEGER NOT NULL, "target_token" TEXT NOT NULL, '
    '"cursor_token" TEXT, "membership_bound" INTEGER NOT NULL, '
    '"account_id" INTEGER NOT NULL, FOREIGN KEY ("account_id") '
    'REFERENCES "accounts" ("id") ON DELETE CASCADE, '
    "UNIQUE(account_id,room_id,generation))",
    'CREATE TABLE "syncrecoveryabandonedrooms" ('
    '"id" INTEGER NOT NULL PRIMARY KEY, "room_id" TEXT NOT NULL, '
    '"reason" TEXT NOT NULL, "account_id" INTEGER NOT NULL, '
    'FOREIGN KEY ("account_id") REFERENCES "accounts" ("id") ON DELETE CASCADE, '
    "UNIQUE(account_id,room_id,reason))",
    'CREATE TABLE "slidingwindowtokens" ('
    '"id" INTEGER NOT NULL PRIMARY KEY, "room_id" TEXT NOT NULL, '
    '"token" TEXT NOT NULL, "membership_event_id" TEXT NOT NULL, '
    '"account_id" INTEGER NOT NULL, FOREIGN KEY ("account_id") '
    'REFERENCES "accounts" ("id") ON DELETE CASCADE, '
    "UNIQUE(account_id,room_id))",
)

# A real released-store encrypted message for account 1, ROOM, generation 1,
# event $pending, and PICKLE_KEY. The old release can still decrypt this row.
PENDING_PAYLOAD = bytes.fromhex(
    "01207de1d3c666d8f4eacee24e67ac17dbacef0b60fdfbc7febe2fccfb41ea311d"
    "9da6a18e640e4c9a22a32b4af497aeb6965f6b1388df39b274793de9f099a3373"
    "25ea45cbc8486862edd42e6135c7b9b120a2d277bd6ed8f5ffa99e77987dc15db"
    "ccd7d26d343f73d28d9da1e0dc625bd3a0d2f09229ee6da54e35b399b3b769a38"
    "62e3e00a904e3949f67e649350f0b8fd1bec9347d39ceeac2a38f81f059baa881"
    "e1d205b5d2a55e0e991e6e525ae93a1f5a6d783c01f59ee610d15f7a728424a4"
    "c856959f830ad66a931e3580aae335929bbf55efe3a680b5999ce87ad16735d2c9"
    "ed82f2c2a7454443daa81db6"
)


def make_legacy_store(path, store_class, state):
    ordinary = store_class(USER, DEVICE, str(path), pickle_key=PICKLE_KEY)
    account = OlmAccount()
    ordinary.save_account(account)
    ordinary.save_sync_token("after-pending")
    devices = DeviceStore()
    for trust in ("verified", "blacklisted", "ignored", "unset"):
        devices.add(OlmDevice("@bob:example.org", trust, OlmAccount().identity_keys))
    ordinary.save_device_keys(devices)
    ordinary.verify_device(devices["@bob:example.org"]["verified"])
    ordinary.blacklist_device(devices["@bob:example.org"]["blacklisted"])
    ordinary.ignore_device(devices["@bob:example.org"]["ignored"])
    database_path = path / f"{USER}_{DEVICE}.db"
    ordinary.database.close()

    with sqlite3.connect(database_path) as database:
        for statement in LEGACY_SCHEMA:
            database.execute(statement)
        for table in (
            "pendingtimelineevents",
            "syncrecoverygaps",
            "syncrecoveryabandonedrooms",
            "slidingwindowtokens",
        ):
            database.execute(
                f'CREATE INDEX "{table}_account_id" ON "{table}" ("account_id")'
            )
        database.execute(
            "INSERT INTO slidingwindowtokens "
            "(room_id, token, membership_event_id, account_id) VALUES (?, ?, ?, 1)",
            (ROOM, "earlier-window", "$join"),
        )
        if state in {
            "pending",
            "redispatch",
            "accepted",
            "completed",
            "completed_encrypted",
        }:
            completed = state.startswith("completed")
            database.execute(
                "INSERT INTO pendingtimelineevents "
                "(room_id, generation, sequence, event_id, event_payload, is_live, "
                "was_encrypted, was_completed, admission_accepted, provenance, "
                "apply_room_state, account_id) VALUES (?, ?, 0, ?, ?, ?, ?, ?, ?, ?, ?, 1)",
                (
                    ROOM,
                    0 if completed else 1,
                    "$pending",
                    b"" if completed else PENDING_PAYLOAD,
                    not completed,
                    state in {"completed_encrypted", "redispatch"},
                    state == "redispatch",
                    state == "accepted",
                    "history" if completed else "live",
                    not completed,
                ),
            )
        if state in {"pending", "redispatch", "accepted", "gap", "synthetic_gap"}:
            database.execute(
                "INSERT INTO syncrecoverygaps "
                "(room_id, generation, target_token, cursor_token, membership_bound, "
                "account_id) VALUES (?, 1, ?, ?, 0, 1)",
                (
                    ROOM,
                    "target" if state == "gap" else "",
                    "earlier" if state == "gap" else None,
                ),
            )
        if state in {"loss", "unknown_loss"}:
            database.execute(
                "INSERT INTO syncrecoveryabandonedrooms "
                "(room_id, reason, account_id) VALUES (?, ?, 1)",
                (ROOM, "event_limit" if state == "loss" else "unknown"),
            )
    return database_path, account, devices


def open_durable(path, store_class):
    return DurableStore(
        path,
        user_id=USER,
        device_id=DEVICE,
        consumer_id=CONSUMER,
        pickle_key=PICKLE_KEY,
        source_store_class=store_class,
    )


@pytest.mark.parametrize("store_class", [SqliteStore, DefaultStore])
@pytest.mark.parametrize(
    "state", ["pending", "redispatch", "gap", "loss", "unknown_loss"]
)
def test_rejection_preserves_released_store_and_obligations(
    tmp_path, store_class, state
):
    database_path, account, devices = make_legacy_store(tmp_path, store_class, state)
    before = database_path.read_bytes()
    sidecars = {path: path.read_bytes() for path in tmp_path.glob("*.?*devices")}

    with pytest.raises(LocalProtocolError, match="prior release"):
        adopted = open_durable(tmp_path, store_class)
        adopted.close()

    assert database_path.read_bytes() == before
    assert {path: path.read_bytes() for path in sidecars} == sidecars
    ordinary = store_class(USER, DEVICE, str(tmp_path), pickle_key=PICKLE_KEY)
    try:
        assert ordinary.load_sync_token() == "after-pending"
        assert ordinary.load_account().identity_keys == account.identity_keys
        assert ordinary.is_device_verified(devices["@bob:example.org"]["verified"])
        assert ordinary.is_device_blacklisted(
            devices["@bob:example.org"]["blacklisted"]
        )
        assert ordinary.is_device_ignored(devices["@bob:example.org"]["ignored"])
    finally:
        ordinary.database.close()


@pytest.mark.parametrize("store_class", [SqliteStore, DefaultStore])
@pytest.mark.parametrize(
    "state", ["clean", "completed", "completed_encrypted", "accepted", "synthetic_gap"]
)
def test_settled_released_store_adopts_with_token_keys_and_trust(
    tmp_path, store_class, state
):
    _, account, devices = make_legacy_store(tmp_path, store_class, state)
    store = open_durable(tmp_path, store_class)
    try:
        assert store.cursor == "after-pending"
        assert store.input is None
        assert store.next_batch() is None
        assert store.matrix.load_account().identity_keys == account.identity_keys
        loaded = store.matrix.load_device_keys()["@bob:example.org"]
        assert {name: device.keys for name, device in loaded.items()} == {
            name: device.keys for name, device in devices["@bob:example.org"].items()
        }
        assert {name: device.trust_state for name, device in loaded.items()} == {
            "verified": TrustState.verified,
            "blacklisted": TrustState.blacklisted,
            "ignored": TrustState.ignored,
            "unset": TrustState.unset,
        }
    finally:
        store.close()
