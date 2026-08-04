import copy
import os
import sqlite3
from collections import defaultdict
from pathlib import Path

import pytest
from helpers import faker

from nio import TimelineEventProvenance
from nio.client.sync_recovery import PendingTimelineEvent, RecoveryGap
from nio.crypto import (
    InboundGroupSession,
    OlmAccount,
    OlmDevice,
    OutboundGroupSession,
    OutboundSession,
    OutgoingKeyRequest,
    TrustState,
)
from nio.exceptions import OlmTrustError
from nio.sliding_sync_tokens import SlidingWindowToken
from nio.store import (
    DefaultStore,
    Ed25519Key,
    Key,
    KeyStore,
    MatrixStore,
    SqliteMemoryStore,
    SqliteStore,
    PendingTimelineEvents,
    SlidingWindowTokens,
    SyncRecoveryGaps,
)

BOB_ID = "@bob:example.org"
BOB_DEVICE = "AGMTSWVYML"
BOB_CURVE = "T9tOKF+TShsn6mk1zisW2IBsBbTtzDNvw99RBFMJOgI"
BOB_ONETIME = "6QlQw3mGUveS735k/JDaviuoaih5eEi6S1J65iHjfgU"

TEST_ROOM = "!test:example.org"
TEST_ROOM_2 = "!test2:example.org"
TEST_FORWARDING_CHAIN = [BOB_CURVE, BOB_ONETIME]


@pytest.fixture
def matrix_store(tempdir):
    return MatrixStore("ephemeral", "DEVICEID", tempdir)


@pytest.fixture
def store(tempdir):
    store = DefaultStore("ephemeral", "DEVICEID", tempdir)
    account = OlmAccount()
    store.save_account(account)
    return store


@pytest.fixture
def sqlstore(tempdir):
    store = SqliteStore("ephemeral", "DEVICEID", tempdir)
    account = OlmAccount()
    store.save_account(account)
    return store


@pytest.fixture
def sqlmemorystore():
    store = SqliteMemoryStore("ephemeral", "DEVICEID")
    account = OlmAccount()
    store.save_account(account)
    return store


def seed_v5_recovery_state(sqlstore):
    gap = RecoveryGap(TEST_ROOM, 1, "p1", None)
    completed = PendingTimelineEvent(
        TEST_ROOM,
        1,
        0,
        "$completed",
        "{}",
        True,
        True,
    )
    pending = PendingTimelineEvent(
        TEST_ROOM,
        1,
        1,
        "$pending",
        "{}",
        True,
        False,
    )
    sqlstore.save_recovery(
        "s1",
        set(),
        [gap],
        [completed, pending],
        None,
    )
    sqlstore.finish_recovery(
        TEST_ROOM,
        gap.generation,
        completed.event_id,
        True,
    )
    sqlstore.save_sliding_window_tokens({TEST_ROOM: SlidingWindowToken("w1", "$join")})
    sqlstore._update_version(5)


class TestClass:
    @pytest.fixture(autouse=True)
    def _store_path(self, tempdir):
        self.store_path = tempdir

    @property
    def ephemeral_store(self):
        return MatrixStore("@ephemeral:example.org", "DEVICEID", self.store_path)

    def test_writable_store_uses_pytest_tmp_path(self, tmp_path):
        store = self.ephemeral_store
        assert Path(store.database_path).parent == tmp_path

    @property
    def example_devices(self):
        devices = defaultdict(dict)

        for _ in range(10):
            device = faker.olm_device()
            devices[device.user_id][device.id] = device

        bob_device = OlmDevice(
            BOB_ID, BOB_DEVICE, {"ed25519": BOB_ONETIME, "curve25519": BOB_CURVE}
        )

        devices[BOB_ID][BOB_DEVICE] = bob_device

        return devices

    def copy_store(self, old_store):
        return MatrixStore(old_store.user_id, old_store.device_id, old_store.store_path)

    def _create_ephemeral_account(self):
        store = self.ephemeral_store
        account = OlmAccount()
        store.save_account(account)
        return account

    def test_key(self):
        user_id = faker.mx_id()
        device_id = faker.device_id()
        fp_key = faker.olm_key_pair()["ed25519"]
        key = Ed25519Key(user_id, device_id, fp_key)

        assert key.to_line() == f"{user_id} {device_id} matrix-ed25519 {fp_key}\n"

        loaded_key = Key.from_line(key.to_line())
        assert isinstance(loaded_key, Ed25519Key)

        assert key.user_id == loaded_key.user_id
        assert key.device_id == loaded_key.device_id
        assert key.key == loaded_key.key
        assert key == loaded_key

    def test_key_store(self, tempdir):
        store_path = os.path.join(tempdir, "test_store")
        store = KeyStore(os.path.join(tempdir, "test_store"))
        assert repr(store) == f"KeyStore object, file: {store_path}"

        key = faker.ed25519_key()

        store.add(key)

        assert key == store.get_key(key.user_id, key.device_id)

    def test_key_store_add_invalid(self, tempdir):
        os.path.join(tempdir, "test_store")
        store = KeyStore(os.path.join(tempdir, "test_store"))

        key = faker.ed25519_key()
        store.add(key)

        fake_key = copy.copy(key)
        fake_key.key = "FAKE_KEY"

        with pytest.raises(OlmTrustError):
            store.add(fake_key)

    def test_key_store_check_invalid(self, tempdir):
        os.path.join(tempdir, "test_store")
        store = KeyStore(os.path.join(tempdir, "test_store"))

        key = faker.ed25519_key()
        store.add(key)

        fake_key = copy.copy(key)
        fake_key.key = "FAKE_KEY"

        assert fake_key not in store
        assert key in store

    def test_key_store_add_many(self, tempdir):
        os.path.join(tempdir, "test_store")
        store = KeyStore(os.path.join(tempdir, "test_store"))

        keys = [
            faker.ed25519_key(),
            faker.ed25519_key(),
            faker.ed25519_key(),
            faker.ed25519_key(),
        ]

        store.add_many(keys)

        store2 = KeyStore(os.path.join(tempdir, "test_store"))

        for key in keys:
            assert key in store2

    def test_key_store_remove_many(self, tempdir):
        os.path.join(tempdir, "test_store")
        store = KeyStore(os.path.join(tempdir, "test_store"))

        keys = [
            faker.ed25519_key(),
            faker.ed25519_key(),
            faker.ed25519_key(),
            faker.ed25519_key(),
        ]
        store.add_many(keys)

        for key in keys:
            assert key in store

        store.remove_many(keys)
        store2 = KeyStore(os.path.join(tempdir, "test_store"))

        for key in keys:
            assert key not in store2

    def test_store_opening(self):
        store = self.ephemeral_store
        account = store.load_account()
        assert not account

    def test_store_account_saving(self):
        account = self._create_ephemeral_account()

        store2 = self.ephemeral_store
        loaded_account = store2.load_account()

        assert account.identity_keys == loaded_account.identity_keys

    def test_store_session(self):
        account = self._create_ephemeral_account()
        store = self.ephemeral_store

        session = OutboundSession(account, BOB_CURVE, BOB_ONETIME)
        store.save_session(BOB_CURVE, session)

        store2 = self.ephemeral_store
        session_store = store2.load_sessions()

        loaded_session = session_store.get(BOB_CURVE)

        assert loaded_session
        assert session.id == loaded_session.id

    def test_store_group_session(self):
        account = self._create_ephemeral_account()
        store = self.ephemeral_store

        out_group = OutboundGroupSession()
        in_group = InboundGroupSession(
            out_group.session_key,
            account.identity_keys["ed25519"],
            account.identity_keys["curve25519"],
            TEST_ROOM,
            TEST_FORWARDING_CHAIN,
        )
        store.save_inbound_group_session(in_group)

        store2 = self.ephemeral_store
        session_store = store2.load_inbound_group_sessions()

        loaded_session = session_store.get(
            TEST_ROOM, account.identity_keys["curve25519"], in_group.id
        )

        assert loaded_session
        assert in_group.id == loaded_session.id
        assert sorted(loaded_session.forwarding_chain) == sorted(TEST_FORWARDING_CHAIN)

    def test_store_device_keys(self):
        self._create_ephemeral_account()
        store = self.ephemeral_store

        devices = self.example_devices
        assert len(devices) == 11

        store.save_device_keys(devices)

        store2 = self.ephemeral_store
        device_store = store2.load_device_keys()

        bob_device = device_store[BOB_ID][BOB_DEVICE]
        assert bob_device
        assert bob_device.user_id == BOB_ID
        assert bob_device.id == BOB_DEVICE
        assert bob_device.ed25519 == BOB_ONETIME
        assert bob_device.curve25519 == BOB_CURVE
        assert not bob_device.deleted
        assert len(device_store.users) == 11

    def test_two_stores(self):
        account = self._create_ephemeral_account()
        store = self.ephemeral_store
        loaded_account = store.load_account()
        assert account.identity_keys == loaded_account.identity_keys

        store2 = MatrixStore("ephemeral2", "DEVICEID2", self.store_path)
        assert not store2.load_account()

        loaded_account = store.load_account()
        assert account.identity_keys == loaded_account.identity_keys

    def test_empty_device_keys(self):
        self._create_ephemeral_account()
        store = self.ephemeral_store
        store.save_device_keys({})

    def test_saving_account_twice(self):
        account = self._create_ephemeral_account()
        store = self.ephemeral_store

        session = OutboundSession(account, BOB_CURVE, BOB_ONETIME)
        store.save_session(BOB_CURVE, session)
        store.save_account(account)

        store2 = self.ephemeral_store
        session_store = store2.load_sessions()

        loaded_session = session_store.get(BOB_CURVE)

        assert loaded_session
        assert session.id == loaded_session.id

    def test_encrypted_room_saving(self):
        self._create_ephemeral_account()
        store = self.ephemeral_store
        encrypted_rooms = store.load_encrypted_rooms()

        assert not encrypted_rooms

        store.save_encrypted_rooms([TEST_ROOM])

        store = self.ephemeral_store
        encrypted_rooms = store.load_encrypted_rooms()
        assert TEST_ROOM in encrypted_rooms

    def test_key_request_saving(self):
        self._create_ephemeral_account()
        store = self.ephemeral_store
        key_requests = store.load_outgoing_key_requests()

        assert not key_requests

        request = OutgoingKeyRequest("ABCDF", "ABCDF", TEST_ROOM, "megolm.v1")
        store.add_outgoing_key_request(request)

        store = self.ephemeral_store
        key_requests = store.load_outgoing_key_requests()
        assert "ABCDF" in key_requests.keys()
        assert request == key_requests["ABCDF"]

    def test_new_store_opening(self, matrix_store):
        account = matrix_store.load_account()
        assert not account

    def test_new_store_account_saving(self, matrix_store):
        account = OlmAccount()
        matrix_store.save_account(account)

        store2 = MatrixStore(
            matrix_store.user_id, matrix_store.device_id, matrix_store.store_path
        )
        loaded_account = store2.load_account()

        assert account.identity_keys == loaded_account.identity_keys

    def test_new_store_session(self, store):
        account = store.load_account()

        session = OutboundSession(account, BOB_CURVE, BOB_ONETIME)
        store.save_session(BOB_CURVE, session)

        store2 = self.copy_store(store)
        session_store = store2.load_sessions()

        loaded_session = session_store.get(BOB_CURVE)

        assert loaded_session
        assert session.id == loaded_session.id

    def test_new_store_group_session(self, store):
        account = store.load_account()

        out_group = OutboundGroupSession()
        in_group = InboundGroupSession(
            out_group.session_key,
            account.identity_keys["ed25519"],
            account.identity_keys["curve25519"],
            TEST_ROOM,
            TEST_FORWARDING_CHAIN,
        )
        store.save_inbound_group_session(in_group)

        store2 = self.copy_store(store)
        session_store = store2.load_inbound_group_sessions()

        loaded_session = session_store.get(
            TEST_ROOM, account.identity_keys["curve25519"], in_group.id
        )

        assert loaded_session
        assert in_group.id == loaded_session.id
        assert sorted(loaded_session.forwarding_chain) == sorted(TEST_FORWARDING_CHAIN)

    def test_new_store_device_keys(self, store):
        store.load_account()

        devices = self.example_devices
        assert len(devices) == 11

        store.save_device_keys(devices)

        store2 = self.copy_store(store)
        device_store = store2.load_device_keys()

        # pdb.set_trace()

        bob_device = device_store[BOB_ID][BOB_DEVICE]
        assert bob_device
        assert bob_device.user_id == BOB_ID
        assert bob_device.id == BOB_DEVICE
        assert bob_device.ed25519 == BOB_ONETIME
        assert bob_device.curve25519 == BOB_CURVE
        assert not bob_device.deleted
        assert len(device_store.users) == 11

    def test_new_saving_account_twice(self, store):
        account = store.load_account()

        session = OutboundSession(account, BOB_CURVE, BOB_ONETIME)
        store.save_session(BOB_CURVE, session)
        store.save_account(account)

        store2 = self.copy_store(store)
        session_store = store2.load_sessions()

        loaded_session = session_store.get(BOB_CURVE)

        assert loaded_session
        assert session.id == loaded_session.id

    def test_new_encrypted_room_saving(self, store):
        encrypted_rooms = store.load_encrypted_rooms()

        assert not encrypted_rooms

        store.save_encrypted_rooms([TEST_ROOM])

        store2 = self.copy_store(store)
        encrypted_rooms = store2.load_encrypted_rooms()
        assert TEST_ROOM in encrypted_rooms

    def test_new_encrypted_room_delete(self, store):
        encrypted_rooms = store.load_encrypted_rooms()

        assert not encrypted_rooms

        store.save_encrypted_rooms([TEST_ROOM, TEST_ROOM_2])

        store2 = self.copy_store(store)
        encrypted_rooms = store2.load_encrypted_rooms()
        assert TEST_ROOM in encrypted_rooms
        assert TEST_ROOM_2 in encrypted_rooms

        store.delete_encrypted_room(TEST_ROOM_2)
        store3 = self.copy_store(store2)
        encrypted_rooms = store3.load_encrypted_rooms()
        assert TEST_ROOM in encrypted_rooms
        assert TEST_ROOM_2 not in encrypted_rooms

    def test_new_key_request_saving(self, store):
        key_requests = store.load_outgoing_key_requests()

        assert not key_requests

        request = OutgoingKeyRequest("ABCDF", "ABCDF", TEST_ROOM, "megolm.v1")
        store.add_outgoing_key_request(request)

        store2 = self.copy_store(store)
        key_requests = store2.load_outgoing_key_requests()
        assert "ABCDF" in key_requests.keys()
        assert request == key_requests["ABCDF"]

    def test_db_upgrade(self, tempdir):
        user = "ephemeral"
        device_id = "DEVICE_ID"
        user2 = "alice"
        device_id2 = "ALICE_ID"

        store = MatrixStore(user, device_id, tempdir, database_name="test.db")
        account = OlmAccount()
        session = OutboundSession(account, BOB_CURVE, BOB_ONETIME)
        out_group = OutboundGroupSession()
        in_group = InboundGroupSession(
            out_group.session_key,
            account.identity_keys["ed25519"],
            account.identity_keys["curve25519"],
            TEST_ROOM,
            TEST_FORWARDING_CHAIN,
        )
        devices = self.example_devices
        assert len(devices) == 11

        store.save_account(account)
        store.save_session(BOB_CURVE, session)
        store.save_inbound_group_session(in_group)
        store.save_device_keys(devices)

        store2 = MatrixStore(user2, device_id2, tempdir, database_name="test.db")
        account2 = OlmAccount()
        store2.save_account(account2)
        del store

        store = MatrixStore(user, device_id, tempdir, database_name="test.db")
        loaded_account = store.load_account()

        assert account.identity_keys == loaded_account.identity_keys
        session_store = store.load_sessions()
        loaded_session = session_store.get(BOB_CURVE)
        session_store = store.load_inbound_group_sessions()

        assert loaded_session
        assert session.id == loaded_session.id

        loaded_session = session_store.get(
            TEST_ROOM, account.identity_keys["curve25519"], in_group.id
        )
        device_store = store.load_device_keys()

        # pdb.set_trace()

        assert loaded_session
        assert in_group.id == loaded_session.id
        assert sorted(loaded_session.forwarding_chain) == sorted(TEST_FORWARDING_CHAIN)
        bob_device = device_store[BOB_ID][BOB_DEVICE]
        assert bob_device
        assert bob_device.user_id == BOB_ID
        assert bob_device.id == BOB_DEVICE
        assert bob_device.ed25519 == BOB_ONETIME
        assert bob_device.curve25519 == BOB_CURVE
        assert not bob_device.deleted
        assert len(device_store.users) == 11

    def test_store_versioning(self, store):
        version = store._get_store_version()

        assert version == 7

    def test_sync_recovery_roundtrip_is_atomic(self, sqlstore, monkeypatch):
        sqlstore.save_sync_token("s1")
        gap = RecoveryGap(TEST_ROOM, 1, "p1", "s1")
        event = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$held",
            '{"content":{},"event_id":"$held","sender":"@a:b","type":"m.test"}',
            True,
            False,
            provenance=TimelineEventProvenance.HISTORY,
            apply_room_state=False,
        )

        original = sqlstore._upsert_pending_events

        def fail(*args):
            raise RuntimeError("write failed")

        monkeypatch.setattr(sqlstore, "_upsert_pending_events", fail)
        with pytest.raises(RuntimeError, match="write failed"):
            sqlstore.save_recovery(
                "s2",
                set(),
                [gap],
                [event],
                None,
            )
        assert sqlstore.load_sync_token() == "s1"
        assert sqlstore.load_sync_recovery() == ([], [])

        monkeypatch.setattr(sqlstore, "_upsert_pending_events", original)
        sqlstore.save_recovery("s2", set(), [gap], [event], None)
        assert sqlstore.load_sync_token() == "s2"
        gaps, events = sqlstore.load_sync_recovery()
        assert [
            (gap.room_id, gap.generation, gap.target_token, gap.cursor_token)
            for gap in gaps
        ] == [(TEST_ROOM, 1, "p1", "s1")]
        assert (
            events[0].room_id,
            events[0].generation,
            events[0].sequence,
            events[0].event_id,
            events[0].is_live,
            events[0].admission_accepted,
            events[0].provenance,
            events[0].apply_room_state,
        ) == (
            TEST_ROOM,
            1,
            0,
            "$held",
            True,
            False,
            TimelineEventProvenance.HISTORY,
            False,
        )

        sqlstore.accept_recovery_event(TEST_ROOM, 1, "$held")
        assert sqlstore.load_sync_recovery()[1][0].admission_accepted

    def test_rewind_sync_recovery_for_startup_clears_recovery_after_rollback(
        self,
        sqlstore,
    ):
        gap = RecoveryGap(TEST_ROOM, 1, "s_advanced", "s_cursor")

        def pending(event_id, sequence):
            return PendingTimelineEvent(
                TEST_ROOM,
                1,
                sequence,
                event_id,
                '{"content":{},"event_id":"%s","sender":"@a:b",'
                '"type":"m.test"}' % event_id,
                True,
                False,
            )

        sqlstore.save_recovery(
            "s_advanced",
            set(),
            [gap],
            [
                pending("$accepted", 0),
                pending("$unaccepted", 1),
                pending("$completed", 2),
            ],
            None,
        )
        sqlstore.accept_recovery_event(TEST_ROOM, 1, "$accepted")
        sqlstore.finish_recovery(TEST_ROOM, 1, "$completed", False)
        sqlstore.save_sliding_window_tokens(
            {TEST_ROOM: SlidingWindowToken("w1", "$join")}
        )

        sqlstore._rewind_sync_recovery("s_safe")

        reopened = SqliteStore(
            sqlstore.user_id,
            sqlstore.device_id,
            sqlstore.store_path,
        )
        assert reopened.load_sync_token() == "s_safe"
        assert reopened.load_sync_recovery() == ([], [])
        assert reopened.load_sliding_window_tokens() == {}

    @pytest.mark.parametrize("target_token", ["s_advanced", None])
    def test_rewind_sync_recovery_for_startup_retains_unaccepted_without_rollback(
        self,
        sqlstore,
        target_token,
    ):
        gap = RecoveryGap(TEST_ROOM, 1, "", None)
        cursor_room = "!cursor:example.org"
        cursor_gap = RecoveryGap(cursor_room, 2, "s_target", "s_cursor")

        def pending(event_id, sequence):
            return PendingTimelineEvent(
                TEST_ROOM,
                1,
                sequence,
                event_id,
                '{"content":{},"event_id":"%s","sender":"@a:b",'
                '"type":"m.test"}' % event_id,
                True,
                False,
            )

        sqlstore.save_recovery(
            "s_advanced",
            set(),
            [gap, cursor_gap],
            [
                pending("$accepted", 0),
                pending("$unaccepted", 1),
                pending("$completed", 2),
            ],
            None,
        )
        sqlstore.accept_recovery_event(TEST_ROOM, 1, "$accepted")
        sqlstore.finish_recovery(TEST_ROOM, 1, "$completed", False)
        sqlstore.save_sliding_window_tokens(
            {TEST_ROOM: SlidingWindowToken("w1", "$join")}
        )

        sqlstore._rewind_sync_recovery(target_token)

        reopened = SqliteStore(
            sqlstore.user_id,
            sqlstore.device_id,
            sqlstore.store_path,
        )
        assert reopened.load_sync_token() == target_token
        gaps, events = reopened.load_sync_recovery()
        assert sorted((item.room_id, item.generation) for item in gaps) == [
            (cursor_room, 2),
            (TEST_ROOM, 1),
        ]
        assert [item.event_id for item in events] == ["$unaccepted"]
        assert not events[0].admission_accepted
        assert reopened.load_sliding_window_tokens() == {}

    def test_rewind_sync_recovery_for_startup_is_atomic(
        self,
        sqlstore,
        monkeypatch,
    ):
        gap = RecoveryGap(TEST_ROOM, 1, "s_advanced", "s_cursor")
        event = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$completed",
            '{"content":{},"event_id":"$completed","sender":"@a:b",' '"type":"m.test"}',
            True,
            False,
        )
        sqlstore.save_recovery("s_advanced", set(), [gap], [event], None)
        sqlstore.finish_recovery(TEST_ROOM, 1, event.event_id, False)
        sqlstore.save_sliding_window_tokens(
            {TEST_ROOM: SlidingWindowToken("w1", "$join")}
        )
        original_execute = sqlstore.database.execute_sql
        window_table = SlidingWindowTokens._meta.table_name

        def fail_window_delete(sql, *args, **kwargs):
            if sql.startswith(f'DELETE FROM "{window_table}"'):
                raise RuntimeError("window delete failed")
            return original_execute(sql, *args, **kwargs)

        monkeypatch.setattr(sqlstore.database, "execute_sql", fail_window_delete)

        with pytest.raises(RuntimeError, match="window delete failed"):
            sqlstore._rewind_sync_recovery("s_safe")

        assert sqlstore.load_sync_token() == "s_advanced"
        _, events = sqlstore.load_sync_recovery()
        assert [(item.event_id, item.generation) for item in events] == [
            ("$completed", 0)
        ]
        assert sqlstore.load_sliding_window_tokens() == {
            TEST_ROOM: SlidingWindowToken("w1", "$join")
        }

    def test_sync_recovery_resequences_existing_generation(self, sqlstore):
        gap = RecoveryGap(TEST_ROOM, 1, "p1", "s1")

        def pending(
            event_id: str,
            sequence: int,
            *,
            is_live: bool,
            admission_accepted: bool = False,
        ) -> PendingTimelineEvent:
            return PendingTimelineEvent(
                TEST_ROOM,
                1,
                sequence,
                event_id,
                '{"content":{},"event_id":"%s","sender":"@a:b",'
                '"type":"m.test"}' % event_id,
                is_live,
                False,
                admission_accepted=admission_accepted,
            )

        sqlstore.save_recovery(
            None,
            set(),
            [gap],
            [pending("$held", 0, is_live=True)],
            None,
        )
        sqlstore.accept_recovery_event(TEST_ROOM, 1, "$held")

        sqlstore.save_recovery(
            None,
            set(),
            [gap],
            [
                pending("$gap1", 0, is_live=False),
                pending("$gap2", 1, is_live=False),
                pending("$held", 2, is_live=True, admission_accepted=True),
            ],
            None,
        )

        _, events = sqlstore.load_sync_recovery()
        assert [(event.event_id, event.sequence) for event in events] == [
            ("$gap1", 0),
            ("$gap2", 1),
            ("$held", 2),
        ]
        assert events[-1].admission_accepted

    def test_accept_recovery_event_survives_missing_row(self, sqlstore):
        """A retained dispatch from a crashed iteration can clear a row
        before admission is recorded; acceptance must not poison every
        later sync iteration with the same ValueError."""
        sqlstore.accept_recovery_event(TEST_ROOM, 1, "$vanished")

        assert sqlstore.load_sync_recovery() == ([], [])

    def test_accept_recovery_event_survives_generation_divergence(self, sqlstore):
        """The store can hold an event under a different generation than
        the sync loop believes after a crashed iteration; the event still
        has exactly one row, so admission must land on it."""
        gap = RecoveryGap(TEST_ROOM, 2, "p1", None)
        event = PendingTimelineEvent(
            TEST_ROOM,
            2,
            0,
            "$held",
            '{"content":{},"event_id":"$held","sender":"@a:b","type":"m.test"}',
            True,
            False,
        )
        sqlstore.save_recovery(None, set(), [gap], [event], None)

        sqlstore.accept_recovery_event(TEST_ROOM, 5, "$held")

        _, events = sqlstore.load_sync_recovery()
        assert events[0].admission_accepted

    def test_finish_recovery_survives_missing_row(self, sqlstore):
        """Completing an event whose row a concurrent iteration already
        cleared still records the completed marker instead of raising."""
        sqlstore.finish_recovery(TEST_ROOM, 3, "$vanished", False)

        _, events = sqlstore.load_sync_recovery()
        assert [(event.event_id, event.generation) for event in events] == [
            ("$vanished", 0)
        ]

    def test_finish_recovery_survives_repeated_completion(self, sqlstore):
        """A second completion of the same event keeps its single
        generation-0 marker instead of raising on the UNIQUE constraint,
        and mirrors record_completed_timeline_event: the marker keeps its
        original provenance and combines encryption state with AND, so an
        event once delivered decrypted cannot be re-dispatched as pending
        decryption after a restart."""
        gap = RecoveryGap(TEST_ROOM, 1, "p1", None)
        event = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$done",
            '{"content":{},"event_id":"$done","sender":"@a:b","type":"m.test"}',
            True,
            False,
            provenance=TimelineEventProvenance.LIVE,
        )
        sqlstore.save_recovery(None, set(), [gap], [event], None)
        sqlstore.finish_recovery(TEST_ROOM, 1, "$done", False)

        sqlstore.finish_recovery(TEST_ROOM, 1, "$done", True)

        _, events = sqlstore.load_sync_recovery()
        assert [
            (
                event.event_id,
                event.generation,
                event.provenance,
                bool(event.was_encrypted),
            )
            for event in events
        ] == [("$done", 0, TimelineEventProvenance.LIVE, False)]

    def test_sync_recovery_load_preserves_write_order_for_sequence_ties(
        self,
        sqlstore,
    ):
        gap = RecoveryGap(TEST_ROOM, 1, "p1", "s1")

        def pending(
            event_id: str,
            sequence: int,
            *,
            is_live: bool,
        ) -> PendingTimelineEvent:
            return PendingTimelineEvent(
                TEST_ROOM,
                1,
                sequence,
                event_id,
                '{"content":{},"event_id":"%s","sender":"@a:b",'
                '"type":"m.test"}' % event_id,
                is_live,
                False,
            )

        sqlstore.save_recovery(
            None,
            set(),
            [gap],
            [
                pending("$recovered-before", 0, is_live=False),
                pending("$live-anchor", 1, is_live=True),
                pending("$recovered-after", 2, is_live=False),
            ],
            None,
        )
        sqlstore.save_recovery(
            None,
            set(),
            [gap],
            [pending("$new-live", 2, is_live=True)],
            None,
        )

        _, events = sqlstore.load_sync_recovery()
        assert [(event.event_id, event.sequence) for event in events] == [
            ("$recovered-before", 0),
            ("$live-anchor", 1),
            ("$recovered-after", 2),
            ("$new-live", 2),
        ]

    def test_v2_store_creates_recovery_tables(self, sqlstore):
        with sqlstore.database.bind_ctx(sqlstore.models):
            sqlstore.database.drop_tables(
                [PendingTimelineEvents, SyncRecoveryGaps],
            )
            sqlstore._update_version(2)

        reopened = SqliteStore(
            sqlstore.user_id,
            sqlstore.device_id,
            sqlstore.store_path,
        )
        assert reopened._get_store_version() == 7
        with reopened.database.bind_ctx(reopened.models):
            assert PendingTimelineEvents.table_exists()
            assert SyncRecoveryGaps.table_exists()
            assert SlidingWindowTokens.table_exists()
            columns = {
                row[1]: row[2]
                for row in reopened.database.execute_sql(
                    f'PRAGMA table_info("{PendingTimelineEvents._meta.table_name}")'
                ).fetchall()
            }
        assert columns["event_payload"] == "BLOB"
        assert columns["admission_accepted"] == "INTEGER"
        assert columns["provenance"] == "TEXT"
        assert columns["apply_room_state"] == "INTEGER"
        assert "source_json" not in columns

    def test_v3_store_creates_sliding_window_tokens(self, sqlstore):
        """A v3 store gains the sliding window token table on open."""
        with sqlstore.database.bind_ctx(sqlstore.models):
            sqlstore.database.drop_tables([SlidingWindowTokens])
            sqlstore._update_version(3)

        reopened = SqliteStore(
            sqlstore.user_id,
            sqlstore.device_id,
            sqlstore.store_path,
        )
        assert reopened._get_store_version() == 7
        with reopened.database.bind_ctx(reopened.models):
            assert SlidingWindowTokens.table_exists()
        assert reopened.load_sliding_window_tokens() == {}

    def test_v4_store_discards_unscoped_sliding_window_tokens(self, sqlstore):
        """A token without its membership event cannot authorize a later walk."""
        account = sqlstore._get_account()
        assert account
        table = SlidingWindowTokens._meta.table_name
        with sqlstore.database.bind_ctx(sqlstore.models):
            sqlstore.database.drop_tables([SlidingWindowTokens])
            sqlstore.database.execute_sql(f"""
                CREATE TABLE "{table}" (
                    "id" INTEGER NOT NULL PRIMARY KEY,
                    "room_id" TEXT NOT NULL,
                    "token" TEXT NOT NULL,
                    "account_id" INTEGER NOT NULL
                )
                """)
            sqlstore.database.execute_sql(
                f'INSERT INTO "{table}" '
                '("room_id", "token", "account_id") VALUES (?, ?, ?)',
                (TEST_ROOM, "w1", account.id),
            )
            sqlstore._update_version(4)

        reopened = SqliteStore(
            sqlstore.user_id,
            sqlstore.device_id,
            sqlstore.store_path,
        )

        assert reopened._get_store_version() == 7
        assert reopened.load_sliding_window_tokens() == {}

    def test_v5_store_adds_durable_admission_phase(self, sqlstore):
        gap = RecoveryGap(TEST_ROOM, 1, "target", "cursor")
        event = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$pending",
            "{}",
            True,
            False,
        )
        sqlstore.save_recovery(None, set(), [gap], [event], None)
        table = PendingTimelineEvents._meta.table_name
        with sqlstore.database.bind_ctx(sqlstore.models):
            sqlstore.database.execute_sql(
                f'ALTER TABLE "{table}" DROP COLUMN admission_accepted'
            )
            sqlstore._update_version(5)

        sqlstore.upgrade_to_v6()

        assert sqlstore._get_store_version() == 6
        assert not sqlstore.load_sync_recovery()[1][0].admission_accepted

    def test_v7_store_adds_provenance_without_dropping_recovery(
        self,
        sqlstore,
    ):
        seed_v5_recovery_state(sqlstore)
        sqlstore.upgrade_to_v6()
        table = PendingTimelineEvents._meta.table_name
        with sqlstore.database.bind_ctx(sqlstore.models):
            sqlstore.database.execute_sql(
                f'ALTER TABLE "{table}" DROP COLUMN apply_room_state'
            )
            sqlstore.database.execute_sql(
                f'ALTER TABLE "{table}" DROP COLUMN provenance'
            )

        reopened = SqliteStore(
            sqlstore.user_id,
            sqlstore.device_id,
            sqlstore.store_path,
        )

        assert reopened._get_store_version() == 7
        assert reopened.load_sync_token() == "s1"
        assert reopened.load_sliding_window_tokens() == {
            TEST_ROOM: SlidingWindowToken("w1", "$join")
        }
        gaps, events = reopened.load_sync_recovery()
        assert [
            (gap.room_id, gap.generation, gap.target_token, gap.cursor_token)
            for gap in gaps
        ] == [(TEST_ROOM, 1, "p1", None)]
        assert [
            (
                event.event_id,
                event.generation,
                event.provenance,
                event.apply_room_state,
            )
            for event in events
        ] == [
            ("$completed", 0, TimelineEventProvenance.HISTORY, False),
            ("$pending", 1, TimelineEventProvenance.HISTORY, True),
        ]
        with reopened.database.bind_ctx(reopened.models):
            columns = {
                row[1]
                for row in reopened.database.execute_sql(
                    f'PRAGMA table_info("{PendingTimelineEvents._meta.table_name}")'
                ).fetchall()
            }
        assert {"provenance", "apply_room_state"} <= columns

    def test_v7_recovery_upgrade_rollback_preserves_state(self, sqlstore, monkeypatch):
        seed_v5_recovery_state(sqlstore)
        sqlstore.upgrade_to_v6()
        table = PendingTimelineEvents._meta.table_name
        gaps_table = SyncRecoveryGaps._meta.table_name
        with sqlstore.database.bind_ctx(sqlstore.models):
            sqlstore.database.execute_sql(
                f'ALTER TABLE "{table}" DROP COLUMN apply_room_state'
            )
            sqlstore.database.execute_sql(
                f'ALTER TABLE "{table}" DROP COLUMN provenance'
            )

        def fail_version_update(_version):
            raise RuntimeError("version write failed")

        monkeypatch.setattr(sqlstore, "_update_version", fail_version_update)

        with pytest.raises(RuntimeError, match="version write failed"):
            sqlstore.upgrade_to_v7()

        assert sqlstore._get_store_version() == 6
        assert sqlstore.load_sync_token() == "s1"
        with sqlstore.database.bind_ctx(sqlstore.models):
            columns = {
                row[1]
                for row in sqlstore.database.execute_sql(
                    f'PRAGMA table_info("{table}")'
                ).fetchall()
            }
            event_count = sqlstore.database.execute_sql(
                f'SELECT COUNT(*) FROM "{table}"'
            ).fetchone()[0]
            gap_count = sqlstore.database.execute_sql(
                f'SELECT COUNT(*) FROM "{gaps_table}"'
            ).fetchone()[0]
        assert "provenance" not in columns
        assert "apply_room_state" not in columns
        assert event_count == 2
        assert gap_count == 1
        assert sqlstore.load_sliding_window_tokens() == {
            TEST_ROOM: SlidingWindowToken("w1", "$join")
        }

    def test_sliding_window_tokens_roundtrip(self, sqlstore):
        """Tokens survive a reopen, and forgotten rooms do not."""
        sqlstore.save_sliding_window_tokens(
            {
                TEST_ROOM: SlidingWindowToken("w1", "$join-a"),
                "!b:example.org": SlidingWindowToken("w2", "$join-b"),
            }
        )
        assert sqlstore.load_sliding_window_tokens() == {
            TEST_ROOM: SlidingWindowToken("w1", "$join-a"),
            "!b:example.org": SlidingWindowToken("w2", "$join-b"),
        }

        # A newer window replaces the token; a left room drops it.
        sqlstore.save_sliding_window_tokens(
            {TEST_ROOM: SlidingWindowToken("w3", "$join-a")},
            ["!b:example.org"],
        )
        assert sqlstore.load_sliding_window_tokens() == {
            TEST_ROOM: SlidingWindowToken("w3", "$join-a")
        }

        reopened = SqliteStore(
            sqlstore.user_id,
            sqlstore.device_id,
            sqlstore.store_path,
        )
        assert reopened.load_sliding_window_tokens() == {
            TEST_ROOM: SlidingWindowToken("w3", "$join-a")
        }

    @pytest.mark.parametrize("membership_event_id", [None, ""])
    def test_sliding_window_token_requires_membership_identity(
        self, membership_event_id
    ):
        with pytest.raises(ValueError, match="membership_event_id"):
            SlidingWindowToken("w1", membership_event_id)

    def test_sliding_window_tokens_chunk_large_batches(self, sqlstore):
        """More rooms than SQLite can bind in one statement still write."""
        connection = sqlstore.database.connection()
        if not hasattr(connection, "setlimit"):
            pytest.skip("sqlite3.Connection.setlimit is unavailable")
        old_limit = connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 500)
        tokens = {
            f"!room{index}:example.org": SlidingWindowToken(
                f"w{index}", f"$join{index}"
            )
            for index in range(750)
        }
        try:
            sqlstore.save_sliding_window_tokens(tokens)
            assert sqlstore.load_sliding_window_tokens() == tokens

            sqlstore.save_sliding_window_tokens({}, list(tokens)[:600])
            assert len(sqlstore.load_sliding_window_tokens()) == 150
        finally:
            connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, old_limit)

    def test_sync_recovery_chunks_large_room_clears(self, sqlstore):
        """A large reset response stays below SQLite's bind limit."""
        connection = sqlstore.database.connection()
        if not hasattr(connection, "setlimit"):
            pytest.skip("sqlite3.Connection.setlimit is unavailable")
        old_limit = connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 500)
        room_ids = [f"!room{index}:example.org" for index in range(600)]
        gaps = [RecoveryGap(room_id, 1, "target", "cursor") for room_id in room_ids]
        try:
            sqlstore.save_recovery(None, set(), gaps, [], None)
            sqlstore.save_recovery(None, set(room_ids), [], [], None)
            remaining, _ = sqlstore.load_sync_recovery()
            assert not {gap.room_id for gap in remaining} & set(room_ids)
        finally:
            connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, old_limit)

    def test_save_recovery_writes_window_tokens_in_one_transaction(
        self, sqlstore, monkeypatch
    ):
        """The plan and its baselines land together or not at all."""
        gap = RecoveryGap(TEST_ROOM, 1, "p1", "s1")

        def fail(*args):
            raise RuntimeError("boom")

        monkeypatch.setattr(sqlstore, "_upsert_pending_events", fail)
        with pytest.raises(RuntimeError):
            sqlstore.save_recovery(
                "s1",
                set(),
                [gap],
                [
                    PendingTimelineEvent(
                        TEST_ROOM,
                        1,
                        0,
                        "$held",
                        '{"content":{},"event_id":"$held","sender":"@a:b",'
                        '"type":"m.test"}',
                        True,
                        False,
                    )
                ],
                None,
                {TEST_ROOM: SlidingWindowToken("w1", "$join")},
            )

        # The write failed, so neither the plan nor the token survives.
        assert sqlstore.load_sliding_window_tokens() == {}
        gaps, events = sqlstore.load_sync_recovery()
        assert not gaps
        assert not events

    def test_forget_sliding_window_token(self, sqlstore):
        sqlstore.save_sliding_window_tokens(
            {
                TEST_ROOM: SlidingWindowToken("w1", "$join-a"),
                "!b:example.org": SlidingWindowToken("w2", "$join-b"),
            }
        )
        sqlstore.forget_sliding_window_token(TEST_ROOM)
        assert sqlstore.load_sliding_window_tokens() == {
            "!b:example.org": SlidingWindowToken("w2", "$join-b")
        }

    def test_pending_recovery_payload_is_encrypted_at_rest(self, tempdir):
        store = SqliteStore(
            "@secure:example.org",
            "DEVICEID",
            tempdir,
            pickle_key="recovery-secret",
        )
        store.save_account(OlmAccount())
        marker = "PRIVATE-RECOVERY-MARKER"
        source = (
            '{"content":{"body":"PRIVATE-RECOVERY-MARKER","msgtype":"m.text"},'
            '"event_id":"$event","sender":"@private:example.org",'
            '"type":"m.room.message"}'
        )
        gap = RecoveryGap(TEST_ROOM, 1, "p1", "s1")
        event = PendingTimelineEvent(TEST_ROOM, 1, 0, "$event", source, False, False)
        store.save_recovery("s2", set(), [gap], [event], None)
        table = PendingTimelineEvents._meta.table_name
        payload, storage_type = store.database.execute_sql(
            f'SELECT event_payload, typeof(event_payload) FROM "{table}"'
        ).fetchone()
        assert storage_type == "blob"
        assert marker.encode() not in payload
        assert b"@private:example.org" not in payload
        database_path = Path(store.database_path)
        store.database.close()
        assert marker.encode() not in database_path.read_bytes()
        assert b"@private:example.org" not in database_path.read_bytes()

        reopened = SqliteStore(
            store.user_id,
            store.device_id,
            store.store_path,
            pickle_key="recovery-secret",
        )
        _, events = reopened.load_sync_recovery()
        assert events == [event]

    def test_pending_recovery_payload_rejects_wrong_key(self, tempdir):
        store = SqliteStore(
            "@secure:example.org",
            "DEVICEID",
            tempdir,
            pickle_key="correct-key",
        )
        store.save_account(OlmAccount())
        event = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$event",
            '{"content":{},"event_id":"$event","sender":"@a:b","type":"m.test"}',
            False,
            False,
        )
        store.save_recovery(
            "s2",
            set(),
            [RecoveryGap(TEST_ROOM, 1, "p1", "s1")],
            [event],
            None,
        )
        store.database.close()

        wrong_key = SqliteStore(
            store.user_id,
            store.device_id,
            store.store_path,
            pickle_key="wrong-key",
        )
        with pytest.raises(ValueError, match="Invalid encrypted recovery payload"):
            wrong_key.load_sync_recovery()

    def test_pending_recovery_payload_rejects_tampering(self, sqlstore):
        event = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$event",
            '{"content":{},"event_id":"$event","sender":"@a:b","type":"m.test"}',
            False,
            False,
        )
        sqlstore.save_recovery(
            "s2",
            set(),
            [RecoveryGap(TEST_ROOM, 1, "p1", "s1")],
            [event],
            None,
        )
        with sqlstore.database.bind_ctx(sqlstore.models):
            row = PendingTimelineEvents.get(
                PendingTimelineEvents.event_id == event.event_id
            )
            payload = bytearray(row.event_payload)
            payload[-1] ^= 1
            row.event_payload = bytes(payload)
            row.save()

        with pytest.raises(ValueError, match="Invalid encrypted recovery payload"):
            sqlstore.load_sync_recovery()

    def test_pending_recovery_payload_authenticates_row_identity(self, sqlstore):
        events = [
            PendingTimelineEvent(
                TEST_ROOM,
                1,
                index,
                event_id,
                (
                    f'{{"content":{{}},"event_id":"{event_id}",'
                    '"sender":"@a:b","type":"m.test"}'
                ),
                False,
                False,
            )
            for index, event_id in enumerate(("$one", "$two"))
        ]
        sqlstore.save_recovery(
            "s2",
            set(),
            [RecoveryGap(TEST_ROOM, 1, "p1", "s1")],
            events,
            None,
        )
        with sqlstore.database.bind_ctx(sqlstore.models):
            first = PendingTimelineEvents.get(PendingTimelineEvents.event_id == "$one")
            second = PendingTimelineEvents.get(PendingTimelineEvents.event_id == "$two")
            first.event_payload = second.event_payload
            first.save()

        with pytest.raises(ValueError, match="Invalid encrypted recovery payload"):
            sqlstore.load_sync_recovery()

    def test_pending_recovery_payload_uses_fresh_nonces(self, sqlstore):
        source = '{"content":{},"sender":"@a:b","type":"m.test"}'
        events = [
            PendingTimelineEvent(
                TEST_ROOM,
                1,
                index,
                event_id,
                source,
                False,
                False,
            )
            for index, event_id in enumerate(("$one", "$two"))
        ]
        sqlstore.save_recovery(
            "s2",
            set(),
            [RecoveryGap(TEST_ROOM, 1, "p1", "s1")],
            events,
            None,
        )
        with sqlstore.database.bind_ctx(sqlstore.models):
            payloads = [
                bytes(row.event_payload)
                for row in PendingTimelineEvents.select().order_by(
                    PendingTimelineEvents.event_id
                )
            ]
        assert payloads[0][1:13] != payloads[1][1:13]

    def test_completed_recovery_row_has_no_payload(self, sqlstore):
        event = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$event",
            '{"content":{},"event_id":"$event","sender":"@a:b","type":"m.test"}',
            False,
            False,
        )
        sqlstore.save_recovery(
            "s2",
            set(),
            [RecoveryGap(TEST_ROOM, 1, "p1", None)],
            [event],
            None,
        )
        sqlstore.finish_recovery(TEST_ROOM, 1, event.event_id, False)
        with sqlstore.database.bind_ctx(sqlstore.models):
            completed = PendingTimelineEvents.get(
                PendingTimelineEvents.event_id == event.event_id
            )
        assert completed.generation == 0
        assert completed.event_payload == b""

    def test_pending_recovery_event_retains_encrypted_source(self, sqlstore):
        gap = RecoveryGap(TEST_ROOM, 1, "p1", None)
        encrypted = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$event",
            '{"content":{},"event_id":"$event","sender":"@a:b",'
            '"type":"m.room.encrypted"}',
            False,
            True,
        )
        decrypted = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$event",
            '{"content":{"body":"clear","msgtype":"m.text"},'
            '"event_id":"$event","sender":"@a:b","type":"m.room.message"}',
            False,
            False,
        )
        sqlstore.save_recovery("s2", set(), [gap], [encrypted], None)
        sqlstore.save_recovery(None, set(), [gap], [decrypted], None)

        gaps, events = sqlstore.load_sync_recovery()
        assert len(gaps) == 1
        assert len(events) == 1
        assert events[0].was_encrypted
        assert '"type":"m.room.encrypted"' in events[0].source_json

    def test_completed_encrypted_event_allows_plaintext_upgrade(self, sqlstore):
        gap = RecoveryGap(TEST_ROOM, 1, "p1", None)
        encrypted = PendingTimelineEvent(
            TEST_ROOM,
            1,
            0,
            "$event",
            '{"content":{},"event_id":"$event","sender":"@a:b",'
            '"type":"m.room.encrypted"}',
            False,
            True,
        )
        decrypted = PendingTimelineEvent(
            TEST_ROOM,
            2,
            0,
            "$event",
            '{"content":{"body":"clear","msgtype":"m.text"},'
            '"event_id":"$event","sender":"@a:b","type":"m.room.message"}',
            False,
            False,
        )
        sqlstore.save_recovery("s2", set(), [gap], [encrypted], None)
        sqlstore.finish_recovery(TEST_ROOM, 1, "$event", True)
        next_gap = RecoveryGap(TEST_ROOM, 2, "p2", None)
        sqlstore.save_recovery("s3", set(), [next_gap], [decrypted], None)

        gaps, events = sqlstore.load_sync_recovery()
        assert len(gaps) == 2
        assert len(events) == 1
        assert events[0].generation == 2
        assert not events[0].was_encrypted
        assert '"body":"clear"' in events[0].source_json

        sqlstore.finish_recovery(TEST_ROOM, 2, "$event", False)
        sqlstore.finish_recovery(TEST_ROOM, 1, None, False)
        sqlstore.finish_recovery(TEST_ROOM, 2, None, False)
        gaps, events = sqlstore.load_sync_recovery()
        assert gaps == []
        assert len(events) == 1
        assert events[0].generation == 0
        assert events[0].source_json == ""

    def test_completed_encrypted_event_allows_pending_retry(self, sqlstore):
        gap = RecoveryGap(TEST_ROOM, 1, "p1", None)
        encrypted = PendingTimelineEvent(TEST_ROOM, 1, 0, "$event", "{}", False, True)
        sqlstore.save_recovery("s1", set(), [gap], [encrypted], None)
        sqlstore.finish_recovery(TEST_ROOM, 1, "$event", True)
        retry = PendingTimelineEvent(TEST_ROOM, 2, 0, "$event", "{}", False, True, True)
        next_gap = RecoveryGap(TEST_ROOM, 2, "p2", None)
        sqlstore.save_recovery("s2", set(), [next_gap], [retry], None)

        _, events = sqlstore.load_sync_recovery()
        assert len(events) == 1
        assert events[0].generation == 2
        assert events[0].was_completed

        sqlstore.finish_recovery(TEST_ROOM, 2, "$event", False)
        _, events = sqlstore.load_sync_recovery()
        assert events[0].generation == 0
        assert events[0].source_json == ""
        assert not events[0].was_completed
        assert not events[0].was_encrypted

    @pytest.mark.parametrize("clear_mode", ["recovered", "room"])
    def test_abandonment_restores_completed_encrypted_marker(
        self, sqlstore, clear_mode
    ):
        first_gap = RecoveryGap(TEST_ROOM, 1, "p1", None)
        encrypted = PendingTimelineEvent(TEST_ROOM, 1, 0, "$event", "{}", False, True)
        sqlstore.save_recovery("s1", set(), [first_gap], [encrypted], None)
        sqlstore.finish_recovery(TEST_ROOM, 1, "$event", True)
        retry_gap = RecoveryGap(TEST_ROOM, 2, "p2", "cursor")
        retry = PendingTimelineEvent(
            TEST_ROOM,
            2,
            0,
            "$event",
            "{}",
            False,
            False,
            True,
        )
        sqlstore.save_recovery("s2", set(), [retry_gap], [retry], None)

        sqlstore.save_recovery(
            None,
            {TEST_ROOM} if clear_mode == "room" else set(),
            [],
            [],
            retry_gap if clear_mode == "recovered" else None,
        )

        _, events = sqlstore.load_sync_recovery()
        assert len(events) == 1
        assert events[0].generation == 0
        assert events[0].was_encrypted
        assert not events[0].was_completed

    def test_synthetic_recovery_key_is_deleted_after_callback(self, sqlstore):
        gap = RecoveryGap(TEST_ROOM, 1, "", None)
        event = PendingTimelineEvent(
            TEST_ROOM, 1, 0, "~sliding:scope:pos:0", "{}", True, False
        )
        sqlstore.save_recovery(None, set(), [gap], [event], None)
        sqlstore.finish_recovery(TEST_ROOM, 1, event.event_id, False)

        gaps, events = sqlstore.load_sync_recovery()
        assert len(gaps) == 1
        assert events == []

    def test_recovery_bulk_writes_respect_sqlite_bind_limit(self, sqlstore):
        connection = sqlstore.database.connection()
        can_set_limit = hasattr(connection, "setlimit")
        row_count = 200 if can_set_limit else 3700
        old_limit = (
            connection.setlimit(sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER, 999)
            if can_set_limit
            else None
        )
        gaps = [
            RecoveryGap(f"!bulk-{index}:example.org", 1, "target", "cursor")
            for index in range(row_count)
        ]
        events = [
            PendingTimelineEvent(
                gap.room_id,
                1,
                0,
                f"$bulk-{index}",
                "{}",
                True,
                False,
            )
            for index, gap in enumerate(gaps)
        ]

        try:
            sqlstore.save_recovery("bulk-token", set(), gaps, events, None)
        finally:
            if old_limit is not None:
                connection.setlimit(
                    sqlite3.SQLITE_LIMIT_VARIABLE_NUMBER,
                    old_limit,
                )

        loaded_gaps, loaded_events = sqlstore.load_sync_recovery()
        assert sqlstore.load_sync_token() == "bulk-token"
        assert len(loaded_gaps) == row_count
        assert len(loaded_events) == row_count

    def test_completed_upgrade_refreshes_pruning_recency(self, sqlstore):
        gap = RecoveryGap(TEST_ROOM, 1, "", None)

        def complete(event_id, was_encrypted):
            event = PendingTimelineEvent(
                TEST_ROOM, 1, 0, event_id, "{}", False, was_encrypted
            )
            sqlstore.save_recovery(None, set(), [gap], [event], None)
            sqlstore.finish_recovery(TEST_ROOM, 1, event_id, was_encrypted)

        complete("$same", True)
        for index in range(1, 512):
            complete(f"${index}", False)
        complete("$same", False)
        complete("$new", False)

        _, events = sqlstore.load_sync_recovery()
        event_ids = [event.event_id for event in events]
        assert len(event_ids) == 512
        assert "$same" in event_ids
        assert "$1" not in event_ids

    def test_sqlitestore_verification(self, sqlstore):
        devices = self.example_devices
        bob_device = devices[BOB_ID][BOB_DEVICE]

        sqlstore.save_device_keys(devices)

        assert not sqlstore.is_device_verified(bob_device)
        assert sqlstore.verify_device(bob_device)
        assert sqlstore.is_device_verified(bob_device)
        assert not sqlstore.verify_device(bob_device)
        assert sqlstore.is_device_verified(bob_device)
        assert sqlstore.unverify_device(bob_device)
        assert not sqlstore.is_device_verified(bob_device)
        assert not sqlstore.unverify_device(bob_device)

    def test_sqlitestore_blacklisting(self, sqlstore):
        devices = self.example_devices
        bob_device = devices[BOB_ID][BOB_DEVICE]

        sqlstore.save_device_keys(devices)

        assert not sqlstore.is_device_blacklisted(bob_device)
        assert sqlstore.blacklist_device(bob_device)
        assert sqlstore.is_device_blacklisted(bob_device)
        assert not sqlstore.is_device_verified(bob_device)
        assert not sqlstore.blacklist_device(bob_device)
        assert sqlstore.unblacklist_device(bob_device)
        assert not sqlstore.is_device_blacklisted(bob_device)
        assert not sqlstore.is_device_verified(bob_device)
        assert not sqlstore.unblacklist_device(bob_device)
        assert sqlstore.blacklist_device(bob_device)
        assert sqlstore.is_device_blacklisted(bob_device)
        assert sqlstore.verify_device(bob_device)
        assert not sqlstore.is_device_blacklisted(bob_device)
        assert sqlstore.is_device_verified(bob_device)

    def test_sqlitememorystore(self, sqlmemorystore):
        devices = self.example_devices
        bob_device = devices[BOB_ID][BOB_DEVICE]
        sqlmemorystore.save_device_keys(devices)

        assert not sqlmemorystore.is_device_verified(bob_device)
        assert sqlmemorystore.verify_device(bob_device)
        assert sqlmemorystore.is_device_verified(bob_device)

    def test_device_deletion(self, store):
        store.load_account()

        devices = self.example_devices
        assert len(devices) == 11

        store.save_device_keys(devices)
        device_store = store.load_device_keys()
        bob_device = device_store[BOB_ID][BOB_DEVICE]
        assert not bob_device.deleted
        bob_device.deleted = True
        store.save_device_keys(device_store)
        device_store = store.load_device_keys()
        bob_device = device_store[BOB_ID][BOB_DEVICE]
        assert bob_device.deleted

    def test_deleting_trusted_device(self, sqlstore):
        devices = self.example_devices
        sqlstore.save_device_keys(devices)

        device_store = sqlstore.load_device_keys()
        bob_device = device_store[BOB_ID][BOB_DEVICE]
        sqlstore.verify_device(bob_device)

        bob_device.deleted = True
        sqlstore.save_device_keys(device_store)
        sqlstore.save_device_keys(devices)

    def test_ignoring_many(self, store):
        devices = self.example_devices

        device_list = [device for d in devices.values() for device in d.values()]

        store.save_device_keys(devices)
        store.ignore_devices(device_list)

        for device in device_list:
            assert store.is_device_ignored(device)

    def test_ignoring_many_sqlite(self, sqlstore):
        devices = self.example_devices

        device_list = [device for d in devices.values() for device in d.values()]

        sqlstore.save_device_keys(devices)
        sqlstore.ignore_devices(device_list)

        for device in device_list:
            assert sqlstore.is_device_ignored(device)

    def test_trust_state_updating_sqlite(self, sqlstore):
        devices = self.example_devices
        bob_device = devices[BOB_ID][BOB_DEVICE]

        device_list = [device for d in devices.values() for device in d.values()]

        sqlstore.save_device_keys(devices)

        assert bob_device.trust_state == TrustState.unset
        sqlstore.verify_device(bob_device)
        assert bob_device.trust_state == TrustState.verified
        sqlstore.unverify_device(bob_device)
        assert bob_device.trust_state == TrustState.unset

        sqlstore.blacklist_device(bob_device)
        assert bob_device.trust_state == TrustState.blacklisted
        sqlstore.unblacklist_device(bob_device)
        assert bob_device.trust_state == TrustState.unset

        sqlstore.ignore_device(bob_device)
        assert bob_device.trust_state == TrustState.ignored
        sqlstore.unignore_device(bob_device)
        assert bob_device.trust_state == TrustState.unset

        sqlstore.ignore_devices(device_list)
        for device in device_list:
            assert device.trust_state == TrustState.ignored

    def test_trust_state_updating_default(self, store):
        devices = self.example_devices
        bob_device = devices[BOB_ID][BOB_DEVICE]

        device_list = [device for d in devices.values() for device in d.values()]

        store.save_device_keys(devices)

        assert bob_device.trust_state == TrustState.unset
        assert not bob_device.verified
        store.verify_device(bob_device)
        assert bob_device.trust_state == TrustState.verified
        assert bob_device.verified
        store.unverify_device(bob_device)
        assert bob_device.trust_state == TrustState.unset
        assert not bob_device.verified

        store.blacklist_device(bob_device)
        assert bob_device.trust_state == TrustState.blacklisted
        assert bob_device.blacklisted
        store.unblacklist_device(bob_device)
        assert bob_device.trust_state == TrustState.unset
        assert not bob_device.blacklisted

        store.ignore_device(bob_device)
        assert bob_device.trust_state == TrustState.ignored
        assert bob_device.ignored
        store.unignore_device(bob_device)
        assert bob_device.trust_state == TrustState.unset
        assert not bob_device.ignored

        store.ignore_devices(device_list)
        for device in device_list:
            assert device.trust_state == TrustState.ignored

    def test_trust_state_loading(self, store):
        devices = self.example_devices
        bob_device = devices[BOB_ID][BOB_DEVICE]
        store.save_device_keys(devices)
        assert not bob_device.verified
        store.verify_device(bob_device)
        assert bob_device.verified

        store2 = DefaultStore(store.user_id, store.device_id, store.store_path)
        loaded_devices = store2.load_device_keys()

        bob_device = loaded_devices[BOB_ID][BOB_DEVICE]

        assert bob_device.verified

    def test_trust_state_loading_sql(self, sqlstore):
        devices = self.example_devices
        bob_device = devices[BOB_ID][BOB_DEVICE]
        sqlstore.save_device_keys(devices)
        assert not bob_device.verified
        sqlstore.verify_device(bob_device)
        assert bob_device.verified

        store2 = SqliteStore(sqlstore.user_id, sqlstore.device_id, sqlstore.store_path)
        loaded_devices = store2.load_device_keys()

        bob_device = loaded_devices[BOB_ID][BOB_DEVICE]

        assert bob_device.verified

    def test_sync_token_loading(self, sqlstore):
        token = "1234"
        sqlstore.save_sync_token(token)
        loaded_token = sqlstore.load_sync_token()
        assert token == loaded_token
