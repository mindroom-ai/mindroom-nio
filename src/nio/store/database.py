# Copyright 2018 Zil0
# Copyright © 2018, 2019 Damir Jelić <poljar@termina.org.uk>
# Licensed under the Apache License, Version 2.0 (the "License");
# you may not use this file except in compliance with the License.
# You may obtain a copy of the License at
# http://www.apache.org/licenses/LICENSE-2.0
# Unless required by applicable law or agreed to in writing, software
# distributed under the License is distributed on an "AS IS" BASIS,
# WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
# See the License for the specific language governing permissions and
# limitations under the License.

from __future__ import annotations

import hashlib
import json
import os
import sqlite3
from dataclasses import asdict, dataclass, field
from functools import wraps
from typing import TYPE_CHECKING

from Crypto.Cipher import AES
from peewee import EXCLUDED, Case, DoesNotExist, SqliteDatabase
from playhouse.sqliteq import SqliteQueueDatabase

from ..crypto import (
    DeviceStore,
    GroupSessionStore,
    InboundGroupSession,
    OlmAccount,
    OlmDevice,
    OutgoingKeyRequest,
    Session,
    SessionStore,
    TrustState,
)
from ..event_provenance import TimelineEventProvenance
from ..sliding_sync_tokens import SlidingWindowToken
from . import (
    Accounts,
    DeviceKeys,
    DeviceKeys_v1,
    DeviceTrustState,
    EncryptedRooms,
    ForwardedChains,
    Key,
    Keys,
    KeyStore,
    MegolmInboundSessions,
    OlmSessions,
    OutgoingKeyRequests,
    PendingTimelineEvents,
    SlidingWindowTokens,
    StoreVersion,
    SyncRecoveryGaps,
    SyncTokens,
)
from .log import logger

if TYPE_CHECKING:
    from collections.abc import Iterable, Mapping

    from ..client.sync_recovery import (
        PendingEventKind,
        PendingTimelineEvent,
        RecoveryGap,
    )


_RECOVERY_PAYLOAD_VERSION = 1
_RECOVERY_NONCE_SIZE = 12
_RECOVERY_TAG_SIZE = 16
# Widest recovery write binds 12 values per row; 80 * 12 stays below
# SQLite's legacy 999-variable statement limit.
_RECOVERY_WRITE_CHUNK_SIZE = 80
_RECOVERY_KEY_DOMAIN = b"mindroom-nio:sync-recovery:v3\0"


def _recovery_payload_aad(
    account_id: int,
    room_id: str,
    generation: int,
    event_id: str,
) -> bytes:
    values = (str(account_id), room_id, str(generation), event_id)
    encoded = (value.encode() for value in values)
    return b"".join(len(value).to_bytes(4, "big") + value for value in encoded)


def use_database(fn):
    """
    Ensure that the correct database context is used for the wrapped function.
    """

    @wraps(fn)
    def inner(self, *args, **kwargs):
        with self.database.bind_ctx(self.models):
            return fn(self, *args, **kwargs)

    return inner


def use_database_atomic(fn):
    """
    Ensure that the correct database context is used for the wrapped function.

    This also ensures that the database transaction will be atomic.
    """

    @wraps(fn)
    def inner(self, *args, **kwargs):
        with self.database.bind_ctx(self.models):
            if isinstance(self.database, SqliteQueueDatabase):
                return fn(self, *args, **kwargs)
            else:
                with self.database.atomic():
                    return fn(self, *args, **kwargs)

    return inner


@dataclass
class MatrixStore:
    """Storage class for matrix state."""

    models = [
        Accounts,
        OlmSessions,
        MegolmInboundSessions,
        ForwardedChains,
        DeviceKeys,
        EncryptedRooms,
        OutgoingKeyRequests,
        StoreVersion,
        Keys,
        SyncTokens,
        SyncRecoveryGaps,
        PendingTimelineEvents,
        SlidingWindowTokens,
    ]
    store_version = 7
    user_id: str = field()
    device_id: str = field()
    store_path: str = field()
    pickle_key: str = ""
    database_name: str = ""
    database_path: str = field(init=False)
    database: SqliteDatabase = field(init=False)

    def _create_database(self):
        return SqliteDatabase(
            self.database_path,
            pragmas={
                "foreign_keys": 1,
                "secure_delete": 1,
            },
        )

    def upgrade_to_v2(self):
        with self.database.bind_ctx([DeviceKeys_v1]):
            self.database.drop_tables(
                [
                    DeviceTrustState,
                    DeviceKeys_v1,
                ],
                safe=True,
            )

        with self.database.bind_ctx(self.models):
            self.database.create_tables([DeviceKeys, DeviceTrustState])
        self._update_version(2)

    def upgrade_to_v3(self):
        with self.database.bind_ctx(self.models):
            self.database.create_tables([SyncRecoveryGaps, PendingTimelineEvents])
        self._update_version(3)

    def upgrade_to_v5(self):
        with self.database.bind_ctx(self.models):
            self.database.drop_tables([SlidingWindowTokens], safe=True)
            self.database.create_tables([SlidingWindowTokens])
        self._update_version(5)

    def upgrade_to_v6(self):
        with self.database.bind_ctx(self.models):
            table = PendingTimelineEvents._meta.table_name
            columns = {
                row[1]
                for row in self.database.execute_sql(
                    f'PRAGMA table_info("{table}")'
                ).fetchall()
            }
            if "admission_accepted" not in columns:
                self.database.execute_sql(
                    f'ALTER TABLE "{table}" '
                    "ADD COLUMN admission_accepted INTEGER NOT NULL DEFAULT 0"
                )
        self._update_version(6)

    @use_database_atomic
    def upgrade_to_v7(self):
        table = PendingTimelineEvents._meta.table_name
        columns = {
            row[1]
            for row in self.database.execute_sql(
                f'PRAGMA table_info("{table}")'
            ).fetchall()
        }
        if "provenance" not in columns:
            self.database.execute_sql(
                f'ALTER TABLE "{table}" '
                "ADD COLUMN provenance TEXT NOT NULL DEFAULT 'live'"
            )
        if "apply_room_state" not in columns:
            self.database.execute_sql(
                f'ALTER TABLE "{table}" '
                "ADD COLUMN apply_room_state INTEGER NOT NULL DEFAULT 1"
            )

        # Legacy rows predate public provenance, so classify them conservatively
        # as history while preserving their exact callback obligations.
        PendingTimelineEvents.update(
            provenance=TimelineEventProvenance.HISTORY.value,
            apply_room_state=PendingTimelineEvents.is_live,
        ).execute()
        self._update_version(7)

    def __post_init__(self):
        self.database_name = self.database_name or f"{self.user_id}_{self.device_id}.db"
        self.database_path = os.path.join(self.store_path, self.database_name)
        self.database = self._create_database()
        self.database.connect()

        store_version = self._get_store_version()

        # Update the store if it's an old version here.
        if store_version == 1:
            self.upgrade_to_v2()
            store_version = 2
        if store_version == 2:
            self.upgrade_to_v3()
            store_version = 3
        if store_version in (3, 4):
            self.upgrade_to_v5()
            store_version = 5
        if store_version == 5:
            self.upgrade_to_v6()
            store_version = 6
        if store_version == 6:
            self.upgrade_to_v7()

        with self.database.bind_ctx(self.models):
            self.database.create_tables(self.models)

    def _get_store_version(self):
        with self.database.bind_ctx([StoreVersion]):
            self.database.create_tables([StoreVersion])
            v, _ = StoreVersion.get_or_create(defaults={"version": self.store_version})
            return v.version

    def _update_version(self, new_version):
        with self.database.bind_ctx([StoreVersion]):
            v, _ = StoreVersion.get_or_create(defaults={"version": new_version})
            v.version = new_version
            v.save()

    @use_database
    def _get_account(self):
        try:
            return Accounts.get(
                Accounts.user_id == self.user_id, Accounts.device_id == self.device_id
            )
        except DoesNotExist:
            return None

    def load_account(self) -> OlmAccount | None:
        """Load the Olm account from the database.

        Returns:
            ``OlmAccount`` object, or ``None`` if it wasn't found for the
                current device_id.

        """
        account = self._get_account()

        if not account:
            return None

        olm_account = OlmAccount.from_pickle(
            account.account, self.pickle_key, account.shared
        )

        # upgrade account pickle in database to vodozemac format
        if olm_account.upgrade_pickle:
            olm_account.upgrade_pickle = False
            self.save_account(olm_account)

        return olm_account

    @use_database
    def save_account(self, account):
        """Save the provided Olm account to the database.

        Args:
            account (OlmAccount): The olm account that will be pickled and
                saved in the database.
        """
        Accounts.insert(
            user_id=self.user_id,
            device_id=self.device_id,
            shared=account.shared,
            account=account.pickle(self.pickle_key),
        ).on_conflict_ignore().execute()

        Accounts.update(
            {
                Accounts.account: account.pickle(self.pickle_key),
                Accounts.shared: account.shared,
            }
        ).where(
            (Accounts.user_id == self.user_id) & (Accounts.device_id == self.device_id)
        ).execute()

    @use_database
    def load_sessions(self) -> SessionStore:
        """Load all Olm sessions from the database.

        Returns:
            ``SessionStore`` object, containing all the loaded sessions.

        """
        session_store = SessionStore()

        account = self._get_account()

        if not account:
            return session_store

        for s in account.olm_sessions:
            session = Session.from_pickle(s.session, s.creation_time, self.pickle_key)
            session_store.add(s.sender_key, session)

            # upgrade session pickles in database to vodozemac format
            if session.upgrade_pickle:
                session.upgrade_pickle = False
                self.save_session(s.sender_key, session)

        return session_store

    @use_database
    def save_session(self, curve_key, session):
        """Save the provided Olm session to the database.

        Args:
            curve_key (str): The curve key that owns the Olm session.
            session (Session): The Olm session that will be pickled and
                saved in the database.
        """
        account = self._get_account()
        assert account

        OlmSessions.replace(
            account=account,
            sender_key=curve_key,
            session=session.pickle(self.pickle_key),
            session_id=session.id,
            creation_time=session.creation_time,
            last_usage_date=session.use_time,
        ).execute()

    @use_database
    def load_inbound_group_sessions(self) -> GroupSessionStore:
        """Load all Olm sessions from the database.

        Returns:
            ``GroupSessionStore`` object, containing all the loaded sessions.

        """
        store = GroupSessionStore()

        account = self._get_account()

        if not account:
            return store

        for s in account.inbound_group_sessions:
            session = InboundGroupSession.from_pickle(
                s.session,
                s.fp_key,
                s.sender_key,
                s.room_id,
                self.pickle_key,
                [chain.sender_key for chain in s.forwarded_chains],
            )
            store.add(session)

            # upgrade session pickles in database to vodozemac format
            if session.upgrade_pickle:
                session.upgrade_pickle = False
                self.save_inbound_group_session(session)

        return store

    @use_database
    def save_inbound_group_session(self, session):
        """Save the provided Megolm inbound group session to the database.

        Args:
            session (InboundGroupSession): The session to save.
        """
        account = self._get_account()
        assert account

        MegolmInboundSessions.insert(
            sender_key=session.sender_key,
            account=account,
            fp_key=session.ed25519,
            room_id=session.room_id,
            session=session.pickle(self.pickle_key),
            session_id=session.id,
        ).on_conflict_ignore().execute()

        MegolmInboundSessions.update(
            {MegolmInboundSessions.session: session.pickle(self.pickle_key)}
        ).where(MegolmInboundSessions.session_id == session.id).execute()

        # TODO, use replace many here
        for chain in session.forwarding_chain:
            ForwardedChains.replace(sender_key=chain, session=session.id).execute()

    @use_database
    def load_device_keys(self) -> DeviceStore:
        """Load all the device keys from the database.

        Returns DeviceStore containing the OlmDevices with the device keys.
        """
        store = DeviceStore()
        account = self._get_account()

        if not account:
            return store

        for d in account.device_keys:
            store.add(
                OlmDevice(
                    d.user_id,
                    d.device_id,
                    {k.key_type: k.key for k in d.keys},
                    display_name=d.display_name,
                    deleted=d.deleted,
                )
            )

        return store

    @use_database_atomic
    def save_device_keys(self, device_keys):
        """Save the provided device keys to the database.

        Args:
            device_keys (Dict[str, Dict[str, OlmDevice]]): A dictionary
                containing a mapping from a user id to a dictionary containing
                a mapping of a device id to a OlmDevice.
        """
        account = self._get_account()
        assert account
        rows = []

        for user_id, devices_dict in device_keys.items():
            for device_id, device in devices_dict.items():
                rows.append(
                    {
                        "account": account,
                        "user_id": user_id,
                        "device_id": device_id,
                        "display_name": device.display_name,
                        "deleted": device.deleted,
                    }
                )

        if not rows:
            return

        for idx in range(0, len(rows), 100):
            data = rows[idx : idx + 100]
            DeviceKeys.insert_many(data).on_conflict_ignore().execute()

        for user_id, devices_dict in device_keys.items():
            for device_id, device in devices_dict.items():
                d = DeviceKeys.get(
                    (DeviceKeys.account == account)
                    & (DeviceKeys.user_id == user_id)
                    & (DeviceKeys.device_id == device_id)
                )

                d.deleted = device.deleted
                d.save()

                for key_type, key in device.keys.items():
                    Keys.replace(key_type=key_type, key=key, device=d).execute()

    @use_database
    def load_encrypted_rooms(self):
        """Load the set of encrypted rooms for this account.

        Returns:
            ``Set`` containing room ids of encrypted rooms.

        """
        account = self._get_account()

        if not account:
            return set()

        return {room.room_id for room in account.encrypted_rooms}

    @use_database
    def load_outgoing_key_requests(self):
        """Load the set of outgoing key requests for this account.

        Returns:
            ``Set`` containing request ids of key requests.

        """
        account = self._get_account()

        if not account:
            return {}

        return {
            request.request_id: OutgoingKeyRequest.from_database(request)
            for request in account.out_key_requests
        }

    @use_database
    def add_outgoing_key_request(self, key_request: OutgoingKeyRequest) -> None:
        """Add an outgoing key request to the store."""
        account = self._get_account()
        assert account

        OutgoingKeyRequests.insert(
            request_id=key_request.request_id,
            session_id=key_request.session_id,
            room_id=key_request.room_id,
            algorithm=key_request.algorithm,
            account=account,
        ).on_conflict_ignore().execute()

    @use_database
    def remove_outgoing_key_request(self, key_request: OutgoingKeyRequest) -> None:
        """Remove an active outgoing key request from the store."""
        account = self._get_account()
        assert account

        db_key_request = OutgoingKeyRequests.get_or_none(
            OutgoingKeyRequests.request_id == key_request.request_id,
            OutgoingKeyRequests.account == account,
        )

        if db_key_request:
            db_key_request.delete_instance()

    @use_database_atomic
    def save_encrypted_rooms(self, rooms):
        """Save the set of room ids for this account."""
        account = self._get_account()

        assert account

        data = [(room_id, account) for room_id in rooms]

        for idx in range(0, len(data), 400):
            rows = data[idx : idx + 400]
            EncryptedRooms.insert_many(
                rows, fields=[EncryptedRooms.room_id, EncryptedRooms.account]
            ).on_conflict_ignore().execute()

    @use_database
    def save_sync_token(self, token: str) -> None:
        """Save the current Matrix transport token."""
        account = self._get_account()
        assert account

        SyncTokens.replace(account=account, token=token).execute()

    @use_database
    def load_sync_token(self) -> str | None:
        account = self._get_account()

        if not account:
            return None

        token = SyncTokens.get_or_none(
            SyncTokens.account == account.id,
        )
        if token:
            return token.token

        return None

    @use_database_atomic
    def rewind_sync_recovery_for_startup(self, token: str | None) -> None:
        """Rewind the transport cursor without dropping unsettled callbacks."""
        account = self._get_account()
        assert account

        if token is None:
            SyncTokens.delete().where(SyncTokens.account == account).execute()
        else:
            SyncTokens.replace(account=account, token=token).execute()
        PendingTimelineEvents.delete().where(
            PendingTimelineEvents.account == account,
            PendingTimelineEvents.generation == 0,
        ).execute()
        SlidingWindowTokens.delete().where(
            SlidingWindowTokens.account == account,
        ).execute()

    @staticmethod
    def _restore_completed_markers(account, *conditions) -> None:
        PendingTimelineEvents.update(
            generation=0,
            sequence=0,
            event_payload=b"",
            is_live=False,
            was_encrypted=True,
            was_completed=False,
            admission_accepted=False,
            apply_room_state=False,
        ).where(
            PendingTimelineEvents.account == account,
            PendingTimelineEvents.generation > 0,
            PendingTimelineEvents.was_completed == True,  # noqa: E712
            *conditions,
        ).execute()

    @use_database_atomic
    def save_recovery(
        self,
        token: str | None,
        clear_rooms: set[str],
        gaps: list[RecoveryGap] | tuple[RecoveryGap, ...],
        events: list[PendingTimelineEvent] | tuple[PendingTimelineEvent, ...],
        clear_recovered: RecoveryGap | None,
        window_tokens: Mapping[str, SlidingWindowToken] | None = None,
        forgotten_rooms: Iterable[str] = (),
    ) -> None:
        account = self._get_account()
        assert account

        # Window tokens ride the same transaction as the plan they belong
        # to: a baseline stored without its plan, or the other way round,
        # would send a restarted walk from the wrong position.
        self._write_sliding_window_tokens(account, window_tokens, forgotten_rooms)

        if token:
            SyncTokens.replace(account=account, token=token).execute()
        clear_rooms = list(clear_rooms)
        for index in range(0, len(clear_rooms), _RECOVERY_WRITE_CHUNK_SIZE):
            room_batch = clear_rooms[index : index + _RECOVERY_WRITE_CHUNK_SIZE]
            self._restore_completed_markers(
                account,
                PendingTimelineEvents.room_id.in_(room_batch),
            )
            PendingTimelineEvents.delete().where(
                PendingTimelineEvents.account == account,
                PendingTimelineEvents.room_id.in_(room_batch),
                PendingTimelineEvents.generation > 0,
            ).execute()
            SyncRecoveryGaps.delete().where(
                SyncRecoveryGaps.account == account,
                SyncRecoveryGaps.room_id.in_(room_batch),
            ).execute()
        if clear_recovered:
            self._restore_completed_markers(
                account,
                PendingTimelineEvents.room_id == clear_recovered.room_id,
                PendingTimelineEvents.generation == clear_recovered.generation,
                PendingTimelineEvents.is_live == False,  # noqa: E712
            )
            PendingTimelineEvents.delete().where(
                PendingTimelineEvents.account == account,
                PendingTimelineEvents.room_id == clear_recovered.room_id,
                PendingTimelineEvents.generation == clear_recovered.generation,
                PendingTimelineEvents.is_live == False,  # noqa: E712
            ).execute()
        rows = [{"account": account, **asdict(gap)} for gap in gaps]
        for index in range(0, len(rows), _RECOVERY_WRITE_CHUNK_SIZE):
            SyncRecoveryGaps.replace_many(
                rows[index : index + _RECOVERY_WRITE_CHUNK_SIZE]
            ).execute()
        self._upsert_pending_events(account, events)

    def _recovery_payload_key(self) -> bytes:
        return hashlib.sha256(_RECOVERY_KEY_DOMAIN + self.pickle_key.encode()).digest()

    @staticmethod
    def _encrypt_recovery_payload(
        key: bytes,
        account_id: int,
        event: PendingTimelineEvent,
    ) -> bytes:
        nonce = os.urandom(_RECOVERY_NONCE_SIZE)
        cipher = AES.new(key, AES.MODE_GCM, nonce=nonce, mac_len=_RECOVERY_TAG_SIZE)
        header = bytes((_RECOVERY_PAYLOAD_VERSION,))
        cipher.update(
            header
            + _recovery_payload_aad(
                account_id,
                event.room_id,
                event.generation,
                event.event_id,
            )
        )
        payload = json.dumps(
            {"kind": event.kind, "source": event.source_json},
            separators=(",", ":"),
        ).encode()
        encrypted, tag = cipher.encrypt_and_digest(payload)
        return header + nonce + tag + encrypted

    @staticmethod
    def _decrypt_recovery_payload(
        key: bytes,
        account_id: int,
        row: PendingTimelineEvents,
    ) -> tuple[str, PendingEventKind]:
        payload = bytes(row.event_payload)
        minimum_size = 1 + _RECOVERY_NONCE_SIZE + _RECOVERY_TAG_SIZE
        if len(payload) < minimum_size or payload[0] != _RECOVERY_PAYLOAD_VERSION:
            raise ValueError("Invalid encrypted recovery payload")
        header = payload[:1]
        nonce_end = 1 + _RECOVERY_NONCE_SIZE
        tag_end = nonce_end + _RECOVERY_TAG_SIZE
        cipher = AES.new(
            key,
            AES.MODE_GCM,
            nonce=payload[1:nonce_end],
            mac_len=_RECOVERY_TAG_SIZE,
        )
        cipher.update(
            header
            + _recovery_payload_aad(
                account_id,
                row.room_id,
                row.generation,
                row.event_id,
            )
        )
        try:
            decrypted = cipher.decrypt_and_verify(
                payload[tag_end:],
                payload[nonce_end:tag_end],
            )
            decoded = json.loads(decrypted)
            kind = decoded["kind"]
            source = decoded["source"]
            if kind not in {
                "timeline",
                "ephemeral",
                "account_data",
            } or not isinstance(source, str):
                raise ValueError
            return source, kind
        except (KeyError, TypeError, UnicodeDecodeError, ValueError) as error:
            raise ValueError("Invalid encrypted recovery payload") from error

    def _upsert_pending_events(self, account, events) -> None:
        key = self._recovery_payload_key()
        rows = [
            {
                "account": account,
                "room_id": event.room_id,
                "generation": event.generation,
                "sequence": event.sequence,
                "event_id": event.event_id,
                "event_payload": (
                    b""
                    if event.generation == 0
                    else self._encrypt_recovery_payload(key, account.id, event)
                ),
                "is_live": event.is_live,
                "was_encrypted": event.was_encrypted,
                "was_completed": event.was_completed,
                "admission_accepted": event.admission_accepted,
                "provenance": event.provenance.value,
                "apply_room_state": event.apply_room_state,
            }
            for event in events
        ]
        for index in range(0, len(rows), _RECOVERY_WRITE_CHUNK_SIZE):
            promote_completed_placeholder = (
                (PendingTimelineEvents.generation == 0)
                & PendingTimelineEvents.was_encrypted
                & (~EXCLUDED.was_encrypted | EXCLUDED.was_completed)
            )
            resequence_same_generation = (PendingTimelineEvents.generation > 0) & (
                PendingTimelineEvents.generation == EXCLUDED.generation
            )
            PendingTimelineEvents.insert_many(
                rows[index : index + _RECOVERY_WRITE_CHUNK_SIZE]
            ).on_conflict(
                conflict_target=[
                    PendingTimelineEvents.account,
                    PendingTimelineEvents.room_id,
                    PendingTimelineEvents.event_id,
                ],
                update={
                    PendingTimelineEvents.generation: EXCLUDED.generation,
                    PendingTimelineEvents.sequence: EXCLUDED.sequence,
                    PendingTimelineEvents.event_payload: Case(
                        None,
                        ((promote_completed_placeholder, EXCLUDED.event_payload),),
                        PendingTimelineEvents.event_payload,
                    ),
                    PendingTimelineEvents.is_live: Case(
                        None,
                        ((promote_completed_placeholder, EXCLUDED.is_live),),
                        PendingTimelineEvents.is_live,
                    ),
                    PendingTimelineEvents.was_encrypted: Case(
                        None,
                        ((promote_completed_placeholder, EXCLUDED.was_encrypted),),
                        PendingTimelineEvents.was_encrypted,
                    ),
                    PendingTimelineEvents.was_completed: Case(
                        None,
                        ((promote_completed_placeholder, EXCLUDED.was_completed),),
                        PendingTimelineEvents.was_completed,
                    ),
                    PendingTimelineEvents.admission_accepted: Case(
                        None,
                        (
                            (
                                promote_completed_placeholder,
                                EXCLUDED.admission_accepted,
                            ),
                        ),
                        PendingTimelineEvents.admission_accepted,
                    ),
                    PendingTimelineEvents.provenance: Case(
                        None,
                        ((promote_completed_placeholder, EXCLUDED.provenance),),
                        PendingTimelineEvents.provenance,
                    ),
                    PendingTimelineEvents.apply_room_state: Case(
                        None,
                        ((promote_completed_placeholder, EXCLUDED.apply_room_state),),
                        PendingTimelineEvents.apply_room_state,
                    ),
                },
                where=promote_completed_placeholder | resequence_same_generation,
            ).execute()

    @use_database
    def load_sync_recovery(
        self,
    ) -> tuple[list[SyncRecoveryGaps], list[PendingTimelineEvent]]:
        """Load room obligations and their ordered pending callbacks."""
        from ..client.sync_recovery import PendingTimelineEvent  # noqa: PLC0415

        account = self._get_account()
        if not account:
            return [], []

        gaps = list(
            SyncRecoveryGaps.select()
            .where(SyncRecoveryGaps.account == account)
            .order_by(SyncRecoveryGaps.room_id, SyncRecoveryGaps.generation)
        )
        rows = list(
            PendingTimelineEvents.select()
            .where(PendingTimelineEvents.account == account)
            .order_by(
                PendingTimelineEvents.room_id,
                PendingTimelineEvents.generation,
                Case(
                    PendingTimelineEvents.generation,
                    ((0, PendingTimelineEvents.id),),
                    PendingTimelineEvents.sequence,
                ),
                PendingTimelineEvents.id,
            )
        )
        key = self._recovery_payload_key()
        events = []
        for row in rows:
            source_json, kind = (
                ("", "timeline")
                if row.generation == 0
                else self._decrypt_recovery_payload(key, account.id, row)
            )
            events.append(
                PendingTimelineEvent(
                    row.room_id,
                    row.generation,
                    row.sequence,
                    row.event_id,
                    source_json,
                    row.is_live,
                    row.was_encrypted,
                    row.was_completed,
                    kind,
                    row.admission_accepted,
                    TimelineEventProvenance(row.provenance),
                    row.apply_room_state,
                )
            )
        return gaps, events

    @use_database_atomic
    def accept_recovery_event(
        self,
        room_id: str,
        generation: int,
        event_id: str,
    ) -> None:
        """Durably record admission before ordinary callback fanout.

        The row is matched without its generation: after a crashed sync
        iteration the store can hold the event under a different generation
        than the running loop believes, and the event has at most one row
        either way. A missing row is tolerated for the same reason — raising
        here poisons every following sync iteration with the same error,
        while the worst case of continuing is one duplicate callback
        dispatch, which admission dedup already absorbs.
        """
        account = self._get_account()
        assert account
        updated = (
            PendingTimelineEvents.update(admission_accepted=True)
            .where(
                PendingTimelineEvents.account == account,
                PendingTimelineEvents.room_id == room_id,
                PendingTimelineEvents.generation > 0,
                PendingTimelineEvents.event_id == event_id,
            )
            .execute()
        )
        if updated != 1:
            logger.warning(
                "Pending recovery event already cleared at admission: %s "
                "(generation %d in %s)",
                event_id,
                generation,
                room_id,
            )

    @use_database
    def load_sliding_window_tokens(self) -> dict[str, SlidingWindowToken]:
        """Load each room's last sliding window token."""
        account = self._get_account()
        if not account:
            return {}
        return {
            row.room_id: SlidingWindowToken(row.token, row.membership_event_id)
            for row in SlidingWindowTokens.select().where(
                SlidingWindowTokens.account == account
            )
        }

    @use_database_atomic
    def save_sliding_window_tokens(
        self,
        tokens: Mapping[str, SlidingWindowToken],
        forgotten: Iterable[str] = (),
    ) -> None:
        """Persist window tokens, dropping the rooms that no longer have one."""
        account = self._get_account()
        assert account
        self._write_sliding_window_tokens(account, tokens, forgotten)

    def _write_sliding_window_tokens(
        self,
        account,
        tokens: Mapping[str, SlidingWindowToken] | None,
        forgotten: Iterable[str],
    ) -> None:
        """Write window tokens in chunks SQLite can bind in one statement."""
        forgotten = list(forgotten)
        for index in range(0, len(forgotten), _RECOVERY_WRITE_CHUNK_SIZE):
            SlidingWindowTokens.delete().where(
                SlidingWindowTokens.account == account,
                SlidingWindowTokens.room_id.in_(
                    forgotten[index : index + _RECOVERY_WRITE_CHUNK_SIZE]
                ),
            ).execute()

        rows = [
            {
                "account": account,
                "room_id": room_id,
                "token": token.token,
                "membership_event_id": token.membership_event_id,
            }
            for room_id, token in (tokens or {}).items()
        ]
        for index in range(0, len(rows), _RECOVERY_WRITE_CHUNK_SIZE):
            SlidingWindowTokens.replace_many(
                rows[index : index + _RECOVERY_WRITE_CHUNK_SIZE]
            ).execute()

    @use_database_atomic
    def forget_sliding_window_token(self, room_id: str) -> None:
        """Drop one room's window token, e.g. after leaving or forgetting it."""
        account = self._get_account()
        if not account:
            return
        SlidingWindowTokens.delete().where(
            SlidingWindowTokens.account == account,
            SlidingWindowTokens.room_id == room_id,
        ).execute()

    @use_database_atomic
    def finish_recovery(
        self,
        room_id: str,
        generation: int,
        event_id: str | None,
        was_encrypted: bool,
    ) -> None:
        account = self._get_account()
        assert account
        event_filter = (
            PendingTimelineEvents.account == account,
            PendingTimelineEvents.room_id == room_id,
            PendingTimelineEvents.generation == generation,
        )
        if event_id:
            # Completion must tolerate a crashed iteration's leftovers: the
            # row may already be gone, sit under another generation, or
            # already be a generation-0 marker. INSERT OR REPLACE collapses
            # every such state into the single marker row, where the old
            # fetch/delete/insert sequence raised DoesNotExist or violated
            # the UNIQUE(account_id, room_id, event_id) constraint.
            pending = PendingTimelineEvents.get_or_none(
                PendingTimelineEvents.account == account,
                PendingTimelineEvents.room_id == room_id,
                PendingTimelineEvents.event_id == event_id,
            )
            if event_id.startswith("~"):
                if pending:
                    pending.delete_instance()
                return
            # The marker mirrors record_completed_timeline_event: the first
            # completion's provenance sticks, and encryption state combines
            # with AND so an event once delivered decrypted is never
            # re-dispatched as pending decryption.
            already_completed = pending is not None and pending.generation == 0
            PendingTimelineEvents.replace(
                account=account,
                room_id=room_id,
                generation=0,
                sequence=0,
                event_id=event_id,
                event_payload=b"",
                is_live=False,
                was_encrypted=(
                    bool(pending.was_encrypted) and was_encrypted
                    if already_completed
                    else was_encrypted
                ),
                was_completed=False,
                admission_accepted=False,
                provenance=(
                    pending.provenance
                    if pending
                    else TimelineEventProvenance.HISTORY.value
                ),
                apply_room_state=False,
            ).execute()
            stale = (
                PendingTimelineEvents.select(PendingTimelineEvents.id)
                .where(
                    PendingTimelineEvents.account == account,
                    PendingTimelineEvents.room_id == room_id,
                    PendingTimelineEvents.generation == 0,
                )
                .order_by(PendingTimelineEvents.id.desc())
                .offset(512)
            )
            PendingTimelineEvents.delete().where(
                PendingTimelineEvents.id.in_(stale)
            ).execute()
            return
        PendingTimelineEvents.delete().where(*event_filter).execute()
        SyncRecoveryGaps.delete().where(
            SyncRecoveryGaps.account == account,
            SyncRecoveryGaps.room_id == room_id,
            SyncRecoveryGaps.generation == generation,
        ).execute()

    @use_database
    def delete_encrypted_room(self, room: str) -> None:
        """Delete an encrypted room from the store."""
        db_room = EncryptedRooms.get_or_none(EncryptedRooms.room_id == room)
        if db_room:
            db_room.delete_instance()

    def blacklist_device(self, device: OlmDevice) -> bool:
        """Mark a device as blacklisted.

        Args:
            device (OlmDevice): The device that will be marked as blacklisted

        Returns True if the device was blacklisted, False otherwise, e.g. if
        the device was already blacklisted.

        """
        raise NotImplementedError

    def unblacklist_device(self, device: OlmDevice) -> bool:
        """Unmark a device as blacklisted.

        Args:
            device (OlmDevice): The device that will be unmarked as blacklisted

        """
        raise NotImplementedError

    def verify_device(self, device: OlmDevice) -> bool:
        """Mark a device as verified.

        Args:
            device (OlmDevice): The device that will be marked as verified

        Returns True if the device was verified, False otherwise, e.g. if the
        device was already verified.

        """
        raise NotImplementedError

    def is_device_verified(self, device: OlmDevice) -> bool:
        """Check if a device is verified.

        Args:
            device (OlmDevice): The device that will be checked if it's
                verified.
        """
        raise NotImplementedError

    def is_device_blacklisted(self, device: OlmDevice) -> bool:
        """Check if a device is blacklisted.

        Args:
            device (OlmDevice): The device that will be checked if it's
                blacklisted.
        """
        raise NotImplementedError

    def unverify_device(self, device: OlmDevice) -> bool:
        """Unmark a device as verified.

        Args:
            device (OlmDevice): The device that will be unmarked as verified

        Returns True if the device was unverified, False otherwise, e.g. if the
        device wasn't verified.

        """
        raise NotImplementedError

    def ignore_device(self, device: OlmDevice) -> bool:
        """Mark a device as ignored.

        Args:
            device (OlmDevice): The device that will be marked as blacklisted

        Returns True if the device was ignored, False otherwise, e.g. if
        the device was already ignored.
        """
        raise NotImplementedError

    def unignore_device(self, device: OlmDevice) -> bool:
        """Unmark a device as ignored.

        Args:
            device (OlmDevice): The device that will be marked as blacklisted

        Returns True if the device was unignored, False otherwise, e.g. if the
        device wasn't ignored in the first place.
        """
        raise NotImplementedError

    def ignore_devices(self, devices: list[OlmDevice]) -> None:
        """Mark a list of devices as ignored.

        This is a more efficient way to mark multiple devices as ignored.

        Args:
            devices (list[OlmDevice]): A list of OlmDevices that will be marked
                as ignored.

        """
        raise NotImplementedError

    def is_device_ignored(self, device: OlmDevice) -> bool:
        """Check if a device is ignored.

        Args:
            device (OlmDevice): The device that will be checked if it's
                ignored.
        """
        raise NotImplementedError


@dataclass
class DefaultStore(MatrixStore):
    """The default nio Matrix Store.

    This store uses an Sqlite database as the main storage format while device
    trust state is stored in plaintext files using a format similar to the ssh
    known_hosts file format. The files will be created in the same directory as
    the main Sqlite database.

    One such file is created for each of the 3 valid states (verified,
    blacklisted, ignored). If a device isn't found in any of those files the
    verification state is considered to be unset.

    Args:
        user_id (str): The fully-qualified ID of the user that owns the store.
        device_id (str): The device id of the user's device.
        store_path (str): The path where the store should be stored.
        pickle_key (str, optional): A passphrase that will be used to encrypt
            encryption keys while they are in storage.
        database_name (str, optional): The file-name of the database that
            should be used.
    """

    trust_db: KeyStore = field(init=False)
    blacklist_db: KeyStore = field(init=False)

    def __post_init__(self):
        super().__post_init__()

        trust_file_path = f"{self.user_id}_{self.device_id}.trusted_devices"
        self.trust_db = KeyStore(os.path.join(self.store_path, trust_file_path))

        blacklist_file_path = f"{self.user_id}_{self.device_id}.blacklisted_devices"
        self.blacklist_db = KeyStore(os.path.join(self.store_path, blacklist_file_path))

        ignore_file_path = f"{self.user_id}_{self.device_id}.ignored_devices"
        self.ignore_db = KeyStore(os.path.join(self.store_path, ignore_file_path))

    def blacklist_device(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)
        self.trust_db.remove(key)
        self.ignore_db.remove(key)
        device.trust_state = TrustState.blacklisted
        return self.blacklist_db.add(key)

    def unblacklist_device(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)

        if self.blacklist_db.remove(key):
            device.trust_state = TrustState.unset
            return True

        return False

    def verify_device(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)
        self.blacklist_db.remove(key)
        self.ignore_db.remove(key)
        device.trust_state = TrustState.verified
        return self.trust_db.add(key)

    def is_device_verified(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)
        return key in self.trust_db

    def is_device_blacklisted(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)
        return key in self.blacklist_db

    def unverify_device(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)

        if self.trust_db.remove(key):
            device.trust_state = TrustState.unset
            return True

        return False

    def ignore_device(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)
        self.blacklist_db.remove(key)
        self.trust_db.remove(key)
        device.trust_state = TrustState.ignored
        return self.ignore_db.add(key)

    def unignore_device(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)

        if self.ignore_db.remove(key):
            device.trust_state = TrustState.unset
            return True

        return False

    def ignore_devices(self, devices: list[OlmDevice]) -> None:
        keys = [Key.from_olmdevice(device) for device in devices]

        self.blacklist_db.remove_many(keys)
        self.trust_db.remove_many(keys)
        self.ignore_db.add_many(keys)

        for device in devices:
            device.trust_state = TrustState.ignored

        return

    def is_device_ignored(self, device: OlmDevice) -> bool:
        key = Key.from_olmdevice(device)
        return key in self.ignore_db

    @use_database
    def load_device_keys(self) -> DeviceStore:
        store = DeviceStore()
        account = self._get_account()

        if not account:
            return store

        for d in account.device_keys:
            device = OlmDevice(
                d.user_id,
                d.device_id,
                {k.key_type: k.key for k in d.keys},
                display_name=d.display_name,
                deleted=d.deleted,
            )

            trust_state = TrustState.unset
            key = Key.from_olmdevice(device)

            if key in self.trust_db:
                trust_state = TrustState.verified
            elif key in self.blacklist_db:
                trust_state = TrustState.blacklisted
            elif key in self.ignore_db:
                trust_state = TrustState.ignored

            device.trust_state = trust_state

            store.add(device)

        return store


@dataclass
class SqliteStore(MatrixStore):
    """The Sqlite only nio Matrix Store.

    This store uses an Sqlite database as the main storage format as well as
    the store format for the trust state.

    Args:
        user_id (str): The fully-qualified ID of the user that owns the store.
        device_id (str): The device id of the user's device.
        store_path (str): The path where the store should be stored.
        pickle_key (str, optional): A passphrase that will be used to encrypt
            encryption keys while they are in storage.
        database_name (str, optional): The file-name of the database that
            should be used.
    """

    models = MatrixStore.models + [DeviceTrustState]

    def _get_device(self, device):
        acc = self._get_account()

        if not acc:
            return None

        try:
            return DeviceKeys.get(
                DeviceKeys.user_id == device.user_id,
                DeviceKeys.device_id == device.id,
                DeviceKeys.account == acc,
            )
        except DoesNotExist:
            return None

    @use_database
    def verify_device(self, device: OlmDevice) -> bool:
        if self.is_device_verified(device):
            return False

        d = self._get_device(device)
        assert d

        DeviceTrustState.replace(device=d, state=TrustState.verified).execute()

        device.trust_state = TrustState.verified

        return True

    @use_database
    def unverify_device(self, device: OlmDevice) -> bool:
        if not self.is_device_verified(device):
            return False

        d = self._get_device(device)
        assert d

        DeviceTrustState.replace(device=d, state=TrustState.unset).execute()

        device.trust_state = TrustState.unset

        return True

    @use_database
    def is_device_verified(self, device: OlmDevice) -> bool:
        d = self._get_device(device)

        if not d:
            return False

        try:
            trust_state = d.trust_state[0].state
        except IndexError:
            return False

        return trust_state == TrustState.verified

    @use_database
    def blacklist_device(self, device: OlmDevice) -> bool:
        if self.is_device_blacklisted(device):
            return False

        d = self._get_device(device)
        assert d

        DeviceTrustState.replace(device=d, state=TrustState.blacklisted).execute()

        device.trust_state = TrustState.blacklisted

        return True

    @use_database
    def unblacklist_device(self, device: OlmDevice) -> bool:
        if not self.is_device_blacklisted(device):
            return False

        d = self._get_device(device)
        assert d

        DeviceTrustState.replace(device=d, state=TrustState.unset).execute()

        device.trust_state = TrustState.unset

        return True

    @use_database
    def is_device_blacklisted(self, device: OlmDevice) -> bool:
        d = self._get_device(device)

        if not d:
            return False

        try:
            trust_state = d.trust_state[0].state
        except IndexError:
            return False

        return trust_state == TrustState.blacklisted

    @use_database
    def ignore_device(self, device: OlmDevice) -> bool:
        if self.is_device_ignored(device):
            return False

        d = self._get_device(device)
        assert d

        DeviceTrustState.replace(device=d, state=TrustState.ignored).execute()

        device.trust_state = TrustState.ignored

        return True

    @use_database
    def unignore_device(self, device: OlmDevice) -> bool:
        if not self.is_device_ignored(device):
            return False

        d = self._get_device(device)
        assert d

        DeviceTrustState.replace(device=d, state=TrustState.unset).execute()

        device.trust_state = TrustState.unset

        return True

    def _legacy_get_device_ids(self, account, devices):
        device_ids = []

        for device in devices:
            d = DeviceKeys.get_or_none(
                DeviceKeys.account == account.id,
                DeviceKeys.user_id == device.user_id,
                DeviceKeys.device_id == device.id,
            )

            assert d

            device_ids.append(d.id)

        return device_ids

    def _get_device_ids(self, account, devices):
        device_ids = []

        tuple_values = [(d.user_id, d.id) for d in devices]
        values = [item for sublist in tuple_values for item in sublist]

        for idx in range(0, len(values), 300):
            data = values[idx : idx + 300]

            query_string = (
                "SELECT devicekeys.* from devicekeys "
                "JOIN accounts ON devicekeys.account_id=accounts.id "
                "WHERE accounts.id == ? AND "
                "(devicekeys.user_id, devicekeys.device_id) IN "
                f"(VALUES {','.join(['(?, ?)'] * (len(data) // 2))})"
            )

            query = DeviceKeys.raw(query_string, account.id, *data)

            device_ids += [device_key.id for device_key in query]

        return device_ids

    @use_database_atomic
    def ignore_devices(self, devices: list[OlmDevice]) -> None:
        acc = self._get_account()

        if not acc:
            return None

        if sqlite3.sqlite_version_info >= (3, 15, 2):
            device_ids = self._get_device_ids(acc, devices)
        else:
            device_ids = self._legacy_get_device_ids(acc, devices)

        rows = [
            {"device_id": device_id, "state": TrustState.ignored}
            for device_id in device_ids
        ]

        assert len(rows) == len(devices)

        for idx in range(0, len(rows), 100):
            trust_data = rows[idx : idx + 100]
            DeviceTrustState.replace_many(trust_data).execute()

        for device in devices:
            device.trust_state = TrustState.ignored

    @use_database
    def is_device_ignored(self, device: OlmDevice) -> bool:
        d = self._get_device(device)

        if not d:
            return False

        try:
            trust_state = d.trust_state[0].state
        except IndexError:
            return False

        return trust_state == TrustState.ignored

    @use_database
    def load_device_keys(self) -> DeviceStore:
        store = DeviceStore()
        account = self._get_account()

        if not account:
            return store

        for d in account.device_keys:
            try:
                trust_state = d.trust_state[0].state
            except IndexError:
                trust_state = TrustState.unset

            store.add(
                OlmDevice(
                    d.user_id,
                    d.device_id,
                    {k.key_type: k.key for k in d.keys},
                    display_name=d.display_name,
                    deleted=d.deleted,
                    trust_state=trust_state,
                )
            )

        return store


class SqliteMemoryStore(SqliteStore):
    """The Sqlite only nio Matrix Store.

    This store uses a Sqlite database as the main storage format as well as
    the store format for the trust state. The Sqlite database will be stored
    only in memory and all the data will be lost after the object is deleted.

    Args:
        user_id (str): The fully-qualified ID of the user that owns the store.
        device_id (str): The device id of the user's device.
        pickle_key (str, optional): A passphrase that will be used to encrypt
            encryption keys while they are in storage.
    """

    def __init__(self, user_id, device_id, pickle_key=""):
        super().__init__(user_id, device_id, "", pickle_key=pickle_key)

    def _create_database(self):
        return SqliteDatabase(
            ":memory:",
            pragmas={
                "foreign_keys": 1,
                "secure_delete": 1,
            },
        )
