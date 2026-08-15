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

import time
from datetime import datetime

from peewee import (
    SQL,
    BlobField,
    BooleanField,
    ForeignKeyField,
    IntegerField,
    Model,
    TextField,
)

from ..crypto import TrustState


class ByteField(BlobField):
    def python_value(self, value):  # pragma: no cover
        if isinstance(value, bytes):
            return value

        return bytes(value, "utf-8")

    def db_value(self, value):  # pragma: no cover
        if isinstance(value, bytearray):
            return bytes(value)

        return value


class DeviceTrustField(IntegerField):
    """Database field to hold a TrustState enum value."""

    def python_value(self, value):  # pragma: no cover
        return TrustState(value)

    def db_value(self, value):  # pragma: no cover
        return value.value


# Please don't remove this.
# This is a workaround for this bug: https://bugs.python.org/issue27400
class DateField(TextField):
    def python_value(self, value):  # pragma: no cover
        format = "%Y-%m-%d %H:%M:%S.%f"
        try:
            return datetime.strptime(value, format)
        except TypeError:
            return datetime(*(time.strptime(value, format)[0:6]))

    def db_value(self, value):  # pragma: no cover
        return value.strftime("%Y-%m-%d %H:%M:%S.%f")


class StoreVersion(Model):
    version = IntegerField()


class Accounts(Model):
    account = ByteField()
    user_id = TextField()
    device_id = TextField()
    shared = BooleanField()

    class Meta:
        constraints = [SQL("UNIQUE(user_id,device_id)")]


class OlmSessions(Model):
    creation_time = DateField()
    last_usage_date = DateField()
    sender_key = TextField()
    account = ForeignKeyField(
        model=Accounts, backref="olm_sessions", on_delete="CASCADE"
    )
    session = ByteField()
    session_id = TextField(primary_key=True)


class DeviceKeys_v1(Model):
    sender_key = TextField()
    deleted = BooleanField()
    account = ForeignKeyField(
        model=Accounts,
        column_name="account_id",
        backref="device_keys",
        on_delete="CASCADE",
    )
    fp_key = TextField()
    device_id = TextField()
    user_id = TextField()

    class Meta:
        constraints = [SQL("UNIQUE(account_id,user_id,device_id)")]
        table_name = "devicekeys"


class DeviceKeys(Model):
    device_id = TextField()
    user_id = TextField()
    display_name = TextField(default="")
    deleted = BooleanField()
    account = ForeignKeyField(
        model=Accounts,
        column_name="account_id",
        backref="device_keys",
        on_delete="CASCADE",
    )

    class Meta:
        constraints = [SQL("UNIQUE(account_id,user_id,device_id)")]


class Keys(Model):
    key_type = TextField()
    key = TextField()
    device = ForeignKeyField(
        model=DeviceKeys,
        column_name="device_id",
        backref="keys",
    )

    class Meta:
        constraints = [SQL("UNIQUE(device_id,key_type)")]


class DeviceTrustState(Model):
    state = DeviceTrustField()
    device = ForeignKeyField(
        model=DeviceKeys,
        primary_key=True,
        backref="trust_state",
        column_name="device_id",
    )


class MegolmInboundSessions(Model):
    sender_key = TextField()
    account = ForeignKeyField(
        model=Accounts,
        backref="inbound_group_sessions",
        on_delete="CASCADE",
    )
    fp_key = TextField()
    room_id = TextField()
    session = ByteField()
    session_id = TextField(primary_key=True)


class ForwardedChains(Model):
    sender_key = TextField()
    session = ForeignKeyField(
        model=MegolmInboundSessions,
        column_name="session_id",
        backref="forwarded_chains",
        on_delete="CASCADE",
    )

    class Meta:
        constraints = [SQL("UNIQUE(sender_key,session_id)")]


class EncryptedRooms(Model):
    room_id = TextField()
    account = ForeignKeyField(
        model=Accounts,
        column_name="account_id",
        on_delete="CASCADE",
        backref="encrypted_rooms",
    )

    class Meta:
        constraints = [SQL("UNIQUE(room_id,account_id)")]


class OutgoingKeyRequests(Model):
    request_id = TextField()
    session_id = TextField()
    room_id = TextField()
    algorithm = TextField()
    account = ForeignKeyField(
        model=Accounts,
        column_name="account_id",
        on_delete="CASCADE",
        backref="out_key_requests",
    )

    class Meta:
        constraints = [SQL("UNIQUE(request_id,account_id)")]


class SyncTokens(Model):
    token = TextField()
    account = ForeignKeyField(
        model=Accounts,
        on_delete="CASCADE",
        backref="sync_token",
    )

    class Meta:
        constraints = [SQL("UNIQUE(account_id)")]


class SyncRecoveryGaps(Model):
    room_id = TextField()
    generation = IntegerField()
    target_token = TextField()
    cursor_token = TextField(null=True)
    membership_bound = BooleanField(default=False)
    account = ForeignKeyField(
        model=Accounts,
        on_delete="CASCADE",
        backref="sync_recovery_gaps",
    )

    class Meta:
        constraints = [SQL("UNIQUE(account_id,room_id,generation)")]


class SyncRecoveryAbandonedRooms(Model):
    """Rooms whose limited-timeline gap was given up on.

    Kept apart from ``SyncRecoveryGaps`` because abandonment outlives the gap
    row it came from: the gap is deleted when the walk is abandoned, and the
    record that work was lost has to survive that deletion or the room silently
    reads as healthy on the next response.
    """

    room_id = TextField()
    # Why the walk was given up on. A row that reaches the store without one
    # records a loss whose cause nio never captured, and saying so is honest
    # where guessing a cause would be indistinguishable from a real finding.
    reason = TextField(default="unknown")
    account = ForeignKeyField(
        model=Accounts,
        on_delete="CASCADE",
        backref="sync_recovery_abandoned_rooms",
    )

    class Meta:
        constraints = [SQL("UNIQUE(account_id,room_id,reason)")]


class PendingTimelineEvents(Model):
    room_id = TextField()
    generation = IntegerField()
    sequence = IntegerField()
    event_id = TextField()
    event_payload = ByteField()
    is_live = BooleanField()
    was_encrypted = BooleanField()
    was_completed = BooleanField()
    admission_accepted = BooleanField(default=False)
    provenance = TextField(default="live")
    apply_room_state = BooleanField(default=True)
    account = ForeignKeyField(
        model=Accounts,
        on_delete="CASCADE",
        backref="pending_timeline_events",
    )

    class Meta:
        constraints = [SQL("UNIQUE(account_id,room_id,event_id)")]


class SlidingWindowTokens(Model):
    """The /messages token a room's last sliding window handed out.

    Simplified Sliding Sync positions are connection-scoped, so a limited
    window can only be recovered from the token the room's previous window
    carried. Keeping it here is what lets a restarted client walk the gap
    its downtime left behind.
    """

    room_id = TextField()
    token = TextField()
    membership_event_id = TextField()
    account = ForeignKeyField(
        model=Accounts,
        on_delete="CASCADE",
        backref="sliding_window_tokens",
    )

    class Meta:
        constraints = [SQL("UNIQUE(account_id,room_id)")]


class TrackedUsers(Model):
    user_id = TextField()
    account = ForeignKeyField(
        model=Accounts,
        column_name="account_id",
        on_delete="CASCADE",
        backref="tracked_users",
    )

    class Meta:
        constraints = [SQL("UNIQUE(account_id,user_id)")]
