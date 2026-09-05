from contextlib import contextmanager
from dataclasses import replace
import json
from pathlib import Path
from typing import Iterator
from uuid import UUID

import pytest

import nio
from nio import AsyncClient, AsyncClientConfig
from nio.crypto import Olm, OlmDevice
from nio.event_builders import DummyMessage, RoomKeyRequestMessage, ToDeviceMessage
from nio.events import (
    Event,
    KeyVerificationCancel,
    MegolmEvent,
    RoomKeyRequest,
    ToDeviceEvent,
)
from nio.exceptions import LocalProtocolError
from nio.ingest.classic import ClassicSource
from nio.ingest.config import ClassicSourceConfig, SlidingSourceConfig
from nio.ingest.model import RecordKind, TransportKind
from nio.ingest.ports import NetworkRequest, NetworkResult
from nio.ingest.reducer import RoomContinuity
from nio.ingest.sliding import (
    RESERVED_ALL_ROOMS_LIST,
    SlidingCursor,
    SlidingRangeAckMode,
    SlidingSource,
    canonical_sliding_cursor,
)
from nio.ingest.source import (
    ClassicCursor,
    SyncFrame,
    canonical_classic_cursor,
    canonical_json,
)
from nio.ingest.state import SourceState
from nio.store import SqliteMemoryStore, SqliteStore
from nio.rooms import MatrixInvitedRoom, MatrixRoom
import nio.store.sync_journal as bootstrap_api
import nio.ingest as ingest_api
import nio.ingest.model as ingest_model

ACCOUNT = "@alice:example.org"
DEVICE = "ALICE"
BOB = "@bob:example.org"
BOB_DEVICE = "BOB"
ROOM = "!room:example.org"
INVITE_ROOM = "!invite:example.org"
STREAM = UUID("34e80daa-8fe8-45aa-bb79-bd2c2496b99f")
CONNECTION = UUID("c31f578f-e556-48a8-9e28-81161261037d")
CONSUMER = UUID("22222222-2222-4222-8222-222222222222")
PICKLE_KEY = "preparation-secret"
CLASSIC_CONFIG = ClassicSourceConfig(30_000, b"{}")
SLIDING_CONFIG = SlidingSourceConfig(30_000, "worker", b"{}", b"{}", b"{}", 2)


def _event(
    event_type: str, content: object | None = None, **fields: object
) -> dict[str, object]:
    return {"content": {} if content is None else content, "type": event_type, **fields}


def _network_result(request: NetworkRequest, body: dict[str, object]) -> NetworkResult:
    return NetworkResult(
        request.stream_id,
        request.transport,
        request.source_epoch,
        request.request_id,
        200,
        json.dumps(body).encode(),
        None,
        None,
    )


def _normalize_frame(
    transport: TransportKind,
    request_id: int,
    body: dict[str, object],
) -> SyncFrame:
    if transport is TransportKind.CLASSIC:
        source = ClassicSource(STREAM, CLASSIC_CONFIG, ACCOUNT)
        cursor = canonical_classic_cursor(ClassicCursor("s0"))
    else:
        source = SlidingSource(STREAM, SLIDING_CONFIG, ACCOUNT)
        cursor = canonical_sliding_cursor(
            SlidingCursor(
                "p0",
                "td0",
                CONNECTION,
                "worker",
                1,
                2,
                SlidingRangeAckMode.TXN_ECHO,
                False,
            )
        )
    request = source.plan_request(
        SourceState(2, transport, cursor, request_id, True),
        request_id,
    )
    assert request is not None
    if transport is TransportKind.CLASSIC:
        payload = {"next_batch": f"s{request_id}", **body}
    else:
        assert request.body is not None
        payload = {
            "lists": {RESERVED_ALL_ROOMS_LIST: {"count": 1}},
            "pos": f"p{request_id}",
            "txn_id": json.loads(request.body)["txn_id"],
            **body,
        }
    result = source.normalize(request, _network_result(request, payload))
    assert result.frame is not None
    return result.frame


def _equivalent_frames() -> tuple[SyncFrame, SyncFrame]:
    ordinary_to_device = _event(
        "org.example.ordinary", {"value": "ordinary"}, sender=BOB
    )
    invite_member = _event(
        "m.room.member",
        {"displayname": "Invited Alice", "membership": "invite"},
        event_id="$invite",
        sender=BOB,
        state_key=ACCOUNT,
    )
    member = _event(
        "m.room.member",
        {"displayname": "Alice", "membership": "join"},
        event_id="$member",
        origin_server_ts=1,
        sender=ACCOUNT,
        state_key=ACCOUNT,
    )
    message = _event(
        "m.room.message",
        {"body": "hello", "msgtype": "m.text"},
        event_id="$message",
        origin_server_ts=2,
        sender=BOB,
    )
    typing = _event("m.typing", {"user_ids": [BOB]})
    room_account_data = _event("m.tag", {"tags": {"u.work": {"order": 1}}})
    global_account_data = _event("org.example.theme", {"theme": "dark"})
    presence = _event(
        "m.presence",
        {
            "currently_active": True,
            "last_active_ago": 0,
            "presence": "online",
            "status_msg": "ready",
        },
        sender=BOB,
    )

    classic_frame = _normalize_frame(
        TransportKind.CLASSIC,
        4,
        {
            "account_data": {"events": [global_account_data]},
            "presence": {"events": [presence]},
            "rooms": {
                "invite": {INVITE_ROOM: {"invite_state": {"events": [invite_member]}}},
                "join": {
                    ROOM: {
                        "account_data": {"events": [room_account_data]},
                        "ephemeral": {"events": [typing]},
                        "state": {"events": [member]},
                        "timeline": {"events": [message]},
                    }
                },
            },
            "to_device": {"events": [ordinary_to_device]},
        },
    )
    sliding_frame = _normalize_frame(
        TransportKind.SLIDING,
        4,
        {
            "extensions": {
                "account_data": {
                    "global": [global_account_data],
                    "rooms": {ROOM: [room_account_data]},
                },
                "presence": {"events": [presence]},
                "to_device": {
                    "events": [ordinary_to_device],
                    "next_batch": "td1",
                },
                "typing": {"rooms": {ROOM: typing}},
            },
            "lists": {RESERVED_ALL_ROOMS_LIST: {"count": 2}},
            "rooms": {
                INVITE_ROOM: {"invite_state": [invite_member]},
                ROOM: {
                    "membership": "join",
                    "required_state": [member],
                    "timeline": [message],
                    "num_live": 1,
                },
            },
        },
    )
    return classic_frame, sliding_frame


def _multi_transition_frame() -> SyncFrame:
    def member(membership: str, event_id: str | None) -> dict[str, object]:
        event = _event(
            "m.room.member",
            {"membership": membership},
            origin_server_ts=1,
            sender=ACCOUNT,
            state_key=ACCOUNT,
        )
        if event_id is not None:
            event["event_id"] = event_id
        return event

    return _normalize_frame(
        TransportKind.SLIDING,
        5,
        {
            "rooms": {
                ROOM: {
                    "membership": "ban",
                    "required_state": [member("join", "$join")],
                    "timeline": [
                        member("leave", "$leave"),
                        member("join", None),
                        member("ban", "$ban"),
                    ],
                    "num_live": 2,
                }
            }
        },
    )


def _same_frame_crypto_material(
    recipient: AsyncClient,
    *,
    clear_event: dict[str, object] | None = None,
    event_id: str = "$secret",
    malformed_to_device: bool = False,
) -> tuple[dict[str, object], dict[str, object], SqliteMemoryStore]:
    assert recipient.olm is not None
    sender_store = SqliteMemoryStore(BOB, BOB_DEVICE, "sender-secret")
    sender = Olm(BOB, BOB_DEVICE, sender_store)
    recipient_device = OlmDevice(
        ACCOUNT,
        DEVICE,
        recipient.olm.account.identity_keys,
    )
    sender_device = OlmDevice(BOB, BOB_DEVICE, sender.account.identity_keys)
    sender.store.save_device_keys({ACCOUNT: {DEVICE: recipient_device}})
    sender.device_store.add(recipient_device)
    sender.verify_device(recipient_device)
    recipient.store.save_device_keys({BOB: {BOB_DEVICE: sender_device}})
    recipient.olm.device_store.add(sender_device)
    recipient.olm.verify_device(sender_device)

    recipient.olm.account.generate_one_time_keys(1)
    one_time_key = next(
        iter(recipient.olm.account.one_time_keys["curve25519"].values())
    )
    recipient.olm.account.mark_keys_as_published()
    sender.create_session(one_time_key, recipient_device.curve25519)
    malformed_content = None
    if malformed_to_device:
        session = sender.session_store.get(recipient_device.curve25519)
        assert session is not None
        malformed_content = sender._olm_encrypt(
            session,
            recipient_device,
            "m.room_key",
            {},
        )
    _sharing_with, to_device = sender.share_group_session(
        ROOM,
        [ACCOUNT],
    )
    sender.outbound_group_sessions[ROOM].shared = True
    olm_content = malformed_content or to_device["messages"][ACCOUNT][DEVICE]
    olm_event = _event("m.room.encrypted", olm_content, sender=BOB)
    encrypted_content = sender.group_encrypt(
        ROOM,
        clear_event
        or {
            "content": {"body": "same-frame secret", "msgtype": "m.text"},
            "type": "m.room.message",
        },
    )
    timeline_event = _event(
        "m.room.encrypted",
        encrypted_content,
        event_id=event_id,
        origin_server_ts=3,
        sender=BOB,
    )
    return olm_event, timeline_event, sender_store


def _crypto_frame(
    transport: TransportKind,
    to_device: dict[str, object] | None,
    timeline: dict[str, object],
    *,
    device_lists: dict[str, object] | None = None,
    key_counts: dict[str, object] | None = None,
) -> SyncFrame:
    state = [
        _event(
            "m.room.encryption",
            {"algorithm": "m.megolm.v1.aes-sha2"},
            event_id="$encryption",
            origin_server_ts=1,
            sender=ACCOUNT,
            state_key="",
        ),
        *(
            _event(
                "m.room.member",
                {"membership": "join"},
                event_id=f"$member-{user_id}",
                origin_server_ts=2,
                sender=user_id,
                state_key=user_id,
            )
            for user_id in (ACCOUNT, BOB)
        ),
    ]
    if transport is TransportKind.CLASSIC:
        body = {
            "device_lists": device_lists or {"changed": [], "left": []},
            "device_one_time_keys_count": key_counts or {},
            "rooms": {
                "join": {
                    ROOM: {
                        "state": {"events": state},
                        "timeline": {"events": [timeline]},
                    }
                }
            },
            "to_device": {"events": [] if to_device is None else [to_device]},
        }
    else:
        body = {
            "extensions": {
                "e2ee": {
                    "device_lists": device_lists or {"changed": [], "left": []},
                    "device_one_time_keys_count": key_counts or {},
                },
                "to_device": {
                    "events": [] if to_device is None else [to_device],
                    "next_batch": "td2",
                },
            },
            "rooms": {
                ROOM: {
                    "membership": "join",
                    "num_live": 1,
                    "required_state": state,
                    "timeline": [timeline],
                }
            },
        }
    return _normalize_frame(transport, 6, body)


def _room_key_request(
    action: str,
    request_id: str,
) -> dict[str, object]:
    content: dict[str, object] = {
        "action": action,
        "request_id": request_id,
        "requesting_device_id": BOB_DEVICE,
    }
    if action == "request":
        content["body"] = {
            "algorithm": "m.megolm.v1.aes-sha2",
            "room_id": ROOM,
            "sender_key": "sender-key",
            "session_id": "session-id",
        }
    return _event("m.room_key_request", content, sender=BOB)


def _to_device_frame(
    events: list[dict[str, object]],
    key_counts: dict[str, object] | None = None,
    fallback: list[str] | None = None,
) -> SyncFrame:
    return _normalize_frame(
        TransportKind.CLASSIC,
        8,
        {
            "device_one_time_keys_count": key_counts or {},
            **(
                {"device_unused_fallback_key_types": fallback}
                if fallback is not None
                else {}
            ),
            "to_device": {"events": events},
        },
    )


def _megolm_event_source(
    event_id: str,
    session_id: str,
) -> dict[str, object]:
    return _event(
        "m.room.encrypted",
        {
            "algorithm": "m.megolm.v1.aes-sha2",
            "ciphertext": "ciphertext",
            "device_id": BOB_DEVICE,
            "sender_key": "sender-key",
            "session_id": session_id,
        },
        event_id=event_id,
        origin_server_ts=9,
        sender=BOB,
    )


def _megolm_event(
    event_id: str, session_id: str
) -> tuple[dict[str, object], MegolmEvent]:
    source = _megolm_event_source(event_id, session_id)
    event = Event.parse_event(source)
    assert isinstance(event, MegolmEvent)
    event.room_id = ROOM
    return source, event


@contextmanager
def _attached_client(root: Path) -> Iterator[AsyncClient]:
    root.mkdir()
    bootstrap = bootstrap_api._open_fresh_ingestion_store(
        root,
        account_id=ACCOUNT,
        device_id=DEVICE,
        consumer_generation=CONSUMER,
        source=CLASSIC_CONFIG,
        pickle_key=PICKLE_KEY,
        database_name="owned.db",
    )
    store = bootstrap._open_owned_store_candidate()._store_for_attachment()
    client = AsyncClient(
        "https://example.org",
        ACCOUNT,
        DEVICE,
        config=AsyncClientConfig(store=SqliteStore, pickle_key=PICKLE_KEY),
    )
    client.restore_login(ACCOUNT, DEVICE, "secret-token")
    client._attach_ingestion_store(store)

    def callback_tripwire(*_args: object) -> None:
        raise AssertionError("ingestion preparation invoked a callback")

    for callback_name in (
        "_on_to_device",
        "_on_invited_rooms",
        "_on_event",
        "_on_ephemeral",
        "_on_room_account_data",
        "_on_presence",
        "_on_global_account_data",
        "_on_expired_verifications",
    ):
        setattr(client, callback_name, callback_tripwire)
    client.add_event_callback(callback_tripwire, None)
    client.add_ephemeral_callback(callback_tripwire, None)
    client.add_global_account_data_callback(callback_tripwire, None)
    client.add_room_account_data_callback(callback_tripwire, None)
    client.add_to_device_callback(callback_tripwire, None)
    client.add_presence_callback(callback_tripwire, None)
    client.add_response_callback(callback_tripwire, None)
    try:
        yield client
    finally:
        client._detach_ingestion_store(store)
        bootstrap.close()


def _semantic_output(prepared: object) -> object:
    return (
        tuple(
            _attrs(
                record,
                "kind",
                "room_id",
                "event_id",
                "source_json",
                "clear_json",
                "decryption",
                "decryption_verified",
                "callback_route",
            )
            for record in prepared.records
        ),
        tuple(
            _attrs(
                transition,
                "room_id",
                "event_id",
                "previous_membership",
                "current_membership",
                "previous_epoch",
                "current_epoch",
                "source_json",
            )
            for transition in prepared.membership_transitions
        ),
        prepared.room_snapshots,
        tuple(
            _attrs(record, "callback_route", "room_id", "source_json", "clear_json")
            for record in _callback_records(prepared)
        ),
        prepared.crypto_delta,
    )


def _callback_records(prepared: object) -> tuple[object, ...]:
    return tuple(
        record for record in prepared.records if record.callback_route is not None
    )


def _attrs(value: object, *names: str) -> tuple[object, ...]:
    return tuple(getattr(value, name) for name in names)


def test_equivalent_frames_prepare_transport_neutral_replay_stable_output(
    tmp_path: Path,
) -> None:
    classic_frame, sliding_frame = _equivalent_frames()

    with (
        _attached_client(tmp_path / "classic") as classic_client,
        _attached_client(tmp_path / "sliding") as sliding_client,
        _attached_client(tmp_path / "replay") as replay_client,
    ):
        classic = classic_client._prepare_ingestion_frame(classic_frame, 9, ())
        sliding = sliding_client._prepare_ingestion_frame(sliding_frame, 9, ())
        replay = replay_client._prepare_ingestion_frame(classic_frame, 9, ())

    assert _semantic_output(classic) == _semantic_output(sliding)
    assert tuple(record.record_id for record in classic.records) == tuple(
        record.record_id for record in replay.records
    )
    assert tuple(
        (
            record.event_id,
            record.kind,
            record.room_id,
            record.callback_route.value if record.callback_route else None,
        )
        for record in classic.records
    ) == (
        (None, RecordKind.TO_DEVICE, None, "to_device"),
        ("$invite", RecordKind.STATE, INVITE_ROOM, "event"),
        ("$member", RecordKind.STATE, ROOM, None),
        ("$message", RecordKind.TIMELINE, ROOM, "event"),
        (None, RecordKind.EPHEMERAL, ROOM, "ephemeral"),
        (None, RecordKind.ROOM_ACCOUNT_DATA, ROOM, "room_account_data"),
        (None, RecordKind.PRESENCE, None, "presence"),
        (None, RecordKind.GLOBAL_ACCOUNT_DATA, None, "global_account_data"),
    )
    invite_snapshot = next(
        snapshot
        for snapshot in classic.room_snapshots
        if snapshot.room_id == INVITE_ROOM
    )
    assert tuple(
        (member.user_id, member.membership, member.display_name)
        for member in invite_snapshot.members
    ) == (
        (ACCOUNT, "invite", "Invited Alice"),
    )
    assert classic_client.invited_rooms[INVITE_ROOM].inviter == BOB
    invite_transition = next(
        value
        for value in classic.membership_transitions
        if value.room_id == INVITE_ROOM
    )
    assert _attrs(
        invite_transition, "source_kind", "event_id", "source_record_id", "source_json"
    ) == (
        ingest_model._MembershipSourceKind.STATE,
        "$invite",
        classic.records[1].record_id,
        classic.records[1].source_json,
    )
    assert _attrs(
        classic,
        "frame_id",
        "transport",
        "source_epoch",
        "request_id",
        "staged_revision",
        "request_cursor_json",
        "candidate_cursor_json",
        "source_sha256",
    ) == (
        classic_frame.frame_id,
        TransportKind.CLASSIC,
        classic_frame.origin.source_epoch,
        classic_frame.origin.request_id,
        9,
        classic_frame.request_cursor_json,
        classic_frame.candidate_cursor_json,
        classic_frame.source_sha256,
    )


def test_same_frame_preserves_every_reported_membership_transition_in_order(
    tmp_path: Path,
) -> None:
    frame = _multi_transition_frame()
    prior = RoomContinuity(ROOM, 7, "invite", None, None, None)

    with _attached_client(tmp_path / "transitions") as client:
        client.invited_rooms[ROOM] = MatrixInvitedRoom(ROOM, ACCOUNT)
        prepared = client._prepare_ingestion_frame(frame, 11, (prior,))

        assert tuple(
            (
                transition.event_id,
                transition.previous_membership,
                transition.current_membership,
                transition.previous_epoch,
                transition.current_epoch,
                transition.source_kind.value,
                (
                    transition.timeline_provenance.value
                    if transition.timeline_provenance is not None
                    else None
                ),
                transition.membership_provenance.value,
                transition.origin.frame_index,
                transition.source_record_id,
            )
            for transition in prepared.membership_transitions
        ) == (
            (
                "$join",
                "invite",
                "join",
                7,
                7,
                "state",
                None,
                "reported",
                0,
                prepared.records[0].record_id,
            ),
            (
                "$leave",
                "join",
                "leave",
                7,
                8,
                "timeline",
                "history",
                "reported",
                1,
                prepared.records[1].record_id,
            ),
            (
                None,
                "leave",
                "join",
                8,
                8,
                "timeline",
                "live",
                "reported",
                2,
                prepared.records[2].record_id,
            ),
            (
                "$ban",
                "join",
                "ban",
                8,
                9,
                "timeline",
                "live",
                "reported",
                3,
                prepared.records[3].record_id,
            ),
        )
        assert _attrs(
            prepared.room_snapshots[0], "own_membership", "membership_epoch"
        ) == ("ban", 9)
        assert client.invited_rooms[ROOM].room_id == ROOM


@pytest.mark.parametrize(
    ("section", "prior", "expected"),
    (
        (
            "leave",
            RoomContinuity(ROOM, 7, "join", None, None, None),
            ("join", "leave", 7, 8),
        ),
        ("join", None, (None, "join", 0, 0)),
        ("knock", None, (None, "knock", 0, 0)),
    ),
)
def test_section_membership_is_fallback_transition_evidence(
    tmp_path: Path,
    section: str,
    prior: RoomContinuity | None,
    expected: tuple[str | None, str, int, int],
) -> None:
    timeline = _event(
        "m.room.message",
        {"body": section, "msgtype": "m.text"},
        event_id=f"${section}",
        origin_server_ts=1,
        sender=BOB,
    )
    room_account_data = _event("m.tag", {"tags": {}})
    ephemeral = _event("m.typing", {"user_ids": [BOB]})
    frame = _normalize_frame(
        TransportKind.CLASSIC,
        36,
        {
            "rooms": {
                section: {
                    ROOM: {
                        "account_data": {"events": [room_account_data]},
                        "ephemeral": {"events": [ephemeral]},
                        "timeline": {"events": [timeline]},
                    }
                }
            }
        },
    )

    with _attached_client(tmp_path / section) as client:
        prepared = client._prepare_ingestion_frame(
            frame,
            12,
            () if prior is None else (prior,),
        )

    assert len(prepared.membership_transitions) == 1
    transition = prepared.membership_transitions[0]
    assert _attrs(
        transition,
        "previous_membership",
        "current_membership",
        "previous_epoch",
        "current_epoch",
        "source_record_id",
        "event_id",
        "timeline_provenance",
        "source_json",
    ) == (*expected, None, None, None, None)
    assert (transition.source_kind.value, transition.membership_provenance.value) == (
        "section",
        "reported",
    )
    assert transition.origin.frame_index == 0
    assert bool(prepared.room_snapshots) is (section in {"join", "knock"})
    assert tuple(
        record.callback_route.value if record.callback_route else None
        for record in prepared.records
    ) == (
        ("event", "ephemeral", "room_account_data")
        if section == "join"
        else (None, None, None)
    )


@pytest.mark.parametrize(
    "transport",
    (TransportKind.CLASSIC, TransportKind.SLIDING),
)
def test_room_key_precedes_same_frame_megolm_preparation(
    tmp_path: Path,
    transport: TransportKind,
) -> None:
    with _attached_client(tmp_path / transport.value) as client:
        to_device, encrypted_timeline, sender_store = _same_frame_crypto_material(
            client
        )
        try:
            frame = _crypto_frame(transport, to_device, encrypted_timeline)
            prepared = client._prepare_ingestion_frame(frame, 13, ())
        finally:
            sender_store.database.close()

    to_device_record = prepared.records[0]
    timeline_record = next(
        record for record in prepared.records if record.event_id == "$secret"
    )
    assert to_device_record.source_json == canonical_json(to_device)
    assert to_device_record.decryption.value == "decrypted"
    assert to_device_record.decryption_verified is None
    assert to_device_record.decrypted_to_device_kind.value == "room_key"
    assert to_device_record.clear_json is not None
    assert to_device_record.clear_json == canonical_json(
        json.loads(to_device_record.clear_json)
    )
    assert json.loads(to_device_record.clear_json)["type"] == "m.room_key"
    assert timeline_record.source_json == canonical_json(encrypted_timeline)
    assert timeline_record.event_id == "$secret"
    assert timeline_record.decryption.value == "decrypted"
    assert timeline_record.decryption_verified is True
    assert timeline_record.clear_json is not None
    assert timeline_record.clear_json == canonical_json(
        json.loads(timeline_record.clear_json)
    )
    assert json.loads(timeline_record.clear_json)["content"] == {
        "body": "same-frame secret",
        "msgtype": "m.text",
    }
    assert tuple(
        record.callback_route.value for record in _callback_records(prepared)
    ) == ("to_device", "event")
    assert tuple(record.record_id for record in _callback_records(prepared)) == (
        to_device_record.record_id,
        timeline_record.record_id,
    )
    assert _callback_records(prepared)[0] is to_device_record


@pytest.mark.parametrize(
    "transport",
    (TransportKind.CLASSIC, TransportKind.SLIDING),
)
def test_missing_megolm_session_is_an_explicit_preparation_failure(
    tmp_path: Path,
    transport: TransportKind,
) -> None:
    with _attached_client(tmp_path / f"missing-{transport.value}") as client:
        _to_device, encrypted_timeline, sender_store = _same_frame_crypto_material(
            client
        )
        try:
            frame = _crypto_frame(transport, None, encrypted_timeline)
            prepared = client._prepare_ingestion_frame(frame, 15, ())
        finally:
            sender_store.database.close()

    timeline_record = next(
        record for record in prepared.records if record.event_id == "$secret"
    )
    assert timeline_record.source_json == canonical_json(encrypted_timeline)
    assert timeline_record.event_id == "$secret"
    assert timeline_record.clear_json is None
    assert timeline_record.decryption.value == "megolm_failed"
    assert timeline_record.callback_route.value == "event"
    assert timeline_record in _callback_records(prepared)


@pytest.mark.parametrize(
    "transport",
    (TransportKind.CLASSIC, TransportKind.SLIDING),
)
def test_projected_encrypted_member_precedes_device_list_handling(
    tmp_path: Path,
    transport: TransportKind,
) -> None:
    frame = _crypto_frame(
        transport,
        None,
        _event(
            "m.room.message",
            {"body": "device-list", "msgtype": "m.text"},
            event_id="$device-list",
            origin_server_ts=4,
            sender=BOB,
        ),
        device_lists={"changed": [BOB], "left": []},
        key_counts={"signed_curve25519": 0},
    )

    with _attached_client(tmp_path / f"device-list-{transport.value}") as client:
        assert client.olm is not None
        client.olm.tracked_users.update({ACCOUNT, BOB})
        client.olm.users_for_key_query.clear()
        prepared = client._prepare_ingestion_frame(frame, 17, ())

        assert client.rooms[ROOM].encrypted is True
        assert BOB in client.rooms[ROOM].users
        assert client.olm.uploaded_key_count == 0
        assert client.users_for_key_query == {BOB}
        assert prepared.crypto_delta.users_for_key_query == (BOB,)
        assert prepared.crypto_delta.encrypted_room_ids == (ROOM,)


def test_callback_eligibility_is_frozen_by_preparation_phase(
    tmp_path: Path,
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    incoming = [
        _room_key_request("request", "automatic-request"),
        _room_key_request("request_cancellation", "automatic-request"),
    ]
    expired_source = {
        "content": {
            "code": "m.timeout",
            "reason": "Expired",
            "transaction_id": "expired-verification",
        },
        "sender": ACCOUNT,
    }
    expired = KeyVerificationCancel.from_dict(expired_source)
    collected_sources = [
        _room_key_request("request", "untrusted-request"),
        _room_key_request("request_cancellation", "earlier-untrusted-request"),
    ]
    collected = [ToDeviceEvent.parse_event(value) for value in collected_sources]
    assert isinstance(expired, KeyVerificationCancel)
    assert all(isinstance(event, ToDeviceEvent) for event in collected)
    frame = _to_device_frame(incoming, {"signed_curve25519": 5}, ["signed_curve25519"])

    def prepare(root: Path) -> tuple[object, list[str]]:
        phase_order: list[str] = []
        with _attached_client(root) as client:
            assert client.olm is not None
            olm = client.olm
            olm.uploaded_key_count = 23
            olm.users_for_key_query.update({"@zed:example.org", BOB})
            olm.outgoing_to_device_messages.append(
                ToDeviceMessage(
                    "org.example.generic",
                    BOB,
                    BOB_DEVICE,
                    {"value": "generic"},
                )
            )
            olm.key_request_devices_no_session.append(
                OlmDevice(
                    BOB,
                    BOB_DEVICE,
                    {"curve25519": "curve", "ed25519": "ed"},
                )
            )

            def clear_verifications() -> list[KeyVerificationCancel]:
                phase_order.append(f"expired:{olm.uploaded_key_count}")
                olm.outgoing_to_device_messages.append(
                    DummyMessage(
                        "m.room.encrypted",
                        BOB,
                        BOB_DEVICE,
                        {"ciphertext": "dummy"},
                    )
                )
                return [expired]

            def collect_key_requests() -> list[ToDeviceEvent]:
                phase_order.append(f"collected:{olm.uploaded_key_count}")
                olm.outgoing_to_device_messages.append(
                    RoomKeyRequestMessage(
                        "m.room_key_request",
                        BOB,
                        BOB_DEVICE,
                        collected_sources[0]["content"],
                        "untrusted-request",
                        "session-id",
                        ROOM,
                        "m.megolm.v1.aes-sha2",
                    )
                )
                return list(collected)  # type: ignore[arg-type]

            add_changed_users = olm.add_changed_users

            def capture_device_list(users: set[str]) -> None:
                phase_order.append(f"device-list:{olm.uploaded_key_count}")
                add_changed_users(users)

            monkeypatch.setattr(olm, "clear_verifications", clear_verifications)
            monkeypatch.setattr(olm, "add_changed_users", capture_device_list)
            monkeypatch.setattr(olm, "collect_key_requests", collect_key_requests)
            return client._prepare_ingestion_frame(frame, 19, ()), phase_order

    prepared, phase_order = prepare(tmp_path / "callbacks")
    replay, replay_phase_order = prepare(tmp_path / "callbacks-replay")

    assert phase_order == ["expired:23", "device-list:5", "collected:5"]
    assert replay_phase_order == phase_order
    assert tuple(
        record.callback_route.value if record.callback_route else None
        for record in prepared.records
    ) == (None, None, "to_device", "to_device", "to_device")
    assert tuple(record.source_json for record in prepared.records) == tuple(
        canonical_json(value)
        for value in [*incoming, expired_source, *collected_sources]
    )
    assert tuple(record.preparation_phase.value for record in prepared.records) == (
        "source",
        "source",
        "expired_verification",
        "collected_key_request",
        "collected_key_request",
    )
    assert tuple(record.effective_event_type for record in prepared.records) == (
        "m.room_key_request",
        "m.room_key_request",
        "m.key.verification.cancel",
        "m.room_key_request",
        "m.room_key_request",
    )
    assert "type" not in json.loads(prepared.records[2].source_json)
    assert tuple(record.record_id for record in replay.records) == tuple(
        record.record_id for record in prepared.records
    )
    synthetic_records = prepared.records[2:]
    assert _callback_records(prepared) == synthetic_records
    assert prepared.crypto_delta.users_for_key_query == (BOB, "@zed:example.org")
    assert prepared.crypto_delta.uploaded_key_count == 5
    assert (
        prepared.crypto_delta.unused_fallback_key_types_json == b'["signed_curve25519"]'
    )
    assert tuple(prepared.crypto_delta.key_claims[0]) == (
        BOB,
        BOB_DEVICE,
        False,
        True,
        (),
        (),
    )
    messages = prepared.crypto_delta.queued_to_device_messages
    assert tuple(
        (
            message.subtype.value,
            message.event_type,
            json.loads(message.content_json),
            _attrs(message, "request_id", "session_id", "room_id", "algorithm"),
        )
        for message in messages
    ) == (
        (
            "generic",
            "org.example.generic",
            {"value": "generic"},
            (None, None, None, None),
        ),
        (
            "dummy",
            "m.room.encrypted",
            {"ciphertext": "dummy"},
            (None, None, None, None),
        ),
        (
            "room_key_request",
            "m.room_key_request",
            collected_sources[0]["content"],
            ("untrusted-request", "session-id", ROOM, "m.megolm.v1.aes-sha2"),
        ),
    )
    assert all(
        _attrs(message, "recipient_user_id", "recipient_device_id") == (BOB, BOB_DEVICE)
        for message in messages
    )
    assert all(
        not hasattr(message, "body") and not hasattr(message, "transaction_id")
        for message in messages
    )


def test_decrypted_own_member_event_drives_timeline_transition(
    tmp_path: Path,
) -> None:
    prior = RoomContinuity(ROOM, 4, "join", None, None, None)
    with _attached_client(tmp_path / "decrypted-membership") as client:
        to_device, encrypted_timeline, sender_store = _same_frame_crypto_material(
            client,
            clear_event=_event(
                "m.room.member",
                {"membership": "leave"},
                state_key=ACCOUNT,
            ),
            event_id="$encrypted-member",
        )
        try:
            frame = _crypto_frame(
                TransportKind.CLASSIC,
                to_device,
                encrypted_timeline,
            )
            prepared = client._prepare_ingestion_frame(frame, 21, (prior,))
        finally:
            sender_store.database.close()

    timeline_record = next(
        record for record in prepared.records if record.event_id == "$encrypted-member"
    )
    assert timeline_record.clear_json is not None
    assert (timeline_record.source_json, timeline_record.decryption.value) == (
        canonical_json(encrypted_timeline),
        "decrypted",
    )
    clear = json.loads(timeline_record.clear_json)
    assert (
        clear["type"],
        clear["state_key"],
        clear["content"]["membership"],
        clear["event_id"],
    ) == ("m.room.member", ACCOUNT, "leave", "$encrypted-member")
    assert len(prepared.membership_transitions) == 1
    transition = prepared.membership_transitions[0]
    assert _attrs(
        transition,
        "event_id",
        "source_record_id",
        "previous_membership",
        "current_membership",
        "previous_epoch",
        "current_epoch",
    ) == ("$encrypted-member", timeline_record.record_id, "join", "leave", 4, 5)
    assert (
        transition.source_kind.value,
        transition.timeline_provenance.value,
        transition.origin.frame_index,
        transition.source_json,
    ) == (
        "timeline",
        "live",
        timeline_record.origin.frame_index,
        timeline_record.clear_json,
    )
    assert (
        timeline_record.callback_route.value == "event"
        and timeline_record in _callback_records(prepared)
    )
    assert _attrs(prepared.room_snapshots[0], "own_membership", "membership_epoch") == (
        "leave",
        5,
    )


def test_crypto_delta_retains_claim_local_apply_context(
    tmp_path: Path,
) -> None:
    waiting_sources = [
        _room_key_request("request", request_id)
        for request_id in ("waiting-request", "second-waiting-request")
    ]
    waiting_requests = [ToDeviceEvent.parse_event(source) for source in waiting_sources]
    rerequest_source, rerequest_event = _megolm_event("$missing-key", "missing-session")
    _duplicate_source, duplicate_event = _megolm_event("$duplicate", "missing-session")
    _second_source, second_rerequest_event = _megolm_event(
        "$second-key", "second-session"
    )
    assert all(isinstance(request, RoomKeyRequest) for request in waiting_requests)
    frame = _to_device_frame([])

    with _attached_client(tmp_path / "crypto-apply-context") as client:
        assert client.olm is not None
        olm = client.olm
        device = OlmDevice(BOB, BOB_DEVICE, {"curve25519": "curve", "ed25519": "ed"})
        olm.wedged_devices.append(device)
        olm.key_request_devices_no_session.append(device)
        for request in waiting_requests:
            olm.key_requests_waiting_for_session[(BOB, BOB_DEVICE)][request.request_id] = request  # type: ignore[union-attr]
        olm.key_re_requests_events[(BOB, BOB_DEVICE)].extend(
            [rerequest_event, duplicate_event, second_rerequest_event]
        )
        prepared = client._prepare_ingestion_frame(frame, 23, ())

    assert len(prepared.crypto_delta.key_claims) == 1
    claim = prepared.crypto_delta.key_claims[0]
    assert _attrs(claim, "user_id", "device_id", "was_wedged", "was_waiting") == (
        BOB,
        BOB_DEVICE,
        True,
        True,
    )
    assert tuple(request.request_id for request in claim.waiting_key_requests) == (
        "waiting-request",
        "second-waiting-request",
    )
    waiting = claim.waiting_key_requests[0]
    assert tuple(waiting) == (
        canonical_json(waiting_sources[0]),
        BOB,
        BOB_DEVICE,
        "waiting-request",
        ROOM,
        "sender-key",
        "session-id",
        "m.megolm.v1.aes-sha2",
    )
    assert prepared.crypto_delta.queued_to_device_messages == ()
    assert tuple(event.event_id for event in claim.rerequest_events) == (
        "$missing-key",
        "$second-key",
    )
    rerequest = claim.rerequest_events[0]
    assert tuple(rerequest) == (
        canonical_json(rerequest_source),
        ROOM,
        "$missing-key",
        BOB,
        BOB_DEVICE,
        "sender-key",
        "missing-session",
        "m.megolm.v1.aes-sha2",
    )


def test_crypto_delta_binds_rerequests_to_first_prequeued_dummy(
    tmp_path: Path,
) -> None:
    events = [
        _megolm_event("$first", "same-session")[1],
        _megolm_event("$duplicate", "same-session")[1],
        _megolm_event("$second", "second-session")[1],
    ]

    with _attached_client(tmp_path / "dummy-apply-context") as client:
        assert client.olm is not None
        client.olm.key_re_requests_events[(BOB, BOB_DEVICE)].extend(events)
        client.olm.outgoing_to_device_messages.extend(
            [
                DummyMessage(
                    "m.room.encrypted", BOB, BOB_DEVICE, {"ciphertext": "first-dummy"}
                ),
                DummyMessage(
                    "m.room.encrypted",
                    BOB,
                    BOB_DEVICE,
                    {"ciphertext": "duplicate-target-dummy"},
                ),
            ]
        )
        prepared = client._prepare_ingestion_frame(_to_device_frame([]), 25, ())

    assert prepared.crypto_delta.key_claims == ()
    first, duplicate = prepared.crypto_delta.queued_to_device_messages
    assert tuple(event.event_id for event in first.rerequest_events) == (
        "$first",
        "$second",
    )
    assert duplicate.rerequest_events == ()


def test_crypto_delta_rejects_unowned_waiting_key_requests(
    tmp_path: Path,
) -> None:
    waiting = ToDeviceEvent.parse_event(
        _room_key_request("request", "orphan-waiting-request")
    )
    assert isinstance(waiting, RoomKeyRequest)

    with _attached_client(tmp_path / "orphan-waiting") as client:
        assert client.olm is not None
        client.olm.key_requests_waiting_for_session[(BOB, BOB_DEVICE)][
            waiting.request_id
        ] = waiting

        with pytest.raises(
            ValueError,
            match="waiting room-key-request bucket has no claim owner",
        ):
            client._prepare_ingestion_frame(_to_device_frame([]), 26, ())


def test_decrypted_bad_to_device_payload_is_frozen_for_callback(
    tmp_path: Path,
) -> None:
    with _attached_client(tmp_path / "bad-to-device") as client:
        encrypted, _unused_timeline, sender_store = _same_frame_crypto_material(
            client,
            malformed_to_device=True,
        )

        try:
            frame = _crypto_frame(
                TransportKind.CLASSIC,
                encrypted,
                _event(
                    "m.room.message",
                    {"body": "clear", "msgtype": "m.text"},
                    event_id="$clear-after-bad-to-device",
                    origin_server_ts=10,
                    sender=BOB,
                ),
            )
            prepared = client._prepare_ingestion_frame(frame, 27, ())
        finally:
            sender_store.database.close()

    record = prepared.records[0]
    assert record.source_json == canonical_json(encrypted)
    assert record.clear_json is not None
    assert record.clear_json == canonical_json(json.loads(record.clear_json))
    clear = json.loads(record.clear_json)
    assert (clear["type"], clear["content"], clear["sender"]) == (
        "m.room_key",
        {},
        BOB,
    )
    assert record.decryption.value == "decrypted"
    assert record.decryption_verified is None
    assert record.decrypted_to_device_kind.value == "unknown_bad"
    assert record.effective_event_type == "m.room_key"
    assert record.callback_route is not None
    assert record.callback_route.value == "to_device"
    assert _callback_records(prepared)[0] is record


def test_omitted_otk_count_retains_effective_prior_value(
    tmp_path: Path,
) -> None:
    frame = _to_device_frame([], {})
    with _attached_client(tmp_path / "omitted-otk") as client:
        assert client.olm is not None
        client.olm.uploaded_key_count = 29
        prepared = client._prepare_ingestion_frame(frame, 29, ())

        assert client.olm.uploaded_key_count == 29
        assert prepared.crypto_delta.uploaded_key_count == 29
        assert prepared.crypto_delta.one_time_key_counts_json == b"{}"


def test_preparation_entrypoint_is_private_and_requires_owned_attachment() -> None:
    frame = _to_device_frame([])
    client = AsyncClient("https://example.org", ACCOUNT, DEVICE)
    client.restore_login(ACCOUNT, DEVICE, "secret-token")
    client.next_batch = "unchanged"

    with pytest.raises(
        LocalProtocolError,
        match="exact attached store and Olm",
    ):
        client._prepare_ingestion_frame(frame, 31, ())

    assert client.next_batch == "unchanged"
    assert hasattr(AsyncClient, "_prepare_ingestion_frame")
    assert not hasattr(AsyncClient, "prepare_ingestion_frame")
    assert not hasattr(nio, "_PreparedIngestionFrame")
    assert not hasattr(ingest_api, "_PreparedIngestionFrame")
    assert not hasattr(ingest_model, "_FrameCompletion")
    assert (
        ingest_model._PreparedIngestionRecord.__annotations__["effective_event_type"]
        is str
    )
    assert all(
        "_FrameCompletion" not in str(value)
        for value in ingest_model._PreparedIngestionFrame.__annotations__.values()
    )


def test_trailing_malformed_payload_rejects_before_any_mutation(
    tmp_path: Path,
) -> None:
    with _attached_client(tmp_path / "prevalidation") as client:
        assert client.olm is not None
        to_device, encrypted_timeline, sender_store = _same_frame_crypto_material(
            client
        )
        encrypted_content = encrypted_timeline["content"]
        assert isinstance(encrypted_content, dict)
        valid_frame = _crypto_frame(
            TransportKind.CLASSIC,
            to_device,
            encrypted_timeline,
        )
        frame = replace(
            valid_frame,
            to_device_json=valid_frame.to_device_json
            + (canonical_json({"content": {}, "sender": BOB}),),
        )
        client.next_batch = "unchanged"
        try:
            with pytest.raises(ValueError, match="event type"):
                client._prepare_ingestion_frame(frame, 33, ())
        finally:
            sender_store.database.close()

        assert client.next_batch == "unchanged"
        assert client.rooms == {}
        assert (
            client.olm.inbound_group_store.get(
                ROOM,
                encrypted_content["sender_key"],
                encrypted_content["session_id"],
            )
            is None
        )


def test_falsey_parsers_suppress_callbacks_but_keep_records_across_transports(
    tmp_path: Path,
) -> None:
    falsey_to_device = _event("org.example.empty", sender=BOB)
    invite = _event("org.example.invite", sender=BOB, state_key="")
    ephemeral = _event("org.example.ephemeral")
    frame = _normalize_frame(
        TransportKind.CLASSIC,
        34,
        {
            "to_device": {"events": [falsey_to_device]},
            "rooms": {
                "invite": {INVITE_ROOM: {"invite_state": {"events": [invite]}}},
                "join": {
                    ROOM: {
                        "ephemeral": {"events": [ephemeral]},
                    },
                },
            },
        },
    )
    orphan_account_data = _event("m.tag", {"tags": {}})
    orphan_typing = _event("m.typing", {"user_ids": [BOB]})
    orphan_frame = _normalize_frame(
        TransportKind.SLIDING,
        35,
        {
            "extensions": {
                "account_data": {"rooms": {ROOM: [orphan_account_data]}},
                "to_device": {"events": [falsey_to_device], "next_batch": "td3"},
                "typing": {"rooms": {ROOM: orphan_typing}},
            }
        },
    )

    with (
        _attached_client(tmp_path / "falsey-callbacks") as client,
        _attached_client(tmp_path / "orphan-callbacks") as orphan_client,
        _attached_client(tmp_path / "existing-callbacks") as existing_client,
    ):
        existing_client.rooms[ROOM] = MatrixRoom(ROOM, ACCOUNT)
        prepared = client._prepare_ingestion_frame(frame, 35, ())
        orphan = orphan_client._prepare_ingestion_frame(orphan_frame, 35, ())
        existing = existing_client._prepare_ingestion_frame(orphan_frame, 35, ())

    assert tuple(record.kind for record in prepared.records) == (
        RecordKind.TO_DEVICE,
        RecordKind.STATE,
        RecordKind.EPHEMERAL,
    )
    assert tuple(record.source_json for record in prepared.records) == (
        canonical_json(falsey_to_device),
        canonical_json(invite),
        canonical_json(ephemeral),
    )
    assert (
        prepared.records[0].effective_event_type
        == orphan.records[0].effective_event_type
        == "org.example.empty"
    )
    assert tuple(record.callback_route for record in prepared.records) == (
        None,
        None,
        None,
    )
    assert _callback_records(prepared) == ()
    assert tuple(record.kind for record in orphan.records) == (
        RecordKind.TO_DEVICE,
        RecordKind.EPHEMERAL,
        RecordKind.ROOM_ACCOUNT_DATA,
    )
    assert tuple(record.callback_route for record in orphan.records) == (
        None,
        None,
        None,
    )
    assert tuple(
        record.callback_route.value if record.callback_route else None
        for record in existing.records
    ) == (
        None,
        "ephemeral",
        "room_account_data",
    )
