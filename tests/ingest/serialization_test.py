import dataclasses
import hashlib
import json
from uuid import UUID, uuid5

import pytest

import nio.ingest as ingest
from nio.ingest import (
    BatchIntegrityError,
    BatchRef,
    ConsumerBinding,
    EventRecord,
    LossBoundary,
    LossReason,
    LossRecord,
    RecordKind,
    RecordOrigin,
    RoomMemberSnapshot,
    RoomSnapshot,
    SyncBatch,
    SystemOrigin,
    SystemOriginKind,
    TimelineEventProvenance,
    TransportKind,
    canonical_batch_payload,
)
from nio.ingest.membership import MembershipBaseline
from nio.ingest.serialization import (
    _batch_from_payload,
    _canonical_membership_baseline_payload,
    _canonical_room_snapshot_payload,
    _membership_baseline_from_payload,
    _membership_baseline_sha256,
    _origin_from_dict,
    _room_snapshot_from_payload,
    batch_from_records,
)

JOURNAL_GENERATION = UUID("11111111-1111-1111-1111-111111111111")
CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")
OPERATION_ID = UUID("33333333-3333-3333-3333-333333333333")
STREAM_ID = UUID("44444444-4444-4444-4444-444444444444")
BINDING = ConsumerBinding(JOURNAL_GENERATION, CONSUMER_GENERATION)

GOLDEN_EVENT_PAYLOAD = (
    b'{"schema_version":1,"account_id":"@alice:\xe4\xbe\x8b.org","device_id":"DEVICE",'
    b'"consumer":{"journal_generation":"11111111-1111-1111-1111-111111111111",'
    b'"consumer_generation":"22222222-2222-2222-2222-222222222222"},'
    b'"stream_id":"44444444-4444-4444-4444-444444444444","sequence":7,'
    b'"created_revision":11,"records":[{"record_type":"event","record_id":"$event",'
    b'"kind":"timeline","origin":{"origin_type":"transport","transport":"classic",'
    b'"source_epoch":2,"request_id":3,"frame_index":4},"room_id":"!room:example.org",'
    b'"membership_epoch":1,"room_sequence":5,"event_id":"$event","provenance":"live",'
    b'"source_json":"eyJib2R5IjoiY2Fmw6kifQ==","clear_json":null}]}'
)
GOLDEN_EVENT_SHA256 = "9ad8f6afc7fc80f6642aa08446d4217d42fc0fb318d4658ba9a8b6d4d8751748"
GOLDEN_SYSTEM_EVENT_ID = "3bc7e4ef-92f1-5ef3-8e86-f1e5f7708846"
GOLDEN_SYSTEM_EVENT_PAYLOAD = (
    b'{"schema_version":1,"account_id":"@alice:\xe4\xbe\x8b.org","device_id":"DEVICE",'
    b'"consumer":{"journal_generation":"11111111-1111-1111-1111-111111111111",'
    b'"consumer_generation":"22222222-2222-2222-2222-222222222222"},'
    b'"stream_id":"44444444-4444-4444-4444-444444444444","sequence":7,'
    b'"created_revision":11,"records":[{"record_type":"event",'
    b'"record_id":"3bc7e4ef-92f1-5ef3-8e86-f1e5f7708846",'
    b'"kind":"room_lifecycle","origin":{"origin_type":"system",'
    b'"kind":"membership_change",'
    b'"operation_id":"33333333-3333-3333-3333-333333333333"},'
    b'"room_id":"!room:example.org","membership_epoch":1,"room_sequence":5,'
    b'"event_id":null,"provenance":null,'
    b'"source_json":"eyJtZW1iZXJzaGlwIjoibGVhdmUifQ==","clear_json":null}]}'
)
GOLDEN_SYSTEM_EVENT_SHA256 = (
    "aab2b88a8de7b8a85f464b50bc1b48d10f004fd1500f6f2f9476e9cf6ce537f1"
)
GOLDEN_SYSTEM_BATCH_ID = UUID("a46c2db4-eb09-5144-9d42-814d81eb8b4f")

GOLDEN_ROOM_SNAPSHOT_PAYLOAD = (
    b'{"room_id":"!room:example.org","membership_epoch":3,'
    b'"own_user_id":"@me:example.org","own_membership":"join","encrypted":true,'
    b'"name":null,"canonical_alias":null,"topic":"Caf\xc3\xa9","avatar_url":null,'
    b'"join_rule":"invite","room_version":"12","guest_access":"forbidden",'
    b'"power_levels_json":"eyJ1c2VycyI6e319","members":['
    b'{"user_id":"@me:example.org","membership":"join","display_name":"Me",'
    b'"avatar_url":null,"power_level":100}]}'
)
GOLDEN_MEMBERSHIP_BASELINE_PAYLOAD = (
    b'{"room_id":"!room:example.org","source_epoch":4,"membership_epoch":7,'
    b'"prev_batch":"old-prev","membership_event_id":"$member"}'
)
GOLDEN_MEMBERSHIP_BASELINE_SHA256 = (
    "b062a60936591f6faf40441955d46e7269892d4d7f57aafa6b6e817a525ab198"
)


def event_record(source_json: bytes = b'{"body":"caf\xc3\xa9"}') -> EventRecord:
    return EventRecord(
        "$event",
        RecordKind.TIMELINE,
        RecordOrigin(TransportKind.CLASSIC, 2, 3, 4),
        "!room:example.org",
        1,
        5,
        "$event",
        TimelineEventProvenance.LIVE,
        source_json,
        None,
    )


def system_lifecycle_record() -> EventRecord:
    return EventRecord(
        GOLDEN_SYSTEM_EVENT_ID,
        RecordKind.ROOM_LIFECYCLE,
        SystemOrigin(SystemOriginKind.MEMBERSHIP_CHANGE, OPERATION_ID),
        "!room:example.org",
        1,
        5,
        None,
        None,
        b'{"membership":"leave"}',
        None,
    )


def hydration_state_record() -> EventRecord:
    return EventRecord(
        "a000241c-2af1-5da0-86c6-790f821c4c9d",
        RecordKind.STATE,
        SystemOrigin(SystemOriginKind.ROOM_HYDRATION, OPERATION_ID),
        "!room:example.org",
        1,
        5,
        "$state",
        None,
        b'{"type":"m.room.name"}',
        None,
    )


def room_readiness_record() -> EventRecord:
    return EventRecord(
        "0f791901-db7c-58ce-97a2-47c054dc3a6d",
        RecordKind.ROOM_READINESS,
        SystemOrigin(SystemOriginKind.ROOM_HYDRATION, OPERATION_ID),
        "!room:example.org",
        1,
        6,
        None,
        None,
        b'{"status":"ready"}',
        None,
    )


def make_batch(*records: EventRecord | LossRecord) -> SyncBatch:
    return batch_from_records(
        account_id="@alice:例.org",
        device_id="DEVICE",
        consumer=BINDING,
        stream_id=STREAM_ID,
        sequence=7,
        created_revision=11,
        records=records,
    )


def boundary_payload(boundary: LossBoundary) -> bytes:
    return json.dumps(
        {
            "prior_event_id": boundary.prior_event_id,
            "prior_origin_server_ts": boundary.prior_origin_server_ts,
            "start_token": boundary.start_token,
            "target_token": boundary.target_token,
        },
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()


def loss_id(
    origin: RecordOrigin | SystemOrigin,
    room_id: str,
    membership_epoch: int,
    reason: LossReason,
    boundary: LossBoundary,
) -> str:
    if isinstance(origin, RecordOrigin):
        origin_id = (
            f"transport:{origin.transport.value}:{origin.source_epoch}:"
            f"{origin.request_id}:{origin.frame_index}"
        )
    else:
        origin_id = f"system:{origin.kind.value}:{origin.operation_id}"
    boundary_sha256 = hashlib.sha256(boundary_payload(boundary)).hexdigest()
    return str(
        uuid5(
            STREAM_ID,
            f"{room_id}:{membership_epoch}:{origin_id}:{reason.value}:"
            f"{boundary_sha256}",
        )
    )


def test_public_ingest_exports_only_stable_contract_and_batch_helper() -> None:
    assert set(ingest.__all__) == {
        "BatchIntegrityError",
        "BatchRef",
        "ConsumerBinding",
        "ConsumerBootstrap",
        "EventRecord",
        "LossBoundary",
        "LossReason",
        "LossRecord",
        "RecordKind",
        "RecordOrigin",
        "RoomHydrationStatus",
        "RoomMemberSnapshot",
        "RoomSnapshot",
        "SyncBatch",
        "SystemOrigin",
        "SystemOriginKind",
        "TimelineEventProvenance",
        "TransportKind",
        "canonical_batch_payload",
    }
    assert not hasattr(ingest, "batch_from_records")
    assert not hasattr(ingest, "batch_from_payload")


def test_canonical_payload_pins_field_order_utf8_and_digest() -> None:
    batch = make_batch(event_record())

    assert canonical_batch_payload(batch) == GOLDEN_EVENT_PAYLOAD
    assert batch.ref.sha256.hex() == GOLDEN_EVENT_SHA256
    assert hashlib.sha256(GOLDEN_EVENT_PAYLOAD).hexdigest() == GOLDEN_EVENT_SHA256
    assert batch.ref.batch_id == uuid5(
        STREAM_ID,
        f"7:{GOLDEN_EVENT_SHA256}",
    )


def test_system_event_canonical_payload_and_identity_are_golden() -> None:
    batch = make_batch(system_lifecycle_record())

    assert canonical_batch_payload(batch) == GOLDEN_SYSTEM_EVENT_PAYLOAD
    assert batch.ref.sha256.hex() == GOLDEN_SYSTEM_EVENT_SHA256
    assert batch.ref.batch_id == GOLDEN_SYSTEM_BATCH_ID
    assert _batch_from_payload(GOLDEN_SYSTEM_EVENT_PAYLOAD) == batch


@pytest.mark.parametrize(
    "record_factory",
    (system_lifecycle_record, hydration_state_record, room_readiness_record),
)
def test_every_allowed_system_event_pair_round_trips_canonically(
    record_factory,
) -> None:
    batch = make_batch(record_factory())

    assert _batch_from_payload(canonical_batch_payload(batch)) == batch


def test_no_event_system_identity_binds_every_frozen_input() -> None:
    record = system_lifecycle_record()
    mutations = (
        dataclasses.replace(record, record_id="stale"),
        dataclasses.replace(record, record_id="é"),
        dataclasses.replace(record, room_id="!other:example.org"),
        dataclasses.replace(record, membership_epoch=2),
        dataclasses.replace(
            record,
            origin=SystemOrigin(
                SystemOriginKind.MEMBERSHIP_CHANGE,
                UUID("99999999-9999-9999-9999-999999999999"),
            ),
        ),
        dataclasses.replace(record, source_json=b'{"membership":"join"}'),
        dataclasses.replace(
            record,
            kind=RecordKind.ROOM_READINESS,
            origin=SystemOrigin(SystemOriginKind.ROOM_HYDRATION, OPERATION_ID),
        ),
    )

    for mutated in mutations:
        with pytest.raises(BatchIntegrityError, match="record_id"):
            make_batch(mutated)
    with pytest.raises(BatchIntegrityError, match="record_id"):
        batch_from_records(
            account_id="@alice:例.org",
            device_id="DEVICE",
            consumer=BINDING,
            stream_id=UUID("99999999-9999-9999-9999-999999999999"),
            sequence=7,
            created_revision=11,
            records=(record,),
        )

    same_identity = dataclasses.replace(
        record,
        provenance=TimelineEventProvenance.HISTORY,
        clear_json=b'{"decrypted":true}',
    )
    assert make_batch(same_identity).records == (same_identity,)


def test_hydration_state_identity_is_keyed_only_to_matrix_event_identity() -> None:
    record = hydration_state_record()

    assert make_batch(record).records == (record,)
    for mutated in (
        dataclasses.replace(record, record_id="stale"),
        dataclasses.replace(record, room_id="!other:example.org"),
        dataclasses.replace(record, event_id="$other"),
    ):
        with pytest.raises(BatchIntegrityError, match="record_id"):
            make_batch(mutated)
    with pytest.raises(BatchIntegrityError, match="record_id"):
        batch_from_records(
            account_id="@alice:例.org",
            device_id="DEVICE",
            consumer=BINDING,
            stream_id=UUID("99999999-9999-9999-9999-999999999999"),
            sequence=7,
            created_revision=11,
            records=(record,),
        )

    same_matrix_event = dataclasses.replace(
        record,
        origin=SystemOrigin(
            SystemOriginKind.ROOM_HYDRATION,
            UUID("99999999-9999-9999-9999-999999999999"),
        ),
        membership_epoch=2,
        room_sequence=6,
        source_json=b'{"type":"m.room.name","content":{"name":"Room"}}',
    )
    assert make_batch(same_matrix_event).records == (same_matrix_event,)


@pytest.mark.parametrize(
    "origin",
    (
        {
            "origin_type": "transport",
            "transport": "classic",
            "source_epoch": 1,
            "request_id": 2,
        },
        {
            "origin_type": "transport",
            "transport": "classic",
            "source_epoch": 1,
            "request_id": 2,
            "frame_index": 3,
            "extra": True,
        },
        {
            "transport": "classic",
            "origin_type": "transport",
            "source_epoch": 1,
            "request_id": 2,
            "frame_index": 3,
        },
        {
            "origin_type": "system",
            "kind": "membership_change",
        },
        {
            "origin_type": "system",
            "kind": "membership_change",
            "operation_id": str(OPERATION_ID),
            "extra": True,
        },
        {
            "kind": "membership_change",
            "origin_type": "system",
            "operation_id": str(OPERATION_ID),
        },
    ),
)
def test_origin_decode_requires_exact_ordered_fields(
    origin: dict[str, object],
) -> None:
    with pytest.raises(ValueError, match="origin.*fields"):
        _origin_from_dict(origin)


@pytest.mark.parametrize("removed", (b"consumer_reset", b"source_rebind"))
def test_decode_rejects_removed_system_origin_kinds(removed: bytes) -> None:
    payload = GOLDEN_SYSTEM_EVENT_PAYLOAD.replace(b"membership_change", removed)

    with pytest.raises(ValueError):
        _batch_from_payload(payload)


def test_changing_one_record_byte_changes_digest_and_batch_id() -> None:
    original = make_batch(event_record(b"a"))
    changed = make_batch(event_record(b"b"))

    assert changed.ref.sha256 != original.ref.sha256
    assert changed.ref.batch_id != original.ref.batch_id


def test_sync_batch_recomputes_integrity_on_dataclass_replace() -> None:
    batch = make_batch(event_record())

    with pytest.raises(BatchIntegrityError, match="sha256"):
        dataclasses.replace(batch, ref=dataclasses.replace(batch.ref, sha256=b""))


def test_batch_ref_digest_requires_exact_immutable_bytes() -> None:
    batch = make_batch(event_record())

    with pytest.raises(TypeError, match="sha256.*bytes"):
        dataclasses.replace(batch.ref, sha256=bytearray(batch.ref.sha256))  # type: ignore[arg-type]


def test_sync_batch_rejects_zero_records_on_construction_and_decode() -> None:
    with pytest.raises(ValueError, match="at least one record"):
        SyncBatch(
            1,
            "@alice:example.org",
            "DEVICE",
            BINDING,
            BatchRef(STREAM_ID, 1, STREAM_ID, b"digest"),
            1,
            (),
        )

    empty_payload = (
        b'{"schema_version":1,"account_id":"@alice:example.org",'
        b'"device_id":"DEVICE","consumer":{"journal_generation":'
        b'"11111111-1111-1111-1111-111111111111","consumer_generation":'
        b'"22222222-2222-2222-2222-222222222222"},"stream_id":'
        b'"44444444-4444-4444-4444-444444444444","sequence":1,'
        b'"created_revision":1,"records":[]}'
    )
    with pytest.raises(ValueError, match="at least one record"):
        _batch_from_payload(empty_payload)


def test_sync_batch_records_are_tuple_only() -> None:
    with pytest.raises(TypeError, match="records must be a tuple"):
        SyncBatch(
            1,
            "@alice:example.org",
            "DEVICE",
            BINDING,
            BatchRef(STREAM_ID, 1, STREAM_ID, b"digest"),
            1,
            [event_record()],  # type: ignore[arg-type]
        )

    batch = make_batch(event_record())
    with pytest.raises(TypeError, match="records.*EventRecord or LossRecord"):
        dataclasses.replace(batch, records=(object(),))  # type: ignore[arg-type]


@pytest.mark.parametrize(
    ("sequence", "created_revision"),
    [(1.5, 1), (1, 1.5)],
)
def test_batch_identity_fields_reject_floats(
    sequence: float | int,
    created_revision: float | int,
) -> None:
    with pytest.raises(TypeError, match="integer"):
        batch_from_records(
            account_id="@alice:example.org",
            device_id="DEVICE",
            consumer=BINDING,
            stream_id=STREAM_ID,
            sequence=sequence,  # type: ignore[arg-type]
            created_revision=created_revision,  # type: ignore[arg-type]
            records=(event_record(),),
        )


def test_sync_batch_rejects_unknown_schema_version_on_construction_and_decode() -> None:
    with pytest.raises(ValueError, match="schema_version"):
        SyncBatch(
            2,
            "@alice:example.org",
            "DEVICE",
            BINDING,
            BatchRef(STREAM_ID, 1, STREAM_ID, b"digest"),
            1,
            (event_record(),),
        )

    unsupported_payload = GOLDEN_EVENT_PAYLOAD.replace(
        b'"schema_version":1', b'"schema_version":2'
    )
    with pytest.raises(ValueError, match="schema_version"):
        _batch_from_payload(unsupported_payload)


def test_transport_and_system_loss_origins_round_trip() -> None:
    transport_origin = RecordOrigin(TransportKind.SLIDING, 8, 13, 2)
    transport_boundary = LossBoundary("$prior", 1_725_000_000_000, "s", "t")
    transport_loss = LossRecord(
        loss_id(
            transport_origin,
            "!transport:example.org",
            4,
            LossReason.EVENT_LIMIT,
            transport_boundary,
        ),
        transport_origin,
        "!transport:example.org",
        4,
        LossReason.EVENT_LIMIT,
        transport_boundary,
        b'{"limit":20}',
    )
    system_origin = SystemOrigin(SystemOriginKind.FRESH_START, OPERATION_ID)
    system_boundary = LossBoundary(None, None, None, None)
    system_loss = LossRecord(
        loss_id(
            system_origin,
            "!system:example.org",
            0,
            LossReason.BASELINE_LOST,
            system_boundary,
        ),
        system_origin,
        "!system:example.org",
        0,
        LossReason.BASELINE_LOST,
        system_boundary,
        b'{"cause":"reset"}',
    )
    batch = make_batch(transport_loss, system_loss)

    decoded = _batch_from_payload(canonical_batch_payload(batch))

    assert decoded == batch
    assert isinstance(decoded.records[0].origin, RecordOrigin)
    assert isinstance(decoded.records[1].origin, SystemOrigin)


def test_system_loss_rejects_kind_or_operation_inconsistent_with_loss_id() -> None:
    origin = SystemOrigin(SystemOriginKind.FRESH_START, OPERATION_ID)
    boundary = LossBoundary(None, None, None, None)
    valid_loss = LossRecord(
        loss_id(
            origin,
            "!room:example.org",
            0,
            LossReason.BASELINE_LOST,
            boundary,
        ),
        origin,
        "!room:example.org",
        0,
        LossReason.BASELINE_LOST,
        boundary,
        b"{}",
    )

    for inconsistent_origin in (
        SystemOrigin(SystemOriginKind.STORE_VALIDATION, OPERATION_ID),
        SystemOrigin(
            SystemOriginKind.FRESH_START,
            UUID("99999999-9999-9999-9999-999999999999"),
        ),
    ):
        with pytest.raises(BatchIntegrityError, match="loss_id"):
            make_batch(dataclasses.replace(valid_loss, origin=inconsistent_origin))


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b'"kind":"timeline"', b'"kind":"unknown"'),
        (b'"provenance":"live"', b'"provenance":"unknown"'),
        (b'"transport":"classic"', b'"transport":"unknown"'),
    ],
)
def test_decode_rejects_unknown_event_enum_strings(old: bytes, new: bytes) -> None:
    with pytest.raises(ValueError):
        _batch_from_payload(GOLDEN_EVENT_PAYLOAD.replace(old, new))


@pytest.mark.parametrize(
    ("old", "new"),
    [
        (b'"kind":"fresh_start"', b'"kind":"unknown"'),
        (b'"reason":"baseline_lost"', b'"reason":"unknown"'),
    ],
)
def test_decode_rejects_unknown_loss_enum_strings(old: bytes, new: bytes) -> None:
    origin = SystemOrigin(SystemOriginKind.FRESH_START, OPERATION_ID)
    boundary = LossBoundary(None, None, None, None)
    loss = LossRecord(
        loss_id(
            origin,
            "!system:example.org",
            0,
            LossReason.BASELINE_LOST,
            boundary,
        ),
        origin,
        "!system:example.org",
        0,
        LossReason.BASELINE_LOST,
        boundary,
        b"{}",
    )
    payload = canonical_batch_payload(make_batch(loss))

    with pytest.raises(ValueError):
        _batch_from_payload(payload.replace(old, new))


def test_room_snapshot_canonical_round_trip_includes_own_identity() -> None:
    snapshot = RoomSnapshot(
        "!room:example.org",
        3,
        "@me:example.org",
        "join",
        True,
        None,
        None,
        "Café",
        None,
        "invite",
        "12",
        "forbidden",
        b'{"users":{}}',
        (RoomMemberSnapshot("@me:example.org", "join", "Me", None, 100),),
    )

    payload = _canonical_room_snapshot_payload(snapshot)

    assert payload == GOLDEN_ROOM_SNAPSHOT_PAYLOAD
    assert _room_snapshot_from_payload(payload) == snapshot


def test_membership_baseline_has_one_strict_canonical_payload_and_digest() -> None:
    baseline = MembershipBaseline(
        "!room:example.org",
        4,
        7,
        "old-prev",
        "$member",
    )

    assert _canonical_membership_baseline_payload(baseline) == (
        GOLDEN_MEMBERSHIP_BASELINE_PAYLOAD
    )
    assert _membership_baseline_sha256(baseline).hex() == (
        GOLDEN_MEMBERSHIP_BASELINE_SHA256
    )
    assert (
        _membership_baseline_from_payload(GOLDEN_MEMBERSHIP_BASELINE_PAYLOAD)
        == baseline
    )
    for malformed in (
        GOLDEN_MEMBERSHIP_BASELINE_PAYLOAD.replace(
            b'"room_id":"!room:example.org","source_epoch":4',
            b'"source_epoch":4,"room_id":"!room:example.org"',
        ),
        GOLDEN_MEMBERSHIP_BASELINE_PAYLOAD.replace(
            b',"membership_event_id":"$member"',
            b"",
        ),
        GOLDEN_MEMBERSHIP_BASELINE_PAYLOAD.replace(
            b'"membership_event_id":"$member"',
            b'"membership_event_id":"$member","extra":true',
        ),
    ):
        with pytest.raises(ValueError, match="canonical"):
            _membership_baseline_from_payload(malformed)
