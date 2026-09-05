import dataclasses
import hashlib
import json
from uuid import UUID, uuid5

import pytest

import nio.ingest as ingest
from nio.ingest import (
    BatchIntegrityError,
    BatchRef,
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
from nio.ingest.serialization import (
    _batch_from_payload,
    _canonical_room_snapshot_payload,
    _origin_from_dict,
    _room_snapshot_from_payload,
    batch_from_records,
)

CONSUMER_GENERATION = UUID("22222222-2222-2222-2222-222222222222")
OPERATION_ID = UUID("33333333-3333-3333-3333-333333333333")
STREAM_ID = UUID("44444444-4444-4444-4444-444444444444")

GOLDEN_EVENT_PAYLOAD = (
    b'{"schema_version":1,"account_id":"@alice:\xe4\xbe\x8b.org","device_id":"DEVICE",'
    b'"consumer_generation":"22222222-2222-2222-2222-222222222222",'
    b'"stream_id":"44444444-4444-4444-4444-444444444444","sequence":7,'
    b'"created_revision":11,"records":[{"record_type":"event","record_id":"$event",'
    b'"kind":"timeline","origin":{"origin_type":"transport","transport":"classic",'
    b'"source_epoch":2,"request_id":3,"frame_index":4},"room_id":"!room:example.org",'
    b'"membership_epoch":1,"room_sequence":5,"event_id":"$event","provenance":"live",'
    b'"source_json":"eyJib2R5IjoiY2Fmw6kifQ==","clear_json":null}]}'
)
GOLDEN_EVENT_SHA256 = "2b713f3e334eff766ba2d0299d64117e756ded54b787a5c60953231b8acb035b"
GOLDEN_ROOM_SNAPSHOT_PAYLOAD = (
    b'{"room_id":"!room:example.org","membership_epoch":3,'
    b'"own_user_id":"@me:example.org","own_membership":"join","encrypted":true,'
    b'"name":null,"canonical_alias":null,"topic":"Caf\xc3\xa9","avatar_url":null,'
    b'"join_rule":"invite","room_version":"12","guest_access":"forbidden",'
    b'"power_levels_json":"eyJ1c2VycyI6e319","members":['
    b'{"user_id":"@me:example.org","membership":"join","display_name":"Me",'
    b'"avatar_url":null,"power_level":100}]}'
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


def make_batch(*records: EventRecord | LossRecord) -> SyncBatch:
    return batch_from_records(
        account_id="@alice:例.org",
        device_id="DEVICE",
        consumer_generation=CONSUMER_GENERATION,
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
        "IngestionBlockedError",
        "IngestionError",
        "IngestionHydrationError",
        "IngestionSourceError",
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


def test_changing_one_record_byte_changes_digest_and_batch_id() -> None:
    original = make_batch(event_record(b"a"))
    changed = make_batch(event_record(b"b"))

    assert changed.ref.sha256 != original.ref.sha256
    assert changed.ref.batch_id != original.ref.batch_id


def test_sync_batch_recomputes_integrity_on_dataclass_replace() -> None:
    batch = make_batch(event_record())

    with pytest.raises(BatchIntegrityError, match="sha256"):
        dataclasses.replace(batch, ref=dataclasses.replace(batch.ref, sha256=b"x" * 32))


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
            CONSUMER_GENERATION,
            BatchRef(STREAM_ID, 1, STREAM_ID, b"d" * 32),
            1,
            (),
        )

    empty_payload = (
        b'{"schema_version":1,"account_id":"@alice:example.org",'
        b'"device_id":"DEVICE","consumer_generation":'
        b'"22222222-2222-2222-2222-222222222222","stream_id":'
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
            CONSUMER_GENERATION,
            BatchRef(STREAM_ID, 1, STREAM_ID, b"d" * 32),
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
            consumer_generation=CONSUMER_GENERATION,
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
            CONSUMER_GENERATION,
            BatchRef(STREAM_ID, 1, STREAM_ID, b"d" * 32),
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


def test_local_membership_lifecycle_event_round_trips() -> None:
    record = EventRecord(
        str(uuid5(OPERATION_ID, "nio:room-lifecycle:v1")),
        RecordKind.ROOM_LIFECYCLE,
        SystemOrigin(SystemOriginKind.MEMBERSHIP_CHANGE, OPERATION_ID),
        "!local:example.org",
        1,
        4,
        None,
        None,
        (
            b'{"event_id":null,"membership":"leave","membership_epoch":1,'
            b'"membership_provenance":"local","previous_membership":"join",'
            b'"previous_membership_epoch":0,"source_kind":"local",'
            b'"source_record_id":null,"timeline_provenance":null}'
        ),
        None,
    )
    payload = canonical_batch_payload(make_batch(record))

    assert _batch_from_payload(payload) == make_batch(record)


@pytest.mark.parametrize("wire_value", ("consumer_reset", "source_rebind"))
def test_retained_system_loss_origin_values_round_trip(wire_value: str) -> None:
    kind = SystemOriginKind(wire_value)
    origin = SystemOrigin(kind, OPERATION_ID)
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
        b'{"cause":"reset"}',
    )
    batch = make_batch(loss)
    payload = canonical_batch_payload(batch)

    assert f'"kind":"{wire_value}"'.encode() in payload
    assert _batch_from_payload(payload) == batch


def test_decode_rejects_membership_change_system_loss_origin() -> None:
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
    root = json.loads(canonical_batch_payload(make_batch(loss)))
    record = root["records"][0]
    membership_origin = SystemOrigin(
        SystemOriginKind.MEMBERSHIP_CHANGE,
        OPERATION_ID,
    )
    record["origin"]["kind"] = "membership_change"
    record["loss_id"] = loss_id(
        membership_origin,
        "!system:example.org",
        0,
        LossReason.BASELINE_LOST,
        boundary,
    )
    payload = json.dumps(
        root,
        ensure_ascii=False,
        separators=(",", ":"),
    ).encode()

    with pytest.raises(ValueError, match="membership"):
        _batch_from_payload(payload)


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
